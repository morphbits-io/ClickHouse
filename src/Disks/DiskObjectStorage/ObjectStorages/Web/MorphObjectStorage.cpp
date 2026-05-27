#include <Disks/DiskObjectStorage/ObjectStorages/Web/MorphObjectStorage.h>

#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Common/FieldVisitorToString.h>
#include <Common/logger_useful.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Web/ReadBufferFromMorphServer.h>
#include <Functions/IFunction.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Poco/DateTime.h>
#include <Poco/DateTimeParser.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>
#include <fmt/format.h>

#include <limits>
#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Morph allows at most 1000 objects per list response.
constexpr size_t MORPH_LIST_MAX_ITEMS = 1000;

/// Encode a single path segment while keeping '/' as a path separator (Morph
/// object names are file-like and may contain slashes).
String encodePathSegment(const String & value)
{
    String encoded;
    Poco::URI::encode(value, "/", encoded);
    return encoded;
}

/// Translate a single comparison node `func(input_col, const)` (or its
/// commutative form `func(const, input_col)`) into one
/// `MorphObjectStorage::QueryPredicate` carrying the SQL relation. The
/// row-group min/max layout is not visible here — the wire format only
/// names the original Parquet column. Returns an empty optional for
/// shapes we can't safely push down (functions of columns,
/// two-column comparisons, non-numeric columns, IN-with-subquery,
/// LIKE, NOT, etc.). Skipped conjuncts are silently dropped from the
/// pushdown set; ClickHouse re-evaluates them after the row-group
/// payload is opened.
std::optional<MorphObjectStorage::QueryPredicate> translateLeafComparison(
    const ActionsDAG::Node * node,
    const NamesAndTypesList & physical_columns)
{
    if (node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
        return std::nullopt;
    if (node->children.size() != 2)
        return std::nullopt;

    const String op_name = node->function_base->getName();
    if (op_name != "equals" && op_name != "notEquals"
        && op_name != "less" && op_name != "greater"
        && op_name != "lessOrEquals" && op_name != "greaterOrEquals")
        return std::nullopt;

    const ActionsDAG::Node * lhs = node->children[0];
    const ActionsDAG::Node * rhs = node->children[1];

    const ActionsDAG::Node * col_node = nullptr;
    const ActionsDAG::Node * const_node = nullptr;
    bool flipped = false;

    if (lhs->type == ActionsDAG::ActionType::INPUT && rhs->column && isColumnConst(*rhs->column))
    {
        col_node = lhs; const_node = rhs;
    }
    else if (rhs->type == ActionsDAG::ActionType::INPUT && lhs->column && isColumnConst(*lhs->column))
    {
        col_node = rhs; const_node = lhs;
        flipped = true;
    }
    else
        return std::nullopt;

    DataTypePtr col_type;
    for (const auto & c : physical_columns)
    {
        if (c.name == col_node->result_name)
        {
            col_type = c.type;
            break;
        }
    }
    if (!col_type)
        return std::nullopt;

    /// Numeric-only pushdown for now — Morph's row-group attributes
    /// store decimal-encoded numerics on which `MatchNum*` works.
    /// String-column pushdown is a future extension.
    DataTypePtr inner_type = removeNullable(col_type);
    if (!isNumber(*inner_type))
        return std::nullopt;

    Field f;
    const_node->column->get(0, f);
    String value_str = applyVisitor(FieldVisitorToString(), f);
    if (value_str.empty())
        return std::nullopt;
    /// `FieldVisitorToString` quotes strings ('foo', "foo"); for numerics
    /// it produces a bare decimal. Drop predicates whose stringified form
    /// looks quoted — pushdown is best-effort.
    if (value_str.front() == '\'' || value_str.front() == '"')
        return std::nullopt;

    String op = op_name;
    if (flipped)
    {
        if (op == "less") op = "greater";
        else if (op == "greater") op = "less";
        else if (op == "lessOrEquals") op = "greaterOrEquals";
        else if (op == "greaterOrEquals") op = "lessOrEquals";
    }

    String wire_op;
    if (op == "equals")              wire_op = "EQ";
    else if (op == "notEquals")      wire_op = "NE";
    else if (op == "less")           wire_op = "LT";
    else if (op == "lessOrEquals")   wire_op = "LE";
    else if (op == "greater")        wire_op = "GT";
    else if (op == "greaterOrEquals") wire_op = "GE";
    else
        return std::nullopt;

    return MorphObjectStorage::QueryPredicate{col_node->result_name, std::move(wire_op), std::move(value_str)};
}

void collectQueryPredicates(
    const ActionsDAG::Node * node,
    const NamesAndTypesList & physical_columns,
    std::vector<MorphObjectStorage::QueryPredicate> & out)
{
    if (!node)
        return;

    if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base)
    {
        const String op_name = node->function_base->getName();
        if (op_name == "and")
        {
            for (const auto * child : node->children)
                collectQueryPredicates(child, physical_columns, out);
            return;
        }
        if (op_name == "or")
        {
            /// NeoFS filters AND-combine; OR pushdown would need
            /// multiple search calls + union. Drop the disjunct
            /// entirely so the overall conjunctive predicate doesn't
            /// lose rows — ClickHouse still evaluates it post-load.
            return;
        }
    }

    if (auto leaf = translateLeafComparison(node, physical_columns))
        out.push_back(std::move(*leaf));
}

}

MorphObjectStorage::MorphObjectStorage(String endpoint_, String bucket_, String token_, ContextPtr context_)
    : endpoint(std::move(endpoint_))
    , bucket(std::move(bucket_))
    , token(std::move(token_))
    , context(std::move(context_))
    , log(getLogger("MorphObjectStorage"))
{
}

std::string MorphObjectStorage::getDescription() const
{
    return fmt::format("{}/{}", endpoint, bucket);
}

bool MorphObjectStorage::exists(const StoredObject & object) const
{
    return tryGetObjectMetadata(object.remote_path, /* with_tags */ false).has_value();
}

std::unique_ptr<ReadBufferFromFileBase> MorphObjectStorage::readObject(
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint) const
{
    /// Reads go through the Parquet-aware endpoint which serves the stored
    /// object verbatim — each object is already a complete, self-contained
    /// single-row-group Parquet file produced at upload time. So the size
    /// reported by `listObjects` is the right `Content-Length`. It is carried
    /// on the `StoredObject` (the channel the S3/Azure backends read), not
    /// `read_hint`, which `StorageObjectStorageSource` never sets on this path;
    /// pass it so `ReadBufferFromMorphServer::tryGetFileSize` can skip the
    /// per-read HEAD. Treat the unset sentinel as "unknown" and fall back to
    /// `read_hint`, then to `0`.
    const size_t file_size = object.bytes_size != std::numeric_limits<uint64_t>::max()
        ? object.bytes_size
        : read_hint.value_or(0);

    return std::make_unique<ReadBufferFromMorphServer>(
        makeParquetObjectURL(object.remote_path),
        context,
        file_size,
        patchSettings(read_settings),
        read_settings.remote_read_buffer_use_external_buffer,
        /* read_until_position */ 0,
        makeAuthHeaders());
}

std::unique_ptr<WriteBufferFromFileBase> MorphObjectStorage::writeObject(
    const StoredObject &,
    WriteMode,
    std::optional<ObjectAttributes>,
    size_t,
    const WriteSettings &)
{
    throwReadOnly();
}

void MorphObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    if (max_keys > 0 && children.size() >= max_keys)
        return;

    /// Schema inference / sample-path probing only need one
    /// representative object. Hit Morph's dedicated
    /// `v1GetParquetsMeta` endpoint instead of the full
    /// `v1SearchParquets` POST — the dedicated endpoint hard-wires
    /// `count=1`, takes only `prefix`, and avoids transmitting the
    /// (always empty in this mode) filter list.
    if (is_metadata_probe)
    {
        auto objects = fetchOneObjectByPrefix(path);
        children.insert(children.end(), std::make_move_iterator(objects.begin()), std::make_move_iterator(objects.end()));
        return;
    }

    const size_t remaining = max_keys > 0 ? max_keys - children.size() : 0;
    auto objects = fetchObjects(path, remaining);
    children.insert(children.end(), std::make_move_iterator(objects.begin()), std::make_move_iterator(objects.end()));
}

ObjectMetadata MorphObjectStorage::getObjectMetadata(const std::string & path, bool with_tags) const
{
    auto metadata = tryGetObjectMetadata(path, with_tags);
    if (!metadata)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "No such object in Morph bucket {}: {}", bucket, path);
    return *metadata;
}

std::optional<ObjectMetadata> MorphObjectStorage::tryGetObjectMetadata(const std::string & path, bool) const
{
    Poco::URI uri(makeObjectURL(path));

    /// `getFileInfo()` always issues a HEAD internally, regardless of the buffer's method.
    /// It also swallows 4xx responses (including 404) and returns an empty `HTTPFileInfo`.
    /// Morph's HEAD always sets `Content-Length` for existing objects, so a missing
    /// `file_size` after this call means "object not found / not readable".
    auto buffer = BuilderRWBufferFromHTTP(uri)
        .withConnectionGroup(HTTPConnectionGroupType::DISK)
        .withSettings(context->getReadSettings())
        .withTimeouts(ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
        .withHostFilter(&context->getRemoteHostFilter())
        .withHeaders(makeAuthHeaders())
        .create(Poco::Net::HTTPBasicCredentials{});

    const auto file_info = buffer->getFileInfo();
    if (!file_info.file_size.has_value())
        return std::nullopt;

    ObjectMetadata metadata;
    metadata.size_bytes = *file_info.file_size;
    if (file_info.last_modified.has_value())
        metadata.last_modified = Poco::Timestamp::fromEpochTime(*file_info.last_modified);

    return metadata;
}

void MorphObjectStorage::removeObjectIfExists(const StoredObject &)
{
    throwReadOnly();
}

void MorphObjectStorage::removeObjectsIfExist(const StoredObjects &)
{
    throwReadOnly();
}

void MorphObjectStorage::copyObject(const StoredObject &, const StoredObject &, const ReadSettings &, const WriteSettings &, std::optional<ObjectAttributes>)
{
    throwReadOnly();
}

ObjectStorageKeyGeneratorPtr MorphObjectStorage::createKeyGenerator() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "createKeyGenerator is not supported for {}", getName());
}

void MorphObjectStorage::throwReadOnly()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only read-only operations are supported in MorphObjectStorage");
}

HTTPHeaderEntries MorphObjectStorage::makeAuthHeaders() const
{
    HTTPHeaderEntries headers;
    if (!token.empty())
        headers.emplace_back("Authorization", fmt::format("Bearer {}", token));
    return headers;
}

String MorphObjectStorage::makeListURL() const
{
    return fmt::format("{}/v1/buckets/{}/objects", endpoint, encodePathSegment(bucket));
}

String MorphObjectStorage::makeParquetSearchURL() const
{
    return fmt::format("{}/v1/buckets/{}/parquets/search", endpoint, encodePathSegment(bucket));
}

String MorphObjectStorage::makeParquetMetaURL() const
{
    return fmt::format("{}/v1/buckets/{}/parquets/meta", endpoint, encodePathSegment(bucket));
}

void MorphObjectStorage::setQueryPredicate(const ActionsDAG::Node * predicate, const StorageMetadataPtr & metadata)
{
    query_predicates.clear();
    /// Schema inference / sample-path probing pass a null
    /// `StorageMetadataPtr`; data reads always pass a real one. One
    /// representative object is enough to recover the parquet schema,
    /// so `listObjects` dispatches to `fetchOneObjectByPrefix`
    /// (Morph's dedicated `v1GetParquetsMeta` endpoint) instead of
    /// the full `v1SearchParquets` POST.
    is_metadata_probe = !metadata;
    if (!predicate || !metadata)
        return;
    auto physical = metadata->getColumns().getAllPhysical();
    collectQueryPredicates(predicate, physical, query_predicates);
}

String MorphObjectStorage::makeObjectURL(const String & object_name) const
{
    return fmt::format("{}/v1/buckets/{}/objects/{}", endpoint, encodePathSegment(bucket), encodePathSegment(object_name));
}

String MorphObjectStorage::makeParquetObjectURL(const String & object_name) const
{
    return fmt::format("{}/v1/buckets/{}/parquets/{}", endpoint, encodePathSegment(bucket), encodePathSegment(object_name));
}

namespace
{

/// Morph's parquet endpoints all return `{objects: [{objectId, attributes}], …}`
/// with a fixed attribute set: FilePath (URL path) and payload length
/// (Content-Length hint for `readObject`). `$Object:creationEpoch` is also
/// returned but deliberately ignored — it's a NeoFS epoch index, not a
/// Unix timestamp, so feeding it into `last_modified` would store a
/// garbage calendar date.
constexpr auto kAttrFilePath      = "FilePath";
constexpr auto kAttrPayloadLength = "$Object:payloadLength";

/// Translate one entry from Morph's `objects` array into a
/// `RelativePathWithMetadata`. Returns nullptr (and logs a warning) when the
/// entry is malformed — a single bad entry shouldn't fail the whole listing.
RelativePathWithMetadataPtr parseSearchObjectEntry(const Poco::Dynamic::Var & entry_value, size_t index, const LoggerPtr & log)
{
    if (entry_value.type() != typeid(Poco::JSON::Object::Ptr))
    {
        LOG_WARNING(log, "Morph parquet response entry at index {} is not a JSON object, skipping", index);
        return nullptr;
    }

    const auto entry = entry_value.extract<Poco::JSON::Object::Ptr>();
    const auto attrs_value = entry->get("attributes");
    if (attrs_value.type() != typeid(Poco::JSON::Object::Ptr))
    {
        LOG_WARNING(log, "Morph parquet response entry at index {} has no `attributes` object, skipping", index);
        return nullptr;
    }
    const auto attrs = attrs_value.extract<Poco::JSON::Object::Ptr>();

    if (!attrs->has(kAttrFilePath))
    {
        LOG_WARNING(log, "Morph parquet response entry at index {} is missing FilePath attribute, skipping", index);
        return nullptr;
    }
    const auto fp_value = attrs->get(kAttrFilePath);
    if (!fp_value.isString())
    {
        LOG_WARNING(log, "Morph parquet response entry at index {} has non-string FilePath, skipping", index);
        return nullptr;
    }
    const auto object_path = fp_value.convert<String>();
    if (object_path.empty())
    {
        LOG_WARNING(log, "Morph parquet response entry at index {} has empty FilePath, skipping", index);
        return nullptr;
    }

    ObjectMetadata meta;
    if (attrs->has(kAttrPayloadLength))
    {
        const auto pl_value = attrs->get(kAttrPayloadLength);
        if (pl_value.isString())
        {
            try
            {
                meta.size_bytes = std::stoull(pl_value.convert<String>());
            }
            catch (...)
            {
                meta.is_size_known = false;
            }
        }
        else
        {
            meta.size_bytes = pl_value.convert<UInt64>();
        }
    }
    else
    {
        meta.is_size_known = false;
    }

    /// `last_modified` stays unset: see the leading-comment note about
    /// `$Object:creationEpoch` not being a Unix timestamp.

    return std::make_shared<RelativePathWithMetadata>(object_path, std::move(meta));
}

}

RelativePathsWithMetadata MorphObjectStorage::fetchObjects(const std::string & prefix, size_t max_keys) const
{
    RelativePathsWithMetadata objects;

    /// Build the JSON request body once. Cursor and pagination size go on
    /// the query string per the OpenAPI spec; the body itself is invariant
    /// between paginated calls. `prefix` and `filters` carry SQL-level data
    /// only — the row-group min/max layout is a server-side concern.
    Poco::JSON::Object::Ptr body = new Poco::JSON::Object;
    if (!prefix.empty())
        body->set("prefix", prefix);

    Poco::JSON::Array::Ptr filters_array = new Poco::JSON::Array;
    for (const auto & p : query_predicates)
    {
        Poco::JSON::Object::Ptr f = new Poco::JSON::Object;
        f->set("column", p.column);
        f->set("op", p.op);
        f->set("value", p.value);
        filters_array->add(f);
    }
    body->set("filters", filters_array);

    String body_str;
    {
        std::ostringstream oss;
        Poco::JSON::Stringifier::stringify(body, oss);
        body_str = oss.str();
    }

    auto auth_headers = makeAuthHeaders();
    /// Body is sent on every paginated POST; advertise its content type
    /// once and let the timeouts/auth share between calls.
    auth_headers.emplace_back("Content-Type", "application/json");

    const auto query_url = makeParquetSearchURL();
    const auto timeouts = ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings());

    String cursor;
    while (true)
    {
        size_t page_limit = MORPH_LIST_MAX_ITEMS;
        if (max_keys > 0)
        {
            const size_t remaining = max_keys > objects.size() ? max_keys - objects.size() : 0;
            if (remaining == 0)
                break;
            page_limit = std::min(page_limit, remaining);
        }

        Poco::URI uri(query_url);
        Poco::URI::QueryParameters query_parameters;
        query_parameters.emplace_back("maxItems", std::to_string(page_limit));
        if (!cursor.empty())
            query_parameters.emplace_back("cursor", cursor);
        uri.setQueryParameters(query_parameters);

        LOG_DEBUG(log, "Searching morph parquets: url={} filters={} body_size={}", uri.toString(), query_predicates.size(), body_str.size());

        auto buffer = BuilderRWBufferFromHTTP(uri)
            .withConnectionGroup(HTTPConnectionGroupType::DISK)
            .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
            .withSettings(context->getReadSettings())
            .withTimeouts(timeouts)
            .withHostFilter(&context->getRemoteHostFilter())
            .withHeaders(auth_headers)
            .withOutCallback([&](std::ostream & ostr) { ostr << body_str; })
            .withDelayInit(false)
            .withSkipNotFound(false)
            .create(Poco::Net::HTTPBasicCredentials{});

        String response;
        readStringUntilEOF(response, *buffer);
        if (response.empty())
        {
            LOG_DEBUG(log, "Morph parquet search response is empty, ending pagination");
            break;
        }

        Poco::JSON::Parser parser;
        const auto parsed = parser.parse(response);
        if (parsed.type() != typeid(Poco::JSON::Object::Ptr))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected Morph API response when searching parquets in {}", bucket);

        const auto root = parsed.extract<Poco::JSON::Object::Ptr>();
        const auto array_value = root->get("objects");
        if (array_value.type() != typeid(Poco::JSON::Array::Ptr))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Morph parquet search response for bucket {} has no `objects` array", bucket);

        const auto array = array_value.extract<Poco::JSON::Array::Ptr>();
        /// `Poco::JSON::Array::get` takes `unsigned int`; pages are capped at 1000 items.
        const auto array_size = static_cast<unsigned int>(array->size());
        size_t parsed_in_page = 0;
        for (unsigned int i = 0; i < array_size; ++i)
        {
            if (max_keys > 0 && objects.size() >= max_keys)
                break;

            if (auto entry = parseSearchObjectEntry(array->get(i), i, log))
            {
                objects.emplace_back(std::move(entry));
                ++parsed_in_page;
            }
        }

        cursor.clear();
        if (root->has("cursor"))
        {
            const auto cursor_value = root->get("cursor");
            if (cursor_value.isString())
                cursor = cursor_value.convert<String>();
        }

        LOG_DEBUG(
            log,
            "Morph parquet search response: response_bytes={} array_size={} parsed_in_page={} total_so_far={} next_cursor={}",
            response.size(),
            array_size,
            parsed_in_page,
            objects.size(),
            cursor.empty() ? String{"<none>"} : cursor);

        if (cursor.empty())
            break;
        if (max_keys > 0 && objects.size() >= max_keys)
            break;
    }

    return objects;
}

RelativePathsWithMetadata MorphObjectStorage::fetchOneObjectByPrefix(const std::string & prefix) const
{
    RelativePathsWithMetadata objects;

    Poco::URI uri(makeParquetMetaURL());
    Poco::URI::QueryParameters query_parameters;
    query_parameters.emplace_back("prefix", prefix);
    uri.setQueryParameters(query_parameters);

    LOG_DEBUG(log, "Fetching morph parquet meta: url={}", uri.toString());

    auto buffer = BuilderRWBufferFromHTTP(uri)
        .withConnectionGroup(HTTPConnectionGroupType::DISK)
        .withSettings(context->getReadSettings())
        .withTimeouts(ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
        .withHostFilter(&context->getRemoteHostFilter())
        .withHeaders(makeAuthHeaders())
        .withDelayInit(false)
        .withSkipNotFound(false)
        .create(Poco::Net::HTTPBasicCredentials{});

    String response;
    readStringUntilEOF(response, *buffer);
    if (response.empty())
        return objects;

    Poco::JSON::Parser parser;
    const auto parsed = parser.parse(response);
    if (parsed.type() != typeid(Poco::JSON::Object::Ptr))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected Morph API response when fetching parquet meta in {}", bucket);

    const auto root = parsed.extract<Poco::JSON::Object::Ptr>();
    const auto array_value = root->get("objects");
    if (array_value.type() != typeid(Poco::JSON::Array::Ptr))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Morph parquet meta response for bucket {} has no `objects` array", bucket);

    const auto array = array_value.extract<Poco::JSON::Array::Ptr>();
    /// The endpoint promises at most one object; defensively iterate in
    /// case the server ever loosens the contract.
    for (unsigned int i = 0; i < array->size(); ++i)
    {
        if (auto entry = parseSearchObjectEntry(array->get(i), i, log))
            objects.emplace_back(std::move(entry));
    }

    LOG_DEBUG(log, "Morph parquet meta response: response_bytes={} parsed={}", response.size(), objects.size());
    return objects;
}

}
