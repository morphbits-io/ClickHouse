#include <Disks/DiskObjectStorage/ObjectStorages/Web/MorphObjectStorage.h>

#include <Common/logger_useful.h>
#include <Disks/IO/ReadBufferFromWebServer.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/Context.h>

#include <Poco/DateTime.h>
#include <Poco/DateTimeParser.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/URI.h>
#include <fmt/format.h>

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
    std::optional<size_t>) const
{
    return std::make_unique<ReadBufferFromWebServer>(
        makeObjectURL(object.remote_path),
        context,
        object.bytes_size,
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

String MorphObjectStorage::makeObjectURL(const String & object_name) const
{
    return fmt::format("{}/v1/buckets/{}/objects/{}", endpoint, encodePathSegment(bucket), encodePathSegment(object_name));
}

RelativePathsWithMetadata MorphObjectStorage::fetchObjects(const std::string & prefix, size_t max_keys) const
{
    RelativePathsWithMetadata objects;
    const auto auth_headers = makeAuthHeaders();
    const auto list_url = makeListURL();
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

        Poco::URI uri(list_url);
        Poco::URI::QueryParameters query_parameters;
        if (!prefix.empty())
            query_parameters.emplace_back("prefix", prefix);
        query_parameters.emplace_back("maxItems", std::to_string(page_limit));
        if (!cursor.empty())
            query_parameters.emplace_back("cursor", cursor);
        uri.setQueryParameters(query_parameters);

        LOG_DEBUG(log, "Listing morph objects: url={}", uri.toString());

        auto buffer = BuilderRWBufferFromHTTP(uri)
            .withConnectionGroup(HTTPConnectionGroupType::DISK)
            .withSettings(context->getReadSettings())
            .withTimeouts(timeouts)
            .withHostFilter(&context->getRemoteHostFilter())
            .withHeaders(auth_headers)
            .withDelayInit(false)
            .withSkipNotFound(false)
            .create(Poco::Net::HTTPBasicCredentials{});

        String response;
        readStringUntilEOF(response, *buffer);
        if (response.empty())
        {
            LOG_DEBUG(log, "Morph list response is empty, ending pagination");
            break;
        }

        Poco::JSON::Parser parser;
        const auto parsed = parser.parse(response);
        if (parsed.type() != typeid(Poco::JSON::Object::Ptr))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected Morph API response when listing objects in {}", bucket);

        const auto root = parsed.extract<Poco::JSON::Object::Ptr>();
        const auto array_value = root->get("objects");
        if (array_value.type() != typeid(Poco::JSON::Array::Ptr))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Morph API response for bucket {} has no `objects` array", bucket);

        const auto array = array_value.extract<Poco::JSON::Array::Ptr>();
        /// `Poco::JSON::Array::get` takes `unsigned int`; pages are capped at 1000 items.
        const auto array_size = static_cast<unsigned int>(array->size());
        size_t parsed_in_page = 0;
        for (unsigned int i = 0; i < array_size; ++i)
        {
            if (max_keys > 0 && objects.size() >= max_keys)
                break;

            const auto entry_value = array->get(i);
            if (entry_value.type() != typeid(Poco::JSON::Object::Ptr))
            {
                LOG_WARNING(log, "Morph list entry at index {} is not a JSON object, skipping", i);
                continue;
            }

            const auto entry = entry_value.extract<Poco::JSON::Object::Ptr>();
            if (!entry->has("path"))
            {
                LOG_WARNING(log, "Morph list entry at index {} is missing `path` field, skipping", i);
                continue;
            }
            const auto path_value = entry->get("path");
            if (!path_value.isString())
            {
                LOG_WARNING(log, "Morph list entry at index {} has non-string `path`, skipping", i);
                continue;
            }

            const auto object_path = path_value.convert<String>();
            if (object_path.empty())
            {
                LOG_WARNING(log, "Morph list entry at index {} has empty `path`, skipping", i);
                continue;
            }

            ObjectMetadata meta;
            if (entry->has("size"))
                meta.size_bytes = entry->get("size").convert<UInt64>();
            else
                meta.is_size_known = false;

            if (entry->has("creationDate"))
            {
                const auto creation_value = entry->get("creationDate");
                if (creation_value.isString())
                {
                    /// Morph returns ISO-8601 like "2026-04-28T12:00:03+04:00"; best-effort parse.
                    int tz_offset = 0;
                    Poco::DateTime dt;
                    if (Poco::DateTimeParser::tryParse(creation_value.convert<String>(), dt, tz_offset))
                        meta.last_modified = Poco::Timestamp::fromEpochTime(dt.timestamp().epochTime());
                }
            }

            objects.emplace_back(std::make_shared<RelativePathWithMetadata>(object_path, std::move(meta)));
            ++parsed_in_page;
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
            "Morph list response: response_bytes={} array_size={} parsed_in_page={} total_so_far={} next_cursor={}",
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

}
