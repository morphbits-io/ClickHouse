#include <Disks/DiskObjectStorage/ObjectStorages/Web/MorphObjectStorage.h>

#include <Common/StringUtils/StringUtils.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/Context.h>

#include <array>

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

String urlEncode(const String & value)
{
    String encoded;
    Poco::URI::encode(value, "", encoded);
    return encoded;
}

std::optional<String> extractObjectName(const Poco::JSON::Object::Ptr & object)
{
    static constexpr std::array keys = {"object", "id", "name", "key", "path"};
    for (const auto & key : keys)
    {
        if (!object->has(key))
            continue;

        const auto value = object->get(key);
        if (value.isString())
            return value.convert<String>();

        if (value.type() == typeid(Poco::JSON::Object::Ptr))
        {
            const auto nested = value.extract<Poco::JSON::Object::Ptr>();
            if (nested->has("id") && nested->isString("id"))
                return nested->getValue<String>("id");
            if (nested->has("name") && nested->isString("name"))
                return nested->getValue<String>("name");
            if (nested->has("object") && nested->isString("object"))
                return nested->getValue<String>("object");
        }
    }

    return std::nullopt;
}

std::optional<ObjectMetadata> extractMetadata(const Poco::JSON::Object::Ptr & object)
{
    ObjectMetadata metadata;
    bool has_metadata = false;

    if (object->has("size"))
    {
        metadata.size_bytes = object->get("size").convert<UInt64>();
        has_metadata = true;
    }
    else if (object->has("size_bytes"))
    {
        metadata.size_bytes = object->get("size_bytes").convert<UInt64>();
        has_metadata = true;
    }
    else if (object->has("bytes"))
    {
        metadata.size_bytes = object->get("bytes").convert<UInt64>();
        has_metadata = true;
    }

    return has_metadata ? std::optional(metadata) : std::nullopt;
}

void addObjectsFromArray(const Poco::JSON::Array::Ptr & array, RelativePathsWithMetadata & result)
{
    for (size_t i = 0; i < array->size(); ++i)
    {
        const auto value = array->get(i);
        if (value.isString())
        {
            result.emplace_back(std::make_shared<RelativePathWithMetadata>(value.convert<String>()));
            continue;
        }

        if (value.type() != typeid(Poco::JSON::Object::Ptr))
            continue;

        const auto object = value.extract<Poco::JSON::Object::Ptr>();
        auto object_name = extractObjectName(object);
        if (!object_name.has_value() || object_name->empty())
            continue;

        result.emplace_back(std::make_shared<RelativePathWithMetadata>(*object_name, extractMetadata(object)));
    }
}

}

MorphObjectStorage::MorphObjectStorage(String endpoint_, String bucket_, String token_, ContextPtr context_)
    : endpoint(std::move(endpoint_))
    , bucket(std::move(bucket_))
    , token(std::move(token_))
    , context(std::move(context_))
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
    Poco::URI uri(makeObjectURL(object.remote_path));
    HTTPHeaderEntries headers{{"Authorization", fmt::format("Bearer {}", token)}};

    return BuilderRWBufferFromHTTP(uri)
        .withConnectionGroup(HTTPConnectionGroupType::DISK)
        .withSettings(patchSettings(read_settings))
        .withTimeouts(ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
        .withHostFilter(&context->getRemoteHostFilter())
        .withHeaders(headers)
        .withExternalBuf(read_settings.remote_read_buffer_use_external_buffer)
        .create(Poco::Net::HTTPBasicCredentials{});
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
    loadObjectsIfNeeded();

    std::shared_lock lock(objects_mutex);
    for (const auto & object : objects)
    {
        if (!startsWith(object->relative_path, path))
            continue;

        children.push_back(object);
        if (max_keys > 0 && children.size() >= max_keys)
            break;
    }
}

ObjectMetadata MorphObjectStorage::getObjectMetadata(const std::string & path, bool) const
{
    auto metadata = tryGetObjectMetadata(path, /* with_tags */ false);
    if (!metadata)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "No such object in Morph bucket {}: {}", bucket, path);
    return *metadata;
}

std::optional<ObjectMetadata> MorphObjectStorage::tryGetObjectMetadata(const std::string & path, bool) const
{
    loadObjectsIfNeeded();
    std::shared_lock lock(objects_mutex);
    for (const auto & object : objects)
    {
        if (object->relative_path == path)
            return object->metadata;
    }
    return std::nullopt;
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

String MorphObjectStorage::makeListURL() const
{
    return fmt::format("{}/v1/buckets/{}/objects", endpoint, urlEncode(bucket));
}

String MorphObjectStorage::makeObjectURL(const String & object_name) const
{
    return fmt::format("{}/v1/buckets/{}/objects/{}", endpoint, urlEncode(bucket), urlEncode(object_name));
}

void MorphObjectStorage::loadObjectsIfNeeded() const
{
    {
        std::shared_lock lock(objects_mutex);
        if (objects_loaded)
            return;
    }

    std::unique_lock lock(objects_mutex);
    if (objects_loaded)
        return;

    Poco::URI list_uri(makeListURL());
    HTTPHeaderEntries headers{{"Authorization", fmt::format("Bearer {}", token)}};
    auto buffer = BuilderRWBufferFromHTTP(list_uri)
        .withConnectionGroup(HTTPConnectionGroupType::DISK)
        .withSettings(context->getReadSettings())
        .withTimeouts(ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
        .withHostFilter(&context->getRemoteHostFilter())
        .withHeaders(headers)
        .withDelayInit(false)
        .withSkipNotFound(false)
        .create(Poco::Net::HTTPBasicCredentials{});

    String response;
    readStringUntilEOF(response, *buffer);

    if (!response.empty())
    {
        Poco::JSON::Parser parser;
        const auto parsed = parser.parse(response);
        if (parsed.type() == typeid(Poco::JSON::Array::Ptr))
        {
            addObjectsFromArray(parsed.extract<Poco::JSON::Array::Ptr>(), objects);
        }
        else if (parsed.type() == typeid(Poco::JSON::Object::Ptr))
        {
            const auto root = parsed.extract<Poco::JSON::Object::Ptr>();
            static constexpr std::array list_keys = {"objects", "items", "results", "data"};
            bool found_array = false;
            for (const auto & key : list_keys)
            {
                if (!root->has(key))
                    continue;
                const auto value = root->get(key);
                if (value.type() == typeid(Poco::JSON::Array::Ptr))
                {
                    addObjectsFromArray(value.extract<Poco::JSON::Array::Ptr>(), objects);
                    found_array = true;
                    break;
                }
            }

            if (!found_array)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot extract object list from Morph API response");
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected Morph API response type when listing objects");
        }
    }

    objects_loaded = true;
}

}
