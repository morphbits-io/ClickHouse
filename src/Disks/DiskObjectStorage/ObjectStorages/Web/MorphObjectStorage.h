#pragma once

#include <Common/Logger.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/HTTPHeaderEntries.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class MorphObjectStorage : public IObjectStorage
{
public:
    MorphObjectStorage(String endpoint_, String bucket_, String token_, ContextPtr context_);

    std::string getName() const override { return "Morph"; }
    ObjectStorageType getType() const override { return ObjectStorageType::Web; }
    std::string getCommonKeyPrefix() const override { return {}; }
    std::string getDescription() const override;

    bool exists(const StoredObject & object) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject(
        const StoredObject & object,
        const ReadSettings & read_settings,
        std::optional<size_t> read_hint = {}) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeObject(
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const override;

    ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override;
    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override;

    void removeObjectIfExists(const StoredObject & object) override;
    void removeObjectsIfExist(const StoredObjects & objects) override;

    void copyObject(
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override {}
    void startup() override {}

    String getObjectsNamespace() const override { return bucket; }
    ObjectStorageKeyGeneratorPtr createKeyGenerator() const override;

    bool isRemote() const override { return true; }
    bool isReadOnly() const override { return true; }

private:
    [[noreturn]] static void throwReadOnly();

    HTTPHeaderEntries makeAuthHeaders() const;
    String makeListURL() const;
    String makeObjectURL(const String & object_name) const;
    RelativePathsWithMetadata fetchObjects(const std::string & prefix, size_t max_keys) const;

    const String endpoint;
    const String bucket;
    const String token;
    const ContextPtr context;
    const LoggerPtr log;
};

}
