#pragma once

#include <Common/Logger.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/HTTPHeaderEntries.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <vector>

namespace DB
{

class ActionsDAG;

class MorphObjectStorage : public IObjectStorage
{
public:
    /// One conjunct of a WHERE-clause predicate ready to be sent to
    /// Morph's `v1SearchParquets` endpoint. Note `column` is the
    /// original Parquet column name and `op` is the SQL relation
    /// (`EQ`/`NE`/`LT`/`LE`/`GT`/`GE`). The mapping to row-group
    /// statistic attributes (`<col>_minValue`/`_maxValue`) and to the
    /// NeoFS `MatchNum*` types lives entirely server-side; this struct
    /// is what travels over the wire.
    struct QueryPredicate
    {
        String column;
        String op;
        String value;
    };

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

    /// Walk the query's filter ActionsDAG, extract pushable
    /// conjuncts, and stash them as `QueryPredicate` triples that the
    /// next `listObjects` call will POST to Morph's
    /// `v1SearchParquets` endpoint. A null `predicate` clears the
    /// stash, so the next listing posts `v1SearchParquets` with an
    /// empty `filters` array (effectively a list-by-prefix). The
    /// signature deliberately accepts only ClickHouse-native types —
    /// nothing about the row-group min/max representation leaks into
    /// the caller (`StorageObjectStorageSource`).
    void setQueryPredicate(const ActionsDAG::Node * predicate, const StorageMetadataPtr & metadata) override;

private:
    [[noreturn]] static void throwReadOnly();

    HTTPHeaderEntries makeAuthHeaders() const;
    String makeListURL() const;
    String makeParquetSearchURL() const;
    /// URL of the dedicated schema-inference endpoint that returns metadata
    /// for at most one row-group object under the given prefix. Used by
    /// `fetchOneObjectByPrefix` instead of the full `v1SearchParquets` POST
    /// when only a representative object is needed.
    String makeParquetMetaURL() const;
    String makeObjectURL(const String & object_name) const;
    /// URL of the Parquet-aware variant of the object endpoint. Each stored
    /// row-group object is already a complete, self-contained
    /// single-row-group Parquet file (synthesized once at upload time on
    /// the Morph side), so this endpoint streams the bytes verbatim for
    /// ClickHouse's standard Parquet reader.
    String makeParquetObjectURL(const String & object_name) const;
    RelativePathsWithMetadata fetchObjects(const std::string & prefix, size_t max_keys) const;
    /// Schema-inference shortcut: GET Morph's `v1GetParquetsMeta` endpoint
    /// and return at most one row-group object's metadata. Always sends a
    /// non-empty prefix; never paginates.
    RelativePathsWithMetadata fetchOneObjectByPrefix(const std::string & prefix) const;

    const String endpoint;
    const String bucket;
    const String token;
    const ContextPtr context;
    const LoggerPtr log;

    /// Stashed by `setQueryPredicate`. `fetchObjects` consumes it.
    std::vector<QueryPredicate> query_predicates;

    /// `true` when the listing serves schema inference / sample-path
    /// probing rather than data reading — `setQueryPredicate` infers
    /// it from a null `StorageMetadataPtr`. In that mode `listObjects`
    /// dispatches to `fetchOneObjectByPrefix` (the dedicated
    /// `v1GetParquetsMeta` endpoint) instead of the full
    /// `v1SearchParquets` POST.
    bool is_metadata_probe = false;
};

}
