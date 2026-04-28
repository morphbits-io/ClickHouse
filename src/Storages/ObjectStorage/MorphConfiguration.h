#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ObjectStorage/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

struct MorphStorageParsedArguments : private StorageParsedArguments
{
    friend class StorageMorphConfiguration;

    static constexpr auto max_number_of_arguments_with_structure = 5;
    static constexpr auto signatures_with_structure
        = " - bucket, token\n"
          " - bucket, token, format\n"
          " - bucket, token, format, structure\n"
          " - bucket, token, format, structure, compression_method\n";

    static constexpr auto max_number_of_arguments_without_structure = 4;
    static constexpr auto signatures_without_structure
        = " - bucket, token\n"
          " - bucket, token, format\n"
          " - bucket, token, format, compression_method\n";

    static constexpr const char * getSignatures(bool with_structure = true)
    {
        return with_structure ? signatures_with_structure : signatures_without_structure;
    }

    static constexpr size_t getMaxNumberOfArguments(bool with_structure = true)
    {
        return with_structure ? max_number_of_arguments_with_structure : max_number_of_arguments_without_structure;
    }

    String bucket;
    String token;
    String endpoint;
    String path;

    void fromNamedCollection(const NamedCollection & collection, ContextPtr context);
    void fromAST(ASTs & args, ContextPtr context, bool with_structure);
};

class StorageMorphConfiguration : public StorageObjectStorageConfiguration
{
public:
    static constexpr auto type = ObjectStorageType::Web;
    static constexpr auto type_name = "morph";
    static constexpr auto engine_name = "Morph";

    StorageMorphConfiguration() = default;

    ObjectStorageType getType() const override { return type; }
    std::string getTypeName() const override { return type_name; }
    std::string getEngineName() const override { return engine_name; }
    std::string getNamespaceType() const override { return "bucket"; }

    Path getRawPath() const override { return path; }
    void setRawPath(const Path & path_) override { path = path_; }
    const String & getRawURI() const override { return raw_uri; }

    const Paths & getPaths() const override { return paths; }
    void setPaths(const Paths & paths_) override
    {
        paths = paths_;
    }

    String getNamespace() const override { return bucket; }
    String getDataSourceDescription() const override;
    StorageObjectStorageQuerySettings getQuerySettings(const ContextPtr & context) const override;

    void check(ContextPtr context) override;
    void validateNamespace(const String & name) const override;
    bool supportsWrites() const override { return false; }

    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly, CredentialsConfigurationCallback refresh_credentials_callback) override;

    void addStructureAndFormatToArgsIfNeeded(
        ASTs & args, const String & structure_, const String & format_, ContextPtr context, bool with_structure) override;

private:
    void initializeFromParsedArguments(const MorphStorageParsedArguments & parsed_arguments);
    void fromNamedCollection(const NamedCollection & collection, ContextPtr context) override;
    void fromAST(ASTs & args, ContextPtr context, bool with_structure) override;

    String bucket;
    String token;
    String endpoint;
    String raw_uri;
    Path path;
    Paths paths;
};

}
