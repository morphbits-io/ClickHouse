#include <Storages/ObjectStorage/MorphConfiguration.h>

#include <Core/Settings.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Web/MorphObjectStorage.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Poco/URI.h>
#include <fmt/format.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool s3_ignore_file_doesnt_exist;
    extern const SettingsUInt64 s3_list_object_keys_size;
    extern const SettingsBool s3_skip_empty_files;
    extern const SettingsBool s3_throw_on_zero_files_match;
    extern const SettingsSchemaInferenceMode schema_inference_mode;
    extern const SettingsBool schema_inference_use_cache_for_s3;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

namespace
{

constexpr auto DEFAULT_MORPH_ENDPOINT = "https://morph.morphbits.io";
constexpr auto DEFAULT_PATH = "*.parquet";

String trimTrailingSlash(String value)
{
    while (!value.empty() && value.back() == '/')
        value.pop_back();
    return value;
}

}

void MorphStorageParsedArguments::fromNamedCollection(const NamedCollection & collection, ContextPtr)
{
    bucket = collection.get<String>("bucket");
    token = collection.get<String>("token");
    endpoint = trimTrailingSlash(collection.getOrDefault<String>("endpoint", DEFAULT_MORPH_ENDPOINT));

    format = collection.getOrDefault<String>("format", "auto");
    compression_method = collection.getOrDefault<String>("compression_method", collection.getOrDefault<String>("compression", "auto"));
    structure = collection.getOrDefault<String>("structure", "auto");
}

void MorphStorageParsedArguments::fromAST(ASTs & args, ContextPtr context, bool with_structure)
{
    if (args.size() < 2 || args.size() > getMaxNumberOfArguments(with_structure))
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Storage Morph requires 2 to {} arguments. All supported signatures:\n{}",
            getMaxNumberOfArguments(with_structure),
            getSignatures(with_structure));

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    bucket = checkAndGetLiteralArgument<String>(args[0], "bucket");
    token = checkAndGetLiteralArgument<String>(args[1], "token");

    if (args.size() > 2)
        format = checkAndGetLiteralArgument<String>(args[2], "format");

    if (with_structure)
    {
        if (args.size() > 3)
            structure = checkAndGetLiteralArgument<String>(args[3], "structure");
        if (args.size() > 4)
            compression_method = checkAndGetLiteralArgument<String>(args[4], "compression_method");
    }
    else if (args.size() > 3)
    {
        compression_method = checkAndGetLiteralArgument<String>(args[3], "compression_method");
    }

    endpoint = DEFAULT_MORPH_ENDPOINT;
}

String StorageMorphConfiguration::getDataSourceDescription() const
{
    return fmt::format("{}/{}", endpoint, bucket);
}

StorageObjectStorageQuerySettings StorageMorphConfiguration::getQuerySettings(const ContextPtr & context) const
{
    const auto & settings = context->getSettingsRef();
    return StorageObjectStorageQuerySettings{
        .truncate_on_insert = false,
        .create_new_file_on_insert = false,
        .schema_inference_use_cache = settings[Setting::schema_inference_use_cache_for_s3],
        .schema_inference_mode = settings[Setting::schema_inference_mode],
        .skip_empty_files = settings[Setting::s3_skip_empty_files],
        .list_object_keys_size = settings[Setting::s3_list_object_keys_size],
        .throw_on_zero_files_match = settings[Setting::s3_throw_on_zero_files_match],
        .ignore_non_existent_file = settings[Setting::s3_ignore_file_doesnt_exist],
    };
}

void StorageMorphConfiguration::check(ContextPtr context)
{
    validateNamespace(bucket);
    if (endpoint.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Morph endpoint cannot be empty");
    context->getRemoteHostFilter().checkURL(Poco::URI(endpoint));
    StorageObjectStorageConfiguration::check(context);
}

void StorageMorphConfiguration::validateNamespace(const String & name) const
{
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Morph bucket cannot be empty");
}

ObjectStoragePtr StorageMorphConfiguration::createObjectStorage(ContextPtr context, bool, CredentialsConfigurationCallback)
{
    assertInitialized();
    return std::make_shared<MorphObjectStorage>(endpoint, bucket, token, context);
}

void StorageMorphConfiguration::addStructureAndFormatToArgsIfNeeded(
    ASTs & args,
    const String & structure_,
    const String & format_,
    ContextPtr context,
    bool with_structure)
{
    if (auto collection = tryGetNamedCollectionWithOverrides(args, context))
    {
        if (collection->getOrDefault<String>("format", "auto") == "auto")
        {
            ASTs format_equal_func_args = {make_intrusive<ASTIdentifier>("format"), make_intrusive<ASTLiteral>(format_)};
            auto format_equal_func = makeASTOperator("equals", std::move(format_equal_func_args));
            args.push_back(format_equal_func);
        }

        if (with_structure && collection->getOrDefault<String>("structure", "auto") == "auto")
        {
            ASTs structure_equal_func_args = {make_intrusive<ASTIdentifier>("structure"), make_intrusive<ASTLiteral>(structure_)};
            auto structure_equal_func = makeASTOperator("equals", std::move(structure_equal_func_args));
            args.push_back(structure_equal_func);
        }

        return;
    }

    if (args.size() < 2 || args.size() > MorphStorageParsedArguments::getMaxNumberOfArguments(with_structure))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected 2 to {} arguments in table function morph, got {}",
            MorphStorageParsedArguments::getMaxNumberOfArguments(with_structure),
            args.size());

    auto format_literal = make_intrusive<ASTLiteral>(format_);
    auto structure_literal = make_intrusive<ASTLiteral>(structure_);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    if (args.size() == 2)
    {
        args.push_back(format_literal);
        if (with_structure)
            args.push_back(structure_literal);
    }
    else if (args.size() == 3)
    {
        if (checkAndGetLiteralArgument<String>(args[2], "format") == "auto")
            args[2] = format_literal;
        if (with_structure)
            args.push_back(structure_literal);
    }
    else if (with_structure && args.size() > 3 && checkAndGetLiteralArgument<String>(args[3], "structure") == "auto")
    {
        args[3] = structure_literal;
    }
}

void StorageMorphConfiguration::initializeFromParsedArguments(const MorphStorageParsedArguments & parsed_arguments)
{
    StorageObjectStorageConfiguration::initializeFromParsedArguments(parsed_arguments);
    bucket = parsed_arguments.bucket;
    token = parsed_arguments.token;
    endpoint = trimTrailingSlash(parsed_arguments.endpoint);
    raw_uri = fmt::format("{}/v1/buckets/{}", endpoint, bucket);
    path = DEFAULT_PATH;
}

void StorageMorphConfiguration::fromNamedCollection(const NamedCollection & collection, ContextPtr context)
{
    MorphStorageParsedArguments parsed_arguments;
    parsed_arguments.fromNamedCollection(collection, context);
    initializeFromParsedArguments(parsed_arguments);
    paths = {path};
}

void StorageMorphConfiguration::fromAST(ASTs & args, ContextPtr context, bool with_structure)
{
    MorphStorageParsedArguments parsed_arguments;
    parsed_arguments.fromAST(args, context, with_structure);
    initializeFromParsedArguments(parsed_arguments);
    paths = {path};
}

}
