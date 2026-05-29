#include <Disks/DiskObjectStorage/ObjectStorages/Web/ReadBufferFromMorphServer.h>

#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <IO/Operators.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WithFileSize.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace Setting
{
    extern const SettingsSeconds http_connection_timeout;
    extern const SettingsSeconds http_receive_timeout;
}

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}


ReadBufferFromMorphServer::ReadBufferFromMorphServer(
    const String & url_,
    ContextPtr context_,
    size_t file_size_,
    const ReadSettings & settings_,
    bool use_external_buffer_,
    size_t read_until_position_,
    HTTPHeaderEntries headers_)
    : ReadBufferFromFileBase(settings_.remote_fs_buffer_size, nullptr, 0, file_size_)
    , log(getLogger("ReadBufferFromMorphServer"))
    , context(context_)
    , url(url_)
    , buf_size(settings_.remote_fs_buffer_size)
    , read_settings(settings_)
    , headers(std::move(headers_))
    , use_external_buffer(use_external_buffer_)
    , read_until_position(read_until_position_)
{
}


std::unique_ptr<SeekableReadBuffer> ReadBufferFromMorphServer::initialize()
{
    Poco::URI uri(url);
    if (read_until_position)
    {
        if (read_until_position < offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset.load(), read_until_position - 1);
    }

    const auto & settings = context->getSettingsRef();
    const auto & server_settings = context->getServerSettings();

    auto connection_timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, server_settings);
    connection_timeouts.withConnectionTimeout(std::max<Poco::Timespan>(settings[Setting::http_connection_timeout], Poco::Timespan(20, 0)));
    connection_timeouts.withReceiveTimeout(std::max<Poco::Timespan>(settings[Setting::http_receive_timeout], Poco::Timespan(20, 0)));

    /// Always lazy-init: the eager path performs an HTTP GET in the
    /// constructor that ignores the `setReadUntilPosition` / `seek` calls
    /// queued below, and any subsequent `setReadUntilPosition` discards the
    /// pre-fetched bytes anyway. Lazy init lets the first `impl->next()` issue
    /// a single GET that respects the final offset and right bound.
    auto res = BuilderRWBufferFromHTTP(uri)
                   .withConnectionGroup(HTTPConnectionGroupType::DISK)
                   .withSettings(read_settings)
                   .withTimeouts(connection_timeouts)
                   .withBufSize(buf_size)
                   .withHostFilter(&context->getRemoteHostFilter())
                   .withHeaders(headers)
                   .withDelayInit(true)
                   .withExternalBuf(use_external_buffer)
                   .create(credentials);

    if (read_until_position)
        res->setReadUntilPosition(read_until_position);
    if (offset)
        res->seek(offset, SEEK_SET);

    return res;
}


void ReadBufferFromMorphServer::setReadUntilPosition(size_t position)
{
    read_until_position = position;
    impl.reset();
    /// `impl.reset()` freed the memory `working_buffer` / `pos` were pointing
    /// into. Clear them so a subsequent `nextImpl` doesn't resync
    /// `impl->position()` to a freed pointer before reinitializing.
    BufferBase::set(nullptr, 0, 0);
}


void ReadBufferFromMorphServer::setReadUntilEnd()
{
    if (!read_until_position)
        return;

    read_until_position = 0;
    if (impl)
    {
        /// Capture the logical position before dropping the buffer the
        /// `working_buffer` / `pos` pointers refer into.
        offset = getPosition();
        impl.reset();
        BufferBase::set(nullptr, 0, 0);
    }
}


bool ReadBufferFromMorphServer::nextImpl()
{
    if (read_until_position)
    {
        if (read_until_position == offset)
            return false;

        if (read_until_position < offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset.load(), read_until_position - 1);
    }

    if (!impl)
    {
        impl = initialize();
        /// Adopt the new impl's (initially empty) buffer immediately so the
        /// `impl->position() = position()` resync below doesn't write through
        /// a stale pointer left over from a previous impl that was just
        /// destroyed by `setReadUntilPosition`.
        working_buffer = impl->buffer();
        pos = impl->position();
    }

    if (use_external_buffer)
        impl->set(internal_buffer.begin(), internal_buffer.size());
    else
        impl->position() = position();

    auto result = impl->next();

    working_buffer = impl->buffer();
    pos = impl->position();

    if (result)
        offset += working_buffer.size();

    return result;
}


off_t ReadBufferFromMorphServer::seek(off_t offset_, int whence)
{
    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed");

    if (offset_ < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", offset_);

    if (impl)
    {
        if (use_external_buffer)
        {
            impl->set(internal_buffer.begin(), internal_buffer.size());
        }

        impl->seek(offset_, SEEK_SET);

        working_buffer = impl->buffer();
        pos = impl->position();
        offset = offset_ + available();
    }
    else
    {
        offset = offset_;
    }

    return offset;
}


off_t ReadBufferFromMorphServer::getPosition()
{
    return offset - available();
}


size_t ReadBufferFromMorphServer::readBigAt(
    char * to,
    size_t n,
    size_t range_begin,
    const std::function<bool(size_t)> & progress_callback) const
{
    /// One-shot bounded `Range` GET, decoupled from `impl`/`offset`/
    /// `read_until_position`. The Parquet `Prefetcher` calls this from
    /// `io_runner` worker threads in `RandomRead` mode; multiple concurrent
    /// calls touch disjoint `RWBufferFromHTTP` instances so no per-buffer lock
    /// is needed. Reuses the same builder/timeouts/host-filter/headers as
    /// `initialize` and the `DISK` connection group so sockets are pooled.
    Poco::URI uri(url);
    Poco::Net::HTTPBasicCredentials creds;

    const auto & settings = context->getSettingsRef();
    const auto & server_settings = context->getServerSettings();

    auto connection_timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, server_settings);
    connection_timeouts.withConnectionTimeout(
        std::max<Poco::Timespan>(settings[Setting::http_connection_timeout], Poco::Timespan(20, 0)));
    connection_timeouts.withReceiveTimeout(
        std::max<Poco::Timespan>(settings[Setting::http_receive_timeout], Poco::Timespan(20, 0)));

    auto buf = BuilderRWBufferFromHTTP(uri)
                   .withConnectionGroup(HTTPConnectionGroupType::DISK)
                   .withSettings(read_settings)
                   .withTimeouts(connection_timeouts)
                   .withBufSize(buf_size)
                   .withHostFilter(&context->getRemoteHostFilter())
                   .withHeaders(headers)
                   .withDelayInit(true)
                   .create(creds);

    buf->seek(range_begin, SEEK_SET);
    buf->setReadUntilPosition(range_begin + n);

    size_t bytes_read = 0;
    while (bytes_read < n)
    {
        size_t chunk = buf->readBig(to + bytes_read, n - bytes_read);
        if (chunk == 0)
            break;
        bytes_read += chunk;
        if (progress_callback && progress_callback(bytes_read))
            return bytes_read;
    }
    return bytes_read;
}


std::optional<size_t> ReadBufferFromMorphServer::tryGetFileSize()
{
    /// Prefer the size supplied at construction (carried from the Morph
    /// listing's `$Object:payloadLength`). Returning it here lets callers
    /// learn the size without a per-object HEAD. Treat `0` as "unknown" so
    /// callers can pass `0` to defer discovery without the schema-inference
    /// iterator interpreting it as an empty file. Only then lazily build
    /// `impl` and let the HTTP buffer discover the size via
    /// `RWBufferFromHTTP::tryGetFileSize`, which issues a HEAD when no
    /// transaction has happened yet. Mirrors `ReadBufferFromS3::tryGetFileSize`.
    if (auto base = ReadBufferFromFileBase::tryGetFileSize();
        base.has_value() && *base > 0)
        return base;

    if (!impl)
        impl = initialize();

    return tryGetFileSizeFromReadBuffer(*impl);
}

}
