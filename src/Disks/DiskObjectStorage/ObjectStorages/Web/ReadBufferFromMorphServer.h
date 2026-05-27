#pragma once

#include <atomic>
#include <IO/HTTPHeaderEntries.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/Net/HTTPBasicCredentials.h>


namespace DB
{

/* Read buffer, which reads via http, but is used as ReadBufferFromFileBase.
 * Used to read row-group objects from a Morph HTTP endpoint: it carries the
 * Morph auth headers, defers connection setup until the first read, and
 * discovers the object size via HEAD when no size hint is supplied.
 */
class ReadBufferFromMorphServer : public ReadBufferFromFileBase
{
public:
    explicit ReadBufferFromMorphServer(
        const String & url_,
        ContextPtr context_,
        size_t file_size_,
        const ReadSettings & settings_ = {},
        bool use_external_buffer_ = false,
        size_t read_until_position = 0,
        HTTPHeaderEntries headers_ = {});

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    String getFileName() const override { return url; }

    /// Prefer the size supplied at construction (carried from the Morph
    /// listing's `$Object:payloadLength`); only when it is unknown (`0`) do we
    /// lazily build `impl` and let the HTTP buffer discover the size via HEAD.
    /// `0` means "unknown" so callers can pass `0` to defer discovery without
    /// the schema-inference iterator interpreting it as an empty file.
    std::optional<size_t> tryGetFileSize() override;

    void setReadUntilPosition(size_t position) override;

    /// Lift the right bound and drop the current connection. The Parquet
    /// `Prefetcher` calls this before every seek precisely so a forward seek
    /// past a previous `setReadUntilPosition` doesn't run the inner HTTP
    /// buffer into its stale right bound (which would throw "read after eof").
    void setReadUntilEnd() override;

    size_t getFileOffsetOfBufferEnd() const override { return offset.load(std::memory_order_relaxed); }

    bool supportsRightBoundedReads() const override { return true; }

    /// Morph objects are consumed whole: each is a self-contained
    /// single-row-group Parquet file, and the search endpoint already returns
    /// only the objects whose row group matches the query (that pruning is the
    /// optimization). Reporting the buffer as not-efficiently-seekable makes
    /// ClickHouse's Parquet readers fetch the object with a single sequential
    /// GET (no HTTP `Range`) and then slice the footer and column chunks in
    /// memory — native v3 `Prefetcher` -> `EntireFileIsInMemory`, arrow
    /// `asArrowFile` -> `asArrowFileLoadIntoMemory`. `supportsReadAt` stays at
    /// its inherited `false`, so the two together select the whole-file path;
    /// this also drops the per-object size-discovery HEAD, which only fires on
    /// the seekable branch. `seek` is kept intact for any seekable fallback
    /// path (e.g. when a filesystem cache wraps this buffer).
    bool checkIfActuallySeekable() override { return false; }

private:
    std::unique_ptr<SeekableReadBuffer> initialize();

    LoggerPtr log;
    ContextPtr context;

    const String url;
    size_t buf_size;

    std::unique_ptr<SeekableReadBuffer> impl;

    ReadSettings read_settings;

    HTTPHeaderEntries headers;

    Poco::Net::HTTPBasicCredentials credentials{};

    bool use_external_buffer;

    /// atomic is required for CachedOnDiskReadBufferFromFile, which can access
    /// to this variable via getFileOffsetOfBufferEnd()/seek() from multiple
    /// threads.
    std::atomic<off_t> offset = 0;
    off_t read_until_position = 0;
};

}
