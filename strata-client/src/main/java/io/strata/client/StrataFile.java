package io.strata.client;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Logical Strata file handle. The handle is lightweight; appenders and readers own network
 * resources and must be closed.
 */
public interface StrataFile {

    FileId id();

    StrataNamespace namespace();

    StrataPath path();

    /** Opens a single-writer appender; metadata allocates the fencing epoch. */
    Appender openForAppend();

    Reader openForRead();

    /** Fences any open writer, seal-recovers the open tail (§7.3), returns the sealed length. */
    SealInfo recoverAndSeal();

    record AppendAck(long endOffset, long durableOffset) {}

    record SealInfo(long sealedLength) {}

    record ReadResult(byte[] data, boolean endOfFile) {}

    interface Appender extends AutoCloseable {
        /**
         * Pipelined; completes on the file's ack quorum. Offsets are file-logical.
         *
         * <p>The appender snapshots the remaining bytes before returning, so callers may reuse or
         * mutate {@code data} after this method returns.
         */
        CompletableFuture<AppendAck> append(ByteBuffer data);

        long durableOffset();

        /** Seals the open chunk at the durable offset and the file at its total length. */
        SealInfo seal();

        @Override
        void close();
    }

    interface Reader extends AutoCloseable {
        /**
         * Reads up to maxBytes from fileOffset (may return fewer — at most to a chunk boundary,
         * and never beyond the durable offset of an open chunk).
         */
        ReadResult read(long fileOffset, int maxBytes);

        /** Re-fetches file metadata (new chunks, seals) for tail-following reads. */
        void refresh();

        @Override
        void close();
    }
}
