package io.strata.client;

import io.strata.common.FileId;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Logical Strata file handle. The handle is lightweight; appenders and readers own network
 * resources and must be closed.
 */
public interface StrataFile {

    FileId id();

    /** Single writer per epoch; a higher epoch anywhere kills this appender permanently. */
    Appender openForAppend(int writeEpoch);

    Reader openForRead();

    /** Fences the file at newEpoch, seal-recovers the open tail (§7.3), returns the sealed length. */
    SealInfo recoverAndSeal(int newEpoch);

    record AppendAck(long endOffset, long durableOffset) {}

    record SealInfo(long sealedLength) {}

    record ReadResult(byte[] data, boolean endOfFile) {}

    interface Appender extends AutoCloseable {
        /** Pipelined; completes on quorum (2/3) ack. Offsets are file-logical. */
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
