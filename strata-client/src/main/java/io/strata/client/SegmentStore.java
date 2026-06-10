package io.strata.client;

import io.strata.common.FileId;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The segment-store client API (tech design §12). Deliberately Kafka-free: files, bytes,
 * offsets, epochs. v0 deviation from the design signatures: file-lifecycle calls are synchronous
 * (they are chunk-boundary operations, off the hot path); append is fully pipelined/async.
 */
public interface SegmentStore extends AutoCloseable {

    record FileSpec(byte fileKind, byte mediaClass, byte ackPolicy, String ownerTag) {
        public static FileSpec log(String ownerTag) {
            return new FileSpec((byte) 0, (byte) 0, (byte) 0, ownerTag);
        }
    }

    record AppendAck(long endOffset, long durableOffset) {}

    record SealInfo(long sealedLength) {}

    record ReadResult(byte[] data, boolean endOfFile) {}

    FileId create(FileSpec spec);

    /** Single writer per epoch; a higher epoch anywhere kills this appender permanently. */
    Appender openForAppend(FileId fileId, int writeEpoch);

    Reader openForRead(FileId fileId);

    /** Fences the file at newEpoch, seal-recovers the open tail (§7.3), returns the sealed length. */
    SealInfo recoverAndSeal(FileId fileId, int newEpoch);

    void delete(List<FileId> fileIds);

    @Override
    void close();

    interface Appender extends AutoCloseable {
        /** Pipelined; completes on quorum (2/3) ack. Offsets are file-logical. */
        CompletableFuture<AppendAck> append(java.nio.ByteBuffer data);

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
