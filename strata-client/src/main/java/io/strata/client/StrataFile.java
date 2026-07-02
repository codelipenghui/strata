package io.strata.client;

import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
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

    record SealInfo(long sealedLength) {}

    final class ReadResult implements AutoCloseable {
        private static final ByteBuffer EMPTY = ByteBuffer.allocate(0).asReadOnlyBuffer();

        private final ByteBuffer buffer;
        private final boolean endOfFile;
        private final AutoCloseable release;

        ReadResult(ByteBuffer buffer, boolean endOfFile, AutoCloseable release) {
            this.buffer = buffer;
            this.endOfFile = endOfFile;
            this.release = release;
        }

        /** Empty result (no bytes, nothing to release). */
        public static ReadResult empty(boolean endOfFile) {
            return new ReadResult(EMPTY.duplicate(), endOfFile, null);
        }

        /** Read-only view of the read bytes. VALID ONLY until {@link #close()}. */
        public ByteBuffer buffer() {
            return buffer.duplicate();
        }

        public boolean endOfFile() {
            return endOfFile;
        }

        public int length() {
            return buffer.remaining();
        }

        @Override
        public void close() {
            if (release != null) {
                try {
                    release.close();
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new ScpException(
                            ErrorCode.INTERNAL, "failed to release read buffer: " + e);
                }
            }
        }

        /** Test-only: the underlying release handle (a Frame in production), for leak assertions. */
        AutoCloseable releaseHandleForTest() {
            return release;
        }
    }

    interface Appender extends AutoCloseable {
        /**
         * Pipelined; completes on the file's ack quorum. Offsets are file-logical.
         *
         * <p>The returned future completes with this append's file-logical {@code endOffset} — the
         * offset just past the last byte written. Completion implies those bytes are quorum-acked.
         *
         * <p>The appender snapshots the remaining bytes before returning, so callers may reuse or
         * mutate {@code data} after this method returns.
         */
        CompletableFuture<Long> append(ByteBuffer data);

        /**
         * Current durable high-watermark: the file-logical offset through which writes are
         * quorum-acked. A moving snapshot, not a per-append fact. Note this reflects
         * quorum-<em>acknowledged</em> bytes, which is weaker than recovery-durable (issue #29):
         * the window between the quorum-ack and the replica-confirmed durable floor is the
         * acked-loss window.
         */
        long durableOffset();

        /** Seals the open chunk at the durable offset and the file at its total length. */
        SealInfo seal();

        @Override
        void close();
    }

    interface Reader extends AutoCloseable {
        /**
         * Reads up to maxBytes from fileOffset (may return fewer — at most to a chunk boundary,
         * never beyond the durable offset of an open chunk, and at most to the server's per-request
         * byte limit).
         *
         * <p>The returned result is {@link AutoCloseable}; the caller owns it and MUST close it
         * (try-with-resources). The buffer it exposes is valid only until close.
         */
        ReadResult read(long fileOffset, int maxBytes);

        /** Re-fetches file metadata (new chunks, seals) for tail-following reads. */
        void refresh();

        @Override
        void close();
    }
}
