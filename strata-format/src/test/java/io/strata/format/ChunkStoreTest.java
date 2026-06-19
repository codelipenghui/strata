package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Crc;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkStoreTest {

    @TempDir
    Path dir;

    private final ChunkId id = new ChunkId(FileId.random(), 0);

    private static ByteBuffer bytes(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private ChunkStore newStore() throws IOException {
        return new ChunkStore(dir);
    }

    private void open(ChunkStore store, ChunkId id, int epoch) throws IOException {
        store.open(id, false, epoch, 1718000000000L);
    }

    private byte[] sealedBytes(ChunkStore store, ChunkId chunkId, String payload) throws IOException {
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        open(store, chunkId, 1);
        store.append(chunkId, 1, 0, 0, ByteBuffer.wrap(bytes));
        store.seal(chunkId, 1, bytes.length, null);
        return store.fetch(chunkId, 0, Integer.MAX_VALUE).bytes();
    }

    private static ChunkFormats.Trailer trailer(byte[] fileBytes) {
        return ChunkFormats.Trailer.decode(Arrays.copyOfRange(
                fileBytes, fileBytes.length - ChunkFormats.TRAILER_SIZE, fileBytes.length));
    }

    private static byte[] footerBytes(byte[] fileBytes) {
        ChunkFormats.Trailer t = trailer(fileBytes);
        return Arrays.copyOfRange(fileBytes, (int) t.footerStart(),
                fileBytes.length - ChunkFormats.TRAILER_SIZE);
    }

    private static void writeTrailer(byte[] fileBytes, ChunkFormats.Trailer trailer) {
        byte[] trailerBytes = trailer.encode();
        System.arraycopy(trailerBytes, 0, fileBytes, fileBytes.length - ChunkFormats.TRAILER_SIZE,
                ChunkFormats.TRAILER_SIZE);
    }

    private static byte[] section(int type, byte[] content) {
        ByteBuffer out = ByteBuffer.allocate(ChunkFormats.sectionSize(content));
        ChunkFormats.writeSection(out, type, content);
        return out.array();
    }

    private static byte[] footer(byte[]... sections) {
        int len = 0;
        for (byte[] section : sections) len += section.length;
        byte[] out = new byte[len];
        int pos = 0;
        for (byte[] section : sections) {
            System.arraycopy(section, 0, out, pos, section.length);
            pos += section.length;
        }
        return out;
    }

    private static byte[] statsSection(long dataLength, int entries) {
        return section(ChunkFormats.SECTION_STATS, ByteBuffer.allocate(12).putLong(dataLength).putInt(entries).array());
    }

    private static byte[] withFooter(byte[] original, byte[] footer, int sectionCount) {
        ChunkFormats.Trailer old = trailer(original);
        int footerStart = (int) old.footerStart();
        byte[] out = new byte[footerStart + footer.length + ChunkFormats.TRAILER_SIZE];
        System.arraycopy(original, 0, out, 0, footerStart);
        System.arraycopy(footer, 0, out, footerStart, footer.length);
        writeTrailer(out, new ChunkFormats.Trailer(old.dataLength(), old.footerStart(), sectionCount,
                old.incompatFlags(), Crc.of(footer), old.dataCrc()));
        return out;
    }

    private static void assertImportFails(ChunkStore store, ChunkId chunkId, ErrorCode code,
                                          byte[] fileBytes, long expectedLength, int expectedCrc) {
        assertEquals(code, assertThrows(ScpException.class,
                () -> store.importSealed(chunkId, fileBytes, expectedLength, expectedCrc)).code());
    }

    @Test
    void defaultChannelCacheCapacityIsPositive() {
        assertTrue(ChunkStore.defaultChannelCacheCapacity() >= 128,
                "auto-sized cache capacity must be a sane floor");
    }

    @Test
    void openFdsIsNonNegativeOnUnixOrMinusOne() throws Exception {
        try (ChunkStore store = newStore()) {
            long fds = store.openFds();
            assertTrue(fds >= 0 || fds == -1, "openFds() is the live count or -1 when unavailable");
        }
    }

    @Test
    void failedOpenCleansOwnedFilesAndReservation() throws Exception {
        ChunkId chunk = new ChunkId(FileId.random(), 0);
        Path dataPath = dir.resolve(ChunkFormats.baseName(chunk) + ".chunk");
        Path ledgerPath = dir.resolve(ChunkFormats.baseName(chunk) + ".j");
        Files.createDirectories(dir);
        Files.write(ledgerPath, new byte[] {1});

        try (ChunkStore store = newStore()) {
            assertThrows(IOException.class, () -> open(store, chunk, 1));

            assertEquals(false, Files.exists(dataPath),
                    "failed open left behind a data file that would poison retries");
            assertEquals(true, Files.exists(ledgerPath),
                    "cleanup must not delete a pre-existing file it did not create");
            assertEquals(false, creating(store).contains(chunk),
                    "failed open left a stuck in-process reservation");

            Files.delete(ledgerPath);
            open(store, chunk, 1);
            assertEquals(true, Files.exists(dataPath));
        }
    }

    @Test
    void usedBytesReflectsPhysicalChunkFootprint() throws Exception {
        byte[] payload = "capacity-accounting".getBytes(StandardCharsets.UTF_8);
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, ByteBuffer.wrap(payload));
            store.seal(id, 1, payload.length, null);

            String base = ChunkFormats.baseName(id);
            long physicalBytes = sizeIfExists(dir.resolve(base + ".chunk"))
                    + sizeIfExists(dir.resolve(base + ".meta"))
                    + sizeIfExists(dir.resolve(base + ".j"));

            assertTrue(store.usedBytes() >= physicalBytes,
                    "capacity reporting must include physical chunk overhead, not only logical data bytes");
        }
    }

    @SuppressWarnings("unchecked")
    private static Object handle(ChunkStore store, ChunkId chunkId) throws Exception {
        Field chunks = ChunkStore.class.getDeclaredField("chunks");
        chunks.setAccessible(true);
        return ((Map<ChunkId, ?>) chunks.get(store)).get(chunkId);
    }

    @SuppressWarnings("unchecked")
    private static Set<ChunkId> creating(ChunkStore store) throws Exception {
        Field creating = ChunkStore.class.getDeclaredField("creating");
        creating.setAccessible(true);
        return (Set<ChunkId>) creating.get(store);
    }

    private static void setHandleLong(ChunkStore store, ChunkId chunkId, String fieldName, long value)
            throws Exception {
        Object handle = handle(store, chunkId);
        Field field = handle.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setLong(handle, value);
    }

    private static void setHandleObject(ChunkStore store, ChunkId chunkId, String fieldName, Object value)
            throws Exception {
        Object handle = handle(store, chunkId);
        setObject(handle, fieldName, value);
    }

    private static void setObject(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object newHandle(ChunkStore store, ChunkId chunkId) throws Exception {
        Class<?> type = Class.forName("io.strata.format.ChunkStore$Handle");
        Constructor<?> ctor = type.getDeclaredConstructor(ChunkStore.class, ChunkId.class, ChunkFormats.Header.class);
        ctor.setAccessible(true);
        return ctor.newInstance(store, chunkId, new ChunkFormats.Header(chunkId, false, 1, 1718000000000L, 0, 0, 0));
    }

    private static int checkedFooterLength(ChunkFormats.Trailer trailer, long fileLen) throws Exception {
        Method method = ChunkStore.class.getDeclaredMethod("checkedFooterLength",
                ChunkFormats.Trailer.class, long.class);
        method.setAccessible(true);
        try {
            return (int) method.invoke(null, trailer, fileLen);
        } catch (InvocationTargetException e) {
            throw rethrowCause(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Integer> decodeCrcRanges(byte[] footerBytes, long dataLength, int expectedSections)
            throws Exception {
        Method method = ChunkStore.class.getDeclaredMethod("decodeCrcRanges", byte[].class, long.class, int.class);
        method.setAccessible(true);
        try {
            return (List<Integer>) method.invoke(null, footerBytes, dataLength, expectedSections);
        } catch (InvocationTargetException e) {
            throw rethrowCause(e);
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws Exception {
        ChunkFormats.readFully(channel, buffer, position);
    }

    private static long sizeIfExists(Path path) throws IOException {
        return Files.exists(path) ? Files.size(path) : 0;
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer, long position) throws Exception {
        ChunkFormats.writeFully(channel, buffer, position);
    }

    private static void cleanupFailedImport(ChunkStore store, Object handle, Path tmp, boolean movedData,
                                            boolean sidecarStarted) throws Exception {
        Method method = ChunkStore.class.getDeclaredMethod("cleanupFailedImport",
                handle.getClass(), Path.class, boolean.class, boolean.class);
        method.setAccessible(true);
        try {
            method.invoke(store, handle, tmp, movedData, sidecarStarted);
        } catch (InvocationTargetException e) {
            throw rethrowCause(e);
        }
    }

    private static Exception rethrowCause(InvocationTargetException e) throws Exception {
        Throwable cause = e.getCause();
        if (cause instanceof Exception exception) {
            return exception;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new RuntimeException(cause);
    }

    private static void waitFor(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), message);
    }

    private static final class ZeroProgressFileChannel extends FileChannel {
        private final boolean failClose;
        private final boolean zeroRead;
        private final boolean zeroWrite;

        ZeroProgressFileChannel(boolean failClose, boolean zeroRead, boolean zeroWrite) {
            this.failClose = failClose;
            this.zeroRead = zeroRead;
            this.zeroWrite = zeroWrite;
        }

        @Override
        public int read(ByteBuffer dst) {
            return zeroRead ? 0 : -1;
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) {
            return zeroRead ? 0 : -1;
        }

        @Override
        public int write(ByteBuffer src) {
            return zeroWrite ? 0 : src.remaining();
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) {
            return zeroWrite ? 0 : 1;
        }

        @Override
        public long position() {
            return 0;
        }

        @Override
        public FileChannel position(long newPosition) {
            return this;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public FileChannel truncate(long size) {
            return this;
        }

        @Override
        public void force(boolean metaData) {
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(ByteBuffer dst, long position) {
            return zeroRead ? 0 : -1;
        }

        @Override
        public int write(ByteBuffer src, long position) {
            return zeroWrite ? 0 : src.remaining();
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void implCloseChannel() throws IOException {
            if (failClose) {
                throw new IOException("close failed");
            }
        }
    }

    private static Object getHandleObject(ChunkStore store, ChunkId chunkId, String fieldName)
            throws Exception {
        Object handle = handle(store, chunkId);
        return getObject(handle, fieldName);
    }

    private static Object getObject(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    @Test
    void appendReadSealLifecycle() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            assertEquals(5, store.append(id, 1, 0, 0, bytes("hello")).endOffset());
            assertEquals(11, store.append(id, 1, 5, 5, bytes(" world")).endOffset());

            var r = store.read(id, 0, 1024);
            assertArrayEquals("hello world".getBytes(), r.bytes());
            assertEquals(11, r.localEndOffset());
            assertEquals(5, r.lastKnownDO()); // piggybacked DO from second append

            var sealed = store.seal(id, 1, 11, null);
            assertEquals(11, sealed.finalLength());
            // idempotent re-seal returns same result
            assertEquals(sealed, store.seal(id, 1, 11, null));
            assertEquals(0, store.readLedger(id, 0).size());

            var r2 = store.read(id, 6, 1024);
            assertArrayEquals("world".getBytes(), r2.bytes());

            try (var region = store.readRegion(id, 6, 1024)) {
                assertEquals(5, region.length());
                assertEquals(11, region.localEndOffset());
                // SEALED reads are zero-copy regions (bytes() == null); read via the channel.
                assertArrayEquals("world".getBytes(), consumeRegion(region));
            }

            var stat = store.stat(id);
            assertEquals(ChunkState.SEALED, stat.state());
            assertEquals(11, stat.sealedLength());

            // append after seal rejected
            assertEquals(ErrorCode.CHUNK_SEALED,
                    assertThrows(ScpException.class, () -> store.append(id, 1, 11, 0, bytes("x"))).code());
        }
    }

    @Test
    void sealRejectsMalformedCallerFooterSectionsWithoutSealing() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("data"));

            ScpException tooShort = assertThrows(ScpException.class,
                    () -> store.seal(id, 1, 4, ByteBuffer.wrap(new byte[] {1, 2, 3})));
            assertEquals(ErrorCode.PRECONDITION_FAILED, tooShort.code());

            ScpException negativeCount = assertThrows(ScpException.class,
                    () -> store.seal(id, 1, 4, ByteBuffer.wrap(ByteBuffer.allocate(4).putInt(-1).array())));
            assertEquals(ErrorCode.PRECONDITION_FAILED, negativeCount.code());

            byte[] trailingBytesPayload = ByteBuffer.allocate(Integer.BYTES + 1)
                    .putInt(1)
                    .put((byte) 1)
                    .array();
            ScpException trailingBytes = assertThrows(ScpException.class,
                    () -> store.seal(id, 1, 4, ByteBuffer.wrap(trailingBytesPayload)));
            assertEquals(ErrorCode.PRECONDITION_FAILED, trailingBytes.code());

            ScpException countMismatch = assertThrows(ScpException.class,
                    () -> store.seal(id, 1, 4, ByteBuffer.wrap(ByteBuffer.allocate(4).putInt(1).array())));
            assertEquals(ErrorCode.PRECONDITION_FAILED, countMismatch.code());

            byte[] badLengthPayload = ByteBuffer.allocate(Integer.BYTES + 12)
                    .putInt(1)
                    .putShort((short) ChunkFormats.SECTION_STATS)
                    .putShort((short) 1)
                    .putInt(100)
                    .putInt(0)
                    .array();
            ScpException badLength = assertThrows(ScpException.class,
                    () -> store.seal(id, 1, 4, ByteBuffer.wrap(badLengthPayload)));
            assertEquals(ErrorCode.PRECONDITION_FAILED, badLength.code());

            byte[] negativeLengthPayload = ByteBuffer.allocate(Integer.BYTES + 12)
                    .putInt(1)
                    .putShort((short) ChunkFormats.SECTION_STATS)
                    .putShort((short) 1)
                    .putInt(-1)
                    .putInt(0)
                    .array();
            ScpException negativeLength = assertThrows(ScpException.class,
                    () -> store.seal(id, 1, 4, ByteBuffer.wrap(negativeLengthPayload)));
            assertEquals(ErrorCode.PRECONDITION_FAILED, negativeLength.code());

            byte[] badSectionCrc = section(ChunkFormats.SECTION_STATS,
                    ByteBuffer.allocate(12).putLong(4).putInt(1).array());
            badSectionCrc[badSectionCrc.length - 1] ^= 1;
            byte[] badCrcPayload = ByteBuffer.allocate(Integer.BYTES + badSectionCrc.length)
                    .putInt(1)
                    .put(badSectionCrc)
                    .array();
            ScpException badCrc = assertThrows(ScpException.class,
                    () -> store.seal(id, 1, 4, ByteBuffer.wrap(badCrcPayload)));
            assertEquals(ErrorCode.PRECONDITION_FAILED, badCrc.code());

            byte[] crcRanges = section(ChunkFormats.SECTION_CRC_RANGES,
                    ByteBuffer.allocate(8).putInt(ChunkFormats.CRC_RANGE_SIZE).putInt(0).array());
            byte[] reservedPayload = ByteBuffer.allocate(Integer.BYTES + crcRanges.length)
                    .putInt(1).put(crcRanges).array();
            ScpException reserved = assertThrows(ScpException.class,
                    () -> store.seal(id, 1, 4, ByteBuffer.wrap(reservedPayload)));
            assertEquals(ErrorCode.PRECONDITION_FAILED, reserved.code());

            assertEquals(ChunkState.OPEN, store.stat(id).state());
            assertEquals(4, store.seal(id, 1, 4, null).finalLength());
            assertEquals(ChunkState.SEALED, store.stat(id).state());
        }
    }

    @Test
    void sealTreatsEmptyCallerSectionBufferLikeNoCallerSections() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("data"));

            assertEquals(4, store.seal(id, 1, 4, ByteBuffer.allocate(0)).finalLength());
            assertEquals(ChunkState.SEALED, store.stat(id).state());
        }
    }

    @Test
    void sealAcceptsValidCallerFooterSectionsAndPreservesCounts() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("data"));

            byte[] callerSection = section(ChunkFormats.SECTION_OFFSET_INDEX, "caller-index".getBytes());
            byte[] callerPayload = ByteBuffer.allocate(Integer.BYTES + callerSection.length)
                    .putInt(1)
                    .put(callerSection)
                    .array();

            store.seal(id, 1, 4, ByteBuffer.wrap(callerPayload));
            byte[] fileBytes = store.fetch(id, 0, Integer.MAX_VALUE).bytes();
            ChunkFormats.Trailer trailer = trailer(fileBytes);
            assertEquals(3, trailer.sectionCount(), "caller section plus CRC_RANGES and STATS");
            byte[] footer = footerBytes(fileBytes);
            assertEquals(ChunkFormats.SECTION_OFFSET_INDEX,
                    ByteBuffer.wrap(footer).getShort() & 0xFFFF);
        }
    }

    @Test
    void sealFailsBeforeMutationWhenCommitterIsPoisoned() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            GroupCommitter failingCommitter = new GroupCommitter(
                    "poison-seal",
                    () -> {
                        throw new IOException("forced fsync failure");
                    },
                    new AtomicLong());
            setHandleObject(store, id, "committer", failingCommitter);
            try {
                ScpException appendFailure = assertThrows(ScpException.class,
                        () -> store.append(id, 1, 0, 0, bytes("data")));
                assertEquals(ErrorCode.INTERNAL, appendFailure.code());

                IOException sealFailure = assertThrows(IOException.class, () -> store.seal(id, 1, 4, null));
                assertTrue(sealFailure.getMessage().contains("group-commit flusher failed"));
                assertEquals(ChunkState.OPEN, store.stat(id).state());
            } finally {
                failingCommitter.closeAndConfirm();
                setHandleObject(store, id, "committer", null);
            }
        }
    }

    @Test
    void malformedCallerFooterDoesNotStopOpenFsyncCommitter() throws Exception {
        try (ChunkStore store = newStore()) {
            store.open(id, true, 1, 1718000000000L);

            assertEquals(ErrorCode.PRECONDITION_FAILED, assertThrows(ScpException.class,
                    () -> store.seal(id, 1, 0, ByteBuffer.wrap(new byte[] {1, 2, 3}))).code());

            assertEquals(ChunkState.OPEN, store.stat(id).state());
            assertTrue(getHandleObject(store, id, "committer") != null,
                    "malformed caller footer must not disable fsync acks on an open chunk");
            assertEquals(0, store.seal(id, 1, 0, null).finalLength());
        }
    }

    @Test
    void synchronousAppendPropagatesFailedFsyncWaiterAsScpException() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            GroupCommitter failingCommitter = new GroupCommitter(
                    "test-failure",
                    () -> {
                        throw new IOException("forced fsync failure");
                    },
                    new AtomicLong());
            setHandleObject(store, id, "committer", failingCommitter);

            ScpException e = assertThrows(ScpException.class,
                    () -> store.append(id, 1, 0, 0, bytes("data")));
            assertEquals(ErrorCode.INTERNAL, e.code());
            failingCommitter.closeAndConfirm();
            setHandleObject(store, id, "committer", null);
        }
    }

    @Test
    void closePropagatesFailuresAndKeepsChunksForRetry() throws Exception {
        ChunkStore store = newStore();
        open(store, id, 1);
        setHandleObject(store, id, "data", new ZeroProgressFileChannel(true, false, false));

        IOException failure = assertThrows(IOException.class, store::close);
        assertTrue(failure.getMessage().contains("close failed"));
        assertTrue(store.contains(id), "failed close must not discard the handle needed for retry");

        store.close();
        assertEquals(false, store.contains(id));
    }

    @Test
    void closeReportsSidecarPersistenceFailureAndKeepsChunkForRetry() throws Exception {
        ChunkStore store = newStore();
        open(store, id, 1);
        Path metaPath = dir.resolve(ChunkFormats.baseName(id) + ".meta");
        Files.delete(metaPath);
        Files.createDirectory(metaPath);

        IOException failure = assertThrows(IOException.class, store::close);
        assertTrue(failure.getMessage().contains("Is a directory")
                || failure.getMessage().contains("is a directory")
                || failure.getMessage().contains(metaPath.toString()), "got: " + failure.getMessage());
        assertTrue(store.contains(id), "failed close must keep the chunk visible for retry");

        Files.delete(metaPath);
        store.close();
        assertEquals(false, store.contains(id));
    }

    @Test
    void closePropagatesPoisonedCommitterButAllowsRetry() throws Exception {
        ChunkStore store = newStore();
        open(store, id, 1);
        GroupCommitter failingCommitter = new GroupCommitter(
                "poison-close",
                () -> {
                    throw new IOException("forced fsync failure");
                },
                new AtomicLong());
        setHandleObject(store, id, "committer", failingCommitter);

        ScpException appendFailure = assertThrows(ScpException.class,
                () -> store.append(id, 1, 0, 0, bytes("data")));
        assertEquals(ErrorCode.INTERNAL, appendFailure.code());

        IOException closeFailure = assertThrows(IOException.class, store::close);
        assertTrue(closeFailure.getMessage().contains("group-commit flusher failed"));
        assertEquals(null, getHandleObject(store, id, "committer"));
        assertTrue(store.contains(id), "failed close must keep the chunk visible for retry");

        store.close();
        assertEquals(false, store.contains(id));
    }

    @Test
    void appendRejectsOffsetOverflowBeforeMutatingLedger() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            setHandleLong(store, id, "end", Long.MAX_VALUE - 1);

            ScpException overflow = assertThrows(ScpException.class,
                    () -> store.append(id, 1, Long.MAX_VALUE - 1, 0, ByteBuffer.wrap(new byte[] {1, 2})));
            assertEquals(ErrorCode.CORRUPT_CHUNK, overflow.code());
            assertEquals(0, store.readLedger(id, 0).size());
        }
    }

    @Test
    void epochFencingRules() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 5);
            store.append(id, 5, 0, 0, bytes("aa"));

            // lower epoch rejected
            var e = assertThrows(ScpException.class, () -> store.append(id, 4, 2, 0, bytes("bb")));
            assertEquals(ErrorCode.FENCED_EPOCH, e.code());
            assertEquals(5, e.detail());

            // fence at 7: epoch 5,6 appends rejected; 7 accepted
            var f = store.fence(id, 7);
            assertEquals(7, f.persistedFenceEpoch());
            assertEquals(2, f.localEndOffset());
            assertEquals(ErrorCode.FENCED_EPOCH,
                    assertThrows(ScpException.class, () -> store.append(id, 5, 2, 0, bytes("bb"))).code());
            assertEquals(ErrorCode.FENCED_EPOCH,
                    assertThrows(ScpException.class, () -> store.append(id, 6, 2, 0, bytes("bb"))).code());
            assertEquals(4, store.append(id, 7, 2, 0, bytes("cc")).endOffset());

            // fence is monotonic: lower fence is a no-op
            assertEquals(7, store.fence(id, 3).persistedFenceEpoch());

            // seal with fenced epoch rejected, with current epoch ok
            assertEquals(ErrorCode.FENCED_EPOCH,
                    assertThrows(ScpException.class, () -> store.seal(id, 5, 4, null)).code());
            assertEquals(4, store.seal(id, 7, 4, null).finalLength());
            assertEquals(ErrorCode.FENCED_EPOCH,
                    assertThrows(ScpException.class, () -> store.seal(id, 5, 4, null)).code());
        }
    }

    @Test
    void offsetGapRejectedWithExpectedDetail() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("abc"));
            var e = assertThrows(ScpException.class, () -> store.append(id, 1, 5, 0, bytes("d")));
            assertEquals(ErrorCode.OFFSET_GAP, e.code());
            assertEquals(3, e.detail());
        }
    }

    @Test
    void duplicateOpenRejectedBeforeMutatingFiles() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            assertEquals(ErrorCode.CHUNK_ALREADY_EXISTS,
                    assertThrows(ScpException.class, () -> open(store, id, 1)).code());
        }

        ChunkId blocked = new ChunkId(FileId.random(), 0);
        try (ChunkStore store = newStore()) {
            java.nio.file.Files.write(dir.resolve(ChunkFormats.baseName(blocked) + ".chunk"), new byte[]{1});
            assertEquals(ErrorCode.CHUNK_ALREADY_EXISTS,
                    assertThrows(ScpException.class, () -> open(store, blocked, 1)).code());
        }

        ChunkId reserved = new ChunkId(FileId.random(), 0);
        try (ChunkStore store = newStore()) {
            creating(store).add(reserved);
            assertEquals(ErrorCode.CHUNK_ALREADY_EXISTS,
                    assertThrows(ScpException.class, () -> open(store, reserved, 1)).code());
            assertTrue(creating(store).contains(reserved));
        }
    }

    @Test
    void deleteRetriesWhileChunkCreationIsReserved() throws Exception {
        try (ChunkStore store = newStore()) {
            creating(store).add(id);
            try {
                assertEquals(ErrorCode.INTERNAL, store.delete(id));
            } finally {
                creating(store).remove(id);
            }
            assertEquals(ErrorCode.CHUNK_NOT_FOUND, store.delete(id));
        }
    }

    @Test
    void sealTruncatesUnackedTail() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("aaaa"));
            store.append(id, 1, 4, 0, bytes("bbbb")); // never quorum-acked, will be cut
            var sealed = store.seal(id, 1, 4, null);
            assertEquals(4, sealed.finalLength());
            var r = store.read(id, 0, 100);
            assertArrayEquals("aaaa".getBytes(), r.bytes());
        }
    }

    @Test
    void openReadRegionDurablePrefixIsZeroCopyAcrossConcurrentSealTruncate() throws Exception {
        // A reader resolves a region on an OPEN chunk; the data plane transfers that region LATER,
        // off the chunk lock. readRegion must only expose bytes below the replica-known durable high
        // watermark, and an independent read FD must keep those bytes stable if a concurrent seal
        // truncates the never-acked tail.
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("COMMITTED"));      // [0,9) survives the seal
            store.append(id, 1, 9, 9, bytes("TAILTAILTAIL"));   // [9,21) never-acked, will be cut
            assertEquals(21, store.stat(id).localEndOffset());

            // The uncommitted tail must not be served at all, even though it is locally present.
            ChunkStore.ReadRegionResult tail = store.readRegion(id, 9, 1024);
            assertEquals(0, tail.length());
            assertEquals(21, tail.localEndOffset());
            assertEquals(9, tail.lastKnownDO());

            // The committed prefix is served as a zero-copy channel region over an independent FD.
            ChunkStore.ReadRegionResult region = store.readRegion(id, 0, 1024);
            assertEquals(9, region.length());
            assertTrue(region.channel() != null, "durable open read should use a zero-copy channel");
            assertEquals(null, region.bytes());
            try {
                // a concurrent seal truncates the never-acked tail after this region was resolved
                var sealed = store.seal(id, 1, 9, null);
                assertEquals(9, sealed.finalLength());

                // the region handed out BEFORE the seal must still yield the original committed bytes
                byte[] got = consumeRegion(region);
                assertArrayEquals("COMMITTED".getBytes(), got,
                        "readRegion result must be a stable snapshot unaffected by a concurrent seal-truncate");
            } finally {
                region.close();
            }
        }
    }

    @Test
    void sealCannotTruncateBelowDurableWatermark() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("SAFE"));
            store.append(id, 1, 4, 4, bytes("TAIL"));
            assertEquals(4, store.stat(id).lastKnownDO());

            ScpException e = assertThrows(ScpException.class, () -> store.seal(id, 1, 3, null));
            assertEquals(ErrorCode.INTERNAL, e.code());
            assertEquals(4, e.detail());

            assertEquals(4, store.seal(id, 1, 4, null).finalLength());
        }
    }

    @Test
    void openReadRejectsLedgerCoveredDataRotButReadRegionIsZeroCopyUnverified() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("verified-open"));
            store.append(id, 1, 13, 13, ByteBuffer.allocate(0)); // make the full range readable

            Path data = dir.resolve(ChunkFormats.baseName(id) + ".chunk");
            try (FileChannel ch = FileChannel.open(data, java.nio.file.StandardOpenOption.WRITE)) {
                ch.write(ByteBuffer.wrap(new byte[] {'X'}), ChunkFormats.DATA_START + 4);
            }

            assertEquals(ErrorCode.CRC_MISMATCH,
                    assertThrows(ScpException.class, () -> store.read(id, 0, 13)).code());
            try (var region = store.readRegion(id, 0, 13)) {
                assertEquals(13, region.length());
                assertTrue(region.channel() != null, "client open readRegion should be zero-copy");
                byte[] got = consumeRegion(region);
                assertEquals('X', got[4]);
            }
        }
    }

    /** Reads a region result whether it is a heap snapshot or a zero-copy channel. */
    private static byte[] consumeRegion(ChunkStore.ReadRegionResult r) throws Exception {
        if (r.bytes() != null) {
            return r.bytes();
        }
        ByteBuffer buf = ByteBuffer.allocate(r.length());
        readFully(r.channel(), buf, r.filePosition());
        buf.flip();
        byte[] out = new byte[buf.remaining()];
        buf.get(out);
        return out;
    }

    @Test
    void sealRejectsInvalidLengthsAndConflictingReseal() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("abcd"));

            assertEquals(ErrorCode.INTERNAL,
                    assertThrows(ScpException.class, () -> store.seal(id, 1, 5, null)).code());

            store.seal(id, 1, 4, null);
            ScpException e = assertThrows(ScpException.class, () -> store.seal(id, 1, 3, null));
            assertEquals(ErrorCode.CHUNK_SEALED, e.code());
            assertEquals(4, e.detail());
        }
    }

    @Test
    void emptyAppendIsDoBeacon() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("data"));
            store.append(id, 1, 4, 4, ByteBuffer.allocate(0)); // beacon advances DO only
            var stat = store.stat(id);
            assertEquals(4, stat.localEndOffset());
            assertEquals(4, stat.lastKnownDO());
            assertEquals(1, store.readLedger(id, 0).size()); // beacon adds no ledger entry
        }
    }

    @Test
    void doNeverExceedsLocalEnd() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            // a DO claim can only cover previously acked data — it is clamped to the
            // pre-append end (0 here), never to the bytes carried by this very append
            store.append(id, 1, 0, 999, bytes("ab"));
            assertEquals(0, store.stat(id).lastKnownDO());
            // next append may legitimately claim the previous bytes as durable
            store.append(id, 1, 2, 2, bytes("cd"));
            assertEquals(2, store.stat(id).lastKnownDO());
        }
    }

    @Test
    void readRejectsChunkFileOffsetOverflowBeforeIo() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            setHandleLong(store, id, "end", Long.MAX_VALUE);

            ScpException overflow = assertThrows(ScpException.class,
                    () -> store.read(id, Long.MAX_VALUE - 1, 1));
            assertEquals(ErrorCode.CORRUPT_CHUNK, overflow.code());
        }
    }

    @Test
    void fetchAndImportProduceByteIdenticalReplica(@TempDir Path otherDir) throws Exception {
        long sealedLen;
        int crc;
        byte[] fileBytes;
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            byte[] big = new byte[100_000];
            for (int i = 0; i < big.length; i++) big[i] = (byte) (i % 251);
            store.append(id, 1, 0, 0, ByteBuffer.wrap(big));
            var sealed = store.seal(id, 1, big.length, null);
            sealedLen = sealed.finalLength();
            crc = sealed.dataCrc();
            var fetched = store.fetch(id, 0, Integer.MAX_VALUE);
            assertEquals(fetched.fileLength(), fetched.bytes().length);
            fileBytes = fetched.bytes();
        }
        try (ChunkStore other = new ChunkStore(otherDir)) {
            other.importSealed(id, fileBytes, sealedLen, crc);
            var fetched2 = other.fetch(id, 0, Integer.MAX_VALUE);
            assertArrayEquals(fileBytes, fetched2.bytes()); // invariant §14.6: byte-identical replicas
            var r = other.read(id, 0, 10);
            assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, r.bytes());
        }
    }

    @Test
    void importAcceptsEmptySealedChunk(@TempDir Path otherDir) throws Exception {
        byte[] fileBytes;
        int crc;
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            var sealed = store.seal(id, 1, 0, null);
            crc = sealed.dataCrc();
            fileBytes = store.fetch(id, 0, Integer.MAX_VALUE).bytes();
        }

        try (ChunkStore other = new ChunkStore(otherDir)) {
            other.importSealed(id, fileBytes, 0, crc);
            assertTrue(other.contains(id));
            assertArrayEquals(new byte[0], other.read(id, 0, 1).bytes());
        }
    }

    @Test
    void readAndFetchBoundaryConditions() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("payload"));

            var endRead = store.read(id, 7, 100);
            assertEquals(0, endRead.bytes().length);
            assertEquals(7, endRead.localEndOffset());

            assertEquals(ErrorCode.INTERNAL,
                    assertThrows(ScpException.class, () -> store.fetch(id, 0, 100)).code());

            var sealed = store.seal(id, 1, 7, null);
            var file = store.fetch(id, 0, Integer.MAX_VALUE);
            assertEquals(sealed.finalLength(), store.stat(id).sealedLength());
            assertEquals(0, store.read(id, 0, 0).bytes().length);

            var emptyFetch = store.fetch(id, file.fileLength(), 100);
            assertEquals(0, emptyFetch.bytes().length);
            assertEquals(file.fileLength(), emptyFetch.fileLength());
        }
    }

    @Test
    void importRejectsCorruptBytes(@TempDir Path otherDir) throws Exception {
        byte[] fileBytes;
        int dataCrc;
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("payload-payload"));
            store.seal(id, 1, 15, null);
            fileBytes = store.fetch(id, 0, Integer.MAX_VALUE).bytes();
            dataCrc = trailer(fileBytes).dataCrc();
        }
        fileBytes[ChunkFormats.HEADER_SIZE + 3] ^= 1; // corrupt data region
        try (ChunkStore other = new ChunkStore(otherDir)) {
            assertEquals(ErrorCode.CRC_MISMATCH,
                    assertThrows(ScpException.class, () -> other.importSealed(id, fileBytes, 15, dataCrc)).code());
        }
    }

    @Test
    void importDoesNotTreatZeroExpectedCrcAsWildcard(@TempDir Path otherDir) throws Exception {
        byte[] fileBytes;
        try (ChunkStore store = newStore()) {
            fileBytes = sealedBytes(store, id, "payload-payload");
        }

        try (ChunkStore other = new ChunkStore(otherDir)) {
            assertEquals(ErrorCode.CRC_MISMATCH,
                    assertThrows(ScpException.class, () -> other.importSealed(id, fileBytes, 15, 0)).code());
        }
    }

    @Test
    void importRejectsExistingOnDiskChunkWithTypedError(@TempDir Path otherDir) throws Exception {
        byte[] fileBytes;
        long sealedLength;
        int dataCrc;
        try (ChunkStore store = newStore()) {
            fileBytes = sealedBytes(store, id, "payload-payload");
            ChunkFormats.Trailer trailer = trailer(fileBytes);
            sealedLength = trailer.dataLength();
            dataCrc = trailer.dataCrc();
        }

        try (ChunkStore other = new ChunkStore(otherDir)) {
            Files.write(otherDir.resolve(ChunkFormats.baseName(id) + ".chunk"), new byte[] {1});
            assertEquals(ErrorCode.CHUNK_ALREADY_EXISTS,
                    assertThrows(ScpException.class,
                            () -> other.importSealed(id, fileBytes, sealedLength, dataCrc)).code());
        }
    }

    @Test
    void failedImportAfterMoveCleansPartialFilesForRetry(@TempDir Path otherDir) throws Exception {
        byte[] fileBytes;
        long sealedLength;
        int dataCrc;
        try (ChunkStore store = newStore()) {
            fileBytes = sealedBytes(store, id, "payload-payload");
            ChunkFormats.Trailer trailer = trailer(fileBytes);
            sealedLength = trailer.dataLength();
            dataCrc = trailer.dataCrc();
        }

        String base = ChunkFormats.baseName(id);
        Path dataPath = otherDir.resolve(base + ".chunk");
        Path tmpPath = otherDir.resolve(base + ".chunk.tmp");
        Path metaPath = otherDir.resolve(base + ".meta");
        Files.createDirectory(metaPath);
        Files.write(metaPath.resolve("blocker"), new byte[] {1});

        try (ChunkStore other = new ChunkStore(otherDir)) {
            assertThrows(IOException.class, () -> other.importSealed(id, fileBytes, sealedLength, dataCrc));
            assertTrue(Files.notExists(dataPath));
            assertTrue(Files.notExists(tmpPath));
            assertEquals(ErrorCode.CHUNK_NOT_FOUND,
                    assertThrows(ScpException.class, () -> other.stat(id)).code());

            Files.delete(metaPath.resolve("blocker"));
            Files.delete(metaPath);
            other.importSealed(id, fileBytes, sealedLength, dataCrc);
            assertTrue(other.contains(id));
        }
    }

    @Test
    void importRejectsMalformedSealedFiles(@TempDir Path otherDir) throws Exception {
        byte[] fileBytes;
        byte[] otherFileBytes;
        long sealedLength;
        int dataCrc;
        try (ChunkStore store = newStore()) {
            fileBytes = sealedBytes(store, id, "payload-payload");
            ChunkId otherId = new ChunkId(FileId.random(), 0);
            otherFileBytes = sealedBytes(store, otherId, "payload-payload");
            var t = trailer(fileBytes);
            sealedLength = t.dataLength();
            dataCrc = t.dataCrc();
        }

        try (ChunkStore other = new ChunkStore(otherDir)) {
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK,
                    new byte[ChunkFormats.HEADER_SIZE + ChunkFormats.TRAILER_SIZE - 1], -1, dataCrc);
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK, otherFileBytes, sealedLength, dataCrc);
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK, fileBytes.clone(), sealedLength + 1, dataCrc);
            assertImportFails(other, id, ErrorCode.CRC_MISMATCH, fileBytes.clone(), sealedLength, dataCrc + 1);

            byte[] badDataLength = fileBytes.clone();
            ChunkFormats.Trailer t = trailer(badDataLength);
            writeTrailer(badDataLength, new ChunkFormats.Trailer(badDataLength.length, t.footerStart(),
                    t.sectionCount(), t.incompatFlags(), t.footerCrc(), t.dataCrc()));
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK, badDataLength, -1, dataCrc);

            byte[] negativeDataLength = fileBytes.clone();
            t = trailer(negativeDataLength);
            writeTrailer(negativeDataLength, new ChunkFormats.Trailer(-1, t.footerStart(),
                    t.sectionCount(), t.incompatFlags(), t.footerCrc(), t.dataCrc()));
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK, negativeDataLength, -1, dataCrc);

            byte[] badGeometry = fileBytes.clone();
            t = trailer(badGeometry);
            writeTrailer(badGeometry, new ChunkFormats.Trailer(t.dataLength(), t.footerStart() + 1,
                    t.sectionCount(), t.incompatFlags(), t.footerCrc(), t.dataCrc()));
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK, badGeometry, -1, dataCrc);

            byte[] badIncompatFlags = fileBytes.clone();
            t = trailer(badIncompatFlags);
            writeTrailer(badIncompatFlags, new ChunkFormats.Trailer(t.dataLength(), t.footerStart(),
                    t.sectionCount(), 1, t.footerCrc(), t.dataCrc()));
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK, badIncompatFlags, -1, dataCrc);

            byte[] negativeSectionCount = fileBytes.clone();
            t = trailer(negativeSectionCount);
            writeTrailer(negativeSectionCount, new ChunkFormats.Trailer(t.dataLength(), t.footerStart(),
                    -1, t.incompatFlags(), t.footerCrc(), t.dataCrc()));
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK, negativeSectionCount, -1, dataCrc);

            byte[] sectionCountMismatch = withFooter(fileBytes, footerBytes(fileBytes),
                    trailer(fileBytes).sectionCount() + 1);
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK, sectionCountMismatch, -1, dataCrc);

            byte[] trailingFooterBytes = Arrays.copyOf(footerBytes(fileBytes), footerBytes(fileBytes).length + 1);
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK,
                    withFooter(fileBytes, trailingFooterBytes, trailer(fileBytes).sectionCount()), -1, dataCrc);

            byte[] badFooterCrc = fileBytes.clone();
            badFooterCrc[(int) trailer(badFooterCrc).footerStart()] ^= 1;
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK, badFooterCrc, -1, dataCrc);

            byte[] badSectionLength = ByteBuffer.allocate(12)
                    .putShort((short) ChunkFormats.SECTION_CRC_RANGES)
                    .putShort((short) 1)
                    .putInt(100)
                    .putInt(0)
                    .array();
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK,
                    withFooter(fileBytes, badSectionLength, 1), -1, dataCrc);

            byte[] negativeSectionLength = ByteBuffer.allocate(12)
                    .putShort((short) ChunkFormats.SECTION_CRC_RANGES)
                    .putShort((short) 1)
                    .putInt(-1)
                    .putInt(0)
                    .array();
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK,
                    withFooter(fileBytes, negativeSectionLength, 1), -1, dataCrc);

            byte[] validRange = ByteBuffer.allocate(12)
                    .putInt(ChunkFormats.CRC_RANGE_SIZE)
                    .putInt(1)
                    .putInt(dataCrc)
                    .array();
            byte[] badSectionCrc = section(ChunkFormats.SECTION_CRC_RANGES, validRange);
            badSectionCrc[badSectionCrc.length - 1] ^= 1;
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK,
                    withFooter(fileBytes, badSectionCrc, 1), -1, dataCrc);

            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK,
                    withFooter(fileBytes, footer(section(ChunkFormats.SECTION_CRC_RANGES, validRange),
                            section(ChunkFormats.SECTION_CRC_RANGES, validRange)), 2), -1, dataCrc);

            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK,
                    withFooter(fileBytes, section(ChunkFormats.SECTION_CRC_RANGES, new byte[4]), 1), -1, dataCrc);

            byte[] wrongRangeSize = ByteBuffer.allocate(8)
                    .putInt(ChunkFormats.CRC_RANGE_SIZE + 1)
                    .putInt(0)
                    .array();
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK,
                    withFooter(fileBytes, section(ChunkFormats.SECTION_CRC_RANGES, wrongRangeSize), 1), -1, dataCrc);

            byte[] negativeRangeCount = ByteBuffer.allocate(8)
                    .putInt(ChunkFormats.CRC_RANGE_SIZE)
                    .putInt(-1)
                    .array();
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK,
                    withFooter(fileBytes, section(ChunkFormats.SECTION_CRC_RANGES, negativeRangeCount), 1),
                    -1, dataCrc);

            byte[] invalidCount = ByteBuffer.allocate(12)
                    .putInt(ChunkFormats.CRC_RANGE_SIZE)
                    .putInt(2)
                    .putInt(dataCrc)
                    .array();
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK,
                    withFooter(fileBytes, section(ChunkFormats.SECTION_CRC_RANGES, invalidCount), 1), -1, dataCrc);

            byte[] countMismatch = ByteBuffer.allocate(8)
                    .putInt(ChunkFormats.CRC_RANGE_SIZE)
                    .putInt(0)
                    .array();
            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK,
                    withFooter(fileBytes, section(ChunkFormats.SECTION_CRC_RANGES, countMismatch), 1), -1, dataCrc);

            assertImportFails(other, id, ErrorCode.CORRUPT_CHUNK,
                    withFooter(fileBytes, footer(statsSection(sealedLength, 1)), 1), -1, dataCrc);
        }
    }

    @Test
    void negativeWireValuesAreRejectedWithoutDamage() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("payload"));

            // a negative seal length passes naive range checks (-5 is not > end) and
            // truncate(DATA_START - 5) would DESTROY the header before anything throws
            assertEquals(ErrorCode.INTERNAL,
                    assertThrows(ScpException.class, () -> store.seal(id, 1, -5, null)).code());
            // negative read/append/fetch offsets likewise must be typed rejections
            assertThrows(ScpException.class, () -> store.read(id, -1, 10));
            assertThrows(ScpException.class, () -> store.read(id, 0, -10));
            assertThrows(ScpException.class, () -> store.fetch(id, 0, -10));
            assertThrows(ScpException.class, () -> store.readLedger(id, -1));
            assertThrows(ScpException.class, () -> store.append(id, 1, -3, 0, bytes("x")));

            // and the chunk must be UNDAMAGED: header intact, still appendable, still sealable
            assertEquals(7, store.stat(id).localEndOffset());
            assertEquals(8, store.append(id, 1, 7, 7, bytes("!")).endOffset());
            assertEquals(8, store.seal(id, 1, 8, null).finalLength());
        }
    }

    @Test
    void sealedReadRejectsMissingCrcRangeMetadata() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("payload"));
            store.seal(id, 1, 7, null);

            setHandleObject(store, id, "sealedRangeCrcs", List.of());
            assertEquals(ErrorCode.CORRUPT_CHUNK,
                    assertThrows(ScpException.class, () -> store.read(id, 0, 1)).code());
        }
    }

    @Test
    void sealedReadRejectsTruncatedCrcRangeMetadata() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            byte[] payload = new byte[ChunkFormats.CRC_RANGE_SIZE + 1];
            store.append(id, 1, 0, 0, ByteBuffer.wrap(payload));
            store.seal(id, 1, payload.length, null);

            setHandleObject(store, id, "sealedRangeCrcs", List.of(Crc.of(payload, 0, ChunkFormats.CRC_RANGE_SIZE)));
            assertEquals(ErrorCode.CORRUPT_CHUNK,
                    assertThrows(ScpException.class,
                            () -> store.read(id, ChunkFormats.CRC_RANGE_SIZE, 1)).code());
        }
    }

    @Test
    void readAndFetchSizesAreClampedServerSide() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            byte[] big = new byte[9 << 20]; // 9 MB > the 8 MB per-request cap
            store.append(id, 1, 0, 0, ByteBuffer.wrap(big));
            store.seal(id, 1, big.length, null);

            // a request asking for "everything" must be clamped, not answered with one
            // chunk-sized allocation (callers loop; the frame layer caps at 64 MB anyway)
            var r = store.read(id, 0, Integer.MAX_VALUE);
            assertEquals(ChunkStore.MAX_REQUEST_BYTES, r.bytes().length);
            var f = store.fetch(id, 0, Integer.MAX_VALUE);
            assertEquals(ChunkStore.MAX_REQUEST_BYTES, f.bytes().length);
        }
    }

    @Test
    void scrubKeepsCleanSealedChunksHealthy() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("pristine"));
            var sealed = store.seal(id, 1, 8, null);

            assertEquals(0, store.scrubOnce());
            assertEquals(sealed.dataCrc(), store.inventory().get(0).crc());
        }
    }

    @Test
    void scrubSkipsOpenChunks() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("open"));

            assertEquals(0, store.scrubOnce());
            assertEquals(ChunkState.OPEN, store.stat(id).state());
        }
    }

    @Test
    void scrubDetectsDataRotAndExposesItThroughInventory() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("pristine-data!"));
            var sealed = store.seal(id, 1, 14, null);

            // inventory reports the seal-time CRC — descriptor and node agree
            assertEquals(sealed.dataCrc(), store.inventory().get(0).crc());

            // bit-rot in the data region: the STORED crc still matches the descriptor, so only
            // recomputation can catch it
            String base = ChunkFormats.baseName(id);
            try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(
                    dir.resolve(base + ".chunk"), java.nio.file.StandardOpenOption.WRITE)) {
                ch.write(ByteBuffer.wrap(new byte[]{0x7F}), ChunkFormats.DATA_START + 3);
            }
            assertEquals(sealed.dataCrc(), store.inventory().get(0).crc(),
                    "before scrub inventory still reports the seal-time crc");
            assertEquals(ErrorCode.CRC_MISMATCH,
                    assertThrows(ScpException.class, () -> store.read(id, 0, 14)).code(),
                    "sealed reads must validate the covering CRC range before returning bytes");

            int corrupt = store.scrubOnce();
            assertEquals(1, corrupt, "scrub must detect the rotted chunk");
            // inventory now exposes the RECOMPUTED crc — the coordinator's existing
            // corrupt-mismatch path drops this replica and re-repairs from good copies
            assertTrue(store.inventory().get(0).crc() != sealed.dataCrc(),
                    "inventory must reflect the actual bytes after scrub");
        }
    }

    @Test
    void recoveryLoadsSealedChunksAndDeletesLedgerRemnants() throws Exception {
        ChunkId chunkId = new ChunkId(FileId.random(), 0);
        String base = ChunkFormats.baseName(chunkId);
        Path ledgerPath = dir.resolve(base + ".j");
        try (ChunkStore store = newStore()) {
            open(store, chunkId, 1);
            store.append(chunkId, 1, 0, 0, bytes("sealed"));
            store.seal(chunkId, 1, 6, null);
        }
        Files.write(ledgerPath, new byte[] {1, 2, 3});

        try (ChunkStore recovered = newStore()) {
            assertTrue(recovered.contains(chunkId));
            assertArrayEquals("sealed".getBytes(), recovered.read(chunkId, 0, 100).bytes());
            assertTrue(Files.notExists(ledgerPath));
        }
    }

    @Test
    void sealedChunkRecoversFullDataWhenSealedSidecarLost() throws Exception {
        // C1 regression: with STRATA_SEAL_FSYNC=false (the default), seal() leaves the SEALED sidecar
        // (and footer/trailer) only in the page cache. If a crash loses that unforced SEALED state, the
        // on-disk sidecar still reads OPEN and recovery takes the OPEN branch, which rebuilds the chunk
        // by REPLAYING THE INTEGRITY LEDGER. So seal() must not remove the ledger until the SEALED state
        // is durable — otherwise recovery finds no ledger and truncates acknowledged data to zero.
        assumeTrue(!ChunkStore.booleanConf("strata.seal.fsync", "STRATA_SEAL_FSYNC", false),
                "guarantee under test is specific to the no-seal-fsync durability path");

        ChunkId chunkId = new ChunkId(FileId.random(), 0);
        String base = ChunkFormats.baseName(chunkId);
        Path metaPath = dir.resolve(base + ".meta");
        byte[] payload = "important-acknowledged-data".getBytes(StandardCharsets.UTF_8);
        int len = payload.length;

        try (ChunkStore store = newStore()) {
            open(store, chunkId, 1);
            store.append(chunkId, 1, 0, 0, ByteBuffer.wrap(payload));
            store.seal(chunkId, 1, len, null);
        }

        // Simulate the crash window: the unforced SEALED sidecar write never reached disk, so on
        // restart the sidecar still reads OPEN — with the chunk's data fully durable (lastKnownDO=len).
        Files.write(metaPath, new ChunkFormats.Sidecar(1, -1, len, ChunkState.OPEN).encode());

        try (ChunkStore recovered = newStore()) {
            assertTrue(recovered.contains(chunkId), "sealed chunk must survive recovery");
            assertArrayEquals(payload, recovered.read(chunkId, 0, len).bytes(),
                    "acknowledged data must be recovered from the retained ledger, not truncated to zero");
        }
    }

    @Test
    void sealRetainsLedgerUntilReclaimedWhenSealFsyncDisabled() throws Exception {
        assumeTrue(!ChunkStore.booleanConf("strata.seal.fsync", "STRATA_SEAL_FSYNC", false),
                "ledger retention is specific to the no-seal-fsync durability path");
        ChunkId chunkId = new ChunkId(FileId.random(), 0);
        Path ledgerPath = dir.resolve(ChunkFormats.baseName(chunkId) + ".j");
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        try (ChunkStore store = newStore()) {
            open(store, chunkId, 1);
            store.append(chunkId, 1, 0, 0, ByteBuffer.wrap(payload));
            store.seal(chunkId, 1, payload.length, null);

            assertTrue(Files.exists(ledgerPath),
                    "seal must retain the integrity ledger until the SEALED state is durable");

            store.reclaimSealedLedgersOnce(); // forces the SEALED state durable, then unlinks the ledger

            assertTrue(Files.notExists(ledgerPath),
                    "reclaim must drop the ledger once the SEALED state is forced durable");
            assertEquals(1, store.sealedLedgerReclaims());
            assertArrayEquals(payload, store.read(chunkId, 0, payload.length).bytes(),
                    "chunk must stay readable from its durable trailer after reclaim");

            store.reclaimSealedLedgersOnce(); // idempotent: nothing left pending
            assertEquals(1, store.sealedLedgerReclaims());
        }
    }

    @Test
    void sealWithSealFsyncDropsLedgerImmediatelyWithoutReclaim() throws Exception {
        ChunkId chunkId = new ChunkId(FileId.random(), 0);
        Path ledgerPath = dir.resolve(ChunkFormats.baseName(chunkId) + ".j");
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        try (ChunkStore store = new ChunkStore(dir, true)) { // seal fsync ON
            open(store, chunkId, 1);
            store.append(chunkId, 1, 0, 0, ByteBuffer.wrap(payload));
            store.seal(chunkId, 1, payload.length, null);

            // With seal fsync on, the SEALED footer/sidecar are forced durable at seal time, so the
            // ledger is dropped immediately (async) rather than retained for background reclamation.
            waitFor(() -> Files.notExists(ledgerPath),
                    "seal with fsync on should delete the ledger without waiting for reclaim");
            store.reclaimSealedLedgersOnce();
            assertEquals(0, store.sealedLedgerReclaims(),
                    "no ledger should be pending reclaim when seal fsync is on");
            assertArrayEquals(payload, store.read(chunkId, 0, payload.length).bytes(),
                    "chunk must remain readable after a seal-fsync seal");
        }
    }

    @Test
    void recoveryReadServesUndurableTailThatClientReadClampsAway() throws Exception {
        ChunkId chunkId = new ChunkId(FileId.random(), 0);
        byte[] durable = "durable-prefix".getBytes(StandardCharsets.UTF_8);
        byte[] tail = "undurable-tail".getBytes(StandardCharsets.UTF_8);
        int d = durable.length;
        int total = d + tail.length;
        byte[] all = new byte[total];
        System.arraycopy(durable, 0, all, 0, d);
        System.arraycopy(tail, 0, all, d, tail.length);

        try (ChunkStore store = newStore()) {
            open(store, chunkId, 1);
            store.append(chunkId, 1, 0, 0, ByteBuffer.wrap(durable)); // below the durable high watermark
            store.append(chunkId, 1, d, d, ByteBuffer.wrap(tail));    // above it: end=total, lastKnownDO=d

            // Client read is clamped to the durable high watermark and never exposes the never-acked tail.
            try (ChunkStore.ReadRegionResult clientTail = store.readRegion(chunkId, d, total)) {
                assertEquals(0, clientTail.length(),
                        "client read must clamp away the never-acked tail above the durable watermark");
            }
            try (ChunkStore.ReadRegionResult clientPrefix = store.readRegion(chunkId, 0, total)) {
                assertEquals(d, clientPrefix.length(), "client read serves only the durable prefix");
                assertTrue(clientPrefix.bytes() == null,
                        "durable-prefix open read is zero-copy (a channel, not materialized bytes)");
            }

            // Recovery read includes the undurable tail, materialized + integrity-verified (not a
            // zero-copy channel), so seal recovery sees quorum-durable bytes instead of sealing short.
            try (ChunkStore.ReadRegionResult recovery = store.readRegionForRecovery(chunkId, 0, total)) {
                assertEquals(total, recovery.length(), "recovery read must include the undurable tail");
                assertTrue(recovery.channel() == null, "recovery tail must be materialized, not zero-copy");
                assertArrayEquals(all, recovery.bytes(), "recovery read must return the full verified bytes");
            }
        }
    }

    @Test
    void recoverySkipsUnparseableAndTruncatedChunkFiles() throws Exception {
        Path invalidName = dir.resolve("not-a-valid-chunk-name.chunk");
        Files.write(invalidName, new byte[] {1});

        ChunkId truncated = new ChunkId(FileId.random(), 0);
        String base = ChunkFormats.baseName(truncated);
        Files.write(dir.resolve(base + ".chunk"), new byte[] {1, 2, 3});
        Files.write(dir.resolve(base + ".meta"),
                new ChunkFormats.Sidecar(1, -1, 0, ChunkState.OPEN).encode());

        try (ChunkStore recovered = newStore()) {
            assertEquals(0, recovered.inventory().size());
            assertTrue(Files.notExists(invalidName));
            assertTrue(Files.notExists(dir.resolve(base + ".chunk")));
            assertTrue(Files.notExists(dir.resolve(base + ".meta")));
            try (Stream<Path> files = Files.list(dir)) {
                assertEquals(3, files.filter(p -> p.getFileName().toString().contains(".quarantine-")).count());
            }
        }
    }

    @Test
    void recoveryTruncatesLedgerEntryBeyondDataSize() throws Exception {
        ChunkId chunkId = new ChunkId(FileId.random(), 0);
        String base = ChunkFormats.baseName(chunkId);
        try (ChunkStore store = newStore()) {
            open(store, chunkId, 1);
            store.append(chunkId, 1, 0, 0, bytes("abcd"));
        }
        Files.write(dir.resolve(base + ".j"),
                new ChunkFormats.LedgerEntry(5, Crc.of("abcde".getBytes()), 1).encode());

        try (ChunkStore recovered = newStore()) {
            assertTrue(recovered.contains(chunkId));
            assertEquals(0, recovered.stat(chunkId).localEndOffset());
        }
    }

    @Test
    void recoveryTruncatesImpossibleLedgerEndOffset() throws Exception {
        ChunkId chunkId = new ChunkId(FileId.random(), 0);
        try (ChunkStore store = newStore()) {
            open(store, chunkId, 1);
        }

        Files.write(dir.resolve(ChunkFormats.baseName(chunkId) + ".j"),
                new ChunkFormats.LedgerEntry(Long.MAX_VALUE, 0, 1).encode());

        try (ChunkStore recovered = newStore()) {
            assertTrue(recovered.contains(chunkId));
            assertEquals(0, recovered.stat(chunkId).localEndOffset());
            assertEquals(0, recovered.readLedger(chunkId, 0).size());
            assertEquals(1, recovered.append(chunkId, 1, 0, 0, bytes("x")).endOffset());
        }
    }

    @Test
    void recoveryRemovesChunkCreatedBeforeSidecarWasPersisted() throws Exception {
        ChunkId chunkId = new ChunkId(FileId.random(), 0);
        String base = ChunkFormats.baseName(chunkId);
        Path dataPath = dir.resolve(base + ".chunk");
        Path ledgerPath = dir.resolve(base + ".j");
        Files.write(dataPath, new byte[] {1, 2, 3});
        Files.write(ledgerPath, new byte[] {4, 5, 6});

        try (ChunkStore recovered = newStore()) {
            assertEquals(false, recovered.contains(chunkId));
            assertTrue(Files.notExists(dataPath));
            assertTrue(Files.notExists(ledgerPath));
        }
    }

    @Test
    void recoveryDiscardsOpenDataWhenLedgerIsMissing() throws Exception {
        ChunkId chunkId = new ChunkId(FileId.random(), 0);
        try (ChunkStore store = newStore()) {
            open(store, chunkId, 1);
            store.append(chunkId, 1, 0, 0, bytes("payload"));
        }

        Files.delete(dir.resolve(ChunkFormats.baseName(chunkId) + ".j"));

        try (ChunkStore recovered = newStore()) {
            assertTrue(recovered.contains(chunkId));
            assertEquals(0, recovered.stat(chunkId).localEndOffset());
            assertEquals(1, recovered.append(chunkId, 1, 0, 0, bytes("x")).endOffset());
        }
    }

    @Test
    void recoveryKeepsEmptyOpenChunkWhenLedgerIsMissing() throws Exception {
        ChunkId chunkId = new ChunkId(FileId.random(), 0);
        try (ChunkStore store = newStore()) {
            open(store, chunkId, 1);
        }

        Files.delete(dir.resolve(ChunkFormats.baseName(chunkId) + ".j"));

        try (ChunkStore recovered = newStore()) {
            assertTrue(recovered.contains(chunkId));
            assertEquals(0, recovered.stat(chunkId).localEndOffset());
            assertEquals(1, recovered.append(chunkId, 1, 0, 0, bytes("x")).endOffset());
        }
    }

    @Test
    void recoveryTruncatesZeroLengthAndCorruptLedgerTails() throws Exception {
        ChunkId zeroExtent = new ChunkId(FileId.random(), 0);
        try (ChunkStore store = newStore()) {
            open(store, zeroExtent, 1);
            store.append(zeroExtent, 1, 0, 0, bytes("abcd"));
        }
        Files.write(dir.resolve(ChunkFormats.baseName(zeroExtent) + ".j"),
                new ChunkFormats.LedgerEntry(0, 0, 1).encode());

        try (ChunkStore recovered = newStore()) {
            assertEquals(0, recovered.stat(zeroExtent).localEndOffset());
        }

        ChunkId badCrc = new ChunkId(FileId.random(), 0);
        try (ChunkStore store = newStore()) {
            open(store, badCrc, 1);
            store.append(badCrc, 1, 0, 0, bytes("abcd"));
        }
        Files.write(dir.resolve(ChunkFormats.baseName(badCrc) + ".j"),
                new ChunkFormats.LedgerEntry(4, 0x12345678, 1).encode());

        try (ChunkStore recovered = newStore()) {
            assertEquals(0, recovered.stat(badCrc).localEndOffset());
        }
    }

    @Test
    void deleteRemovesEverything() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("x"));
            assertEquals(ErrorCode.OK, store.delete(id));
            assertEquals(ErrorCode.CHUNK_NOT_FOUND, store.delete(id));
            assertEquals(ErrorCode.CHUNK_NOT_FOUND,
                    assertThrows(ScpException.class, () -> store.read(id, 0, 1)).code());
            assertEquals(0, store.inventory().size());
        }
    }

    @Test
    void footerLengthRejectsDefensiveGeometryCases() throws Exception {
        long emptySealedFileLen = ChunkFormats.DATA_START + ChunkFormats.TRAILER_SIZE;
        assertEquals(0, checkedFooterLength(new ChunkFormats.Trailer(
                0, ChunkFormats.DATA_START, 0, 0, 0, 0), emptySealedFileLen));

        ScpException negativeLength = assertThrows(ScpException.class, () -> checkedFooterLength(
                new ChunkFormats.Trailer(-1, ChunkFormats.DATA_START, 0, 0, 0, 0), emptySealedFileLen));
        assertEquals(ErrorCode.CORRUPT_CHUNK, negativeLength.code());

        ScpException negativeFooterLen = assertThrows(ScpException.class, () -> checkedFooterLength(
                new ChunkFormats.Trailer(0, emptySealedFileLen - ChunkFormats.TRAILER_SIZE + 1,
                        0, 0, 0, 0), emptySealedFileLen));
        assertEquals(ErrorCode.CORRUPT_CHUNK, negativeFooterLen.code());

        long enormousFileLen = (long) Integer.MAX_VALUE + ChunkFormats.TRAILER_SIZE + 1;
        ScpException hugeFooterLen = assertThrows(ScpException.class, () -> checkedFooterLength(
                new ChunkFormats.Trailer(0, 0, 0, 0, 0, 0), enormousFileLen));
        assertEquals(ErrorCode.CORRUPT_CHUNK, hugeFooterLen.code());
    }

    @Test
    void crcRangeFooterRejectsHugeExpectedCountAndTrailingPayload() throws Exception {
        byte[] emptyCrcRanges = section(ChunkFormats.SECTION_CRC_RANGES,
                ByteBuffer.allocate(8)
                        .putInt(ChunkFormats.CRC_RANGE_SIZE)
                        .putInt(0)
                        .array());
        long tooManyRangesLength = (long) ChunkFormats.CRC_RANGE_SIZE * (Integer.MAX_VALUE + 1L);

        ScpException tooMany = assertThrows(ScpException.class,
                () -> decodeCrcRanges(emptyCrcRanges, tooManyRangesLength, 1));
        assertEquals(ErrorCode.CORRUPT_CHUNK, tooMany.code());

        byte[] trailingPayload = section(ChunkFormats.SECTION_CRC_RANGES,
                ByteBuffer.allocate(16)
                        .putInt(ChunkFormats.CRC_RANGE_SIZE)
                        .putInt(1)
                        .putInt(0)
                        .putInt(0)
                        .array());

        ScpException trailing = assertThrows(ScpException.class,
                () -> decodeCrcRanges(trailingPayload, ChunkFormats.CRC_RANGE_SIZE, 1));
        assertEquals(ErrorCode.CORRUPT_CHUNK, trailing.code());
    }

    @Test
    void ioHelpersRejectZeroProgressChannels() throws Exception {
        try (FileChannel zeroRead = new ZeroProgressFileChannel(false, true, false)) {
            IOException e = assertThrows(IOException.class,
                    () -> readFully(zeroRead, ByteBuffer.allocate(1), 0));
            assertTrue(e.getMessage().contains("zero-byte read"));
        }

        try (FileChannel zeroWrite = new ZeroProgressFileChannel(false, false, true)) {
            IOException e = assertThrows(IOException.class,
                    () -> writeFully(zeroWrite, ByteBuffer.wrap(new byte[] {1}), 0));
            assertTrue(e.getMessage().contains("write failed"));
        }
    }

    @Test
    void failedImportCleanupContinuesWhenCleanupOperationsFail() throws Exception {
        try (ChunkStore store = newStore()) {
            ChunkId failed = new ChunkId(FileId.random(), 0);
            Object failedHandle = newHandle(store, failed);
            setObject(failedHandle, "data", new ZeroProgressFileChannel(true, false, false));

            Path tmp = dir.resolve("failed-import.tmp");
            Files.createDirectory(tmp);
            Files.write(tmp.resolve("blocker"), new byte[] {1});

            Path dataPath = (Path) getObject(failedHandle, "dataPath");
            Files.createDirectory(dataPath);
            Files.write(dataPath.resolve("blocker"), new byte[] {1});

            Path metaPath = (Path) getObject(failedHandle, "metaPath");
            Files.createDirectory(metaPath);
            Files.write(metaPath.resolve("blocker"), new byte[] {1});

            cleanupFailedImport(store, failedHandle, tmp, true, true);

            assertTrue(Files.exists(tmp));
            assertTrue(Files.exists(dataPath));
            assertTrue(Files.exists(metaPath));
        }
    }

    @Test
    void readCountersTrackServedReads() throws Exception {
        try (ChunkStore store = newStore()) {
            assertEquals(0, store.readOps());
            assertEquals(0, store.readBytes());

            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("hello world")); // 11 bytes, chunk still OPEN
            store.append(id, 1, 11, 11, ByteBuffer.allocate(0)); // durable-offset beacon

            // OPEN-chunk read: bytes materialized under the lock, counted by served length
            var r1 = store.readRegion(id, 0, 1024);
            assertEquals(11, r1.length());
            assertEquals(1, store.readOps());
            assertEquals(11, store.readBytes());

            // partial read accrues
            var r2 = store.readRegion(id, 6, 1024); // "world" -> 5 bytes
            assertEquals(5, r2.length());
            assertEquals(2, store.readOps());
            assertEquals(16, store.readBytes());

            // read at/after end serves nothing and must NOT count
            var r3 = store.readRegion(id, 11, 1024);
            assertEquals(0, r3.length());
            assertEquals(2, store.readOps());
            assertEquals(16, store.readBytes());

            // SEALED read path also counts after CRC verification
            store.seal(id, 1, 11, null);
            var r4 = store.readRegion(id, 0, 1024);
            assertEquals(11, r4.length());
            assertEquals(3, store.readOps());
            assertEquals(27, store.readBytes());
        }
    }

    @Test
    void readCountersIgnoreNonClientReadPaths() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("abc")); // 3 bytes

            // legacy synchronous read() is NOT the client data-path and must not count
            store.read(id, 0, 1024);
            assertEquals(0, store.readOps());
            assertEquals(0, store.readBytes());

            // FETCH_CHUNK (replication) goes through fetch(), also out of scope
            store.seal(id, 1, 3, null);
            store.fetch(id, 0, Integer.MAX_VALUE);
            assertEquals(0, store.readOps());
            assertEquals(0, store.readBytes());
        }
    }

    @Test
    void chunkStateMetricsUseSynchronizedSnapshotOrVolatileState() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            Object handle = handle(store, id);
            Field state = handle.getClass().getDeclaredField("state");
            if (Modifier.isVolatile(state.getModifiers())) {
                return;
            }

            AtomicReference<Integer> openCount = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread reader = new Thread(() -> {
                try {
                    openCount.set(store.openChunks());
                } catch (Throwable t) {
                    failure.set(t);
                }
            }, "chunk-state-metric-reader");

            synchronized (handle) {
                reader.start();
                waitFor(() -> reader.getState() == Thread.State.BLOCKED || !reader.isAlive(),
                        "metric reader did not reach the handle state read");
                assertTrue(reader.isAlive() && reader.getState() == Thread.State.BLOCKED,
                        "openChunks() must synchronize on each handle or make Handle.state volatile");
                assertEquals(null, openCount.get());
            }

            reader.join(TimeUnit.SECONDS.toMillis(3));
            assertTrue(!reader.isAlive(), "metric reader did not finish");
            assertEquals(null, failure.get());
            assertEquals(1, openCount.get());
        }
    }

    @Test
    void failedDeleteKeepsChunkVisibleForRetry() throws Exception {
        try (ChunkStore store = newStore()) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("x"));

            Set<PosixFilePermission> originalPermissions;
            try {
                originalPermissions = Files.getPosixFilePermissions(dir);
            } catch (UnsupportedOperationException e) {
                assumeTrue(false, "requires POSIX file permissions");
                return;
            }

            ErrorCode result;
            try {
                Files.setPosixFilePermissions(dir, Set.of(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_EXECUTE));
                result = store.delete(id);
            } finally {
                Files.setPosixFilePermissions(dir, originalPermissions);
            }

            assumeTrue(result != ErrorCode.OK, "delete failure not reproducible on this filesystem");
            assertEquals(ErrorCode.INTERNAL, result);
            assertTrue(store.contains(id));
            assertEquals(1, store.inventory().size());
            assertEquals(ErrorCode.OK, store.delete(id));
            assertEquals(ErrorCode.CHUNK_NOT_FOUND, store.delete(id));
        }
    }

    @Test
    void exposesChannelCacheAccessorsAndClosesCleanly() throws Exception {
        try (ChunkStore store = newStore()) {
            assertEquals(0, store.cachedChannels());
            assertEquals(0, store.channelCacheHits());
            assertEquals(0, store.channelCacheMisses());
            assertEquals(0, store.channelCacheEvictions());
            assertTrue(store.channelCacheCapacity() >= 128);
        }
    }

    @Test
    void sealedVerifiedReadGoesThroughChannelCache() throws Exception {
        try (ChunkStore store = newStore()) {
            sealedBytes(store, id, "cache-me-please");
            store.reclaimSealedLedgersOnce(); // become evictable (h.data nulled in Task 8; harmless here)
            assertArrayEquals("cache".getBytes(),
                    store.read(id, 0, 5).bytes());
            assertTrue(store.channelCacheMisses() + store.channelCacheHits() >= 1,
                    "a sealed read must consult the channel cache");
            assertTrue(store.cachedChannels() >= 1, "the sealed channel is cached after a read");
        }
    }

    @Test
    void sealedFetchGoesThroughChannelCache() throws Exception {
        try (ChunkStore store = newStore()) {
            byte[] full = sealedBytes(store, id, "fetch-via-cache");
            long missesBefore = store.channelCacheMisses() + store.channelCacheHits();
            byte[] got = store.fetch(id, 0, Integer.MAX_VALUE).bytes();
            assertArrayEquals(full, got);
            assertTrue(store.channelCacheMisses() + store.channelCacheHits() > missesBefore,
                    "fetch of a sealed chunk must consult the channel cache");
        }
    }

    @Test
    void scrubReadsSealedDataThroughCache() throws Exception {
        try (ChunkStore store = newStore()) {
            sealedBytes(store, id, "scrub-me");
            assertEquals(0, store.scrubOnce(), "clean chunk has no corruption");
            assertTrue(store.cachedChannels() >= 1);
        }
    }

    @Test
    void sealedReadRegionSharesOneCachedFdAcrossConcurrentReaders() throws Exception {
        try (ChunkStore store = newStore()) {
            sealedBytes(store, id, "shared-zero-copy");
            ChunkStore.ReadRegionResult r1 = store.readRegion(id, 0, 6);
            ChunkStore.ReadRegionResult r2 = store.readRegion(id, 6, 4);
            try {
                assertTrue(r1.channel() != null && r2.channel() != null, "sealed reads are zero-copy");
                assertSame(r1.channel(), r2.channel(), "concurrent sealed readers share one cached FD");
                assertEquals(1, store.cachedChannels());
                assertArrayEquals("shared".getBytes(), consumeRegion(r1));
            } finally {
                r1.close();
                r2.close();
            }
            // releasing the leases leaves the channel cached (idle), not closed
            assertEquals(1, store.cachedChannels());
        }
    }

    @Test
    void sealedReadRegionLeaseKeepsFdOpenUntilReleased() throws Exception {
        try (ChunkStore store = newStore()) {
            sealedBytes(store, id, "lease-lifetime");
            ChunkStore.ReadRegionResult r = store.readRegion(id, 0, 5);
            FileChannel ch = r.channel();
            assertTrue(ch.isOpen());
            r.close(); // releases the lease; channel stays cached/open
            assertTrue(ch.isOpen(), "release returns the FD to the cache, does not close it");
        }
    }

    @Test
    void recoveryOpensNoPersistentFdPerSealedChunk() throws Exception {
        ChunkId a = new ChunkId(FileId.random(), 0);
        ChunkId b = new ChunkId(FileId.random(), 0);
        try (ChunkStore store = new ChunkStore(dir, true)) { // seal-fsync on: sealed + durable immediately
            sealedBytes(store, a, "alpha");
            sealedBytes(store, b, "bravo");
        }
        try (ChunkStore recovered = new ChunkStore(dir, true)) {
            assertEquals(2, recovered.sealedChunks());
            assertNull(handleData(recovered, a), "recovery must keep no persistent data FD for a sealed chunk");
            assertNull(handleData(recovered, b), "recovery must keep no persistent data FD for a sealed chunk");
            assertEquals(0, recovered.cachedChannels(),
                    "recovery must not open a persistent data FD per sealed chunk");
            // a read lazily opens (and caches) exactly one channel
            assertArrayEquals("alpha".getBytes(), recovered.read(a, 0, 5).bytes());
            assertEquals(1, recovered.cachedChannels());
        }
    }

    @Test
    void sealClosesPersistentDataFdWhenSealFsyncOn() throws Exception {
        try (ChunkStore store = new ChunkStore(dir, true)) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("durable-seal"));
            store.seal(id, 1, "durable-seal".length(), null);
            assertNull(handleData(store, id), "seal-fsync=on must drop the writable FD at seal");
            assertArrayEquals("durable-seal".getBytes(), store.read(id, 0, 12).bytes());
        }
    }

    @Test
    void reclaimClosesPersistentDataFdWhenSealFsyncOff() throws Exception {
        try (ChunkStore store = newStore()) { // seal-fsync off
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("reclaim-me"));
            store.seal(id, 1, "reclaim-me".length(), null);
            assertNotNull(handleData(store, id), "seal-fsync=off keeps the FD until reclaim");
            store.reclaimSealedLedgersOnce();
            assertNull(handleData(store, id), "reclaim drops the writable FD once durable");
            assertArrayEquals("reclaim-me".getBytes(), store.read(id, 0, 10).bytes());
        }
    }

    @Test
    void deleteInvalidatesCachedChannel() throws Exception {
        try (ChunkStore store = newStore()) {
            sealedBytes(store, id, "delete-me");
            store.read(id, 0, 4); // caches the channel
            assertEquals(1, store.cachedChannels());
            assertEquals(ErrorCode.OK, store.delete(id));
            assertEquals(0, store.cachedChannels(), "delete must invalidate the cached channel");
        }
    }

    @Test
    void readRegionLeaseSurvivesConcurrentDelete() throws Exception {
        try (ChunkStore store = newStore()) {
            sealedBytes(store, id, "inode-alive");
            ChunkStore.ReadRegionResult region = store.readRegion(id, 0, 5);
            try {
                assertEquals(ErrorCode.OK, store.delete(id)); // unlinks file; leased FD keeps inode alive
                assertArrayEquals("inode".getBytes(), consumeRegion(region),
                        "an in-flight leased transfer still reads correct bytes after delete");
            } finally {
                region.close();
            }
        }
    }

    /** Reflectively reads the private Handle.data field for the given chunk (test-only). */
    private static FileChannel handleData(ChunkStore store, ChunkId chunkId) throws Exception {
        Object handle = handle(store, chunkId);
        if (handle == null) return null;
        Field dataField = handle.getClass().getDeclaredField("data");
        dataField.setAccessible(true);
        return (FileChannel) dataField.get(handle);
    }

    @Test
    void cachedChannelsStayBoundedAsSealedCountExceedsCapacity() throws Exception {
        // Use a tiny explicit capacity so the bound is observable without thousands of files.
        // CHANNEL_CACHE_MAX_SIZE is static-final (read once at class load), so setting a system
        // property inside a test is unreliable once the class is already loaded by earlier tests.
        // The package-private ChunkStore(dir, sealFsync, capacity) constructor sidesteps that.
        int tinyCapacity = 4;
        int n = 32; // >> tinyCapacity, guarantees eviction
        try (ChunkStore store = new ChunkStore(dir, true, tinyCapacity)) {
            List<ChunkId> ids = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                ChunkId c = new ChunkId(FileId.random(), 0);
                ids.add(c);
                sealedBytes(store, c, "payload-" + i);
            }
            // read every chunk once: misses open channels, eviction keeps the open set bounded
            for (ChunkId c : ids) {
                store.read(c, 0, 3);
            }
            assertTrue(store.cachedChannels() <= store.channelCacheCapacity(),
                    "open cached channels must not exceed capacity when no leases are held");
            assertTrue(store.channelCacheEvictions() > 0, "eviction must have fired");
        }
    }
}
