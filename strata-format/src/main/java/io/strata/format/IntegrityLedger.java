package io.strata.format;

import io.strata.common.Crc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.strata.format.ChunkFormats.LEDGER_ENTRY_SIZE;

/**
 * Per-chunk integrity ledger `<chunk>.j` (tech design §11.3): one 24-byte entry per append.
 * Crash recovery reads the valid entry prefix and verifies tail data CRCs — the data node
 * never parses payload bytes, even to recover. Deleted at seal.
 */
public final class IntegrityLedger implements AutoCloseable {
    private static final int INITIAL_ENTRIES = 1024;
    private static final int INITIAL_REUSABLE_SPAN_ENTRIES = 64;
    private static final ThreadLocal<EntryScratch> REUSABLE_ENTRY_SPAN =
            ThreadLocal.withInitial(() -> new EntryScratch(INITIAL_REUSABLE_SPAN_ENTRIES));

    private final FileChannel channel;
    // In-memory mirror, ordered by endOffset. Kept as primitives so the append hot path does not
    // allocate one LedgerEntry record per write; public APIs materialize objects only at boundaries.
    private long[] endOffsets;
    private int[] payloadCrcs;
    // Most chunks are written under a single epoch. Keep that hot case as a scalar and expand only
    // when a recovery/fence path appends entries with a different epoch.
    private int[] writeEpochs;
    private int uniformWriteEpoch;
    private int size;
    // appends are single-threaded under the owning chunk's monitor, so one scratch buffer can be
    // reused for every entry encode instead of allocating a byte[] + two wrappers per record
    private final ByteBuffer scratch = ByteBuffer.allocate(LEDGER_ENTRY_SIZE);

    private IntegrityLedger(FileChannel channel) {
        this(channel, new long[INITIAL_ENTRIES], new int[INITIAL_ENTRIES], null, 0, 0);
    }

    private IntegrityLedger(FileChannel channel, int initialEntries) {
        this(channel, new long[initialCapacity(initialEntries)], new int[initialCapacity(initialEntries)],
                null, 0, 0);
    }

    private IntegrityLedger(FileChannel channel, long[] endOffsets, int[] payloadCrcs, int[] writeEpochs,
                            int size, int uniformWriteEpoch) {
        this.channel = channel;
        this.endOffsets = endOffsets;
        this.payloadCrcs = payloadCrcs;
        this.writeEpochs = writeEpochs;
        this.uniformWriteEpoch = uniformWriteEpoch;
        this.size = size;
    }

    public static IntegrityLedger create(Path path) throws IOException {
        return create(path, INITIAL_ENTRIES);
    }

    static IntegrityLedger create(Path path, int initialEntries) throws IOException {
        FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE,
                StandardOpenOption.READ);
        return new IntegrityLedger(ch, initialEntries);
    }

    /**
     * In-memory ledger with no on-disk backing: append/force/truncateTo/close are no-ops against disk,
     * so entries do NOT survive a restart. For tests and transient in-memory verification only — never
     * for a chunk that must recover its integrity ledger after a crash.
     */
    public static IntegrityLedger memory() {
        return new IntegrityLedger(null);
    }

    static IntegrityLedger memory(int initialEntries) {
        return new IntegrityLedger(null, initialEntries);
    }

    /**
     * Opens an existing ledger, keeping only the prefix of CRC-valid entries with strictly
     * increasing endOffset; the file is truncated to that prefix (torn tail discarded).
     */
    public static IntegrityLedger open(Path path) throws IOException {
        FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.READ);
        byte[] all = Files.readAllBytes(path);
        int fullEntries = all.length / LEDGER_ENTRY_SIZE;
        int capacity = Math.max(INITIAL_ENTRIES, fullEntries);
        long[] endOffsets = new long[capacity];
        int[] payloadCrcs = new int[capacity];
        int[] loadedWriteEpochs = new int[capacity];
        int size = 0;
        long prevEnd = 0;
        int uniformWriteEpoch = 0;
        boolean uniform = true;
        for (int i = 0; i < fullEntries; i++) {
            ChunkFormats.LedgerEntry e = ChunkFormats.LedgerEntry.decodeOrNull(all, i * LEDGER_ENTRY_SIZE);
            if (e == null || e.endOffset() <= prevEnd) break;
            endOffsets[size] = e.endOffset();
            payloadCrcs[size] = e.payloadCrc();
            loadedWriteEpochs[size] = e.writeEpoch();
            if (size == 0) {
                uniformWriteEpoch = e.writeEpoch();
            } else if (e.writeEpoch() != uniformWriteEpoch) {
                uniform = false;
            }
            size++;
            prevEnd = e.endOffset();
        }
        ch.truncate((long) size * LEDGER_ENTRY_SIZE);
        ch.position(ch.size());
        return new IntegrityLedger(ch, endOffsets, payloadCrcs, uniform ? null : loadedWriteEpochs, size,
                uniformWriteEpoch);
    }

    public void append(ChunkFormats.LedgerEntry entry) throws IOException {
        append(entry.endOffset(), entry.payloadCrc(), entry.writeEpoch());
    }

    public void append(long endOffset, int payloadCrc, int writeEpoch) throws IOException {
        ensureCapacity(size + 1);
        if (writeEpochs == null) {
            if (size == 0) {
                uniformWriteEpoch = writeEpoch;
            } else if (writeEpoch != uniformWriteEpoch) {
                expandWriteEpochs();
            }
        }
        if (channel != null) {
            scratch.clear();
            encodeInto(scratch, endOffset, payloadCrc, writeEpoch);
            scratch.flip();
            ChunkFormats.writeFully(channel, scratch, (long) size * LEDGER_ENTRY_SIZE);
        }
        endOffsets[size] = endOffset;
        payloadCrcs[size] = payloadCrc;
        if (writeEpochs != null) {
            writeEpochs[size] = writeEpoch;
        }
        size++;
    }

    public void force() throws IOException {
        if (channel != null) {
            channel.force(false);
        }
    }

    public List<ChunkFormats.LedgerEntry> entries() {
        List<ChunkFormats.LedgerEntry> out = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            out.add(entryAt(i));
        }
        return List.copyOf(out);
    }

    public static final class EntrySpan {
        private long firstStart;
        private long[] endOffsets;
        private int[] payloadCrcs;
        private int[] writeEpochs;
        private int uniformWriteEpoch;
        private int length;
        private boolean reusable;
        private EntryScratch scratch;

        private EntrySpan() {
        }

        private EntrySpan reset(long firstStart, long[] endOffsets, int[] payloadCrcs, int[] writeEpochs,
                                int uniformWriteEpoch, int length, boolean reusable, EntryScratch scratch) {
            this.firstStart = firstStart;
            this.endOffsets = endOffsets;
            this.payloadCrcs = payloadCrcs;
            this.writeEpochs = writeEpochs;
            this.uniformWriteEpoch = uniformWriteEpoch;
            this.length = length;
            this.reusable = reusable;
            this.scratch = scratch;
            return this;
        }

        public long firstStart() {
            return firstStart;
        }

        public ChunkFormats.LedgerEntry[] entries() {
            ChunkFormats.LedgerEntry[] out = new ChunkFormats.LedgerEntry[length];
            for (int i = 0; i < length; i++) {
                out[i] = new ChunkFormats.LedgerEntry(endOffsets[i], payloadCrcs[i], writeEpochAt(i));
            }
            return out;
        }

        long endOffset(int index) {
            return endOffsets[index];
        }

        int payloadCrc(int index) {
            return payloadCrcs[index];
        }

        private int writeEpochAt(int index) {
            return writeEpochs == null ? uniformWriteEpoch : writeEpochs[index];
        }

        int length() {
            return length;
        }

        void clear() {
            if (reusable) {
                firstStart = 0;
                uniformWriteEpoch = 0;
                length = 0;
                EntryScratch localScratch = scratch;
                scratch = null;
                if (localScratch != null) {
                    localScratch.inUse = false;
                }
            }
        }
    }

    /**
     * Returns the minimal ledger-entry span that covers [offset, readEnd). Entries are monotonic by
     * endOffset, so the first covering entry is found by binary search instead of scanning every append.
     */
    public EntrySpan entriesCovering(long offset, long readEnd) {
        return entriesCovering(offset, readEnd, false);
    }

    /**
     * Returns a thread-local reusable span. The span is valid only until {@link EntrySpan#clear()} or
     * the next reusable lookup on this thread; use {@link #entriesCovering(long, long)} for a copy.
     */
    EntrySpan reusableEntriesCovering(long offset, long readEnd) {
        return entriesCovering(offset, readEnd, true);
    }

    private EntrySpan entriesCovering(long offset, long readEnd, boolean reusable) {
        int first = firstEntryEndingAfter(offset);
        long firstStart = first == 0 ? 0 : endOffsets[first - 1];
        int end = first;
        for (; end < size; end++) {
            if (endOffsets[end] >= readEnd) {
                end++;
                break;
            }
        }
        int length = end - first;
        if (reusable) {
            EntryScratch scratch = reusableSpanScratch(length);
            assert !scratch.inUse : "reusable EntrySpan is already live on this thread";
            scratch.inUse = true;
            copyEntries(first, length, scratch.endOffsets, scratch.payloadCrcs);
            int[] epochs = null;
            if (writeEpochs != null) {
                System.arraycopy(writeEpochs, first, scratch.writeEpochs, 0, length);
                epochs = scratch.writeEpochs;
            }
            return scratch.span.reset(firstStart, scratch.endOffsets, scratch.payloadCrcs, epochs,
                    uniformWriteEpoch, length, true, scratch);
        }
        long[] ends = Arrays.copyOfRange(endOffsets, first, end);
        int[] crcs = Arrays.copyOfRange(payloadCrcs, first, end);
        int[] epochs = copyEpochRange(first, end);
        return new EntrySpan().reset(firstStart, ends, crcs, epochs, uniformWriteEpoch, length, false, null);
    }

    private void copyEntries(int first, int length, long[] ends, int[] crcs) {
        System.arraycopy(endOffsets, first, ends, 0, length);
        System.arraycopy(payloadCrcs, first, crcs, 0, length);
    }

    private int[] copyEpochRange(int first, int end) {
        if (writeEpochs != null) {
            return Arrays.copyOfRange(writeEpochs, first, end);
        }
        int[] epochs = new int[end - first];
        Arrays.fill(epochs, uniformWriteEpoch);
        return epochs;
    }

    private static EntryScratch reusableSpanScratch(int length) {
        EntryScratch scratch = REUSABLE_ENTRY_SPAN.get();
        scratch.ensureCapacity(length);
        return scratch;
    }

    private int firstEntryEndingAfter(long offset) {
        int lo = 0;
        int hi = size;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (endOffsets[mid] > offset) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }

    public int size() {
        return size;
    }

    public long lastEndOffset() {
        return size == 0 ? 0 : endOffsets[size - 1];
    }

    /** Entries with endOffset > fromOffset, in order. */
    public List<ChunkFormats.LedgerEntry> entriesAfter(long fromOffset) {
        List<ChunkFormats.LedgerEntry> out = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (endOffsets[i] > fromOffset) out.add(entryAt(i));
        }
        return out;
    }

    int entriesThrough(long endOffset) {
        int count = 0;
        while (count < size && endOffsets[count] <= endOffset) {
            count++;
        }
        return count;
    }

    long endOffsetAt(int index) {
        return endOffsets[index];
    }

    /** Truncates the ledger so the last entry's endOffset is <= newEnd. */
    public void truncateTo(long newEnd) throws IOException {
        int keep = 0;
        for (int i = 0; i < size; i++) {
            if (endOffsets[i] <= newEnd) keep++;
            else break;
        }
        Arrays.fill(endOffsets, keep, size, 0);
        Arrays.fill(payloadCrcs, keep, size, 0);
        if (writeEpochs != null) {
            Arrays.fill(writeEpochs, keep, size, 0);
            if (keep == 0) {
                writeEpochs = null;
                uniformWriteEpoch = 0;
            }
        } else if (keep == 0) {
            uniformWriteEpoch = 0;
        }
        size = keep;
        if (channel != null) {
            channel.truncate((long) keep * LEDGER_ENTRY_SIZE);
        }
    }

    @Override
    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
    }

    private ChunkFormats.LedgerEntry entryAt(int index) {
        return new ChunkFormats.LedgerEntry(endOffsets[index], payloadCrcs[index], writeEpochAt(index));
    }

    private void ensureCapacity(int minCapacity) {
        if (endOffsets.length >= minCapacity) {
            return;
        }
        int capacity = endOffsets.length;
        while (capacity < minCapacity) {
            capacity <<= 1;
        }
        endOffsets = Arrays.copyOf(endOffsets, capacity);
        payloadCrcs = Arrays.copyOf(payloadCrcs, capacity);
        if (writeEpochs != null) {
            writeEpochs = Arrays.copyOf(writeEpochs, capacity);
        }
    }

    private void expandWriteEpochs() {
        writeEpochs = new int[endOffsets.length];
        Arrays.fill(writeEpochs, 0, size, uniformWriteEpoch);
    }

    private int writeEpochAt(int index) {
        return writeEpochs == null ? uniformWriteEpoch : writeEpochs[index];
    }

    private static int initialCapacity(int entries) {
        return Math.max(1, entries);
    }

    private static void encodeInto(ByteBuffer b, long endOffset, int payloadCrc, int writeEpoch) {
        int start = b.position();
        b.putLong(endOffset).putInt(payloadCrc).putInt(writeEpoch).putInt(0);
        b.putInt(Crc.of(b.array(), b.arrayOffset() + start, LEDGER_ENTRY_SIZE - 4));
    }

    private static final class EntryScratch {
        private long[] endOffsets;
        private int[] payloadCrcs;
        private int[] writeEpochs;
        private final EntrySpan span = new EntrySpan();
        private boolean inUse;

        private EntryScratch(int capacity) {
            this.endOffsets = new long[capacity];
            this.payloadCrcs = new int[capacity];
            this.writeEpochs = new int[capacity];
        }

        private void ensureCapacity(int minCapacity) {
            if (endOffsets.length >= minCapacity) {
                return;
            }
            int capacity = endOffsets.length;
            while (capacity < minCapacity) {
                capacity <<= 1;
            }
            endOffsets = new long[capacity];
            payloadCrcs = new int[capacity];
            writeEpochs = new int[capacity];
        }
    }
}
