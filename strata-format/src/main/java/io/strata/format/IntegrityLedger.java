package io.strata.format;

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
    private static final int INITIAL_REUSABLE_SPAN_ENTRIES = 64;
    private static final ThreadLocal<ChunkFormats.LedgerEntry[]> REUSABLE_ENTRY_SPAN =
            ThreadLocal.withInitial(() -> new ChunkFormats.LedgerEntry[INITIAL_REUSABLE_SPAN_ENTRIES]);

    private final FileChannel channel;
    private final List<ChunkFormats.LedgerEntry> entries; // in-memory mirror, ordered by endOffset
    // appends are single-threaded under the owning chunk's monitor, so one scratch buffer can be
    // reused for every entry encode instead of allocating a byte[] + two wrappers per record
    private final ByteBuffer scratch = ByteBuffer.allocate(LEDGER_ENTRY_SIZE);

    private IntegrityLedger(FileChannel channel, List<ChunkFormats.LedgerEntry> entries) {
        this.channel = channel;
        this.entries = entries;
    }

    public static IntegrityLedger create(Path path) throws IOException {
        FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE,
                StandardOpenOption.READ);
        return new IntegrityLedger(ch, new ArrayList<>());
    }

    /**
     * In-memory ledger with no on-disk backing: append/force/truncateTo/close are no-ops against disk,
     * so entries do NOT survive a restart. For tests and transient in-memory verification only — never
     * for a chunk that must recover its integrity ledger after a crash.
     */
    public static IntegrityLedger memory() {
        return new IntegrityLedger(null, new ArrayList<>());
    }

    /**
     * Opens an existing ledger, keeping only the prefix of CRC-valid entries with strictly
     * increasing endOffset; the file is truncated to that prefix (torn tail discarded).
     */
    public static IntegrityLedger open(Path path) throws IOException {
        FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.READ);
        byte[] all = Files.readAllBytes(path);
        List<ChunkFormats.LedgerEntry> valid = new ArrayList<>();
        int fullEntries = all.length / LEDGER_ENTRY_SIZE;
        long prevEnd = 0;
        for (int i = 0; i < fullEntries; i++) {
            ChunkFormats.LedgerEntry e = ChunkFormats.LedgerEntry.decodeOrNull(all, i * LEDGER_ENTRY_SIZE);
            if (e == null || e.endOffset() <= prevEnd) break;
            valid.add(e);
            prevEnd = e.endOffset();
        }
        ch.truncate((long) valid.size() * LEDGER_ENTRY_SIZE);
        ch.position(ch.size());
        return new IntegrityLedger(ch, valid);
    }

    public void append(ChunkFormats.LedgerEntry entry) throws IOException {
        if (channel != null) {
            scratch.clear();
            entry.encodeInto(scratch);
            scratch.flip();
            ChunkFormats.writeFully(channel, scratch, (long) entries.size() * LEDGER_ENTRY_SIZE);
        }
        entries.add(entry);
    }

    public void force() throws IOException {
        if (channel != null) {
            channel.force(false);
        }
    }

    public List<ChunkFormats.LedgerEntry> entries() {
        return List.copyOf(entries);
    }

    public static final class EntrySpan {
        private final long firstStart;
        private final ChunkFormats.LedgerEntry[] entries;
        private final int length;
        private final boolean reusable;

        private EntrySpan(long firstStart, ChunkFormats.LedgerEntry[] entries, int length, boolean reusable) {
            this.firstStart = firstStart;
            this.entries = entries;
            this.length = length;
            this.reusable = reusable;
        }

        public long firstStart() {
            return firstStart;
        }

        public ChunkFormats.LedgerEntry[] entries() {
            return entries.length == length ? entries : Arrays.copyOf(entries, length);
        }

        ChunkFormats.LedgerEntry[] rawEntries() {
            return entries;
        }

        int length() {
            return length;
        }

        void clear() {
            if (reusable) {
                Arrays.fill(entries, 0, length, null);
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

    EntrySpan reusableEntriesCovering(long offset, long readEnd) {
        return entriesCovering(offset, readEnd, true);
    }

    private EntrySpan entriesCovering(long offset, long readEnd, boolean reusable) {
        int first = firstEntryEndingAfter(offset);
        long firstStart = first == 0 ? 0 : entries.get(first - 1).endOffset();
        int end = first;
        for (; end < entries.size(); end++) {
            ChunkFormats.LedgerEntry e = entries.get(end);
            if (e.endOffset() >= readEnd) {
                end++;
                break;
            }
        }
        int length = end - first;
        ChunkFormats.LedgerEntry[] out = reusable ? reusableSpanArray(length) : new ChunkFormats.LedgerEntry[length];
        for (int i = first; i < end; i++) {
            ChunkFormats.LedgerEntry e = entries.get(i);
            out[i - first] = e;
        }
        return new EntrySpan(firstStart, out, length, reusable);
    }

    private static ChunkFormats.LedgerEntry[] reusableSpanArray(int length) {
        ChunkFormats.LedgerEntry[] out = REUSABLE_ENTRY_SPAN.get();
        if (out.length < length) {
            int capacity = out.length;
            while (capacity < length) {
                capacity <<= 1;
            }
            out = new ChunkFormats.LedgerEntry[capacity];
            REUSABLE_ENTRY_SPAN.set(out);
        }
        return out;
    }

    private int firstEntryEndingAfter(long offset) {
        int lo = 0;
        int hi = entries.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (entries.get(mid).endOffset() > offset) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }

    public int size() {
        return entries.size();
    }

    public long lastEndOffset() {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).endOffset();
    }

    /** Entries with endOffset > fromOffset, in order. */
    public List<ChunkFormats.LedgerEntry> entriesAfter(long fromOffset) {
        List<ChunkFormats.LedgerEntry> out = new ArrayList<>();
        for (ChunkFormats.LedgerEntry e : entries) {
            if (e.endOffset() > fromOffset) out.add(e);
        }
        return out;
    }

    /** Truncates the ledger so the last entry's endOffset is <= newEnd. */
    public void truncateTo(long newEnd) throws IOException {
        int keep = 0;
        for (ChunkFormats.LedgerEntry e : entries) {
            if (e.endOffset() <= newEnd) keep++;
            else break;
        }
        while (entries.size() > keep) entries.remove(entries.size() - 1);
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
}
