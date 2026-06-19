package io.strata.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static io.strata.format.ChunkFormats.LEDGER_ENTRY_SIZE;

/**
 * Per-chunk integrity ledger `<chunk>.j` (tech design §11.3): one 24-byte entry per append.
 * Crash recovery reads the valid entry prefix and verifies tail data CRCs — the storage node
 * never parses payload bytes, even to recover. Deleted at seal.
 */
public final class IntegrityLedger implements AutoCloseable {
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
