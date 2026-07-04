package io.strata.format;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IntegrityLedgerTest {
    @TempDir
    Path dir;

    @Test
    void reusableEntrySpanClearResetsLogicalLengthAndCanBeReused() throws Exception {
        try (IntegrityLedger ledger = IntegrityLedger.memory()) {
            ChunkFormats.LedgerEntry first = new ChunkFormats.LedgerEntry(1, 11, 1);
            ChunkFormats.LedgerEntry second = new ChunkFormats.LedgerEntry(2, 22, 1);
            ChunkFormats.LedgerEntry third = new ChunkFormats.LedgerEntry(3, 33, 1);
            ledger.append(first);
            ledger.append(second);
            ledger.append(third);

            IntegrityLedger.EntrySpan span = ledger.reusableEntriesCovering(0, 3);
            assertEquals(3, span.length());
            ChunkFormats.LedgerEntry[] entries = span.entries();
            assertEquals(3, entries.length);
            assertEquals(first, entries[0]);
            assertEquals(second, entries[1]);
            assertEquals(third, entries[2]);
            assertEquals(1, span.endOffset(0));
            assertEquals(22, span.payloadCrc(1));

            span.clear();
            assertEquals(0, span.length());
            assertEquals(0, span.entries().length);

            IntegrityLedger.EntrySpan reused = ledger.reusableEntriesCovering(1, 2);
            assertEquals(1, reused.length());
            assertEquals(second, reused.entries()[0]);
            assertEquals(2, reused.endOffset(0));
            assertEquals(22, reused.payloadCrc(0));
        }
    }

    @Test
    void primitiveAppendPreservesPublicEntries() throws Exception {
        try (IntegrityLedger ledger = IntegrityLedger.memory()) {
            ledger.append(10, 123, 2);

            assertEquals(1, ledger.size());
            assertEquals(10, ledger.lastEndOffset());
            assertEquals(List.of(new ChunkFormats.LedgerEntry(10, 123, 2)), ledger.entries());
        }
    }

    @Test
    void smallInitialCapacityStillExpands() throws Exception {
        try (IntegrityLedger ledger = IntegrityLedger.memory(1)) {
            ledger.append(10, 123, 2);
            ledger.append(20, 234, 2);
            ledger.append(30, 345, 3);

            assertEquals(3, ledger.size());
            assertEquals(30, ledger.lastEndOffset());
            assertEquals(List.of(
                    new ChunkFormats.LedgerEntry(10, 123, 2),
                    new ChunkFormats.LedgerEntry(20, 234, 2),
                    new ChunkFormats.LedgerEntry(30, 345, 3)), ledger.entries());
        }
    }

    @Test
    void reopenedLedgerPreservesUniformAndMixedEpochEntries() throws Exception {
        Path uniformPath = dir.resolve("uniform.j");
        try (IntegrityLedger ledger = IntegrityLedger.create(uniformPath, 1)) {
            ledger.append(10, 123, 7);
            ledger.append(20, 234, 7);
        }
        try (IntegrityLedger ledger = IntegrityLedger.open(uniformPath)) {
            assertEquals(List.of(
                    new ChunkFormats.LedgerEntry(10, 123, 7),
                    new ChunkFormats.LedgerEntry(20, 234, 7)), ledger.entries());
            assertEquals(List.of(new ChunkFormats.LedgerEntry(20, 234, 7)),
                    ledger.entriesAfter(10));
        }

        Path mixedPath = dir.resolve("mixed.j");
        try (IntegrityLedger ledger = IntegrityLedger.create(mixedPath, 1)) {
            ledger.append(10, 123, 7);
            ledger.append(20, 234, 8);
            ledger.append(30, 345, 8);
        }
        try (IntegrityLedger ledger = IntegrityLedger.open(mixedPath)) {
            assertEquals(List.of(
                    new ChunkFormats.LedgerEntry(10, 123, 7),
                    new ChunkFormats.LedgerEntry(20, 234, 8),
                    new ChunkFormats.LedgerEntry(30, 345, 8)), ledger.entries());
            assertEquals(List.of(
                    new ChunkFormats.LedgerEntry(20, 234, 8),
                    new ChunkFormats.LedgerEntry(30, 345, 8)), ledger.entriesAfter(10));
        }
    }
}
