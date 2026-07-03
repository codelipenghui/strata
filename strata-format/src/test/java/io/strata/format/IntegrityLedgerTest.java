package io.strata.format;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IntegrityLedgerTest {
    @Test
    void reusableEntrySpanCanBeClearedWithoutChangingPublicEntryShape() throws Exception {
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
            assertEquals(0, span.endOffset(0));
            assertEquals(0, span.payloadCrc(1));
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
}
