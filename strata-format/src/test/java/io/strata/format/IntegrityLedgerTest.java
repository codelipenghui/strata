package io.strata.format;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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
            assertEquals(3, span.entries().length);
            assertTrue(span.rawEntries().length >= 3);
            assertSame(first, span.rawEntries()[0]);
            assertSame(second, span.rawEntries()[1]);
            assertSame(third, span.rawEntries()[2]);

            span.clear();
            assertNull(span.rawEntries()[0]);
            assertNull(span.rawEntries()[1]);
            assertNull(span.rawEntries()[2]);
        }
    }
}
