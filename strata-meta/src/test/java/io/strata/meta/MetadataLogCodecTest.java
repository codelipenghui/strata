package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MetadataLogCodecTest {

    private static final FileId F = FileId.of(0x1122334455667788L);

    @Test
    void roundTripsEveryRecordType() {
        List<MetadataLogRecord> records = List.of(
                new MetadataLogRecord.FileCreated(F, StrataNamespace.of("tenant-a"),
                        StrataPath.of("/logs/topic-0/seg-0"), 3, 2, true, 1234567890L, 11, 22),
                new MetadataLogRecord.WriterEpochAllocated(F, 7),
                new MetadataLogRecord.ChunkCreated(F, 0, 1, List.of(4, 5, 6), 11, 22),
                new MetadataLogRecord.ChunkSealed(F, 0, 4096, 0xCAFEBABE, 1, List.of(4, 5)),
                new MetadataLogRecord.ChunkAborted(F, 2),
                new MetadataLogRecord.ChunkDeleted(F, 3),
                new MetadataLogRecord.FileSealed(F),
                new MetadataLogRecord.FileDeleting(F),
                new MetadataLogRecord.FileDeleted(F, 987654321L),
                new MetadataLogRecord.PathUnbound(StrataNamespace.of("tenant-a"),
                        StrataPath.of("/logs/topic-0/seg-0"), F),
                new MetadataLogRecord.ReplicaSwapped(F, 0, 4, 9),
                new MetadataLogRecord.ReplicaDropped(F, 0, 5),
                new MetadataLogRecord.ReplicaAdded(F, 0, 8),
                new MetadataLogRecord.TombstoneSwept(F));

        // guard: every permitted record family is exercised — adding a new one without a test fails here
        assertEquals(MetadataLogRecord.class.getPermittedSubclasses().length, records.size(),
                "every MetadataLogRecord family must be covered by this round-trip");

        for (MetadataLogRecord r : records) {
            byte[] encoded = MetadataLogCodec.encode(r);
            assertEquals(r, MetadataLogCodec.decode(encoded), r.getClass().getSimpleName());
        }
    }

    @Test
    void emptyReplicaSetsRoundTrip() {
        MetadataLogRecord.ChunkCreated created =
                new MetadataLogRecord.ChunkCreated(F, 0, 1, List.of(), 0, 0);
        assertEquals(created, MetadataLogCodec.decode(MetadataLogCodec.encode(created)));
    }

    @Test
    void rejectsUnknownRecordType() {
        assertThrows(IllegalArgumentException.class, () -> MetadataLogCodec.decode(new byte[]{(byte) 99}));
    }
}
