package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NamespaceMetadataSnapshotCodecTest {

    private static final StrataNamespace NS = StrataNamespace.of("tenant-a");

    @Test
    void roundTripsAndRestoresObservableState() {
        NamespaceMetadataState state = new NamespaceMetadataState(NS);
        FileId live = FileId.of(1);
        FileId dead = FileId.of(2);
        state.apply(new MetadataLogRecord.FileCreated(live, NS, StrataPath.of("/a"), 3, 2, true, 100, 1, 1));
        state.apply(new MetadataLogRecord.ChunkCreated(live, 0, 1, List.of(1, 2, 3), 1, 1));
        state.apply(new MetadataLogRecord.ChunkSealed(live, 0, 4096, 7, 1, List.of(1, 2, 3)));
        state.apply(new MetadataLogRecord.FileCreated(dead, NS, StrataPath.of("/b"), 3, 2, true, 100, 2, 2));
        state.apply(new MetadataLogRecord.FileDeleting(dead));
        state.apply(new MetadataLogRecord.PathUnbound(NS, StrataPath.of("/b"), dead));
        state.apply(new MetadataLogRecord.FileDeleted(dead, 999));

        NamespaceMetadataState.Snapshot snapshot = state.exportSnapshot(12_345);
        byte[] bytes = NamespaceMetadataSnapshotCodec.encode(snapshot);
        NamespaceMetadataState.Snapshot decoded = NamespaceMetadataSnapshotCodec.decode(bytes);
        assertEquals(12_345, decoded.nextLogStartOffset());
        assertEquals(snapshot.nextFileId(), decoded.nextFileId(), "nextFileId survives the codec roundtrip");

        NamespaceMetadataState restored = new NamespaceMetadataState(NS);
        restored.restore(decoded);
        assertEquals(state.file(live), restored.file(live));
        assertEquals(state.resolvePath(StrataPath.of("/a")), restored.resolvePath(StrataPath.of("/a")));
        assertEquals(state.chunksOn(1), restored.chunksOn(1), "node index is re-derived on restore");
        assertEquals(state.liveFiles(), restored.liveFiles());
        assertTrue(restored.hasTombstone(dead), "tombstone survives the snapshot to keep fencing the id");
        assertEquals(999, restored.tombstoneDeletedAt(dead));
        assertTrue(restored.file(dead).isEmpty());
    }

    @Test
    void emptySnapshotRoundTrips() {
        NamespaceMetadataState.Snapshot snapshot = new NamespaceMetadataState(NS).exportSnapshot(0);
        NamespaceMetadataState.Snapshot decoded =
                NamespaceMetadataSnapshotCodec.decode(NamespaceMetadataSnapshotCodec.encode(snapshot));
        assertEquals(0, decoded.nextFileId());
        assertEquals(0, decoded.nextLogStartOffset());
        assertTrue(decoded.files().isEmpty());
        assertTrue(decoded.tombstones().isEmpty());
    }

    @Test
    void detectsCrcCorruption() {
        NamespaceMetadataState state = new NamespaceMetadataState(NS);
        state.apply(new MetadataLogRecord.FileCreated(FileId.of(1), NS, StrataPath.of("/a"),
                3, 2, true, 1, 1, 1));
        byte[] bytes = NamespaceMetadataSnapshotCodec.encode(state.exportSnapshot(0));
        bytes[5] ^= 0xFF;
        assertThrows(IllegalArgumentException.class, () -> NamespaceMetadataSnapshotCodec.decode(bytes));
    }
}
