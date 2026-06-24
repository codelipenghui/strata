package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NamespaceMetadataStateTest {

    private static final StrataNamespace NS = StrataNamespace.of("tenant-a");
    private static final StrataPath P = StrataPath.of("/logs/seg-0");
    private static final FileId F = FileId.of(1);

    private NamespaceMetadataState state;

    @BeforeEach
    void setUp() {
        state = new NamespaceMetadataState(NS);
    }

    private static MetadataLogRecord fileCreated() {
        return new MetadataLogRecord.FileCreated(F, NS, P, 3, 2, true, 1000, 7, 8);
    }

    @Test
    void fileCreateBindsPathAndLists() {
        state.apply(fileCreated());
        assertTrue(state.file(F).isPresent());
        assertEquals(F, state.resolvePath(P).orElseThrow());
        assertEquals(List.of(F), state.liveFiles());
        assertEquals(FileState.OPEN, state.file(F).orElseThrow().state());
    }

    @Test
    void chunkLifecycleBuildsTheDerivedNodeIndex() {
        state.apply(fileCreated());
        state.apply(new MetadataLogRecord.WriterEpochAllocated(F, 1));
        state.apply(new MetadataLogRecord.ChunkCreated(F, 0, 1, List.of(1, 2, 3), 7, 8));
        assertEquals(Set.of(new ChunkId(F, 0)), state.chunksOn(1), "open chunk replicas are indexed");

        state.apply(new MetadataLogRecord.ChunkSealed(F, 0, 4096, 0x1234, 1, List.of(1, 2)));
        assertEquals(Set.of(new ChunkId(F, 0)), state.chunksOn(1));
        assertEquals(Set.of(new ChunkId(F, 0)), state.chunksOn(2));
        assertTrue(state.chunksOn(3).isEmpty(), "a replica dropped at seal leaves the node index");

        Records.FileRecord f = state.file(F).orElseThrow();
        assertEquals(1, f.chunks().size());
        assertEquals(ChunkState.SEALED, f.chunks().get(0).state());
        assertEquals(4096, f.chunks().get(0).length());
        assertEquals(List.of(1, 2), f.chunks().get(0).replicas());
    }

    @Test
    void replicaSwapDropAddUpdateTheIndexAndDescriptor() {
        state.apply(fileCreated());
        state.apply(new MetadataLogRecord.ChunkCreated(F, 0, 1, List.of(1, 2, 3), 7, 8));
        state.apply(new MetadataLogRecord.ChunkSealed(F, 0, 10, 0, 1, List.of(1, 2, 3)));

        state.apply(new MetadataLogRecord.ReplicaSwapped(F, 0, 3, 4));
        assertTrue(state.chunksOn(3).isEmpty());
        assertEquals(Set.of(new ChunkId(F, 0)), state.chunksOn(4));

        state.apply(new MetadataLogRecord.ReplicaDropped(F, 0, 4));
        assertTrue(state.chunksOn(4).isEmpty());

        state.apply(new MetadataLogRecord.ReplicaAdded(F, 0, 5));
        assertEquals(Set.of(new ChunkId(F, 0)), state.chunksOn(5));
        assertEquals(List.of(1, 2, 5), state.file(F).orElseThrow().chunks().get(0).replicas());
    }

    @Test
    void deleteLifecycleTombstonesThenSweeps() {
        state.apply(fileCreated());
        state.apply(new MetadataLogRecord.ChunkCreated(F, 0, 1, List.of(1, 2, 3), 7, 8));
        state.apply(new MetadataLogRecord.ChunkSealed(F, 0, 10, 0, 1, List.of(1, 2, 3)));

        state.apply(new MetadataLogRecord.FileDeleting(F));
        state.apply(new MetadataLogRecord.PathUnbound(NS, P, F));
        assertTrue(state.resolvePath(P).isEmpty());
        assertEquals(FileState.DELETING, state.file(F).orElseThrow().state());
        assertTrue(state.chunksOn(1).isEmpty(), "DELETING chunks are not repair candidates");

        state.apply(new MetadataLogRecord.FileDeleted(F, 5000));
        assertTrue(state.file(F).isEmpty());
        assertTrue(state.hasTombstone(F), "the DELETED tombstone fences a recreate of the id");
        assertEquals(List.of(), state.liveFiles());
        assertEquals(5000, state.tombstoneDeletedAt(F));
        assertEquals(List.of(F), state.tombstonesDeletedAtOrBefore(5000));
        assertEquals(List.of(), state.tombstonesDeletedAtOrBefore(4999));

        state.apply(new MetadataLogRecord.TombstoneSwept(F));
        assertFalse(state.hasTombstone(F));
        assertEquals(-1, state.tombstoneDeletedAt(F));
        assertEquals(0, state.fileCount());
    }

    @Test
    void chunkAbortRemovesTheTailChunk() {
        state.apply(fileCreated());
        state.apply(new MetadataLogRecord.ChunkCreated(F, 0, 1, List.of(1, 2, 3), 7, 8));
        state.apply(new MetadataLogRecord.ChunkAborted(F, 0));
        assertEquals(0, state.file(F).orElseThrow().chunks().size());
        assertTrue(state.chunksOn(1).isEmpty());
    }

    @Test
    void replayIsDeterministicAcrossTwoStates() {
        List<MetadataLogRecord> log = List.of(
                fileCreated(),
                new MetadataLogRecord.ChunkCreated(F, 0, 1, List.of(1, 2, 3), 7, 8),
                new MetadataLogRecord.ChunkSealed(F, 0, 4096, 123, 1, List.of(1, 2, 3)),
                new MetadataLogRecord.ReplicaSwapped(F, 0, 2, 9));
        NamespaceMetadataState a = new NamespaceMetadataState(NS);
        NamespaceMetadataState b = new NamespaceMetadataState(NS);
        for (MetadataLogRecord r : log) {
            a.apply(r);
            b.apply(r);
        }
        assertEquals(a.file(F), b.file(F));
        assertEquals(a.chunksOn(9), b.chunksOn(9));
        assertEquals(a.liveFiles(), b.liveFiles());
    }
}
