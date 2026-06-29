package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A namespace owner must be able to reap its own namespaces' DELETED tombstones without holding the
 * global leader latch — the per-namespace metadata-log tombstones live in the owner's loaded repo,
 * which no other controller holds. {@code sweepOwnedNamespaceTombstones} is that owner-scoped reaper
 * (the leader's {@code sweepDeletedFiles} additionally reaps the shared system-root tombstones).
 */
class NamespaceLogOwnedTombstoneSweepTest {

    private static final StrataNamespace NS = StrataNamespace.of("tenant-a");

    @Test
    void ownerReapsItsOwnNamespaceTombstoneWithoutTheLeaderSweep() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            NamespaceLogBackend backend =
                    new NamespaceLogBackend(root, new TestNamespaceMetadataFileStore(), false);
            NamespaceLogMetadataStore store = new NamespaceLogMetadataStore(backend);

            FileId fileId = FileId.of(7);
            StrataPath path = StrataPath.of("/logs/topic-0/segment-0");
            store.createFile(new Records.FileRecord(fileId, NS, path, 3, 2, true,
                    FileState.OPEN, 1_000, List.of(), 1, 1));
            int version = store.getFile(NS, fileId).orElseThrow().version();
            assertTrue(store.deleteFile(NS, fileId, version));

            // Unswept, the tombstone still fences a recreate of the same id.
            assertThrows(Exception.class,
                    () -> store.createFile(new Records.FileRecord(fileId, NS, StrataPath.of("/logs/topic-0/segment-1"),
                            3, 2, true, FileState.OPEN, 1_000, List.of(), 1, 1)),
                    "an unswept tombstone must still fence a recreate of the same id");

            // The owner-scoped sweep alone (no leader, no system-root sweep) reaps the owned-namespace
            // tombstone; the id is then reusable.
            assertEquals(1, store.sweepOwnedNamespaceTombstones(0));
            assertEquals(0, store.sweepOwnedNamespaceTombstones(0), "nothing left to reap on a second pass");

            Records.FileRecord reborn = new Records.FileRecord(fileId, NS,
                    StrataPath.of("/logs/topic-0/segment-1"), 3, 2, true, FileState.OPEN, 1_000, List.of(), 1, 1);
            store.createFile(reborn);
            assertEquals(reborn, store.getFile(NS, fileId).orElseThrow().value());
            backend.close();
        }
    }
}
