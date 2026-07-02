package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The metadata-log repository: durable append, recovery through manifest + snapshot + open-log tail,
 * compaction, and manifest version-CAS fencing (design §8–§10, §13; tla/MetadataManifestCAS).
 */
class NamespaceMetadataLogRepositoryTest {

    private static final StrataNamespace NS = StrataNamespace.of("tenant-a");

    private static MetadataLogRecord fileCreated(FileId id, String path, long op) {
        return new MetadataLogRecord.FileCreated(id, NS, StrataPath.of(path), 3, 2, true, 100, op, op);
    }

    @Test
    void appendsRecoverThroughManifestAndOpenLogTail() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            TestNamespaceMetadataFileStore fs = new TestNamespaceMetadataFileStore();
            NamespaceMetadataLogRepository repo = NamespaceMetadataLogRepository.open(NS, fs, root, 1);

            FileId f = FileId.of(1);
            repo.append(fileCreated(f, "/a", 1));
            repo.append(new MetadataLogRecord.ChunkCreated(f, 0, 1, List.of(1, 2, 3), 1, 1));
            repo.append(new MetadataLogRecord.ChunkSealed(f, 0, 4096, 7, 1, List.of(1, 2, 3)));
            assertTrue(repo.state().file(f).isPresent());

            // A successor opens under a higher epoch and recovers the same state from the open-log tail
            // (these records were never compacted into a snapshot — they live only in the open log).
            NamespaceMetadataLogRepository successor = NamespaceMetadataLogRepository.open(NS, fs, root, 2);
            assertEquals(repo.state().file(f), successor.state().file(f));
            assertEquals(repo.state().chunksOn(1), successor.state().chunksOn(1));
            assertEquals(repo.appliedOffset(), successor.appliedOffset(),
                    "the successor recovered the same durable end offset");
        }
    }

    @Test
    void compactionPreservesStateAcrossSnapshotAndNewLog() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            TestNamespaceMetadataFileStore fs = new TestNamespaceMetadataFileStore();
            NamespaceMetadataLogRepository repo = NamespaceMetadataLogRepository.open(NS, fs, root, 1);

            FileId f = FileId.of(1);
            repo.append(fileCreated(f, "/a", 1));
            long genBefore = repo.generation();
            repo.compactAndPublish();                 // snapshot f, roll a new open log
            assertTrue(repo.generation() > genBefore, "compaction advances the snapshot generation");

            FileId g = FileId.of(2);
            repo.append(fileCreated(g, "/b", 2));      // appended only to the post-compaction log

            // A successor must see BOTH: f from the snapshot and g from the new open log.
            NamespaceMetadataLogRepository successor = NamespaceMetadataLogRepository.open(NS, fs, root, 2);
            assertTrue(successor.state().file(f).isPresent(), "snapshotted file survives compaction");
            assertTrue(successor.state().file(g).isPresent(), "post-compaction append survives");
        }
    }

    @Test
    void recoveryFallsBackToPreviousGenerationWhenCurrentSnapshotCrcFails() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            TestNamespaceMetadataFileStore fs = new TestNamespaceMetadataFileStore();
            NamespaceMetadataLogRepository repo = NamespaceMetadataLogRepository.open(NS, fs, root, 1);

            FileId a = FileId.of(1);
            repo.append(fileCreated(a, "/a", 1));
            repo.compactAndPublish(); // generation 2: snapshot has /a, log will carry /b below

            FileId b = FileId.of(2);
            repo.append(fileCreated(b, "/b", 2));
            repo.compactAndPublish(); // generation 3: current snapshot has /a + /b

            Records.NamespaceManifest current = root.getNamespaceManifest(NS).orElseThrow().value();
            fs.corruptSnapshot(current.snapshotFileId().orElseThrow());

            NamespaceMetadataLogRepository successor = NamespaceMetadataLogRepository.open(NS, fs, root, 2);

            assertTrue(successor.state().file(a).isPresent(), "fallback snapshot recovers pre-compaction file");
            assertTrue(successor.state().file(b).isPresent(), "fallback log recovers tail file");
            assertTrue(successor.generation() > current.generation(),
                    "successful fallback is republished as a fresh generation");
        }
    }

    @Test
    void compactionRetainsTheSupersededGenerationRatherThanDeletingItInline() throws Exception {
        // Issue #8: publishCompacted must NOT delete the just-superseded snapshot/log pair inline the instant
        // the new manifest is durable. The prior generation is retained as a rollback margin and reclaimed
        // later by the retention-gated sweep (NamespaceLogBackend.gcOrphanedSystemFiles), not here.
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            TestNamespaceMetadataFileStore fs = new TestNamespaceMetadataFileStore();
            NamespaceMetadataLogRepository repo = NamespaceMetadataLogRepository.open(NS, fs, root, 1);

            repo.append(fileCreated(FileId.of(1), "/a", 1));
            assertEquals(2, fs.liveFileCount(), "generation 1: one snapshot + one open log");

            repo.compactAndPublish(); // publishes generation 2; generation 1 is now superseded

            assertEquals(4, fs.liveFileCount(),
                    "both the superseded (gen 1) and the new (gen 2) snapshot/log pairs are retained");
        }
    }

    @Test
    void aFencedLeadersPublishLosesTheManifestCas() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            TestNamespaceMetadataFileStore fs = new TestNamespaceMetadataFileStore();

            NamespaceMetadataLogRepository leader = NamespaceMetadataLogRepository.open(NS, fs, root, 1);
            leader.append(fileCreated(FileId.of(1), "/a", 1));

            // A new leader recovers under a higher epoch — its open republishes the manifest, fencing
            // the old leader (whose cached manifest version is now stale).
            NamespaceMetadataLogRepository successor = NamespaceMetadataLogRepository.open(NS, fs, root, 2);
            successor.append(fileCreated(FileId.of(2), "/b", 2));

            assertThrows(IllegalStateException.class, leader::compactAndPublish,
                    "the fenced leader's manifest CAS must lose");
        }
    }
}
