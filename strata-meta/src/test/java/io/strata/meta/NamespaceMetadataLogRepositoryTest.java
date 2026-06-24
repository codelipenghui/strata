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
