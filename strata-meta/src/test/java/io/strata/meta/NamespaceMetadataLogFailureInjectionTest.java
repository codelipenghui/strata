package io.strata.meta;

import io.strata.common.FailureInjector;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fault injection on the namespace metadata log's durability seams: a crash after a durable append (but
 * before it is applied/acked) must not lose the metadata, and a crash mid-compaction (before the
 * manifest CAS) must leave the previous manifest fully recoverable. These exercise the byte-durability
 * and compaction-atomicity invariants (tla/MetadataByteDurability, design §10) under real faults.
 */
class NamespaceMetadataLogFailureInjectionTest {

    private static final StrataNamespace NS = StrataNamespace.of("tenant-a");

    @AfterEach
    void disarm() {
        FailureInjector.reset();
    }

    private static MetadataLogRecord fileCreated(FileId id, String path, long op) {
        return new MetadataLogRecord.FileCreated(id, NS, StrataPath.of(path), 3, 2, true, 100, op, op);
    }

    @Test
    void aCrashAfterDurableAppendStillRecoversTheRecord() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            TestNamespaceMetadataFileStore fileStore = new TestNamespaceMetadataFileStore();
            NamespaceMetadataLogRepository repo = NamespaceMetadataLogRepository.open(NS, fileStore, root, 1);

            FileId f = new FileId(1, 1);
            repo.append(fileCreated(f, "/a", 1)); // applied normally

            // Crash AFTER the durable append, BEFORE the in-memory apply, for the next record.
            FileId g = new FileId(2, 2);
            FailureInjector.arm("meta.log.afterDurableAppend",
                    p -> { throw new RuntimeException("injected crash after durable append"); });
            assertThrows(RuntimeException.class, () -> repo.append(fileCreated(g, "/b", 2)));
            FailureInjector.reset();

            // The crashing leader never made the record visible (apply was skipped)...
            assertTrue(repo.state().file(g).isEmpty(),
                    "the interrupted append is not visible to the crashing leader");

            // ...but a successor recovers BOTH records from the durable log — the metadata is not lost.
            NamespaceMetadataLogRepository successor =
                    NamespaceMetadataLogRepository.open(NS, fileStore, root, 2);
            assertTrue(successor.state().file(f).isPresent());
            assertTrue(successor.state().file(g).isPresent(),
                    "a durably-appended record survives a crash before it is applied/acked");
        }
    }

    @Test
    void aCrashBeforeTheManifestCasLeavesThePreviousManifestRecoverable() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            TestNamespaceMetadataFileStore fileStore = new TestNamespaceMetadataFileStore();
            NamespaceMetadataLogRepository repo = NamespaceMetadataLogRepository.open(NS, fileStore, root, 1);

            FileId f = new FileId(1, 1);
            repo.append(fileCreated(f, "/a", 1));
            long generationBefore = repo.generation();

            // Crash during compaction, BEFORE the manifest version-CAS.
            FailureInjector.arm("meta.log.beforeManifestPublish",
                    p -> { throw new RuntimeException("injected crash before manifest publish"); });
            assertThrows(RuntimeException.class, repo::compactAndPublish);
            FailureInjector.reset();

            // The published manifest is unchanged — compaction did not advance the generation.
            assertEquals(generationBefore,
                    root.getNamespaceManifest(NS).orElseThrow().value().generation(),
                    "a crash before the manifest CAS must not advance the published generation");

            // A successor recovers the committed state from the still-current manifest (atomic compaction).
            NamespaceMetadataLogRepository successor =
                    NamespaceMetadataLogRepository.open(NS, fileStore, root, 2);
            assertTrue(successor.state().file(f).isPresent(),
                    "compaction is atomic at the manifest CAS — the committed record survives");
        }
    }
}
