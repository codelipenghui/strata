package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SYSTEM-namespace orphan GC (design §8/§10): reaps snapshot/log system files left behind by a crash between
 * writing a new generation's files and the manifest CAS, WITHOUT ever deleting a file that is, or may yet
 * become, referenced. Safety is BY CONSTRUCTION on generation: an in-flight publish's files are always at a
 * generation strictly greater than the namespace's published generation, so reaping only
 * {@code unreferenced && gen <= published} can never touch a live or in-flight file — no clock involved.
 */
class NamespaceLogSystemFileGcTest {

    private static final StrataNamespace SYS = NamespaceLogBackend.SYSTEM_NAMESPACE;

    /** Records deleteFile() calls; every other NamespaceMetadataFileStore method is unused by the GC. */
    private static final class RecordingFileStore implements NamespaceMetadataFileStore {
        final List<FileId> deleted = new CopyOnWriteArrayList<>();

        @Override public void deleteFile(FileId fileId) {
            deleted.add(fileId);
        }
        @Override public FileId createLogFile(StrataNamespace ns, long generation) {
            throw new UnsupportedOperationException();
        }
        @Override public void appendLog(FileId logFileId, byte[] frameBytes) {
            throw new UnsupportedOperationException();
        }
        @Override public byte[] readLog(FileId logFileId) {
            throw new UnsupportedOperationException();
        }
        @Override public FileId writeSnapshot(StrataNamespace ns, long generation, byte[] snapshotBytes) {
            throw new UnsupportedOperationException();
        }
        @Override public byte[] readSnapshot(FileId snapshotFileId) {
            throw new UnsupportedOperationException();
        }
    }

    /** Creates a SYSTEM-namespace file whose path encodes (ns, generation) the way the production store does. */
    private static void putSystemFile(ZkMetadataStore root, FileId id, String ns, long generation, String kind,
                                      FileState state) throws Exception {
        StrataPath path = StrataPath.of("/metadata-log/" + ns + "/gen-" + generation + "/" + kind + "-" + id.id());
        root.createFile(new Records.FileRecord(id, SYS, path, 3, 2, false, state, 1_000L, List.of(), 0, 0));
    }

    @Test
    void reapsSupersededOrphansButNeverReferencedOrHigherGenerationFilesEvenWithNoLiveUserFiles()
            throws Exception {
        // The owning namespace has a PUBLISHED MANIFEST at generation 7 but NO live user files. listNamespaces()
        // would omit it (it only reports namespaces with live files), so a GC keyed off listNamespaces() would
        // wrongly reap its live snapshot/log — the GC must enumerate by metadata namespaces.
        String owner = "tenant-no-user-files";
        FileId refSnapshot = FileId.of(0xA1); // gen 7, referenced by the manifest -> live
        FileId refLog = FileId.of(0xA2);       // gen 7, referenced by the manifest -> live
        FileId orphanSuperseded = FileId.of(0xB1); // gen 6, unreferenced, gen <= published -> reap
        FileId orphanSameGen = FileId.of(0xB2);    // gen 7, unreferenced (lost the CAS), gen <= published -> reap
        FileId inflightHigher = FileId.of(0xC1);   // gen 8, unreferenced -> in-flight/future, MUST be kept

        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            putSystemFile(root, refSnapshot, owner, 7, "snapshot", FileState.SEALED);
            putSystemFile(root, refLog, owner, 7, "log", FileState.OPEN);
            putSystemFile(root, orphanSuperseded, owner, 6, "snapshot", FileState.SEALED);
            putSystemFile(root, orphanSameGen, owner, 7, "snapshot", FileState.SEALED);
            putSystemFile(root, inflightHigher, owner, 8, "snapshot", FileState.SEALED);
            // Publish a manifest at generation 7 referencing ONLY refSnapshot/refLog (no user files created).
            root.putNamespaceManifest(new Records.NamespaceManifest(StrataNamespace.of(owner), 1L, 7L, 0L, 0L,
                    Optional.of(refSnapshot), Optional.of(refLog)), -1);

            RecordingFileStore fs = new RecordingFileStore();
            NamespaceLogBackend backend = new NamespaceLogBackend(root, fs, false);

            int reaped = backend.gcOrphanedSystemFiles();

            assertEquals(2, reaped, "both unreferenced files at gen <= published are reaped");
            assertTrue(fs.deleted.contains(orphanSuperseded), "an older-generation orphan is reaped");
            assertTrue(fs.deleted.contains(orphanSameGen), "a same-generation CAS-loser orphan is reaped");
            assertFalse(fs.deleted.contains(refSnapshot), "a manifest-referenced snapshot is NEVER reaped");
            assertFalse(fs.deleted.contains(refLog), "a manifest-referenced log is NEVER reaped");
            assertFalse(fs.deleted.contains(inflightHigher),
                    "a higher-generation (in-flight/not-yet-published) file is NEVER reaped — no clock involved");
            backend.close();
        }
    }

    @Test
    void retainsTheJustSupersededGenerationUntilTheSafetyDelayElapsesThenReclaimsIt() throws Exception {
        // Issue #8: a superseded snapshot/log generation must survive a configurable safety delay (a rollback
        // margin), not be deleted the instant the new manifest is durable. The window is timed off the
        // SUCCESSOR generation's createdAtMs — the durable instant the prior generation was superseded — so it
        // is honored across failover with no in-memory queue. Here gen 7 (live) was created at t=1000, which is
        // when gen 6 was superseded; with a 500ms window gen 6 survives at t=1400 and is reclaimed at t=1600.
        String owner = "tenant-retention";
        FileId liveSnapshot = FileId.of(0x71); // gen 7, referenced -> live, createdAtMs=1000 (supersedes gen 6)
        FileId liveLog = FileId.of(0x72);      // gen 7, referenced -> live
        FileId supersededGen6 = FileId.of(0x60); // gen 6, unreferenced, just superseded -> retained then reaped

        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            putSystemFile(root, liveSnapshot, owner, 7, "snapshot", FileState.SEALED);
            putSystemFile(root, liveLog, owner, 7, "log", FileState.OPEN);
            putSystemFile(root, supersededGen6, owner, 6, "snapshot", FileState.SEALED);
            root.putNamespaceManifest(new Records.NamespaceManifest(StrataNamespace.of(owner), 1L, 7L, 0L, 0L,
                    Optional.of(liveSnapshot), Optional.of(liveLog)), -1);

            RecordingFileStore fs = new RecordingFileStore();
            NamespaceLogBackend backend = new NamespaceLogBackend(root, fs, false);

            // Within the 500ms window (age = 1400 - 1000 = 400): the superseded generation is RETAINED.
            assertEquals(0, backend.gcOrphanedSystemFiles(1400L, 500L),
                    "a just-superseded generation is retained while inside the safety delay");
            assertFalse(fs.deleted.contains(supersededGen6), "retained generation is not reclaimed yet");

            // Past the window (age = 1600 - 1000 = 600 >= 500): it is reclaimed.
            assertEquals(1, backend.gcOrphanedSystemFiles(1600L, 500L),
                    "a superseded generation is reclaimed once the safety delay has elapsed");
            assertTrue(fs.deleted.contains(supersededGen6), "the superseded generation is reaped past the window");
            assertFalse(fs.deleted.contains(liveSnapshot), "the live generation is never reaped");
            assertFalse(fs.deleted.contains(liveLog), "the live generation is never reaped");
            backend.close();
        }
    }

    @Test
    void retentionWindowIsHonoredAcrossFailoverWithNoInMemoryQueue() throws Exception {
        // The retention window must be durable: a new owner (a fresh backend with NO shared in-memory state)
        // over the same consensus root must still honor it — proving the window is timed off the successor
        // generation's durable FileRecord.createdAtMs, not an in-memory queue a leadership change would lose
        // (issue #8 failover correction). gen 5 (live) was created at t=1000, superseding gen 4.
        String owner = "tenant-failover";
        FileId liveSnapshot = FileId.of(0x51);   // gen 5, referenced -> live, createdAtMs=1000
        FileId supersededGen4 = FileId.of(0x40); // gen 4, unreferenced, just superseded

        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            putSystemFile(root, liveSnapshot, owner, 5, "snapshot", FileState.SEALED);
            putSystemFile(root, supersededGen4, owner, 4, "snapshot", FileState.SEALED);
            root.putNamespaceManifest(new Records.NamespaceManifest(StrataNamespace.of(owner), 1L, 5L, 0L, 0L,
                    Optional.of(liveSnapshot), Optional.empty()), -1);

            RecordingFileStore fs1 = new RecordingFileStore();
            NamespaceLogBackend owner1 = new NamespaceLogBackend(root, fs1, false);
            assertEquals(0, owner1.gcOrphanedSystemFiles(1400L, 500L),
                    "the original owner retains the superseded generation inside the window");
            owner1.close();

            // Failover: a brand-new backend, still inside the window, must ALSO retain — no in-memory queue.
            RecordingFileStore fs2 = new RecordingFileStore();
            NamespaceLogBackend owner2 = new NamespaceLogBackend(root, fs2, false);
            assertEquals(0, owner2.gcOrphanedSystemFiles(1499L, 500L),
                    "a new owner after failover still honors the window (recomputed from durable state)");
            assertTrue(fs2.deleted.isEmpty(), "nothing reclaimed by the new owner while inside the window");

            assertEquals(1, owner2.gcOrphanedSystemFiles(1600L, 500L),
                    "past the window the new owner reclaims the superseded generation");
            assertTrue(fs2.deleted.contains(supersededGen4));
            owner2.close();
        }
    }

    @Test
    void keepsEverythingForANamespaceWithNoPublishedManifestYet() throws Exception {
        // A namespace mid its FIRST publish: gen-1 files exist but no manifest is published. The GC cannot
        // prove these are dead, so it must keep them (a crashed first publish leaks until the namespace
        // republishes — never data loss).
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            putSystemFile(root, FileId.of(0xD1), "tenant-first-publish", 1, "snapshot", FileState.SEALED);
            putSystemFile(root, FileId.of(0xD2), "tenant-first-publish", 1, "log", FileState.OPEN);

            RecordingFileStore fs = new RecordingFileStore();
            NamespaceLogBackend backend = new NamespaceLogBackend(root, fs, false);
            assertEquals(0, backend.gcOrphanedSystemFiles(), "no manifest yet -> nothing is provably dead");
            assertTrue(fs.deleted.isEmpty());
            backend.close();
        }
    }

    @Test
    void doesNotMisparseAndReapAnInFlightFileForANamespaceNamedLikeThePathLiterals() throws Exception {
        // Adversarial: a tenant namespace literally named "metadata-log" with an in-flight (unpublished)
        // gen-2 file, while a SIBLING namespace literally named "gen-2" is published at a high generation.
        // A scanning parser would misattribute the file to namespace "gen-2" (published high) and reap the
        // live in-flight file. Positional parsing must place it in "metadata-log" (published gen 1) and keep
        // it because gen 2 > published 1.
        String tricky = "metadata-log";
        FileId inflight = FileId.of(0xF1);   // ns "metadata-log", gen 2, unreferenced -> in-flight, MUST keep
        FileId metaLogLive = FileId.of(0xF2); // ns "metadata-log", gen 1, referenced -> live
        FileId siblingLive = FileId.of(0xF3); // ns "gen-2", gen 9, referenced -> live
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            putSystemFile(root, metaLogLive, tricky, 1, "snapshot", FileState.SEALED);
            putSystemFile(root, inflight, tricky, 2, "snapshot", FileState.SEALED);
            putSystemFile(root, siblingLive, "gen-2", 9, "snapshot", FileState.SEALED);
            root.putNamespaceManifest(new Records.NamespaceManifest(StrataNamespace.of(tricky), 1L, 1L, 0L, 0L,
                    Optional.of(metaLogLive), Optional.empty()), -1);
            root.putNamespaceManifest(new Records.NamespaceManifest(StrataNamespace.of("gen-2"), 1L, 9L, 0L, 0L,
                    Optional.of(siblingLive), Optional.empty()), -1);

            RecordingFileStore fs = new RecordingFileStore();
            NamespaceLogBackend backend = new NamespaceLogBackend(root, fs, false);
            int reaped = backend.gcOrphanedSystemFiles();

            assertEquals(0, reaped, "nothing is reapable: live files are referenced, the in-flight file is gen>published");
            assertFalse(fs.deleted.contains(inflight),
                    "the in-flight file for namespace 'metadata-log' must NOT be misparsed into sibling 'gen-2' and reaped");
            backend.close();
        }
    }

    @Test
    void backgroundSweepReapsCrashOrphanWithoutAnyFailover() throws Exception {
        FileId orphan = FileId.of(0xE1); // gen 1, namespace published at gen 2 -> orphan is dead
        FileId live = FileId.of(0xE2);   // gen 2, referenced -> live
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            putSystemFile(root, orphan, "tenant-x", 1, "snapshot", FileState.SEALED);
            putSystemFile(root, live, "tenant-x", 2, "snapshot", FileState.SEALED);
            root.putNamespaceManifest(new Records.NamespaceManifest(StrataNamespace.of("tenant-x"), 1L, 2L, 0L, 0L,
                    Optional.of(live), Optional.empty()), -1);

            RecordingFileStore fs = new RecordingFileStore();
            NamespaceLogBackend backend = new NamespaceLogBackend(root, fs, false);
            backend.startBackgroundCompaction(1, 20, true); // tiny interval; orphan GC enabled

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!fs.deleted.contains(orphan) && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertTrue(fs.deleted.contains(orphan),
                    "the background sweep must reap a crash-orphaned system file with no failover");
            assertFalse(fs.deleted.contains(live), "the live (referenced) file is never reaped");
            backend.close();
        }
    }
}
