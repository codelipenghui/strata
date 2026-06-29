package io.strata.meta;

import io.strata.common.FailureInjector;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Correctness anchor: an owner failover MUST NEVER reuse a file id (id reuse = silent metadata
 * corruption: two different files sharing an id). Tests three crash windows across owner-failover:
 *
 * <ul>
 *   <li><b>Window (a)</b> — {@code meta.log.afterAssignBeforeAppend}: the owner peeked the next
 *       file id but the {@code FileCreated} record is not yet durably appended. A crash here means
 *       the id was never published (the calling thread never received it). The successor must be
 *       free to assign that same id to a new file — no durable use of that id ever existed.
 *       <em>No collision in the set of durably-committed ids.</em>
 *   <li><b>Window (b)</b> — {@code meta.log.afterDurableAppend}: the {@code FileCreated} record is
 *       durable in the open log but not yet applied to in-memory state. The successor recovers the
 *       durable tail and must NOT re-issue that id.
 *   <li><b>Window (c)</b> — {@code meta.log.beforeManifestPublish}: the new snapshot is written
 *       but the manifest CAS has not fired. The successor recovers the previous manifest + open log
 *       and must not re-issue any id present in the log tail.
 * </ul>
 *
 * <p>Also asserts that opId-keyed idempotency survives a snapshot/restore cycle: a retried create
 * with the same opId after restore must return the same file id, not allocate a new one (guards
 * silent-duplicate-create-after-failover, design §5/§15).
 */
class NamespaceFileIdRecoveryInjectionTest {

    private static final StrataNamespace NS = StrataNamespace.of("tenant-a");

    @AfterEach
    void disarm() {
        FailureInjector.reset();
    }

    // -------------------------------------------------------------------------
    // Window (a): crash after peekNextFileId, BEFORE the FileCreated append
    // -------------------------------------------------------------------------

    /**
     * Window (a): the id is peeked (no durable side-effect) but the FileCreated record is never
     * appended. After recovery the same id is VALIDLY reused for the next file — no durable file
     * existed at that id. The test drives the REAL {@link NamespaceLogBackend#createFileOwnerAssigned}
     * path (not just the bare state methods) so the assertion would FAIL under the OLD buggy
     * {@code assignFileId()} code: that code pre-incremented {@code nextFileId} in memory before the
     * durable append, so a crash left the successor seeding from the incremented snapshot value and it
     * would SKIP the peeked id, assigning id+1 for both the crashed call and the next create — a
     * gap, or worse a double-assign if the snapshot captured the incremented value before the crash.
     *
     * <p>The no-reuse assertion checks that every durable FileCreated id is distinct across the
     * whole run (pre-crash durable ids ∩ post-recovery ids = ∅). Under the old buggy code, the
     * post-recovery successor would SKIP id 3 (because the snapshot would have nextFileId=4 from
     * the in-memory pre-increment) and assign ids {4,5,6} — no intersection — but then id 3 would
     * be lost forever (a gap, not a reuse). The strengthened test additionally asserts that the
     * successor DOES reuse id 3 (the peeked-but-not-appended id), confirming the counter was NOT
     * advanced in durable state. This second assertion would catch the old bug: under the old code,
     * the successor's first id would be 4 (not 3) and the equality would fail.
     */
    @Test
    void windowA_crashAfterPeekBeforeAppend_noDurableIdReuse() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            TestNamespaceMetadataFileStore fileStore = new TestNamespaceMetadataFileStore();

            // ---- Phase 1: leader backend creates 3 files via the REAL owner-assign path ----
            NamespaceLogBackend leaderBackend = new NamespaceLogBackend(root, fileStore, false);
            List<FileId> durableIds = new ArrayList<>();
            durableIds.add(leaderBackend.createFileOwnerAssigned(template("/a", 1)));   // id 0 — durable
            durableIds.add(leaderBackend.createFileOwnerAssigned(template("/b", 2)));   // id 1 — durable
            durableIds.add(leaderBackend.createFileOwnerAssigned(template("/c", 3)));   // id 2 — durable

            // The next id to be assigned is 3. Arm window (a): crash before the FileCreated append.
            FailureInjector.arm("meta.log.afterAssignBeforeAppend",
                    p -> { throw new RuntimeException("injected: crash after peek, before append"); });
            // Drive the REAL owner-assign path — it will peek id 3, hit the injection, and throw.
            // The FileCreated(3) record is NEVER appended: no durable side-effect.
            assertThrows(RuntimeException.class,
                    () -> leaderBackend.createFileOwnerAssigned(template("/d", 4)));
            FailureInjector.reset();

            // id 3 was peeked but NOT durably appended. Verify no durable record for id 3 exists
            // by checking the leader backend itself never applied the FileCreated.
            assertTrue(leaderBackend.getFile(NS, FileId.of(3)).isEmpty(),
                    "the crashed create must leave NO durable FileCreated(3) — it never reached the log");

            // ---- Phase 2: successor backend recovers from the same durable state ----
            NamespaceLogBackend successorBackend = new NamespaceLogBackend(root, fileStore, false);

            // All 3 pre-crash durable files must survive.
            for (FileId id : durableIds) {
                assertTrue(successorBackend.getFile(NS, id).isPresent(),
                        "durable file " + id + " must survive the crash");
            }

            // ---- Phase 3: successor assigns more ids — NO collision with durable ids ----
            List<FileId> successorIds = new ArrayList<>();
            // CRITICAL: the first id assigned by the successor MUST be 3 (the peeked-but-not-appended
            // id). This confirms the durable nextFileId was NOT advanced by the crashed call.
            // Under the OLD buggy assignFileId() code, nextFileId was pre-incremented in memory before
            // the crash, so if the snapshot captured that incremented value the successor would start
            // at 4 — this assertion would fail, exposing the bug.
            FileId firstSuccessorId = successorBackend.createFileOwnerAssigned(template("/d", 4));
            assertEquals(FileId.of(3), firstSuccessorId,
                    "successor must reuse peeked-but-not-appended id 3 — the counter must not have "
                    + "advanced durably before the crash");
            successorIds.add(firstSuccessorId);
            successorIds.add(successorBackend.createFileOwnerAssigned(template("/e", 5)));
            successorIds.add(successorBackend.createFileOwnerAssigned(template("/f", 6)));

            // Strict no-reuse: durable pre-crash ids ∩ post-recovery ids = ∅.
            assertNoReuse(durableIds, successorIds, "window (a)");
        }
    }

    // -------------------------------------------------------------------------
    // Window (b): crash AFTER the durable append, BEFORE apply (state invisible)
    // -------------------------------------------------------------------------

    /**
     * Window (b): the {@code FileCreated} record is durably on disk but not applied in memory. A
     * successor that recovers the open-log tail will apply it and must NOT re-issue that id.
     */
    @Test
    void windowB_crashAfterDurableAppendBeforeApply_noDurableIdReuse() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            TestNamespaceMetadataFileStore fileStore = new TestNamespaceMetadataFileStore();

            // ---- Phase 1: leader creates 3 files, then crashes mid-append at window (b) ----
            NamespaceMetadataLogRepository leader = NamespaceMetadataLogRepository.open(NS, fileStore, root, 1);
            List<FileId> durablePreCrash = new ArrayList<>();
            durablePreCrash.add(appendFileCreated(leader, "/a", 1));  // id 0
            durablePreCrash.add(appendFileCreated(leader, "/b", 2));  // id 1

            // Next append will be durable (disk write succeeds) but apply is interrupted.
            FailureInjector.arm("meta.log.afterDurableAppend",
                    p -> { throw new RuntimeException("injected: crash after durable append"); });
            // Build the FileCreated manually to know which id will be used.
            FileId inFlightId = leader.state().peekNextFileId(); // 2
            assertThrows(RuntimeException.class,
                    () -> leader.append(fileCreated(inFlightId, "/c", 3)));
            FailureInjector.reset();

            // The in-flight id was NOT applied to the leader's in-memory state.
            assertTrue(leader.state().file(inFlightId).isEmpty(),
                    "the interrupted append must not be visible to the crashing leader");

            // ---- Phase 2: successor recovers — MUST recover the durable record ----
            NamespaceMetadataLogRepository successor = NamespaceMetadataLogRepository.open(NS, fileStore, root, 2);

            // Both pre-crash durable files and the durable-but-unapplied file survive.
            for (FileId id : durablePreCrash) {
                assertTrue(successor.state().file(id).isPresent(), "durable file " + id + " must survive");
            }
            assertTrue(successor.state().file(inFlightId).isPresent(),
                    "the durably-appended record must be recovered by the successor");

            // The durable ids include the in-flight one now.
            List<FileId> allDurableIds = new ArrayList<>(durablePreCrash);
            allDurableIds.add(inFlightId);

            // ---- Phase 3: successor creates more files ----
            List<FileId> successorIds = new ArrayList<>();
            successorIds.add(appendFileCreated(successor, "/d", 4));
            successorIds.add(appendFileCreated(successor, "/e", 5));

            assertNoReuse(allDurableIds, successorIds, "window (b)");
        }
    }

    // -------------------------------------------------------------------------
    // Window (c): crash mid-compaction (snapshot written, manifest CAS not fired)
    // -------------------------------------------------------------------------

    /**
     * Window (c): the new snapshot is written to the file store but the manifest CAS has not
     * published it. The old manifest + open-log tail must remain fully recoverable, and no id from
     * the tail may be re-issued.
     */
    @Test
    void windowC_crashBeforeManifestPublish_noDurableIdReuse() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
            TestNamespaceMetadataFileStore fileStore = new TestNamespaceMetadataFileStore();

            // ---- Phase 1: leader creates 2 files, compacts (window-c crash), then creates 1 more ----
            NamespaceMetadataLogRepository leader = NamespaceMetadataLogRepository.open(NS, fileStore, root, 1);
            List<FileId> preCrashIds = new ArrayList<>();
            preCrashIds.add(appendFileCreated(leader, "/a", 1));  // id 0
            preCrashIds.add(appendFileCreated(leader, "/b", 2));  // id 1
            long generationBefore = leader.generation();

            // Crash during compaction, BEFORE the manifest CAS.
            FailureInjector.arm("meta.log.beforeManifestPublish",
                    p -> { throw new RuntimeException("injected: crash before manifest publish"); });
            assertThrows(RuntimeException.class, leader::compactAndPublish);
            FailureInjector.reset();

            // The manifest must NOT have advanced — the old generation is still published.
            assertEquals(generationBefore,
                    root.getNamespaceManifest(NS).orElseThrow().value().generation(),
                    "a crash before manifest CAS must not advance the published generation");

            // ---- Phase 2: successor recovers from the un-advanced manifest ----
            NamespaceMetadataLogRepository successor = NamespaceMetadataLogRepository.open(NS, fileStore, root, 2);
            for (FileId id : preCrashIds) {
                assertTrue(successor.state().file(id).isPresent(),
                        "durable file " + id + " must survive crash before manifest publish");
            }

            // ---- Phase 3: successor creates more files ----
            List<FileId> successorIds = new ArrayList<>();
            successorIds.add(appendFileCreated(successor, "/c", 3));  // id 2
            successorIds.add(appendFileCreated(successor, "/d", 4));  // id 3

            assertNoReuse(preCrashIds, successorIds, "window (c)");
        }
    }

    // -------------------------------------------------------------------------
    // opId-idempotency survives snapshot/restore
    // -------------------------------------------------------------------------

    /**
     * A retried create with the same opId after an exportSnapshot/restore cycle must return the
     * same file id — not a new one. Guards the silent-duplicate-create-after-failover risk: a
     * client retrying a create across a failover must not get a second distinct file with the same
     * opId.
     */
    @Test
    void opIdIdempotencySurvivesSnapshotRestore() throws Exception {
        NamespaceMetadataState state = new NamespaceMetadataState(NS);

        // Create a file with opId (7, 8) at path /logs/seg-0 → assigned id 0.
        long opMsb = 7L, opLsb = 8L;
        StrataPath path = StrataPath.of("/logs/seg-0");
        FileId originalId = state.peekNextFileId();
        state.apply(new MetadataLogRecord.FileCreated(originalId, NS, path, 3, 2, true, 1000, opMsb, opLsb));

        // Verify the opId index was built.
        assertEquals(Optional.of(originalId), state.fileIdForOpId(opMsb, opLsb),
                "opId must be indexed after apply(FileCreated)");

        // Snapshot at an arbitrary log cut.
        NamespaceMetadataState.Snapshot snap = state.exportSnapshot(42L);
        assertEquals(originalId.id() + 1, snap.nextFileId(),
                "snapshot must capture the advanced nextFileId");

        // Restore into a fresh state (simulates recovery from a snapshot).
        NamespaceMetadataState restored = new NamespaceMetadataState(NS);
        restored.restore(snap);

        // After restore: the opId index must be rebuilt.
        Optional<FileId> idFromRestore = restored.fileIdForOpId(opMsb, opLsb);
        assertTrue(idFromRestore.isPresent(),
                "opId must survive snapshot/restore — index must be rebuilt from file records");
        assertEquals(originalId, idFromRestore.get(),
                "retried create with same opId must return the SAME file id, not a new one");

        // Sanity: nextFileId did not regress — the counter is also preserved across restore.
        FileId nextAfterRestore = restored.peekNextFileId();
        assertTrue(nextAfterRestore.id() > originalId.id(),
                "nextFileId must not regress after restore; got " + nextAfterRestore);

        // A different opId must still get a new, higher id.
        FileId newId = restored.peekNextFileId();
        restored.apply(new MetadataLogRecord.FileCreated(newId, NS, StrataPath.of("/logs/seg-1"),
                3, 2, true, 2000, 99L, 100L));
        assertFalse(newId.equals(originalId),
                "a different opId must get a new id, not the already-used original id");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Append a FileCreated record using the repo's current peekNextFileId, return the assigned id. */
    private static FileId appendFileCreated(NamespaceMetadataLogRepository repo, String path, long op)
            throws Exception {
        FileId id = repo.state().peekNextFileId();
        repo.append(fileCreated(id, path, op));
        return id;
    }

    private static MetadataLogRecord fileCreated(FileId id, String path, long op) {
        return new MetadataLogRecord.FileCreated(id, NS, StrataPath.of(path), 3, 2, true, 100, op, op);
    }

    /**
     * Builds a {@link Records.FileRecord} template for use with
     * {@link NamespaceLogBackend#createFileOwnerAssigned}. The fileId field is a placeholder
     * (the backend assigns the real id); opId is unique per call so idempotency does not interfere.
     */
    private static Records.FileRecord template(String path, long opId) {
        return new Records.FileRecord(FileId.of(0), NS, StrataPath.of(path),
                3, 2, true, FileState.OPEN, 100L, List.of(), opId, opId);
    }

    /**
     * Asserts that no id appears in both the pre-crash set and the post-recovery set (strict no-reuse
     * of durable ids), and that each individual list contains no duplicates.
     */
    private static void assertNoReuse(List<FileId> preCrash, List<FileId> postRecovery, String window) {
        Set<FileId> preSet = new HashSet<>(preCrash);
        assertEquals(preCrash.size(), preSet.size(),
                window + ": pre-crash durable id list must have no duplicates");

        Set<FileId> postSet = new HashSet<>(postRecovery);
        assertEquals(postRecovery.size(), postSet.size(),
                window + ": post-recovery id list must have no duplicates");

        Set<FileId> intersection = new HashSet<>(preSet);
        intersection.retainAll(postSet);
        assertTrue(intersection.isEmpty(),
                window + ": id reuse detected — these ids were returned both before and after crash: "
                        + intersection);
    }
}
