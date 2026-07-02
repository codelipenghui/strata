package io.strata.node;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import io.strata.format.ChunkStore;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Node-local orphan GC (design §20.4/§20.5): confirm-before-delete and the fail-safe data-loss guard. */
class OrphanGcTest {
    private static final StrataNamespace NS = StrataNamespace.of("test");
    private static final int NODE_ID = 7;

    @TempDir
    Path dir;

    private void seal(ChunkStore store, ChunkId id) throws IOException {
        seal(store, NS, id);
    }

    private void seal(ChunkStore store, StrataNamespace ns, ChunkId id) throws IOException {
        byte[] bytes = "orphan-bytes".getBytes(StandardCharsets.UTF_8);
        store.open(ns, id, false, 1, 1_700_000_000_000L);
        store.append(ns, id, 1, 0, 0, ByteBuffer.wrap(bytes));
        store.seal(ns, id, 1, bytes.length, null);
    }

    @Test
    void deletesConfirmedOrphanButKeepsAChunkTheOwnerStillLists() throws Exception {
        ChunkId orphan = new ChunkId(FileId.of(1), 0); // owner answers FILE_NOT_FOUND
        ChunkId listed = new ChunkId(FileId.of(2), 0); // owner still lists this node for the chunk
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = new ScpServer(0, 0, 0, 0, req -> {
                 if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                     throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
                 }
                 Messages.LookupFile m = Messages.LookupFile.decode(req.headerSlice());
                 if (m.fileId().id() == 1) {
                     throw new ScpException(ErrorCode.FILE_NOT_FOUND, "no such file");
                 }
                 Messages.ChunkInfo ci = new Messages.ChunkInfo(listed, ChunkState.SEALED, 12, 0, 1,
                         List.of(new Messages.Replica(NODE_ID, "127.0.0.1:1")));
                 return ScpServer.ok(req, new Messages.LookupFileResp(NS, StrataPath.of("/f2"),
                         Messages.WritePolicy.DEFAULT, (byte) 0, List.of(ci)).encode(), null);
             })) {
            seal(store, orphan);
            seal(store, listed);
            String endpoint = "127.0.0.1:" + owner.port();
            // grace 0 + startup 0 → every sealed chunk is an immediate suspect; gcOnce confirms each.
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000);
            gc.gcOnce();

            assertFalse(store.contains(NS, orphan), "an unreferenced chunk (FILE_NOT_FOUND) must be GC'd");
            assertTrue(store.contains(NS, listed), "a chunk the owner still lists must be kept");
        }
    }

    @Test
    void keepsSuspectWhenOwnerUnreachableFailSafe() throws Exception {
        ChunkId chunk = new ChunkId(FileId.of(1), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"))) {
            seal(store, chunk);
            // a controller endpoint with nothing listening — the confirm cannot complete.
            OrphanGc gc = orphanGc(store, List.of("127.0.0.1:1"), 0, 60_000, 0, 5_000);
            gc.gcOnce();
            assertTrue(store.contains(NS, chunk),
                    "an unreachable owner must never trigger a delete (fail-safe data-loss guard, §20.5)");
        }
    }

    @Test
    void confirmSkipsNotLeaderControllersAndTrustsTheOwningController() throws Exception {
        ChunkId orphan = new ChunkId(FileId.of(1), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer notOwner = notLeaderServer();
             ScpServer owner = fileNotFoundServer()) {
            seal(store, orphan);
            // the non-owner is listed FIRST: confirm must redirect past its NOT_LEADER to the real owner.
            OrphanGc gc = orphanGc(store,
                    List.of("127.0.0.1:" + notOwner.port(), "127.0.0.1:" + owner.port()), 0, 60_000, 0, 5_000);
            gc.gcOnce();

            assertFalse(store.contains(NS, orphan),
                    "confirm must skip the NOT_LEADER controller and act on the owner's authoritative FILE_NOT_FOUND");
        }
    }

    @Test
    void keepsSuspectWhenEveryControllerRedirectsNotLeaderFailSafe() throws Exception {
        ChunkId chunk = new ChunkId(FileId.of(1), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer a = notLeaderServer();
             ScpServer b = notLeaderServer()) {
            seal(store, chunk);
            OrphanGc gc = orphanGc(store,
                    List.of("127.0.0.1:" + a.port(), "127.0.0.1:" + b.port()), 0, 60_000, 0, 5_000);
            gc.gcOnce();

            assertTrue(store.contains(NS, chunk),
                    "all controllers redirect (NOT_LEADER) and none owns the namespace → no definitive answer → keep");
        }
    }

    @Test
    void startupGraceSuppressesGcUntilWarmupElapses() throws Exception {
        ChunkId orphan = new ChunkId(FileId.of(1), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = fileNotFoundServer()) {
            seal(store, orphan);
            String endpoint = "127.0.0.1:" + owner.port();
            // grace 0 makes the chunk an immediate suspect and the owner would confirm it an orphan, but a
            // 600ms node-startup grace must suppress the GC loop (scanning every 30ms) until warm-up elapses.
            try (OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 30, 600, 5_000)) {
                gc.start();
                Thread.sleep(200); // several scan intervals in, still inside the 600ms startup grace
                assertTrue(store.contains(NS, orphan),
                        "the node-startup grace must defer GC until the owner-pull verify has had a cycle to attest");

                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                while (store.contains(NS, orphan) && System.nanoTime() < deadline) {
                    Thread.sleep(20);
                }
                assertFalse(store.contains(NS, orphan),
                        "once the startup grace elapses the confirmed orphan is GC'd by the loop");
            }
        }
    }

    @Test
    void massConfirmedOrphansDrainUpToNamespaceLimitPerPass() throws Exception {
        ChunkId first = new ChunkId(FileId.of(1), 0);
        ChunkId second = new ChunkId(FileId.of(2), 0);
        ChunkId third = new ChunkId(FileId.of(3), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = fileNotFoundServer()) {
            seal(store, first);
            seal(store, second);
            seal(store, third);
            String endpoint = "127.0.0.1:" + owner.port();
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000, 2, 0, 0);

            gc.gcOnce();

            assertEquals(1, present(store, NS, first, second, third),
                    "a large orphan wave should drain up to the namespace budget instead of wedging");
            assertEquals(1, gc.budgetLimitedNamespaces());
            assertEquals(1, gc.budgetLimitedChunks());

            gc.gcOnce();

            assertEquals(0, present(store, NS, first, second, third),
                    "deferred confirmed orphans should be reclaimed on later passes");
            assertEquals(0, gc.budgetLimitedNamespaces());
            assertEquals(0, gc.budgetLimitedChunks());
        }
    }

    @Test
    void smallNamespaceUsesPercentBudgetEvenWhenAbsoluteLimitIsHigh() throws Exception {
        ChunkId first = new ChunkId(FileId.of(1), 0);
        ChunkId second = new ChunkId(FileId.of(2), 0);
        ChunkId third = new ChunkId(FileId.of(3), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = fileNotFoundServer()) {
            seal(store, first);
            seal(store, second);
            seal(store, third);
            String endpoint = "127.0.0.1:" + owner.port();
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000, 64, 34, 0);

            gc.gcOnce();

            assertEquals(2, present(store, NS, first, second, third),
                    "the percent budget must prevent a small namespace from being fully deleted in one pass");
            assertEquals(1, gc.budgetLimitedNamespaces());
            assertEquals(2, gc.budgetLimitedChunks());
        }
    }

    @Test
    void nodeWideBudgetCapsDeletesAcrossNamespaces() throws Exception {
        StrataNamespace a = StrataNamespace.of("a");
        StrataNamespace b = StrataNamespace.of("b");
        ChunkId a1 = new ChunkId(FileId.of(1), 0);
        ChunkId a2 = new ChunkId(FileId.of(2), 0);
        ChunkId b1 = new ChunkId(FileId.of(3), 0);
        ChunkId b2 = new ChunkId(FileId.of(4), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = fileNotFoundServer()) {
            seal(store, a, a1);
            seal(store, a, a2);
            seal(store, b, b1);
            seal(store, b, b2);
            String endpoint = "127.0.0.1:" + owner.port();
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000, 64, 0, 2);

            gc.gcOnce();

            int remaining = present(store, a, a1, a2) + present(store, b, b1, b2);
            assertEquals(2, remaining, "the node-wide budget must cap total deletes across namespaces");
            assertEquals(2, gc.budgetLimitedChunks());
        }
    }

    @Test
    void reconfirmsImmediatelyBeforeDelete() throws Exception {
        ChunkId chunk = new ChunkId(FileId.of(1), 0);
        AtomicInteger confirms = new AtomicInteger();
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = new ScpServer(0, 0, 0, 0, req -> {
                 if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                     throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
                 }
                 if (confirms.incrementAndGet() == 1) {
                     throw new ScpException(ErrorCode.FILE_NOT_FOUND, "transiently absent");
                 }
                 Messages.ChunkInfo ci = new Messages.ChunkInfo(chunk, ChunkState.SEALED, 12, 0, 1,
                         List.of(new Messages.Replica(NODE_ID, "127.0.0.1:1")));
                 return ScpServer.ok(req, new Messages.LookupFileResp(NS, StrataPath.of("/f1"),
                         Messages.WritePolicy.DEFAULT, (byte) 0, List.of(ci)).encode(), null);
             })) {
            seal(store, chunk);
            String endpoint = "127.0.0.1:" + owner.port();
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000, 64, 0, 0);

            gc.gcOnce();

            assertTrue(store.contains(NS, chunk),
                    "a chunk re-listed by its owner immediately before delete must be kept");
            assertEquals(2, confirms.get(), "confirmed orphans must be checked again at delete time");
        }
    }

    @Test
    void zeroDeleteBudgetsDisableCaps() throws Exception {
        ChunkId first = new ChunkId(FileId.of(1), 0);
        ChunkId second = new ChunkId(FileId.of(2), 0);
        ChunkId third = new ChunkId(FileId.of(3), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = fileNotFoundServer()) {
            seal(store, first);
            seal(store, second);
            seal(store, third);
            String endpoint = "127.0.0.1:" + owner.port();
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000, 0, 0, 0);

            gc.gcOnce();

            assertFalse(store.contains(NS, first), "zero budgets disable delete caps");
            assertFalse(store.contains(NS, second), "zero budgets disable delete caps");
            assertFalse(store.contains(NS, third), "zero budgets disable delete caps");
        }
    }

    private static int present(ChunkStore store, StrataNamespace ns, ChunkId... ids) {
        int count = 0;
        for (ChunkId id : ids) {
            if (store.contains(ns, id)) {
                count++;
            }
        }
        return count;
    }

    private static ScpServer notLeaderServer() throws Exception {
        return new ScpServer(0, 0, 0, 0, req -> {
            if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
            }
            throw new ScpException(ErrorCode.NOT_LEADER, "not the owner of this namespace");
        });
    }

    private static ScpServer fileNotFoundServer() throws Exception {
        return new ScpServer(0, 0, 0, 0, req -> {
            if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
            }
            throw new ScpException(ErrorCode.FILE_NOT_FOUND, "no such file");
        });
    }

    private static OrphanGc orphanGc(ChunkStore store, List<String> controllerEndpoints,
                                     long graceMs, long scanIntervalMs,
                                     long startupGraceMs, int confirmTimeoutMs) {
        return orphanGc(store, controllerEndpoints, graceMs, scanIntervalMs, startupGraceMs,
                confirmTimeoutMs, OrphanGc.DEFAULT_MAX_CONFIRMED_DELETES_PER_NAMESPACE_PER_PASS);
    }

    private static OrphanGc orphanGc(ChunkStore store, List<String> controllerEndpoints,
                                     long graceMs, long scanIntervalMs,
                                     long startupGraceMs, int confirmTimeoutMs,
                                     int maxConfirmedDeletesPerNamespacePerPass) {
        return orphanGc(store, controllerEndpoints, graceMs, scanIntervalMs, startupGraceMs,
                confirmTimeoutMs, maxConfirmedDeletesPerNamespacePerPass,
                OrphanGc.DEFAULT_MAX_CONFIRMED_DELETE_PERCENT_PER_NAMESPACE_PER_PASS,
                OrphanGc.DEFAULT_MAX_CONFIRMED_DELETES_PER_NODE_PASS);
    }

    private static OrphanGc orphanGc(ChunkStore store, List<String> controllerEndpoints,
                                     long graceMs, long scanIntervalMs,
                                     long startupGraceMs, int confirmTimeoutMs,
                                     int maxConfirmedDeletesPerNamespacePerPass,
                                     int maxConfirmedDeletePercentPerNamespacePerPass,
                                     int maxConfirmedDeletesPerNodePass) {
        ChunkDeleteService deletes = new ChunkDeleteService(store, 1, 0);
        return new OrphanGc(store, deletes, NODE_ID, controllerEndpoints,
                graceMs, scanIntervalMs, startupGraceMs, confirmTimeoutMs,
                maxConfirmedDeletesPerNamespacePerPass, maxConfirmedDeletePercentPerNamespacePerPass,
                maxConfirmedDeletesPerNodePass);
    }
}
