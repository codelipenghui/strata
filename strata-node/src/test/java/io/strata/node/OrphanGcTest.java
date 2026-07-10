package io.strata.node;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.NsChunkId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import io.strata.format.ChunkStore;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.RequestContext;
import io.strata.proto.ScpClient;
import io.strata.proto.ScpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Node-local orphan GC (design §9.2): confirm-before-delete and the fail-safe data-loss guard. */
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
    void systemNamespaceConfirmUsesMetadataClientKind() throws Exception {
        StrataNamespace system = StrataNamespace.of("strata-meta");
        ChunkId chunk = new ChunkId(FileId.of(1), 0);
        AtomicInteger clientKind = new AtomicInteger();
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = new ScpServer(0, 0, 0, 0, req -> {
                 if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                     throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
                 }
                 clientKind.set(RequestContext.clientKind());
                 Messages.ChunkInfo listed = new Messages.ChunkInfo(chunk, ChunkState.SEALED, 12, 0, 1,
                         List.of(new Messages.Replica(NODE_ID, "127.0.0.1:1")));
                 return ScpServer.ok(req, new Messages.LookupFileResp(system, StrataPath.of("/metadata-log/test"),
                         Messages.WritePolicy.DEFAULT, (byte) 0, List.of(listed), 1).encode(), null);
             })) {
            seal(store, system, chunk);
            OrphanGc gc = orphanGc(store, List.of("127.0.0.1:" + owner.port()), 0, 60_000, 0, 5_000);

            gc.gcOnce();

            assertEquals(ScpClient.KIND_METADATA, clientKind.get());
            assertTrue(store.contains(system, chunk), "the controller still lists this system chunk");
        }
    }

    @Test
    void ordinaryNamespaceConfirmUsesToolClientKind() throws Exception {
        ChunkId chunk = new ChunkId(FileId.of(1), 0);
        AtomicInteger clientKind = new AtomicInteger();
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = new ScpServer(0, 0, 0, 0, req -> {
                 if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                     throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
                 }
                 clientKind.set(RequestContext.clientKind());
                 Messages.ChunkInfo listed = new Messages.ChunkInfo(chunk, ChunkState.SEALED, 12, 0, 1,
                         List.of(new Messages.Replica(NODE_ID, "127.0.0.1:1")));
                 return ScpServer.ok(req, new Messages.LookupFileResp(NS, StrataPath.of("/ordinary/test"),
                         Messages.WritePolicy.DEFAULT, (byte) 0, List.of(listed), 1).encode(), null);
             })) {
            seal(store, chunk);
            OrphanGc gc = orphanGc(store, List.of("127.0.0.1:" + owner.port()), 0, 60_000, 0, 5_000);

            gc.gcOnce();

            assertEquals(ScpClient.KIND_TOOL, clientKind.get());
            assertTrue(store.contains(NS, chunk), "the controller still lists this ordinary chunk");
        }
    }

    @Test
    void keepsSuspectAndWarnsWhenOwnerRejectsConfirm() throws Exception {
        ChunkId chunk = new ChunkId(FileId.of(1), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = new ScpServer(0, 0, 0, 0, req -> {
                 if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                     throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
                 }
                 throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                         "namespace is reserved for internal metadata");
             })) {
            seal(store, chunk);
            OrphanGc gc = orphanGc(store, List.of("127.0.0.1:" + owner.port()), 0, 60_000, 0, 5_000);

            gc.gcOnce();

            assertTrue(store.contains(NS, chunk),
                    "a rejected owner confirm must never authorize physical deletion");
            assertEquals(1, gc.unreachableConfirmWarns(),
                    "a rejected confirm must surface through unreachable-confirm observability");
        }
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
            // grace 0 + startup 0 -> every sealed chunk is an immediate suspect. FILE_NOT_FOUND
            // needs a later pass before delete; descriptor-present orphan answers can delete immediately.
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000);
            gc.gcOnce();
            assertTrue(store.contains(NS, orphan),
                    "FILE_NOT_FOUND must be corroborated by a later pass before deleting");

            gc.gcOnce();

            assertFalse(store.contains(NS, orphan), "an unreferenced chunk (FILE_NOT_FOUND) must be GC'd");
            assertTrue(store.contains(NS, listed), "a chunk the owner still lists must be kept");
        }
    }

    @Test
    void deletesChunkWhoseFileIdExistsOnlyInAnotherNamespace() throws Exception {
        StrataNamespace ownerNamespace = StrataNamespace.of("owner-namespace");
        StrataNamespace onDiskNamespace = StrataNamespace.of("wrong-on-disk-namespace");
        ChunkId chunk = new ChunkId(FileId.of(1), 0);
        AtomicInteger onDiskNamespaceLookups = new AtomicInteger();
        AtomicInteger ownerNamespaceLookups = new AtomicInteger();
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = new ScpServer(0, 0, 0, 0, req -> {
                 if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                     throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
                 }
                 Messages.LookupFile lookup = Messages.LookupFile.decode(req.headerSlice());
                 if (lookup.namespace().equals(onDiskNamespace)) {
                     onDiskNamespaceLookups.incrementAndGet();
                     throw new ScpException(ErrorCode.FILE_NOT_FOUND,
                             "the file id is absent from the chunk's on-disk namespace");
                 }
                 if (lookup.namespace().equals(ownerNamespace)) {
                     ownerNamespaceLookups.incrementAndGet();
                     Messages.ChunkInfo ci = new Messages.ChunkInfo(chunk, ChunkState.SEALED, 12, 0, 1,
                             List.of(new Messages.Replica(NODE_ID, "127.0.0.1:1")));
                     return ScpServer.ok(req, new Messages.LookupFileResp(ownerNamespace,
                             StrataPath.of("/same-id-in-owner-namespace"), Messages.WritePolicy.DEFAULT,
                             (byte) 0, List.of(ci)).encode(), null);
                 }
                 throw new ScpException(ErrorCode.FILE_NOT_FOUND, "unexpected namespace");
             })) {
            seal(store, onDiskNamespace, chunk);
            OrphanGc gc = orphanGc(store, List.of("127.0.0.1:" + owner.port()),
                    0, 60_000, 0, 5_000);

            gc.gcOnce();
            assertTrue(store.contains(onDiskNamespace, chunk),
                    "namespace-scoped FILE_NOT_FOUND still requires a corroborating pass");

            gc.gcOnce();
            assertFalse(store.contains(onDiskNamespace, chunk),
                    "a descriptor in another namespace must not keep this on-disk logical chunk alive");
            assertEquals(3, onDiskNamespaceLookups.get(),
                    "confirm and delete checks must stay bound to the chunk's on-disk namespace");
            assertEquals(0, ownerNamespaceLookups.get(),
                    "orphan confirmation must never fall back to a same-id file in another namespace");
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
                    "an unreachable owner must never trigger a delete (fail-safe data-loss guard, §9.2)");
        }
    }

    @Test
    void unreachableConfirmSweepWarnsOncePerIntervalNotSilently() throws Exception {
        ChunkId first = new ChunkId(FileId.of(1), 0);
        ChunkId second = new ChunkId(FileId.of(2), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"))) {
            seal(store, first);
            seal(store, second);
            // an unparsable endpoint plus one with nothing listening: every confirm sweep exhausts
            // all endpoints as UNREACHABLE, so the node can never reclaim and must say why.
            OrphanGc gc = orphanGc(store, List.of("not-an-endpoint", "127.0.0.1:1"), 0, 60_000, 0, 5_000);

            gc.gcOnce();

            assertEquals(2, present(store, NS, first, second),
                    "unreachable suspects are kept (fail-safe), but the sweep must leave log evidence");
            assertEquals(1, gc.unreachableConfirmWarns(),
                    "one rate-limited warn per interval, not one per suspect and not zero");

            gc.gcOnce();
            assertEquals(1, gc.unreachableConfirmWarns(),
                    "repeat sweeps inside the rate-limit interval must not warn again");
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
            assertTrue(store.contains(NS, orphan),
                    "FILE_NOT_FOUND from the owning controller must be corroborated by a later pass");

            gc.gcOnce();

            assertFalse(store.contains(NS, orphan),
                    "confirm must skip the NOT_LEADER controller and act on the owner's authoritative FILE_NOT_FOUND");
        }
    }

    @Test
    void keepsSuspectWhenFileNotFoundComesFromStaleOwnerEpoch() throws Exception {
        ChunkId chunk = new ChunkId(FileId.of(1), 0);
        AtomicInteger calls = new AtomicInteger();
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer staleOwner = new ScpServer(0, 0, 0, 0, req -> {
                 if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                     throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
                 }
                 calls.incrementAndGet();
                 throw new ScpException(ErrorCode.FILE_NOT_FOUND, "stale owner missing file", 7);
             })) {
            seal(store, chunk);
            String endpoint = "127.0.0.1:" + staleOwner.port();
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000,
                    (namespace, ownerEpoch) -> {
                        if (ownerEpoch < 8) {
                            throw new ScpException(ErrorCode.FENCED_EPOCH,
                                    "stale owner epoch " + ownerEpoch, 8);
                        }
                    });

            gc.gcOnce();

            assertTrue(store.contains(NS, chunk),
                    "a stale owner's FILE_NOT_FOUND must not authorize physical deletion");
            assertEquals(1, calls.get(), "a fenced confirm must not be retried as a delete candidate");
        }
    }

    @Test
    void keepsSuspectWhenLookupSuccessComesFromStaleOwnerEpoch() throws Exception {
        ChunkId chunk = new ChunkId(FileId.of(1), 0);
        AtomicInteger calls = new AtomicInteger();
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer staleOwner = new ScpServer(0, 0, 0, 0, req -> {
                 if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                     throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
                 }
                 calls.incrementAndGet();
                 return ScpServer.ok(req, new Messages.LookupFileResp(NS, StrataPath.of("/f1"),
                         Messages.WritePolicy.DEFAULT, (byte) 0, List.of(), 7).encode(), null);
             })) {
            seal(store, chunk);
            String endpoint = "127.0.0.1:" + staleOwner.port();
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000,
                    (namespace, ownerEpoch) -> {
                        if (ownerEpoch < 8) {
                            throw new ScpException(ErrorCode.FENCED_EPOCH,
                                    "stale owner epoch " + ownerEpoch, 8);
                        }
                    });

            gc.gcOnce();

            assertTrue(store.contains(NS, chunk),
                    "a stale owner's descriptor must not authorize physical deletion");
            assertEquals(1, calls.get(), "a fenced confirm must not be retried as a delete candidate");
        }
    }

    @Test
    void skipsStaleOwnerEpochAndTrustsLaterFreshOwnerConfirm() throws Exception {
        ChunkId orphan = new ChunkId(FileId.of(1), 0);
        AtomicInteger staleCalls = new AtomicInteger();
        AtomicInteger freshCalls = new AtomicInteger();
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer staleOwner = new ScpServer(0, 0, 0, 0, req -> {
                 if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                     throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
                 }
                 staleCalls.incrementAndGet();
                 throw new ScpException(ErrorCode.FILE_NOT_FOUND, "stale owner missing file", 7);
             });
             ScpServer freshOwner = new ScpServer(0, 0, 0, 0, req -> {
                 if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                     throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
                 }
                 freshCalls.incrementAndGet();
                 throw new ScpException(ErrorCode.FILE_NOT_FOUND, "fresh owner missing file", 8);
             })) {
            seal(store, orphan);
            OrphanGc gc = orphanGc(store, List.of(
                            "127.0.0.1:" + staleOwner.port(),
                            "127.0.0.1:" + freshOwner.port()),
                    0, 60_000, 0, 5_000,
                    (namespace, ownerEpoch) -> {
                        if (ownerEpoch < 8) {
                            throw new ScpException(ErrorCode.FENCED_EPOCH,
                                    "stale owner epoch " + ownerEpoch, 8);
                        }
                    });

            gc.gcOnce();

            assertTrue(store.contains(NS, orphan),
                    "fresh FILE_NOT_FOUND must still wait for a later corroborating pass");
            assertEquals(1, staleCalls.get(), "stale answer is skipped on the first confirm pass");
            assertEquals(1, freshCalls.get(), "fresh owner is consulted on the first confirm pass");

            gc.gcOnce();

            assertFalse(store.contains(NS, orphan),
                    "a fresh owner's FILE_NOT_FOUND should still authorize ordinary orphan cleanup");
            assertEquals(3, staleCalls.get(), "stale answers are skipped during confirm and delete checks");
            assertEquals(3, freshCalls.get(), "fresh owner is consulted during confirm and delete checks");
        }
    }

    @Test
    void skipsMalformedOwnerEpochAndTrustsLaterFreshOwnerConfirm() throws Exception {
        ChunkId orphan = new ChunkId(FileId.of(1), 0);
        AtomicInteger malformedCalls = new AtomicInteger();
        AtomicInteger freshCalls = new AtomicInteger();
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer malformedOwner = new ScpServer(0, 0, 0, 0, req -> {
                 if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                     throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
                 }
                 malformedCalls.incrementAndGet();
                 throw new ScpException(ErrorCode.FILE_NOT_FOUND, "malformed owner epoch", -1);
             });
             ScpServer freshOwner = new ScpServer(0, 0, 0, 0, req -> {
                 if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                     throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
                 }
                 freshCalls.incrementAndGet();
                 throw new ScpException(ErrorCode.FILE_NOT_FOUND, "fresh owner missing file", 8);
             })) {
            seal(store, orphan);
            OrphanGc gc = orphanGc(store, List.of(
                            "127.0.0.1:" + malformedOwner.port(),
                            "127.0.0.1:" + freshOwner.port()),
                    0, 60_000, 0, 5_000,
                    (namespace, ownerEpoch) -> {
                        if (ownerEpoch < 0) {
                            throw new IllegalArgumentException("ownerEpoch must be non-negative: " + ownerEpoch);
                        }
                    });

            gc.gcOnce();

            assertTrue(store.contains(NS, orphan),
                    "fresh FILE_NOT_FOUND must still wait for a later corroborating pass");
            assertEquals(1, malformedCalls.get(), "malformed answer is skipped on the first confirm pass");
            assertEquals(1, freshCalls.get(), "fresh owner is consulted on the first confirm pass");

            gc.gcOnce();

            assertFalse(store.contains(NS, orphan),
                    "a malformed owner epoch must be skipped without aborting later fresh owner cleanup");
            assertEquals(3, malformedCalls.get(), "malformed answers are skipped during confirm and delete checks");
            assertEquals(3, freshCalls.get(), "fresh owner is still consulted during confirm and delete checks");
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
    void massConfirmedOrphansOpenNamespaceBreakerInsteadOfDraining() throws Exception {
        ChunkId first = new ChunkId(FileId.of(1), 0);
        ChunkId second = new ChunkId(FileId.of(2), 0);
        ChunkId third = new ChunkId(FileId.of(3), 0);
        AtomicInteger confirms = new AtomicInteger();
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = fileNotFoundServer(confirms)) {
            seal(store, first);
            seal(store, second);
            seal(store, third);
            String endpoint = "127.0.0.1:" + owner.port();
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000, 2, 0, 0);

            gc.gcOnce();
            assertEquals(3, present(store, NS, first, second, third),
                    "FILE_NOT_FOUND suspects must wait for a later corroborating pass");

            gc.gcOnce();

            assertEquals(3, present(store, NS, first, second, third),
                    "a large orphan wave should open the namespace breaker before deleting any chunk");
            assertTrue(gc.namespaceBreakerOpen(NS));
            assertEquals(1, gc.breakerOpenNamespaces());
            assertEquals(1, gc.breakerTrips());
            assertEquals(3, gc.breakerSkippedChunkTotal());
            assertEquals(3, gc.breakerHaltedChunks());
            assertEquals(6, confirms.get());

            gc.gcOnce();

            assertEquals(3, present(store, NS, first, second, third),
                    "an open namespace breaker must halt later passes until restart");
            assertTrue(gc.namespaceBreakerOpen(NS));
            assertEquals(1, gc.breakerTrips(), "an already-open breaker must not count as a new trip");
            assertEquals(3, gc.breakerSkippedChunkTotal(), "skipped chunks are counted only on the trip pass");
            assertEquals(3, gc.breakerHaltedChunks());
            assertEquals(6, confirms.get(), "open namespace breakers must skip owner confirm RPCs");
        }
    }

    @Test
    void namespaceBreakerDoesNotStopHealthyNamespaces() throws Exception {
        StrataNamespace bad = StrataNamespace.of("bad");
        StrataNamespace healthy = StrataNamespace.of("healthy");
        ChunkId bad1 = new ChunkId(FileId.of(1), 0);
        ChunkId bad2 = new ChunkId(FileId.of(2), 0);
        ChunkId bad3 = new ChunkId(FileId.of(3), 0);
        ChunkId healthy1 = new ChunkId(FileId.of(4), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = fileNotFoundServer()) {
            seal(store, bad, bad1);
            seal(store, bad, bad2);
            seal(store, bad, bad3);
            seal(store, healthy, healthy1);
            String endpoint = "127.0.0.1:" + owner.port();
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000, 2, 0, 0);

            gc.gcOnce();
            assertEquals(3, present(store, bad, bad1, bad2, bad3));
            assertEquals(1, present(store, healthy, healthy1));

            gc.gcOnce();

            assertEquals(3, present(store, bad, bad1, bad2, bad3),
                    "the bad namespace's confirmed wave must be halted");
            assertEquals(0, present(store, healthy, healthy1),
                    "a healthy sibling namespace below the breaker threshold must still drain");
            assertTrue(gc.namespaceBreakerOpen(bad));
            assertFalse(gc.namespaceBreakerOpen(healthy));
            assertFalse(gc.nodeBreakerOpen());
        }
    }

    @Test
    void exactNamespaceBudgetDeletesWithoutOpeningBreaker() throws Exception {
        ChunkId first = new ChunkId(FileId.of(1), 0);
        ChunkId second = new ChunkId(FileId.of(2), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = fileNotFoundServer()) {
            seal(store, first);
            seal(store, second);
            String endpoint = "127.0.0.1:" + owner.port();
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000, 2, 0, 0);

            gc.gcOnce();
            assertEquals(2, present(store, NS, first, second));

            gc.gcOnce();

            assertEquals(0, present(store, NS, first, second));
            assertFalse(gc.namespaceBreakerOpen(NS));
            assertEquals(0, gc.breakerTrips());
        }
    }

    @Test
    void cumulativeConfirmedOrphansOpenNamespaceBreakerAcrossPasses() throws Exception {
        ChunkId first = new ChunkId(FileId.of(1), 0);
        ChunkId second = new ChunkId(FileId.of(2), 0);
        ChunkId third = new ChunkId(FileId.of(3), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = fileNotFoundServer()) {
            seal(store, first);
            seal(store, second);
            String endpoint = "127.0.0.1:" + owner.port();
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000, 2, 0, 0);

            gc.gcOnce();
            assertEquals(2, present(store, NS, first, second));

            gc.gcOnce();
            assertEquals(0, present(store, NS, first, second));
            assertFalse(gc.namespaceBreakerOpen(NS));

            seal(store, third);
            gc.gcOnce();
            assertTrue(store.contains(NS, third));

            gc.gcOnce();

            assertTrue(store.contains(NS, third),
                    "the breaker must account for recent confirmed orphans, not only the current pass");
            assertTrue(gc.namespaceBreakerOpen(NS));
        }
    }

    @Test
    void smallNamespaceOpensBreakerWhenConfirmedWaveExceedsPercentBudget() throws Exception {
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
            assertEquals(3, present(store, NS, first, second, third));

            gc.gcOnce();

            assertEquals(3, present(store, NS, first, second, third),
                    "a confirmed wave above the percent budget must halt instead of rate-limiting deletes");
            assertTrue(gc.namespaceBreakerOpen(NS));
        }
    }

    @Test
    void nodeWideBudgetOpensGlobalBreakerAcrossNamespaces() throws Exception {
        StrataNamespace a = StrataNamespace.of("a");
        StrataNamespace b = StrataNamespace.of("b");
        ChunkId a1 = new ChunkId(FileId.of(1), 0);
        ChunkId a2 = new ChunkId(FileId.of(2), 0);
        ChunkId b1 = new ChunkId(FileId.of(3), 0);
        ChunkId b2 = new ChunkId(FileId.of(4), 0);
        AtomicInteger confirms = new AtomicInteger();
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = fileNotFoundServer(confirms)) {
            seal(store, a, a1);
            seal(store, a, a2);
            seal(store, b, b1);
            seal(store, b, b2);
            String endpoint = "127.0.0.1:" + owner.port();
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000, 64, 0, 3);

            gc.gcOnce();
            assertEquals(4, present(store, a, a1, a2) + present(store, b, b1, b2));

            gc.gcOnce();

            int remaining = present(store, a, a1, a2) + present(store, b, b1, b2);
            assertEquals(4, remaining, "a node-wide confirmed wave must open the global breaker before deletion");
            assertTrue(gc.nodeBreakerOpen());
            assertEquals(0, gc.breakerOpenNamespaces());
            assertEquals(1, gc.breakerTrips());
            assertEquals(4, gc.breakerSkippedChunkTotal());
            assertEquals(4, gc.breakerHaltedChunks());
            assertEquals(8, confirms.get());

            gc.gcOnce();

            assertEquals(4, present(store, a, a1, a2) + present(store, b, b1, b2));
            assertTrue(gc.nodeBreakerOpen());
            assertEquals(0, gc.breakerOpenNamespaces());
            assertEquals(1, gc.breakerTrips());
            assertEquals(4, gc.breakerSkippedChunkTotal());
            assertEquals(8, confirms.get(), "open node breakers must skip owner confirm RPCs");
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
                    "first FILE_NOT_FOUND only arms the delayed corroboration gate");

            gc.gcOnce();

            assertTrue(store.contains(NS, chunk),
                    "a chunk re-listed by its owner immediately before delete must be kept");
            assertEquals(2, confirms.get(), "confirmed orphans must be checked again at delete time");
        }
    }

    @Test
    void fileNotFoundPendingIsPrunedWhenChunkLeavesSuspectSet() throws Exception {
        ChunkId chunk = new ChunkId(FileId.of(1), 0);
        AtomicInteger confirms = new AtomicInteger();
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = fileNotFoundServer(confirms)) {
            seal(store, chunk);
            String endpoint = "127.0.0.1:" + owner.port();
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000, 64, 0, 0);

            gc.gcOnce();
            assertTrue(store.contains(NS, chunk),
                    "first FILE_NOT_FOUND only arms the delayed corroboration gate");

            assertEquals(ErrorCode.OK, store.delete(NS, chunk));
            gc.gcOnce();

            seal(store, chunk);
            gc.gcOnce();

            assertTrue(store.contains(NS, chunk),
                    "a reused chunk id must not inherit a stale FILE_NOT_FOUND corroboration key");
            assertEquals(2, confirms.get(), "the empty pass should prune without another owner confirm");
        }
    }

    @Test
    void gcOnceTreatsAlreadyDeletedChunkAsBenign() throws Exception {
        ChunkId chunk = new ChunkId(FileId.of(1), 0);
        AtomicInteger confirms = new AtomicInteger();
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"))) {
            seal(store, chunk);
            ChunkDeleteService deletes = new ChunkDeleteService(store, 1, 0);
            OrphanGc gc;
            try (ScpServer owner = new ScpServer(0, 0, 0, 0, req -> {
                if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                    throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
                }
                if (confirms.incrementAndGet() == 2
                        && deletes.delete(NS, chunk) != ErrorCode.OK) {
                    throw new ScpException(ErrorCode.INTERNAL, "normal delete should win first");
                }
                throw new ScpException(ErrorCode.FILE_NOT_FOUND, "no such file");
            })) {
                gc = new OrphanGc(store, deletes, NODE_ID,
                        List.of("127.0.0.1:" + owner.port()), 0, 60_000, 0, 5_000, 64, 0, 0, 0, 0);

                gc.gcOnce();
                assertTrue(store.contains(NS, chunk),
                        "first FILE_NOT_FOUND only arms the delayed corroboration gate");

                gc.gcOnce();
            }

            assertEquals(3, confirms.get(),
                    "two-pass FILE_NOT_FOUND must still reconfirm immediately before delete");
            assertFalse(store.contains(NS, chunk));
            assertEquals(1, deletes.okDeletes());
            assertEquals(1, deletes.notFoundDeletes());
            assertEquals(0, deletes.failedDeletes());
            assertEquals(1, gc.alreadyDeletedTotal());
        }
    }

    @Test
    void gcOnceKeepsInternalDeleteFailureOnFailurePath() throws Exception {
        ChunkId chunk = new ChunkId(FileId.of(1), 0);
        AtomicInteger confirms = new AtomicInteger();
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"))) {
            seal(store, chunk);
            ChunkDeleteService deletes = new ChunkDeleteService(store, 1, 0);
            OrphanGc gc;
            try (ScpServer owner = new ScpServer(0, 0, 0, 0, req -> {
                if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                    throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
                }
                if (confirms.incrementAndGet() == 2) {
                    if (deletes.delete(NS, chunk) != ErrorCode.OK) {
                        throw new ScpException(ErrorCode.INTERNAL, "normal delete should win first");
                    }
                    creatingSet(store).add(newNsChunkId(chunk));
                }
                throw new ScpException(ErrorCode.FILE_NOT_FOUND, "no such file");
            })) {
                gc = new OrphanGc(store, deletes, NODE_ID,
                        List.of("127.0.0.1:" + owner.port()), 0, 60_000, 0, 5_000, 64, 0, 0, 0, 0);

                gc.gcOnce();
                assertTrue(store.contains(NS, chunk),
                        "first FILE_NOT_FOUND only arms the delayed corroboration gate");

                gc.gcOnce();
            }

            assertEquals(3, confirms.get(), "gcOnce must still reach the delete-time reconfirm");
            assertEquals(1, deletes.okDeletes());
            assertEquals(0, deletes.notFoundDeletes(),
                    "INTERNAL must not be widened into the idempotent already-deleted path");
            assertEquals(1, deletes.failedDeletes());
            assertEquals(0, gc.alreadyDeletedTotal());
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
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000, 0, 0, 0, 0, 0);

            gc.gcOnce();
            assertTrue(store.contains(NS, first));
            assertTrue(store.contains(NS, second));
            assertTrue(store.contains(NS, third));

            gc.gcOnce();

            assertFalse(store.contains(NS, first), "zero budgets disable delete caps");
            assertFalse(store.contains(NS, second), "zero budgets disable delete caps");
            assertFalse(store.contains(NS, third), "zero budgets disable delete caps");
        }
    }

    @Test
    void cumulativeNamespaceDeleteCapHaltsSlowDripAcrossPasses() throws Exception {
        ChunkId first = new ChunkId(FileId.of(1), 0);
        ChunkId second = new ChunkId(FileId.of(2), 0);
        ChunkId third = new ChunkId(FileId.of(3), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = fileExistsWithoutChunkServer()) {
            seal(store, first);
            seal(store, second);
            String endpoint = "127.0.0.1:" + owner.port();
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000,
                    64, 0, 0, 2, 0);

            gc.gcOnce();
            assertEquals(0, present(store, NS, first, second));
            assertFalse(gc.namespaceBreakerOpen(NS));

            seal(store, third);
            gc.gcOnce();

            assertTrue(store.contains(NS, third),
                    "once the lifetime namespace delete cap is spent, later slow-drip orphans must halt");
            assertTrue(gc.namespaceBreakerOpen(NS));
            assertEquals(1, gc.breakerTrips());
            assertEquals(1, gc.cumulativeBreakerTrips());
            assertEquals(1, gc.breakerSkippedChunkTotal());
        }
    }

    @Test
    void cumulativeNodeDeleteCapHaltsAcrossNamespaces() throws Exception {
        StrataNamespace a = StrataNamespace.of("a");
        StrataNamespace b = StrataNamespace.of("b");
        StrataNamespace c = StrataNamespace.of("c");
        ChunkId a1 = new ChunkId(FileId.of(1), 0);
        ChunkId b1 = new ChunkId(FileId.of(2), 0);
        ChunkId c1 = new ChunkId(FileId.of(3), 0);
        try (ChunkStore store = new ChunkStore(dir.resolve("chunks"));
             ScpServer owner = fileExistsWithoutChunkServer()) {
            seal(store, a, a1);
            seal(store, b, b1);
            seal(store, c, c1);
            String endpoint = "127.0.0.1:" + owner.port();
            OrphanGc gc = orphanGc(store, List.of(endpoint), 0, 60_000, 0, 5_000,
                    64, 0, 0, 0, 2);

            gc.gcOnce();

            assertEquals(1, present(store, a, a1) + present(store, b, b1) + present(store, c, c1),
                    "node lifetime cap allows only two physical orphan deletes before halting the node");
            assertTrue(gc.nodeBreakerOpen());
            assertEquals(1, gc.breakerTrips());
            assertEquals(1, gc.cumulativeBreakerTrips());
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
        return fileNotFoundServer(null);
    }

    private static ScpServer fileNotFoundServer(AtomicInteger calls) throws Exception {
        return new ScpServer(0, 0, 0, 0, req -> {
            if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
            }
            if (calls != null) {
                calls.incrementAndGet();
            }
            throw new ScpException(ErrorCode.FILE_NOT_FOUND, "no such file");
        });
    }

    @SuppressWarnings("unchecked")
    private static Set<Object> creatingSet(ChunkStore store) throws Exception {
        Field field = ChunkStore.class.getDeclaredField("creating");
        field.setAccessible(true);
        return (Set<Object>) field.get(store);
    }

    private static Object newNsChunkId(ChunkId id) {
        return new NsChunkId(NS, id);
    }

    private static ScpServer fileExistsWithoutChunkServer() throws Exception {
        return new ScpServer(0, 0, 0, 0, req -> {
            if (req.opcode() != Opcode.LOOKUP_FILE.code) {
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
            }
            return ScpServer.ok(req, new Messages.LookupFileResp(NS, StrataPath.of("/empty"),
                    Messages.WritePolicy.DEFAULT, (byte) 0, List.of()).encode(), null);
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
                                     OrphanGc.OwnerEpochAcceptor ownerEpochAcceptor) {
        return orphanGc(store, controllerEndpoints, graceMs, scanIntervalMs, startupGraceMs,
                confirmTimeoutMs, OrphanGc.DEFAULT_MAX_CONFIRMED_DELETES_PER_NAMESPACE_PER_PASS,
                OrphanGc.DEFAULT_MAX_CONFIRMED_DELETE_PERCENT_PER_NAMESPACE_PER_PASS,
                OrphanGc.DEFAULT_MAX_CONFIRMED_DELETES_PER_NODE_PASS,
                OrphanGc.DEFAULT_MAX_CUMULATIVE_DELETES_PER_NAMESPACE,
                OrphanGc.DEFAULT_MAX_CUMULATIVE_DELETES_PER_NODE, ownerEpochAcceptor);
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
        return orphanGc(store, controllerEndpoints, graceMs, scanIntervalMs, startupGraceMs,
                confirmTimeoutMs, maxConfirmedDeletesPerNamespacePerPass,
                maxConfirmedDeletePercentPerNamespacePerPass, maxConfirmedDeletesPerNodePass,
                OrphanGc.DEFAULT_MAX_CUMULATIVE_DELETES_PER_NAMESPACE,
                OrphanGc.DEFAULT_MAX_CUMULATIVE_DELETES_PER_NODE);
    }

    private static OrphanGc orphanGc(ChunkStore store, List<String> controllerEndpoints,
                                     long graceMs, long scanIntervalMs,
                                     long startupGraceMs, int confirmTimeoutMs,
                                     int maxConfirmedDeletesPerNamespacePerPass,
                                     int maxConfirmedDeletePercentPerNamespacePerPass,
                                     int maxConfirmedDeletesPerNodePass,
                                     int maxCumulativeDeletesPerNamespace,
                                     int maxCumulativeDeletesPerNode) {
        return orphanGc(store, controllerEndpoints, graceMs, scanIntervalMs, startupGraceMs,
                confirmTimeoutMs, maxConfirmedDeletesPerNamespacePerPass,
                maxConfirmedDeletePercentPerNamespacePerPass, maxConfirmedDeletesPerNodePass,
                maxCumulativeDeletesPerNamespace, maxCumulativeDeletesPerNode,
                (namespace, ownerEpoch) -> {});
    }

    private static OrphanGc orphanGc(ChunkStore store, List<String> controllerEndpoints,
                                     long graceMs, long scanIntervalMs,
                                     long startupGraceMs, int confirmTimeoutMs,
                                     int maxConfirmedDeletesPerNamespacePerPass,
                                     int maxConfirmedDeletePercentPerNamespacePerPass,
                                     int maxConfirmedDeletesPerNodePass,
                                     int maxCumulativeDeletesPerNamespace,
                                     int maxCumulativeDeletesPerNode,
                                     OrphanGc.OwnerEpochAcceptor ownerEpochAcceptor) {
        ChunkDeleteService deletes = new ChunkDeleteService(store, 1, 0);
        return new OrphanGc(store, deletes, NODE_ID, controllerEndpoints,
                graceMs, scanIntervalMs, startupGraceMs, confirmTimeoutMs,
                maxConfirmedDeletesPerNamespacePerPass, maxConfirmedDeletePercentPerNamespacePerPass,
                maxConfirmedDeletesPerNodePass, maxCumulativeDeletesPerNamespace,
                maxCumulativeDeletesPerNode, ownerEpochAcceptor);
    }
}
