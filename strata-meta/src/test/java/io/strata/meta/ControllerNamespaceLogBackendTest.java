package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end: a {@link Controller} wired to the namespace-log backend serves the full file
 * lifecycle over its SCP surface, with user-file metadata persisted through per-namespace metadata
 * logs instead of direct ZooKeeper znodes (design §16 Step 3). The SCP surface is unchanged.
 */
class ControllerNamespaceLogBackendTest {

    @Test
    void brokerCannotAccessSystemNamespaceButMetadataClientCan() throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            TestNamespaceMetadataFileStore fileStore = new TestNamespaceMetadataFileStore();
            BiFunction<ZkMetadataStore, String, MetadataStore> backend =
                    (root, endpoint) -> new NamespaceLogMetadataStore(new NamespaceLogBackend(root, fileStore, true));
            try (Controller service =
                         new Controller(ControllerConfig.forTests(zk.getConnectString()), null, backend);
                 ScpClient metadata = new ScpClient("127.0.0.1", service.port(),
                         ScpClient.KIND_METADATA, "system-metadata-test");
                 ScpClient broker = new ScpClient("127.0.0.1", service.port(),
                         ScpClient.KIND_BROKER, "application-test");
                 ScpClient tool = new ScpClient("127.0.0.1", service.port(),
                         ScpClient.KIND_TOOL, "operator-test")) {
                awaitLeader(service);

                FileId systemFile = Messages.CreateFileResp.decode(metadata.call(Opcode.CREATE_FILE,
                        new Messages.CreateFile("strata-meta", "/metadata-log/internal-test",
                                new Messages.WritePolicy(3, 2, false)).encode(), null, 5_000)).fileId();

                assertSystemNamespaceRejected(() -> broker.call(Opcode.CREATE_FILE,
                        new Messages.CreateFile("strata-meta", "/application-file",
                                new Messages.WritePolicy(3, 2, false)).encode(), null, 5_000));
                assertSystemNamespaceRejected(() -> broker.call(Opcode.LOOKUP_PATH,
                        new Messages.LookupPath("strata-meta", "/metadata-log/internal-test").encode(),
                        null, 5_000));
                assertSystemNamespaceRejected(() -> broker.call(Opcode.LOOKUP_FILE,
                        new Messages.LookupFile(NamespaceLogBackend.SYSTEM_NAMESPACE, systemFile).encode(),
                        null, 5_000));
                assertSystemNamespaceRejected(() -> broker.call(Opcode.ALLOCATE_WRITER_EPOCH,
                        Messages.AllocateWriterEpoch.forRecovery(
                                NamespaceLogBackend.SYSTEM_NAMESPACE, systemFile).encode(), null, 5_000));
                assertSystemNamespaceRejected(() -> broker.call(Opcode.CREATE_CHUNK,
                        new Messages.CreateChunk(NamespaceLogBackend.SYSTEM_NAMESPACE, systemFile, 1).encode(),
                        null, 5_000));
                assertSystemNamespaceRejected(() -> broker.call(Opcode.SEAL_CHUNK_META,
                        new Messages.SealChunkMeta(NamespaceLogBackend.SYSTEM_NAMESPACE,
                                new ChunkId(systemFile, 0), 1, 0, 0, List.of()).encode(), null, 5_000));
                assertSystemNamespaceRejected(() -> broker.call(Opcode.ABORT_CHUNK_META,
                        new Messages.AbortChunkMeta(NamespaceLogBackend.SYSTEM_NAMESPACE,
                                new ChunkId(systemFile, 0), 1, 0, 0).encode(), null, 5_000));
                assertSystemNamespaceRejected(() -> broker.call(Opcode.SEAL_FILE,
                        new Messages.SealFile(NamespaceLogBackend.SYSTEM_NAMESPACE, systemFile, 0).encode(),
                        null, 5_000));
                assertSystemNamespaceRejected(() -> broker.call(Opcode.DELETE_FILES,
                        new Messages.DeleteFiles(NamespaceLogBackend.SYSTEM_NAMESPACE, List.of(systemFile)).encode(),
                        null, 5_000));
                assertSystemNamespaceRejected(() -> tool.call(Opcode.LOOKUP_FILE,
                        new Messages.LookupFile(NamespaceLogBackend.SYSTEM_NAMESPACE, systemFile).encode(),
                        null, 5_000));

                var lookup = Messages.LookupFileResp.decode(metadata.call(Opcode.LOOKUP_FILE,
                        new Messages.LookupFile(NamespaceLogBackend.SYSTEM_NAMESPACE, systemFile).encode(),
                        null, 5_000));
                var byPath = Messages.LookupPathResp.decode(metadata.call(Opcode.LOOKUP_PATH,
                        new Messages.LookupPath("strata-meta", "/metadata-log/internal-test").encode(),
                        null, 5_000));
                assertEquals(systemFile, byPath.fileId());
                assertEquals(NamespaceLogBackend.SYSTEM_NAMESPACE, lookup.namespace());
                assertTrue(Messages.AllocateWriterEpochResp.decode(metadata.call(Opcode.ALLOCATE_WRITER_EPOCH,
                        Messages.AllocateWriterEpoch.forRecovery(
                                NamespaceLogBackend.SYSTEM_NAMESPACE, systemFile).encode(), null, 5_000))
                        .writerEpoch() > 0);
            }
        }
    }

    @Test
    void serviceServesFileLifecycleOverTheNamespaceLogBackend() throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            TestNamespaceMetadataFileStore fileStore = new TestNamespaceMetadataFileStore();
            BiFunction<ZkMetadataStore, String, MetadataStore> backend =
                    (root, endpoint) -> new NamespaceLogMetadataStore(new NamespaceLogBackend(root, fileStore, true));
            try (Controller service =
                         new Controller(ControllerConfig.forTests(zk.getConnectString()), null, backend);
                 ScpClient client = new ScpClient("127.0.0.1", service.port(), ScpClient.KIND_TOOL, "nslog")) {
                awaitLeader(service);
                assertEquals("namespace-log", service.metadataBackend());

                var created = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                        new Messages.CreateFile("tenant-a", "/logs/seg-0",
                                new Messages.WritePolicy(3, 2, true)).encode(), null, 5_000));

                var lookup = Messages.LookupFileResp.decode(client.call(Opcode.LOOKUP_FILE,
                        new Messages.LookupFile(StrataNamespace.of("tenant-a"), created.fileId()).encode(), null, 5_000));
                assertEquals("tenant-a", lookup.namespace().value(), "metadata served from the namespace log");
                assertTrue(lookup.ownerEpoch() > 0, "LOOKUP_FILE responses must carry the namespace owner epoch");

                ScpException missing = assertThrows(ScpException.class, () -> client.call(Opcode.LOOKUP_FILE,
                        new Messages.LookupFile(StrataNamespace.of("tenant-a"), FileId.of(999)).encode(), null,
                        5_000));
                assertEquals(ErrorCode.FILE_NOT_FOUND, missing.code());
                assertEquals(lookup.ownerEpoch(), missing.detail(),
                        "destructive orphan-GC FILE_NOT_FOUND confirms must carry the namespace owner epoch");

                var byPath = Messages.LookupPathResp.decode(client.call(Opcode.LOOKUP_PATH,
                        new Messages.LookupPath("tenant-a", "/logs/seg-0").encode(), null, 5_000));
                assertEquals(created.fileId(), byPath.fileId());

                var codes = Messages.DeleteFilesResp.decode(client.call(Opcode.DELETE_FILES,
                        new Messages.DeleteFiles(StrataNamespace.of("tenant-a"), List.of(created.fileId())).encode(), null, 5_000));
                assertEquals(ErrorCode.OK.code, codes.codes().get(0));
            }
        }
    }

    @Test
    void perNamespaceLogStatsTrackOwnedNamespaceActivity() throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            TestNamespaceMetadataFileStore fileStore = new TestNamespaceMetadataFileStore();
            BiFunction<ZkMetadataStore, String, MetadataStore> backend =
                    (root, endpoint) -> new NamespaceLogMetadataStore(new NamespaceLogBackend(root, fileStore, true));
            try (Controller service =
                         new Controller(ControllerConfig.forTests(zk.getConnectString()), null, backend);
                 ScpClient client = new ScpClient("127.0.0.1", service.port(), ScpClient.KIND_TOOL, "nslog")) {
                awaitLeader(service);
                client.call(Opcode.CREATE_FILE, new Messages.CreateFile("tenant-a", "/logs/seg-0",
                        new Messages.WritePolicy(3, 2, true)).encode(), null, 5_000);

                long[] stat = service.namespaceLogStats().get("tenant-a");
                assertNotNull(stat, "namespace-log stats must be keyed by the owned namespace");
                assertTrue(stat[NamespaceLogMetrics.APPEND_RECORDS] >= 1, "create appended a metadata-log record");
                assertEquals(1, stat[NamespaceLogMetrics.OWNER_CHANGES],
                        "tenant-a was cold-acquired exactly once (an ownership handoff to this controller)");
                assertTrue(service.namespaceStats().containsKey("tenant-a"),
                        "this controller owns tenant-a (so it emits the owner gauge for it)");
                assertNotNull(service.localControllerEndpoint(), "owner label for the namespace-owner gauge");
            }
        }
    }

    @Test
    void ownedRecoveringNamespaceRejectsConcurrentOpsWithMetadataRecovering() throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            TestNamespaceMetadataFileStore delegate = new TestNamespaceMetadataFileStore();
            NamespaceLogCowCompactionTest.BlockingSnapshotFileStore blocking =
                    new NamespaceLogCowCompactionTest.BlockingSnapshotFileStore(delegate);
            AtomicReference<NamespaceLogBackend> backendRef = new AtomicReference<>();
            StrataNamespace namespace = StrataNamespace.of("tenant-a");
            BiFunction<ZkMetadataStore, String, MetadataStore> backend = (root, endpoint) -> {
                NamespaceLogBackend logBackend = new NamespaceLogBackend(root, blocking, true);
                backendRef.set(logBackend);
                return new NamespaceLogMetadataStore(logBackend);
            };

            try (Controller service =
                         new Controller(ControllerConfig.forTests(zk.getConnectString()), null, backend);
                 ScpClient opener = new ScpClient("127.0.0.1", service.port(), ScpClient.KIND_TOOL, "nslog-open");
                 ScpClient probe = new ScpClient("127.0.0.1", service.port(), ScpClient.KIND_TOOL, "nslog-probe")) {
                awaitLeader(service);
                blocking.armed = true;

                CompletableFuture<FileId> create = CompletableFuture.supplyAsync(() -> sup(() ->
                        Messages.CreateFileResp.decode(opener.call(Opcode.CREATE_FILE,
                                new Messages.CreateFile("tenant-a", "/logs/seg-recovering",
                                        new Messages.WritePolicy(3, 2, true)).encode(), null, 5_000)).fileId()));
                try {
                    assertTrue(blocking.entered.await(2, TimeUnit.SECONDS),
                            "first request must park inside namespace-log recovery");
                    assertEquals(NamespaceLeaderState.RECOVERING, backendRef.get().leaderState(namespace));
                    assertEquals(0, backendRef.get().namespaceOwnerEpoch(namespace),
                            "RECOVERING owns a manifest epoch but must not expose it to owner RPCs yet");

                    ScpException recovering = assertThrows(ScpException.class, () ->
                            probe.call(Opcode.LOOKUP_PATH,
                                    new Messages.LookupPath("tenant-a", "/logs/seg-recovering").encode(),
                                    null, 1_000));

                    assertEquals(ErrorCode.METADATA_RECOVERING, recovering.code());
                    assertTrue(recovering.retriable(), "client should back off and retry the same owner");
                } finally {
                    blocking.block.countDown();
                }
                assertEquals(FileId.of(0), create.get(5, TimeUnit.SECONDS));
                assertTrue(backendRef.get().namespaceOwnerEpoch(namespace) > 0,
                        "ACTIVE namespaces expose their owner epoch");
            }
        }
    }

    @Test
    void fencedNamespaceFallsThroughToLazyReopenOnNextOperation() throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            TestNamespaceMetadataFileStore fileStore = new TestNamespaceMetadataFileStore();
            AtomicBoolean loseNextManifestCas = new AtomicBoolean(false);
            AtomicReference<NamespaceLogBackend> backendRef = new AtomicReference<>();
            StrataNamespace namespace = StrataNamespace.of("tenant-a");
            BiFunction<ZkMetadataStore, String, MetadataStore> backend = (root, endpoint) -> {
                NamespaceLogBackend logBackend = new NamespaceLogBackend(
                        failManifestCasOnce(root, loseNextManifestCas), fileStore, true);
                backendRef.set(logBackend);
                return new NamespaceLogMetadataStore(logBackend);
            };

            try (Controller service =
                         new Controller(ControllerConfig.forTests(zk.getConnectString()), null, backend);
                 ScpClient client = new ScpClient("127.0.0.1", service.port(), ScpClient.KIND_TOOL,
                         "nslog-fenced")) {
                awaitLeader(service);
                var created = Messages.CreateFileResp.decode(client.call(Opcode.CREATE_FILE,
                        new Messages.CreateFile(namespace.value(), "/logs/seg-fenced",
                                new Messages.WritePolicy(3, 2, true)).encode(), null, 5_000));
                NamespaceLogBackend logBackend = backendRef.get();
                assertEquals(NamespaceLeaderState.ACTIVE, logBackend.leaderState(namespace));

                loseNextManifestCas.set(true);
                assertEquals(0, logBackend.compactOversizedRepos(1),
                        "lost manifest CAS fences and evicts without publishing a compaction");
                assertEquals(NamespaceLeaderState.FENCED, logBackend.leaderState(namespace));
                assertEquals(0, logBackend.namespaceOwnerEpoch(namespace),
                        "FENCED namespaces must not expose their stale owner epoch");

                var byPath = Messages.LookupPathResp.decode(client.call(Opcode.LOOKUP_PATH,
                        new Messages.LookupPath(namespace.value(), "/logs/seg-fenced").encode(), null, 5_000));

                assertEquals(created.fileId(), byPath.fileId());
                assertEquals(NamespaceLeaderState.ACTIVE, logBackend.leaderState(namespace),
                        "the next client op lazily re-opens a FENCED namespace");
                assertTrue(logBackend.namespaceOwnerEpoch(namespace) > 0,
                        "the reopened ACTIVE namespace exposes the fresh owner epoch");
            }
        }
    }

    private static void awaitLeader(Controller service) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!service.isLeader() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(service.isLeader(), "service must acquire leadership");
    }

    private static void assertSystemNamespaceRejected(Executable operation) {
        ScpException rejected = assertThrows(ScpException.class, operation);
        assertEquals(ErrorCode.PRECONDITION_FAILED, rejected.code());
        assertFalse(rejected.retriable(), "reserved namespace rejection must not be retried");
        assertTrue(rejected.getMessage().contains("reserved for internal metadata"),
                "the request must be rejected by the system-namespace ingress gate");
        assertTrue(rejected.getMessage().contains("client kind="),
                "the rejection must identify the presented role for client-side diagnosis");
    }

    private static MetadataStore failManifestCasOnce(MetadataStore delegate, AtomicBoolean armed) {
        return (MetadataStore) Proxy.newProxyInstance(
                MetadataStore.class.getClassLoader(),
                new Class<?>[]{MetadataStore.class},
                (proxy, method, methodArgs) -> {
                    if (method.getName().equals("putNamespaceManifest") && armed.compareAndSet(true, false)) {
                        return OptionalInt.empty();
                    }
                    try {
                        return method.invoke(delegate, methodArgs);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    private static <T> T sup(ThrowingSupplier<T> s) {
        try {
            return s.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
