package io.strata.meta;

import io.strata.common.ErrorCode;
import io.strata.common.StrataNamespace;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end: a {@link Controller} wired to the namespace-log backend serves the full file
 * lifecycle over its SCP surface, with user-file metadata persisted through per-namespace metadata
 * logs instead of direct ZooKeeper znodes (design §16 Step 3). The SCP surface is unchanged.
 */
class ControllerNamespaceLogBackendTest {

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

    private static void awaitLeader(Controller service) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!service.isLeader() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(service.isLeader(), "service must acquire leadership");
    }
}
