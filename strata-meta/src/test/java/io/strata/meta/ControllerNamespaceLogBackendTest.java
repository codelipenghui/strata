package io.strata.meta;

import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
                        new Messages.LookupFile(created.fileId()).encode(), null, 5_000));
                assertEquals("tenant-a", lookup.namespace().value(), "metadata served from the namespace log");

                var byPath = Messages.LookupPathResp.decode(client.call(Opcode.LOOKUP_PATH,
                        new Messages.LookupPath("tenant-a", "/logs/seg-0").encode(), null, 5_000));
                assertEquals(created.fileId(), byPath.fileId());

                var codes = Messages.DeleteFilesResp.decode(client.call(Opcode.DELETE_FILES,
                        new Messages.DeleteFiles(java.util.List.of(created.fileId())).encode(), null, 5_000));
                assertEquals(io.strata.common.ErrorCode.OK.code, codes.codes().get(0));
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
