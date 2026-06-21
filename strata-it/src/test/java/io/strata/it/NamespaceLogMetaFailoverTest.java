package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;
import io.strata.meta.ControllerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayOutputStream;
import java.net.ServerSocket;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Chaos/failover for the namespace-log backend: because each namespace's metadata log is stored as
 * replicated Strata chunks (not on the meta's local disk), a metadata node can be lost and replaced and
 * the user metadata survives — the replacement recovers it from the data-node replicas (design §13). The
 * meta runs on a fixed port so a restart is reachable by the data nodes that re-register with it.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NamespaceLogMetaFailoverTest {

    private MiniCluster cluster;

    @BeforeAll
    void setup() throws Exception {
        System.setProperty("strata.controller.backend", "namespace-log");
        System.setProperty("strata.controller.log.rf", "3");
        System.setProperty("strata.controller.log.ack", "2");
        int metaPort = freePort();
        cluster = new MiniCluster(3, null, 1,
                zk -> new ControllerConfig(zk, metaPort, 200, 1_000, 1_500, 300, 3_000));
    }

    @AfterAll
    void teardown() throws Exception {
        try {
            if (cluster != null) {
                cluster.close();
            }
        } finally {
            System.clearProperty("strata.controller.backend");
            System.clearProperty("strata.controller.log.rf");
            System.clearProperty("strata.controller.log.ack");
        }
    }

    @Test
    void userMetadataSurvivesMetaRestartViaTheReplicatedLog() throws Exception {
        byte[] data = new byte[12_288];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i * 17 + 3);
        }

        FileId fileId;
        try (StrataClient client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()))) {
            fileId = client.create(StrataClient.FileSpec.log("tenant-a", "/topic-0")).id();
            try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
                appender.append(ByteBuffer.wrap(data)).get();
                appender.seal();
            }
            assertArrayEquals(data, readAll(client, fileId));
        }

        // Lose and replace the metadata node. It keeps NO local state — the only way the user file's
        // metadata can survive is if it was durably stored as replicated chunks on the data nodes.
        cluster.restartControllers();
        awaitNodesAlive(3);

        try (StrataClient fresh = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()))) {
            assertEquals(fileId, fresh.open("tenant-a", "/topic-0").id(),
                    "the user file's path binding was recovered from the replicated metadata log");
            assertArrayEquals(data, readAll(fresh, fileId),
                    "the user file's chunk metadata + data survived losing the metadata node");
        }
    }

    private void awaitNodesAlive(int count) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (cluster.meta.aliveNodes() < count && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        assertTrue(cluster.meta.aliveNodes() >= count,
                "data nodes must re-register with the restarted meta (have "
                        + cluster.meta.aliveNodes() + ")");
    }

    private static byte[] readAll(StrataClient client, FileId fileId) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (StrataFile.Reader reader = client.openById(fileId).openForRead()) {
            long offset = 0;
            while (true) {
                try (StrataFile.ReadResult result = reader.read(offset, 1 << 20)) {
                    int n = result.length();
                    if (n > 0) {
                        byte[] bytes = new byte[n];
                        result.buffer().get(bytes);
                        out.writeBytes(bytes);
                        offset += n;
                    }
                    if (n == 0 || result.endOfFile()) {
                        break;
                    }
                }
            }
        }
        return out.toByteArray();
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
