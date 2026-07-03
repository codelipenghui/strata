package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.meta.ControllerConfig;
import io.strata.meta.Records;
import io.strata.meta.ZkMetadataStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The namespace-log metadata backend stores each user namespace's file/chunk metadata as a replicated
 * Strata file (design §5, §8): the metadata-log and snapshot bytes are ordinary replicated chunks on the
 * data nodes, with descriptors in ZooKeeper. This 3-node integration test drives a real cluster with
 * the backend enabled and asserts (a) user file metadata works end to end, and (b) the metadata log is
 * physically stored as replicated chunks in the reserved {@code strata-meta} system namespace — not on
 * local disk.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NamespaceLogMetadataBackendTest {

    private MiniCluster cluster;
    private StrataClient client;

    @BeforeAll
    void setup() throws Exception {
        // Select the namespace-log backend for this class; the meta-log itself is replicated RF=3.
        cluster = MiniCluster.namespaceLog(3);
        client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()));
    }

    @AfterAll
    void teardown() throws Exception {
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception ignore) {
            // best-effort
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void userMetadataIsStoredAsReplicatedStrataChunks() throws Exception {
        assertEquals("namespace-log", cluster.meta.metadataBackend());

        // create a user file and write data through the normal client path
        FileId fileId = client.create(StrataClient.FileSpec.log("tenant-a", "/topic-0")).id();
        byte[] data = new byte[16_384];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i * 31 + 7);
        }
        try (StrataFile.Appender appender = client.openById(StrataNamespace.of("tenant-a"), fileId).openForAppend()) {
            appender.append(ByteBuffer.wrap(data)).get();
            appender.seal();
        }

        // the user file's metadata — served from the per-namespace metadata log — round-trips
        assertEquals(fileId, client.open("tenant-a", "/topic-0").id());
        assertArrayEquals(data, readAll(client, fileId));

        // ...and that metadata was physically stored as a replicated Strata file in the reserved system
        // namespace: descriptors in ZooKeeper, bytes as replicated chunks on data nodes (not local).
        try (ZkMetadataStore root = new ZkMetadataStore(cluster.zk.getConnectString())) {
            List<FileId> metaLogFiles = root.listFiles(StrataNamespace.of("strata-meta"));
            assertFalse(metaLogFiles.isEmpty(),
                    "the namespace metadata log must be stored as Strata system files");
            boolean replicatedAcrossNodes = false;
            for (FileId sys : metaLogFiles) {
                Records.FileRecord record = root.getFile(StrataNamespace.of("strata-meta"), sys).orElseThrow().value();
                for (Records.ChunkRecord chunk : record.chunks()) {
                    assertFalse(chunk.replicas().isEmpty(), "a meta-log chunk must have data-node replicas");
                    if (chunk.replicas().size() >= 2) {
                        replicatedAcrossNodes = true;
                    }
                }
            }
            assertTrue(replicatedAcrossNodes,
                    "the metadata log must be replicated across multiple data nodes, not on local disk");
        }
    }

    @Test
    void multiChunkMetadataLogRecoversAfterControllerRestart() throws Exception {
        String namespaceName = "tenant-multi-chunk";
        int fileCount = 48;
        String[] paths = new String[fileCount];
        FileId[] fileIds = new FileId[fileCount];

        try (MiniCluster smallRollCluster = new MiniCluster(3, null, 1, (zk, idx) ->
                ControllerConfig.forTests(zk).withMetadataBackend(
                        ControllerConfig.MetadataBackendConfig.namespaceLog()
                                .withNamespaceLogChunkRollBytes(512)
                                .withNamespaceLogReadChunkBytes(333)
                                .withNamespaceLogCompaction(0, 0)))) {
            try (StrataClient writer = StrataClient.connect(ClientConfig.of(smallRollCluster.metaEndpoint()))) {
                for (int i = 0; i < fileCount; i++) {
                    paths[i] = "/multi-chunk-metadata-log/file-" + i
                            + "-path-padding-to-force-metadata-records-across-roll-boundaries";
                    fileIds[i] = writer.create(StrataClient.FileSpec.log(namespaceName, paths[i])).id();
                }
            }

            Records.FileRecord openLog = metadataLogRecordWithMostChunks(smallRollCluster, namespaceName);
            FileId recoveredLogFileId = openLog.fileId();
            int chunkCount = openLog.chunks().size();
            assertEquals(FileState.OPEN, openLog.state(), "metadata log should still be open before failover");
            assertTrue(chunkCount >= 3, "expected a multi-chunk metadata log, got " + chunkCount);
            assertTrue(openLog.chunks().stream().anyMatch(c -> c.state() == ChunkState.SEALED),
                    "at least one rolled metadata-log chunk must be sealed before failover");
            assertTrue(openLog.chunks().stream().anyMatch(c -> c.state() == ChunkState.OPEN),
                    "the metadata log should have an open tail before recovery");

            smallRollCluster.restartControllers();

            try (StrataClient reader = StrataClient.connect(ClientConfig.of(smallRollCluster.metaEndpoint()))) {
                for (int i = 0; i < fileCount; i++) {
                    assertEquals(fileIds[i], reader.open(namespaceName, paths[i]).id(),
                            "file " + i + " must survive multi-chunk metadata-log recovery");
                }
            }

            try (ZkMetadataStore root = new ZkMetadataStore(smallRollCluster.zk.getConnectString())) {
                Records.FileRecord sealed = root.getFile(StrataNamespace.of("strata-meta"), recoveredLogFileId)
                        .orElseThrow().value();
                assertEquals(FileState.SEALED, sealed.state(),
                        "readLog recovery must seal the old multi-chunk metadata log");
                assertEquals(chunkCount, sealed.chunks().size(),
                        "recovery should seal the same chunk sequence, not truncate the file");
                assertTrue(sealed.chunks().stream().allMatch(c -> c.state() == ChunkState.SEALED),
                        "all old metadata-log chunks should be sealed after recovery");
            }
        }
    }

    private static byte[] readAll(StrataClient client, FileId fileId) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (StrataFile.Reader reader = client.openById(StrataNamespace.of("tenant-a"), fileId).openForRead()) {
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

    private static Records.FileRecord metadataLogRecordWithMostChunks(MiniCluster cluster, String namespaceName)
            throws Exception {
        Records.FileRecord best = null;
        try (ZkMetadataStore root = new ZkMetadataStore(cluster.zk.getConnectString())) {
            for (FileId sys : root.listFiles(StrataNamespace.of("strata-meta"))) {
                Records.FileRecord record = root.getFile(StrataNamespace.of("strata-meta"), sys).orElseThrow().value();
                String path = record.path().toString();
                if (path.startsWith("/metadata-log/" + namespaceName + "/") && path.contains("/log-")
                        && (best == null || record.chunks().size() > best.chunks().size())) {
                    best = record;
                }
            }
        }
        assertNotNull(best, "expected a metadata log system file for " + namespaceName);
        return best;
    }
}
