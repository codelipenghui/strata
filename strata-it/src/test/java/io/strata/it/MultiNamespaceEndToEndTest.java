package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ChunkId;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.format.ChunkFormats;
import io.strata.meta.ZkMetadataStore;
import io.strata.node.DataNode;
import io.strata.proto.Messages;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration capstone (Task 9): proves the long-fileId + namespace-aware data plane works
 * end-to-end across two namespaces, through a data-node restart.
 *
 * <p><b>Backend:</b> namespace-log (the production/docker-compose default). This is required:
 * only the namespace-log backend gives per-namespace owner-assigned ids that start at 0.
 * The ZK-direct backend would hand out globally-unique IDs that do NOT start at 0 per namespace.
 *
 * <p><b>What is tested:</b>
 * <ol>
 *   <li>Files created in {@code ns-t1-0} and {@code ns-t1-1} each receive FileId(0) as their
 *       first id (the core per-namespace counter property Tasks 1-4 established).
 *   <li>Chunks land on disk at {@code chunks/<ns>/00/00/0000000000000000.0} — the
 *       namespace-sharded layout Task 6 defined.
 *   <li>After a data-node restart, the chunk store walks the namespace-sharded tree, recovers
 *       both namespaces independently, and both files read back correctly.
 *   <li>Cross-namespace id(0) files are independent — no collision via the {@code (ns, ChunkId)}
 *       composite key in ChunkStore (NsChunkId).
 * </ol>
 *
 * <p>Each test method uses its own namespace pair to avoid state leaking between tests.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultiNamespaceEndToEndTest {

    private MiniCluster cluster;
    private StrataClient client;

    @BeforeAll
    void setup() throws Exception {
        // Select the namespace-log backend: per-namespace owner-assigned fileIds starting at 0.
        cluster = MiniCluster.namespaceLog(3);
        client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint())
                .withChunkRollBytes(4096)
                .withDataNodeConnectionsPerEndpoint(3));
    }

    @AfterAll
    void teardown() throws Exception {
        try {
            if (client != null) client.close();
        } catch (Exception ignore) {
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    /**
     * Creates one file in each of two namespaces, writes and seals data, then asserts:
     * <ul>
     *   <li>Both files received FileId(0) — the per-namespace counter starts at 0 per namespace.
     *   <li>After sealing, data reads back correctly for both namespaces.
     *   <li>Chunks are on disk under the namespace-sharded path
     *       {@code chunks/<ns>/00/00/0000000000000000.0} (FileId 0, index 0).
     * </ul>
     *
     * <p>Uses namespace pair {@code ns-t1-0} / {@code ns-t1-1} to isolate from other tests.
     */
    @Test
    void perNamespaceIdsStartAtZeroAndDataReadsBack() throws Exception {
        assertEquals("namespace-log", cluster.meta.metadataBackend(),
                "this test requires the namespace-log backend");

        StrataNamespace ns0 = StrataNamespace.of("ns-t1-0");
        StrataNamespace ns1 = StrataNamespace.of("ns-t1-1");

        // Create one file in each namespace; each should receive FileId(0).
        FileId fileId0 = client.create(StrataClient.FileSpec.log("ns-t1-0", "/topic-0")).id();
        FileId fileId1 = client.create(StrataClient.FileSpec.log("ns-t1-1", "/topic-0")).id();

        assertEquals(FileId.of(0), fileId0,
                "first file in ns-t1-0 must receive FileId(0) — per-namespace counter starts at 0");
        assertEquals(FileId.of(0), fileId1,
                "first file in ns-t1-1 must receive FileId(0) — per-namespace counter starts at 0");

        // Write distinctive data into each namespace, seal, and read back.
        byte[] data0 = makeData(8192, 7);
        byte[] data1 = makeData(8192, 13);

        try (StrataFile.Appender a = client.openById(ns0, fileId0).openForAppend()) {
            a.append(ByteBuffer.wrap(data0)).get();
            a.seal();
        }
        try (StrataFile.Appender a = client.openById(ns1, fileId1).openForAppend()) {
            a.append(ByteBuffer.wrap(data1)).get();
            a.seal();
        }

        assertArrayEquals(data0, readAll(client, ns0, fileId0), "ns-t1-0 data must read back");
        assertArrayEquals(data1, readAll(client, ns1, fileId1), "ns-t1-1 data must read back");

        // Verify chunk paths: for FileId(0) the layout is <ns>/00/00/0000000000000000.0
        // The expected path fragment validates the Task-6 namespace-sharded disk layout.
        String expectedSuffix0 = ChunkFormats.chunkRelativePath(ns0,
                new ChunkId(FileId.of(0), 0)) + ".chunk";
        String expectedSuffix1 = ChunkFormats.chunkRelativePath(ns1,
                new ChunkId(FileId.of(0), 0)) + ".chunk";
        // ns-t1-0/00/00/0000000000000000.0.chunk
        assertTrue(expectedSuffix0.startsWith("ns-t1-0/00/00/"),
                "path " + expectedSuffix0 + " must start with ns-t1-0/00/00/");
        assertTrue(expectedSuffix1.startsWith("ns-t1-1/00/00/"),
                "path " + expectedSuffix1 + " must start with ns-t1-1/00/00/");

        // Assert the chunk files are physically present on at least one node's data dir.
        assertChunkFileExists(cluster, ns0, FileId.of(0), 0,
                "ns-t1-0 chunk file must exist on disk");
        assertChunkFileExists(cluster, ns1, FileId.of(0), 0,
                "ns-t1-1 chunk file must exist on disk");
    }

    /**
     * Writes and seals data in two namespaces, restarts one data node (in-process), then asserts
     * that both namespaces recover independently — no cross-namespace collision from having the
     * same FileId(0) in each namespace.
     *
     * <p>Uses namespace pair {@code ns-t2-0} / {@code ns-t2-1} to isolate from other tests.
     */
    @Test
    void dataNodeRestartRecoversBothNamespacesIndependently() throws Exception {
        assertEquals("namespace-log", cluster.meta.metadataBackend(),
                "this test requires the namespace-log backend");

        StrataNamespace ns0 = StrataNamespace.of("ns-t2-0");
        StrataNamespace ns1 = StrataNamespace.of("ns-t2-1");

        // Create first files in fresh namespaces — both should receive FileId(0).
        FileId fileId0 = client.create(StrataClient.FileSpec.log("ns-t2-0", "/topic-0")).id();
        FileId fileId1 = client.create(StrataClient.FileSpec.log("ns-t2-1", "/topic-0")).id();
        assertEquals(FileId.of(0), fileId0, "first file in ns-t2-0 must be FileId(0)");
        assertEquals(FileId.of(0), fileId1, "first file in ns-t2-1 must be FileId(0)");

        byte[] data0 = makeData(4096, 19);
        byte[] data1 = makeData(4096, 23);
        try (StrataFile.Appender a = client.openById(ns0, fileId0).openForAppend()) {
            a.append(ByteBuffer.wrap(data0)).get();
            a.seal();
        }
        try (StrataFile.Appender a = client.openById(ns1, fileId1).openForAppend()) {
            a.append(ByteBuffer.wrap(data1)).get();
            a.seal();
        }

        // Restart node 0 — this triggers ChunkStore.recoverAll(), which must walk the
        // namespace-sharded tree and recover both namespaces' chunks independently.
        cluster.restartNode(0);

        // Allow a brief moment for the node to re-register and become active.
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            try (ZkMetadataStore zk =
                         new ZkMetadataStore(cluster.zk.getConnectString())) {
                if (zk.listNodes().size() >= 3) break;
            }
            Thread.sleep(100);
        }

        // After restart, sealed files in both namespaces must read back correctly.
        // Use a direct namespace-aware metadata lookup (ConsistencyVerifier.lookupFile uses the
        // TEST_NS hardcode for the data-plane STAT/FETCH calls, which would be wrong here).
        Messages.LookupFileResp lookup0 =
                ConsistencyVerifier.lookupFile(cluster.metaEndpoints(), ns0, fileId0);
        Messages.LookupFileResp lookup1 =
                ConsistencyVerifier.lookupFile(cluster.metaEndpoints(), ns1, fileId1);

        assertEquals(FileState.SEALED.value, lookup0.fileState(),
                "ns-t2-0 file must be SEALED after restart");
        assertEquals(FileState.SEALED.value, lookup1.fileState(),
                "ns-t2-1 file must be SEALED after restart");
        assertFalse(lookup0.chunks().isEmpty(), "ns-t2-0 file must have at least one chunk");
        assertFalse(lookup1.chunks().isEmpty(), "ns-t2-1 file must have at least one chunk");

        // Cross-namespace independence: the two FileId(0) files in different namespaces are
        // served as distinct items with no collision in the (namespace, ChunkId) key space.
        for (Messages.ChunkInfo c : lookup0.chunks()) {
            assertTrue(c.replicas().size() >= 2,
                    "ns-t2-0 chunk must have replicas after restart");
        }
        for (Messages.ChunkInfo c : lookup1.chunks()) {
            assertTrue(c.replicas().size() >= 2,
                    "ns-t2-1 chunk must have replicas after restart");
        }

        // Final readback for both files — proves the restarted data node recovered the chunk data.
        assertArrayEquals(data0, readAll(client, ns0, fileId0),
                "ns-t2-0 data must survive node restart");
        assertArrayEquals(data1, readAll(client, ns1, fileId1),
                "ns-t2-1 data must survive node restart");
    }

    /**
     * Exercises {@code deleteFile} routing under cross-namespace FileId(0) collision (Task 9 regression fix).
     *
     * <p>With per-namespace ids both {@code ns-t3-0} and {@code ns-t3-1} get {@code FileId(0)}.
     * The old global {@code fileIndex} would record only one of them (last-writer-wins), so a
     * {@code deleteFile} for {@code ns-t3-0/FileId(0)} could silently resolve to
     * {@code ns-t3-1/FileId(0)} and destroy the wrong namespace's file. The fix routes every
     * delete by namespace, making cross-namespace collision impossible.
     *
     * <p>Assertion: after deleting {@code ns-t3-0}'s file, {@code ns-t3-1}'s file STILL exists
     * and reads back its correct bytes — a misrouted delete would have destroyed it.
     *
     * <p>Uses namespace pair {@code ns-t3-0} / {@code ns-t3-1} to isolate from other tests.
     */
    @Test
    void deleteRoutingIsNamespaceScopedUnderFileIdCollision() throws Exception {
        assertEquals("namespace-log", cluster.meta.metadataBackend(),
                "this test requires the namespace-log backend");

        StrataNamespace ns0 = StrataNamespace.of("ns-t3-0");
        StrataNamespace ns1 = StrataNamespace.of("ns-t3-1");

        // Create the first file in each namespace — both receive FileId(0).
        FileId fileId0 = client.create(StrataClient.FileSpec.log("ns-t3-0", "/topic-0")).id();
        FileId fileId1 = client.create(StrataClient.FileSpec.log("ns-t3-1", "/topic-0")).id();
        assertEquals(FileId.of(0), fileId0, "ns-t3-0 first file must be FileId(0)");
        assertEquals(FileId.of(0), fileId1, "ns-t3-1 first file must be FileId(0)");

        // Write distinctive data into each and seal.
        byte[] data0 = makeData(4096, 31);
        byte[] data1 = makeData(4096, 37);
        try (StrataFile.Appender a = client.openById(ns0, fileId0).openForAppend()) {
            a.append(ByteBuffer.wrap(data0)).get();
            a.seal();
        }
        try (StrataFile.Appender a = client.openById(ns1, fileId1).openForAppend()) {
            a.append(ByteBuffer.wrap(data1)).get();
            a.seal();
        }

        // Both files readable before deletion.
        assertArrayEquals(data0, readAll(client, ns0, fileId0), "ns-t3-0 data must read before delete");
        assertArrayEquals(data1, readAll(client, ns1, fileId1), "ns-t3-1 data must read before delete");

        // Delete only ns-t3-0's file — if routing uses the global index it would misroute and
        // delete ns-t3-1's file instead (whichever was last in the index).
        client.deleteById(ns0, fileId0);

        // ns-t3-1's FileId(0) file must still be present and readable with its original data.
        // A misrouted delete would have destroyed ns-t3-1's file, causing this assertion to fail.
        assertArrayEquals(data1, readAll(client, ns1, fileId1),
                "ns-t3-1/FileId(0) must survive deletion of ns-t3-0/FileId(0) — "
                + "failure means deleteFile misrouted across namespaces");
    }

    // ---- helpers ----

    private static byte[] makeData(int size, int seed) {
        byte[] d = new byte[size];
        for (int i = 0; i < size; i++) {
            d[i] = (byte) ((i * seed + seed) & 0xFF);
        }
        return d;
    }

    private static byte[] readAll(StrataClient client, StrataNamespace ns, FileId fileId) {
        try (StrataFile.Reader reader = client.openById(ns, fileId).openForRead()) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            long offset = 0;
            int idle = 0;
            while (idle < 3) {
                try (StrataFile.ReadResult r = reader.read(offset, 1 << 20)) {
                    int n = r.length();
                    if (n > 0) {
                        byte[] tmp = new byte[n];
                        r.buffer().get(tmp);
                        out.writeBytes(tmp);
                        offset += n;
                        idle = 0;
                    } else if (r.endOfFile()) {
                        break;
                    } else {
                        idle++;
                        reader.refresh();
                    }
                }
            }
            return out.toByteArray();
        }
    }

    /**
     * Asserts that at least one data node's chunk store contains the chunk file at the expected
     * namespace-sharded path: {@code <dataDir>/chunks/<ns>/<l1>/<l2>/<fileId>.<index>.chunk}.
     *
     * <p>DataNode wires ChunkStore to {@code config.dataDir().resolve("chunks")}, so the full
     * physical path is {@code <dataDir>/chunks/<relPath>}.
     */
    private static void assertChunkFileExists(MiniCluster cluster,
                                               StrataNamespace ns, FileId fileId, int index,
                                               String message) {
        ChunkId chunkId = new ChunkId(fileId, index);
        String rel = ChunkFormats.chunkRelativePath(ns, chunkId) + ".chunk";
        boolean found = false;
        for (DataNode node : cluster.nodes) {
            // ChunkStore root = dataDir/chunks (see DataNode constructor)
            Path chunkStoreDir = node.config().dataDir().resolve("chunks");
            Path chunkPath = chunkStoreDir.resolve(rel);
            if (Files.exists(chunkPath)) {
                found = true;
                break;
            }
        }
        assertTrue(found, message + " — expected at path chunks/" + rel
                + " under one of the data node dirs");
    }
}
