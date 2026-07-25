package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ChunkId;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.meta.NamespaceAssignmentPolicy;
import io.strata.node.DataNode;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end over a namespace-SHARDED two-controller cluster (design §6). The other integration tests
 * run a single controller that owns every namespace; this one drives the real {@link StrataClient}
 * write+read data path through a cluster where each namespace is owned by a different controller, so the
 * client must follow the non-owner's {@code NOT_LEADER} redirect to the owning controller (and the load
 * actually splits across both). Closes the coverage gap that only {@code MetadataShardingRoutingTest}'s
 * raw-ScpClient meta-layer checks previously touched.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ShardedClusterEndToEndTest {

    private MiniCluster cluster;
    private StrataClient client;
    private List<String> endpoints;

    @BeforeAll
    void setup() throws Exception {
        // Sharding requires the namespace-log backend (per-namespace owner-assigned ids + ownership routing).
        cluster = MiniCluster.sharded(3, 2);
        endpoints = cluster.metaEndpoints();
        // Seed the client with the FIRST controller only: every namespace owned by the second controller
        // therefore forces controller-0 to answer NOT_LEADER, and the owner-aware client must re-route.
        client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint())
                .withChunkRollBytes(4096)
                .withDataNodeConnectionsPerEndpoint(3));
    }

    @AfterAll
    void teardown() throws Exception {
        try {
            if (client != null) client.close();
        } catch (Exception ignore) {
            // best-effort
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void clientWritesAndReadsAcrossNamespacesOwnedByDifferentControllers() throws Exception {
        // Pick two namespaces whose initial persisted replica sets prefer DIFFERENT controllers: one served
        // directly by the seed, one reached by following the NOT_LEADER redirect to the other controller.
        StrataNamespace nsA = namespaceOwnedBy(endpoints.get(0));
        StrataNamespace nsB = namespaceOwnedBy(endpoints.get(1));
        assertNotEquals(owner(nsA), owner(nsB), "the two namespaces must have distinct owning controllers");

        FileId idA = client.create(StrataClient.FileSpec.log(nsA.value(), "/topic")).id();
        FileId idB = client.create(StrataClient.FileSpec.log(nsB.value(), "/topic")).id();

        byte[] dataA = makeData(8192, 7);
        byte[] dataB = makeData(8192, 13);
        write(nsA, idA, dataA);
        write(nsB, idB, dataB);

        assertArrayEquals(dataA, readAll(nsA, idA), "namespace A reads back through its owning controller");
        assertArrayEquals(dataB, readAll(nsB, idB), "namespace B reads back through the NOT_LEADER redirect");

        // Both controllers must have actually opened a per-namespace repository — proof the metadata load
        // was sharded across the two controllers rather than all served by one.
        assertTrue(cluster.metas.get(0).loadedNamespaces() >= 1, "controller 0 must own+load at least one namespace");
        assertTrue(cluster.metas.get(1).loadedNamespaces() >= 1, "controller 1 must own+load at least one namespace");
    }

    @Test
    void orphanGcWalksPastNonOwnerAfterShardedControllerRestart() throws Exception {
        StrataNamespace namespace = namespaceOwnedBy(endpoints.get(1));
        FileId placeholder = client.create(
                StrataClient.FileSpec.log(namespace.value(), "/authority-placeholder")).id();
        long ownerEpoch =
                ConsistencyVerifier.lookupFile(endpoints, namespace, placeholder).ownerEpoch();

        // Stop the old controller processes before planting the orphan. This makes it impossible for a
        // pre-restart GC pass to delete the chunk and pins the reclamation to the replacement-owner path.
        client.close();
        cluster.stopControllers();

        DataNode node = cluster.nodes.get(0);
        ChunkId orphan = new ChunkId(FileId.of(9_000_000), 0);
        try (ScpClient direct = new ScpClient(
                "127.0.0.1", node.port(), ScpClient.KIND_TOOL, "sharded-orphan-planter")) {
            direct.call(Opcode.OPEN_CHUNK,
                    new Messages.OpenChunk(orphan, 1, false, 1 << 20,
                            System.currentTimeMillis(), namespace).encode(), null, 5_000);
            direct.call(Opcode.APPEND,
                    new Messages.Append(orphan, 1, 0, 0, namespace).encode(),
                    ByteBuffer.wrap("sharded-orphan".getBytes()), 5_000);
            direct.call(Opcode.SEAL_CHUNK,
                    new Messages.SealChunk(orphan, 1, 14, namespace, ownerEpoch).encode(), null, 5_000);
        }
        assertTrue(node.store().contains(namespace, orphan));

        // Start replacement processes on the same fixed endpoints. The first endpoint is still a
        // non-owner for this namespace; orphan GC must walk past its NOT_LEADER response, let the owner
        // recover the namespace-log manifest, obtain an authoritative verdict, and reclaim the orphan.
        cluster.startControllers();
        client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint())
                .withChunkRollBytes(4096)
                .withDataNodeConnectionsPerEndpoint(3));
        assertEquals(placeholder, client.open(namespace.value(), "/authority-placeholder").id(),
                "the replacement owner must recover and activate the namespace-log repository");

        long deadline = System.currentTimeMillis() + 45_000;
        while (System.currentTimeMillis() < deadline && node.store().contains(namespace, orphan)) {
            Thread.sleep(250);
        }
        assertFalse(node.store().contains(namespace, orphan),
                "sharded namespace-log orphan GC did not recover authority and drain through the endpoint walk");
    }

    private StrataNamespace namespaceOwnedBy(String endpoint) {
        for (int i = 0; i < 10_000; i++) {
            StrataNamespace ns = StrataNamespace.of("e2e-shard-" + i);
            if (owner(ns).equals(endpoint)) {
                return ns;
            }
        }
        throw new IllegalStateException("no namespace owned by " + endpoint);
    }

    /** Initial persisted owner of {@code ns}; HRW's first endpoint is independent of replica-set width. */
    private String owner(StrataNamespace ns) {
        return NamespaceAssignmentPolicy.assign(ns, 0, endpoints, endpoints.size()).preferredLeader();
    }

    private void write(StrataNamespace ns, FileId id, byte[] data) throws Exception {
        try (StrataFile.Appender a = client.openById(ns, id).openForAppend()) {
            a.append(ByteBuffer.wrap(data)).get();
            a.seal();
        }
    }

    private byte[] readAll(StrataNamespace ns, FileId fileId) {
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

    private static byte[] makeData(int size, int seed) {
        byte[] d = new byte[size];
        for (int i = 0; i < size; i++) {
            d[i] = (byte) ((i * seed + seed) & 0xFF);
        }
        return d;
    }
}
