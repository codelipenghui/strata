package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.meta.NamespaceAssignmentPolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
        System.setProperty("strata.controller.backend", "namespace-log");
        System.setProperty("strata.controller.log.rf", "3");
        System.setProperty("strata.controller.log.ack", "2");
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
        try {
            if (cluster != null) cluster.close();
        } finally {
            System.clearProperty("strata.controller.backend");
            System.clearProperty("strata.controller.log.rf");
            System.clearProperty("strata.controller.log.ack");
        }
    }

    @Test
    void clientWritesAndReadsAcrossNamespacesOwnedByDifferentControllers() throws Exception {
        // Pick two namespaces that rendezvous-hash to DIFFERENT owners: one served directly by the seed
        // controller, one reached only by following the NOT_LEADER redirect to the other controller.
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

    private StrataNamespace namespaceOwnedBy(String endpoint) {
        for (int i = 0; i < 10_000; i++) {
            StrataNamespace ns = StrataNamespace.of("e2e-shard-" + i);
            if (owner(ns).equals(endpoint)) {
                return ns;
            }
        }
        throw new IllegalStateException("no namespace owned by " + endpoint);
    }

    /** Rendezvous owner of {@code ns} over the cluster's endpoints — must match the controllers' replicaCount=1. */
    private String owner(StrataNamespace ns) {
        return NamespaceAssignmentPolicy.assign(ns, 0, endpoints, 1).preferredLeader();
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
