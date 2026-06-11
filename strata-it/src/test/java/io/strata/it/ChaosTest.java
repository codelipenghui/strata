package io.strata.it;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataClient;
import io.strata.common.FileId;
import io.strata.node.NodeConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.ServerSocket;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Chaos suite (tech design §16): real network faults via Toxiproxy and a containerized
 * ZooKeeper. Run with: mvn -pl strata-it test -Dgroups=chaos -DexcludedGroups=
 *
 * Scenarios assert the STRUCTURAL product claims:
 *  - a slow replica never gates produce latency (quorum absorbs single-node stalls)
 *  - a black-holed replica triggers seal-and-roll with zero acked loss
 *  - a ZooKeeper outage does not touch the data path (metadata off the per-message path)
 */
@Tag("chaos")
class ChaosTest {

    private static final DockerImageName TOXIPROXY_IMAGE =
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0");
    private static final DockerImageName ZK_IMAGE = DockerImageName.parse("zookeeper:3.9.2");

    /** One replica answers 3s late (a long GC pause / fsync stall). Quorum must not care. */
    @Test
    void slowReplicaDoesNotGateProduceLatency() throws Exception {
        try (GenericContainer<?> toxiproxy = new GenericContainer<>(TOXIPROXY_IMAGE)
                .withExposedPorts(8474, 8666)) {

            int nodePort = freePort();
            Testcontainers.exposeHostPorts(nodePort);
            toxiproxy.start();
            ToxiproxyClient tp = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getMappedPort(8474));
            Proxy proxy = tp.createProxy("node-slow", "0.0.0.0:8666",
                    "host.testcontainers.internal:" + nodePort);
            String proxiedEndpoint = "127.0.0.1:" + toxiproxy.getMappedPort(8666);

            try (MiniCluster cluster = new MiniCluster(2)) {
                cluster.addNode(NodeConfig.withMetadata(cluster.nodeDir("host-slow"),
                                List.of(cluster.metaEndpoint()), "host-slow")
                        .withListenPort(nodePort)
                        .withAdvertisedEndpoint(proxiedEndpoint));
                cluster.awaitRegistered(3);

                try (StrataClient client = StrataClient.connect(
                        ClientConfig.of(cluster.metaEndpoint()).withChunkRollBytes(1 << 20))) {
                    FileId fileId = client.create(StrataClient.FileSpec.log("slow-replica"));
                    Workload workload = new Workload();

                    try (StrataClient.Appender appender = client.openForAppend(fileId, 1)) {
                        workload.appendAcked(appender, 0, 20); // warm-up: chunk spans all 3 nodes

                        // stall the proxied replica's responses by 3s
                        proxy.toxics().latency("slow-acks", ToxicDirection.DOWNSTREAM, 3_000);
                        long start = System.nanoTime();
                        workload.appendAcked(appender, 20, 100);
                        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                        proxy.toxics().get("slow-acks").remove();

                        // 100 quorum acks through the two healthy replicas; if the slow replica
                        // gated produce, this would take >= 100 * 3s
                        assertTrue(elapsedMs < 10_000,
                                "produce was gated by the slow replica: " + elapsedMs + "ms for 100 appends");
                        appender.seal();
                    }
                    workload.verifyAckedPrefix(client, fileId);
                }
            }
        }
    }

    /** One replica's responses are black-holed (partition). Seal-and-roll, no acked loss. */
    @Test
    void blackholedReplicaTriggersSealAndRollWithoutAckedLoss() throws Exception {
        try (GenericContainer<?> toxiproxy = new GenericContainer<>(TOXIPROXY_IMAGE)
                .withExposedPorts(8474, 8666)) {

            int nodePort = freePort();
            Testcontainers.exposeHostPorts(nodePort);
            toxiproxy.start();
            ToxiproxyClient tp = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getMappedPort(8474));
            Proxy proxy = tp.createProxy("node-dark", "0.0.0.0:8666",
                    "host.testcontainers.internal:" + nodePort);
            String proxiedEndpoint = "127.0.0.1:" + toxiproxy.getMappedPort(8666);

            try (MiniCluster cluster = new MiniCluster(3)) { // 3 healthy + 1 proxied = spare for rolls
                cluster.addNode(NodeConfig.withMetadata(cluster.nodeDir("host-dark"),
                                List.of(cluster.metaEndpoint()), "host-dark")
                        .withListenPort(nodePort)
                        .withAdvertisedEndpoint(proxiedEndpoint));
                cluster.awaitRegistered(4);

                // short per-replica timeout so the blackhole is detected quickly
                ClientConfig cfg = new ClientConfig(List.of(cluster.metaEndpoint()), 1 << 14, 2_000);
                try (StrataClient client = StrataClient.connect(cfg)) {
                    FileId fileId = client.create(StrataClient.FileSpec.log("blackhole"));
                    Workload workload = new Workload();

                    try (StrataClient.Appender appender = client.openForAppend(fileId, 1)) {
                        workload.appendAcked(appender, 0, 100);

                        // drop all responses from the proxied replica, forever
                        proxy.toxics().timeout("blackhole", ToxicDirection.DOWNSTREAM, 0);

                        // keep writing through it: per-replica timeouts fire, the appender
                        // seal-and-rolls onto healthy nodes, every append still acks
                        workload.appendAcked(appender, 100, 200);
                        appender.seal();
                    }
                    workload.verifyAckedPrefix(client, fileId);
                }
            }
        }
    }

    /** Pause ZooKeeper mid-stream: appends continue (no metadata on the data path). */
    @Test
    void zookeeperPauseDoesNotAffectDataPath() throws Exception {
        try (GenericContainer<?> zkContainer = new GenericContainer<>(ZK_IMAGE)
                .withExposedPorts(2181)) {
            zkContainer.start();
            String zkConnect = zkContainer.getHost() + ":" + zkContainer.getMappedPort(2181);

            try (MiniCluster cluster = new MiniCluster(3, zkConnect);
                 StrataClient client = StrataClient.connect(
                         ClientConfig.of(cluster.metaEndpoint()).withChunkRollBytes(1 << 24))) {

                FileId fileId = client.create(StrataClient.FileSpec.log("zk-outage"));
                Workload workload = new Workload();

                try (StrataClient.Appender appender = client.openForAppend(fileId, 1)) {
                    workload.appendAcked(appender, 0, 50);

                    // freeze ZooKeeper (SIGSTOP semantics)
                    zkContainer.getDockerClient().pauseContainerCmd(zkContainer.getContainerId()).exec();
                    try {
                        long start = System.nanoTime();
                        workload.appendAcked(appender, 50, 200); // same chunk: zero metadata involvement
                        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                        assertTrue(elapsedMs < 10_000,
                                "appends stalled during ZK outage: " + elapsedMs + "ms");
                    } finally {
                        zkContainer.getDockerClient().unpauseContainerCmd(zkContainer.getContainerId()).exec();
                    }

                    Thread.sleep(500); // let curator settle
                    appender.seal();   // chunk-boundary op needs ZK again — must work after unpause
                }
                workload.verifyAckedPrefix(client, fileId);
            }
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
