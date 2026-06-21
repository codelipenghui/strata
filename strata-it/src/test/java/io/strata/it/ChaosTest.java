package io.strata.it;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.node.DataNodeConfig;
import io.strata.proto.Messages;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Chaos suite (tech design §16): real network faults via Toxiproxy and a containerized
 * ZooKeeper. Run with: mvn -Pchaos -pl strata-it test
 *
 * Scenarios assert the STRUCTURAL product claims:
 *  - a slow replica never gates produce latency (quorum absorbs single-node stalls)
 *  - a black-holed replica triggers seal-and-roll with zero acked loss
 *  - a healed child-JVM data-node partition repairs stale local data back to full RF
 *  - a black-holed controller leader endpoint plus leader death is retried on the new leader
 *  - a ZooKeeper outage does not touch the data path (metadata off the per-message path)
 *  - a short ZooKeeper outage at chunk-roll time is retried without acked loss
 *  - a short ZooKeeper outage at final seal time is retried without acked loss
 *  - a long ZooKeeper outage at metadata boundaries fails cleanly and remains recoverable
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
                cluster.addNode(DataNodeConfig.withMetadata(cluster.nodeDir("host-slow"),
                                List.of(cluster.metaEndpoint()), "host-slow")
                        .withListenPort(nodePort)
                        .withAdvertisedEndpoint(proxiedEndpoint));
                cluster.awaitRegistered(3);

                try (StrataClient client = StrataClient.connect(
                        ClientConfig.of(cluster.metaEndpoint()).withChunkRollBytes(1 << 20))) {
                    FileId fileId = client.create(StrataClient.FileSpec.log("test", "/slow-replica")).id();
                    Workload workload = new Workload();

                    try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
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
                cluster.addNode(DataNodeConfig.withMetadata(cluster.nodeDir("host-dark"),
                                List.of(cluster.metaEndpoint()), "host-dark")
                        .withListenPort(nodePort)
                        .withAdvertisedEndpoint(proxiedEndpoint));
                cluster.awaitRegistered(4);

                // short per-replica timeout so the blackhole is detected quickly
                ClientConfig cfg = new ClientConfig(List.of(cluster.metaEndpoint()), 1 << 14, 2_000);
                try (StrataClient client = StrataClient.connect(cfg)) {
                    FileId fileId = client.create(StrataClient.FileSpec.log("test", "/blackhole")).id();
                    Workload workload = new Workload();

                    try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
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

    /**
     * A fully partitioned replica may miss the quorum seal and keep a stale OPEN local copy. After
     * the link heals, repair must replace that copy with the sealed image instead of trusting it.
     */
    @Test
    void healedReplicaPartitionRepairsStaleOpenCopyToSealedRf() throws Exception {
        try (GenericContainer<?> toxiproxy = new GenericContainer<>(TOXIPROXY_IMAGE)
                .withExposedPorts(8474, 8666)) {

            int nodePort = freePort();
            Testcontainers.exposeHostPorts(nodePort);
            toxiproxy.start();
            ToxiproxyClient tp = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getMappedPort(8474));
            Proxy proxy = tp.createProxy("node-heal", "0.0.0.0:8666",
                    "host.testcontainers.internal:" + nodePort);
            String proxiedEndpoint = "127.0.0.1:" + toxiproxy.getMappedPort(8666);

            try (MiniCluster cluster = new MiniCluster(3)) {
                cluster.addNode(DataNodeConfig.withMetadata(cluster.nodeDir("host-heal"),
                                List.of(cluster.metaEndpoint()), "host-heal")
                        .withListenPort(nodePort)
                        .withAdvertisedEndpoint(proxiedEndpoint));
                cluster.awaitRegistered(4);

                ClientConfig cfg = new ClientConfig(List.of(cluster.metaEndpoint()), 1 << 20, 1_500);
                try (StrataClient client = StrataClient.connect(cfg)) {
                    FileId fileId = client.create(new StrataClient.FileSpec("test", "/healed-partition",
                            StrataClient.WritePolicy.replicated(4, 3))).id();
                    Workload workload = new Workload();
                    StrataFile.SealInfo sealed;

                    try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
                        workload.appendAcked(appender, 0, 40);
                        assertOpenChunkContainsEndpoint(cluster, fileId, proxiedEndpoint);

                        proxy.toxics().timeout("partition-upstream", ToxicDirection.UPSTREAM, 0);
                        proxy.toxics().timeout("partition-downstream", ToxicDirection.DOWNSTREAM, 0);
                        try {
                            workload.appendAcked(appender, 40, 40);
                            sealed = appender.seal();
                        } finally {
                            removeToxic(proxy, "partition-upstream");
                            removeToxic(proxy, "partition-downstream");
                        }
                    }

                    assertEquals(workload.ackedBytes(), sealed.sealedLength(),
                            "partitioned appender sealed at a non-acked length");
                    workload.verifyAckedPrefix(client, fileId);
                    waitForFullReplicaConsistency(cluster, client, fileId, sealed.sealedLength(), 4);
                }
            }
        }
    }

    /**
     * Same stale-open repair property as the in-process partition test, but data nodes run in
     * child JVMs so the data plane crosses process boundaries while Toxiproxy owns the partition.
     */
    @Test
    void processDataNodePartitionHealsAndRepairsStaleOpenCopy() throws Exception {
        List<ExternalDataNode> dataNodes = new ArrayList<>();
        try (GenericContainer<?> toxiproxy = new GenericContainer<>(TOXIPROXY_IMAGE)
                .withExposedPorts(8474, 8666);
             MiniCluster cluster = new MiniCluster(0)) {

            int nodePort = freePort();
            Testcontainers.exposeHostPorts(nodePort);
            toxiproxy.start();
            ToxiproxyClient tp = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getMappedPort(8474));
            Proxy proxy = tp.createProxy("process-node-heal", "0.0.0.0:8666",
                    "host.testcontainers.internal:" + nodePort);
            String proxiedEndpoint = "127.0.0.1:" + toxiproxy.getMappedPort(8666);

            try {
                for (int i = 0; i < 3; i++) {
                    dataNodes.add(startDataNodeProcess(cluster, "process-heal-host-" + i));
                }
                dataNodes.add(startDataNodeProcess(cluster, "process-heal-host-proxied",
                        nodePort, proxiedEndpoint));
                cluster.awaitRegistered(4);

                ClientConfig cfg = new ClientConfig(List.of(cluster.metaEndpoint()), 1 << 20, 1_500);
                try (StrataClient client = StrataClient.connect(cfg)) {
                    FileId fileId = client.create(new StrataClient.FileSpec("test",
                            "/process-healed-partition",
                            StrataClient.WritePolicy.replicated(4, 3))).id();
                    Workload workload = new Workload();
                    StrataFile.SealInfo sealed;

                    try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
                        workload.appendAcked(appender, 0, 40);
                        assertOpenChunkContainsEndpoint(cluster, fileId, proxiedEndpoint);

                        proxy.toxics().timeout("process-partition-upstream", ToxicDirection.UPSTREAM, 0);
                        proxy.toxics().timeout("process-partition-downstream", ToxicDirection.DOWNSTREAM, 0);
                        try {
                            workload.appendAcked(appender, 40, 40);
                            sealed = appender.seal();
                        } finally {
                            removeToxic(proxy, "process-partition-upstream");
                            removeToxic(proxy, "process-partition-downstream");
                        }
                    }

                    assertEquals(workload.ackedBytes(), sealed.sealedLength(),
                            "process partition sealed at a non-acked length");
                    workload.verifyAckedPrefix(client, fileId);
                    waitForFullReplicaConsistency(cluster, client, fileId, sealed.sealedLength(), 4);
                }
            } finally {
                for (ExternalDataNode node : dataNodes) {
                    kill(node);
                }
            }
        }
    }

    /**
     * The client may be pinned to a controller leader endpoint that becomes black-holed before the
     * leader process dies. Boundary operations must time out, rotate, and complete on the new
     * leader without losing acknowledged bytes.
     */
    @Test
    void metadataLeaderProxyBlackholeDuringChunkRollRetriesOnNewLeader() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3, null, 2)) {
            int leaderIndex = leaderIndex(cluster);
            MetadataEndpoint leader = metadataEndpoint(cluster, leaderIndex);
            List<String> fallbackEndpoints = cluster.metaEndpoints().stream()
                    .filter(endpoint -> !endpoint.equals(leader.endpoint()))
                    .toList();
            assertEquals(1, fallbackEndpoints.size(), "test expects exactly one fallback controller endpoint");

            Testcontainers.exposeHostPorts(leader.port());
            try (GenericContainer<?> toxiproxy = new GenericContainer<>(TOXIPROXY_IMAGE)
                    .withExposedPorts(8474, 8666)) {
                toxiproxy.start();
                ToxiproxyClient tp = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getMappedPort(8474));
                Proxy proxy = tp.createProxy("meta-leader", "0.0.0.0:8666",
                        "host.testcontainers.internal:" + leader.port());
                String proxiedLeader = "127.0.0.1:" + toxiproxy.getMappedPort(8666);

                int payloadBytes = Workload.payload(0).length;
                int warmupRecords = 8;
                long rollBytes = (long) payloadBytes * warmupRecords;
                List<String> controllerEndpoints = List.of(proxiedLeader, fallbackEndpoints.get(0));
                try (StrataClient client = StrataClient.connect(new ClientConfig(
                        controllerEndpoints, rollBytes, 1_500))) {
                    FileId fileId = client.create(StrataClient.FileSpec.log(
                            "test", "/metadata-leader-proxy-blackhole")).id();
                    Workload workload = new Workload();

                    try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
                        workload.appendAcked(appender, 0, warmupRecords);

                        proxy.toxics().timeout("metadata-blackhole", ToxicDirection.DOWNSTREAM, 0);
                        try {
                            cluster.killMeta(leaderIndex);
                            workload.appendAcked(appender, warmupRecords, 25);
                            StrataFile.SealInfo sealed = appender.seal();
                            assertEquals(workload.ackedBytes(), sealed.sealedLength(),
                                    "metadata failover sealed at a non-acked length");
                        } finally {
                            removeToxic(proxy, "metadata-blackhole");
                        }
                    }

                    workload.verifyAckedPrefix(client, fileId);
                    ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId,
                            workload.ackedBytes());
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

                FileId fileId = client.create(StrataClient.FileSpec.log("test", "/zk-outage")).id();
                Workload workload = new Workload();

                try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
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

    /** A short ZooKeeper outage at a chunk boundary is retried without losing acked bytes. */
    @Test
    void zookeeperPauseDuringChunkRollIsRetriedWithoutAckedLoss() throws Exception {
        try (GenericContainer<?> zkContainer = new GenericContainer<>(ZK_IMAGE)
                .withExposedPorts(2181)) {
            zkContainer.start();
            String zkConnect = zkContainer.getHost() + ":" + zkContainer.getMappedPort(2181);

            int payloadBytes = Workload.payload(0).length;
            int warmupRecords = 8;
            long rollBytes = (long) payloadBytes * warmupRecords;
            try (MiniCluster cluster = new MiniCluster(3, zkConnect);
                 StrataClient client = StrataClient.connect(new ClientConfig(
                         List.of(cluster.metaEndpoint()), rollBytes, 5_000))) {

                FileId fileId = client.create(StrataClient.FileSpec.log("test", "/zk-roll-outage")).id();
                Workload workload = new Workload();

                try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
                    workload.appendAcked(appender, 0, warmupRecords);

                    zkContainer.getDockerClient().pauseContainerCmd(zkContainer.getContainerId()).exec();
                    CompletableFuture<Void> unpause = unpauseAfterDelay(zkContainer, 1_500);
                    try {
                        long start = System.nanoTime();
                        workload.appendAcked(appender, warmupRecords, 40);
                        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                        assertTrue(elapsedMs < 15_000,
                                "chunk roll did not recover after short ZK outage: " + elapsedMs + "ms");
                    } finally {
                        unpauseContainer(zkContainer);
                        unpause.get(10, TimeUnit.SECONDS);
                    }

                    appender.seal();
                }

                workload.verifyAckedPrefix(client, fileId);
                Messages.LookupFileResp lookup = ConsistencyVerifier.lookupFile(cluster, fileId);
                assertTrue(lookup.chunks().size() >= 2, "test did not exercise a chunk roll");
            }
        }
    }

    /** Final file sealing also crosses metadata; a short ZooKeeper pause there is retried. */
    @Test
    void zookeeperPauseDuringFinalSealIsRetriedWithoutAckedLoss() throws Exception {
        try (GenericContainer<?> zkContainer = new GenericContainer<>(ZK_IMAGE)
                .withExposedPorts(2181)) {
            zkContainer.start();
            String zkConnect = zkContainer.getHost() + ":" + zkContainer.getMappedPort(2181);

            try (MiniCluster cluster = new MiniCluster(3, zkConnect);
                 StrataClient client = StrataClient.connect(new ClientConfig(
                         List.of(cluster.metaEndpoint()), 1 << 20, 5_000))) {

                FileId fileId = client.create(StrataClient.FileSpec.log("test", "/zk-final-seal-outage")).id();
                Workload workload = new Workload();
                StrataFile.SealInfo sealed;

                try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
                    workload.appendAcked(appender, 0, 80);

                    zkContainer.getDockerClient().pauseContainerCmd(zkContainer.getContainerId()).exec();
                    CompletableFuture<Void> unpause = unpauseAfterDelay(zkContainer, 1_500);
                    try {
                        long start = System.nanoTime();
                        sealed = appender.seal();
                        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                        assertTrue(elapsedMs < 15_000,
                                "final seal did not recover after short ZK outage: " + elapsedMs + "ms");
                    } finally {
                        unpauseContainer(zkContainer);
                        unpause.get(10, TimeUnit.SECONDS);
                    }
                }

                assertEquals(workload.ackedBytes(), sealed.sealedLength(),
                        "final seal committed a non-acked length after ZK outage");
                workload.verifyAckedPrefix(client, fileId);
                ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealed.sealedLength());
            }
        }
    }

    /** If metadata stays unavailable past the retry deadline during roll, acked bytes recover. */
    @Test
    void zookeeperLongPauseDuringChunkRollFailsCleanlyAndRecoversAckedPrefix() throws Exception {
        try (GenericContainer<?> zkContainer = new GenericContainer<>(ZK_IMAGE)
                .withExposedPorts(2181)) {
            zkContainer.start();
            String zkConnect = zkContainer.getHost() + ":" + zkContainer.getMappedPort(2181);

            int payloadBytes = Workload.payload(0).length;
            int warmupRecords = 8;
            long rollBytes = (long) payloadBytes * warmupRecords;
            try (MiniCluster cluster = new MiniCluster(3, zkConnect);
                 StrataClient client = StrataClient.connect(new ClientConfig(
                         List.of(cluster.metaEndpoint()), rollBytes, 1_000))) {

                FileId fileId = client.create(StrataClient.FileSpec.log(
                        "test", "/zk-long-roll-outage")).id();
                Workload workload = new Workload();

                try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
                    workload.appendAcked(appender, 0, warmupRecords);

                    zkContainer.getDockerClient().pauseContainerCmd(zkContainer.getContainerId()).exec();
                    try {
                        assertThrows(ScpException.class,
                                () -> workload.appendAcked(appender, warmupRecords, 1),
                                "chunk roll must fail after metadata remains unavailable past the retry deadline");
                    } finally {
                        unpauseContainer(zkContainer);
                    }
                }

                waitForMetadataAvailable(cluster, fileId);
                StrataFile.SealInfo recovered = client.openById(fileId).recoverAndSeal();
                assertTrue(recovered.sealedLength() >= workload.ackedBytes(),
                        "recovery sealed " + recovered.sealedLength()
                                + " below acked " + workload.ackedBytes());
                workload.verifyAckedPrefix(client, fileId);
                ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId,
                        recovered.sealedLength());
            }
        }
    }

    /** If final seal times out against metadata, the abandoned file is still recoverable. */
    @Test
    void zookeeperLongPauseDuringFinalSealFailsCleanlyAndRecoversAckedPrefix() throws Exception {
        try (GenericContainer<?> zkContainer = new GenericContainer<>(ZK_IMAGE)
                .withExposedPorts(2181)) {
            zkContainer.start();
            String zkConnect = zkContainer.getHost() + ":" + zkContainer.getMappedPort(2181);

            try (MiniCluster cluster = new MiniCluster(3, zkConnect);
                 StrataClient client = StrataClient.connect(new ClientConfig(
                         List.of(cluster.metaEndpoint()), 1 << 20, 1_000))) {

                FileId fileId = client.create(StrataClient.FileSpec.log(
                        "test", "/zk-long-final-seal-outage")).id();
                Workload workload = new Workload();

                try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
                    workload.appendAcked(appender, 0, 80);

                    zkContainer.getDockerClient().pauseContainerCmd(zkContainer.getContainerId()).exec();
                    try {
                        assertThrows(ScpException.class, appender::seal,
                                "final seal must fail after metadata remains unavailable past the retry deadline");
                    } finally {
                        unpauseContainer(zkContainer);
                    }
                }

                waitForMetadataAvailable(cluster, fileId);
                StrataFile.SealInfo recovered = client.openById(fileId).recoverAndSeal();
                assertTrue(recovered.sealedLength() >= workload.ackedBytes(),
                        "recovery sealed " + recovered.sealedLength()
                                + " below acked " + workload.ackedBytes());
                workload.verifyAckedPrefix(client, fileId);
                ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId,
                        recovered.sealedLength());
            }
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static int leaderIndex(MiniCluster cluster) {
        for (int i = 0; i < cluster.metas.size(); i++) {
            if (cluster.metas.get(i).isLeader()) {
                return i;
            }
        }
        throw new AssertionError("no controller leader");
    }

    private static MetadataEndpoint metadataEndpoint(MiniCluster cluster, int index) {
        String endpoint = cluster.metas.get(index).endpoint();
        int colon = endpoint.lastIndexOf(':');
        assertTrue(colon > 0, "invalid controller endpoint: " + endpoint);
        return new MetadataEndpoint(endpoint, Integer.parseInt(endpoint.substring(colon + 1)));
    }

    private record MetadataEndpoint(String endpoint, int port) {
    }

    private static ExternalDataNode startDataNodeProcess(MiniCluster cluster, String host) throws Exception {
        return startDataNodeProcess(cluster, host, 0, null);
    }

    private static ExternalDataNode startDataNodeProcess(MiniCluster cluster, String host, int listenPort,
                                                       String advertisedEndpoint) throws Exception {
        Path logRoot = processLogRoot();
        String processName = host + "-" + System.nanoTime();
        Path readyFile = logRoot.resolve(processName + ".ready");
        Path logFile = logRoot.resolve(processName + ".log");
        Files.createFile(logFile);

        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Dorg.slf4j.simpleLogger.defaultLogLevel=warn");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(DataNodeProcessMain.class.getName());
        command.add(cluster.nodeDir(host).toString());
        command.add(String.join(",", cluster.metaEndpoints()));
        command.add(host);
        command.add(readyFile.toString());
        if (advertisedEndpoint != null) {
            command.add(Integer.toString(listenPort));
            command.add(advertisedEndpoint);
        }

        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        builder.redirectOutput(logFile.toFile());
        Process process = builder.start();
        return waitDataNodeReady(new ExternalDataNode(host, readyFile, logFile, process, -1, ""));
    }

    private static ExternalDataNode waitDataNodeReady(ExternalDataNode node) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(node.readyFile())) {
                String[] parts = Files.readString(node.readyFile()).trim().split("\\s+");
                if (parts.length == 2) {
                    return new ExternalDataNode(node.host(), node.readyFile(), node.logFile(),
                            node.process(), Integer.parseInt(parts[0]), parts[1]);
                }
            }
            if (!node.process().isAlive()) {
                throw new AssertionError("data-node process exited early with code "
                        + node.process().exitValue() + "\n" + childLog(node));
            }
            Thread.sleep(50);
        }
        throw new AssertionError("data-node process did not become ready\n" + childLog(node));
    }

    private static void kill(ExternalDataNode node) throws Exception {
        if (node == null || !node.process().isAlive()) {
            return;
        }
        node.process().destroyForcibly();
        if (!node.process().waitFor(10, TimeUnit.SECONDS)) {
            throw new AssertionError("data-node process did not exit after destroyForcibly");
        }
    }

    private static String childLog(ExternalDataNode node) throws Exception {
        return Files.exists(node.logFile()) ? Files.readString(node.logFile()) : "";
    }

    private static Path processLogRoot() throws Exception {
        Path root = Path.of("target", "chaos-process-logs");
        Files.createDirectories(root);
        return root;
    }

    private record ExternalDataNode(String host, Path readyFile, Path logFile, Process process,
                                   int nodeId, String endpoint) {
    }

    private static void assertOpenChunkContainsEndpoint(MiniCluster cluster, FileId fileId,
                                                        String endpoint) throws Exception {
        Messages.LookupFileResp file = ConsistencyVerifier.lookupFile(cluster, fileId);
        Messages.ChunkInfo tail = file.chunks().get(file.chunks().size() - 1);
        assertTrue(tail.replicas().stream().anyMatch(r -> r.endpoint().equals(endpoint)),
                "test setup requires the proxied node in the open replica set");
    }

    private static void waitForFullReplicaConsistency(MiniCluster cluster, StrataClient client,
                                                      FileId fileId, long sealedLength,
                                                      int replicationFactor) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        AssertionError last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                cluster.meta.reconcileNow();
                Messages.LookupFileResp lookup =
                        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealedLength);
                boolean fullRf = lookup.chunks().stream()
                        .allMatch(chunk -> ConsistencyVerifier.readableSealedReplicaCount(chunk)
                                == replicationFactor);
                if (fullRf) {
                    return;
                }
                last = new AssertionError("sealed file has not repaired to RF="
                        + replicationFactor + ": " + lookup.chunks());
            } catch (AssertionError e) {
                last = e;
            }
            Thread.sleep(250);
        }
        throw last != null ? last : new AssertionError("file did not repair to RF=" + replicationFactor);
    }

    private static void removeToxic(Proxy proxy, String name) {
        try {
            proxy.toxics().get(name).remove();
        } catch (Exception ignored) {
        }
    }

    private static void waitForMetadataAvailable(MiniCluster cluster, FileId fileId) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        Throwable last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                ConsistencyVerifier.lookupFile(cluster, fileId);
                return;
            } catch (Throwable t) {
                last = t;
                Thread.sleep(250);
            }
        }
        AssertionError failure = new AssertionError("metadata did not recover after ZooKeeper unpause");
        if (last != null) {
            failure.addSuppressed(last);
        }
        throw failure;
    }

    private static CompletableFuture<Void> unpauseAfterDelay(GenericContainer<?> container, long delayMs) {
        return CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(delayMs);
                unpauseContainer(container);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted before unpausing ZooKeeper", e);
            }
        });
    }

    private static void unpauseContainer(GenericContainer<?> container) {
        try {
            container.getDockerClient().unpauseContainerCmd(container.getContainerId()).exec();
        } catch (Exception ignored) {
        }
    }
}
