package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.meta.ZkMetadataStore;
import io.strata.node.DataNodeConfig;
import io.strata.node.DataNode;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OS-process crash coverage for the metadata control plane. This complements MiniCluster's
 * in-process metadata failover by killing the JVM that owns the active SCP controller endpoint.
 */
@Tag("chaos")
class MetadataProcessFailoverTest {

    @Test
    void clientAndDataNodesRecoverAfterMetadataLeaderProcessCrashDuringChunkRoll() throws Exception {
        List<ExternalMetadata> metas = new ArrayList<>();
        List<DataNode> nodes = new ArrayList<>();
        Path root = Files.createTempDirectory("strata-meta-process");

        try (TestingServer zk = new TestingServer(true)) {
            metas.add(startMetadata(zk.getConnectString(), "meta-a"));
            metas.add(startMetadata(zk.getConnectString(), "meta-b"));
            List<String> controllerEndpoints = metas.stream().map(ExternalMetadata::endpoint).toList();

            String firstLeader = awaitLeader(controllerEndpoints);
            for (int i = 0; i < 3; i++) {
                nodes.add(new DataNode(DataNodeConfig.withMetadata(root.resolve("host-" + i),
                        controllerEndpoints, "host-" + i)));
            }
            awaitRegistered(zk.getConnectString(), 3);

            ClientConfig config = new ClientConfig(controllerEndpoints, 512, 5_000)
                    .withDataNodeConnectionsPerEndpoint(2);
            try (StrataClient client = StrataClient.connect(config)) {
                FileId fileId = client.create(new StrataClient.FileSpec("test",
                        "/metadata-process-failover",
                        StrataClient.WritePolicy.replicated(3, 2))).id();
                Workload workload = new Workload();
                StrataFile.SealInfo sealed;

                try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
                    workload.appendAcked(appender, 0, 20);

                    kill(metadataByEndpoint(metas, firstLeader));
                    String newLeader = awaitLeader(controllerEndpoints);
                    assertFalse(firstLeader.equals(newLeader), "killed controller endpoint remained leader");

                    workload.appendAcked(appender, 20, 80);
                    sealed = appender.seal();
                }

                assertEquals(workload.ackedBytes(), sealed.sealedLength(),
                        "metadata process failover sealed at a non-acked length");
                workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);
                ConsistencyVerifier.assertSealedFileConsistent(controllerEndpoints, client, fileId,
                        sealed.sealedLength());
            }
        } finally {
            for (DataNode node : nodes) {
                try {
                    node.close();
                } catch (Exception ignored) {
                }
            }
            for (ExternalMetadata meta : metas) {
                kill(meta);
            }
        }
    }

    @Test
    void clientSurvivesMetadataLeaderAndDataNodeReplicaProcessCrashInSameOpenSession() throws Exception {
        List<ExternalMetadata> metas = new ArrayList<>();
        List<ExternalDataNode> dataNodes = new ArrayList<>();
        Path root = Files.createTempDirectory("strata-combined-process");

        try (TestingServer zk = new TestingServer(true)) {
            metas.add(startMetadata(zk.getConnectString(), "combined-meta-a"));
            metas.add(startMetadata(zk.getConnectString(), "combined-meta-b"));
            List<String> controllerEndpoints = metas.stream().map(ExternalMetadata::endpoint).toList();

            for (int i = 0; i < 4; i++) {
                dataNodes.add(startDataNode(root.resolve("host-" + i), controllerEndpoints,
                        "combined-host-" + i));
            }
            awaitRegistered(zk.getConnectString(), 4);

            ClientConfig config = new ClientConfig(controllerEndpoints, 512, 5_000)
                    .withDataNodeConnectionsPerEndpoint(2);
            try (StrataClient client = StrataClient.connect(config)) {
                FileId fileId = client.create(new StrataClient.FileSpec("test",
                        "/metadata-and-data-node-process-failover",
                        StrataClient.WritePolicy.replicated(3, 2))).id();
                Workload workload = new Workload();
                StrataFile.SealInfo sealed;

                try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
                    workload.appendAcked(appender, 0, 20);

                    Messages.ChunkInfo openChunk = lastChunk(
                            ConsistencyVerifier.lookupFile(controllerEndpoints, fileId));
                    ExternalDataNode dataNodeVictim = dataNodeByNodeId(dataNodes,
                            openChunk.replicas().get(0).nodeId());
                    kill(dataNodeVictim);

                    String firstLeader = awaitLeader(controllerEndpoints);
                    kill(metadataByEndpoint(metas, firstLeader));
                    String newLeader = awaitLeader(controllerEndpoints);
                    assertFalse(firstLeader.equals(newLeader), "killed controller endpoint remained leader");

                    workload.appendAcked(appender, 20, 80);
                    sealed = appender.seal();
                }

                assertEquals(workload.ackedBytes(), sealed.sealedLength(),
                        "combined metadata/data-node process crash sealed at a non-acked length");
                workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);
                ConsistencyVerifier.assertSealedFileConsistent(controllerEndpoints, client, fileId,
                        sealed.sealedLength());
            }
        } finally {
            for (ExternalDataNode node : dataNodes) {
                kill(node);
            }
            for (ExternalMetadata meta : metas) {
                kill(meta);
            }
        }
    }

    @Test
    void deterministicProcessFaultSchedulePreservesAckedBytes() throws Exception {
        long seed = 0x51eaf00d20260612L;
        Random random = new Random(seed);
        String seedHex = Long.toUnsignedString(seed, 16);
        CorrectnessArtifact artifact = CorrectnessArtifact.create("process-stress-fault", seedHex);
        List<ExternalMetadata> metas = new ArrayList<>();
        List<ExternalDataNode> dataNodes = new ArrayList<>();

        artifact.add("scenario=process-stress-fault",
                "seed=" + seedHex,
                "batches=12",
                "replayCommand=./scripts/verify.sh --skip-default --fault"
                        + " -Dtest=MetadataProcessFailoverTest#deterministicProcessFaultSchedulePreservesAckedBytes",
                "writePolicy=rf3-aq2-fsynctrue",
                "dataNodeConnectionsPerEndpoint=2");

        try (TestingServer zk = new TestingServer(true)) {
            Path root = Files.createTempDirectory("strata-process-stress-fault");
            metas.add(startMetadata(zk.getConnectString(), "stress-meta-a"));
            metas.add(startMetadata(zk.getConnectString(), "stress-meta-b"));
            List<String> controllerEndpoints = controllerEndpoints(metas);
            artifact.add("initialMetadataEndpoints=" + controllerEndpoints);

            for (int i = 0; i < 4; i++) {
                dataNodes.add(startDataNode(root.resolve("host-" + i), controllerEndpoints,
                        "stress-host-" + i));
            }
            awaitRegistered(zk.getConnectString(), 4);
            artifact.add("initialDataNode=" + dataNodeSummary(dataNodes));

            ClientConfig config = new ClientConfig(controllerEndpoints, 512, 10_000)
                    .withDataNodeConnectionsPerEndpoint(2);
            try (StrataClient client = StrataClient.connect(config)) {
                FileId fileId = client.create(new StrataClient.FileSpec("test",
                        "/process-stress-fault",
                        StrataClient.WritePolicy.fsync(3, 2))).id();
                artifact.add("fileId=" + fileId);

                BinaryWorkload workload = new BinaryWorkload();
                try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
                    ExternalDataNode killedDataNode = null;

                    try {
                        for (int batch = 0; batch < 12; batch++) {
                            int records = 2 + random.nextInt(4);
                            int nextRecord = workload.appendRandomBatch(appender, random, records);
                            artifact.add("batch=" + batch
                                    + " appendedRecords=" + records
                                    + " nextRecord=" + nextRecord
                                    + " ackedBytes=" + workload.ackedBytes()
                                    + " ackedSha256=" + workload.ackedSha256());

                            if (batch == 2) {
                                Messages.ChunkInfo openChunk = lastChunk(
                                        ConsistencyVerifier.lookupFile(controllerEndpoints, fileId));
                                int victimReplica = random.nextInt(openChunk.replicas().size());
                                killedDataNode = dataNodeByNodeId(dataNodes,
                                        openChunk.replicas().get(victimReplica).nodeId());
                                artifact.add("fault=kill-data-node-process batch=" + batch
                                        + " nodeId=" + killedDataNode.nodeId()
                                        + " endpoint=" + killedDataNode.endpoint()
                                        + " openChunk=" + openChunk.chunkId());
                                kill(killedDataNode);
                            }

                            if (batch == 4 || batch == 8) {
                                workload.verifyOpenReadIsAckedPrefix(client, StrataNamespace.of("test"), fileId,
                                        "process stress/fault batch " + batch);
                                ConsistencyVerifier.assertLiveFileDescriptorConsistent(
                                        controllerEndpoints, fileId);
                                artifact.add("openReadVerifiedBatch=" + batch);
                                artifact.add("liveDescriptorVerifiedBatch=" + batch);
                            }

                            if (batch == 5) {
                                String oldLeader = awaitLeader(controllerEndpoints);
                                artifact.add("fault=kill-metadata-leader batch=" + batch
                                        + " endpoint=" + oldLeader);
                                kill(metadataByEndpoint(metas, oldLeader));
                                String newLeader = awaitLeader(controllerEndpoints);
                                assertFalse(oldLeader.equals(newLeader),
                                        "killed controller endpoint remained leader");
                                artifact.add("metadataLeaderAfterFault=" + newLeader);
                            }

                            if (batch == 7 && killedDataNode != null) {
                                ExternalDataNode restarted = startDataNode(killedDataNode.dataDir(),
                                        controllerEndpoints, killedDataNode.host());
                                assertEquals(killedDataNode.nodeId(), restarted.nodeId(),
                                        "node id changed after data-node process restart");
                                dataNodes.set(dataNodeIndexByNodeId(dataNodes,
                                        killedDataNode.nodeId()), restarted);
                                waitForAllReplicaEndpoints(controllerEndpoints, fileId,
                                        dataNodeMapByNodeId(dataNodes));
                                ConsistencyVerifier.assertLiveFileDescriptorConsistent(
                                        controllerEndpoints, fileId);
                                artifact.add("fault=restarted-data-node-process batch=" + batch
                                        + " nodeId=" + restarted.nodeId()
                                        + " endpoint=" + restarted.endpoint());
                                artifact.add("liveDescriptorVerifiedAfterDataNodeRestart=true");
                            }
                        }

                        String leader = awaitLeader(controllerEndpoints);
                        artifact.add("leaderAvailableBeforeSeal=" + leader);
                        ConsistencyVerifier.assertLiveFileDescriptorConsistent(controllerEndpoints, fileId);
                        artifact.add("liveDescriptorVerifiedBeforeSeal=true");
                        StrataFile.SealInfo sealed = appender.seal();
                        assertEquals(workload.ackedBytes(), sealed.sealedLength(),
                                "process stress/fault sealed at a non-acked length");

                        workload.verifySealedAckedPrefix(client, StrataNamespace.of("test"), fileId, "process stress/fault");
                        Messages.LookupFileResp finalDescriptor =
                                ConsistencyVerifier.waitForFullSealedFileConsistent(
                                        controllerEndpoints, client, fileId, sealed.sealedLength());
                        artifact.add("sealedLength=" + sealed.sealedLength(),
                                "finalAckedBytes=" + workload.ackedBytes(),
                                "finalAckedSha256=" + workload.ackedSha256(),
                                "fullReplicaConsistencyAfterRepair=true");
                        artifact.addDescriptor("finalDescriptor", finalDescriptor);
                        artifact.markPassed();
                        requireProcessStressArtifactEvidence(artifact);
                    } catch (Exception | AssertionError t) {
                        artifact.addFailure(t);
                        throw t;
                    }
                }
            }
        } catch (Exception | AssertionError t) {
            artifact.addFailure(t);
            throw t;
        } finally {
            killAllDataNode(dataNodes);
            killAllMetadata(metas);
        }
    }

    @Test
    void fullProcessClusterRestartRecoversAckedOpenChunkFromSameDisks() throws Exception {
        long seed = 0x5eed_f00d_2026_0612L;
        String context = "full process cluster restart seed " + Long.toUnsignedString(seed, 16);
        String seedHex = Long.toUnsignedString(seed, 16);
        CorrectnessArtifact artifact = CorrectnessArtifact.create("full-process-cluster-restart", seedHex);
        artifact.add("scenario=full-process-cluster-restart",
                "seed=" + seedHex,
                "replayCommand=./scripts/verify.sh --skip-default --fault"
                        + " -Dtest=MetadataProcessFailoverTest#"
                        + "fullProcessClusterRestartRecoversAckedOpenChunkFromSameDisks",
                "writePolicy=rf3-aq2-fsynctrue");

        List<ExternalMetadata> metas = new ArrayList<>();
        List<ExternalMetadata> restartedMetas = new ArrayList<>();
        List<ExternalDataNode> dataNodes = new ArrayList<>();
        List<ExternalDataNode> restartedDataNodes = new ArrayList<>();
        Path root = Files.createTempDirectory("strata-full-process-restart");

        try (TestingServer zk = new TestingServer(true)) {
            metas.add(startMetadata(zk.getConnectString(), "restart-meta-a"));
            metas.add(startMetadata(zk.getConnectString(), "restart-meta-b"));
            List<String> controllerEndpoints = controllerEndpoints(metas);
            artifact.add("initialMetadataEndpoints=" + controllerEndpoints);

            for (int i = 0; i < 4; i++) {
                dataNodes.add(startDataNode(root.resolve("host-" + i), controllerEndpoints,
                        "restart-host-" + i));
            }
            awaitRegistered(zk.getConnectString(), 4);
            artifact.add("initialDataNode=" + dataNodeSummary(dataNodes));

            ClientConfig config = new ClientConfig(controllerEndpoints, 512, 10_000)
                    .withDataNodeConnectionsPerEndpoint(2);
            FileId fileId;
            BinaryWorkload workload = new BinaryWorkload();
            StrataClient writer = StrataClient.connect(config);
            StrataFile.Appender abandoned = null;
            try {
                fileId = writer.create(new StrataClient.FileSpec("test",
                        "/full-process-cluster-restart",
                        StrataClient.WritePolicy.fsync(3, 2))).id();
                artifact.add("fileId=" + fileId);
                abandoned = writer.openById(StrataNamespace.of("test"), fileId).openForAppend();
                workload.appendRandomBatch(abandoned, new Random(seed), 96);

                Messages.LookupFileResp beforeRestart = ConsistencyVerifier.lookupFile(controllerEndpoints, fileId);
                Messages.ChunkInfo openTail = lastChunk(beforeRestart);
                assertEquals(ChunkState.OPEN, openTail.state(), "restart scenario must leave an open tail");
                workload.verifyOpenReadIsAckedPrefix(writer, StrataNamespace.of("test"), fileId, context + " before restart");
                ConsistencyVerifier.assertLiveFileDescriptorConsistent(controllerEndpoints, fileId);
                artifact.add(
                        "ackedBytes=" + workload.ackedBytes(),
                        "ackedSha256=" + workload.ackedSha256(),
                        "chunkCountBeforeCrash=" + beforeRestart.chunks().size(),
                        "openTailBeforeCrash=" + openTail.chunkId(),
                        "liveDescriptorVerifiedBeforeCrash=true");
                artifact.addDescriptor("beforeRestartDescriptor", beforeRestart);

                killAllDataNode(dataNodes);
                killAllMetadata(metas);
                artifact.add("allProcessesKilled=true");
            } finally {
                if (abandoned != null) {
                    abandoned.close();
                }
                writer.close();
            }

            restartedMetas.add(startMetadata(zk.getConnectString(), "restart-meta-c"));
            restartedMetas.add(startMetadata(zk.getConnectString(), "restart-meta-d"));
            List<String> restartedMetadataEndpoints = controllerEndpoints(restartedMetas);
            String leader = awaitLeader(restartedMetadataEndpoints);
            artifact.add(
                    "restartedMetadataEndpoints=" + restartedMetadataEndpoints,
                    "restartedLeader=" + leader);

            for (ExternalDataNode old : dataNodes) {
                ExternalDataNode restarted = startDataNode(old.dataDir(), restartedMetadataEndpoints, old.host());
                assertEquals(old.nodeId(), restarted.nodeId(),
                        "node id changed for " + old.host() + " after full process restart");
                restartedDataNodes.add(restarted);
            }
            waitForAllReplicaEndpoints(restartedMetadataEndpoints, fileId, dataNodeMapByNodeId(restartedDataNodes));
            ConsistencyVerifier.assertLiveFileDescriptorConsistent(restartedMetadataEndpoints, fileId);
            artifact.add("restartedDataNode=" + dataNodeSummary(restartedDataNodes));
            artifact.add("liveDescriptorVerifiedAfterFullRestart=true");

            ClientConfig recoveryConfig = new ClientConfig(restartedMetadataEndpoints, 512, 10_000)
                    .withDataNodeConnectionsPerEndpoint(2);
            try (StrataClient recoveryClient = StrataClient.connect(recoveryConfig)) {
                StrataFile.SealInfo sealed = recoveryClient.openById(StrataNamespace.of("test"), fileId).recoverAndSeal();
                assertEquals(workload.ackedBytes(), sealed.sealedLength(),
                        "full process restart recovery sealed at a non-acked length");
                workload.verifySealedAckedPrefix(recoveryClient, StrataNamespace.of("test"), fileId, context);
                Messages.LookupFileResp finalDescriptor =
                        ConsistencyVerifier.assertSealedFileConsistent(restartedMetadataEndpoints, recoveryClient,
                                fileId, sealed.sealedLength());
                artifact.add("sealedLength=" + sealed.sealedLength(),
                        "finalAckedBytes=" + workload.ackedBytes(),
                        "finalAckedSha256=" + workload.ackedSha256());
                artifact.addDescriptor("finalDescriptor", finalDescriptor);
                artifact.markPassed();
                requireFullRestartArtifactEvidence(artifact);
            }
        } catch (Exception | AssertionError t) {
            artifact.addFailure(t);
            throw t;
        } finally {
            killAllDataNode(dataNodes);
            killAllDataNode(restartedDataNodes);
            killAllMetadata(metas);
            killAllMetadata(restartedMetas);
        }
    }

    private static void requireProcessStressArtifactEvidence(CorrectnessArtifact artifact) {
        artifact.requireReplayableSuccess(
                "seed",
                "batches",
                "writePolicy",
                "dataNodeConnectionsPerEndpoint",
                "initialMetadataEndpoints",
                "initialDataNode",
                "fileId",
                "leaderAvailableBeforeSeal",
                "liveDescriptorVerifiedBeforeSeal",
                "sealedLength",
                "finalAckedBytes",
                "finalAckedSha256",
                "fullReplicaConsistencyAfterRepair",
                "finalDescriptor.fileState",
                "finalDescriptor.policy",
                "finalDescriptor.chunkCount");
        artifact.requireAnyLineStartingWith(
                "batch=",
                "fault=",
                "openReadVerifiedBatch=",
                "liveDescriptorVerifiedBatch=",
                "finalDescriptor.chunk=");
        artifact.requireAnyLineContaining(" ackedBytes=", " ackedSha256=");
    }

    private static void requireFullRestartArtifactEvidence(CorrectnessArtifact artifact) {
        artifact.requireReplayableSuccess(
                "seed",
                "writePolicy",
                "initialMetadataEndpoints",
                "initialDataNode",
                "fileId",
                "ackedBytes",
                "ackedSha256",
                "allProcessesKilled",
                "restartedMetadataEndpoints",
                "restartedLeader",
                "restartedDataNode",
                "liveDescriptorVerifiedBeforeCrash",
                "liveDescriptorVerifiedAfterFullRestart",
                "sealedLength",
                "finalAckedBytes",
                "finalAckedSha256",
                "beforeRestartDescriptor.fileState",
                "beforeRestartDescriptor.policy",
                "beforeRestartDescriptor.chunkCount",
                "finalDescriptor.fileState",
                "finalDescriptor.policy",
                "finalDescriptor.chunkCount");
        artifact.requireAnyLineStartingWith("beforeRestartDescriptor.chunk=",
                "finalDescriptor.chunk=");
    }

    private static ExternalMetadata startMetadata(String zkConnect, String name) throws Exception {
        int port = freePort();
        Path logRoot = processLogRoot();
        Path readyFile = logRoot.resolve(name + "-" + System.nanoTime() + ".ready");
        Path logFile = logRoot.resolve(name + "-" + System.nanoTime() + ".log");
        Files.createFile(logFile);

        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");
        ProcessBuilder builder = new ProcessBuilder(
                java,
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
                "-cp", classpath,
                ControllerProcessMain.class.getName(),
                zkConnect,
                Integer.toString(port),
                readyFile.toString());
        builder.redirectErrorStream(true);
        builder.redirectOutput(logFile.toFile());
        Process process = builder.start();
        return waitReady(new ExternalMetadata("127.0.0.1:" + port, readyFile, logFile, process));
    }

    private static ExternalDataNode startDataNode(Path dataDir, List<String> controllerEndpoints,
                                                String host) throws Exception {
        String processName = host + "-" + System.nanoTime();
        Path logRoot = processLogRoot("process-crash-logs");
        Path readyFile = logRoot.resolve(processName + ".ready");
        Path logFile = logRoot.resolve(processName + ".log");
        Files.createFile(logFile);

        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");
        ProcessBuilder builder = new ProcessBuilder(
                java,
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
                "-cp", classpath,
                DataNodeProcessMain.class.getName(),
                dataDir.toString(),
                String.join(",", controllerEndpoints),
                host,
                readyFile.toString());
        builder.redirectErrorStream(true);
        builder.redirectOutput(logFile.toFile());
        Process process = builder.start();
        return waitReady(new ExternalDataNode(dataDir, host, readyFile, logFile, process, -1, ""));
    }

    private static ExternalMetadata waitReady(ExternalMetadata meta) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(meta.readyFile())) {
                String endpoint = Files.readString(meta.readyFile()).trim();
                if (!endpoint.isBlank()) {
                    return new ExternalMetadata(endpoint, meta.readyFile(), meta.logFile(), meta.process());
                }
            }
            if (!meta.process().isAlive()) {
                throw new AssertionError("metadata process exited early with code "
                        + meta.process().exitValue() + "\n" + childLog(meta));
            }
            Thread.sleep(50);
        }
        throw new AssertionError("metadata process did not become ready\n" + childLog(meta));
    }

    private static ExternalDataNode waitReady(ExternalDataNode node) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(node.readyFile())) {
                String[] parts = Files.readString(node.readyFile()).trim().split("\\s+");
                if (parts.length == 2) {
                    return new ExternalDataNode(node.dataDir(), node.host(), node.readyFile(), node.logFile(),
                            node.process(), Integer.parseInt(parts[0]), parts[1]);
                }
            }
            if (!node.process().isAlive()) {
                throw new AssertionError("data node process exited early with code "
                        + node.process().exitValue() + "\n" + childLog(node));
            }
            Thread.sleep(50);
        }
        throw new AssertionError("data node process did not become ready\n" + childLog(node));
    }

    private static String awaitLeader(List<String> controllerEndpoints) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        AssertionError failure = new AssertionError("no metadata process became leader");
        while (System.currentTimeMillis() < deadline) {
            for (String endpoint : controllerEndpoints) {
                String[] hp = endpoint.split(":");
                try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]),
                        ScpClient.KIND_TOOL, "metadata-process-leader-probe")) {
                    direct.call(Opcode.PING, Messages.okHeader(), null, 1_000);
                    return endpoint;
                } catch (ScpException e) {
                    if (e.code() != ErrorCode.NOT_LEADER) {
                        failure.addSuppressed(e);
                    }
                } catch (Exception e) {
                    failure.addSuppressed(e);
                }
            }
            Thread.sleep(50);
        }
        throw failure;
    }

    private static void awaitRegistered(String zkConnect, int expected) throws Exception {
        try (ZkMetadataStore store = new ZkMetadataStore(zkConnect)) {
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                if (store.listNodes().size() >= expected) {
                    return;
                }
                Thread.sleep(50);
            }
        }
        throw new AssertionError("nodes did not register in time");
    }

    private static List<String> controllerEndpoints(List<ExternalMetadata> metas) {
        return metas.stream().map(ExternalMetadata::endpoint).toList();
    }

    private static ExternalMetadata metadataByEndpoint(List<ExternalMetadata> metas, String endpoint) {
        return metas.stream()
                .filter(meta -> meta.endpoint().equals(endpoint))
                .findFirst()
                .orElseThrow(() -> new AssertionError("controller endpoint not found: " + endpoint));
    }

    private static ExternalDataNode dataNodeByNodeId(List<ExternalDataNode> nodes, int nodeId) {
        return nodes.stream()
                .filter(node -> node.nodeId() == nodeId)
                .findFirst()
                .orElseThrow(() -> new AssertionError("data node not found: " + nodeId));
    }

    private static Map<Integer, ExternalDataNode> dataNodeMapByNodeId(List<ExternalDataNode> nodes) {
        Map<Integer, ExternalDataNode> byId = new HashMap<>();
        for (ExternalDataNode node : nodes) {
            byId.put(node.nodeId(), node);
        }
        return byId;
    }

    private static int dataNodeIndexByNodeId(List<ExternalDataNode> nodes, int nodeId) {
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).nodeId() == nodeId) {
                return i;
            }
        }
        throw new AssertionError("data node not found: " + nodeId);
    }

    private static Messages.ChunkInfo lastChunk(Messages.LookupFileResp file) {
        assertTrue(!file.chunks().isEmpty(), "file has no chunks");
        return file.chunks().get(file.chunks().size() - 1);
    }

    private static void waitForAllReplicaEndpoints(List<String> controllerEndpoints, FileId fileId,
                                                   Map<Integer, ExternalDataNode> expected)
            throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            Messages.LookupFileResp lookup = ConsistencyVerifier.lookupFile(controllerEndpoints, fileId);
            boolean allRefreshed = true;
            int replicasSeen = 0;
            for (Messages.ChunkInfo chunk : lookup.chunks()) {
                for (Messages.Replica replica : chunk.replicas()) {
                    ExternalDataNode node = expected.get(replica.nodeId());
                    if (node == null || !node.endpoint().equals(replica.endpoint())) {
                        allRefreshed = false;
                        break;
                    }
                    replicasSeen++;
                }
                if (!allRefreshed) {
                    break;
                }
            }
            if (allRefreshed && replicasSeen > 0) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("replica endpoints did not refresh after full process restart");
    }

    private static String dataNodeSummary(List<ExternalDataNode> nodes) {
        List<String> parts = new ArrayList<>(nodes.size());
        for (ExternalDataNode node : nodes) {
            parts.add(node.host() + "#" + node.nodeId() + "@" + node.endpoint()
                    + " dir=" + node.dataDir());
        }
        return String.join(",", parts);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static Path processLogRoot() throws Exception {
        return processLogRoot("metadata-process-logs");
    }

    private static Path processLogRoot(String directory) throws Exception {
        Path root = Path.of("target", directory);
        Files.createDirectories(root);
        return root;
    }

    private static String childLog(ExternalMetadata meta) throws Exception {
        return Files.exists(meta.logFile()) ? Files.readString(meta.logFile()) : "";
    }

    private static String childLog(ExternalDataNode node) throws Exception {
        return Files.exists(node.logFile()) ? Files.readString(node.logFile()) : "";
    }

    private static void kill(ExternalMetadata meta) throws Exception {
        if (meta == null || !meta.process().isAlive()) {
            return;
        }
        meta.process().destroyForcibly();
        if (!meta.process().waitFor(10, TimeUnit.SECONDS)) {
            throw new AssertionError("metadata process did not exit after destroyForcibly");
        }
    }

    private static void kill(ExternalDataNode node) throws Exception {
        if (node == null || !node.process().isAlive()) {
            return;
        }
        node.process().destroyForcibly();
        if (!node.process().waitFor(10, TimeUnit.SECONDS)) {
            throw new AssertionError("data node process did not exit after destroyForcibly");
        }
    }

    private static void killAllMetadata(List<ExternalMetadata> metas) throws Exception {
        for (ExternalMetadata meta : metas) {
            kill(meta);
        }
    }

    private static void killAllDataNode(List<ExternalDataNode> nodes) throws Exception {
        for (ExternalDataNode node : nodes) {
            kill(node);
        }
    }

    private record ExternalMetadata(String endpoint, Path readyFile, Path logFile, Process process) {
    }

    private record ExternalDataNode(Path dataDir, String host, Path readyFile, Path logFile,
                                   Process process, int nodeId, String endpoint) {
    }
}
