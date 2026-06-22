package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.meta.ControllerConfig;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OS-process crash coverage for the data-node data plane. Unlike the in-process MiniCluster node
 * restarts, these tests kill child JVMs so ChunkStore recovery sees a real abrupt process stop.
 */
@Tag("chaos")
class ProcessCrashRecoveryTest {
    private static final String PROCESS_CRASH_CASE_PROPERTY = "strata.processCrash.case";
    private static final List<CrashCase> CRASH_CASES = List.of(
            new CrashCase("rf3-aq2", StrataClient.WritePolicy.replicated(3, 2), 1, 3),
            new CrashCase("rf3-aq2-fsync", StrataClient.WritePolicy.fsync(3, 2), 2, 3),
            new CrashCase("rf4-aq3", StrataClient.WritePolicy.replicated(4, 3), 2, 4));

    private MiniCluster cluster;

    @Test
    void forciblyKilledDataNodeProcessesRecoverAckedOpenChunkAcrossPolicies() throws Exception {
        for (CrashCase crashCase : selectedCrashCases()) {
            runAllOpenChunkReplicasCrashAndRecover(crashCase, 180, 1);
        }
    }

    @Test
    void singleDataNodeProcessCrashDuringAppendRollsAndSealsAckedData() throws Exception {
        List<ExternalNode> processes = new ArrayList<>();
        try (MiniCluster c = new MiniCluster(0, null, 1)) {
            cluster = c;
            try {
                for (int i = 0; i < 4; i++) {
                    processes.add(startNode("single-crash-host-" + i));
                }

                ClientConfig config = ClientConfig.of(cluster.metaEndpoint())
                        .withChunkRollBytes(384)
                        .withDataNodeConnectionsPerEndpoint(2);
                try (StrataClient client = StrataClient.connect(config)) {
                    FileId fileId = client.create(new StrataClient.FileSpec("test",
                            "/process-single-crash-roll",
                            StrataClient.WritePolicy.replicated(3, 2))).id();
                    Workload workload = new Workload();
                    try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
                        workload.appendAcked(appender, 0, 80);

                        Messages.ChunkInfo openChunk = latestChunk(fileId);
                        ExternalNode victim = byNodeId(processes).get(openChunk.replicas().get(0).nodeId());
                        assertTrue(victim != null, "missing child process for victim replica");
                        kill(victim);

                        workload.appendAcked(appender, 80, 120);
                        ConsistencyVerifier.assertLiveFileDescriptorConsistent(cluster, fileId);
                        StrataFile.SealInfo sealed = appender.seal();
                        assertEquals(workload.ackedBytes(), sealed.sealedLength(),
                                "single process crash sealed at non-acked length");
                    }

                    workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);
                    ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId,
                            workload.ackedBytes());
                }
            } finally {
                for (ExternalNode node : processes) {
                    kill(node);
                }
            }
        } finally {
            cluster = null;
        }
    }

    @Test
    void dataNodeProcessCrashThenMetadataFailoverBeforeRecoveryPreservesAckedOpenChunk() throws Exception {
        runAllOpenChunkReplicasCrashAndRecover(new CrashCase("meta-failover-rf3-aq2-fsync",
                StrataClient.WritePolicy.fsync(3, 2), 2, 3), 140, 2);
    }

    @Test
    void repairTargetProcessCrashAfterCopyBeforeCompletionIsRetried() throws Exception {
        List<ExternalNode> processes = new ArrayList<>();
        try (MiniCluster c = newCommandWindowCluster(1)) {
            cluster = c;
            try {
                for (int i = 0; i < 4; i++) {
                    processes.add(startNode("repair-copy-crash-host-" + i));
                }

                ClientConfig config = ClientConfig.of(cluster.metaEndpoint())
                        .withChunkRollBytes(1 << 16)
                        .withDataNodeConnectionsPerEndpoint(2);
                try (StrataClient client = StrataClient.connect(config)) {
                    FileId fileId = client.create(StrataClient.FileSpec.log("test",
                            "/process-repair-copy-crash")).id();
                    Workload workload = new Workload();
                    StrataFile.SealInfo sealed;
                    try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
                        workload.appendAcked(appender, 0, 180);
                        sealed = appender.seal();
                    }
                    ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId,
                            sealed.sealedLength());

                    Messages.ChunkInfo chunk = lookupFile(fileId).chunks().get(0);
                    ChunkId chunkId = chunk.chunkId();
                    ExternalNode dropped = byNodeId(processes).get(chunk.replicas().get(0).nodeId());
                    deleteChunk(dropped, chunkId);
                    waitForChunkReplicaMissing(chunkId, dropped.nodeId(), 2);

                    ExternalNode target = waitForUndescriptorCopy(processes, chunkId);
                    kill(target);
                    waitForChunkRepairedExcluding(processes, chunkId, target.nodeId(), 3);

                    workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);
                    ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId,
                            sealed.sealedLength());
                }
            } finally {
                for (ExternalNode node : processes) {
                    kill(node);
                }
            }
        } finally {
            cluster = null;
        }
    }

    @Test
    void repairCompletionLostAcrossMetadataFailoverIsRediscovered() throws Exception {
        List<ExternalNode> processes = new ArrayList<>();
        try (MiniCluster c = newCommandWindowCluster(2)) {
            cluster = c;
            try {
                for (int i = 0; i < 4; i++) {
                    processes.add(startNode("repair-failover-host-" + i));
                }

                ClientConfig config = new ClientConfig(cluster.metaEndpoints(), 1 << 16, 10_000)
                        .withDataNodeConnectionsPerEndpoint(2);
                try (StrataClient client = StrataClient.connect(config)) {
                    FileId fileId = client.create(StrataClient.FileSpec.log("test",
                            "/process-repair-failover")).id();
                    Workload workload = new Workload();
                    StrataFile.SealInfo sealed;
                    try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
                        workload.appendAcked(appender, 0, 180);
                        sealed = appender.seal();
                    }
                    ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId,
                            sealed.sealedLength());

                    Messages.ChunkInfo chunk = lookupFile(fileId).chunks().get(0);
                    ChunkId chunkId = chunk.chunkId();
                    ExternalNode dropped = byNodeId(processes).get(chunk.replicas().get(0).nodeId());
                    deleteChunk(dropped, chunkId);
                    waitForChunkReplicaMissing(chunkId, dropped.nodeId(), 2);

                    ExternalNode target = waitForUndescriptorCopy(processes, chunkId);
                    killLeader("repair failover after copy on node " + target.nodeId());
                    cluster.awaitAnyLeader();

                    waitForChunkRepaired(processes, chunkId, 3);
                    workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);
                    ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId,
                            sealed.sealedLength());
                }
            } finally {
                for (ExternalNode node : processes) {
                    kill(node);
                }
            }
        } finally {
            cluster = null;
        }
    }

    @Test
    void deleteHolderProcessCrashAfterLocalDeleteBeforeCompletionStillConverges() throws Exception {
        List<ExternalNode> processes = new ArrayList<>();
        try (MiniCluster c = newCommandWindowCluster(1)) {
            cluster = c;
            try {
                for (int i = 0; i < 3; i++) {
                    processes.add(startNode("delete-local-crash-host-" + i));
                }

                ClientConfig config = ClientConfig.of(cluster.metaEndpoint())
                        .withChunkRollBytes(1 << 16)
                        .withDataNodeConnectionsPerEndpoint(2);
                try (StrataClient client = StrataClient.connect(config)) {
                    FileId fileId = client.create(StrataClient.FileSpec.log("test",
                            "/process-delete-local-crash")).id();
                    Workload workload = new Workload();
                    StrataFile.SealInfo sealed;
                    try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
                        workload.appendAcked(appender, 0, 160);
                        sealed = appender.seal();
                    }
                    ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId,
                            sealed.sealedLength());

                    ChunkId chunkId = lookupFile(fileId).chunks().get(0).chunkId();
                    client.deleteById(StrataNamespace.of("test"), fileId);

                    ExternalNode victim = waitForAnyProcessMissingLocalCopy(processes, chunkId);
                    kill(victim);
                    waitForFileDeleted(fileId);
                    for (ExternalNode node : processes) {
                        if (node.process().isAlive()) {
                            assertFalse(processContainsChunk(node, chunkId),
                                    "live process still holds deleted chunk " + chunkId);
                        }
                    }
                }
            } finally {
                for (ExternalNode node : processes) {
                    kill(node);
                }
            }
        } finally {
            cluster = null;
        }
    }

    @Test
    void deleteCompletionLostAcrossMetadataFailoverStillConverges() throws Exception {
        List<ExternalNode> processes = new ArrayList<>();
        try (MiniCluster c = newCommandWindowCluster(2)) {
            cluster = c;
            try {
                for (int i = 0; i < 3; i++) {
                    processes.add(startNode("delete-failover-host-" + i));
                }

                ClientConfig config = new ClientConfig(cluster.metaEndpoints(), 1 << 16, 10_000)
                        .withDataNodeConnectionsPerEndpoint(2);
                try (StrataClient client = StrataClient.connect(config)) {
                    FileId fileId = client.create(StrataClient.FileSpec.log("test",
                            "/process-delete-failover")).id();
                    Workload workload = new Workload();
                    StrataFile.SealInfo sealed;
                    try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
                        workload.appendAcked(appender, 0, 160);
                        sealed = appender.seal();
                    }
                    ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId,
                            sealed.sealedLength());

                    ChunkId chunkId = lookupFile(fileId).chunks().get(0).chunkId();
                    client.deleteById(StrataNamespace.of("test"), fileId);

                    waitForAnyProcessMissingLocalCopy(processes, chunkId);
                    killLeader("delete failover after local delete");
                    cluster.awaitAnyLeader();

                    waitForFileDeleted(fileId);
                    for (ExternalNode node : processes) {
                        assertFalse(processContainsChunk(node, chunkId),
                                "process still holds deleted chunk " + chunkId);
                    }
                }
            } finally {
                for (ExternalNode node : processes) {
                    kill(node);
                }
            }
        } finally {
            cluster = null;
        }
    }

    private void runAllOpenChunkReplicasCrashAndRecover(CrashCase crashCase, int recordCount,
                                                        int metadataServiceCount) throws Exception {
        CorrectnessArtifact artifact = CorrectnessArtifact.create("process-crash-recovery", crashCase.name());
        artifact.add("scenario=process-crash-recovery",
                "case=" + crashCase.name(),
                "recordCount=" + recordCount,
                "metadataServiceCount=" + metadataServiceCount,
                "metadataFailoverDuringRecovery=" + (metadataServiceCount > 1),
                "replayCommand=" + replayCommand(crashCase, metadataServiceCount),
                "writePolicy=rf" + crashCase.writePolicy().replicationFactor()
                        + "-aq" + crashCase.writePolicy().ackQuorum()
                        + "-fsync" + crashCase.writePolicy().fsyncOnAck(),
                "dataNodeConnectionsPerEndpoint=" + crashCase.dataNodeConnectionsPerEndpoint(),
                "nodeCount=" + crashCase.nodeCount());
        List<ExternalNode> processes = new ArrayList<>();
        try {
            try (MiniCluster c = new MiniCluster(0, null, metadataServiceCount)) {
                cluster = c;
                try {
                    artifact.add("initialMetadataEndpoints=" + cluster.metaEndpoints());
                    for (int i = 0; i < crashCase.nodeCount(); i++) {
                        processes.add(startNode(crashCase.name() + "-host-" + i));
                    }
                    artifact.add("initialDataNode=" + dataNodeSummary(processes));

                    ClientConfig config = new ClientConfig(cluster.metaEndpoints(), 1 << 16, 10_000)
                            .withDataNodeConnectionsPerEndpoint(crashCase.dataNodeConnectionsPerEndpoint());
                    try (StrataClient client = StrataClient.connect(config)) {
                        FileId fileId = client.create(new StrataClient.FileSpec("test",
                                "/process-crash-" + crashCase.name(), crashCase.writePolicy())).id();
                        artifact.add("fileId=" + fileId);
                        Workload workload = new Workload();
                        StrataFile.Appender abandoned = client.openById(StrataNamespace.of("test"), fileId).openForAppend();
                        try {
                            workload.appendAcked(abandoned, 0, recordCount);
                            artifact.add("ackedBeforeCrashBytes=" + workload.ackedBytes(),
                                    "ackedBeforeCrashSha256=" + workload.ackedSha256());

                            Messages.LookupFileResp beforeCrash =
                                    ConsistencyVerifier.assertLiveFileDescriptorConsistent(cluster, fileId);
                            Messages.ChunkInfo openChunk = latestChunk(fileId);
                            assertEquals(io.strata.common.ChunkState.OPEN, openChunk.state());
                            artifact.add("liveDescriptorVerifiedBeforeCrash=true",
                                    "openChunkBeforeCrash=" + openChunk.chunkId());
                            artifact.addDescriptor("beforeCrashDescriptor", beforeCrash);
                            Map<Integer, ExternalNode> liveById = byNodeId(processes);
                            for (var replica : openChunk.replicas()) {
                                assertTrue(liveById.containsKey(replica.nodeId()),
                                        "missing child process for replica node " + replica.nodeId());
                            }

                            for (var replica : openChunk.replicas()) {
                                kill(liveById.get(replica.nodeId()));
                                artifact.add("fault=kill-open-replica-process nodeId=" + replica.nodeId()
                                        + " endpoint=" + replica.endpoint()
                                        + " openChunk=" + openChunk.chunkId());
                            }
                            artifact.add("allOpenChunkReplicasKilled=true");
                            if (metadataServiceCount > 1) {
                                killLeader(crashCase.name());
                                cluster.awaitAnyLeader();
                                artifact.add("fault=kill-metadata-leader-before-recovery",
                                        "metadataLeaderAfterFault=" + currentLeaderEndpoint());
                            }

                            Map<Integer, ExternalNode> restartedById = new HashMap<>();
                            for (var replica : openChunk.replicas()) {
                                ExternalNode old = liveById.get(replica.nodeId());
                                ExternalNode restarted = startNode(old.host(), old.dataDir());
                                processes.add(restarted);
                                assertEquals(replica.nodeId(), restarted.nodeId(),
                                        "node id changed after process crash/restart");
                                restartedById.put(restarted.nodeId(), restarted);
                                artifact.add("fault=restarted-open-replica-process nodeId="
                                        + restarted.nodeId()
                                        + " endpoint=" + restarted.endpoint()
                                        + " openChunk=" + openChunk.chunkId());
                            }
                            waitForReplicaEndpoints(fileId, restartedById);
                            ConsistencyVerifier.assertLiveFileDescriptorConsistent(cluster, fileId);
                            artifact.add("liveDescriptorVerifiedAfterRestart=true",
                                    "restartedReplicaNodeIds=" + restartedById.keySet());

                            StrataFile.SealInfo sealed = client.openById(StrataNamespace.of("test"), fileId).recoverAndSeal();
                            assertEquals(workload.ackedBytes(), sealed.sealedLength(),
                                    "recovery sealed at a non-acked length");
                            workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);
                            Messages.LookupFileResp finalDescriptor =
                                    ConsistencyVerifier.waitForFullSealedFileConsistent(cluster, client,
                                            fileId, sealed.sealedLength());
                            artifact.add("sealedLength=" + sealed.sealedLength(),
                                    "finalAckedBytes=" + workload.ackedBytes(),
                                    "finalAckedSha256=" + workload.ackedSha256(),
                                    "fullReplicaConsistencyAfterRepair=true");
                            artifact.addDescriptor("finalDescriptor", finalDescriptor);
                            artifact.markPassed();
                            requireProcessCrashArtifactEvidence(artifact);
                        } finally {
                            abandoned.close();
                        }
                    }
                } finally {
                    for (ExternalNode node : processes) {
                        kill(node);
                    }
                }
            } finally {
                cluster = null;
            }
        } catch (Exception | AssertionError t) {
            artifact.addFailure(t);
            throw t;
        }
    }

    private void killLeader(String context) throws Exception {
        int leaderIndex = -1;
        for (int i = 0; i < cluster.metas.size(); i++) {
            if (cluster.metas.get(i).isLeader()) {
                leaderIndex = i;
                break;
            }
        }
        assertTrue(leaderIndex >= 0, context + " had no controller leader to kill");
        cluster.killMeta(leaderIndex);
    }

    private String currentLeaderEndpoint() {
        for (var meta : cluster.metas) {
            if (meta.isLeader()) {
                return meta.endpoint();
            }
        }
        throw new AssertionError("no controller leader is available");
    }

    private record CrashCase(String name, StrataClient.WritePolicy writePolicy,
                             int dataNodeConnectionsPerEndpoint, int nodeCount) {
    }

    private static List<CrashCase> selectedCrashCases() {
        String filter = System.getProperty(PROCESS_CRASH_CASE_PROPERTY, "").trim();
        if (filter.isBlank() || "all".equalsIgnoreCase(filter)) {
            return CRASH_CASES;
        }
        for (CrashCase crashCase : CRASH_CASES) {
            if (crashCase.name().equals(filter)) {
                return List.of(crashCase);
            }
        }
        throw new IllegalArgumentException(PROCESS_CRASH_CASE_PROPERTY + " must be one of "
                + CRASH_CASES.stream().map(CrashCase::name).toList() + " or all: " + filter);
    }

    private static String replayCommand(CrashCase crashCase, int metadataServiceCount) {
        if (metadataServiceCount > 1) {
            return "./scripts/verify.sh --skip-default --fault"
                    + " -Dtest=ProcessCrashRecoveryTest#"
                    + "dataNodeProcessCrashThenMetadataFailoverBeforeRecoveryPreservesAckedOpenChunk";
        }
        return "./scripts/verify.sh --skip-default --fault"
                + " -Dtest=ProcessCrashRecoveryTest#"
                + "forciblyKilledDataNodeProcessesRecoverAckedOpenChunkAcrossPolicies"
                + " -D" + PROCESS_CRASH_CASE_PROPERTY + "=" + crashCase.name();
    }

    private static void requireProcessCrashArtifactEvidence(CorrectnessArtifact artifact) {
        artifact.requireReplayableSuccess(
                "case",
                "recordCount",
                "metadataServiceCount",
                "metadataFailoverDuringRecovery",
                "writePolicy",
                "dataNodeConnectionsPerEndpoint",
                "nodeCount",
                "initialMetadataEndpoints",
                "initialDataNode",
                "fileId",
                "ackedBeforeCrashBytes",
                "ackedBeforeCrashSha256",
                "allOpenChunkReplicasKilled",
                "liveDescriptorVerifiedBeforeCrash",
                "liveDescriptorVerifiedAfterRestart",
                "openChunkBeforeCrash",
                "restartedReplicaNodeIds",
                "sealedLength",
                "finalAckedBytes",
                "finalAckedSha256",
                "fullReplicaConsistencyAfterRepair",
                "beforeCrashDescriptor.fileState",
                "beforeCrashDescriptor.policy",
                "beforeCrashDescriptor.chunkCount",
                "finalDescriptor.fileState",
                "finalDescriptor.policy",
                "finalDescriptor.chunkCount");
        artifact.requireAnyLineStartingWith(
                "fault=kill-open-replica-process",
                "fault=restarted-open-replica-process",
                "beforeCrashDescriptor.chunk=",
                "finalDescriptor.chunk=");
    }

    private MiniCluster newCommandWindowCluster(int metadataServiceCount) throws Exception {
        // Short missing-replica grace: these tests delete a chunk on a live node and assert the
        // metadata drops that replica within seconds (the 90s production default would outlast the
        // missing-replica deadline). The in-process metadata can't see a static-field override.
        return new MiniCluster(0, null, metadataServiceCount,
                zkConnect -> new ControllerConfig(zkConnect, 0, 5_000, 9_000, 1_000, 250, 9_000)
                        .withReplicaMissingGraceMs(2_000));
    }

    private ExternalNode startNode(String host) throws Exception {
        return startNode(host, cluster.root.resolve(host));
    }

    private ExternalNode startNode(String host, Path dataDir) throws Exception {
        String processName = host + "-" + System.nanoTime();
        Path processLogRoot = processLogRoot();
        Path readyFile = processLogRoot.resolve(processName + ".ready");
        Path logFile = processLogRoot.resolve(processName + ".log");
        Files.createFile(logFile);
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");
        ProcessBuilder builder = new ProcessBuilder(
                java,
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
                "-cp", classpath,
                DataNodeProcessMain.class.getName(),
                dataDir.toString(),
                String.join(",", cluster.metaEndpoints()),
                host,
                readyFile.toString());
        builder.redirectErrorStream(true);
        builder.redirectOutput(logFile.toFile());
        Process process = builder.start();
        return waitReady(new ExternalNode(dataDir, host, readyFile, logFile, process, -1, ""));
    }

    private static Path processLogRoot() throws Exception {
        Path root = Path.of("target", "process-crash-logs");
        Files.createDirectories(root);
        return root;
    }

    private ExternalNode waitReady(ExternalNode node) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(node.readyFile())) {
                String[] parts = Files.readString(node.readyFile()).trim().split("\\s+");
                if (parts.length == 2) {
                    return new ExternalNode(node.dataDir(), node.host(), node.readyFile(), node.logFile(),
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

    private static String childLog(ExternalNode node) throws Exception {
        return Files.exists(node.logFile()) ? Files.readString(node.logFile()) : "";
    }

    private static Map<Integer, ExternalNode> byNodeId(List<ExternalNode> nodes) {
        Map<Integer, ExternalNode> byId = new HashMap<>();
        for (ExternalNode node : nodes) {
            if (node.process().isAlive()) {
                byId.put(node.nodeId(), node);
            }
        }
        return byId;
    }

    private static void kill(ExternalNode node) throws Exception {
        if (node == null || !node.process().isAlive()) {
            return;
        }
        node.process().destroyForcibly();
        if (!node.process().waitFor(10, TimeUnit.SECONDS)) {
            throw new AssertionError("data node process did not exit after destroyForcibly");
        }
    }

    private Messages.LookupFileResp lookupFile(FileId fileId) throws Exception {
        return ConsistencyVerifier.lookupFile(cluster, fileId);
    }

    private Messages.ChunkInfo latestChunk(FileId fileId) throws Exception {
        var chunks = lookupFile(fileId).chunks();
        return chunks.get(chunks.size() - 1);
    }

    private Messages.ChunkInfo chunkById(ChunkId chunkId) throws Exception {
        for (Messages.ChunkInfo chunk : lookupFile(chunkId.fileId()).chunks()) {
            if (chunk.chunkId().equals(chunkId)) {
                return chunk;
            }
        }
        throw new AssertionError("chunk " + chunkId + " not found in metadata");
    }

    private void waitForChunkReplicaMissing(ChunkId chunkId, int missingNodeId, int expectedReplicas)
            throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            Messages.ChunkInfo chunk = chunkById(chunkId);
            Set<Integer> replicaIds = replicaNodeIds(chunk);
            if (chunk.replicas().size() == expectedReplicas && !replicaIds.contains(missingNodeId)) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("metadata did not drop missing replica " + missingNodeId
                + " for " + chunkId);
    }

    private ExternalNode waitForUndescriptorCopy(List<ExternalNode> processes, ChunkId chunkId)
            throws Exception {
        long deadline = System.currentTimeMillis() + 45_000;
        while (System.currentTimeMillis() < deadline) {
            Set<Integer> descriptorReplicas = replicaNodeIds(chunkById(chunkId));
            for (ExternalNode node : processes) {
                if (!node.process().isAlive() || descriptorReplicas.contains(node.nodeId())) {
                    continue;
                }
                if (processContainsChunk(node, chunkId)) {
                    return node;
                }
            }
            Thread.sleep(50);
        }
        throw new AssertionError("repair copy was not observed before completion for " + chunkId);
    }

    private void waitForChunkRepaired(List<ExternalNode> processes, ChunkId chunkId, int expectedReplicas)
            throws Exception {
        waitForChunkRepairedExcluding(processes, chunkId, -1, expectedReplicas);
    }

    private void waitForChunkRepairedExcluding(List<ExternalNode> processes, ChunkId chunkId,
                                               int excludedNodeId, int expectedReplicas) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            Messages.ChunkInfo chunk = chunkById(chunkId);
            Set<Integer> replicaIds = replicaNodeIds(chunk);
            if (chunk.replicas().size() == expectedReplicas && !replicaIds.contains(excludedNodeId)) {
                Map<Integer, ExternalNode> live = byNodeId(processes);
                boolean allPresent = true;
                for (int nodeId : replicaIds) {
                    ExternalNode node = live.get(nodeId);
                    if (node == null || !processContainsChunk(node, chunkId)) {
                        allPresent = false;
                        break;
                    }
                }
                if (allPresent) {
                    return;
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("repair did not retry after process " + excludedNodeId
                + " died before command completion for " + chunkId);
    }

    private ExternalNode waitForAnyProcessMissingLocalCopy(List<ExternalNode> processes, ChunkId chunkId)
            throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            for (ExternalNode node : processes) {
                if (node.process().isAlive() && !processContainsChunk(node, chunkId)) {
                    return node;
                }
            }
            Thread.sleep(50);
        }
        throw new AssertionError("delete command did not remove a local copy before completion for "
                + chunkId);
    }

    private void waitForFileDeleted(FileId fileId) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            if (isFileDeleted(fileId)) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("file " + fileId + " did not converge to deleted");
    }

    private boolean isFileDeleted(FileId fileId) throws Exception {
        AssertionError failure = new AssertionError("no controller endpoint could confirm deletion for " + fileId);
        for (String endpoint : cluster.metaEndpoints()) {
            String[] hp = endpoint.split(":");
            try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]),
                    ScpClient.KIND_TOOL, "process-delete-lookup")) {
                direct.call(Opcode.LOOKUP_FILE, new Messages.LookupFile(StrataNamespace.of("test"), fileId).encode(), null, 5_000);
                return false;
            } catch (ScpException e) {
                if (e.code() == ErrorCode.FILE_NOT_FOUND) {
                    return true;
                }
                if (e.code() != ErrorCode.NOT_LEADER) {
                    throw e;
                }
                failure.addSuppressed(e);
            } catch (Exception e) {
                failure.addSuppressed(e);
            }
        }
        throw failure;
    }

    private static Set<Integer> replicaNodeIds(Messages.ChunkInfo chunk) {
        Set<Integer> ids = new HashSet<>();
        for (Messages.Replica replica : chunk.replicas()) {
            ids.add(replica.nodeId());
        }
        return ids;
    }

    private static String dataNodeSummary(List<ExternalNode> nodes) {
        return nodes.stream()
                .map(node -> node.host() + "#" + node.nodeId() + "@" + node.endpoint())
                .toList()
                .toString();
    }

    private void deleteChunk(ExternalNode node, ChunkId chunkId) throws Exception {
        String[] hp = node.endpoint().split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]),
                ScpClient.KIND_TOOL, "process-direct-delete")) {
            var resp = Messages.DeleteChunksResp.decode(direct.call(Opcode.DELETE_CHUNKS,
                    new Messages.DeleteChunks(List.of(chunkId)).encode(), null, 5_000));
            assertEquals(ErrorCode.OK.code, resp.codes().get(0).shortValue(),
                    "direct delete failed for " + chunkId + " on node " + node.nodeId());
        }
    }

    private boolean processContainsChunk(ExternalNode node, ChunkId chunkId) throws Exception {
        if (!node.process().isAlive()) {
            return false;
        }
        String[] hp = node.endpoint().split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]),
                ScpClient.KIND_TOOL, "process-stat")) {
            direct.call(Opcode.STAT_CHUNK, new Messages.StatChunk(chunkId).encode(), null, 5_000);
            return true;
        } catch (ScpException e) {
            if (e.code() == ErrorCode.CHUNK_NOT_FOUND) {
                return false;
            }
            throw e;
        }
    }

    private void waitForReplicaEndpoints(FileId fileId, Map<Integer, ExternalNode> expected) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            Messages.ChunkInfo chunk = latestChunk(fileId);
            boolean allRefreshed = true;
            Set<Integer> seen = new HashSet<>();
            for (var replica : chunk.replicas()) {
                ExternalNode node = expected.get(replica.nodeId());
                if (node == null) {
                    continue;
                }
                seen.add(replica.nodeId());
                if (!node.endpoint().equals(replica.endpoint())) {
                    allRefreshed = false;
                    break;
                }
            }
            if (allRefreshed && seen.containsAll(expected.keySet())) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("metadata did not refresh restarted process endpoints");
    }

    private record ExternalNode(Path dataDir, String host, Path readyFile, Path logFile,
                                Process process, int nodeId, String endpoint) {
    }
}
