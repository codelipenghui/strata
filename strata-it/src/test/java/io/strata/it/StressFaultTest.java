package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.proto.Messages;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Longer deterministic stress/fault checks. These are opt-in because they exercise several
 * component failures in one run and are intended for pre-merge/soak verification.
 *
 * Exact artifact replay:
 * -Dstrata.stress.case=<case> -Dstrata.stress.seed=<seed> -Dstrata.stress.batches=<batches>
 */
@Tag("chaos")
class StressFaultTest {
    private static final long DEFAULT_BASE_SEED = 0x51a7a_2026_0612L;
    private static final int DEFAULT_BATCHES = 30;
    private static final long CASE_SEED_STEP = 0x517c_c1b7_2722_0a95L;
    private static final String STRESS_SEED_PROPERTY = "strata.stress.seed";
    private static final String STRESS_BATCHES_PROPERTY = "strata.stress.batches";
    private static final String STRESS_CASE_PROPERTY = "strata.stress.case";
    private static final List<FaultCase> FAULT_CASES = List.of(
            new FaultCase("rf3-aq2-single", StrataClient.WritePolicy.replicated(3, 2), 1, 5),
            new FaultCase("rf3-aq2-pooled", StrataClient.WritePolicy.replicated(3, 2), 3, 5),
            new FaultCase("rf3-aq2-fsync", StrataClient.WritePolicy.fsync(3, 2), 3, 5),
            new FaultCase("rf4-aq3-pooled", StrataClient.WritePolicy.replicated(4, 3), 2, 5),
            new FaultCase("rf4-aq3-fsync", StrataClient.WritePolicy.fsync(4, 3), 2, 5));

    @Test
    void policyMatrixMixedReplicaAndMetadataFaultsPreserveAckedPrefix() throws Exception {
        runPolicyMatrix(configuredBaseSeed(), configuredBatches());
    }

    static void runPolicyMatrix(long baseSeed, int batches) throws Exception {
        String caseFilter = configuredCaseFilter();
        for (FaultRun run : selectedRuns(baseSeed, caseFilter)) {
            FaultCase faultCase = run.faultCase();
            long seed = run.seed();
            try {
                runMixedReplicaAndMetadataFaults(seed, batches, faultCase);
            } catch (Throwable t) {
                throw new AssertionError("stress/fault case failed: case=" + faultCase.name()
                        + " seed=" + Long.toUnsignedString(seed, 16)
                        + " batches=" + batches
                        + " writePolicy=" + faultCase.writePolicy()
                        + " dataNodeConnectionsPerEndpoint="
                        + faultCase.dataNodeConnectionsPerEndpoint()
                        + " nodeCount=" + faultCase.nodeCount(), t);
            }
        }
    }

    private static long configuredBaseSeed() {
        String value = System.getProperty(STRESS_SEED_PROPERTY);
        if (value == null || value.isBlank()) {
            return DEFAULT_BASE_SEED;
        }
        return parseSeed(value);
    }

    private static int configuredBatches() {
        int batches = Integer.getInteger(STRESS_BATCHES_PROPERTY, DEFAULT_BATCHES);
        if (batches < 15) {
            throw new IllegalArgumentException(STRESS_BATCHES_PROPERTY + " must be at least 15");
        }
        return batches;
    }

    private static String configuredCaseFilter() {
        String value = System.getProperty(STRESS_CASE_PROPERTY);
        return value == null ? "" : value.trim();
    }

    private static List<FaultRun> selectedRuns(long baseSeed, String caseFilter) {
        if (caseFilter == null || caseFilter.isBlank() || "all".equalsIgnoreCase(caseFilter)) {
            return IntStream.range(0, FAULT_CASES.size())
                    .mapToObj(i -> new FaultRun(FAULT_CASES.get(i), baseSeed + i * CASE_SEED_STEP))
                    .toList();
        }
        for (FaultCase faultCase : FAULT_CASES) {
            if (faultCase.name().equals(caseFilter)) {
                return List.of(new FaultRun(faultCase, baseSeed));
            }
        }
        throw new IllegalArgumentException(STRESS_CASE_PROPERTY + " must be one of "
                + FAULT_CASES.stream().map(FaultCase::name).toList()
                + " or all: " + caseFilter);
    }

    static long parseSeed(String raw) {
        String value = raw.trim().replace("_", "");
        try {
            if (value.startsWith("0x") || value.startsWith("0X")) {
                return Long.parseUnsignedLong(value.substring(2), 16);
            }
            if (value.startsWith("#")) {
                return Long.parseUnsignedLong(value.substring(1), 16);
            }
            if (containsHexLetter(value)) {
                return Long.parseUnsignedLong(value, 16);
            }
            try {
                return Long.decode(value);
            } catch (NumberFormatException e) {
                return Long.parseUnsignedLong(value, 10);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("seed must be decimal, 0x-prefixed hex,"
                    + " or the unprefixed hex value printed by the stress/fault artifact: " + raw, e);
        }
    }

    private static boolean containsHexLetter(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if ((c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
                return true;
            }
        }
        return false;
    }

    static void runMixedReplicaAndMetadataFaults(long seed, int batches) throws Exception {
        runMixedReplicaAndMetadataFaults(seed, batches, FAULT_CASES.get(0));
    }

    private static void runMixedReplicaAndMetadataFaults(long seed, int batches, FaultCase faultCase)
            throws Exception {
        Random random = new Random(seed);
        String seedHex = Long.toUnsignedString(seed, 16);
        String context = "case " + faultCase.name() + " seed " + seedHex;
        CorrectnessArtifact artifact = CorrectnessArtifact.create("stress-fault",
                faultCase.name() + "-" + seedHex);
        artifact.add("scenario=stress-fault",
                "case=" + faultCase.name(),
                "seed=" + seedHex,
                "batches=" + batches,
                "replayCommand=./scripts/verify.sh --skip-default --fault"
                        + " -Dstrata.stress.case=" + faultCase.name()
                        + " -Dstrata.stress.seed=" + seedHex
                        + " -Dstrata.stress.batches=" + batches,
                "writePolicy=rf" + faultCase.writePolicy().replicationFactor()
                        + "-aq" + faultCase.writePolicy().ackQuorum()
                        + "-fsync" + faultCase.writePolicy().fsyncOnAck(),
                "dataNodeConnectionsPerEndpoint=" + faultCase.dataNodeConnectionsPerEndpoint(),
                "nodeCount=" + faultCase.nodeCount());
        try {
            try (MiniCluster cluster = new MiniCluster(faultCase.nodeCount(), null, 2)) {
                artifact.add("initialMetadataEndpoints=" + cluster.metaEndpoints(),
                        "initialDataNode=" + dataNodeSummary(cluster));
                ClientConfig config = new ClientConfig(cluster.metaEndpoints(), 768, 10_000)
                        .withDataNodeConnectionsPerEndpoint(faultCase.dataNodeConnectionsPerEndpoint());
                try (StrataClient client = StrataClient.connect(config)) {
                    FileId fileId = client.create(new StrataClient.FileSpec("test",
                            "/mixed-chaos-" + faultCase.name(), faultCase.writePolicy())).id();
                    artifact.add("fileId=" + fileId);
                    BinaryWorkload workload = new BinaryWorkload();
                    StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend();
                    boolean appenderClosed = false;

                    try {
                        int killedNodeIndex = -1;
                        int killedNodeId = -1;
                        for (int batch = 0; batch < batches; batch++) {
                            int batchRecords = 2 + random.nextInt(6);
                            int nextRecord = workload.appendRandomBatch(appender, random, batchRecords);
                            artifact.add("batch=" + batch
                                    + " appendedRecords=" + batchRecords
                                    + " nextRecord=" + nextRecord
                                    + " ackedBytes=" + workload.ackedBytes()
                                    + " ackedSha256=" + workload.ackedSha256());

                            if (batch == 4) {
                                var openChunk = lastChunk(ConsistencyVerifier.lookupFile(cluster, fileId));
                                int victimReplica = random.nextInt(openChunk.replicas().size());
                                killedNodeId = openChunk.replicas().get(victimReplica).nodeId();
                                killedNodeIndex = nodeIndex(cluster, killedNodeId);
                                artifact.add("fault=kill-data-node batch=" + batch
                                        + " nodeIndex=" + killedNodeIndex
                                        + " nodeId=" + killedNodeId
                                        + " endpoint=" + cluster.nodes.get(killedNodeIndex).endpoint()
                                        + " openChunk=" + openChunk.chunkId()
                                        + " openChunkState=" + openChunk.state());
                                cluster.killNode(killedNodeIndex);
                            }

                            if (batch == 9) {
                                int leaderIndex = leaderIndex(cluster);
                                assertTrue(leaderIndex >= 0, context + " had no controller leader to kill");
                                artifact.add("fault=kill-metadata-leader batch=" + batch
                                        + " leaderIndex=" + leaderIndex
                                        + " endpoint=" + cluster.metas.get(leaderIndex).endpoint());
                                cluster.killMeta(leaderIndex);
                            }

                            if (batch == 14 && killedNodeIndex >= 0) {
                                var restarted = cluster.restartNode(killedNodeIndex);
                                long deadline = System.currentTimeMillis() + 10_000;
                                while (restarted.nodeId() != killedNodeId
                                        && System.currentTimeMillis() < deadline) {
                                    Thread.sleep(50);
                                }
                                assertEquals(killedNodeId, restarted.nodeId(),
                                        context + " data node identity changed after restart");
                                artifact.add("fault=restarted-data-node batch=" + batch
                                        + " nodeIndex=" + killedNodeIndex
                                        + " nodeId=" + restarted.nodeId()
                                        + " endpoint=" + restarted.endpoint());
                            }

                            if (batch % 3 == 2) {
                                workload.verifyOpenReadIsAckedPrefix(client, StrataNamespace.of("test"), fileId,
                                        context + " batch " + batch);
                                ConsistencyVerifier.assertLiveFileDescriptorConsistent(cluster, fileId);
                                artifact.add("openReadVerifiedBatch=" + batch);
                                artifact.add("liveDescriptorVerifiedBatch=" + batch);
                            }
                        }

                        cluster.awaitAnyLeader();
                        artifact.add("leaderAvailableBeforeSeal=true");
                        ConsistencyVerifier.assertLiveFileDescriptorConsistent(cluster, fileId);
                        artifact.add("liveDescriptorVerifiedBeforeSeal=true");
                        StrataFile.SealInfo sealed = appender.seal();
                        appenderClosed = true;
                        assertEquals(workload.ackedBytes(), sealed.sealedLength(),
                                context + " appender sealed at a non-acked length");

                        workload.verifySealedAckedPrefix(client, StrataNamespace.of("test"), fileId, context);
                        Messages.LookupFileResp finalDescriptor =
                                ConsistencyVerifier.waitForFullSealedFileConsistent(cluster, client,
                                        fileId, sealed.sealedLength());
                        artifact.add("sealedLength=" + sealed.sealedLength(),
                                "finalAckedBytes=" + workload.ackedBytes(),
                                "finalAckedSha256=" + workload.ackedSha256(),
                                "fullReplicaConsistencyAfterRepair=true");
                        artifact.addDescriptor("finalDescriptor", finalDescriptor);
                        artifact.markPassed();
                        requireStressFaultArtifactEvidence(artifact);
                    } catch (Exception | AssertionError t) {
                        artifact.addFailure(t);
                        throw t;
                    } finally {
                        if (!appenderClosed) {
                            appender.close();
                        }
                    }
                }
            }
        } catch (Exception | AssertionError t) {
            artifact.addFailure(t);
            throw t;
        }
    }

    private record FaultCase(String name, StrataClient.WritePolicy writePolicy,
                             int dataNodeConnectionsPerEndpoint, int nodeCount) {
    }

    private record FaultRun(FaultCase faultCase, long seed) {
    }

    private static Messages.ChunkInfo lastChunk(Messages.LookupFileResp file) {
        assertTrue(!file.chunks().isEmpty(), "file has no chunk");
        return file.chunks().get(file.chunks().size() - 1);
    }

    private static int leaderIndex(MiniCluster cluster) {
        for (int i = 0; i < cluster.metas.size(); i++) {
            try {
                if (cluster.metas.get(i).isLeader()) {
                    return i;
                }
            } catch (Exception ignored) {
            }
        }
        return -1;
    }

    private static int nodeIndex(MiniCluster cluster, int nodeId) {
        for (int i = 0; i < cluster.nodes.size(); i++) {
            if (cluster.nodes.get(i).nodeId() == nodeId) {
                return i;
            }
        }
        throw new AssertionError("node " + nodeId + " not found");
    }

    private static void requireStressFaultArtifactEvidence(CorrectnessArtifact artifact) {
        artifact.requireReplayableSuccess(
                "case",
                "seed",
                "batches",
                "writePolicy",
                "dataNodeConnectionsPerEndpoint",
                "nodeCount",
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

    private static String dataNodeSummary(MiniCluster cluster) {
        return cluster.nodes.stream()
                .map(node -> node.config().host() + "#" + node.nodeId() + "@" + node.endpoint())
                .toList()
                .toString();
    }
}
