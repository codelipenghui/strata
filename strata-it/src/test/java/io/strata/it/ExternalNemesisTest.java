package io.strata.it;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;
import io.strata.proto.Messages;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** External-style child-JVM nemesis schedules with Toxiproxy-owned network faults. */
@Tag("chaos")
class ExternalNemesisTest {
    private static final DockerImageName TOXIPROXY_IMAGE =
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0");
    private static final long DEFAULT_SEED = 0x3e7e_4a11_2026_0612L;
    private static final String EXTERNAL_SEED_PROPERTY = "strata.external.seed";

    @Test
    void childProcessStoragePartitionAndMetadataLeaderLossPreserveAckedBytes()
            throws Exception {
        ExternalNemesisSchedule schedule = ExternalNemesisSchedule.configured();
        Random random = new Random(schedule.seed());
        CorrectnessArtifact artifact = CorrectnessArtifact.create("external-nemesis",
                schedule.seedHex());

        try (ExternalCluster cluster = new ExternalCluster("external-nemesis")) {
            cluster.startMetadata("external-meta-a");
            cluster.startMetadata("external-meta-b");
            String initialLeader = cluster.awaitLeader();
            int storageListenPort = ExternalCluster.freePort();
            Testcontainers.exposeHostPorts(storageListenPort);
            Testcontainers.exposeHostPorts(ExternalCluster.port(initialLeader));

            try (GenericContainer<?> toxiproxy = new GenericContainer<>(TOXIPROXY_IMAGE)
                    .withExposedPorts(8474, 8666, 8667)) {
                toxiproxy.start();
                ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(),
                        toxiproxy.getMappedPort(8474));
                Proxy storageProxy = toxiproxyClient.createProxy("external-storage", "0.0.0.0:8666",
                        "host.testcontainers.internal:" + storageListenPort);
                Proxy metadataProxy = toxiproxyClient.createProxy("external-metadata-leader",
                        "0.0.0.0:8667",
                        "host.testcontainers.internal:" + ExternalCluster.port(initialLeader));

                String proxiedStorageEndpoint = "127.0.0.1:" + toxiproxy.getMappedPort(8666);
                String proxiedLeaderEndpoint = "127.0.0.1:" + toxiproxy.getMappedPort(8667);
                List<String> clientMetadataEndpoints = clientMetadataEndpoints(cluster,
                        initialLeader, proxiedLeaderEndpoint);

                artifact.add("scenario=external-nemesis",
                        "seed=" + schedule.seedHex(),
                        "replayCommand=./scripts/verify.sh --skip-default --chaos"
                                + " -Dstrata.external.seed=" + schedule.seedHex()
                                + " -Dtest=ExternalNemesisTest#"
                                + "childProcessStoragePartitionAndMetadataLeaderLossPreserveAckedBytes",
                        "schedule=" + schedule.events(),
                        "initialLeader=" + initialLeader,
                        "clientMetadataEndpoints=" + clientMetadataEndpoints,
                        "proxiedStorageEndpoint=" + proxiedStorageEndpoint,
                        "writePolicy=rf4-aq3-fsynctrue",
                        "storageConnectionsPerEndpoint=2");

                for (int i = 0; i < 3; i++) {
                    cluster.startStorage("external-host-" + i);
                }
                cluster.startStorage("external-host-proxied", storageListenPort,
                        proxiedStorageEndpoint);
                cluster.awaitRegistered(4);
                artifact.add("initialStorage=" + cluster.storageSummary());

                ClientConfig config = new ClientConfig(clientMetadataEndpoints, 1 << 20, 5_000)
                        .withStorageConnectionsPerEndpoint(2);
                try (StrataClient client = StrataClient.connect(config)) {
                    FileId fileId = client.create(new StrataClient.FileSpec("test",
                            "/external-nemesis-" + schedule.seedHex(),
                            StrataClient.WritePolicy.fsync(4, 3))).id();
                    artifact.add("fileId=" + fileId);

                    BinaryWorkload workload = new BinaryWorkload();
                    StrataFile.SealInfo sealed;
                    try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
                        appendBatch(artifact, workload, appender, random, 0,
                                schedule.warmupRecords());
                        assertOpenChunkContainsEndpoint(clientMetadataEndpoints, fileId,
                                proxiedStorageEndpoint);

                        storageProxy.toxics().timeout("storage-partition-upstream",
                                ToxicDirection.UPSTREAM, 0);
                        storageProxy.toxics().timeout("storage-partition-downstream",
                                ToxicDirection.DOWNSTREAM, 0);
                        artifact.add("fault=partition-storage endpoint=" + proxiedStorageEndpoint);

                        try {
                            appendBatch(artifact, workload, appender, random, 1,
                                    schedule.partitionedRecords());
                            workload.verifyOpenReadIsAckedPrefix(client, fileId,
                                    "external nemesis after storage partition");
                            ConsistencyVerifier.assertLiveFileDescriptorConsistent(
                                    clientMetadataEndpoints, fileId);
                            artifact.add("openReadVerifiedAfterStoragePartition=true");
                            artifact.add("liveDescriptorVerifiedAfterStoragePartition=true");

                            metadataProxy.toxics().timeout("metadata-leader-blackhole",
                                    ToxicDirection.DOWNSTREAM, 0);
                            artifact.add("fault=blackhole-metadata-leader endpoint="
                                    + initialLeader);
                            cluster.kill(cluster.metadataByEndpoint(initialLeader));
                            String newLeader = ExternalCluster.awaitLeader(cluster.metadataEndpoints());
                            assertFalse(initialLeader.equals(newLeader),
                                    "killed metadata endpoint remained leader");
                            artifact.add("fault=kill-metadata-leader oldLeader=" + initialLeader,
                                    "newLeader=" + newLeader);

                            sealed = appender.seal();
                        } finally {
                            removeToxic(storageProxy, "storage-partition-upstream");
                            removeToxic(storageProxy, "storage-partition-downstream");
                            removeToxic(metadataProxy, "metadata-leader-blackhole");
                        }
                    }

                    assertEquals(workload.ackedBytes(), sealed.sealedLength(),
                            "external nemesis sealed at a non-acked length");
                    workload.verifySealedAckedPrefix(client, fileId, "external nemesis");
                    Messages.LookupFileResp descriptor = waitForFullReplicaConsistency(
                            clientMetadataEndpoints, client, fileId, sealed.sealedLength(), 4);
                    ExternalCluster.ExternalStorage proxiedStorage = storageByEndpoint(cluster,
                            descriptor, proxiedStorageEndpoint);
                    ExternalCluster.ExternalStorage restarted = cluster.restartStorage(proxiedStorage);
                    assertEquals(proxiedStorage.nodeId(), restarted.nodeId(),
                            "healed storage node id changed after restart");
                    cluster.waitForAllReplicaEndpoints(fileId);
                    Messages.LookupFileResp afterRestart = waitForFullReplicaConsistency(
                            clientMetadataEndpoints, client, fileId, sealed.sealedLength(), 4);
                    artifact.add("sealedLength=" + sealed.sealedLength(),
                            "finalAckedBytes=" + workload.ackedBytes(),
                            "finalAckedSha256=" + workload.ackedSha256(),
                            "fullReplicaConsistencyAfterRepair=true",
                            "fault=restart-healed-storage nodeId=" + restarted.nodeId()
                                    + " endpoint=" + restarted.endpoint());
                    artifact.addDescriptor("finalDescriptor", afterRestart);
                    artifact.markPassed();
                    requireExternalNemesisArtifactEvidence(artifact);
                } catch (Exception | AssertionError t) {
                    artifact.addFailure(t);
                    throw t;
                }
            }
        } catch (Exception | AssertionError t) {
            artifact.addFailure(t);
            throw t;
        }
    }

    @Test
    void concurrentClientFilesSurviveSharedStorageAndMetadataFaults() throws Exception {
        ExternalNemesisSchedule schedule = ExternalNemesisSchedule.configured();
        Random primaryRandom = new Random(schedule.seed() ^ 0x4910_2026_0612L);
        Random secondaryRandom = new Random(schedule.seed() ^ 0x5e55_1020_2606L);
        CorrectnessArtifact artifact = CorrectnessArtifact.create("external-concurrent-nemesis",
                schedule.seedHex());

        try (ExternalCluster cluster = new ExternalCluster("external-concurrent-nemesis")) {
            cluster.startMetadata("concurrent-meta-a");
            cluster.startMetadata("concurrent-meta-b");
            String initialLeader = cluster.awaitLeader();
            int storageListenPort = ExternalCluster.freePort();
            Testcontainers.exposeHostPorts(storageListenPort);
            Testcontainers.exposeHostPorts(ExternalCluster.port(initialLeader));

            try (GenericContainer<?> toxiproxy = new GenericContainer<>(TOXIPROXY_IMAGE)
                    .withExposedPorts(8474, 8669, 8670)) {
                toxiproxy.start();
                ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(),
                        toxiproxy.getMappedPort(8474));
                Proxy storageProxy = toxiproxyClient.createProxy("concurrent-storage",
                        "0.0.0.0:8669",
                        "host.testcontainers.internal:" + storageListenPort);
                Proxy metadataProxy = toxiproxyClient.createProxy("concurrent-metadata-leader",
                        "0.0.0.0:8670",
                        "host.testcontainers.internal:" + ExternalCluster.port(initialLeader));

                String proxiedStorageEndpoint = "127.0.0.1:" + toxiproxy.getMappedPort(8669);
                String proxiedLeaderEndpoint = "127.0.0.1:" + toxiproxy.getMappedPort(8670);
                List<String> clientMetadataEndpoints = clientMetadataEndpoints(cluster,
                        initialLeader, proxiedLeaderEndpoint);

                artifact.add("scenario=external-concurrent-nemesis",
                        "seed=" + schedule.seedHex(),
                        "replayCommand=./scripts/verify.sh --skip-default --chaos"
                                + " -Dstrata.external.seed=" + schedule.seedHex()
                                + " -Dtest=ExternalNemesisTest#"
                                + "concurrentClientFilesSurviveSharedStorageAndMetadataFaults",
                        "schedule=two-client-files," + schedule.events(),
                        "initialLeader=" + initialLeader,
                        "clientMetadataEndpoints=" + clientMetadataEndpoints,
                        "proxiedStorageEndpoint=" + proxiedStorageEndpoint,
                        "writePolicy=rf4-aq3-fsynctrue",
                        "storageConnectionsPerEndpoint=2",
                        "concurrentFiles=2");

                for (int i = 0; i < 3; i++) {
                    cluster.startStorage("concurrent-host-" + i);
                }
                cluster.startStorage("concurrent-host-proxied", storageListenPort,
                        proxiedStorageEndpoint);
                cluster.awaitRegistered(4);
                artifact.add("initialStorage=" + cluster.storageSummary());

                ClientConfig config = new ClientConfig(clientMetadataEndpoints, 1 << 20, 5_000)
                        .withStorageConnectionsPerEndpoint(2);
                try (StrataClient primaryClient = StrataClient.connect(config);
                     StrataClient secondaryClient = StrataClient.connect(config)) {
                    FileId primaryFileId = primaryClient.create(new StrataClient.FileSpec("test",
                            "/external-concurrent-primary-" + schedule.seedHex(),
                            StrataClient.WritePolicy.fsync(4, 3))).id();
                    FileId secondaryFileId = secondaryClient.create(new StrataClient.FileSpec("test",
                            "/external-concurrent-secondary-" + schedule.seedHex(),
                            StrataClient.WritePolicy.fsync(4, 3))).id();
                    artifact.add("primaryFileId=" + primaryFileId,
                            "secondaryFileId=" + secondaryFileId);

                    BinaryWorkload primaryWorkload = new BinaryWorkload();
                    BinaryWorkload secondaryWorkload = new BinaryWorkload();
                    StrataFile.Appender primaryAppender =
                            primaryClient.openById(primaryFileId).openForAppend();
                    StrataFile.Appender secondaryAppender =
                            secondaryClient.openById(secondaryFileId).openForAppend();
                    boolean primarySealed = false;
                    boolean secondarySealed = false;
                    StrataFile.SealInfo primarySeal;
                    StrataFile.SealInfo secondarySeal;
                    try {
                        appendBatch(artifact, primaryWorkload, primaryAppender, primaryRandom, 0,
                                schedule.warmupRecords(), "primary");
                        appendBatch(artifact, secondaryWorkload, secondaryAppender, secondaryRandom,
                                0, schedule.warmupRecords(), "secondary");
                        assertOpenChunkContainsEndpoint(clientMetadataEndpoints, primaryFileId,
                                proxiedStorageEndpoint);
                        assertOpenChunkContainsEndpoint(clientMetadataEndpoints, secondaryFileId,
                                proxiedStorageEndpoint);
                        try (StrataFile.Reader primaryReader =
                                     secondaryClient.openById(primaryFileId).openForRead();
                             StrataFile.Reader secondaryReader =
                                     primaryClient.openById(secondaryFileId).openForRead()) {
                            primaryWorkload.verifyReaderReadIsAckedPrefix(primaryReader,
                                    "concurrent primary long-lived before partition");
                            secondaryWorkload.verifyReaderReadIsAckedPrefix(secondaryReader,
                                    "concurrent secondary long-lived before partition");
                            primaryWorkload.verifyOpenReadIsAckedPrefix(secondaryClient,
                                    primaryFileId, "concurrent primary before partition");
                            secondaryWorkload.verifyOpenReadIsAckedPrefix(primaryClient,
                                    secondaryFileId, "concurrent secondary before partition");
                            artifact.add("crossClientOpenReadVerifiedBeforePartition=true",
                                    "longLivedReaderVerifiedBeforePartition=true");

                            storageProxy.toxics().timeout("concurrent-storage-partition-upstream",
                                    ToxicDirection.UPSTREAM, 0);
                            storageProxy.toxics().timeout("concurrent-storage-partition-downstream",
                                    ToxicDirection.DOWNSTREAM, 0);
                            artifact.add("fault=partition-storage endpoint="
                                    + proxiedStorageEndpoint);

                            try {
                                appendBatch(artifact, primaryWorkload, primaryAppender,
                                        primaryRandom, 1, schedule.partitionedRecords(), "primary");
                                appendBatch(artifact, secondaryWorkload, secondaryAppender,
                                        secondaryRandom, 1, schedule.partitionedRecords(),
                                        "secondary");
                                primaryWorkload.verifyReaderReadIsAckedPrefix(primaryReader,
                                        "concurrent primary long-lived after storage partition");
                                secondaryWorkload.verifyReaderReadIsAckedPrefix(secondaryReader,
                                        "concurrent secondary long-lived after storage partition");
                                primaryWorkload.verifyOpenReadIsAckedPrefix(secondaryClient,
                                        primaryFileId,
                                        "concurrent primary after storage partition");
                                secondaryWorkload.verifyOpenReadIsAckedPrefix(primaryClient,
                                        secondaryFileId,
                                        "concurrent secondary after storage partition");
                                ConsistencyVerifier.assertLiveFileDescriptorConsistent(
                                        clientMetadataEndpoints, primaryFileId);
                                ConsistencyVerifier.assertLiveFileDescriptorConsistent(
                                        clientMetadataEndpoints, secondaryFileId);
                                artifact.add("crossClientOpenReadVerifiedAfterStoragePartition=true",
                                        "longLivedReaderVerifiedAfterStoragePartition=true",
                                        "liveDescriptorVerifiedAfterStoragePartition=true");

                                metadataProxy.toxics().timeout(
                                        "concurrent-metadata-leader-blackhole",
                                        ToxicDirection.DOWNSTREAM, 0);
                                artifact.add("fault=blackhole-metadata-leader endpoint="
                                        + initialLeader);
                                cluster.kill(cluster.metadataByEndpoint(initialLeader));
                                String newLeader = ExternalCluster.awaitLeader(
                                        cluster.metadataEndpoints());
                                assertFalse(initialLeader.equals(newLeader),
                                        "killed metadata endpoint remained leader");
                                artifact.add("fault=kill-metadata-leader oldLeader="
                                                + initialLeader,
                                        "newLeader=" + newLeader);

                                primarySeal = primaryAppender.seal();
                                primarySealed = true;
                                secondarySeal = secondaryAppender.seal();
                                secondarySealed = true;
                            } finally {
                                removeToxic(storageProxy,
                                        "concurrent-storage-partition-upstream");
                                removeToxic(storageProxy,
                                        "concurrent-storage-partition-downstream");
                                removeToxic(metadataProxy,
                                        "concurrent-metadata-leader-blackhole");
                            }
                        }
                    } finally {
                        if (!secondarySealed) {
                            secondaryAppender.close();
                        }
                        if (!primarySealed) {
                            primaryAppender.close();
                        }
                    }

                    assertEquals(primaryWorkload.ackedBytes(), primarySeal.sealedLength(),
                            "concurrent primary sealed at a non-acked length");
                    assertEquals(secondaryWorkload.ackedBytes(), secondarySeal.sealedLength(),
                            "concurrent secondary sealed at a non-acked length");
                    primaryWorkload.verifySealedAckedPrefix(secondaryClient, primaryFileId,
                            "concurrent primary");
                    secondaryWorkload.verifySealedAckedPrefix(primaryClient, secondaryFileId,
                            "concurrent secondary");
                    Messages.LookupFileResp primaryDescriptor = waitForFullReplicaConsistency(
                            clientMetadataEndpoints, primaryClient, primaryFileId,
                            primarySeal.sealedLength(), 4);
                    Messages.LookupFileResp secondaryDescriptor = waitForFullReplicaConsistency(
                            clientMetadataEndpoints, secondaryClient, secondaryFileId,
                            secondarySeal.sealedLength(), 4);

                    ExternalCluster.ExternalStorage proxiedStorage = storageByEndpoint(cluster,
                            primaryDescriptor, proxiedStorageEndpoint);
                    ExternalCluster.ExternalStorage restarted = cluster.restartStorage(proxiedStorage);
                    assertEquals(proxiedStorage.nodeId(), restarted.nodeId(),
                            "healed storage node id changed after restart");
                    cluster.waitForAllReplicaEndpoints(primaryFileId);
                    cluster.waitForAllReplicaEndpoints(secondaryFileId);
                    Messages.LookupFileResp primaryAfterRestart = waitForFullReplicaConsistency(
                            clientMetadataEndpoints, primaryClient, primaryFileId,
                            primarySeal.sealedLength(), 4);
                    Messages.LookupFileResp secondaryAfterRestart = waitForFullReplicaConsistency(
                            clientMetadataEndpoints, secondaryClient, secondaryFileId,
                            secondarySeal.sealedLength(), 4);

                    artifact.add("primarySealedLength=" + primarySeal.sealedLength(),
                            "primaryFinalAckedBytes=" + primaryWorkload.ackedBytes(),
                            "primaryFinalAckedSha256=" + primaryWorkload.ackedSha256(),
                            "primaryFullReplicaConsistencyAfterRepair=true",
                            "secondarySealedLength=" + secondarySeal.sealedLength(),
                            "secondaryFinalAckedBytes=" + secondaryWorkload.ackedBytes(),
                            "secondaryFinalAckedSha256=" + secondaryWorkload.ackedSha256(),
                            "secondaryFullReplicaConsistencyAfterRepair=true",
                            "fault=restart-healed-storage nodeId=" + restarted.nodeId()
                                    + " endpoint=" + restarted.endpoint());
                    artifact.addDescriptor("primaryFinalDescriptor", primaryAfterRestart);
                    artifact.addDescriptor("secondaryFinalDescriptor", secondaryAfterRestart);
                    artifact.markPassed();
                    requireExternalConcurrentArtifactEvidence(artifact);
                } catch (Exception | AssertionError t) {
                    artifact.addFailure(t);
                    throw t;
                }
            }
        } catch (Exception | AssertionError t) {
            artifact.addFailure(t);
            throw t;
        }
    }

    @Test
    void storageControlPartitionDelaysRepairUntilHeartbeatPathHeals() throws Exception {
        ExternalNemesisSchedule schedule = ExternalNemesisSchedule.configured();
        CorrectnessArtifact artifact = CorrectnessArtifact.create("external-control-nemesis",
                schedule.seedHex());

        try (ExternalCluster cluster = new ExternalCluster("external-control-nemesis")) {
            cluster.startMetadata("control-meta-a");
            String leader = cluster.awaitLeader();
            int leaderPort = ExternalCluster.port(leader);
            Testcontainers.exposeHostPorts(leaderPort);

            try (GenericContainer<?> toxiproxy = new GenericContainer<>(TOXIPROXY_IMAGE)
                    .withExposedPorts(8474, 8668)) {
                toxiproxy.start();
                ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(),
                        toxiproxy.getMappedPort(8474));
                Proxy controlProxy = toxiproxyClient.createProxy("storage-control", "0.0.0.0:8668",
                        "host.testcontainers.internal:" + leaderPort);
                String proxiedMetadataEndpoint = "127.0.0.1:" + toxiproxy.getMappedPort(8668);

                artifact.add("scenario=external-control-nemesis",
                        "seed=" + schedule.seedHex(),
                        "replayCommand=./scripts/verify.sh --skip-default --chaos"
                                + " -Dstrata.external.seed=" + schedule.seedHex()
                                + " -Dtest=ExternalNemesisTest#"
                                + "storageControlPartitionDelaysRepairUntilHeartbeatPathHeals",
                        "schedule=seal-rf3,start-spare-through-control-proxy,"
                                + "partition-spare-control,kill-replica,verify-readable,"
                                + "heal-control,repair-full-rf",
                        "metadataLeader=" + leader,
                        "proxiedMetadataEndpoint=" + proxiedMetadataEndpoint,
                        "writePolicy=rf3-aq2-fsynctrue",
                        "storageConnectionsPerEndpoint=2");

                for (int i = 0; i < 3; i++) {
                    cluster.startStorage("control-host-" + i);
                }
                cluster.awaitRegistered(3);
                artifact.add("initialStorage=" + cluster.storageSummary());

                ClientConfig config = new ClientConfig(cluster.metadataEndpoints(), 1 << 20, 5_000)
                        .withStorageConnectionsPerEndpoint(2);
                try (StrataClient client = StrataClient.connect(config)) {
                    FileId fileId = client.create(new StrataClient.FileSpec("test",
                            "/external-control-nemesis-" + schedule.seedHex(),
                            StrataClient.WritePolicy.fsync(3, 2))).id();
                    artifact.add("fileId=" + fileId);

                    BinaryWorkload workload = new BinaryWorkload();
                    StrataFile.SealInfo sealed;
                    try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
                        int nextRecord = workload.appendRandomBatch(appender,
                                new Random(schedule.seed()), schedule.warmupRecords());
                        artifact.add("batch=0"
                                + " appendedRecords=" + schedule.warmupRecords()
                                + " nextRecord=" + nextRecord
                                + " ackedBytes=" + workload.ackedBytes()
                                + " ackedSha256=" + workload.ackedSha256());
                        sealed = appender.seal();
                    }
                    assertEquals(workload.ackedBytes(), sealed.sealedLength(),
                            "control nemesis sealed at a non-acked length");

                    Messages.LookupFileResp initial = ConsistencyVerifier.assertSealedFileConsistent(
                            cluster.metadataEndpoints(), client, fileId, sealed.sealedLength());
                    artifact.addDescriptor("initialDescriptor", initial);

                    ExternalCluster.ExternalStorage spare = cluster.startStorage(
                            cluster.root().resolve("control-host-spare"), "control-host-spare",
                            0, null, List.of(proxiedMetadataEndpoint));
                    cluster.awaitRegistered(4);
                    artifact.add("spareNodeId=" + spare.nodeId(),
                            "spareDataEndpoint=" + spare.endpoint());

                    controlProxy.toxics().timeout("control-partition-upstream",
                            ToxicDirection.UPSTREAM, 0);
                    controlProxy.toxics().timeout("control-partition-downstream",
                            ToxicDirection.DOWNSTREAM, 0);
                    ExternalCluster.ExternalStorage victim = cluster.storageByNodeId(
                            initial.chunks().get(0).replicas().get(0).nodeId());
                    artifact.add("fault=partition-storage-control nodeId=" + spare.nodeId(),
                            "fault=kill-storage-replica nodeId=" + victim.nodeId()
                                    + " endpoint=" + victim.endpoint());
                    cluster.kill(victim);

                    Messages.LookupFileResp duringPartition = waitForReadableButNotFullRf(
                            cluster.metadataEndpoints(), client, fileId, sealed.sealedLength(), 3);
                    workload.verifySealedAckedPrefix(client, fileId,
                            "external control nemesis during control partition");
                    artifact.add("duringControlPartitionUnderReplicated=true");
                    artifact.addDescriptor("duringControlPartition", duringPartition);

                    removeToxic(controlProxy, "control-partition-upstream");
                    removeToxic(controlProxy, "control-partition-downstream");
                    Messages.LookupFileResp finalDescriptor = waitForFullReplicaConsistency(
                            cluster.metadataEndpoints(), client, fileId, sealed.sealedLength(), 3);
                    assertTrue(finalDescriptor.chunks().stream()
                                    .flatMap(chunk -> chunk.replicas().stream())
                                    .anyMatch(replica -> replica.nodeId() == spare.nodeId()),
                            "repaired descriptor did not include the healed control-path spare");
                    workload.verifySealedAckedPrefix(client, fileId, "external control nemesis");
                    artifact.add("sealedLength=" + sealed.sealedLength(),
                            "finalAckedBytes=" + workload.ackedBytes(),
                            "finalAckedSha256=" + workload.ackedSha256(),
                            "fullReplicaConsistencyAfterRepair=true");
                    artifact.addDescriptor("finalDescriptor", finalDescriptor);
                    artifact.markPassed();
                    requireExternalControlArtifactEvidence(artifact);
                } catch (Exception | AssertionError t) {
                    artifact.addFailure(t);
                    throw t;
                } finally {
                    removeToxic(controlProxy, "control-partition-upstream");
                    removeToxic(controlProxy, "control-partition-downstream");
                }
            }
        } catch (Exception | AssertionError t) {
            artifact.addFailure(t);
            throw t;
        }
    }

    private static void requireExternalNemesisArtifactEvidence(CorrectnessArtifact artifact) {
        artifact.requireReplayableSuccess(
                "seed",
                "schedule",
                "initialLeader",
                "clientMetadataEndpoints",
                "proxiedStorageEndpoint",
                "writePolicy",
                "storageConnectionsPerEndpoint",
                "initialStorage",
                "fileId",
                "openReadVerifiedAfterStoragePartition",
                "liveDescriptorVerifiedAfterStoragePartition",
                "sealedLength",
                "finalAckedBytes",
                "finalAckedSha256",
                "fullReplicaConsistencyAfterRepair",
                "finalDescriptor.fileState",
                "finalDescriptor.policy",
                "finalDescriptor.chunkCount");
        artifact.requireAnyLineStartingWith("batch=", "fault=", "finalDescriptor.chunk=");
        artifact.requireAnyLineContaining(" ackedBytes=", " ackedSha256=");
    }

    private static void requireExternalConcurrentArtifactEvidence(CorrectnessArtifact artifact) {
        artifact.requireReplayableSuccess(
                "seed",
                "schedule",
                "initialLeader",
                "clientMetadataEndpoints",
                "proxiedStorageEndpoint",
                "writePolicy",
                "storageConnectionsPerEndpoint",
                "concurrentFiles",
                "initialStorage",
                "primaryFileId",
                "secondaryFileId",
                "crossClientOpenReadVerifiedBeforePartition",
                "longLivedReaderVerifiedBeforePartition",
                "crossClientOpenReadVerifiedAfterStoragePartition",
                "longLivedReaderVerifiedAfterStoragePartition",
                "liveDescriptorVerifiedAfterStoragePartition",
                "primarySealedLength",
                "primaryFinalAckedBytes",
                "primaryFinalAckedSha256",
                "primaryFullReplicaConsistencyAfterRepair",
                "secondarySealedLength",
                "secondaryFinalAckedBytes",
                "secondaryFinalAckedSha256",
                "secondaryFullReplicaConsistencyAfterRepair",
                "primaryFinalDescriptor.fileState",
                "primaryFinalDescriptor.policy",
                "primaryFinalDescriptor.chunkCount",
                "secondaryFinalDescriptor.fileState",
                "secondaryFinalDescriptor.policy",
                "secondaryFinalDescriptor.chunkCount");
        artifact.requireAnyLineStartingWith("batch=", "fault=", "primaryFinalDescriptor.chunk=",
                "secondaryFinalDescriptor.chunk=");
        artifact.requireAnyLineContaining(" ackedBytes=", " ackedSha256=");
    }

    private static void requireExternalControlArtifactEvidence(CorrectnessArtifact artifact) {
        artifact.requireReplayableSuccess(
                "seed",
                "schedule",
                "metadataLeader",
                "proxiedMetadataEndpoint",
                "writePolicy",
                "storageConnectionsPerEndpoint",
                "initialStorage",
                "fileId",
                "spareNodeId",
                "spareDataEndpoint",
                "sealedLength",
                "finalAckedBytes",
                "finalAckedSha256",
                "initialDescriptor.fileState",
                "initialDescriptor.policy",
                "initialDescriptor.chunkCount",
                "duringControlPartitionUnderReplicated",
                "duringControlPartition.fileState",
                "duringControlPartition.policy",
                "duringControlPartition.chunkCount",
                "fullReplicaConsistencyAfterRepair",
                "finalDescriptor.fileState",
                "finalDescriptor.policy",
                "finalDescriptor.chunkCount");
        artifact.requireAnyLineStartingWith("batch=", "fault=", "initialDescriptor.chunk=",
                "duringControlPartition.chunk=", "finalDescriptor.chunk=");
        artifact.requireAnyLineContaining(" ackedBytes=", " ackedSha256=");
    }

    private record ExternalNemesisSchedule(long seed, int warmupRecords, int partitionedRecords) {
        static ExternalNemesisSchedule configured() {
            return new ExternalNemesisSchedule(configuredSeed(), 8, 4);
        }

        String seedHex() {
            return Long.toUnsignedString(seed, 16);
        }

        String events() {
            return "append-warmup,partition-storage,append-partitioned,open-read,"
                    + "blackhole-metadata-leader,kill-metadata-leader,heal-storage,"
                    + "seal,repair-full-rf,restart-healed-storage,verify-full-rf";
        }
    }

    private static long configuredSeed() {
        String value = System.getProperty(EXTERNAL_SEED_PROPERTY);
        if (value == null || value.isBlank()) {
            return DEFAULT_SEED;
        }
        return StressFaultTest.parseSeed(value);
    }

    private static List<String> clientMetadataEndpoints(ExternalCluster cluster,
                                                        String proxiedLeaderTarget,
                                                        String proxiedLeaderEndpoint) {
        List<String> endpoints = new ArrayList<>();
        endpoints.add(proxiedLeaderEndpoint);
        endpoints.addAll(cluster.metadataEndpoints().stream()
                .filter(endpoint -> !endpoint.equals(proxiedLeaderTarget))
                .toList());
        return endpoints;
    }

    private static void appendBatch(CorrectnessArtifact artifact, BinaryWorkload workload,
                                    StrataFile.Appender appender, Random random, int batch,
                                    int records) throws Exception {
        appendBatch(artifact, workload, appender, random, batch, records, "");
    }

    private static void appendBatch(CorrectnessArtifact artifact, BinaryWorkload workload,
                                    StrataFile.Appender appender, Random random, int batch,
                                    int records, String fileLabel) throws Exception {
        int nextRecord = workload.appendRandomBatch(appender, random, records);
        String file = fileLabel == null || fileLabel.isBlank() ? "" : " file=" + fileLabel;
        artifact.add("batch=" + batch
                + file
                + " appendedRecords=" + records
                + " nextRecord=" + nextRecord
                + " ackedBytes=" + workload.ackedBytes()
                + " ackedSha256=" + workload.ackedSha256());
    }

    private static void assertOpenChunkContainsEndpoint(List<String> metadataEndpoints,
                                                        FileId fileId,
                                                        String endpoint) throws Exception {
        Messages.LookupFileResp file = ConsistencyVerifier.lookupFile(metadataEndpoints, fileId);
        Messages.ChunkInfo tail = ExternalCluster.lastChunk(file);
        assertTrue(tail.replicas().stream().anyMatch(r -> r.endpoint().equals(endpoint)),
                "test setup requires the proxied storage endpoint in the open replica set");
    }

    private static ExternalCluster.ExternalStorage storageByEndpoint(ExternalCluster cluster,
                                                                     Messages.LookupFileResp file,
                                                                     String endpoint) {
        for (Messages.ChunkInfo chunk : file.chunks()) {
            for (Messages.Replica replica : chunk.replicas()) {
                if (replica.endpoint().equals(endpoint)) {
                    return cluster.storageByNodeId(replica.nodeId());
                }
            }
        }
        throw new AssertionError("descriptor does not contain storage endpoint " + endpoint);
    }

    private static Messages.LookupFileResp waitForFullReplicaConsistency(
            List<String> metadataEndpoints, StrataClient client, FileId fileId,
            long sealedLength, int replicationFactor) throws Exception {
        long deadline = System.currentTimeMillis() + 45_000;
        AssertionError last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                Messages.LookupFileResp lookup = ConsistencyVerifier.assertSealedFileConsistent(
                        metadataEndpoints, client, fileId, sealedLength);
                boolean fullRf = lookup.chunks().stream()
                        .allMatch(chunk -> ConsistencyVerifier.readableSealedReplicaCount(chunk)
                                == replicationFactor);
                if (fullRf) {
                    return lookup;
                }
                last = new AssertionError("sealed file has not repaired to RF="
                        + replicationFactor + ": " + lookup.chunks());
            } catch (AssertionError e) {
                last = e;
            }
            Thread.sleep(250);
        }
        throw last != null ? last
                : new AssertionError("file did not repair to RF=" + replicationFactor);
    }

    private static Messages.LookupFileResp waitForReadableButNotFullRf(
            List<String> metadataEndpoints, StrataClient client, FileId fileId,
            long sealedLength, int replicationFactor) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        AssertionError last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                Messages.LookupFileResp lookup = ConsistencyVerifier.assertSealedFileConsistent(
                        metadataEndpoints, client, fileId, sealedLength);
                boolean belowRf = lookup.chunks().stream()
                        .anyMatch(chunk -> ConsistencyVerifier.readableSealedReplicaCount(chunk)
                                < replicationFactor);
                if (belowRf) {
                    return lookup;
                }
                last = new AssertionError("sealed file has not dropped below RF="
                        + replicationFactor + ": " + lookup.chunks());
            } catch (AssertionError e) {
                last = e;
            }
            Thread.sleep(250);
        }
        throw last != null ? last
                : new AssertionError("file did not become under-replicated");
    }

    private static void removeToxic(Proxy proxy, String name) {
        try {
            proxy.toxics().get(name).remove();
        } catch (Exception ignored) {
        }
    }
}
