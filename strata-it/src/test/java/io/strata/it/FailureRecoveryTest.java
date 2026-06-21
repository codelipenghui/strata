package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.ScpException;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic failure injection (tech design §16): kill a replica mid-write (seal-and-roll),
 * kill the writer (seal recovery §7.3), fencing of zombie writers. The invariant under test is
 * always the same: NO ACKED BYTE IS EVER LOST, and no fenced writer can ack again.
 */
class FailureRecoveryTest {

    private MiniCluster cluster;
    private StrataClient client;

    @BeforeEach
    void setup() throws Exception {
        cluster = new MiniCluster(4); // one spare so rolls/repair can re-place
        client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint())
                .withChunkRollBytes(1 << 16)
                .withDataNodeConnectionsPerEndpoint(3));
    }

    @AfterEach
    void teardown() throws Exception {
        if (client != null) client.close();
        if (cluster != null) cluster.close();
    }

    @Test
    void replicaDeathDuringWritesTriggersSealAndRollWithoutAckedLoss() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/kill-replica")).id();
        Workload workload = new Workload();

        try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
            workload.appendAcked(appender, 0, 200);

            // find a data node hosting the current open chunk and kill it
            var lookup = lookupFile(fileId);
            var openChunk = lookup.chunks().get(lookup.chunks().size() - 1);
            int victimNodeId = openChunk.replicas().get(0).nodeId();
            int victimIndex = -1;
            for (int i = 0; i < cluster.nodes.size(); i++) {
                if (cluster.nodes.get(i).nodeId() == victimNodeId) victimIndex = i;
            }
            assertTrue(victimIndex >= 0, "victim node not found");
            cluster.killNode(victimIndex);

            // keep writing through the failure: appender must seal-and-roll and keep acking
            workload.appendAcked(appender, 200, 300);
            var sealed = appender.seal();
            assertEquals(workload.ackedBytes(), sealed.sealedLength());
        }

        workload.verifyAckedPrefix(client, fileId);
        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, workload.ackedBytes());
    }

    @Test
    void pooledConnectionsRollingReplicaFailureThenRecoveryPreservesAckedPrefix() throws Exception {
        ClientConfig config = ClientConfig.of(cluster.metaEndpoint())
                .withChunkRollBytes(512)
                .withDataNodeConnectionsPerEndpoint(3);
        try (StrataClient pooled = StrataClient.connect(config)) {
            FileId fileId = pooled.create(StrataClient.FileSpec.log("test", "/pooled-roll-fault-recover")).id();
            Workload workload = new Workload();
            StrataFile.Appender zombie = pooled.openById(fileId).openForAppend();

            workload.appendAcked(zombie, 0, 75);
            var lookup = lookupFile(fileId);
            var openChunk = lookup.chunks().get(lookup.chunks().size() - 1);
            int victimIndex = nodeIndex(openChunk.replicas().get(0).nodeId());
            cluster.killNode(victimIndex);

            workload.appendAcked(zombie, 75, 100);

            try (StrataFile.Reader reader = pooled.openById(fileId).openForRead()) {
                try (StrataFile.ReadResult tail = reader.read(0, 1 << 20)) {
                    assertTrue(tail.length() <= workload.ackedBytes(),
                            "open read exposed " + tail.length() + " bytes above acked "
                                    + workload.ackedBytes());
                }
            }

            StrataFile.SealInfo sealed = pooled.openById(fileId).recoverAndSeal();
            assertTrue(sealed.sealedLength() >= workload.ackedBytes(),
                    "recovery sealed " + sealed.sealedLength() + " < acked " + workload.ackedBytes());
            zombie.close();

            workload.verifyAckedPrefix(pooled, fileId);
            ConsistencyVerifier.assertSealedFileConsistent(cluster, pooled, fileId, sealed.sealedLength());
        }
    }

    @Test
    void fixedSeedPooledStressWithReplicaFaultAndRecoveryPreservesAckedPrefix() throws Exception {
        long seed = 0x5eed_5eed_2026_0612L;
        Random random = new Random(seed);
        ClientConfig config = ClientConfig.of(cluster.metaEndpoint())
                .withChunkRollBytes(384)
                .withDataNodeConnectionsPerEndpoint(3);
        try (StrataClient pooled = StrataClient.connect(config)) {
            FileId fileId = pooled.create(StrataClient.FileSpec.log("test", "/pooled-random-stress")).id();
            BinaryWorkload workload = new BinaryWorkload();
            StrataFile.Appender appender = pooled.openById(fileId).openForAppend();
            boolean appenderClosed = false;
            try {
                int faultBatch = 2 + random.nextInt(3);
                boolean replicaKilled = false;

                for (int batch = 0; batch < 12; batch++) {
                    int batchSize = 1 + random.nextInt(5);
                    workload.appendRandomBatch(appender, random, batchSize);

                    if (!replicaKilled && batch == faultBatch) {
                        var lookup = lookupFile(fileId);
                        var openChunk = lookup.chunks().get(lookup.chunks().size() - 1);
                        int victimReplica = random.nextInt(openChunk.replicas().size());
                        cluster.killNode(nodeIndex(openChunk.replicas().get(victimReplica).nodeId()));
                        replicaKilled = true;
                    }

                    if ((batch & 1) == 1) {
                        workload.verifyOpenReadIsAckedPrefix(pooled, fileId,
                                "seed " + seed + " batch " + batch);
                    }
                }

                StrataFile.SealInfo sealed = pooled.openById(fileId).recoverAndSeal();
                assertTrue(sealed.sealedLength() >= workload.ackedBytes(),
                        "seed " + seed + " recovery sealed " + sealed.sealedLength()
                                + " < acked " + workload.ackedBytes());
                appender.close();
                appenderClosed = true;

                workload.verifySealedAckedPrefix(pooled, fileId, "seed " + seed);
                ConsistencyVerifier.assertSealedFileConsistent(cluster, pooled, fileId, sealed.sealedLength());
            } finally {
                if (!appenderClosed) {
                    appender.close();
                }
            }
        }
    }

    @Test
    void openReaderDoesNotExposeSingleReplicaUncommittedTail() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/dirty-open-tail")).id();
        byte[] acked = "acked-prefix".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] dirtyTail = "never-acked-tail".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        StrataFile.Appender appender = client.openById(fileId).openForAppend();
        try {
            var ack = appender.append(ByteBuffer.wrap(acked)).join();
            assertEquals(acked.length, ack.endOffset());
            assertEquals(acked.length, ack.durableOffset());

            Messages.ChunkInfo openChunk = latestChunk(fileId);
            assertEquals(ChunkState.OPEN, openChunk.state());
            Messages.Replica dirtyReplica = openChunk.replicas().get(0);
            appendOnlyToReplica(dirtyReplica, openChunk, acked.length, acked.length, dirtyTail);

            for (Messages.Replica replica : openChunk.replicas()) {
                if (replica.nodeId() != dirtyReplica.nodeId()) {
                    cluster.killNode(nodeIndex(replica.nodeId()));
                }
            }

            try (StrataFile.Reader reader = client.openById(fileId).openForRead()) {
                try (StrataFile.ReadResult full = reader.read(0, 1 << 20)) {
                    byte[] got = new byte[full.length()];
                    full.buffer().get(got);
                    assertArrayEquals(acked, got,
                            "open read exposed bytes that were only present on one replica");
                }
                try (StrataFile.ReadResult tail = reader.read(acked.length, 1024)) {
                    assertEquals(0, tail.length(),
                            "open read exposed the single-replica tail past durable offset");
                }
            }
        } finally {
            appender.close();
        }
    }

    @Test
    void sealRecoveryDoesNotCommitSingleReplicaUncommittedTail() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/recover-dirty-tail")).id();
        byte[] acked = "acked-prefix".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] dirtyTail = "never-acked-tail".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        StrataFile.Appender appender = client.openById(fileId).openForAppend();
        try {
            var ack = appender.append(ByteBuffer.wrap(acked)).join();
            assertEquals(acked.length, ack.endOffset());
            assertEquals(acked.length, ack.durableOffset());

            Messages.ChunkInfo openChunk = latestChunk(fileId);
            assertEquals(ChunkState.OPEN, openChunk.state());
            Messages.Replica dirtyReplica = openChunk.replicas().get(0);
            appendOnlyToReplica(dirtyReplica, openChunk, acked.length, acked.length, dirtyTail);

            StrataFile.SealInfo sealed = client.openById(fileId).recoverAndSeal();
            assertEquals(acked.length, sealed.sealedLength(),
                    "recovery committed bytes that were only present on one replica");

            try (StrataFile.Reader reader = client.openById(fileId).openForRead()) {
                try (StrataFile.ReadResult full = reader.read(0, 1 << 20)) {
                    byte[] got = new byte[full.length()];
                    full.buffer().get(got);
                    assertArrayEquals(acked, got,
                            "sealed read exposed bytes that were only present on one replica");
                    assertTrue(full.endOfFile(), "sealed read should end at acknowledged prefix");
                }
            }
            ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealed.sealedLength());
        } finally {
            appender.close();
        }
    }

    @Test
    void writerDeathThenSealRecoveryPreservesAckedPrefixAndFencesZombie() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/kill-writer")).id();
        Workload workload = new Workload();

        // the "old leader" writes and is then abandoned without sealing (broker died)
        StrataFile.Appender zombie = client.openById(fileId).openForAppend();
        workload.appendAcked(zombie, 0, 137);

        // the "new leader" recovers with epoch 2
        var sealed = client.openById(fileId).recoverAndSeal();
        assertTrue(sealed.sealedLength() >= workload.ackedBytes(),
                "recovery sealed " + sealed.sealedLength() + " < acked " + workload.ackedBytes());

        // zombie can never ack again (invariant §14.1)
        Throwable t = assertThrows(CompletionException.class,
                () -> zombie.append(ByteBuffer.wrap("zombie-write".getBytes())).join()).getCause();
        assertTrue(t instanceof ScpException se && se.code() == ErrorCode.FENCED_EPOCH,
                "expected FENCED_EPOCH, got " + t);
        zombie.close();

        // every acked byte survives recovery
        workload.verifyAckedPrefix(client, fileId);

        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealed.sealedLength());
    }

    @Test
    void recoveryCompletesChunkWhenWriterDiesAfterReplicaSealBeforeMetadataCommit() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/mid-seal-before-meta")).id();
        Workload workload = new Workload();

        StrataFile.Appender abandoned = client.openById(fileId).openForAppend();
        try {
            workload.appendAcked(abandoned, 0, 60);
            long ackedBytes = workload.ackedBytes();

            Messages.ChunkInfo openChunk = latestChunk(fileId);
            assertEquals(ChunkState.OPEN, openChunk.state());
            Messages.Replica sealedReplica = ConsistencyVerifier.waitForOpenReplicaEndAtLeast(openChunk, ackedBytes);
            Messages.SealResp replicaSeal = ConsistencyVerifier.sealReplicaOnly(sealedReplica, openChunk, ackedBytes);
            assertEquals(ackedBytes, replicaSeal.finalLength(),
                    "directly sealed replica must stop exactly at the acked prefix");

            Messages.ChunkInfo stillOpenInMetadata = latestChunk(fileId);
            assertEquals(ChunkState.OPEN, stillOpenInMetadata.state(),
                    "test setup requires metadata to stay open after only a replica seal");

            StrataFile.SealInfo sealed = client.openById(fileId).recoverAndSeal();
            assertEquals(ackedBytes, sealed.sealedLength(),
                    "mid-seal recovery must not move the commit point");

            workload.verifyAckedPrefix(client, fileId);
            ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealed.sealedLength());
        } finally {
            abandoned.close();
        }
    }

    @Test
    void recoverySealsFileWhenWriterDiesAfterChunkMetadataCommitBeforeFileSeal() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/mid-chunk-meta-before-file-seal")).id();
        Workload workload = new Workload();

        StrataFile.Appender abandoned = client.openById(fileId).openForAppend();
        try {
            workload.appendAcked(abandoned, 0, 60);
            long ackedBytes = workload.ackedBytes();

            Messages.ChunkInfo openChunk = latestChunk(fileId);
            assertEquals(ChunkState.OPEN, openChunk.state());

            List<Integer> sealedReplicas = new ArrayList<>();
            Integer crc = null;
            for (Messages.Replica replica : openChunk.replicas()) {
                Messages.SealResp sealedReplica =
                        ConsistencyVerifier.sealReplicaOnly(replica, openChunk, ackedBytes);
                assertEquals(ackedBytes, sealedReplica.finalLength(),
                        "direct replica seal must stop exactly at the acked prefix");
                if (crc == null) {
                    crc = sealedReplica.chunkCrc();
                } else {
                    assertEquals(crc.intValue(), sealedReplica.chunkCrc(),
                            "replica seals must agree before metadata is committed");
                }
                sealedReplicas.add(replica.nodeId());
            }
            sealChunkMeta(openChunk, ackedBytes, crc, sealedReplicas);

            Messages.LookupFileResp halfCommitted = lookupFile(fileId);
            assertEquals(FileState.OPEN.value, halfCommitted.fileState(),
                    "test setup requires file seal metadata to be missing");
            assertEquals(ChunkState.SEALED,
                    halfCommitted.chunks().get(halfCommitted.chunks().size() - 1).state(),
                    "test setup requires chunk metadata to be committed");

            StrataFile.SealInfo sealed = client.openById(fileId).recoverAndSeal();
            assertEquals(ackedBytes, sealed.sealedLength(),
                    "recovery must only add the missing file seal");

            workload.verifyAckedPrefix(client, fileId);
            ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealed.sealedLength());
        } finally {
            abandoned.close();
        }
    }

    @Test
    void recoverAndSealIsIdempotentWhenClientDiesAfterFinalFileSealCommit() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/retry-after-file-seal")).id();
        Workload workload = new Workload();
        long sealedLength;
        try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
            workload.appendAcked(appender, 0, 75);
            sealedLength = appender.seal().sealedLength();
        }

        StrataFile.SealInfo retried = client.openById(fileId).recoverAndSeal();
        assertEquals(sealedLength, retried.sealedLength(),
                "retry after observed-or-lost file seal commit must be idempotent");
        assertEquals(workload.ackedBytes(), retried.sealedLength());
        workload.verifyAckedPrefix(client, fileId);
        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, retried.sealedLength());
    }

    @Test
    void recoveryWithOneReplicaDownStillPreservesAckedData() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/recover-degraded")).id();
        Workload workload = new Workload();

        StrataFile.Appender zombie = client.openById(fileId).openForAppend();
        workload.appendAcked(zombie, 0, 80);

        // kill one replica of the open chunk, then recover with only 2 reachable
        var lookup = lookupFile(fileId);
        var openChunk = lookup.chunks().get(lookup.chunks().size() - 1);
        int victimNodeId = openChunk.replicas().get(0).nodeId();
        for (int i = 0; i < cluster.nodes.size(); i++) {
            if (cluster.nodes.get(i).nodeId() == victimNodeId) cluster.killNode(i);
        }

        var sealed = client.openById(fileId).recoverAndSeal();
        assertTrue(sealed.sealedLength() >= workload.ackedBytes(),
                "acked data lost: sealed " + sealed.sealedLength() + " < acked " + workload.ackedBytes());
        zombie.close();
        workload.verifyAckedPrefix(client, fileId);
        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealed.sealedLength());
    }

    @Test
    void recoveryHonorsConfiguredAckQuorumWhenReplicaIsUnavailable() throws Exception {
        FileId fileId = client.create(new StrataClient.FileSpec("test", "/recover-aq3",
                StrataClient.WritePolicy.replicated(3, 3))).id();
        Workload workload = new Workload();
        StrataFile.Appender abandoned = client.openById(fileId).openForAppend();
        try {
            workload.appendAcked(abandoned, 0, 40);

            Messages.ChunkInfo openChunk = latestChunk(fileId);
            assertEquals(ChunkState.OPEN, openChunk.state());
            cluster.killNode(nodeIndex(openChunk.replicas().get(0).nodeId()));

            ScpException e = assertThrows(ScpException.class, () -> client.openById(fileId).recoverAndSeal());
            assertEquals(ErrorCode.INTERNAL, e.code());
            assertTrue(e.getMessage().contains("need 3"),
                    "recovery must enforce the file ack quorum, got: " + e.getMessage());

            var after = latestChunk(fileId);
            assertEquals(ChunkState.OPEN, after.state(),
                    "failed recovery must not commit sealed metadata below ack quorum");
        } finally {
            abandoned.close();
        }
    }

    @Test
    void staleRecoveryEpochFailsWhenAnyReplicaIsAlreadyFencedHigher() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/recover-stale-epoch")).id();
        StrataFile.Appender zombie = client.openById(fileId).openForAppend();
        new Workload().appendAcked(zombie, 0, 10);

        var lookup = lookupFile(fileId);
        var openChunk = lookup.chunks().get(lookup.chunks().size() - 1);
        var fencedReplica = openChunk.replicas().get(0);
        String[] hp = fencedReplica.endpoint().split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]),
                ScpClient.KIND_BROKER, "fence")) {
            direct.call(Opcode.FENCE, new Messages.Fence(openChunk.chunkId(), 3).encode(), null, 5000);
        }

        ScpException e = assertThrows(ScpException.class, () -> client.openById(fileId).recoverAndSeal());
        assertEquals(ErrorCode.FENCED_EPOCH, e.code());
        assertEquals(3, e.detail());

        var after = lookupFile(fileId);
        assertEquals(io.strata.common.ChunkState.OPEN,
                after.chunks().get(after.chunks().size() - 1).state(),
                "stale recovery must not seal metadata after seeing a higher replica fence");

        zombie.close();
    }

    @Test
    void nodeRestartKeepsIdentityAndData() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/restart")).id();
        Workload workload = new Workload();
        try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
            workload.appendAcked(appender, 0, 100);
            appender.seal();
        }

        int oldNodeId = cluster.nodes.get(0).nodeId();
        var restarted = cluster.restartNode(0);
        // volume-bound identity: after re-registration the node id must be unchanged
        long deadline = System.currentTimeMillis() + 10_000;
        while (restarted.nodeId() != oldNodeId && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(oldNodeId, restarted.nodeId(), "node identity must survive restart");

        workload.verifyAckedPrefix(client, fileId);
        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, workload.ackedBytes());
    }

    @Test
    void restartingAllOpenChunkReplicasUnderFsyncPreservesAckedDataAfterRecovery() throws Exception {
        FileId fileId = client.create(new StrataClient.FileSpec("test", "/restart-open-fsync",
                StrataClient.WritePolicy.fsync(3, 2))).id();
        Workload workload = new Workload();
        StrataFile.Appender abandoned = client.openById(fileId).openForAppend();
        try {
            workload.appendAcked(abandoned, 0, 160);

            var openChunk = latestChunk(fileId);
            assertEquals(io.strata.common.ChunkState.OPEN, openChunk.state());
            Set<Integer> restartedNodeIds = new HashSet<>();
            for (var replica : openChunk.replicas()) {
                int nodeId = replica.nodeId();
                restartedNodeIds.add(nodeId);
                var restarted = cluster.restartNode(nodeIndex(nodeId));
                waitForNodeId(restarted, nodeId);
            }
            waitForReplicaEndpoints(fileId, restartedNodeIds);

            StrataFile.SealInfo sealed = client.openById(fileId).recoverAndSeal();
            assertTrue(sealed.sealedLength() >= workload.ackedBytes(),
                    "recovery sealed " + sealed.sealedLength() + " < acked " + workload.ackedBytes());

            workload.verifyAckedPrefix(client, fileId);
            ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealed.sealedLength());
        } finally {
            abandoned.close();
        }
    }

    @Test
    void fullClusterColdRestartPreservesSealedDataAndRecoversOpenChunk() throws Exception {
        runFullClusterColdRestart(false);
    }

    @Test
    void fullClusterColdRestartAfterZooKeeperRestartPreservesMetadataAndData() throws Exception {
        runFullClusterColdRestart(true);
    }

    @Test
    void pathDeleteAndRecreateAfterZooKeeperRestartBindsOnlyNewFile() throws Exception {
        String namespace = "test";
        String path = "/zk-path-delete-recreate";
        StrataFile original = client.create(StrataClient.FileSpec.log(namespace, path));
        FileId originalId = original.id();
        Workload originalWorkload = new Workload();
        long originalLength;
        try (StrataFile.Appender appender = original.openForAppend()) {
            originalWorkload.appendAcked(appender, 0, 90);
            originalLength = appender.seal().sealedLength();
        }

        Set<Integer> originalReplicaNodeIds = replicaNodeIds(lookupFile(originalId));
        List<String> hosts = cluster.nodes.stream().map(node -> node.config().host()).toList();
        List<Integer> nodeIds = cluster.nodes.stream().map(node -> node.nodeId()).toList();
        client.close();
        client = null;

        restartServices(hosts, nodeIds, true);
        waitForReplicaEndpoints(originalId, originalReplicaNodeIds);

        client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint())
                .withChunkRollBytes(1 << 16)
                .withDataNodeConnectionsPerEndpoint(3));
        assertEquals(originalId, client.open(namespace, path).id(),
                "path binding changed after ZooKeeper restart");
        originalWorkload.verifyAckedPrefix(client, originalId);
        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, originalId, originalLength);

        client.delete(namespace, path);
        ScpException missing = assertThrows(ScpException.class, () -> client.open(namespace, path));
        assertEquals(ErrorCode.FILE_NOT_FOUND, missing.code(),
                "deleted path must not resolve to the old file");

        StrataFile replacement = client.create(StrataClient.FileSpec.log(namespace, path));
        assertNotEquals(originalId, replacement.id(), "path reuse must create a new file id");
        Workload replacementWorkload = new Workload();
        long replacementLength;
        try (StrataFile.Appender appender = replacement.openForAppend()) {
            replacementWorkload.appendAcked(appender, 1000, 1030);
            replacementLength = appender.seal().sealedLength();
        }

        assertEquals(replacement.id(), client.open(namespace, path).id(),
                "recreated path must resolve to the replacement file");
        replacementWorkload.verifyAckedPrefix(client, replacement.id());
        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, replacement.id(),
                replacementLength);
    }

    private void runFullClusterColdRestart(boolean restartZooKeeper) throws Exception {
        FileId sealedFileId = client.create(StrataClient.FileSpec.log("test",
                restartZooKeeper ? "/zk-cold-restart-sealed" : "/cold-restart-sealed")).id();
        Workload sealedWorkload = new Workload();
        long sealedLength;
        try (StrataFile.Appender appender = client.openById(sealedFileId).openForAppend()) {
            sealedWorkload.appendAcked(appender, 0, 120);
            sealedLength = appender.seal().sealedLength();
        }

        FileId openFileId = client.create(new StrataClient.FileSpec("test",
                restartZooKeeper ? "/zk-cold-restart-open" : "/cold-restart-open",
                StrataClient.WritePolicy.fsync(3, 2))).id();
        Workload openWorkload = new Workload();
        StrataFile.Appender abandoned = client.openById(openFileId).openForAppend();
        openWorkload.appendAcked(abandoned, 0, 140);

        Set<Integer> sealedReplicaNodeIds = replicaNodeIds(lookupFile(sealedFileId));
        Set<Integer> openReplicaNodeIds = replicaNodeIds(lookupFile(openFileId));
        List<String> hosts = cluster.nodes.stream().map(node -> node.config().host()).toList();
        List<Integer> nodeIds = cluster.nodes.stream().map(node -> node.nodeId()).toList();
        abandoned.close();
        client.close();
        client = null;

        restartServices(hosts, nodeIds, restartZooKeeper);
        waitForReplicaEndpoints(sealedFileId, sealedReplicaNodeIds);
        waitForReplicaEndpoints(openFileId, openReplicaNodeIds);

        client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint())
                .withChunkRollBytes(1 << 16)
                .withDataNodeConnectionsPerEndpoint(3));

        assertEquals(sealedWorkload.ackedBytes(), sealedLength,
                "sealed file committed a non-acked length before cold restart");
        sealedWorkload.verifyAckedPrefix(client, sealedFileId);
        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, sealedFileId, sealedLength);

        StrataFile.SealInfo recovered = client.openById(openFileId).recoverAndSeal();
        assertTrue(recovered.sealedLength() >= openWorkload.ackedBytes(),
                "cold recovery sealed " + recovered.sealedLength()
                        + " < acked " + openWorkload.ackedBytes());
        openWorkload.verifyAckedPrefix(client, openFileId);
        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, openFileId,
                recovered.sealedLength());
    }

    private void restartServices(List<String> hosts, List<Integer> nodeIds, boolean restartZooKeeper)
            throws Exception {
        cluster.stopDataNodes();
        cluster.stopControllers();
        if (restartZooKeeper) {
            cluster.restartZooKeeper();
        }
        cluster.startControllers();
        cluster.startDataNodes(hosts);
        for (int i = 0; i < nodeIds.size(); i++) {
            waitForNodeId(cluster.nodes.get(i), nodeIds.get(i));
        }
    }

    private Messages.LookupFileResp lookupFile(FileId fileId) throws Exception {
        return ConsistencyVerifier.lookupFile(cluster, fileId);
    }

    private int nodeIndex(int nodeId) {
        for (int i = 0; i < cluster.nodes.size(); i++) {
            if (cluster.nodes.get(i).nodeId() == nodeId) {
                return i;
            }
        }
        throw new AssertionError("node " + nodeId + " not found");
    }

    private Messages.ChunkInfo latestChunk(FileId fileId) throws Exception {
        var lookup = lookupFile(fileId);
        return lookup.chunks().get(lookup.chunks().size() - 1);
    }

    private void appendOnlyToReplica(Messages.Replica replica, Messages.ChunkInfo chunk, long baseOffset,
                                     long durableOffset, byte[] payload) throws Exception {
        String[] hp = replica.endpoint().split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]),
                ScpClient.KIND_TOOL, "dirty-open-tail")) {
            ByteBuffer h = direct.call(Opcode.APPEND,
                    new Messages.Append(chunk.chunkId(), chunk.writeEpoch(), baseOffset, durableOffset).encode(),
                    ByteBuffer.wrap(payload), 5000);
            assertEquals(baseOffset + payload.length, Messages.AppendResp.decode(h).endOffset());
        }
    }

    private void sealChunkMeta(Messages.ChunkInfo chunk, long length, int crc, List<Integer> sealedReplicas)
            throws Exception {
        String[] hp = cluster.metaEndpoint().split(":");
        try (ScpClient meta = new ScpClient(hp[0], Integer.parseInt(hp[1]),
                ScpClient.KIND_TOOL, "seal-chunk-meta")) {
            meta.call(Opcode.SEAL_CHUNK_META,
                    new Messages.SealChunkMeta(chunk.chunkId(), chunk.writeEpoch(), length, crc,
                            sealedReplicas).encode(),
                    null, 5000);
        }
    }

    private void waitForNodeId(io.strata.node.DataNode node, int expectedNodeId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (node.nodeId() != expectedNodeId && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(expectedNodeId, node.nodeId(), "node identity must survive restart");
    }

    private void waitForReplicaEndpoints(FileId fileId, Set<Integer> nodeIds) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            var lookup = lookupFile(fileId);
            boolean allRefreshed = true;
            Set<Integer> seen = new HashSet<>();
            for (var chunk : lookup.chunks()) {
                for (var replica : chunk.replicas()) {
                    if (!nodeIds.contains(replica.nodeId())) {
                        continue;
                    }
                    seen.add(replica.nodeId());
                    if (!replica.endpoint().equals(cluster.nodes.get(nodeIndex(replica.nodeId())).endpoint())) {
                        allRefreshed = false;
                        break;
                    }
                }
                if (!allRefreshed) {
                    break;
                }
            }
            if (allRefreshed && seen.containsAll(nodeIds)) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("replica endpoints did not refresh after node restart");
    }

    private static Set<Integer> replicaNodeIds(Messages.LookupFileResp file) {
        Set<Integer> ids = new HashSet<>();
        for (var chunk : file.chunks()) {
            for (var replica : chunk.replicas()) {
                ids.add(replica.nodeId());
            }
        }
        return ids;
    }

}
