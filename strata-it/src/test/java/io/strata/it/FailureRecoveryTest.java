package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()).withChunkRollBytes(1 << 16));
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

            // find a storage node hosting the current open chunk and kill it
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

        // sealed length never exceeds what was physically written (acked + possibly in-flight tail)
        var lookup = lookupFile(fileId);
        long total = 0;
        for (var c : lookup.chunks()) {
            assertEquals(io.strata.common.ChunkState.SEALED, c.state());
            total += c.length();
        }
        assertEquals(sealed.sealedLength(), total);
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
    }

    private Messages.LookupFileResp lookupFile(FileId fileId) throws Exception {
        String[] hp = cluster.metaEndpoint().split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
            ByteBuffer h = direct.call(Opcode.LOOKUP_FILE, new Messages.LookupFile(fileId).encode(), null, 5000);
            return Messages.LookupFileResp.decode(h);
        }
    }
}
