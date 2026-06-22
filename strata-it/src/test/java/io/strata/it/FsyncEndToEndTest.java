package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.format.ChunkFormats;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ack-on-fsync end-to-end (tech design §5.3): the file write policy is carried through
 * lookup -> OPEN_CHUNK -> the chunk header on disk, and replicas fsync data+ledger before acking.
 * Verifies the policy physically lands in the header of every replica, the full write/seal/read
 * path works, and seal recovery works under fsync mode.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FsyncEndToEndTest {

    private MiniCluster cluster;
    private StrataClient client;

    @BeforeAll
    void setup() throws Exception {
        cluster = new MiniCluster(3);
        client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint())
                .withChunkRollBytes(4 * 1024)
                .withDataNodeConnectionsPerEndpoint(3));
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
        if (cluster != null) cluster.close();
    }

    @Test
    void fsyncPolicyReachesEveryReplicaHeaderAndDataSurvives() throws Exception {
        FileId fileId = client.create(fsyncSpec("/fsync-policy")).id();
        Workload workload = new Workload();
        StrataFile.SealInfo sealed;
        try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
            workload.appendAcked(appender, 0, 500); // several rolls under fsync
            sealed = appender.seal();
        }
        workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);

        // the policy must be burned into the chunk header on EVERY replica
        var lookup = ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId,
                sealed.sealedLength());
        assertTrue(lookup.chunks().size() >= 2);
        for (var c : lookup.chunks()) {
            for (var replica : c.replicas()) {
                byte[] headerBytes = fetchHeader(replica.endpoint(), c.chunkId());
                ChunkFormats.Header header = ChunkFormats.Header.decode(headerBytes);
                assertTrue(header.fsyncOnAck(),
                        "replica " + replica.nodeId() + " of " + c.chunkId() + " missing fsync policy");
            }
        }
    }

    @Test
    void sealRecoveryWorksUnderFsyncMode() throws Exception {
        FileId fileId = client.create(fsyncSpec("/fsync-recovery")).id();
        Workload workload = new Workload();
        StrataFile.Appender zombie = client.openById(StrataNamespace.of("test"), fileId).openForAppend();
        workload.appendAcked(zombie, 0, 120);

        var sealed = client.openById(StrataNamespace.of("test"), fileId).recoverAndSeal();
        assertTrue(sealed.sealedLength() >= workload.ackedBytes());
        zombie.close();
        workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);
        ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId, sealed.sealedLength());
    }

    private byte[] fetchHeader(String endpoint, io.strata.common.ChunkId chunkId) throws Exception {
        String[] hp = endpoint.split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "hdr")) {
            var frame = direct.callFrame(Opcode.FETCH_CHUNK,
                    new Messages.FetchChunk(chunkId, 0, ChunkFormats.HEADER_SIZE).encode(), null, 5000);
            ByteBuffer h = frame.headerSlice();
            Resp.check(h);
            byte[] bytes = new byte[frame.payloadLength()];
            frame.payloadSlice().get(bytes);
            return bytes;
        }
    }

    private static StrataClient.FileSpec fsyncSpec(String path) {
        return new StrataClient.FileSpec("test", path, StrataClient.WritePolicy.fsync(3, 2));
    }

}
