package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.SegmentStore;
import io.strata.client.StrataClient;
import io.strata.common.FileId;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ack-on-fsync end-to-end (tech design §5.3): the durability policy is per-file, carried through
 * CreateFile -> lookup -> OPEN_CHUNK -> the chunk header on disk, and replicas fsync data+ledger
 * before acking. Verifies the policy physically lands in the header of every replica, the full
 * write/seal/read path works, and seal recovery works under fsync mode.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FsyncEndToEndTest {

    private static final SegmentStore.FileSpec FSYNC_SPEC =
            new SegmentStore.FileSpec((byte) 0, (byte) 0, (byte) 1 /* ack-on-fsync */, "fsync-topic");

    private MiniCluster cluster;
    private StrataClient client;

    @BeforeAll
    void setup() throws Exception {
        cluster = new MiniCluster(3);
        client = new StrataClient(ClientConfig.of(cluster.metaEndpoint()).withChunkRollBytes(4 * 1024));
    }

    @AfterAll
    void teardown() throws Exception {
        client.close();
        cluster.close();
    }

    @Test
    void fsyncPolicyReachesEveryReplicaHeaderAndDataSurvives() throws Exception {
        FileId fileId = client.create(FSYNC_SPEC);
        Workload workload = new Workload();
        try (SegmentStore.Appender appender = client.openForAppend(fileId, 1)) {
            workload.appendAcked(appender, 0, 500); // several rolls under fsync
            appender.seal();
        }
        workload.verifyAckedPrefix(client, fileId);

        // the policy must be burned into the chunk header on EVERY replica
        var lookup = lookupFile(fileId);
        assertTrue(lookup.chunks().size() >= 2);
        for (var c : lookup.chunks()) {
            for (var replica : c.replicas()) {
                byte[] headerBytes = fetchHeader(replica.endpoint(), c.chunkId());
                ChunkFormats.Header header = ChunkFormats.Header.decode(headerBytes);
                assertEquals(1, header.ackPolicy(),
                        "replica " + replica.nodeId() + " of " + c.chunkId() + " missing fsync policy");
            }
        }
    }

    @Test
    void sealRecoveryWorksUnderFsyncMode() throws Exception {
        FileId fileId = client.create(FSYNC_SPEC);
        Workload workload = new Workload();
        SegmentStore.Appender zombie = client.openForAppend(fileId, 1);
        workload.appendAcked(zombie, 0, 120);

        var sealed = client.recoverAndSeal(fileId, 2);
        assertTrue(sealed.sealedLength() >= workload.ackedBytes());
        zombie.close();
        workload.verifyAckedPrefix(client, fileId);
    }

    @Test
    void fsyncAndReplicateFilesCoexist() throws Exception {
        FileId fsyncFile = client.create(FSYNC_SPEC);
        FileId fastFile = client.create(SegmentStore.FileSpec.log("fast-topic"));
        Workload w1 = new Workload(), w2 = new Workload();
        try (SegmentStore.Appender a1 = client.openForAppend(fsyncFile, 1);
             SegmentStore.Appender a2 = client.openForAppend(fastFile, 1)) {
            w1.appendAcked(a1, 0, 100);
            w2.appendAcked(a2, 0, 100);
            a1.seal();
            a2.seal();
        }
        w1.verifyAckedPrefix(client, fsyncFile);
        w2.verifyAckedPrefix(client, fastFile);

        // the fast file's headers carry ack-on-replicate
        var lookup = lookupFile(fastFile);
        var c = lookup.chunks().get(0);
        ChunkFormats.Header header = ChunkFormats.Header.decode(
                fetchHeader(c.replicas().get(0).endpoint(), c.chunkId()));
        assertEquals(0, header.ackPolicy());
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

    private Messages.LookupFileResp lookupFile(FileId fileId) throws Exception {
        String[] hp = cluster.metaEndpoint().split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
            ByteBuffer h = direct.call(Opcode.LOOKUP_FILE, new Messages.LookupFile(fileId).encode(), null, 5000);
            return Messages.LookupFileResp.decode(h);
        }
    }
}
