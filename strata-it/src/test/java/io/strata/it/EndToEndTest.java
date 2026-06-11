package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.client.StrataClient;
import io.strata.common.FileId;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 3-node end-to-end happy path: multi-chunk write, seal, full read-back, replica byte-identity. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EndToEndTest {

    private MiniCluster cluster;
    private StrataClient client;

    @BeforeAll
    void setup() throws Exception {
        cluster = new MiniCluster(3);
        // small chunks force several rolls in one test
        client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()).withChunkRollBytes(4096));
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
        if (cluster != null) cluster.close();
    }

    @Test
    void writeRollSealReadBack() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("topicA-0")).id();
        Workload workload = new Workload();

        try (StrataFile.Appender appender = client.open(fileId).openForAppend(1)) {
            workload.appendAcked(appender, 0, 1500); // ~24 KB -> several 4 KB chunk rolls
            assertEquals(workload.ackedBytes(), appender.durableOffset());
            var sealed = appender.seal();
            assertEquals(workload.ackedBytes(), sealed.sealedLength());
        }

        // every acked byte reads back, in order, to EOF
        workload.verifyAckedPrefix(client, fileId);

        // metadata: multiple chunks, all sealed, RF=3 each
        var lookup = lookup(fileId);
        assertTrue(lookup.chunks().size() >= 5, "expected several chunks, got " + lookup.chunks().size());
        long totalLength = 0;
        for (var c : lookup.chunks()) {
            assertEquals(io.strata.common.ChunkState.SEALED, c.state());
            assertEquals(3, c.replicas().size());
            totalLength += c.length();
        }
        assertEquals(workload.ackedBytes(), totalLength);

        // invariant §14.6: sealed replicas are byte-identical, whole file including header+footer
        for (var c : lookup.chunks()) {
            Set<String> distinctFiles = new HashSet<>();
            for (var replica : c.replicas()) {
                String[] hp = replica.endpoint().split(":");
                try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]),
                        ScpClient.KIND_TOOL, "verify")) {
                    var frame = direct.callFrame(Opcode.FETCH_CHUNK,
                            new Messages.FetchChunk(c.chunkId(), 0, Integer.MAX_VALUE).encode(), null, 5000);
                    ByteBuffer h = frame.headerSlice();
                    Resp.check(h);
                    byte[] bytes = new byte[frame.payloadLength()];
                    frame.payloadSlice().get(bytes);
                    distinctFiles.add(java.util.HexFormat.of().formatHex(
                            java.security.MessageDigest.getInstance("SHA-256").digest(bytes)));
                }
            }
            assertEquals(1, distinctFiles.size(), "replicas of " + c.chunkId() + " diverge");
        }
    }

    @Test
    void tailReadNeverExceedsDurableOffset() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("topicB-0")).id();
        Workload workload = new Workload();
        try (StrataFile.Appender appender = client.open(fileId).openForAppend(1)) {
            workload.appendAcked(appender, 0, 50);

            try (StrataFile.Reader reader = client.open(fileId).openForRead()) {
                var r = reader.read(0, 1 << 20);
                // open-chunk read is clamped to the replica-known DO (may lag by one append round)
                assertTrue(r.data().length <= workload.ackedBytes(),
                        "read beyond acked bytes: " + r.data().length + " > " + workload.ackedBytes());
                byte[] expected = new byte[r.data().length];
                System.arraycopy(Workload.readAll(client, fileId, 0), 0, expected, 0, r.data().length);
                assertArrayEquals(expected, r.data());
            }
            appender.seal();
        }
        workload.verifyAckedPrefix(client, fileId);
    }

    @Test
    void emptyAppendCompletesWithoutCreatingAChunk() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("empty-append")).id();

        try (StrataFile.Appender appender = client.open(fileId).openForAppend(1)) {
            var ack = appender.append(ByteBuffer.allocate(0)).get(1, TimeUnit.SECONDS);
            assertEquals(0, ack.endOffset());
            assertEquals(0, ack.durableOffset());
            assertEquals(0, appender.durableOffset());
            assertEquals(0, appender.seal().sealedLength());
        }

        var lookup = lookup(fileId);
        assertEquals(1, lookup.fileState(), "empty sealed file should be SEALED");
        assertEquals(0, lookup.chunks().size(), "empty append must not create an empty chunk");
    }

    private Messages.LookupFileResp lookup(FileId fileId) throws Exception {
        String[] hp = cluster.metaEndpoint().split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "t")) {
            ByteBuffer h = direct.call(Opcode.LOOKUP_FILE, new Messages.LookupFile(fileId).encode(), null, 5000);
            return Messages.LookupFileResp.decode(h);
        }
    }
}
