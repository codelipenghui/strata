package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.ByteBuffer;
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
        client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint())
                .withChunkRollBytes(4096)
                .withDataNodeConnectionsPerEndpoint(3));
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
        if (cluster != null) cluster.close();
    }

    @Test
    void writeRollSealReadBack() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/topicA-0")).id();
        Workload workload = new Workload();

        try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
            workload.appendAcked(appender, 0, 1500); // ~24 KB -> several 4 KB chunk rolls
            assertEquals(workload.ackedBytes(), appender.durableOffset());
            var sealed = appender.seal();
            assertEquals(workload.ackedBytes(), sealed.sealedLength());
        }

        // every acked byte reads back, in order, to EOF
        workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);

        // metadata and data-node replicas agree on the sealed file shape
        var lookup = ConsistencyVerifier.assertSealedFileConsistent(cluster, client, fileId,
                workload.ackedBytes());
        assertTrue(lookup.chunks().size() >= 5, "expected several chunks, got " + lookup.chunks().size());
        for (var c : lookup.chunks()) {
            assertEquals(3, c.replicas().size());
        }
    }

    @Test
    void tailReadNeverExceedsDurableOffset() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/topicB-0")).id();
        Workload workload = new Workload();
        try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
            workload.appendAcked(appender, 0, 50);

            try (StrataFile.Reader reader = client.openById(StrataNamespace.of("test"), fileId).openForRead()) {
                try (StrataFile.ReadResult r = reader.read(0, 1 << 20)) {
                    int n = r.length();
                    // open-chunk read is clamped to the replica-known DO (may lag by one append round)
                    assertTrue(n <= workload.ackedBytes(),
                            "read beyond acked bytes: " + n + " > " + workload.ackedBytes());
                    byte[] got = new byte[n];
                    r.buffer().get(got);
                    byte[] expected = new byte[n];
                    System.arraycopy(Workload.readAll(client, StrataNamespace.of("test"), fileId, 0), 0, expected, 0, n);
                    assertArrayEquals(expected, got);
                }
            }
            appender.seal();
        }
        workload.verifyAckedPrefix(client, StrataNamespace.of("test"), fileId);
    }

    @Test
    void emptyAppendCompletesWithoutCreatingAChunk() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/empty-append")).id();

        try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
            var ack = appender.append(ByteBuffer.allocate(0)).get(1, TimeUnit.SECONDS);
            assertEquals(0, ack.endOffset());
            assertEquals(0, ack.durableOffset());
            assertEquals(0, appender.durableOffset());
            assertEquals(0, appender.seal().sealedLength());
        }

        var lookup = ConsistencyVerifier.lookupFile(cluster, fileId);
        assertEquals(1, lookup.fileState(), "empty sealed file should be SEALED");
        assertEquals(0, lookup.chunks().size(), "empty append must not create an empty chunk");
    }
}
