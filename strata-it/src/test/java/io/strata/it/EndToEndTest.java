package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.proto.Messages;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 3-node end-to-end happy path: multi-chunk write, seal, full read-back, replica byte-identity. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EndToEndTest {

    private MiniCluster cluster;
    private ClientConfig config;
    private StrataClient client;

    @BeforeAll
    void setup() throws Exception {
        cluster = new MiniCluster(3);
        // small chunks force several rolls in one test
        config = ClientConfig.of(cluster.metaEndpoint())
                .withChunkRollBytes(4096)
                .withDataNodeConnectionsPerEndpoint(3)
                .withDurableBeaconIdleMs(25);
        client = StrataClient.connect(config);
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
                    // Open-chunk reads clamp to the replica-known DO, which may briefly lag until
                    // the next payload piggyback or the automatic idle beacon.
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
    void idleBeaconPublishesFinalDurableOffsetToOpenReaders() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/idle-durable-beacon")).id();
        byte[] payload = "final-acknowledged-open-tail".getBytes(StandardCharsets.UTF_8);

        try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
            long acknowledged = appender.append(ByteBuffer.wrap(payload))
                    .get(config.callTimeoutMs(), TimeUnit.MILLISECONDS);
            assertEquals(payload.length, acknowledged);
            assertEquals(acknowledged, appender.durableOffset());

            // Do not append again (including an explicit empty append) or seal: the idle beacon must
            // publish the final acknowledged offset by itself.
            Messages.LookupFileResp lookup = ConsistencyVerifier.lookupFile(cluster, fileId);
            assertEquals(1, lookup.chunks().size());
            Messages.ChunkInfo openChunk = lookup.chunks().get(0);
            assertEquals(ChunkState.OPEN, openChunk.state());
            waitForDurableOffsetOnEveryReplica(openChunk, acknowledged);

            for (Messages.Replica replica : openChunk.replicas()) {
                Messages.StatResp stat = ConsistencyVerifier.statReplica(replica, openChunk);
                assertEquals(acknowledged, stat.localEndOffset());
                assertEquals(acknowledged, stat.lastKnownDO());
            }

            try (StrataFile.Reader reader = client.openById(StrataNamespace.of("test"), fileId).openForRead();
                 StrataFile.ReadResult result = reader.read(0, payload.length + 1)) {
                byte[] actual = new byte[result.length()];
                result.buffer().get(actual);
                assertArrayEquals(payload, actual);
            }

            assertEquals(acknowledged, appender.seal().sealedLength());
        }
    }

    @Test
    void emptyAppendCompletesWithoutCreatingAChunk() throws Exception {
        FileId fileId = client.create(StrataClient.FileSpec.log("test", "/empty-append")).id();

        try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
            long ack = appender.append(ByteBuffer.allocate(0)).get(1, TimeUnit.SECONDS);
            assertEquals(0, ack);
            assertEquals(0, appender.durableOffset());
            assertEquals(0, appender.seal().sealedLength());
        }

        var lookup = ConsistencyVerifier.lookupFile(cluster, fileId);
        assertEquals(1, lookup.fileState(), "empty sealed file should be SEALED");
        assertEquals(0, lookup.chunks().size(), "empty append must not create an empty chunk");
    }

    private void waitForDurableOffsetOnEveryReplica(Messages.ChunkInfo chunk, long expected) throws Exception {
        long healthyBoundMs = Math.addExact(config.durableBeaconIdleMs(), config.callTimeoutMs());
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(healthyBoundMs);
        long pollMs = Math.max(1L, Math.min(25L, config.durableBeaconIdleMs() / 4));

        while (System.nanoTime() < deadline) {
            boolean allPublished = true;
            for (Messages.Replica replica : chunk.replicas()) {
                if (ConsistencyVerifier.statReplica(replica, chunk).lastKnownDO() != expected) {
                    allPublished = false;
                }
            }
            if (allPublished) {
                return;
            }
            Thread.sleep(pollMs);
        }

        for (Messages.Replica replica : chunk.replicas()) {
            assertEquals(expected, ConsistencyVerifier.statReplica(replica, chunk).lastKnownDO(),
                    "idle beacon did not publish the final durable offset to node " + replica.nodeId()
                            + " within the healthy bound of " + healthyBoundMs + "ms");
        }
    }
}
