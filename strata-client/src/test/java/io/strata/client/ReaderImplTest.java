package io.strata.client;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.proto.Frame;
import io.strata.proto.ManagedScpConnection;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReaderImplTest {

    @Test
    void sealedMetadataLengthOverflowIsTypedCorruption() throws Exception {
        FileId fileId = FileId.random();
        Messages.LookupFileResp lookup = new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 1,
                List.of(chunk(new ChunkId(fileId, 0), ChunkState.SEALED, Long.MAX_VALUE,
                                new Messages.Replica(1, "")),
                        chunk(new ChunkId(fileId, 1), ChunkState.SEALED, 1,
                                new Messages.Replica(1, ""))));

        try (ScpServer metaServer = metadataServer(new AtomicReference<>(lookup))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId, StrataNamespace.of("test"));

                ScpException e = assertThrows(ScpException.class, () -> reader.read(Long.MAX_VALUE, 1));
                assertEquals(ErrorCode.CORRUPT_CHUNK, e.code());
            }
        }
    }

    @Test
    void emptyFileReadUsesMetadataFileStateForEof() throws Exception {
        FileId fileId = FileId.random();
        AtomicReference<Messages.LookupFileResp> lookup = new AtomicReference<>(
                new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 1, List.of()));

        try (ScpServer metaServer = metadataServer(lookup)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId, StrataNamespace.of("test"));

                try (StrataFile.ReadResult r = reader.read(0, 1)) {
                    assertTrue(r.endOfFile());
                }

                lookup.set(new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 0, List.of()));
                reader.refresh();
                try (StrataFile.ReadResult r = reader.read(0, 1)) {
                    assertFalse(r.endOfFile());
                }
            }
        }
    }

    @Test
    void sealedReadOnOpenFileDoesNotReportEof() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer replica = readReplica(new Messages.ReadResp(3, 3), new byte[] {1, 2, 3});
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 0,
                             List.of(chunk(chunkId, ChunkState.SEALED, 3,
                                     new Messages.Replica(1, endpoint(replica)))))))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId, StrataNamespace.of("test"));

                try (StrataFile.ReadResult result = reader.read(0, 3)) {
                    assertArrayEquals(new byte[] {1, 2, 3}, drain(result.buffer()));
                    assertFalse(result.endOfFile());
                }
            }
        }
    }

    @Test
    void sealedReplicaShorterThanDescriptorIsRejected() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer shortReplica = readReplica(new Messages.ReadResp(3, 3), new byte[] {1, 2, 3});
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 1,
                             List.of(chunk(chunkId, ChunkState.SEALED, 5,
                                     new Messages.Replica(1, endpoint(shortReplica)))))))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId, StrataNamespace.of("test"));

                ScpException e = assertThrows(ScpException.class, () -> reader.read(0, 3));
                assertEquals(ErrorCode.CORRUPT_CHUNK, e.code());
            }
        }
    }

    @Test
    void replicaScpExceptionIsPropagatedAfterReplicaAttemptsAreExhausted() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer failingReplica = new ScpServer(0, 1, 0, 0, req -> {
            if (Opcode.fromCode(req.opcode()) == Opcode.READ) {
                Messages.Read.decode(req.headerSlice());
                throw new ScpException(ErrorCode.CRC_MISMATCH, "bad crc");
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + req.opcode());
        });
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 1,
                             List.of(chunk(chunkId, ChunkState.SEALED, 3,
                                     new Messages.Replica(1, endpoint(failingReplica)))))))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId, StrataNamespace.of("test"));

                ScpException e = assertThrows(ScpException.class, () -> reader.read(0, 3));
                assertEquals(ErrorCode.CRC_MISMATCH, e.code());
            }
        }
    }

    @Test
    void openReadKeepsPayloadWhenDurableOffsetCoversIt() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer replica = readReplica(new Messages.ReadResp(4, 4), new byte[] {1, 2, 3, 4});
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 0,
                             List.of(chunk(chunkId, ChunkState.OPEN, 0,
                                     new Messages.Replica(1, endpoint(replica)))))))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId, StrataNamespace.of("test"));

                try (StrataFile.ReadResult result = reader.read(0, 4)) {
                    assertArrayEquals(new byte[] {1, 2, 3, 4}, drain(result.buffer()));
                    assertFalse(result.endOfFile());
                }
            }
        }
    }

    @Test
    void readerPinsEndpointConnectionAcrossReads() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer replica = readReplica(new Messages.ReadResp(1, 1), new byte[] {7});
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 1,
                             List.of(chunk(chunkId, ChunkState.SEALED, 1,
                                     new Messages.Replica(1, endpoint(replica)))))))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500)
                    .withDataNodeConnectionsPerEndpoint(2);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool(config)) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId, StrataNamespace.of("test"));
                String replicaEndpoint = endpoint(replica);

                try (StrataFile.ReadResult rr = reader.read(0, 1)) {
                    assertArrayEquals(new byte[] {7}, drain(rr.buffer()));
                }
                ManagedScpConnection pinned = pinnedConnection(reader, replicaEndpoint);
                assertNotNull(pinned);
                assertSame(pinned, pinnedConnection(reader, replicaEndpoint));

                try (StrataFile.ReadResult rr = reader.read(0, 1)) {
                    assertArrayEquals(new byte[] {7}, drain(rr.buffer()));
                }
                assertSame(pinned, pinnedConnection(reader, replicaEndpoint));
                assertNotSame(pinned, pool.get(replicaEndpoint),
                        "a reader must reuse its pinned connection instead of round-robining per read");
            }
        }
    }

    @Test
    void independentReadersCanUseDifferentEndpointPoolConnections() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer replica = readReplica(new Messages.ReadResp(1, 1), new byte[] {9});
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 1,
                             List.of(chunk(chunkId, ChunkState.SEALED, 1,
                                     new Messages.Replica(1, endpoint(replica)))))))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500)
                    .withDataNodeConnectionsPerEndpoint(2);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool(config)) {
                ReaderImpl first = new ReaderImpl(meta, pool, config, fileId, StrataNamespace.of("test"));
                ReaderImpl second = new ReaderImpl(meta, pool, config, fileId, StrataNamespace.of("test"));
                String replicaEndpoint = endpoint(replica);

                try (StrataFile.ReadResult firstRead = first.read(0, 1)) {
                    assertArrayEquals(new byte[] {9}, drain(firstRead.buffer()));
                }
                try (StrataFile.ReadResult secondRead = second.read(0, 1)) {
                    assertArrayEquals(new byte[] {9}, drain(secondRead.buffer()));
                }

                assertNotSame(pinnedConnection(first, replicaEndpoint),
                        pinnedConnection(second, replicaEndpoint),
                        "connection pooling should spread independent reader sessions");
            }
        }
    }

    @Test
    void negativeReplicaOffsetsAreRejected() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer negativeLocalEnd = readReplica(new Messages.ReadResp(-1, 0), new byte[0]);
             ScpServer negativeDurable = readReplica(new Messages.ReadResp(1, -1), new byte[0]);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 0,
                             List.of(chunk(chunkId, ChunkState.OPEN, 0,
                                     new Messages.Replica(1, endpoint(negativeLocalEnd)),
                                     new Messages.Replica(2, endpoint(negativeDurable)))))))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId, StrataNamespace.of("test"));

                ScpException e = assertThrows(ScpException.class, () -> reader.read(0, 1));
                assertEquals(ErrorCode.CORRUPT_CHUNK, e.code());
            }
        }
    }

    @Test
    void malformedReplicaReadResponseIsTypedFailure() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer badReplica = new ScpServer(0, 1, 0, 0, req -> {
            if (Opcode.fromCode(req.opcode()) == Opcode.READ) {
                Messages.Read.decode(req.headerSlice());
                return ScpServer.ok(req, new byte[] {0, 0}, ByteBuffer.wrap(new byte[] {1, 2, 3}));
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + req.opcode());
        });
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 if (Opcode.fromCode(req.opcode()) == Opcode.LOOKUP_FILE) {
                     Messages.LookupFile.decode(req.headerSlice());
                     Messages.ChunkInfo chunk = new Messages.ChunkInfo(chunkId, ChunkState.SEALED, 3, 0, 1,
                             List.of(new Messages.Replica(1, endpoint(badReplica))));
                     return ScpServer.ok(req,
                             new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 1, List.of(chunk)).encode(),
                             null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + req.opcode());
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId, StrataNamespace.of("test"));

                ScpException e = assertThrows(ScpException.class, () -> reader.read(0, 3));
                assertEquals(ErrorCode.CORRUPT_CHUNK, e.code());
            }
        }
    }

    private static byte[] drain(java.nio.ByteBuffer b) {
        byte[] a = new byte[b.remaining()];
        b.get(a);
        return a;
    }

    @Test
    void borrowedReadReleasesPooledBufferOnClose() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        try (ScpServer replica = readReplica(new Messages.ReadResp(3, 3), new byte[] {1, 2, 3});
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 0,
                             List.of(chunk(chunkId, ChunkState.SEALED, 3,
                                     new Messages.Replica(1, endpoint(replica)))))))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId, StrataNamespace.of("test"));
                StrataFile.ReadResult result = reader.read(0, 3);
                Frame owner = (Frame) result.releaseHandleForTest();
                assertTrue(owner.ownsBuffer());
                assertTrue(owner.ownerRefCnt() > 0, "buffer live before close");
                assertArrayEquals(new byte[] {1, 2, 3}, drain(result.buffer()));
                result.close();
                assertEquals(0, owner.ownerRefCnt(), "close releases the pooled buffer");
                result.close(); // idempotent
                assertEquals(0, owner.ownerRefCnt());
            }
        }
    }

    @Test
    void readFailsOverToHealthyReplicaWhenAnotherIsUnreachable() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        String deadEndpoint = unusedEndpoint(); // nothing listening -> Connection refused
        try (ScpServer replica = readReplica(new Messages.ReadResp(3, 3), new byte[] {1, 2, 3});
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 0,
                             List.of(chunk(chunkId, ChunkState.SEALED, 3,
                                     new Messages.Replica(1, deadEndpoint),
                                     new Messages.Replica(2, endpoint(replica)))))))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId, StrataNamespace.of("test"));
                // readFromReplicas picks a random start replica, so repeat enough that the
                // unreachable replica is tried first at least once; every read must still
                // succeed by failing over to the healthy replica.
                for (int i = 0; i < 24; i++) {
                    try (StrataFile.ReadResult result = reader.read(0, 3)) {
                        assertArrayEquals(new byte[] {1, 2, 3}, drain(result.buffer()),
                                "read must fail over to the healthy replica (iteration " + i + ")");
                    }
                }
            }
        }
    }

    private static String unusedEndpoint() throws java.io.IOException {
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            return "127.0.0.1:" + s.getLocalPort(); // closed on return -> port refuses connections
        }
    }

    private static String endpoint(ScpServer server) {
        return "127.0.0.1:" + server.port();
    }

    private static ScpServer metadataServer(AtomicReference<Messages.LookupFileResp> lookup) throws Exception {
        return new ScpServer(0, 0, 0, 0, req -> {
            if (Opcode.fromCode(req.opcode()) == Opcode.LOOKUP_FILE) {
                Messages.LookupFile.decode(req.headerSlice());
                return ScpServer.ok(req, lookup.get().encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + req.opcode());
        });
    }

    private static ScpServer readReplica(Messages.ReadResp resp, byte[] payload) throws Exception {
        return new ScpServer(0, 1, 0, 0, req -> {
            if (Opcode.fromCode(req.opcode()) == Opcode.READ) {
                Messages.Read.decode(req.headerSlice());
                return ScpServer.ok(req, resp.encode(), ByteBuffer.wrap(payload));
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + req.opcode());
        });
    }

    private static Messages.ChunkInfo chunk(ChunkId chunkId, ChunkState state, long length,
                                            Messages.Replica... replicas) {
        return new Messages.ChunkInfo(chunkId, state, length, 0, 1, List.of(replicas));
    }

    @SuppressWarnings("unchecked")
    private static ManagedScpConnection pinnedConnection(ReaderImpl reader, String endpoint) throws Exception {
        Field field = ReaderImpl.class.getDeclaredField("pinnedConnections");
        field.setAccessible(true);
        Map<String, ManagedScpConnection> connections =
                (Map<String, ManagedScpConnection>) field.get(reader);
        return connections.get(endpoint);
    }
}
