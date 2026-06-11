package io.strata.client;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpServer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReaderImplTest {

    @Test
    void sealedMetadataLengthOverflowIsTypedCorruption() throws Exception {
        FileId fileId = FileId.random();
        Messages.LookupFileResp lookup = new Messages.LookupFileResp((byte) 0, (byte) 0, (byte) 1,
                List.of(chunk(new ChunkId(fileId, 0), ChunkState.SEALED, Long.MAX_VALUE,
                                new Messages.Replica(1, "")),
                        chunk(new ChunkId(fileId, 1), ChunkState.SEALED, 1,
                                new Messages.Replica(1, ""))));

        try (ScpServer metaServer = metadataServer(new AtomicReference<>(lookup))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId);

                ScpException e = assertThrows(ScpException.class, () -> reader.read(Long.MAX_VALUE, 1));
                assertEquals(ErrorCode.CORRUPT_CHUNK, e.code());
            }
        }
    }

    @Test
    void emptyFileReadUsesMetadataFileStateForEof() throws Exception {
        FileId fileId = FileId.random();
        AtomicReference<Messages.LookupFileResp> lookup = new AtomicReference<>(
                new Messages.LookupFileResp((byte) 0, (byte) 0, (byte) 1, List.of()));

        try (ScpServer metaServer = metadataServer(lookup)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId);

                assertTrue(reader.read(0, 1).endOfFile());

                lookup.set(new Messages.LookupFileResp((byte) 0, (byte) 0, (byte) 0, List.of()));
                reader.refresh();
                assertFalse(reader.read(0, 1).endOfFile());
            }
        }
    }

    @Test
    void sealedReadOnOpenFileDoesNotReportEof() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer replica = readReplica(new Messages.ReadResp(3, 3), new byte[] {1, 2, 3});
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     new Messages.LookupFileResp((byte) 0, (byte) 0, (byte) 0,
                             List.of(chunk(chunkId, ChunkState.SEALED, 3,
                                     new Messages.Replica(1, endpoint(replica)))))))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId);

                StrataClient.ReadResult result = reader.read(0, 3);
                assertArrayEquals(new byte[] {1, 2, 3}, result.data());
                assertFalse(result.endOfFile());
            }
        }
    }

    @Test
    void sealedReplicaShorterThanDescriptorIsRejected() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer shortReplica = readReplica(new Messages.ReadResp(3, 3), new byte[] {1, 2, 3});
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     new Messages.LookupFileResp((byte) 0, (byte) 0, (byte) 1,
                             List.of(chunk(chunkId, ChunkState.SEALED, 5,
                                     new Messages.Replica(1, endpoint(shortReplica)))))))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId);

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
                     new Messages.LookupFileResp((byte) 0, (byte) 0, (byte) 1,
                             List.of(chunk(chunkId, ChunkState.SEALED, 3,
                                     new Messages.Replica(1, endpoint(failingReplica)))))))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId);

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
                     new Messages.LookupFileResp((byte) 0, (byte) 0, (byte) 0,
                             List.of(chunk(chunkId, ChunkState.OPEN, 0,
                                     new Messages.Replica(1, endpoint(replica)))))))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId);

                StrataClient.ReadResult result = reader.read(0, 4);
                assertArrayEquals(new byte[] {1, 2, 3, 4}, result.data());
                assertFalse(result.endOfFile());
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
                     new Messages.LookupFileResp((byte) 0, (byte) 0, (byte) 0,
                             List.of(chunk(chunkId, ChunkState.OPEN, 0,
                                     new Messages.Replica(1, endpoint(negativeLocalEnd)),
                                     new Messages.Replica(2, endpoint(negativeDurable)))))))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId);

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
                             new Messages.LookupFileResp((byte) 0, (byte) 0, (byte) 1, List.of(chunk)).encode(),
                             null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + req.opcode());
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId);

                ScpException e = assertThrows(ScpException.class, () -> reader.read(0, 3));
                assertEquals(ErrorCode.CORRUPT_CHUNK, e.code());
            }
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
}
