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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StrataClientBehaviorTest {

    @Test
    void fileSpecRejectsInvalidInputs() {
        assertThrows(NullPointerException.class, () -> StrataClient.FileSpec.log(null, "/test-file"));
        assertThrows(NullPointerException.class, () -> StrataClient.FileSpec.log("test", null));
        assertThrows(IllegalArgumentException.class, () -> new StrataClient.FileSpec("", "/test-file"));
        assertThrows(IllegalArgumentException.class, () -> new StrataClient.FileSpec("test", "relative"));

        StrataClient.FilePath path = new StrataClient.FilePath("test", "/test-file");
        assertEquals("test", path.namespace().toString());
        assertEquals("/test-file", path.path().toString());
        assertThrows(NullPointerException.class,
                () -> new StrataClient.FilePath(null, io.strata.common.StrataPath.of("/test-file")));
        assertThrows(NullPointerException.class,
                () -> new StrataClient.FilePath(io.strata.common.StrataNamespace.of("test"), null));
        assertThrows(NullPointerException.class, () -> StrataClient.connect(null));
    }

    @Test
    void createAndDeletePropagateMetadataResponses() throws Exception {
        FileId created = FileId.random();
        FileId denied = FileId.random();
        try (ScpServer meta = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.CREATE_FILE) {
                return ScpServer.ok(req, new Messages.CreateFileResp(created).encode(), null);
            }
            if (op == Opcode.DELETE_FILES) {
                var msg = Messages.DeleteFiles.decode(req.headerSlice());
                return ScpServer.ok(req,
                        new Messages.DeleteFilesResp(msg.fileIds(),
                                List.of(ErrorCode.OK.code, ErrorCode.FILE_NOT_FOUND.code)).encode(),
                        null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StrataClient client = StrataClient.connect(ClientConfig.of(endpoint(meta)))) {
            assertEquals(created, client.create(StrataClient.FileSpec.log("test", "/test-file")).id());

            ScpException e = assertThrows(ScpException.class,
                    () -> client.deleteById(List.of(created, denied)));
            assertEquals(ErrorCode.FILE_NOT_FOUND, e.code());
        }
    }

    @Test
    void openByPathResolvesFileAndExposesPathOnHandle() throws Exception {
        FileId fileId = FileId.random();
        try (ScpServer meta = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.LOOKUP_PATH) {
                var lookup = Messages.LookupPath.decode(req.headerSlice());
                assertEquals("kafka-a", lookup.namespace().toString());
                assertEquals("/tenant/topic/segment-0", lookup.path().toString());
                return ScpServer.ok(req, new Messages.LookupPathResp(fileId).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StrataClient client = StrataClient.connect(ClientConfig.of(endpoint(meta)))) {
            StrataFile file = client.open("kafka-a", "/tenant/topic/segment-0");

            assertEquals(fileId, file.id());
            assertEquals("kafka-a", file.namespace().toString());
            assertEquals("/tenant/topic/segment-0", file.path().toString());
        }
    }

    @Test
    void deleteByPathResolvesCurrentPathBinding() throws Exception {
        FileId fileId = FileId.random();
        AtomicInteger deletes = new AtomicInteger();
        try (ScpServer meta = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.LOOKUP_PATH) {
                var lookup = Messages.LookupPath.decode(req.headerSlice());
                assertEquals("kafka-a", lookup.namespace().toString());
                assertEquals("/tenant/topic/segment-1", lookup.path().toString());
                return ScpServer.ok(req, new Messages.LookupPathResp(fileId).encode(), null);
            }
            if (op == Opcode.DELETE_FILES) {
                deletes.incrementAndGet();
                assertEquals(List.of(fileId), Messages.DeleteFiles.decode(req.headerSlice()).fileIds());
                return ScpServer.ok(req,
                        new Messages.DeleteFilesResp(List.of(fileId), List.of(ErrorCode.OK.code)).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StrataClient client = StrataClient.connect(ClientConfig.of(endpoint(meta)))) {
            client.delete("kafka-a", "/tenant/topic/segment-1");

            assertEquals(1, deletes.get());
        }
    }

    @Test
    void deleteFileHandleUsesImmutableFileId() throws Exception {
        FileId fileId = FileId.random();
        AtomicInteger deletes = new AtomicInteger();
        try (ScpServer meta = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.CREATE_FILE) {
                return ScpServer.ok(req, new Messages.CreateFileResp(fileId).encode(), null);
            }
            if (op == Opcode.DELETE_FILES) {
                deletes.incrementAndGet();
                assertEquals(List.of(fileId), Messages.DeleteFiles.decode(req.headerSlice()).fileIds());
                return ScpServer.ok(req,
                        new Messages.DeleteFilesResp(List.of(fileId), List.of(ErrorCode.OK.code)).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StrataClient client = StrataClient.connect(ClientConfig.of(endpoint(meta)))) {
            StrataFile file = client.create(StrataClient.FileSpec.log("test", "/tenant/topic/segment-2"));

            client.delete(file);

            assertEquals(1, deletes.get());
        }
    }

    @Test
    void malformedMetadataSuccessBodyBecomesTypedClientError() throws Exception {
        try (ScpServer meta = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.CREATE_FILE) {
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StrataClient client = StrataClient.connect(ClientConfig.of(endpoint(meta)))) {
            ScpException e = assertThrows(ScpException.class,
                    () -> client.create(StrataClient.FileSpec.log("test", "/test-file")));
            assertEquals(ErrorCode.INTERNAL, e.code());
        }
    }

    @Test
    void deleteRejectsMetadataResponsesThatDoNotMatchTheRequest() throws Exception {
        FileId first = FileId.random();
        FileId second = FileId.random();
        try (ScpServer meta = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.DELETE_FILES) {
                Messages.DeleteFiles.decode(req.headerSlice());
                return ScpServer.ok(req,
                        new Messages.DeleteFilesResp(List.of(first), List.of(ErrorCode.OK.code)).encode(),
                        null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StrataClient client = StrataClient.connect(ClientConfig.of(endpoint(meta)))) {
            ScpException e = assertThrows(ScpException.class,
                    () -> client.deleteById(List.of(first, second)));
            assertEquals(ErrorCode.INTERNAL, e.code());
        }
    }

    @Test
    void deleteRejectsCodeCountMismatches() throws Exception {
        FileId first = FileId.random();
        FileId second = FileId.random();
        try (ScpServer meta = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.DELETE_FILES) {
                Messages.DeleteFiles.decode(req.headerSlice());
                return ScpServer.ok(req,
                        new Messages.DeleteFilesResp(List.of(first, second), List.of(ErrorCode.OK.code)).encode(),
                        null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StrataClient client = StrataClient.connect(ClientConfig.of(endpoint(meta)))) {
            ScpException e = assertThrows(ScpException.class,
                    () -> client.deleteById(List.of(first, second)));
            assertEquals(ErrorCode.INTERNAL, e.code());
        }
    }

    @Test
    void openForAppendRejectsCorruptOrIncompatibleDescriptors() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Messages.LookupFileResp> lookup = new AtomicReference<>();
        try (ScpServer meta = metadataServer(lookup);
             StrataClient client = StrataClient.connect(ClientConfig.of(endpoint(meta)))) {
            lookup.set(new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 1, List.of()));
            assertEquals(ErrorCode.FILE_SEALED,
                    assertThrows(ScpException.class, () -> client.openById(fileId).openForAppend()).code());

            lookup.set(new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 0,
                    List.of(chunk(chunkId, ChunkState.OPEN, 0, 0, 1))));
            assertEquals(ErrorCode.INTERNAL,
                    assertThrows(ScpException.class, () -> client.openById(fileId).openForAppend()).code());

            lookup.set(new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 0,
                    List.of(chunk(chunkId, ChunkState.SEALED, -1, 0, 1))));
            assertEquals(ErrorCode.CORRUPT_CHUNK,
                    assertThrows(ScpException.class, () -> client.openById(fileId).openForAppend()).code());

            lookup.set(new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 0,
                    List.of(chunk(new ChunkId(fileId, 0), ChunkState.SEALED, Long.MAX_VALUE, 0, 1),
                            chunk(new ChunkId(fileId, 1), ChunkState.SEALED, 1, 0, 1))));
            assertEquals(ErrorCode.CORRUPT_CHUNK,
                    assertThrows(ScpException.class, () -> client.openById(fileId).openForAppend()).code());
        }
    }

    @Test
    void readerHandlesSealedOpenAndUnreadableReplicaCases() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Messages.LookupFileResp> lookup = new AtomicReference<>();
        try (ScpServer storage = new ScpServer(0, 1, 0, 0, req -> {
            var read = Messages.Read.decode(req.headerSlice());
            if (read.offset() == 0 && read.maxBytes() == 3) {
                return ScpServer.ok(req, new Messages.ReadResp(3, 3).encode(),
                        ByteBuffer.wrap(new byte[] {1, 2, 3}));
            }
            return ScpServer.ok(req, new Messages.ReadResp(10, 2).encode(),
                    ByteBuffer.wrap(new byte[] {4, 5, 6, 7}));
        });
             ScpServer meta = metadataServer(lookup);
             StrataClient client = StrataClient.connect(ClientConfig.of(endpoint(meta)))) {
            lookup.set(new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 1,
                    List.of(chunk(chunkId, ChunkState.SEALED, 3, 0, 1,
                            new Messages.Replica(1, endpoint(storage))))));
            try (StrataFile.Reader reader = client.openById(fileId).openForRead()) {
                StrataFile.ReadResult result = reader.read(0, 10);
                assertArrayEquals(new byte[] {1, 2, 3}, result.data());
                assertEquals(true, result.endOfFile());
            }

            lookup.set(new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 0,
                    List.of(chunk(chunkId, ChunkState.OPEN, 0, 0, 1,
                            new Messages.Replica(1, endpoint(storage))))));
            try (StrataFile.Reader reader = client.openById(fileId).openForRead()) {
                StrataFile.ReadResult result = reader.read(0, 4);
                assertArrayEquals(new byte[] {4, 5}, result.data());
                assertEquals(false, result.endOfFile());
            }

            lookup.set(new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 1,
                    List.of(chunk(chunkId, ChunkState.SEALED, 3, 0, 1,
                            new Messages.Replica(2, "")))));
            try (StrataFile.Reader reader = client.openById(fileId).openForRead()) {
                assertEquals(ErrorCode.INTERNAL,
                        assertThrows(ScpException.class, () -> reader.read(0, 10)).code());
            }
        }
    }

    @Test
    void readerRejectsCorruptDescriptorLengths() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Messages.LookupFileResp> lookup = new AtomicReference<>();
        try (ScpServer meta = metadataServer(lookup);
             StrataClient client = StrataClient.connect(ClientConfig.of(endpoint(meta)))) {
            lookup.set(new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 1,
                    List.of(chunk(chunkId, ChunkState.SEALED, -1, 0, 1,
                            new Messages.Replica(1, "")))));
            try (StrataFile.Reader reader = client.openById(fileId).openForRead()) {
                assertEquals(ErrorCode.CORRUPT_CHUNK,
                        assertThrows(ScpException.class, () -> reader.read(0, 10)).code());
            }
        }
    }

    @Test
    void readerRejectsNegativeArgumentsBeforeCallingStorage() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Messages.LookupFileResp> lookup = new AtomicReference<>();
        AtomicInteger reads = new AtomicInteger();
        try (ScpServer storage = new ScpServer(0, 1, 0, 0, req -> {
            reads.incrementAndGet();
            return ScpServer.ok(req, new Messages.ReadResp(3, 3).encode(), ByteBuffer.allocate(0));
        });
             ScpServer meta = metadataServer(lookup);
             StrataClient client = StrataClient.connect(ClientConfig.of(endpoint(meta)))) {
            lookup.set(new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 1,
                    List.of(chunk(chunkId, ChunkState.SEALED, 3, 0, 1,
                            new Messages.Replica(1, endpoint(storage))))));
            try (StrataFile.Reader reader = client.openById(fileId).openForRead()) {
                assertEquals(ErrorCode.INTERNAL,
                        assertThrows(ScpException.class, () -> reader.read(-1, 10)).code());
                assertEquals(ErrorCode.INTERNAL,
                        assertThrows(ScpException.class, () -> reader.read(0, -1)).code());
                assertEquals(0, reads.get());
            }
        }
    }

    @Test
    void readerRejectsMalformedReplicaReadResponses() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Messages.LookupFileResp> lookup = new AtomicReference<>();
        try (ScpServer overlong = new ScpServer(0, 1, 0, 0, req ->
                ScpServer.ok(req, new Messages.ReadResp(3, 3).encode(),
                        ByteBuffer.wrap(new byte[] {1, 2, 3, 4})));
             ScpServer badOffsets = new ScpServer(0, 2, 0, 0, req ->
                     ScpServer.ok(req, new Messages.ReadResp(2, 3).encode(),
                             ByteBuffer.wrap(new byte[] {1})));
             ScpServer shortSealed = new ScpServer(0, 3, 0, 0, req ->
                     ScpServer.ok(req, new Messages.ReadResp(3, 3).encode(),
                             ByteBuffer.wrap(new byte[] {1, 2})));
             ScpServer meta = metadataServer(lookup);
             StrataClient client = StrataClient.connect(ClientConfig.of(endpoint(meta)))) {
            lookup.set(new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 1,
                    List.of(chunk(chunkId, ChunkState.SEALED, 3, 0, 1,
                            new Messages.Replica(1, endpoint(overlong))))));
            try (StrataFile.Reader reader = client.openById(fileId).openForRead()) {
                assertEquals(ErrorCode.CORRUPT_CHUNK,
                        assertThrows(ScpException.class, () -> reader.read(0, 3)).code());
            }

            lookup.set(new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 1,
                    List.of(chunk(chunkId, ChunkState.SEALED, 3, 0, 1,
                            new Messages.Replica(3, endpoint(shortSealed))))));
            try (StrataFile.Reader reader = client.openById(fileId).openForRead()) {
                assertEquals(ErrorCode.CORRUPT_CHUNK,
                        assertThrows(ScpException.class, () -> reader.read(0, 3)).code());
            }

            lookup.set(new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 0,
                    List.of(chunk(chunkId, ChunkState.OPEN, 0, 0, 1,
                            new Messages.Replica(2, endpoint(badOffsets))))));
            try (StrataFile.Reader reader = client.openById(fileId).openForRead()) {
                assertEquals(ErrorCode.CORRUPT_CHUNK,
                        assertThrows(ScpException.class, () -> reader.read(0, 3)).code());
            }
        }
    }

    private static ScpServer metadataServer(AtomicReference<Messages.LookupFileResp> lookup) throws Exception {
        return new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.LOOKUP_FILE) {
                Messages.LookupFile.decode(req.headerSlice());
                return ScpServer.ok(req, lookup.get().encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static Messages.ChunkInfo chunk(ChunkId id, ChunkState state, long length, int crc,
                                            int epoch, Messages.Replica... replicas) {
        return new Messages.ChunkInfo(id, state, length, crc, epoch, List.of(replicas));
    }

    private static String endpoint(ScpServer server) {
        return "127.0.0.1:" + server.port();
    }
}
