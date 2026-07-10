package io.strata.client;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Crc;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecoveryTest {

    @Test
    void sealedChunkLengthValidationRejectsNegativeAndOverflow() throws Exception {
        FileId fileId = FileId.of(1);
        ChunkId c0 = new ChunkId(fileId, 0);
        ChunkId c1 = new ChunkId(fileId, 1);
        AtomicReference<Messages.LookupFileResp> lookup = new AtomicReference<>();

        try (ScpServer metaServer = metadataServer(lookup, null)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                Recovery recovery = new Recovery(meta, pool, config, StrataNamespace.of("test"));

                lookup.set(lookup(chunk(c0, ChunkState.SEALED, -1, 1)));
                assertEquals(ErrorCode.CORRUPT_CHUNK,
                        assertThrows(ScpException.class, () -> recovery.recoverAndSeal(fileId, 2)).code());

                lookup.set(lookup(
                        chunk(c0, ChunkState.SEALED, Long.MAX_VALUE, 1),
                        chunk(c1, ChunkState.SEALED, 1, 1)));
                assertEquals(ErrorCode.CORRUPT_CHUNK,
                        assertThrows(ScpException.class, () -> recovery.recoverAndSeal(fileId, 2)).code());
            }
        }
    }

    @Test
    void openChunkWithNoReachableReplicasFailsQuorumBeforeDataNodeUse() throws Exception {
        FileId fileId = FileId.of(2);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Messages.LookupFileResp> lookup = new AtomicReference<>(
                lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                        new Messages.Replica(1, ""),
                        new Messages.Replica(2, ""))));

        try (ScpServer metaServer = metadataServer(lookup, null)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));
                assertEquals(ErrorCode.INTERNAL, e.code());
            }
        }
    }

    @Test
    void unreachableOpenTailDoesNotSealZeroWithoutQuorumEvidence() throws Exception {
        FileId fileId = FileId.of(3);
        ChunkId sealedChunk = new ChunkId(fileId, 0);
        ChunkId abandonedTail = new ChunkId(fileId, 1);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicReference<Messages.LookupFileResp> lookup = new AtomicReference<>(
                lookup(
                        chunk(sealedChunk, ChunkState.SEALED, 4, 1),
                        chunk(abandonedTail, ChunkState.OPEN, 0, 1,
                                new Messages.Replica(1, ""),
                                new Messages.Replica(2, ""))));

        try (ScpServer metaServer = metadataServer(lookup, sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));
                assertEquals(ErrorCode.INTERNAL, e.code(),
                        "open chunk metadata length is not proof that the tail never held acked bytes");
                assertEquals(null, sealedFileLength.get());
            }
        }
    }

    @Test
    void deletingFileRecoveryFailsBeforeFencingReplicas() throws Exception {
        FileId fileId = FileId.of(4);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean dataNodeTouched = new AtomicBoolean(false);

        try (ScpServer dataNode = new ScpServer(0, 1, 0, 0, req -> {
            dataNodeTouched.set(true);
            throw new ScpException(ErrorCode.INTERNAL, "data node should not be contacted");
        });
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 2,
                             List.of(chunk(chunkId, ChunkState.OPEN, 0, 1,
                                     new Messages.Replica(1, endpoint(dataNode)))))), null)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));
                assertEquals(ErrorCode.PRECONDITION_FAILED, e.code());
                assertFalse(dataNodeTouched.get());
            }
        }
    }

    @Test
    void unknownFileStateRecoveryFailsBeforeFencingReplicas() throws Exception {
        FileId fileId = FileId.of(5);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean dataNodeTouched = new AtomicBoolean(false);

        try (ScpServer dataNode = new ScpServer(0, 1, 0, 0, req -> {
            dataNodeTouched.set(true);
            throw new ScpException(ErrorCode.INTERNAL, "data node should not be contacted");
        });
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 99,
                             List.of(chunk(chunkId, ChunkState.OPEN, 0, 1,
                                     new Messages.Replica(1, endpoint(dataNode)))))), null)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertFalse(dataNodeTouched.get());
            }
        }
    }

    @Test
    void fencedEpochFromReplicaIsPropagated() throws Exception {
        FileId fileId = FileId.of(6);
        ChunkId chunkId = new ChunkId(fileId, 0);
        try (ScpServer dataNode = new ScpServer(0, 1, 0, 0, req -> {
            if (Opcode.fromCode(req.opcode()) == Opcode.FENCE) {
                throw new ScpException(ErrorCode.FENCED_EPOCH, "fenced", 7);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
        });
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(dataNode))))), null)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));
                assertEquals(ErrorCode.FENCED_EPOCH, e.code());
                assertEquals(7, e.detail());
            }
        }
    }

    @Test
    void fencedEpochFromLedgerReadIsPropagated() throws Exception {
        FileId fileId = FileId.of(7);
        ChunkId chunkId = new ChunkId(fileId, 0);
        try (ScpServer first = replicaThatFencesOnLedger(1, 3);
             ScpServer second = replicaThatFencesOnLedger(2, 3);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(first)),
                             new Messages.Replica(2, endpoint(second))))), null)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test"))
                                .recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.FENCED_EPOCH, e.code());
                assertEquals(3, e.detail());
            }
        }
    }

    @Test
    void catchUpEvictsReplicaWhenDonorReadFails() throws Exception {
        FileId fileId = FileId.of(7);
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer donor = new ScpServer(0, 1, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, 4, 4, ChunkState.OPEN).encode(), null);
            }
            if (isReadOp(op)) {
                throw new ScpException(ErrorCode.INTERNAL, "read failed");
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             ScpServer lagging = new ScpServer(0, 2, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.FENCE) {
                     return ScpServer.ok(req, new Messages.FenceResp(2, 0, 0, ChunkState.OPEN).encode(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(donor)),
                             new Messages.Replica(2, endpoint(lagging))))), null)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));
                assertEquals(ErrorCode.INTERNAL, e.code());
            }
        }
    }

    @Test
    void sealQuorumLossReturnsLastSealError() throws Exception {
        FileId fileId = FileId.of(8);
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer s1 = openReplicaThatFailsSeal(1, "first seal failed");
             ScpServer s2 = openReplicaThatFailsSeal(2, "second seal failed");
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(s1)),
                             new Messages.Replica(2, endpoint(s2))))), null)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));
                assertEquals(ErrorCode.INTERNAL, e.code());
                Assertions.assertTrue(e.getMessage().contains("second seal failed"));
            }
        }
    }

    @Test
    void recoverySealRejectsQuorumThatReportsWrongFinalLength() throws Exception {
        FileId fileId = FileId.of(9);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer s1 = openReplicaThatSealsAt(1, 1);
             ScpServer s2 = openReplicaThatSealsAt(2, 1);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(s1)),
                             new Messages.Replica(2, endpoint(s2))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));
                assertEquals(ErrorCode.CORRUPT_CHUNK, e.code());
                assertEquals(null, sealedFileLength.get());
            }
        }
    }

    @Test
    void recoveryMalformedSealResponseCountsAgainstQuorum() throws Exception {
        FileId fileId = FileId.of(10);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer malformed = openReplicaWithMalformedSeal(1);
             ScpServer valid = openReplicaThatSealsAt(2, 0);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(malformed)),
                             new Messages.Replica(2, endpoint(valid))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertEquals(null, sealedFileLength.get());
            }
        }
    }

    @Test
    void recoveryRejectsMalformedFenceOffsetsBeforeCountingReplica() throws Exception {
        FileId fileId = FileId.of(11);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer malformed = new ScpServer(0, 1, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, 0, -1, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of()).encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                return ScpServer.ok(req, new Messages.SealResp(0, 123).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             ScpServer valid = openReplicaThatSealsAt(2, 0);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(malformed)),
                             new Messages.Replica(2, endpoint(valid))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertEquals(null, sealedFileLength.get());
            }
        }
    }

    @Test
    void recoveryRejectsMalformedFenceHeaderBeforeCountingReplica() throws Exception {
        FileId fileId = FileId.of(12);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer malformed = new ScpServer(0, 1, 0, 0, req -> {
            if (Opcode.fromCode(req.opcode()) == Opcode.FENCE) {
                return ScpServer.ok(req, new byte[] {0, 0}, null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
        });
             ScpServer valid = openReplicaThatSealsAt(2, 0);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(malformed)),
                             new Messages.Replica(2, endpoint(valid))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertEquals(null, sealedFileLength.get());
            }
        }
    }

    @Test
    void recoveryRejectsFenceDurableOffsetBeyondReplicaEnd() throws Exception {
        FileId fileId = FileId.of(13);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer malformed = new ScpServer(0, 1, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, 4, 8, ChunkState.OPEN).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             ScpServer valid = openReplicaThatSealsAt(2, 0);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(malformed)),
                             new Messages.Replica(2, endpoint(valid))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertEquals(null, sealedFileLength.get());
            }
        }
    }

    @Test
    void recoveryEvictsReplicaWhenCatchUpAppendReturnsWrongEnd() throws Exception {
        FileId fileId = FileId.of(14);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer donor = new ScpServer(0, 1, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, 4, 4, ChunkState.OPEN).encode(), null);
            }
            if (isReadOp(op)) {
                return ScpServer.ok(req, new Messages.ReadResp(4, 4).encode(),
                        ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of()).encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                return ScpServer.ok(req, new Messages.SealResp(4, 123).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             ScpServer lagging = new ScpServer(0, 2, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.FENCE) {
                     return ScpServer.ok(req, new Messages.FenceResp(2, 0, 0, ChunkState.OPEN).encode(), null);
                 }
                 if (op == Opcode.APPEND) {
                     return ScpServer.ok(req, new Messages.AppendResp(3).encode(), null);
                 }
                 if (op == Opcode.READ_LEDGER) {
                     return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of()).encode(), null);
                 }
                 if (op == Opcode.SEAL_CHUNK) {
                     return ScpServer.ok(req, new Messages.SealResp(4, 123).encode(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(donor)),
                             new Messages.Replica(2, endpoint(lagging))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertEquals(null, sealedFileLength.get());
            }
        }
    }

    @Test
    void recoveryRejectsMalformedReadOffsetsDuringCatchUp() throws Exception {
        FileId fileId = FileId.of(15);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer donor = new ScpServer(0, 1, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, 4, 4, ChunkState.OPEN).encode(), null);
            }
            if (isReadOp(op)) {
                return ScpServer.ok(req, new Messages.ReadResp(4, 8).encode(),
                        ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of()).encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                return ScpServer.ok(req, new Messages.SealResp(4, 123).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             ScpServer lagging = new ScpServer(0, 2, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.FENCE) {
                     return ScpServer.ok(req, new Messages.FenceResp(2, 0, 0, ChunkState.OPEN).encode(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(donor)),
                             new Messages.Replica(2, endpoint(lagging))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertEquals(null, sealedFileLength.get());
            }
        }
    }

    @Test
    void invalidFirstLedgerBoundaryAbortsInsteadOfSkippingToLaterBoundary() throws Exception {
        FileId fileId = FileId.of(16);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        byte[] full = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
        int forgedFullRangeCrc = Crc.of(full);

        try (ScpServer s1 = replicaWithForgedLaterLedger(1, full, forgedFullRangeCrc);
             ScpServer s2 = replicaWithForgedLaterLedger(2, full, forgedFullRangeCrc);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(s1)),
                             new Messages.Replica(2, endpoint(s2))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertTrue(e.getMessage().contains("unverified above-floor holder"));
                assertEquals(null, sealedFileLength.get());
            }
        }
    }

    @Test
    void sealedReplicaLengthBecomesRecoverySealPoint() throws Exception {
        FileId fileId = FileId.of(17);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer sealed = sealedFenceReplica(1, 5, 123);
             ScpServer open = openReplicaWithFenceAndSeal(2, 5, 5, 5, 123);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(sealed)),
                             new Messages.Replica(2, endpoint(open))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2);

                assertEquals(5, sealedInfo.sealedLength());
                assertEquals(5L, sealedFileLength.get());
            }
        }
    }

    @Test
    void sealedReplicaShorterThanDurableFloorIsEvicted() throws Exception {
        FileId fileId = FileId.of(18);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer shortSealed = sealedFenceReplica(1, 2, 123);
             ScpServer validA = openReplicaWithFenceAndSeal(2, 4, 4, 4, 456);
             ScpServer validB = openReplicaWithFenceAndSeal(3, 4, 4, 4, 456);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(shortSealed)),
                             new Messages.Replica(2, endpoint(validA)),
                             new Messages.Replica(3, endpoint(validB))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2);

                assertEquals(4, sealedInfo.sealedLength());
                assertEquals(4L, sealedFileLength.get());
            }
        }
    }

    @Test
    void singleReplicaLedgerContinuationPastDurableFloorIsDiscardedWhenAllReplicasReachable() throws Exception {
        // Counterpart to issue #29: when ALL placed replicas are reachable and only one holds the
        // continuation above the floor, that batch could not have reached ackQuorum, so it was never
        // producer-acked — recovery must discard it (seal at the floor), not manufacture a tail. The
        // re-replicate path only fires when the unreachable replica count leaves room for the batch to
        // have been acked on a holder we cannot see (see ackedTailSurvivesSingleHolderLossDuringSealRecovery).
        FileId fileId = FileId.of(19);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean laggingAppend = new AtomicBoolean();
        byte[] data = new byte[] {1, 2, 3, 4};
        int crc = Crc.of(data);

        try (ScpServer donor = ledgerOrderingReplica(1, data.length,
                     List.of(new Messages.LedgerEntry(data.length, crc, 1)), data, null);
             ScpServer lagging = ledgerOrderingReplica(2, 0, List.of(), data, laggingAppend);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(donor)),
                             new Messages.Replica(2, endpoint(lagging))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2);

                assertEquals(0, sealedInfo.sealedLength());
                assertEquals(0L, sealedFileLength.get());
                assertFalse(laggingAppend.get());
            }
        }
    }

    @Test
    void ackedTailSurvivesSingleHolderLossDuringSealRecovery() throws Exception {
        // Issue #29 end-to-end shape: RF=3 / ackQuorum=2. The batch ending at 4 was acked on two
        // holders; one of them (R2) is unreachable during recovery — a single node fault. Only the
        // surviving holder R1 still proves the batch; R3 lags at the durable floor. Recovery must
        // re-replicate the acked tail to R3 and seal at 4, not truncate to the floor (which would
        // be a single-fault acked-data loss).
        FileId fileId = FileId.of(37);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean laggingAppend = new AtomicBoolean();
        byte[] data = new byte[] {1, 2, 3, 4};
        int crc = Crc.of(data);

        try (ScpServer holder = ledgerOrderingReplica(1, data.length,
                     List.of(new Messages.LedgerEntry(data.length, crc, 1)), data, null);
             ScpServer lagging = ledgerOrderingReplica(3, 0, List.of(), data, laggingAppend);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(holder)),
                             new Messages.Replica(2, "127.0.0.1:1"),
                             new Messages.Replica(3, endpoint(lagging))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2);

                assertEquals(4, sealedInfo.sealedLength(),
                        "a producer-acked tail must survive a single holder loss");
                assertEquals(4L, sealedFileLength.get());
                assertTrue(laggingAppend.get(),
                        "the surviving CRC-valid copy must be re-replicated to the lagging replica");
            }
        }
    }

    @Test
    void emptyEndpointDescriptorBlocksInsteadOfTruncatingAckedTail() throws Exception {
        FileId fileId = FileId.of(52);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean laggingAppend = new AtomicBoolean();
        AtomicBoolean holderSeal = new AtomicBoolean();
        AtomicBoolean laggingSeal = new AtomicBoolean();
        byte[] data = new byte[] {1, 2, 3, 4};
        List<Messages.LedgerEntry> ledger = List.of(
                new Messages.LedgerEntry(data.length, Crc.of(data), 1));

        try (ScpServer holder = ledgerOrderingReplica(1, data.length, ledger, data, null, holderSeal);
             ScpServer lagging = ledgerOrderingReplica(3, 0, List.of(), data, laggingAppend, laggingSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(holder)),
                             new Messages.Replica(2, ""),
                             new Messages.Replica(3, endpoint(lagging))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test"))
                                .recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertTrue(e.getMessage().contains("unresolved replica classification"));
                assertEquals(null, sealedFileLength.get());
                assertFalse(laggingAppend.get());
                assertFalse(holderSeal.get());
                assertFalse(laggingSeal.get());
            }
        }
    }

    @Test
    void chunkNotFoundFenceBlocksInsteadOfPromotingSingleReplicaTail() throws Exception {
        FileId fileId = FileId.of(53);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean laggingAppend = new AtomicBoolean();
        AtomicBoolean holderSeal = new AtomicBoolean();
        AtomicBoolean laggingSeal = new AtomicBoolean();
        AtomicBoolean missingFenceCalled = new AtomicBoolean();
        byte[] data = new byte[] {1, 2, 3, 4};
        List<Messages.LedgerEntry> ledger = List.of(
                new Messages.LedgerEntry(data.length, Crc.of(data), 1));

        try (ScpServer holder = ledgerOrderingReplica(1, data.length, ledger, data, null, holderSeal);
             ScpServer lagging = ledgerOrderingReplica(2, 0, List.of(), data, laggingAppend, laggingSeal);
             ScpServer missing = new ScpServer(0, 3, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.FENCE) {
                     missingFenceCalled.set(true);
                     throw new ScpException(ErrorCode.CHUNK_NOT_FOUND, "chunk is not installed");
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(holder)),
                             new Messages.Replica(2, endpoint(lagging)),
                             new Messages.Replica(3, endpoint(missing))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test"))
                                .recoverAndSeal(fileId, 2));

                assertTrue(missingFenceCalled.get());
                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertTrue(e.getMessage().contains("unresolved replica classification"));
                assertEquals(null, sealedFileLength.get());
                assertFalse(laggingAppend.get());
                assertFalse(holderSeal.get());
                assertFalse(laggingSeal.get());
            }
        }
    }

    @Test
    void malformedFenceResponseBlocksInsteadOfPromotingSingleReplicaTail() throws Exception {
        FileId fileId = FileId.of(57);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean laggingAppend = new AtomicBoolean();
        AtomicBoolean holderSeal = new AtomicBoolean();
        AtomicBoolean laggingSeal = new AtomicBoolean();
        byte[] data = new byte[] {1, 2, 3, 4};
        List<Messages.LedgerEntry> ledger = List.of(
                new Messages.LedgerEntry(data.length, Crc.of(data), 1));

        try (ScpServer holder = ledgerOrderingReplica(1, data.length, ledger, data, null, holderSeal);
             ScpServer lagging = ledgerOrderingReplica(2, 0, List.of(), data, laggingAppend, laggingSeal);
             ScpServer malformed = new ScpServer(0, 3, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.FENCE) {
                     return ScpServer.ok(req, new byte[] {0}, null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(holder)),
                             new Messages.Replica(2, endpoint(lagging)),
                             new Messages.Replica(3, endpoint(malformed))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test"))
                                .recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertEquals(null, sealedFileLength.get());
                assertFalse(laggingAppend.get());
                assertFalse(holderSeal.get());
                assertFalse(laggingSeal.get());
            }
        }
    }

    @Test
    void sealedReplicaFastPathBlocksOnUnresolvedHigherTail() throws Exception {
        FileId fileId = FileId.of(58);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean sealedReplicaSeal = new AtomicBoolean();
        AtomicBoolean higherReplicaSeal = new AtomicBoolean();
        byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
        List<Messages.LedgerEntry> ledger = List.of(
                new Messages.LedgerEntry(data.length, Crc.of(data), 1));

        try (ScpServer sealed = sealedFenceReplica(1, 4, 777, sealedReplicaSeal);
             ScpServer higher = ledgerOrderingReplica(2, data.length, ledger, data, null, higherReplicaSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(sealed)),
                             new Messages.Replica(2, endpoint(higher)),
                             new Messages.Replica(3, "")))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test"))
                                .recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertEquals(null, sealedFileLength.get());
                assertFalse(sealedReplicaSeal.get());
                assertFalse(higherReplicaSeal.get());
            }
        }
    }

    @Test
    void sealedReplicaFastPathBlocksOnTransportUnreachableHigherTailHolder() throws Exception {
        FileId fileId = FileId.of(59);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean sealedReplicaSeal = new AtomicBoolean();
        AtomicBoolean higherReplicaSeal = new AtomicBoolean();
        byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
        List<Messages.LedgerEntry> ledger = List.of(
                new Messages.LedgerEntry(data.length, Crc.of(data), 1));

        try (ScpServer sealed = sealedFenceReplica(1, 4, 777, sealedReplicaSeal);
             ScpServer higher = ledgerOrderingReplica(2, data.length, ledger, data, null, higherReplicaSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(sealed)),
                             new Messages.Replica(2, endpoint(higher)),
                             new Messages.Replica(3, "127.0.0.1:1")))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test"))
                                .recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertEquals(null, sealedFileLength.get());
                assertFalse(sealedReplicaSeal.get());
                assertFalse(higherReplicaSeal.get());
            }
        }
    }

    @Test
    void sealedReplicaFastPathBlocksOnReachableHigherTailQuorum() throws Exception {
        FileId fileId = FileId.of(60);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean sealedReplicaSeal = new AtomicBoolean();
        AtomicBoolean higherASeal = new AtomicBoolean();
        AtomicBoolean higherBSeal = new AtomicBoolean();
        byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
        List<Messages.LedgerEntry> ledger = List.of(
                new Messages.LedgerEntry(data.length, Crc.of(data), 1));

        try (ScpServer sealed = sealedFenceReplica(1, 4, 777, sealedReplicaSeal);
             ScpServer higherA = ledgerOrderingReplica(2, data.length, ledger, data, null, higherASeal);
             ScpServer higherB = ledgerOrderingReplica(3, data.length, ledger, data, null, higherBSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(sealed)),
                             new Messages.Replica(2, endpoint(higherA)),
                             new Messages.Replica(3, endpoint(higherB))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test"))
                                .recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertEquals(null, sealedFileLength.get());
                assertFalse(sealedReplicaSeal.get());
                assertFalse(higherASeal.get());
                assertFalse(higherBSeal.get());
            }
        }
    }

    @Test
    void sealedReplicaFastPathAcceptsSealedPointWhenSingleHigherTailCannotReachQuorum() throws Exception {
        FileId fileId = FileId.of(61);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean sealedReplicaSeal = new AtomicBoolean();
        AtomicBoolean higherReplicaSeal = new AtomicBoolean();
        AtomicBoolean matchingReplicaSeal = new AtomicBoolean();
        byte[] higherData = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
        byte[] matchingData = new byte[] {1, 2, 3, 4};
        List<Messages.LedgerEntry> higherLedger = List.of(
                new Messages.LedgerEntry(higherData.length, Crc.of(higherData), 1));
        List<Messages.LedgerEntry> matchingLedger = List.of(
                new Messages.LedgerEntry(matchingData.length, Crc.of(matchingData), 1));

        try (ScpServer sealed = sealedFenceReplica(1, matchingData.length, 777, sealedReplicaSeal);
             ScpServer higher = ledgerOrderingReplica(2, higherData.length, higherLedger, higherData,
                     null, higherReplicaSeal);
             ScpServer matching = ledgerOrderingReplica(3, matchingData.length, matchingLedger, matchingData,
                     null, matchingReplicaSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(sealed)),
                             new Messages.Replica(2, endpoint(higher)),
                             new Messages.Replica(3, endpoint(matching))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo seal = new Recovery(meta, pool, config, StrataNamespace.of("test"))
                        .recoverAndSeal(fileId, 2);

                assertEquals(matchingData.length, seal.sealedLength());
                assertEquals((long) matchingData.length, sealedFileLength.get());
                assertTrue(sealedReplicaSeal.get());
                assertTrue(higherReplicaSeal.get());
                assertTrue(matchingReplicaSeal.get());
            }
        }
    }

    @Test
    void unsafeSealOverrideAllowsFloorSealWithUnresolvedDescriptor() throws Exception {
        FileId fileId = FileId.of(54);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean laggingAppend = new AtomicBoolean();
        AtomicBoolean holderSeal = new AtomicBoolean();
        AtomicBoolean laggingSeal = new AtomicBoolean();
        byte[] data = new byte[] {1, 2, 3, 4};
        List<Messages.LedgerEntry> ledger = List.of(
                new Messages.LedgerEntry(data.length, Crc.of(data), 1));

        try (ScpServer holder = ledgerOrderingReplica(1, data.length, ledger, data, null, holderSeal);
             ScpServer lagging = ledgerOrderingReplica(3, 0, List.of(), data, laggingAppend, laggingSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(holder)),
                             new Messages.Replica(2, ""),
                             new Messages.Replica(3, endpoint(lagging))))), sealedFileLength);
             AutoCloseable ignored = unsafeSealOverride(chunkId)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test"))
                        .recoverAndSeal(fileId, 2);

                assertEquals(0, sealedInfo.sealedLength());
                assertEquals(0L, sealedFileLength.get());
                assertFalse(laggingAppend.get());
                assertTrue(holderSeal.get());
                assertTrue(laggingSeal.get());
                assertEquals(null, System.getProperty(Recovery.UNSAFE_SEAL_OVERRIDE_CHUNKS_PROPERTY),
                        "the unsafe override must be consumed after one matching recovery");
            }
        }
    }

    @Test
    void emptyEndpointWithoutQuorumPossibleTailDoesNotConsumeOverride() throws Exception {
        FileId fileId = FileId.of(55);
        ChunkId chunkId = new ChunkId(fileId, 0);
        String overrideKey = "test:" + chunkId;
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean floorASeal = new AtomicBoolean();
        AtomicBoolean floorBSeal = new AtomicBoolean();

        try (ScpServer floorA = openReplicaWithFenceAndSeal(1, 0, 0, 0, 777, floorASeal);
             ScpServer floorB = openReplicaWithFenceAndSeal(3, 0, 0, 0, 777, floorBSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(floorA)),
                             new Messages.Replica(2, ""),
                             new Messages.Replica(3, endpoint(floorB))))), sealedFileLength);
             AutoCloseable ignored = unsafeSealOverride(chunkId)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test"))
                        .recoverAndSeal(fileId, 2);

                assertEquals(0, sealedInfo.sealedLength());
                assertEquals(0L, sealedFileLength.get());
                assertTrue(floorASeal.get());
                assertTrue(floorBSeal.get());
                assertEquals(overrideKey, System.getProperty(Recovery.UNSAFE_SEAL_OVERRIDE_CHUNKS_PROPERTY),
                        "an unnecessary unsafe override must remain available");
            }
        }
    }

    @Test
    void emptyEndpointCoveredByReachableQuorumTailDoesNotBlockOrConsumeOverride() throws Exception {
        FileId fileId = FileId.of(56);
        ChunkId chunkId = new ChunkId(fileId, 0);
        String overrideKey = "test:" + chunkId;
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean holderASeal = new AtomicBoolean();
        AtomicBoolean holderBSeal = new AtomicBoolean();
        byte[] data = new byte[] {1, 2, 3, 4};
        List<Messages.LedgerEntry> ledger = List.of(
                new Messages.LedgerEntry(data.length, Crc.of(data), 1));

        try (ScpServer holderA = ledgerOrderingReplica(1, data.length, ledger, data, null, holderASeal);
             ScpServer holderB = ledgerOrderingReplica(2, data.length, ledger, data, null, holderBSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(holderA)),
                             new Messages.Replica(2, endpoint(holderB)),
                             new Messages.Replica(3, "")))), sealedFileLength);
             AutoCloseable ignored = unsafeSealOverride(chunkId)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test"))
                        .recoverAndSeal(fileId, 2);

                assertEquals(data.length, sealedInfo.sealedLength());
                assertEquals((long) data.length, sealedFileLength.get());
                assertTrue(holderASeal.get());
                assertTrue(holderBSeal.get());
                assertEquals(overrideKey, System.getProperty(Recovery.UNSAFE_SEAL_OVERRIDE_CHUNKS_PROPERTY),
                        "a covered descriptor outcome must not consume the unsafe override");
            }
        }
    }

    @Test
    void ackedTailSurvivesTransientReadFailureFromFencedHolder() throws Exception {
        // Issue #42: the second holder fenced successfully and reported end=4, but its READ_RECOVERY
        // transiently fails. That is holder evidence, unlike a CRC-mismatching read, so a single
        // CRC-valid copy plus the read-failed holder can still represent an acked batch.
        FileId fileId = FileId.of(40);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean laggingAppend = new AtomicBoolean();
        byte[] data = new byte[] {1, 2, 3, 4};
        int crc = Crc.of(data);
        List<Messages.LedgerEntry> ledger = List.of(new Messages.LedgerEntry(data.length, crc, 1));

        try (ScpServer holder = ledgerOrderingReplica(1, data.length, ledger, data, null);
             ScpServer readFailingHolder = new ScpServer(0, 2, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.FENCE) {
                     return ScpServer.ok(req, new Messages.FenceResp(2, data.length, 0, ChunkState.OPEN).encode(),
                             null);
                 }
                 if (op == Opcode.READ_LEDGER) {
                     return ScpServer.ok(req, new Messages.ReadLedgerResp(ledger).encode(), null);
                 }
                 if (op == Opcode.READ_RECOVERY) {
                     throw new ScpException(ErrorCode.INTERNAL, "transient recovery read failure");
                 }
                 if (op == Opcode.SEAL_CHUNK) {
                     Messages.SealChunk seal = Messages.SealChunk.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.SealResp(seal.dataLength(), 777).encode(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             ScpServer lagging = ledgerOrderingReplica(3, 0, List.of(), data, laggingAppend);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(holder)),
                             new Messages.Replica(2, endpoint(readFailingHolder)),
                             new Messages.Replica(3, endpoint(lagging))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test"))
                        .recoverAndSeal(fileId, 2);

                assertEquals(4, sealedInfo.sealedLength(),
                        "a producer-acked tail must survive a transient read failure from a fenced holder");
                assertEquals(4L, sealedFileLength.get());
                assertTrue(laggingAppend.get(),
                        "the surviving CRC-valid copy must be re-replicated to the lagging replica");
            }
        }
    }

    @Test
    void aboveFloorLedgerReadFailureAbortsInsteadOfFloorSeal() throws Exception {
        FileId fileId = FileId.of(41);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean holderASeal = new AtomicBoolean();
        AtomicBoolean holderBSeal = new AtomicBoolean();
        AtomicBoolean laggingSeal = new AtomicBoolean();

        try (ScpServer holderA = aboveFloorLedgerFailingReplica(1, 8, 4, holderASeal);
             ScpServer holderB = aboveFloorLedgerFailingReplica(2, 8, 4, holderBSeal);
             ScpServer lagging = openReplicaWithFenceAndSeal(3, 4, 4, 4, 777, laggingSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(holderA)),
                             new Messages.Replica(2, endpoint(holderB)),
                             new Messages.Replica(3, endpoint(lagging))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertTrue(e.getMessage().contains("unverified above-floor holder"));
                assertEquals(null, sealedFileLength.get());
                assertFalse(holderASeal.get());
                assertFalse(holderBSeal.get());
                assertFalse(laggingSeal.get());
            }
        }
    }

    @Test
    void unsafeSealOverrideEvictsBlockedHolderAndSealsRemainingQuorum() throws Exception {
        FileId fileId = FileId.of(48);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean blockedHolderSeal = new AtomicBoolean();
        AtomicBoolean floorASeal = new AtomicBoolean();
        AtomicBoolean floorBSeal = new AtomicBoolean();

        try (ScpServer blockedHolder = aboveFloorLedgerFailingReplica(1, 8, 4, blockedHolderSeal);
             ScpServer floorA = openReplicaWithFenceAndSeal(2, 4, 4, 4, 777, floorASeal);
             ScpServer floorB = openReplicaWithFenceAndSeal(3, 4, 4, 4, 777, floorBSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(blockedHolder)),
                             new Messages.Replica(2, endpoint(floorA)),
                             new Messages.Replica(3, endpoint(floorB))))), sealedFileLength);
             AutoCloseable ignored = unsafeSealOverride(chunkId)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test"))
                        .recoverAndSeal(fileId, 2);

                assertEquals(4, sealedInfo.sealedLength());
                assertEquals(4L, sealedFileLength.get());
                assertFalse(blockedHolderSeal.get(),
                        "the override must evict the unverified holder instead of sealing it short");
                assertTrue(floorASeal.get());
                assertTrue(floorBSeal.get());
                assertEquals(null, System.getProperty(Recovery.UNSAFE_SEAL_OVERRIDE_CHUNKS_PROPERTY),
                        "the unsafe override must be consumed after one matching recovery");
            }
        }
    }

    @Test
    void unsafeSealOverrideForDifferentChunkDoesNotBypassBlockedHolder() throws Exception {
        FileId fileId = FileId.of(49);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean blockedHolderSeal = new AtomicBoolean();
        AtomicBoolean floorASeal = new AtomicBoolean();
        AtomicBoolean floorBSeal = new AtomicBoolean();

        try (ScpServer blockedHolder = aboveFloorLedgerFailingReplica(1, 8, 4, blockedHolderSeal);
             ScpServer floorA = openReplicaWithFenceAndSeal(2, 4, 4, 4, 777, floorASeal);
             ScpServer floorB = openReplicaWithFenceAndSeal(3, 4, 4, 4, 777, floorBSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(blockedHolder)),
                             new Messages.Replica(2, endpoint(floorA)),
                             new Messages.Replica(3, endpoint(floorB))))), sealedFileLength);
             AutoCloseable ignored = unsafeSealOverride(new ChunkId(FileId.of(999), 0))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertEquals(null, sealedFileLength.get());
                assertFalse(blockedHolderSeal.get());
                assertFalse(floorASeal.get());
                assertFalse(floorBSeal.get());
            }
        }
    }

    @Test
    void unsafeSealOverrideBelowQuorumStillReportsSealRecoveryBlocked() throws Exception {
        FileId fileId = FileId.of(51);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean blockedHolderSeal = new AtomicBoolean();
        AtomicBoolean floorSeal = new AtomicBoolean();

        try (ScpServer blockedHolder = aboveFloorLedgerFailingReplica(1, 8, 4, blockedHolderSeal);
             ScpServer floor = openReplicaWithFenceAndSeal(2, 4, 4, 4, 777, floorSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(blockedHolder)),
                             new Messages.Replica(2, endpoint(floor)),
                             new Messages.Replica(3, "127.0.0.1:1")))), sealedFileLength);
             AutoCloseable ignored = unsafeSealOverride(chunkId)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertTrue(e.getMessage().contains("leaving 1 usable replica(s), need 2"));
                assertEquals(null, sealedFileLength.get());
                assertFalse(blockedHolderSeal.get());
                assertFalse(floorSeal.get());
            }
        }
    }

    @Test
    void unsafeSealOverrideMatchesOneTokenFromListAndConsumesOnlyThatToken() throws Exception {
        FileId fileId = FileId.of(50);
        ChunkId chunkId = new ChunkId(fileId, 0);
        ChunkId otherChunk = new ChunkId(FileId.of(1000), 0);
        String otherKey = "test:" + otherChunk;
        String targetKey = "test:" + chunkId;
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean blockedHolderSeal = new AtomicBoolean();
        AtomicBoolean floorASeal = new AtomicBoolean();
        AtomicBoolean floorBSeal = new AtomicBoolean();

        try (ScpServer blockedHolder = aboveFloorLedgerFailingReplica(1, 8, 4, blockedHolderSeal);
             ScpServer floorA = openReplicaWithFenceAndSeal(2, 4, 4, 4, 777, floorASeal);
             ScpServer floorB = openReplicaWithFenceAndSeal(3, 4, 4, 4, 777, floorBSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(blockedHolder)),
                             new Messages.Replica(2, endpoint(floorA)),
                             new Messages.Replica(3, endpoint(floorB))))), sealedFileLength);
             AutoCloseable ignored = unsafeSealOverride(otherKey + ", " + targetKey)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test"))
                        .recoverAndSeal(fileId, 2);

                assertEquals(4, sealedInfo.sealedLength());
                assertEquals(4L, sealedFileLength.get());
                assertFalse(blockedHolderSeal.get());
                assertTrue(floorASeal.get());
                assertTrue(floorBSeal.get());
                String remaining = System.getProperty(Recovery.UNSAFE_SEAL_OVERRIDE_CHUNKS_PROPERTY);
                assertTrue(remaining.contains(otherKey));
                assertFalse(remaining.contains(targetKey));
            }
        }
    }

    @Test
    void allAboveFloorRecoveryReadsFailAbortInsteadOfFloorSeal() throws Exception {
        FileId fileId = FileId.of(42);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean holderASeal = new AtomicBoolean();
        AtomicBoolean holderBSeal = new AtomicBoolean();
        AtomicBoolean laggingSeal = new AtomicBoolean();
        byte[] tail = new byte[] {5, 6, 7, 8};
        List<Messages.LedgerEntry> ledger = List.of(new Messages.LedgerEntry(8, Crc.of(tail), 1));

        try (ScpServer holderA = aboveFloorReadFailingReplica(1, 8, 4, ledger, holderASeal);
             ScpServer holderB = aboveFloorReadFailingReplica(2, 8, 4, ledger, holderBSeal);
             ScpServer lagging = openReplicaWithFenceAndSeal(3, 4, 4, 4, 777, laggingSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(holderA)),
                             new Messages.Replica(2, endpoint(holderB)),
                             new Messages.Replica(3, endpoint(lagging))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertTrue(e.getMessage().contains("unverified above-floor holder"));
                assertEquals(null, sealedFileLength.get());
                assertFalse(holderASeal.get());
                assertFalse(holderBSeal.get());
                assertFalse(laggingSeal.get());
            }
        }
    }

    @Test
    void crcInvalidAboveFloorHolderAbortsInsteadOfFloorSeal() throws Exception {
        FileId fileId = FileId.of(43);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean holderSeal = new AtomicBoolean();
        AtomicBoolean laggingSeal = new AtomicBoolean();
        byte[] expectedTail = new byte[] {5, 6, 7, 8};
        byte[] corruptFull = new byte[] {0, 0, 0, 0, 9, 9, 9, 9};
        List<Messages.LedgerEntry> ledger = List.of(new Messages.LedgerEntry(8, Crc.of(expectedTail), 1));

        try (ScpServer holder = crcInvalidAboveFloorReplica(1, 8, 4, ledger, corruptFull, holderSeal);
             ScpServer lagging = openReplicaWithFenceAndSeal(2, 4, 4, 4, 777, laggingSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(holder)),
                             new Messages.Replica(2, endpoint(lagging)),
                             new Messages.Replica(3, "127.0.0.1:1")))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertTrue(e.getMessage().contains("unverified above-floor holder"));
                assertEquals(null, sealedFileLength.get());
                assertFalse(holderSeal.get());
                assertFalse(laggingSeal.get());
            }
        }
    }

    @Test
    void crcInvalidHolderWithMissingUpperLedgerBoundaryAbortsInsteadOfFloorSeal() throws Exception {
        FileId fileId = FileId.of(47);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean holderSeal = new AtomicBoolean();
        byte[] full = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
        byte[] prefix = new byte[] {1, 2, 3, 4};
        byte[] corruptFull = new byte[] {1, 2, 3, 4, 9, 9, 9, 9};
        List<Messages.LedgerEntry> fullLedger = List.of(new Messages.LedgerEntry(8, Crc.of(full), 1));
        List<Messages.LedgerEntry> prefixOnlyLedger = List.of(new Messages.LedgerEntry(4, Crc.of(prefix), 1));

        try (ScpServer donor = ledgerOrderingReplica(1, full.length, fullLedger, full, null);
             ScpServer holder = crcInvalidAboveFloorReplica(2, 8, 0, prefixOnlyLedger, corruptFull, holderSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(donor)),
                             new Messages.Replica(2, endpoint(holder)),
                             new Messages.Replica(3, "127.0.0.1:1")))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertTrue(e.getMessage().contains("unverified above-floor holder"));
                assertEquals(null, sealedFileLength.get());
                assertFalse(holderSeal.get());
            }
        }
    }

    @Test
    void malformedAboveFloorLedgerResponseAbortsInsteadOfFloorSeal() throws Exception {
        FileId fileId = FileId.of(44);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean holderASeal = new AtomicBoolean();
        AtomicBoolean holderBSeal = new AtomicBoolean();
        AtomicBoolean laggingSeal = new AtomicBoolean();

        try (ScpServer holderA = malformedAboveFloorLedgerReplica(1, 8, 4, holderASeal);
             ScpServer holderB = malformedAboveFloorLedgerReplica(2, 8, 4, holderBSeal);
             ScpServer lagging = openReplicaWithFenceAndSeal(3, 4, 4, 4, 777, laggingSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(holderA)),
                             new Messages.Replica(2, endpoint(holderB)),
                             new Messages.Replica(3, endpoint(lagging))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertTrue(e.getMessage().contains("unverified above-floor holder"));
                assertEquals(null, sealedFileLength.get());
                assertFalse(holderASeal.get());
                assertFalse(holderBSeal.get());
                assertFalse(laggingSeal.get());
            }
        }
    }

    @Test
    void partialOverlapReadFailureMarksHolderWhenCandidateIsRejected() throws Exception {
        FileId fileId = FileId.of(45);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        byte[] candidate = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
        byte[] conflict = new byte[] {9, 9, 9, 9};

        try (ScpServer donor = ledgerOrderingReplica(1, candidate.length,
                     List.of(new Messages.LedgerEntry(candidate.length, Crc.of(candidate), 1)), candidate, null);
             ScpServer partialUnreadable = partialReadFailingReplica(2, 4);
             ScpServer partialConflict = partialReadableReplica(3, conflict);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(donor)),
                             new Messages.Replica(2, endpoint(partialUnreadable)),
                             new Messages.Replica(3, endpoint(partialConflict)),
                             new Messages.Replica(4, "127.0.0.1:1")))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertTrue(e.getMessage().contains("unverified above-floor holder"));
                assertEquals(null, sealedFileLength.get());
            }
        }
    }

    @Test
    void ledgerReadFailureCoveredByQuorumTailDoesNotAbort() throws Exception {
        FileId fileId = FileId.of(46);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean markedHolderSeal = new AtomicBoolean();
        byte[] full = new byte[] {0, 0, 0, 0, 5, 6, 7, 8};
        byte[] tail = new byte[] {5, 6, 7, 8};
        List<Messages.LedgerEntry> ledger = List.of(new Messages.LedgerEntry(8, Crc.of(tail), 1));

        try (ScpServer donorA = ledgerOrderingReplica(1, full.length, ledger, full, null);
             ScpServer donorB = ledgerOrderingReplica(2, full.length, ledger, full, null);
             ScpServer markedHolder = aboveFloorLedgerFailingReplica(3, 8, 4, markedHolderSeal);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(donorA)),
                             new Messages.Replica(2, endpoint(donorB)),
                             new Messages.Replica(3, endpoint(markedHolder))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test"))
                        .recoverAndSeal(fileId, 2);

                assertEquals(8, sealedInfo.sealedLength());
                assertEquals(8L, sealedFileLength.get());
                assertTrue(markedHolderSeal.get(),
                        "a holder whose claim is covered by the final seal point must not cause an abort");
            }
        }
    }

    @Test
    void conflictingSingleHolderContinuationsAboveFloorTruncateToFloor() throws Exception {
        // PR #34 review: with one replica unreachable, two reachable replicas can each hold a CRC-valid
        // but DIFFERENT continuation from the floor — R1 has A[0,4), R2 has B[0,8) with a different
        // prefix. Neither has a reachable quorum. The reachability gate alone would let the farther
        // B[0,8) through and graft B[4,8) onto R1's A prefix, then fail the seal with divergence.
        // Recovery must treat conflicting continuations from the same floor as a divergent split and
        // truncate at the floor instead.
        FileId fileId = FileId.of(39);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean r1Grafted = new AtomicBoolean();
        byte[] a = new byte[] {1, 1, 1, 1};
        byte[] b = new byte[] {2, 2, 2, 2, 2, 2, 2, 2};

        try (ScpServer r1 = ledgerOrderingReplica(1, a.length,
                     List.of(new Messages.LedgerEntry(a.length, Crc.of(a), 1)), a, r1Grafted);
             ScpServer r2 = ledgerOrderingReplica(2, b.length,
                     List.of(new Messages.LedgerEntry(b.length, Crc.of(b), 1)), b, null);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(r1)),
                             new Messages.Replica(2, endpoint(r2)),
                             new Messages.Replica(3, "127.0.0.1:1")))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2);

                assertEquals(0, sealedInfo.sealedLength(),
                        "conflicting single-holder continuations must truncate at the floor");
                assertEquals(0L, sealedFileLength.get());
                assertFalse(r1Grafted.get(),
                        "a conflicting longer continuation must not be grafted onto R1's prefix");
            }
        }
    }

    @Test
    void divergentContinuationAboveFloorIsTruncatedNotCommitted() throws Exception {
        // Safety counterpart to issue #29: a single CRC-valid copy above the floor is re-replicated,
        // but a genuine divergent split (replicas hold DIFFERENT CRC-valid bytes for the same range,
        // §14.6) must still be truncated to the floor — recovery must never silently pick one.
        FileId fileId = FileId.of(38);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        byte[] a = new byte[] {1, 1, 1, 1};
        byte[] b = new byte[] {2, 2, 2, 2};
        byte[] c = new byte[] {3, 3, 3, 3};

        try (ScpServer r1 = ledgerOrderingReplica(1, a.length,
                     List.of(new Messages.LedgerEntry(a.length, Crc.of(a), 1)), a, null);
             ScpServer r2 = ledgerOrderingReplica(2, b.length,
                     List.of(new Messages.LedgerEntry(b.length, Crc.of(b), 1)), b, null);
             ScpServer r3 = ledgerOrderingReplica(3, c.length,
                     List.of(new Messages.LedgerEntry(c.length, Crc.of(c), 1)), c, null);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(r1)),
                             new Messages.Replica(2, endpoint(r2)),
                             new Messages.Replica(3, endpoint(r3))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2);

                assertEquals(0, sealedInfo.sealedLength(),
                        "a divergent split above the floor must be truncated, not committed");
                assertEquals(0L, sealedFileLength.get());
            }
        }
    }

    @Test
    void quorumLedgerContinuationIsReplicatedToLaggingReplica() throws Exception {
        FileId fileId = FileId.of(20);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean laggingAppend = new AtomicBoolean();
        byte[] data = new byte[] {1, 2, 3, 4};
        int crc = Crc.of(data);
        List<Messages.LedgerEntry> ledger = List.of(new Messages.LedgerEntry(data.length, crc, 1));

        try (ScpServer donorA = ledgerOrderingReplica(1, data.length, ledger, data, null);
             ScpServer donorB = ledgerOrderingReplica(2, data.length, ledger, data, null);
             ScpServer lagging = ledgerOrderingReplica(3, 0, List.of(), data, laggingAppend);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(donorA)),
                             new Messages.Replica(2, endpoint(donorB)),
                             new Messages.Replica(3, endpoint(lagging))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2);

                assertEquals(4, sealedInfo.sealedLength());
                assertEquals(4L, sealedFileLength.get());
                assertTrue(laggingAppend.get());
            }
        }
    }

    @Test
    void ledgerContinuationSkipsShortReplicaAndKeepsShorterValidCandidateWhenLaterBoundaryIsInvalid()
            throws Exception {
        FileId fileId = FileId.of(21);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean laggingAppend = new AtomicBoolean();
        byte[] prefix = new byte[] {1, 2, 3, 4};
        byte[] longer = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
        int prefixCrc = Crc.of(prefix);
        int invalidLongerCrc = Crc.of(longer) ^ 1;

        try (ScpServer lagging = ledgerOrderingReplica(1, 0, List.of(), prefix, laggingAppend);
             ScpServer validShortA = ledgerOrderingReplica(2, 4,
                     List.of(new Messages.LedgerEntry(4, prefixCrc, 1)), prefix, null);
             ScpServer validShortB = ledgerOrderingReplica(3, 4,
                     List.of(new Messages.LedgerEntry(4, prefixCrc, 1)), prefix, null);
             ScpServer invalidLong = ledgerOrderingReplica(4, 8,
                     List.of(new Messages.LedgerEntry(8, invalidLongerCrc, 1)), longer, null);
             ScpServer metaServer = metadataServer(new AtomicReference<>(new Messages.LookupFileResp(
                     "test", "/test/file", new Messages.WritePolicy(4, 3, false), (byte) 0, List.of(
                             chunk(chunkId, ChunkState.OPEN, 0, 1,
                                     new Messages.Replica(1, endpoint(lagging)),
                                     new Messages.Replica(2, endpoint(validShortA)),
                                     new Messages.Replica(3, endpoint(validShortB)),
                                     new Messages.Replica(4, endpoint(invalidLong)))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2);

                assertEquals(4, sealedInfo.sealedLength());
                assertEquals(4L, sealedFileLength.get());
                assertTrue(laggingAppend.get());
            }
        }
    }

    @Test
    void ledgerReadFailuresAreIgnoredWhenSealQuorumStillExists() throws Exception {
        FileId fileId = FileId.of(22);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer failingLedger = new ScpServer(0, 1, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, 0, 0, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                throw new ScpException(ErrorCode.INTERNAL, "ledger failed");
            }
            if (op == Opcode.SEAL_CHUNK) {
                return ScpServer.ok(req, new Messages.SealResp(0, 777).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             ScpServer malformedLedger = new ScpServer(0, 2, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.FENCE) {
                     return ScpServer.ok(req, new Messages.FenceResp(2, 0, 0, ChunkState.OPEN).encode(), null);
                 }
                 if (op == Opcode.READ_LEDGER) {
                     return ScpServer.ok(req, new byte[] {0, 0, (byte) 0x80}, null);
                 }
                 if (op == Opcode.SEAL_CHUNK) {
                     return ScpServer.ok(req, new Messages.SealResp(0, 777).encode(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(failingLedger)),
                             new Messages.Replica(2, endpoint(malformedLedger))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2);

                assertEquals(0, sealedInfo.sealedLength());
                assertEquals(0L, sealedFileLength.get());
            }
        }
    }

    @Test
    void reReplicationAppendFailureEvictsLaggingReplicaAndKeepsQuorum() throws Exception {
        FileId fileId = FileId.of(23);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean laggingAppend = new AtomicBoolean();
        byte[] data = new byte[] {1, 2, 3, 4};
        int crc = Crc.of(data);
        List<Messages.LedgerEntry> ledger = List.of(new Messages.LedgerEntry(data.length, crc, 1));

        try (ScpServer donorA = ledgerOrderingReplica(1, data.length, ledger, data, null);
             ScpServer donorB = ledgerOrderingReplica(2, data.length, ledger, data, null);
             ScpServer lagging = new ScpServer(0, 3, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.FENCE) {
                     return ScpServer.ok(req, new Messages.FenceResp(2, 0, 0, ChunkState.OPEN).encode(), null);
                 }
                 if (op == Opcode.READ_LEDGER) {
                     return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of()).encode(), null);
                 }
                 if (op == Opcode.APPEND) {
                     laggingAppend.set(true);
                     throw new ScpException(ErrorCode.INTERNAL, "append failed");
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(donorA)),
                             new Messages.Replica(2, endpoint(donorB)),
                             new Messages.Replica(3, endpoint(lagging))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2);

                assertEquals(4, sealedInfo.sealedLength());
                assertEquals(4L, sealedFileLength.get());
                assertTrue(laggingAppend.get());
            }
        }
    }

    @Test
    void reReplicationFencedAppendIsPropagated() throws Exception {
        FileId fileId = FileId.of(24);
        ChunkId chunkId = new ChunkId(fileId, 0);
        byte[] data = new byte[] {5, 6, 7, 8};
        int crc = Crc.of(data);
        List<Messages.LedgerEntry> ledger = List.of(new Messages.LedgerEntry(data.length, crc, 1));

        try (ScpServer donorA = ledgerOrderingReplica(1, data.length, ledger, data, null);
             ScpServer donorB = ledgerOrderingReplica(2, data.length, ledger, data, null);
             ScpServer lagging = new ScpServer(0, 3, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.FENCE) {
                     return ScpServer.ok(req, new Messages.FenceResp(2, 0, 0, ChunkState.OPEN).encode(), null);
                 }
                 if (op == Opcode.READ_LEDGER) {
                     return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of()).encode(), null);
                 }
                 if (op == Opcode.APPEND) {
                     throw new ScpException(ErrorCode.FENCED_EPOCH, "fenced", 9);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(donorA)),
                             new Messages.Replica(2, endpoint(donorB)),
                             new Messages.Replica(3, endpoint(lagging))))), null)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.FENCED_EPOCH, e.code());
                assertEquals(9, e.detail());
            }
        }
    }

    @Test
    void catchUpFencedAppendIsPropagated() throws Exception {
        FileId fileId = FileId.of(25);
        ChunkId chunkId = new ChunkId(fileId, 0);
        byte[] data = new byte[] {1, 3, 5, 7};

        try (ScpServer donor = catchUpDonor(1, data);
             ScpServer lagging = new ScpServer(0, 2, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.FENCE) {
                     return ScpServer.ok(req, new Messages.FenceResp(2, 0, 0, ChunkState.OPEN).encode(), null);
                 }
                 if (op == Opcode.APPEND) {
                     throw new ScpException(ErrorCode.FENCED_EPOCH, "fenced", 10);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(donor)),
                             new Messages.Replica(2, endpoint(lagging))))), null)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.FENCED_EPOCH, e.code());
                assertEquals(10, e.detail());
            }
        }
    }

    @Test
    void malformedAppendResponseDuringCatchUpEvictsReplica() throws Exception {
        FileId fileId = FileId.of(26);
        ChunkId chunkId = new ChunkId(fileId, 0);
        byte[] data = new byte[] {2, 4, 6, 8};

        try (ScpServer donor = catchUpDonor(1, data);
             ScpServer lagging = new ScpServer(0, 2, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.FENCE) {
                     return ScpServer.ok(req, new Messages.FenceResp(2, 0, 0, ChunkState.OPEN).encode(), null);
                 }
                 if (op == Opcode.APPEND) {
                     return ScpServer.ok(req, new byte[] {0, 0, 0}, null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(donor)),
                             new Messages.Replica(2, endpoint(lagging))))), null)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(e.getMessage().contains("usable replicas"));
            }
        }
    }

    @Test
    void oversizedLedgerBoundaryAbortsAsUnverifiedAboveFloorHolder() throws Exception {
        FileId fileId = FileId.of(27);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean readCalled = new AtomicBoolean();
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        long oversizedEnd = (long) Integer.MAX_VALUE + 1L;

        try (ScpServer s1 = oversizedLedgerReplica(1, oversizedEnd, readCalled);
             ScpServer s2 = oversizedLedgerReplica(2, oversizedEnd, readCalled);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(s1)),
                             new Messages.Replica(2, endpoint(s2))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertTrue(e.getMessage().contains("unverified above-floor holder"));
                assertEquals(null, sealedFileLength.get());
                assertEquals(false, readCalled.get());
            }
        }
    }

    @Test
    void malformedReadResponseAbortsAsUnverifiedAboveFloorHolder() throws Exception {
        FileId fileId = FileId.of(28);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        byte[] data = new byte[] {1, 1, 2, 3};
        int crc = Crc.of(data);

        try (ScpServer s1 = malformedReadLedgerReplica(1, data.length, crc);
             ScpServer s2 = malformedReadLedgerReplica(2, data.length, crc);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(s1)),
                             new Messages.Replica(2, endpoint(s2))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.SEAL_RECOVERY_BLOCKED, e.code());
                assertTrue(e.getMessage().contains("unverified above-floor holder"));
                assertEquals(null, sealedFileLength.get());
            }
        }
    }

    @Test
    void recoverySealDivergenceWithoutAgreeingQuorumFails() throws Exception {
        FileId fileId = FileId.of(29);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer s1 = openReplicaWithFenceAndSeal(1, 0, 0, 0, 111);
             ScpServer s2 = openReplicaWithFenceAndSeal(2, 0, 0, 0, 222);
             ScpServer s3 = openReplicaWithFenceAndSeal(3, 0, 0, 0, 333);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(s1)),
                             new Messages.Replica(2, endpoint(s2)),
                             new Messages.Replica(3, endpoint(s3))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(e.getMessage().contains("replica seal divergence"));
                assertEquals(null, sealedFileLength.get());
            }
        }
    }

    @Test
    void recoverySealFencedEpochIsPropagated() throws Exception {
        FileId fileId = FileId.of(30);
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer fenced = openReplicaThatFencesOnSeal(1, 12);
             ScpServer valid = openReplicaThatSealsAt(2, 0);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(fenced)),
                             new Messages.Replica(2, endpoint(valid))))), null)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                ScpException e = assertThrows(ScpException.class,
                        () -> new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2));

                assertEquals(ErrorCode.FENCED_EPOCH, e.code());
                assertEquals(12, e.detail());
            }
        }
    }

    @Test
    void recoverySealCommitsAgreeingQuorumWhenOneReplicaDiverges() throws Exception {
        FileId fileId = FileId.of(31);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer a = openReplicaWithFenceAndSeal(1, 0, 0, 0, 111);
             ScpServer b = openReplicaWithFenceAndSeal(2, 0, 0, 0, 111);
             ScpServer divergent = openReplicaWithFenceAndSeal(3, 0, 0, 0, 222);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(a)),
                             new Messages.Replica(2, endpoint(b)),
                             new Messages.Replica(3, endpoint(divergent))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2);

                assertEquals(0, sealedInfo.sealedLength());
                assertEquals(0L, sealedFileLength.get());
            }
        }
    }

    @Test
    void catchUpSkipsSelfAndShortDonorBeforeUsingFullReplica() throws Exception {
        FileId fileId = FileId.of(32);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicBoolean laggingAppend = new AtomicBoolean();
        AtomicBoolean partialAppend = new AtomicBoolean();

        try (ScpServer lagging = catchUpReplica(1, 0, 0, 4, laggingAppend);
             ScpServer partial = catchUpReplica(2, 2, 0, 4, partialAppend);
             ScpServer full = catchUpReplica(3, 4, 4, 4, null);
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     lookup(chunk(chunkId, ChunkState.OPEN, 0, 1,
                             new Messages.Replica(1, endpoint(lagging)),
                             new Messages.Replica(2, endpoint(partial)),
                             new Messages.Replica(3, endpoint(full))))), sealedFileLength)) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                StrataFile.SealInfo sealedInfo = new Recovery(meta, pool, config, StrataNamespace.of("test")).recoverAndSeal(fileId, 2);

                assertEquals(4, sealedInfo.sealedLength());
                assertEquals(4L, sealedFileLength.get());
                assertEquals(true, laggingAppend.get());
                assertEquals(true, partialAppend.get());
            }
        }
    }

    @Test
    void readRangeRejectsInvalidArgumentsBeforeDataNodeUse() throws Exception {
        Recovery recovery = new Recovery(null, null, null, StrataNamespace.of("test"));
        ChunkId chunkId = new ChunkId(FileId.of(33), 0);
        Object source = replicaState(new Messages.Replica(1, "unused"), 10, 10, ChunkState.OPEN);

        assertEquals(null, invokeReadRange(recovery, chunkId, source, -1, 1));
        assertEquals(null, invokeReadRange(recovery, chunkId, source, 0, -1));
        assertEquals(null, invokeReadRange(recovery, chunkId, source, 3, 3));
        assertEquals(null, invokeReadRange(recovery, chunkId, source, 0, (long) Integer.MAX_VALUE + 1L));
    }

    @Test
    void readRangeRejectsMalformedResponseShapes() throws Exception {
        assertEquals(null, readRangeFromServer(new Messages.ReadResp(3, 3), 4));
        assertEquals(null, readRangeFromServer(new Messages.ReadResp(-1, 0), 4));
        assertEquals(null, readRangeFromServer(new Messages.ReadResp(4, -1), 4));
        assertEquals(null, readRangeFromServer(new Messages.ReadResp(4, 5), 4));
        assertEquals(null, readRangeFromServer(new Messages.ReadResp(4, 4), 3));
    }

    @Test
    void readRangePropagatesFencedEpoch() throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
            if (Opcode.fromCode(req.opcode()) == Opcode.READ_RECOVERY) {
                throw new ScpException(ErrorCode.FENCED_EPOCH, "re-fenced", 3);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + req.opcode());
        })) {
            ClientConfig config = new ClientConfig(List.of("127.0.0.1:1"), 1024, 500);
            try (NodePool pool = new NodePool()) {
                Recovery recovery = new Recovery(null, pool, config, StrataNamespace.of("test"));
                ChunkId chunkId = new ChunkId(FileId.of(38), 0);
                Object source = replicaState(new Messages.Replica(1, endpoint(server)), 4, 4, ChunkState.OPEN);

                ScpException e = assertThrows(ScpException.class,
                        () -> invokeReadRange(recovery, chunkId, source, 0, 4));
                assertEquals(ErrorCode.FENCED_EPOCH, e.code());
                assertEquals(3, e.detail());
            }
        }
    }

    @Test
    void readRangeAcceptsServerCappedPartialResponses() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        byte[] source = new byte[] {1, 2, 3, 4};
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
            if (isReadOp(Opcode.fromCode(req.opcode()))) {
                reads.incrementAndGet();
                Messages.Read read = Messages.Read.decode(req.headerSlice());
                int offset = (int) read.offset();
                int length = Math.min(2, source.length - offset);
                return ScpServer.ok(req, new Messages.ReadResp(source.length, source.length).encode(),
                        ByteBuffer.wrap(source, offset, length));
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + req.opcode());
        })) {
            ClientConfig config = new ClientConfig(List.of("127.0.0.1:1"), 1024, 500);
            try (NodePool pool = new NodePool()) {
                Recovery recovery = new Recovery(null, pool, config, StrataNamespace.of("test"));
                ChunkId chunkId = new ChunkId(FileId.of(34), 0);
                Object sourceReplica = replicaState(new Messages.Replica(1, endpoint(server)), 4, 4, ChunkState.OPEN);

                assertArrayEquals(source, invokeReadRange(recovery, chunkId, sourceReplica, 0, 4));
                assertEquals(2, reads.get());
            }
        }
    }

    @Test
    void readRangeStopsDribblingDonorAfterBoundedAttempts() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        byte[] source = new byte[] {1, 2, 3};
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
            if (isReadOp(Opcode.fromCode(req.opcode()))) {
                reads.incrementAndGet();
                Messages.Read read = Messages.Read.decode(req.headerSlice());
                int offset = (int) read.offset();
                return ScpServer.ok(req, new Messages.ReadResp(source.length, source.length).encode(),
                        ByteBuffer.wrap(source, offset, 1));
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + req.opcode());
        })) {
            ClientConfig config = new ClientConfig(List.of("127.0.0.1:1"), 1024, 500);
            try (NodePool pool = new NodePool()) {
                Recovery recovery = new Recovery(null, pool, config, StrataNamespace.of("test"));
                ChunkId chunkId = new ChunkId(FileId.of(37), 0);
                Object sourceReplica = replicaState(new Messages.Replica(1, endpoint(server)), 3, 3, ChunkState.OPEN);

                assertEquals(null, invokeReadRange(recovery, chunkId, sourceReplica, 0, 3));
                assertEquals(2, reads.get());
            }
        }
    }

    @Test
    void validateFenceRespRejectsNegativeLocalEnd() throws Exception {
        ChunkId chunkId = new ChunkId(FileId.of(34), 0);
        Messages.Replica replica = new Messages.Replica(1, "node");

        ScpException e = assertThrows(ScpException.class,
                () -> invokeValidateFenceResp(chunkId, replica,
                        new Messages.FenceResp(2, -1, 0, ChunkState.OPEN), 2));

        assertEquals(ErrorCode.CORRUPT_CHUNK, e.code());
    }

    @Test
    void validateFenceRespRejectsHigherReplicaFenceEpoch() throws Exception {
        ChunkId chunkId = new ChunkId(FileId.of(34), 0);
        Messages.Replica replica = new Messages.Replica(1, "node");

        ScpException e = assertThrows(ScpException.class,
                () -> invokeValidateFenceResp(chunkId, replica,
                        new Messages.FenceResp(3, 0, 0, ChunkState.OPEN), 2));

        assertEquals(ErrorCode.FENCED_EPOCH, e.code());
        assertEquals(3, e.detail());
    }

    @Test
    void validateFenceRespRejectsLowerReplicaFenceEpoch() throws Exception {
        ChunkId chunkId = new ChunkId(FileId.of(34), 0);
        Messages.Replica replica = new Messages.Replica(1, "node");

        ScpException e = assertThrows(ScpException.class,
                () -> invokeValidateFenceResp(chunkId, replica,
                        new Messages.FenceResp(1, 0, 0, ChunkState.OPEN), 2));

        assertEquals(ErrorCode.PRECONDITION_FAILED, e.code());
    }

    @Test
    void finishSealReportsQuorumLossWhenOnlyOneReplicaSealsSuccessfully() throws Exception {
        ChunkId chunkId = new ChunkId(FileId.of(35), 0);

        try (ScpServer sealed = openReplicaThatSealsAt(1, 4)) {
            ClientConfig config = new ClientConfig(List.of("127.0.0.1:1"), 1024, 500);
            try (NodePool pool = new NodePool()) {
                Recovery recovery = new Recovery(null, pool, config, StrataNamespace.of("test"));
                Object replica = replicaState(new Messages.Replica(1, endpoint(sealed)), 4, 4, ChunkState.OPEN);

                ScpException e = assertThrows(ScpException.class,
                        () -> invokeFinishSeal(recovery, chunkId, 2, 4, List.of(replica), 2));

                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(e.getMessage().contains("quorum lost"));
            }
        }
    }

    private static ScpServer sealedFenceReplica(int nodeId, long length, int sealCrc) throws Exception {
        return sealedFenceReplica(nodeId, length, sealCrc, null);
    }

    private static ScpServer sealedFenceReplica(int nodeId, long length, int sealCrc,
                                                AtomicBoolean sealCalled) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, length, length, ChunkState.SEALED).encode(),
                        null);
            }
            if (isReadOp(op)) {
                Messages.Read read = Messages.Read.decode(req.headerSlice());
                byte[] data = new byte[read.maxBytes()];
                return ScpServer.ok(req, new Messages.ReadResp(length, length).encode(), ByteBuffer.wrap(data));
            }
            if (op == Opcode.SEAL_CHUNK) {
                if (sealCalled != null) {
                    sealCalled.set(true);
                }
                Messages.SealChunk seal = Messages.SealChunk.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.SealResp(seal.dataLength(), sealCrc).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer catchUpReplica(int nodeId, long end, long durable, long sealLength,
                                            AtomicBoolean appendCalled) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, end, durable, ChunkState.OPEN).encode(), null);
            }
            if (isReadOp(op)) {
                Messages.Read read = Messages.Read.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.ReadResp(sealLength, sealLength).encode(),
                        ByteBuffer.wrap(new byte[read.maxBytes()]));
            }
            if (op == Opcode.APPEND) {
                Messages.Append append = Messages.Append.decode(req.headerSlice());
                if (appendCalled != null) {
                    appendCalled.set(true);
                }
                return ScpServer.ok(req, new Messages.AppendResp(append.baseOffset() + req.payloadLength()).encode(),
                        null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of()).encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                return ScpServer.ok(req, new Messages.SealResp(sealLength, 777).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer ledgerOrderingReplica(int nodeId, long end, List<Messages.LedgerEntry> ledger,
                                                   byte[] data, AtomicBoolean appendCalled) throws Exception {
        return ledgerOrderingReplica(nodeId, end, ledger, data, appendCalled, null);
    }

    private static ScpServer ledgerOrderingReplica(int nodeId, long end, List<Messages.LedgerEntry> ledger,
                                                   byte[] data, AtomicBoolean appendCalled,
                                                   AtomicBoolean sealCalled) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, end, 0, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(ledger).encode(), null);
            }
            if (isReadOp(op)) {
                Messages.Read read = Messages.Read.decode(req.headerSlice());
                byte[] payload = new byte[read.maxBytes()];
                System.arraycopy(data, (int) read.offset(), payload, 0, payload.length);
                return ScpServer.ok(req, new Messages.ReadResp(end, 0).encode(), ByteBuffer.wrap(payload));
            }
            if (op == Opcode.APPEND) {
                Messages.Append append = Messages.Append.decode(req.headerSlice());
                if (appendCalled != null) {
                    appendCalled.set(true);
                }
                return ScpServer.ok(req, new Messages.AppendResp(append.baseOffset() + req.payloadLength()).encode(),
                        null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                if (sealCalled != null) {
                    sealCalled.set(true);
                }
                Messages.SealChunk seal = Messages.SealChunk.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.SealResp(seal.dataLength(), 777).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer aboveFloorLedgerFailingReplica(int nodeId, long end, long durable,
                                                            AtomicBoolean sealCalled) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, end, durable, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                throw new ScpException(ErrorCode.INTERNAL, "ledger unavailable");
            }
            if (op == Opcode.SEAL_CHUNK) {
                Messages.SealChunk seal = Messages.SealChunk.decode(req.headerSlice());
                if (sealCalled != null) {
                    sealCalled.set(true);
                }
                return ScpServer.ok(req, new Messages.SealResp(seal.dataLength(), 777).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer aboveFloorReadFailingReplica(int nodeId, long end, long durable,
                                                          List<Messages.LedgerEntry> ledger,
                                                          AtomicBoolean sealCalled) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, end, durable, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(ledger).encode(), null);
            }
            if (op == Opcode.READ_RECOVERY) {
                throw new ScpException(ErrorCode.INTERNAL, "recovery read unavailable");
            }
            if (op == Opcode.SEAL_CHUNK) {
                Messages.SealChunk seal = Messages.SealChunk.decode(req.headerSlice());
                if (sealCalled != null) {
                    sealCalled.set(true);
                }
                return ScpServer.ok(req, new Messages.SealResp(seal.dataLength(), 777).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer malformedAboveFloorLedgerReplica(int nodeId, long end, long durable,
                                                              AtomicBoolean sealCalled) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, end, durable, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new byte[] {0, 0, (byte) 0x80}, null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                Messages.SealChunk seal = Messages.SealChunk.decode(req.headerSlice());
                if (sealCalled != null) {
                    sealCalled.set(true);
                }
                return ScpServer.ok(req, new Messages.SealResp(seal.dataLength(), 777).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer crcInvalidAboveFloorReplica(int nodeId, long end, long durable,
                                                         List<Messages.LedgerEntry> ledger, byte[] data,
                                                         AtomicBoolean sealCalled) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, end, durable, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(ledger).encode(), null);
            }
            if (isReadOp(op)) {
                Messages.Read read = Messages.Read.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.ReadResp(end, durable).encode(),
                        ByteBuffer.wrap(data, (int) read.offset(), read.maxBytes()));
            }
            if (op == Opcode.SEAL_CHUNK) {
                Messages.SealChunk seal = Messages.SealChunk.decode(req.headerSlice());
                if (sealCalled != null) {
                    sealCalled.set(true);
                }
                return ScpServer.ok(req, new Messages.SealResp(seal.dataLength(), 777).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer partialReadFailingReplica(int nodeId, long end) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, end, 0, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of()).encode(), null);
            }
            if (isReadOp(op)) {
                throw new ScpException(ErrorCode.INTERNAL, "partial overlap read unavailable");
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer partialReadableReplica(int nodeId, byte[] data) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, data.length, 0, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of()).encode(), null);
            }
            if (isReadOp(op)) {
                Messages.Read read = Messages.Read.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.ReadResp(data.length, 0).encode(),
                        ByteBuffer.wrap(data, (int) read.offset(), read.maxBytes()));
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer openReplicaWithFenceAndSeal(int nodeId, long end, long durable,
                                                         long sealLength, int sealCrc) throws Exception {
        return openReplicaWithFenceAndSeal(nodeId, end, durable, sealLength, sealCrc, null);
    }

    private static ScpServer openReplicaWithFenceAndSeal(int nodeId, long end, long durable,
                                                         long sealLength, int sealCrc,
                                                         AtomicBoolean sealCalled) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, end, durable, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of()).encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                if (sealCalled != null) {
                    sealCalled.set(true);
                }
                return ScpServer.ok(req, new Messages.SealResp(sealLength, sealCrc).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer openReplicaThatFailsSeal(int nodeId, String message) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, 0, 0, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of()).encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                throw new ScpException(ErrorCode.INTERNAL, message);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer replicaWithForgedLaterLedger(int nodeId, byte[] full, int forgedFullRangeCrc)
            throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, full.length, 0, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of(
                        new Messages.LedgerEntry(4, 0x12345678, 1),
                        new Messages.LedgerEntry(8, forgedFullRangeCrc, 1))).encode(), null);
            }
            if (isReadOp(op)) {
                Messages.Read read = Messages.Read.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.ReadResp(full.length, 0).encode(),
                        ByteBuffer.wrap(full, (int) read.offset(), read.maxBytes()));
            }
            if (op == Opcode.SEAL_CHUNK) {
                Messages.SealChunk seal = Messages.SealChunk.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.SealResp(seal.dataLength(), 123).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer ledgerDonor(int nodeId, byte[] data) throws Exception {
        int crc = Crc.of(data);
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, data.length, 0, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of(
                        new Messages.LedgerEntry(data.length, crc, 1))).encode(), null);
            }
            if (isReadOp(op)) {
                Messages.Read read = Messages.Read.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.ReadResp(data.length, 0).encode(),
                        ByteBuffer.wrap(data, (int) read.offset(), read.maxBytes()));
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer catchUpDonor(int nodeId, byte[] data) throws Exception {
        int crc = Crc.of(data);
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, data.length, data.length,
                        ChunkState.OPEN).encode(), null);
            }
            if (isReadOp(op)) {
                Messages.Read read = Messages.Read.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.ReadResp(data.length, data.length).encode(),
                        ByteBuffer.wrap(data, (int) read.offset(), read.maxBytes()));
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of()).encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                return ScpServer.ok(req, new Messages.SealResp(data.length, crc).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer oversizedLedgerReplica(int nodeId, long oversizedEnd, AtomicBoolean readCalled)
            throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, oversizedEnd, 0, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of(
                        new Messages.LedgerEntry(oversizedEnd, 123, 1))).encode(), null);
            }
            if (isReadOp(op)) {
                readCalled.set(true);
                throw new ScpException(ErrorCode.INTERNAL, "read should not be called");
            }
            if (op == Opcode.SEAL_CHUNK) {
                return ScpServer.ok(req, new Messages.SealResp(0, 123).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer malformedReadLedgerReplica(int nodeId, long end, int crc) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, end, 0, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of(
                        new Messages.LedgerEntry(end, crc, 1))).encode(), null);
            }
            if (isReadOp(op)) {
                return ScpServer.ok(req, new byte[] {0}, ByteBuffer.wrap(new byte[(int) end]));
            }
            if (op == Opcode.SEAL_CHUNK) {
                return ScpServer.ok(req, new Messages.SealResp(0, 123).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer openReplicaThatSealsAt(int nodeId, long finalLength) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, 0, 0, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of()).encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                return ScpServer.ok(req, new Messages.SealResp(finalLength, 123).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer openReplicaWithMalformedSeal(int nodeId) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, 0, 0, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of()).encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                return ScpServer.ok(req, new byte[] {0, 0}, null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer replicaThatFencesOnLedger(int nodeId, long detail) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, 0, 0, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                Messages.ReadLedger read = Messages.ReadLedger.decode(req.headerSlice());
                if (read.recoveryEpoch() != 2) {
                    throw new ScpException(ErrorCode.PRECONDITION_FAILED, "missing recovery epoch");
                }
                throw new ScpException(ErrorCode.FENCED_EPOCH, "re-fenced", detail);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer openReplicaThatFencesOnSeal(int nodeId, long detail) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.FENCE) {
                return ScpServer.ok(req, new Messages.FenceResp(2, 0, 0, ChunkState.OPEN).encode(), null);
            }
            if (op == Opcode.READ_LEDGER) {
                return ScpServer.ok(req, new Messages.ReadLedgerResp(List.of()).encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                throw new ScpException(ErrorCode.FENCED_EPOCH, "fenced", detail);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    /** Recovery byte reads arrive as READ_RECOVERY; plain READ is accepted too so fakes stay opcode-agnostic. */
    private static boolean isReadOp(Opcode op) {
        return op == Opcode.READ || op == Opcode.READ_RECOVERY;
    }

    private static byte[] readRangeFromServer(Messages.ReadResp response, int payloadLength) throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
            if (isReadOp(Opcode.fromCode(req.opcode()))) {
                return ScpServer.ok(req, response.encode(), ByteBuffer.wrap(new byte[payloadLength]));
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
        })) {
            ClientConfig config = new ClientConfig(List.of("127.0.0.1:1"), 1024, 500);
            try (NodePool pool = new NodePool()) {
                Recovery recovery = new Recovery(null, pool, config, StrataNamespace.of("test"));
                ChunkId chunkId = new ChunkId(FileId.of(36), 0);
                Object source = replicaState(new Messages.Replica(1, endpoint(server)), 4, 4, ChunkState.OPEN);
                return invokeReadRange(recovery, chunkId, source, 0, 4);
            }
        }
    }

    private static Object replicaState(Messages.Replica replica, long localEnd, long durable, ChunkState state)
            throws Exception {
        return new Recovery.ReplicaState(replica, new Messages.FenceResp(2, localEnd, durable, state));
    }

    private static byte[] invokeReadRange(Recovery recovery, ChunkId chunkId, Object source, long from, long to)
            throws Exception {
        Method method = Recovery.class.getDeclaredMethod("readRange", ChunkId.class,
                source.getClass(), long.class, long.class);
        method.setAccessible(true);
        return (byte[]) invoke(method, recovery, chunkId, source, from, to);
    }

    private static void invokeValidateFenceResp(ChunkId chunkId, Messages.Replica replica,
                                                Messages.FenceResp fence, int recoveryEpoch) throws Exception {
        Method method = Recovery.class.getDeclaredMethod("validateFenceResp",
                ChunkId.class, Messages.Replica.class, Messages.FenceResp.class, int.class);
        method.setAccessible(true);
        invoke(method, null, chunkId, replica, fence, recoveryEpoch);
    }

    private static long invokeFinishSeal(Recovery recovery, ChunkId chunkId, int epoch, long dataLength,
                                         List<Object> replicas, int ackQuorum) throws Exception {
        Method method = Recovery.class.getDeclaredMethod("finishSeal",
                ChunkId.class, int.class, long.class, List.class, int.class);
        method.setAccessible(true);
        return (long) invoke(method, recovery, chunkId, epoch, dataLength, replicas, ackQuorum);
    }

    private static Object invoke(Method method, Object target, Object... args) throws Exception {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw e;
        }
    }

    private static ScpServer metadataServer(AtomicReference<Messages.LookupFileResp> lookup,
                                            AtomicReference<Long> sealedFileLength) throws Exception {
        return new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.LOOKUP_FILE) {
                Messages.LookupFile.decode(req.headerSlice());
                return ScpServer.ok(req, lookup.get().encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK_META) {
                Messages.SealChunkMeta.decode(req.headerSlice());
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            if (op == Opcode.SEAL_FILE) {
                Messages.SealFile seal = Messages.SealFile.decode(req.headerSlice());
                if (sealedFileLength != null) {
                    sealedFileLength.set(seal.totalLength());
                }
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static Messages.LookupFileResp lookup(Messages.ChunkInfo... chunks) {
        return new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 0, List.of(chunks));
    }

    private static Messages.ChunkInfo chunk(ChunkId id, ChunkState state, long length, int epoch,
                                            Messages.Replica... replicas) {
        return new Messages.ChunkInfo(id, state, length, 0, epoch, List.of(replicas));
    }

    private static AutoCloseable unsafeSealOverride(ChunkId chunkId) {
        return unsafeSealOverride("test:" + chunkId);
    }

    private static AutoCloseable unsafeSealOverride(String value) {
        String previous = System.getProperty(Recovery.UNSAFE_SEAL_OVERRIDE_CHUNKS_PROPERTY);
        System.setProperty(Recovery.UNSAFE_SEAL_OVERRIDE_CHUNKS_PROPERTY, value);
        return () -> {
            if (previous == null) {
                System.clearProperty(Recovery.UNSAFE_SEAL_OVERRIDE_CHUNKS_PROPERTY);
            } else {
                System.setProperty(Recovery.UNSAFE_SEAL_OVERRIDE_CHUNKS_PROPERTY, previous);
            }
        };
    }

    private static String endpoint(ScpServer server) {
        return "127.0.0.1:" + server.port();
    }
}
