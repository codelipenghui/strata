package io.strata.it;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.proto.Messages;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConsistencyVerifierTest {

    @Test
    void liveDescriptorAcceptsSealedPrefixWithOpenTail() {
        FileId fileId = FileId.of(1);
        Messages.LookupFileResp descriptor = descriptor(fileId, FileState.OPEN, List.of(
                chunk(fileId, 0, ChunkState.SEALED, 128, 1, 2, 3),
                chunk(fileId, 1, ChunkState.OPEN, 0, 1, 2, 3)));

        assertDoesNotThrow(() -> ConsistencyVerifier.assertLiveDescriptorInvariants(fileId, descriptor));
    }

    @Test
    void liveDescriptorRejectsOpenChunkBeforeTail() {
        FileId fileId = FileId.of(1);
        Messages.LookupFileResp descriptor = descriptor(fileId, FileState.OPEN, List.of(
                chunk(fileId, 0, ChunkState.OPEN, 0, 1, 2, 3),
                chunk(fileId, 1, ChunkState.SEALED, 128, 1, 2, 3)));

        assertThrows(AssertionError.class,
                () -> ConsistencyVerifier.assertLiveDescriptorInvariants(fileId, descriptor));
    }

    @Test
    void liveDescriptorRejectsDuplicateReplicaNode() {
        FileId fileId = FileId.of(1);
        Messages.LookupFileResp descriptor = descriptor(fileId, FileState.SEALED, List.of(
                chunk(fileId, 0, ChunkState.SEALED, 128,
                        replica(1, "127.0.0.1:10001"),
                        replica(1, "127.0.0.1:10002"))));

        assertThrows(AssertionError.class,
                () -> ConsistencyVerifier.assertLiveDescriptorInvariants(fileId, descriptor));
    }

    @Test
    void liveDescriptorRejectsChunkBelowAckQuorum() {
        FileId fileId = FileId.of(1);
        Messages.LookupFileResp descriptor = descriptor(fileId, FileState.SEALED, List.of(
                chunk(fileId, 0, ChunkState.SEALED, 128, 1)));

        assertThrows(AssertionError.class,
                () -> ConsistencyVerifier.assertLiveDescriptorInvariants(fileId, descriptor));
    }

    @Test
    void liveDescriptorRejectsNonContiguousChunkIndex() {
        FileId fileId = FileId.of(1);
        Messages.LookupFileResp descriptor = descriptor(fileId, FileState.SEALED, List.of(
                chunk(fileId, 0, ChunkState.SEALED, 128, 1, 2, 3),
                chunk(fileId, 2, ChunkState.SEALED, 128, 1, 2, 3)));

        assertThrows(AssertionError.class,
                () -> ConsistencyVerifier.assertLiveDescriptorInvariants(fileId, descriptor));
    }

    @Test
    void liveDescriptorRejectsOpenChunkWithCommittedLength() {
        FileId fileId = FileId.of(1);
        Messages.LookupFileResp descriptor = descriptor(fileId, FileState.OPEN, List.of(
                chunkWithCrc(fileId, 0, ChunkState.OPEN, 64, 0, 1, 2, 3)));

        assertThrows(AssertionError.class,
                () -> ConsistencyVerifier.assertLiveDescriptorInvariants(fileId, descriptor));
    }

    @Test
    void liveDescriptorRejectsOpenChunkWithSealedCrc() {
        FileId fileId = FileId.of(1);
        Messages.LookupFileResp descriptor = descriptor(fileId, FileState.OPEN, List.of(
                chunkWithCrc(fileId, 0, ChunkState.OPEN, 0, 1234, 1, 2, 3)));

        assertThrows(AssertionError.class,
                () -> ConsistencyVerifier.assertLiveDescriptorInvariants(fileId, descriptor));
    }

    private static Messages.LookupFileResp descriptor(FileId fileId, FileState state,
                                                      List<Messages.ChunkInfo> chunks) {
        return new Messages.LookupFileResp("test", "/" + fileId,
                new Messages.WritePolicy(3, 2, false), state.value, chunks);
    }

    private static Messages.ChunkInfo chunk(FileId fileId, int index, ChunkState state,
                                            long length, int... nodeIds) {
        List<Messages.Replica> replicas = Arrays.stream(nodeIds)
                .mapToObj(ConsistencyVerifierTest::replica)
                .toList();
        return chunk(fileId, index, state, length, replicas);
    }

    private static Messages.ChunkInfo chunk(FileId fileId, int index, ChunkState state,
                                            long length, Messages.Replica... replicas) {
        return chunk(fileId, index, state, length, List.of(replicas));
    }

    private static Messages.ChunkInfo chunk(FileId fileId, int index, ChunkState state,
                                            long length, List<Messages.Replica> replicas) {
        int crc = state == ChunkState.OPEN ? 0 : 1234;
        return new Messages.ChunkInfo(new ChunkId(fileId, index), state, length, crc, 1, replicas);
    }

    private static Messages.ChunkInfo chunkWithCrc(FileId fileId, int index, ChunkState state,
                                                   long length, int crc, int... nodeIds) {
        List<Messages.Replica> replicas = Arrays.stream(nodeIds)
                .mapToObj(ConsistencyVerifierTest::replica)
                .toList();
        return new Messages.ChunkInfo(new ChunkId(fileId, index), state, length, crc, 1, replicas);
    }

    private static Messages.Replica replica(int nodeId) {
        return replica(nodeId, "127.0.0.1:" + (10_000 + nodeId));
    }

    private static Messages.Replica replica(int nodeId, String endpoint) {
        return new Messages.Replica(nodeId, endpoint);
    }
}
