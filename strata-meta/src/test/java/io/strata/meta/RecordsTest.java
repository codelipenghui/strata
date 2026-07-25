package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import io.strata.proto.BufWriter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordsTest {

    @Test
    void fileAndNodeStatesDecodeAndRejectUnknownValues() {
        assertEquals(FileState.OPEN, FileState.fromValue((byte) 0));
        assertEquals(FileState.SEALED, FileState.fromValue((byte) 1));
        assertEquals(FileState.DELETING, FileState.fromValue((byte) 2));
        assertThrows(IllegalArgumentException.class, () -> FileState.fromValue((byte) 99));

        assertEquals(Records.NodeState.REGISTERED, Records.NodeState.from((byte) 0));
        assertEquals(Records.NodeState.DRAINING, Records.NodeState.from((byte) 1));
        assertEquals(Records.NodeState.DEAD, Records.NodeState.from((byte) 2));
        assertThrows(IllegalArgumentException.class, () -> Records.NodeState.from((byte) 99));
    }

    @Test
    void chunkRecordHelpersPreserveOperationIdentity() {
        Records.ChunkRecord legacy = new Records.ChunkRecord(2, ChunkState.OPEN, 0, 0, 7, List.of(1, 2, 3));
        assertTrue(legacy.createdBy(0, 0));

        Records.ChunkRecord created = new Records.ChunkRecord(2, ChunkState.OPEN, 0, 0, 7,
                List.of(1, 2, 3), 11, 22);
        assertTrue(created.createdBy(11, 22));
        assertFalse(created.createdBy(11, 23));

        assertEquals(List.of(1, 9, 3), created.withReplicaSwapped(2, 9).replicas());
        assertEquals(List.of(4, 5), created.withReplicas(List.of(4, 5)).replicas());

        Records.ChunkRecord sealed = created.sealed(128, 0xCAFE, 8);
        assertEquals(ChunkState.SEALED, sealed.state());
        assertEquals(128, sealed.length());
        assertEquals(0xCAFE, sealed.crc());
        assertEquals(8, sealed.writeEpoch());
        assertTrue(sealed.createdBy(11, 22));
    }

    @Test
    void fileRecordRoundtripAndRejectsUnknownVersions() {
        FileId fileId = FileId.of(1);
        Records.ChunkRecord chunk = new Records.ChunkRecord(3, ChunkState.SEALED, 4096, 0xAA, 5,
                List.of(7, 8), 33, 44);
        Records.FileRecord record = new Records.FileRecord(fileId, "test", "/test-file",
                3, 2, true, FileState.OPEN, 1234, List.of(chunk), 11, 22);

        assertEquals(new ChunkId(fileId, 3), record.chunkId(3));
        assertEquals(StrataNamespace.of("test"), record.namespace());
        assertEquals(3, record.replicationFactor());
        assertEquals(2, record.ackQuorum());
        assertTrue(record.fsyncOnAck());
        assertEquals(5, record.writerEpoch());
        assertEquals(4, record.nextChunkIndex());
        assertTrue(record.createdBy(11, 22));
        assertFalse(record.createdBy(11, 23));
        assertEquals(FileState.SEALED, record.withState(FileState.SEALED).state());
        assertEquals(0, record.withChunks(List.of()).chunks().size());
        assertEquals(5, record.withChunks(List.of()).writerEpoch());
        assertEquals(4, record.withChunks(List.of()).nextChunkIndex());
        assertEquals(6, record.withWriterEpoch(6).writerEpoch());
        Records.FileRecord typed = new Records.FileRecord(fileId, StrataNamespace.of("typed"),
                StrataPath.of("/typed-file"), 3, 2, false,
                FileState.OPEN, 1234, List.of());
        assertTrue(typed.createdBy(0, 0));
        assertEquals(0, typed.nextChunkIndex());
        assertEquals(StrataPath.of("/typed-file"), typed.path());
        assertEquals(record, Records.FileRecord.decode(record.encode()));
        assertThrows(IllegalArgumentException.class,
                () -> new Records.FileRecord(fileId, StrataNamespace.of("test"),
                        StrataPath.of("/bad-negative-writer-epoch"),
                        3, 2, false, -1, FileState.OPEN, 1234,
                        List.of(), 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new Records.FileRecord(fileId, StrataNamespace.of("test"),
                        StrataPath.of("/bad-stale-writer-epoch"),
                        3, 2, false, 4, FileState.OPEN, 1234,
                        List.of(chunk), 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new Records.FileRecord(fileId, StrataNamespace.of("test"),
                        StrataPath.of("/bad-non-intersecting-quorum"),
                        4, 2, false, FileState.OPEN, 1234,
                        List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new Records.FileRecord(fileId, StrataNamespace.of("test"),
                        StrataPath.of("/bad-chunk-high-water-mark"),
                        3, 2, false, 5, FileState.OPEN, 1234,
                        List.of(chunk), 0, 0, 3));

        byte[] invalid = record.encode();
        invalid[0] = 99;
        assertThrows(IllegalArgumentException.class, () -> Records.FileRecord.decode(invalid));
        assertThrows(IllegalArgumentException.class, () -> Records.FileRecord.decode(legacyFileRecordBytes(fileId)));
    }

    @Test
    void fileRecordNextChunkIndexIsMonotonicAcrossTailRemovalAndRoundtrip() {
        Records.ChunkRecord chunk3 = new Records.ChunkRecord(
                3, ChunkState.SEALED, 4096, 0xAA, 5, List.of(7, 8));
        Records.FileRecord record = new Records.FileRecord(
                FileId.of(1), "test", "/chunk-high-water", 3, 2, true,
                FileState.OPEN, 1234, List.of(chunk3));
        assertEquals(4, record.nextChunkIndex());

        Records.FileRecord afterRemoval = record.withChunks(List.of());
        assertEquals(4, afterRemoval.nextChunkIndex(), "removing the tail must not lower the high-water mark");

        Records.ChunkRecord chunk7 = new Records.ChunkRecord(
                7, ChunkState.OPEN, 0, 0, 6, List.of(7, 8));
        Records.FileRecord afterAddition = afterRemoval.withChunks(List.of(chunk7));
        assertEquals(8, afterAddition.nextChunkIndex(), "adding a chunk must raise the high-water mark");

        Records.FileRecord removedAgain = afterAddition.withChunks(List.of());
        assertEquals(8, removedAgain.nextChunkIndex());
        assertEquals(removedAgain, Records.FileRecord.decode(removedAgain.encode()));
    }

    @Test
    void fileRecordV7DecodeMigratesHighWaterMarkFromLiveChunks() {
        Records.ChunkRecord chunk3 = new Records.ChunkRecord(
                3, ChunkState.SEALED, 4096, 0xAA, 5, List.of(7, 8), 33, 44);
        Records.FileRecord source = new Records.FileRecord(
                FileId.of(1), "test", "/legacy-v7", 3, 2, true,
                FileState.OPEN, 1234, List.of(chunk3), 11, 22);

        Records.FileRecord migrated = Records.FileRecord.decode(fileRecordV7Bytes(source));

        assertEquals(source, migrated);
        assertEquals(4, migrated.nextChunkIndex());
        Records.FileRecord afterRemoval = migrated.withChunks(List.of());
        assertEquals(4, afterRemoval.nextChunkIndex());
        assertEquals(8, Byte.toUnsignedInt(afterRemoval.encode()[0]));
        assertEquals(afterRemoval, Records.FileRecord.decode(afterRemoval.encode()));
    }

    @Test
    void recordsDefensivelyCopyLists() {
        List<Integer> replicas = new ArrayList<>(List.of(1, 2));
        Records.ChunkRecord chunk = new Records.ChunkRecord(0, ChunkState.OPEN, 0, 0, 1, replicas);
        replicas.add(3);
        assertEquals(List.of(1, 2), chunk.replicas());
        assertThrows(UnsupportedOperationException.class, () -> chunk.replicas().add(4));

        List<Records.ChunkRecord> chunks = new ArrayList<>(List.of(chunk));
        Records.FileRecord file = new Records.FileRecord(FileId.of(1), "test", "/copy-test",
                3, 2, false, FileState.OPEN, 1, chunks);
        chunks.clear();
        assertEquals(List.of(chunk), file.chunks());
        assertThrows(UnsupportedOperationException.class, () -> file.chunks().clear());

        List<String> endpoints = new ArrayList<>(List.of("h:1"));
        Records.NodeRecord node = new Records.NodeRecord(1, 1, 2, endpoints, "z", "r", "h", 1, Records.NodeState.REGISTERED);
        endpoints.add("h:2");
        assertEquals(List.of("h:1"), node.endpoints());
        assertThrows(UnsupportedOperationException.class, () -> node.endpoints().add("h:3"));
    }

    @Test
    void recordDecodeRejectsUnboundedCountsBeforeAllocation() {
        assertThrows(IllegalArgumentException.class,
                () -> Records.FileRecord.decode(fileRecordWithChunkCount(-1)));
        assertThrows(IllegalArgumentException.class,
                () -> Records.FileRecord.decode(fileRecordWithChunkCount(1_000_001)));
        assertThrows(IllegalArgumentException.class,
                () -> Records.FileRecord.decode(fileRecordWithReplicaCount(-1)));
        assertThrows(IllegalArgumentException.class,
                () -> Records.FileRecord.decode(fileRecordWithReplicaCount(1_000_001)));
        assertThrows(IllegalArgumentException.class,
                () -> Records.NodeRecord.decode(nodeRecordWithEndpointCount(-1)));
        assertThrows(IllegalArgumentException.class,
                () -> Records.NodeRecord.decode(nodeRecordWithEndpointCount(1_000_001)));
    }

    @Test
    void nodeRecordRoundtripAndHelpers() {
        Records.NodeRecord node = new Records.NodeRecord(7, 1, 2, List.of("h:9000"), "z", "r", "host", 1000, Records.NodeState.DRAINING);
        assertEquals("h:9000", node.endpoint());
        assertEquals("", new Records.NodeRecord(8, 1, 2, List.of(), "z", "r", "host", 1000, Records.NodeState.REGISTERED).endpoint());

        assertEquals(Records.NodeState.DEAD, node.withState(Records.NodeState.DEAD).state());
        assertEquals(node, Records.NodeRecord.decode(node.encode()));

        byte[] invalid = node.encode();
        invalid[0] = 9;
        assertThrows(IllegalArgumentException.class, () -> Records.NodeRecord.decode(invalid));
    }

    @Test
    void namespaceManifestV2RoundTripsPreviousReference() {
        Records.NamespaceManifest.NamespaceManifestRef previous =
                new Records.NamespaceManifest.NamespaceManifestRef(6, 128, 256,
                        Optional.of(FileId.of(10)), Optional.of(FileId.of(11)));
        Records.NamespaceManifest manifest = new Records.NamespaceManifest(StrataNamespace.of("test"), 3, 7,
                256, 384, Optional.of(FileId.of(12)), Optional.of(FileId.of(13)), Optional.of(previous));

        Records.NamespaceManifest decoded = Records.NamespaceManifest.decode(manifest.encode());

        assertEquals(manifest, decoded);
        assertEquals(previous, decoded.previous().orElseThrow());
    }

    @Test
    void namespaceManifestV1BytesDecodeWithoutPreviousReference() {
        Records.NamespaceManifest decoded = Records.NamespaceManifest.decode(namespaceManifestV1Bytes());

        assertEquals(StrataNamespace.of("legacy"), decoded.namespace());
        assertEquals(4, decoded.metadataEpoch());
        assertEquals(5, decoded.generation());
        assertEquals(1024, decoded.logStartOffset());
        assertEquals(2048, decoded.publishedLogOffset());
        assertEquals(Optional.of(FileId.of(21)), decoded.snapshotFileId());
        assertEquals(Optional.of(FileId.of(22)), decoded.logFileId());
        assertTrue(decoded.previous().isEmpty());
    }

    @Test
    void namespaceAssignmentV2RoundTripsLeaderIncarnation() {
        UUID incarnation = UUID.randomUUID();
        Records.NamespaceAssignment assignment = new Records.NamespaceAssignment(
                StrataNamespace.of("tenant-a"), 7, List.of("m2:9301", "m3:9301"), incarnation);

        Records.NamespaceAssignment decoded = Records.NamespaceAssignment.decode(assignment.encode());

        assertEquals(assignment, decoded);
        assertTrue(decoded.hasBoundLeader());
        assertEquals(incarnation, decoded.leaderIncarnation());
    }

    @Test
    void namespaceAssignmentV1MigratesToFailClosedUnboundLeader() {
        Records.NamespaceAssignment decoded =
                Records.NamespaceAssignment.decode(namespaceAssignmentV1Bytes(false));

        assertEquals(StrataNamespace.of("legacy-assignment"), decoded.namespace());
        assertEquals(List.of("m1:9301", "m2:9301"), decoded.replicaSet());
        assertFalse(decoded.hasBoundLeader());
        assertEquals(Records.NamespaceAssignment.UNBOUND_LEADER_INCARNATION,
                decoded.leaderIncarnation());
    }

    @Test
    void namespaceAssignmentRejectsMalformedOrTrailingIncarnationBytes() {
        assertThrows(IllegalArgumentException.class,
                () -> Records.NamespaceAssignment.decode(namespaceAssignmentV2WithUuidBytes(8)));
        assertThrows(IllegalArgumentException.class,
                () -> Records.NamespaceAssignment.decode(namespaceAssignmentV2WithUuidBytes(17)));
        assertThrows(IllegalArgumentException.class,
                () -> Records.NamespaceAssignment.decode(namespaceAssignmentV1Bytes(true)));
    }

    @Test
    void namespaceAssignmentRejectsTruncationAndInvalidReplicaSets() {
        assertThrows(IllegalArgumentException.class,
                () -> Records.NamespaceAssignment.decode(Records.sealRecord(new byte[0])));
        BufWriter emptyReplicaSet = new BufWriter();
        emptyReplicaSet.u8(2).string("decoded-empty").i32(0).varint(0).u64(1).u64(2);
        assertThrows(IllegalArgumentException.class,
                () -> Records.NamespaceAssignment.decode(
                        Records.sealRecord(emptyReplicaSet.toBytes())));
        assertThrows(IllegalArgumentException.class, () -> new Records.NamespaceAssignment(
                StrataNamespace.of("empty"), 0, List.of(), UUID.randomUUID()));
        assertThrows(IllegalArgumentException.class, () -> new Records.NamespaceAssignment(
                StrataNamespace.of("blank"), 0, List.of("m1:9301", " "), UUID.randomUUID()));
        assertThrows(IllegalArgumentException.class, () -> new Records.NamespaceAssignment(
                StrataNamespace.of("duplicate"), 0, List.of("m1:9301", "m1:9301"), UUID.randomUUID()));
    }

    @Test
    void recordEncodingCarriesCrcThatCatchesSilentCorruption() {
        Records.ChunkRecord chunk = new Records.ChunkRecord(3, ChunkState.SEALED, 4096, 0xAA, 5,
                List.of(7, 8), 33, 44);
        Records.FileRecord record = new Records.FileRecord(FileId.of(1), "test", "/crc-file",
                3, 2, true, FileState.OPEN, 1234, List.of(chunk), 11, 22);
        byte[] enc = record.encode();
        assertEquals(record, Records.FileRecord.decode(enc));   // round-trip is intact

        // A silent single-bit flip anywhere in the encoded record (a ZooKeeper bit-rot) must be caught
        // by the trailing CRC32C envelope, not decoded as a different-but-plausible record.
        byte[] corrupt = enc.clone();
        corrupt[enc.length / 2] ^= 0x01;
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> Records.FileRecord.decode(corrupt));
        assertTrue(e.getMessage().contains("crc"));
    }

    @Test
    void preEnvelopeRecordBytesAreRejected() {
        // Clean break (no back-compat / no migration shim — project policy): a record written by pre-PR
        // code carries no CRC envelope, so its trailing 4 bytes are real record fields, not a checksum.
        // Decode must fail closed rather than parse it as a plausible record. (ZK is redeployed fresh, so
        // there is no legacy data to migrate; this documents and pins the intentional break.)
        Records.FileRecord record = new Records.FileRecord(FileId.of(1), "test", "/pre-envelope",
                3, 2, false, FileState.OPEN, 1234, List.of());
        byte[] enveloped = record.encode();
        byte[] preEnvelope = Arrays.copyOf(enveloped, enveloped.length - 4); // strip CRC = old format
        assertThrows(IllegalArgumentException.class, () -> Records.FileRecord.decode(preEnvelope));
    }

    private static byte[] legacyFileRecordBytes(FileId fileId) {
        BufWriter w = new BufWriter();
        w.u8(1);
        w.fileId(fileId);
        w.u8(1).u8(2).u8(3).string("legacy").u8(FileState.DELETING.value).u64(99);
        w.varint(1);
        w.u32(4).u8(ChunkState.OPEN.value).u64(10).u32(0xBEEF).i32(6);
        w.varint(2).u32(1).u32(2);
        return Records.sealRecord(w.toBytes());
    }

    private static byte[] fileRecordV7Bytes(Records.FileRecord record) {
        BufWriter w = new BufWriter();
        w.u8(7);
        w.fileId(record.fileId());
        w.string(record.namespace().toString()).string(record.path().toString());
        w.u32(record.replicationFactor()).u32(record.ackQuorum()).u8(record.fsyncOnAck() ? 1 : 0);
        w.i32(record.writerEpoch()).u8(record.state().value).u64(record.createdAtMs());
        w.u64(record.createOpMsb()).u64(record.createOpLsb());
        w.varint(record.chunks().size());
        for (Records.ChunkRecord chunk : record.chunks()) {
            w.u32(chunk.index()).u8(chunk.state().value).u64(chunk.length()).u32(chunk.crc())
                    .i32(chunk.writeEpoch());
            w.u64(chunk.createOpMsb()).u64(chunk.createOpLsb());
            w.varint(chunk.replicas().size());
            for (int replica : chunk.replicas()) {
                w.u32(replica);
            }
        }
        return Records.sealRecord(w.toBytes());
    }

    private static byte[] namespaceManifestV1Bytes() {
        BufWriter w = new BufWriter();
        w.u8(1).string("legacy").u64(4).u64(5).u64(1024).u64(2048);
        writeOptionalFileId(w, Optional.of(FileId.of(21)));
        writeOptionalFileId(w, Optional.of(FileId.of(22)));
        return Records.sealRecord(w.toBytes());
    }

    private static byte[] namespaceAssignmentV1Bytes(boolean trailingByte) {
        BufWriter w = new BufWriter();
        w.u8(1).string("legacy-assignment").i32(3).varint(2)
                .string("m1:9301").string("m2:9301");
        if (trailingByte) {
            w.u8(1);
        }
        return Records.sealRecord(w.toBytes());
    }

    private static byte[] namespaceAssignmentV2WithUuidBytes(int uuidBytes) {
        BufWriter w = new BufWriter();
        w.u8(2).string("bad-assignment").i32(3).varint(2)
                .string("m1:9301").string("m2:9301");
        for (int i = 0; i < uuidBytes; i++) {
            w.u8(i);
        }
        return Records.sealRecord(w.toBytes());
    }

    private static void writeOptionalFileId(BufWriter w, Optional<FileId> id) {
        if (id.isPresent()) {
            w.u8(1).fileId(id.get());
        } else {
            w.u8(0);
        }
    }

    private static byte[] fileRecordWithChunkCount(int count) {
        BufWriter w = new BufWriter();
        w.u8(7);
        w.fileId(FileId.of(1));
        w.string("test").string("/test-file");
        w.u32(3).u32(2).u8(0).i32(0).u8(FileState.OPEN.value).u64(1);
        w.u64(0).u64(0);
        w.varint(count);
        return Records.sealRecord(w.toBytes());
    }

    private static byte[] fileRecordWithReplicaCount(int count) {
        BufWriter w = new BufWriter();
        w.u8(7);
        w.fileId(FileId.of(1));
        w.string("test").string("/test-file");
        w.u32(3).u32(2).u8(0).i32(1).u8(FileState.OPEN.value).u64(1);
        w.u64(0).u64(0);
        w.varint(1);
        w.u32(0).u8(ChunkState.OPEN.value).u64(0).u32(0).i32(1);
        w.u64(0).u64(0);
        w.varint(count);
        return Records.sealRecord(w.toBytes());
    }

    private static byte[] nodeRecordWithEndpointCount(int count) {
        BufWriter w = new BufWriter();
        w.u8(1);
        w.u32(1).u64(1).u64(2);
        w.varint(count);
        return Records.sealRecord(w.toBytes());
    }
}
