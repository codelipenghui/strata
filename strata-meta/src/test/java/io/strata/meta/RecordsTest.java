package io.strata.meta;

import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.proto.BufWriter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordsTest {

    @Test
    void fileAndNodeStatesDecodeAndRejectUnknownValues() {
        assertEquals(Records.FileState.OPEN, Records.FileState.from((byte) 0));
        assertEquals(Records.FileState.SEALED, Records.FileState.from((byte) 1));
        assertEquals(Records.FileState.DELETING, Records.FileState.from((byte) 2));
        assertThrows(IllegalArgumentException.class, () -> Records.FileState.from((byte) 99));

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
        FileId fileId = new FileId(1, 2);
        Records.ChunkRecord chunk = new Records.ChunkRecord(3, ChunkState.SEALED, 4096, 0xAA, 5,
                List.of(7, 8), 33, 44);
        Records.FileRecord record = new Records.FileRecord(fileId, "test", "/test-file",
                (byte) 1, (byte) 2, (byte) 3, Records.FileState.OPEN, 1234, List.of(chunk), 11, 22);

        assertEquals(new io.strata.common.ChunkId(fileId, 3), record.chunkId(3));
        assertEquals(io.strata.common.StrataNamespace.of("test"), record.namespace());
        assertTrue(record.createdBy(11, 22));
        assertFalse(record.createdBy(11, 23));
        assertEquals(Records.FileState.SEALED, record.withState(Records.FileState.SEALED).state());
        assertEquals(0, record.withChunks(List.of()).chunks().size());
        Records.FileRecord typed = new Records.FileRecord(fileId, io.strata.common.StrataNamespace.of("typed"),
                io.strata.common.StrataPath.of("/typed-file"), (byte) 1, (byte) 2, (byte) 3,
                Records.FileState.OPEN, 1234, List.of());
        assertTrue(typed.createdBy(0, 0));
        assertEquals(io.strata.common.StrataPath.of("/typed-file"), typed.path());
        assertEquals(record, Records.FileRecord.decode(record.encode()));

        byte[] invalid = record.encode();
        invalid[0] = 99;
        assertThrows(IllegalArgumentException.class, () -> Records.FileRecord.decode(invalid));
        assertThrows(IllegalArgumentException.class, () -> Records.FileRecord.decode(legacyFileRecordBytes(fileId)));
    }

    @Test
    void recordsDefensivelyCopyLists() {
        List<Integer> replicas = new ArrayList<>(List.of(1, 2));
        Records.ChunkRecord chunk = new Records.ChunkRecord(0, ChunkState.OPEN, 0, 0, 1, replicas);
        replicas.add(3);
        assertEquals(List.of(1, 2), chunk.replicas());
        assertThrows(UnsupportedOperationException.class, () -> chunk.replicas().add(4));

        List<Records.ChunkRecord> chunks = new ArrayList<>(List.of(chunk));
        Records.FileRecord file = new Records.FileRecord(new FileId(1, 2), "test", "/test-file",
                (byte) 0, (byte) 0, (byte) 0, Records.FileState.OPEN, 1, chunks);
        chunks.clear();
        assertEquals(List.of(chunk), file.chunks());
        assertThrows(UnsupportedOperationException.class, () -> file.chunks().clear());

        List<String> endpoints = new ArrayList<>(List.of("h:1"));
        Records.NodeRecord node = new Records.NodeRecord(1, 1, 2, endpoints,
                "z", "r", "h", (byte) 0, 1, Records.NodeState.REGISTERED);
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
        Records.NodeRecord node = new Records.NodeRecord(7, 1, 2, List.of("h:9000"),
                "z", "r", "host", (byte) 3, 1000, Records.NodeState.DRAINING);
        assertEquals("h:9000", node.endpoint());
        assertEquals("", new Records.NodeRecord(8, 1, 2, List.of(), "z", "r", "host",
                (byte) 3, 1000, Records.NodeState.REGISTERED).endpoint());

        Records.NodeRecord reincarnated = node.withIncarnation(10, 20, List.of("new:1"),
                "z2", "r2", "host2", (byte) 4, 2000);
        assertEquals(7, reincarnated.nodeId());
        assertEquals(Records.NodeState.REGISTERED, reincarnated.state());
        assertEquals("new:1", reincarnated.endpoint());

        assertEquals(Records.NodeState.DEAD, node.withState(Records.NodeState.DEAD).state());
        assertEquals(node, Records.NodeRecord.decode(node.encode()));

        byte[] invalid = node.encode();
        invalid[0] = 9;
        assertThrows(IllegalArgumentException.class, () -> Records.NodeRecord.decode(invalid));
    }

    @Test
    void productionConfigUsesExpectedDefaults() {
        MetaConfig cfg = MetaConfig.production("zk:2181", 7000);
        assertEquals("zk:2181", cfg.zkConnect());
        assertEquals(7000, cfg.listenPort());
        assertEquals(1_000, cfg.heartbeatIntervalMs());
        assertEquals(10_000, cfg.leaseMs());
        assertEquals(300_000, cfg.deadGraceMs());
        assertEquals(30_000, cfg.repairScanIntervalMs());
        assertEquals(600_000, cfg.repairCommandTimeoutMs());
    }

    private static byte[] legacyFileRecordBytes(FileId fileId) {
        BufWriter w = new BufWriter();
        w.u8(1);
        w.fileId(fileId);
        w.u8(1).u8(2).u8(3).string("legacy").u8(Records.FileState.DELETING.value).u64(99);
        w.varint(1);
        w.u32(4).u8(ChunkState.OPEN.value).u64(10).u32(0xBEEF).i32(6);
        w.varint(2).u32(1).u32(2);
        return w.toBytes();
    }

    private static byte[] fileRecordWithChunkCount(int count) {
        BufWriter w = new BufWriter();
        w.u8(4);
        w.fileId(new FileId(1, 2));
        w.string("test").string("/test-file").u8(0).u8(0).u8(0).u8(Records.FileState.OPEN.value).u64(1);
        w.u64(0).u64(0);
        w.varint(count);
        return w.toBytes();
    }

    private static byte[] fileRecordWithReplicaCount(int count) {
        BufWriter w = new BufWriter();
        w.u8(4);
        w.fileId(new FileId(1, 2));
        w.string("test").string("/test-file").u8(0).u8(0).u8(0).u8(Records.FileState.OPEN.value).u64(1);
        w.u64(0).u64(0);
        w.varint(1);
        w.u32(0).u8(ChunkState.OPEN.value).u64(0).u32(0).i32(1);
        w.u64(0).u64(0);
        w.varint(count);
        return w.toBytes();
    }

    private static byte[] nodeRecordWithEndpointCount(int count) {
        BufWriter w = new BufWriter();
        w.u8(1);
        w.u32(1).u64(1).u64(2);
        w.varint(count);
        return w.toBytes();
    }
}
