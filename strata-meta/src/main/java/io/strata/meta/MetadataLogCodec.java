package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import io.strata.common.Varint;
import io.strata.proto.BufWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Versioned binary codec for {@link MetadataLogRecord} (design §8). A leading type byte selects the
 * record family. Append-only: type bytes are never reused or renumbered, so a sealed log written by an
 * older version always decodes. Node-id lists use fixed u32s (ids are small positive ints).
 */
public final class MetadataLogCodec {
    private MetadataLogCodec() {}

    private static final byte FILE_CREATED = 1;
    private static final byte WRITER_EPOCH_ALLOCATED = 2;
    private static final byte CHUNK_CREATED = 3;
    private static final byte CHUNK_SEALED = 4;
    private static final byte CHUNK_ABORTED = 5;
    private static final byte CHUNK_DELETED = 6;
    private static final byte FILE_SEALED = 7;
    private static final byte FILE_DELETING = 8;
    private static final byte FILE_DELETED = 9;
    private static final byte PATH_UNBOUND = 10;
    private static final byte REPLICA_SWAPPED = 11;
    private static final byte REPLICA_DROPPED = 12;
    private static final byte REPLICA_ADDED = 13;
    private static final byte TOMBSTONE_SWEPT = 14;

    public static byte[] encode(MetadataLogRecord record) {
        BufWriter w = new BufWriter(64);
        switch (record) {
            case MetadataLogRecord.FileCreated r -> {
                w.u8(FILE_CREATED).fileId(r.fileId())
                        .string(r.namespace().toString()).string(r.path().toString())
                        .u32(r.replicationFactor()).u32(r.ackQuorum()).u8(r.fsyncOnAck() ? 1 : 0)
                        .u64(r.createdAtMs()).u64(r.createOpMsb()).u64(r.createOpLsb());
            }
            case MetadataLogRecord.WriterEpochAllocated r ->
                    w.u8(WRITER_EPOCH_ALLOCATED).fileId(r.fileId()).i32(r.writerEpoch());
            case MetadataLogRecord.ChunkCreated r -> {
                w.u8(CHUNK_CREATED).fileId(r.fileId()).u32(r.chunkIndex()).i32(r.writeEpoch())
                        .u64(r.createOpMsb()).u64(r.createOpLsb()).varint(r.replicas().size());
                for (int n : r.replicas()) w.u32(n);
            }
            case MetadataLogRecord.ChunkSealed r -> {
                w.u8(CHUNK_SEALED).fileId(r.fileId()).u32(r.chunkIndex()).u64(r.length())
                        .u32(r.crc()).i32(r.writeEpoch()).varint(r.sealedReplicas().size());
                for (int n : r.sealedReplicas()) w.u32(n);
            }
            case MetadataLogRecord.ChunkAborted r ->
                    w.u8(CHUNK_ABORTED).fileId(r.fileId()).u32(r.chunkIndex());
            case MetadataLogRecord.ChunkDeleted r ->
                    w.u8(CHUNK_DELETED).fileId(r.fileId()).u32(r.chunkIndex());
            case MetadataLogRecord.FileSealed r -> w.u8(FILE_SEALED).fileId(r.fileId());
            case MetadataLogRecord.FileDeleting r -> w.u8(FILE_DELETING).fileId(r.fileId());
            case MetadataLogRecord.FileDeleted r -> w.u8(FILE_DELETED).fileId(r.fileId()).u64(r.deletedAtMs());
            case MetadataLogRecord.PathUnbound r -> w.u8(PATH_UNBOUND)
                    .string(r.namespace().toString()).string(r.path().toString()).fileId(r.expectedFileId());
            case MetadataLogRecord.ReplicaSwapped r -> w.u8(REPLICA_SWAPPED)
                    .fileId(r.fileId()).u32(r.chunkIndex()).u32(r.fromNode()).u32(r.toNode());
            case MetadataLogRecord.ReplicaDropped r -> w.u8(REPLICA_DROPPED)
                    .fileId(r.fileId()).u32(r.chunkIndex()).u32(r.nodeId());
            case MetadataLogRecord.ReplicaAdded r -> w.u8(REPLICA_ADDED)
                    .fileId(r.fileId()).u32(r.chunkIndex()).u32(r.nodeId());
            case MetadataLogRecord.TombstoneSwept r -> w.u8(TOMBSTONE_SWEPT).fileId(r.fileId());
        }
        return w.toBytes();
    }

    public static MetadataLogRecord decode(byte[] bytes) {
        ByteBuffer b = ByteBuffer.wrap(bytes);
        byte type = b.get();
        return switch (type) {
            case FILE_CREATED -> new MetadataLogRecord.FileCreated(FileId.readFrom(b),
                    StrataNamespace.of(Varint.readString(b)), StrataPath.of(Varint.readString(b)),
                    b.getInt(), b.getInt(), Varint.readBoolean(b), b.getLong(), b.getLong(), b.getLong());
            case WRITER_EPOCH_ALLOCATED ->
                    new MetadataLogRecord.WriterEpochAllocated(FileId.readFrom(b), b.getInt());
            case CHUNK_CREATED -> {
                FileId f = FileId.readFrom(b);
                int idx = b.getInt();
                int writeEpoch = b.getInt();
                long opMsb = b.getLong();
                long opLsb = b.getLong();
                List<Integer> replicas = readInts(b);
                yield new MetadataLogRecord.ChunkCreated(f, idx, writeEpoch, replicas, opMsb, opLsb);
            }
            case CHUNK_SEALED -> {
                FileId f = FileId.readFrom(b);
                int idx = b.getInt();
                long length = b.getLong();
                int crc = b.getInt();
                int writeEpoch = b.getInt();
                List<Integer> replicas = readInts(b);
                yield new MetadataLogRecord.ChunkSealed(f, idx, length, crc, writeEpoch, replicas);
            }
            case CHUNK_ABORTED -> new MetadataLogRecord.ChunkAborted(FileId.readFrom(b), b.getInt());
            case CHUNK_DELETED -> new MetadataLogRecord.ChunkDeleted(FileId.readFrom(b), b.getInt());
            case FILE_SEALED -> new MetadataLogRecord.FileSealed(FileId.readFrom(b));
            case FILE_DELETING -> new MetadataLogRecord.FileDeleting(FileId.readFrom(b));
            case FILE_DELETED -> new MetadataLogRecord.FileDeleted(FileId.readFrom(b), b.getLong());
            case PATH_UNBOUND -> new MetadataLogRecord.PathUnbound(
                    StrataNamespace.of(Varint.readString(b)), StrataPath.of(Varint.readString(b)),
                    FileId.readFrom(b));
            case REPLICA_SWAPPED -> new MetadataLogRecord.ReplicaSwapped(FileId.readFrom(b),
                    b.getInt(), b.getInt(), b.getInt());
            case REPLICA_DROPPED -> new MetadataLogRecord.ReplicaDropped(FileId.readFrom(b),
                    b.getInt(), b.getInt());
            case REPLICA_ADDED -> new MetadataLogRecord.ReplicaAdded(FileId.readFrom(b),
                    b.getInt(), b.getInt());
            case TOMBSTONE_SWEPT -> new MetadataLogRecord.TombstoneSwept(FileId.readFrom(b));
            default -> throw new IllegalArgumentException("unknown metadata-log record type " + type);
        };
    }

    private static List<Integer> readInts(ByteBuffer b) {
        int n = Varint.readCount(b, "replica");
        List<Integer> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(b.getInt());
        }
        return out;
    }
}
