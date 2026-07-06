package io.strata.meta;

import io.strata.common.Crc;
import io.strata.common.FileId;
import io.strata.common.Varint;
import io.strata.proto.BufWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CRC-protected envelope for a compacted namespace snapshot (design §4.2). The body holds the
 * snapshot version, next file id high-water, next log start offset, file table, per-file CAS versions,
 * and tombstone deletion timestamps; a trailing CRC32C detects corruption of a sealed snapshot system file.
 */
final class NamespaceMetadataSnapshotCodec {
    private NamespaceMetadataSnapshotCodec() {}

    static byte[] encode(NamespaceMetadataState.Snapshot snapshot) {
        BufWriter w = new BufWriter(256);
        w.u8(2).u64(snapshot.nextFileId()).u64(snapshot.nextLogStartOffset()).varint(snapshot.files().size());
        for (Records.FileRecord f : snapshot.files()) {
            byte[] rec = f.encode();
            w.varint(rec.length).raw(rec);
        }
        w.varint(snapshot.versions().size());
        for (Map.Entry<FileId, Integer> e : snapshot.versions().entrySet()) {
            w.fileId(e.getKey()).i32(e.getValue());
        }
        w.varint(snapshot.tombstones().size());
        for (Map.Entry<FileId, Long> e : snapshot.tombstones().entrySet()) {
            w.fileId(e.getKey()).u64(e.getValue());
        }
        byte[] body = w.toBytes();
        return new BufWriter(body.length + 4).raw(body).u32(Crc.of(body)).toBytes();
    }

    static NamespaceMetadataState.Snapshot decode(byte[] bytes) {
        if (bytes.length < 4) {
            throw new IllegalArgumentException("snapshot too short: " + bytes.length);
        }
        int bodyLen = bytes.length - 4;
        int expectedCrc = ByteBuffer.wrap(bytes, bodyLen, 4).getInt();
        if (Crc.of(bytes, 0, bodyLen) != expectedCrc) {
            throw new IllegalArgumentException("snapshot crc mismatch");
        }
        ByteBuffer b = ByteBuffer.wrap(bytes, 0, bodyLen);
        byte version = b.get();
        if (version != 2) {
            throw new IllegalArgumentException("snapshot version " + version);
        }
        long nextFid = b.getLong();
        long nextOffset = b.getLong();
        int fileCount = Varint.readCount(b, "file");
        List<Records.FileRecord> files = new ArrayList<>(fileCount);
        for (int i = 0; i < fileCount; i++) {
            int len = Varint.readCount(b, "file record");
            byte[] rec = new byte[len];
            b.get(rec);
            files.add(Records.FileRecord.decode(rec));
        }
        Map<FileId, Integer> versions = new HashMap<>();
        int versionCount = Varint.readCount(b, "file version");
        for (int i = 0; i < versionCount; i++) {
            versions.put(FileId.readFrom(b), b.getInt());
        }
        int tombCount = Varint.readCount(b, "tombstone");
        Map<FileId, Long> tombstones = new HashMap<>();
        for (int i = 0; i < tombCount; i++) {
            FileId id = FileId.readFrom(b);
            tombstones.put(id, b.getLong());
        }
        return new NamespaceMetadataState.Snapshot(nextFid, nextOffset, files, versions, tombstones);
    }
}
