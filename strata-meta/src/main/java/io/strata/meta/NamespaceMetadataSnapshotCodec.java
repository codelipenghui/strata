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
 * CRC-protected envelope for a compacted namespace snapshot (design §9, §10). The body holds the
 * next log start offset, the file table (each FileRecord length-prefixed), and the tombstone deletion
 * timestamps; a trailing CRC32C over the body detects corruption of a sealed snapshot system file.
 */
final class NamespaceMetadataSnapshotCodec {
    private NamespaceMetadataSnapshotCodec() {}

    static byte[] encode(NamespaceMetadataState.Snapshot snapshot) {
        BufWriter w = new BufWriter(256);
        w.u8(1).u64(snapshot.nextFileId()).u64(snapshot.nextLogStartOffset()).varint(snapshot.files().size());
        for (Records.FileRecord f : snapshot.files()) {
            byte[] rec = f.encode();
            w.varint(rec.length).raw(rec);
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
        if (version != 1) {
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
        int tombCount = Varint.readCount(b, "tombstone");
        Map<FileId, Long> tombstones = new HashMap<>();
        for (int i = 0; i < tombCount; i++) {
            FileId id = FileId.readFrom(b);
            tombstones.put(id, b.getLong());
        }
        return new NamespaceMetadataState.Snapshot(nextFid, nextOffset, files, tombstones);
    }
}
