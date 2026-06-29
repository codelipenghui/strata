package io.strata.meta;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * One active metadata-log segment (design §8): an append-only sequence of CRC-framed records starting
 * at {@code baseOffset}. {@link #append} frames a record and advances the byte end offset; {@link
 * #recover} rebuilds a segment from durable bytes keeping only the valid prefix (a torn tail append is
 * discarded). Byte offsets are the log's positions — {@code baseOffset} aligns with the manifest's
 * {@code logStartOffset} and {@link #endOffset()} is the durable end a publish records.
 */
final class MetadataLogSegment {
    private final long baseOffset;
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final List<MetadataLogRecord> records = new ArrayList<>();

    MetadataLogSegment(long baseOffset) {
        if (baseOffset < 0) {
            throw new IllegalArgumentException("baseOffset must be >= 0: " + baseOffset);
        }
        this.baseOffset = baseOffset;
    }

    long baseOffset() {
        return baseOffset;
    }

    long endOffset() {
        return baseOffset + bytes.size();
    }

    int byteLength() {
        return bytes.size();
    }

    int recordCount() {
        return records.size();
    }

    List<MetadataLogRecord> records() {
        return Collections.unmodifiableList(records);
    }

    byte[] toBytes() {
        return bytes.toByteArray();
    }

    /** Frames {@code record}, appends it to the segment, and returns the new end offset. */
    long append(MetadataLogRecord record) {
        byte[] frame = MetadataLogSegmentCodec.frame(MetadataLogCodec.encode(record));
        bytes.writeBytes(frame);
        records.add(record);
        return endOffset();
    }

    /** Rebuilds a segment from durable bytes, keeping only the valid (CRC-verified, untorn) prefix. */
    static MetadataLogSegment recover(long baseOffset, byte[] durableBytes) {
        MetadataLogSegmentCodec.Prefix prefix = MetadataLogSegmentCodec.recoverPrefix(durableBytes);
        MetadataLogSegment segment = new MetadataLogSegment(baseOffset);
        segment.bytes.write(durableBytes, 0, prefix.validBytes());
        segment.records.addAll(prefix.records());
        return segment;
    }
}
