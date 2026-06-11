package io.strata.common;

import java.nio.ByteBuffer;

/** Identifier of a chunk: (fileId, chunkIndex) — tech design §3. Wire size: 20 bytes. */
public record ChunkId(FileId fileId, int index) implements Comparable<ChunkId> {
    public static final int WIRE_SIZE = 20;

    public ChunkId {
        if (fileId == null) {
            throw new IllegalArgumentException("fileId must not be null");
        }
        if (index < 0) {
            throw new IllegalArgumentException("chunk index must be non-negative: " + index);
        }
    }

    public void writeTo(ByteBuffer buf) {
        fileId.writeTo(buf);
        buf.putInt(index);
    }

    public static ChunkId readFrom(ByteBuffer buf) {
        return new ChunkId(FileId.readFrom(buf), buf.getInt());
    }

    @Override
    public String toString() {
        return fileId + "." + index;
    }

    @Override
    public int compareTo(ChunkId o) {
        int c = fileId.compareTo(o.fileId);
        return c != 0 ? c : Integer.compare(index, o.index);
    }
}
