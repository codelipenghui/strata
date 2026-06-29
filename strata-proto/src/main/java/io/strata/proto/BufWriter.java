package io.strata.proto;

import io.strata.common.ChunkId;
import io.strata.common.FileId;
import io.strata.common.Varint;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Growable big-endian buffer for encoding SCP headers. Not thread-safe. */
public final class BufWriter {
    private byte[] bytes;
    private int pos;

    public BufWriter() {
        this(128);
    }

    public BufWriter(int initial) {
        bytes = new byte[initial];
    }

    private void ensure(int n) {
        if (pos + n > bytes.length) {
            bytes = Arrays.copyOf(bytes, Math.max(bytes.length * 2, pos + n));
        }
    }

    public BufWriter u8(int v) {
        ensure(1);
        bytes[pos++] = (byte) v;
        return this;
    }

    public BufWriter u16(int v) {
        ensure(2);
        bytes[pos++] = (byte) (v >>> 8);
        bytes[pos++] = (byte) v;
        return this;
    }

    public BufWriter u32(int v) {
        ensure(4);
        bytes[pos++] = (byte) (v >>> 24);
        bytes[pos++] = (byte) (v >>> 16);
        bytes[pos++] = (byte) (v >>> 8);
        bytes[pos++] = (byte) v;
        return this;
    }

    public BufWriter u64(long v) {
        ensure(8);
        for (int i = 7; i >= 0; i--) {
            bytes[pos++] = (byte) (v >>> (8 * i));
        }
        return this;
    }

    public BufWriter i32(int v) {
        return u32(v);
    }

    public BufWriter varint(long v) {
        ensure(10);
        ByteBuffer tmp = ByteBuffer.wrap(bytes, pos, bytes.length - pos);
        Varint.writeUnsigned(tmp, v);
        pos = tmp.position();
        return this;
    }

    public BufWriter string(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        varint(b.length);
        raw(b);
        return this;
    }

    public BufWriter raw(byte[] b) {
        ensure(b.length);
        System.arraycopy(b, 0, bytes, pos, b.length);
        pos += b.length;
        return this;
    }

    public BufWriter fileId(FileId f) {
        return u64(f.id());
    }

    public BufWriter chunkId(ChunkId c) {
        return fileId(c.fileId()).u32(c.index());
    }

    /** Empty tagged-fields block. varint(0) is exactly one 0x00 byte, so u8 avoids varint()'s
     *  per-call ByteBuffer.wrap allocation — and every encoded message ends with one of these. */
    public BufWriter noTags() {
        return u8(0);
    }

    public int size() {
        return pos;
    }

    public byte[] toBytes() {
        return Arrays.copyOf(bytes, pos);
    }
}
