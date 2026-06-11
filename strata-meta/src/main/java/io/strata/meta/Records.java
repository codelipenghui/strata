package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.Varint;
import io.strata.proto.BufWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Metadata records persisted by the MetadataStore backend (tech design §4.2 state, v0 shapes).
 * Binary encoding with a leading version byte; additive evolution only.
 */
public final class Records {
    private static final int MAX_COUNT = 1_000_000;

    private Records() {}

    private static int count(ByteBuffer b, String what) {
        long n = Varint.readUnsigned(b);
        if (n < 0 || n > MAX_COUNT) {
            throw new IllegalArgumentException("bad " + what + " count on wire: " + n);
        }
        return (int) n;
    }

    public enum FileState {
        OPEN(0), SEALED(1), DELETING(2);

        public final byte value;

        FileState(int v) {
            this.value = (byte) v;
        }

        static FileState from(byte v) {
            return switch (v) {
                case 0 -> OPEN;
                case 1 -> SEALED;
                case 2 -> DELETING;
                default -> throw new IllegalArgumentException("file state " + v);
            };
        }
    }

    public enum NodeState {
        REGISTERED(0), DRAINING(1), DEAD(2);

        public final byte value;

        NodeState(int v) {
            this.value = (byte) v;
        }

        static NodeState from(byte v) {
            return switch (v) {
                case 0 -> REGISTERED;
                case 1 -> DRAINING;
                case 2 -> DEAD;
                default -> throw new IllegalArgumentException("node state " + v);
            };
        }
    }

    /** One chunk's descriptor inside a FileRecord (tech design §3). Replicas are node ids. */
    public record ChunkRecord(int index, ChunkState state, long length, int crc, int writeEpoch,
                              List<Integer> replicas, long createOpMsb, long createOpLsb) {
        public ChunkRecord {
            replicas = List.copyOf(replicas);
        }

        public ChunkRecord(int index, ChunkState state, long length, int crc, int writeEpoch,
                           List<Integer> replicas) {
            this(index, state, length, crc, writeEpoch, replicas, 0, 0);
        }

        public ChunkRecord withReplicaSwapped(int from, int to) {
            List<Integer> r = new ArrayList<>(replicas);
            r.replaceAll(n -> n == from ? to : n);
            return withReplicas(r);
        }

        public ChunkRecord withReplicas(List<Integer> newReplicas) {
            return new ChunkRecord(index, state, length, crc, writeEpoch, newReplicas,
                    createOpMsb, createOpLsb);
        }

        public ChunkRecord sealed(long newLength, int newCrc, int epoch) {
            return new ChunkRecord(index, ChunkState.SEALED, newLength, newCrc, epoch, replicas,
                    createOpMsb, createOpLsb);
        }

        public boolean createdBy(long opMsb, long opLsb) {
            return createOpMsb == opMsb && createOpLsb == opLsb;
        }
    }

    public record FileRecord(FileId fileId, byte fileKind, byte mediaClass, byte ackPolicy,
                             String ownerTag, FileState state, long createdAtMs, List<ChunkRecord> chunks,
                             long createOpMsb, long createOpLsb) {
        public FileRecord {
            chunks = List.copyOf(chunks);
        }

        public FileRecord(FileId fileId, byte fileKind, byte mediaClass, byte ackPolicy,
                          String ownerTag, FileState state, long createdAtMs, List<ChunkRecord> chunks) {
            this(fileId, fileKind, mediaClass, ackPolicy, ownerTag, state, createdAtMs, chunks, 0, 0);
        }

        public ChunkId chunkId(int index) {
            return new ChunkId(fileId, index);
        }

        public FileRecord withState(FileState newState) {
            return new FileRecord(fileId, fileKind, mediaClass, ackPolicy, ownerTag, newState,
                    createdAtMs, chunks, createOpMsb, createOpLsb);
        }

        public FileRecord withChunks(List<ChunkRecord> newChunks) {
            return new FileRecord(fileId, fileKind, mediaClass, ackPolicy, ownerTag, state,
                    createdAtMs, newChunks, createOpMsb, createOpLsb);
        }

        public boolean createdBy(long opMsb, long opLsb) {
            return createOpMsb == opMsb && createOpLsb == opLsb;
        }

        public byte[] encode() {
            BufWriter w = new BufWriter(256);
            w.u8(2); // record version
            w.fileId(fileId);
            w.u8(fileKind).u8(mediaClass).u8(ackPolicy).string(ownerTag).u8(state.value).u64(createdAtMs);
            w.u64(createOpMsb).u64(createOpLsb);
            w.varint(chunks.size());
            for (ChunkRecord c : chunks) {
                w.u32(c.index()).u8(c.state().value).u64(c.length()).u32(c.crc()).i32(c.writeEpoch());
                w.u64(c.createOpMsb()).u64(c.createOpLsb());
                w.varint(c.replicas().size());
                for (int n : c.replicas()) w.u32(n);
            }
            return w.toBytes();
        }

        public static FileRecord decode(byte[] bytes) {
            ByteBuffer b = ByteBuffer.wrap(bytes);
            byte version = b.get();
            if (version != 1 && version != 2) throw new IllegalArgumentException("file record version " + version);
            FileId id = FileId.readFrom(b);
            byte kind = b.get(), media = b.get(), ack = b.get();
            String owner = Varint.readString(b);
            FileState state = FileState.from(b.get());
            long created = b.getLong();
            long fileOpMsb = version >= 2 ? b.getLong() : 0;
            long fileOpLsb = version >= 2 ? b.getLong() : 0;
            int n = count(b, "chunk");
            List<ChunkRecord> chunks = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                int index = b.getInt();
                ChunkState cs = ChunkState.fromValue(b.get());
                long len = b.getLong();
                int crc = b.getInt();
                int epoch = b.getInt();
                long chunkOpMsb = version >= 2 ? b.getLong() : 0;
                long chunkOpLsb = version >= 2 ? b.getLong() : 0;
                int nr = count(b, "replica");
                List<Integer> replicas = new ArrayList<>(nr);
                for (int j = 0; j < nr; j++) replicas.add(b.getInt());
                chunks.add(new ChunkRecord(index, cs, len, crc, epoch, replicas, chunkOpMsb, chunkOpLsb));
            }
            return new FileRecord(id, kind, media, ack, owner, state, created, chunks, fileOpMsb, fileOpLsb);
        }
    }

    public record NodeRecord(int nodeId, long incMsb, long incLsb, List<String> endpoints,
                             String zone, String rack, String host, byte mediaClass,
                             long capacityBytes, NodeState state) {
        public NodeRecord {
            endpoints = List.copyOf(endpoints);
        }

        public NodeRecord withState(NodeState s) {
            return new NodeRecord(nodeId, incMsb, incLsb, endpoints, zone, rack, host, mediaClass,
                    capacityBytes, s);
        }

        public NodeRecord withIncarnation(long msb, long lsb, List<String> eps, String z, String r, String h,
                                          byte media, long capacity) {
            return new NodeRecord(nodeId, msb, lsb, eps, z, r, h, media, capacity, NodeState.REGISTERED);
        }

        public String endpoint() {
            return endpoints.isEmpty() ? "" : endpoints.get(0);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter(128);
            w.u8(1);
            w.u32(nodeId).u64(incMsb).u64(incLsb);
            w.varint(endpoints.size());
            for (String e : endpoints) w.string(e);
            w.string(zone).string(rack).string(host);
            w.u8(mediaClass).u64(capacityBytes).u8(state.value);
            return w.toBytes();
        }

        public static NodeRecord decode(byte[] bytes) {
            ByteBuffer b = ByteBuffer.wrap(bytes);
            byte version = b.get();
            if (version != 1) throw new IllegalArgumentException("node record version " + version);
            int id = b.getInt();
            long msb = b.getLong(), lsb = b.getLong();
            int ne = count(b, "endpoint");
            List<String> eps = new ArrayList<>(ne);
            for (int i = 0; i < ne; i++) eps.add(Varint.readString(b));
            String zone = Varint.readString(b), rack = Varint.readString(b), host = Varint.readString(b);
            byte media = b.get();
            long cap = b.getLong();
            NodeState state = NodeState.from(b.get());
            return new NodeRecord(id, msb, lsb, eps, zone, rack, host, media, cap, state);
        }
    }
}
