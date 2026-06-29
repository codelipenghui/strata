package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import io.strata.common.Varint;
import io.strata.proto.BufWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Metadata records persisted by the MetadataStore backend (tech design §4.2 state, v0 shapes).
 * Binary encoding with a leading version byte.
 */
public final class Records {
    private Records() {}

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

    public record FileRecord(FileId fileId, StrataNamespace namespace, StrataPath path,
                             int replicationFactor, int ackQuorum, boolean fsyncOnAck,
                             int writerEpoch, FileState state, long createdAtMs,
                             List<ChunkRecord> chunks, long createOpMsb, long createOpLsb) {
        public FileRecord {
            fileId = Objects.requireNonNull(fileId, "fileId");
            namespace = Objects.requireNonNull(namespace, "namespace");
            path = Objects.requireNonNull(path, "path");
            if (replicationFactor <= 0) {
                throw new IllegalArgumentException("replicationFactor must be positive: " + replicationFactor);
            }
            if (ackQuorum <= 0 || ackQuorum > replicationFactor) {
                throw new IllegalArgumentException("ackQuorum must be in 1..replicationFactor: " + ackQuorum);
            }
            if (ackQuorum <= replicationFactor / 2) {
                throw new IllegalArgumentException("ackQuorum must intersect any other quorum: "
                        + ackQuorum + " for replicationFactor " + replicationFactor);
            }
            if (writerEpoch < 0) {
                throw new IllegalArgumentException("writerEpoch must be non-negative: " + writerEpoch);
            }
            state = Objects.requireNonNull(state, "state");
            chunks = List.copyOf(chunks);
            int chunkMaxEpoch = maxChunkEpoch(chunks);
            if (writerEpoch < chunkMaxEpoch) {
                throw new IllegalArgumentException("writerEpoch " + writerEpoch
                        + " is below max chunk epoch " + chunkMaxEpoch);
            }
        }

        public FileRecord(FileId fileId, StrataNamespace namespace, StrataPath path,
                          int replicationFactor, int ackQuorum, boolean fsyncOnAck,
                          FileState state, long createdAtMs, List<ChunkRecord> chunks) {
            this(fileId, namespace, path, replicationFactor, ackQuorum, fsyncOnAck,
                    maxChunkEpoch(chunks), state, createdAtMs, chunks, 0, 0);
        }

        public FileRecord(FileId fileId, StrataNamespace namespace, StrataPath path,
                          int replicationFactor, int ackQuorum, boolean fsyncOnAck,
                          FileState state, long createdAtMs, List<ChunkRecord> chunks,
                          long createOpMsb, long createOpLsb) {
            this(fileId, namespace, path, replicationFactor, ackQuorum, fsyncOnAck,
                    maxChunkEpoch(chunks), state, createdAtMs, chunks, createOpMsb, createOpLsb);
        }

        public FileRecord(FileId fileId, String namespace, String path,
                          int replicationFactor, int ackQuorum, boolean fsyncOnAck,
                          FileState state, long createdAtMs, List<ChunkRecord> chunks) {
            this(fileId, StrataNamespace.of(namespace), StrataPath.of(path),
                    replicationFactor, ackQuorum, fsyncOnAck,
                    maxChunkEpoch(chunks), state, createdAtMs, chunks, 0, 0);
        }

        public FileRecord(FileId fileId, String namespace, String path,
                          int replicationFactor, int ackQuorum, boolean fsyncOnAck,
                          FileState state, long createdAtMs, List<ChunkRecord> chunks,
                          long createOpMsb, long createOpLsb) {
            this(fileId, StrataNamespace.of(namespace), StrataPath.of(path),
                    replicationFactor, ackQuorum, fsyncOnAck, maxChunkEpoch(chunks), state, createdAtMs, chunks,
                    createOpMsb, createOpLsb);
        }

        private static int maxChunkEpoch(List<ChunkRecord> chunks) {
            int max = 0;
            for (ChunkRecord chunk : chunks) {
                max = Math.max(max, chunk.writeEpoch());
            }
            return max;
        }

        public ChunkId chunkId(int index) {
            return new ChunkId(fileId, index);
        }

        public FileRecord withState(FileState newState) {
            return new FileRecord(fileId, namespace, path, replicationFactor, ackQuorum, fsyncOnAck, writerEpoch,
                    newState,
                    createdAtMs, chunks, createOpMsb, createOpLsb);
        }

        public FileRecord withChunks(List<ChunkRecord> newChunks) {
            return new FileRecord(fileId, namespace, path, replicationFactor, ackQuorum, fsyncOnAck,
                    Math.max(writerEpoch, maxChunkEpoch(newChunks)), state,
                    createdAtMs, newChunks, createOpMsb, createOpLsb);
        }

        public FileRecord withWriterEpoch(int newWriterEpoch) {
            return new FileRecord(fileId, namespace, path, replicationFactor, ackQuorum, fsyncOnAck,
                    newWriterEpoch, state, createdAtMs, chunks, createOpMsb, createOpLsb);
        }

        public boolean createdBy(long opMsb, long opLsb) {
            return createOpMsb == opMsb && createOpLsb == opLsb;
        }

        public byte[] encode() {
            BufWriter w = new BufWriter(256);
            w.u8(7); // record version
            w.fileId(fileId);
            w.string(namespace.toString()).string(path.toString())
                    .u32(replicationFactor).u32(ackQuorum).u8(fsyncOnAck ? 1 : 0)
                    .i32(writerEpoch).u8(state.value).u64(createdAtMs);
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
            if (version != 7) throw new IllegalArgumentException("file record version " + version);
            FileId id = FileId.readFrom(b);
            StrataNamespace namespace = StrataNamespace.of(Varint.readString(b));
            StrataPath path = StrataPath.of(Varint.readString(b));
            int replicationFactor = b.getInt();
            int ackQuorum = b.getInt();
            boolean fsyncOnAck = Varint.readBoolean(b);
            int writerEpoch = b.getInt();
            FileState state = FileState.fromValue(b.get());
            long created = b.getLong();
            long fileOpMsb = b.getLong();
            long fileOpLsb = b.getLong();
            int n = Varint.readCount(b, "chunk");
            List<ChunkRecord> chunks = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                int index = b.getInt();
                ChunkState cs = ChunkState.fromValue(b.get());
                long len = b.getLong();
                int crc = b.getInt();
                int epoch = b.getInt();
                long chunkOpMsb = b.getLong();
                long chunkOpLsb = b.getLong();
                int nr = Varint.readCount(b, "replica");
                List<Integer> replicas = new ArrayList<>(nr);
                for (int j = 0; j < nr; j++) replicas.add(b.getInt());
                chunks.add(new ChunkRecord(index, cs, len, crc, epoch, replicas, chunkOpMsb, chunkOpLsb));
            }
            return new FileRecord(id, namespace, path, replicationFactor, ackQuorum, fsyncOnAck,
                    writerEpoch, state, created, chunks, fileOpMsb, fileOpLsb);
        }
    }

    public record NodeRecord(int nodeId, long incMsb, long incLsb, List<String> endpoints,
                             String zone, String rack, String host, long capacityBytes, NodeState state) {
        public NodeRecord {
            endpoints = List.copyOf(endpoints);
        }

        public NodeRecord withState(NodeState s) {
            return new NodeRecord(nodeId, incMsb, incLsb, endpoints, zone, rack, host, capacityBytes, s);
        }

        public String endpoint() {
            return endpoints.isEmpty() ? "" : endpoints.get(0);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter(128);
            w.u8(2);
            w.u32(nodeId).u64(incMsb).u64(incLsb);
            w.varint(endpoints.size());
            for (String e : endpoints) w.string(e);
            w.string(zone).string(rack).string(host);
            w.u64(capacityBytes).u8(state.value);
            return w.toBytes();
        }

        public static NodeRecord decode(byte[] bytes) {
            ByteBuffer b = ByteBuffer.wrap(bytes);
            byte version = b.get();
            if (version != 2) throw new IllegalArgumentException("node record version " + version);
            int id = b.getInt();
            long msb = b.getLong(), lsb = b.getLong();
            int ne = Varint.readCount(b, "endpoint");
            List<String> eps = new ArrayList<>(ne);
            for (int i = 0; i < ne; i++) eps.add(Varint.readString(b));
            String zone = Varint.readString(b), rack = Varint.readString(b), host = Varint.readString(b);
            long cap = b.getLong();
            NodeState state = NodeState.from(b.get());
            return new NodeRecord(id, msb, lsb, eps, zone, rack, host, cap, state);
        }
    }

    /**
     * Persisted rendezvous assignment of a namespace to an ordered replica set of controller endpoints
     * (design §6.1). {@code preferredLeader} is {@code replicaSet[0]}; {@code generation} pins the
     * membership the assignment was computed against, so it stays stable while nodes are added.
     */
    public record NamespaceAssignment(StrataNamespace namespace, int generation, List<String> replicaSet) {
        public NamespaceAssignment {
            namespace = Objects.requireNonNull(namespace, "namespace");
            replicaSet = List.copyOf(replicaSet);
        }

        public String preferredLeader() {
            if (replicaSet.isEmpty()) {
                throw new IllegalStateException("namespace assignment has no replicas: " + namespace);
            }
            return replicaSet.get(0);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter(128);
            w.u8(1); // record version
            w.string(namespace.toString()).i32(generation).varint(replicaSet.size());
            for (String e : replicaSet) w.string(e);
            return w.toBytes();
        }

        public static NamespaceAssignment decode(byte[] bytes) {
            ByteBuffer b = ByteBuffer.wrap(bytes);
            byte version = b.get();
            if (version != 1) throw new IllegalArgumentException("namespace assignment version " + version);
            StrataNamespace namespace = StrataNamespace.of(Varint.readString(b));
            int generation = b.getInt();
            int n = Varint.readCount(b, "replica");
            List<String> replicaSet = new ArrayList<>(n);
            for (int i = 0; i < n; i++) replicaSet.add(Varint.readString(b));
            return new NamespaceAssignment(namespace, generation, replicaSet);
        }
    }

    /**
     * A compact snapshot of currently-live data nodes published by the cluster controller to the
     * consensus root (design §11). A non-controller namespace owner — which has no heartbeat channel —
     * merges this into its placement view so it can place and repair replicas. {@code publishedAtMs}
     * is the controller's wall clock at publish; a stale snapshot (older than the liveness window) is
     * ignored. Each entry carries the node record plus its last-reported free bytes for placement.
     */
    public record ClusterLiveNodes(long publishedAtMs, List<LiveEntry> entries) {
        public ClusterLiveNodes {
            entries = List.copyOf(entries);
        }

        public record LiveEntry(NodeRecord record, long freeBytes) {}

        public byte[] encode() {
            BufWriter w = new BufWriter(256);
            w.u8(1); // record version
            w.u64(publishedAtMs).varint(entries.size());
            for (LiveEntry e : entries) {
                byte[] rec = e.record().encode();
                w.varint(rec.length);
                w.raw(rec);
                w.u64(e.freeBytes());
            }
            return w.toBytes();
        }

        public static ClusterLiveNodes decode(byte[] bytes) {
            ByteBuffer b = ByteBuffer.wrap(bytes);
            byte version = b.get();
            if (version != 1) throw new IllegalArgumentException("cluster live-nodes version " + version);
            long publishedAtMs = b.getLong();
            int n = Varint.readCount(b, "live node");
            List<LiveEntry> entries = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                int len = Varint.readCount(b, "live node record");
                byte[] rec = new byte[len];
                b.get(rec);
                long freeBytes = b.getLong();
                entries.add(new LiveEntry(NodeRecord.decode(rec), freeBytes));
            }
            return new ClusterLiveNodes(publishedAtMs, entries);
        }
    }

    /**
     * The consensus-root manifest pointer for one namespace's metadata log (design §5, §9). It is the
     * linearizable publication barrier: a namespace leader CAS-publishes a new manifest (version-CAS on
     * the znode) to atomically cut over the snapshot generation and durable log range. {@code
     * metadataEpoch} fences stale leaders; {@code logStartOffset} is where replay begins (the snapshot
     * cut) and {@code publishedLogOffset} is the published durable end. The snapshot/log references are
     * the physical Strata system-file ids (absent before the first snapshot/log is created).
     */
    public record NamespaceManifest(StrataNamespace namespace, long metadataEpoch, long generation,
                                    long logStartOffset, long publishedLogOffset,
                                    Optional<FileId> snapshotFileId, Optional<FileId> logFileId) {
        public NamespaceManifest {
            namespace = Objects.requireNonNull(namespace, "namespace");
            snapshotFileId = Objects.requireNonNull(snapshotFileId, "snapshotFileId");
            logFileId = Objects.requireNonNull(logFileId, "logFileId");
        }

        public byte[] encode() {
            BufWriter w = new BufWriter(96);
            w.u8(1).string(namespace.toString()).u64(metadataEpoch).u64(generation)
                    .u64(logStartOffset).u64(publishedLogOffset);
            encodeOptionalFileId(w, snapshotFileId);
            encodeOptionalFileId(w, logFileId);
            return w.toBytes();
        }

        public static NamespaceManifest decode(byte[] bytes) {
            ByteBuffer b = ByteBuffer.wrap(bytes);
            byte version = b.get();
            if (version != 1) throw new IllegalArgumentException("namespace manifest version " + version);
            StrataNamespace namespace = StrataNamespace.of(Varint.readString(b));
            long metadataEpoch = b.getLong();
            long generation = b.getLong();
            long logStartOffset = b.getLong();
            long publishedLogOffset = b.getLong();
            Optional<FileId> snapshotFileId = decodeOptionalFileId(b);
            Optional<FileId> logFileId = decodeOptionalFileId(b);
            return new NamespaceManifest(namespace, metadataEpoch, generation, logStartOffset,
                    publishedLogOffset, snapshotFileId, logFileId);
        }

        private static void encodeOptionalFileId(BufWriter w, Optional<FileId> id) {
            if (id.isPresent()) {
                w.u8(1).fileId(id.get());
            } else {
                w.u8(0);
            }
        }

        private static Optional<FileId> decodeOptionalFileId(ByteBuffer b) {
            return b.get() == 1 ? Optional.of(FileId.readFrom(b)) : Optional.empty();
        }
    }
}
