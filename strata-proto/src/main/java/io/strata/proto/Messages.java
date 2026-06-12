package io.strata.proto;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import io.strata.common.Varint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * SCP message structs (tech design §10.3/§10.4 + v0 additions). Every header ends with a
 * tagged-field block. Request decoders take the header buffer; response decoders are called
 * AFTER Resp.check(buf) consumed the error header.
 */
public final class Messages {
    private Messages() {}

    /** Sanity bound for any list count on the wire — far above legitimate use. */
    static final int MAX_COUNT = 1_000_000;

    /**
     * Reads a list count and validates it: an adversarial varint (e.g. 0xFFFFFFFF) narrows to a
     * negative int and would otherwise surface as ArrayList's "Illegal Capacity" — an unchecked
     * exception that kills the connection thread instead of producing a typed protocol error.
     */
    static int count(ByteBuffer b) {
        long n = Varint.readUnsigned(b);
        if (n < 0 || n > MAX_COUNT) {
            throw new IllegalArgumentException("bad list count on wire: " + n);
        }
        return (int) n;
    }

    /* ---------- shared sub-structs ---------- */

    public record Replica(int nodeId, String endpoint) {
        static void write(BufWriter w, Replica r) {
            w.u32(r.nodeId).string(r.endpoint);
        }

        static Replica read(ByteBuffer b) {
            return new Replica(b.getInt(), Varint.readString(b));
        }
    }

    static void writeReplicas(BufWriter w, List<Replica> rs) {
        w.varint(rs.size());
        for (Replica r : rs) Replica.write(w, r);
    }

    static List<Replica> readReplicas(ByteBuffer b) {
        int n = count(b);
        List<Replica> rs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) rs.add(Replica.read(b));
        return rs;
    }

    /* ---------- HELLO ---------- */

    public record Hello(byte clientKind, long featureBits, String clientId) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.u16(Frame.FRAME_VERSION).u16(Frame.FRAME_VERSION);
            w.u8(clientKind).u64(featureBits).string(clientId);
            w.noTags();
            return w.toBytes();
        }

        public static Hello decode(ByteBuffer b) {
            int fvMin = b.getShort() & 0xFFFF;
            int fvMax = b.getShort() & 0xFFFF;
            if (fvMin > Frame.FRAME_VERSION || fvMax < Frame.FRAME_VERSION) {
                throw new IllegalArgumentException("no common frame version: [" + fvMin + "," + fvMax + "]");
            }
            byte kind = b.get();
            long features = b.getLong();
            String clientId = Varint.readString(b);
            TaggedFields.readFrom(b);
            return new Hello(kind, features, clientId);
        }
    }

    public record HelloResp(long featureBits, int nodeId, long incMsb, long incLsb,
                            int maxFrameBytes, long maxInflightBytes) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.u16(Frame.FRAME_VERSION);
            w.u64(featureBits).u32(nodeId).u64(incMsb).u64(incLsb).u32(maxFrameBytes).u64(maxInflightBytes);
            Opcode[] ops = Opcode.values();
            w.varint(ops.length);
            for (Opcode op : ops) w.u16(op.code).u16(1);
            w.noTags();
            return w.toBytes();
        }

        public static HelloResp decode(ByteBuffer b) {
            b.getShort(); // chosen frame version
            long features = b.getLong();
            int nodeId = b.getInt();
            long msb = b.getLong(), lsb = b.getLong();
            int maxFrame = b.getInt();
            long maxInflight = b.getLong();
            int n = count(b);
            for (int i = 0; i < n; i++) { b.getShort(); b.getShort(); }
            TaggedFields.readFrom(b);
            return new HelloResp(features, nodeId, msb, lsb, maxFrame, maxInflight);
        }
    }

    /* ---------- data plane ---------- */

    public record OpenChunk(ChunkId chunkId, int writeEpoch, byte ackPolicy, byte mediaClass,
                            byte fileKind, long expectedMaxBytes, long createdAtMs) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).i32(writeEpoch).u8(ackPolicy).u8(mediaClass).u8(fileKind)
                    .u64(expectedMaxBytes).u64(createdAtMs).noTags();
            return w.toBytes();
        }

        public static OpenChunk decode(ByteBuffer b) {
            OpenChunk m = new OpenChunk(ChunkId.readFrom(b), b.getInt(), b.get(), b.get(), b.get(),
                    b.getLong(), b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record Append(ChunkId chunkId, int writeEpoch, long baseOffset, long durableOffset) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).i32(writeEpoch).u64(baseOffset).u64(durableOffset).noTags();
            return w.toBytes();
        }

        public static Append decode(ByteBuffer b) {
            Append m = new Append(ChunkId.readFrom(b), b.getInt(), b.getLong(), b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record AppendResp(long endOffset) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.u64(endOffset).noTags();
            return w.toBytes();
        }

        public static AppendResp decode(ByteBuffer b) {
            AppendResp m = new AppendResp(b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record Read(ChunkId chunkId, long offset, int maxBytes) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).u64(offset).u32(maxBytes).noTags();
            return w.toBytes();
        }

        public static Read decode(ByteBuffer b) {
            Read m = new Read(ChunkId.readFrom(b), b.getLong(), b.getInt());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record ReadResp(long localEndOffset, long durableOffset) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.u64(localEndOffset).u64(durableOffset).noTags();
            return w.toBytes();
        }

        public static ReadResp decode(ByteBuffer b) {
            ReadResp m = new ReadResp(b.getLong(), b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record Fence(ChunkId chunkId, int fenceEpoch) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).i32(fenceEpoch).noTags();
            return w.toBytes();
        }

        public static Fence decode(ByteBuffer b) {
            Fence m = new Fence(ChunkId.readFrom(b), b.getInt());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record FenceResp(int persistedFenceEpoch, long localEndOffset, long lastKnownDO, ChunkState state) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.i32(persistedFenceEpoch).u64(localEndOffset).u64(lastKnownDO).u8(state.value).noTags();
            return w.toBytes();
        }

        public static FenceResp decode(ByteBuffer b) {
            FenceResp m = new FenceResp(b.getInt(), b.getLong(), b.getLong(), ChunkState.fromValue(b.get()));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record StatChunk(ChunkId chunkId) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).noTags();
            return w.toBytes();
        }

        public static StatChunk decode(ByteBuffer b) {
            StatChunk m = new StatChunk(ChunkId.readFrom(b));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record StatResp(ChunkState state, long localEndOffset, long lastKnownDO,
                           int writeEpoch, int fenceEpoch, long sealedLength, int sealedCrc) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.u8(state.value).u64(localEndOffset).u64(lastKnownDO).i32(writeEpoch).i32(fenceEpoch)
                    .u64(sealedLength).u32(sealedCrc).noTags();
            return w.toBytes();
        }

        public static StatResp decode(ByteBuffer b) {
            StatResp m = new StatResp(ChunkState.fromValue(b.get()), b.getLong(), b.getLong(),
                    b.getInt(), b.getInt(), b.getLong(), b.getInt());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record SealChunk(ChunkId chunkId, int writeEpoch, long dataLength) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).i32(writeEpoch).u64(dataLength).noTags();
            return w.toBytes();
        }

        public static SealChunk decode(ByteBuffer b) {
            SealChunk m = new SealChunk(ChunkId.readFrom(b), b.getInt(), b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record SealResp(long finalLength, int chunkCrc) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.u64(finalLength).u32(chunkCrc).noTags();
            return w.toBytes();
        }

        public static SealResp decode(ByteBuffer b) {
            SealResp m = new SealResp(b.getLong(), b.getInt());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record DeleteChunks(List<ChunkId> chunkIds) {
        public DeleteChunks {
            chunkIds = List.copyOf(chunkIds);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.varint(chunkIds.size());
            for (ChunkId c : chunkIds) w.chunkId(c);
            w.noTags();
            return w.toBytes();
        }

        public static DeleteChunks decode(ByteBuffer b) {
            int n = count(b);
            List<ChunkId> ids = new ArrayList<>(n);
            for (int i = 0; i < n; i++) ids.add(ChunkId.readFrom(b));
            TaggedFields.readFrom(b);
            return new DeleteChunks(ids);
        }
    }

    public record DeleteChunksResp(List<ChunkId> chunkIds, List<Short> codes) {
        public DeleteChunksResp {
            chunkIds = List.copyOf(chunkIds);
            codes = List.copyOf(codes);
            if (chunkIds.size() != codes.size()) {
                throw new IllegalArgumentException("chunkIds/codes size mismatch");
            }
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.varint(chunkIds.size());
            for (int i = 0; i < chunkIds.size(); i++) {
                w.chunkId(chunkIds.get(i)).u16(codes.get(i));
            }
            w.noTags();
            return w.toBytes();
        }

        public static DeleteChunksResp decode(ByteBuffer b) {
            int n = count(b);
            List<ChunkId> ids = new ArrayList<>(n);
            List<Short> codes = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                ids.add(ChunkId.readFrom(b));
                codes.add(b.getShort());
            }
            TaggedFields.readFrom(b);
            return new DeleteChunksResp(ids, codes);
        }
    }

    public record FetchChunk(ChunkId chunkId, long offset, int maxBytes) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).u64(offset).u32(maxBytes).noTags();
            return w.toBytes();
        }

        public static FetchChunk decode(ByteBuffer b) {
            FetchChunk m = new FetchChunk(ChunkId.readFrom(b), b.getLong(), b.getInt());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record FetchResp(long fileLength, ChunkState state) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.u64(fileLength).u8(state.value).noTags();
            return w.toBytes();
        }

        public static FetchResp decode(ByteBuffer b) {
            FetchResp m = new FetchResp(b.getLong(), ChunkState.fromValue(b.get()));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record ReadLedger(ChunkId chunkId, long fromOffset) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).u64(fromOffset).noTags();
            return w.toBytes();
        }

        public static ReadLedger decode(ByteBuffer b) {
            ReadLedger m = new ReadLedger(ChunkId.readFrom(b), b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    /** One integrity-ledger entry on the wire: bytes (prevEnd, endOffset] with the given payload CRC. */
    public record LedgerEntry(long endOffset, int payloadCrc, int writeEpoch) {}

    public record ReadLedgerResp(List<LedgerEntry> entries) {
        public ReadLedgerResp {
            entries = List.copyOf(entries);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.varint(entries.size());
            for (LedgerEntry e : entries) w.u64(e.endOffset()).u32(e.payloadCrc()).i32(e.writeEpoch());
            w.noTags();
            return w.toBytes();
        }

        public static ReadLedgerResp decode(ByteBuffer b) {
            int n = count(b);
            List<LedgerEntry> es = new ArrayList<>(n);
            for (int i = 0; i < n; i++) es.add(new LedgerEntry(b.getLong(), b.getInt(), b.getInt()));
            TaggedFields.readFrom(b);
            return new ReadLedgerResp(es);
        }
    }

    /* ---------- control plane: storage node <-> metadata ---------- */

    public record MediaCapacity(byte mediaClass, long capacityBytes) {}

    public record RegisterNode(long incMsb, long incLsb, List<String> endpoints,
                               String zone, String rack, String host,
                               List<MediaCapacity> capacities, int onDiskFormatMax, long featureBits) {
        public RegisterNode {
            endpoints = List.copyOf(endpoints);
            capacities = List.copyOf(capacities);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.u64(incMsb).u64(incLsb);
            w.varint(endpoints.size());
            for (String e : endpoints) w.string(e);
            w.string(zone).string(rack).string(host);
            w.varint(capacities.size());
            for (MediaCapacity c : capacities) w.u8(c.mediaClass()).u64(c.capacityBytes());
            w.u32(onDiskFormatMax).u64(featureBits).noTags();
            return w.toBytes();
        }

        public static RegisterNode decode(ByteBuffer b) {
            long msb = b.getLong(), lsb = b.getLong();
            int ne = count(b);
            List<String> eps = new ArrayList<>(ne);
            for (int i = 0; i < ne; i++) eps.add(Varint.readString(b));
            String zone = Varint.readString(b), rack = Varint.readString(b), host = Varint.readString(b);
            int nc = count(b);
            List<MediaCapacity> caps = new ArrayList<>(nc);
            for (int i = 0; i < nc; i++) caps.add(new MediaCapacity(b.get(), b.getLong()));
            int fmt = b.getInt();
            long features = b.getLong();
            TaggedFields.readFrom(b);
            return new RegisterNode(msb, lsb, eps, zone, rack, host, caps, fmt, features);
        }
    }

    public record RegisterResp(int nodeId, long sessionEpoch, int heartbeatIntervalMs, int leaseMs) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.u32(nodeId).u64(sessionEpoch).u32(heartbeatIntervalMs).u32(leaseMs).noTags();
            return w.toBytes();
        }

        public static RegisterResp decode(ByteBuffer b) {
            RegisterResp m = new RegisterResp(b.getInt(), b.getLong(), b.getInt(), b.getInt());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record MediaUsage(byte mediaClass, long usedBytes, long freeBytes) {}

    public record CompletedCommand(long commandId, short status) {}

    public record NodeHeartbeat(int nodeId, long incMsb, long incLsb, long sessionEpoch,
                                List<MediaUsage> usages, int repairQueueDepth,
                                List<CompletedCommand> completedCommands) {
        public static final int TAG_COMPLETED_COMMANDS = 0;

        public NodeHeartbeat {
            usages = List.copyOf(usages);
            completedCommands = List.copyOf(completedCommands);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.u32(nodeId).u64(incMsb).u64(incLsb).u64(sessionEpoch);
            w.varint(usages.size());
            for (MediaUsage u : usages) w.u8(u.mediaClass()).u64(u.usedBytes()).u64(u.freeBytes());
            w.u32(repairQueueDepth);
            if (completedCommands.isEmpty()) {
                w.noTags();
            } else {
                BufWriter cc = new BufWriter();
                cc.varint(completedCommands.size());
                for (CompletedCommand c : completedCommands) cc.u64(c.commandId()).u16(c.status());
                TaggedFields.of(Map.of(TAG_COMPLETED_COMMANDS, cc.toBytes())).writeTo(w);
            }
            return w.toBytes();
        }

        public static NodeHeartbeat decode(ByteBuffer b) {
            int nodeId = b.getInt();
            long msb = b.getLong(), lsb = b.getLong();
            long session = b.getLong();
            int nu = count(b);
            List<MediaUsage> us = new ArrayList<>(nu);
            for (int i = 0; i < nu; i++) us.add(new MediaUsage(b.get(), b.getLong(), b.getLong()));
            int depth = b.getInt();
            TaggedFields tags = TaggedFields.readFrom(b);
            List<CompletedCommand> done = new ArrayList<>();
            byte[] cc = tags.get(TAG_COMPLETED_COMMANDS);
            if (cc != null) {
                ByteBuffer cb = ByteBuffer.wrap(cc);
                int n = count(cb);
                for (int i = 0; i < n; i++) done.add(new CompletedCommand(cb.getLong(), cb.getShort()));
                if (cb.hasRemaining()) {
                    throw new IllegalArgumentException("trailing bytes in completed-command tag");
                }
            }
            return new NodeHeartbeat(nodeId, msb, lsb, session, us, depth, done);
        }
    }

    public sealed interface Command permits ReplicateCmd, DeleteCmd, DrainCmd {
        long commandId();

        static void write(BufWriter w, Command c) {
            w.u64(c.commandId());
            switch (c) {
                case ReplicateCmd r -> {
                    w.u8(1).chunkId(r.chunkId());
                    writeReplicas(w, r.sources());
                    w.u8(r.priority()).u32(r.expectedCrc()).u64(r.expectedLength());
                }
                case DeleteCmd d -> {
                    w.u8(2).varint(d.chunkIds().size());
                    for (ChunkId id : d.chunkIds()) w.chunkId(id);
                }
                case DrainCmd dr -> w.u8(3);
            }
        }

        static Command read(ByteBuffer b) {
            long id = b.getLong();
            byte type = b.get();
            return switch (type) {
                case 1 -> new ReplicateCmd(id, ChunkId.readFrom(b), readReplicas(b), b.get(), b.getInt(), b.getLong());
                case 2 -> {
                    int n = count(b);
                    List<ChunkId> ids = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) ids.add(ChunkId.readFrom(b));
                    yield new DeleteCmd(id, ids);
                }
                case 3 -> new DrainCmd(id);
                default -> throw new IllegalArgumentException("unknown command type " + type);
            };
        }
    }

    public record ReplicateCmd(long commandId, ChunkId chunkId, List<Replica> sources,
                               byte priority, int expectedCrc, long expectedLength) implements Command {
        public ReplicateCmd {
            sources = List.copyOf(sources);
        }
    }

    public record DeleteCmd(long commandId, List<ChunkId> chunkIds) implements Command {
        public DeleteCmd {
            chunkIds = List.copyOf(chunkIds);
        }
    }

    public record DrainCmd(long commandId) implements Command {}

    public record HeartbeatResp(long leaseValidUntilMs, List<Command> commands) {
        public HeartbeatResp {
            commands = List.copyOf(commands);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.u64(leaseValidUntilMs);
            w.varint(commands.size());
            for (Command c : commands) Command.write(w, c);
            w.noTags();
            return w.toBytes();
        }

        public static HeartbeatResp decode(ByteBuffer b) {
            long lease = b.getLong();
            int n = count(b);
            List<Command> cs = new ArrayList<>(n);
            for (int i = 0; i < n; i++) cs.add(Command.read(b));
            TaggedFields.readFrom(b);
            return new HeartbeatResp(lease, cs);
        }
    }

    public record InventoryEntry(ChunkId chunkId, ChunkState state, long length, int crc) {}

    public record InventoryReport(int nodeId, int shardIndex, int shardCount, List<InventoryEntry> entries) {
        public InventoryReport {
            entries = List.copyOf(entries);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.u32(nodeId).u32(shardIndex).u32(shardCount);
            w.varint(entries.size());
            for (InventoryEntry e : entries) {
                w.chunkId(e.chunkId()).u8(e.state().value).u64(e.length()).u32(e.crc());
            }
            w.noTags();
            return w.toBytes();
        }

        public static InventoryReport decode(ByteBuffer b) {
            int nodeId = b.getInt(), shard = b.getInt(), count = b.getInt();
            int n = count(b);
            List<InventoryEntry> es = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                es.add(new InventoryEntry(ChunkId.readFrom(b), ChunkState.fromValue(b.get()), b.getLong(), b.getInt()));
            }
            TaggedFields.readFrom(b);
            return new InventoryReport(nodeId, shard, count, es);
        }
    }

    /* ---------- v0 client <-> metadata ---------- */

    public record CreateFile(StrataNamespace namespace, StrataPath path, byte fileKind, byte mediaClass, byte ackPolicy,
                             FileId fileId, long opIdMsb, long opIdLsb) {
        public CreateFile {
            namespace = Objects.requireNonNull(namespace, "namespace");
            path = Objects.requireNonNull(path, "path");
            fileId = Objects.requireNonNull(fileId, "fileId");
        }

        public CreateFile(StrataNamespace namespace, StrataPath path, byte fileKind, byte mediaClass, byte ackPolicy) {
            this(namespace, path, fileKind, mediaClass, ackPolicy, FileId.random(), UUID.randomUUID());
        }

        public CreateFile(String namespace, String path, byte fileKind, byte mediaClass, byte ackPolicy) {
            this(StrataNamespace.of(namespace), StrataPath.of(path), fileKind, mediaClass, ackPolicy);
        }

        private CreateFile(StrataNamespace namespace, StrataPath path, byte fileKind, byte mediaClass, byte ackPolicy,
                           FileId fileId, UUID opId) {
            this(namespace, path, fileKind, mediaClass, ackPolicy, fileId,
                    opId.getMostSignificantBits(), opId.getLeastSignificantBits());
        }

        public CreateFile(String namespace, String path, byte fileKind, byte mediaClass, byte ackPolicy,
                          FileId fileId, long opIdMsb, long opIdLsb) {
            this(StrataNamespace.of(namespace), StrataPath.of(path), fileKind, mediaClass, ackPolicy,
                    fileId, opIdMsb, opIdLsb);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.string(namespace.toString()).string(path.toString()).u8(fileKind).u8(mediaClass).u8(ackPolicy)
                    .fileId(fileId).u64(opIdMsb).u64(opIdLsb).noTags();
            return w.toBytes();
        }

        public static CreateFile decode(ByteBuffer b) {
            CreateFile m = new CreateFile(StrataNamespace.of(Varint.readString(b)),
                    StrataPath.of(Varint.readString(b)), b.get(), b.get(), b.get(), FileId.readFrom(b),
                    b.getLong(), b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record CreateFileResp(FileId fileId) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.fileId(fileId).noTags();
            return w.toBytes();
        }

        public static CreateFileResp decode(ByteBuffer b) {
            CreateFileResp m = new CreateFileResp(FileId.readFrom(b));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record CreateChunk(FileId fileId, int writeEpoch, byte mediaClassHint,
                              long opIdMsb, long opIdLsb) {
        public CreateChunk(FileId fileId, int writeEpoch, byte mediaClassHint) {
            this(fileId, writeEpoch, mediaClassHint, UUID.randomUUID());
        }

        private CreateChunk(FileId fileId, int writeEpoch, byte mediaClassHint, UUID opId) {
            this(fileId, writeEpoch, mediaClassHint,
                    opId.getMostSignificantBits(), opId.getLeastSignificantBits());
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.fileId(fileId).i32(writeEpoch).u8(mediaClassHint).u64(opIdMsb).u64(opIdLsb).noTags();
            return w.toBytes();
        }

        public static CreateChunk decode(ByteBuffer b) {
            CreateChunk m = new CreateChunk(FileId.readFrom(b), b.getInt(), b.get(),
                    b.getLong(), b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record CreateChunkResp(ChunkId chunkId, int writeEpoch, List<Replica> replicas) {
        public CreateChunkResp {
            replicas = List.copyOf(replicas);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.chunkId(chunkId).i32(writeEpoch);
            writeReplicas(w, replicas);
            w.noTags();
            return w.toBytes();
        }

        public static CreateChunkResp decode(ByteBuffer b) {
            CreateChunkResp m = new CreateChunkResp(ChunkId.readFrom(b), b.getInt(), readReplicas(b));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    /**
     * sealedReplicas: node ids that confirmed the seal — the descriptor keeps only these (a
     * replica skipped during seal would otherwise stay listed and serve short/stale reads).
     * Empty = keep all (compat). v0 fixed-field addition (pre-release; single apiVersion).
     */
    public record SealChunkMeta(ChunkId chunkId, int writeEpoch, long length, int crc,
                                List<Integer> sealedReplicas) {
        public SealChunkMeta {
            sealedReplicas = List.copyOf(sealedReplicas);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).i32(writeEpoch).u64(length).u32(crc);
            w.varint(sealedReplicas.size());
            for (int id : sealedReplicas) w.u32(id);
            w.noTags();
            return w.toBytes();
        }

        public static SealChunkMeta decode(ByteBuffer b) {
            ChunkId id = ChunkId.readFrom(b);
            int epoch = b.getInt();
            long length = b.getLong();
            int crc = b.getInt();
            int n = count(b);
            List<Integer> sealed = new ArrayList<>(n);
            for (int i = 0; i < n; i++) sealed.add(b.getInt());
            TaggedFields.readFrom(b);
            return new SealChunkMeta(id, epoch, length, crc, sealed);
        }
    }

    public record AbortChunkMeta(ChunkId chunkId, int writeEpoch, long opIdMsb, long opIdLsb) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.chunkId(chunkId).i32(writeEpoch).u64(opIdMsb).u64(opIdLsb).noTags();
            return w.toBytes();
        }

        public static AbortChunkMeta decode(ByteBuffer b) {
            AbortChunkMeta m = new AbortChunkMeta(ChunkId.readFrom(b), b.getInt(), b.getLong(), b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record LookupFile(FileId fileId) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.fileId(fileId).noTags();
            return w.toBytes();
        }

        public static LookupFile decode(ByteBuffer b) {
            LookupFile m = new LookupFile(FileId.readFrom(b));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record LookupPath(StrataNamespace namespace, StrataPath path) {
        public LookupPath {
            namespace = Objects.requireNonNull(namespace, "namespace");
            path = Objects.requireNonNull(path, "path");
        }

        public LookupPath(String namespace, String path) {
            this(StrataNamespace.of(namespace), StrataPath.of(path));
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.string(namespace.toString()).string(path.toString()).noTags();
            return w.toBytes();
        }

        public static LookupPath decode(ByteBuffer b) {
            LookupPath m = new LookupPath(StrataNamespace.of(Varint.readString(b)),
                    StrataPath.of(Varint.readString(b)));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record LookupPathResp(FileId fileId) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.fileId(fileId).noTags();
            return w.toBytes();
        }

        public static LookupPathResp decode(ByteBuffer b) {
            LookupPathResp m = new LookupPathResp(FileId.readFrom(b));
            TaggedFields.readFrom(b);
            return m;
        }
    }

    public record ChunkInfo(ChunkId chunkId, ChunkState state, long length, int crc,
                            int writeEpoch, List<Replica> replicas) {
        public ChunkInfo {
            replicas = List.copyOf(replicas);
        }

        static void write(BufWriter w, ChunkInfo c) {
            w.chunkId(c.chunkId()).u8(c.state().value).u64(c.length()).u32(c.crc()).i32(c.writeEpoch());
            writeReplicas(w, c.replicas());
        }

        static ChunkInfo read(ByteBuffer b) {
            return new ChunkInfo(ChunkId.readFrom(b), ChunkState.fromValue(b.get()), b.getLong(),
                    b.getInt(), b.getInt(), readReplicas(b));
        }
    }

    public record LookupFileResp(StrataNamespace namespace, StrataPath path, byte fileKind, byte ackPolicy,
                                 byte fileState,
                                 List<ChunkInfo> chunks) {
        public LookupFileResp(String namespace, String path, byte fileKind, byte ackPolicy, byte fileState,
                              List<ChunkInfo> chunks) {
            this(StrataNamespace.of(namespace), StrataPath.of(path), fileKind, ackPolicy, fileState, chunks);
        }

        public LookupFileResp {
            namespace = Objects.requireNonNull(namespace, "namespace");
            path = Objects.requireNonNull(path, "path");
            chunks = List.copyOf(chunks);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.string(namespace.toString()).string(path.toString()).u8(fileKind).u8(ackPolicy)
                    .u8(fileState).varint(chunks.size());
            for (ChunkInfo c : chunks) ChunkInfo.write(w, c);
            w.noTags();
            return w.toBytes();
        }

        public static LookupFileResp decode(ByteBuffer b) {
            StrataNamespace namespace = StrataNamespace.of(Varint.readString(b));
            StrataPath path = StrataPath.of(Varint.readString(b));
            byte kind = b.get(), ack = b.get(), state = b.get();
            int n = count(b);
            List<ChunkInfo> cs = new ArrayList<>(n);
            for (int i = 0; i < n; i++) cs.add(ChunkInfo.read(b));
            TaggedFields.readFrom(b);
            return new LookupFileResp(namespace, path, kind, ack, state, cs);
        }
    }

    public record DeleteFiles(List<FileId> fileIds) {
        public DeleteFiles {
            fileIds = List.copyOf(fileIds);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.varint(fileIds.size());
            for (FileId f : fileIds) w.fileId(f);
            w.noTags();
            return w.toBytes();
        }

        public static DeleteFiles decode(ByteBuffer b) {
            int n = count(b);
            List<FileId> ids = new ArrayList<>(n);
            for (int i = 0; i < n; i++) ids.add(FileId.readFrom(b));
            TaggedFields.readFrom(b);
            return new DeleteFiles(ids);
        }
    }

    public record DeleteFilesResp(List<FileId> fileIds, List<Short> codes) {
        public DeleteFilesResp {
            fileIds = List.copyOf(fileIds);
            codes = List.copyOf(codes);
            if (fileIds.size() != codes.size()) {
                throw new IllegalArgumentException("fileIds/codes size mismatch");
            }
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            Resp.writeOk(w);
            w.varint(fileIds.size());
            for (int i = 0; i < fileIds.size(); i++) {
                w.fileId(fileIds.get(i)).u16(codes.get(i));
            }
            w.noTags();
            return w.toBytes();
        }

        public static DeleteFilesResp decode(ByteBuffer b) {
            int n = count(b);
            List<FileId> ids = new ArrayList<>(n);
            List<Short> codes = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                ids.add(FileId.readFrom(b));
                codes.add(b.getShort());
            }
            TaggedFields.readFrom(b);
            return new DeleteFilesResp(ids, codes);
        }
    }

    public record SealFile(FileId fileId, long totalLength) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.fileId(fileId).u64(totalLength).noTags();
            return w.toBytes();
        }

        public static SealFile decode(ByteBuffer b) {
            SealFile m = new SealFile(FileId.readFrom(b), b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }

    /** Shared empty success response (error header only). */
    public static byte[] okHeader() {
        BufWriter w = new BufWriter(8);
        Resp.writeOk(w);
        w.noTags();
        return w.toBytes();
    }

    /** Consumes the error header and the empty tagged block of an ok-only response. */
    public static void decodeOkHeader(ByteBuffer b) {
        TaggedFields.readFrom(b);
    }
}
