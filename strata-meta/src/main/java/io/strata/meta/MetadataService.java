package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.proto.Frame;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpServer;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * v0 metadata service (tech design §4.4): ZooKeeper-backed MetadataStore behind the same SCP
 * surface the v1 KRaft backend will serve. Single active leader (Curator LeaderLatch);
 * non-leaders answer NOT_LEADER. Placement, leases, repair, and retention orchestration live here.
 */
public final class MetadataService implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MetadataService.class);
    private static final int CAS_RETRIES = 5;

    private final MetaConfig config;
    private final ZkMetadataStore store;
    private final NodeRegistry registry;
    private final RepairCoordinator repair;
    private final ScpServer server;
    private final LeaderLatch leaderLatch;

    public MetadataService(MetaConfig config) throws Exception {
        this.config = config;
        this.store = new ZkMetadataStore(config.zkConnect());
        this.registry = new NodeRegistry(store, config);
        UUID serviceId = UUID.randomUUID();
        this.leaderLatch = new LeaderLatch(store.curator(), "/strata/leader", serviceId.toString());
        this.repair = new RepairCoordinator(store, registry, config, leaderLatch::hasLeadership);
        this.leaderLatch.start();
        this.server = new ScpServer(config.listenPort(), 0,
                serviceId.getMostSignificantBits(), serviceId.getLeastSignificantBits(), this::handle);
        this.repair.start();
        log.info("metadata service started on port {} (zk {})", server.port(), config.zkConnect());
    }

    public int port() {
        return server.port();
    }

    public String endpoint() {
        return "127.0.0.1:" + port();
    }

    public boolean isLeader() {
        return leaderLatch.hasLeadership();
    }

    /** Test hook: force one reconciliation pass now. */
    public void reconcileNow() throws Exception {
        registry.expireScan();
        repair.scanOnce();
    }

    private void requireLeader() {
        if (!leaderLatch.hasLeadership()) {
            throw new ScpException(ErrorCode.NOT_LEADER, "not the metadata leader");
        }
    }

    private Frame handle(Frame req) throws Exception {
        Opcode op = Opcode.fromCode(req.opcode());
        if (op == null) throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "0x" + Integer.toHexString(req.opcode()));
        ByteBuffer h = req.headerSlice();
        requireLeader();
        return switch (op) {
            case PING -> ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());

            /* ---- storage-node control (tech design §10.4) ---- */

            case REGISTER_NODE -> {
                var m = Messages.RegisterNode.decode(h);
                yield ScpServer.ok(req, registry.register(m).encode(), null);
            }

            case NODE_HEARTBEAT -> {
                var m = Messages.NodeHeartbeat.decode(h);
                yield ScpServer.ok(req, registry.heartbeat(m, repair::onCommandCompleted).encode(), null);
            }

            case INVENTORY_REPORT -> {
                var m = Messages.InventoryReport.decode(h);
                repair.onInventory(m);
                yield ScpServer.ok(req, Messages.okHeader(), null);
            }

            /* ---- v0 client APIs (v1: Kafka RPC on the controller) ---- */

            case CREATE_FILE -> {
                var m = Messages.CreateFile.decode(h);
                FileId id = FileId.random();
                store.createFile(new Records.FileRecord(id, m.fileKind(), m.mediaClass(), m.ackPolicy(),
                        m.ownerTag(), Records.FileState.OPEN, System.currentTimeMillis(), List.of()));
                yield ScpServer.ok(req, new Messages.CreateFileResp(id).encode(), null);
            }

            case CREATE_CHUNK -> {
                var m = Messages.CreateChunk.decode(h);
                yield ScpServer.ok(req, createChunk(m).encode(), null);
            }

            case SEAL_CHUNK_META -> {
                var m = Messages.SealChunkMeta.decode(h);
                sealChunk(m);
                yield ScpServer.ok(req, Messages.okHeader(), null);
            }

            case SEAL_FILE -> {
                var m = Messages.SealFile.decode(h);
                // a file seals only when every chunk is sealed and the lengths add up — a stale
                // or buggy client must not freeze a file mid-write (validated under CAS, so it
                // is race-free against concurrent chunk creates)
                mutateFile(m.fileId(), file -> {
                    long total = 0;
                    for (Records.ChunkRecord c : file.chunks()) {
                        if (c.state() != ChunkState.SEALED) {
                            throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                                    "chunk " + c.index() + " is " + c.state());
                        }
                        total += c.length();
                    }
                    if (total != m.totalLength()) {
                        throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                                "total length " + total + " != requested " + m.totalLength(), total);
                    }
                    return new Records.FileRecord(file.fileId(), file.fileKind(),
                            file.mediaClass(), file.ackPolicy(), file.ownerTag(), Records.FileState.SEALED,
                            file.createdAtMs(), file.chunks());
                });
                yield ScpServer.ok(req, Messages.okHeader(), null);
            }

            case LOOKUP_FILE -> {
                var m = Messages.LookupFile.decode(h);
                yield ScpServer.ok(req, lookup(m.fileId()).encode(), null);
            }

            case DELETE_FILES -> {
                var m = Messages.DeleteFiles.decode(h);
                List<Short> codes = new ArrayList<>();
                for (FileId id : m.fileIds()) {
                    try {
                        mutateFile(id, file -> new Records.FileRecord(file.fileId(), file.fileKind(),
                                file.mediaClass(), file.ackPolicy(), file.ownerTag(),
                                Records.FileState.DELETING, file.createdAtMs(), file.chunks()));
                        codes.add(ErrorCode.OK.code);
                    } catch (ScpException e) {
                        codes.add(e.code().code);
                    }
                }
                yield ScpServer.ok(req, new Messages.DeleteFilesResp(m.fileIds(), codes).encode(), null);
            }

            default -> throw new ScpException(ErrorCode.UNKNOWN_OPCODE, op + " not served by metadata plane");
        };
    }

    private Messages.CreateChunkResp createChunk(Messages.CreateChunk m) throws Exception {
        for (int attempt = 0; attempt < CAS_RETRIES; attempt++) {
            var opt = store.getFile(m.fileId());
            if (opt.isEmpty()) throw new ScpException(ErrorCode.FILE_NOT_FOUND, m.fileId().toString());
            Records.FileRecord file = opt.get().value();
            if (file.state() != Records.FileState.OPEN) {
                throw new ScpException(ErrorCode.FILE_SEALED, "file state " + file.state());
            }
            // stale-leader guard: a new epoch may already own this file
            int maxEpoch = file.chunks().stream().mapToInt(Records.ChunkRecord::writeEpoch).max().orElse(-1);
            if (m.writeEpoch() < maxEpoch) {
                throw new ScpException(ErrorCode.FENCED_EPOCH, "file epoch " + maxEpoch, maxEpoch);
            }
            byte mediaClass = m.mediaClassHint() != (byte) 0xFF ? m.mediaClassHint() : file.mediaClass();
            List<NodeRegistry.LiveNode> nodes = Placement.choose(registry, 3, mediaClass,
                    Set.of(), Set.of());
            List<Integer> replicaIds = new ArrayList<>(3);
            List<Messages.Replica> replicas = new ArrayList<>(3);
            for (NodeRegistry.LiveNode n : nodes) {
                replicaIds.add(n.record.nodeId());
                replicas.add(new Messages.Replica(n.record.nodeId(), n.record.endpoint()));
            }
            int index = file.chunks().isEmpty() ? 0
                    : file.chunks().get(file.chunks().size() - 1).index() + 1;
            List<Records.ChunkRecord> chunks = new ArrayList<>(file.chunks());
            chunks.add(new Records.ChunkRecord(index, ChunkState.OPEN, 0, 0, m.writeEpoch(), replicaIds));
            Records.FileRecord updated = new Records.FileRecord(file.fileId(), file.fileKind(),
                    file.mediaClass(), file.ackPolicy(), file.ownerTag(), file.state(),
                    file.createdAtMs(), chunks);
            // commit-before-write: the descriptor exists before the client opens chunks anywhere
            if (store.updateFile(updated, opt.get().version())) {
                return new Messages.CreateChunkResp(new ChunkId(m.fileId(), index), m.writeEpoch(), replicas);
            }
        }
        throw new ScpException(ErrorCode.INTERNAL, "createChunk CAS exhausted");
    }

    private void sealChunk(Messages.SealChunkMeta m) throws Exception {
        for (int attempt = 0; attempt < CAS_RETRIES; attempt++) {
            var opt = store.getFile(m.chunkId().fileId());
            if (opt.isEmpty()) throw new ScpException(ErrorCode.FILE_NOT_FOUND, m.chunkId().toString());
            Records.FileRecord file = opt.get().value();
            List<Records.ChunkRecord> chunks = new ArrayList<>(file.chunks());
            boolean found = false;
            for (int i = 0; i < chunks.size(); i++) {
                Records.ChunkRecord c = chunks.get(i);
                if (c.index() == m.chunkId().index()) {
                    found = true;
                    if (c.state() == ChunkState.SEALED) {
                        if (c.length() == m.length() && c.crc() == m.crc()) return; // idempotent retry
                        // same length but different crc is byte divergence — never swallow it
                        throw new ScpException(ErrorCode.CHUNK_SEALED,
                                "sealed at " + c.length() + " crc " + c.crc(), c.length());
                    }
                    if (m.writeEpoch() < c.writeEpoch()) {
                        throw new ScpException(ErrorCode.FENCED_EPOCH, "chunk epoch " + c.writeEpoch(),
                                c.writeEpoch());
                    }
                    chunks.set(i, c.sealed(m.length(), m.crc(), m.writeEpoch()));
                }
            }
            if (!found) throw new ScpException(ErrorCode.CHUNK_NOT_FOUND, m.chunkId().toString());
            Records.FileRecord updated = new Records.FileRecord(file.fileId(), file.fileKind(),
                    file.mediaClass(), file.ackPolicy(), file.ownerTag(), file.state(),
                    file.createdAtMs(), chunks);
            if (store.updateFile(updated, opt.get().version())) return;
        }
        throw new ScpException(ErrorCode.INTERNAL, "sealChunk CAS exhausted");
    }

    private Messages.LookupFileResp lookup(FileId fileId) throws Exception {
        var opt = store.getFile(fileId);
        if (opt.isEmpty()) throw new ScpException(ErrorCode.FILE_NOT_FOUND, fileId.toString());
        Records.FileRecord file = opt.get().value();
        List<Messages.ChunkInfo> chunks = new ArrayList<>(file.chunks().size());
        for (Records.ChunkRecord c : file.chunks()) {
            List<Messages.Replica> replicas = new ArrayList<>(c.replicas().size());
            for (int nodeId : c.replicas()) {
                replicas.add(registry.replicaOf(nodeId));
            }
            chunks.add(new Messages.ChunkInfo(file.chunkId(c.index()), c.state(), c.length(), c.crc(),
                    c.writeEpoch(), replicas));
        }
        return new Messages.LookupFileResp(file.fileKind(), file.ackPolicy(), file.state().value, chunks);
    }

    private void mutateFile(FileId id, java.util.function.UnaryOperator<Records.FileRecord> fn) throws Exception {
        for (int attempt = 0; attempt < CAS_RETRIES; attempt++) {
            var opt = store.getFile(id);
            if (opt.isEmpty()) throw new ScpException(ErrorCode.FILE_NOT_FOUND, id.toString());
            if (store.updateFile(fn.apply(opt.get().value()), opt.get().version())) return;
        }
        throw new ScpException(ErrorCode.INTERNAL, "file mutation CAS exhausted");
    }

    @Override
    public void close() throws IOException {
        repair.close();
        server.close();
        try {
            leaderLatch.close();
        } catch (IOException ignored) {
        }
        store.close();
    }
}
