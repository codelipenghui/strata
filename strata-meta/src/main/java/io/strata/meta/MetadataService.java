package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Closeables;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.ScpException;
import io.strata.proto.Frame;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpServer;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * v0 metadata service (tech design §4.4): ZooKeeper-backed MetadataStore behind the same SCP
 * surface the v1 KRaft backend will serve. Single active leader (Curator LeaderLatch);
 * non-leaders answer NOT_LEADER. Placement, leases, repair, and retention orchestration live here.
 */
public final class MetadataService implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MetadataService.class);
    /** Optimistic-concurrency retry bound, shared with RepairCoordinator's descriptor CAS loops. */
    static final int CAS_RETRIES = 5;

    private final MetaConfig config;
    private final MetadataStore store;
    private final NodeRegistry registry;
    private final RepairCoordinator repair;
    private final ScpServer server;
    private final LeaderLatch leaderLatch;

    public MetadataService(MetaConfig config) throws Exception {
        this.config = config;
        ZkMetadataStore openedStore = null;
        LeaderLatch openedLatch = null;
        RepairCoordinator openedRepair = null;
        ScpServer openedServer = null;
        try {
            openedStore = new ZkMetadataStore(config.zkConnect(),
                    config.zkSessionTimeoutMs(), config.zkConnectionTimeoutMs());
            NodeRegistry openedRegistry = new NodeRegistry(openedStore, config);
            UUID serviceId = UUID.randomUUID();
            openedLatch = new LeaderLatch(openedStore.curator(), "/strata/leader", serviceId.toString());
            openedRepair = new RepairCoordinator(openedStore, openedRegistry, config, openedLatch::hasLeadership);

            this.store = openedStore;
            this.registry = openedRegistry;
            this.leaderLatch = openedLatch;
            this.repair = openedRepair;

            openedServer = new ScpServer(config.listenPort(), 0,
                    serviceId.getMostSignificantBits(), serviceId.getLeastSignificantBits(), this::handle);
            this.server = openedServer;
            openedLatch.start();
            openedRepair.start();
        } catch (Exception e) {
            Throwable closeFailure = closeAll(openedRepair, openedServer, openedLatch, openedStore);
            if (closeFailure != null) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
        log.info("metadata service started on port {} (zk {})", server.port(), config.zkConnect());
    }

    /** Closes whatever subset of the service's resources exists; returns the accumulated failure. */
    private static Throwable closeAll(RepairCoordinator repair, ScpServer server,
                                      LeaderLatch leaderLatch, MetadataStore store) {
        Throwable failure = null;
        if (repair != null) {
            try {
                repair.close();
            } catch (RuntimeException e) {
                failure = Closeables.suppress(failure, e);
            }
        }
        if (server != null) {
            try {
                server.close();
            } catch (RuntimeException e) {
                failure = Closeables.suppress(failure, e);
            }
        }
        if (leaderLatch != null) {
            try {
                leaderLatch.close();
            } catch (IOException | RuntimeException e) {
                failure = Closeables.suppress(failure, e);
            }
        }
        if (store != null) {
            try {
                store.close();
            } catch (RuntimeException e) {
                failure = Closeables.suppress(failure, e);
            }
        }
        return failure;
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

    // --- observability accessors (read-only; consumed by the metrics layer in strata-server) ---

    /** Whether ZooKeeper is reachable; a LOST connection freezes all metadata mutations. */
    public boolean zkConnected() {
        return !(store instanceof ZkMetadataStore zk) || zk.isConnected();
    }

    /** SEALED chunks below their replication factor (last reconciliation scan). */
    public int underReplicatedChunks() {
        return repair.underReplicatedChunks();
    }

    /** SEALED chunks with zero live replicas — data-loss exposure (last scan). */
    public int unavailableChunks() {
        return repair.unavailableChunks();
    }

    /** SEALED chunks down to a single live replica — one failure from loss (last scan). */
    public int chunksAtMinRedundancy() {
        return repair.chunksAtMinRedundancy();
    }

    public int repairInflight() {
        return repair.repairInflight();
    }

    public int repairBacklog() {
        return repair.repairBacklog();
    }

    public int aliveNodes() {
        return registry.livenessCounts().alive();
    }

    public int suspectNodes() {
        return registry.livenessCounts().suspect();
    }

    public int deadNodes() {
        return registry.livenessCounts().dead();
    }

    /** Installs a per-request latency observer on the control-plane server (used by the metrics layer). */
    public void setRequestObserver(io.strata.proto.RequestObserver observer) {
        server.setRequestObserver(observer);
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
                FileId id = createFile(m);
                yield ScpServer.ok(req, new Messages.CreateFileResp(id).encode(), null);
            }

            case CREATE_CHUNK -> {
                var m = Messages.CreateChunk.decode(h);
                yield ScpServer.ok(req, createChunk(m).encode(), null);
            }

            case ALLOCATE_WRITER_EPOCH -> {
                var m = Messages.AllocateWriterEpoch.decode(h);
                yield ScpServer.ok(req, new Messages.AllocateWriterEpochResp(allocateWriterEpoch(m)).encode(),
                        null);
            }

            case SEAL_CHUNK_META -> {
                var m = Messages.SealChunkMeta.decode(h);
                sealChunk(m);
                yield ScpServer.ok(req, Messages.okHeader(), null);
            }

            case ABORT_CHUNK_META -> {
                var m = Messages.AbortChunkMeta.decode(h);
                abortChunk(m);
                yield ScpServer.ok(req, Messages.okHeader(), null);
            }

            case SEAL_FILE -> {
                var m = Messages.SealFile.decode(h);
                // a file seals only when every chunk is sealed and the lengths add up — a stale
                // or buggy client must not freeze a file mid-write (validated under CAS, so it
                // is race-free against concurrent chunk creates)
                mutateFile(m.fileId(), file -> {
                    if (file.state() == FileState.DELETING) {
                        // sealing must never resurrect a DELETING file: deletion would stop
                        // half-way, leaving a "live" file with missing chunks
                        throw new ScpException(ErrorCode.PRECONDITION_FAILED, "file is DELETING");
                    }
                    long total = 0;
                    for (Records.ChunkRecord c : file.chunks()) {
                        if (c.state() != ChunkState.SEALED) {
                            throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                                    "chunk " + c.index() + " is " + c.state());
                        }
                        if (c.length() < 0) {
                            throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                                    "negative sealed length on chunk " + c.index());
                        }
                        try {
                            total = Math.addExact(total, c.length());
                        } catch (ArithmeticException e) {
                            throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                                    "sealed chunk lengths overflow");
                        }
                    }
                    if (total != m.totalLength()) {
                        throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                                "total length " + total + " != requested " + m.totalLength(), total);
                    }
                    return file.withState(FileState.SEALED);
                });
                yield ScpServer.ok(req, Messages.okHeader(), null);
            }

            case LOOKUP_FILE -> {
                var m = Messages.LookupFile.decode(h);
                yield ScpServer.ok(req, lookup(m.fileId()).encode(), null);
            }

            case LOOKUP_PATH -> {
                var m = Messages.LookupPath.decode(h);
                yield ScpServer.ok(req, new Messages.LookupPathResp(lookupPath(m.namespace(), m.path())).encode(),
                        null);
            }

            case DELETE_FILES -> {
                var m = Messages.DeleteFiles.decode(h);
                List<Short> codes = new ArrayList<>();
                for (FileId id : m.fileIds()) {
                    try {
                        markDeleting(id);
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

    private int allocateWriterEpoch(Messages.AllocateWriterEpoch m) throws Exception {
        for (int attempt = 0; attempt < CAS_RETRIES; attempt++) {
            var opt = store.getFile(m.fileId());
            if (opt.isEmpty()) throw new ScpException(ErrorCode.FILE_NOT_FOUND, m.fileId().toString());
            Records.FileRecord file = opt.get().value();
            if (file.state() == FileState.DELETING) {
                throw new ScpException(ErrorCode.PRECONDITION_FAILED, "file is DELETING");
            }
            if (file.state() == FileState.SEALED) {
                throw new ScpException(ErrorCode.FILE_SEALED, "file state " + file.state());
            }
            boolean hasOpenChunk = file.chunks().stream().anyMatch(c -> c.state() != ChunkState.SEALED);
            if (m.purpose() == Messages.AllocateWriterEpoch.FOR_APPEND && hasOpenChunk) {
                throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                        "file has an open chunk — run recoverAndSeal or resume the owning appender");
            }
            int nextEpoch;
            try {
                nextEpoch = Math.addExact(file.writerEpoch(), 1);
            } catch (ArithmeticException e) {
                throw new ScpException(ErrorCode.PRECONDITION_FAILED, "writer epoch overflow");
            }
            if (store.updateFile(file.withWriterEpoch(nextEpoch), opt.get().version())) {
                return nextEpoch;
            }
        }
        throw new ScpException(ErrorCode.INTERNAL, "allocateWriterEpoch CAS exhausted");
    }

    private FileId createFile(Messages.CreateFile m) throws Exception {
        Messages.WritePolicy policy = m.writePolicy();
        try {
            store.createFile(new Records.FileRecord(m.fileId(), m.namespace(), m.path(),
                    policy.replicationFactor(), policy.ackQuorum(), policy.fsyncOnAck(),
                    FileState.OPEN, System.currentTimeMillis(), List.of(),
                    m.opIdMsb(), m.opIdLsb()));
            return m.fileId();
        } catch (KeeperException.NodeExistsException e) {
            var existing = store.getFile(m.fileId());
            if (existing.isPresent()) {
                Records.FileRecord record = existing.get().value();
                if (sameCreateRequest(record, m) && record.state() != FileState.DELETING
                        && pathStillOwnsCreate(record, m)) {
                    return m.fileId();
                }
                throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                        "file id already exists for a different create request: " + m.fileId());
            }
            var pathOwner = store.resolvePath(m.namespace(), m.path());
            if (pathOwner.isPresent()) {
                throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                        "path already exists: " + m.namespace() + ":" + m.path());
            }
            throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                    "file id already exists for a different create request: " + m.fileId());
        }
    }

    private boolean pathStillOwnsCreate(Records.FileRecord existing, Messages.CreateFile requested)
            throws Exception {
        return store.resolvePath(existing.namespace(), existing.path())
                .filter(requested.fileId()::equals)
                .isPresent();
    }

    private static boolean sameCreateRequest(Records.FileRecord existing, Messages.CreateFile requested) {
        Messages.WritePolicy requestedPolicy = requested.writePolicy();
        return existing.createdBy(requested.opIdMsb(), requested.opIdLsb())
                && existing.namespace().equals(requested.namespace())
                && existing.path().equals(requested.path())
                && existing.replicationFactor() == requestedPolicy.replicationFactor()
                && existing.ackQuorum() == requestedPolicy.ackQuorum()
                && existing.fsyncOnAck() == requestedPolicy.fsyncOnAck();
    }

    private Messages.CreateChunkResp createChunk(Messages.CreateChunk m) throws Exception {
        for (int attempt = 0; attempt < CAS_RETRIES; attempt++) {
            var opt = store.getFile(m.fileId());
            if (opt.isEmpty()) throw new ScpException(ErrorCode.FILE_NOT_FOUND, m.fileId().toString());
            Records.FileRecord file = opt.get().value();
            if (file.state() == FileState.DELETING) {
                throw new ScpException(ErrorCode.PRECONDITION_FAILED, "file is DELETING");
            }
            if (file.state() != FileState.OPEN) {
                throw new ScpException(ErrorCode.FILE_SEALED, "file state " + file.state());
            }
            if (m.writeEpoch() <= 0) {
                throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                        "writeEpoch must be positive: " + m.writeEpoch());
            }
            // stale-leader guard: a newer writer/recovery may already own this file
            if (m.writeEpoch() < file.writerEpoch()) {
                throw new ScpException(ErrorCode.FENCED_EPOCH, "file epoch " + file.writerEpoch(),
                        file.writerEpoch());
            }
            // legacy/internal callers may still provide the first epoch directly; record that
            // allocation atomically with the chunk descriptor.
            int updatedWriterEpoch = Math.max(file.writerEpoch(), m.writeEpoch());
            int maxEpoch = file.chunks().stream().mapToInt(Records.ChunkRecord::writeEpoch).max().orElse(-1);
            if (m.writeEpoch() < maxEpoch) {
                throw new ScpException(ErrorCode.FENCED_EPOCH, "file epoch " + maxEpoch, maxEpoch);
            }
            // no legitimate flow creates a chunk while the tail is OPEN (appenders seal before
            // rolling, recovery seals before new appenders open) — racing same-epoch writers
            // would otherwise both claim the same file offsets in different chunks
            if (!file.chunks().isEmpty()) {
                Records.ChunkRecord tail = file.chunks().get(file.chunks().size() - 1);
                if (tail.state() == ChunkState.OPEN) {
                    if (tail.createdBy(m.opIdMsb(), m.opIdLsb())) {
                        return new Messages.CreateChunkResp(new ChunkId(m.fileId(), tail.index()),
                                tail.writeEpoch(), replicasFor(tail.replicas()));
                    }
                    throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                            "tail chunk " + tail.index() + " is OPEN — seal or recover it first");
                }
            }
            List<NodeRegistry.LiveNode> nodes = Placement.choose(registry, file.replicationFactor(),
                    Set.copyOf(m.excludedNodeIds()), Set.of());
            List<Integer> replicaIds = new ArrayList<>(file.replicationFactor());
            List<Messages.Replica> replicas = new ArrayList<>(file.replicationFactor());
            for (NodeRegistry.LiveNode n : nodes) {
                replicaIds.add(n.record.nodeId());
                replicas.add(new Messages.Replica(n.record.nodeId(), n.record.endpoint()));
            }
            int index = file.chunks().isEmpty() ? 0
                    : file.chunks().get(file.chunks().size() - 1).index() + 1;
            List<Records.ChunkRecord> chunks = new ArrayList<>(file.chunks());
            chunks.add(new Records.ChunkRecord(index, ChunkState.OPEN, 0, 0, m.writeEpoch(), replicaIds,
                    m.opIdMsb(), m.opIdLsb()));
            Records.FileRecord updated = file.withWriterEpoch(updatedWriterEpoch).withChunks(chunks);
            // commit-before-write: the descriptor exists before the client opens chunks anywhere
            if (store.updateFile(updated, opt.get().version())) {
                return new Messages.CreateChunkResp(new ChunkId(m.fileId(), index), m.writeEpoch(), replicas);
            }
        }
        throw new ScpException(ErrorCode.INTERNAL, "createChunk CAS exhausted");
    }

    private List<Messages.Replica> replicasFor(List<Integer> replicaIds) {
        List<Messages.Replica> replicas = new ArrayList<>(replicaIds.size());
        for (int nodeId : replicaIds) {
            replicas.add(registry.replicaOf(nodeId));
        }
        return replicas;
    }

    private void sealChunk(Messages.SealChunkMeta m) throws Exception {
        if (m.length() < 0) {
            throw new ScpException(ErrorCode.PRECONDITION_FAILED, "negative sealed length " + m.length());
        }
        for (int attempt = 0; attempt < CAS_RETRIES; attempt++) {
            var opt = store.getFile(m.chunkId().fileId());
            if (opt.isEmpty()) throw new ScpException(ErrorCode.FILE_NOT_FOUND, m.chunkId().toString());
            Records.FileRecord file = opt.get().value();
            if (file.state() == FileState.DELETING) {
                // an in-flight appender must not mutate a file that deletion is dismantling
                throw new ScpException(ErrorCode.PRECONDITION_FAILED, "file is DELETING");
            }
            List<Records.ChunkRecord> chunks = new ArrayList<>(file.chunks());
            boolean found = false;
            for (int i = 0; i < chunks.size(); i++) {
                Records.ChunkRecord c = chunks.get(i);
                if (c.index() == m.chunkId().index()) {
                    found = true;
                    if (m.writeEpoch() < c.writeEpoch()) {
                        throw new ScpException(ErrorCode.FENCED_EPOCH, "chunk epoch " + c.writeEpoch(),
                                c.writeEpoch());
                    }
                    if (c.state() == ChunkState.SEALED) {
                        if (c.length() == m.length() && c.crc() == m.crc()) return; // idempotent retry
                        // same length but different crc is byte divergence — never swallow it
                        throw new ScpException(ErrorCode.CHUNK_SEALED,
                                "sealed at " + c.length() + " crc " + c.crc(), c.length());
                    }
                    if (m.writeEpoch() < file.writerEpoch()) {
                        throw new ScpException(ErrorCode.FENCED_EPOCH, "file epoch " + file.writerEpoch(),
                                file.writerEpoch());
                    }
                    Records.ChunkRecord sealed = c.sealed(m.length(), m.crc(), m.writeEpoch());
                    if (!m.sealedReplicas().isEmpty()) {
                        // keep only replicas that actually sealed: a skipped/failed replica left
                        // in a SEALED descriptor stays alive, never repairs, and serves short
                        // reads; the under-replication scan add-repairs RF afterwards
                        List<Integer> confirmed = new ArrayList<>(c.replicas());
                        confirmed.retainAll(m.sealedReplicas());
                        if (confirmed.isEmpty()) {
                            throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                                    "sealed-replica set disjoint from descriptor replicas");
                        }
                        if (confirmed.size() < file.ackQuorum()) {
                            throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                                    "sealed-replica set is below quorum");
                        }
                        sealed = sealed.withReplicas(confirmed);
                    }
                    chunks.set(i, sealed);
                }
            }
            if (!found) throw new ScpException(ErrorCode.CHUNK_NOT_FOUND, m.chunkId().toString());
            Records.FileRecord updated = file.withChunks(chunks);
            if (store.updateFile(updated, opt.get().version())) return;
        }
        throw new ScpException(ErrorCode.INTERNAL, "sealChunk CAS exhausted");
    }

    private void abortChunk(Messages.AbortChunkMeta m) throws Exception {
        for (int attempt = 0; attempt < CAS_RETRIES; attempt++) {
            var opt = store.getFile(m.chunkId().fileId());
            if (opt.isEmpty()) return; // idempotent after deletion
            Records.FileRecord file = opt.get().value();
            if (file.state() == FileState.DELETING) {
                throw new ScpException(ErrorCode.PRECONDITION_FAILED, "file is DELETING");
            }
            if (file.chunks().isEmpty()) return; // already aborted
            List<Records.ChunkRecord> chunks = new ArrayList<>(file.chunks());
            Records.ChunkRecord tail = chunks.get(chunks.size() - 1);
            if (tail.index() != m.chunkId().index()) {
                boolean stillExists = chunks.stream().anyMatch(c -> c.index() == m.chunkId().index());
                if (!stillExists) return; // already aborted
                throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                        "can only abort the current tail chunk " + m.chunkId());
            }
            if (tail.state() != ChunkState.OPEN) {
                throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                        "cannot abort " + tail.state() + " chunk " + m.chunkId());
            }
            if (m.writeEpoch() < file.writerEpoch()) {
                throw new ScpException(ErrorCode.FENCED_EPOCH, "file epoch " + file.writerEpoch(),
                        file.writerEpoch());
            }
            if (tail.writeEpoch() != m.writeEpoch() || !tail.createdBy(m.opIdMsb(), m.opIdLsb())) {
                throw new ScpException(ErrorCode.FENCED_EPOCH,
                        "chunk owned by another create request or epoch", tail.writeEpoch());
            }
            chunks.remove(chunks.size() - 1);
            if (store.updateFile(file.withChunks(chunks), opt.get().version())) return;
        }
        throw new ScpException(ErrorCode.INTERNAL, "abortChunk CAS exhausted");
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
        return new Messages.LookupFileResp(file.namespace(), file.path(),
                new Messages.WritePolicy(file.replicationFactor(), file.ackQuorum(), file.fsyncOnAck()),
                file.state().value, chunks);
    }

    private FileId lookupPath(io.strata.common.StrataNamespace namespace,
                              io.strata.common.StrataPath path) throws Exception {
        var id = store.resolvePath(namespace, path);
        if (id.isEmpty()) {
            throw new ScpException(ErrorCode.FILE_NOT_FOUND, namespace + ":" + path);
        }
        var file = store.getFile(id.get());
        if (file.isEmpty() || file.get().value().state() == FileState.DELETING) {
            throw new ScpException(ErrorCode.FILE_NOT_FOUND, namespace + ":" + path);
        }
        Records.FileRecord record = file.get().value();
        if (!record.namespace().equals(namespace) || !record.path().equals(path)) {
            throw new ScpException(ErrorCode.FILE_NOT_FOUND, namespace + ":" + path);
        }
        return id.get();
    }

    private void mutateFile(FileId id, java.util.function.UnaryOperator<Records.FileRecord> fn) throws Exception {
        for (int attempt = 0; attempt < CAS_RETRIES; attempt++) {
            var opt = store.getFile(id);
            if (opt.isEmpty()) throw new ScpException(ErrorCode.FILE_NOT_FOUND, id.toString());
            if (store.updateFile(fn.apply(opt.get().value()), opt.get().version())) return;
        }
        throw new ScpException(ErrorCode.INTERNAL, "file mutation CAS exhausted");
    }

    private void markDeleting(FileId id) throws Exception {
        for (int attempt = 0; attempt < CAS_RETRIES; attempt++) {
            var opt = store.getFile(id);
            if (opt.isEmpty()) throw new ScpException(ErrorCode.FILE_NOT_FOUND, id.toString());
            Records.FileRecord current = opt.get().value();
            Records.FileRecord deleting = current.withState(FileState.DELETING);
            if (store.updateFile(deleting, opt.get().version())) {
                if (!store.deletePath(current.namespace(), current.path(), id)) {
                    throw new ScpException(ErrorCode.INTERNAL,
                            "path " + current.namespace() + ":" + current.path() + " is bound to a different file");
                }
                return;
            }
        }
        throw new ScpException(ErrorCode.INTERNAL, "delete CAS exhausted");
    }

    @Override
    public void close() throws IOException {
        Closeables.throwIfFailed(closeAll(repair, server, leaderLatch, store));
    }
}
