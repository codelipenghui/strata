package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Closeables;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * Controller (tech design §4): the metadata plane — a ZooKeeper-backed MetadataStore behind the SCP
 * control surface, plus the per-namespace metadata logs. Single active cluster leader (Curator
 * LeaderLatch); a controller that does not own a namespace answers NOT_LEADER with the owner's hint.
 * Placement, leases, repair, and retention orchestration live here.
 */
public final class Controller implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Controller.class);
    /** Optimistic-concurrency retry bound, shared with RepairCoordinator's descriptor CAS loops. */
    static final int CAS_RETRIES = 5;

    private final ControllerConfig config;
    private final MetadataStore store;
    private final ZkMetadataStore rootZk; // the consensus root (latch + ZK metrics), under any backend
    private final NodeRegistry registry;
    private final RepairCoordinator repair;
    private final ScpServer server;
    private final LeaderLatch leaderLatch;
    private final String advertisedEndpoint;  // this node's reachable host:port — the leader hint clients redirect to
    private final NamespaceOwnership ownership; // resolves the controller owner of each namespace (design §6)

    public Controller(ControllerConfig config) throws Exception {
        this(config, null);
    }

    /**
     * Embedded mode when {@code advertisedEndpoint} is non-null: runs the latch/repair/store but binds
     * NO SCP server — the caller serves {@link #handler()} on its own listener (a combined node's data
     * port), and {@code advertisedEndpoint} is that listener's reachable host:port (the NOT_LEADER
     * redirect hint). Null = standalone: bind an own server on {@code config.listenPort()}.
     */
    public Controller(ControllerConfig config, String advertisedEndpoint) throws Exception {
        this(config, advertisedEndpoint, defaultBackendFactory());
    }

    /**
     * Advanced/test hook: the metadata-store backend is supplied as a function of the ZooKeeper root
     * store. The consensus root (leader latch, node registry, sharding root) is always ZooKeeper; the
     * factory chooses whether file/path metadata is served from ZooKeeper directly (default) or from the
     * namespace-log backend wrapping that same root (design §16 Step 3).
     */
    Controller(ControllerConfig config, String advertisedEndpoint,
                    BiFunction<ZkMetadataStore, String, MetadataStore> backendFactory) throws Exception {
        this.config = config;
        boolean embedded = advertisedEndpoint != null;
        ZkMetadataStore openedStore = null;
        LeaderLatch openedLatch = null;
        RepairCoordinator openedRepair = null;
        ScpServer openedServer = null;
        try {
            openedStore = new ZkMetadataStore(config.zkConnect(),
                    config.zkSessionTimeoutMs(), config.zkConnectionTimeoutMs(),
                    config.zkRetryBaseMs(), config.zkRetryMaxRetries());
            UUID serviceId = UUID.randomUUID();

            if (embedded) {
                // No own listener — the caller (a combined node) serves handler() on its port; that
                // is the endpoint the latch advertises as the NOT_LEADER redirect hint.
                this.advertisedEndpoint = advertisedEndpoint;
            } else {
                // Build the SCP server first so the latch advertises this node's real bound endpoint
                // (the listen port may be ephemeral) — what a standby returns as the redirect hint.
                openedServer = new ScpServer(config.listenPort(), 0,
                        serviceId.getMostSignificantBits(), serviceId.getLeastSignificantBits(), this::handle);
                this.advertisedEndpoint = config.advertisedHost() + ":" + openedServer.port();
            }
            // Build the backend once the endpoint is known: the namespace-log backend's system-file store
            // connects an embedded client back to this node's own endpoint to store metadata-log bytes as
            // replicated Strata chunks (design §5, §8).
            MetadataStore backendStore = backendFactory.apply(openedStore, this.advertisedEndpoint);
            NodeRegistry openedRegistry = new NodeRegistry(backendStore, config);
            openedLatch = new LeaderLatch(openedStore.curator(), "/strata/leader", this.advertisedEndpoint);
            // Static rendezvous ownership over the configured controller endpoints (design §6.1). With an
            // empty/single-endpoint membership this node owns every namespace (no behavior change).
            NamespaceOwnership openedOwnership = new NamespaceOwnership(this.advertisedEndpoint,
                    config.controllerEndpoints(), 0, config.controllerReplicaCount());
            // Repair's orphan-deletion is gated on owning every namespace (a sharded controller never
            // deletes an inventory chunk owned by another controller node), and a non-controller owner heals
            // only the namespaces it owns via the direct EXEC_REPLICATE pass.
            openedRepair = new RepairCoordinator(backendStore, openedRegistry, config,
                    openedLatch::hasLeadership, openedOwnership::ownsAll, openedOwnership::isOwner);

            this.store = backendStore;
            this.rootZk = openedStore;
            this.registry = openedRegistry;
            this.server = openedServer;
            this.leaderLatch = openedLatch;
            this.repair = openedRepair;
            this.ownership = openedOwnership;
            // Eager namespace recovery on the namespace-log backend is scoped to the namespaces this
            // node owns, so it never republishes (and fences) another owner's namespace.
            if (backendStore instanceof NamespaceLogMetadataStore namespaceLog) {
                namespaceLog.setOwnership(openedOwnership::isOwner);
            }

            // The owner-pull verifier identifies itself by its advertised endpoint (design §20.4) so a
            // node can record which owner attested each chunk; it is also this node's rendezvous identity.
            openedRepair.advertisedEndpoint(this.advertisedEndpoint);
            openedLatch.start();
            openedRepair.start();
        } catch (Exception e) {
            Throwable closeFailure = closeAll(openedRepair, openedServer, openedLatch, openedStore);
            if (closeFailure != null) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
        log.info("controller started ({}) on {} (zk {})",
                embedded ? "embedded" : "standalone", this.advertisedEndpoint, config.zkConnect());
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

    /**
     * The backend factory selected by environment: {@code STRATA_CONTROLLER_BACKEND=namespace-log} stores
     * each namespace's file/path metadata in a per-namespace metadata log whose bytes are replicated
     * Strata chunks (a {@link StrataSystemMetadataFileStore}, RF/ack from {@code STRATA_CONTROLLER_LOG_RF}
     * / {@code STRATA_CONTROLLER_LOG_ACK}; durability is by replication, so fsync-per-append is off by
     * default and gated by {@code STRATA_CONTROLLER_LOG_FSYNC}); anything else uses the ZooKeeper store
     * directly (the default).
     */
    private static BiFunction<ZkMetadataStore, String, MetadataStore> defaultBackendFactory() {
        String backend = setting("STRATA_CONTROLLER_BACKEND", "strata.controller.backend", "zk");
        if (!"namespace-log".equalsIgnoreCase(backend)) {
            return (root, endpoint) -> root;
        }
        int replicationFactor = intSetting("STRATA_CONTROLLER_LOG_RF", "strata.controller.log.rf", 3);
        int ackQuorum = intSetting("STRATA_CONTROLLER_LOG_ACK", "strata.controller.log.ack", 2);
        // Durability by replication (RF/ack); per-append fsync off by default — see StrataSystemMetadataFileStore.
        boolean logFsync = boolSetting("STRATA_CONTROLLER_LOG_FSYNC", "strata.controller.log.fsync", false);
        // Steady-state open-log compaction: snapshot+roll an owned namespace once its open log passes this
        // size, so a stable leader's log is bounded by snapshot cadence (design §8/§10) rather than only
        // compacting at open/failover. <=0 on either knob disables the background sweep.
        int compactBytes = intSetting("STRATA_CONTROLLER_LOG_COMPACT_BYTES",
                "strata.controller.log.compact.bytes", 4 * 1024 * 1024);
        int compactIntervalMs = intSetting("STRATA_CONTROLLER_LOG_COMPACT_INTERVAL_MS",
                "strata.controller.log.compact.interval.ms", 30_000);
        // Reap snapshot/log system files orphaned by a crash between file-create and manifest CAS. Safe by
        // construction: a file is reaped only if no manifest references it AND its generation is <= its
        // namespace's currently-published generation (an in-flight publish is always at a higher generation).
        boolean orphanGc = boolSetting("STRATA_CONTROLLER_LOG_ORPHAN_GC",
                "strata.controller.log.orphan.gc", true);
        // Safety delay (design §10 step 6 / issue #8): retain a superseded metadata-log generation this many
        // ms after it is superseded before the sweep reclaims it — a rollback margin against a bad newest
        // generation. 0 (default) disables the window: superseded generations are reclaimed on the next sweep.
        int retentionMs = intSetting("STRATA_CONTROLLER_LOG_RETENTION_MS",
                "strata.controller.log.retention.ms", 0);
        return (root, endpoint) -> {
            NamespaceLogBackend logBackend = new NamespaceLogBackend(root,
                    new StrataSystemMetadataFileStore(() -> endpoint, replicationFactor, ackQuorum, logFsync), true);
            logBackend.setLogRetentionMs(retentionMs);
            logBackend.startBackgroundCompaction(compactBytes, compactIntervalMs, orphanGc);
            return new NamespaceLogMetadataStore(logBackend);
        };
    }

    /** Reads an environment variable, falling back to a JVM system property (settable in tests). */
    private static String setting(String envName, String propName, String fallback) {
        String value = System.getenv(envName);
        if (value == null || value.isBlank()) {
            value = System.getProperty(propName);
        }
        return value == null || value.isBlank() ? fallback : value;
    }

    private static int intSetting(String envName, String propName, int fallback) {
        String value = setting(envName, propName, null);
        if (value == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            log.warn("ignoring non-numeric {}/{}='{}', using default {}", envName, propName, value, fallback);
            return fallback;
        }
    }

    private static boolean boolSetting(String envName, String propName, boolean fallback) {
        String value = setting(envName, propName, null);
        return value == null ? fallback : Boolean.parseBoolean(value.trim());
    }

    public int port() {
        return server != null ? server.port() : -1;  // -1 in embedded mode (served on the host's port)
    }

    public String endpoint() {
        return advertisedEndpoint;
    }

    /** SCP handler for the metadata (control-plane) opcodes. In embedded mode the caller serves this
     *  on its own listener; standalone, it is already wired to this service's own server. */
    public ScpServer.Handler handler() {
        return this::handle;
    }

    public boolean isLeader() {
        return leaderLatch.hasLeadership();
    }

    // --- observability accessors (read-only; consumed by the metrics layer in strata-server) ---

    /** Whether ZooKeeper is reachable; a LOST connection freezes all metadata mutations. */
    public boolean zkConnected() {
        return rootZk.isConnected();
    }

    /** The metadata-store backend identifier (the {@code backend} metric label). */
    public String metadataBackend() {
        return store instanceof NamespaceLogMetadataStore ? "namespace-log" : "zk";
    }

    /** 1 if the namespace-log backend is active (file/path metadata stored as replicated Strata files). */
    public boolean namespaceLogActive() {
        return store instanceof NamespaceLogMetadataStore;
    }

    /**
     * The configured controller-endpoint membership size — the number of controllers that share the
     * namespaces (rendezvous-hash owners). Same on every node; {@code max()} across the fleet is the
     * controller-server count. Backend-independent (sharding is keyed on endpoint count, not the store kind).
     */
    public int controllerEndpointsConfigured() {
        return ownership.eligibleEndpoints().size();
    }

    /** 1 if namespaces are sharded across multiple controllers (multi-endpoint); 0 = single global leader. */
    public boolean shardingActive() {
        return !ownership.ownsAll();
    }

    /** Namespaces this instance owns a live metadata-log repository for (sharding load); 0 under ZK. */
    public int loadedNamespaces() {
        return store instanceof NamespaceLogMetadataStore log ? log.loadedNamespaceCount() : 0;
    }

    /**
     * Per-namespace stats for the namespaces this controller owns: {@code namespace -> [liveFiles,
     * openLogBytes]}. Empty under the ZK backend. For the per-namespace dashboard panels (namespace-
     * stacked) — cardinality grows with the namespace count, so it is namespace-log + control-plane only.
     */
    public java.util.Map<String, long[]> namespaceStats() {
        if (!(store instanceof NamespaceLogMetadataStore log)) {
            return java.util.Map.of();
        }
        java.util.Map<String, long[]> out = new java.util.HashMap<>();
        log.namespaceStats().forEach((ns, stat) -> out.put(ns.value(), stat));
        return out;
    }

    /**
     * Per-namespace namespace-log counters for the namespaces this controller owns; empty under the ZK
     * backend. Value is the fixed index order documented on {@code NamespaceLogMetrics.stats()}:
     * [appendRecords, appendBytes, readRecords, readBytes, compactions, recoveries, reacquisitions,
     * ownerChanges]. Drives the per-namespace write-log / read-log / compaction / owner-change panels;
     * the global controller view is {@code sum without(namespace)}.
     */
    public java.util.Map<String, long[]> namespaceLogStats() {
        return store instanceof NamespaceLogMetadataStore log ? log.metrics().stats() : java.util.Map.of();
    }

    /** Namespaces this controller has namespace-log activity for — drives lazy per-namespace meter
     *  registration without a snapshot allocation; empty under the ZK backend. */
    public java.util.Set<String> namespaceLogNamespaces() {
        return store instanceof NamespaceLogMetadataStore log ? log.metrics().namespaces() : java.util.Set.of();
    }

    /** One per-namespace namespace-log counter by index (see {@link #namespaceLogStats}); 0 under ZK or if
     *  absent. O(1) — bound per Micrometer FunctionCounter so each scrape avoids rebuilding the stats map. */
    public long namespaceLogValue(String namespace, int index) {
        return store instanceof NamespaceLogMetadataStore log ? log.metrics().value(namespace, index) : 0L;
    }

    /** This controller's rendezvous endpoint identity — the {@code owner} label for the namespace-owner
     *  gauge ({@code strata_controller_namespace_owner}); same value the latch advertises. */
    public String localControllerEndpoint() {
        return ownership.localEndpoint();
    }

    /** Consensus-root requests this service has issued against a {@code /strata} subtree, by op kind. */
    public long metadataStoreOps(String subtree, boolean write) {
        return rootZk.zkOps(subtree, write);
    }

    /** Consensus-root payload bytes this service has read/written against a {@code /strata} subtree. */
    public long metadataStoreBytes(String subtree, boolean write) {
        return rootZk.zkBytes(subtree, write);
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

    /** Repairs issued by the event lane (node-death driven) since start — monotonic. */
    public long eventRepairs() {
        return repair.eventRepairs();
    }

    /** Repairs issued by the reconcile backstop lane since start — monotonic. */
    public long reconcileRepairs() {
        return repair.reconcileRepairs();
    }

    /** Files skipped due to per-file errors in the reconcile pass since start — monotonic. */
    public long reconcileSkippedFiles() {
        return repair.reconcileSkippedFiles();
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
        if (server != null) {  // embedded mode: requests are observed on the host node's server
            server.setRequestObserver(observer);
        }
    }

    /** Test hook: force one reconciliation pass now. */
    public void reconcileNow() throws Exception {
        registry.expireScan();
        repair.scanOnce();
    }

    private void requireLeader() {
        LeaderLatch latch = this.leaderLatch;
        if (latch == null || !latch.hasLeadership()) {
            throw new ScpException(ErrorCode.NOT_LEADER, "not the controller leader", 0, leaderHint());
        }
    }

    /**
     * The current leader's advertised endpoint, for a NOT_LEADER redirect — or null if unknown
     * (election in progress, or this node thinks it is the leader). A ZooKeeper read, only walked on
     * the rejection path (never on the leader), so misdirected clients leave after one round-trip.
     */
    private String leaderHint() {
        LeaderLatch latch = this.leaderLatch;
        if (latch == null) {
            return null;
        }
        try {
            String leader = latch.getLeader().getId();
            if (leader == null || leader.isBlank() || leader.equals(advertisedEndpoint)) {
                return null;
            }
            return leader;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Gate a namespace-scoped op (design §6): the controller owner serves; a non-owner is redirected with a
     * NOT_LEADER hint pointing at the owner endpoint. A non-sharded (single-endpoint) deployment falls
     * back to the global leader latch, so single-leader behavior is unchanged.
     */
    private void requireNamespaceOwner(StrataNamespace namespace) {
        // Tag this request's metrics with its namespace (read back by ScpServer's request observer).
        io.strata.proto.RequestContext.setNamespace(namespace.value());
        if (NamespaceLogBackend.isSystem(namespace)) {
            // Metadata-log system files live in the shared ZK root (CAS-guarded), so any node may serve
            // them — a non-controller owner writes its own namespace's metadata-log files here.
            return;
        }
        if (ownership.ownsAll()) {
            requireLeader();
            return;
        }
        if (!ownership.isOwner(namespace)) {
            // Redirect to the namespace's owner; the owner-aware client caches namespace->owner and
            // routes directly, re-resolving only on this exception (an ownership change) (design §6).
            throw new ScpException(ErrorCode.NOT_LEADER,
                    "namespace " + namespace + " is owned by another controller", 0,
                    ownership.ownerOf(namespace));
        }
    }

    /** Test/inspection hook: this node's namespace-ownership resolver. */
    NamespaceOwnership ownership() {
        return ownership;
    }

    private Frame handle(Frame req) throws Exception {
        Opcode op = Opcode.fromCode(req.opcode());
        if (op == null) throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "0x" + Integer.toHexString(req.opcode()));
        ByteBuffer h = req.headerSlice();
        return switch (op) {
            case PING -> {
                requireLeader();
                yield ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());
            }

            /* ---- data-node control (tech design §10.4): cluster-controller scope, global latch ---- */

            case REGISTER_NODE -> {
                requireLeader();
                var m = Messages.RegisterNode.decode(h);
                yield ScpServer.ok(req, registry.register(m).encode(), null);
            }

            case NODE_HEARTBEAT -> {
                requireLeader();
                var m = Messages.NodeHeartbeat.decode(h);
                yield ScpServer.ok(req, registry.heartbeat(m, repair::onCommandCompletedAsync).encode(), null);
            }

            /* ---- v0 client APIs (v1: Kafka RPC on the controller) ---- */

            case CREATE_FILE -> {
                var m = Messages.CreateFile.decode(h);
                requireNamespaceOwner(m.namespace());
                FileId id = createFile(m);
                yield ScpServer.ok(req, new Messages.CreateFileResp(id).encode(), null);
            }

            case CREATE_CHUNK -> {
                var m = Messages.CreateChunk.decode(h);
                requireNamespaceOwner(m.namespace());
                yield ScpServer.ok(req, createChunk(m).encode(), null);
            }

            case ALLOCATE_WRITER_EPOCH -> {
                var m = Messages.AllocateWriterEpoch.decode(h);
                requireNamespaceOwner(m.namespace());
                yield ScpServer.ok(req, new Messages.AllocateWriterEpochResp(allocateWriterEpoch(m)).encode(),
                        null);
            }

            case SEAL_CHUNK_META -> {
                var m = Messages.SealChunkMeta.decode(h);
                requireNamespaceOwner(m.namespace());
                sealChunk(m);
                yield ScpServer.ok(req, Messages.okHeader(), null);
            }

            case ABORT_CHUNK_META -> {
                var m = Messages.AbortChunkMeta.decode(h);
                requireNamespaceOwner(m.namespace());
                abortChunk(m);
                yield ScpServer.ok(req, Messages.okHeader(), null);
            }

            case SEAL_FILE -> {
                var m = Messages.SealFile.decode(h);
                requireNamespaceOwner(m.namespace());
                // a file seals only when every chunk is sealed and the lengths add up — a stale
                // or buggy client must not freeze a file mid-write (validated under CAS, so it
                // is race-free against concurrent chunk creates)
                mutateFile(m.namespace(), m.fileId(), file -> {
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
                requireNamespaceOwner(m.namespace());
                yield ScpServer.ok(req, lookup(m.namespace(), m.fileId()).encode(), null);
            }

            case LOOKUP_PATH -> {
                var m = Messages.LookupPath.decode(h);
                requireNamespaceOwner(m.namespace());
                yield ScpServer.ok(req, new Messages.LookupPathResp(lookupPath(m.namespace(), m.path())).encode(),
                        null);
            }

            case DELETE_FILES -> {
                var m = Messages.DeleteFiles.decode(h);
                requireNamespaceOwner(m.namespace());
                List<Short> codes = new ArrayList<>();
                for (FileId id : m.fileIds()) {
                    try {
                        markDeleting(m.namespace(), id);
                        repair.driveDeletionSoon(m.namespace(), id); // prompt reclaim, but don't block metadata delete responses
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
            var opt = getFile(m.namespace(), m.fileId());
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
        // Namespace-log backend: opId-keyed idempotency and id assignment are handled atomically
        // inside the owner's namespace lock (O(1) index lookup, no id waste on retry).
        if (store instanceof NamespaceLogMetadataStore log) {
            Records.FileRecord template = new Records.FileRecord(
                    FileId.of(0), // placeholder — backend assigns the real id
                    m.namespace(), m.path(),
                    policy.replicationFactor(), policy.ackQuorum(), policy.fsyncOnAck(),
                    FileState.OPEN, System.currentTimeMillis(), List.of(),
                    m.opIdMsb(), m.opIdLsb());
            return log.createFileOwnerAssigned(template);
        }
        // ZK backend (v0 prototype): assign via global ZK counter, then create.
        // Pre-check: scan namespace for any live or tombstoned file with this opId; if found,
        // either return it (idempotent retry) or reject (stale replay after deletion). O(N) per-create
        // is acceptable for the v0 prototype.
        if (store instanceof ZkMetadataStore zk) {
            for (FileId candidate : zk.listFilesIncludingTombstones(m.namespace())) {
                var opt = zk.getFileIncludingTombstone(candidate);
                if (opt.isPresent()) {
                    Records.FileRecord record = opt.get();
                    if (record.createdBy(m.opIdMsb(), m.opIdLsb())) {
                        if (record.state() == FileState.DELETED) {
                            throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                                    "create opId was already used for a file that has been deleted");
                        }
                        if (record.state() == FileState.DELETING) {
                            throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                                    "create opId belongs to a file that is being deleted");
                        }
                        if (sameCreateRequest(record, m)) {
                            return record.fileId();  // idempotent retry
                        }
                        throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                                "create opId already in use for a different request");
                    }
                }
            }
        }
        FileId assigned = store.assignFileId(m.namespace());
        try {
            store.createFile(new Records.FileRecord(assigned, m.namespace(), m.path(),
                    policy.replicationFactor(), policy.ackQuorum(), policy.fsyncOnAck(),
                    FileState.OPEN, System.currentTimeMillis(), List.of(),
                    m.opIdMsb(), m.opIdLsb()));
            return assigned;
        } catch (KeeperException.NodeExistsException e) {
            // Path already bound (race or same-opId retry we missed above) — scan live files.
            for (FileId candidate : store.listFiles(m.namespace())) {
                var opt = store.getFile(m.namespace(), candidate);
                if (opt.isPresent()) {
                    Records.FileRecord record = opt.get().value();
                    if (sameCreateRequest(record, m) && record.state() != FileState.DELETING) {
                        return record.fileId();
                    }
                }
            }
            // Path exists but doesn't match this opId/request — conflict.
            var pathOwner = store.resolvePath(m.namespace(), m.path());
            if (pathOwner.isPresent()) {
                throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                        "path already exists: " + m.namespace() + ":" + m.path());
            }
            throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                    "create opId already in use for a different request");
        }
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
            var opt = getFile(m.namespace(), m.fileId());
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
            List<NodeRegistry.LiveNode> nodes = Placement.choose(file.namespace(), registry,
                    file.replicationFactor(), Set.copyOf(m.excludedNodeIds()), Set.of());
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
            var opt = getFile(m.namespace(), m.chunkId().fileId());
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
                    // incarnation pin: a seal that names its create-op must target the same chunk
                    // incarnation it created. At equal write epochs the fence cannot separate an
                    // abort + same-epoch recreate, so a stale seal from the old incarnation would
                    // otherwise apply its length/crc to the new one. opId (0,0) opts out — seal
                    // recovery seals at a strictly higher epoch and relies on the fence alone.
                    if ((m.opIdMsb() != 0 || m.opIdLsb() != 0)
                            && m.writeEpoch() == c.writeEpoch()
                            && !c.createdBy(m.opIdMsb(), m.opIdLsb())) {
                        throw new ScpException(ErrorCode.FENCED_EPOCH,
                                "seal opId does not match chunk incarnation at epoch " + c.writeEpoch(),
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
            var opt = getFile(m.namespace(), m.chunkId().fileId());
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

    private Optional<MetadataStore.Versioned<Records.FileRecord>> getFile(
            StrataNamespace namespace, FileId fileId) throws Exception {
        return store.getFile(namespace, fileId);
    }

    private Messages.LookupFileResp lookup(StrataNamespace namespace, FileId fileId) throws Exception {
        var opt = getFile(namespace, fileId);
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
        var file = getFile(namespace, id.get());
        if (file.isEmpty() || file.get().value().state() == FileState.DELETING) {
            throw new ScpException(ErrorCode.FILE_NOT_FOUND, namespace + ":" + path);
        }
        Records.FileRecord record = file.get().value();
        if (!record.namespace().equals(namespace) || !record.path().equals(path)) {
            throw new ScpException(ErrorCode.FILE_NOT_FOUND, namespace + ":" + path);
        }
        return id.get();
    }

    private void mutateFile(StrataNamespace namespace, FileId id,
                            java.util.function.UnaryOperator<Records.FileRecord> fn) throws Exception {
        for (int attempt = 0; attempt < CAS_RETRIES; attempt++) {
            var opt = getFile(namespace, id);
            if (opt.isEmpty()) throw new ScpException(ErrorCode.FILE_NOT_FOUND, id.toString());
            if (store.updateFile(fn.apply(opt.get().value()), opt.get().version())) return;
        }
        throw new ScpException(ErrorCode.INTERNAL, "file mutation CAS exhausted");
    }

    private void markDeleting(StrataNamespace namespace, FileId id) throws Exception {
        for (int attempt = 0; attempt < CAS_RETRIES; attempt++) {
            var opt = getFile(namespace, id);
            // idempotent after deletion: a DELETED tombstone (and a later swept record) read as
            // empty here, as does a never-created id. A delete retry whose first response was lost
            // — or one that lands after a controller failover/tombstone reap — must ack OK rather
            // than FILE_NOT_FOUND, so the caller observes a single logical deletion. (abortChunk
            // returns idempotently on the same empty condition.)
            if (opt.isEmpty()) return;
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
