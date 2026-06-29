package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Endpoint;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.NsChunkId;
import io.strata.common.StrataNamespace;
import io.strata.proto.BufWriter;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The repair coordinator (tech design §7.2, §9): a periodic reconciliation scan is the single
 * engine for under-replication repair, DELETING-file driving, and command retry — individual
 * command failures are simply forgotten and rediscovered by the next scan. Exposure-prioritized:
 * chunks with fewer live replicas are commanded first.
 */
// Non-final so RepairCoordinatorLoopTest can subclass and count tick()/reconcile() dispatches —
// a deterministic seam over the loop's cadence (no injectable clock to fake otherwise).
class RepairCoordinator implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RepairCoordinator.class);

    /**
     * Which repair lane issued a repair, for the {@code strata_repair_actions} trigger-tagged counter.
     * Observational only — the trigger never changes placement, dedup, or whether a repair is issued.
     */
    enum RepairTrigger {
        /** Lane B: a node-death event ({@code repairForDeadNode}). */
        EVENT,
        /** Lane C: the periodic backstop reconcile ({@code scanOnce} / {@code ownerRepairPass}). */
        RECONCILE
    }

    private sealed interface Action permits ReplicateAction, DeleteAction {
        int executingNode();

        long issuedAtMs();
    }

    private record ReplicateAction(StrataNamespace namespace, FileId fileId, ChunkId chunkId,
                                   int deadNode, int targetNode, long issuedAtMs) implements Action {
        @Override
        public int executingNode() {
            return targetNode;
        }
    }

    private record DeleteAction(StrataNamespace namespace, FileId fileId, ChunkId chunkId,
                                int nodeId, long issuedAtMs) implements Action {
        @Override
        public int executingNode() {
            return nodeId;
        }
    }

    private final MetadataStore store;
    private final NodeRegistry registry;
    private final ControllerConfig config;
    private final java.util.function.BooleanSupplier isLeader;
    // Whether this node owns every namespace (non-sharded / single-endpoint). When false (sharded), an
    // inventory chunk whose file this node cannot see may belong to a namespace owned by another meta
    // node, so it must NOT be deleted as an orphan (design §11 single-writer safety / data-loss guard).
    private final java.util.function.BooleanSupplier ownsAll;
    // Whether this node is the controller owner of a namespace — scopes the non-controller owner repair pass.
    private final java.util.function.Predicate<StrataNamespace> ownsNamespace;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong commandIds = new AtomicLong(System.currentTimeMillis());
    private final Map<Long, Action> inflight = new ConcurrentHashMap<>();
    private final ExecutorService deleteDispatchExecutor = Executors.newSingleThreadExecutor(
            Thread.ofVirtual().name("meta-delete-dispatch-", 0).factory());
    private final ExecutorService completionExecutor = Executors.newSingleThreadExecutor(
            Thread.ofVirtual().name("meta-command-completion-", 0).factory());
    // Lane B: a node death surfaced by expireScan() is fanned out here so the slow reconcile no longer
    // gates how fast under-replication repair starts. Single-thread (serialized with itself) and off the
    // tick() thread so a slow per-namespace enumeration can never stall lease expiry / command sweeping.
    private final ExecutorService repairEventExecutor = Executors.newSingleThreadExecutor(
            Thread.ofVirtual().name("meta-repair-event-", 0).factory());
    // (nodeId:chunkId) -> first time an owner-pull VERIFY_CHUNKS report showed a sealed replica
    // unhealthy (missing or still locally OPEN/DELETING). A just-sealed chunk can race with an
    // in-flight verify pass; only drop the replica if it stays unhealthy past the grace, so churn
    // can't falsely remove every good replica and trigger a re-replication storm.
    private final Map<String, Long> replicaMissingSince = new ConcurrentHashMap<>();
    private final Set<NsChunkId> chunksBeingRepaired = ConcurrentHashMap.newKeySet();
    private final Map<ReplicaKey, Long> recentlyCommittedReplicas = new ConcurrentHashMap<>();
    // Monotonic count of repairs actually issued, split by trigger lane (event vs reconcile). Bumped
    // exactly once at the point a REPLICATE is enqueued / an owner EXEC_REPLICATE is committed, after
    // the chunksBeingRepaired dedup add succeeds — never on a dedup-skip, placement miss, or failure.
    private final AtomicLong eventRepairs = new AtomicLong();
    private final AtomicLong reconcileRepairs = new AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong reconcileSkippedFiles = new java.util.concurrent.atomic.AtomicLong();
    private volatile Thread scanThread;

    // A deleted file's record is kept as a DELETED tombstone (fencing a delayed CREATE replay from
    // resurrecting it) and reaped only after this window. INVARIANT: it must exceed the longest
    // possible create-replay delay — the client create-retry deadline (max(15s, callTimeoutMs), see
    // ControllerClient) plus the controller<->ZK clock-skew the mtime-based sweep tolerates — and it must exceed
    // repairScanIntervalMs (the sweep cadence). 10 min vs the 15s default is a ~40x margin; raise it
    // if callTimeoutMs is ever configured near it.
    private static final long DELETED_TOMBSTONE_TTL_MS = 600_000;

    // Durability gauges, refreshed by each scanOnce so the metrics endpoint reads them in O(1)
    // (no extra ZK scan per scrape). volatile: written by the scan thread, read by the HTTP thread.
    private volatile int underReplicatedChunks;
    private volatile int unavailableChunks;
    private volatile int chunksAtMinRedundancy;

    /** SEALED chunks with 0 &lt; live replicas &lt; replicationFactor (last scan). */
    public int underReplicatedChunks() {
        return underReplicatedChunks;
    }

    /** SEALED chunks with zero live replicas — unreadable, unrepairable, data-loss exposure (last scan). */
    public int unavailableChunks() {
        return unavailableChunks;
    }

    /** SEALED chunks down to a single live replica — one failure from loss (last scan). */
    public int chunksAtMinRedundancy() {
        return chunksAtMinRedundancy;
    }

    /** Repair/delete commands currently outstanding. */
    public int repairInflight() {
        return inflight.size();
    }

    /** Distinct chunks currently being repaired (working-set / backlog depth). */
    public int repairBacklog() {
        return chunksBeingRepaired.size();
    }

    /** Repairs issued by the event lane (node-death driven) since start — monotonic. */
    long eventRepairs() {
        return eventRepairs.get();
    }

    /** Repairs issued by the reconcile backstop lane since start — monotonic. */
    long reconcileRepairs() {
        return reconcileRepairs.get();
    }

    /** Files skipped due to per-file errors in the reconcile pass — monotonic. */
    long reconcileSkippedFiles() {
        return reconcileSkippedFiles.get();
    }

    /** Bumps the counter for {@code trigger}'s lane — called once per repair actually issued. */
    private void recordRepairIssued(RepairTrigger trigger) {
        (trigger == RepairTrigger.EVENT ? eventRepairs : reconcileRepairs).incrementAndGet();
    }

    private record ReplicaKey(StrataNamespace namespace, ChunkId chunkId, int nodeId) {}

    RepairCoordinator(MetadataStore store, NodeRegistry registry, ControllerConfig config,
                      java.util.function.BooleanSupplier isLeader) {
        this(store, registry, config, isLeader, () -> true, ns -> true);
    }

    RepairCoordinator(MetadataStore store, NodeRegistry registry, ControllerConfig config,
                      java.util.function.BooleanSupplier isLeader,
                      java.util.function.BooleanSupplier ownsAllNamespaces) {
        this(store, registry, config, isLeader, ownsAllNamespaces, ns -> true);
    }

    RepairCoordinator(MetadataStore store, NodeRegistry registry, ControllerConfig config,
                      java.util.function.BooleanSupplier isLeader,
                      java.util.function.BooleanSupplier ownsAllNamespaces,
                      java.util.function.Predicate<StrataNamespace> ownsNamespace) {
        this.store = store;
        this.registry = registry;
        this.config = config;
        this.isLeader = isLeader;
        this.ownsAll = ownsAllNamespaces;
        this.ownsNamespace = ownsNamespace;
    }

    void start() {
        scanThread = Thread.ofVirtual().name("meta-repair-scan").start(this::scanLoop);
        verifyThread = Thread.ofVirtual().name("meta-verify-scan").start(this::verifyLoop);
    }

    /** This owner's advertised endpoint, sent as the {@code verifierEndpoint} in VERIFY_CHUNKS so a node
     *  can record which owner attested it (design §20.4). Set by the controller before {@link #start}. */
    private volatile String advertisedEndpoint = "";

    void advertisedEndpoint(String endpoint) {
        this.advertisedEndpoint = endpoint == null ? "" : endpoint;
    }

    private volatile Thread verifyThread;
    // Owner-pull verification cadence (design §20.3). A constant for now — the project does not ship to
    // prod, and this is the only missing/corrupt-replica detection path once the inventory push is gone,
    // so it stays brisk enough that detection + repair lands well inside the integration deadlines.
    private static final long VERIFY_INTERVAL_MS = 2_000;
    private static final int VERIFY_BATCH_SIZE = 256;

    // leaderSince is written by the scan thread (tick/reconcile) and read by the verify thread's settle
    // gate, so it must publish safely across threads.
    private volatile long leaderSince;
    /**
     * Serializes the descriptor-mutating reconcile passes — scanOnce() (leader), ownerRepairPass()
     * (non-leader owner), and driveDeletionNow() (prompt delete) — against each other (replaces a
     * {@code synchronized(this)} monitor). A ReentrantLock does not pin the virtual-thread carrier across
     * the blocking store/ZK and node-RPC I/O these methods perform, unlike {@code synchronized} on Java 21.
     */
    private final java.util.concurrent.locks.ReentrantLock reconcileLock =
            new java.util.concurrent.locks.ReentrantLock();

    private void scanLoop() {
        long lastTombstoneSweep = 0;
        while (!closed.get()) {
            try {
                Thread.sleep(config.repairScanIntervalMs());
                // Lane A — cheap in-memory housekeeping: lease expiry plus event-driven repair of
                // newly-dead nodes (leader-gated inside).
                tick();
                // Lane C — the full under-replication repair and DELETING re-drive pass, run every
                // fast interval (self-gates on leader + settle). Convergence needs *repeated* passes: a
                // pass drives the outstanding deletes/repairs, the holders re-register and confirm over
                // a heartbeat cycle, and a later pass finalizes. The event path only re-drives newly-dead
                // nodes — not a delete/repair a metadata failover or node crash interrupted mid-flight —
                // so a single slow pass would strand that work until the next interval, outlasting every
                // convergence deadline. Until the event path also covers missing replicas and delete
                // finalization, this pass carries that correctness.
                reconcile();
                // Tombstone GC is the one genuinely periodic chore and does not gate convergence (a file
                // reads as deleted the moment it reaches DELETED, well before its tombstone is swept), so
                // it stays on the slow reconcileIntervalMs cadence instead of running every tick.
                long now = System.currentTimeMillis();
                if (isLeader.getAsBoolean() && now - lastTombstoneSweep >= config.reconcileIntervalMs()) {
                    store.sweepDeletedFiles(DELETED_TOMBSTONE_TTL_MS);
                    lastTombstoneSweep = now;
                }
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                if (!closed.get()) {
                    log.warn("repair scan failed", e);
                }
            }
        }
    }

    /**
     * Lane A: light in-memory housekeeping run every {@code repairScanIntervalMs}. Leader-gated — a
     * standby must never run failure detection (no heartbeats are routed to it, so it would declare
     * every node DEAD). Publishes the shared live-node snapshot every pass (NOT gated by the settle
     * period: sharded non-controller owners need a placement view immediately or every forwarded op
     * stalls), then — past the settle period — expires leases and releases stuck repair commands.
     */
    void tick() {
        if (!isLeader.getAsBoolean()) {
            leaderSince = 0;
            return;
        }
        if (leaderSince == 0) {
            leaderSince = System.currentTimeMillis();
        }
        registry.publishClusterLiveNodes();
        // settle period after acquiring leadership: nodes registered with the previous leader need a
        // lease cycle to re-register here, or expireScan() would mark them DEAD spuriously.
        if (System.currentTimeMillis() - leaderSince < config.leaseMs() + config.deadGraceMs()) {
            return;
        }
        // Lane B: feed each newly-dead node to a targeted repair so a death starts repair immediately
        // instead of waiting up to a full reconcileIntervalMs. The slow reconcile remains the backstop;
        // the event path goes through the same dedup'd per-chunk issue, so the two cannot double-issue.
        List<Integer> dead = registry.expireScan();
        for (int deadNodeId : dead) {
            repairEventExecutor.submit(() -> repairForDeadNodeSafe(deadNodeId));
        }
        sweepStuckCommands();
    }

    /** Test seam: stamps {@code leaderSince} far enough in the past that the settle gate is open. */
    void becomeLeaderForTest() {
        leaderSince = System.currentTimeMillis() - (config.leaseMs() + config.deadGraceMs()) - 1;
    }

    /**
     * Wraps {@link #repairForDeadNode} so a failure on the event path never escapes the executor: the
     * slow reconcile is the correctness backstop, so a missed event-driven repair only delays healing.
     */
    private void repairForDeadNodeSafe(int deadNodeId) {
        try {
            repairForDeadNode(deadNodeId);
        } catch (Exception e) {
            log.warn("event repair for dead node {} failed — reconcile backstops", deadNodeId, e);
        }
    }

    /**
     * Lane B targeted repair: re-replicate exactly the sealed chunks that had a replica on
     * {@code deadNodeId} and are now under-replicated. Leader-only (owners are wired in a later task)
     * and settle-gated like the reconcile, so a freshly-elected leader does not repair before nodes
     * re-register. Enumerates the namespaces this leader can authoritatively account for — the ones it
     * owns (or all, when non-sharded) plus the system meta-log namespace — and reuses the shared
     * per-chunk path, so it dedups against {@code chunksBeingRepaired} with any concurrent reconcile.
     */
    void repairForDeadNode(int deadNodeId) throws Exception {
        if (!isLeader.getAsBoolean()) {
            return;
        }
        if (System.currentTimeMillis() - leaderSince < config.leaseMs() + config.deadGraceMs()) {
            return;
        }
        for (StrataNamespace ns : store.listNamespaces()) {
            if (!ownsAll.getAsBoolean() && !ownsNamespace.test(ns) && !NamespaceLogBackend.isSystem(ns)) {
                continue;
            }
            for (FileId fileId : store.listFiles(ns)) {
                Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(ns, fileId);
                if (opt.isEmpty()) {
                    continue;
                }
                repairFileChunksOnNode(ns, fileId, opt.get().value(), deadNodeId);
            }
        }
    }

    /**
     * Issues repair for every sealed, under-replicated chunk of {@code file} that still references
     * {@code deadNodeId}. Shared by the event path; {@link #issueReplicate} performs the placement,
     * the {@code chunksBeingRepaired} dedup, and the command enqueue exactly as the reconcile does —
     * this method only narrows the candidate set to chunks affected by the dead node.
     */
    private void repairFileChunksOnNode(StrataNamespace ns, FileId fileId, Records.FileRecord file,
                                         int deadNodeId) {
        if (file.state() == FileState.DELETING) {
            return; // deletion, not repair, drives a DELETING file's chunks
        }
        for (Records.ChunkRecord chunk : file.chunks()) {
            if (chunk.state() != ChunkState.SEALED || !chunk.replicas().contains(deadNodeId)) {
                continue;
            }
            ChunkId chunkId = file.chunkId(chunk.index());
            if (chunksBeingRepaired.contains(new NsChunkId(ns, chunkId))) {
                continue;
            }
            int live = 0;
            for (int nodeId : chunk.replicas()) {
                if (!registry.isDead(nodeId)) live++;
            }
            if (live < file.replicationFactor() && live > 0) {
                issueReplicate(ns, fileId, file, chunk, RepairTrigger.EVENT);
            }
        }
    }

    /**
     * Lane C: the full backstop reconciliation, run every {@code reconcileIntervalMs}. The controller
     * scans every file for under-replication and drives deletions; a sharded non-controller owner heals
     * only the namespaces it owns directly via EXEC_REPLICATE. Leader path is settle-gated so a freshly
     * elected leader does not issue spurious repairs before nodes re-register.
     */
    void reconcile() throws Exception {
        if (!isLeader.getAsBoolean()) {
            ownerRepairPass();
            return;
        }
        if (leaderSince == 0) {
            leaderSince = System.currentTimeMillis();
        }
        if (System.currentTimeMillis() - leaderSince < config.leaseMs() + config.deadGraceMs()) {
            return;
        }
        scanOnce();
    }

    /**
     * Live (namespace, fileId) pairs across every namespace. Repair reconciles the whole cluster,
     * not one tenant, so it composes per-namespace listings — there is no global flat file enumeration.
     * Returns a list (not a map) so that colliding numeric FileIds in different namespaces are never
     * collapsed — each (namespace, fileId) pair is a distinct entry (design §Task-9 final review fix).
     */
    private List<NsFileKey> allFilesByNamespace() throws Exception {
        List<NsFileKey> out = new ArrayList<>();
        for (var ns : store.listNamespaces()) {
            for (FileId id : store.listFiles(ns)) {
                out.add(new NsFileKey(ns, id));
            }
        }
        return out;
    }

    /** One reconciliation pass over all files. Idempotent; safe to call while serving. */
    void scanOnce() throws Exception {
        reconcileLock.lock();
        try {
        // The controller refreshes the shared live-node snapshot each pass so non-controller namespace
        // owners have a current placement view (design §11). Leader-gated: a standby must not publish.
        if (isLeader.getAsBoolean()) {
            registry.publishClusterLiveNodes();
        }
        sweepStuckCommands();
        record Repair(StrataNamespace ns, FileId fileId, Records.FileRecord file,
                      Records.ChunkRecord chunk, int liveReplicas) {}
        List<Repair> repairs = new ArrayList<>();
        // durability census, published to gauges at the end; int[] lets lambdas increment them
        int[] under = {0}, unavailable = {0}, atMin = {0};

        for (NsFileKey entry : allFilesByNamespace()) {
            FileId fileId = entry.fileId();
            StrataNamespace ns = entry.namespace();
            perFileIsolated(ns, fileId, "scanOnce", () -> {
                Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(ns, fileId);
                if (opt.isEmpty()) return;
                Records.FileRecord file = opt.get().value();

                if (file.state() == FileState.DELETING) {
                    driveDeletion(file, opt.get().version());
                    return;
                }
                for (Records.ChunkRecord chunk : file.chunks()) {
                    if (chunk.state() != ChunkState.SEALED) continue; // open chunks belong to their writer
                    ChunkId chunkId = file.chunkId(chunk.index());
                    int live = 0;
                    for (int nodeId : chunk.replicas()) {
                        if (!registry.isDead(nodeId)) live++;
                    }
                    // durability census — counted for every sealed chunk regardless of in-flight repair
                    if (live == 0) {
                        unavailable[0]++;
                    } else if (live < file.replicationFactor()) {
                        under[0]++;
                        if (live == 1) atMin[0]++;
                    }
                    if (chunksBeingRepaired.contains(new NsChunkId(ns, chunkId))) continue;
                    if (live < file.replicationFactor() && live > 0) {
                        repairs.add(new Repair(ns, fileId, file, chunk, live));
                    } else if (live == 0) {
                        log.error("chunk {} has NO live replicas — data loss exposure, cannot repair", chunkId);
                    }
                }
            });
        }
        underReplicatedChunks = under[0];
        unavailableChunks = unavailable[0];
        chunksAtMinRedundancy = atMin[0];

        // exposure priority: fewest live replicas first (tech design §7.2)
        repairs.sort(java.util.Comparator.comparingInt(Repair::liveReplicas));
        for (Repair r : repairs) {
            issueReplicate(r.ns(), r.fileId(), r.file(), r.chunk(), RepairTrigger.RECONCILE);
        }
        } finally {
            reconcileLock.unlock();
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * Runs {@code body} for a single (ns, fileId) pair, isolating per-file failures: an
     * {@link InterruptedException} is re-thrown so that close()/shutdown() can interrupt the outer
     * loop; any other exception is logged as a warning and swallowed so the pass continues.
     */
    private void perFileIsolated(StrataNamespace ns, FileId fileId, String pass,
                                 ThrowingRunnable body) throws InterruptedException {
        try {
            body.run();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        } catch (Exception e) {
            log.warn("{}: skipping ns={} fileId={} due to error — repair pass continues",
                    pass, ns, fileId, e);
            reconcileSkippedFiles.incrementAndGet();
        }
    }

    /**
     * Releases in-flight commands whose executing node died or which aged past the command
     * timeout without a completion — otherwise the chunksBeingRepaired marker would suppress an
     * under-replicated chunk forever (until a service restart). The next scan re-issues.
     */
    private void sweepStuckCommands() {
        long now = System.currentTimeMillis();
        for (Map.Entry<Long, Action> e : inflight.entrySet()) {
            Action a = e.getValue();
            boolean dead = registry.isDead(a.executingNode());
            boolean expired = now - a.issuedAtMs() > config.repairCommandTimeoutMs();
            if (!dead && !expired) continue;
            if (inflight.remove(e.getKey()) == null) continue; // completion raced us — it won
            if (a instanceof ReplicateAction r) {
                chunksBeingRepaired.remove(new NsChunkId(r.namespace(), r.chunkId()));
                log.warn("repair cmd {} for {} abandoned (target {} {}) — will re-issue",
                        e.getKey(), r.chunkId(), r.targetNode(), dead ? "dead" : "timed out");
            }
        }
    }

    private void issueReplicate(StrataNamespace ns, FileId fileId, Records.FileRecord file,
                                Records.ChunkRecord chunk, RepairTrigger trigger) {
        ChunkId chunkId = file.chunkId(chunk.index());
        int deadNode = -1;
        List<Messages.Replica> sources = new ArrayList<>();
        Set<Integer> existing = new HashSet<>(chunk.replicas());
        Set<String> usedHosts = new HashSet<>();
        for (int nodeId : chunk.replicas()) {
            if (registry.isDead(nodeId)) {
                deadNode = nodeId;
            } else {
                sources.add(registry.replicaOf(nodeId));
                String host = registry.hostOf(nodeId);
                if (host != null) usedHosts.add(host);
            }
        }
        if (sources.isEmpty()) return;
        if (deadNode < 0 && chunk.replicas().size() >= file.replicationFactor()) {
            return; // fully replicated on live nodes — nothing to do
        }
        // deadNode >= 0: swap the dead member out. deadNode < 0 with a short replica list:
        // ADD a member — happens after a corrupt/missing replica was dropped from the
        // descriptor (inventory reconciliation); without add-mode the chunk would be stranded
        // under-replicated with no dead node to replace.

        List<NodeRegistry.LiveNode> targets;
        try {
            targets = Placement.choose(ns, registry, 1, existing, usedHosts);
        } catch (Exception e) {
            log.warn("no repair target for {}: {}", chunkId, e.getMessage());
            return;
        }
        NodeRegistry.LiveNode target = targets.get(0);
        if (!chunksBeingRepaired.add(new NsChunkId(ns, chunkId))) {
            return;
        }
        long cmdId = commandIds.incrementAndGet();
        inflight.put(cmdId, new ReplicateAction(ns, fileId, chunkId, deadNode, target.record.nodeId(),
                System.currentTimeMillis()));
        registry.enqueue(target.record.nodeId(),
                new Messages.ReplicateCmd(cmdId, chunkId, sources, (byte) 1, chunk.crc(), chunk.length(),
                        ns));
        recordRepairIssued(trigger);
        log.info("repair: {} dead={} -> target={} (cmd {})", chunkId, deadNode, target.record.nodeId(), cmdId);
    }

    /**
     * Non-controller owner repair (design §11): a namespace owner that does not hold the global latch
     * cannot use the controller's heartbeat command channel, so it heals its own namespaces'
     * under-replicated sealed chunks directly. It reads the controller's authoritative DEAD set from the
     * consensus root, picks a replacement target from the shared live-node snapshot, tells that target
     * to pull the chunk via EXEC_REPLICATE, then writes the replica change itself (single writer for the
     * namespace). Non-sharded deployments return immediately — the cluster controller does all repair.
     */
    void ownerRepairPass() throws Exception {
        if (ownsAll.getAsBoolean()) {
            return;
        }
        reconcileLock.lock();
        try {
            Map<Integer, Records.NodeRecord> nodes = nodesById();
            Set<Integer> dead = new HashSet<>();
            for (Records.NodeRecord n : nodes.values()) {
                if (n.state() == Records.NodeState.DEAD) {
                    dead.add(n.nodeId());
                }
            }
            for (StrataNamespace ns : store.listNamespaces()) {
                if (!ownsNamespace.test(ns)) {
                    continue;
                }
                for (FileId fileId : store.listFiles(ns)) {
                    perFileIsolated(ns, fileId, "ownerRepairPass", () -> {
                        Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(ns, fileId);
                        if (opt.isEmpty()) {
                            return;
                        }
                        Records.FileRecord file = opt.get().value();
                        if (file.state() == FileState.DELETING) {
                            // the owner reclaims its own deleted files: the leader's heartbeat command channel
                            // cannot reach a non-leader-owned namespace's chunks, so without this they leak.
                            ownerDriveDeletion(file, opt.get().version(), nodes);
                            return;
                        }
                        for (Records.ChunkRecord chunk : file.chunks()) {
                            if (chunk.state() == ChunkState.SEALED) {
                                ownerRepairChunk(ns, file, chunk, dead, RepairTrigger.RECONCILE);
                            }
                        }
                    });
                }
            }
        } finally {
            reconcileLock.unlock();
        }
    }

    /* ---------- owner-pull verification (design §20.3): replaces the central inventory push ---------- */

    private void verifyLoop() {
        while (!closed.get()) {
            try {
                Thread.sleep(VERIFY_INTERVAL_MS);
                verifyPass();
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                if (!closed.get()) {
                    log.warn("verify pass failed", e);
                }
            }
        }
    }

    /**
     * Owner-pull verification (design §20.3): for each namespace this controller owns, ask every live
     * node that should hold a sealed chunk to report its local state, and compare against the descriptor
     * — missing/corrupt drops the replica so the under-replication scan re-replicates within the
     * namespace. Replaces "every node pushes its full chunk list to the leader". Runs off the repair
     * thread (the verify RPCs block); never holds the reconcile lock — the descriptor drops are
     * CAS-idempotent, exactly as the old push reconciliation was.
     */
    void verifyPass() throws Exception {
        if (isLeader.getAsBoolean()) {
            // A just-elected leader's node-liveness view is stale until nodes re-register; a missing
            // verdict in that window could falsely drop a healthy replica, so wait out the settle period.
            long since = leaderSince;
            if (since == 0
                    || System.currentTimeMillis() - since < (long) config.leaseMs() + config.deadGraceMs()) {
                return;
            }
        }
        long now = System.currentTimeMillis();
        Map<Integer, Records.NodeRecord> nodes = nodesById();
        for (StrataNamespace ns : store.listNamespaces()) {
            if (!ownsNamespace.test(ns)) {
                continue;
            }
            for (FileId fileId : store.listFiles(ns)) {
                perFileIsolated(ns, fileId, "verifyPass", () -> verifyFile(ns, fileId, nodes, now));
            }
        }
    }

    private void verifyFile(StrataNamespace ns, FileId fileId, Map<Integer, Records.NodeRecord> nodes, long now)
            throws Exception {
        Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(ns, fileId);
        if (opt.isEmpty() || opt.get().value().state() == FileState.DELETING) {
            return; // deletion, not verification, drives a DELETING file's chunks
        }
        Records.FileRecord file = opt.get().value();
        Map<Integer, List<ChunkId>> byNode = new java.util.LinkedHashMap<>();
        Map<ChunkId, Records.ChunkRecord> expected = new HashMap<>();
        for (Records.ChunkRecord c : file.chunks()) {
            if (c.state() != ChunkState.SEALED) {
                continue;
            }
            ChunkId chunkId = file.chunkId(c.index());
            expected.put(chunkId, c);
            for (int nodeId : c.replicas()) {
                // A dead node's replicas are healed by the node-death/reconcile path, not verify. Liveness
                // comes from the persisted snapshot, NOT registry.isDead: a non-leader namespace owner does
                // not receive heartbeats (they are leader-gated), so its in-memory registry is frozen at boot
                // and would report every post-boot-registered node as dead — silently skipping verification
                // of their replicas. The persisted snapshot is the leader-written authoritative view.
                if (!isPersistedLive(nodeId, nodes)) {
                    continue;
                }
                byNode.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(chunkId);
            }
        }
        for (var e : byNode.entrySet()) {
            Records.NodeRecord node = nodes.get(e.getKey());
            if (node == null) {
                continue;
            }
            List<ChunkId> ids = e.getValue();
            for (int i = 0; i < ids.size(); i += VERIFY_BATCH_SIZE) {
                List<ChunkId> batch = ids.subList(i, Math.min(i + VERIFY_BATCH_SIZE, ids.size()));
                List<Messages.VerifyChunkResult> results = execVerify(node, ns, batch);
                // Empty == the RPC could not reach the node: no verdict, so we never drop a replica on an
                // unreachable owner-verify (fail-safe). A truly dead node is handled by the reconcile scan.
                for (Messages.VerifyChunkResult r : results) {
                    Records.ChunkRecord exp = expected.get(r.chunkId());
                    if (exp != null) {
                        applyVerifyVerdict(ns, fileId, exp, e.getKey(), node, r, now, nodes);
                    }
                }
            }
        }
    }

    /** Synchronous VERIFY_CHUNKS to {@code node}; empty list on any RPC failure (treated as no verdict). */
    private List<Messages.VerifyChunkResult> execVerify(Records.NodeRecord node, StrataNamespace ns,
                                                        List<ChunkId> chunkIds) {
        Endpoint endpoint;
        try {
            endpoint = Endpoint.parse(node.endpoint(), "node endpoint", ErrorCode.INTERNAL);
        } catch (Exception e) {
            log.warn("owner-verify: bad endpoint '{}' for node {}: {}",
                    node.endpoint(), node.nodeId(), e.getMessage());
            return List.of();
        }
        try (ScpClient client = new ScpClient(endpoint.host(), endpoint.port(), ScpClient.KIND_TOOL, "owner-verify")) {
            ByteBuffer resp = client.call(Opcode.VERIFY_CHUNKS,
                    new Messages.VerifyChunks(ns, advertisedEndpoint, chunkIds).encode(), null,
                    config.repairCommandTimeoutMs());
            return Messages.VerifyChunksResp.decode(resp).results();
        } catch (Exception e) {
            log.debug("owner-verify to node {} failed: {}", node.nodeId(), e.getMessage());
            return List.of();
        }
    }

    /**
     * Compares one VERIFY_CHUNKS result against the descriptor and drops the replica when it is missing,
     * non-SEALED past grace, or corrupt — the owner-pull equivalent of the inventory-push reconciliation.
     * A freshly-repaired replica is protected from a stale verdict; corrupt/bad bytes are physically
     * deleted now so a re-pick of this node cannot read them (the under-replication scan re-replicates).
     */
    private void applyVerifyVerdict(StrataNamespace ns, FileId fileId, Records.ChunkRecord exp, int nodeId,
                                    Records.NodeRecord node, Messages.VerifyChunkResult r, long now,
                                    Map<Integer, Records.NodeRecord> nodes)
            throws Exception {
        ChunkId chunkId = r.chunkId();
        if (isRepairProtected(ns, chunkId, nodeId)) {
            return;
        }
        String key = nodeId + ":" + ns + ":" + chunkId;
        boolean healthy = r.present() && r.state() == ChunkState.SEALED
                && r.length() == exp.length() && r.crc() == exp.crc();
        // Last-live-replica guard (data-loss hardening): never let a verdict drop the only remaining
        // live replica of a chunk. A false-positive verdict — a transient miss or a bogus crc — must not
        // turn a recoverable degraded chunk into an unavailable one (0 live replicas). The reconcile scan
        // re-replicates once a healthy peer exists; until then keeping the (possibly bad) last copy
        // referenced is strictly safer than dropping it. The anomaly stays tracked, so the drop fires as
        // soon as another live replica exists. Liveness is judged from the persisted snapshot (not
        // registry.isDead), so a non-leader owner with a frozen in-memory registry does not miscount a
        // genuinely-DEAD peer as live and drop the last actually-live copy.
        if (!healthy && exp.replicas().stream().filter(n -> n != nodeId && isPersistedLive(n, nodes)).count() == 0) {
            log.warn("verify: keeping last live replica {} of chunk {} despite verdict "
                            + "(present={} state={}) — dropping it would leave 0 live replicas",
                    nodeId, chunkId, r.present(), r.state());
            return;
        }
        if (!r.present()) {
            if (replicaUnhealthyPastGrace(key, now)) {
                log.warn("verify: node {} missing sealed chunk {} (>= {}ms) — dropping replica for re-repair",
                        nodeId, chunkId, config.replicaMissingGraceMs());
                replicaMissingSince.remove(key);
                applyDeleteConfirmed(ns, fileId, chunkId, nodeId);
            }
        } else if (r.state() != ChunkState.SEALED) {
            if (replicaUnhealthyPastGrace(key, now)) {
                log.warn("verify: node {} holds {} copy of sealed chunk {} (>= {}ms) — dropping replica",
                        nodeId, r.state(), chunkId, config.replicaMissingGraceMs());
                replicaMissingSince.remove(key);
                applyDeleteConfirmed(ns, fileId, chunkId, nodeId);
                execDelete(node, chunkId, ns);
            }
        } else if (r.length() != exp.length() || r.crc() != exp.crc()) {
            replicaMissingSince.remove(key);
            log.warn("verify: node {} holds corrupt sealed chunk {} (len {}/{} crc {}/{}) — dropping replica",
                    nodeId, chunkId, r.length(), exp.length(), r.crc(), exp.crc());
            applyDeleteConfirmed(ns, fileId, chunkId, nodeId);
            execDelete(node, chunkId, ns);
        } else {
            replicaMissingSince.remove(key); // healthy attestation — clear any pending anomaly
        }
    }

    private void ownerRepairChunk(StrataNamespace ns, Records.FileRecord file, Records.ChunkRecord chunk,
                                  Set<Integer> dead, RepairTrigger trigger) {
        ChunkId chunkId = file.chunkId(chunk.index());
        if (chunksBeingRepaired.contains(new NsChunkId(ns, chunkId))) {
            return;
        }
        int deadNode = -1;
        List<Messages.Replica> sources = new ArrayList<>();
        Set<String> usedHosts = new HashSet<>();
        for (int nodeId : chunk.replicas()) {
            if (dead.contains(nodeId)) {
                deadNode = nodeId;
            } else {
                sources.add(registry.replicaOf(nodeId));
                String host = registry.hostOf(nodeId);
                if (host != null) {
                    usedHosts.add(host);
                }
            }
        }
        if (sources.isEmpty()) {
            return; // no live source to pull from
        }
        int liveReplicas = chunk.replicas().size() - (deadNode >= 0 ? 1 : 0);
        if (liveReplicas >= file.replicationFactor()) {
            return; // adequately replicated
        }
        List<NodeRegistry.LiveNode> targets;
        try {
            targets = Placement.choose(ns, registry, 1, new HashSet<>(chunk.replicas()), usedHosts);
        } catch (Exception e) {
            log.warn("no repair target for {} (owner repair): {}", chunkId, e.getMessage());
            return; // no placement capacity right now; a later pass retries
        }
        NodeRegistry.LiveNode target = targets.get(0);
        if (!chunksBeingRepaired.add(new NsChunkId(ns, chunkId))) {
            return;
        }
        try {
            long cmdId = commandIds.incrementAndGet();
            Messages.ReplicateCmd cmd = new Messages.ReplicateCmd(cmdId, chunkId, sources,
                    (byte) 1, chunk.crc(), chunk.length(), ns);
            if (execReplicate(target, cmd)
                    && applyOwnerRepair(ns, file.fileId(), chunkId, deadNode, target.record.nodeId())) {
                recordRepairIssued(trigger);
                log.info("owner repair: {} dead={} -> target={} (cmd {})", chunkId, deadNode,
                        target.record.nodeId(), cmdId);
            }
        } catch (Exception e) {
            log.warn("owner repair of {} failed", chunkId, e);
        } finally {
            chunksBeingRepaired.remove(new NsChunkId(ns, chunkId));
        }
    }

    @FunctionalInterface
    private interface NodeCall {
        boolean run(ScpClient client, int timeoutMs) throws Exception;
    }

    /**
     * Shared owner-direct transport: opens a one-shot tool connection to {@code node} and runs {@code call},
     * returning its result. Logs and returns false on a bad endpoint or any RPC failure. Both owner lanes —
     * repair ({@link #execReplicate}) and delete ({@link #execDelete}) — route through here so endpoint
     * parsing, timeout, connection lifecycle, and failure logging stay in one place.
     */
    private boolean directNodeCall(Records.NodeRecord node, ChunkId chunkId, String label, NodeCall call) {
        Endpoint endpoint;
        try {
            endpoint = Endpoint.parse(node.endpoint(), "node endpoint", ErrorCode.INTERNAL);
        } catch (Exception e) {
            log.warn("{}: bad endpoint '{}' for node {} (chunk {}): {}",
                    label, node.endpoint(), node.nodeId(), chunkId, e.getMessage());
            return false;
        }
        int timeoutMs = Math.max(30_000, config.repairCommandTimeoutMs());
        try (ScpClient client = new ScpClient(endpoint.host(), endpoint.port(), ScpClient.KIND_TOOL, label)) {
            return call.run(client, timeoutMs);
        } catch (Exception e) {
            log.warn("{} to node {} for {} failed: {}", label, node.nodeId(), chunkId, e.getMessage());
            return false;
        }
    }

    /** Synchronously tells {@code target} to pull the chunk (EXEC_REPLICATE); true if it acked OK. */
    private boolean execReplicate(NodeRegistry.LiveNode target, Messages.ReplicateCmd cmd) {
        BufWriter w = new BufWriter();
        Messages.Command.write(w, cmd);
        return directNodeCall(target.record, cmd.chunkId(), "owner-repair", (client, timeoutMs) -> {
            client.call(Opcode.EXEC_REPLICATE, w.toBytes(), null, timeoutMs);
            return true;
        });
    }

    /**
     * Synchronously tells {@code node} to physically delete {@code chunkId} via a direct DELETE_CHUNKS
     * call — the owner-direct deletion transport, mirroring {@link #execReplicate} for repair. A sharded
     * non-controller owner has no heartbeat command channel, so it deletes its own namespaces' chunks
     * this way. True if the node acked the chunk gone (deleted, or already absent).
     */
    private boolean execDelete(Records.NodeRecord node, ChunkId chunkId, io.strata.common.StrataNamespace ns) {
        return directNodeCall(node, chunkId, "owner-delete", (client, timeoutMs) -> {
            ByteBuffer resp = client.call(Opcode.DELETE_CHUNKS,
                    new Messages.DeleteChunks(List.of(chunkId), ns).encode(), null, timeoutMs);
            Messages.DeleteChunksResp r = Messages.DeleteChunksResp.decode(resp);
            short code = r.codes().isEmpty() ? ErrorCode.OK.code : r.codes().get(0);
            if (code != ErrorCode.OK.code && code != ErrorCode.CHUNK_NOT_FOUND.code) {
                log.warn("owner-delete of {} on node {} returned {} — will retry next pass",
                        chunkId, node.nodeId(), code);
            }
            return code == ErrorCode.OK.code || code == ErrorCode.CHUNK_NOT_FOUND.code;
        });
    }

    /**
     * Writes the owner-repair replica change: swap the dead replica for the target, or add the target.
     * Returns true if the descriptor swap landed; false if the file vanished or every CAS attempt lost
     * (so the caller does not log success or bump the repair metric for a swap that never committed).
     */
    private boolean applyOwnerRepair(StrataNamespace ns, FileId fileId, ChunkId chunkId,
                                      int deadNode, int targetNode) throws Exception {
        for (int attempt = 0; attempt < Controller.CAS_RETRIES; attempt++) {
            Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(ns, fileId);
            if (opt.isEmpty()) {
                return false;
            }
            Records.FileRecord file = opt.get().value();
            List<Records.ChunkRecord> chunks = new ArrayList<>();
            for (Records.ChunkRecord c : file.chunks()) {
                if (file.chunkId(c.index()).equals(chunkId)) {
                    List<Integer> replicas = new ArrayList<>(c.replicas());
                    if (deadNode >= 0) {
                        replicas.replaceAll(n -> n == deadNode ? targetNode : n);
                    } else if (!replicas.contains(targetNode)) {
                        replicas.add(targetNode);
                    }
                    chunks.add(c.withReplicas(replicas));
                } else {
                    chunks.add(c);
                }
            }
            if (store.updateFile(file.withChunks(chunks), opt.get().version())) {
                return true;
            }
        }
        log.warn("owner repair descriptor swap for {} kept failing CAS — next scan reconciles", chunkId);
        return false;
    }

    /**
     * Dispatches a just-deleted file's chunk deletions immediately, instead of waiting for the next
     * background scan (which is slow under heavy churn), so physical space is reclaimed promptly and the
     * disk stays bounded under sustained delete load. Synchronized with the scan so they don't race.
     */
    void driveDeletionNow(StrataNamespace namespace, FileId fileId) {
        reconcileLock.lock();
        try {
            Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(namespace, fileId);
            if (opt.isEmpty() || opt.get().value().state() != FileState.DELETING) {
                return;
            }
            MetadataStore.Versioned<Records.FileRecord> vf = opt.get();
            if (isLeader.getAsBoolean()) {
                driveDeletion(vf.value(), vf.version());
            } else {
                // a sharded non-leader owner has no heartbeat command channel to the data nodes,
                // so it reclaims its own namespace's chunks directly via DELETE_CHUNKS.
                ownerDriveDeletion(vf.value(), vf.version(), nodesById());
            }
        } catch (Exception e) {
            log.warn("prompt delete dispatch for {} failed — background scan will retry", fileId, e);
        } finally {
            reconcileLock.unlock();
        }
    }

    void driveDeletionSoon(StrataNamespace namespace, FileId fileId) {
        try {
            deleteDispatchExecutor.execute(() -> driveDeletionNow(namespace, fileId));
        } catch (RuntimeException e) {
            log.warn("prompt delete dispatch for {} was not scheduled — background scan will retry", fileId, e);
        }
    }

    private void driveDeletion(Records.FileRecord file, int version) throws Exception {
        StrataNamespace ns = file.namespace();
        for (Records.ChunkRecord chunk : file.chunks()) {
            ChunkId chunkId = file.chunkId(chunk.index());
            if (chunk.replicas().isEmpty()) {
                // A DELETING file can contain zero-replica chunks if all copies were already
                // lost while the file was still live. There is no node left to confirm the
                // physical delete, so let descriptor deletion converge immediately.
                applyDeleteConfirmed(ns, file.fileId(), chunkId, -1);
                continue;
            }
            for (int nodeId : chunk.replicas()) {
                if (registry.isDead(nodeId)) {
                    // a dead node's data is unreachable; its files vanish with the volume —
                    // drop the replica reference so deletion can converge
                    applyDeleteConfirmed(ns, file.fileId(), chunkId, nodeId);
                    continue;
                }
                boolean alreadyInflight = inflight.values().stream()
                        .anyMatch(a -> a instanceof DeleteAction d
                                && d.chunkId().equals(chunkId) && d.nodeId() == nodeId
                                && d.namespace().equals(ns));
                if (!alreadyInflight) {
                    long cmdId = commandIds.incrementAndGet();
                    inflight.put(cmdId, new DeleteAction(ns, file.fileId(), chunkId, nodeId,
                            System.currentTimeMillis()));
                    registry.enqueue(nodeId, new Messages.DeleteCmd(cmdId, List.of(chunkId), ns));
                }
            }
        }
        // chunked files converge via applyDeleteConfirmed (which drops the record once the last
        // replica confirms); only a chunkless file is deleted here, where `version` is still
        // valid because no mutation happened in this pass
        if (file.chunks().isEmpty()) {
            if (store.deleteFile(file.namespace(), file.fileId(), version)) {
                log.info("file {} fully deleted", file.fileId());
            }
        }
    }

    /**
     * Owner-direct physical deletion of a DELETING file's chunks: a sharded non-controller owner deletes
     * each replica via a direct DELETE_CHUNKS ({@link #execDelete}) — not the leader-only heartbeat command
     * channel — then converges the descriptor with {@link #applyDeleteConfirmed}. Synchronous, mirroring
     * {@link #driveDeletion}, just as {@code ownerRepairChunk} mirrors the leader's {@code issueReplicate}.
     * Without this, deleted files in non-leader-owned namespaces never have their physical chunks reclaimed.
     */
    private void ownerDriveDeletion(Records.FileRecord file, int version,
                                    Map<Integer, Records.NodeRecord> nodes) throws Exception {
        StrataNamespace ns = file.namespace();
        for (Records.ChunkRecord chunk : file.chunks()) {
            ChunkId chunkId = file.chunkId(chunk.index());
            if (chunk.replicas().isEmpty()) {
                applyDeleteConfirmed(ns, file.fileId(), chunkId, -1);
                continue;
            }
            for (int nodeId : chunk.replicas()) {
                Records.NodeRecord node = nodes.get(nodeId);
                if (node == null || node.state() == Records.NodeState.DEAD) {
                    // node gone/dead — its data is unreachable; drop the replica so deletion can converge
                    applyDeleteConfirmed(ns, file.fileId(), chunkId, nodeId);
                } else if (execDelete(node, chunkId, ns)) {
                    applyDeleteConfirmed(ns, file.fileId(), chunkId, nodeId);
                }
                // else: still present — a later ownerRepairPass retries
            }
        }
        // a chunkless DELETING file has no replica to confirm; delete the record directly (version still
        // valid since no chunk was mutated in this pass), matching driveDeletion.
        if (file.chunks().isEmpty() && store.deleteFile(file.namespace(), file.fileId(), version)) {
            log.info("file {} fully deleted (owner)", file.fileId());
        }
    }

    private Map<Integer, Records.NodeRecord> nodesById() throws Exception {
        Map<Integer, Records.NodeRecord> nodes = new HashMap<>();
        for (MetadataStore.Versioned<Records.NodeRecord> v : store.listNodes()) {
            nodes.put(v.value().nodeId(), v.value());
        }
        return nodes;
    }

    /**
     * Liveness as judged from the persisted node snapshot ({@link #nodesById()}), the leader-written
     * authoritative view. Used by the verify lane instead of {@code registry.isDead}: a non-leader namespace
     * owner does not receive heartbeats (leader-gated), so its in-memory registry is frozen at boot and would
     * misreport node liveness. A node is live iff it has a persisted record that is not DEAD (REGISTERED or
     * DRAINING both still hold their data). This mirrors the dead-set the owner-repair lane already derives.
     */
    private static boolean isPersistedLive(int nodeId, Map<Integer, Records.NodeRecord> nodes) {
        Records.NodeRecord n = nodes.get(nodeId);
        return n != null && n.state() != Records.NodeState.DEAD;
    }

    /**
     * Called from heartbeat handling with each node-reported command completion. The heartbeat request
     * thread only enqueues the work; descriptor CAS/delete-confirmation I/O runs on the completion worker.
     */
    CompletableFuture<Void> onCommandCompletedAsync(int reportingNode, long incMsb, long incLsb, long sessionEpoch,
                                                    Messages.CompletedCommand completion) {
        if (closed.get()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            completionExecutor.execute(() -> {
                try {
                    if (closed.get()) {
                        return;
                    }
                    if (!registry.isCurrentSession(reportingNode, incMsb, incLsb, sessionEpoch)) {
                        log.debug("ignoring cmd {} completion from stale or non-live node {}",
                                completion.commandId(), reportingNode);
                        return;
                    }
                    onCommandCompleted(reportingNode, completion);
                } catch (RuntimeException e) {
                    // onCommandCompleted guards its own apply logic, but an unchecked failure in the
                    // session check (or anywhere else here) would otherwise vanish into the executor's
                    // default handler. Log it instead; the barrier still releases via finally and the
                    // next background scan reconciles.
                    log.warn("completion {} failed — next scan reconciles", completion.commandId(), e);
                } finally {
                    done.complete(null);
                }
            });
        } catch (RuntimeException e) {
            if (!closed.get()) {
                log.warn("scheduling completion {} failed — next scan reconciles", completion.commandId(), e);
            }
            return CompletableFuture.completedFuture(null);
        }
        return done;
    }

    /** Applies a validated node-reported command completion. */
    void onCommandCompleted(int reportingNode, Messages.CompletedCommand completion) {
        Action action = inflight.get(completion.commandId());
        if (action == null) return;
        if (action.executingNode() != reportingNode) {
            log.warn("ignoring cmd {} completion from node {}; assigned executor is {}",
                    completion.commandId(), reportingNode, action.executingNode());
            return;
        }
        if (!inflight.remove(completion.commandId(), action)) return;
        try {
            if (action instanceof ReplicateAction r) {
                chunksBeingRepaired.remove(new NsChunkId(r.namespace(), r.chunkId()));
                if (completion.status() == 0) {
                    applyReplicaSwap(r);
                } else {
                    log.warn("replicate cmd {} for {} failed with {} — next scan retries",
                            completion.commandId(), r.chunkId(), completion.status());
                }
            } else if (action instanceof DeleteAction d) {
                if (completion.status() == 0) {
                    applyDeleteConfirmed(d.namespace(), d.fileId(), d.chunkId(), d.nodeId());
                } else {
                    log.warn("delete cmd {} for {} failed with {} — next scan retries",
                            completion.commandId(), d.chunkId(), completion.status());
                }
            }
        } catch (Exception e) {
            log.warn("applying completion {} failed — next scan reconciles", completion.commandId(), e);
        }
    }

    private void applyReplicaSwap(ReplicateAction r) throws Exception {
        if (registry.isDead(r.targetNode())) {
            // target completed the copy but died before the swap landed: writing a dead node
            // into the descriptor would hand readers a bad replica — skip; the next scan
            // re-repairs (and the dead target's copy becomes an orphan)
            log.warn("repair target {} for {} died before descriptor swap — next scan retries",
                    r.targetNode(), r.chunkId());
            return;
        }
        for (int attempt = 0; attempt < Controller.CAS_RETRIES; attempt++) {
            Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(r.namespace(), r.fileId());
            if (opt.isEmpty()) return;
            Records.FileRecord file = opt.get().value();
            if (file.state() == FileState.DELETING) {
                return;
            }
            List<Records.ChunkRecord> chunks = new ArrayList<>(file.chunks());
            boolean changed = false;
            for (int i = 0; i < chunks.size(); i++) {
                Records.ChunkRecord c = chunks.get(i);
                if (!file.chunkId(c.index()).equals(r.chunkId()) || c.replicas().contains(r.targetNode())) {
                    continue;
                }
                if (r.deadNode() >= 0 && c.replicas().contains(r.deadNode())) {
                    chunks.set(i, c.withReplicaSwapped(r.deadNode(), r.targetNode()));
                    changed = true;
                } else if (r.deadNode() < 0 && c.replicas().size() < file.replicationFactor()) {
                    List<Integer> grown = new ArrayList<>(c.replicas());
                    grown.add(r.targetNode());
                    chunks.set(i, c.withReplicas(grown));
                    changed = true;
                }
            }
            if (!changed) return;
            Records.FileRecord updated = file.withChunks(chunks);
            if (store.updateFile(updated, opt.get().version())) {
                recentlyCommittedReplicas.put(new ReplicaKey(r.namespace(), r.chunkId(), r.targetNode()),
                        System.currentTimeMillis());
                registry.removePending(r.targetNode(), command ->
                        command instanceof Messages.DeleteCmd d
                                && d.chunkIds().contains(r.chunkId())
                                && !inflight.containsKey(d.commandId()));
                log.info("descriptor swap: {} {} -> {}", r.chunkId(), r.deadNode(), r.targetNode());
                return;
            }
        }
        log.warn("descriptor swap for {} kept failing CAS — next scan reconciles", r.chunkId());
    }

    private void applyDeleteConfirmed(StrataNamespace namespace, FileId fileId, ChunkId chunkId,
                                      int nodeId) throws Exception {
        for (int attempt = 0; attempt < Controller.CAS_RETRIES; attempt++) {
            Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(namespace, fileId);
            if (opt.isEmpty()) return;
            Records.FileRecord file = opt.get().value();
            List<Records.ChunkRecord> chunks = new ArrayList<>();
            for (Records.ChunkRecord c : file.chunks()) {
                if (file.chunkId(c.index()).equals(chunkId)) {
                    List<Integer> replicas = new ArrayList<>(c.replicas());
                    replicas.remove(Integer.valueOf(nodeId));
                    if (!replicas.isEmpty() || file.state() != FileState.DELETING) {
                        // a LIVE file keeps the chunk record even with zero replicas: erasing it
                        // would silently shorten the file (readers' offset accounting shifts) —
                        // total loss must surface as a hard read failure, not missing data
                        if (replicas.isEmpty()) {
                            log.error("chunk {} of live file {} has lost ALL replicas — readers "
                                    + "will hard-fail until operator intervention", chunkId, fileId);
                        }
                        chunks.add(c.withReplicas(replicas));
                    } // empty AND deleting -> chunk record dropped
                } else {
                    chunks.add(c);
                }
            }
            Records.FileRecord updated = file.withChunks(chunks);
            if (chunks.isEmpty() && file.state() == FileState.DELETING) {
                if (store.deleteFile(namespace, fileId, opt.get().version())) {
                    log.info("file {} fully deleted", fileId);
                    return;
                }
                continue;
            }
            if (store.updateFile(updated, opt.get().version())) return;
        }
        log.warn("delete-confirm descriptor update for {} kept failing CAS — next scan reconciles", chunkId);
    }


    private boolean replicaUnhealthyPastGrace(String key, long now) {
        Long firstUnhealthy = replicaMissingSince.putIfAbsent(key, now);
        long unhealthyForMs = firstUnhealthy == null ? 0 : now - firstUnhealthy;
        return unhealthyForMs >= config.replicaMissingGraceMs();
    }

    private boolean isRepairProtected(StrataNamespace namespace, ChunkId chunkId, int nodeId) {
        if (isRepairInFlightTo(namespace, chunkId, nodeId)) {
            return true;
        }
        ReplicaKey key = new ReplicaKey(namespace, chunkId, nodeId);
        Long committedAt = recentlyCommittedReplicas.get(key);
        if (committedAt == null) {
            return false;
        }
        long graceMs = verifyStalenessGraceMs();
        if (System.currentTimeMillis() - committedAt <= graceMs) {
            return true;
        }
        recentlyCommittedReplicas.remove(key, committedAt);
        return false;
    }

    private boolean isRepairInFlightTo(StrataNamespace namespace, ChunkId chunkId, int nodeId) {
        for (Action action : inflight.values()) {
            if (action instanceof ReplicateAction r
                    && r.targetNode() == nodeId
                    && r.namespace().equals(namespace)
                    && r.chunkId().equals(chunkId)) {
                return true;
            }
        }
        return false;
    }

    private long verifyStalenessGraceMs() {
        return Math.max(config.repairCommandTimeoutMs(), config.leaseMs() + config.deadGraceMs());
    }

    // Cache key that pairs namespace + fileId for unambiguous per-namespace lookup.
    private record NsFileKey(StrataNamespace namespace, FileId fileId) {}

    @Override
    public void close() {
        closed.set(true);
        for (Thread t : new Thread[] {scanThread, verifyThread}) {
            if (t != null) {
                t.interrupt();
                try {
                    t.join(2_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        deleteDispatchExecutor.shutdown();
        completionExecutor.shutdown();
        repairEventExecutor.shutdown();
        awaitTermination(deleteDispatchExecutor);
        awaitTermination(completionExecutor);
        awaitTermination(repairEventExecutor);
    }

    private static void awaitTermination(ExecutorService executor) {
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
