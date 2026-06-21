package io.strata.meta;

import io.strata.common.Endpoint;
import io.strata.common.ErrorCode;
import io.strata.common.FileState;
import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.proto.BufWriter;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

    private record ReplicateAction(FileId fileId, ChunkId chunkId, int deadNode, int targetNode,
                                   long issuedAtMs) implements Action {
        @Override
        public int executingNode() {
            return targetNode;
        }
    }

    private record DeleteAction(FileId fileId, ChunkId chunkId, int nodeId, long issuedAtMs)
            implements Action {
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
    // (nodeId:chunkId) -> first time the node's inventory reported a sealed replica unhealthy
    // (missing or still locally OPEN/DELETING). A just-sealed chunk can race with an in-flight
    // inventory snapshot; only drop the replica if it stays unhealthy past the grace, so churn can't
    // falsely remove every good replica and trigger a re-replication storm.
    private final Map<String, Long> replicaMissingSince = new ConcurrentHashMap<>();
    private final Set<ChunkId> chunksBeingRepaired = ConcurrentHashMap.newKeySet();
    private final Map<ReplicaKey, Long> recentlyCommittedReplicas = new ConcurrentHashMap<>();
    // Monotonic count of repairs actually issued, split by trigger lane (event vs reconcile). Bumped
    // exactly once at the point a REPLICATE is enqueued / an owner EXEC_REPLICATE is committed, after
    // the chunksBeingRepaired dedup add succeeds — never on a dedup-skip, placement miss, or failure.
    private final AtomicLong eventRepairs = new AtomicLong();
    private final AtomicLong reconcileRepairs = new AtomicLong();
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

    /** Bumps the counter for {@code trigger}'s lane — called once per repair actually issued. */
    private void recordRepairIssued(RepairTrigger trigger) {
        (trigger == RepairTrigger.EVENT ? eventRepairs : reconcileRepairs).incrementAndGet();
    }

    private record ReplicaKey(ChunkId chunkId, int nodeId) {}

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
    }

    private long leaderSince;

    private void scanLoop() {
        long lastReconcile = 0;
        while (!closed.get()) {
            try {
                Thread.sleep(config.repairScanIntervalMs());
                // Lane A — cheap in-memory housekeeping every fast interval (leader-gated inside).
                tick();
                // Lane C — full backstop pass on the slow cadence. Until the event path lands, repair
                // correctness still rests entirely on this (now slower) reconcile.
                long now = System.currentTimeMillis();
                if (now - lastReconcile >= config.reconcileIntervalMs()) {
                    reconcile();
                    // Advance the cadence clock only after a reconcile that actually ran to completion:
                    // if reconcile() throws (e.g. a transient store outage), the backstop did no work, so
                    // the next fast tick retries it promptly instead of waiting a whole reconcile interval.
                    lastReconcile = now;
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
                Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(fileId);
                if (opt.isEmpty()) {
                    continue;
                }
                repairFileChunksOnNode(fileId, opt.get().value(), deadNodeId);
            }
        }
    }

    /**
     * Issues repair for every sealed, under-replicated chunk of {@code file} that still references
     * {@code deadNodeId}. Shared by the event path; {@link #issueReplicate} performs the placement,
     * the {@code chunksBeingRepaired} dedup, and the command enqueue exactly as the reconcile does —
     * this method only narrows the candidate set to chunks affected by the dead node.
     */
    private void repairFileChunksOnNode(FileId fileId, Records.FileRecord file, int deadNodeId) {
        if (file.state() == FileState.DELETING) {
            return; // deletion, not repair, drives a DELETING file's chunks
        }
        for (Records.ChunkRecord chunk : file.chunks()) {
            if (chunk.state() != ChunkState.SEALED || !chunk.replicas().contains(deadNodeId)) {
                continue;
            }
            ChunkId chunkId = file.chunkId(chunk.index());
            if (chunksBeingRepaired.contains(chunkId)) {
                continue;
            }
            int live = 0;
            for (int nodeId : chunk.replicas()) {
                if (!registry.isDead(nodeId)) live++;
            }
            if (live < file.replicationFactor() && live > 0) {
                issueReplicate(fileId, file, chunk, RepairTrigger.EVENT);
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
        store.sweepDeletedFiles(DELETED_TOMBSTONE_TTL_MS);
    }

    /**
     * Live file ids across every namespace. Repair reconciles the whole cluster, not one tenant, so it
     * composes the per-namespace listing — there is no global flat file enumeration in the store.
     */
    private List<FileId> allFileIds() throws Exception {
        List<FileId> out = new ArrayList<>();
        for (var ns : store.listNamespaces()) {
            out.addAll(store.listFiles(ns));
        }
        return out;
    }

    /** One reconciliation pass over all files. Idempotent; safe to call while serving. */
    synchronized void scanOnce() throws Exception {
        // The controller refreshes the shared live-node snapshot each pass so non-controller namespace
        // owners have a current placement view (design §11). Leader-gated: a standby must not publish.
        if (isLeader.getAsBoolean()) {
            registry.publishClusterLiveNodes();
        }
        sweepStuckCommands();
        record Repair(FileId fileId, Records.FileRecord file, Records.ChunkRecord chunk, int liveReplicas) {}
        List<Repair> repairs = new ArrayList<>();
        int under = 0, unavailable = 0, atMin = 0; // durability census, published to gauges at the end

        for (FileId fileId : allFileIds()) {
            Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(fileId);
            if (opt.isEmpty()) continue;
            Records.FileRecord file = opt.get().value();

            if (file.state() == FileState.DELETING) {
                driveDeletion(file, opt.get().version());
                continue;
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
                    unavailable++;
                } else if (live < file.replicationFactor()) {
                    under++;
                    if (live == 1) atMin++;
                }
                if (chunksBeingRepaired.contains(chunkId)) continue;
                if (live < file.replicationFactor() && live > 0) {
                    repairs.add(new Repair(fileId, file, chunk, live));
                } else if (live == 0) {
                    log.error("chunk {} has NO live replicas — data loss exposure, cannot repair", chunkId);
                }
            }
        }
        underReplicatedChunks = under;
        unavailableChunks = unavailable;
        chunksAtMinRedundancy = atMin;

        // exposure priority: fewest live replicas first (tech design §7.2)
        repairs.sort(java.util.Comparator.comparingInt(Repair::liveReplicas));
        for (Repair r : repairs) {
            issueReplicate(r.fileId(), r.file(), r.chunk(), RepairTrigger.RECONCILE);
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
                chunksBeingRepaired.remove(r.chunkId());
                log.warn("repair cmd {} for {} abandoned (target {} {}) — will re-issue",
                        e.getKey(), r.chunkId(), r.targetNode(), dead ? "dead" : "timed out");
            }
        }
    }

    private void issueReplicate(FileId fileId, Records.FileRecord file, Records.ChunkRecord chunk,
                                RepairTrigger trigger) {
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
            targets = Placement.choose(file.namespace(), registry, 1, existing, usedHosts);
        } catch (Exception e) {
            log.warn("no repair target for {}: {}", chunkId, e.getMessage());
            return;
        }
        NodeRegistry.LiveNode target = targets.get(0);
        if (!chunksBeingRepaired.add(chunkId)) {
            return;
        }
        long cmdId = commandIds.incrementAndGet();
        inflight.put(cmdId, new ReplicateAction(fileId, chunkId, deadNode, target.record.nodeId(),
                System.currentTimeMillis()));
        registry.enqueue(target.record.nodeId(),
                new Messages.ReplicateCmd(cmdId, chunkId, sources, (byte) 1, chunk.crc(), chunk.length()));
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
        Set<Integer> dead = new HashSet<>();
        for (MetadataStore.Versioned<Records.NodeRecord> v : store.listNodes()) {
            if (v.value().state() == Records.NodeState.DEAD) {
                dead.add(v.value().nodeId());
            }
        }
        for (StrataNamespace ns : store.listNamespaces()) {
            if (!ownsNamespace.test(ns)) {
                continue;
            }
            for (FileId fileId : store.listFiles(ns)) {
                Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(fileId);
                if (opt.isEmpty() || opt.get().value().state() == FileState.DELETING) {
                    continue;
                }
                Records.FileRecord file = opt.get().value();
                for (Records.ChunkRecord chunk : file.chunks()) {
                    if (chunk.state() == ChunkState.SEALED) {
                        ownerRepairChunk(ns, file, chunk, dead, RepairTrigger.RECONCILE);
                    }
                }
            }
        }
    }

    private void ownerRepairChunk(StrataNamespace ns, Records.FileRecord file, Records.ChunkRecord chunk,
                                  Set<Integer> dead, RepairTrigger trigger) {
        ChunkId chunkId = file.chunkId(chunk.index());
        if (chunksBeingRepaired.contains(chunkId)) {
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
            return; // no placement capacity right now; a later pass retries
        }
        NodeRegistry.LiveNode target = targets.get(0);
        if (!chunksBeingRepaired.add(chunkId)) {
            return;
        }
        try {
            long cmdId = commandIds.incrementAndGet();
            Messages.ReplicateCmd cmd = new Messages.ReplicateCmd(cmdId, chunkId, sources,
                    (byte) 1, chunk.crc(), chunk.length());
            if (execReplicate(target, cmd)) {
                applyOwnerRepair(file.fileId(), chunkId, deadNode, target.record.nodeId());
                recordRepairIssued(trigger);
                log.info("owner repair: {} dead={} -> target={} (cmd {})", chunkId, deadNode,
                        target.record.nodeId(), cmdId);
            }
        } catch (Exception e) {
            log.warn("owner repair of {} failed", chunkId, e);
        } finally {
            chunksBeingRepaired.remove(chunkId);
        }
    }

    /** Synchronously tells {@code target} to pull the chunk (EXEC_REPLICATE); true if it acked OK. */
    private boolean execReplicate(NodeRegistry.LiveNode target, Messages.ReplicateCmd cmd) {
        Endpoint endpoint;
        try {
            endpoint = Endpoint.parse(target.record.endpoint(), "node endpoint", ErrorCode.INTERNAL);
        } catch (Exception e) {
            return false;
        }
        BufWriter w = new BufWriter();
        Messages.Command.write(w, cmd);
        int timeoutMs = Math.max(30_000, config.repairCommandTimeoutMs());
        try (ScpClient client = new ScpClient(endpoint.host(), endpoint.port(),
                ScpClient.KIND_TOOL, "owner-repair")) {
            client.call(Opcode.EXEC_REPLICATE, w.toBytes(), null, timeoutMs);
            return true;
        } catch (Exception e) {
            log.warn("EXEC_REPLICATE to node {} for {} failed: {}", target.record.nodeId(),
                    cmd.chunkId(), e.getMessage());
            return false;
        }
    }

    /** Writes the owner-repair replica change: swap the dead replica for the target, or add the target. */
    private void applyOwnerRepair(FileId fileId, ChunkId chunkId, int deadNode, int targetNode)
            throws Exception {
        for (int attempt = 0; attempt < Controller.CAS_RETRIES; attempt++) {
            Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(fileId);
            if (opt.isEmpty()) {
                return;
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
                return;
            }
        }
    }

    /**
     * Dispatches a just-deleted file's chunk deletions immediately, instead of waiting for the next
     * background scan (which is slow under heavy churn), so physical space is reclaimed promptly and the
     * disk stays bounded under sustained delete load. Synchronized with the scan so they don't race.
     */
    synchronized void driveDeletionNow(FileId fileId) {
        try {
            Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(fileId);
            if (opt.isPresent() && opt.get().value().state() == FileState.DELETING) {
                driveDeletion(opt.get().value(), opt.get().version());
            }
        } catch (Exception e) {
            log.warn("prompt delete dispatch for {} failed — background scan will retry", fileId, e);
        }
    }

    void driveDeletionSoon(FileId fileId) {
        try {
            deleteDispatchExecutor.execute(() -> driveDeletionNow(fileId));
        } catch (RuntimeException e) {
            log.warn("prompt delete dispatch for {} was not scheduled — background scan will retry", fileId, e);
        }
    }

    private void driveDeletion(Records.FileRecord file, int version) throws Exception {
        for (Records.ChunkRecord chunk : file.chunks()) {
            ChunkId chunkId = file.chunkId(chunk.index());
            if (chunk.replicas().isEmpty()) {
                // A DELETING file can contain zero-replica chunks if all copies were already
                // lost while the file was still live. There is no node left to confirm the
                // physical delete, so let descriptor deletion converge immediately.
                applyDeleteConfirmed(file.fileId(), chunkId, -1);
                continue;
            }
            for (int nodeId : chunk.replicas()) {
                if (registry.isDead(nodeId)) {
                    // a dead node's data is unreachable; its files vanish with the volume —
                    // drop the replica reference so deletion can converge
                    applyDeleteConfirmed(file.fileId(), chunkId, nodeId);
                    continue;
                }
                boolean alreadyInflight = inflight.values().stream()
                        .anyMatch(a -> a instanceof DeleteAction d
                                && d.chunkId().equals(chunkId) && d.nodeId() == nodeId);
                if (!alreadyInflight) {
                    long cmdId = commandIds.incrementAndGet();
                    inflight.put(cmdId, new DeleteAction(file.fileId(), chunkId, nodeId,
                            System.currentTimeMillis()));
                    registry.enqueue(nodeId, new Messages.DeleteCmd(cmdId, List.of(chunkId)));
                }
            }
        }
        // chunked files converge via applyDeleteConfirmed (which drops the record once the last
        // replica confirms); only a chunkless file is deleted here, where `version` is still
        // valid because no mutation happened in this pass
        if (file.chunks().isEmpty()) {
            if (store.deleteFile(file.fileId(), version)) {
                log.info("file {} fully deleted", file.fileId());
            }
        }
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
                chunksBeingRepaired.remove(r.chunkId());
                if (completion.status() == 0) {
                    applyReplicaSwap(r);
                } else {
                    log.warn("replicate cmd {} for {} failed with {} — next scan retries",
                            completion.commandId(), r.chunkId(), completion.status());
                }
            } else if (action instanceof DeleteAction d) {
                if (completion.status() == 0) {
                    applyDeleteConfirmed(d.fileId(), d.chunkId(), d.nodeId());
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
            Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(r.fileId());
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
                recentlyCommittedReplicas.put(new ReplicaKey(r.chunkId(), r.targetNode()),
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

    private void applyDeleteConfirmed(FileId fileId, ChunkId chunkId, int nodeId) throws Exception {
        for (int attempt = 0; attempt < Controller.CAS_RETRIES; attempt++) {
            Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(fileId);
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
                if (store.deleteFile(fileId, opt.get().version())) {
                    log.info("file {} fully deleted", fileId);
                    return;
                }
                continue;
            }
            if (store.updateFile(updated, opt.get().version())) return;
        }
    }

    /**
     * Inventory reconciliation (tech design §9.2): orphans (on disk, not in the map) are deleted —
     * safe because of commit-before-write; expected-but-missing sealed replicas are dropped from
     * the descriptor so the under-replication scan repairs them.
     */
    void onInventory(Messages.InventoryReport report) {
        try {
            if (!registry.isCurrentSession(report.nodeId(), report.incMsb(), report.incLsb(),
                    report.sessionEpoch())) {
                // Inventory is destructive: a missing/corrupt report can drop replicas from file
                // descriptors. Only accept it from the currently leased incarnation/session;
                // stale or forged reports must not erase healthy replicas.
                log.warn("ignoring inventory report from stale or non-live node {}", report.nodeId());
                return;
            }
            long now = System.currentTimeMillis();
            Map<ChunkId, Messages.InventoryEntry> reported = new java.util.HashMap<>();
            for (Messages.InventoryEntry e : report.entries()) {
                reported.put(e.chunkId(), e);
            }
            // one descriptor fetch per file for the whole report: chunks of the same file would
            // otherwise re-fetch the identical record, and the missing/corrupt sweep below would
            // fetch every file a second time (reconciliation already tolerates a stale snapshot)
            Map<FileId, Optional<MetadataStore.Versioned<Records.FileRecord>>> records = new java.util.HashMap<>();
            // orphan detection
            List<ChunkId> orphans = new ArrayList<>();
            for (Messages.InventoryEntry e : report.entries()) {
                Optional<MetadataStore.Versioned<Records.FileRecord>> opt = cachedFile(records, e.chunkId().fileId());
                boolean known = false;
                if (opt.isPresent()) {
                    for (Records.ChunkRecord c : opt.get().value().chunks()) {
                        if (c.index() == e.chunkId().index() && c.replicas().contains(report.nodeId())) {
                            known = true;
                            break;
                        }
                    }
                }
                // Orphan only when this node can authoritatively account for the chunk's file: either
                // it owns every namespace (non-sharded), or the file IS present (so the descriptor just
                // does not reference this replica). A not-found file under sharding may belong to a
                // namespace owned by another controller node — deleting it would destroy that owner's data.
                if (!known && !isRepairProtected(e.chunkId(), report.nodeId())
                        && (ownsAll.getAsBoolean() || opt.isPresent())) {
                    orphans.add(e.chunkId());
                }
            }
            if (!orphans.isEmpty()) {
                long cmdId = commandIds.incrementAndGet();
                // no inflight action needed: orphan deletion has no descriptor effect
                registry.enqueue(report.nodeId(), new Messages.DeleteCmd(cmdId, orphans));
                log.info("node {}: {} orphan chunk(s) scheduled for deletion", report.nodeId(), orphans.size());
            }
            // missing or corrupt detection: a sealed chunk this node should hold must be reported
            // with the descriptor's exact length and crc — right id with wrong bytes is corruption
            // and the replica must stop being a read/repair source (the under-replication scan
            // then re-repairs, and the dropped node's copy becomes an orphan to delete)
            for (FileId fileId : allFileIds()) {
                Optional<MetadataStore.Versioned<Records.FileRecord>> opt = cachedFile(records, fileId);
                if (opt.isEmpty() || opt.get().value().state() == FileState.DELETING) continue;
                Records.FileRecord file = opt.get().value();
                for (Records.ChunkRecord c : file.chunks()) {
                    if (c.state() != ChunkState.SEALED || !c.replicas().contains(report.nodeId())) {
                        continue;
                    }
                    ChunkId chunkId = file.chunkId(c.index());
                    if (isRepairProtected(chunkId, report.nodeId())) {
                        continue;
                    }
                    String missKey = report.nodeId() + ":" + chunkId;
                    Messages.InventoryEntry entry = reported.get(chunkId);
                    if (entry == null) {
                        // A node can omit a just-sealed chunk from an in-flight inventory snapshot.
                        if (replicaUnhealthyPastGrace(missKey, now)) {
                            log.warn("node {} lost sealed chunk {} (missing >= {}ms) — dropping replica for re-repair",
                                    report.nodeId(), chunkId, config.replicaMissingGraceMs());
                            replicaMissingSince.remove(missKey);
                            applyDeleteConfirmed(fileId, chunkId, report.nodeId());
                        }
                    } else {
                        if (entry.state() != ChunkState.SEALED) {
                            // A just-sealed chunk can still appear OPEN in an inventory snapshot that
                            // started before local seal completed. Give it the same grace as a missing
                            // report; persistent non-SEALED state is then treated like corruption.
                            if (replicaUnhealthyPastGrace(missKey, now)) {
                                log.warn("node {} holds {} copy of sealed chunk {} for >= {}ms — dropping replica "
                                                + "for re-repair", report.nodeId(), entry.state(), chunkId,
                                        config.replicaMissingGraceMs());
                                replicaMissingSince.remove(missKey);
                                applyDeleteConfirmed(fileId, chunkId, report.nodeId());
                                registry.enqueue(report.nodeId(), new Messages.DeleteCmd(
                                        commandIds.incrementAndGet(), List.of(chunkId)));
                            }
                        } else if (entry.length() != c.length() || entry.crc() != c.crc()) {
                            replicaMissingSince.remove(missKey); // definitely reported; this copy is corrupt
                            log.warn("node {} holds corrupt sealed chunk {} (len {}/{} crc {}/{}) — "
                                            + "dropping replica for re-repair", report.nodeId(), chunkId,
                                    entry.length(), c.length(), entry.crc(), c.crc());
                            applyDeleteConfirmed(fileId, chunkId, report.nodeId());
                            // schedule physical deletion NOW: placement may legitimately re-pick this
                            // node as the add-repair target, and the corrupt bytes must be gone first
                            // (FIFO command delivery guarantees delete-before-replicate)
                            registry.enqueue(report.nodeId(), new Messages.DeleteCmd(
                                    commandIds.incrementAndGet(), List.of(chunkId)));
                        } else {
                            replicaMissingSince.remove(missKey); // healthy report — clear any pending anomaly
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("inventory reconciliation for node {} failed", report.nodeId(), e);
        }
    }

    private boolean replicaUnhealthyPastGrace(String key, long now) {
        Long firstUnhealthy = replicaMissingSince.putIfAbsent(key, now);
        long unhealthyForMs = firstUnhealthy == null ? 0 : now - firstUnhealthy;
        return unhealthyForMs >= config.replicaMissingGraceMs();
    }

    private boolean isRepairProtected(ChunkId chunkId, int nodeId) {
        if (isRepairInFlightTo(chunkId, nodeId)) {
            return true;
        }
        ReplicaKey key = new ReplicaKey(chunkId, nodeId);
        Long committedAt = recentlyCommittedReplicas.get(key);
        if (committedAt == null) {
            return false;
        }
        long graceMs = inventoryStalenessGraceMs();
        if (System.currentTimeMillis() - committedAt <= graceMs) {
            return true;
        }
        recentlyCommittedReplicas.remove(key, committedAt);
        return false;
    }

    private boolean isRepairInFlightTo(ChunkId chunkId, int nodeId) {
        for (Action action : inflight.values()) {
            if (action instanceof ReplicateAction r
                    && r.targetNode() == nodeId
                    && r.chunkId().equals(chunkId)) {
                return true;
            }
        }
        return false;
    }

    private long inventoryStalenessGraceMs() {
        return Math.max(config.repairCommandTimeoutMs(), config.leaseMs() + config.deadGraceMs());
    }

    private Optional<MetadataStore.Versioned<Records.FileRecord>> cachedFile(
            Map<FileId, Optional<MetadataStore.Versioned<Records.FileRecord>>> cache, FileId fileId) throws Exception {
        Optional<MetadataStore.Versioned<Records.FileRecord>> cached = cache.get(fileId);
        if (cached == null) {
            cached = store.getFile(fileId);
            cache.put(fileId, cached);
        }
        return cached;
    }

    @Override
    public void close() {
        closed.set(true);
        Thread t = scanThread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
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
