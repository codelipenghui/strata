package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.Endpoint;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.NsChunkId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.proto.BufWriter;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

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

    record ReplicateAction(StrataNamespace namespace, FileId fileId, ChunkId chunkId,
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
    private final BooleanSupplier isLeader;
    // Whether this node owns every namespace (non-sharded / single-endpoint). In sharded mode this scopes
    // cluster-leader targeted repair to namespaces this node owns (plus the system namespace); all-owner
    // deployments skip the separate non-controller owner-repair pass (tech design §4.4 and §7.2).
    private final BooleanSupplier ownsAll;
    // Whether this node is the controller owner of a namespace — scopes the non-controller owner repair pass.
    private final Predicate<StrataNamespace> ownsNamespace;
    // Real per-namespace leadership state when the namespace-log backend is active; null for root/test stores.
    private final NamespaceLeadership namespaceLeadership;
    private final ConcurrentHashMap<StrataNamespace, ReentrantLock> fallbackReconcileLocks = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong commandIds = new AtomicLong(System.currentTimeMillis());
    private final Map<Long, Action> inflight = new ConcurrentHashMap<>();
    private final Set<StrataNamespace> zeroOwnerEpochWarned = ConcurrentHashMap.newKeySet();
    private final Set<StrataNamespace> authorityRevalidationWarned = ConcurrentHashMap.newKeySet();
    private final Object globalOwnerEpochLock = new Object();
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
    // A verify verdict drops the descriptor before asking the node to quarantine the local bytes. Keep the
    // target out of repair placement during that CAS + RPC window so a fresh import cannot race the quarantine.
    private final Set<ReplicaKey> verifyQuarantinesInFlight = ConcurrentHashMap.newKeySet();
    // Monotonic count of repairs actually issued, split by trigger lane (event vs reconcile). Bumped
    // exactly once at the point a REPLICATE is enqueued / an owner EXEC_REPLICATE is committed, after
    // the chunksBeingRepaired dedup add succeeds — never on a dedup-skip, placement miss, or failure.
    private final AtomicLong eventRepairs = new AtomicLong();
    private final AtomicLong reconcileRepairs = new AtomicLong();
    private final AtomicLong reconcileSkippedFiles = new AtomicLong();
    private final AtomicLong authorityRevalidationSkips = new AtomicLong();
    private final AtomicLong verifyNoMatchBreaks = new AtomicLong();
    private volatile Thread scanThread;

    // Deleted-tombstone TTL is now sourced from config.deletedTombstoneTtlMs() (default 600 000 ms).

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

    /** Destructive namespace passes skipped because consensus authority could not be established. */
    long authorityRevalidationSkips() {
        return authorityRevalidationSkips.get();
    }

    /** Verify chunks withheld because no returned/rescued replica matched the descriptor — monotonic. */
    long verifyNoMatchBreaks() {
        return verifyNoMatchBreaks.get();
    }

    /** Bumps the counter for {@code trigger}'s lane — called once per repair actually issued. */
    private void recordRepairIssued(RepairTrigger trigger) {
        (trigger == RepairTrigger.EVENT ? eventRepairs : reconcileRepairs).incrementAndGet();
    }

    private record ReplicaKey(StrataNamespace namespace, ChunkId chunkId, int nodeId) {}

    private record VerifyVerdict(int nodeId, Records.NodeRecord node, Messages.VerifyChunkResult result) {}

    RepairCoordinator(MetadataStore store, NodeRegistry registry, ControllerConfig config,
                      BooleanSupplier isLeader) {
        this(store, registry, config, isLeader, () -> true, ns -> true);
    }

    RepairCoordinator(MetadataStore store, NodeRegistry registry, ControllerConfig config,
                      BooleanSupplier isLeader,
                      BooleanSupplier ownsAllNamespaces) {
        this(store, registry, config, isLeader, ownsAllNamespaces, ns -> true);
    }

    RepairCoordinator(MetadataStore store, NodeRegistry registry, ControllerConfig config,
                      BooleanSupplier isLeader,
                      BooleanSupplier ownsAllNamespaces,
                      Predicate<StrataNamespace> ownsNamespace) {
        this(store, registry, config, isLeader, ownsAllNamespaces, ownsNamespace, null);
    }

    RepairCoordinator(MetadataStore store, NodeRegistry registry, ControllerConfig config,
                      BooleanSupplier isLeader,
                      BooleanSupplier ownsAllNamespaces,
                      Predicate<StrataNamespace> ownsNamespace,
                      NamespaceLeadership namespaceLeadership) {
        this.store = store;
        this.registry = registry;
        this.config = config;
        this.isLeader = isLeader;
        this.ownsAll = ownsAllNamespaces;
        this.ownsNamespace = ownsNamespace;
        this.namespaceLeadership = namespaceLeadership;
        this.systemVerifyIntervalMs = config.systemVerifyIntervalMs();
    }

    void start() {
        scanThread = Thread.ofVirtual().name("meta-repair-scan").start(this::scanLoop);
        verifyThread = Thread.ofVirtual().name("meta-verify-scan").start(this::verifyLoop);
    }

    /** This owner's advertised endpoint, sent as the {@code verifierEndpoint} in VERIFY_CHUNKS so a node
     *  can record which owner attested it (tech design §9.2). Set by the controller before {@link #start}. */
    private volatile String advertisedEndpoint = "";

    void advertisedEndpoint(String endpoint) {
        this.advertisedEndpoint = endpoint == null ? "" : endpoint;
    }

    private volatile Thread verifyThread;
    // Verify cadence/batch/system-interval are now sourced from config (verifyIntervalMs,
    // verifyBatchSize, systemVerifyIntervalMs). systemVerifyIntervalMs is set in the canonical
    // constructor body so the config value is always applied at construction.
    private volatile long systemVerifyIntervalMs;
    // Last wall-clock ms the system namespace was owner-verified (0 = never). The verify loop is single
    // threaded, but tests drive verifyPass() directly, so this stays a field.
    private long lastSystemVerifyMs;

    // Global controller-leader settle time for cluster-scope node liveness work. Namespace activation only gates
    // access to a recovered local metadata view; verify verdicts add a namespace-level settle delay separately.
    private volatile long clusterLeaderSince;
    private volatile long globalOwnerEpoch;

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
                // it stays on the slow reconcileIntervalMs cadence instead of running every tick. Run on
                // every controller, not just the leader: a namespace owner reaps its own namespaces'
                // tombstones (sweepTombstones() routes leader vs owner).
                long now = System.currentTimeMillis();
                if (now - lastTombstoneSweep >= config.reconcileIntervalMs()) {
                    sweepTombstones();
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
            clusterLeaderSince = 0;
            resetGlobalOwnerEpoch();
            return;
        }
        if (clusterLeaderSince == 0) {
            clusterLeaderSince = System.currentTimeMillis();
        }
        registry.publishClusterLiveNodes();
        // settle period after acquiring leadership: nodes registered with the previous leader need a
        // lease cycle to re-register here, or expireScan() would mark them DEAD spuriously.
        if (!clusterLeaderSettled(System.currentTimeMillis())) {
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

    /** Test seam: shrink/grow the system-namespace owner-verify cadence to exercise the throttle. */
    void systemVerifyIntervalMsForTest(long ms) {
        this.systemVerifyIntervalMs = ms;
    }

    /** Test seam: stamps {@code clusterLeaderSince} far enough in the past that the settle gate is open. */
    void becomeLeaderForTest() {
        clusterLeaderSince = System.currentTimeMillis() - settleMs() - 1;
    }

    private long settleMs() {
        return (long) config.leaseMs() + config.deadGraceMs();
    }

    private boolean clusterLeaderSettled(long now) {
        long since = clusterLeaderSince;
        return since != 0 && now - since >= settleMs();
    }

    private boolean namespaceActive(StrataNamespace namespace) {
        if (NamespaceLogBackend.isSystem(namespace)) {
            return true;
        }
        return namespaceLeadership == null || namespaceLeadership.isNamespaceActive(namespace);
    }

    private boolean namespaceSettledForVerify(StrataNamespace namespace, long now) {
        if (!namespaceActive(namespace)) {
            return false;
        }
        if (NamespaceLogBackend.isSystem(namespace) || namespaceLeadership == null) {
            return true;
        }
        long activeSince = namespaceLeadership.namespaceActiveSinceMs(namespace);
        return activeSince != 0 && now - activeSince >= settleMs();
    }

    /**
     * Captures the locally ACTIVE owner epoch for non-destructive durability-restoring repair. Empty means
     * the namespace-log namespace is not ACTIVE yet, or this controller is not the global leader for a
     * global/system lane; callers must skip rather than silently sending owner RPCs at epoch 0. A stale
     * repair can copy data but cannot delete it, while its eventual metadata CAS remains owner-fenced.
     */
    private OptionalLong readyOwnerEpoch(StrataNamespace namespace) {
        if (usesGlobalOwnerEpoch(namespace)) {
            if (!isLeader.getAsBoolean()) {
                return OptionalLong.empty();
            }
            try {
                return OptionalLong.of(globalOwnerEpoch());
            } catch (ScpException e) {
                log.warn("namespace {} owner epoch is not ready; skipping owner repair/verify/delete until "
                        + "the global epoch can be allocated", namespace, e);
                return OptionalLong.empty();
            }
        }
        long epoch = namespaceLeadership.namespaceOwnerEpoch(namespace);
        if (epoch == 0) {
            if (zeroOwnerEpochWarned.add(namespace)) {
                log.warn("namespace {} has no ACTIVE owner epoch; skipping owner repair until "
                        + "the namespace becomes ACTIVE", namespace);
            }
            return OptionalLong.empty();
        }
        zeroOwnerEpochWarned.remove(namespace);
        return OptionalLong.of(epoch);
    }

    /**
     * Captures a consensus-revalidated epoch for operations that delete physical replicas or remove a
     * replica from authoritative metadata. Empty additionally means that authority revalidation failed;
     * callers must skip only that destructive work, while ordinary re-replication may continue using
     * {@link #readyOwnerEpoch(StrataNamespace)}.
     */
    private OptionalLong readyDestructiveOwnerEpoch(StrataNamespace namespace) {
        if (usesGlobalOwnerEpoch(namespace)) {
            return readyOwnerEpoch(namespace);
        }
        final long epoch;
        try {
            epoch = namespaceLeadership.authoritativeOwnerEpoch(namespace);
        } catch (Exception e) {
            authorityRevalidationSkips.incrementAndGet();
            if (authorityRevalidationWarned.add(namespace)) {
                log.warn("namespace {} owner authority could not be revalidated; skipping destructive "
                        + "verify/delete work until validation succeeds", namespace, e);
            }
            return OptionalLong.empty();
        }
        if (epoch == 0) {
            authorityRevalidationSkips.incrementAndGet();
            if (authorityRevalidationWarned.add(namespace)) {
                log.warn("namespace {} authority revalidation returned no ACTIVE epoch; skipping destructive "
                        + "verify/delete work until validation succeeds", namespace);
            }
            return OptionalLong.empty();
        }
        authorityRevalidationWarned.remove(namespace);
        zeroOwnerEpochWarned.remove(namespace);
        return OptionalLong.of(epoch);
    }

    /**
     * Returns the epoch stamped on metadata read responses and root-lane CONFIRM_ORPHAN verdicts
     * (leader-gated, epoch &gt; 0 enforced by the caller). Namespace-log CONFIRM_ORPHAN takes its epoch
     * from the stricter manifest-revalidated repository path instead.
     */
    long lookupOwnerEpoch(StrataNamespace namespace) {
        if (usesGlobalOwnerEpoch(namespace)) {
            return isLeader.getAsBoolean() ? globalOwnerEpoch() : 0;
        }
        return namespaceLeadership.namespaceOwnerEpoch(namespace);
    }

    private boolean usesGlobalOwnerEpoch(StrataNamespace namespace) {
        return NamespaceLogBackend.isSystem(namespace) || namespaceLeadership == null;
    }

    /**
     * Global/system repair lanes share one monotonically allocated owner epoch for this global-leader term.
     * Resetting it when leadership is lost or a node fences the command is the fence: the next leader-side
     * pass must allocate a fresh, higher epoch instead of re-driving stale commands.
     */
    private long globalOwnerEpoch() {
        long epoch = globalOwnerEpoch;
        if (epoch > 0) {
            return epoch;
        }
        synchronized (globalOwnerEpochLock) {
            if (globalOwnerEpoch == 0) {
                try {
                    globalOwnerEpoch = store.allocateMetadataEpoch();
                } catch (Exception e) {
                    throw new ScpException(ErrorCode.INTERNAL, "failed to allocate owner epoch", e);
                }
            }
            return globalOwnerEpoch;
        }
    }

    private void invalidateGlobalOwnerEpoch(StrataNamespace namespace) {
        if (!usesGlobalOwnerEpoch(namespace)) {
            return;
        }
        resetGlobalOwnerEpoch();
    }

    private void resetGlobalOwnerEpoch() {
        synchronized (globalOwnerEpochLock) {
            globalOwnerEpoch = 0;
        }
    }

    private ReentrantLock namespaceReconcileLock(StrataNamespace namespace) {
        if (namespaceLeadership != null && !NamespaceLogBackend.isSystem(namespace)) {
            return namespaceLeadership.namespaceReconcileLock(namespace);
        }
        return fallbackReconcileLocks.computeIfAbsent(namespace, ignored -> new ReentrantLock());
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
        long now = System.currentTimeMillis();
        if (!clusterLeaderSettled(now)) {
            return;
        }
        for (StrataNamespace ns : store.listNamespaces()) {
            if (!ownsAll.getAsBoolean() && !ownsNamespace.test(ns) && !NamespaceLogBackend.isSystem(ns)) {
                continue;
            }
            if (!namespaceActive(ns)) {
                continue;
            }
            OptionalLong ownerEpochOpt = readyOwnerEpoch(ns);
            if (ownerEpochOpt.isEmpty()) {
                continue;
            }
            long ownerEpoch = ownerEpochOpt.getAsLong();
            ReentrantLock lock = namespaceReconcileLock(ns);
            lock.lock();
            try {
                for (FileId fileId : store.listFileIds(ns)) {
                    Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(ns, fileId);
                    if (opt.isEmpty()) {
                        continue;
                    }
                    repairFileChunksOnNode(ns, fileId, opt.get().value(), deadNodeId, ownerEpoch);
                }
            } finally {
                lock.unlock();
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
                                         int deadNodeId, long ownerEpoch) {
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
                issueReplicate(ns, fileId, file, chunk, RepairTrigger.EVENT, ownerEpoch);
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
            clusterLeaderSince = 0;
            resetGlobalOwnerEpoch();
            ownerRepairPass();
            return;
        }
        if (clusterLeaderSince == 0) {
            clusterLeaderSince = System.currentTimeMillis();
        }
        if (!clusterLeaderSettled(System.currentTimeMillis())) {
            return;
        }
        scanOnce();
    }

    /**
     * Reaps DELETED-file tombstones at the slow reconcile cadence (tech design §4.2; see §17.20 for the
     * namespace-log publication-fence gap). The leader sweeps the shared system-root
     * tombstones globally — and its own loaded repos — via {@link MetadataStore#sweepDeletedFiles}; a
     * non-leader owner reaps only the namespaces it owns ({@link MetadataStore#sweepOwnedNamespaceTombstones}),
     * because their per-namespace metadata-log tombstones live in repos the leader does not hold. Gating the
     * whole sweep on the single global latch leaks tombstones in every non-leader-owned namespace forever.
     */
    void sweepTombstones() throws Exception {
        if (isLeader.getAsBoolean()) {
            store.sweepDeletedFiles(config.deletedTombstoneTtlMs());
        } else {
            store.sweepOwnedNamespaceTombstones(config.deletedTombstoneTtlMs());
        }
    }

    /** One reconciliation pass over all files. Idempotent; safe to call while serving. */
    void scanOnce() throws Exception {
        // The controller refreshes the shared live-node snapshot each pass so non-controller namespace
        // owners have a current placement view (tech design §4.1). Leader-gated: a standby must not publish.
        if (isLeader.getAsBoolean()) {
            registry.publishClusterLiveNodes();
        }
        sweepStuckCommands();
        record Repair(StrataNamespace ns, FileId fileId, Records.FileRecord file,
                      Records.ChunkRecord chunk, int liveReplicas) {}
        // durability census, published to gauges at the end; int[] lets lambdas increment them
        int[] under = {0}, unavailable = {0}, atMin = {0};

        for (StrataNamespace ns : store.listNamespaces()) {
            if (!namespaceActive(ns)) {
                continue;
            }
            OptionalLong ownerEpochOpt = readyOwnerEpoch(ns);
            if (ownerEpochOpt.isEmpty()) {
                continue;
            }
            long ownerEpoch = ownerEpochOpt.getAsLong();
            ReentrantLock lock = namespaceReconcileLock(ns);
            lock.lock();
            try {
                List<Repair> repairs = new ArrayList<>();
                // Resolve the stronger authority only if this namespace actually has deletion work.
                // The single-element holder caches the result across all DELETING files in this pass.
                OptionalLong[] destructiveOwnerEpoch = {null};
                for (FileId fileId : store.listFileIds(ns)) {
                    perFileIsolated(ns, fileId, "scanOnce", () -> {
                        Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(ns, fileId);
                        if (opt.isEmpty()) return;
                        Records.FileRecord file = opt.get().value();

                        if (file.state() == FileState.DELETING) {
                            if (destructiveOwnerEpoch[0] == null) {
                                destructiveOwnerEpoch[0] = readyDestructiveOwnerEpoch(ns);
                            }
                            if (destructiveOwnerEpoch[0].isPresent()) {
                                driveDeletion(file, opt.get().version(), destructiveOwnerEpoch[0].getAsLong());
                            }
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
                // Exposure priority is intentionally namespace-local: the per-namespace reconcile lock avoids
                // cross-namespace head-of-line blocking, so we order fewest-live-first within this namespace.
                repairs.sort(Comparator.comparingInt(Repair::liveReplicas));
                for (Repair r : repairs) {
                    issueReplicate(r.ns(), r.fileId(), r.file(), r.chunk(), RepairTrigger.RECONCILE, ownerEpoch);
                }
            } finally {
                lock.unlock();
            }
        }
        underReplicatedChunks = under[0];
        unavailableChunks = unavailable[0];
        chunksAtMinRedundancy = atMin[0];
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
                                Records.ChunkRecord chunk, RepairTrigger trigger, long ownerEpoch) {
        ChunkId chunkId = file.chunkId(chunk.index());
        int deadNode = -1;
        List<Messages.Replica> sources = new ArrayList<>();
        Set<Integer> existing = new HashSet<>(chunk.replicas());
        excludeVerifyQuarantines(ns, chunkId, existing);
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
                        ns, ownerEpoch));
        recordRepairIssued(trigger);
        log.info("repair: {} dead={} -> target={} (cmd {})", chunkId, deadNode, target.record.nodeId(), cmdId);
    }

    /**
     * Non-controller owner repair (tech design §4.4 and §7.2): a namespace owner that does not hold the
     * global latch cannot use the controller's heartbeat command channel, so it heals its own namespaces'
     * under-replicated sealed chunks directly. It reads the controller's authoritative DEAD set from the
     * consensus root, picks a replacement target from the shared live-node snapshot, tells that target
     * to pull the chunk via EXEC_REPLICATE, then writes the replica change itself (single writer for the
     * namespace). Non-sharded deployments return immediately — the cluster controller does all repair.
     */
    void ownerRepairPass() throws Exception {
        if (ownsAll.getAsBoolean()) {
            return;
        }
        Map<Integer, Records.NodeRecord> nodes = nodesById();
        Set<Integer> dead = new HashSet<>();
        for (Records.NodeRecord n : nodes.values()) {
            if (n.state() == Records.NodeState.DEAD) {
                dead.add(n.nodeId());
            }
        }
        for (StrataNamespace ns : store.listNamespaces()) {
            if (!ownsNamespace.test(ns) || !namespaceActive(ns)) {
                continue;
            }
            OptionalLong ownerEpochOpt = readyOwnerEpoch(ns);
            if (ownerEpochOpt.isEmpty()) {
                continue;
            }
            long ownerEpoch = ownerEpochOpt.getAsLong();
            ReentrantLock lock = namespaceReconcileLock(ns);
            lock.lock();
            try {
                // Missing-replica repair stays available during a root-read outage. Only a DELETING file
                // triggers the stronger authority check, cached once for this namespace/pass.
                OptionalLong[] destructiveOwnerEpoch = {null};
                for (FileId fileId : store.listFileIds(ns)) {
                    perFileIsolated(ns, fileId, "ownerRepairPass", () -> {
                        Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(ns, fileId);
                        if (opt.isEmpty()) {
                            return;
                        }
                        Records.FileRecord file = opt.get().value();
                        if (file.state() == FileState.DELETING) {
                            // the owner reclaims its own deleted files: the leader's heartbeat command channel
                            // cannot reach a non-leader-owned namespace's chunks, so without this they leak.
                            if (destructiveOwnerEpoch[0] == null) {
                                destructiveOwnerEpoch[0] = readyDestructiveOwnerEpoch(ns);
                            }
                            if (destructiveOwnerEpoch[0].isPresent()) {
                                ownerDriveDeletion(file, opt.get().version(), nodes,
                                        destructiveOwnerEpoch[0].getAsLong());
                            }
                            return;
                        }
                        for (Records.ChunkRecord chunk : file.chunks()) {
                            if (chunk.state() == ChunkState.SEALED) {
                                ownerRepairChunk(ns, file, chunk, dead, RepairTrigger.RECONCILE, ownerEpoch);
                            }
                        }
                    });
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /* ---------- owner-pull verification (tech design §9.2): replaces the central inventory push ---------- */

    private void verifyLoop() {
        while (!closed.get()) {
            try {
                Thread.sleep(config.verifyIntervalMs());
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
     * Owner-pull verification (tech design §9.2): for each namespace this controller owns, ask every live
     * node that should hold a sealed chunk to report its local state, and compare against the descriptor
     * — missing/corrupt drops the replica so the under-replication scan re-replicates within the
     * namespace. Replaces "every node pushes its full chunk list to the leader". Runs off the repair
     * thread (the verify RPCs block); collection never holds the reconcile lock, while the short apply
     * phase is serialized with repair placement/commit so stale verdicts cannot quarantine a fresh import.
     */
    void verifyPass() throws Exception {
        if (isLeader.getAsBoolean()) {
            // A just-elected leader's node-liveness view is stale until nodes re-register; a missing
            // verdict in that window could falsely drop a healthy replica, so wait out the settle period.
            if (!clusterLeaderSettled(System.currentTimeMillis())) {
                return;
            }
        }
        long now = System.currentTimeMillis();
        Map<Integer, Records.NodeRecord> nodes = nodesById();
        for (StrataNamespace ns : store.listNamespaces()) {
            if (usesGlobalOwnerEpoch(ns)) {
                if (!isLeader.getAsBoolean()) {
                    continue;
                }
            } else if (!ownsNamespace.test(ns)) {
                continue;
            }
            if (!namespaceSettledForVerify(ns, now)) {
                continue;
            }
            if (NamespaceLogBackend.isSystem(ns)) {
                // metadata-log segments rarely change; verify them on the slower systemVerifyIntervalMs
                // cadence instead of every brisk pass. reconcile() + the node-death path still backstop loss.
                if (lastSystemVerifyMs != 0 && now - lastSystemVerifyMs < systemVerifyIntervalMs) {
                    continue;
                }
            }
            OptionalLong ownerEpochOpt = readyDestructiveOwnerEpoch(ns);
            if (ownerEpochOpt.isEmpty()) {
                continue;
            }
            long ownerEpoch = ownerEpochOpt.getAsLong();
            if (NamespaceLogBackend.isSystem(ns)) {
                lastSystemVerifyMs = now;
            }
            for (FileId fileId : store.listFileIds(ns)) {
                perFileIsolated(ns, fileId, "verifyPass", () -> verifyFile(ns, fileId, nodes, now, ownerEpoch));
            }
        }
    }

    private void verifyFile(StrataNamespace ns, FileId fileId, Map<Integer, Records.NodeRecord> nodes, long now,
                            long ownerEpoch) throws Exception {
        Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(ns, fileId);
        if (opt.isEmpty() || opt.get().value().state() == FileState.DELETING) {
            return; // deletion, not verification, drives a DELETING file's chunks
        }
        Records.FileRecord file = opt.get().value();
        Map<Integer, List<ChunkId>> byNode = new LinkedHashMap<>();
        Map<ChunkId, Records.ChunkRecord> expected = new LinkedHashMap<>();
        Map<ChunkId, List<VerifyVerdict>> verdictsByChunk = new HashMap<>();
        Map<ChunkId, Set<Integer>> droppedThisPass = new HashMap<>();
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
            int batchSize = config.verifyBatchSize();
            for (int i = 0; i < ids.size(); i += batchSize) {
                List<ChunkId> batch = ids.subList(i, Math.min(i + batchSize, ids.size()));
                List<Messages.VerifyChunkResult> results = execVerify(node, ns, batch, ownerEpoch);
                // Empty == the RPC could not reach the node: no verdict, so we never drop a replica on an
                // unreachable owner-verify (fail-safe). A truly dead node is handled by the reconcile scan.
                if (results.isEmpty()) {
                    continue;
                }
                if (!hasExactVerifyResultIds(batch, results)) {
                    log.error("owner-verify: rejecting malformed result batch from node {} ns={} "
                                    + "requested={} returned={}",
                            node.nodeId(), ns, batch, results.stream()
                                    .map(Messages.VerifyChunkResult::chunkId).toList());
                    continue;
                }
                for (Messages.VerifyChunkResult r : results) {
                    verdictsByChunk.computeIfAbsent(r.chunkId(), ignored -> new ArrayList<>())
                            .add(new VerifyVerdict(e.getKey(), node, r));
                }
            }
        }

        // Apply only after every node/batch has been collected. Destructive decisions need the complete
        // response census so a wrong descriptor cannot make every local copy look independently corrupt.
        for (Map.Entry<ChunkId, Records.ChunkRecord> entry : expected.entrySet()) {
            ChunkId chunkId = entry.getKey();
            Records.ChunkRecord exp = entry.getValue();
            List<VerifyVerdict> verdicts = verdictsByChunk.getOrDefault(chunkId, List.of());
            if (verdicts.isEmpty()) {
                continue; // no returned fact at all; unreachable verification remains fail-safe
            }

            ReentrantLock applyLock = namespaceReconcileLock(ns);
            applyLock.lock();
            try {
                // The census was collected without the namespace lock. Refuse to apply it if repair or any
                // other writer changed this descriptor meanwhile; the next pass will verify the new snapshot.
                if (!verifySnapshotStillCurrent(ns, fileId, chunkId, exp)) {
                    continue;
                }

                int confirmedMatches = (int) verdicts.stream()
                        .filter(verdict -> matchesDescriptor(exp, verdict.result()))
                        .count();
                Set<ReplicaKey> resealed = new HashSet<>();

                if (confirmedMatches == 0) {
                    // Re-sealing at a shorter descriptor length can truncate an OPEN copy. With no SEALED
                    // corroboration, only mutate an OPEN copy whose current full length+CRC already exactly
                    // matches the descriptor; this writes a trailer but cannot discard evidence. One such
                    // successful seal then safely corroborates the descriptor for recovering peers with an
                    // unacknowledged tail.
                    for (VerifyVerdict verdict : verdicts) {
                        if (matchesOpenDescriptorBytes(exp, verdict.result())
                                && tryResealVerifyVerdict(ns, exp, verdict, ownerEpoch)) {
                            confirmedMatches++;
                            resealed.add(new ReplicaKey(ns, chunkId, verdict.nodeId()));
                        }
                    }
                }
                if (confirmedMatches > 0) {
                    for (VerifyVerdict verdict : verdicts) {
                        ReplicaKey key = new ReplicaKey(ns, chunkId, verdict.nodeId());
                        if (!resealed.contains(key) && !matchesDescriptor(exp, verdict.result())
                                && tryResealVerifyVerdict(ns, exp, verdict, ownerEpoch)) {
                            confirmedMatches++;
                            resealed.add(key);
                        }
                    }
                }

                boolean hasUnhealthyVerdict = verdicts.stream().anyMatch(verdict ->
                        !matchesDescriptor(exp, verdict.result())
                                && !resealed.contains(new ReplicaKey(ns, chunkId, verdict.nodeId())));
                if (hasUnhealthyVerdict && confirmedMatches == 0) {
                    verifyNoMatchBreaks.incrementAndGet();
                    long expectedLive = exp.replicas().stream()
                            .filter(nodeId -> isPersistedLive(nodeId, nodes)).count();
                    log.error("owner-verify: NO-MATCH BREAKER for ns={} chunk={} — {}/{} live replica "
                                    + "verdicts returned, but none match descriptor len/crc {}/{}; preserving "
                                    + "every replica",
                            ns, chunkId, verdicts.size(), expectedLive, exp.length(), exp.crc());
                    continue;
                }

                for (VerifyVerdict verdict : verdicts) {
                    if (resealed.contains(new ReplicaKey(ns, chunkId, verdict.nodeId()))) {
                        continue;
                    }
                    applyVerifyVerdict(ns, fileId, exp, verdict.nodeId(), verdict.node(), verdict.result(), now,
                            droppedThisPass, ownerEpoch);
                }
            } finally {
                applyLock.unlock();
            }
        }
    }

    private boolean verifySnapshotStillCurrent(StrataNamespace namespace, FileId fileId, ChunkId chunkId,
                                               Records.ChunkRecord expected) throws Exception {
        Optional<MetadataStore.Versioned<Records.FileRecord>> current = store.getFile(namespace, fileId);
        if (current.isEmpty() || current.get().value().state() == FileState.DELETING) {
            return false;
        }
        Records.FileRecord file = current.get().value();
        return file.chunks().stream().anyMatch(chunk ->
                file.chunkId(chunk.index()).equals(chunkId) && chunk.equals(expected));
    }

    private static boolean hasExactVerifyResultIds(List<ChunkId> requested,
                                                   List<Messages.VerifyChunkResult> returned) {
        if (requested.size() != returned.size()) {
            return false;
        }
        Set<ChunkId> requestedIds = new HashSet<>(requested);
        if (requestedIds.size() != requested.size()) {
            return false;
        }
        Set<ChunkId> returnedIds = new HashSet<>();
        for (Messages.VerifyChunkResult result : returned) {
            if (!requestedIds.contains(result.chunkId()) || !returnedIds.add(result.chunkId())) {
                return false;
            }
        }
        return returnedIds.size() == requestedIds.size();
    }

    private static boolean matchesDescriptor(Records.ChunkRecord expected, Messages.VerifyChunkResult result) {
        return result.present() && result.state() == ChunkState.SEALED
                && result.length() == expected.length() && result.crc() == expected.crc();
    }

    private static boolean matchesOpenDescriptorBytes(Records.ChunkRecord expected,
                                                      Messages.VerifyChunkResult result) {
        return result.present() && result.state() == ChunkState.OPEN
                && result.length() == expected.length() && result.crc() == expected.crc();
    }

    private boolean tryResealVerifyVerdict(StrataNamespace ns, Records.ChunkRecord expected,
                                           VerifyVerdict verdict, long ownerEpoch) {
        Messages.VerifyChunkResult result = verdict.result();
        ChunkId chunkId = result.chunkId();
        if (isRepairProtected(ns, chunkId, verdict.nodeId())
                || !result.present() || result.state() != ChunkState.OPEN
                || result.length() < expected.length()
                || !execReseal(verdict.node(), chunkId, ns, expected, ownerEpoch)) {
            return false;
        }
        log.info("verify: re-sealed OPEN copy of descriptor-sealed chunk {} on node {} at len {}",
                chunkId, verdict.nodeId(), expected.length());
        replicaMissingSince.remove(verdict.nodeId() + ":" + ns + ":" + chunkId);
        return true;
    }

    /** Synchronous VERIFY_CHUNKS to {@code node}; empty list on any RPC failure (treated as no verdict). */
    private List<Messages.VerifyChunkResult> execVerify(Records.NodeRecord node, StrataNamespace ns,
                                                        List<ChunkId> chunkIds, long ownerEpoch) {
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
                    new Messages.VerifyChunks(ns, advertisedEndpoint, chunkIds, ownerEpoch).encode(), null,
                    config.repairCommandTimeoutMs());
            return Messages.VerifyChunksResp.decode(resp).results();
        } catch (ScpException e) {
            if (e.code() == ErrorCode.FENCED_EPOCH || !e.retriable()) {
                log.warn("owner-verify to node {} failed with {} ns={} offeredOwnerEpoch={} "
                                + "requiredOwnerEpoch={} — treating verdict as terminal for this pass",
                        node.nodeId(), e.code(), ns, ownerEpoch, e.detail());
            } else {
                log.debug("owner-verify to node {} failed: {}", node.nodeId(), e.getMessage());
            }
            return List.of();
        } catch (Exception e) {
            log.debug("owner-verify to node {} failed: {}", node.nodeId(), e.getMessage());
            return List.of();
        }
    }

    /**
     * Compares one VERIFY_CHUNKS result against the descriptor and drops the replica when it is missing,
     * non-SEALED past grace, or corrupt — the owner-pull equivalent of the inventory-push reconciliation.
     * A freshly-repaired replica is protected from a stale verdict; corrupt/bad bytes are quarantined after
     * grace so a re-pick of this node cannot read them (the under-replication scan re-replicates).
     */
    private void applyVerifyVerdict(StrataNamespace ns, FileId fileId, Records.ChunkRecord exp, int nodeId,
                                    Records.NodeRecord node, Messages.VerifyChunkResult r, long now,
                                    Map<ChunkId, Set<Integer>> droppedThisPass, long ownerEpoch)
            throws Exception {
        ChunkId chunkId = r.chunkId();
        if (isRepairProtected(ns, chunkId, nodeId)) {
            return;
        }
        String key = nodeId + ":" + ns + ":" + chunkId;
        if (!r.present()) {
            if (replicaUnhealthyPastGrace(key, now)) {
                if (shouldKeepLastPersistedLiveReplica(exp, nodeId, chunkId, r, droppedThisPass)) {
                    return;
                }
                if (applyDeleteConfirmed(ns, fileId, chunkId, nodeId)) {
                    replicaMissingSince.remove(key);
                    recordDroppedThisPass(droppedThisPass, chunkId, nodeId);
                    log.warn("verify: node {} missing sealed chunk {} (>= {}ms) — dropped replica for re-repair",
                            nodeId, chunkId, config.replicaMissingGraceMs());
                }
            }
        } else if (r.state() != ChunkState.SEALED) {
            if (replicaUnhealthyPastGrace(key, now)) {
                if (shouldKeepLastPersistedLiveReplica(exp, nodeId, chunkId, r, droppedThisPass)) {
                    return;
                }
                ReplicaKey quarantine = new ReplicaKey(ns, chunkId, nodeId);
                if (!verifyQuarantinesInFlight.add(quarantine)) {
                    return;
                }
                try {
                    // A repair may have started after the pass-level stale-verdict check but before this
                    // quarantine marker was installed. Re-check now; repair commit paths also honor the
                    // marker, closing the opposite side of the placement/descriptor race.
                    if (isRepairProtected(ns, chunkId, nodeId)) {
                        return;
                    }
                    if (applyDeleteConfirmed(ns, fileId, chunkId, nodeId)) {
                        replicaMissingSince.remove(key);
                        recordDroppedThisPass(droppedThisPass, chunkId, nodeId);
                        log.warn("verify: node {} holds {} copy of sealed chunk {} (>= {}ms) "
                                        + "— dropped replica and quarantining local bytes",
                                nodeId, r.state(), chunkId, config.replicaMissingGraceMs());
                        if (!execQuarantine(node, chunkId, ns, ownerEpoch)) {
                            log.warn("verify: dropped replica {} from descriptor for chunk {} but physical "
                                            + "quarantine failed/fenced on node {} — orphan GC must reclaim "
                                            + "the stranded copy",
                                    nodeId, chunkId, nodeId);
                        }
                    }
                } finally {
                    verifyQuarantinesInFlight.remove(quarantine);
                }
            }
        } else if (r.length() != exp.length() || r.crc() != exp.crc()) {
            // Corrupt sealed bytes are still protected by the store's digest check, so honor grace
            // before delete; immediate delete could let one bogus verdict destroy the last live copy.
            if (replicaUnhealthyPastGrace(key, now)) {
                if (shouldKeepLastPersistedLiveReplica(exp, nodeId, chunkId, r, droppedThisPass)) {
                    return;
                }
                ReplicaKey quarantine = new ReplicaKey(ns, chunkId, nodeId);
                if (!verifyQuarantinesInFlight.add(quarantine)) {
                    return;
                }
                try {
                    if (isRepairProtected(ns, chunkId, nodeId)) {
                        return;
                    }
                    if (applyDeleteConfirmed(ns, fileId, chunkId, nodeId)) {
                        replicaMissingSince.remove(key);
                        recordDroppedThisPass(droppedThisPass, chunkId, nodeId);
                        log.warn("verify: node {} holds corrupt sealed chunk {} "
                                        + "(len {}/{} crc {}/{}, >= {}ms) — dropped replica and quarantining",
                                nodeId, chunkId, r.length(), exp.length(), r.crc(), exp.crc(),
                                config.replicaMissingGraceMs());
                        if (!execQuarantine(node, chunkId, ns, ownerEpoch)) {
                            log.warn("verify: dropped replica {} from descriptor for corrupt chunk {} but "
                                            + "physical quarantine failed/fenced on node {} — orphan GC must "
                                            + "reclaim the stranded copy",
                                    nodeId, chunkId, nodeId);
                        }
                    }
                } finally {
                    verifyQuarantinesInFlight.remove(quarantine);
                }
            }
        } else {
            replicaMissingSince.remove(key); // healthy attestation — clear any pending anomaly
        }
    }

    private void recordDroppedThisPass(Map<ChunkId, Set<Integer>> droppedThisPass, ChunkId chunkId, int nodeId) {
        droppedThisPass.computeIfAbsent(chunkId, k -> new HashSet<>()).add(nodeId);
    }

    private boolean shouldKeepLastPersistedLiveReplica(Records.ChunkRecord exp, int nodeId, ChunkId chunkId,
                                                       Messages.VerifyChunkResult r,
                                                       Map<ChunkId, Set<Integer>> droppedThisPass)
            throws Exception {
        if (!wouldDropLastPersistedLiveReplica(exp, nodeId, chunkId, droppedThisPass)) {
            return false;
        }
        log.warn("verify: keeping last live replica {} of chunk {} despite verdict "
                        + "(present={} state={}) — dropping it would leave 0 live replicas",
                nodeId, chunkId, r.present(), r.state());
        return true;
    }

    /**
     * Last-live-replica guard (data-loss hardening): once a verdict has passed grace and is about
     * to drop/delete a replica, re-read persisted liveness and make sure a peer still survives.
     * Liveness stays persisted-based (see {@link #isPersistedLive}) rather than registry-based so
     * non-leader namespace owners do not use a frozen in-memory registry. Already-dropped replicas
     * from this verify pass also cannot count as survivors because they may already be unlinked.
     */
    private boolean wouldDropLastPersistedLiveReplica(Records.ChunkRecord exp, int nodeId, ChunkId chunkId,
                                                      Map<ChunkId, Set<Integer>> droppedThisPass)
            throws Exception {
        Map<Integer, Records.NodeRecord> latestNodes = nodesById();
        Set<Integer> dropped = droppedThisPass.getOrDefault(chunkId, Set.of());
        return exp.replicas().stream()
                .noneMatch(n -> n != nodeId && !dropped.contains(n) && isPersistedLive(n, latestNodes));
    }

    private void ownerRepairChunk(StrataNamespace ns, Records.FileRecord file, Records.ChunkRecord chunk,
                                  Set<Integer> dead, RepairTrigger trigger, long ownerEpoch) {
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
        Set<Integer> excludedTargets = new HashSet<>(chunk.replicas());
        excludeVerifyQuarantines(ns, chunkId, excludedTargets);
        List<NodeRegistry.LiveNode> targets;
        try {
            targets = Placement.choose(ns, registry, 1, excludedTargets, usedHosts);
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
                    (byte) 1, chunk.crc(), chunk.length(), ns, ownerEpoch);
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

    private void excludeVerifyQuarantines(StrataNamespace namespace, ChunkId chunkId,
                                          Set<Integer> excludedNodes) {
        for (ReplicaKey quarantine : verifyQuarantinesInFlight) {
            if (quarantine.namespace().equals(namespace) && quarantine.chunkId().equals(chunkId)) {
                excludedNodes.add(quarantine.nodeId());
            }
        }
    }

    @FunctionalInterface
    private interface NodeCall {
        boolean run(ScpClient client, int timeoutMs) throws Exception;
    }

    /**
     * Shared owner-direct transport: opens a one-shot tool connection to {@code node} and runs {@code call},
     * returning its result. Logs and returns false on a bad endpoint or any RPC failure. Owner lanes route
     * through here so endpoint parsing, timeout, connection lifecycle, and failure logging stay in one place.
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
        } catch (ScpException e) {
            if (e.code() == ErrorCode.FENCED_EPOCH || !e.retriable()) {
                log.warn("{} to node {} for {} failed with {} requiredOwnerEpoch={} — terminal for this attempt",
                        label, node.nodeId(), chunkId, e.code(), e.detail());
            } else {
                log.warn("{} to node {} for {} failed: {}", label, node.nodeId(), chunkId, e.getMessage());
            }
            return false;
        } catch (Exception e) {
            log.warn("{} to node {} for {} failed: {}", label, node.nodeId(), chunkId, e.getMessage());
            return false;
        }
    }

    /** Synchronously tells {@code target} to pull the chunk (EXEC_REPLICATE); true if it acked OK. */
    private boolean execReplicate(NodeRegistry.LiveNode target, Messages.ReplicateCmd cmd) {
        BufWriter w = new BufWriter();
        Messages.Command.writeRequest(w, cmd);
        return directNodeCall(target.record, cmd.chunkId(), "owner-repair", (client, timeoutMs) -> {
            client.call(Opcode.EXEC_REPLICATE, w.toBytes(), null, timeoutMs);
            return true;
        });
    }

    /**
     * Re-seals an intact OPEN local copy whose descriptor is already SEALED. This recovers the
     * non-fsync correlated-crash window where the retained ledger can prove the data prefix, but the
     * unforced trailer did not survive restart, so the replica reports OPEN at or beyond the descriptor end.
     */
    private boolean execReseal(Records.NodeRecord node, ChunkId chunkId, StrataNamespace ns,
                               Records.ChunkRecord exp, long ownerEpoch) {
        return directNodeCall(node, chunkId, "owner-reseal", (client, timeoutMs) -> {
            ByteBuffer resp = client.call(Opcode.SEAL_CHUNK,
                    new Messages.SealChunk(chunkId, exp.writeEpoch(), exp.length(), ns, ownerEpoch).encode(),
                    null, timeoutMs);
            Messages.SealResp r = Messages.SealResp.decode(resp);
            if (r.finalLength() != exp.length() || r.chunkCrc() != exp.crc()) {
                // A successful seal with a bad CRC leaves a sealed-corrupt replica for the next verify
                // pass to classify and grace-drop through the normal corrupt-sealed path.
                log.warn("owner-reseal of {} on node {} returned len/crc {}/{} expected {}/{}",
                        chunkId, node.nodeId(), r.finalLength(), r.chunkCrc(), exp.length(), exp.crc());
                return false;
            }
            return true;
        });
    }

    /**
     * Synchronously tells {@code node} to physically delete {@code chunkId} via a direct DELETE_CHUNKS
     * call — the owner-direct deletion transport, mirroring {@link #execReplicate} for repair. A sharded
     * non-controller owner has no heartbeat command channel, so it deletes its own namespaces' chunks
     * this way. True if the node acked the chunk gone (deleted, or already absent).
     */
    private boolean execDelete(Records.NodeRecord node, ChunkId chunkId, StrataNamespace ns, long ownerEpoch) {
        return directNodeCall(node, chunkId, "owner-delete", (client, timeoutMs) -> {
            ByteBuffer resp = client.call(Opcode.DELETE_CHUNKS,
                    new Messages.DeleteChunks(List.of(chunkId), ns, ownerEpoch).encode(), null, timeoutMs);
            Messages.DeleteChunksResp r = Messages.DeleteChunksResp.decode(resp);
            short code = r.codes().isEmpty() ? ErrorCode.OK.code : r.codes().get(0);
            if (code != ErrorCode.OK.code && code != ErrorCode.CHUNK_NOT_FOUND.code) {
                log.warn("owner-delete of {} on node {} returned {}",
                        chunkId, node.nodeId(), code);
            }
            return code == ErrorCode.OK.code || code == ErrorCode.CHUNK_NOT_FOUND.code;
        });
    }

    /**
     * Synchronously removes a verify-rejected replica from the live chunk namespace while preserving its files
     * under quarantine names. A dedicated opcode deliberately fails closed against an older node: extending
     * DELETE_CHUNKS with a tag would let an old decoder ignore the tag and unlink the evidence instead.
     */
    private boolean execQuarantine(Records.NodeRecord node, ChunkId chunkId, StrataNamespace ns, long ownerEpoch) {
        return directNodeCall(node, chunkId, "owner-quarantine", (client, timeoutMs) -> {
            ByteBuffer resp = client.call(Opcode.QUARANTINE_CHUNKS,
                    new Messages.DeleteChunks(List.of(chunkId), ns, ownerEpoch).encode(), null, timeoutMs);
            Messages.DeleteChunksResp r = Messages.DeleteChunksResp.decode(resp);
            short code = r.codes().isEmpty() ? ErrorCode.OK.code : r.codes().get(0);
            if (code != ErrorCode.OK.code && code != ErrorCode.CHUNK_NOT_FOUND.code) {
                log.warn("owner-quarantine of {} on node {} returned {}", chunkId, node.nodeId(), code);
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
        ReplicaKey target = new ReplicaKey(ns, chunkId, targetNode);
        for (int attempt = 0; attempt < Controller.CAS_RETRIES; attempt++) {
            if (verifyQuarantinesInFlight.contains(target)) {
                log.warn("owner repair descriptor swap for {} to node {} deferred while verify quarantine is "
                        + "in flight", chunkId, targetNode);
                return false;
            }
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
                recentlyCommittedReplicas.put(target, System.currentTimeMillis());
                return true;
            }
        }
        log.warn("owner repair descriptor swap for {} kept failing CAS — next scan reconciles", chunkId);
        return false;
    }

    /**
     * Dispatches a just-deleted file's chunk deletions immediately, instead of waiting for the next
     * background scan (which is slow under heavy churn), so physical space is reclaimed promptly and the
     * disk stays bounded under sustained delete load. If the namespace owner epoch is not ready, promptness
     * defers silently to the next background pass rather than sending an unstamped destructive command.
     * Synchronized with the scan so they don't race.
     */
    void driveDeletionNow(StrataNamespace namespace, FileId fileId) {
        if (!namespaceActive(namespace)) {
            return;
        }
        ReentrantLock lock = namespaceReconcileLock(namespace);
        lock.lock();
        try {
            if (!namespaceActive(namespace)) {
                return;
            }
            OptionalLong ownerEpochOpt = readyDestructiveOwnerEpoch(namespace);
            if (ownerEpochOpt.isEmpty()) {
                return;
            }
            long ownerEpoch = ownerEpochOpt.getAsLong();
            Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(namespace, fileId);
            if (opt.isEmpty() || opt.get().value().state() != FileState.DELETING) {
                return;
            }
            MetadataStore.Versioned<Records.FileRecord> vf = opt.get();
            if (isLeader.getAsBoolean()) {
                driveDeletion(vf.value(), vf.version(), ownerEpoch);
            } else {
                // a sharded non-leader owner has no heartbeat command channel to the data nodes,
                // so it reclaims its own namespace's chunks directly via DELETE_CHUNKS.
                ownerDriveDeletion(vf.value(), vf.version(), nodesById(), ownerEpoch);
            }
        } catch (Exception e) {
            log.warn("prompt delete dispatch for {} failed — background scan will retry", fileId, e);
        } finally {
            lock.unlock();
        }
    }

    void driveDeletionSoon(StrataNamespace namespace, FileId fileId) {
        try {
            deleteDispatchExecutor.execute(() -> driveDeletionNow(namespace, fileId));
        } catch (RuntimeException e) {
            log.warn("prompt delete dispatch for {} was not scheduled — background scan will retry", fileId, e);
        }
    }

    private void driveDeletion(Records.FileRecord file, int version, long ownerEpoch) throws Exception {
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
                    registry.enqueue(nodeId, new Messages.DeleteCmd(cmdId, List.of(chunkId), ns,
                            ownerEpoch));
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
                                    Map<Integer, Records.NodeRecord> nodes, long ownerEpoch) throws Exception {
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
                } else if (execDelete(node, chunkId, ns, ownerEpoch)) {
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
                try {
                    if (completion.status() == 0) {
                        applyReplicaSwap(r);
                    } else if (ErrorCode.fromCode(completion.status()) == ErrorCode.FENCED_EPOCH) {
                        invalidateGlobalOwnerEpoch(r.namespace());
                        log.warn("replicate cmd {} for {} was fenced by node {} — stale owner command dropped",
                                completion.commandId(), r.chunkId(), reportingNode);
                    } else {
                        log.warn("replicate cmd {} for {} failed with {} — next scan retries",
                                completion.commandId(), r.chunkId(), ErrorCode.fromCode(completion.status()));
                    }
                } finally {
                    // Keep stale verify verdicts repair-protected until the descriptor commit is serialized
                    // and recentlyCommittedReplicas has been published.
                    chunksBeingRepaired.remove(new NsChunkId(r.namespace(), r.chunkId()));
                }
            } else if (action instanceof DeleteAction d) {
                if (completion.status() == 0) {
                    applyDeleteConfirmed(d.namespace(), d.fileId(), d.chunkId(), d.nodeId());
                } else if (ErrorCode.fromCode(completion.status()) == ErrorCode.FENCED_EPOCH) {
                    invalidateGlobalOwnerEpoch(d.namespace());
                    log.warn("delete cmd {} for {} was fenced by node {} — stale owner command dropped",
                            completion.commandId(), d.chunkId(), reportingNode);
                } else {
                    log.warn("delete cmd {} for {} failed with {} — next scan retries",
                            completion.commandId(), d.chunkId(), ErrorCode.fromCode(completion.status()));
                }
            }
        } catch (Exception e) {
            log.warn("applying completion {} failed — next scan reconciles", completion.commandId(), e);
        }
    }

    private void applyReplicaSwap(ReplicateAction r) throws Exception {
        ReentrantLock lock = namespaceReconcileLock(r.namespace());
        lock.lock();
        try {
            applyReplicaSwapLocked(r);
        } finally {
            lock.unlock();
        }
    }

    private void applyReplicaSwapLocked(ReplicateAction r) throws Exception {
        if (registry.isDead(r.targetNode())) {
            // target completed the copy but died before the swap landed: writing a dead node
            // into the descriptor would hand readers a bad replica — skip; the next scan
            // re-repairs (and the dead target's copy becomes an orphan)
            log.warn("repair target {} for {} died before descriptor swap — next scan retries",
                    r.targetNode(), r.chunkId());
            return;
        }
        ReplicaKey target = new ReplicaKey(r.namespace(), r.chunkId(), r.targetNode());
        for (int attempt = 0; attempt < Controller.CAS_RETRIES; attempt++) {
            if (verifyQuarantinesInFlight.contains(target)) {
                log.warn("descriptor swap for {} to node {} deferred while verify quarantine is in flight",
                        r.chunkId(), r.targetNode());
                return;
            }
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
                recentlyCommittedReplicas.put(target, System.currentTimeMillis());
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

    private boolean applyDeleteConfirmed(StrataNamespace namespace, FileId fileId, ChunkId chunkId,
                                         int nodeId) throws Exception {
        for (int attempt = 0; attempt < Controller.CAS_RETRIES; attempt++) {
            Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(namespace, fileId);
            if (opt.isEmpty()) {
                return false;
            }
            Records.FileRecord file = opt.get().value();
            List<Records.ChunkRecord> chunks = new ArrayList<>();
            boolean foundChunk = false;
            boolean changed = false;
            for (Records.ChunkRecord c : file.chunks()) {
                if (file.chunkId(c.index()).equals(chunkId)) {
                    foundChunk = true;
                    List<Integer> replicas = new ArrayList<>(c.replicas());
                    boolean removed = replicas.remove(Integer.valueOf(nodeId));
                    if (!replicas.isEmpty() || file.state() != FileState.DELETING) {
                        // a LIVE file keeps the chunk record even with zero replicas: erasing it
                        // would silently shorten the file (readers' offset accounting shifts) —
                        // total loss must surface as a hard read failure, not missing data
                        if (removed && replicas.isEmpty()) {
                            log.error("chunk {} of live file {} has lost ALL replicas — readers "
                                    + "will hard-fail until operator intervention", chunkId, fileId);
                        }
                        chunks.add(removed ? c.withReplicas(replicas) : c);
                        changed |= removed;
                    } else {
                        changed = true; // empty AND deleting -> chunk record dropped
                    }
                } else {
                    chunks.add(c);
                }
            }
            if (!foundChunk) {
                // The authoritative descriptor already stopped naming this chunk/replica. For a fully-drained
                // DELETING file, preserve the old eager finalization behavior; otherwise absence is success.
                if (file.state() != FileState.DELETING || !file.chunks().isEmpty()) {
                    return true;
                }
            } else if (!changed) {
                return true; // existing descriptor, but this replica was removed by a concurrent writer
            }
            Records.FileRecord updated = file.withChunks(chunks);
            if (chunks.isEmpty() && file.state() == FileState.DELETING) {
                if (store.deleteFile(namespace, fileId, opt.get().version())) {
                    log.info("file {} fully deleted", fileId);
                    return true;
                }
                continue;
            }
            if (store.updateFile(updated, opt.get().version())) {
                return true;
            }
        }
        log.warn("delete-confirm descriptor update for {} kept failing CAS — next scan reconciles", chunkId);
        return false;
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
