package io.strata.meta;

import io.strata.common.FileState;
import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.proto.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The repair coordinator (tech design §7.2, §9): a periodic reconciliation scan is the single
 * engine for under-replication repair, DELETING-file driving, and command retry — individual
 * command failures are simply forgotten and rediscovered by the next scan. Exposure-prioritized:
 * chunks with fewer live replicas are commanded first.
 */
final class RepairCoordinator implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RepairCoordinator.class);

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
    private final MetaConfig config;
    private final java.util.function.BooleanSupplier isLeader;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong commandIds = new AtomicLong(System.currentTimeMillis());
    private final Map<Long, Action> inflight = new ConcurrentHashMap<>();
    // (nodeId:chunkId) -> first time the node's inventory omitted a sealed chunk it should hold. A
    // just-sealed chunk can miss the in-flight inventory snapshot; only drop the replica if it stays
    // missing past config.replicaMissingGraceMs(), so churn can't trigger a false re-replication storm.
    private final Map<String, Long> replicaMissingSince = new ConcurrentHashMap<>();
    private final Set<ChunkId> chunksBeingRepaired = ConcurrentHashMap.newKeySet();
    private final Map<ReplicaKey, Long> recentlyCommittedReplicas = new ConcurrentHashMap<>();
    private volatile Thread scanThread;

    // A deleted file's record is kept as a DELETED tombstone (fencing a delayed CREATE replay from
    // resurrecting it) and reaped only after this window. INVARIANT: it must exceed the longest
    // possible create-replay delay — the client create-retry deadline (max(15s, callTimeoutMs), see
    // MetaClient) plus the meta<->ZK clock-skew the mtime-based sweep tolerates — and it must exceed
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

    private record ReplicaKey(ChunkId chunkId, int nodeId) {}

    RepairCoordinator(MetadataStore store, NodeRegistry registry, MetaConfig config,
                      java.util.function.BooleanSupplier isLeader) {
        this.store = store;
        this.registry = registry;
        this.config = config;
        this.isLeader = isLeader;
    }

    void start() {
        scanThread = Thread.ofVirtual().name("meta-repair-scan").start(this::scanLoop);
    }

    private long leaderSince;

    private void scanLoop() {
        while (!closed.get()) {
            try {
                Thread.sleep(config.repairScanIntervalMs());
                if (!isLeader.getAsBoolean()) {
                    // a standby must never run failure detection or repair: with no heartbeats
                    // routed to it, it would declare every node DEAD and persist that to ZK
                    leaderSince = 0;
                    continue;
                }
                if (leaderSince == 0) {
                    leaderSince = System.currentTimeMillis();
                }
                // settle period after acquiring leadership: nodes registered with the previous
                // leader need a lease cycle to re-register here, or every chunk looks
                // under-replicated and the scan issues spurious repairs
                if (System.currentTimeMillis() - leaderSince < config.leaseMs() + config.deadGraceMs()) {
                    continue;
                }
                registry.expireScan();
                scanOnce();
                store.sweepDeletedFiles(DELETED_TOMBSTONE_TTL_MS);
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
            issueReplicate(r.fileId(), r.file(), r.chunk());
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

    private void issueReplicate(FileId fileId, Records.FileRecord file, Records.ChunkRecord chunk) {
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
        log.info("repair: {} dead={} -> target={} (cmd {})", chunkId, deadNode, target.record.nodeId(), cmdId);
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

    /** Called from heartbeat handling with each node-reported command completion. */
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
        for (int attempt = 0; attempt < MetadataService.CAS_RETRIES; attempt++) {
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
        for (int attempt = 0; attempt < MetadataService.CAS_RETRIES; attempt++) {
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
                if (!known && !isRepairProtected(e.chunkId(), report.nodeId())) {
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
                        // A node can omit a just-sealed chunk from an in-flight inventory snapshot; under
                        // churn that race would falsely drop a healthy replica and trigger a re-replication
                        // storm that steals write bandwidth. Only treat it as lost if it stays missing past
                        // the grace (genuine loss is still caught, just one or two reports later).
                        Long firstMissing = replicaMissingSince.putIfAbsent(missKey, now);
                        long missingForMs = firstMissing == null ? 0 : now - firstMissing;
                        if (missingForMs >= config.replicaMissingGraceMs()) {
                            log.warn("node {} lost sealed chunk {} (missing >= {}ms) — dropping replica for re-repair",
                                    report.nodeId(), chunkId, config.replicaMissingGraceMs());
                            replicaMissingSince.remove(missKey);
                            applyDeleteConfirmed(fileId, chunkId, report.nodeId());
                        }
                    } else {
                        replicaMissingSince.remove(missKey); // reported now — clear any pending miss
                        if (entry.state() != ChunkState.SEALED) {
                            // A non-SEALED local state under a SEALED descriptor cannot serve reads or
                            // repair fetches. Drop it like a corrupt copy and delete the local bytes.
                            log.warn("node {} holds {} copy of sealed chunk {} — dropping replica "
                                    + "for re-repair", report.nodeId(), entry.state(), chunkId);
                            applyDeleteConfirmed(fileId, chunkId, report.nodeId());
                            registry.enqueue(report.nodeId(), new Messages.DeleteCmd(
                                    commandIds.incrementAndGet(), List.of(chunkId)));
                        } else if (entry.length() != c.length() || entry.crc() != c.crc()) {
                            log.warn("node {} holds corrupt sealed chunk {} (len {}/{} crc {}/{}) — "
                                            + "dropping replica for re-repair", report.nodeId(), chunkId,
                                    entry.length(), c.length(), entry.crc(), c.crc());
                            applyDeleteConfirmed(fileId, chunkId, report.nodeId());
                            // schedule physical deletion NOW: placement may legitimately re-pick this
                            // node as the add-repair target, and the corrupt bytes must be gone first
                            // (FIFO command delivery guarantees delete-before-replicate)
                            registry.enqueue(report.nodeId(), new Messages.DeleteCmd(
                                    commandIds.incrementAndGet(), List.of(chunkId)));
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("inventory reconciliation for node {} failed", report.nodeId(), e);
        }
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
    }
}
