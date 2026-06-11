package io.strata.meta;

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
    private static final int REPLICATION_FACTOR = 3;

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
    private final Set<ChunkId> chunksBeingRepaired = ConcurrentHashMap.newKeySet();
    private volatile Thread scanThread;

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
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                if (!closed.get()) {
                    log.warn("repair scan failed", e);
                }
            }
        }
    }

    /** One reconciliation pass over all files. Idempotent; safe to run concurrently with serving. */
    void scanOnce() throws Exception {
        sweepStuckCommands();
        record Repair(FileId fileId, Records.FileRecord file, Records.ChunkRecord chunk, int liveReplicas) {}
        List<Repair> repairs = new ArrayList<>();

        for (FileId fileId : store.listFiles()) {
            Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(fileId);
            if (opt.isEmpty()) continue;
            Records.FileRecord file = opt.get().value();

            if (file.state() == Records.FileState.DELETING) {
                driveDeletion(file, opt.get().version());
                continue;
            }
            for (Records.ChunkRecord chunk : file.chunks()) {
                if (chunk.state() != ChunkState.SEALED) continue; // open chunks belong to their writer
                ChunkId chunkId = file.chunkId(chunk.index());
                if (chunksBeingRepaired.contains(chunkId)) continue;
                int live = 0;
                for (int nodeId : chunk.replicas()) {
                    if (!registry.isDead(nodeId)) live++;
                }
                if (live < REPLICATION_FACTOR && live > 0) {
                    repairs.add(new Repair(fileId, file, chunk, live));
                } else if (live == 0) {
                    log.error("chunk {} has NO live replicas — data loss exposure, cannot repair", chunkId);
                }
            }
        }

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
        if (deadNode < 0 && chunk.replicas().size() >= REPLICATION_FACTOR) {
            return; // fully replicated on live nodes — nothing to do
        }
        // deadNode >= 0: swap the dead member out. deadNode < 0 with a short replica list:
        // ADD a member — happens after a corrupt/missing replica was dropped from the
        // descriptor (inventory reconciliation); without add-mode the chunk would be stranded
        // under-replicated with no dead node to replace.

        List<NodeRegistry.LiveNode> targets;
        try {
            targets = Placement.choose(registry, 1, file.mediaClass(), existing, usedHosts);
        } catch (Exception e) {
            log.warn("no repair target for {}: {}", chunkId, e.getMessage());
            return;
        }
        NodeRegistry.LiveNode target = targets.get(0);
        long cmdId = commandIds.incrementAndGet();
        inflight.put(cmdId, new ReplicateAction(fileId, chunkId, deadNode, target.record.nodeId(),
                System.currentTimeMillis()));
        chunksBeingRepaired.add(chunkId);
        registry.enqueue(target.record.nodeId(),
                new Messages.ReplicateCmd(cmdId, chunkId, sources, (byte) 1, chunk.crc(), chunk.length()));
        log.info("repair: {} dead={} -> target={} (cmd {})", chunkId, deadNode, target.record.nodeId(), cmdId);
    }

    private void driveDeletion(Records.FileRecord file, int version) throws Exception {
        for (Records.ChunkRecord chunk : file.chunks()) {
            ChunkId chunkId = file.chunkId(chunk.index());
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
            store.deleteFile(file.fileId(), version);
            log.info("file {} fully deleted", file.fileId());
        }
    }

    /** Called from heartbeat handling with each node-reported command completion. */
    void onCommandCompleted(Messages.CompletedCommand completion) {
        Action action = inflight.remove(completion.commandId());
        if (action == null) return;
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
        for (int attempt = 0; attempt < 5; attempt++) {
            Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(r.fileId());
            if (opt.isEmpty()) return;
            Records.FileRecord file = opt.get().value();
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
                } else if (r.deadNode() < 0 && c.replicas().size() < REPLICATION_FACTOR) {
                    List<Integer> grown = new ArrayList<>(c.replicas());
                    grown.add(r.targetNode());
                    chunks.set(i, new Records.ChunkRecord(c.index(), c.state(), c.length(), c.crc(),
                            c.writeEpoch(), grown));
                    changed = true;
                }
            }
            if (!changed) return;
            Records.FileRecord updated = new Records.FileRecord(file.fileId(), file.fileKind(),
                    file.mediaClass(), file.ackPolicy(), file.ownerTag(), file.state(),
                    file.createdAtMs(), chunks);
            if (store.updateFile(updated, opt.get().version())) {
                log.info("descriptor swap: {} {} -> {}", r.chunkId(), r.deadNode(), r.targetNode());
                return;
            }
        }
        log.warn("descriptor swap for {} kept failing CAS — next scan reconciles", r.chunkId());
    }

    private void applyDeleteConfirmed(FileId fileId, ChunkId chunkId, int nodeId) throws Exception {
        for (int attempt = 0; attempt < 5; attempt++) {
            Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(fileId);
            if (opt.isEmpty()) return;
            Records.FileRecord file = opt.get().value();
            List<Records.ChunkRecord> chunks = new ArrayList<>();
            for (Records.ChunkRecord c : file.chunks()) {
                if (file.chunkId(c.index()).equals(chunkId)) {
                    List<Integer> replicas = new ArrayList<>(c.replicas());
                    replicas.remove(Integer.valueOf(nodeId));
                    if (!replicas.isEmpty()) {
                        chunks.add(new Records.ChunkRecord(c.index(), c.state(), c.length(), c.crc(),
                                c.writeEpoch(), replicas));
                    } // empty -> chunk record dropped
                } else {
                    chunks.add(c);
                }
            }
            Records.FileRecord updated = new Records.FileRecord(file.fileId(), file.fileKind(),
                    file.mediaClass(), file.ackPolicy(), file.ownerTag(), file.state(),
                    file.createdAtMs(), chunks);
            if (chunks.isEmpty() && file.state() == Records.FileState.DELETING) {
                store.deleteFile(fileId, opt.get().version());
                log.info("file {} fully deleted", fileId);
                return;
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
            Map<ChunkId, Messages.InventoryEntry> reported = new java.util.HashMap<>();
            for (Messages.InventoryEntry e : report.entries()) {
                reported.put(e.chunkId(), e);
            }
            // orphan detection
            List<ChunkId> orphans = new ArrayList<>();
            for (Messages.InventoryEntry e : report.entries()) {
                Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(e.chunkId().fileId());
                boolean known = false;
                if (opt.isPresent()) {
                    for (Records.ChunkRecord c : opt.get().value().chunks()) {
                        if (c.index() == e.chunkId().index() && c.replicas().contains(report.nodeId())) {
                            known = true;
                            break;
                        }
                    }
                }
                if (!known) orphans.add(e.chunkId());
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
            for (FileId fileId : store.listFiles()) {
                Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(fileId);
                if (opt.isEmpty() || opt.get().value().state() == Records.FileState.DELETING) continue;
                Records.FileRecord file = opt.get().value();
                for (Records.ChunkRecord c : file.chunks()) {
                    if (c.state() != ChunkState.SEALED || !c.replicas().contains(report.nodeId())) {
                        continue;
                    }
                    ChunkId chunkId = file.chunkId(c.index());
                    Messages.InventoryEntry entry = reported.get(chunkId);
                    if (entry == null) {
                        log.warn("node {} lost sealed chunk {} — dropping replica for re-repair",
                                report.nodeId(), chunkId);
                        applyDeleteConfirmed(fileId, chunkId, report.nodeId());
                    } else if (entry.state() == ChunkState.SEALED
                            && (entry.length() != c.length() || entry.crc() != c.crc())) {
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
                    // entry.state() == OPEN while descriptor is SEALED: seal straggler (§9.2
                    // convergent state) — its prefix is valid; FETCH from it fails harmlessly
                }
            }
        } catch (Exception e) {
            log.warn("inventory reconciliation for node {} failed", report.nodeId(), e);
        }
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
