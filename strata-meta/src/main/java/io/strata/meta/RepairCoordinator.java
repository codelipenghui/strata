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

    private sealed interface Action permits ReplicateAction, DeleteAction {}

    private record ReplicateAction(FileId fileId, ChunkId chunkId, int deadNode, int targetNode)
            implements Action {}

    private record DeleteAction(FileId fileId, ChunkId chunkId, int nodeId) implements Action {}

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
        if (deadNode < 0 || sources.isEmpty()) return;

        List<NodeRegistry.LiveNode> targets;
        try {
            targets = Placement.choose(registry, 1, file.mediaClass(), existing, usedHosts);
        } catch (Exception e) {
            log.warn("no repair target for {}: {}", chunkId, e.getMessage());
            return;
        }
        NodeRegistry.LiveNode target = targets.get(0);
        long cmdId = commandIds.incrementAndGet();
        inflight.put(cmdId, new ReplicateAction(fileId, chunkId, deadNode, target.record.nodeId()));
        chunksBeingRepaired.add(chunkId);
        registry.enqueue(target.record.nodeId(),
                new Messages.ReplicateCmd(cmdId, chunkId, sources, (byte) 1, chunk.crc(), chunk.length()));
        log.info("repair: {} dead={} -> target={} (cmd {})", chunkId, deadNode, target.record.nodeId(), cmdId);
    }

    private void driveDeletion(Records.FileRecord file, int version) throws Exception {
        boolean allConfirmed = true;
        for (Records.ChunkRecord chunk : file.chunks()) {
            ChunkId chunkId = file.chunkId(chunk.index());
            for (int nodeId : chunk.replicas()) {
                allConfirmed = false;
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
                    inflight.put(cmdId, new DeleteAction(file.fileId(), chunkId, nodeId));
                    registry.enqueue(nodeId, new Messages.DeleteCmd(cmdId, List.of(chunkId)));
                }
            }
        }
        if (file.chunks().isEmpty() || allConfirmed && file.chunks().stream().allMatch(c -> c.replicas().isEmpty())) {
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
        for (int attempt = 0; attempt < 5; attempt++) {
            Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(r.fileId());
            if (opt.isEmpty()) return;
            Records.FileRecord file = opt.get().value();
            List<Records.ChunkRecord> chunks = new ArrayList<>(file.chunks());
            boolean changed = false;
            for (int i = 0; i < chunks.size(); i++) {
                Records.ChunkRecord c = chunks.get(i);
                if (file.chunkId(c.index()).equals(r.chunkId()) && c.replicas().contains(r.deadNode())) {
                    chunks.set(i, c.withReplicaSwapped(r.deadNode(), r.targetNode()));
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
            Set<ChunkId> reported = new HashSet<>();
            for (Messages.InventoryEntry e : report.entries()) {
                reported.add(e.chunkId());
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
            // missing detection (sealed chunks this node should hold but didn't report)
            for (FileId fileId : store.listFiles()) {
                Optional<MetadataStore.Versioned<Records.FileRecord>> opt = store.getFile(fileId);
                if (opt.isEmpty() || opt.get().value().state() == Records.FileState.DELETING) continue;
                Records.FileRecord file = opt.get().value();
                for (Records.ChunkRecord c : file.chunks()) {
                    if (c.state() == ChunkState.SEALED && c.replicas().contains(report.nodeId())
                            && !reported.contains(file.chunkId(c.index()))) {
                        log.warn("node {} lost sealed chunk {} — dropping replica for re-repair",
                                report.nodeId(), file.chunkId(c.index()));
                        applyDeleteConfirmed(fileId, file.chunkId(c.index()), report.nodeId());
                    }
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
