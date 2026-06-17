package io.strata.meta;

import io.strata.common.Endpoint;
import io.strata.common.ErrorCode;
import io.strata.common.FailureInjector;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.proto.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * Liveness and command delivery (tech design §7.2 detection, §10.4 command flow).
 * Registrations persist in the MetadataStore; leases and pending commands are leader-memory —
 * a metadata failover resets leases, which is why dead-grace must exceed failover time.
 */
final class NodeRegistry {
    private static final Logger log = LoggerFactory.getLogger(NodeRegistry.class);
    private static final int MAX_COMMANDS_PER_HEARTBEAT = 16;

    static final class LiveNode {
        volatile Records.NodeRecord record;
        volatile int recordVersion; // znode version for CAS writes
        volatile long sessionEpoch;
        volatile long leaseUntil;
        volatile long freeBytes;
        final Queue<Messages.Command> pending = new ConcurrentLinkedQueue<>();
        // (record, recordVersion) is a coupled invariant updated by a read-modify-write from the
        // repair-scan thread, heartbeat handler threads, and register. ReentrantLock (not
        // synchronized) so the ZK I/O inside the critical section does not pin a virtual thread.
        final java.util.concurrent.locks.ReentrantLock lock = new java.util.concurrent.locks.ReentrantLock();
        // Session gate serializes long heartbeat completion processing against same-node
        // re-registration without holding the state lock across metadata-store callbacks.
        final ReentrantReadWriteLock sessionGate = new ReentrantReadWriteLock();

        boolean alive(long now) {
            return record.state() == Records.NodeState.REGISTERED && leaseUntil >= now;
        }
    }

    private final MetadataStore store;
    private final MetaConfig config;
    private final Map<Integer, LiveNode> live = new ConcurrentHashMap<>();
    private final AtomicLong sessionCounter = new AtomicLong(System.currentTimeMillis());

    NodeRegistry(MetadataStore store, MetaConfig config) throws Exception {
        this.store = store;
        this.config = config;
        // warm the in-memory view from persisted registrations (their leases start expired)
        for (MetadataStore.Versioned<Records.NodeRecord> v : store.listNodes()) {
            LiveNode n = new LiveNode();
            n.record = v.value();
            n.recordVersion = v.version();
            n.sessionEpoch = -1;
            n.leaseUntil = 0;
            live.put(v.value().nodeId(), n);
        }
    }

    public record LivenessCounts(int alive, int suspect, int dead) {}

    /**
     * Best-effort liveness snapshot for metrics: ALIVE = lease valid; SUSPECT = lease expired but
     * within the dead-grace window (or awaiting the next expireScan); DEAD = persisted dead. Reads
     * leaseUntil without the per-node lock — fine for a gauge, which only needs an approximate view.
     */
    public LivenessCounts livenessCounts() {
        long now = System.currentTimeMillis();
        int alive = 0, suspect = 0, dead = 0;
        for (LiveNode n : live.values()) {
            if (n.record.state() == Records.NodeState.DEAD) {
                dead++;
            } else if (n.record.state() == Records.NodeState.REGISTERED) {
                if (n.leaseUntil >= now) {
                    alive++;
                } else {
                    suspect++;
                }
            }
        }
        return new LivenessCounts(alive, suspect, dead);
    }

    // ReentrantLock, not synchronized: registration does ZK I/O and runs on server virtual threads
    private final java.util.concurrent.locks.ReentrantLock registerLock =
            new java.util.concurrent.locks.ReentrantLock();

    Messages.RegisterResp register(Messages.RegisterNode msg) throws Exception {
        registerLock.lock();
        try {
            return registerLocked(msg);
        } finally {
            registerLock.unlock();
        }
    }

    private Messages.RegisterResp registerLocked(Messages.RegisterNode msg) throws Exception {
        validateRegistration(msg);
        // identity = incarnation persisted on the node's volumes; same incarnation -> same nodeId
        Integer nodeIdFound = null;
        int existingVersion = -1;
        for (LiveNode n : live.values()) {
            if (n.record.incMsb() == msg.incMsb() && n.record.incLsb() == msg.incLsb()) {
                nodeIdFound = n.record.nodeId();
                existingVersion = n.recordVersion;
                break;
            }
        }
        if (nodeIdFound == null) {
            // not in this leader's memory — a node that registered with a PREVIOUS leader after
            // this instance booted. The persistent store is the identity source of truth:
            // re-registration across metadata failover must keep the nodeId stable.
            for (MetadataStore.Versioned<Records.NodeRecord> v : store.listNodes()) {
                if (v.value().incMsb() == msg.incMsb() && v.value().incLsb() == msg.incLsb()) {
                    nodeIdFound = v.value().nodeId();
                    existingVersion = v.version();
                    break;
                }
            }
        }
        int nodeId = nodeIdFound != null ? nodeIdFound : store.nextNodeId();
        long capacity = msg.capacities().isEmpty() ? 0 : msg.capacities().get(0).capacityBytes();
        Records.NodeRecord record = new Records.NodeRecord(nodeId, msg.incMsb(), msg.incLsb(),
                msg.endpoints(), msg.zone(), msg.rack(), msg.host(), capacity, Records.NodeState.REGISTERED);

        LiveNode existing = live.get(nodeId);
        Lock sessionWrite = existing == null ? null : existing.sessionGate.writeLock();
        if (sessionWrite != null) {
            sessionWrite.lock();
        }
        LiveNode n = existing;
        long sessionEpoch;
        try {
            int committedVersion = putNodeWithRetry(record, existingVersion);
            FailureInjector.point("meta.register.afterPersistBeforePublish");
            if (n == null) {
                n = live.computeIfAbsent(nodeId, k -> new LiveNode());
            }
            n.lock.lock();
            try {
                // publish the new session's (record, recordVersion) pair atomically w.r.t. markState
                n.record = record;
                n.recordVersion = committedVersion;
                n.sessionEpoch = sessionCounter.incrementAndGet();
                n.leaseUntil = System.currentTimeMillis() + config.leaseMs();
                n.freeBytes = capacity;
                sessionEpoch = n.sessionEpoch;
            } finally {
                n.lock.unlock();
            }
        } finally {
            if (sessionWrite != null) {
                sessionWrite.unlock();
            }
        }
        log.info("node {} registered ({} @ {}), session {}", nodeId, msg.host(), msg.endpoints(), sessionEpoch);
        return new Messages.RegisterResp(nodeId, sessionEpoch, config.heartbeatIntervalMs(), config.leaseMs());
    }

    private int putNodeWithRetry(Records.NodeRecord record, int initialVersion) throws Exception {
        // CAS write; on conflict re-read once and retry — persistent conflict means another
        // leader is mutating this record, so the node should re-register with the real leader
        int version = initialVersion;
        boolean written = false;
        for (int attempt = 0; attempt < 3 && !written; attempt++) {
            written = store.putNode(record, version);
            if (!written) {
                var current = store.getNode(record.nodeId());
                version = current.map(MetadataStore.Versioned::version).orElse(-1);
            }
        }
        if (!written) {
            throw new ScpException(ErrorCode.NOT_LEADER, "node record contention — retry registration");
        }
        return store.getNode(record.nodeId()).map(MetadataStore.Versioned::version).orElse(version + 1);
    }

    private static void validateRegistration(Messages.RegisterNode msg) {
        if (msg.endpoints().isEmpty() || msg.endpoints().stream().anyMatch(e -> e == null || e.isBlank())) {
            throw new ScpException(ErrorCode.PRECONDITION_FAILED, "node registration requires an endpoint");
        }
        for (String endpoint : msg.endpoints()) {
            Endpoint.parse(endpoint, "node endpoint", ErrorCode.PRECONDITION_FAILED);
        }
        if (msg.zone() == null || msg.zone().isBlank()
                || msg.rack() == null || msg.rack().isBlank()
                || msg.host() == null || msg.host().isBlank()) {
            throw new ScpException(ErrorCode.PRECONDITION_FAILED, "node registration requires zone, rack, and host");
        }
        if (msg.capacities().isEmpty()) {
            throw new ScpException(ErrorCode.PRECONDITION_FAILED, "node registration requires capacity");
        }
        if (msg.capacities().get(0).capacityBytes() <= 0) {
            throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                    "node capacity must be positive: " + msg.capacities().get(0).capacityBytes());
        }
    }

    Messages.HeartbeatResp heartbeat(Messages.NodeHeartbeat msg,
                                     BiConsumer<Integer, Messages.CompletedCommand> onCompleted) {
        LiveNode n = live.get(msg.nodeId());
        if (n == null) {
            throw new ScpException(ErrorCode.NOT_REGISTERED, "node " + msg.nodeId());
        }
        List<Messages.Command> out = new ArrayList<>();
        Lock sessionRead = n.sessionGate.readLock();
        sessionRead.lock();
        try {
            // Renew the lease (and, if needed, resurrect) under n.lock so the (state, leaseUntil) pair
            // is published atomically. Otherwise expireScan could observe a just-resurrected REGISTERED
            // node still carrying its stale expired lease and immediately re-declare it DEAD.
            n.lock.lock();
            try {
                validateCurrentSessionLocked(n, msg);
                if (!msg.usages().isEmpty()) {
                    Messages.StorageUsage usage = msg.usages().get(0);
                    if (usage.usedBytes() < 0 || usage.freeBytes() < 0) {
                        throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                                "negative storage usage from node " + msg.nodeId());
                    }
                    n.freeBytes = usage.freeBytes();
                }
                if (n.record.state() == Records.NodeState.DEAD) {
                    // it came back within the same incarnation — resurrect (repair may already be
                    // running; descriptor CAS keeps both sides idempotent)
                    if (!markState(n, Records.NodeState.REGISTERED) // reentrant on n.lock
                            && n.record.state() != Records.NodeState.REGISTERED) {
                        throw new ScpException(ErrorCode.LEASE_EXPIRED,
                                "node " + msg.nodeId() + " state is " + n.record.state());
                    }
                }
                n.leaseUntil = System.currentTimeMillis() + config.leaseMs();
            } finally {
                n.lock.unlock();
            }

            FailureInjector.point("meta.heartbeat.beforeCompletions");
            for (Messages.CompletedCommand c : msg.completedCommands()) {
                onCompleted.accept(msg.nodeId(), c);
            }

            long newLeaseUntil;
            n.lock.lock();
            try {
                validateCurrentSessionLocked(n, msg);
                newLeaseUntil = System.currentTimeMillis() + config.leaseMs();
                n.leaseUntil = newLeaseUntil;
                for (int i = 0; i < MAX_COMMANDS_PER_HEARTBEAT; i++) {
                    Messages.Command c = n.pending.poll();
                    if (c == null) break;
                    out.add(c);
                }
            } finally {
                n.lock.unlock();
            }
            return new Messages.HeartbeatResp(newLeaseUntil, out);
        } finally {
            sessionRead.unlock();
        }
    }

    private static void validateCurrentSessionLocked(LiveNode n, Messages.NodeHeartbeat msg) {
        if (n.record.incMsb() != msg.incMsb() || n.record.incLsb() != msg.incLsb()) {
            throw new ScpException(ErrorCode.NOT_REGISTERED, "node " + msg.nodeId());
        }
        if (n.sessionEpoch != msg.sessionEpoch()) {
            throw new ScpException(ErrorCode.LEASE_EXPIRED, "stale session " + msg.sessionEpoch());
        }
    }

    void enqueue(int nodeId, Messages.Command cmd) {
        LiveNode n = live.get(nodeId);
        if (n != null) {
            n.pending.add(cmd);
        }
    }

    void removePending(int nodeId, Predicate<Messages.Command> predicate) {
        LiveNode n = live.get(nodeId);
        if (n != null) {
            n.pending.removeIf(predicate);
        }
    }

    /** Marks nodes whose lease expired past the dead grace as DEAD; returns newly dead node ids. */
    List<Integer> expireScan() {
        long now = System.currentTimeMillis();
        List<Integer> newlyDead = new ArrayList<>();
        for (LiveNode n : live.values()) {
            // cheap unlocked pre-filter; the authoritative decision re-validates under n.lock
            if (n.record.state() == Records.NodeState.REGISTERED
                    && n.leaseUntil + config.deadGraceMs() < now) {
                if (declareDeadIfStillExpired(n, now)) {
                    n.pending.clear();
                    newlyDead.add(n.record.nodeId());
                    log.warn("node {} declared DEAD (lease expired {}ms ago)",
                            n.record.nodeId(), now - n.leaseUntil);
                }
            }
        }
        return newlyDead;
    }

    /**
     * Transitions a node to DEAD only if it is STILL an expired REGISTERED node when re-checked
     * under n.lock. The expireScan pre-filter reads (state, leaseUntil) WITHOUT the lock; a
     * heartbeat can renew the lease or resurrect the node in the window before this runs, so the
     * decision must be re-validated atomically with the CAS — otherwise a freshly-heartbeated live
     * node could be declared DEAD (TOCTOU, same shape as the idle-connection eviction race).
     */
    private boolean declareDeadIfStillExpired(LiveNode n, long now) {
        Lock sessionWrite = n.sessionGate.writeLock();
        sessionWrite.lock();
        try {
            n.lock.lock();
            try {
                long recheckNow = System.currentTimeMillis();
                if (n.record.state() != Records.NodeState.REGISTERED
                        || n.leaseUntil + config.deadGraceMs() >= recheckNow) {
                    return false;
                }
                return markState(n, Records.NodeState.DEAD);
            } finally {
                n.lock.unlock();
            }
        } finally {
            sessionWrite.unlock();
        }
    }

    private boolean markState(LiveNode n, Records.NodeState state) {
        // Serialize the (record, recordVersion) read-modify-write per node: this runs concurrently
        // from the repair-scan thread (expireScan), heartbeat handler threads (DEAD->REGISTERED
        // resurrection), and register. Without the lock the version increment can tear or the pair
        // can be written half-from-one-update, leaving recordVersion out of step with the znode.
        n.lock.lock();
        try {
            Records.NodeRecord updated = n.record.withState(state);
            try {
                if (store.putNode(updated, n.recordVersion)) {
                    n.recordVersion = n.recordVersion + 1;
                    n.record = updated;
                    return true;
                } else {
                    // CAS lost: another leader owns this record now (e.g. we are deposed and it just
                    // re-registered the node) — adopt the store's view instead of overwriting it
                    store.getNode(n.record.nodeId()).ifPresent(v -> {
                        n.record = v.value();
                        n.recordVersion = v.version();
                    });
                    log.warn("node {} state transition to {} lost CAS — adopted store state {}",
                            updated.nodeId(), state, n.record.state());
                    return false;
                }
            } catch (Exception e) {
                log.warn("persisting node {} state {} failed", n.record.nodeId(), state, e);
                return false;
            }
        } finally {
            n.lock.unlock();
        }
    }

    List<LiveNode> aliveNodes() {
        long now = System.currentTimeMillis();
        List<LiveNode> out = new ArrayList<>();
        for (LiveNode n : live.values()) {
            if (n.alive(now)) out.add(n);
        }
        return out;
    }

    /**
     * Nodes eligible to hold a replica for {@code namespace}. The per-namespace placement hook: today
     * every alive node is eligible (namespace-agnostic), but this is where a future per-tenant
     * affinity/isolation policy (e.g. namespace pinned to a node set) would filter candidates.
     */
    List<LiveNode> candidatesFor(StrataNamespace namespace) {
        return aliveNodes();
    }

    boolean isDead(int nodeId) {
        LiveNode n = live.get(nodeId);
        return n == null || n.record.state() == Records.NodeState.DEAD;
    }

    boolean isAlive(int nodeId) {
        LiveNode n = live.get(nodeId);
        return n != null && n.alive(System.currentTimeMillis());
    }

    boolean isCurrentSession(int nodeId, long incMsb, long incLsb, long sessionEpoch) {
        LiveNode n = live.get(nodeId);
        if (n == null) {
            return false;
        }
        Lock sessionRead = n.sessionGate.readLock();
        sessionRead.lock();
        try {
            n.lock.lock();
            try {
                return n.record.incMsb() == incMsb
                        && n.record.incLsb() == incLsb
                        && n.sessionEpoch == sessionEpoch
                        && n.alive(System.currentTimeMillis());
            } finally {
                n.lock.unlock();
            }
        } finally {
            sessionRead.unlock();
        }
    }

    String endpointOf(int nodeId) {
        LiveNode n = liveNodeOrPersisted(nodeId);
        return n == null ? "" : n.record.endpoint();
    }

    String hostOf(int nodeId) {
        LiveNode n = liveNodeOrPersisted(nodeId);
        return n == null ? null : n.record.host();
    }

    Messages.Replica replicaOf(int nodeId) {
        return new Messages.Replica(nodeId, endpointOf(nodeId));
    }

    private LiveNode liveNodeOrPersisted(int nodeId) {
        LiveNode n = live.get(nodeId);
        if (n != null) {
            return n;
        }
        try {
            var persisted = store.getNode(nodeId);
            if (persisted.isEmpty()) {
                return null;
            }
            LiveNode loaded = new LiveNode();
            loaded.record = persisted.get().value();
            loaded.recordVersion = persisted.get().version();
            loaded.sessionEpoch = -1;
            loaded.leaseUntil = 0;
            loaded.freeBytes = loaded.record.capacityBytes();
            LiveNode raced = live.putIfAbsent(nodeId, loaded);
            return raced != null ? raced : loaded;
        } catch (Exception e) {
            log.warn("loading node {} from metadata store failed", nodeId, e);
            return null;
        }
    }
}
