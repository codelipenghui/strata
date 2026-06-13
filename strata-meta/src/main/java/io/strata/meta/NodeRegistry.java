package io.strata.meta;

import io.strata.common.Endpoint;
import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
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

        // CAS write; on conflict re-read once and retry — persistent conflict means another
        // leader is mutating this record, so the node should re-register with the real leader
        int version = existingVersion;
        boolean written = false;
        for (int attempt = 0; attempt < 3 && !written; attempt++) {
            written = store.putNode(record, version);
            if (!written) {
                var current = store.getNode(nodeId);
                version = current.map(MetadataStore.Versioned::version).orElse(-1);
            }
        }
        if (!written) {
            throw new ScpException(ErrorCode.NOT_LEADER, "node record contention — retry registration");
        }
        int committedVersion = store.getNode(nodeId).map(MetadataStore.Versioned::version).orElse(version + 1);

        LiveNode n = live.computeIfAbsent(nodeId, k -> new LiveNode());
        n.record = record;
        n.recordVersion = committedVersion;
        n.sessionEpoch = sessionCounter.incrementAndGet();
        n.leaseUntil = System.currentTimeMillis() + config.leaseMs();
        n.freeBytes = capacity;
        log.info("node {} registered ({} @ {}), session {}", nodeId, msg.host(), msg.endpoints(), n.sessionEpoch);
        return new Messages.RegisterResp(nodeId, n.sessionEpoch, config.heartbeatIntervalMs(), config.leaseMs());
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
        if (n == null || n.record.incMsb() != msg.incMsb() || n.record.incLsb() != msg.incLsb()) {
            throw new ScpException(ErrorCode.NOT_REGISTERED, "node " + msg.nodeId());
        }
        if (n.sessionEpoch != msg.sessionEpoch()) {
            throw new ScpException(ErrorCode.LEASE_EXPIRED, "stale session " + msg.sessionEpoch());
        }
        long now = System.currentTimeMillis();
        long newLeaseUntil = now + config.leaseMs();
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
            if (!markState(n, Records.NodeState.REGISTERED)
                    && n.record.state() != Records.NodeState.REGISTERED) {
                throw new ScpException(ErrorCode.LEASE_EXPIRED,
                        "node " + msg.nodeId() + " state is " + n.record.state());
            }
        }
        n.leaseUntil = newLeaseUntil;
        for (Messages.CompletedCommand c : msg.completedCommands()) {
            onCompleted.accept(msg.nodeId(), c);
        }
        List<Messages.Command> out = new ArrayList<>();
        for (int i = 0; i < MAX_COMMANDS_PER_HEARTBEAT; i++) {
            Messages.Command c = n.pending.poll();
            if (c == null) break;
            out.add(c);
        }
        return new Messages.HeartbeatResp(newLeaseUntil, out);
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
            if (n.record.state() == Records.NodeState.REGISTERED
                    && n.leaseUntil + config.deadGraceMs() < now) {
                if (markState(n, Records.NodeState.DEAD)) {
                    n.pending.clear();
                    newlyDead.add(n.record.nodeId());
                    log.warn("node {} declared DEAD (lease expired {}ms ago)",
                            n.record.nodeId(), now - n.leaseUntil);
                }
            }
        }
        return newlyDead;
    }

    private boolean markState(LiveNode n, Records.NodeState state) {
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
    }

    List<LiveNode> aliveNodes() {
        long now = System.currentTimeMillis();
        List<LiveNode> out = new ArrayList<>();
        for (LiveNode n : live.values()) {
            if (n.alive(now)) out.add(n);
        }
        return out;
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
        return n != null
                && n.record.incMsb() == incMsb
                && n.record.incLsb() == incLsb
                && n.sessionEpoch == sessionEpoch
                && n.alive(System.currentTimeMillis());
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
