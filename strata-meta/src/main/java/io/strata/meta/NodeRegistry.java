package io.strata.meta;

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
import java.util.function.Consumer;

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
        for (Records.NodeRecord r : store.listNodes()) {
            LiveNode n = new LiveNode();
            n.record = r;
            n.sessionEpoch = -1;
            n.leaseUntil = 0;
            live.put(r.nodeId(), n);
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
        // identity = incarnation persisted on the node's volumes; same incarnation -> same nodeId
        Records.NodeRecord existing = null;
        for (LiveNode n : live.values()) {
            if (n.record.incMsb() == msg.incMsb() && n.record.incLsb() == msg.incLsb()) {
                existing = n.record;
                break;
            }
        }
        int nodeId = existing != null ? existing.nodeId() : store.nextNodeId();
        byte mediaClass = msg.capacities().isEmpty() ? 0 : msg.capacities().get(0).mediaClass();
        long capacity = msg.capacities().isEmpty() ? 0 : msg.capacities().get(0).capacityBytes();
        Records.NodeRecord record = new Records.NodeRecord(nodeId, msg.incMsb(), msg.incLsb(),
                msg.endpoints(), msg.zone(), msg.rack(), msg.host(), mediaClass, capacity,
                Records.NodeState.REGISTERED);
        store.putNode(record);

        LiveNode n = live.computeIfAbsent(nodeId, k -> new LiveNode());
        n.record = record;
        n.sessionEpoch = sessionCounter.incrementAndGet();
        n.leaseUntil = System.currentTimeMillis() + config.leaseMs();
        n.freeBytes = capacity;
        log.info("node {} registered ({} @ {}), session {}", nodeId, msg.host(), msg.endpoints(), n.sessionEpoch);
        return new Messages.RegisterResp(nodeId, n.sessionEpoch, config.heartbeatIntervalMs(), config.leaseMs());
    }

    Messages.HeartbeatResp heartbeat(Messages.NodeHeartbeat msg,
                                     Consumer<Messages.CompletedCommand> onCompleted) {
        LiveNode n = live.get(msg.nodeId());
        if (n == null || n.record.incMsb() != msg.incMsb() || n.record.incLsb() != msg.incLsb()) {
            throw new ScpException(ErrorCode.NOT_REGISTERED, "node " + msg.nodeId());
        }
        if (n.sessionEpoch != msg.sessionEpoch()) {
            throw new ScpException(ErrorCode.LEASE_EXPIRED, "stale session " + msg.sessionEpoch());
        }
        long now = System.currentTimeMillis();
        n.leaseUntil = now + config.leaseMs();
        if (!msg.usages().isEmpty()) {
            n.freeBytes = msg.usages().get(0).freeBytes();
        }
        if (n.record.state() == Records.NodeState.DEAD) {
            // it came back within the same incarnation — resurrect (repair may already be
            // running; descriptor CAS keeps both sides idempotent)
            markState(n, Records.NodeState.REGISTERED);
        }
        for (Messages.CompletedCommand c : msg.completedCommands()) {
            onCompleted.accept(c);
        }
        List<Messages.Command> out = new ArrayList<>();
        for (int i = 0; i < MAX_COMMANDS_PER_HEARTBEAT; i++) {
            Messages.Command c = n.pending.poll();
            if (c == null) break;
            out.add(c);
        }
        return new Messages.HeartbeatResp(n.leaseUntil, out);
    }

    void enqueue(int nodeId, Messages.Command cmd) {
        LiveNode n = live.get(nodeId);
        if (n != null) {
            n.pending.add(cmd);
        }
    }

    /** Marks nodes whose lease expired past the dead grace as DEAD; returns newly dead node ids. */
    List<Integer> expireScan() {
        long now = System.currentTimeMillis();
        List<Integer> newlyDead = new ArrayList<>();
        for (LiveNode n : live.values()) {
            if (n.record.state() == Records.NodeState.REGISTERED
                    && n.leaseUntil + config.deadGraceMs() < now) {
                markState(n, Records.NodeState.DEAD);
                n.pending.clear();
                newlyDead.add(n.record.nodeId());
                log.warn("node {} declared DEAD (lease expired {}ms ago)",
                        n.record.nodeId(), now - n.leaseUntil);
            }
        }
        return newlyDead;
    }

    private void markState(LiveNode n, Records.NodeState state) {
        Records.NodeRecord updated = n.record.withState(state);
        try {
            store.putNode(updated);
        } catch (Exception e) {
            log.warn("persisting node {} state {} failed", n.record.nodeId(), state, e);
        }
        n.record = updated;
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

    String endpointOf(int nodeId) {
        LiveNode n = live.get(nodeId);
        return n == null ? "" : n.record.endpoint();
    }

    String hostOf(int nodeId) {
        LiveNode n = live.get(nodeId);
        return n == null ? null : n.record.host();
    }

    Messages.Replica replicaOf(int nodeId) {
        return new Messages.Replica(nodeId, endpointOf(nodeId));
    }
}
