package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FailureInjector;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.proto.Messages;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeRegistryTest {

    @AfterEach
    void clearInjector() {
        FailureInjector.reset();
    }

    @Test
    void constructorWarmsPersistedNodesAndCasLossAdoptsStoreState() throws Exception {
        FakeStore store = new FakeStore();
        Records.NodeRecord record = node(7, Records.NodeState.REGISTERED);
        store.nodes.put(record.nodeId(), new MetadataStore.Versioned<>(record, 4));
        store.failNextPutNode = true;

        NodeRegistry registry = new NodeRegistry(store, config());

        assertEquals("node-7:9000", registry.endpointOf(7));
        assertFalse(registry.isDead(7));

        assertEquals(List.of(), registry.expireScan());

        assertFalse(registry.isDead(7));
        NodeRegistry.LiveNode adopted = liveNodes(registry).get(7);
        assertEquals(5, adopted.recordVersion);
        assertEquals(Records.NodeState.REGISTERED, adopted.record.state());
    }

    @Test
    void placementCandidatesIncludePersistedSuspectNodesDuringDeadGrace() throws Exception {
        FakeStore store = new FakeStore();
        store.nodes.put(1, new MetadataStore.Versioned<>(node(1, Records.NodeState.REGISTERED), 1));
        store.nodes.put(2, new MetadataStore.Versioned<>(node(2, Records.NodeState.REGISTERED), 1));
        store.nodes.put(3, new MetadataStore.Versioned<>(node(3, Records.NodeState.REGISTERED), 1));
        MetaConfig config = new MetaConfig("unused", 0, 1, 1_000, 60_000, 1, 1);

        NodeRegistry registry = new NodeRegistry(store, config);

        assertEquals(0, registry.aliveNodes().size(), "warmed nodes should start as SUSPECT, not ALIVE");
        List<NodeRegistry.LiveNode> candidates = registry.candidatesFor(StrataNamespace.of("test"));
        assertEquals(List.of(1, 2, 3), candidates.stream()
                .map(n -> n.record.nodeId())
                .sorted()
                .toList());
        assertTrue(candidates.stream().allMatch(n -> n.freeBytes == n.record.capacityBytes()),
                "warmed placement candidates need non-zero capacity for weighted placement");
    }

    @Test
    void placementCandidatesExcludeNodesPastDeadGrace() throws Exception {
        FakeStore store = new FakeStore();
        MetaConfig config = new MetaConfig("unused", 0, 1, 1_000, 50, 1, 1);
        NodeRegistry registry = new NodeRegistry(store, config);
        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                101, 102, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(10)), 1, 0));
        NodeRegistry.LiveNode live = liveNodes(registry).get(registered.nodeId());

        live.leaseUntil = System.currentTimeMillis() - config.deadGraceMs() - 10;

        assertEquals(List.of(), registry.candidatesFor(StrataNamespace.of("test")));
    }

    @Test
    void registrationRequiresValidEndpointAndCapacity() throws Exception {
        NodeRegistry registry = new NodeRegistry(new FakeStore(), config());

        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of(), "z", "r", "h",
                List.of(new Messages.StorageCapacity(1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node:9000"), "z", "r", "h", List.of(), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(0)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node :9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("[::1:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node:not-a-port"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node:0"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node:9000"), "", "r", "h",
                List.of(new Messages.StorageCapacity(1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of(""), "z", "r", "h",
                List.of(new Messages.StorageCapacity(1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node:65536"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(1)), 1, 0)));
        assertEquals(1, registry.register(new Messages.RegisterNode(
                1, 2, List.of("[::1]:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(1)), 1, 0)).nodeId());
    }

    @Test
    void registrationFailsAfterPersistentCasContention() throws Exception {
        FakeStore store = new FakeStore();
        store.failPutNodeAttempts = 3;
        NodeRegistry registry = new NodeRegistry(store, config());

        ScpException e = assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(1)), 1, 0)));
        assertEquals(ErrorCode.NOT_LEADER, e.code());
    }

    @Test
    void registerReusesPersistedIdentityAndDrainsHeartbeatCommandsInBatches() throws Exception {
        FakeStore store = new FakeStore();
        Records.NodeRecord existing = new Records.NodeRecord(9, 11, 12, List.of("old:9000"), "z", "r", "old-host", 10, Records.NodeState.REGISTERED);
        store.nodes.put(9, new MetadataStore.Versioned<>(existing, 2));

        NodeRegistry registry = new NodeRegistry(store, config());

        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                11, 12, List.of("new:9000"), "z", "r", "new-host",
                List.of(new Messages.StorageCapacity(99)), 1, 0));
        assertEquals(9, registered.nodeId());
        assertEquals("new:9000", registry.endpointOf(9));
        assertEquals("new-host", registry.hostOf(9));

        for (int i = 0; i < 18; i++) {
            registry.enqueue(9, new Messages.DeleteCmd(i, List.of(new ChunkId(new FileId(i, i + 1), 0))));
        }
        Messages.HeartbeatResp first = registry.heartbeat(new Messages.NodeHeartbeat(
                9, 11, 12, registered.sessionEpoch(),
                List.of(new Messages.StorageUsage(1, 98)), 0, List.of()), ignoreCompletions());
        Messages.HeartbeatResp second = registry.heartbeat(new Messages.NodeHeartbeat(
                9, 11, 12, registered.sessionEpoch(),
                List.of(new Messages.StorageUsage(1, 98)), 0, List.of()), ignoreCompletions());

        assertEquals(16, first.commands().size());
        assertEquals(2, second.commands().size());
        assertTrue(registry.isAlive(9));
    }

    @Test
    void replicaLookupLoadsNodesPersistedAfterStandbyConstruction() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry standbyRegistry = new NodeRegistry(store, config());
        Records.NodeRecord late = new Records.NodeRecord(17, 171, 172,
                List.of("late:9000"), "z", "r", "late-host", 10, Records.NodeState.REGISTERED);
        store.nodes.put(17, new MetadataStore.Versioned<>(late, 4));

        assertEquals("late:9000", standbyRegistry.endpointOf(17));
        assertEquals("late-host", standbyRegistry.hostOf(17));
        assertEquals(new Messages.Replica(17, "late:9000"), standbyRegistry.replicaOf(17));
        assertFalse(standbyRegistry.isAlive(17));

        NodeRegistry.LiveNode cached = liveNodes(standbyRegistry).get(17);
        assertEquals(4, cached.recordVersion);
        assertTrue(cached.leaseUntil > 0);
        assertFalse(cached.alive(System.currentTimeMillis()));
    }

    @Test
    void heartbeatWithEmptyUsageReportsCompletionsAndUnknownHelpersAreEmpty() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                31, 32, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(10)), 1, 0));
        List<Integer> completedBy = new ArrayList<>();

        Messages.HeartbeatResp heartbeat = registry.heartbeat(new Messages.NodeHeartbeat(
                registered.nodeId(), 31, 32, registered.sessionEpoch(), List.of(), 0,
                List.of(new Messages.CompletedCommand(99, ErrorCode.OK.code))),
                (nodeId, incMsb, incLsb, sessionEpoch, ignored) -> {
                    completedBy.add(nodeId);
                    return CompletableFuture.completedFuture(null);
                });

        assertEquals(registered.nodeId(), completedBy.get(0));
        assertTrue(heartbeat.commands().isEmpty());
        assertEquals("", registry.endpointOf(999));
        assertEquals(null, registry.hostOf(999));
        assertEquals(new Messages.Replica(999, ""), registry.replicaOf(999));
        registry.enqueue(999, new Messages.DrainCmd(1));
    }

    @Test
    void heartbeatCompletionCallbackRunsOutsideNodeLock() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                33, 34, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(10)), 1, 0));
        NodeRegistry.LiveNode live = liveNodes(registry).get(registered.nodeId());
        AtomicReference<Boolean> callbackHeldNodeLock = new AtomicReference<>();

        registry.heartbeat(new Messages.NodeHeartbeat(
                        registered.nodeId(), 33, 34, registered.sessionEpoch(), List.of(), 0,
                        List.of(new Messages.CompletedCommand(99, ErrorCode.OK.code))),
                (nodeId, incMsb, incLsb, sessionEpoch, ignored) -> {
                    callbackHeldNodeLock.set(live.lock.isHeldByCurrentThread());
                    return CompletableFuture.completedFuture(null);
                });

        assertEquals(Boolean.FALSE, callbackHeldNodeLock.get(),
                "completion callbacks can do metadata I/O and must not run under the node lock");
    }

    @Test
    void heartbeatDefersCommandDeliveryUntilAsyncCompletionsFinish() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                37, 38, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(10)), 1, 0));
        CompletableFuture<Void> completion = new CompletableFuture<>();
        registry.enqueue(registered.nodeId(), new Messages.DrainCmd(123));

        Messages.HeartbeatResp blocked = registry.heartbeat(new Messages.NodeHeartbeat(
                        registered.nodeId(), 37, 38, registered.sessionEpoch(), List.of(), 0,
                        List.of(new Messages.CompletedCommand(99, ErrorCode.OK.code))),
                (nodeId, incMsb, incLsb, sessionEpoch, ignored) -> completion);

        assertTrue(blocked.commands().isEmpty(),
                "pending commands must not be delivered before reported completions apply");

        completion.complete(null);
        Messages.HeartbeatResp unblocked = registry.heartbeat(new Messages.NodeHeartbeat(
                        registered.nodeId(), 37, 38, registered.sessionEpoch(), List.of(), 0, List.of()),
                ignoreCompletions());

        assertEquals(List.of(new Messages.DrainCmd(123)), unblocked.commands());
    }

    @Test
    void heartbeatLeaseIsComputedAfterWaitingForNodeLock() throws Exception {
        FakeStore store = new FakeStore();
        MetaConfig config = config();
        NodeRegistry registry = new NodeRegistry(store, config);
        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                35, 36, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(10)), 1, 0));
        NodeRegistry.LiveNode live = liveNodes(registry).get(registered.nodeId());
        AtomicReference<Messages.HeartbeatResp> heartbeat = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread heartbeatThread = new Thread(() -> {
            try {
                heartbeat.set(registry.heartbeat(new Messages.NodeHeartbeat(
                        registered.nodeId(), 35, 36, registered.sessionEpoch(),
                        List.of(new Messages.StorageUsage(1, 9)), 0, List.of()), ignoreCompletions()));
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "lease-heartbeat");

        long beforeUnlock;
        live.lock.lock();
        try {
            heartbeatThread.start();
            waitFor(() -> live.lock.hasQueuedThread(heartbeatThread),
                    "heartbeat did not queue behind the per-node lock");
            Thread.sleep(100);
            beforeUnlock = System.currentTimeMillis();
        } finally {
            live.lock.unlock();
        }

        heartbeatThread.join(TimeUnit.SECONDS.toMillis(3));
        assertFalse(heartbeatThread.isAlive(), "heartbeat did not finish");
        assertEquals(null, failure.get());
        assertNotNull(heartbeat.get());
        assertTrue(heartbeat.get().leaseValidUntilMs() >= beforeUnlock + config.leaseMs(),
                "heartbeat returned a lease timestamp computed before it waited for the node lock");
    }

    @Test
    void heartbeatCompletionProcessingIsSerializedAgainstReregister() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        long incMsb = 37;
        long incLsb = 38;
        Messages.RegisterResp first = registry.register(new Messages.RegisterNode(
                incMsb, incLsb, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(10)), 1, 0));
        NodeRegistry.LiveNode live = liveNodes(registry).get(first.nodeId());
        CountDownLatch heartbeatBeforeCompletions = new CountDownLatch(1);
        CountDownLatch releaseHeartbeat = new CountDownLatch(1);
        AtomicReference<Throwable> heartbeatFailure = new AtomicReference<>();
        AtomicReference<Throwable> registerFailure = new AtomicReference<>();
        AtomicBoolean registerStarted = new AtomicBoolean();
        AtomicBoolean registerFinished = new AtomicBoolean();
        AtomicReference<Long> callbackSession = new AtomicReference<>();

        FailureInjector.arm("meta.heartbeat.beforeCompletions", p -> {
            heartbeatBeforeCompletions.countDown();
            releaseHeartbeat.await();
        });

        Thread heartbeatThread = new Thread(() -> {
            try {
                registry.heartbeat(new Messages.NodeHeartbeat(
                                first.nodeId(), incMsb, incLsb, first.sessionEpoch(), List.of(), 0,
                                List.of(new Messages.CompletedCommand(99, ErrorCode.OK.code))),
                        (nodeId, seenIncMsb, seenIncLsb, sessionEpoch, ignored) -> {
                            callbackSession.set(sessionEpoch);
                            return CompletableFuture.completedFuture(null);
                        });
            } catch (Throwable t) {
                heartbeatFailure.set(t);
            }
        }, "completion-heartbeat");
        heartbeatThread.start();
        assertTrue(heartbeatBeforeCompletions.await(3, TimeUnit.SECONDS),
                "heartbeat did not reach the completion window");

        Thread registerThread = new Thread(() -> {
            try {
                registerStarted.set(true);
                registry.register(new Messages.RegisterNode(
                        incMsb, incLsb, List.of("node-new:9000"), "z", "r", "h",
                        List.of(new Messages.StorageCapacity(10)), 1, 0));
                registerFinished.set(true);
            } catch (Throwable t) {
                registerFailure.set(t);
            }
        }, "reregister-during-completion");
        registerThread.start();
        waitFor(() -> registerFinished.get() || isWaiting(registerThread),
                "re-register did not attempt to publish while heartbeat was parked");
        assertTrue(registerStarted.get());
        assertFalse(registerFinished.get(), "re-register published a new session while heartbeat was processing completions");

        releaseHeartbeat.countDown();
        heartbeatThread.join(TimeUnit.SECONDS.toMillis(3));
        registerThread.join(TimeUnit.SECONDS.toMillis(3));
        assertFalse(heartbeatThread.isAlive(), "heartbeat did not finish");
        assertFalse(registerThread.isAlive(), "re-register did not finish");
        assertEquals(null, heartbeatFailure.get());
        assertEquals(null, registerFailure.get());
        assertEquals(first.sessionEpoch(), callbackSession.get(),
                "completion callback did not run under the session it had validated");
        assertTrue(registerFinished.get());
        assertTrue(live.sessionEpoch != first.sessionEpoch(), "re-register must publish a new session after heartbeat finishes");
    }

    @Test
    void heartbeatCompletionProcessingPreventsExpireScanFromDeclaringNodeDead() throws Exception {
        FakeStore store = new FakeStore();
        MetaConfig config = new MetaConfig("unused", 0, 1, 50, 0, 1, 1);
        NodeRegistry registry = new NodeRegistry(store, config);
        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                39, 40, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(10)), 1, 0));
        CountDownLatch heartbeatBeforeCompletions = new CountDownLatch(1);
        CountDownLatch releaseHeartbeat = new CountDownLatch(1);
        AtomicReference<Throwable> heartbeatFailure = new AtomicReference<>();
        AtomicReference<List<Integer>> expired = new AtomicReference<>();
        AtomicReference<Throwable> expireFailure = new AtomicReference<>();

        FailureInjector.arm("meta.heartbeat.beforeCompletions", p -> {
            heartbeatBeforeCompletions.countDown();
            releaseHeartbeat.await();
        });

        Thread heartbeatThread = new Thread(() -> {
            try {
                registry.heartbeat(new Messages.NodeHeartbeat(
                        registered.nodeId(), 39, 40, registered.sessionEpoch(), List.of(), 0,
                        List.of(new Messages.CompletedCommand(99, ErrorCode.OK.code))), ignoreCompletions());
            } catch (Throwable t) {
                heartbeatFailure.set(t);
            }
        }, "slow-completion-heartbeat");
        heartbeatThread.start();
        assertTrue(heartbeatBeforeCompletions.await(3, TimeUnit.SECONDS),
                "heartbeat did not reach the completion window");
        Thread.sleep(config.leaseMs() + 25L);

        Thread expireThread = new Thread(() -> {
            try {
                expired.set(registry.expireScan());
            } catch (Throwable t) {
                expireFailure.set(t);
            }
        }, "expire-during-completion");
        expireThread.start();
        waitFor(() -> expired.get() != null || isWaiting(expireThread),
                "expireScan did not attempt to inspect the node while heartbeat was parked");
        assertEquals(null, expired.get(), "expireScan declared a node dead while its heartbeat was still processing");

        releaseHeartbeat.countDown();
        heartbeatThread.join(TimeUnit.SECONDS.toMillis(3));
        expireThread.join(TimeUnit.SECONDS.toMillis(3));
        assertFalse(heartbeatThread.isAlive(), "heartbeat did not finish");
        assertFalse(expireThread.isAlive(), "expireScan did not finish");
        assertEquals(null, heartbeatFailure.get());
        assertEquals(null, expireFailure.get());
        assertEquals(List.of(), expired.get());
        assertTrue(registry.isAlive(registered.nodeId()));
    }

    @Test
    void heartbeatResurrectsDeadNodeWhenCasSucceeds() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                41, 42, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(10)), 1, 0));
        NodeRegistry.LiveNode live = liveNodes(registry).get(registered.nodeId());
        Records.NodeRecord dead = live.record.withState(Records.NodeState.DEAD);
        live.record = dead;
        store.nodes.put(registered.nodeId(), new MetadataStore.Versioned<>(dead, live.recordVersion));

        registry.heartbeat(new Messages.NodeHeartbeat(registered.nodeId(), 41, 42, registered.sessionEpoch(),
                List.of(new Messages.StorageUsage(1, 9)), 0, List.of()), ignoreCompletions());

        assertTrue(registry.isAlive(registered.nodeId()));
        assertEquals(Records.NodeState.REGISTERED, live.record.state());
    }

    @Test
    void deadHeartbeatMustNotExtendLeaseWhenResurrectionCasLosesToDeadStoreState() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());

        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                21, 22, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(10)), 1, 0));
        NodeRegistry.LiveNode live = liveNodes(registry).get(registered.nodeId());
        Records.NodeRecord dead = live.record.withState(Records.NodeState.DEAD);
        live.record = dead;
        store.nodes.put(registered.nodeId(), new MetadataStore.Versioned<>(dead, live.recordVersion));
        store.failNextPutNode = true;

        ScpException e = assertThrows(ScpException.class, () -> registry.heartbeat(
                new Messages.NodeHeartbeat(registered.nodeId(), 21, 22, registered.sessionEpoch(),
                        List.of(new Messages.StorageUsage(1, 9)), 0, List.of()),
                ignoreCompletions()));

        assertEquals(ErrorCode.LEASE_EXPIRED, e.code());
        assertTrue(registry.isDead(registered.nodeId()));
    }

    @Test
    void expireScanDoesNotDeclareDeadWhenStoreUpdateThrows() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                51, 52, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(10)), 1, 0));
        NodeRegistry.LiveNode live = liveNodes(registry).get(registered.nodeId());
        live.leaseUntil = 0;
        store.throwOnPutNode = true;

        assertEquals(List.of(), registry.expireScan());
        assertEquals(Records.NodeState.REGISTERED, live.record.state());
    }

    @Test
    void heartbeatRejectsUnknownNodeAndWrongIncarnation() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                61, 62, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(10)), 1, 0));

        assertEquals(ErrorCode.NOT_REGISTERED, assertThrows(ScpException.class, () -> registry.heartbeat(
                new Messages.NodeHeartbeat(999, 61, 62, registered.sessionEpoch(), List.of(), 0, List.of()),
                ignoreCompletions())).code());
        assertEquals(ErrorCode.NOT_REGISTERED, assertThrows(ScpException.class, () -> registry.heartbeat(
                new Messages.NodeHeartbeat(registered.nodeId(), 61, 999, registered.sessionEpoch(),
                        List.of(), 0, List.of()), ignoreCompletions())).code());
    }

    @Test
    void staleHeartbeatCannotRenewLeaseAfterSameIncarnationReregisters() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        long incMsb = 71;
        long incLsb = 72;
        Messages.RegisterResp first = registry.register(new Messages.RegisterNode(
                incMsb, incLsb, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(10)), 1, 0));
        registry.enqueue(first.nodeId(), new Messages.DeleteCmd(99, List.of(new ChunkId(FileId.random(), 0))));

        Messages.RegisterResp second = registry.register(new Messages.RegisterNode(
                incMsb, incLsb, List.of("node-new:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(10)), 1, 0));
        assertTrue(second.sessionEpoch() != first.sessionEpoch(), "re-register must create a new session");
        ScpException stale = assertThrows(ScpException.class, () -> registry.heartbeat(new Messages.NodeHeartbeat(
                first.nodeId(), incMsb, incLsb, first.sessionEpoch(),
                List.of(new Messages.StorageUsage(1, 9)), 0, List.of()), ignoreCompletions()));
        assertEquals(ErrorCode.LEASE_EXPIRED, stale.code());

        Messages.HeartbeatResp current = registry.heartbeat(new Messages.NodeHeartbeat(
                second.nodeId(), incMsb, incLsb, second.sessionEpoch(),
                List.of(new Messages.StorageUsage(1, 9)), 0, List.of()), ignoreCompletions());
        assertEquals(1, current.commands().size(), "stale heartbeat must not drain commands for the new session");
    }

    @Test
    void currentSessionCheckUsesPerNodeLockForConsistentSnapshot() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                81, 82, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(10)), 1, 0));
        NodeRegistry.LiveNode live = liveNodes(registry).get(registered.nodeId());

        AtomicReference<Boolean> result = new AtomicReference<>();
        Thread checker = new Thread(() -> result.set(registry.isCurrentSession(
                registered.nodeId(), 81, 82, registered.sessionEpoch())), "session-checker");

        live.lock.lock();
        try {
            checker.start();
            waitFor(() -> live.lock.hasQueuedThread(checker),
                    "isCurrentSession did not queue behind the per-node lock");
            assertEquals(null, result.get());
        } finally {
            live.lock.unlock();
        }

        checker.join(TimeUnit.SECONDS.toMillis(3));
        assertFalse(checker.isAlive(), "session checker did not finish");
        assertEquals(Boolean.TRUE, result.get());
    }

    @Test
    void currentSessionCheckWaitsForReregisterSessionPublish() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        long incMsb = 91;
        long incLsb = 92;
        Messages.RegisterResp first = registry.register(new Messages.RegisterNode(
                incMsb, incLsb, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.StorageCapacity(10)), 1, 0));
        CountDownLatch registerPersisted = new CountDownLatch(1);
        CountDownLatch publishSession = new CountDownLatch(1);
        AtomicReference<Throwable> registerFailure = new AtomicReference<>();
        AtomicReference<Boolean> currentSession = new AtomicReference<>();

        FailureInjector.arm("meta.register.afterPersistBeforePublish", p -> {
            registerPersisted.countDown();
            publishSession.await();
        });

        Thread registerThread = new Thread(() -> {
            try {
                registry.register(new Messages.RegisterNode(
                        incMsb, incLsb, List.of("node-new:9000"), "z", "r", "h",
                        List.of(new Messages.StorageCapacity(10)), 1, 0));
            } catch (Throwable t) {
                registerFailure.set(t);
            }
        }, "reregister-before-session-publish");
        registerThread.start();
        assertTrue(registerPersisted.await(3, TimeUnit.SECONDS),
                "re-register did not reach the persistent-write window");

        Thread checker = new Thread(() -> currentSession.set(registry.isCurrentSession(
                first.nodeId(), incMsb, incLsb, first.sessionEpoch())), "session-check-during-reregister");
        checker.start();
        waitFor(() -> currentSession.get() != null || isWaiting(checker),
                "isCurrentSession did not attempt to inspect the node while re-register was parked");
        assertEquals(null, currentSession.get(),
                "old session was accepted while re-register had persisted but not published its new session");

        publishSession.countDown();
        registerThread.join(TimeUnit.SECONDS.toMillis(3));
        checker.join(TimeUnit.SECONDS.toMillis(3));
        assertFalse(registerThread.isAlive(), "re-register did not finish");
        assertFalse(checker.isAlive(), "session checker did not finish");
        assertEquals(null, registerFailure.get());
        assertEquals(Boolean.FALSE, currentSession.get());
    }

    private static MetaConfig config() {
        return new MetaConfig("unused", 0, 1, 1_000, 0, 1, 1);
    }

    private static Records.NodeRecord node(int id, Records.NodeState state) {
        return new Records.NodeRecord(id, id * 10L, id * 10L + 1, List.of("node-" + id + ":9000"), "z", "r", "host-" + id, 1_000, state);
    }

    @SuppressWarnings("unchecked")
    private static Map<Integer, NodeRegistry.LiveNode> liveNodes(NodeRegistry registry) throws Exception {
        Field live = NodeRegistry.class.getDeclaredField("live");
        live.setAccessible(true);
        return (Map<Integer, NodeRegistry.LiveNode>) live.get(registry);
    }

    private static void waitFor(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), message);
    }

    private static NodeRegistry.CompletionSink ignoreCompletions() {
        return (nodeId, incMsb, incLsb, sessionEpoch, ignored) -> CompletableFuture.completedFuture(null);
    }

    private static boolean isWaiting(Thread thread) {
        Thread.State state = thread.getState();
        return state == Thread.State.BLOCKED
                || state == Thread.State.WAITING
                || state == Thread.State.TIMED_WAITING;
    }

    private static final class FakeStore implements MetadataStore {
        private final Map<Integer, MetadataStore.Versioned<Records.NodeRecord>> nodes = new LinkedHashMap<>();
        private int nextNodeId = 1;
        private boolean failNextPutNode;
        private int failPutNodeAttempts;
        private boolean throwOnPutNode;

        @Override
        public void createFile(Records.FileRecord record) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Versioned<Records.FileRecord>> getFile(FileId id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<FileId> resolvePath(io.strata.common.StrataNamespace namespace,
                                            io.strata.common.StrataPath path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int sweepDeletedFiles(long olderThanMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean updateFile(Records.FileRecord record, int expectedVersion) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean deletePath(io.strata.common.StrataNamespace namespace, io.strata.common.StrataPath path,
                                  FileId expectedFileId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean deleteFile(FileId id, int expectedVersion) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<FileId> listFiles(io.strata.common.StrataNamespace namespace) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<io.strata.common.StrataNamespace> listNamespaces() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int nextNodeId() {
            return nextNodeId++;
        }

        @Override
        public boolean putNode(Records.NodeRecord record, int expectedVersion) {
            if (throwOnPutNode) {
                throw new IllegalStateException("putNode failure");
            }
            MetadataStore.Versioned<Records.NodeRecord> current = nodes.get(record.nodeId());
            if (failPutNodeAttempts > 0) {
                failPutNodeAttempts--;
                if (current != null) {
                    nodes.put(record.nodeId(), new MetadataStore.Versioned<>(current.value(), current.version() + 1));
                }
                return false;
            }
            if (failNextPutNode) {
                failNextPutNode = false;
                if (current != null) {
                    nodes.put(record.nodeId(), new MetadataStore.Versioned<>(current.value(), current.version() + 1));
                }
                return false;
            }
            if (current == null) {
                if (expectedVersion != -1) {
                    return false;
                }
                nodes.put(record.nodeId(), new MetadataStore.Versioned<>(record, 0));
                return true;
            }
            if (current.version() != expectedVersion) {
                return false;
            }
            nodes.put(record.nodeId(), new MetadataStore.Versioned<>(record, current.version() + 1));
            return true;
        }

        @Override
        public Optional<Versioned<Records.NodeRecord>> getNode(int nodeId) {
            return Optional.ofNullable(nodes.get(nodeId));
        }

        @Override
        public List<Versioned<Records.NodeRecord>> listNodes() {
            return new ArrayList<>(nodes.values());
        }

        @Override
        public void close() {
        }
    }
}
