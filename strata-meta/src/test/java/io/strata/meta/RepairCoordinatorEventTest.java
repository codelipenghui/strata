package io.strata.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import io.strata.proto.Messages;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

/**
 * Lane B (event-driven repair): a node death surfaced by {@code expireScan()} must start targeted
 * repair on the leader immediately — without waiting for, or invoking, the slow {@code reconcile()}.
 * Self-contained doubles (minimal FakeStore + register/file helpers) keep this independent of the
 * larger {@code RepairCoordinatorTest} fixture while reusing the same package-private surface.
 */
class RepairCoordinatorEventTest {

    @Test
    void nodeDeathTriggersTargetedRepairViaTickBeforeAnyReconcile() throws Exception {
        FakeStore store = new FakeStore();
        ControllerConfig config = config();
        NodeRegistry registry = new NodeRegistry(store, config);
        Registered source = register(registry, 2010, "source");
        Registered other = register(registry, 2011, "other");
        Registered target = register(registry, 2012, "target");
        FileId fileId = fileId(2100);
        // RF 3, sealed, currently on {source, other}; `target` is the only spare.
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 4096, 0xABCD, List.of(source.nodeId(), other.nodeId())))));

        RepairCoordinator coord = new RepairCoordinator(store, registry, config, () -> true);
        coord.becomeLeaderForTest();
        expire(registry, source.nodeId());

        // ONE tick(): expireScan() returns [source]; the event path issues REPLICATE. No reconcile().
        coord.tick();

        Messages.ReplicateCmd replicate = awaitReplicate(registry, coord, target);
        assertEquals(new ChunkId(fileId, 0), replicate.chunkId());
        // `other` is the surviving replica and the only valid pull source; the dead `source` is never one
        assertEquals(List.of(new Messages.Replica(other.nodeId(), "other:9000")), replicate.sources());
    }

    @Test
    void directRepairForDeadNodeIssuesReplicateToSpareTarget() throws Exception {
        FakeStore store = new FakeStore();
        ControllerConfig config = config();
        NodeRegistry registry = new NodeRegistry(store, config);
        Registered source = register(registry, 2210, "source");
        Registered target = register(registry, 2211, "target");
        int deadNode = 909_090; // never registered -> isDead == true
        FileId fileId = fileId(2200);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 1024, 0xBEEF, List.of(source.nodeId(), deadNode)))));

        RepairCoordinator coord = new RepairCoordinator(store, registry, config, () -> true);
        coord.becomeLeaderForTest();

        coord.repairForDeadNode(deadNode);

        Messages.ReplicateCmd replicate = onlyReplicate(registry, coord, target);
        assertEquals(new ChunkId(fileId, 0), replicate.chunkId());
    }

    /**
     * Trigger metrics: an event-lane repair (leader {@code repairForDeadNode}) increments
     * {@code eventRepairs()} and leaves {@code reconcileRepairs()} at zero — the counters distinguish
     * the two repair lanes that share the same dedup'd issuance path.
     */
    @Test
    void eventRepairIncrementsEventCounterNotReconcileCounter() throws Exception {
        FakeStore store = new FakeStore();
        ControllerConfig config = config();
        NodeRegistry registry = new NodeRegistry(store, config);
        Registered source = register(registry, 2510, "source");
        Registered target = register(registry, 2511, "target");
        int deadNode = 707_070; // never registered -> isDead == true
        FileId fileId = fileId(2500);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 1024, 0xCAFE, List.of(source.nodeId(), deadNode)))));

        RepairCoordinator coord = new RepairCoordinator(store, registry, config, () -> true);
        coord.becomeLeaderForTest();

        long beforeEvent = coord.eventRepairs();
        long beforeReconcile = coord.reconcileRepairs();
        coord.repairForDeadNode(deadNode);

        onlyReplicate(registry, coord, target); // a REPLICATE was actually issued
        assertTrue(coord.eventRepairs() > beforeEvent, "event repair must bump the event counter");
        assertEquals(beforeReconcile, coord.reconcileRepairs(), "event repair must not touch the reconcile counter");
    }

    /**
     * Trigger metrics (reconcile lane): a backstop {@code scanOnce()} repair increments
     * {@code reconcileRepairs()} and leaves {@code eventRepairs()} at zero.
     */
    @Test
    void reconcileRepairIncrementsReconcileCounterNotEventCounter() throws Exception {
        FakeStore store = new FakeStore();
        ControllerConfig config = config();
        NodeRegistry registry = new NodeRegistry(store, config);
        Registered source = register(registry, 2610, "source");
        Registered target = register(registry, 2611, "target");
        int deadNode = 606_060; // never registered -> isDead == true
        FileId fileId = fileId(2600);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 1024, 0xD00D, List.of(source.nodeId(), deadNode)))));

        RepairCoordinator coord = new RepairCoordinator(store, registry, config, () -> true);
        coord.becomeLeaderForTest();

        long beforeEvent = coord.eventRepairs();
        long beforeReconcile = coord.reconcileRepairs();
        coord.scanOnce();

        onlyReplicate(registry, coord, target); // a REPLICATE was actually issued
        assertTrue(coord.reconcileRepairs() > beforeReconcile, "reconcile repair must bump the reconcile counter");
        assertEquals(beforeEvent, coord.eventRepairs(), "reconcile repair must not touch the event counter");
    }

    @Test
    void repairForDeadNodeIsNoopWhenNotLeader() throws Exception {
        FakeStore store = new FakeStore();
        ControllerConfig config = config();
        NodeRegistry registry = new NodeRegistry(store, config);
        Registered source = register(registry, 2310, "source");
        Registered target = register(registry, 2311, "target");
        int deadNode = 818_181;
        FileId fileId = fileId(2300);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 1024, 0xF00D, List.of(source.nodeId(), deadNode)))));

        RepairCoordinator coord = new RepairCoordinator(store, registry, config, () -> false);
        coord.becomeLeaderForTest();

        coord.repairForDeadNode(deadNode);

        assertTrue(heartbeat(registry, coord, target).commands().isEmpty(),
                "a standby must not issue event-driven repair");
    }

    /** Drives heartbeats until the targeted node gets a ReplicateCmd (the event path is async via tick()). */
    private static Messages.ReplicateCmd awaitReplicate(NodeRegistry registry, RepairCoordinator coord,
                                                        Registered node) throws Exception {
        long deadline = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < deadline) {
            List<Messages.Command> commands = heartbeat(registry, coord, node).commands();
            if (!commands.isEmpty()) {
                assertEquals(1, commands.size());
                assertTrue(commands.get(0) instanceof Messages.ReplicateCmd, "expected a REPLICATE command");
                return (Messages.ReplicateCmd) commands.get(0);
            }
            Thread.sleep(5);
        }
        throw new AssertionError("expected event-driven REPLICATE to the spare target, none arrived");
    }

    private static Messages.ReplicateCmd onlyReplicate(NodeRegistry registry, RepairCoordinator coord,
                                                       Registered node) {
        List<Messages.Command> commands = heartbeat(registry, coord, node).commands();
        assertEquals(1, commands.size());
        assertTrue(commands.get(0) instanceof Messages.ReplicateCmd, "expected a REPLICATE command");
        return (Messages.ReplicateCmd) commands.get(0);
    }

    private static ControllerConfig config() {
        // leaseMs 60s so freshly-registered live nodes are NOT swept by expireScan(); deadGraceMs 0 so a
        // node we explicitly push past its lease (expire(...)) is declared DEAD on the next scan. The
        // settle gate (leaseMs+deadGraceMs) is opened by becomeLeaderForTest(). grace 0 -> prompt checks.
        return new ControllerConfig("unused", 0, 1, 60_000, 0, 1, 1).withReplicaMissingGraceMs(0);
    }

    private static Registered register(NodeRegistry registry, long incMsb, String host) throws Exception {
        long incLsb = incMsb + 1;
        Messages.RegisterResp resp = registry.register(new Messages.RegisterNode(
                (int) incMsb, incMsb, incLsb, List.of(host + ":9000"), "zone", "rack", host,
                List.of(new Messages.StorageCapacity(1_000_000)), 1, 0));
        return new Registered(resp.nodeId(), incMsb, incLsb, resp.sessionEpoch());
    }

    private static Messages.HeartbeatResp heartbeat(NodeRegistry registry, RepairCoordinator coord,
                                                    Registered node) {
        return registry.heartbeat(new Messages.NodeHeartbeat(node.nodeId(), node.incMsb(), node.incLsb(),
                node.sessionEpoch(), List.of(new Messages.StorageUsage(0, 1_000_000)), 0, List.of()),
                (nodeId, incMsb, incLsb, sessionEpoch, completion) -> {
                    coord.onCommandCompleted(nodeId, completion);
                    return CompletableFuture.completedFuture(null);
                });
    }

    /** Force a registered node's lease into the past so the next expireScan() declares it DEAD. */
    @SuppressWarnings("unchecked")
    private static void expire(NodeRegistry registry, int nodeId) throws Exception {
        Field liveField = NodeRegistry.class.getDeclaredField("live");
        liveField.setAccessible(true);
        Map<Integer, NodeRegistry.LiveNode> live = (Map<Integer, NodeRegistry.LiveNode>) liveField.get(registry);
        live.get(nodeId).leaseUntil = System.currentTimeMillis() - 10_000;
    }

    private static Records.FileRecord file(FileId fileId, FileState state, List<Records.ChunkRecord> chunks) {
        return new Records.FileRecord(fileId, "test", "/file-" + fileId, 3, 2, false, state, 1234, chunks);
    }

    private static Records.ChunkRecord sealed(int index, long length, int crc, List<Integer> replicas) {
        return new Records.ChunkRecord(index, ChunkState.SEALED, length, crc, 1, replicas);
    }

    private static FileId fileId(long lsb) {
        return FileId.of(0);
    }

    private record Registered(int nodeId, long incMsb, long incLsb, long sessionEpoch) {}

    /** Minimal in-memory MetadataStore: only the file/node surface the repair path touches. */
    private static final class FakeStore implements MetadataStore {
        private final Map<FileId, Versioned<Records.FileRecord>> files = new LinkedHashMap<>();
        private final Map<Integer, Versioned<Records.NodeRecord>> nodes = new LinkedHashMap<>();
        private volatile byte[] clusterLiveNodes;

        @Override
        public void putClusterLiveNodes(byte[] snapshot) {
            this.clusterLiveNodes = snapshot;
        }

        @Override
        public Optional<byte[]> getClusterLiveNodes() {
            return Optional.ofNullable(clusterLiveNodes);
        }

        @Override
        public void createFile(Records.FileRecord record) {
            files.put(record.fileId(), new Versioned<>(record, 0));
        }

        @Override
        public Optional<Versioned<Records.FileRecord>> getFile(FileId id) {
            return Optional.ofNullable(files.get(id));
        }

        @Override
        public Optional<FileId> resolvePath(StrataNamespace namespace, StrataPath path) {
            return Optional.empty();
        }

        @Override
        public boolean updateFile(Records.FileRecord record, int expectedVersion) {
            Versioned<Records.FileRecord> current = files.get(record.fileId());
            if (current == null || current.version() != expectedVersion) {
                return false;
            }
            files.put(record.fileId(), new Versioned<>(record, current.version() + 1));
            return true;
        }

        @Override
        public boolean deletePath(StrataNamespace namespace, StrataPath path, FileId expectedFileId) {
            return true;
        }

        @Override
        public boolean deleteFile(FileId id, int expectedVersion) {
            files.remove(id);
            return true;
        }

        @Override
        public List<FileId> listFiles(StrataNamespace namespace) {
            List<FileId> listed = new ArrayList<>();
            for (var e : files.entrySet()) {
                if (e.getValue().value().namespace().equals(namespace)) {
                    listed.add(e.getKey());
                }
            }
            return listed;
        }

        @Override
        public List<StrataNamespace> listNamespaces() {
            java.util.TreeSet<StrataNamespace> namespaces = new java.util.TreeSet<>();
            for (var v : files.values()) {
                namespaces.add(v.value().namespace());
            }
            return new ArrayList<>(namespaces);
        }

        @Override
        public int sweepDeletedFiles(long olderThanMs) {
            return 0;
        }

        @Override
        public boolean putNode(Records.NodeRecord record, int expectedVersion) {
            Versioned<Records.NodeRecord> current = nodes.get(record.nodeId());
            if (current == null) {
                if (expectedVersion != -1) {
                    return false;
                }
                nodes.put(record.nodeId(), new Versioned<>(record, 0));
                return true;
            }
            if (current.version() != expectedVersion) {
                return false;
            }
            nodes.put(record.nodeId(), new Versioned<>(record, current.version() + 1));
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
