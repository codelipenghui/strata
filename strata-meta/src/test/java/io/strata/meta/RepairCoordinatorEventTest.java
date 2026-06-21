package io.strata.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpServer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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

    /**
     * Lane B owner path: a non-leader namespace owner has no heartbeat channel, so it detects a node
     * death by that node DISAPPEARING from the published cluster-live-nodes snapshot (alive-only). On
     * that delta it repairs the owned chunks that had a replica on the departed node, using OWNER
     * liveness (the snapshot alive set) and the owner per-chunk repair — NOT {@code registry.isDead()}.
     */
    @Test
    void ownerRepairsWhenNodeLeavesPublishedSnapshot() throws Exception {
        FakeStore store = new FakeStore();
        ControllerConfig config = config();
        NodeRegistry registry = new NodeRegistry(store, config);
        Registered source = register(registry, 2110, "source");
        Registered other = register(registry, 2111, "other");

        UUID inc = UUID.randomUUID();
        try (ScpServer targetServer = new ScpServer(0, 999, inc.getMostSignificantBits(),
                inc.getLeastSignificantBits(), req -> {
                    if (req.opcode() == Opcode.EXEC_REPLICATE.code) {
                        return ScpServer.ok(req, Messages.okHeader(), null);
                    }
                    throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected opcode");
                })) {
            Registered target = registerAt(registry, 2112, "target", "127.0.0.1:" + targetServer.port());

            // RF 3, sealed, currently on {source, other}; `target` is the only spare.
            FileId fileId = fileId(2400);
            store.createFile(file(fileId, FileState.SEALED,
                    List.of(sealed(0, 4096, 0xABCD, List.of(source.nodeId(), other.nodeId())))));

            // Non-leader owner: isLeader=false, ownsAll=false, ownsNamespace=true for the file's namespace.
            RepairCoordinator owner = new RepairCoordinator(store, registry, config,
                    () -> false, () -> false, ns -> true);

            // tick 1: snapshot has all three -> nothing to repair (and it records the baseline ids).
            publishSnapshotWith(registry);
            owner.tick();
            // tick 2: `source` is gone from the snapshot -> owner detects the delta and repairs.
            expire(registry, source.nodeId());
            publishSnapshotWith(registry);
            owner.tick();

            assertTrue(replicateIssuedFor(owner, store, fileId, target.nodeId()),
                    "owner must replicate the source's chunk to the spare target after it leaves the snapshot");
            List<Integer> replicas = store.files.get(fileId).value().chunks().get(0).replicas();
            assertFalse(replicas.contains(source.nodeId()), "the departed replica was swapped out");
        }
    }

    /**
     * Awaits the owner event path (async via {@code repairEventExecutor}) writing the target into the
     * chunk's replica set — the owner repair applies the swap itself after EXEC_REPLICATE acks.
     */
    private static boolean replicateIssuedFor(RepairCoordinator coord, FakeStore store, FileId fileId,
                                              int targetNode) throws Exception {
        long deadline = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < deadline) {
            var v = store.files.get(fileId);
            if (v != null && v.value().chunks().get(0).replicas().contains(targetNode)) {
                return true;
            }
            Thread.sleep(5);
        }
        return false;
    }

    /** Publishes a live-node snapshot from the registry's current in-memory live (alive) set. */
    private static void publishSnapshotWith(NodeRegistry registry) {
        registry.publishClusterLiveNodes();
    }

    private static Registered registerAt(NodeRegistry registry, long incMsb, String host, String endpoint)
            throws Exception {
        long incLsb = incMsb + 1;
        Messages.RegisterResp resp = registry.register(new Messages.RegisterNode(
                incMsb, incLsb, List.of(endpoint), "zone", "rack", host,
                List.of(new Messages.StorageCapacity(1_000_000)), 1, 0));
        return new Registered(resp.nodeId(), incMsb, incLsb, resp.sessionEpoch());
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
                incMsb, incLsb, List.of(host + ":9000"), "zone", "rack", host,
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
        return new FileId(0, lsb);
    }

    private record Registered(int nodeId, long incMsb, long incLsb, long sessionEpoch) {}

    /** Minimal in-memory MetadataStore: only the file/node surface the repair path touches. */
    private static final class FakeStore implements MetadataStore {
        private final Map<FileId, Versioned<Records.FileRecord>> files = new LinkedHashMap<>();
        private final Map<Integer, Versioned<Records.NodeRecord>> nodes = new LinkedHashMap<>();
        private volatile byte[] clusterLiveNodes;
        private int nextNodeId = 1;

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
        public int nextNodeId() {
            return nextNodeId++;
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
