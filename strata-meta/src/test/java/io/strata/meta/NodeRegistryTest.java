package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.proto.Messages;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeRegistryTest {

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
    void registrationRequiresValidEndpointAndCapacity() throws Exception {
        NodeRegistry registry = new NodeRegistry(new FakeStore(), config());

        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of(), "z", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node:9000"), "z", "r", "h", List.of(), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 0)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node"), "z", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node :9000"), "z", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("[::1:9000"), "z", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node:not-a-port"), "z", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node:0"), "z", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node:9000"), "", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of(""), "z", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 1)), 1, 0)));
        assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node:65536"), "z", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 1)), 1, 0)));
        assertEquals(1, registry.register(new Messages.RegisterNode(
                1, 2, List.of("[::1]:9000"), "z", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 1)), 1, 0)).nodeId());
    }

    @Test
    void registrationFailsAfterPersistentCasContention() throws Exception {
        FakeStore store = new FakeStore();
        store.failPutNodeAttempts = 3;
        NodeRegistry registry = new NodeRegistry(store, config());

        ScpException e = assertThrows(ScpException.class, () -> registry.register(new Messages.RegisterNode(
                1, 2, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 1)), 1, 0)));
        assertEquals(ErrorCode.NOT_LEADER, e.code());
    }

    @Test
    void registerReusesPersistedIdentityAndDrainsHeartbeatCommandsInBatches() throws Exception {
        FakeStore store = new FakeStore();
        Records.NodeRecord existing = new Records.NodeRecord(9, 11, 12, List.of("old:9000"),
                "z", "r", "old-host", (byte) 0, 10, Records.NodeState.REGISTERED);
        store.nodes.put(9, new MetadataStore.Versioned<>(existing, 2));

        NodeRegistry registry = new NodeRegistry(store, config());

        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                11, 12, List.of("new:9000"), "z", "r", "new-host",
                List.of(new Messages.MediaCapacity((byte) 3, 99)), 1, 0));
        assertEquals(9, registered.nodeId());
        assertEquals("new:9000", registry.endpointOf(9));
        assertEquals("new-host", registry.hostOf(9));

        for (int i = 0; i < 18; i++) {
            registry.enqueue(9, new Messages.DeleteCmd(i, List.of(new ChunkId(new FileId(i, i + 1), 0))));
        }
        Messages.HeartbeatResp first = registry.heartbeat(new Messages.NodeHeartbeat(
                9, 11, 12, registered.sessionEpoch(),
                List.of(new Messages.MediaUsage((byte) 3, 1, 98)), 0, List.of()), (nodeId, ignored) -> {});
        Messages.HeartbeatResp second = registry.heartbeat(new Messages.NodeHeartbeat(
                9, 11, 12, registered.sessionEpoch(),
                List.of(new Messages.MediaUsage((byte) 3, 1, 98)), 0, List.of()), (nodeId, ignored) -> {});

        assertEquals(16, first.commands().size());
        assertEquals(2, second.commands().size());
        assertTrue(registry.isAlive(9));
    }

    @Test
    void heartbeatWithEmptyUsageReportsCompletionsAndUnknownHelpersAreEmpty() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                31, 32, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 10)), 1, 0));
        List<Integer> completedBy = new ArrayList<>();

        Messages.HeartbeatResp heartbeat = registry.heartbeat(new Messages.NodeHeartbeat(
                registered.nodeId(), 31, 32, registered.sessionEpoch(), List.of(), 0,
                List.of(new Messages.CompletedCommand(99, ErrorCode.OK.code))),
                (nodeId, ignored) -> completedBy.add(nodeId));

        assertEquals(registered.nodeId(), completedBy.get(0));
        assertTrue(heartbeat.commands().isEmpty());
        assertEquals("", registry.endpointOf(999));
        assertEquals(null, registry.hostOf(999));
        assertEquals(new Messages.Replica(999, ""), registry.replicaOf(999));
        registry.enqueue(999, new Messages.DrainCmd(1));
    }

    @Test
    void heartbeatResurrectsDeadNodeWhenCasSucceeds() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                41, 42, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 10)), 1, 0));
        NodeRegistry.LiveNode live = liveNodes(registry).get(registered.nodeId());
        Records.NodeRecord dead = live.record.withState(Records.NodeState.DEAD);
        live.record = dead;
        store.nodes.put(registered.nodeId(), new MetadataStore.Versioned<>(dead, live.recordVersion));

        registry.heartbeat(new Messages.NodeHeartbeat(registered.nodeId(), 41, 42, registered.sessionEpoch(),
                List.of(new Messages.MediaUsage((byte) 0, 1, 9)), 0, List.of()), (nodeId, ignored) -> {});

        assertTrue(registry.isAlive(registered.nodeId()));
        assertEquals(Records.NodeState.REGISTERED, live.record.state());
    }

    @Test
    void deadHeartbeatMustNotExtendLeaseWhenResurrectionCasLosesToDeadStoreState() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());

        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                21, 22, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 10)), 1, 0));
        NodeRegistry.LiveNode live = liveNodes(registry).get(registered.nodeId());
        Records.NodeRecord dead = live.record.withState(Records.NodeState.DEAD);
        live.record = dead;
        store.nodes.put(registered.nodeId(), new MetadataStore.Versioned<>(dead, live.recordVersion));
        store.failNextPutNode = true;

        ScpException e = assertThrows(ScpException.class, () -> registry.heartbeat(
                new Messages.NodeHeartbeat(registered.nodeId(), 21, 22, registered.sessionEpoch(),
                        List.of(new Messages.MediaUsage((byte) 0, 1, 9)), 0, List.of()),
                (nodeId, ignored) -> {}));

        assertEquals(ErrorCode.LEASE_EXPIRED, e.code());
        assertTrue(registry.isDead(registered.nodeId()));
    }

    @Test
    void expireScanDoesNotDeclareDeadWhenStoreUpdateThrows() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Messages.RegisterResp registered = registry.register(new Messages.RegisterNode(
                51, 52, List.of("node:9000"), "z", "r", "h",
                List.of(new Messages.MediaCapacity((byte) 0, 10)), 1, 0));
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
                List.of(new Messages.MediaCapacity((byte) 0, 10)), 1, 0));

        assertEquals(ErrorCode.NOT_REGISTERED, assertThrows(ScpException.class, () -> registry.heartbeat(
                new Messages.NodeHeartbeat(999, 61, 62, registered.sessionEpoch(), List.of(), 0, List.of()),
                (nodeId, ignored) -> {})).code());
        assertEquals(ErrorCode.NOT_REGISTERED, assertThrows(ScpException.class, () -> registry.heartbeat(
                new Messages.NodeHeartbeat(registered.nodeId(), 61, 999, registered.sessionEpoch(),
                        List.of(), 0, List.of()), (nodeId, ignored) -> {})).code());
    }

    private static MetaConfig config() {
        return new MetaConfig("unused", 0, 1, 1_000, 0, 1, 1);
    }

    private static Records.NodeRecord node(int id, Records.NodeState state) {
        return new Records.NodeRecord(id, id * 10L, id * 10L + 1, List.of("node-" + id + ":9000"),
                "z", "r", "host-" + id, (byte) 0, 1_000, state);
    }

    @SuppressWarnings("unchecked")
    private static Map<Integer, NodeRegistry.LiveNode> liveNodes(NodeRegistry registry) throws Exception {
        Field live = NodeRegistry.class.getDeclaredField("live");
        live.setAccessible(true);
        return (Map<Integer, NodeRegistry.LiveNode>) live.get(registry);
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
        public List<FileId> listFiles() {
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
