package io.strata.meta;

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
import io.strata.proto.ScpClient;
import io.strata.proto.ScpServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepairCoordinatorTest {
    private static final StrataNamespace TEST_NS = StrataNamespace.of("test");
    private static final byte MEDIA = 1;

    @Test
    void scanSkipsMissingRecordsAndCannotRepairZeroLiveChunk() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        FileId liveFile = fileId(1);
        FileId missingFile = fileId(404);
        store.extraListedFiles.add(missingFile);
        store.createFile(file(liveFile, FileState.SEALED, List.of(sealed(0, 128, 1001, List.of(99)))));

        new RepairCoordinator(store, registry, config(), () -> true).scanOnce();

        Records.ChunkRecord chunk = store.files.get(liveFile).value().chunks().get(0);
        assertEquals(List.of(99), chunk.replicas());
        assertFalse(store.files.containsKey(missingFile));
    }

    @Test
    void noPlacementTargetDoesNotSuppressLaterRepair() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered source = register(registry, 10, "source");
        FileId fileId = fileId(2);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 256, 2002, List.of(source.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.scanOnce();
        assertTrue(heartbeat(registry, coordinator, source, List.of()).commands().isEmpty());

        Registered target = register(registry, 20, "target");
        coordinator.scanOnce();

        Messages.Command command = onlyCommand(heartbeat(registry, coordinator, target, List.of()));
        Messages.ReplicateCmd replicate = assertInstanceOf(Messages.ReplicateCmd.class, command);
        assertEquals(new ChunkId(fileId, 0), replicate.chunkId());
        assertEquals(List.of(new Messages.Replica(source.nodeId(), "source:9000")), replicate.sources());
    }

    @Test
    void successfulAddRepairGrowsShortReplicaList() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered source = register(registry, 30, "source");
        Registered target = register(registry, 40, "target");
        FileId fileId = fileId(3);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 512, 3003, List.of(source.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.scanOnce();
        Messages.ReplicateCmd replicate = assertInstanceOf(Messages.ReplicateCmd.class,
                onlyCommand(heartbeat(registry, coordinator, target, List.of())));
        coordinator.onCommandCompleted(target.nodeId(),
                new Messages.CompletedCommand(replicate.commandId(), (short) 0));

        Records.ChunkRecord chunk = store.files.get(fileId).value().chunks().get(0);
        assertEquals(List.of(source.nodeId(), target.nodeId()), chunk.replicas());
    }

    @Test
    void duplicateReplicateIssueIsSuppressedByChunkMarker() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered source = register(registry, 31, "source");
        Registered target = register(registry, 41, "target");
        FileId fileId = fileId(31);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 512, 3031, List.of(source.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);
        Records.FileRecord file = store.files.get(fileId).value();
        Records.ChunkRecord chunk = file.chunks().get(0);

        issueReplicate(coordinator, fileId, file, chunk);
        issueReplicate(coordinator, fileId, file, chunk);

        Messages.HeartbeatResp heartbeat = heartbeat(registry, coordinator, target, List.of());
        assertEquals(1, heartbeat.commands().size());
        assertInstanceOf(Messages.ReplicateCmd.class, heartbeat.commands().get(0));
    }

    @Test
    void issueReplicateNoopsWithoutSourceOrWhenAlreadyFullyReplicated() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);
        FileId noSourceFile = fileId(32);
        Records.FileRecord noSource = file(noSourceFile, FileState.SEALED,
                List.of(sealed(0, 512, 3032, List.of(99, 100, 101))));

        issueReplicate(coordinator, noSourceFile, noSource, noSource.chunks().get(0));

        Registered a = register(registry, 320, "full-a");
        Registered b = register(registry, 330, "full-b");
        Registered c = register(registry, 340, "full-c");
        FileId fullFile = fileId(33);
        Records.FileRecord full = file(fullFile, FileState.SEALED,
                List.of(sealed(0, 512, 3033, List.of(a.nodeId(), b.nodeId(), c.nodeId()))));

        issueReplicate(coordinator, fullFile, full, full.chunks().get(0));

        assertTrue(heartbeat(registry, coordinator, a, List.of()).commands().isEmpty());
        assertTrue(heartbeat(registry, coordinator, b, List.of()).commands().isEmpty());
        assertTrue(heartbeat(registry, coordinator, c, List.of()).commands().isEmpty());
    }

    @Test
    void deadReplicaRepairSwapsOutDeadNode() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered source = register(registry, 50, "source");
        Registered target = register(registry, 60, "target");
        int deadNode = 999;
        FileId fileId = fileId(4);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 1024, 4004, List.of(source.nodeId(), deadNode)))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.scanOnce();
        Messages.ReplicateCmd replicate = assertInstanceOf(Messages.ReplicateCmd.class,
                onlyCommand(heartbeat(registry, coordinator, target, List.of())));
        coordinator.onCommandCompleted(target.nodeId(),
                new Messages.CompletedCommand(replicate.commandId(), (short) 0));

        Records.ChunkRecord chunk = store.files.get(fileId).value().chunks().get(0);
        assertEquals(List.of(source.nodeId(), target.nodeId()), chunk.replicas());
    }

    @Test
    void targetDeathBeforeCompletionLeavesDescriptorUnchangedAndAllowsRetry() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered source = register(registry, 70, "source");
        Registered target = register(registry, 80, "target");
        FileId fileId = fileId(5);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 2048, 5005, List.of(source.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.scanOnce();
        Messages.ReplicateCmd first = assertInstanceOf(Messages.ReplicateCmd.class,
                onlyCommand(heartbeat(registry, coordinator, target, List.of())));
        markDead(registry, target.nodeId());
        coordinator.onCommandCompleted(target.nodeId(),
                new Messages.CompletedCommand(first.commandId(), (short) 0));

        assertEquals(List.of(source.nodeId()), store.files.get(fileId).value().chunks().get(0).replicas());

        Registered retryTarget = register(registry, 90, "retry-target");
        coordinator.scanOnce();

        Messages.ReplicateCmd retry = assertInstanceOf(Messages.ReplicateCmd.class,
                onlyCommand(heartbeat(registry, coordinator, retryTarget, List.of())));
        assertEquals(new ChunkId(fileId, 0), retry.chunkId());
    }

    @Test
    void replicaSwapNoopsForMissingFileMismatchedChunkExistingTargetAndAbsentDeadNode() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered source = register(registry, 71, "source");
        Registered target = register(registry, 72, "target");
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        applyReplicaSwap(coordinator, replicateAction(fileId(51), new ChunkId(fileId(51), 0),
                -1, target.nodeId()));

        FileId fileId = fileId(52);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(1, 128, 5052, List.of(source.nodeId())))));
        applyReplicaSwap(coordinator, replicateAction(fileId, new ChunkId(fileId, 0), -1, target.nodeId()));
        assertEquals(List.of(source.nodeId()), store.files.get(fileId).value().chunks().get(0).replicas());

        FileId targetPresent = fileId(53);
        store.createFile(file(targetPresent, FileState.SEALED,
                List.of(sealed(0, 128, 5053, List.of(source.nodeId(), target.nodeId())))));
        applyReplicaSwap(coordinator, replicateAction(targetPresent, new ChunkId(targetPresent, 0),
                999, target.nodeId()));
        assertEquals(List.of(source.nodeId(), target.nodeId()),
                store.files.get(targetPresent).value().chunks().get(0).replicas());

        FileId missingDead = fileId(54);
        store.createFile(file(missingDead, FileState.SEALED,
                List.of(sealed(0, 128, 5054, List.of(source.nodeId())))));
        applyReplicaSwap(coordinator, replicateAction(missingDead, new ChunkId(missingDead, 0),
                999, target.nodeId()));
        assertEquals(List.of(source.nodeId()), store.files.get(missingDead).value().chunks().get(0).replicas());
    }

    @Test
    void wrongReporterDoesNotClearReplicateCommand() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered source = register(registry, 100, "source");
        Registered target = register(registry, 110, "target");
        FileId fileId = fileId(6);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 4096, 6006, List.of(source.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.scanOnce();
        Messages.ReplicateCmd replicate = assertInstanceOf(Messages.ReplicateCmd.class,
                onlyCommand(heartbeat(registry, coordinator, target, List.of())));
        coordinator.onCommandCompleted(source.nodeId(),
                new Messages.CompletedCommand(replicate.commandId(), (short) 0));
        coordinator.onCommandCompleted(target.nodeId(),
                new Messages.CompletedCommand(replicate.commandId(), (short) 0));

        Records.ChunkRecord chunk = store.files.get(fileId).value().chunks().get(0);
        assertEquals(List.of(source.nodeId(), target.nodeId()), chunk.replicas());
    }

    @Test
    void unknownAndFailedReplicateCompletionsAllowLaterRetry() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered source = register(registry, 111, "source");
        Registered target = register(registry, 112, "target");
        FileId fileId = fileId(61);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 4096, 6061, List.of(source.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.onCommandCompleted(target.nodeId(), new Messages.CompletedCommand(-1, (short) 0));
        coordinator.scanOnce();
        Messages.ReplicateCmd first = assertInstanceOf(Messages.ReplicateCmd.class,
                onlyCommand(heartbeat(registry, coordinator, target, List.of())));
        coordinator.onCommandCompleted(target.nodeId(),
                new Messages.CompletedCommand(first.commandId(), (short) 42));

        assertEquals(List.of(source.nodeId()), store.files.get(fileId).value().chunks().get(0).replicas());

        coordinator.scanOnce();
        Messages.ReplicateCmd retry = assertInstanceOf(Messages.ReplicateCmd.class,
                onlyCommand(heartbeat(registry, coordinator, target, List.of())));
        assertEquals(first.chunkId(), retry.chunkId());
    }

    @Test
    void expiredReplicateCommandIsRetried() throws Exception {
        FakeStore store = new FakeStore();
        ControllerConfig config = config(-1);
        NodeRegistry registry = new NodeRegistry(store, config);
        Registered source = register(registry, 120, "source");
        Registered target = register(registry, 130, "target");
        FileId fileId = fileId(7);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 8192, 7007, List.of(source.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config, () -> true);

        coordinator.scanOnce();
        coordinator.scanOnce();

        Messages.HeartbeatResp heartbeat = heartbeat(registry, coordinator, target, List.of());
        assertEquals(2, heartbeat.commands().size());
        assertInstanceOf(Messages.ReplicateCmd.class, heartbeat.commands().get(0));
        assertInstanceOf(Messages.ReplicateCmd.class, heartbeat.commands().get(1));
    }

    @Test
    void failedDeleteCompletionIsRetriedByNextScan() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered node = register(registry, 140, "node");
        FileId fileId = fileId(8);
        store.createFile(file(fileId, FileState.DELETING,
                List.of(sealed(0, 64, 8008, List.of(node.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.scanOnce();
        Messages.DeleteCmd first = assertInstanceOf(Messages.DeleteCmd.class,
                onlyCommand(heartbeat(registry, coordinator, node, List.of())));
        coordinator.onCommandCompleted(node.nodeId(),
                new Messages.CompletedCommand(first.commandId(), (short) 17));

        assertEquals(List.of(node.nodeId()), store.files.get(fileId).value().chunks().get(0).replicas());

        coordinator.scanOnce();
        Messages.DeleteCmd retry = assertInstanceOf(Messages.DeleteCmd.class,
                onlyCommand(heartbeat(registry, coordinator, node, List.of())));
        assertEquals(List.of(new ChunkId(fileId, 0)), retry.chunkIds());
    }

    @Test
    void deletingFileWithNoChunksIsRemovedImmediately() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        FileId fileId = fileId(81);
        store.createFile(file(fileId, FileState.DELETING, List.of()));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.scanOnce();

        assertFalse(store.files.containsKey(fileId));
    }

    @Test
    void deleteConfirmationNoopsForMissingFileAndKeepsLiveZeroReplicaChunk() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered node = register(registry, 148, "node");
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        applyDeleteConfirmed(coordinator, fileId(811), new ChunkId(fileId(811), 0), node.nodeId());

        FileId liveFile = fileId(812);
        store.createFile(file(liveFile, FileState.SEALED,
                List.of(sealed(0, 64, 8012, List.of(node.nodeId())))));
        applyDeleteConfirmed(coordinator, liveFile, new ChunkId(liveFile, 0), node.nodeId());

        Records.ChunkRecord chunk = store.files.get(liveFile).value().chunks().get(0);
        assertEquals(List.of(), chunk.replicas());
    }

    @Test
    void deletingReplicaCommandIsNotDuplicatedWhileInflight() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered node = register(registry, 141, "node");
        FileId fileId = fileId(82);
        store.createFile(file(fileId, FileState.DELETING,
                List.of(sealed(0, 64, 8082, List.of(node.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.scanOnce();
        coordinator.scanOnce();

        Messages.HeartbeatResp heartbeat = heartbeat(registry, coordinator, node, List.of());
        assertEquals(1, heartbeat.commands().size());
        assertInstanceOf(Messages.DeleteCmd.class, heartbeat.commands().get(0));
    }

    @Test
    void expiredDeleteCommandIsRetriedByNextScan() throws Exception {
        FakeStore store = new FakeStore();
        ControllerConfig config = config(-1);
        NodeRegistry registry = new NodeRegistry(store, config);
        Registered node = register(registry, 1411, "delete-target");
        FileId fileId = fileId(821);
        store.createFile(file(fileId, FileState.DELETING,
                List.of(sealed(0, 64, 8821, List.of(node.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config, () -> true);

        coordinator.scanOnce();
        coordinator.scanOnce();

        Messages.HeartbeatResp heartbeat = heartbeat(registry, coordinator, node, List.of());
        assertEquals(2, heartbeat.commands().size());
        assertEquals(List.of(new ChunkId(fileId, 0)),
                assertInstanceOf(Messages.DeleteCmd.class, heartbeat.commands().get(0)).chunkIds());
        assertEquals(List.of(new ChunkId(fileId, 0)),
                assertInstanceOf(Messages.DeleteCmd.class, heartbeat.commands().get(1)).chunkIds());
    }

    @Test
    void replicaSwapCasExhaustionLeavesDescriptorForNextScan() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered source = register(registry, 142, "source");
        Registered target = register(registry, 143, "target");
        FileId fileId = fileId(83);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 128, 8083, List.of(source.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.scanOnce();
        Messages.ReplicateCmd replicate = assertInstanceOf(Messages.ReplicateCmd.class,
                onlyCommand(heartbeat(registry, coordinator, target, List.of())));
        store.failUpdateFileAttempts = 5;
        coordinator.onCommandCompleted(target.nodeId(),
                new Messages.CompletedCommand(replicate.commandId(), (short) 0));

        assertEquals(List.of(source.nodeId()), store.files.get(fileId).value().chunks().get(0).replicas());

        coordinator.scanOnce();
        Messages.ReplicateCmd retry = assertInstanceOf(Messages.ReplicateCmd.class,
                onlyCommand(heartbeat(registry, coordinator, target, List.of())));
        assertEquals(replicate.chunkId(), retry.chunkId());
    }

    @Test
    void completionApplyFailureIsSwallowedForNextScan() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered source = register(registry, 144, "source");
        Registered target = register(registry, 145, "target");
        FileId fileId = fileId(84);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 128, 8084, List.of(source.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.scanOnce();
        Messages.ReplicateCmd replicate = assertInstanceOf(Messages.ReplicateCmd.class,
                onlyCommand(heartbeat(registry, coordinator, target, List.of())));
        store.throwOnGetFile = true;
        coordinator.onCommandCompleted(target.nodeId(),
                new Messages.CompletedCommand(replicate.commandId(), (short) 0));
        store.throwOnGetFile = false;

        assertEquals(List.of(source.nodeId()), store.files.get(fileId).value().chunks().get(0).replicas());
        coordinator.scanOnce();
        Messages.ReplicateCmd retry = assertInstanceOf(Messages.ReplicateCmd.class,
                onlyCommand(heartbeat(registry, coordinator, target, List.of())));
        assertEquals(replicate.chunkId(), retry.chunkId());
    }

    @Test
    void zeroReplicaDeletingChunkRetriesFileDeleteAfterCasMiss() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        FileId fileId = fileId(9);
        store.createFile(file(fileId, FileState.DELETING,
                List.of(sealed(0, 32, 9009, List.of()))));
        store.failDeleteFileAttempts = 1;
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.scanOnce();

        assertFalse(store.files.containsKey(fileId));
    }

    @Test
    void closeInterruptsStartedScannerAndRestoresInterrupt() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config(60_000));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(60_000), () -> false);
        coordinator.start();

        Thread.currentThread().interrupt();
        try {
            coordinator.close();
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void scanLoopContinuesAfterStoreFailure() throws Exception {
        FakeStore store = new FakeStore();
        ControllerConfig fast = new ControllerConfig("unused", 0, 1, 10_000, 0, 1, 1);
        NodeRegistry registry = new NodeRegistry(store, fast);
        Registered source = register(registry, 149, "source");
        Registered target = register(registry, 159, "target");
        FileId fileId = fileId(813);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 64, 8013, List.of(source.nodeId())))));
        store.throwOnListFiles = true;
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, fast, () -> true);
        Field leaderSince = RepairCoordinator.class.getDeclaredField("leaderSince");
        leaderSince.setAccessible(true);
        leaderSince.setLong(coordinator, System.currentTimeMillis() - 20_000);
        coordinator.start();
        try {
            Thread.sleep(20);
            store.throwOnListFiles = false;
            Messages.Command command = null;
            long deadline = System.currentTimeMillis() + 1000;
            while (command == null && System.currentTimeMillis() < deadline) {
                List<Messages.Command> commands = heartbeat(registry, coordinator, target, List.of()).commands();
                if (!commands.isEmpty()) {
                    command = commands.get(0);
                    break;
                }
                Thread.sleep(10);
            }
            assertInstanceOf(Messages.ReplicateCmd.class, command);
        } finally {
            coordinator.close();
        }
    }

    @Test
    void nonControllerOwnerHealsUnderReplicatedChunkViaExecReplicate() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());

        List<Messages.ReplicateCmd> received = new CopyOnWriteArrayList<>();
        UUID inc = UUID.randomUUID();
        try (ScpServer targetServer = new ScpServer(0, 999, inc.getMostSignificantBits(),
                inc.getLeastSignificantBits(), req -> {
                    if (req.opcode() == Opcode.EXEC_REPLICATE.code) {
                        received.add((Messages.ReplicateCmd) Messages.Command.read(req.headerSlice()));
                        return ScpServer.ok(req, Messages.okHeader(), null);
                    }
                    throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected opcode");
                })) {
            Registered dead = register(registry, 500, "dead-host");
            Registered live1 = register(registry, 510, "live1-host");
            Registered live2 = register(registry, 520, "live2-host");
            Registered target = registerAt(registry, 530, "target-host", "127.0.0.1:" + targetServer.port());

            // the controller's authoritative view: the dead node is DEAD in the root store
            Optional<MetadataStore.Versioned<Records.NodeRecord>> deadRecord = store.getNode(dead.nodeId());
            store.putNode(deadRecord.orElseThrow().value().withState(Records.NodeState.DEAD),
                    deadRecord.get().version());

            // an under-replicated sealed chunk (RF 3, one dead replica) in an owned namespace
            FileId fileId = fileId(42);
            Records.ChunkRecord chunk = sealed(0, 4096, 0xCAFE,
                    List.of(dead.nodeId(), live1.nodeId(), live2.nodeId()));
            store.createFile(file(fileId, FileState.SEALED, List.of(chunk)));

            // a sharded non-controller owner of the namespace heals the chunk directly
            RepairCoordinator owner = new RepairCoordinator(store, registry, config(),
                    () -> false, () -> false, ns -> true);
            owner.ownerRepairPass();

            assertEquals(1, received.size(), "owner sent exactly one EXEC_REPLICATE");
            assertEquals(new ChunkId(fileId, 0), received.get(0).chunkId());
            List<Integer> replicas = store.files.get(fileId).value().chunks().get(0).replicas();
            assertTrue(replicas.contains(target.nodeId()), "the repair target became a replica");
            assertFalse(replicas.contains(dead.nodeId()), "the dead replica was swapped out");
            assertEquals(3, replicas.size(), "chunk restored to its replication factor");
        }
    }

    @Test
    void nonLeaderOwnerVerifiesNodeAbsentFromItsFrozenRegistry() throws Exception {
        // A non-leader namespace owner does not receive heartbeats (they are leader-gated), so its in-memory
        // registry is frozen at boot: a data node that (re-)registered AFTER it booted is absent from
        // registry.live and reads as isDead. The verify lane must judge liveness from the persisted snapshot,
        // not registry.isDead — otherwise it silently skips VERIFY_CHUNKS for that node's replicas (a
        // missing/corrupt-copy detection blind spot, since verify is the only physical-integrity check).
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());

        List<Messages.VerifyChunks> verifies = new CopyOnWriteArrayList<>();
        UUID inc = UUID.randomUUID();
        try (ScpServer node = new ScpServer(0, 777, inc.getMostSignificantBits(),
                inc.getLeastSignificantBits(), req -> {
                    if (req.opcode() == Opcode.VERIFY_CHUNKS.code) {
                        Messages.VerifyChunks vc = Messages.VerifyChunks.decode(req.headerSlice());
                        verifies.add(vc);
                        List<Messages.VerifyChunkResult> results = new ArrayList<>();
                        for (ChunkId id : vc.chunkIds()) {
                            results.add(new Messages.VerifyChunkResult(id, true, ChunkState.SEALED, 4096, 0xCAFE));
                        }
                        return ScpServer.ok(req, new Messages.VerifyChunksResp(results).encode(), null);
                    }
                    throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected opcode");
                })) {
            // Persist the node in the store (with the stub endpoint), then drop it from the registry's live
            // map to simulate a non-leader owner that never warmed/heard this (post-boot) node.
            Registered late = registerAt(registry, 880, "late-host", "127.0.0.1:" + node.port());
            liveNodes(registry).remove(late.nodeId());
            assertTrue(registry.isDead(late.nodeId()),
                    "premise: the node is absent from the frozen registry (the old liveness source reads it dead)");

            FileId fileId = fileId(0x5151);
            Records.ChunkRecord chunk = sealed(0, 4096, 0xCAFE, List.of(late.nodeId()));
            store.createFile(file(fileId, FileState.SEALED, List.of(chunk)));

            // config(5000): give the stub VERIFY_CHUNKS RPC a generous timeout — the default config()'s 1ms
            // repairCommandTimeoutMs is too tight for the real ScpClient round-trip under CI load, so the
            // verdict would be lost (empty result) and the drop/keep decision would flake.
            RepairCoordinator owner = new RepairCoordinator(store, registry, config(5000),
                    () -> false, () -> false, ns -> true);
            owner.verifyPass();

            assertEquals(1, verifies.size(),
                    "owner issued VERIFY_CHUNKS for a node present only in the persisted snapshot");
            assertEquals(List.of(new ChunkId(fileId, 0)), verifies.get(0).chunkIds());
        }
    }

    @Test
    void nonLeaderOwnerKeepsLastLiveReplicaWhenPeerIsDeadInPersistedState() throws Exception {
        // The last-live-replica guard must judge survivors from the persisted snapshot. A peer the leader
        // declared DEAD (persisted) but still stale-REGISTERED in a non-leader owner's frozen registry must
        // NOT count as a live survivor — else an unhealthy verdict on the only actually-live replica would
        // drop it, leaving 0 live copies (data loss).
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());

        UUID inc = UUID.randomUUID();
        try (ScpServer node = new ScpServer(0, 778, inc.getMostSignificantBits(),
                inc.getLeastSignificantBits(), req -> {
                    if (req.opcode() == Opcode.VERIFY_CHUNKS.code) {
                        Messages.VerifyChunks vc = Messages.VerifyChunks.decode(req.headerSlice());
                        List<Messages.VerifyChunkResult> results = new ArrayList<>();
                        for (ChunkId id : vc.chunkIds()) {
                            // corrupt: present + SEALED but a crc that mismatches the descriptor (0xCAFE)
                            results.add(new Messages.VerifyChunkResult(id, true, ChunkState.SEALED, 4096, 0xBAD));
                        }
                        return ScpServer.ok(req, new Messages.VerifyChunksResp(results).encode(), null);
                    }
                    // a destructive delete must never reach the last live replica; reject so any attempt is loud
                    throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected opcode " + req.opcode());
                })) {
            Registered verified = registerAt(registry, 980, "verified-host", "127.0.0.1:" + node.port());
            Registered peer = register(registry, 990, "peer-host");
            // leader's authoritative view: the peer is DEAD in the persisted store, but the non-leader owner's
            // frozen registry still lists it as live (we do NOT mark it dead in the registry).
            Optional<MetadataStore.Versioned<Records.NodeRecord>> peerRec = store.getNode(peer.nodeId());
            store.putNode(peerRec.orElseThrow().value().withState(Records.NodeState.DEAD), peerRec.get().version());
            assertFalse(registry.isDead(peer.nodeId()),
                    "premise: the dead peer is stale-live in the frozen registry");

            FileId fileId = fileId(0x6262);
            Records.ChunkRecord chunk = sealed(0, 4096, 0xCAFE, List.of(verified.nodeId(), peer.nodeId()));
            store.createFile(file(fileId, FileState.SEALED, List.of(chunk)));

            // config(5000): give the stub VERIFY_CHUNKS RPC a generous timeout — the default config()'s 1ms
            // repairCommandTimeoutMs is too tight for the real ScpClient round-trip under CI load, so the
            // verdict would be lost (empty result) and the drop/keep decision would flake.
            RepairCoordinator owner = new RepairCoordinator(store, registry, config(5000),
                    () -> false, () -> false, ns -> true);
            owner.verifyPass();

            List<Integer> replicas = store.files.get(fileId).value().chunks().get(0).replicas();
            assertTrue(replicas.contains(verified.nodeId()),
                    "the last actually-live replica was kept (guard counted survivors from the persisted view)");
            assertEquals(2, replicas.size(), "the corrupt verdict did not drop the last live replica");
        }
    }

    @Test
    void lastLiveGuardCountsDrainingPeerAsSurvivor() throws Exception {
        // A DRAINING node still holds its bytes, so it counts as a live survivor: a corrupt verdict on
        // another replica IS dropped (the draining copy + reconcile re-replicate). This pins DRAINING != DEAD
        // so the persisted-liveness predicate can't be silently narrowed to state == REGISTERED.
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());

        UUID inc = UUID.randomUUID();
        try (ScpServer node = new ScpServer(0, 779, inc.getMostSignificantBits(),
                inc.getLeastSignificantBits(), req -> {
                    if (req.opcode() == Opcode.VERIFY_CHUNKS.code) {
                        Messages.VerifyChunks vc = Messages.VerifyChunks.decode(req.headerSlice());
                        List<Messages.VerifyChunkResult> results = new ArrayList<>();
                        for (ChunkId id : vc.chunkIds()) {
                            results.add(new Messages.VerifyChunkResult(id, true, ChunkState.SEALED, 4096, 0xBAD));
                        }
                        return ScpServer.ok(req, new Messages.VerifyChunksResp(results).encode(), null);
                    }
                    // the corrupt replica's delete is expected (a draining survivor exists); ack via failure is
                    // fine — applyDeleteConfirmed has already mutated the descriptor, which is what we assert.
                    throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected opcode " + req.opcode());
                })) {
            Registered verified = registerAt(registry, 981, "verified2-host", "127.0.0.1:" + node.port());
            Registered draining = register(registry, 991, "draining-host");
            Optional<MetadataStore.Versioned<Records.NodeRecord>> rec = store.getNode(draining.nodeId());
            store.putNode(rec.orElseThrow().value().withState(Records.NodeState.DRAINING), rec.get().version());

            FileId fileId = fileId(0x7373);
            Records.ChunkRecord chunk = sealed(0, 4096, 0xCAFE, List.of(verified.nodeId(), draining.nodeId()));
            store.createFile(file(fileId, FileState.SEALED, List.of(chunk)));

            // config(5000): give the stub VERIFY_CHUNKS RPC a generous timeout — the default config()'s 1ms
            // repairCommandTimeoutMs is too tight for the real ScpClient round-trip under CI load, so the
            // verdict would be lost (empty result) and the drop/keep decision would flake.
            RepairCoordinator owner = new RepairCoordinator(store, registry, config(5000),
                    () -> false, () -> false, ns -> true);
            owner.verifyPass();

            List<Integer> replicas = store.files.get(fileId).value().chunks().get(0).replicas();
            assertFalse(replicas.contains(verified.nodeId()),
                    "the corrupt replica was dropped because a DRAINING peer counts as a live survivor");
            assertEquals(List.of(draining.nodeId()), replicas, "only the draining survivor remains");
        }
    }

    @Test
    void ownerRepairThatLosesEveryCasDoesNotReportFalseSuccess() throws Exception {
        FakeStore store = new FakeStore();
        store.failUpdateFileAttempts = 10; // > CAS_RETRIES: every descriptor-swap CAS loses
        NodeRegistry registry = new NodeRegistry(store, config());

        List<Messages.ReplicateCmd> received = new CopyOnWriteArrayList<>();
        UUID inc = UUID.randomUUID();
        try (ScpServer targetServer = new ScpServer(0, 999, inc.getMostSignificantBits(),
                inc.getLeastSignificantBits(), req -> {
                    if (req.opcode() == Opcode.EXEC_REPLICATE.code) {
                        received.add((Messages.ReplicateCmd) Messages.Command.read(req.headerSlice()));
                        return ScpServer.ok(req, Messages.okHeader(), null);
                    }
                    throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected opcode");
                })) {
            Registered dead = register(registry, 600, "dead-host");
            Registered live1 = register(registry, 610, "live1-host");
            Registered live2 = register(registry, 620, "live2-host");
            Registered target = registerAt(registry, 630, "target-host", "127.0.0.1:" + targetServer.port());

            Optional<MetadataStore.Versioned<Records.NodeRecord>> deadRecord = store.getNode(dead.nodeId());
            store.putNode(deadRecord.orElseThrow().value().withState(Records.NodeState.DEAD),
                    deadRecord.get().version());

            FileId fileId = fileId(43);
            Records.ChunkRecord chunk = sealed(0, 4096, 0xCAFE,
                    List.of(dead.nodeId(), live1.nodeId(), live2.nodeId()));
            store.createFile(file(fileId, FileState.SEALED, List.of(chunk)));

            RepairCoordinator owner = new RepairCoordinator(store, registry, config(),
                    () -> false, () -> false, ns -> true);
            long beforeEvent = owner.eventRepairs();
            long beforeReconcile = owner.reconcileRepairs();

            owner.ownerRepairPass();

            // the physical pull was still attempted...
            assertEquals(1, received.size(), "owner still sent EXEC_REPLICATE");
            // ...but with every CAS lost, the descriptor must NOT be falsely updated...
            List<Integer> replicas = store.files.get(fileId).value().chunks().get(0).replicas();
            assertTrue(replicas.contains(dead.nodeId()), "dead replica still present — no swap landed");
            assertFalse(replicas.contains(target.nodeId()), "target not added — no swap landed");
            // ...and the repair must NOT be counted as a success (the false-success bug this guards).
            assertEquals(beforeEvent, owner.eventRepairs(), "no event repair counted when the CAS never landed");
            assertEquals(beforeReconcile, owner.reconcileRepairs(),
                    "no reconcile repair counted when the CAS never landed");
        }
    }

    @Test
    void nonControllerOwnerReclaimsDeletedFileViaExecDelete() throws Exception {
        // The reclamation-gap regression: a DELETING file in a namespace owned by a non-leader controller
        // was skipped by ownerRepairPass and never reached the leader's heartbeat command channel, so its
        // physical chunks leaked forever. The owner must now reclaim them directly via DELETE_CHUNKS.
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());

        List<Messages.DeleteChunks> received = new CopyOnWriteArrayList<>();
        UUID inc = UUID.randomUUID();
        try (ScpServer targetServer = new ScpServer(0, 999, inc.getMostSignificantBits(),
                inc.getLeastSignificantBits(), req -> {
                    if (req.opcode() == Opcode.DELETE_CHUNKS.code) {
                        Messages.DeleteChunks m = Messages.DeleteChunks.decode(req.headerSlice());
                        received.add(m);
                        List<Short> codes = new ArrayList<>();
                        for (var id : m.chunkIds()) codes.add(ErrorCode.OK.code);
                        return ScpServer.ok(req, new Messages.DeleteChunksResp(m.chunkIds(), codes).encode(), null);
                    }
                    throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected opcode");
                })) {
            Registered target = registerAt(registry, 700, "target-host", "127.0.0.1:" + targetServer.port());

            FileId fileId = fileId(44);
            store.createFile(file(fileId, FileState.DELETING,
                    List.of(sealed(0, 4096, 0xCAFE, List.of(target.nodeId())))));

            RepairCoordinator owner = new RepairCoordinator(store, registry, config(),
                    () -> false, () -> false, ns -> true);
            owner.ownerRepairPass();

            assertEquals(1, received.size(), "owner sent exactly one DELETE_CHUNKS");
            assertEquals(List.of(new ChunkId(fileId, 0)), received.get(0).chunkIds());
            assertFalse(store.files.containsKey(fileId),
                    "the deleted file's record was reclaimed once its only replica confirmed");
        }
    }

    @Test
    void nonControllerOwnerReclaimsDeletedFileWhoseReplicaIsDeadWithoutRpc() throws Exception {
        // A DELETING file whose only replica sits on a DEAD node: the data is already unreachable, so the
        // owner must converge the descriptor (and reclaim the record) without attempting a DELETE_CHUNKS RPC.
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered dead = register(registry, 710, "dead-host");
        Optional<MetadataStore.Versioned<Records.NodeRecord>> deadRecord = store.getNode(dead.nodeId());
        store.putNode(deadRecord.orElseThrow().value().withState(Records.NodeState.DEAD),
                deadRecord.get().version());

        FileId fileId = fileId(45);
        store.createFile(file(fileId, FileState.DELETING,
                List.of(sealed(0, 4096, 0xBEEF, List.of(dead.nodeId())))));

        RepairCoordinator owner = new RepairCoordinator(store, registry, config(),
                () -> false, () -> false, ns -> true);
        owner.ownerRepairPass();

        assertFalse(store.files.containsKey(fileId),
                "a deleted file whose only replica is dead is reclaimed without an RPC");
    }

    /**
     * Regression test for the per-file liveness bug in ownerRepairPass: a poison file (getFile throws)
     * must not abort the whole pass — the DELETING file that follows it must still be processed.
     * Pre-fix: the exception propagates out of the loop and the DELETING file is never reached.
     * Post-fix: the poison file is logged-and-skipped, the DELETING file is driven to DELETED.
     */
    @Test
    void ownerRepairPassContinuesAfterPoisonFileThrows() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());

        // poison file — listed by the store but getFile throws; placed first in iteration order
        FileId poisonFile = fileId(9001);
        store.createFile(file(poisonFile, FileState.SEALED,
                List.of(sealed(0, 128, 1111, List.of(777)))));

        // DELETING file that must still be processed after the poison file
        FileId deletingFile = fileId(9002);
        store.createFile(file(deletingFile, FileState.DELETING, List.of()));

        // mark the node as DEAD in the store (required by ownerRepairPass's nodesById path)
        // (no live nodes needed: the DELETING file has no chunks so it converges immediately)

        // arm the poison: getFile for poisonFile now throws
        store.throwOnGetFileId = poisonFile;

        // non-controller owner of the namespace
        RepairCoordinator owner = new RepairCoordinator(store, registry, config(),
                () -> false, () -> false, ns -> true);
        owner.ownerRepairPass();

        // the DELETING file must have been reclaimed (no chunks → deleteFile was called)
        assertFalse(store.files.containsKey(deletingFile),
                "DELETING file must be reclaimed even when a preceding file's getFile throws");
    }

    /**
     * Regression test for the cross-namespace DeleteAction in-flight dedup collision.
     *
     * Pre-fix: the alreadyInflight predicate in driveDeletion matched on (chunkId, nodeId) only, with no
     * namespace check. FileId values are per-namespace and ChunkId = (FileId, index) carries no namespace,
     * so two DELETING files in different namespaces with the same numeric FileId and chunk index produce the
     * same ChunkId. The second namespace's DeleteAction was suppressed by the first's in-flight entry, so
     * its physical chunk delete was never enqueued and the file stayed DELETING forever.
     *
     * Post-fix: && d.namespace().equals(ns) is added to the predicate, so the dedup is per-namespace.
     *
     * Setup: one coordinator, one store. scanOnce drives nsA/fileId(2001)/chunk(0) into inflight.
     * Then driveDeletion is called reflectively for nsB/fileId(2001)/chunk(0) — same ChunkId, different
     * namespace. The nsB DeleteCmd must be enqueued. Pre-fix: suppressed → assertion fails.
     */
    @Test
    void driveDeletionEnqueuesDeleteForBothNamespacesWhenChunkIdsCollide() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered node = register(registry, 9200, "collision-node");

        StrataNamespace nsA = StrataNamespace.of("ns-a");
        StrataNamespace nsB = StrataNamespace.of("ns-b");
        FileId sharedFileId = fileId(2001); // same numeric id in both namespaces → same ChunkId

        // nsA file in the store so scanOnce picks it up and populates inflight.
        store.createFile(new Records.FileRecord(sharedFileId, nsA,
                StrataPath.of("/da"), 3, 2, false,
                FileState.DELETING, 1,
                List.of(sealed(0, 64, 0xAA, List.of(node.nodeId())))));

        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        // scanOnce drives nsA's deletion: enqueues DeleteCmd(nsA/sharedFileId/chunk0, node) into inflight.
        coordinator.scanOnce();

        // Drain nsA's DeleteCmd from the registry queue (heartbeat without completions — action stays inflight).
        Messages.HeartbeatResp afterNsA = heartbeat(registry, coordinator, node, List.of());
        assertEquals(1, afterNsA.commands().size(), "nsA DeleteCmd must be enqueued");
        assertInstanceOf(Messages.DeleteCmd.class, afterNsA.commands().get(0));

        // Drive driveDeletion for nsB with the SAME fileId and chunk index (identical ChunkId, different ns).
        // driveDeletion is private; call it reflectively, same pattern as issueReplicate above.
        Records.FileRecord fileBRecord = new Records.FileRecord(sharedFileId, nsB,
                StrataPath.of("/db"), 3, 2, false,
                FileState.DELETING, 1,
                List.of(sealed(0, 64, 0xBB, List.of(node.nodeId()))));
        invokeDriveDeletion(coordinator, fileBRecord, 0 /* store version */);

        // Post-fix: nsB's DeleteCmd must be enqueued (namespace-aware dedup does not suppress it).
        // Pre-fix: nsB's DeleteCmd is suppressed because (chunkId, nodeId) matches nsA's inflight entry
        //          → assertEquals fails with 0 commands.
        Messages.HeartbeatResp afterNsB = heartbeat(registry, coordinator, node, List.of());
        assertEquals(1, afterNsB.commands().size(),
                "nsB DeleteCmd must be enqueued; namespace-blind dedup suppressed it (pre-fix bug)");
        assertInstanceOf(Messages.DeleteCmd.class, afterNsB.commands().get(0));
    }

    /**
     * Regression test for the per-file liveness bug in scanOnce (leader path): a poison file
     * must not abort the whole pass — the DELETING file that follows it must still be processed.
     * Pre-fix: the exception propagates out of the loop and the DELETING file is never reached.
     * Post-fix: the poison file is logged-and-skipped, the DELETING file is driven to DELETED.
     */
    @Test
    void scanOnceContinuesAfterPoisonFileThrows() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());

        // poison file — listed by the store but getFile throws; placed first in iteration order
        FileId poisonFile = fileId(9003);
        store.createFile(file(poisonFile, FileState.SEALED,
                List.of(sealed(0, 128, 2222, List.of(888)))));

        // DELETING file (no chunks) that must be driven to DELETED by the same pass
        FileId deletingFile = fileId(9004);
        store.createFile(file(deletingFile, FileState.DELETING, List.of()));

        // arm the poison: getFile for poisonFile now throws
        store.throwOnGetFileId = poisonFile;

        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);
        coordinator.scanOnce();

        // the DELETING file must have been reclaimed (no chunks → deleteFile was called)
        assertFalse(store.files.containsKey(deletingFile),
                "DELETING file must be reclaimed even when a preceding file's getFile throws");
    }

    private static Registered registerAt(NodeRegistry registry, long incMsb, String host, String endpoint)
            throws Exception {
        long incLsb = incMsb + 1;
        Messages.RegisterResp resp = registry.register(new Messages.RegisterNode(
                (int) incMsb, incMsb, incLsb, List.of(endpoint), "zone", "rack", host,
                List.of(new Messages.StorageCapacity(1_000_000)), 1, 0));
        return new Registered(resp.nodeId(), incMsb, incLsb, resp.sessionEpoch());
    }

    @Test
    void verifyPassThrottlesSystemNamespaceToSlowCadence() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        FileId sysFile = fileId(700);
        // OPEN file in the system metadata-log namespace, no sealed chunks -> verifyFile is a no-op after
        // its single getFile, so getFile count == number of times the system namespace was verified.
        store.createFile(new Records.FileRecord(sysFile, "strata-meta", "/metadata-log/seg-700",
                3, 2, false, FileState.OPEN, 1234, List.of()));
        RepairCoordinator coordinator =
                new RepairCoordinator(store, registry, config(), () -> false, () -> false, ns -> true);
        coordinator.systemVerifyIntervalMsForTest(60_000);

        coordinator.verifyPass();
        coordinator.verifyPass();
        coordinator.verifyPass();

        assertEquals(1, store.getFileCalls(sysFile),
                "system namespace is owner-verified once per slow window, not every brisk verify pass");
    }

    @Test
    void verifyPassRunsSystemNamespaceEveryPassWhenIntervalElapsed() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        FileId sysFile = fileId(701);
        store.createFile(new Records.FileRecord(sysFile, "strata-meta", "/metadata-log/seg-701",
                3, 2, false, FileState.OPEN, 1234, List.of()));
        RepairCoordinator coordinator =
                new RepairCoordinator(store, registry, config(), () -> false, () -> false, ns -> true);
        coordinator.systemVerifyIntervalMsForTest(0);  // window always elapsed

        coordinator.verifyPass();
        coordinator.verifyPass();

        assertEquals(2, store.getFileCalls(sysFile),
                "with the throttle window elapsed the system namespace is verified every pass");
    }

    @Test
    void systemVerifyCadenceComesFromConfig() throws Exception {
        // Verify the cadence is read from config.systemVerifyIntervalMs(), not from the old static constant.
        // Strategy: configure a 50 ms window. Sleep 100 ms between passes. Before the fix, the field was
        // initialized from SYSTEM_VERIFY_INTERVAL_MS (30 000 ms), so 100 ms would NOT re-trigger — only
        // 1 getFile call for 2 passes. After the fix, 100 ms > 50 ms → the second pass re-verifies → 2 calls.
        ControllerConfig cfg = config().withSystemVerifyIntervalMs(50);
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, cfg);
        FileId sysFile = fileId(702);
        // OPEN file in the system metadata-log namespace, no sealed chunks -> verifyFile is a no-op after
        // its single getFile, so getFile count == number of times the system namespace was verified.
        store.createFile(new Records.FileRecord(sysFile, "strata-meta", "/metadata-log/seg-702",
                3, 2, false, FileState.OPEN, 1234, List.of()));
        // non-leader owner that owns all namespaces (ownsAll=false so non-leader, ownsNamespace=true always)
        RepairCoordinator coordinator =
                new RepairCoordinator(store, registry, cfg, () -> false, () -> false, ns -> true);

        coordinator.verifyPass();                // stamps lastSystemVerifyMs
        Thread.sleep(100);                       // 100 ms > configured 50 ms window → should re-verify
        coordinator.verifyPass();                // must re-verify after the window elapses

        assertEquals(2, store.getFileCalls(sysFile),
                "system verify cadence must come from config.systemVerifyIntervalMs() (50 ms), "
                        + "not the deleted 30 000 ms constant");
    }

    private static ControllerConfig config() {
        return config(1);
    }

    private static ControllerConfig config(int repairCommandTimeoutMs) {
        // grace 0: these tests assert prompt missing-replica drops
        return new ControllerConfig("unused", 0, 1, 60_000, 0, 1,
                repairCommandTimeoutMs).withReplicaMissingGraceMs(0);
    }

    private static Registered register(NodeRegistry registry, long incMsb, String host) throws Exception {
        long incLsb = incMsb + 1;
        Messages.RegisterResp resp = registry.register(new Messages.RegisterNode(
                (int) incMsb, incMsb, incLsb, List.of(host + ":9000"), "zone", "rack", host,
                List.of(new Messages.StorageCapacity(1_000_000)), 1, 0));
        return new Registered(resp.nodeId(), incMsb, incLsb, resp.sessionEpoch());
    }

    private static Messages.HeartbeatResp heartbeat(NodeRegistry registry, RepairCoordinator coordinator,
                                                    Registered node,
                                                    List<Messages.CompletedCommand> completedCommands) {
        return registry.heartbeat(new Messages.NodeHeartbeat(node.nodeId(), node.incMsb(), node.incLsb(),
                node.sessionEpoch(), List.of(new Messages.StorageUsage(0, 1_000_000)),
                0, completedCommands),
                (nodeId, incMsb, incLsb, sessionEpoch, completion) -> {
                    coordinator.onCommandCompleted(nodeId, completion);
                    return CompletableFuture.completedFuture(null);
                });
    }

    private static Messages.Command onlyCommand(Messages.HeartbeatResp heartbeat) {
        assertEquals(1, heartbeat.commands().size());
        return heartbeat.commands().get(0);
    }

    private static void issueReplicate(RepairCoordinator coordinator, FileId fileId,
                                       Records.FileRecord file, Records.ChunkRecord chunk) throws Exception {
        Method method = RepairCoordinator.class.getDeclaredMethod(
                "issueReplicate", StrataNamespace.class, FileId.class, Records.FileRecord.class,
                Records.ChunkRecord.class, RepairCoordinator.RepairTrigger.class);
        method.setAccessible(true);
        method.invoke(coordinator, file.namespace(), fileId, file, chunk, RepairCoordinator.RepairTrigger.RECONCILE);
    }

    private static Object replicateAction(FileId fileId, ChunkId chunkId, int deadNode, int targetNode)
            throws Exception {
        Class<?> type = Class.forName("io.strata.meta.RepairCoordinator$ReplicateAction");
        Constructor<?> ctor = type.getDeclaredConstructor(StrataNamespace.class, FileId.class, ChunkId.class,
                int.class, int.class, long.class);
        ctor.setAccessible(true);
        return ctor.newInstance(TEST_NS, fileId, chunkId, deadNode, targetNode, System.currentTimeMillis());
    }

    private static void applyReplicaSwap(RepairCoordinator coordinator, Object action) throws Exception {
        Method method = RepairCoordinator.class.getDeclaredMethod("applyReplicaSwap", action.getClass());
        method.setAccessible(true);
        method.invoke(coordinator, action);
    }

    private static void applyDeleteConfirmed(RepairCoordinator coordinator, FileId fileId, ChunkId chunkId, int nodeId)
            throws Exception {
        Method method = RepairCoordinator.class.getDeclaredMethod(
                "applyDeleteConfirmed", StrataNamespace.class, FileId.class, ChunkId.class, int.class);
        method.setAccessible(true);
        method.invoke(coordinator, TEST_NS, fileId, chunkId, nodeId);
    }

    /** Calls the private driveDeletion(FileRecord, int) on the coordinator via reflection. */
    private static void invokeDriveDeletion(RepairCoordinator coordinator,
                                            Records.FileRecord file, int version) throws Exception {
        Method method = RepairCoordinator.class.getDeclaredMethod(
                "driveDeletion", Records.FileRecord.class, int.class);
        method.setAccessible(true);
        method.invoke(coordinator, file, version);
    }

    private static Records.FileRecord file(FileId fileId, FileState state,
                                           List<Records.ChunkRecord> chunks) {
        return new Records.FileRecord(fileId, "test", "/file-" + fileId, 3, 2, false, state, 1234, chunks);
    }

    private static Records.ChunkRecord sealed(int index, long length, int crc, List<Integer> replicas) {
        return new Records.ChunkRecord(index, ChunkState.SEALED, length, crc, 1, replicas);
    }

    private static FileId fileId(long lsb) {
        return FileId.of(lsb);
    }

    private static void markDead(NodeRegistry registry, int nodeId) throws Exception {
        NodeRegistry.LiveNode node = liveNodes(registry).get(nodeId);
        node.record = node.record.withState(Records.NodeState.DEAD);
    }

    @SuppressWarnings("unchecked")
    private static Map<Integer, NodeRegistry.LiveNode> liveNodes(NodeRegistry registry) throws Exception {
        Field live = NodeRegistry.class.getDeclaredField("live");
        live.setAccessible(true);
        return (Map<Integer, NodeRegistry.LiveNode>) live.get(registry);
    }

    private record Registered(int nodeId, long incMsb, long incLsb, long sessionEpoch) {}

    private static final class FakeStore implements MetadataStore {
        private final Map<FileId, Versioned<Records.FileRecord>> files = new LinkedHashMap<>();
        private final Map<Name, FileId> paths = new LinkedHashMap<>();
        private final Map<Integer, Versioned<Records.NodeRecord>> nodes = new LinkedHashMap<>();
        private final List<FileId> extraListedFiles = new ArrayList<>();
        /** Per-file getFile call counter — test observability for the system-namespace verify throttle. */
        private final Map<FileId, Integer> getFileCalls = new LinkedHashMap<>();
        // a fileId that enumeration surfaces but getFile cannot resolve (orphan/race) lives under this
        // sentinel namespace, so per-namespace enumeration still exercises repair's skip-on-missing path
        private static final StrataNamespace ORPHAN_NS =
                StrataNamespace.of("orphan-listed");
        private int failUpdateFileAttempts;
        private int failDeleteFileAttempts;
        private boolean throwOnGetFile;
        private boolean throwOnListFiles;
        /** If non-null, getFile throws for this specific fileId (poison-file injection). */
        private FileId throwOnGetFileId;

        @Override
        public void createFile(Records.FileRecord record) {
            if (files.containsKey(record.fileId())) {
                throw new IllegalStateException("file already exists");
            }
            if (paths.containsKey(new Name(record.namespace(), record.path()))) {
                throw new IllegalStateException("path already exists");
            }
            files.put(record.fileId(), new Versioned<>(record, 0));
            paths.put(new Name(record.namespace(), record.path()), record.fileId());
        }

        @Override
        public int sweepDeletedFiles(long olderThanMs) {
            return 0;
        }

        @Override
        public Optional<Versioned<Records.FileRecord>> getFile(StrataNamespace namespace, FileId id) {
            getFileCalls.merge(id, 1, Integer::sum);
            if (throwOnGetFile) {
                throw new IllegalStateException("getFile failure");
            }
            if (throwOnGetFileId != null && throwOnGetFileId.equals(id)) {
                throw new IllegalStateException("poison file getFile failure for " + id);
            }
            return Optional.ofNullable(files.get(id));
        }

        int getFileCalls(FileId id) {
            return getFileCalls.getOrDefault(id, 0);
        }

        @Override
        public Optional<FileId> resolvePath(StrataNamespace namespace,
                                            StrataPath path) {
            return Optional.ofNullable(paths.get(new Name(namespace, path)));
        }

        @Override
        public boolean updateFile(Records.FileRecord record, int expectedVersion) {
            Versioned<Records.FileRecord> current = files.get(record.fileId());
            if (current == null || current.version() != expectedVersion) {
                return false;
            }
            if (failUpdateFileAttempts > 0) {
                failUpdateFileAttempts--;
                files.put(record.fileId(), new Versioned<>(current.value(), current.version() + 1));
                return false;
            }
            files.put(record.fileId(), new Versioned<>(record, current.version() + 1));
            return true;
        }

        @Override
        public boolean deletePath(StrataNamespace namespace, StrataPath path,
                                  FileId expectedFileId) {
            Name name = new Name(namespace, path);
            FileId current = paths.get(name);
            if (current == null) {
                return true;
            }
            if (!current.equals(expectedFileId)) {
                return false;
            }
            paths.remove(name);
            return true;
        }

        @Override
        public boolean deleteFile(StrataNamespace namespace, FileId id, int expectedVersion) {
            Versioned<Records.FileRecord> current = files.get(id);
            if (current == null) {
                return true;
            }
            if (current.version() != expectedVersion) {
                return false;
            }
            if (failDeleteFileAttempts > 0) {
                failDeleteFileAttempts--;
                files.put(id, new Versioned<>(current.value(), current.version() + 1));
                return false;
            }
            files.remove(id);
            paths.remove(new Name(current.value().namespace(), current.value().path()), id);
            return true;
        }

        @Override
        public List<FileId> listFiles(StrataNamespace namespace) {
            if (throwOnListFiles) {
                throw new IllegalStateException("listFiles failure");
            }
            if (namespace.equals(ORPHAN_NS)) {
                return new ArrayList<>(extraListedFiles);
            }
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
            if (throwOnListFiles) {
                throw new IllegalStateException("listFiles failure");
            }
            TreeSet<StrataNamespace> namespaces = new TreeSet<>();
            for (var v : files.values()) {
                namespaces.add(v.value().namespace());
            }
            if (!extraListedFiles.isEmpty()) {
                namespaces.add(ORPHAN_NS);  // surface orphan/recordless ids through enumeration
            }
            return new ArrayList<>(namespaces);
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

    private record Name(StrataNamespace namespace, StrataPath path) {}
}
