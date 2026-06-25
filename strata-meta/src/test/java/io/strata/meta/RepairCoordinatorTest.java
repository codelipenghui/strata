package io.strata.meta;

import io.strata.common.ErrorCode;
import io.strata.common.FileState;
import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.StrataNamespace;
import io.strata.common.FileId;
import io.strata.common.ScpException;
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
    void inventoryDoesNotDeleteRepairTargetCopyWhileCommandIsInFlight() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered source = register(registry, 301, "source");
        Registered target = register(registry, 401, "target");
        FileId fileId = fileId(301);
        ChunkId chunkId = new ChunkId(fileId, 0);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 512, 3001, List.of(source.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.scanOnce();
        Messages.ReplicateCmd replicate = assertInstanceOf(Messages.ReplicateCmd.class,
                onlyCommand(heartbeat(registry, coordinator, target, List.of())));
        coordinator.onInventory(inventory(target, List.of(
                new Messages.InventoryEntry(chunkId, ChunkState.SEALED, 512, 3001, TEST_NS))));
        assertTrue(heartbeat(registry, coordinator, target, List.of()).commands().isEmpty());
        registry.enqueue(target.nodeId(), new Messages.DeleteCmd(123, List.of(chunkId), TEST_NS));

        Messages.HeartbeatResp afterCompletion = heartbeat(registry, coordinator, target,
                List.of(new Messages.CompletedCommand(replicate.commandId(), (short) 0)));

        assertTrue(afterCompletion.commands().isEmpty());
        assertEquals(List.of(source.nodeId(), target.nodeId()),
                store.files.get(fileId).value().chunks().get(0).replicas());
    }

    @Test
    void staleMissingInventoryAfterRepairSwapDoesNotDropNewReplica() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered source = register(registry, 302, "source");
        Registered target = register(registry, 402, "target");
        FileId fileId = fileId(302);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 512, 3002, List.of(source.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.scanOnce();
        Messages.ReplicateCmd replicate = assertInstanceOf(Messages.ReplicateCmd.class,
                onlyCommand(heartbeat(registry, coordinator, target, List.of())));
        coordinator.onCommandCompleted(target.nodeId(),
                new Messages.CompletedCommand(replicate.commandId(), (short) 0));
        coordinator.onInventory(inventory(target, List.of()));

        assertEquals(List.of(source.nodeId(), target.nodeId()),
                store.files.get(fileId).value().chunks().get(0).replicas());
        assertTrue(heartbeat(registry, coordinator, target, List.of()).commands().isEmpty());
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
    void inventoryDropsMissingCorruptOpenAndDeletingReplicasAndDeletesOrphans() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered node = register(registry, 150, "node");
        FileId fileId = fileId(10);
        ChunkId missing = new ChunkId(fileId, 0);
        ChunkId corrupt = new ChunkId(fileId, 1);
        ChunkId open = new ChunkId(fileId, 2);
        ChunkId deleting = new ChunkId(fileId, 3);
        ChunkId orphan = new ChunkId(fileId(11), 0);
        store.createFile(file(fileId, FileState.SEALED, List.of(
                sealed(0, 100, 100, List.of(node.nodeId())),
                sealed(1, 200, 200, List.of(node.nodeId())),
                sealed(2, 300, 300, List.of(node.nodeId())),
                sealed(3, 400, 400, List.of(node.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.onInventory(inventory(node, List.of(
                new Messages.InventoryEntry(corrupt, ChunkState.SEALED, 201, 200, TEST_NS),
                new Messages.InventoryEntry(open, ChunkState.OPEN, 300, 300, TEST_NS),
                new Messages.InventoryEntry(deleting, ChunkState.DELETING, 400, 400, TEST_NS),
                new Messages.InventoryEntry(orphan, ChunkState.SEALED, 1, 1, TEST_NS))));

        Records.FileRecord file = store.files.get(fileId).value();
        assertEquals(List.of(List.of(), List.of(), List.of(), List.of()),
                file.chunks().stream().map(Records.ChunkRecord::replicas).toList());
        Messages.HeartbeatResp heartbeat = heartbeat(registry, coordinator, node, List.of());
        assertEquals(4, heartbeat.commands().size());
        assertEquals(List.of(orphan),
                assertInstanceOf(Messages.DeleteCmd.class, heartbeat.commands().get(0)).chunkIds());
        assertEquals(List.of(corrupt),
                assertInstanceOf(Messages.DeleteCmd.class, heartbeat.commands().get(1)).chunkIds());
        assertEquals(List.of(open),
                assertInstanceOf(Messages.DeleteCmd.class, heartbeat.commands().get(2)).chunkIds());
        assertEquals(List.of(deleting),
                assertInstanceOf(Messages.DeleteCmd.class, heartbeat.commands().get(3)).chunkIds());
        assertEquals(missing, file.chunkId(0));
    }

    @Test
    void inventoryGracesStaleOpenReportForJustSealedReplica() throws Exception {
        FakeStore store = new FakeStore();
        ControllerConfig graceConfig = config().withReplicaMissingGraceMs(60_000);
        NodeRegistry registry = new NodeRegistry(store, graceConfig);
        Registered node = register(registry, 151, "node");
        FileId fileId = fileId(12);
        ChunkId chunkId = new ChunkId(fileId, 0);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 100, 100, List.of(node.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, graceConfig, () -> true);

        coordinator.onInventory(inventory(node, List.of(
                new Messages.InventoryEntry(chunkId, ChunkState.OPEN, 100, 100, TEST_NS))));

        assertEquals(List.of(node.nodeId()), store.files.get(fileId).value().chunks().get(0).replicas());
        assertTrue(heartbeat(registry, coordinator, node, List.of()).commands().isEmpty());

        coordinator.onInventory(inventory(node, List.of(
                new Messages.InventoryEntry(chunkId, ChunkState.SEALED, 100, 100, TEST_NS))));

        assertEquals(List.of(node.nodeId()), store.files.get(fileId).value().chunks().get(0).replicas());
        assertTrue(heartbeat(registry, coordinator, node, List.of()).commands().isEmpty());
    }

    @Test
    void inventoryIgnoresNonLiveNodesAndContinuesAfterStoreFailure() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.onInventory(new Messages.InventoryReport(999, 1, 2, 3, 0, 1,
                List.of(new Messages.InventoryEntry(new ChunkId(fileId(85), 0), ChunkState.SEALED, 1, 1, TEST_NS))));

        Registered node = register(registry, 146, "node");
        store.throwOnListFiles = true;
        coordinator.onInventory(inventory(node, List.of()));
        store.throwOnListFiles = false;

        assertTrue(heartbeat(registry, coordinator, node, List.of()).commands().isEmpty());
    }

    @Test
    void inventoryIgnoresStaleSessionForLiveNode() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered node = register(registry, 1461, "node");
        FileId fileId = fileId(851);
        store.createFile(file(fileId, FileState.SEALED,
                List.of(sealed(0, 10, 100, List.of(node.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.onInventory(new Messages.InventoryReport(node.nodeId(), node.incMsb(), node.incLsb(),
                node.sessionEpoch() - 1, 0, 1, List.of()));

        assertEquals(List.of(node.nodeId()), store.files.get(fileId).value().chunks().get(0).replicas());
        assertTrue(heartbeat(registry, coordinator, node, List.of()).commands().isEmpty());
    }

    @Test
    void inventoryLeavesKnownHealthyDeletingAndOpenEntriesAlone() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        Registered node = register(registry, 147, "node");
        FileId healthyFile = fileId(86);
        ChunkId healthy = new ChunkId(healthyFile, 0);
        FileId deletingFile = fileId(87);
        FileId openFile = fileId(88);
        store.createFile(file(healthyFile, FileState.SEALED,
                List.of(sealed(0, 10, 100, List.of(node.nodeId())))));
        store.createFile(file(deletingFile, FileState.DELETING,
                List.of(sealed(0, 20, 200, List.of(node.nodeId())))));
        store.createFile(new Records.FileRecord(openFile, "test", "/open-file", 3, 2, true, FileState.OPEN, 1234,
                List.of(new Records.ChunkRecord(0, ChunkState.OPEN, 0, 0, 1, List.of(node.nodeId())))));
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.onInventory(inventory(node, List.of(
                new Messages.InventoryEntry(healthy, ChunkState.SEALED, 10, 100, TEST_NS))));

        assertEquals(List.of(node.nodeId()), store.files.get(healthyFile).value().chunks().get(0).replicas());
        assertEquals(List.of(node.nodeId()), store.files.get(deletingFile).value().chunks().get(0).replicas());
        assertEquals(List.of(node.nodeId()), store.files.get(openFile).value().chunks().get(0).replicas());
        assertTrue(heartbeat(registry, coordinator, node, List.of()).commands().isEmpty());
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
    void shardedControllerDoesNotOrphanDeleteAChunkItDoesNotOwn() throws Exception {
        // A sealed chunk reported by a node whose file this node cannot see. Under sharding the file may
        // belong to a namespace owned by another controller node, so it must NOT be deleted as an orphan.
        FileId foreignFile = fileId(7777);
        ChunkId foreignChunk = new ChunkId(foreignFile, 0);
        Messages.InventoryEntry entry =
                new Messages.InventoryEntry(foreignChunk, ChunkState.SEALED, 4096, 0xAB, TEST_NS);

        // sharded (ownsAll = false): the unknown chunk is left alone (no delete command).
        FakeStore shardedStore = new FakeStore();
        NodeRegistry shardedRegistry = new NodeRegistry(shardedStore, config());
        Registered shardedTarget = register(shardedRegistry, 900, "target");
        RepairCoordinator sharded = new RepairCoordinator(shardedStore, shardedRegistry, config(),
                () -> true, () -> false);
        sharded.onInventory(inventory(shardedTarget, List.of(entry)));
        assertTrue(heartbeat(shardedRegistry, sharded, shardedTarget, List.of()).commands().isEmpty(),
                "a sharded controller must not orphan-delete another owner's chunk");

        // non-sharded (ownsAll = true): the same unknown chunk is a genuine orphan and is deleted.
        FakeStore ownsAllStore = new FakeStore();
        NodeRegistry ownsAllRegistry = new NodeRegistry(ownsAllStore, config());
        Registered ownsAllTarget = register(ownsAllRegistry, 901, "target");
        RepairCoordinator ownsAll = new RepairCoordinator(ownsAllStore, ownsAllRegistry, config(), () -> true);
        ownsAll.onInventory(inventory(ownsAllTarget, List.of(entry)));
        assertInstanceOf(Messages.DeleteCmd.class,
                onlyCommand(heartbeat(ownsAllRegistry, ownsAll, ownsAllTarget, List.of())),
                "a non-sharded controller deletes a genuine orphan");
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

    private static Messages.InventoryReport inventory(Registered node, List<Messages.InventoryEntry> entries) {
        return new Messages.InventoryReport(node.nodeId(), node.incMsb(), node.incLsb(),
                node.sessionEpoch(), 0, 1, entries);
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
        // a fileId that enumeration surfaces but getFile cannot resolve (orphan/race) lives under this
        // sentinel namespace, so per-namespace enumeration still exercises repair's skip-on-missing path
        private static final io.strata.common.StrataNamespace ORPHAN_NS =
                io.strata.common.StrataNamespace.of("orphan-listed");
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
        public Optional<Versioned<Records.FileRecord>> getFile(io.strata.common.StrataNamespace namespace, FileId id) {
            if (throwOnGetFile) {
                throw new IllegalStateException("getFile failure");
            }
            if (throwOnGetFileId != null && throwOnGetFileId.equals(id)) {
                throw new IllegalStateException("poison file getFile failure for " + id);
            }
            return Optional.ofNullable(files.get(id));
        }

        @Override
        public Optional<FileId> resolvePath(io.strata.common.StrataNamespace namespace,
                                            io.strata.common.StrataPath path) {
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
        public boolean deletePath(io.strata.common.StrataNamespace namespace, io.strata.common.StrataPath path,
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
        public boolean deleteFile(io.strata.common.StrataNamespace namespace, FileId id, int expectedVersion) {
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
        public List<FileId> listFiles(io.strata.common.StrataNamespace namespace) {
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
        public List<io.strata.common.StrataNamespace> listNamespaces() {
            if (throwOnListFiles) {
                throw new IllegalStateException("listFiles failure");
            }
            java.util.TreeSet<io.strata.common.StrataNamespace> namespaces = new java.util.TreeSet<>();
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

    private record Name(io.strata.common.StrataNamespace namespace, io.strata.common.StrataPath path) {}
}
