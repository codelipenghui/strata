package io.strata.meta;

import io.strata.common.FileState;
import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.proto.Messages;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepairCoordinatorTest {
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
                new Messages.InventoryEntry(chunkId, ChunkState.SEALED, 512, 3001))));
        assertTrue(heartbeat(registry, coordinator, target, List.of()).commands().isEmpty());
        registry.enqueue(target.nodeId(), new Messages.DeleteCmd(123, List.of(chunkId)));

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
        MetaConfig config = config(-1);
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
        MetaConfig config = config(-1);
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
                new Messages.InventoryEntry(corrupt, ChunkState.SEALED, 201, 200),
                new Messages.InventoryEntry(open, ChunkState.OPEN, 300, 300),
                new Messages.InventoryEntry(deleting, ChunkState.DELETING, 400, 400),
                new Messages.InventoryEntry(orphan, ChunkState.SEALED, 1, 1))));

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
    void inventoryIgnoresNonLiveNodesAndContinuesAfterStoreFailure() throws Exception {
        FakeStore store = new FakeStore();
        NodeRegistry registry = new NodeRegistry(store, config());
        RepairCoordinator coordinator = new RepairCoordinator(store, registry, config(), () -> true);

        coordinator.onInventory(new Messages.InventoryReport(999, 1, 2, 3, 0, 1,
                List.of(new Messages.InventoryEntry(new ChunkId(fileId(85), 0), ChunkState.SEALED, 1, 1))));

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
                new Messages.InventoryEntry(healthy, ChunkState.SEALED, 10, 100))));

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
        MetaConfig fast = new MetaConfig("unused", 0, 1, 10_000, 0, 1, 1);
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

    private static MetaConfig config() {
        return config(1);
    }

    private static MetaConfig config(int repairCommandTimeoutMs) {
        // grace 0: these tests assert prompt missing-replica drops
        return new MetaConfig("unused", 0, 1, 60_000, 0, 1,
                repairCommandTimeoutMs).withReplicaMissingGraceMs(0);
    }

    private static Registered register(NodeRegistry registry, long incMsb, String host) throws Exception {
        long incLsb = incMsb + 1;
        Messages.RegisterResp resp = registry.register(new Messages.RegisterNode(
                incMsb, incLsb, List.of(host + ":9000"), "zone", "rack", host,
                List.of(new Messages.StorageCapacity(1_000_000)), 1, 0));
        return new Registered(resp.nodeId(), incMsb, incLsb, resp.sessionEpoch());
    }

    private static Messages.HeartbeatResp heartbeat(NodeRegistry registry, RepairCoordinator coordinator,
                                                    Registered node,
                                                    List<Messages.CompletedCommand> completedCommands) {
        return registry.heartbeat(new Messages.NodeHeartbeat(node.nodeId(), node.incMsb(), node.incLsb(),
                node.sessionEpoch(), List.of(new Messages.StorageUsage(0, 1_000_000)),
                0, completedCommands), coordinator::onCommandCompleted);
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
                "issueReplicate", FileId.class, Records.FileRecord.class, Records.ChunkRecord.class);
        method.setAccessible(true);
        method.invoke(coordinator, fileId, file, chunk);
    }

    private static Object replicateAction(FileId fileId, ChunkId chunkId, int deadNode, int targetNode)
            throws Exception {
        Class<?> type = Class.forName("io.strata.meta.RepairCoordinator$ReplicateAction");
        Constructor<?> ctor = type.getDeclaredConstructor(FileId.class, ChunkId.class, int.class, int.class,
                long.class);
        ctor.setAccessible(true);
        return ctor.newInstance(fileId, chunkId, deadNode, targetNode, System.currentTimeMillis());
    }

    private static void applyReplicaSwap(RepairCoordinator coordinator, Object action) throws Exception {
        Method method = RepairCoordinator.class.getDeclaredMethod("applyReplicaSwap", action.getClass());
        method.setAccessible(true);
        method.invoke(coordinator, action);
    }

    private static void applyDeleteConfirmed(RepairCoordinator coordinator, FileId fileId, ChunkId chunkId, int nodeId)
            throws Exception {
        Method method = RepairCoordinator.class.getDeclaredMethod(
                "applyDeleteConfirmed", FileId.class, ChunkId.class, int.class);
        method.setAccessible(true);
        method.invoke(coordinator, fileId, chunkId, nodeId);
    }

    private static Records.FileRecord file(FileId fileId, FileState state,
                                           List<Records.ChunkRecord> chunks) {
        return new Records.FileRecord(fileId, "test", "/file-" + fileId, 3, 2, false, state, 1234, chunks);
    }

    private static Records.ChunkRecord sealed(int index, long length, int crc, List<Integer> replicas) {
        return new Records.ChunkRecord(index, ChunkState.SEALED, length, crc, 1, replicas);
    }

    private static FileId fileId(long lsb) {
        return new FileId(0, lsb);
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
        private int nextNodeId = 1;
        private int failUpdateFileAttempts;
        private int failDeleteFileAttempts;
        private boolean throwOnGetFile;
        private boolean throwOnListFiles;

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
        public Optional<Versioned<Records.FileRecord>> getFile(FileId id) {
            if (throwOnGetFile) {
                throw new IllegalStateException("getFile failure");
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
        public boolean deleteFile(FileId id, int expectedVersion) {
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

    private record Name(io.strata.common.StrataNamespace namespace, io.strata.common.StrataPath path) {}
}
