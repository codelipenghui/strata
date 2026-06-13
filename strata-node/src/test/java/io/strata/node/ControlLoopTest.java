package io.strata.node;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.ConnectionPolicy;
import io.strata.common.Endpoint;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.format.ChunkFormats;
import io.strata.format.ChunkStore;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import io.strata.proto.ScpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlLoopTest {

    @TempDir
    Path dir;

    @Test
    void closingControlLoopOpensNoMetadataConnection() throws Exception {
        // Finding: close() used to disconnect() BEFORE joining the control thread, and
        // ensureRegistered() never checked the closed flag — so the control thread could open a
        // fresh ManagedScpConnection (and its monitor virtual thread) after shutdown began,
        // leaking it. The guard below is the deterministic half of that fix: a control loop that
        // observes itself closed must not open a connection at all.
        try (ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.REGISTER_NODE) {
                Messages.RegisterNode.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.RegisterResp(7, 11, 1, 60_000).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, config(metaServer, 60_000), node.store());
            getClosed(loop).set(true);

            invoke(loop, "ensureRegistered"); // the control thread runs this each iteration

            assertNull(get(loop, "meta"),
                    "a control loop observed as closed must not open a new metadata connection");
            loop.close();
        }
    }

    @Test
    void startLaunchesWorkersAndCloseStopsThem() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, configWithoutMetadata(), node.store());

            loop.start();
            List<Thread> threads = get(loop, "threads");
            assertEquals(3, threads.size());

            loop.close();
            for (Thread thread : threads) {
                assertFalse(thread.isAlive());
            }
        }
    }

    @Test
    void runReturnsWhenInterruptedAfterSuccessfulHeartbeat() throws Exception {
        AtomicInteger heartbeats = new AtomicInteger();
        try (ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.REGISTER_NODE) {
                Messages.RegisterNode.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.RegisterResp(7, 11, 1, 60_000).encode(), null);
            }
            if (op == Opcode.NODE_HEARTBEAT) {
                Messages.NodeHeartbeat.decode(req.headerSlice());
                heartbeats.incrementAndGet();
                return ScpServer.ok(req,
                        new Messages.HeartbeatResp(System.currentTimeMillis() + 60_000, List.of()).encode(),
                        null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, config(metaServer, 60_000), node.store());

            Thread worker = Thread.ofVirtual().name("control-loop-test-interrupt").start(() -> {
                try {
                    invoke(loop, "run");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            waitFor(() -> heartbeats.get() == 1);
            worker.interrupt();
            worker.join(2_000);

            assertFalse(worker.isAlive());
            loop.close();
        }
    }

    @Test
    void runClearsSessionWhenMetadataRejectsHeartbeatSession() throws Exception {
        AtomicInteger heartbeats = new AtomicInteger();
        AtomicReference<AtomicBoolean> closedRef = new AtomicReference<>();
        try (ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.REGISTER_NODE) {
                Messages.RegisterNode.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.RegisterResp(7, 11, 1, 100).encode(), null);
            }
            if (op == Opcode.NODE_HEARTBEAT) {
                Messages.NodeHeartbeat.decode(req.headerSlice());
                heartbeats.incrementAndGet();
                closedRef.get().set(true);
                throw new ScpException(ErrorCode.NOT_REGISTERED, "session expired");
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, config(metaServer, 60_000), node.store());
            closedRef.set(getClosed(loop));

            Thread worker = Thread.ofVirtual().name("control-loop-test-not-registered").start(() -> {
                try {
                    invoke(loop, "run");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            waitFor(() -> heartbeats.get() == 1);
            worker.join(2_000);

            assertEquals(1, heartbeats.get());
            assertEquals(-1, getLong(loop, "sessionEpoch"));
            loop.close();
        }
    }

    @Test
    void runClearsSessionWhenLeaseExpires() throws Exception {
        AtomicInteger heartbeats = new AtomicInteger();
        AtomicReference<AtomicBoolean> closedRef = new AtomicReference<>();
        try (ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.REGISTER_NODE) {
                Messages.RegisterNode.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.RegisterResp(7, 11, 1, 100).encode(), null);
            }
            if (op == Opcode.NODE_HEARTBEAT) {
                Messages.NodeHeartbeat.decode(req.headerSlice());
                heartbeats.incrementAndGet();
                closedRef.get().set(true);
                throw new ScpException(ErrorCode.LEASE_EXPIRED, "lease expired");
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, config(metaServer, 60_000), node.store());
            closedRef.set(getClosed(loop));

            Thread worker = Thread.ofVirtual().name("control-loop-test-lease-expired").start(() -> {
                try {
                    invoke(loop, "run");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            waitFor(() -> heartbeats.get() == 1);
            worker.join(2_000);

            assertEquals(1, heartbeats.get());
            assertEquals(-1, getLong(loop, "sessionEpoch"));
            loop.close();
        }
    }

    @Test
    void runRotatesMetadataEndpointWhenHeartbeatReportsNotLeader() throws Exception {
        AtomicInteger heartbeats = new AtomicInteger();
        AtomicReference<AtomicBoolean> closedRef = new AtomicReference<>();
        try (ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.REGISTER_NODE) {
                Messages.RegisterNode.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.RegisterResp(7, 11, 1, 100).encode(), null);
            }
            if (op == Opcode.NODE_HEARTBEAT) {
                Messages.NodeHeartbeat.decode(req.headerSlice());
                heartbeats.incrementAndGet();
                closedRef.get().set(true);
                throw new ScpException(ErrorCode.NOT_LEADER, "standby");
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, config(metaServer, 60_000), node.store());
            closedRef.set(getClosed(loop));

            Thread worker = Thread.ofVirtual().name("control-loop-test-not-leader").start(() -> {
                try {
                    invoke(loop, "run");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            waitFor(() -> heartbeats.get() == 1);
            worker.join(2_000);

            assertEquals(1, heartbeats.get());
            assertEquals(1, getInt(loop, "endpointIndex"));
            assertEquals(-1, getLong(loop, "sessionEpoch"));
            loop.close();
        }
    }

    @Test
    void runLogsGenericScpExceptionWithoutClearingSession() throws Exception {
        AtomicInteger heartbeats = new AtomicInteger();
        AtomicReference<AtomicBoolean> closedRef = new AtomicReference<>();
        try (ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.REGISTER_NODE) {
                Messages.RegisterNode.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.RegisterResp(7, 11, 1, 100).encode(), null);
            }
            if (op == Opcode.NODE_HEARTBEAT) {
                Messages.NodeHeartbeat.decode(req.headerSlice());
                heartbeats.incrementAndGet();
                closedRef.get().set(true);
                throw new ScpException(ErrorCode.INTERNAL, "transient metadata failure");
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, config(metaServer, 60_000), node.store());
            closedRef.set(getClosed(loop));

            Thread worker = Thread.ofVirtual().name("control-loop-test-generic-scp").start(() -> {
                try {
                    invoke(loop, "run");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            waitFor(() -> heartbeats.get() == 1);
            worker.join(2_000);

            assertEquals(1, heartbeats.get());
            assertEquals(11, getLong(loop, "sessionEpoch"));
            loop.close();
        }
    }

    @Test
    void registrationAndHeartbeatQueueCommandsAndRequeueFailedCompletions() throws Exception {
        AtomicInteger heartbeats = new AtomicInteger();
        try (ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.REGISTER_NODE) {
                Messages.RegisterNode.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.RegisterResp(7, 11, 1, 1_000).encode(), null);
            }
            if (op == Opcode.NODE_HEARTBEAT) {
                Messages.NodeHeartbeat.decode(req.headerSlice());
                if (heartbeats.incrementAndGet() == 1) {
                    return ScpServer.ok(req,
                            new Messages.HeartbeatResp(System.currentTimeMillis() + 1_000,
                                    List.of(new Messages.DrainCmd(44))).encode(), null);
                }
                throw new ScpException(ErrorCode.NOT_LEADER, "moved");
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, config(metaServer, 60_000), node.store());

            invoke(loop, "ensureRegistered");
            assertEquals(7, node.nodeId());
            assertEquals(11L, getLong(loop, "sessionEpoch"));

            invoke(loop, "heartbeatOnce");
            LinkedBlockingQueue<Messages.Command> commands = get(loop, "commandQueue");
            assertEquals(1, commands.size());
            assertTrue(commands.peek() instanceof Messages.DrainCmd);

            ConcurrentLinkedQueue<Messages.CompletedCommand> completed = get(loop, "completed");
            completed.add(new Messages.CompletedCommand(99, ErrorCode.OK.code));
            ScpException e = assertThrows(ScpException.class, () -> invoke(loop, "heartbeatOnce"));
            assertEquals(ErrorCode.NOT_LEADER, e.code());
            assertEquals(1, completed.size(), "failed heartbeat must preserve completions for retry");

            loop.close();
        }
    }

    @Test
    void malformedHeartbeatResponseRequeuesCompletions() throws Exception {
        try (ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.REGISTER_NODE) {
                Messages.RegisterNode.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.RegisterResp(7, 11, 1, 1_000).encode(), null);
            }
            if (op == Opcode.NODE_HEARTBEAT) {
                Messages.NodeHeartbeat.decode(req.headerSlice());
                return ScpServer.ok(req, new byte[] {0, 0, (byte) 0x80}, null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, config(metaServer, 60_000), node.store());
            invoke(loop, "ensureRegistered");

            ConcurrentLinkedQueue<Messages.CompletedCommand> completed = get(loop, "completed");
            completed.add(new Messages.CompletedCommand(99, ErrorCode.OK.code));

            assertThrows(RuntimeException.class, () -> invoke(loop, "heartbeatOnce"));
            assertEquals(1, completed.size(), "malformed heartbeat response must preserve completions for retry");

            loop.close();
        }
    }

    @Test
    void failedHeartbeatCompletionIsResentAfterEndpointRotationAndReregistration() throws Exception {
        AtomicInteger firstHeartbeats = new AtomicInteger();
        AtomicInteger secondHeartbeats = new AtomicInteger();
        AtomicReference<List<Messages.CompletedCommand>> firstReported = new AtomicReference<>(List.of());
        AtomicReference<List<Messages.CompletedCommand>> secondReported = new AtomicReference<>(List.of());
        AtomicReference<AtomicBoolean> closedRef = new AtomicReference<>();

        try (ScpServer standby = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.REGISTER_NODE) {
                Messages.RegisterNode.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.RegisterResp(7, 11, 1, 1_000).encode(), null);
            }
            if (op == Opcode.NODE_HEARTBEAT) {
                Messages.NodeHeartbeat heartbeat = Messages.NodeHeartbeat.decode(req.headerSlice());
                firstReported.set(heartbeat.completedCommands());
                firstHeartbeats.incrementAndGet();
                throw new ScpException(ErrorCode.NOT_LEADER, "standby");
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             ScpServer leader = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.REGISTER_NODE) {
                     Messages.RegisterNode.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.RegisterResp(7, 22, 1, 1_000).encode(), null);
                 }
                 if (op == Opcode.NODE_HEARTBEAT) {
                     Messages.NodeHeartbeat heartbeat = Messages.NodeHeartbeat.decode(req.headerSlice());
                     secondReported.set(heartbeat.completedCommands());
                     secondHeartbeats.incrementAndGet();
                     closedRef.get().set(true);
                     return ScpServer.ok(req,
                             new Messages.HeartbeatResp(System.currentTimeMillis() + 1_000, List.of()).encode(),
                             null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node,
                    config(List.of(endpoint(standby), endpoint(leader)), 60_000), node.store());
            closedRef.set(getClosed(loop));
            ConcurrentLinkedQueue<Messages.CompletedCommand> completed = get(loop, "completed");
            completed.add(new Messages.CompletedCommand(99, ErrorCode.OK.code));

            Thread worker = Thread.ofVirtual().name("control-loop-test-completion-rotation").start(() -> {
                try {
                    invoke(loop, "run");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            waitFor(() -> secondHeartbeats.get() == 1);
            worker.join(2_000);

            assertEquals(1, firstHeartbeats.get());
            assertEquals(List.of(new Messages.CompletedCommand(99, ErrorCode.OK.code)), firstReported.get());
            assertEquals(List.of(new Messages.CompletedCommand(99, ErrorCode.OK.code)), secondReported.get(),
                    "completion drained into a failed heartbeat must be resent after re-registration");
            assertTrue(completed.isEmpty(), "successful resend should clear the completion queue");
            assertEquals(1, getInt(loop, "endpointIndex"));
            assertEquals(22, getLong(loop, "sessionEpoch"));
            loop.close();
        }
    }

    @Test
    void executeCommandsReportsDrainDeleteAndReplicateFailures() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, configWithoutMetadata(), node.store());
            LinkedBlockingQueue<Messages.Command> commands = get(loop, "commandQueue");
            ConcurrentLinkedQueue<Messages.CompletedCommand> completed = get(loop, "completed");

            commands.add(new Messages.DrainCmd(1));
            commands.add(new Messages.DeleteCmd(2, List.of(new ChunkId(FileId.random(), 0))));
            commands.add(new Messages.ReplicateCmd(3, new ChunkId(FileId.random(), 1),
                    List.of(), (byte) 0, 0, 0));

            Thread worker = Thread.ofVirtual().name("control-loop-test-exec").start(() -> {
                try {
                    invoke(loop, "executeCommands");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            waitFor(() -> completed.size() == 3);
            getClosed(loop).set(true);
            worker.interrupt();
            worker.join(2_000);

            assertTrue(node.isDraining());
            Map<Long, Short> statuses = completed.stream().collect(Collectors.toMap(
                    Messages.CompletedCommand::commandId, Messages.CompletedCommand::status));
            assertEquals(ErrorCode.OK.code, statuses.get(1L));
            assertEquals(ErrorCode.OK.code, statuses.get(2L));
            assertEquals(ErrorCode.INTERNAL.code, statuses.get(3L));
        }
    }

    @Test
    void executeCommandsReportsDeleteFailureStatus() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, configWithoutMetadata(), node.store());
            LinkedBlockingQueue<Messages.Command> commands = get(loop, "commandQueue");
            ConcurrentLinkedQueue<Messages.CompletedCommand> completed = get(loop, "completed");
            ChunkId id = new ChunkId(FileId.random(), 0);
            Set<ChunkId> creating = getField(node.store(), "creating");
            creating.add(id);

            commands.add(new Messages.DeleteCmd(55, List.of(id)));

            Thread worker = Thread.ofVirtual().name("control-loop-test-delete-failure").start(() -> {
                try {
                    invoke(loop, "executeCommands");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            waitFor(() -> completed.size() == 1);
            getClosed(loop).set(true);
            worker.interrupt();
            worker.join(2_000);

            assertEquals(ErrorCode.INTERNAL.code, completed.peek().status());
        }
    }

    @Test
    void executeCommandsReportsUnexpectedRuntimeFailureStatus() throws Exception {
        ControlLoop loop = new ControlLoop(null, configWithoutMetadata(), null);
        LinkedBlockingQueue<Messages.Command> commands = get(loop, "commandQueue");
        ConcurrentLinkedQueue<Messages.CompletedCommand> completed = get(loop, "completed");
        commands.add(new Messages.DrainCmd(77));

        Thread worker = Thread.ofVirtual().name("control-loop-test-runtime-failure").start(() -> {
            try {
                invoke(loop, "executeCommands");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        waitFor(() -> completed.size() == 1);
        getClosed(loop).set(true);
        worker.interrupt();
        worker.join(2_000);

        assertEquals(ErrorCode.INTERNAL.code, completed.peek().status());
    }

    @Test
    void replicateReturnsForValidLocalCopyAndDeletesMismatchedReplay() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, configWithoutMetadata(), node.store());
            ChunkStore store = node.store();

            ChunkId valid = new ChunkId(FileId.random(), 0);
            byte[] data = "valid".getBytes();
            store.open(valid, false, 1, 1L);
            store.append(valid, 1, 0, 0, ByteBuffer.wrap(data));
            var sealed = store.seal(valid, 1, data.length, null);

            invokeReplicate(loop, new Messages.ReplicateCmd(10, valid, List.of(), (byte) 0,
                    sealed.dataCrc(), data.length));
            assertTrue(store.contains(valid));

            ChunkId stale = new ChunkId(FileId.random(), 1);
            store.open(stale, false, 1, 1L);
            store.append(stale, 1, 0, 0, ByteBuffer.wrap(data));
            store.seal(stale, 1, data.length, null);

            ScpException e = assertThrows(ScpException.class,
                    () -> invokeReplicate(loop, new Messages.ReplicateCmd(11, stale, List.of(), (byte) 0,
                            sealed.dataCrc(), data.length + 1)));
            assertEquals(ErrorCode.INTERNAL, e.code());
            assertFalse(store.contains(stale), "mismatched local replay copy must be deleted before retrying sources");

            ChunkId crcZeroDescriptor = new ChunkId(FileId.random(), 2);
            byte[] crcData = "crc-must-not-be-wildcard".getBytes();
            store.open(crcZeroDescriptor, false, 1, 1L);
            store.append(crcZeroDescriptor, 1, 0, 0, ByteBuffer.wrap(crcData));
            int localCrc = store.seal(crcZeroDescriptor, 1, crcData.length, null).dataCrc();
            assertTrue(localCrc != 0, "test payload must exercise the nonzero-local/zero-expected path");

            ScpException crcMismatch = assertThrows(ScpException.class,
                    () -> invokeReplicate(loop, new Messages.ReplicateCmd(12, crcZeroDescriptor, List.of(),
                            (byte) 0, 0, crcData.length)));
            assertEquals(ErrorCode.INTERNAL, crcMismatch.code());
            assertFalse(store.contains(crcZeroDescriptor),
                    "expected CRC zero must not be treated as a wildcard for local replay");
        }
    }

    @Test
    void replicateDeletesOpenLocalReplayBeforeTryingSources() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, configWithoutMetadata(), node.store());
            ChunkId open = new ChunkId(FileId.random(), 0);
            node.store().open(open, false, 1, 1L);

            ScpException e = assertThrows(ScpException.class,
                    () -> invokeReplicate(loop, new Messages.ReplicateCmd(15, open, List.of(),
                            (byte) 0, 0, 0)));

            assertEquals(ErrorCode.INTERNAL, e.code());
            assertFalse(node.store().contains(open));
        }
    }

    @Test
    void replicateThrowsLastSourceFailureWhenAllSourcesFail() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, configWithoutMetadata(), node.store());
            ChunkId chunkId = new ChunkId(FileId.random(), 0);

            ScpException e = assertThrows(ScpException.class,
                    () -> invokeReplicate(loop, new Messages.ReplicateCmd(16, chunkId,
                            List.of(new Messages.Replica(77, "malformed-source")),
                            (byte) 0, 0, 1)));

            assertEquals(ErrorCode.INTERNAL, e.code());
            assertTrue(e.getMessage().contains("invalid endpoint"));
        }
    }

    @Test
    void replicateSkipsConnectionFailureAndUsesNextSource() throws Exception {
        ChunkId chunkId = new ChunkId(FileId.random(), 0);
        byte[] data = "repair-source".getBytes();
        try (ChunkStore sourceStore = new ChunkStore(dir.resolve("source-io"));
             StorageNode node = new StorageNode(NodeConfig.standalone(dir.resolve("target-io")))) {
            sourceStore.open(chunkId, false, 1, 1L);
            sourceStore.append(chunkId, 1, 0, 0, ByteBuffer.wrap(data));
            int crc = sourceStore.seal(chunkId, 1, data.length, null).dataCrc();

            try (ScpServer source = new ScpServer(0, 77, 0, 0, req -> {
                Opcode op = Opcode.fromCode(req.opcode());
                if (op == Opcode.FETCH_CHUNK) {
                    Messages.FetchChunk fetch = Messages.FetchChunk.decode(req.headerSlice());
                    var result = sourceStore.fetch(fetch.chunkId(), fetch.offset(), fetch.maxBytes());
                    return ScpServer.ok(req, new Messages.FetchResp(result.fileLength(), result.state()).encode(),
                            ByteBuffer.wrap(result.bytes()));
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
            })) {
                ControlLoop loop = new ControlLoop(node, configWithoutMetadata(), node.store());
                invokeReplicate(loop, new Messages.ReplicateCmd(13, chunkId, List.of(
                        new Messages.Replica(11, "127.0.0.1:1"),
                        new Messages.Replica(77, endpoint(source))), (byte) 0, crc, data.length));

                assertTrue(node.store().contains(chunkId), "repair should continue after a source connection failure");
            }
        }
    }

    @Test
    void replicateSkipsMalformedSourceEndpointAndUsesNextSource() throws Exception {
        ChunkId chunkId = new ChunkId(FileId.random(), 0);
        byte[] data = "repair-source".getBytes();
        try (ChunkStore sourceStore = new ChunkStore(dir.resolve("source"));
             StorageNode node = new StorageNode(NodeConfig.standalone(dir.resolve("target")))) {
            sourceStore.open(chunkId, false, 1, 1L);
            sourceStore.append(chunkId, 1, 0, 0, ByteBuffer.wrap(data));
            int crc = sourceStore.seal(chunkId, 1, data.length, null).dataCrc();

            try (ScpServer source = new ScpServer(0, 77, 0, 0, req -> {
                Opcode op = Opcode.fromCode(req.opcode());
                if (op == Opcode.FETCH_CHUNK) {
                    Messages.FetchChunk fetch = Messages.FetchChunk.decode(req.headerSlice());
                    var result = sourceStore.fetch(fetch.chunkId(), fetch.offset(), fetch.maxBytes());
                    return ScpServer.ok(req, new Messages.FetchResp(result.fileLength(), result.state()).encode(),
                            ByteBuffer.wrap(result.bytes()));
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
            })) {
                ControlLoop loop = new ControlLoop(node, configWithoutMetadata(), node.store());
                invokeReplicate(loop, new Messages.ReplicateCmd(12, chunkId, List.of(
                        new Messages.Replica(11, "malformed-source"),
                        new Messages.Replica(77, endpoint(source))), (byte) 0, crc, data.length));

                assertTrue(node.store().contains(chunkId), "repair should continue to the valid source");
            }
        }
    }

    @Test
    void replicateIgnoresSelfSourcesBeforeReportingNoUsableSource() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, configWithoutMetadata(), node.store());
            ChunkId chunkId = new ChunkId(FileId.random(), 0);

            ScpException e = assertThrows(ScpException.class,
                    () -> invokeReplicate(loop, new Messages.ReplicateCmd(14, chunkId,
                            List.of(new Messages.Replica(node.nodeId(), "127.0.0.1:1")),
                            (byte) 0, 0, 1)));
            assertEquals(ErrorCode.INTERNAL, e.code());
        }
    }

    @Test
    void endpointParserRejectsUnbalancedBrackets() throws Exception {
        ScpException e = assertThrows(ScpException.class, () -> parseEndpoint("[::1:9000"));
        assertEquals(ErrorCode.INTERNAL, e.code());

        assertThrows(ScpException.class, () -> parseEndpoint(null));
        assertThrows(ScpException.class, () -> parseEndpoint(""));
        assertThrows(ScpException.class, () -> parseEndpoint(":9000"));
        assertThrows(ScpException.class, () -> parseEndpoint("host:"));
        assertThrows(ScpException.class, () -> parseEndpoint("[]:9000"));
        assertThrows(ScpException.class, () -> parseEndpoint("host:not-a-port"));
        assertThrows(ScpException.class, () -> parseEndpoint(" host:9000"));
        assertThrows(ScpException.class, () -> parseEndpoint("host:0"));
        assertThrows(ScpException.class, () -> parseEndpoint("host:65536"));
        assertTrue(parseEndpoint("[::1]:9000") != null);
    }

    @Test
    void fetchWholeFileRejectsInvalidRepairLengthBeforeUsingSource() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, configWithoutMetadata(), node.store());
            ChunkId chunkId = new ChunkId(FileId.random(), 0);

            ScpException negative = assertThrows(ScpException.class,
                    () -> loop.fetchWholeFile(null, new Messages.ReplicateCmd(20, chunkId, List.of(),
                            (byte) 0, 0, -1)));
            assertEquals(ErrorCode.CORRUPT_CHUNK, negative.code());

            ScpException tooLarge = assertThrows(ScpException.class,
                    () -> loop.fetchWholeFile(null, new Messages.ReplicateCmd(21, chunkId, List.of(),
                            (byte) 0, 0, Integer.MAX_VALUE)));
            assertEquals(ErrorCode.INTERNAL, tooLarge.code());

            ScpException overflow = assertThrows(ScpException.class,
                    () -> loop.fetchWholeFile(null, new Messages.ReplicateCmd(22, chunkId, List.of(),
                            (byte) 0, 0, Long.MAX_VALUE)));
            assertEquals(ErrorCode.CORRUPT_CHUNK, overflow.code());
        }
    }

    @Test
    void fetchWholeFileRejectsMalformedOpenShortAndChangingSources() throws Exception {
        assertFetchFailure(ErrorCode.CORRUPT_CHUNK, req -> ScpServer.ok(req, new byte[] {0, 0, 0}, null));

        long minLength = ChunkFormats.HEADER_SIZE + ChunkFormats.TRAILER_SIZE;
        assertFetchFailure(ErrorCode.CORRUPT_CHUNK, req -> ScpServer.ok(req,
                new Messages.FetchResp(minLength - 1, ChunkState.SEALED).encode(), null));
        assertFetchFailure(ErrorCode.INTERNAL, req -> ScpServer.ok(req,
                new Messages.FetchResp(minLength, ChunkState.OPEN).encode(), null));
        assertFetchFailure(ErrorCode.INTERNAL, req -> ScpServer.ok(req,
                new Messages.FetchResp(minLength + 1, ChunkState.SEALED).encode(), null));
        assertFetchFailure(ErrorCode.CRC_MISMATCH, req -> {
            throw new ScpException(ErrorCode.CRC_MISMATCH, "source read failed");
        });
        assertFetchFailure(ErrorCode.CORRUPT_CHUNK, req -> ScpServer.ok(req,
                new Messages.FetchResp(minLength, ChunkState.SEALED).encode(),
                ByteBuffer.wrap(new byte[(int) minLength + 1])));

        int overFetchLimit = 4 * 1024 * 1024 + 1;
        assertFetchFailure(ErrorCode.CORRUPT_CHUNK, req -> ScpServer.ok(req,
                new Messages.FetchResp(overFetchLimit, ChunkState.SEALED).encode(),
                ByteBuffer.wrap(new byte[overFetchLimit])));

        AtomicInteger calls = new AtomicInteger();
        assertFetchFailure(ErrorCode.CORRUPT_CHUNK, req -> {
            long length = calls.incrementAndGet() == 1 ? minLength + 2 : minLength + 3;
            return ScpServer.ok(req, new Messages.FetchResp(length, ChunkState.SEALED).encode(),
                    ByteBuffer.wrap(new byte[] {1}));
        });
    }

    @Test
    void fetchWholeFileAcceptsConsistentMultiPartSource() throws Exception {
        ChunkId chunkId = new ChunkId(FileId.random(), 0);
        byte[] file = new byte[(4 * 1024 * 1024) + 1];
        for (int i = 0; i < file.length; i++) {
            file[i] = (byte) i;
        }
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir));
             ScpServer source = new ScpServer(0, 77, 0, 0, req -> {
                 Messages.FetchChunk fetch = Messages.FetchChunk.decode(req.headerSlice());
                 int offset = Math.toIntExact(fetch.offset());
                 int len = Math.min(fetch.maxBytes(), file.length - offset);
                 return ScpServer.ok(req, new Messages.FetchResp(file.length, ChunkState.SEALED).encode(),
                         ByteBuffer.wrap(file, offset, len));
             });
             ScpClient client = new ScpClient("127.0.0.1", source.port(),
                     ScpClient.KIND_STORAGE_NODE, "fetch-multipart-test")) {
            ControlLoop loop = new ControlLoop(node, configWithoutMetadata(), node.store());

            byte[] fetched = loop.fetchWholeFile(client, new Messages.ReplicateCmd(23, chunkId,
                    List.of(), (byte) 0, 0, 1));

            assertEquals(file.length, fetched.length);
            assertEquals(file[0], fetched[0]);
            assertEquals(file[file.length - 1], fetched[fetched.length - 1]);
        }
    }

    @Test
    void closeRestoresInterruptWhenInterruptedDuringJoin() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, configWithoutMetadata(), node.store());
            List<Thread> threads = get(loop, "threads");
            Thread sleeper = Thread.ofVirtual().name("control-loop-test-sleeper").start(() -> {
                try {
                    Thread.sleep(60_000);
                } catch (InterruptedException ignored) {
                }
            });
            threads.add(sleeper);

            Thread.currentThread().interrupt();
            try {
                loop.close();
                assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
                sleeper.interrupt();
                sleeper.join(2_000);
            }
        }
    }

    @Test
    void inventoryLoopReportsInventoryAndContinuesAfterReportFailure() throws Exception {
        AtomicInteger inventoryReports = new AtomicInteger();
        try (ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.REGISTER_NODE) {
                Messages.RegisterNode.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.RegisterResp(9, 12, 1, 1_000).encode(), null);
            }
            if (op == Opcode.INVENTORY_REPORT) {
                Messages.InventoryReport.decode(req.headerSlice());
                if (inventoryReports.incrementAndGet() == 1) {
                    throw new ScpException(ErrorCode.INTERNAL, "temporary inventory failure");
                }
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, config(metaServer, 1), node.store());
            invoke(loop, "ensureRegistered");

            Thread worker = Thread.ofVirtual().name("control-loop-test-inventory").start(() -> {
                try {
                    invoke(loop, "inventoryLoop");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            waitFor(() -> inventoryReports.get() >= 10);
            getClosed(loop).set(true);
            worker.interrupt();
            worker.join(2_000);

            assertTrue(inventoryReports.get() >= 10);
            loop.close();
        }
    }

    @Test
    void inventoryLoopSkipsWhenNotRegistered() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            ControlLoop loop = new ControlLoop(node, configWithoutMetadata(), node.store());

            Thread worker = Thread.ofVirtual().name("control-loop-test-inventory-unregistered").start(() -> {
                try {
                    invoke(loop, "inventoryLoop");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Thread.sleep(20);
            getClosed(loop).set(true);
            worker.interrupt();
            worker.join(2_000);

            assertFalse(worker.isAlive());
            loop.close();
        }
    }

    private static NodeConfig config(ScpServer metaServer, int inventoryIntervalMs) {
        return config(List.of(endpoint(metaServer)), inventoryIntervalMs);
    }

    private static NodeConfig config(List<String> metadataEndpoints, int inventoryIntervalMs) {
        ConnectionPolicy testPolicy = new ConnectionPolicy(1_000, 1_000, 100, 1_000, 1, 1);
        return new NodeConfig(Path.of("."), 0, "127.0.0.1", null, metadataEndpoints,
                "z", "r", "h", 1L << 20, inventoryIntervalMs, testPolicy);
    }

    private static NodeConfig configWithoutMetadata() {
        return new NodeConfig(Path.of("."), 0, "127.0.0.1", null, List.of(),
                "z", "r", "h", 1L << 20, 60_000);
    }

    private static void invokeReplicate(ControlLoop loop, Messages.ReplicateCmd cmd) throws Exception {
        Method method = ControlLoop.class.getDeclaredMethod("replicate", Messages.ReplicateCmd.class);
        method.setAccessible(true);
        try {
            method.invoke(loop, cmd);
        } catch (InvocationTargetException e) {
            throwCause(e);
        }
    }

    private static void invoke(ControlLoop loop, String methodName) throws Exception {
        Method method = ControlLoop.class.getDeclaredMethod(methodName);
        method.setAccessible(true);
        try {
            method.invoke(loop);
        } catch (InvocationTargetException e) {
            throwCause(e);
        }
    }

    private static Endpoint parseEndpoint(String endpoint) {
        return Endpoint.parse(endpoint, "endpoint", ErrorCode.INTERNAL);
    }

    private static void throwCause(InvocationTargetException e) throws Exception {
        Throwable cause = e.getCause();
        if (cause instanceof Exception ex) {
            throw ex;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        throw new RuntimeException(cause);
    }

    private void assertFetchFailure(ErrorCode expectedCode, FetchResponder responder) throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir));
             ScpServer source = new ScpServer(0, 77, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.FETCH_CHUNK) {
                     Messages.FetchChunk.decode(req.headerSlice());
                     return responder.respond(req);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             });
             ScpClient client = new ScpClient("127.0.0.1", source.port(),
                     ScpClient.KIND_STORAGE_NODE, "fetch-test")) {
            ControlLoop loop = new ControlLoop(node, configWithoutMetadata(), node.store());
            ScpException e = assertThrows(ScpException.class,
                    () -> loop.fetchWholeFile(client, new Messages.ReplicateCmd(22,
                            new ChunkId(FileId.random(), 0), List.of(), (byte) 0, 0, 1)));
            assertEquals(expectedCode, e.code());
        }
    }

    @FunctionalInterface
    private interface FetchResponder {
        io.strata.proto.Frame respond(io.strata.proto.Frame req);
    }

    @SuppressWarnings("unchecked")
    private static <T> T get(ControlLoop loop, String fieldName) throws Exception {
        Field field = ControlLoop.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(loop);
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
    }

    private static long getLong(ControlLoop loop, String fieldName) throws Exception {
        Field field = ControlLoop.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getLong(loop);
    }

    private static int getInt(ControlLoop loop, String fieldName) throws Exception {
        Field field = ControlLoop.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getInt(loop);
    }

    private static AtomicBoolean getClosed(ControlLoop loop) throws Exception {
        return get(loop, "closed");
    }

    private static void waitFor(BooleanSupplier supplier) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000;
        while (!supplier.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(supplier.getAsBoolean(), "condition did not become true before deadline");
    }

    private static String endpoint(ScpServer server) {
        return "127.0.0.1:" + server.port();
    }

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean();
    }
}
