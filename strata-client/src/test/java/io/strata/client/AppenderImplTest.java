package io.strata.client;

import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.proto.Frame;
import io.strata.proto.ManagedScpConnection;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;
import io.strata.proto.ScpServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AppenderImplTest {

    @Test
    void staleReplicaSuccessIsIgnoredAfterRoll() throws Exception {
        AppenderImpl appender = appender();
        Object staleSession = chunkSession();
        Frame ok = Frame.response(Frame.request(Opcode.APPEND, new byte[0], null, 7),
                Messages.okHeader(), null);

        onReplicaResponse(appender, staleSession, ok, null);

        long ack = appender.append(ByteBuffer.allocate(0)).join();
        assertEquals(0, ack);
        assertEquals(0, appender.durableOffset());
    }

    @Test
    void staleReplicaFailureIsIgnoredAfterRoll() throws Exception {
        AppenderImpl appender = appender();
        Object staleSession = chunkSession();

        onReplicaResponse(appender, staleSession, null,
                new ScpException(ErrorCode.INTERNAL, "old chunk timed out"));

        long ack = appender.append(ByteBuffer.allocate(0)).join();
        assertEquals(0, ack);
        assertEquals(0, appender.durableOffset());
    }

    @Test
    void staleReplicaFencedEpochStillKillsAppender() throws Exception {
        AppenderImpl appender = appender();
        Object staleSession = chunkSession();
        Frame fenced = Frame.response(Frame.request(Opcode.APPEND, new byte[0], null, 7),
                Resp.error(ErrorCode.FENCED_EPOCH, "fenced", 12), null);

        onReplicaResponse(appender, staleSession, fenced, null);

        ScpException e = assertThrows(ScpException.class,
                () -> appender.append(ByteBuffer.allocate(0)));
        assertEquals(ErrorCode.FENCED_EPOCH, e.code());
        assertEquals(12, e.detail());
    }

    @Test
    void staleWrappedFencedFailureStillKillsAppender() throws Exception {
        AppenderImpl appender = appender();
        Object staleSession = chunkSession();

        onReplicaResponse(appender, staleSession, null,
                new CompletionException(
                        new ScpException(ErrorCode.FENCED_EPOCH, "stale fenced", 21)));

        ScpException e = assertThrows(ScpException.class,
                () -> appender.append(ByteBuffer.allocate(0)));
        assertEquals(ErrorCode.FENCED_EPOCH, e.code());
        assertEquals(21, e.detail());
    }

    @Test
    void staleNestedWrappedFencedFailureStillKillsAppender() throws Exception {
        AppenderImpl appender = appender();
        Object staleSession = chunkSession();

        onReplicaResponse(appender, staleSession, null,
                new CompletionException(
                        new CompletionException(
                                new ScpException(ErrorCode.FENCED_EPOCH, "nested fenced", 22))));

        ScpException e = assertThrows(ScpException.class,
                () -> appender.append(ByteBuffer.allocate(0)));
        assertEquals(ErrorCode.FENCED_EPOCH, e.code());
        assertEquals(22, e.detail());
    }

    @Test
    void staleResponseAfterAppenderDeathIsIgnored() throws Exception {
        AppenderImpl appender = appender();
        Object staleSession = chunkSession();
        appender.close();

        onReplicaResponse(appender, staleSession,
                errorResp(ErrorCode.FENCED_EPOCH, "late fence", 88), null);

        ScpException e = assertThrows(ScpException.class,
                () -> appender.append(ByteBuffer.allocate(0)));
        assertEquals(ErrorCode.INTERNAL, e.code());
        assertTrue(e.getMessage().contains("appender closed"));
    }

    @Test
    void staleNonFencedErrorResponseIsIgnoredAfterRoll() throws Exception {
        AppenderImpl appender = appender();
        Object staleSession = chunkSession();

        onReplicaResponse(appender, staleSession,
                errorResp(ErrorCode.INTERNAL, "old error", 77), null);

        long ack = appender.append(ByteBuffer.allocate(0)).join();
        assertEquals(0, ack);
        assertEquals(0, appender.durableOffset());
    }

    @Test
    void staleMalformedReplicaResponseIsIgnoredAfterRoll() throws Exception {
        AppenderImpl appender = appender();
        Object staleSession = chunkSession();
        Frame malformed = Frame.response(Frame.request(Opcode.APPEND, new byte[0], null, 7),
                new byte[] {0}, null);

        onReplicaResponse(appender, staleSession, malformed, null);

        long ack = appender.append(ByteBuffer.allocate(0)).join();
        assertEquals(0, ack);
        assertEquals(0, appender.durableOffset());
    }

    @Test
    void zeroLengthAppendWaitsForOutstandingDurability() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 7);
        setLong(session, "durable", 3);

        CompletableFuture<Long> ack = appender.append(ByteBuffer.allocate(0));
        assertFalse(ack.isDone());

        onReplicaResponse(appender, session, 0, appendResp(7), null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 1, appendResp(7), null);

        long result = ack.get(1, TimeUnit.SECONDS);
        assertEquals(7, result);
        assertEquals(7, appender.durableOffset());
    }

    @Test
    void zeroLengthAppendReturnsImmediatelyWhenSessionAlreadyDurable() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 7);
        setLong(session, "durable", 7);

        long ack = appender.append(ByteBuffer.allocate(0)).get(1, TimeUnit.SECONDS);

        assertEquals(7, ack);
        assertEquals(7, appender.durableOffset());
    }

    @Test
    void staleNullReplicaResponseIsIgnoredAfterRoll() throws Exception {
        AppenderImpl appender = appender();
        Object staleSession = chunkSession();

        onReplicaResponse(appender, staleSession, null, null);

        long ack = appender.append(ByteBuffer.allocate(0)).join();
        assertEquals(0, ack);
        assertEquals(0, appender.durableOffset());
    }

    @Test
    void twoReplicaFailuresLoseQuorumAndFailPendingAppends() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 11);

        CompletableFuture<Long> ack = appender.append(ByteBuffer.allocate(0));

        onReplicaResponse(appender, session, 0, null,
                new ScpException(ErrorCode.INTERNAL, "first replica failed"));
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 1, null,
                new ScpException(ErrorCode.INTERNAL, "second replica failed"));

        assertTrue(ack.isCompletedExceptionally());
        ScpException e = assertThrows(ScpException.class,
                () -> appender.append(ByteBuffer.allocate(0)));
        assertEquals(ErrorCode.INTERNAL, e.code());
    }

    @Test
    void appendRejectsReplicaEndThatDoesNotMatchRequest() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 5);

        CompletableFuture<Long> ack = appender.append(ByteBuffer.allocate(0));

        onReplicaResponse(appender, session, 0, 5, appendResp(6), null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 1, appendResp(5), null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 2, appendResp(5), null);

        long result = ack.get(1, TimeUnit.SECONDS);
        assertEquals(5, result);
        assertEquals(5, appender.durableOffset());
    }

    @Test
    void malformedAppendResponseCountsAsReplicaFailure() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 5);

        CompletableFuture<Long> ack = appender.append(ByteBuffer.allocate(0));
        Frame malformed = Frame.response(Frame.request(Opcode.APPEND, new byte[0], null, 7),
                new byte[] {0, 0}, null);

        onReplicaResponse(appender, session, 0, 5, malformed, null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 1, null,
                new ScpException(ErrorCode.INTERNAL, "second replica failed"));

        assertTrue(ack.isCompletedExceptionally());
        ScpException later = assertThrows(ScpException.class,
                () -> appender.append(ByteBuffer.allocate(0)));
        assertEquals(ErrorCode.INTERNAL, later.code());
    }

    @Test
    void activeNullReplicaResponseCountsAsReplicaFailure() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 5);

        CompletableFuture<Long> ack = appender.append(ByteBuffer.allocate(0));

        onReplicaResponse(appender, session, 0, 5, null, null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 1, appendResp(5), null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 2, appendResp(5), null);

        long result = ack.get(1, TimeUnit.SECONDS);
        assertEquals(5, result);
        assertEquals(5, appender.durableOffset());
    }

    @Test
    void activeAsyncFencedFailureKillsAppender() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 5);

        CompletableFuture<Long> ack = appender.append(ByteBuffer.allocate(0));

        onReplicaResponse(appender, session, 0, null,
                new ScpException(ErrorCode.FENCED_EPOCH, "fenced", 9));

        assertTrue(ack.isCompletedExceptionally());
        ScpException later = assertThrows(ScpException.class,
                () -> appender.append(ByteBuffer.allocate(0)));
        assertEquals(ErrorCode.FENCED_EPOCH, later.code());
        assertEquals(9, later.detail());
    }

    @Test
    void wrappedAsyncFencedFailureKillsAppender() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 5);

        CompletableFuture<Long> ack = appender.append(ByteBuffer.allocate(0));

        onReplicaResponse(appender, session, 0, null,
                new CompletionException(
                        new ScpException(ErrorCode.FENCED_EPOCH, "wrapped fenced", 13)));

        assertTrue(ack.isCompletedExceptionally());
        ScpException later = assertThrows(ScpException.class,
                () -> appender.append(ByteBuffer.allocate(0)));
        assertEquals(ErrorCode.FENCED_EPOCH, later.code());
        assertEquals(13, later.detail());
    }

    @Test
    void nonScpAsyncFailureCountsAsReplicaFailure() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 5);

        CompletableFuture<Long> ack = appender.append(ByteBuffer.allocate(0));

        onReplicaResponse(appender, session, 0, null, new RuntimeException("transport closed"));
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 1, appendResp(5), null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 2, appendResp(5), null);

        long result = ack.get(1, TimeUnit.SECONDS);
        assertEquals(5, result);
        assertEquals(5, appender.durableOffset());
    }

    @Test
    void completionExceptionWithoutCauseCountsAsReplicaFailure() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 5);

        CompletableFuture<Long> ack = appender.append(ByteBuffer.allocate(0));

        onReplicaResponse(appender, session, 0, null,
                new CompletionException("closed", null));
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 1, appendResp(5), null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 2, appendResp(5), null);

        long result = ack.get(1, TimeUnit.SECONDS);
        assertEquals(5, result);
        assertEquals(5, appender.durableOffset());
    }

    @Test
    void synchronousReplicaFailuresStopFanoutAfterQuorumLost() throws Exception {
        CountDownLatch unexpectedAppend = new CountDownLatch(1);
        FileId fileId = FileId.of(1);
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer shouldNotReceive = new ScpServer(0, 3, 0, 0, req -> {
            if (Opcode.fromCode(req.opcode()) == Opcode.APPEND) {
                unexpectedAppend.countDown();
                Messages.Append append = Messages.Append.decode(req.headerSlice());
                return ScpServer.ok(req,
                        new Messages.AppendResp(append.baseOffset() + req.payloadLength()).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected opcode");
        });
             NodePool pool = new NodePool()) {
            AppenderImpl appender = new AppenderImpl(null, pool,
                    new ClientConfig(List.of("127.0.0.1:1"), 1024, 500),
                    fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
            Object session = chunkSession(chunkId,
                    new Messages.Replica(1, "invalid-replica-a"),
                    new Messages.Replica(2, "invalid-replica-b"),
                    new Messages.Replica(3, endpoint(shouldNotReceive)));
            setSession(appender, session);

            CompletableFuture<Long> ack = appender.append(ByteBuffer.wrap(new byte[] {1}));

            assertTrue(ack.isCompletedExceptionally());
            assertFalse(unexpectedAppend.await(100, TimeUnit.MILLISECONDS),
                    "append fan-out continued after quorum was already lost");
        }
    }

    @Test
    void duplicateReplicaFailureReportIsIgnored() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 5);

        CompletableFuture<Long> ack = appender.append(ByteBuffer.allocate(0));

        onReplicaResponse(appender, session, 0, null,
                new ScpException(ErrorCode.INTERNAL, "first failure"));
        onReplicaResponse(appender, session, 0, null,
                new ScpException(ErrorCode.INTERNAL, "duplicate failure"));
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 1, appendResp(5), null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 2, appendResp(5), null);

        long result = ack.get(1, TimeUnit.SECONDS);
        assertEquals(5, result);
        assertEquals(5, appender.durableOffset());
    }

    @Test
    void appendWhileRollingRestoresInterruptAndFails() throws Exception {
        AppenderImpl appender = appender();
        setBoolean(appender, "rolling", true);

        Thread.currentThread().interrupt();
        try {
            ScpException e = assertThrows(ScpException.class,
                    () -> appender.append(ByteBuffer.allocate(0)));
            assertEquals(ErrorCode.INTERNAL, e.code());
            assertTrue(e.getMessage().contains("interrupted"));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void appendWhileRollingFailsWhenAppenderIsClosed() throws Exception {
        AppenderImpl appender = appender();
        setBoolean(appender, "rolling", true);

        CompletableFuture<ScpException> failure = CompletableFuture.supplyAsync(() -> {
            try {
                appender.append(ByteBuffer.allocate(0));
                return null;
            } catch (ScpException e) {
                return e;
            }
        });

        Thread.sleep(50);
        appender.close();

        ScpException e = failure.get(1, TimeUnit.SECONDS);
        assertEquals(ErrorCode.INTERNAL, e.code());
        assertTrue(e.getMessage().contains("appender closed"));
    }

    @Test
    void deadAppenderWithoutCauseReportsClosed() throws Exception {
        AppenderImpl appender = appender();
        setBoolean(appender, "dead", true);

        ScpException e = assertThrows(ScpException.class,
                () -> appender.append(ByteBuffer.allocate(0)));
        assertEquals(ErrorCode.INTERNAL, e.code());
        assertTrue(e.getMessage().contains("appender closed"));
    }

    @Test
    void appendRejectsChunkOffsetOverflowBeforeSending() throws Exception {
        AppenderImpl appender = new AppenderImpl(null, null,
                new ClientConfig(List.of("127.0.0.1:1"), Long.MAX_VALUE, 100),
                FileId.of(2), StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", Long.MAX_VALUE - 1);

        ScpException e = assertThrows(ScpException.class,
                () -> appender.append(ByteBuffer.wrap(new byte[] {1, 2})));
        assertEquals(ErrorCode.CORRUPT_CHUNK, e.code());
    }

    @Test
    void appendAckFileOffsetOverflowKillsAppender() throws Exception {
        AppenderImpl appender = new AppenderImpl(null, null,
                new ClientConfig(List.of("127.0.0.1:1"), 1024, 100),
                FileId.of(3), StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, Long.MAX_VALUE - 1);
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 2);

        CompletableFuture<Long> ack = appender.append(ByteBuffer.allocate(0));
        onReplicaResponse(appender, session, 0, appendResp(2), null);
        onReplicaResponse(appender, session, 1, appendResp(2), null);

        assertTrue(ack.isCompletedExceptionally());
        ScpException later = assertThrows(ScpException.class,
                () -> appender.append(ByteBuffer.allocate(0)));
        assertEquals(ErrorCode.CORRUPT_CHUNK, later.code());
    }

    @Test
    void rollTimesOutDrainingPendingAppendAndDies() throws Exception {
        AppenderImpl appender = new AppenderImpl(null, null,
                new ClientConfig(List.of("127.0.0.1:1"), 1024, 5),
                FileId.of(4), StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 1);
        setLong(session, "durable", 0);
        setBoolean(session, "needRoll", true);
        CompletableFuture<Long> pending = new CompletableFuture<>();
        pending(session).add(pending(1, pending));

        ScpException e = assertThrows(ScpException.class,
                () -> appender.append(ByteBuffer.wrap(new byte[] {1})));

        assertEquals(ErrorCode.INTERNAL, e.code());
        assertTrue(e.getMessage().contains("timed out draining"));
        assertTrue(pending.isCompletedExceptionally());
    }

    @Test
    void rollInterruptedWhileDrainingRestoresInterruptAndDies() throws Exception {
        AppenderImpl appender = new AppenderImpl(null, null,
                new ClientConfig(List.of("127.0.0.1:1"), 1024, 100),
                FileId.of(5), StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 1);
        setLong(session, "durable", 0);
        setBoolean(session, "needRoll", true);
        CompletableFuture<Long> pending = new CompletableFuture<>();
        pending(session).add(pending(1, pending));

        Thread.currentThread().interrupt();
        try {
            ScpException e = assertThrows(ScpException.class,
                    () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
            assertEquals(ErrorCode.INTERNAL, e.code());
            assertTrue(e.getMessage().contains("interrupted"));
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(pending.isCompletedExceptionally());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void rollStopsWhenSealChunkDies() throws Exception {
        FileId fileId = FileId.of(6);
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer s1 = dataNodeServer(1, 123, false);
             ScpServer s2 = dataNodeServer(2, 0, true);
             ScpServer s3 = dataNodeServer(3, 0, true);
             NodePool pool = new NodePool()) {
            ClientConfig config = new ClientConfig(List.of("127.0.0.1:1"), 1024, 500);
            AppenderImpl appender = new AppenderImpl(null, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
            Object session = chunkSession(chunkId,
                    new Messages.Replica(1, endpoint(s1)),
                    new Messages.Replica(2, endpoint(s2)),
                    new Messages.Replica(3, endpoint(s3)));
            setSession(appender, session);
            setLong(session, "end", 1);
            setLong(session, "durable", 1);
            setBoolean(session, "needRoll", true);

            ScpException e = assertThrows(ScpException.class,
                    () -> appender.append(ByteBuffer.wrap(new byte[] {1})));

            assertEquals(ErrorCode.INTERNAL, e.code());
            assertTrue(e.getMessage().contains("seal failed"));
        }
    }

    @Test
    void openQuorumFailureAbortsMetadataAndDeletesOpenedReplica() throws Exception {
        FileId fileId = FileId.of(7);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean deletedOpenedReplica = new AtomicBoolean();

        try (ScpServer opened = new ScpServer(0, 1, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.OPEN_CHUNK) {
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            if (op == Opcode.DELETE_CHUNKS) {
                Messages.DeleteChunks.decode(req.headerSlice());
                deletedOpenedReplica.set(true);
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             ScpServer failedA = failingOpenServer(2);
             ScpServer failedB = failingOpenServer(3);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                             new Messages.Replica(1, endpoint(opened)),
                             new Messages.Replica(2, endpoint(failedA)),
                             new Messages.Replica(3, endpoint(failedB)))).encode(), null);
                 }
                 if (op == Opcode.ABORT_CHUNK_META) {
                     Messages.AbortChunkMeta.decode(req.headerSlice());
                     aborted.set(true);
                     return ScpServer.ok(req, Messages.okHeader(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
                assertTrue(deletedOpenedReplica.get());
            }
        }
    }

    @Test
    void openQuorumFailureKeepsOriginalFailureWhenCleanupDeleteFails() throws Exception {
        FileId fileId = FileId.of(8);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean cleanupDeleteAttempted = new AtomicBoolean();

        try (ScpServer opened = new ScpServer(0, 1, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.OPEN_CHUNK) {
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            if (op == Opcode.DELETE_CHUNKS) {
                Messages.DeleteChunks.decode(req.headerSlice());
                cleanupDeleteAttempted.set(true);
                throw new ScpException(ErrorCode.INTERNAL, "delete failed");
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             ScpServer failedA = failingOpenServer(2);
             ScpServer failedB = failingOpenServer(3);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                             new Messages.Replica(1, endpoint(opened)),
                             new Messages.Replica(2, endpoint(failedA)),
                             new Messages.Replica(3, endpoint(failedB)))).encode(), null);
                 }
                 if (op == Opcode.ABORT_CHUNK_META) {
                     Messages.AbortChunkMeta.decode(req.headerSlice());
                     aborted.set(true);
                     return ScpServer.ok(req, Messages.okHeader(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(e.getMessage().contains("cannot open chunk on a quorum"));
                assertTrue(aborted.get());
                assertTrue(cleanupDeleteAttempted.get());
            }
        }
    }

    @Test
    void openRetryWithExclusionsKeepsOriginalQuorumFailureWhenPlacementRunsOut() throws Exception {
        FileId fileId = FileId.of(9);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicInteger createCalls = new AtomicInteger();
        AtomicReference<List<Integer>> excludedRetry = new AtomicReference<>();
        AtomicBoolean fallbackWithoutExclusions = new AtomicBoolean();

        try (ScpServer opened = new ScpServer(0, 1, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.OPEN_CHUNK) {
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            if (op == Opcode.DELETE_CHUNKS) {
                Messages.DeleteChunks.decode(req.headerSlice());
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             ScpServer failedA = failingOpenServer(2);
             ScpServer failedB = failingOpenServer(3);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk create = Messages.CreateChunk.decode(req.headerSlice());
                     int call = createCalls.incrementAndGet();
                     if (call == 1) {
                         assertTrue(create.excludedNodeIds().isEmpty());
                         return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                                 new Messages.Replica(1, endpoint(opened)),
                                 new Messages.Replica(2, endpoint(failedA)),
                                 new Messages.Replica(3, endpoint(failedB)))).encode(), null);
                     }
                     if (call == 2) {
                         excludedRetry.set(create.excludedNodeIds());
                         throw new ScpException(ErrorCode.NO_CAPACITY, "no spare nodes");
                     }
                     fallbackWithoutExclusions.set(create.excludedNodeIds().isEmpty());
                     throw new ScpException(ErrorCode.NO_CAPACITY, "still no quorum");
                 }
                 if (op == Opcode.ABORT_CHUNK_META) {
                     Messages.AbortChunkMeta.decode(req.headerSlice());
                     aborted.set(true);
                     return ScpServer.ok(req, Messages.okHeader(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId,
                        StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(e.getMessage().contains("cannot open chunk on a quorum"));
                assertTrue(aborted.get());
                assertEquals(Set.of(2, 3), Set.copyOf(excludedRetry.get()));
                assertTrue(fallbackWithoutExclusions.get());
            }
        }
    }

    @Test
    void createChunkWithWrongReplicaSetIsAbortedBeforeOpeningDataNode() throws Exception {
        FileId fileId = FileId.of(10);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean aborted = new AtomicBoolean();

        try (ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.CREATE_CHUNK) {
                Messages.CreateChunk.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                        new Messages.Replica(1, "127.0.0.1:1"),
                        new Messages.Replica(2, "127.0.0.1:2"),
                        new Messages.Replica(3, "127.0.0.1:3"),
                        new Messages.Replica(4, "127.0.0.1:4"))).encode(), null);
            }
            if (op == Opcode.ABORT_CHUNK_META) {
                Messages.AbortChunkMeta.decode(req.headerSlice());
                aborted.set(true);
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
            }
        }
    }

    @Test
    void createChunkWithWrongFileIdIsAbortedBeforeOpeningDataNode() throws Exception {
        FileId fileId = FileId.of(11);
        ChunkId wrongChunkId = new ChunkId(FileId.of(12), 0);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean openedDataNode = new AtomicBoolean();

        try (ScpServer dataNode = openTrackingDataNode(1, openedDataNode);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.CreateChunkResp(wrongChunkId, 1, List.of(
                             new Messages.Replica(1, endpoint(dataNode)),
                             new Messages.Replica(2, "127.0.0.1:2"),
                             new Messages.Replica(3, "127.0.0.1:3"))).encode(), null);
                 }
                 if (op == Opcode.ABORT_CHUNK_META) {
                     Messages.AbortChunkMeta.decode(req.headerSlice());
                     aborted.set(true);
                     return ScpServer.ok(req, Messages.okHeader(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
                assertFalse(openedDataNode.get());
            }
        }
    }

    @Test
    void createChunkWithDuplicateNodeIdIsAbortedBeforeOpeningDataNode() throws Exception {
        FileId fileId = FileId.of(13);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean openedDataNode = new AtomicBoolean();

        try (ScpServer dataNode = openTrackingDataNode(1, openedDataNode);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                             new Messages.Replica(1, endpoint(dataNode)),
                             new Messages.Replica(1, "127.0.0.1:2"),
                             new Messages.Replica(3, "127.0.0.1:3"))).encode(), null);
                 }
                 if (op == Opcode.ABORT_CHUNK_META) {
                     Messages.AbortChunkMeta.decode(req.headerSlice());
                     aborted.set(true);
                     return ScpServer.ok(req, Messages.okHeader(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
                assertFalse(openedDataNode.get());
            }
        }
    }

    @Test
    void createChunkWithBlankEndpointIsAbortedBeforeOpeningDataNode() throws Exception {
        FileId fileId = FileId.of(14);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean openedDataNode = new AtomicBoolean();

        try (ScpServer dataNode = openTrackingDataNode(1, openedDataNode);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                             new Messages.Replica(1, endpoint(dataNode)),
                             new Messages.Replica(2, " "),
                             new Messages.Replica(3, "127.0.0.1:3"))).encode(), null);
                 }
                 if (op == Opcode.ABORT_CHUNK_META) {
                     Messages.AbortChunkMeta.decode(req.headerSlice());
                     aborted.set(true);
                     return ScpServer.ok(req, Messages.okHeader(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
                assertFalse(openedDataNode.get());
            }
        }
    }

    @Test
    void replicaSetWithNullEndpointIsInvalid() throws Exception {
        assertFalse(validReplicaSet(List.of(
                new Messages.Replica(1, "127.0.0.1:1"),
                new Messages.Replica(2, null),
                new Messages.Replica(3, "127.0.0.1:3"))));
    }

    @Test
    void createChunkWithDuplicateEndpointIsAbortedBeforeOpeningDataNode() throws Exception {
        FileId fileId = FileId.of(15);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean aborted = new AtomicBoolean();

        try (ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.CREATE_CHUNK) {
                Messages.CreateChunk.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                        new Messages.Replica(1, "127.0.0.1:1"),
                        new Messages.Replica(2, "127.0.0.1:1"),
                        new Messages.Replica(3, "127.0.0.1:3"))).encode(), null);
            }
            if (op == Opcode.ABORT_CHUNK_META) {
                Messages.AbortChunkMeta.decode(req.headerSlice());
                aborted.set(true);
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
            }
        }
    }

    @Test
    void createChunkWithInvalidNodeIdIsAbortedBeforeOpeningDataNode() throws Exception {
        FileId fileId = FileId.of(16);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean openedDataNode = new AtomicBoolean();

        try (ScpServer dataNode = openTrackingDataNode(1, openedDataNode);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                             new Messages.Replica(0, endpoint(dataNode)),
                             new Messages.Replica(2, "127.0.0.1:2"),
                             new Messages.Replica(3, "127.0.0.1:3"))).encode(), null);
                 }
                 if (op == Opcode.ABORT_CHUNK_META) {
                     Messages.AbortChunkMeta.decode(req.headerSlice());
                     aborted.set(true);
                     return ScpServer.ok(req, Messages.okHeader(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
                assertFalse(openedDataNode.get());
            }
        }
    }

    @Test
    void createChunkWithMismatchedEpochIsAbortedBeforeOpeningDataNode() throws Exception {
        FileId fileId = FileId.of(17);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean openedDataNode = new AtomicBoolean();

        try (ScpServer dataNode = openTrackingDataNode(1, openedDataNode);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 2, List.of(
                             new Messages.Replica(1, endpoint(dataNode)),
                             new Messages.Replica(2, "127.0.0.1:2"),
                             new Messages.Replica(3, "127.0.0.1:3"))).encode(), null);
                 }
                 if (op == Opcode.ABORT_CHUNK_META) {
                     Messages.AbortChunkMeta.decode(req.headerSlice());
                     aborted.set(true);
                     return ScpServer.ok(req, Messages.okHeader(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
                assertFalse(openedDataNode.get());
            }
        }
    }

    @Test
    void createChunkFailureKillsAppenderWithoutOpeningDataNode() throws Exception {
        FileId fileId = FileId.of(18);
        AtomicBoolean openedDataNode = new AtomicBoolean();

        try (ScpServer dataNode = openTrackingDataNode(1, openedDataNode);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     throw new ScpException(ErrorCode.INTERNAL, "create failed");
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertFalse(openedDataNode.get());
            }
        }
    }

    @Test
    void abortMetadataFailureBecomesAppenderDeathCause() throws Exception {
        FileId fileId = FileId.of(19);
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.CREATE_CHUNK) {
                Messages.CreateChunk.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                        new Messages.Replica(1, "127.0.0.1:1"),
                        new Messages.Replica(2, "127.0.0.1:2"),
                        new Messages.Replica(3, "127.0.0.1:3"),
                        new Messages.Replica(4, "127.0.0.1:4"))).encode(), null);
            }
            if (op == Opcode.ABORT_CHUNK_META) {
                Messages.AbortChunkMeta.decode(req.headerSlice());
                throw new ScpException(ErrorCode.INTERNAL, "abort failed");
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(e.getMessage().contains("abort failed"));
            }
        }
    }

    @Test
    void fencedOpenAbortsCreatedChunkAndKillsAppender() throws Exception {
        FileId fileId = FileId.of(20);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean deletedOpenedReplica = new AtomicBoolean();
        AtomicBoolean openedAfterFence = new AtomicBoolean();

        try (ScpServer opened = new ScpServer(0, 1, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.OPEN_CHUNK) {
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            if (op == Opcode.DELETE_CHUNKS) {
                Messages.DeleteChunks.decode(req.headerSlice());
                deletedOpenedReplica.set(true);
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             ScpServer fenced = openErrorServer(2, ErrorCode.FENCED_EPOCH);
             ScpServer shouldNotOpen = openTrackingDataNode(3, openedAfterFence);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                             new Messages.Replica(1, endpoint(opened)),
                             new Messages.Replica(2, endpoint(fenced)),
                             new Messages.Replica(3, endpoint(shouldNotOpen)))).encode(), null);
                 }
                 if (op == Opcode.ABORT_CHUNK_META) {
                     Messages.AbortChunkMeta.decode(req.headerSlice());
                     aborted.set(true);
                     return ScpServer.ok(req, Messages.okHeader(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.FENCED_EPOCH, e.code());
                assertTrue(aborted.get());
                assertTrue(deletedOpenedReplica.get());
                assertFalse(openedAfterFence.get());
            }
        }
    }

    @Test
    void closeDuringChunkCreateAbortsMetadataBeforeOpeningDataNode() throws Exception {
        FileId fileId = FileId.of(21);
        ChunkId chunkId = new ChunkId(fileId, 0);
        CountDownLatch createStarted = new CountDownLatch(1);
        CountDownLatch releaseCreate = new CountDownLatch(1);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean openedDataNode = new AtomicBoolean();

        try (ScpServer s1 = openTrackingDataNode(1, openedDataNode);
             ScpServer s2 = openTrackingDataNode(2, openedDataNode);
             ScpServer s3 = openTrackingDataNode(3, openedDataNode);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     createStarted.countDown();
                     if (!releaseCreate.await(1, TimeUnit.SECONDS)) {
                         throw new ScpException(ErrorCode.INTERNAL, "test did not release create");
                     }
                     return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                             new Messages.Replica(1, endpoint(s1)),
                             new Messages.Replica(2, endpoint(s2)),
                             new Messages.Replica(3, endpoint(s3)))).encode(), null);
                 }
                 if (op == Opcode.ABORT_CHUNK_META) {
                     Messages.AbortChunkMeta.decode(req.headerSlice());
                     aborted.set(true);
                     return ScpServer.ok(req, Messages.okHeader(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
                CompletableFuture<ScpException> appendFailure = CompletableFuture.supplyAsync(() -> {
                    try {
                        appender.append(ByteBuffer.wrap(new byte[] {1}));
                        return null;
                    } catch (ScpException e) {
                        return e;
                    }
                });

                assertTrue(createStarted.await(1, TimeUnit.SECONDS));
                appender.close();
                releaseCreate.countDown();

                ScpException e = appendFailure.get(1, TimeUnit.SECONDS);
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
                assertFalse(openedDataNode.get());
            }
        }
    }

    @Test
    void sealCommitsOnlyMatchingQuorumAndThenSealsFile() throws Exception {
        FileId fileId = FileId.of(22);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<List<Integer>> sealedReplicas = new AtomicReference<>();
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer s1 = dataNodeServer(1, 123, false);
             ScpServer s2 = dataNodeServer(2, 123, false);
             ScpServer s3 = dataNodeServer(3, 456, false);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, sealedReplicas, sealedFileLength,
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                long ack = appender.append(ByteBuffer.wrap(new byte[] {9})).get(1, TimeUnit.SECONDS);
                assertEquals(1, ack);
                assertEquals(1, appender.durableOffset());

                StrataFile.SealInfo seal = appender.seal();
                assertEquals(1, seal.sealedLength());
                assertEquals(List.of(1, 2), sealedReplicas.get());
                assertEquals(1L, sealedFileLength.get());
            }
        }
    }

    @Test
    void replacedReplicaConnectionFailsReplicaWithoutReplayingAppend() throws Exception {
        FileId fileId = FileId.of(23);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<List<Integer>> sealedReplicas = new AtomicReference<>();
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicInteger s1Appends = new AtomicInteger();
        AtomicInteger s2Appends = new AtomicInteger();
        AtomicInteger s3Appends = new AtomicInteger();

        try (ScpServer s1 = countedDataNodeServer(1, s1Appends);
             ScpServer s2 = countedDataNodeServer(2, s2Appends);
             ScpServer s3 = countedDataNodeServer(3, s3Appends);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, sealedReplicas, sealedFileLength,
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool(config)) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId,
                        StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                assertEquals(1, (long) appender.append(ByteBuffer.wrap(new byte[] {1})).get(1, TimeUnit.SECONDS));
                // Quiesce before disconnect so the disconnect cannot lose s1's in-flight first-append
                // response and trigger a roll that re-opens s1 (see the multi-connection variant below).
                waitFor(() -> s1Appends.get() == 1 && s2Appends.get() == 1 && s3Appends.get() == 1
                        && appendsQuiesced(appender));
                pool.get(endpoint(s1)).disconnect();

                assertEquals(2, (long) appender.append(ByteBuffer.wrap(new byte[] {2})).get(1, TimeUnit.SECONDS));
                assertEquals(1, s1Appends.get(), "replaced replica connection must not receive replayed append");
                assertEquals(2, s2Appends.get());
                assertEquals(2, s3Appends.get());

                StrataFile.SealInfo seal = appender.seal();
                assertEquals(2, seal.sealedLength());
                assertEquals(List.of(2, 3), sealedReplicas.get());
                assertEquals(2L, sealedFileLength.get());
            }
        }
    }

    @Test
    void appendPinsReplicaConnectionWhenEndpointHasMultipleConnections() throws Exception {
        FileId fileId = FileId.of(24);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<List<Integer>> sealedReplicas = new AtomicReference<>();
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();
        AtomicInteger s1Appends = new AtomicInteger();
        AtomicInteger s2Appends = new AtomicInteger();
        AtomicInteger s3Appends = new AtomicInteger();

        try (ScpServer s1 = countedDataNodeServer(1, s1Appends);
             ScpServer s2 = countedDataNodeServer(2, s2Appends);
             ScpServer s3 = countedDataNodeServer(3, s3Appends);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, sealedReplicas, sealedFileLength,
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500)
                    .withDataNodeConnectionsPerEndpoint(2);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool(config)) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId,
                        StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                assertEquals(1, (long) appender.append(ByteBuffer.wrap(new byte[] {1})).get(1, TimeUnit.SECONDS));
                // append(...).get() returns on the s2+s3 ack quorum, so s1's first append can still be in
                // flight client-side (its response not yet processed). Wait until the pipeline is fully
                // quiesced before disconnecting s1's pinned connection: otherwise the disconnect can lose
                // s1's in-flight first-append response, fail s1, and trigger a roll that re-opens s1 on a
                // fresh connection — legitimately re-replicating to s1 and defeating this gen-gate check.
                waitFor(() -> s1Appends.get() == 1 && s2Appends.get() == 1 && s3Appends.get() == 1
                        && appendsQuiesced(appender));
                Object session = currentSession(appender);
                ManagedScpConnection pinnedFirstReplica = connections(session)[0];
                assertNotNull(pinnedFirstReplica);
                assertNotSame(pinnedFirstReplica, pool.get(endpoint(s1)));

                pinnedFirstReplica.disconnect();
                assertEquals(2, (long) appender.append(ByteBuffer.wrap(new byte[] {2})).get(1, TimeUnit.SECONDS));

                assertEquals(1, s1Appends.get(), "replaced pinned connection must fail without replaying append");
                assertEquals(2, s2Appends.get());
                assertEquals(2, s3Appends.get());
                assertTrue(booleanField(currentSession(appender), "needRoll"));

                StrataFile.SealInfo seal = appender.seal();
                assertEquals(2, seal.sealedLength());
                assertEquals(List.of(2, 3), sealedReplicas.get());
                assertEquals(2L, sealedFileLength.get());
            }
        }
    }

    @Test
    void sealQuorumLossFromSkippedReplicasUsesGenericCause() throws Exception {
        FileId fileId = FileId.of(25);
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer s1 = dataNodeServer(1, 123, false);
             ScpServer s2 = dataNodeServer(2, 123, false);
             ScpServer s3 = dataNodeServer(3, 123, false);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, new AtomicReference<>(),
                     new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
                Object session = chunkSession(chunkId,
                        new Messages.Replica(1, endpoint(s1)),
                        new Messages.Replica(2, endpoint(s2)),
                        new Messages.Replica(3, endpoint(s3)));
                setSession(appender, session);
                setBooleanArray(session, "failed", 1, true);
                setBooleanArray(session, "failed", 2, true);

                ScpException e = assertThrows(ScpException.class, appender::seal);
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(e.getMessage().contains("seal quorum lost"));
            }
        }
    }

    @Test
    void sealRejectsQuorumThatReportsWrongFinalLength() throws Exception {
        FileId fileId = FileId.of(26);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<List<Integer>> sealedReplicas = new AtomicReference<>();

        try (ScpServer s1 = dataNodeServer(1, 123, false, 1);
             ScpServer s2 = dataNodeServer(2, 123, false, 1);
             ScpServer s3 = dataNodeServer(3, 123, false, 0);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, sealedReplicas, new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
                appender.append(ByteBuffer.wrap(new byte[] {9})).get(1, TimeUnit.SECONDS);

                ScpException e = assertThrows(ScpException.class, appender::seal);
                assertEquals(ErrorCode.CORRUPT_CHUNK, e.code());
                assertEquals(null, sealedReplicas.get());
            }
        }
    }

    @Test
    void sealQuorumLossKillsAppender() throws Exception {
        FileId fileId = FileId.of(27);
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer s1 = dataNodeServer(1, 123, false);
             ScpServer s2 = dataNodeServer(2, 0, true);
             ScpServer s3 = dataNodeServer(3, 0, true);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, new AtomicReference<>(),
                     new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
                appender.append(ByteBuffer.wrap(new byte[] {9})).get(1, TimeUnit.SECONDS);

                ScpException e = assertThrows(ScpException.class, appender::seal);
                assertEquals(ErrorCode.INTERNAL, e.code());

                ScpException later = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.allocate(0)));
                assertEquals(ErrorCode.INTERNAL, later.code());
            }
        }
    }

    @Test
    void sealDivergenceWithoutAnyAgreeingQuorumKillsAppender() throws Exception {
        FileId fileId = FileId.of(28);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<List<Integer>> sealedReplicas = new AtomicReference<>();

        try (ScpServer s1 = dataNodeServer(1, 111, false);
             ScpServer s2 = dataNodeServer(2, 222, false);
             ScpServer s3 = dataNodeServer(3, 333, false);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, sealedReplicas, new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
                appender.append(ByteBuffer.wrap(new byte[] {9})).get(1, TimeUnit.SECONDS);

                ScpException e = assertThrows(ScpException.class, appender::seal);
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(e.getMessage().contains("replica seal divergence"));
                assertEquals(null, sealedReplicas.get());
            }
        }
    }

    @Test
    void malformedSealResponseCountsAgainstSealQuorum() throws Exception {
        FileId fileId = FileId.of(29);
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer s1 = dataNodeServer(1, 123, false);
             ScpServer s2 = malformedSealServer(2);
             ScpServer s3 = dataNodeServer(3, 0, true);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, new AtomicReference<>(),
                     new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
                appender.append(ByteBuffer.wrap(new byte[] {9})).get(1, TimeUnit.SECONDS);

                ScpException e = assertThrows(ScpException.class, appender::seal);
                assertEquals(ErrorCode.INTERNAL, e.code());

                ScpException later = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.allocate(0)));
                assertEquals(ErrorCode.INTERNAL, later.code());
            }
        }
    }

    @Test
    void sealFencedEpochKillsAppender() throws Exception {
        FileId fileId = FileId.of(30);
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer fencedSeal = new ScpServer(0, 1, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.OPEN_CHUNK) {
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            if (op == Opcode.APPEND) {
                Messages.Append append = Messages.Append.decode(req.headerSlice());
                return ScpServer.ok(req,
                        new Messages.AppendResp(append.baseOffset() + req.payloadLength()).encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                throw new ScpException(ErrorCode.FENCED_EPOCH, "seal fenced", 31);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
             ScpServer s2 = dataNodeServer(2, 123, false);
             ScpServer s3 = dataNodeServer(3, 123, false);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, new AtomicReference<>(),
                     new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(fencedSeal)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
                appender.append(ByteBuffer.wrap(new byte[] {9})).get(1, TimeUnit.SECONDS);

                ScpException e = assertThrows(ScpException.class, appender::seal);
                assertEquals(ErrorCode.FENCED_EPOCH, e.code());
                assertEquals(31, e.detail());
            }
        }
    }

    @Test
    void sealChunkMetadataFailureKillsAppenderAfterReplicaSeal() throws Exception {
        FileId fileId = FileId.of(31);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean sealFileCalled = new AtomicBoolean();

        try (ScpServer s1 = dataNodeServer(1, 123, false);
             ScpServer s2 = dataNodeServer(2, 123, false);
             ScpServer s3 = dataNodeServer(3, 123, false);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                             new Messages.Replica(1, endpoint(s1)),
                             new Messages.Replica(2, endpoint(s2)),
                             new Messages.Replica(3, endpoint(s3)))).encode(), null);
                 }
                 if (op == Opcode.SEAL_CHUNK_META) {
                     Messages.SealChunkMeta.decode(req.headerSlice());
                     throw new ScpException(ErrorCode.INTERNAL, "seal chunk meta failed");
                 }
                 if (op == Opcode.SEAL_FILE) {
                     sealFileCalled.set(true);
                     return ScpServer.ok(req, Messages.okHeader(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
                appender.append(ByteBuffer.wrap(new byte[] {9})).get(1, TimeUnit.SECONDS);

                ScpException e = assertThrows(ScpException.class, appender::seal);
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(e.getMessage().contains("seal chunk meta failed"));
                assertFalse(sealFileCalled.get());
            }
        }
    }

    @Test
    void sealDetectsFileOffsetOverflowAfterChunkSeal() throws Exception {
        FileId fileId = FileId.of(32);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<List<Integer>> sealedReplicas = new AtomicReference<>();

        try (ScpServer s1 = dataNodeServer(1, 123, false);
             ScpServer s2 = dataNodeServer(2, 123, false);
             ScpServer s3 = dataNodeServer(3, 123, false);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, sealedReplicas, new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT,
                        Long.MAX_VALUE - 1);
                Object session = chunkSession(chunkId,
                        new Messages.Replica(1, endpoint(s1)),
                        new Messages.Replica(2, endpoint(s2)),
                        new Messages.Replica(3, endpoint(s3)));
                setSession(appender, session);
                setLong(session, "end", 2);
                setLong(session, "durable", 2);

                ScpException e = assertThrows(ScpException.class, appender::seal);
                assertEquals(ErrorCode.CORRUPT_CHUNK, e.code());
                assertCommittedDefaultSealQuorum(sealedReplicas.get());
            }
        }
    }

    @Test
    void rollDetectsFileOffsetOverflowAfterChunkSeal() throws Exception {
        FileId fileId = FileId.of(33);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<List<Integer>> sealedReplicas = new AtomicReference<>();

        try (ScpServer s1 = dataNodeServer(1, 123, false);
             ScpServer s2 = dataNodeServer(2, 123, false);
             ScpServer s3 = dataNodeServer(3, 123, false);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, sealedReplicas, new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT,
                        Long.MAX_VALUE - 1);
                Object session = chunkSession(chunkId,
                        new Messages.Replica(1, endpoint(s1)),
                        new Messages.Replica(2, endpoint(s2)),
                        new Messages.Replica(3, endpoint(s3)));
                setSession(appender, session);
                setLong(session, "end", 2);
                setLong(session, "durable", 2);
                setBoolean(session, "needRoll", true);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.CORRUPT_CHUNK, e.code());
                assertCommittedDefaultSealQuorum(sealedReplicas.get());
            }
        }
    }

    @Test
    void sealFileMetadataFailureKillsAppenderAfterChunkSeal() throws Exception {
        FileId fileId = FileId.of(34);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<List<Integer>> sealedReplicas = new AtomicReference<>();
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer s1 = dataNodeServer(1, 123, false);
             ScpServer s2 = dataNodeServer(2, 123, false);
             ScpServer s3 = dataNodeServer(3, 123, false);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, sealedReplicas, sealedFileLength, true,
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
                appender.append(ByteBuffer.wrap(new byte[] {9})).get(1, TimeUnit.SECONDS);

                ScpException sealFailure = assertThrows(ScpException.class, appender::seal);
                assertEquals(ErrorCode.INTERNAL, sealFailure.code());
                assertCommittedDefaultSealQuorum(sealedReplicas.get());
                assertEquals(null, sealedFileLength.get());

                ScpException later = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.allocate(0)));
                assertEquals(ErrorCode.INTERNAL, later.code());
                assertTrue(later.getMessage().contains("seal file failed"));
            }
        }
    }

    @Test
    void appendAdmissionBlocksOnAnyLiveReplicaWithoutCapacityButIgnoresFailedReplica() throws Exception {
        Object session = chunkSession();      // 3 replicas, no connections established yet

        int watermark = 64;
        AppenderImpl appender = new AppenderImpl(null, null,
                new ClientConfig(List.of("127.0.0.1:1"), 1024, 100).withAppendWatermarks(watermark, 1),
                FileId.of(35), StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
        Field inFlightField = session.getClass().getDeclaredField("inFlight");
        inFlightField.setAccessible(true);
        int[] inFlight = (int[]) inFlightField.get(session);
        Method hasCapacity = AppenderImpl.class.getDeclaredMethod(
                "hasAppendConnectionCapacity", session.getClass());
        hasCapacity.setAccessible(true);

        // A live replica must receive every append for this open chunk; a saturated live replica
        // backpressures instead of being sent past the connection's hard pending limit.
        inFlight[0] = watermark;
        assertFalse((boolean) hasCapacity.invoke(appender, session),
                "appends must block when any live replica lacks capacity");

        // Once that replica is failed out of the current chunk, the remaining quorum may proceed.
        setBooleanArray(session, "failed", 0, true);
        assertTrue((boolean) hasCapacity.invoke(appender, session),
                "failed replicas no longer gate the live append quorum");
    }

    @Test
    void partialOpenQuorumAcceptsFirstAppendBeforeRollingShortSession() throws Exception {
        FileId fileId = FileId.of(38);
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicInteger appends = new AtomicInteger();
        AtomicInteger createCalls = new AtomicInteger();
        AtomicInteger sealedMetadata = new AtomicInteger();

        try (ScpServer ok1 = countedDataNodeServer(1, appends);
             ScpServer ok2 = countedDataNodeServer(2, appends);
             ScpServer failed = failingOpenServer(3);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     if (createCalls.incrementAndGet() > 1) {
                         throw new ScpException(ErrorCode.NO_CAPACITY, "need 3 nodes, found 2");
                     }
                     return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                             new Messages.Replica(1, endpoint(ok1)),
                             new Messages.Replica(2, endpoint(ok2)),
                             new Messages.Replica(3, endpoint(failed)))).encode(), null);
                 }
                 if (op == Opcode.SEAL_CHUNK_META) {
                     Messages.SealChunkMeta.decode(req.headerSlice());
                     sealedMetadata.incrementAndGet();
                     return ScpServer.ok(req, Messages.okHeader(), null);
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (ControllerClient meta = new ControllerClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId,
                        StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);

                CompletableFuture<Long> appended = appender.append(ByteBuffer.wrap(new byte[] {1, 2, 3}));

                assertEquals(3L, appended.get(1, TimeUnit.SECONDS));
                assertEquals(1, createCalls.get(), "first append must not immediately roll the short session");
                assertEquals(0, sealedMetadata.get(), "short session should not seal before accepting first append");
                assertEquals(2, appends.get(), "append should fan out to the remaining quorum");
                assertTrue(booleanField(currentSession(appender), "needRoll"));
                appender.close();
            }
        }
    }

    @Test
    void appendRetriesOnActiveSessionAfterBackpressureWaitSeesRolledSession() throws Exception {
        FileId fileId = FileId.of(37);
        AtomicInteger oldAppends = new AtomicInteger();
        AtomicInteger newAppends = new AtomicInteger();

        try (ScpServer old1 = countedDataNodeServer(1, oldAppends);
             ScpServer old2 = countedDataNodeServer(2, oldAppends);
             ScpServer old3 = countedDataNodeServer(3, oldAppends);
             ScpServer new1 = countedDataNodeServer(4, newAppends);
             ScpServer new2 = countedDataNodeServer(5, newAppends);
             ScpServer new3 = countedDataNodeServer(6, newAppends);
             NodePool pool = new NodePool()) {
            ClientConfig config = new ClientConfig(List.of("127.0.0.1:1"), 1024, 500);
            AppenderImpl appender = new AppenderImpl(null, pool, config, fileId, StrataNamespace.of("test"),
                    1, Messages.WritePolicy.DEFAULT, 0);
            Object oldSession = chunkSession(new ChunkId(fileId, 0),
                    new Messages.Replica(1, endpoint(old1)),
                    new Messages.Replica(2, endpoint(old2)),
                    new Messages.Replica(3, endpoint(old3)));
            Object newSession = chunkSession(new ChunkId(fileId, 1),
                    new Messages.Replica(4, endpoint(new1)),
                    new Messages.Replica(5, endpoint(new2)),
                    new Messages.Replica(6, endpoint(new3)));
            setSession(appender, oldSession);

            inFlight(oldSession)[0] = config.appendReplicaInflightHighWatermark();

            AtomicReference<CompletableFuture<Long>> appendResult = new AtomicReference<>();
            AtomicReference<Throwable> appendFailure = new AtomicReference<>();
            Thread appendThread = Thread.ofVirtual().start(() -> {
                try {
                    appendResult.set(appender.append(ByteBuffer.wrap(new byte[] {1, 2, 3})));
                } catch (Throwable t) {
                    appendFailure.set(t);
                }
            });

            waitForProgressWaiter(appender);
            setSessionUnderLock(appender, newSession);
            onReplicaResponse(appender, oldSession, 0, 0, appendResp(0), null);

            appendThread.join(2_000);
            assertFalse(appendThread.isAlive(), "append should return after retrying on the active session");
            assertEquals(null, appendFailure.get());
            CompletableFuture<Long> result = appendResult.get();
            assertNotNull(result);
            assertEquals(3L, result.get(1, TimeUnit.SECONDS));
            waitFor(() -> newAppends.get() == 3);
            assertEquals(0, oldAppends.get(), "stale session must not receive the append after the wait");
            assertEquals(3, newAppends.get());
        }
    }

    @Test
    void appendWaitsAfterBackpressureIfSameSessionStartsRolling() throws Exception {
        FileId fileId = FileId.of(44);
        AtomicInteger appends = new AtomicInteger();

        try (ScpServer s1 = countedDataNodeServer(1, appends);
             ScpServer s2 = countedDataNodeServer(2, appends);
             ScpServer s3 = countedDataNodeServer(3, appends);
             NodePool pool = new NodePool()) {
            ClientConfig config = new ClientConfig(List.of("127.0.0.1:1"), 1024, 500);
            AppenderImpl appender = new AppenderImpl(null, pool, config, fileId, StrataNamespace.of("test"),
                    1, Messages.WritePolicy.DEFAULT, 0);
            Object session = chunkSession(new ChunkId(fileId, 0),
                    new Messages.Replica(1, endpoint(s1)),
                    new Messages.Replica(2, endpoint(s2)),
                    new Messages.Replica(3, endpoint(s3)));
            setSession(appender, session);

            inFlight(session)[0] = config.appendReplicaInflightHighWatermark();

            AtomicReference<CompletableFuture<Long>> appendResult = new AtomicReference<>();
            AtomicReference<Throwable> appendFailure = new AtomicReference<>();
            Thread appendThread = Thread.ofVirtual().start(() -> {
                try {
                    appendResult.set(appender.append(ByteBuffer.wrap(new byte[] {1, 2, 3})));
                } catch (Throwable t) {
                    appendFailure.set(t);
                }
            });

            waitForProgressWaiter(appender);
            ReentrantLock lock = appenderLock(appender);
            Condition progress = appenderProgress(appender);
            lock.lock();
            try {
                setBoolean(appender, "rolling", true);
                inFlight(session)[0] = 0;
                progress.signalAll();
            } finally {
                lock.unlock();
            }

            try {
                TimeUnit.MILLISECONDS.sleep(200);
                assertEquals(0, appends.get(), "append must not enter a session while seal/roll owns it");
                assertNull(appendResult.get());
                assertNull(appendFailure.get());
                assertTrue(appendThread.isAlive(), "append should still be waiting for rolling to finish");
            } finally {
                lock.lock();
                try {
                    setBoolean(appender, "rolling", false);
                    progress.signalAll();
                } finally {
                    lock.unlock();
                }
                appendThread.join(2_000);
                appender.close();
            }

            assertFalse(appendThread.isAlive(), "append should proceed once rolling clears");
            assertNull(appendFailure.get());
            assertNotNull(appendResult.get());
            assertEquals(3L, appendResult.get().get(1, TimeUnit.SECONDS));
            waitFor(() -> appends.get() == 3);
            assertEquals(3, appends.get());
        }
    }

    private static AppenderImpl appender() {
        return new AppenderImpl(null, null, new ClientConfig(List.of("127.0.0.1:1"), 1024, 100),
                FileId.of(35), StrataNamespace.of("test"), 1, Messages.WritePolicy.DEFAULT, 0);
    }

    private static Object chunkSession() throws Exception {
        return chunkSession(new ChunkId(FileId.of(36), 0),
                new Messages.Replica(1, "n1"),
                new Messages.Replica(2, "n2"),
                new Messages.Replica(3, "n3"));
    }

    private static Object chunkSession(ChunkId chunkId, Messages.Replica... replicas) throws Exception {
        return new AppenderImpl.ChunkSession(chunkId, List.of(replicas), 0L, 0L);
    }

    private static void onReplicaResponse(AppenderImpl appender, Object session,
                                          Frame frame, Throwable err) throws Exception {
        onReplicaResponse(appender, session, 0, frame, err);
    }

    private static void onReplicaResponse(AppenderImpl appender, Object session, int replicaIndex,
                                          Frame frame, Throwable err) throws Exception {
        long expectedEnd = 0;
        if (frame != null) {
            ByteBuffer h = frame.headerSlice();
            try {
                Resp.check(h);
                expectedEnd = Messages.AppendResp.decode(h).endOffset();
            } catch (RuntimeException ignored) {
                // Error responses do not carry an AppendResp; the production method ignores
                // expectedEnd after Resp.check throws, and stale responses are ignored before
                // append-response decoding.
            }
        }
        onReplicaResponse(appender, session, replicaIndex, expectedEnd, frame, err);
    }

    private static void onReplicaResponse(AppenderImpl appender, Object session, int replicaIndex,
                                          long expectedEnd, Frame frame, Throwable err) throws Exception {
        Method method = AppenderImpl.class.getDeclaredMethod("onReplicaResponse",
                session.getClass(), int.class, long.class, Frame.class, Throwable.class);
        method.setAccessible(true);
        method.invoke(appender, session, replicaIndex, expectedEnd, frame, err);
    }

    private static Frame appendResp(long end) {
        return Frame.response(Frame.request(Opcode.APPEND, new byte[0], null, 7),
                new Messages.AppendResp(end).encode(), null);
    }

    private static Frame errorResp(ErrorCode code, String message, int detail) {
        return Frame.response(Frame.request(Opcode.APPEND, new byte[0], null, 7),
                Resp.error(code, message, detail), null);
    }

    private static void setSession(AppenderImpl appender, Object session) throws Exception {
        Field field = AppenderImpl.class.getDeclaredField("session");
        field.setAccessible(true);
        field.set(appender, session);
    }

    private static Object currentSession(AppenderImpl appender) throws Exception {
        Field field = AppenderImpl.class.getDeclaredField("session");
        field.setAccessible(true);
        return field.get(appender);
    }

    private static void setSessionUnderLock(AppenderImpl appender, Object session) throws Exception {
        ReentrantLock lock = appenderLock(appender);
        lock.lock();
        try {
            setSession(appender, session);
        } finally {
            lock.unlock();
        }
    }

    private static ReentrantLock appenderLock(AppenderImpl appender) throws Exception {
        Field field = AppenderImpl.class.getDeclaredField("lock");
        field.setAccessible(true);
        return (ReentrantLock) field.get(appender);
    }

    private static Condition appenderProgress(AppenderImpl appender) throws Exception {
        Field field = AppenderImpl.class.getDeclaredField("progress");
        field.setAccessible(true);
        return (Condition) field.get(appender);
    }

    private static void waitForProgressWaiter(AppenderImpl appender) throws Exception {
        ReentrantLock lock = appenderLock(appender);
        Condition progress = appenderProgress(appender);
        waitFor(() -> {
            lock.lock();
            try {
                return lock.hasWaiters(progress);
            } finally {
                lock.unlock();
            }
        });
    }

    private static void setLong(Object target, String fieldName, long value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setLong(target, value);
    }

    private static void setBoolean(Object target, String fieldName, boolean value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setBoolean(target, value);
    }

    private static boolean booleanField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getBoolean(target);
    }

    private static void setBooleanArray(Object target, String fieldName, int index, boolean value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        boolean[] values = (boolean[]) field.get(target);
        values[index] = value;
    }

    private static void assertCommittedDefaultSealQuorum(List<Integer> sealedReplicas) {
        assertNotNull(sealedReplicas);
        assertTrue(sealedReplicas.size() >= Messages.WritePolicy.DEFAULT.ackQuorum());
        assertTrue(sealedReplicas.size() <= Messages.WritePolicy.DEFAULT.replicationFactor());
        assertEquals(sealedReplicas.size(), new HashSet<>(sealedReplicas).size());
        assertTrue(List.of(1, 2, 3).containsAll(sealedReplicas));
    }

    private static ManagedScpConnection[] connections(Object session) throws Exception {
        Field field = session.getClass().getDeclaredField("connections");
        field.setAccessible(true);
        return (ManagedScpConnection[]) field.get(session);
    }

    private static int[] inFlight(Object session) throws Exception {
        Field field = session.getClass().getDeclaredField("inFlight");
        field.setAccessible(true);
        return (int[]) field.get(session);
    }

    @SuppressWarnings("unchecked")
    private static ArrayDeque<Object> pending(Object session) throws Exception {
        Field field = session.getClass().getDeclaredField("pending");
        field.setAccessible(true);
        return (ArrayDeque<Object>) field.get(session);
    }

    private static Object pending(long chunkEnd, CompletableFuture<Long> future) throws Exception {
        return new AppenderImpl.Pending(chunkEnd, future);
    }

    @SuppressWarnings("unchecked")
    private static boolean validReplicaSet(List<Messages.Replica> replicas) throws Exception {
        Method method = AppenderImpl.class.getDeclaredMethod("validReplicaSet", List.class);
        method.setAccessible(true);
        return (Boolean) method.invoke(appender(), replicas);
    }

    private static ScpServer failingOpenServer(int nodeId) throws Exception {
        return openErrorServer(nodeId, ErrorCode.INTERNAL);
    }

    private static ScpServer openErrorServer(int nodeId, ErrorCode code) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.OPEN_CHUNK) {
                throw new ScpException(code, "open failed");
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer openTrackingDataNode(int nodeId, AtomicBoolean openedDataNode) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.OPEN_CHUNK) {
                openedDataNode.set(true);
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            if (op == Opcode.DELETE_CHUNKS) {
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer countedDataNodeServer(int nodeId, AtomicInteger appends) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.OPEN_CHUNK) {
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            if (op == Opcode.APPEND) {
                appends.incrementAndGet();
                Messages.Append append = Messages.Append.decode(req.headerSlice());
                return ScpServer.ok(req,
                        new Messages.AppendResp(append.baseOffset() + req.payloadLength()).encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                Messages.SealChunk seal = Messages.SealChunk.decode(req.headerSlice());
                return ScpServer.ok(req, new Messages.SealResp(seal.dataLength(), 123).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    /** True once the appender's current chunk session has no in-flight replica requests (all acked). */
    private static boolean appendsQuiesced(AppenderImpl appender) {
        try {
            Object session = currentSession(appender);
            if (session == null) {
                return false;
            }
            Field f = session.getClass().getDeclaredField("inFlight");
            f.setAccessible(true);
            for (int n : (int[]) f.get(session)) {
                if (n != 0) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static ScpServer dataNodeServer(int nodeId, int sealCrc, boolean failSeal) throws Exception {
        return dataNodeServer(nodeId, sealCrc, failSeal, 0);
    }

    private static ScpServer dataNodeServer(int nodeId, int sealCrc, boolean failSeal, long sealLengthDelta)
            throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.OPEN_CHUNK) {
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            if (op == Opcode.APPEND) {
                Messages.Append append = Messages.Append.decode(req.headerSlice());
                return ScpServer.ok(req,
                        new Messages.AppendResp(append.baseOffset() + req.payloadLength()).encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                Messages.SealChunk seal = Messages.SealChunk.decode(req.headerSlice());
                if (failSeal) {
                    throw new ScpException(ErrorCode.INTERNAL, "seal failed");
                }
                return ScpServer.ok(req,
                        new Messages.SealResp(seal.dataLength() + sealLengthDelta, sealCrc).encode(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer malformedSealServer(int nodeId) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.OPEN_CHUNK) {
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            if (op == Opcode.APPEND) {
                Messages.Append append = Messages.Append.decode(req.headerSlice());
                return ScpServer.ok(req,
                        new Messages.AppendResp(append.baseOffset() + req.payloadLength()).encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK) {
                Messages.SealChunk.decode(req.headerSlice());
                return ScpServer.ok(req, new byte[] {0, 0}, null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer metadataForAppender(FileId fileId, ChunkId chunkId,
                                                 AtomicReference<List<Integer>> sealedReplicas,
                                                 AtomicReference<Long> sealedFileLength,
                                                 Messages.Replica... replicas) throws Exception {
        return metadataForAppender(fileId, chunkId, sealedReplicas, sealedFileLength, false, replicas);
    }

    private static ScpServer metadataForAppender(FileId fileId, ChunkId chunkId,
                                                 AtomicReference<List<Integer>> sealedReplicas,
                                                 AtomicReference<Long> sealedFileLength,
                                                 boolean failSealFile,
                                                 Messages.Replica... replicas) throws Exception {
        return new ScpServer(0, 0, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.CREATE_CHUNK) {
                Messages.CreateChunk.decode(req.headerSlice());
                return ScpServer.ok(req,
                        new Messages.CreateChunkResp(chunkId, 1, List.of(replicas)).encode(), null);
            }
            if (op == Opcode.SEAL_CHUNK_META) {
                Messages.SealChunkMeta seal = Messages.SealChunkMeta.decode(req.headerSlice());
                sealedReplicas.set(seal.sealedReplicas());
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            if (op == Opcode.SEAL_FILE) {
                Messages.SealFile seal = Messages.SealFile.decode(req.headerSlice());
                assertEquals(fileId, seal.fileId());
                if (failSealFile) {
                    throw new ScpException(ErrorCode.INTERNAL, "seal file failed");
                }
                sealedFileLength.set(seal.totalLength());
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static String endpoint(ScpServer server) {
        return "127.0.0.1:" + server.port();
    }

    private static void waitFor(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean());
    }
}
