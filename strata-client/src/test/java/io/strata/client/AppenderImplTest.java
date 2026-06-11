package io.strata.client;

import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.proto.Frame;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;
import io.strata.proto.ScpServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

        var ack = appender.append(ByteBuffer.allocate(0)).join();
        assertEquals(0, ack.endOffset());
        assertEquals(0, ack.durableOffset());
    }

    @Test
    void staleReplicaFailureIsIgnoredAfterRoll() throws Exception {
        AppenderImpl appender = appender();
        Object staleSession = chunkSession();

        onReplicaResponse(appender, staleSession, null,
                new ScpException(ErrorCode.INTERNAL, "old chunk timed out"));

        var ack = appender.append(ByteBuffer.allocate(0)).join();
        assertEquals(0, ack.endOffset());
        assertEquals(0, ack.durableOffset());
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
                new java.util.concurrent.CompletionException(
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
                new java.util.concurrent.CompletionException(
                        new java.util.concurrent.CompletionException(
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

        var ack = appender.append(ByteBuffer.allocate(0)).join();
        assertEquals(0, ack.endOffset());
        assertEquals(0, ack.durableOffset());
    }

    @Test
    void staleMalformedReplicaResponseIsIgnoredAfterRoll() throws Exception {
        AppenderImpl appender = appender();
        Object staleSession = chunkSession();
        Frame malformed = Frame.response(Frame.request(Opcode.APPEND, new byte[0], null, 7),
                new byte[] {0}, null);

        onReplicaResponse(appender, staleSession, malformed, null);

        var ack = appender.append(ByteBuffer.allocate(0)).join();
        assertEquals(0, ack.endOffset());
        assertEquals(0, ack.durableOffset());
    }

    @Test
    void zeroLengthAppendWaitsForOutstandingDurability() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 7);
        setLong(session, "durable", 3);

        CompletableFuture<StrataClient.AppendAck> ack = appender.append(ByteBuffer.allocate(0));
        assertFalse(ack.isDone());

        onReplicaResponse(appender, session, 0, appendResp(7), null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 1, appendResp(7), null);

        StrataClient.AppendAck result = ack.get(1, TimeUnit.SECONDS);
        assertEquals(7, result.endOffset());
        assertEquals(7, result.durableOffset());
    }

    @Test
    void zeroLengthAppendReturnsImmediatelyWhenSessionAlreadyDurable() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 7);
        setLong(session, "durable", 7);

        StrataClient.AppendAck ack = appender.append(ByteBuffer.allocate(0)).get(1, TimeUnit.SECONDS);

        assertEquals(7, ack.endOffset());
        assertEquals(7, ack.durableOffset());
    }

    @Test
    void staleNullReplicaResponseIsIgnoredAfterRoll() throws Exception {
        AppenderImpl appender = appender();
        Object staleSession = chunkSession();

        onReplicaResponse(appender, staleSession, null, null);

        var ack = appender.append(ByteBuffer.allocate(0)).join();
        assertEquals(0, ack.endOffset());
        assertEquals(0, ack.durableOffset());
    }

    @Test
    void twoReplicaFailuresLoseQuorumAndFailPendingAppends() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 11);

        CompletableFuture<StrataClient.AppendAck> ack = appender.append(ByteBuffer.allocate(0));

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

        CompletableFuture<StrataClient.AppendAck> ack = appender.append(ByteBuffer.allocate(0));

        onReplicaResponse(appender, session, 0, 5, appendResp(6), null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 1, appendResp(5), null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 2, appendResp(5), null);

        StrataClient.AppendAck result = ack.get(1, TimeUnit.SECONDS);
        assertEquals(5, result.endOffset());
        assertEquals(5, result.durableOffset());
    }

    @Test
    void malformedAppendResponseCountsAsReplicaFailure() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 5);

        CompletableFuture<StrataClient.AppendAck> ack = appender.append(ByteBuffer.allocate(0));
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

        CompletableFuture<StrataClient.AppendAck> ack = appender.append(ByteBuffer.allocate(0));

        onReplicaResponse(appender, session, 0, 5, null, null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 1, appendResp(5), null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 2, appendResp(5), null);

        StrataClient.AppendAck result = ack.get(1, TimeUnit.SECONDS);
        assertEquals(5, result.endOffset());
        assertEquals(5, result.durableOffset());
    }

    @Test
    void activeAsyncFencedFailureKillsAppender() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 5);

        CompletableFuture<StrataClient.AppendAck> ack = appender.append(ByteBuffer.allocate(0));

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

        CompletableFuture<StrataClient.AppendAck> ack = appender.append(ByteBuffer.allocate(0));

        onReplicaResponse(appender, session, 0, null,
                new java.util.concurrent.CompletionException(
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

        CompletableFuture<StrataClient.AppendAck> ack = appender.append(ByteBuffer.allocate(0));

        onReplicaResponse(appender, session, 0, null, new RuntimeException("transport closed"));
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 1, appendResp(5), null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 2, appendResp(5), null);

        StrataClient.AppendAck result = ack.get(1, TimeUnit.SECONDS);
        assertEquals(5, result.endOffset());
        assertEquals(5, result.durableOffset());
    }

    @Test
    void completionExceptionWithoutCauseCountsAsReplicaFailure() throws Exception {
        AppenderImpl appender = appender();
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 5);

        CompletableFuture<StrataClient.AppendAck> ack = appender.append(ByteBuffer.allocate(0));

        onReplicaResponse(appender, session, 0, null,
                new java.util.concurrent.CompletionException("closed", null));
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 1, appendResp(5), null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 2, appendResp(5), null);

        StrataClient.AppendAck result = ack.get(1, TimeUnit.SECONDS);
        assertEquals(5, result.endOffset());
        assertEquals(5, result.durableOffset());
    }

    @Test
    void synchronousReplicaFailuresStopFanoutAfterQuorumLost() throws Exception {
        CountDownLatch unexpectedAppend = new CountDownLatch(1);
        FileId fileId = FileId.random();
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
                    fileId, 1, (byte) 0, (byte) 0, 0);
            Object session = chunkSession(chunkId,
                    new Messages.Replica(1, "invalid-replica-a"),
                    new Messages.Replica(2, "invalid-replica-b"),
                    new Messages.Replica(3, endpoint(shouldNotReceive)));
            setSession(appender, session);

            CompletableFuture<StrataClient.AppendAck> ack = appender.append(ByteBuffer.wrap(new byte[] {1}));

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

        CompletableFuture<StrataClient.AppendAck> ack = appender.append(ByteBuffer.allocate(0));

        onReplicaResponse(appender, session, 0, null,
                new ScpException(ErrorCode.INTERNAL, "first failure"));
        onReplicaResponse(appender, session, 0, null,
                new ScpException(ErrorCode.INTERNAL, "duplicate failure"));
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 1, appendResp(5), null);
        assertFalse(ack.isDone());
        onReplicaResponse(appender, session, 2, appendResp(5), null);

        StrataClient.AppendAck result = ack.get(1, TimeUnit.SECONDS);
        assertEquals(5, result.endOffset());
        assertEquals(5, result.durableOffset());
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
                FileId.random(), 1, (byte) 0, (byte) 0, 0);
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
                FileId.random(), 1, (byte) 0, (byte) 0, Long.MAX_VALUE - 1);
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 2);

        CompletableFuture<StrataClient.AppendAck> ack = appender.append(ByteBuffer.allocate(0));
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
                FileId.random(), 1, (byte) 0, (byte) 0, 0);
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 1);
        setLong(session, "durable", 0);
        setBoolean(session, "needRoll", true);
        CompletableFuture<StrataClient.AppendAck> pending = new CompletableFuture<>();
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
                FileId.random(), 1, (byte) 0, (byte) 0, 0);
        Object session = chunkSession();
        setSession(appender, session);
        setLong(session, "end", 1);
        setLong(session, "durable", 0);
        setBoolean(session, "needRoll", true);
        CompletableFuture<StrataClient.AppendAck> pending = new CompletableFuture<>();
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
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer s1 = storageServer(1, 123, false);
             ScpServer s2 = storageServer(2, 0, true);
             ScpServer s3 = storageServer(3, 0, true);
             NodePool pool = new NodePool()) {
            ClientConfig config = new ClientConfig(List.of("127.0.0.1:1"), 1024, 500);
            AppenderImpl appender = new AppenderImpl(null, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);
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
        FileId fileId = FileId.random();
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
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);

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
        FileId fileId = FileId.random();
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
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);

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
    void createChunkWithWrongReplicaSetIsAbortedBeforeOpeningStorage() throws Exception {
        FileId fileId = FileId.random();
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
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
            }
        }
    }

    @Test
    void createChunkWithWrongFileIdIsAbortedBeforeOpeningStorage() throws Exception {
        FileId fileId = FileId.random();
        ChunkId wrongChunkId = new ChunkId(FileId.random(), 0);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean openedStorage = new AtomicBoolean();

        try (ScpServer storage = openTrackingStorage(1, openedStorage);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.CreateChunkResp(wrongChunkId, 1, List.of(
                             new Messages.Replica(1, endpoint(storage)),
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
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
                assertFalse(openedStorage.get());
            }
        }
    }

    @Test
    void createChunkWithDuplicateNodeIdIsAbortedBeforeOpeningStorage() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean openedStorage = new AtomicBoolean();

        try (ScpServer storage = openTrackingStorage(1, openedStorage);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                             new Messages.Replica(1, endpoint(storage)),
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
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
                assertFalse(openedStorage.get());
            }
        }
    }

    @Test
    void createChunkWithBlankEndpointIsAbortedBeforeOpeningStorage() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean openedStorage = new AtomicBoolean();

        try (ScpServer storage = openTrackingStorage(1, openedStorage);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                             new Messages.Replica(1, endpoint(storage)),
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
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
                assertFalse(openedStorage.get());
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
    void createChunkWithDuplicateEndpointIsAbortedBeforeOpeningStorage() throws Exception {
        FileId fileId = FileId.random();
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
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
            }
        }
    }

    @Test
    void createChunkWithInvalidNodeIdIsAbortedBeforeOpeningStorage() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean openedStorage = new AtomicBoolean();

        try (ScpServer storage = openTrackingStorage(1, openedStorage);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 1, List.of(
                             new Messages.Replica(0, endpoint(storage)),
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
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
                assertFalse(openedStorage.get());
            }
        }
    }

    @Test
    void createChunkWithMismatchedEpochIsAbortedBeforeOpeningStorage() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean openedStorage = new AtomicBoolean();

        try (ScpServer storage = openTrackingStorage(1, openedStorage);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     return ScpServer.ok(req, new Messages.CreateChunkResp(chunkId, 2, List.of(
                             new Messages.Replica(1, endpoint(storage)),
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
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(aborted.get());
                assertFalse(openedStorage.get());
            }
        }
    }

    @Test
    void createChunkFailureKillsAppenderWithoutOpeningStorage() throws Exception {
        FileId fileId = FileId.random();
        AtomicBoolean openedStorage = new AtomicBoolean();

        try (ScpServer storage = openTrackingStorage(1, openedStorage);
             ScpServer metaServer = new ScpServer(0, 0, 0, 0, req -> {
                 Opcode op = Opcode.fromCode(req.opcode());
                 if (op == Opcode.CREATE_CHUNK) {
                     Messages.CreateChunk.decode(req.headerSlice());
                     throw new ScpException(ErrorCode.INTERNAL, "create failed");
                 }
                 throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
             })) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertFalse(openedStorage.get());
            }
        }
    }

    @Test
    void abortMetadataFailureBecomesAppenderDeathCause() throws Exception {
        FileId fileId = FileId.random();
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
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);

                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[] {1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(e.getMessage().contains("abort failed"));
            }
        }
    }

    @Test
    void fencedOpenAbortsCreatedChunkAndKillsAppender() throws Exception {
        FileId fileId = FileId.random();
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
             ScpServer shouldNotOpen = openTrackingStorage(3, openedAfterFence);
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
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);

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
    void closeDuringChunkCreateAbortsMetadataBeforeOpeningStorage() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        CountDownLatch createStarted = new CountDownLatch(1);
        CountDownLatch releaseCreate = new CountDownLatch(1);
        AtomicBoolean aborted = new AtomicBoolean();
        AtomicBoolean openedStorage = new AtomicBoolean();

        try (ScpServer s1 = openTrackingStorage(1, openedStorage);
             ScpServer s2 = openTrackingStorage(2, openedStorage);
             ScpServer s3 = openTrackingStorage(3, openedStorage);
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
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);
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
                assertFalse(openedStorage.get());
            }
        }
    }

    @Test
    void sealCommitsOnlyMatchingQuorumAndThenSealsFile() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<List<Integer>> sealedReplicas = new AtomicReference<>();
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer s1 = storageServer(1, 123, false);
             ScpServer s2 = storageServer(2, 123, false);
             ScpServer s3 = storageServer(3, 456, false);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, sealedReplicas, sealedFileLength,
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);

                StrataClient.AppendAck ack = appender.append(ByteBuffer.wrap(new byte[] {9})).get(1, TimeUnit.SECONDS);
                assertEquals(1, ack.endOffset());
                assertEquals(1, ack.durableOffset());

                StrataClient.SealInfo seal = appender.seal();
                assertEquals(1, seal.sealedLength());
                assertEquals(List.of(1, 2), sealedReplicas.get());
                assertEquals(1L, sealedFileLength.get());
            }
        }
    }

    @Test
    void sealQuorumLossFromSkippedReplicasUsesGenericCause() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer s1 = storageServer(1, 123, false);
             ScpServer s2 = storageServer(2, 123, false);
             ScpServer s3 = storageServer(3, 123, false);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, new AtomicReference<>(),
                     new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);
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
    void bestSealQuorumPrefersLargestAgreeingGroup() throws Exception {
        Object smallerKey = sealKey(1, 11);
        Object largerKey = sealKey(1, 22);
        Map<Object, List<Integer>> votes = new LinkedHashMap<>();
        votes.put(smallerKey, List.of(1, 2));
        votes.put(largerKey, List.of(1, 2, 3));

        Map.Entry<?, ?> best = bestSealQuorum(votes);

        assertEquals(largerKey, best.getKey());
        assertEquals(List.of(1, 2, 3), best.getValue());
    }

    @Test
    void bestSealQuorumKeepsFirstQuorumWhenVotesTie() throws Exception {
        Object firstKey = sealKey(1, 11);
        Object secondKey = sealKey(1, 22);
        Map<Object, List<Integer>> votes = new LinkedHashMap<>();
        votes.put(firstKey, List.of(1, 2));
        votes.put(secondKey, List.of(2, 3));

        Map.Entry<?, ?> best = bestSealQuorum(votes);

        assertEquals(firstKey, best.getKey());
        assertEquals(List.of(1, 2), best.getValue());
    }

    @Test
    void sealRejectsQuorumThatReportsWrongFinalLength() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<List<Integer>> sealedReplicas = new AtomicReference<>();

        try (ScpServer s1 = storageServer(1, 123, false, 1);
             ScpServer s2 = storageServer(2, 123, false, 1);
             ScpServer s3 = storageServer(3, 123, false, 0);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, sealedReplicas, new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);
                appender.append(ByteBuffer.wrap(new byte[] {9})).get(1, TimeUnit.SECONDS);

                ScpException e = assertThrows(ScpException.class, appender::seal);
                assertEquals(ErrorCode.CORRUPT_CHUNK, e.code());
                assertEquals(null, sealedReplicas.get());
            }
        }
    }

    @Test
    void sealQuorumLossKillsAppender() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer s1 = storageServer(1, 123, false);
             ScpServer s2 = storageServer(2, 0, true);
             ScpServer s3 = storageServer(3, 0, true);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, new AtomicReference<>(),
                     new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);
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
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<List<Integer>> sealedReplicas = new AtomicReference<>();

        try (ScpServer s1 = storageServer(1, 111, false);
             ScpServer s2 = storageServer(2, 222, false);
             ScpServer s3 = storageServer(3, 333, false);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, sealedReplicas, new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);
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
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);

        try (ScpServer s1 = storageServer(1, 123, false);
             ScpServer s2 = malformedSealServer(2);
             ScpServer s3 = storageServer(3, 0, true);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, new AtomicReference<>(),
                     new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);
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
        FileId fileId = FileId.random();
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
             ScpServer s2 = storageServer(2, 123, false);
             ScpServer s3 = storageServer(3, 123, false);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, new AtomicReference<>(),
                     new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(fencedSeal)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);
                appender.append(ByteBuffer.wrap(new byte[] {9})).get(1, TimeUnit.SECONDS);

                ScpException e = assertThrows(ScpException.class, appender::seal);
                assertEquals(ErrorCode.FENCED_EPOCH, e.code());
                assertEquals(31, e.detail());
            }
        }
    }

    @Test
    void sealChunkMetadataFailureKillsAppenderAfterReplicaSeal() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicBoolean sealFileCalled = new AtomicBoolean();

        try (ScpServer s1 = storageServer(1, 123, false);
             ScpServer s2 = storageServer(2, 123, false);
             ScpServer s3 = storageServer(3, 123, false);
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
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);
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
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<List<Integer>> sealedReplicas = new AtomicReference<>();

        try (ScpServer s1 = storageServer(1, 123, false);
             ScpServer s2 = storageServer(2, 123, false);
             ScpServer s3 = storageServer(3, 123, false);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, sealedReplicas, new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0,
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
                assertEquals(List.of(1, 2, 3), sealedReplicas.get());
            }
        }
    }

    @Test
    void rollDetectsFileOffsetOverflowAfterChunkSeal() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<List<Integer>> sealedReplicas = new AtomicReference<>();

        try (ScpServer s1 = storageServer(1, 123, false);
             ScpServer s2 = storageServer(2, 123, false);
             ScpServer s3 = storageServer(3, 123, false);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, sealedReplicas, new AtomicReference<>(),
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0,
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
                assertEquals(List.of(1, 2, 3), sealedReplicas.get());
            }
        }
    }

    @Test
    void sealFileMetadataFailureKillsAppenderAfterChunkSeal() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        AtomicReference<List<Integer>> sealedReplicas = new AtomicReference<>();
        AtomicReference<Long> sealedFileLength = new AtomicReference<>();

        try (ScpServer s1 = storageServer(1, 123, false);
             ScpServer s2 = storageServer(2, 123, false);
             ScpServer s3 = storageServer(3, 123, false);
             ScpServer metaServer = metadataForAppender(fileId, chunkId, sealedReplicas, sealedFileLength, true,
                     new Messages.Replica(1, endpoint(s1)),
                     new Messages.Replica(2, endpoint(s2)),
                     new Messages.Replica(3, endpoint(s3)))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                AppenderImpl appender = new AppenderImpl(meta, pool, config, fileId, 1, (byte) 0, (byte) 0, 0);
                appender.append(ByteBuffer.wrap(new byte[] {9})).get(1, TimeUnit.SECONDS);

                ScpException sealFailure = assertThrows(ScpException.class, appender::seal);
                assertEquals(ErrorCode.INTERNAL, sealFailure.code());
                assertEquals(List.of(1, 2, 3), sealedReplicas.get());
                assertEquals(null, sealedFileLength.get());

                ScpException later = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.allocate(0)));
                assertEquals(ErrorCode.INTERNAL, later.code());
                assertTrue(later.getMessage().contains("seal file failed"));
            }
        }
    }

    private static AppenderImpl appender() {
        return new AppenderImpl(null, null, new ClientConfig(List.of("127.0.0.1:1"), 1024, 100),
                FileId.random(), 1, (byte) 0, (byte) 0, 0);
    }

    private static Object chunkSession() throws Exception {
        return chunkSession(new ChunkId(FileId.random(), 0),
                new Messages.Replica(1, "n1"),
                new Messages.Replica(2, "n2"),
                new Messages.Replica(3, "n3"));
    }

    private static Object chunkSession(ChunkId chunkId, Messages.Replica... replicas) throws Exception {
        Class<?> type = Class.forName("io.strata.client.AppenderImpl$ChunkSession");
        Constructor<?> ctor = type.getDeclaredConstructor(ChunkId.class, List.class);
        ctor.setAccessible(true);
        return ctor.newInstance(chunkId, List.of(replicas));
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

    private static void setBooleanArray(Object target, String fieldName, int index, boolean value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        boolean[] values = (boolean[]) field.get(target);
        values[index] = value;
    }

    @SuppressWarnings("unchecked")
    private static ArrayDeque<Object> pending(Object session) throws Exception {
        Field field = session.getClass().getDeclaredField("pending");
        field.setAccessible(true);
        return (ArrayDeque<Object>) field.get(session);
    }

    private static Object pending(long chunkEnd, CompletableFuture<StrataClient.AppendAck> future) throws Exception {
        Class<?> type = Class.forName("io.strata.client.AppenderImpl$Pending");
        Constructor<?> ctor = type.getDeclaredConstructor(long.class, CompletableFuture.class);
        ctor.setAccessible(true);
        return ctor.newInstance(chunkEnd, future);
    }

    @SuppressWarnings("unchecked")
    private static boolean validReplicaSet(List<Messages.Replica> replicas) throws Exception {
        Method method = AppenderImpl.class.getDeclaredMethod("validReplicaSet", List.class);
        method.setAccessible(true);
        return (Boolean) method.invoke(null, replicas);
    }

    private static Object sealKey(long finalLength, int crc) throws Exception {
        Class<?> type = Class.forName("io.strata.client.AppenderImpl$SealKey");
        Constructor<?> ctor = type.getDeclaredConstructor(long.class, int.class);
        ctor.setAccessible(true);
        return ctor.newInstance(finalLength, crc);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Map.Entry<?, ?> bestSealQuorum(Map<Object, List<Integer>> votes) throws Exception {
        Method method = AppenderImpl.class.getDeclaredMethod("bestSealQuorum", Map.class);
        method.setAccessible(true);
        return (Map.Entry<?, ?>) method.invoke(null, new LinkedHashMap(votes));
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

    private static ScpServer openTrackingStorage(int nodeId, AtomicBoolean openedStorage) throws Exception {
        return new ScpServer(0, nodeId, 0, 0, req -> {
            Opcode op = Opcode.fromCode(req.opcode());
            if (op == Opcode.OPEN_CHUNK) {
                openedStorage.set(true);
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            if (op == Opcode.DELETE_CHUNKS) {
                return ScpServer.ok(req, Messages.okHeader(), null);
            }
            throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected " + op);
        });
    }

    private static ScpServer storageServer(int nodeId, int sealCrc, boolean failSeal) throws Exception {
        return storageServer(nodeId, sealCrc, failSeal, 0);
    }

    private static ScpServer storageServer(int nodeId, int sealCrc, boolean failSeal, long sealLengthDelta)
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
}
