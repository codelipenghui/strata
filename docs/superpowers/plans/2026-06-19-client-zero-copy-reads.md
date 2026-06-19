# Client Zero-Copy Borrowed Reads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the two client-side heap copies on the `READ` path by returning a borrowed, read-only direct `ByteBuffer` (a slice of the retained pooled Netty `ByteBuf`) released on `ReadResult.close()`.

**Architecture:** Add an opt-in "borrow" path to the Netty client transport so a `READ` response frame is delivered to the caller *retained* (not copied to heap). `ReaderImpl` returns that frame's payload as a read-only `ByteBuffer` wrapped in an `AutoCloseable StrataFile.ReadResult`; closing the result releases the pooled buffer. Every non-`READ` opcode keeps today's copy-to-heap behavior.

**Tech Stack:** Java 21, Maven (multi-module), JUnit 5, Netty (pooled allocator, `DefaultFileRegion`).

## Global Constraints

- Java 21; Maven multi-module. Build/test a single module: `mvn -q -pl <module> -am test`; single class: `mvn -q -pl <module> -am test -Dtest=ClassName`.
- **Non-`READ` opcodes must remain byte-for-byte unchanged.** HELLO, APPEND, STAT, SEAL, FENCE, DELETE, FETCH, meta, admin all keep `copyToHeap()` delivery. Existing assertion `assertFalse(resp.ownsBuffer())` for non-borrow client responses MUST stay true.
- **`StrataFile.ReadResult` must not expose `io.netty.*` types in any public method signature.** The borrowed frame is held only as a private `AutoCloseable`.
- No new third-party dependencies.
- `ReadResult` is `AutoCloseable`; callers use try-with-resources. The borrowed `ByteBuffer` is valid only until `close()`.
- Run client/proto tests with `-Dio.netty.leakDetection.level=paranoid` as the leak safety net (added to verification commands below).

---

### Task 1: Borrow plumbing in `ScpClient`

Add an opt-in borrow path to the Netty client. Additive and internal — no existing behavior changes, so the module stays green.

**Files:**
- Modify: `strata-proto/src/main/java/io/strata/proto/ScpClient.java`
- Test: `strata-proto/src/test/java/io/strata/proto/ClientServerTest.java`

**Interfaces:**
- Consumes: existing `Frame` (`AutoCloseable`, `ownsBuffer()`, `ownerRefCnt()`, `payloadSlice()`, `headerSlice()`, `copyToHeap()`, `close()`), `ScpServer.ok(Frame, byte[], ByteBuffer)`.
- Produces:
  - `Frame ScpClient.callFrameBorrowed(Opcode op, byte[] header, ByteBuffer payload, long timeoutMs)` — returns a response frame whose pooled buffer is **retained**; the caller must `close()` it. Throws `ScpException` on error/timeout (no frame to close in that case).
  - Internal `record Pending(CompletableFuture<Frame> future, boolean borrow)`.

- [ ] **Step 1: Write the failing test**

Add to `ClientServerTest.java`:

```java
    @Test
    void callFrameBorrowedRetainsBufferUntilClosed() throws Exception {
        byte[] body = "borrowed-payload".getBytes();
        try (ScpServer server = new ScpServer(0, 1, 0xA, 0xB, req -> {
                if (Opcode.fromCode(req.opcode()) == Opcode.PING) {
                    return ScpServer.ok(req, Messages.okHeader(), ByteBuffer.wrap(body));
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "nope");
             });
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {

            Frame borrowed = client.callFrameBorrowed(Opcode.PING, emptyHeader(), ByteBuffer.wrap(body), 2000);
            try {
                assertTrue(borrowed.ownsBuffer(), "borrowed response must retain the pooled buffer");
                assertTrue(borrowed.ownerRefCnt() > 0, "buffer must be live before close");
                byte[] got = new byte[borrowed.payloadLength()];
                borrowed.payloadSlice().get(got);
                assertArrayEquals(body, got);
            } finally {
                borrowed.close();
            }
            assertEquals(0, borrowed.ownerRefCnt(), "close() must release the pooled buffer");
        }
    }

    @Test
    void nonBorrowResponsesStillCopyToHeap() throws Exception {
        try (ScpServer server = new ScpServer(0, 1, 0xA, 0xB, req -> {
                if (Opcode.fromCode(req.opcode()) == Opcode.PING) {
                    return ScpServer.ok(req, Messages.okHeader(), req.payloadSlice());
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "nope");
             });
             ScpClient client = new ScpClient("127.0.0.1", server.port(), ScpClient.KIND_TOOL, "t")) {
            Frame resp = client.callFrame(Opcode.PING, emptyHeader(), ByteBuffer.wrap("x".getBytes()), 2000);
            assertFalse(resp.ownsBuffer(), "non-borrow client responses must not retain Netty buffers");
        }
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl strata-proto -am test -Dtest=ClientServerTest#callFrameBorrowedRetainsBufferUntilClosed`
Expected: FAIL — `callFrameBorrowed` does not compile / is not defined.

- [ ] **Step 3: Change the pending registry to carry the borrow flag**

In `ScpClient.java`, change the field and add the holder type:

```java
    private record Pending(CompletableFuture<Frame> future, boolean borrow) {}

    private final Map<Long, Pending> pending = new ConcurrentHashMap<>();
```

Update `removePending` to match by holder identity:

```java
    private boolean removePending(long id, Pending holder) {
        boolean removed = pending.remove(id, holder);
        if (removed) {
            pendingPermits.release();
        }
        return removed;
    }
```

Update `failAll` to unwrap the holder:

```java
    private void failAll(Exception e) {
        closed.set(true);
        handshake.completeExceptionally(e);
        pending.keySet().forEach(id -> {
            Pending holder = pending.remove(id);
            if (holder != null) {
                pendingPermits.release();
                holder.future().completeExceptionally(e);
            }
        });
    }
```

- [ ] **Step 4: Thread the borrow flag through `send` and branch in `channelRead0`**

Replace the public `send` with a thin delegate and a borrow-aware core:

```java
    public CompletableFuture<Frame> send(Opcode op, byte[] header, ByteBuffer payload) {
        return send(op, header, payload, false);
    }

    public CompletableFuture<Frame> send(Opcode op, byte[] header, ByteBuffer payload, boolean borrow) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IOException("connection closed"));
        }
        if (!pendingPermits.tryAcquire()) {
            return CompletableFuture.failedFuture(new ScpException(ErrorCode.THROTTLED,
                    "too many pending requests on connection: " + pending.size()
                            + " >= " + MAX_PENDING_REQUESTS));
        }
        long id = correlation.getAndIncrement();
        Frame request;
        try {
            if (op == null) {
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "opcode is null");
            }
            request = Frame.request(op, copyHeader(header), copyPayload(payload), id);
        } catch (RuntimeException e) {
            pendingPermits.release();
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<Frame> fut = new CompletableFuture<>();
        Pending holder = new Pending(fut, borrow);
        pending.put(id, holder);
        fut.whenComplete((r, e) -> removePending(id, holder));

        ChannelFuture write;
        try {
            write = channel.writeAndFlush(request);
        } catch (RuntimeException e) {
            IOException failure = asIOException(e);
            removePending(id, holder);
            failAll(failure);
            fut.completeExceptionally(failure);
            return fut;
        }
        write.addListener(f -> {
            if (!f.isSuccess()) {
                removePending(id, holder);
                IOException failure = asIOException(f.cause());
                failAll(failure);
                fut.completeExceptionally(failure);
            }
        });
        if (closed.get() && removePending(id, holder)) {
            fut.completeExceptionally(new IOException("connection closed"));
        }
        return fut;
    }
```

Update `channelRead0` to branch on borrow and release abandoned borrowed frames:

```java
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
            if (!handshake.isDone()) {
                Frame response;
                try {
                    response = frame.copyToHeap();
                } finally {
                    frame.close();
                }
                handshake.complete(response);
                return;
            }
            Pending holder = pending.remove(frame.correlationId());
            if (holder == null) {
                frame.close();   // abandoned/timed-out: release the retained pooled buffer
                return;
            }
            pendingPermits.release();
            if (holder.borrow()) {
                holder.future().complete(frame);  // ownership transfers to caller; no copy, no close
            } else {
                Frame response;
                try {
                    response = frame.copyToHeap();
                } finally {
                    frame.close();
                }
                holder.future().complete(response);
            }
        }
```

`sendWithTimeout` already calls `send(op, header, payload)`; leave it (borrow = false).

- [ ] **Step 5: Add `callFrameBorrowed`**

Add next to `callFrame`:

```java
    /** Synchronous call returning a response frame whose pooled buffer is retained; caller MUST close it. */
    public Frame callFrameBorrowed(Opcode op, byte[] header, ByteBuffer payload, long timeoutMs) {
        CompletableFuture<Frame> fut = send(op, header, payload, true);
        try {
            return fut.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ScpException se) throw se;
            throw new ScpException(ErrorCode.INTERNAL, String.valueOf(e.getCause()));
        } catch (TimeoutException e) {
            ScpException timeout = new ScpException(ErrorCode.INTERNAL,
                    "timeout after " + timeoutMs + "ms for " + op);
            fut.completeExceptionally(timeout);
            close();
            throw timeout;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ScpException interrupted = new ScpException(ErrorCode.INTERNAL, "interrupted");
            fut.completeExceptionally(interrupted);
            throw interrupted;
        }
    }
```

- [ ] **Step 6: Run the new tests and the full proto suite**

Run: `mvn -q -pl strata-proto -am test -Dtest=ClientServerTest -Dio.netty.leakDetection.level=paranoid`
Expected: PASS (new borrow tests pass; existing `pipelinedEchoAndErrors` and managed-connection tests unchanged; no leak warnings).

Then the whole module: `mvn -q -pl strata-proto -am test -Dio.netty.leakDetection.level=paranoid`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add strata-proto/src/main/java/io/strata/proto/ScpClient.java strata-proto/src/test/java/io/strata/proto/ClientServerTest.java
git commit -m "feat(proto): opt-in borrowed (zero-copy) response frames in ScpClient"
```

---

### Task 2: `ManagedScpConnection.callFrameBorrowed`

Expose the borrow path through the lifecycle-managed connection that `ReaderImpl` uses. Additive; module stays green.

**Files:**
- Modify: `strata-proto/src/main/java/io/strata/proto/ManagedScpConnection.java`
- Test: `strata-proto/src/test/java/io/strata/proto/ClientServerTest.java`

**Interfaces:**
- Consumes: `ScpClient.callFrameBorrowed(...)` (Task 1).
- Produces: `Frame ManagedScpConnection.callFrameBorrowed(Opcode op, byte[] header, ByteBuffer payload, long timeoutMs)` — returns a retained frame; caller closes. The connection's in-flight accounting (`acquire`/`release`) is independent of buffer lifetime.

- [ ] **Step 1: Write the failing test**

Add to `ClientServerTest.java` (reuses existing `managed(...)`, `endpoint(...)`, `emptyHeader()` helpers):

```java
    @Test
    void managedCallFrameBorrowedRetainsBufferUntilClosed() throws Exception {
        byte[] body = "managed-borrow".getBytes();
        try (ScpServer server = new ScpServer(0, 1, 0, 0, req -> {
                if (Opcode.fromCode(req.opcode()) == Opcode.PING) {
                    return ScpServer.ok(req, Messages.okHeader(), ByteBuffer.wrap(body));
                }
                throw new ScpException(ErrorCode.UNKNOWN_OPCODE, "unexpected");
             });
             ManagedScpConnection conn = managed(endpoint(server), 1_000, 1_000, 10_000)) {
            Frame borrowed = conn.callFrameBorrowed(Opcode.PING, emptyHeader(), ByteBuffer.wrap(body), 2000);
            try {
                assertTrue(borrowed.ownsBuffer());
                byte[] got = new byte[borrowed.payloadLength()];
                borrowed.payloadSlice().get(got);
                assertArrayEquals(body, got);
            } finally {
                borrowed.close();
            }
            assertEquals(0, borrowed.ownerRefCnt());
        }
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl strata-proto -am test -Dtest=ClientServerTest#managedCallFrameBorrowedRetainsBufferUntilClosed`
Expected: FAIL — `callFrameBorrowed` not defined on `ManagedScpConnection`.

- [ ] **Step 3: Implement the delegate**

Add to `ManagedScpConnection.java` next to `callFrame` (line ~114), mirroring its structure exactly:

```java
    public Frame callFrameBorrowed(Opcode op, byte[] header, ByteBuffer payload, long timeoutMs) {
        Ref ref = acquire(null);
        try {
            return ref.client().callFrameBorrowed(op, header, payload, timeoutMs);
        } catch (RuntimeException e) {
            throw classifyFailure(ref.client(), e);
        } finally {
            release();
        }
    }
```

- [ ] **Step 4: Run the test**

Run: `mvn -q -pl strata-proto -am test -Dtest=ClientServerTest#managedCallFrameBorrowedRetainsBufferUntilClosed -Dio.netty.leakDetection.level=paranoid`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add strata-proto/src/main/java/io/strata/proto/ManagedScpConnection.java strata-proto/src/test/java/io/strata/proto/ClientServerTest.java
git commit -m "feat(proto): callFrameBorrowed on ManagedScpConnection"
```

---

### Task 3: `StrataFile.ReadResult` → `AutoCloseable` + `ReaderImpl` rewrite + `strata-client` tests

The breaking API change, contained to `strata-client`. After this task `strata-client` compiles and its tests pass; **downstream modules (`strata-it`, `strata-server`) will not compile until Task 4** — do not run a full reactor build here.

**Files:**
- Modify: `strata-client/src/main/java/io/strata/client/StrataFile.java` (the `ReadResult` type at line 34; `Reader.read` Javadoc)
- Modify: `strata-client/src/main/java/io/strata/client/ReaderImpl.java` (`read` lines 47-80, `readFromReplicas` lines 82-145)
- Test: `strata-client/src/test/java/io/strata/client/ReaderImplTest.java`
- Test: `strata-client/src/test/java/io/strata/client/StrataClientBehaviorTest.java`

**Interfaces:**
- Consumes: `ManagedScpConnection.callFrameBorrowed(...)` (Task 2); `io.strata.proto.Frame` (already imported in `ReaderImpl`).
- Produces:
  - `final class StrataFile.ReadResult implements AutoCloseable` with `ByteBuffer buffer()`, `boolean endOfFile()`, `int length()`, `void close()`, package-private ctor `ReadResult(ByteBuffer, boolean, AutoCloseable)`, static `ReadResult empty(boolean endOfFile)`, and package-private `AutoCloseable releaseHandleForTest()`.
  - `private record ReaderImpl.Borrowed(Frame owner, ByteBuffer view)`.

- [ ] **Step 1: Write the new `ReadResult` type**

In `StrataFile.java`, replace `record ReadResult(byte[] data, boolean endOfFile) {}` (line 34) with:

```java
    final class ReadResult implements AutoCloseable {
        private static final ByteBuffer EMPTY = ByteBuffer.allocate(0).asReadOnlyBuffer();

        private final ByteBuffer buffer;
        private final boolean endOfFile;
        private final AutoCloseable release;

        ReadResult(ByteBuffer buffer, boolean endOfFile, AutoCloseable release) {
            this.buffer = buffer;
            this.endOfFile = endOfFile;
            this.release = release;
        }

        /** Empty result (no bytes, nothing to release). */
        public static ReadResult empty(boolean endOfFile) {
            return new ReadResult(EMPTY.duplicate(), endOfFile, null);
        }

        /** Read-only view of the read bytes. VALID ONLY until {@link #close()}. */
        public ByteBuffer buffer() {
            return buffer.duplicate();
        }

        public boolean endOfFile() {
            return endOfFile;
        }

        public int length() {
            return buffer.remaining();
        }

        @Override
        public void close() {
            if (release != null) {
                try {
                    release.close();
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new io.strata.common.ScpException(
                            io.strata.common.ErrorCode.INTERNAL, "failed to release read buffer: " + e);
                }
            }
        }

        /** Test-only: the underlying release handle (a Frame in production), for leak assertions. */
        AutoCloseable releaseHandleForTest() {
            return release;
        }
    }
```

`StrataFile.java` already imports `java.nio.ByteBuffer`. No `io.netty.*` import is added.

Update the `Reader.read` Javadoc (line ~57-59) to add: `The returned result is {@link AutoCloseable}; the caller owns it and MUST close it (try-with-resources). The buffer it exposes is valid only until close.`

- [ ] **Step 2: Rewrite `ReaderImpl.read` and `readFromReplicas`**

Replace `read` (lines 47-80) and `readFromReplicas` (lines 82-145) in `ReaderImpl.java` with:

```java
    @Override
    public StrataFile.ReadResult read(long fileOffset, int maxBytes) {
        if (fileOffset < 0) {
            throw new ScpException(ErrorCode.INTERNAL, "negative read offset " + fileOffset);
        }
        if (maxBytes < 0) {
            throw new ScpException(ErrorCode.INTERNAL, "negative read maxBytes " + maxBytes);
        }
        Messages.LookupFileResp f = file;
        long base = 0;
        for (int i = 0; i < f.chunks().size(); i++) {
            Messages.ChunkInfo chunk = f.chunks().get(i);
            boolean last = i == f.chunks().size() - 1;
            if (chunk.state() == ChunkState.SEALED) {
                long chunkEnd = addChunkLength(base, chunk.length());
                if (fileOffset < chunkEnd) {
                    long chunkOffset = fileOffset - base;
                    int want = (int) Math.min(maxBytes, chunk.length() - chunkOffset);
                    Borrowed b = readFromReplicas(chunk, chunkOffset, want, false);
                    boolean eof = f.fileState() == FileState.SEALED.value && last
                            && fileOffset + b.view().remaining() == chunkEnd;
                    return new StrataFile.ReadResult(b.view(), eof, b.owner());
                }
                base = chunkEnd;
            } else {
                long chunkOffset = fileOffset - base;
                Borrowed b = readFromReplicas(chunk, chunkOffset, maxBytes, true);
                return new StrataFile.ReadResult(b.view(), false, b.owner());
            }
        }
        boolean eof = f.fileState() == FileState.SEALED.value;
        return StrataFile.ReadResult.empty(eof);
    }

    private record Borrowed(Frame owner, ByteBuffer view) {}

    private Borrowed readFromReplicas(Messages.ChunkInfo chunk, long offset, int maxBytes, boolean open) {
        List<Messages.Replica> replicas = chunk.replicas();
        if (replicas.isEmpty()) {
            throw new ScpException(ErrorCode.INTERNAL, "no readable replica");
        }
        int start = ThreadLocalRandom.current().nextInt(replicas.size());
        byte[] readHeader = new Messages.Read(chunk.chunkId(), offset, maxBytes).encode();
        ScpException last = null;
        for (int i = 0; i < replicas.size(); i++) {
            Messages.Replica r = replicas.get((start + i) % replicas.size());
            if (r.endpoint().isEmpty()) continue;
            Frame frame = connectionFor(r.endpoint()).callFrameBorrowed(Opcode.READ,
                    readHeader, null, config.callTimeoutMs());
            boolean transferred = false;
            try {
                ByteBuffer h = frame.headerSlice();
                Resp.check(h);
                var resp = Messages.ReadResp.decode(h);
                if (resp.localEndOffset() < 0 || resp.durableOffset() < 0
                        || resp.durableOffset() > resp.localEndOffset()) {
                    last = new ScpException(ErrorCode.CORRUPT_CHUNK,
                            "bad read offsets from replica " + r.nodeId());
                    continue;
                }
                if (!open && resp.localEndOffset() < chunk.length()) {
                    last = new ScpException(io.strata.common.ErrorCode.CORRUPT_CHUNK,
                            "replica " + r.nodeId() + " short for sealed chunk " + chunk.chunkId()
                                    + ": " + resp.localEndOffset() + " < " + chunk.length());
                    continue;
                }
                if (frame.payloadLength() > maxBytes) {
                    last = new ScpException(ErrorCode.CORRUPT_CHUNK,
                            "replica " + r.nodeId() + " returned " + frame.payloadLength()
                                    + " bytes for max " + maxBytes);
                    continue;
                }
                if (!open && frame.payloadLength() != maxBytes) {
                    last = new ScpException(ErrorCode.CORRUPT_CHUNK,
                            "replica " + r.nodeId() + " returned short sealed read "
                                    + frame.payloadLength() + " != " + maxBytes);
                    continue;
                }
                ByteBuffer view = frame.payloadSlice();
                if (open) {
                    // never expose bytes above the replica-known durable offset (limit, not copy)
                    long readable = Math.min(resp.localEndOffset(), resp.durableOffset());
                    long visible = Math.max(0, readable - offset);
                    if (view.remaining() > visible) {
                        view.limit(view.position() + (int) visible);
                    }
                }
                transferred = true;
                return new Borrowed(frame, view);
            } catch (ScpException e) {
                last = e;
            } catch (RuntimeException e) {
                last = new ScpException(ErrorCode.CORRUPT_CHUNK,
                        "malformed read response from replica " + r.nodeId() + ": " + e);
            } finally {
                if (!transferred) frame.close();  // release on validation failure / exception / continue
            }
        }
        throw last != null ? last : new ScpException(ErrorCode.INTERNAL, "no readable replica");
    }
```

(`continue` inside the `try` runs the `finally`, so every non-`return` path releases the borrowed frame; the single `return` transfers ownership.)

- [ ] **Step 3: Add a test helper and migrate `ReaderImplTest` assertions**

In `ReaderImplTest.java` add a private helper (the file already imports `java.nio.ByteBuffer` and `io.strata.proto`):

```java
    private static byte[] drain(java.nio.ByteBuffer b) {
        byte[] a = new byte[b.remaining()];
        b.get(a);
        return a;
    }
```

Migrate each assertion to try-with-resources + `buffer()`:

- Lines 62 / 66 (`emptyFileReadUsesMetadataFileStateForEof`):

```java
                try (StrataFile.ReadResult r = reader.read(0, 1)) {
                    assertTrue(r.endOfFile());
                }
                // ...after refresh...
                try (StrataFile.ReadResult r = reader.read(0, 1)) {
                    assertFalse(r.endOfFile());
                }
```

- Lines 85-87 (`sealedReadOnOpenFileDoesNotReportEof`):

```java
                try (StrataFile.ReadResult result = reader.read(0, 3)) {
                    assertArrayEquals(new byte[] {1, 2, 3}, drain(result.buffer()));
                    assertFalse(result.endOfFile());
                }
```

- Lines 152-154:

```java
                try (StrataFile.ReadResult result = reader.read(0, 4)) {
                    assertArrayEquals(new byte[] {1, 2, 3, 4}, drain(result.buffer()));
                    assertFalse(result.endOfFile());
                }
```

- Lines 175 / 180 (inline `.data()` calls): wrap each in try-with-resources:

```java
                try (StrataFile.ReadResult rr = reader.read(0, 1)) {
                    assertArrayEquals(new byte[] {7}, drain(rr.buffer()));
                }
```

- Lines 205-206:

```java
                try (StrataFile.ReadResult firstRead = first.read(0, 1)) {
                    assertArrayEquals(new byte[] {9}, drain(firstRead.buffer()));
                }
                try (StrataFile.ReadResult secondRead = second.read(0, 1)) {
                    assertArrayEquals(new byte[] {9}, drain(secondRead.buffer()));
                }
```

- [ ] **Step 4: Add the close-release leak test**

Add to `ReaderImplTest.java` (uses the real `readReplica`/`metadataServer`/`chunk`/`endpoint` helpers the class already defines; import `io.strata.proto.Frame`):

```java
    @Test
    void borrowedReadReleasesPooledBufferOnClose() throws Exception {
        FileId fileId = FileId.random();
        ChunkId chunkId = new ChunkId(fileId, 0);
        try (ScpServer replica = readReplica(new Messages.ReadResp(3, 3), new byte[] {1, 2, 3});
             ScpServer metaServer = metadataServer(new AtomicReference<>(
                     new Messages.LookupFileResp("test", "/test/file", Messages.WritePolicy.DEFAULT, (byte) 0,
                             List.of(chunk(chunkId, ChunkState.SEALED, 3,
                                     new Messages.Replica(1, endpoint(replica)))))))) {
            ClientConfig config = new ClientConfig(List.of(endpoint(metaServer)), 1024, 500);
            try (MetaClient meta = new MetaClient(config); NodePool pool = new NodePool()) {
                ReaderImpl reader = new ReaderImpl(meta, pool, config, fileId);
                StrataFile.ReadResult result = reader.read(0, 3);
                io.strata.proto.Frame owner = (io.strata.proto.Frame) result.releaseHandleForTest();
                assertTrue(owner.ownsBuffer());
                assertTrue(owner.ownerRefCnt() > 0, "buffer live before close");
                assertArrayEquals(new byte[] {1, 2, 3}, drain(result.buffer()));
                result.close();
                assertEquals(0, owner.ownerRefCnt(), "close releases the pooled buffer");
                result.close(); // idempotent
                assertEquals(0, owner.ownerRefCnt());
            }
        }
    }
```

- [ ] **Step 5: Migrate `StrataClientBehaviorTest` assertions**

In `StrataClientBehaviorTest.java`, add the same `drain(...)` helper if not present, and migrate lines 251-253 and 260-262:

```java
                try (StrataFile.ReadResult result = reader.read(0, 10)) {
                    assertArrayEquals(new byte[] {1, 2, 3}, drain(result.buffer()));
                    assertEquals(true, result.endOfFile());
                }
                // ...
                try (StrataFile.ReadResult result = reader.read(0, 4)) {
                    assertArrayEquals(new byte[] {4, 5}, drain(result.buffer()));
                    assertEquals(false, result.endOfFile());
                }
```

- [ ] **Step 6: Run the `strata-client` suite (scoped — downstream is intentionally broken)**

Run: `mvn -q -pl strata-client -am test -Dio.netty.leakDetection.level=paranoid`
Expected: PASS (`ReaderImplTest`, `StrataClientBehaviorTest`, new leak test green; no leak warnings).

Note: `mvn -q test` (full reactor) WILL fail to compile `strata-it`/`strata-server` until Task 4 — that is expected.

- [ ] **Step 7: Commit**

```bash
git add strata-client/src/main/java/io/strata/client/StrataFile.java \
        strata-client/src/main/java/io/strata/client/ReaderImpl.java \
        strata-client/src/test/java/io/strata/client/ReaderImplTest.java \
        strata-client/src/test/java/io/strata/client/StrataClientBehaviorTest.java
git commit -m "feat(client): AutoCloseable ReadResult exposing borrowed zero-copy ByteBuffer"
```

---

### Task 4: Migrate downstream consumers (`strata-it`, `strata-server`)

Mechanical migration of every remaining `ReadResult.data()` caller to try-with-resources + `buffer()`. Restores a fully-green reactor build.

**Files:**
- Modify: `strata-it/src/test/java/io/strata/it/Workload.java:89-95`
- Modify: `strata-it/src/test/java/io/strata/it/ConsistencyVerifier.java:284-290`
- Modify: `strata-it/src/test/java/io/strata/it/BinaryWorkload.java:52-62`
- Modify: `strata-it/src/test/java/io/strata/it/EndToEndTest.java:74-79`
- Modify: `strata-it/src/test/java/io/strata/it/FailureRecoveryTest.java:104-106,195-200,230-233`
- Modify: `strata-it/src/test/java/io/strata/it/RecoveryDivergenceTest.java:45`
- Modify: `strata-it/src/test/java/io/strata/it/RecoveryCatchUpTest.java:152`
- Modify: `strata-server/src/main/java/io/strata/server/StrataPerf.java:432-446`

**Interfaces:**
- Consumes: `StrataFile.ReadResult` (`buffer()`, `length()`, `endOfFile()`, `close()`) from Task 3.

- [ ] **Step 1: `Workload.readAll`**

Original lines 89-99:

```java
                StrataFile.ReadResult r = reader.read(offset, 1 << 20);
                if (r.data().length > 0) {
                    out.write(r.data(), 0, r.data().length);
                    offset += r.data().length;
                    idleRounds = 0;
                } else if (r.endOfFile() || out.size() >= atLeast) {
                    break;
                } else {
                    idleRounds++;
                    reader.refresh();
                }
```

Replace with (the `break` still closes the resource):

```java
                try (StrataFile.ReadResult r = reader.read(offset, 1 << 20)) {
                    int n = r.length();
                    if (n > 0) {
                        byte[] tmp = new byte[n];
                        r.buffer().get(tmp);
                        out.write(tmp, 0, n);
                        offset += n;
                        idleRounds = 0;
                    } else if (r.endOfFile() || out.size() >= atLeast) {
                        break;
                    } else {
                        idleRounds++;
                        reader.refresh();
                    }
                }
```

- [ ] **Step 2: `ConsistencyVerifier`**

Original lines 284-294 (loop header is `for (int idle = 0; idle < 3; )`, no increment clause — keep it):

```java
                StrataFile.ReadResult result = reader.read(offset, 1 << 20);
                if (result.data().length > 0) {
                    out.write(result.data(), 0, result.data().length);
                    offset += result.data().length;
                    idle = 0;
                } else if (result.endOfFile()) {
                    break;
                } else {
                    idle++;
                    reader.refresh();
                }
```

Replace with:

```java
                try (StrataFile.ReadResult result = reader.read(offset, 1 << 20)) {
                    int n = result.length();
                    if (n > 0) {
                        byte[] tmp = new byte[n];
                        result.buffer().get(tmp);
                        out.write(tmp, 0, n);
                        offset += n;
                        idle = 0;
                    } else if (result.endOfFile()) {
                        break;
                    } else {
                        idle++;
                        reader.refresh();
                    }
                }
```

- [ ] **Step 3: `BinaryWorkload`**

Original lines 52-62:

```java
        StrataFile.ReadResult result;
        try {
            result = reader.read(0, 1 << 20);
        } catch (RuntimeException e) {
            throw new AssertionError(context + " read failed", e);
        }
        assertTrue(result.data().length <= expectedBytes.length,
                context + " read exposed " + result.data().length
                        + " bytes above acked " + expectedBytes.length);
        assertArrayEquals(Arrays.copyOf(expectedBytes, result.data().length), result.data(),
                context + " read returned a non-prefix");
```

Replace with (keep the read-failure `try/catch` as-is; `result` is assigned once so it is effectively final and usable in `try (result)`; preserve the exact assertion messages):

```java
        StrataFile.ReadResult result;
        try {
            result = reader.read(0, 1 << 20);
        } catch (RuntimeException e) {
            throw new AssertionError(context + " read failed", e);
        }
        try (result) {
            int n = result.length();
            assertTrue(n <= expectedBytes.length,
                    context + " read exposed " + n
                            + " bytes above acked " + expectedBytes.length);
            byte[] got = new byte[n];
            result.buffer().get(got);
            assertArrayEquals(Arrays.copyOf(expectedBytes, n), got,
                    context + " read returned a non-prefix");
        }
```

- [ ] **Step 4: `EndToEndTest`**

Replace lines 74-79:

```java
                try (StrataFile.ReadResult r = /* existing reader.read(...) */) {
                    int n = r.length();
                    assertTrue(n <= workload.ackedBytes(),
                            "read beyond acked bytes: " + n + " > " + workload.ackedBytes());
                    byte[] got = new byte[n];
                    r.buffer().get(got);
                    byte[] expected = new byte[n];
                    System.arraycopy(Workload.readAll(client, fileId, 0), 0, expected, 0, n);
                    assertArrayEquals(expected, got);
                }
```

- [ ] **Step 5: `FailureRecoveryTest` (three sites)**

Lines 104-106:

```java
                try (StrataFile.ReadResult tail = reader.read(0, 1 << 20)) {
                    assertTrue(tail.length() <= workload.ackedBytes(),
                            "open read exposed " + tail.length() + " bytes above acked "
                                    + workload.ackedBytes());
                }
```

Lines 195-200:

```java
                try (StrataFile.ReadResult full = reader.read(0, 1 << 20)) {
                    byte[] got = new byte[full.length()];
                    full.buffer().get(got);
                    assertArrayEquals(acked, got, "sealed read must equal acked prefix");
                }
                try (StrataFile.ReadResult tail = reader.read(acked.length, 1024)) {
                    assertEquals(0, tail.length(), "no bytes past the sealed end");
                }
```

Lines 230-233:

```java
                try (StrataFile.ReadResult full = reader.read(0, 1 << 20)) {
                    byte[] got = new byte[full.length()];
                    full.buffer().get(got);
                    assertArrayEquals(acked, got, "sealed read must equal acked prefix");
                    assertTrue(full.endOfFile(), "sealed read should end at acknowledged prefix");
                }
```

(Preserve the exact original assertion messages where they differ from the above.)

- [ ] **Step 6: `RecoveryDivergenceTest` and `RecoveryCatchUpTest` (inline reads)**

`RecoveryDivergenceTest.java:45`:

```java
                try (StrataFile.ReadResult rr = reader.read(0, 4)) {
                    byte[] got = new byte[rr.length()];
                    rr.buffer().get(got);
                    assertArrayEquals("AAAA".getBytes(), got);
                }
```

`RecoveryCatchUpTest.java:152`:

```java
                try (StrataFile.ReadResult rr = reader.read(0, 16)) {
                    byte[] got = new byte[rr.length()];
                    rr.buffer().get(got);
                    assertEquals("AAAABBBB", new String(got, StandardCharsets.UTF_8));
                }
```

(Both files already use `StrataFile`; add the import if absent.)

- [ ] **Step 7: `StrataPerf`**

Original lines 431-456:

```java
            long t0 = System.nanoTime();
            StrataFile.ReadResult result;
            try {
                result = reader.read(offset, remaining);
            } catch (RuntimeException e) {
                if (isTransientReadUnavailable(e) && retries++ < 20) {
                    reader.refresh();
                    sleepInterruptibly(100);
                    continue;
                }
                readStats.recordError("perf read range", e);
                file.abort();
                return reads;
            }
            readStats.record(System.nanoTime() - t0);
            int n = result.data().length;
            if (n == 0) {
                reader.refresh();
                sleepInterruptibly(5);
                continue;
            }
            retries = 0;
            readStats.ops.increment();
            readStats.bytes.add(n);
            offset += n;
            remaining -= n;
```

Replace with try-with-resources so the pooled buffer is released every iteration (critical: this is the throughput/GC harness). The `catch` on a try-with-resources block also catches initializer (`reader.read`) failures, preserving the transient-retry path; `n` is declared before and definitely assigned on the normal-completion path (all catch paths `continue`/`return`):

```java
            long t0 = System.nanoTime();
            int n;
            try (StrataFile.ReadResult result = reader.read(offset, remaining)) {
                readStats.record(System.nanoTime() - t0);
                n = result.length();
            } catch (RuntimeException e) {
                if (isTransientReadUnavailable(e) && retries++ < 20) {
                    reader.refresh();
                    sleepInterruptibly(100);
                    continue;
                }
                readStats.recordError("perf read range", e);
                file.abort();
                return reads;
            }
            if (n == 0) {
                reader.refresh();
                sleepInterruptibly(5);
                continue;
            }
            retries = 0;
            readStats.ops.increment();
            readStats.bytes.add(n);
            offset += n;
            remaining -= n;
```

`StrataPerf` measures throughput only by byte count (`n`), so the payload bytes do not need to be drained.

- [ ] **Step 8: Full reactor build + test**

Run: `mvn -q test -Dio.netty.leakDetection.level=paranoid`
Expected: PASS across all modules; no compile errors; no Netty leak warnings.

- [ ] **Step 9: Commit**

```bash
git add strata-it/src/test/java/io/strata/it/Workload.java \
        strata-it/src/test/java/io/strata/it/ConsistencyVerifier.java \
        strata-it/src/test/java/io/strata/it/BinaryWorkload.java \
        strata-it/src/test/java/io/strata/it/EndToEndTest.java \
        strata-it/src/test/java/io/strata/it/FailureRecoveryTest.java \
        strata-it/src/test/java/io/strata/it/RecoveryDivergenceTest.java \
        strata-it/src/test/java/io/strata/it/RecoveryCatchUpTest.java \
        strata-server/src/main/java/io/strata/server/StrataPerf.java
git commit -m "refactor: migrate read consumers to AutoCloseable ReadResult buffer API"
```

---

## Notes on coverage the plan intentionally defers

- **Abandoned/timeout borrowed-frame release** (`channelRead0` `holder == null` branch) and **failover release** (`readFromReplicas` `finally`) are structurally guaranteed by the code and verified by running the proto/client/it suites with `-Dio.netty.leakDetection.level=paranoid` rather than by a bespoke per-buffer refCnt assertion (the discarded frames are internal and not observable from a black-box test). The deterministic refCnt assertion covers the happy-path close in Task 3 Step 4.
- **Pool residency** under slow consumers is a documented tradeoff (spec §5), bounded by `MAX_PENDING_REQUESTS` and the 64 MiB max frame; no code change.
