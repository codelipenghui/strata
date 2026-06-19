# Zero-copy borrowed reads on the client

**Date:** 2026-06-19
**Status:** Approved design, pending implementation plan
**Scope:** `strata-client`, `strata-proto`, and read consumers (`strata-it`, `strata-server`)

## 1. Background

A client read currently lands its data in a JVM-heap `byte[]`, exposed through
`StrataFile.ReadResult(byte[] data, boolean endOfFile)`
([`StrataFile.java:34`](../../../strata-client/src/main/java/io/strata/client/StrataFile.java)).

There are three `ReadResult`-shaped types in the system; only the client one is worth changing:

- **`StrataFile.ReadResult(byte[] data, …)`** — client API. On-heap, on the hot read path. **This is the target.**
- **`ChunkStore.ReadResult(byte[] bytes, …)`** — server *verified* read (`ChunkStore.read()`). A side/recovery path; its `byte[]` is mandated by integrity-CRC verification. **Out of scope.**
- **`ChunkStore.ReadRegionResult(FileChannel | byte[], …)`** — the wire `READ` path. Already zero-copy: the server returns a bare `FileChannel` streamed via Netty `DefaultFileRegion`/sendfile; the `byte[]` arm is only the `READ_RECOVERY` branch. **Out of scope.**

So the server hot read path already allocates no `byte[]` and copies no data on-heap. The remaining heap traffic is entirely on the **client receive path**, where bytes are copied **twice**:

1. socket → Netty pooled **direct** `ByteBuf` → **heap**, in `Frame.copyToHeap()`
   ([`Frame.java:162-172`](../../../strata-proto/src/main/java/io/strata/proto/Frame.java)), invoked unconditionally for every response in
   `ScpClient.ClientHandler.channelRead0` ([`ScpClient.java:149-165`](../../../strata-proto/src/main/java/io/strata/proto/ScpClient.java)); then
2. heap → application `byte[]`, in `ReaderImpl.readFromReplicas`
   ([`ReaderImpl.java:126-127`](../../../strata-client/src/main/java/io/strata/client/ReaderImpl.java)).

## 2. Goal & non-goals

**Goal:** eliminate both client-side heap copies on the `READ` path by handing the caller a **borrowed, read-only direct `ByteBuffer`** that is a slice of the retained pooled Netty `ByteBuf`, released when the caller closes the result. The benefit is reduced per-read allocation / young-gen GC pressure on read-heavy clients; it is **not** a fix for the measured server throughput ceiling (single-disk fsync/IO) or for the documented server-side carrier-pinning issue.

**Non-goals:**
- No change to the server, `ChunkStore`, or recovery paths (already zero-copy / CRC-bound to heap).
- No change to any non-`READ` opcode — `copyToHeap()` remains the behavior for HELLO, APPEND acks, STAT, meta calls, admin, etc.
- No change to the append path.
- No `byte[]`-returning convenience on `ReadResult` — callers that need an array drain the `ByteBuffer` themselves (explicit decision: keep the zero-copy default honest).

The public API **will break**; this is accepted (project is not in production).

## 3. Decisions (resolved)

| Decision | Choice |
|---|---|
| Buffer ownership | **Borrowed + explicit release** — full end-to-end zero-copy; caller must `close()`. |
| Return type | **Read-only `java.nio.ByteBuffer` view** on an `AutoCloseable ReadResult`. |
| Transport mechanism | **A1** — per-call `borrow` flag threaded through the pending registry. |
| Array convenience | **None.** Callers drain the `ByteBuffer`. |

## 4. Detailed design

### 4.1 Public API — `StrataFile.ReadResult`

Replace the record with an `AutoCloseable` handle (nested in the `StrataFile` interface, so implicitly `public static`):

```java
final class ReadResult implements AutoCloseable {
    private final ByteBuffer buffer;      // read-only direct view (canonical; callers get duplicates)
    private final boolean endOfFile;
    private final AutoCloseable release;  // the borrowed Frame, or null for an empty result

    // package-private ctor used by ReaderImpl (same package); static empty(boolean eof) factory
    public ByteBuffer buffer();    // returns buffer.duplicate() — read-only; VALID ONLY until close()
    public boolean endOfFile();
    public int length();           // buffer.remaining()
    public void close();           // releases the borrowed pooled buffer; idempotent; no-op when release == null
}
```

- `Reader.read(...)` Javadoc gains the contract: **the caller owns the result and must `close()` it** (try-with-resources). Reading `buffer()` after `close()` is undefined.
- The `Frame` is held only as an `AutoCloseable`, so `io.netty.*` does not leak into the public surface. `strata-client` already depends on `strata-proto`.
- `buffer()` returns a `duplicate()` so repeated calls each get an independent cursor and the durable-clamp `limit` is preserved.

### 4.2 Transport borrow mechanism (A1)

**`ScpClient`:**
- The pending registry value carries the borrow intent: `Map<Long, CompletableFuture<Frame>>` → `Map<Long, Pending>` where `record Pending(CompletableFuture<Frame> future, boolean borrow)`.
- `send(op, header, payload)` delegates to a new `send(op, header, payload, boolean borrow)`. The holder is registered and the existing cleanup hook captures the holder by reference:
  ```java
  Pending holder = new Pending(fut, borrow);
  pending.put(id, holder);
  fut.whenComplete((r, e) -> removePending(id, holder)); // remove(id, holder) — identity match
  ```
  `removePending(long, Pending)` uses `pending.remove(id, holder)` and releases a permit only on a real removal (preserves today's no-double-release behavior).
- `channelRead0` reads `frame.correlationId()` **before** any close, then branches:
  ```java
  if (!handshake.isDone()) { Frame r = frame.copyToHeap(); frame.close(); handshake.complete(r); return; }
  Pending holder = pending.remove(frame.correlationId());
  if (holder == null) { frame.close(); return; }   // abandoned/timed-out: release the retained buffer (was: drop heap copy)
  pendingPermits.release();
  if (holder.borrow()) {
      holder.future().complete(frame);              // ownership transfers to the caller; NO copyToHeap, NO close
  } else {
      Frame response;
      try { response = frame.copyToHeap(); } finally { frame.close(); }
      holder.future().complete(response);
  }
  ```
  The `holder == null` → `frame.close()` branch is the key new leak-safety rule: a borrowed response that arrives after its call was abandoned (caller-side timeout) must release the pooled buffer rather than be silently dropped.
- New `callFrameBorrowed(op, header, payload, timeoutMs)`: mirrors `callFrame` but sends with `borrow = true`. On success returns a frame whose pooled buffer is retained (caller closes). On timeout/interrupt it completes the future exceptionally and closes the connection exactly as `callFrame` does; a late borrowed frame is released by the `holder == null` branch above. Non-borrow `callFrame`/`call`/`send` semantics are byte-for-byte unchanged.

**`ManagedScpConnection`:** add `Frame callFrameBorrowed(op, header, payload, timeoutMs)` mirroring `callFrame` ([`ManagedScpConnection.java:114-123`](../../../strata-proto/src/main/java/io/strata/proto/ManagedScpConnection.java)). The `activeApplicationCalls` accounting (`acquire`/`release`) is unchanged and is independent of buffer lifetime — the `release()` in `finally` only decrements the in-flight counter; the returned frame's pooled buffer outlives the call window. Netty pooled buffers belong to the allocator arena (refcounted independently of the channel), so a borrowed buffer also survives an idle-monitor connection drop. `classifyFailure` only runs on exception (no frame produced), so no leak path there.

### 4.3 `ReaderImpl`

`readFromReplicas` returns a private `record Borrowed(Frame owner, ByteBuffer view)` instead of `byte[]`:

```java
for (int i = 0; i < replicas.size(); i++) {
    Messages.Replica r = ...; if (r.endpoint().isEmpty()) continue;
    Frame frame = connectionFor(r.endpoint()).callFrameBorrowed(Opcode.READ, readHeader, null, timeout);
    boolean transferred = false;
    try {
        ByteBuffer hb = frame.headerSlice(); Resp.check(hb);
        var resp = Messages.ReadResp.decode(hb);
        // ... existing validation: bad offsets / short sealed replica / payload > maxBytes /
        //     short sealed read → set last and `continue` (finally closes the borrowed frame) ...
        ByteBuffer view = frame.payloadSlice();                 // read-only direct slice
        if (open) {                                             // durable-HWM clamp = limit(), NOT a copy
            long readable = Math.min(resp.localEndOffset(), resp.durableOffset());
            long visible = Math.max(0, readable - offset);
            if (view.remaining() > visible) view.limit(view.position() + (int) visible);
        }
        transferred = true;
        return new Borrowed(frame, view);
    } catch (ScpException e) { last = e; }
      catch (RuntimeException e) { last = new ScpException(ErrorCode.CORRUPT_CHUNK, "malformed read response from replica " + r.nodeId() + ": " + e); }
      finally { if (!transferred) frame.close(); }              // releases on validation failure / exception before next replica
}
throw last != null ? last : new ScpException(ErrorCode.INTERNAL, "no readable replica");
```

`read(...)`:
- success: `Borrowed b = readFromReplicas(...)`; `eof` computed from `b.view().remaining()` (replacing `data.length`); `return new StrataFile.ReadResult(b.view(), eof, b.owner());`
- the two empty-result cases (`new byte[0]` today, [`ReaderImpl.java:79`](../../../strata-client/src/main/java/io/strata/client/ReaderImpl.java)) → `StrataFile.ReadResult.empty(eof)` (empty read-only buffer, `release == null`, no-op close).

`continue` inside the `try` still runs the `finally`, so every non-`return` path releases the borrowed frame; the single `return` is the only ownership transfer.

### 4.4 Consumers

All consumers of `ReadResult.data()` migrate to try-with-resources + `buffer()` and drain the `ByteBuffer` themselves where an array is needed:

- `strata-it`: `Workload.java:89-91`, `BinaryWorkload.java:58-62`, `ConsistencyVerifier.java:285-287`.
- `strata-server`: `StrataPerf.java:432-446` — also **must** close results (it is the throughput/GC harness where the win is measured).
- `strata-client` tests: `ReaderImplTest` (assertions at lines ~86, 153, 175, 180, 205-206), `StrataClientBehaviorTest:251-260`. These use **real Netty `ScpServer`** harnesses, so they exercise the borrow path against real pooled buffers.

### 4.5 Error handling & edge cases

- **Exactly-once release:** via the existing `Frame.closed` `AtomicBoolean` ([`Frame.java:145-156`](../../../strata-proto/src/main/java/io/strata/proto/Frame.java)); `ReadResult.close()` is idempotent.
- **Non-owning frames degrade gracefully:** `Frame.close()` is already a no-op when `owner == null`, so any heap/non-pooled frame works unchanged.
- **Failover:** a replica that fails validation has its borrowed frame closed in the `finally` before the next attempt — no accumulation.
- **Abandoned/timeout:** late borrowed responses hit the `holder == null` branch and are closed.
- **Connection drop:** does not invalidate already-handed-out pooled buffers.
- **Use-after-close:** documented as undefined; mitigated by the read-only view and the try-with-resources convention.

## 5. Risks

- **Pool residency:** holding the pooled `ByteBuf` until the caller closes increases direct-pool residency under slow consumers. Bounded by `MAX_PENDING_REQUESTS` ([`ScpClient.java:39-40`](../../../strata-proto/src/main/java/io/strata/proto/ScpClient.java)) and the 64 MiB max frame ([`FrameIO.MAX_FRAME_BYTES`](../../../strata-proto/src/main/java/io/strata/proto/FrameIO.java)). Documented tradeoff.
- **Caller leaks:** a caller that forgets `close()` leaks a pooled buffer. Mitigated by try-with-resources convention and leak tests (§6).
- **API break:** every read consumer changes. Accepted.

## 6. Testing strategy (TDD)

Leak-centric, using the real `ScpServer` harness the existing tests already use:

1. **Correctness:** sealed read and open (durable-clamped) read return the right bytes through `buffer()`; clamp yields the expected `remaining()`.
2. **Release on close:** a test-only hook on `ReadResult` (package-private, delegating to `Frame.ownerRefCnt()`) asserts the owner refCnt is non-zero before `close()` and `0` after.
3. **Failover releases:** a replica failing validation must release its borrowed frame before the next attempt — assert no net retained buffers.
4. **Abandoned/timeout releases:** a borrowed response delivered after the call was abandoned is released (`holder == null` branch).
5. **Empty/zero-length read:** returns a closeable no-op result with an empty buffer.
6. **Migrated assertions:** existing `ReaderImplTest` / `StrataClientBehaviorTest` cases adapted to the new API.
7. **CI belt-and-suspenders:** run the client/it suites with `-Dio.netty.leakDetection.level=paranoid` to catch any missed release at GC time.

## 7. File-by-file change list

- `strata-client/.../StrataFile.java` — `ReadResult` record → `AutoCloseable` class; `Reader.read` Javadoc contract.
- `strata-client/.../ReaderImpl.java` — `Borrowed` record; `readFromReplicas` returns borrowed frame + clamped view (closing the frame on validation failure); `read` builds the result and uses the empty-result factory for the zero-byte cases.
- `strata-proto/.../ScpClient.java` — `Pending` holder; `send(..., borrow)`; `channelRead0` borrow branch + `holder == null` close; `callFrameBorrowed`.
- `strata-proto/.../ManagedScpConnection.java` — `callFrameBorrowed`.
- `strata-it/.../{Workload,BinaryWorkload,ConsistencyVerifier}.java` — try-with-resources + `buffer()`.
- `strata-server/.../StrataPerf.java` — try-with-resources + `buffer()`/`length()`.
- `strata-client/src/test/.../{ReaderImplTest,StrataClientBehaviorTest}.java` — migrate assertions + add leak/lifecycle tests.
