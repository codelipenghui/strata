# Writer-origin per-record digest (promote the frame CRC) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Store the client's frame payload CRC as the per-record digest instead of recomputing it on the node.

**Architecture:** For an APPEND, one frame = one append = one `.j` record, and the frame's payload CRC (already computed by the client and verified by the node's frame decoder) is exactly the per-record content digest. Plumb that already-verified value from the frame into the append path and store it; delete the node-side `Crc.of(payload)` origination. No on-disk format change, no wire-message change, read path untouched, and the running whole/range aggregate (`crcAccumulate`) stays node-computed.

**Tech Stack:** Java 21, Maven multi-module (`strata-proto`, `strata-format`, `strata-node`), JUnit 5. CRC32C via `io.strata.common.Crc`. Build/test from the **worktree root** (the build runs there; do not edit the main checkout).

**Spec:** `docs/superpowers/specs/2026-06-30-writer-origin-digest-design.md`

## Global Constraints

- **No on-disk format change** — `.j` ledger, footer, header all unchanged.
- **No wire-message change** — `Messages.Append` is untouched; the digest rides the existing frame header field (`payloadCrc`, frame offset 20).
- **No-compat clean break** — this project never ships to prod; do not add fallbacks/aliases. Rename/replace in place.
- **Per-chunk `.j` is kept** — the shared digest log is out of scope (dropped).
- **Aggregate stays node-computed** — `crcAccumulate` (whole/range CRC for seal + SealVotes) is unchanged. Only the per-record digest origin moves to the client.
- **Reads unchanged** — zero-copy, no digest verification.
- **Run tests from the worktree** with `mvn -pl <module> [-am] test -Dsurefire.failIfNoSpecifiedTests=false` (no `cd`; the shell's cwd is already the worktree root).

---

### Task 1: Frame retains and exposes the decoded payload CRC

The node's frame decoder already recomputes CRC32C over the received payload and verifies it against the client's value, then **discards** it. This task retains that verified value on the `Frame` so the handler can read it. (`Frame.flags()` already records `FLAG_PAYLOAD_CRC` presence; we add the value.)

**Files:**
- Modify: `strata-proto/src/main/java/io/strata/proto/Frame.java`
- Modify: `strata-proto/src/main/java/io/strata/proto/NettyFrameCodec.java:113-114`
- Modify: `strata-proto/src/main/java/io/strata/proto/FrameIO.java:121`
- Test: `strata-proto/src/test/java/io/strata/proto/FramePayloadCrcTest.java` (create)

**Interfaces:**
- Produces: `int Frame.payloadCrc()` — the decoded, verified CRC32C of the frame payload (0 when the payload is empty / `FLAG_PAYLOAD_CRC` unset). Task 2's handler change consumes it.

- [ ] **Step 1: Write the failing test**

Create `strata-proto/src/test/java/io/strata/proto/FramePayloadCrcTest.java`:

```java
package io.strata.proto;

import io.strata.common.Crc;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FramePayloadCrcTest {

    private static Frame roundTrip(Frame f) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        FrameIO.write(new DataOutputStream(bytes), f);
        return FrameIO.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
    }

    @Test
    void decodedFrameExposesTheClientPayloadCrc() throws Exception {
        byte[] payload = "client-computed-payload".getBytes();
        Frame sent = Frame.request(Opcode.APPEND, new byte[] {1, 2, 3}, ByteBuffer.wrap(payload), 7L);

        Frame decoded = roundTrip(sent);

        assertEquals(Crc.of(ByteBuffer.wrap(payload)), decoded.payloadCrc());
    }

    @Test
    void emptyPayloadFrameReportsZeroCrc() throws Exception {
        Frame sent = Frame.request(Opcode.PING, new byte[] {1}, ByteBuffer.allocate(0), 1L);

        Frame decoded = roundTrip(sent);

        assertEquals(0, decoded.payloadCrc());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl strata-proto test -Dtest=FramePayloadCrcTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: **compile failure** — `cannot find symbol: method payloadCrc()` on `Frame`.

- [ ] **Step 3: Implement — retain `payloadCrc` on `Frame`**

In `Frame.java`:

1. Add the field next to the other final fields (after `private final long correlationId;`):
```java
    private final int payloadCrc;
```

2. Add `int payloadCrc` as the final parameter of the all-args private constructor and assign it:
```java
    private Frame(short opcode, short apiVersion, short flags, long correlationId,
                  ByteBuffer header, ByteBuffer payload, FilePayload filePayload, ByteBuf owner, int payloadCrc) {
        this.opcode = opcode;
        this.apiVersion = apiVersion;
        this.flags = flags;
        this.correlationId = correlationId;
        this.header = header;
        this.payload = payload;
        this.filePayload = filePayload;
        this.owner = owner;
        this.payloadCrc = payloadCrc;
    }
```

3. The public constructor delegates with `0` (outgoing frames compute their CRC at encode time, not here):
```java
    public Frame(short opcode, short apiVersion, short flags, long correlationId,
                 ByteBuffer header, ByteBuffer payload) {
        this(opcode, apiVersion, flags, correlationId, readOnlySlice(header), readOnlySlice(payload), null, null, 0);
    }
```

4. `fromOwnedBuffer` (Netty decode path) gains an `int payloadCrc` parameter and threads it through:
```java
    static Frame fromOwnedBuffer(short opcode, short apiVersion, short flags, long correlationId,
                                 ByteBuf owner, int headerIndex, int headerLen, int payloadIndex, int payloadLen,
                                 int payloadCrc) {
        ByteBuffer header = owner.nioBuffer(headerIndex, headerLen).asReadOnlyBuffer();
        ByteBuffer payload = owner.nioBuffer(payloadIndex, payloadLen).asReadOnlyBuffer();
        return new Frame(opcode, apiVersion, flags, correlationId, header, payload, null, owner, payloadCrc);
    }
```

5. Add a package-private factory for the heap decode path (`FrameIO.read`):
```java
    static Frame decoded(short opcode, short apiVersion, short flags, long correlationId,
                         ByteBuffer header, ByteBuffer payload, int payloadCrc) {
        return new Frame(opcode, apiVersion, flags, correlationId,
                readOnlySlice(header), readOnlySlice(payload), null, null, payloadCrc);
    }
```

6. `copyToHeap` preserves the CRC (call the all-args private constructor):
```java
    Frame copyToHeap() {
        if (filePayload != null) {
            throw new IllegalStateException("file payload cannot be copied to heap");
        }
        return new Frame(opcode, apiVersion, flags, correlationId,
                readOnlySlice(copy(header)), readOnlySlice(copy(payload)), null, null, payloadCrc);
    }
```

7. `fileResponse` delegates with `0` (its payload is a file region, no payload CRC):
```java
    public static Frame fileResponse(Frame req, byte[] header, FilePayload filePayload) {
        return new Frame(req.opcode(), req.apiVersion(), FLAG_RESPONSE, req.correlationId(),
                readOnlySlice(headerBuffer(header)), readOnlySlice(EMPTY.duplicate()), filePayload, null, 0);
    }
```

8. Add the accessor (next to `payloadLength()`):
```java
    /** CRC32C of the payload as computed by the sender and verified at decode; 0 when no payload CRC. */
    public int payloadCrc() {
        return payloadCrc;
    }
```

In `NettyFrameCodec.java`, pass the decoded `payloadCrc` (already read at line 104 and verified at 111) into `fromOwnedBuffer` (line 113-114):
```java
                out.add(Frame.fromOwnedBuffer(opcode, apiVersion, flags, correlationId,
                        frame, headerIndex, headerLen, payloadIndex, payloadLen, payloadCrc));
```

In `FrameIO.java`, build the decoded frame with the CRC (line 121):
```java
        return Frame.decoded(opcode, apiVersion, flags, correlationId, header, payload, payloadCrc);
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -pl strata-proto test -Dtest=FramePayloadCrcTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS (2 tests).

- [ ] **Step 5: Run the full strata-proto suite (no wire-format regression)**

Run: `mvn -pl strata-proto test`
Expected: BUILD SUCCESS, 0 failures (FrameGoldenTest / MessageGoldenCorpusTest still green — the wire bytes are unchanged; we only retained a field already on the wire).

- [ ] **Step 6: Commit**

```bash
git add strata-proto/src/main/java/io/strata/proto/Frame.java \
        strata-proto/src/main/java/io/strata/proto/NettyFrameCodec.java \
        strata-proto/src/main/java/io/strata/proto/FrameIO.java \
        strata-proto/src/test/java/io/strata/proto/FramePayloadCrcTest.java
git commit -m "feat(proto): retain and expose the decoded frame payload CRC"
```

---

### Task 2: Append path stores the client digest; drop node-side origination

Change `ChunkStore.appendAsync` to take the digest from the caller and store it verbatim, deleting the `Crc.of(payload)` it computes today; wire the APPEND handler to pass `req.payloadCrc()`. A 6-arg convenience overload (computes the CRC) is kept so existing test/sync callers are unaffected — only the production handler path becomes client-origin.

**Files:**
- Modify: `strata-format/src/main/java/io/strata/format/ChunkStore.java:757-821` (appendAsync) and `:823-830` (sync `append`)
- Modify: `strata-node/src/main/java/io/strata/node/DataNodeHandlers.java:39-40`
- Test: `strata-format/src/test/java/io/strata/format/ChunkStoreTest.java` (add one test)
- Test: `strata-node/src/test/java/io/strata/node/DataNodeWireTest.java` (add one test)

**Interfaces:**
- Consumes: `int Frame.payloadCrc()` from Task 1.
- Produces:
  - `CompletableFuture<AppendResult> ChunkStore.appendAsync(StrataNamespace ns, ChunkId id, int epoch, long baseOffset, long durableOffset, ByteBuffer payload, int payloadCrc)` — stores `payloadCrc` as the record digest; **does not** recompute it.
  - `CompletableFuture<AppendResult> ChunkStore.appendAsync(..., ByteBuffer payload)` — 6-arg convenience: computes `Crc.of(payload)` then delegates (for tests/simple callers).

- [ ] **Step 1: Write the failing test (format layer — verbatim storage)**

Add to `strata-format/src/test/java/io/strata/format/ChunkStoreTest.java` (it already has `newStore()`, `dir`, `id`, `TEST_NS`):

```java
    @Test
    void appendStoresTheSuppliedDigestVerbatim() throws Exception {
        try (ChunkStore store = newStore()) {
            store.open(TEST_NS, id, false, 1, 1718000000000L);
            // a digest that is deliberately NOT Crc.of(payload): proves the node stores the
            // caller's value rather than recomputing its own.
            int suppliedDigest = 0x1234_5678;
            store.appendAsync(TEST_NS, id, 1, 0, 0, ByteBuffer.wrap("payload".getBytes()), suppliedDigest).join();

            var entries = store.readLedger(TEST_NS, id, 0);
            assertEquals(1, entries.size());
            assertEquals(suppliedDigest, entries.get(0).payloadCrc());
        }
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `mvn -pl strata-format test -Dtest='ChunkStoreTest#appendStoresTheSuppliedDigestVerbatim' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: **compile failure** — no 7-argument `appendAsync` (the call has an extra `int`).

- [ ] **Step 3: Implement — appendAsync takes the digest**

In `ChunkStore.java`, change the `appendAsync` signature to accept `int payloadCrc` and **delete** the line that computes it. Replace:

```java
    public java.util.concurrent.CompletableFuture<AppendResult> appendAsync(
            StrataNamespace ns, ChunkId id, int epoch, long baseOffset, long durableOffset, ByteBuffer payload) throws IOException {
        Handle h = lookup(ns, id);
        // CRC the payload before taking the chunk monitor: the pass is state-independent and
        // would otherwise serialize behind every other append to this chunk
        // Crc.of(ByteBuffer) duplicates the buffer internally, so it leaves payload's position
        // and limit intact — no need to allocate an outer duplicate here.
        long t0 = System.nanoTime();
        int payloadCrc = payload.hasRemaining() ? Crc.of(payload) : 0;
        long tPayloadCrc = System.nanoTime();
```

with:

```java
    public java.util.concurrent.CompletableFuture<AppendResult> appendAsync(
            StrataNamespace ns, ChunkId id, int epoch, long baseOffset, long durableOffset,
            ByteBuffer payload, int payloadCrc) throws IOException {
        Handle h = lookup(ns, id);
        // payloadCrc is the writer's CRC32C over this payload, already verified by the frame decoder;
        // the node stores it as the per-record digest and never originates its own.
        long t0 = System.nanoTime();
        long tPayloadCrc = t0;
```

(The rest of the method is unchanged — `h.ledger.append(new ChunkFormats.LedgerEntry(newEnd, payloadCrc, epoch))` now stores the supplied value; `crcAccumulate(payload)` for the running aggregate is untouched.)

Add the 6-arg convenience overload immediately **above** the new `appendAsync` (so existing async callers and the sync `append` keep working):

```java
    /** Convenience for tests/simple callers without a precomputed digest: computes it then delegates. */
    public java.util.concurrent.CompletableFuture<AppendResult> appendAsync(
            StrataNamespace ns, ChunkId id, int epoch, long baseOffset, long durableOffset, ByteBuffer payload) throws IOException {
        return appendAsync(ns, id, epoch, baseOffset, durableOffset, payload,
                payload.hasRemaining() ? Crc.of(payload) : 0);
    }
```

The synchronous `append(...)` is unchanged — it already calls the 6-arg `appendAsync`.

- [ ] **Step 4: Run the format test + full strata-format suite**

Run: `mvn -pl strata-format test -Dtest='ChunkStoreTest#appendStoresTheSuppliedDigestVerbatim' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS.

Run: `mvn -pl strata-format test`
Expected: BUILD SUCCESS (the 6-arg convenience keeps every existing append/recovery test green; recovery still verifies the stored digest, whose value is identical for the test callers).

- [ ] **Step 5: Write the failing test (wire layer — handler passes the frame CRC)**

Add to `strata-node/src/test/java/io/strata/node/DataNodeWireTest.java` (model on `fullChunkLifecycleOverTheWire`):

```java
    @Test
    void appendStoresTheClientFramePayloadCrc() throws Exception {
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir));
             ScpClient client = new ScpClient("127.0.0.1", node.port(), ScpClient.KIND_BROKER, "test")) {
            client.call(Opcode.OPEN_CHUNK, new Messages.OpenChunk(id, 1, false,
                    1 << 20, 1718000000000L, TEST_NS).encode(), null, 5000);

            byte[] payload = "client-computed".getBytes();
            client.call(Opcode.APPEND, new Messages.Append(id, 1, 0, 0, TEST_NS).encode(),
                    ByteBuffer.wrap(payload), 5000);

            ByteBuffer lh = client.call(Opcode.READ_LEDGER,
                    new Messages.ReadLedger(id, 0, TEST_NS).encode(), null, 5000);
            var ledger = Messages.ReadLedgerResp.decode(lh);
            assertEquals(1, ledger.entries().size());
            assertEquals(io.strata.common.Crc.of(ByteBuffer.wrap(payload)),
                    ledger.entries().get(0).payloadCrc());
        }
    }
```

- [ ] **Step 6: Run it to verify it fails (RED), then wire the handler**

Run: `mvn -pl strata-node -am test -Dtest='DataNodeWireTest#appendStoresTheClientFramePayloadCrc' -Dsurefire.failIfNoSpecifiedTests=false`
Expected RED: the test compiles and runs, but the stored digest is currently produced by the **6-arg** (server-recompute) handler path. Confirm it is GREEN-as-written only after the handler change below — if it already passes, that is the value-coincidence noted in the spec (frame CRC == `Crc.of(received payload)`); proceed with the handler change regardless so the production path is client-origin and skips the redundant CRC pass.

> Note (honest TDD caveat): because the frame decoder guarantees `frame.payloadCrc() == Crc.of(received payload)`, this wire assertion holds both before and after the handler change — it is an end-to-end regression guard, not an isolating RED. The behavioral RED lives in Step 1 (the format layer rejects a server-recompute by storing a digest that is *not* `Crc.of(payload)`).

In `DataNodeHandlers.java`, change the APPEND dispatch (line 39-40) to pass the frame's CRC:

```java
            return store.appendAsync(m.namespace(), m.chunkId(), m.writeEpoch(), m.baseOffset(), m.durableOffset(),
                            req.payloadSlice(), req.payloadCrc())
```

- [ ] **Step 7: Run the wire test + full strata-node suite**

Run: `mvn -pl strata-node -am test -Dtest='DataNodeWireTest#appendStoresTheClientFramePayloadCrc' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS.

Run: `mvn -pl strata-node -am test`
Expected: BUILD SUCCESS, 0 failures.

- [ ] **Step 8: Commit**

```bash
git add strata-format/src/main/java/io/strata/format/ChunkStore.java \
        strata-format/src/test/java/io/strata/format/ChunkStoreTest.java \
        strata-node/src/main/java/io/strata/node/DataNodeHandlers.java \
        strata-node/src/test/java/io/strata/node/DataNodeWireTest.java
git commit -m "feat(format,node): store the client-origin frame CRC as the per-record digest"
```

---

## Final verification

- [ ] **Run the three affected module suites together**

Run: `mvn -pl strata-proto,strata-format,strata-node -am test`
Expected: BUILD SUCCESS across all three (Task 1 + Task 2).

- [ ] **(Optional) End-to-end perf smoke** — rebuild the image and run the default perf per the project's deploy flow; expect `errors=0` and unchanged throughput (the change removes one CRC pass on the write path).

---

## Self-Review

**Spec coverage:**
- "Promote the frame CRC to the per-record digest; stop `Crc.of(payload)` at ingest" → Task 2, Steps 3 + 6.
- "Frame exposes the verified payload CRC" → Task 1.
- "Verify-on-ingest is the frame decoder, free" → unchanged decoder (Task 1 retains, does not alter, the `checkPayloadCrc` verification); no new knob added. ✓
- "No `.j`/format/wire-message change" → Global Constraints; Task 1 only retains an already-on-wire field; Task 2 touches no format. ✓
- "Aggregate stays node-computed" → Task 2 Step 3 leaves `crcAccumulate` untouched. ✓
- "Empty payload → digest 0" → covered by the 6-arg convenience (`hasRemaining() ? ... : 0`) and `Frame.payloadCrc()==0` for empty frames (Task 1 test 2); the production 7-arg path stores `req.payloadCrc()` which is 0 for an empty append. ✓
- "Reads unchanged" → no read-path task. ✓

**Placeholder scan:** none — every step has exact code and exact commands.

**Type consistency:** `Frame.payloadCrc()` (int) defined in Task 1 and consumed in Task 2 Step 6; `appendAsync(..., int payloadCrc)` defined in Task 2 Step 3 and consumed by the handler in Step 6; `Messages.LedgerEntry.payloadCrc()` used in both module tests matches the wire `LedgerEntry` record. Consistent.

**Out-of-scope (explicitly not in this plan):** shared digest log, aggregate-via-CRC-combine (option ii), any read-time digest verification.
