# Writer-origin per-record digest (promote the frame CRC)

**Status:** Proposed (design)
**Date:** 2026-06-30
**Scope:** `strata-proto` (Frame), `strata-node` (DataNodeHandlers), `strata-format` (ChunkStore). No on-disk format change, no wire message change.
**Relationship to prior ADRs:** This narrows `strata-durability-v2-design.md`. It implements only the *writer-origin per-record digest* half and **drops the shared per-volume digest log** — the per-chunk `.j` ledger is kept as-is (it only ever exists for the bounded OPEN working set, so the shared-log merge isn't worth its WAL+compaction complexity). It also supersedes that ADR's "reuse the producer batch CRC" idea (R3): the digest is the Strata client's frame CRC, computed regardless of payload contents.

---

## Context

Today the storage node **originates** the per-record content digest: on every append it computes `Crc.of(payload)` ([ChunkStore.java:728](../../../strata-format/src/main/java/io/strata/format/ChunkStore.java)) and stores it in the per-chunk integrity ledger (`.j`). Because the node computes the CRC over whatever bytes arrived, any corruption between the client and that computation is sealed into a "valid" stored digest — there is no end-to-end guarantee that the stored digest reflects the bytes the *client* produced.

Separately, the wire already carries a **frame payload CRC** (`FLAG_PAYLOAD_CRC`): the Strata client computes CRC32C over each frame's payload ([NettyFrameCodec.java:33](../../../strata-proto/src/main/java/io/strata/proto/NettyFrameCodec.java)), and the receiving node's frame decoder recomputes and verifies it ([NettyFrameCodec.java:111](../../../strata-proto/src/main/java/io/strata/proto/NettyFrameCodec.java)), rejecting a corrupted frame. Today that CRC is **verified and then discarded**.

### Key insight

For an APPEND, **one Append = one frame = one `.j` record**, and the frame payload *is* the append payload. So the frame payload CRC and the per-record content digest are **the same value** — CRC32C over the same bytes, both computed by the client. The node is computing CRC32C over the append payload **twice**: once in the frame decoder (to verify) and again at `ChunkStore.java:728` (to originate the stored digest).

This design **promotes the frame CRC to the durable per-record digest**: stop computing the digest on the node; store the client's frame CRC instead.

---

## Design

The per-record digest stored in `.j` becomes the client's frame payload CRC, plumbed from the frame layer to the storage layer. Nothing else about the ledger, the on-disk format, or the wire messages changes.

What this buys, all at once:

1. **Writer-origin / end-to-end (per record).** The stored digest is the value the client computed over its original bytes — not a node re-computation over arrived bytes.
2. **Verify-on-ingest, for free.** The frame decoder *already* recomputes CRC32C over the received payload and compares it to the client's value, rejecting a mismatch. That is verify-on-ingest, already running today on every non-empty append — so the wire-corruption fail-fast is retained at no new cost, and the earlier "verify-on-ingest default ON/OFF to trade CPU" question is moot (the CPU was always being spent).
3. **One fewer CPU pass.** Removing the `Crc.of(payload)` at `:728` eliminates the pass that duplicates the decoder's verification. Steady-state node CRC passes over the payload drop from two (decoder verify + per-record origin) to one (decoder, reused as the digest) — plus the running aggregate (below).

### What stays server-side (this iteration): the running aggregate

The node still computes the running whole-chunk + per-4 MiB-range CRC via `crcAccumulate` ([ChunkStore.java:764](../../../strata-format/src/main/java/io/strata/format/ChunkStore.java)), which is snapshotted into the sealed footer and underpins cross-replica seal-divergence detection (SealVotes — each replica independently CRCs its own bytes and the client votes on `(length, chunkCrc)`). Keeping it server-computed means:
- SealVotes keeps working unchanged (replicas compute independently over their own bytes).
- The node still makes **one** byte pass for the aggregate.

Deriving the aggregate from the (now client-origin) per-record CRCs via CRC-combine — so the node touches no bytes for CRC at all — is **explicitly out of scope** here. It is more complex (a GF(2) `crc32_combine`, the 4 MiB-range-boundary problem where arbitrary-length record CRCs don't combine to range-aligned CRCs, and SealVotes degeneration since combined chunk CRCs would be identical across replicas). It is a clean follow-up if the aggregate byte pass ever needs to go.

### The per-record digest is the fundamental one

The per-record digest is kept (not redundant with the aggregate) because it is the **only** durable structure that lets crash recovery find the exact last-intact-record boundary of an OPEN chunk (record-granular torn-tail detection). The aggregate cannot do this: during the open window it is only in memory (lost on crash — the `.j` is the sole durable digest), and even a durable whole-chunk CRC carries no record boundaries while range CRCs are 4 MiB-granular and seal-only. (The reverse holds: the aggregate is derivable from the per-record CRCs via combine, which is why a future iteration could drop the separate aggregate pass — option (ii) — but never the per-record CRC.)

---

## Scope

**In:**
- Plumb the frame payload CRC into the append path and store it as the per-record digest.
- Remove the node-side `Crc.of(payload)` digest origination.

**Out (explicitly):**
- Shared per-volume digest log / merging `.j` files (dropped; per-chunk `.j` kept).
- Deriving the aggregate (whole/range) CRC from per-record CRCs via combine — option (ii), deferred.
- Any `Append` message field, any `.j`/footer/header format change.
- Read-path changes (reads stay zero-copy with no digest verification).
- A separate verify-on-ingest knob (the frame decoder's verification is it).

---

## Load-bearing assumptions

1. **Reads do not verify the per-record digest.** The client read path is zero-copy (sendfile) and performs no digest verification; end-to-end data integrity for the Kafka tenant comes from the *embedded* Kafka batch CRC (consumer-verified), with scrub + replication for at-rest. This is what lets the per-record digest be a recovery-only structure. **If Strata ever adds node-side read-time digest verification, revisit this design** — the digest would then need per-read access semantics, not just durable storage.
2. **One APPEND = one frame = one `.j` record**, so the frame payload CRC maps 1:1 to the per-record digest. (True today: `AppenderImpl` sends one APPEND frame per append.)
3. **Non-empty APPEND frames carry `FLAG_PAYLOAD_CRC`.** True today (the encoder sets it whenever `payloadLen > 0`). The node now *requires* it for non-empty appends (it is the digest); its absence on a non-empty append is a protocol violation, not a fall-back-to-server-compute.

---

## Implementation touchpoints

1. **`strata-proto` / `Frame`:** retain the decoded `payloadCrc` on the `Frame` and expose an accessor (today the decoder verifies it and drops it). A flag/sentinel distinguishes "no payload CRC" (empty payload) from "CRC present".
2. **`strata-node` / `DataNodeHandlers` (APPEND case):** pass the request frame's `payloadCrc` into `store.appendAsync(...)`.
3. **`strata-format` / `ChunkStore.appendAsync`:** add a `payloadCrc` parameter; store it directly in the `LedgerEntry`; **delete the `Crc.of(payload)` origination** at `:728`. Empty payload (no frame CRC) → digest `0` (matches today's `payload.hasRemaining() ? ... : 0`). The signature change ripples to the synchronous `append` wrapper and to tests, which now pass a digest (or compute one in a test helper).
4. **Aggregate:** `crcAccumulate` is unchanged.

---

## Edge cases

- **Empty append** (`payloadLen == 0`, no `FLAG_PAYLOAD_CRC`): digest `0`, as today.
- **Frame CRC mismatch:** the frame decoder already rejects the frame before the handler runs — the node never stores a digest for a wire-corrupted append. No new handling needed.
- **Recovery** (`recoverOne` OPEN replay): unchanged — it reads `.j` entries and verifies the data tail against the stored `payloadCrc`. The stored value is now client-origin, but the verification mechanism is identical.
- **Seal / SealVotes:** unchanged (aggregate stays server-computed).
- **Frame CRC absent on a non-empty append:** reject (`PRECONDITION_FAILED`/protocol error) — should never happen with the real client; guards against a malformed peer.

---

## Testing (TDD)

Write failing tests first, at the layer of each change:
- **ChunkStore (format):** an append stores the *supplied* digest in `.j` verbatim (not a re-computation); recovery of an open chunk truncates the torn tail using the supplied digests; an empty append stores digest `0`.
- **DataNodeHandlers / wire (strata-node, `DataNodeWireTest`):** an APPEND over the real frame codec results in the frame's payload CRC being the stored digest; a frame whose payload is corrupted in flight is rejected by the decoder (no store).
- **Frame (strata-proto):** the decoded frame exposes the verified `payloadCrc`; empty payload reports "absent".
- **Regression:** the full `strata-format` + `strata-node` suites stay green (append/seal/recovery/repair paths).

---

## Consequences

**Easier / better**
- Per-record digest is client-origin and end-to-end; verify-on-ingest retained for free; one redundant CRC pass removed.
- No format or wire-message change → small blast radius, trivially "no-compat" (clean break, never ships to prod).

**Harder / to revisit**
- The design is pinned to assumption (1) — reads don't verify the digest. A future read-verification feature, or a future move to derive the aggregate via combine (option ii), each reopen parts of this.
- The node still makes one byte pass for the aggregate; the "node touches no bytes for CRC" end state requires the deferred option (ii).
