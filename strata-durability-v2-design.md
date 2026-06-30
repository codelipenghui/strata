# ADR: Storage durability v2 — writer-origin record digest + shared per-volume digest log

**Status:** Proposed
**Date:** 2026-06-29
**Deciders:** storage-engine owners
**Scope:** `strata-format` (ChunkStore, ChunkFormats, IntegrityLedger), `strata-proto` (Append/Read messages), `strata-node` (DataNodeHandlers, ControlLoop), recovery & repair paths. No broker changes beyond supplying the digest.

---

## Context

The Strata file service is, by design, a **single-writer, append-only log of records** (product-definition.md: "in substance a general primitive for single-writer log workloads"; tech-design: `LOG` file = a Kafka segment, stored as an **opaque byte stream**). The storage layer is deliberately **Kafka-ignorant** — "the layer stays Kafka-ignorant inside, the product speaks only Kafka outside."

Today's per-record integrity (CRC32C) has three properties we want to change, and several we must preserve.

### Current on-disk footprint — 3 files per chunk

`ChunkStore.Handle` opens, per chunk (`ChunkStore.java:478-482`, `:498-499`):

| File | Content | Write pattern |
|------|---------|---------------|
| `<chunk>.chunk` | header(4096) + data region + footer + trailer | append-only |
| `<chunk>.meta` | 512B sidecar: writeEpoch, fenceEpoch, lastKnownDO, state | in-place atomic sector rewrite |
| `<chunk>.j` | integrity ledger: one 24B entry per append `(endOffset, payloadCrc, writeEpoch, entryCrc)` | append-only |

Lifecycle: at **seal** the `.j` ledger is deleted (`ChunkStore.java:329`) and its CRCs fold into the footer `SECTION_CRC_RANGES` (`:1030`). So **open chunk = 3 files, sealed chunk = 2 files**.

### Forces at play

1. **Server originates the content CRC.** The node computes `Crc.of(payload)` at ingest (`ChunkStore.java:728`) over the bytes that arrived, then stores it (`:758`). Because the stored CRC always self-agrees with the stored bytes, corruption in the node-memory window (frame-decode → disk-write) gets laundered into a "valid" CRC. This is not true end-to-end integrity.

2. **`.j` doubles the write fan-out of the open working set.** For N actively-written chunks there are **2N concurrently-written files** (data + ledger). On `fsyncOnAck` files the group committer forces **both** data and ledger before ack (`ChunkStore.java:92`) → 2 fsyncs/commit. (Default `WritePolicy.fsyncOnAck=false` acks on quorum replication, so the per-record pain there is write-syscall + dirty-page + background-writeback fan-out rather than ack fsync — but it is still 2× the file count.) This hits the single-disk fsync/IO ceiling directly.

3. **`.meta` multiplies inodes at scale.** Every chunk (all ~100M, including cold sealed ones) carries a `.meta`. (Addressed by a companion lever, below; not the focus of this ADR.)

### Invariants we must NOT break

- **Byte-offset O(1) seek.** Reads address by byte offset; `filePos = DATA_START + offset` is pure arithmetic → sendfile (`ChunkStore.java:888`). The data region is **raw logical bytes**; logical offset X lives at file offset 4096+X. There is **no offset index** today.
- **Whole-file repair for sealed chunks.** `importSealed` copies a sealed `.chunk` byte-for-byte and re-verifies `dataCrc` (`ChunkStore.java:1260`); FETCH sendfiles the whole file. This is a headline feature ("re-replicated by the whole pool in parallel").
- **Kafka-ignorant storage layer.** The layer must not parse Kafka's RecordBatch format, compression, or EOS semantics.

---

## Decision

Adopt the **BookKeeper-shaped** model, in two composable changes:

### 1. Writer-origin per-record digest (server stores + re-verifies, never originates)

- The **writer** computes the per-record CRC32C and supplies it on the wire in `Append`. (For the Kafka tenant, the writer is the broker, the record = one batch, and the supplied digest = the **producer's batch CRC** — already client-origin and end-to-end. The storage layer stays format-blind: it sees only `(byte-range, digest)`.)
- The node **stores the writer's value** (never computes its own competing value) and **re-verifies** it during scrub, repair-import, and crash recovery.
- Optional cheap fail-fast knob: verify-on-ingest (node recomputes once, compares to the writer value, rejects on mismatch) — but **always stores the writer's value** (required for true end-to-end). Default OFF, opt-in to enable (see R2).
- **Digest unit = the append.** Strata stores one digest per append and stays batch-agnostic. For the Kafka tenant `append = one batch` lets the broker pass the producer's batch CRC verbatim (zero recompute); any non-aligned append (oversized-batch split, multi-batch bundle) carries a broker-computed CRC over the append bytes. Append boundaries are invisible to the reader. See R3.

### 2. Shared per-volume digest log (replaces the per-chunk `.j`)

- All open chunks on a volume append their digest entries to **one shared, append-only digest log**, keyed by `(chunkId, endOffset)` — the bookie **journal** model, but **digest-only** (24-ish bytes/record, no data double-write).
- Group-commit fsync of the shared log **amortizes the digest barrier across all chunks**: one fsync covers many chunks' records.
- **Data stays in per-chunk `.chunk` files** (unchanged write path, no double-write, whole-file repair preserved).
- At **seal**, the chunk's digests fold into the footer `SECTION_CRC_RANGES` exactly as today; the chunk becomes **self-contained**, and its entries are reclaimable from the shared log (compaction drops sealed chunks' entries). The shared log therefore only ever holds the **open working set** (thousands), never the cold majority.

### The server-CRC three-way split (made explicit)

| Role | Disposition |
|------|-------------|
| **originate** (compute the authoritative value at ingest, `ChunkStore.java:728`) | **removed** → writer supplies it |
| **re-verify** (scrub, repair-import, recovery torn-tail — node-local, no reader present) | **kept** — the layer must self-heal without a reader; needs the CRC32C function + record boundaries |
| **structural** (header CRC, trailer, sidecar CRC, per-entry `entryCrc`) | **kept** — protects the layer's own on-disk structures; never the writer's concern |

"Remove server CRC" means remove **origination**, not the re-verify capability or the structural CRCs.

---

## Options Considered

### Option A: Status quo — per-chunk `.j`, server-origin CRC

| Dimension | Assessment |
|-----------|------------|
| Complexity | Low (exists) |
| End-to-end integrity | **No** — server launders node-memory-window corruption |
| Open-chunk write fan-out | **2N files**, 2 fsyncs/commit on fsyncOnAck |
| Seek / repair | O(1) seek ✓, whole-file repair ✓ |

**Cons:** no true end-to-end; `.j` doubles open write fan-out. This is the baseline being replaced.

### Option B: Writer-origin digest + shared per-volume digest log — **RECOMMENDED**

| Dimension | Assessment |
|-----------|------------|
| Complexity | Medium-High (adds a WAL-shaped shared log + replay) |
| End-to-end integrity | **Yes** — stored digest is the writer's; consumer/reader verifies; reused Kafka batch CRC for the Kafka tenant |
| Open-chunk write fan-out | **N+1 files**; digest fsync group-committed across all chunks |
| Seek / repair | O(1) seek ✓ (data region stays pure), sealed whole-file repair ✓ (digest in footer, self-contained) |
| Kafka-ignorance | ✓ preserved (format-blind: byte-range + digest) |

**Pros:** writer-origin end-to-end; 2N→N+1 write fan-out; group-commit fsync amortization; better write locality (one sequential digest stream); seek + sealed repair untouched; composes with the digest-reuse of the Kafka batch CRC.

**Cons:** adds a journal/WAL to the engine; recovery **couples** open chunks (replay the shared log to rebuild their tails); a corrupt shared log has a multi-chunk blast radius; cross-file durability ordering (data-before-digest) must hold with a batched digest fsync; shared-log compaction needed to drop sealed entries.

### Option C: Inline digest in the data region (Kafka segment byte-identical on disk)

| Dimension | Assessment |
|-----------|------------|
| Complexity | High (data plane becomes record/offset-addressed) |
| Seek | **Breaks O(1) byte-offset seek** — needs an offset index that does not exist today; reads become record-aligned |

**Rejected.** Inlining `[len|crc|bytes]` makes logical offset ≠ physical offset; the headline byte-offset arithmetic invariant dies. Only worth it under a full commitment to record/offset addressing. The client-reassembly argument solves *presentation* of pure bytes, not *addressing*.

### Option D: Full BookKeeper journal (data **and** digest through the WAL)

**Rejected.** Strata's data is the bulk and is throughput-bound at the SSD ceiling; double-writing every data byte through a journal roughly halves write throughput. Journaling **digest only** (Option B) captures the fsync-amortization benefit at ~24 B/record.

### Option E: Strata natively recognizes the Kafka segment format

**Rejected.** Violates the deliberate Kafka-ignorant discipline (product-definition.md:73), couples the storage layer to Kafka's format-version/compression/EOS surface (the compatibility long tail the architecture quarantines to the forked broker), and forfeits the banked second-tenant option. The same end-to-end-CRC benefit is obtained by **caller-supplied digest** (Option B) without parsing the format.

---

## Trade-off Analysis

- **Integrity vs. simplicity:** Option B buys true end-to-end integrity and a 2N→N+1 write-fan-out reduction at the cost of a new WAL-shaped component and coupled recovery. The coupling blast radius is bounded by per-entry CRC + chunkId keying (replay skips bad entries; each affected chunk recovers to its last good record) and by the fact that the shared log holds only the open working set.
- **Where the digest lives is dictated by the seek invariant:** out-of-band is mandatory (open → shared log; sealed → footer). Inline (Option C) is the only thing that would break seek, and it is not required to get writer-origin digests.
- **Repair simplicity is preserved by keeping sealed chunks self-contained:** the shared log is an *open-window* structure only; sealed repair stays a whole-file byte copy + `dataCrc` re-verify.
- **fsync amortization is the core perf win:** today every open chunk is its own fsync target; the shared log turns N digest barriers into one group-committed barrier per window.

---

## Consequences

**Easier**
- True end-to-end integrity; the Kafka batch CRC flows through as the stored digest (no second CRC for the Kafka tenant).
- Lower open-chunk write fan-out and amortized digest fsync → headroom against the single-disk ceiling.
- `ChunkStore.appendAsync` no longer computes CRC over the payload — one less CPU pass and one less reason to touch the bytes.

**Harder**
- Recovery gains a shared-log replay stage and a cross-file (data ↔ shared-log) ordering contract.
- A new compaction/retention concern for the shared log (drop sealed chunks' entries).
- The shared-log fsync is a potential serialization point under high open-chunk concurrency.

**To revisit (genuinely open)**
- Shared-log compaction cadence (when to drop sealed chunks' entries) and its interaction with retention.
- Read-side digest delivery for **non-Kafka** tenants (a Kafka consumer already verifies the embedded batch CRC; a generic reader needs the digest delivered — extend `ReadResp` / reuse `READ_LEDGER`).
- Striped in-memory log buffer — only if append-path CPU lock contention is measured (see R1).

The three review questions (shared-log sharding, verify-on-ingest default, append=batch contract) are resolved below.

## Resolved Open Questions

Raised during review; resolved with reasoning. Revisit only if the stated assumption breaks.

### R1. One shared log per volume — do NOT shard within a volume
**Resolved: one log per volume; scale parallelism by volumes/devices, not by logs-per-device.**

Group commit's benefit is that many chunks' digests ride one fsync — fewer logs = better amortization. On a single device, fsync is a hardware barrier: k logs/device serialize k× barriers on the same device with no real parallelism, *eroding* the amortization the shared log exists to capture. The real parallel axis is physical devices (one log per volume). Per-volume sharding is only ever justified by **append-buffer CPU lock contention** at very high core / open-chunk counts — and even then the fix is a striped in-memory buffer feeding **one** fsync, not k separately-fsync'd logs. (Matches BookKeeper's "one journal per physical device" guidance.)

Cost accepted: a single log means a device fsync stall correlates ack latency across all open chunks on that volume — but the device serializes barriers regardless, so sharding would not decorrelate it; the remedy is faster / more devices.

### R2. verify-on-ingest — default OFF, opt-in to enable
**Resolved: default OFF (bank the CPU saving); opt-in knob to enable per-WritePolicy/volume.**

What it would catch that the rest of the chain does not: the frame CRC (`FLAG_PAYLOAD_CRC`) covers the wire and scrub covers at-rest, but neither covers (a) the **node-memory window** (frame-decode → store: bad RAM, buffer bugs) nor (b) a **writer that ships a digest not matching its bytes**.

**Why OFF is acceptable as the default**
- ON costs the *same* CPU as today — the node already CRCs every append at ingest (`ChunkStore.java:728`); making the digest writer-origin only **banks** that saving when verify is OFF. So OFF is where the writer-origin CPU win actually lands (~a few % of a core at the write ceiling, on a disk-bound node).
- Failure mode (a) is single-replica and **self-heals**: the other replicas stored good bytes (each passed its own frame CRC); scrub catches the bytes≠digest mismatch and re-repairs from a clean replica, and a Kafka consumer's embedded batch CRC rejects any bad read and refetches. OFF only **defers** detection here; it does not lose data.

**Residual risk OFF carries (know it before relying on the default)**
- Failure mode (b) is **not** merely deferred. A wrong digest from the writer reaches **all replicas identically** → every replica stores bytes≠digest → scrub flags a mismatch everywhere with **no clean source**: a stuck/ambiguous repair state that interacts badly with the known unfloored-destructive-repair hazard. ON would reject it at the ingest door; OFF lets it poison the quorum.
- The highest-risk source of (b) is the broker **computing** a digest for a non-aligned append (oversized-batch split / multi-batch bundle, R3) — new code. When the broker merely forwards a validated producer batch CRC, the risk is low.

**Mitigations while the default is OFF**
1. **Enable it in test/CI and for high-assurance (regulated / air-gapped) deployments** — fail-fast surfaces broker digest bugs at the door instead of as downstream scrub/repair churn.
2. **Recommended refinement — conditional verify by digest source:** skip when the writer forwards a producer batch CRC (trusted, append=batch), enforce when the writer **computed** the digest (non-aligned appends). Spends CPU only on the new-code path and neutralizes most of (b) even with the global default OFF.
3. Ensure repair **floors destructive action on an all-replica mismatch** (cross-ref the destructive-repair hazard) so a (b) event degrades to "stuck, needs operator" rather than data deletion.

### R3. Digest unit = the append, not the batch — there is no "sub-batch" problem
**Resolved: Strata's digest unit is the append, always; `append = batch` is an optimization, not a contract.**

Strata stays format-blind: one digest per append, used identically at store / scrub / repair / recovery — it never aligns to or knows about Kafka batches.
- When an append is exactly one batch, the broker passes the **producer's batch CRC** as the append digest (zero broker recompute).
- When it is not — a batch larger than the append size limit (`MAX_REQUEST_BYTES` / `maxFrameBytes`) must be split, or the broker bundles several batches — the broker computes one CRC32C over the append bytes and supplies that. The reuse optimization simply does not apply to that append.

End-to-end reader integrity is **unaffected and independent**: a Kafka consumer reassembles whole batches from the **contiguous** on-disk byte stream and verifies the **embedded per-batch CRC** — append boundaries are invisible to the reader, so a batch split across appends is harmless. Strata's per-append digest and Kafka's per-batch CRC are two layers at potentially different granularities (storage self-integrity vs. end-to-end), by design.

Broker guidance: align appends to whole-batch boundaries when convenient (the common case — it receives whole batches) to harvest the reuse optimization; not required for correctness.

---

## Action Items

1. [ ] **Wire:** add `payloadCrc` (u32) to `Append` (`Messages.java:126`) = the digest over **this append's** bytes (broker passes the producer batch CRC when the append is one batch, else computes it — see R3); decide fixed-field (41→45 B) vs. tagged-field. No back-compat needed (never ships to prod).
2. [ ] **Ingest:** in `appendAsync` (`ChunkStore.java:728`) stop originating; take the writer's `payloadCrc`. Add verify-on-ingest knob, **default OFF**, opt-in to enable per-WritePolicy/volume; consider conditional verify of writer-**computed** digests (see R2).
3. [ ] **Shared digest log:** new component — **one** append-only log **per volume** (not sharded within a volume — see R1), entry `[chunkId | endOffset u64 | payloadCrc u32 | writeEpoch i32 | entryCrc u32]`, group-commit fsync; replace `IntegrityLedger`/`.j` usage on the open path.
4. [ ] **Durability ordering:** preserve "data durable before the record's digest entry is acked" across data file ↔ shared log; integrate with the existing `GroupCommitter`.
5. [ ] **Recovery:** replay the shared log per volume, group by chunkId, validate each open chunk's data tail against writer digests (torn-tail truncation); per-entry CRC localizes corruption.
6. [ ] **Seal:** keep the fold into footer `SECTION_CRC_RANGES` (`:1030`); reclaim the chunk's entries from the shared log (compaction).
7. [ ] **Repair:** confirm sealed-chunk whole-file FETCH/import (`:1260`) is unchanged; open-chunk repair stays on the recovery/seal path.
8. [ ] **Read-side (separate, optional):** deliver the covering digests on read (extend `ReadResp` / reuse `READ_LEDGER`) so a generic (non-Kafka) reader can verify; the Kafka consumer already verifies the embedded batch CRC, so this is only for non-Kafka tenants.
9. [ ] **Companion `.meta` lever (separate ADR):** eliminate sealed `.meta` (state recoverable from trailer) and fold open mutable state into a reserved 512B header sector → sealed = 1 file, open = 2 files.

---

## Appendix: file footprint, before → after

| | `.chunk` | mutable state | digest | files (open / sealed) |
|---|---|---|---|---|
| **today** | per-chunk | `.meta` per-chunk | `.j` per-chunk (open) → footer (sealed) | 3 / 2 |
| **v2 (this ADR)** | per-chunk | `.meta` per-chunk | **shared log** (open) → footer (sealed) | ~2 + share / 2 |
| **v2 + `.meta` lever** | per-chunk | header sector / trailer | shared log (open) → footer (sealed) | ~1 + share / **1** |

The `.chunk` is irreducible (it is the data). The digest write-fan-out collapses from per-chunk to one shared log; the inode multiplier collapses by removing `.meta`.
