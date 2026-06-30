# ADR: Chunk file footprint — eliminate the `.meta` sidecar

**Status:** Proposed
**Date:** 2026-06-29
**Deciders:** storage-engine owners
**Companion to:** `strata-durability-v2-design.md` (which removes the per-chunk `.j` via a shared per-volume digest log). This ADR removes the remaining per-chunk dedicated sidecar, `.meta`.

---

## Context

After durability-v2 moves the digest off the per-chunk `.j` into a shared log, the per-chunk footprint is: open = `.chunk` + `.meta` (+ a share of the shared digest log), sealed = `.chunk` + `.meta`. **`.meta` is the last per-chunk dedicated sidecar.** Every chunk has one — including all ~100M cold sealed chunks — so it dominates the inode/dirent count, and it costs a file-create + `force` + dirent-fsync per open chunk (`ChunkStore.java:508-512`).

### What `.meta` holds and how often it is written (grounded)

`ChunkFormats.Sidecar` (512 B): `{writeEpoch, fenceEpoch, lastKnownDO, state}` + its own CRC, written as an **atomic single-sector overwrite**.

`persistSidecar` is called only at state transitions — create (`:647`), **fence** (`:979/:982`), seal (`:1088`, `:328`), recovery (`:1334/:1762/:1808`), clean shutdown (`:2111`). It is **not** on the append hot path: `lastKnownDO` advances in memory on append (`:783`) without a persist.

### Field criticality

| Field | Criticality | Recoverable from |
|-------|-------------|------------------|
| `fenceEpoch` | **correctness-critical** — a torn/lost fence epoch can resurrect a fenced writer (split-brain / data loss) | only the sidecar (open chunks); `-1` (meaningless) for sealed |
| `writeEpoch` | important | also `header.createWriteEpoch()` |
| `lastKnownDO` | **advisory** — the true durable tail is rebuilt from the ledger/digest at recovery | trailer.dataLength (sealed); ledger replay (open) |
| `state` | OPEN/SEALED | SEALED is implied by a **valid trailer** (`MAGIC_TRAILER` + footerCrc + dataCrc) |

### Recovery already half-supports this

Recovery has a **trailer-based SEALED rebuild** (`:1330-1334`): `writeEpoch ← header.createWriteEpoch`, `lastKnownDO ← trailer.dataLength`, `fenceEpoch ← -1`. The OPEN branch (`:1750-1771`) is the one that genuinely reads the sidecar (for `fenceEpoch`, `writeEpoch`).

---

## Decision — two independent levers

### Lever 1 (recommended — the inode win): eliminate the SEALED `.meta`

A sealed chunk's durable state is fully reconstructable from its own `.chunk`: SEALED is implied by a valid trailer, length from `trailer.dataLength`, `writeEpoch` from `header.createWriteEpoch`, `fenceEpoch` is meaningless post-seal. **Recovery already does this** (`:1330-1334`).

- **Change:** seal/reclaim/import no longer write a SEALED `.meta`; the `.meta` is unlinked once the chunk is durably sealed (fsync-seal, and reclaim for the non-fsync default). Recovery classifies SEALED from the **trailer**, authoritatively, even over a sidecar still reading OPEN in the pre-reclaim window.
- **Net: cold (reclaimed) sealed chunk = 1 file** (`.chunk`). Removes ~100M inodes at the target scale.
- **Risk: MEDIUM, not low (ADR originally underestimated this).** The `.meta` is not merely redundant in the default non-fsync mode: during the pre-reclaim window a sealed chunk keeps a retained ledger, and "valid trailer + ledger" is ambiguous against an OPEN chunk whose payload is *crafted to look like a footer* (guarded by `missingSidecarOpenChunkWithFooterShapedPayloadRecoversFromLedger`). Making the trailer authoritative therefore required a **ledger-coverage disambiguator**: a real sealed chunk's ledger ends exactly at `trailer.dataLength`; a footer-shaped OPEN chunk's ledger covers the whole appended payload (strictly past `dataLength`, since the footer/trailer physically occupy `[dataLength, EOF)`). A naive "check the sealed footer first" would have mis-quarantined/mis-classified the footer-shaped open chunk.

**Status: IMPLEMENTED** (TDD, `strata-format` 136 green, `strata-node` 51 green). Touchpoints: `recoverOne` made trailer-authoritative via `sealedDataLength` + `ledgerLastEndOffset` (ledger-coverage disambiguator), deleting any stale OPEN sidecar; `seal()`/`reclaimSealedLedgersOnce()`/`importSealed()` drop the SEALED `.meta` write and unlink the `.meta` once durably sealed. Tests: `sealedChunkWithRetainedLedgerAndOpenSidecarRecoversAsSealed`, `fsyncSealLeavesNoSidecar`, `reclaimedSealedChunkLeavesNoSidecarOrLedger`, `importedSealedChunkWritesNoSidecarAndRecoversFromTrailer`; the footer-shaped adversarial guard stays green.

**Residual nuances (not blockers):** (1) `close()` still persists a `.meta` for a sealed-but-not-yet-reclaimed chunk on clean shutdown — a transient artifact that the next recovery deletes (trailer-authoritative) and reclaim removes in steady state; it also keeps the corrupt-footer **quarantine** path working (the close-persisted SEALED `.meta` routes a rotted-footer chunk through the SEALED branch). Fully removing it (close-skip-sealed) would require reconstructing the corrupt-sealed quarantine signal from the trailer magic — deferred. (2) A *crashed* (no clean close) corrupt-footer sealed chunk with a retained ledger recovers as OPEN (data intact, footer cut, re-sealable) rather than quarantined — no data loss, but an inconsistency vs the cleanly-closed quarantine path.

### Lever 2 (optional — the create-path/uniformity win): fold OPEN mutable state into a reserved header sector

Carve a 512 B **mutable-state sector** inside the 4096 B header (which today uses only a few hundred bytes), atomic single-sector overwrite in place, replacing the separate `.meta` file.

- **Header-CRC interaction (must get right):** the header CRC covers `[0, 4092)`. A mutable sector inside that range would be invalidated on every rewrite. So the mutable sector must be **excluded from the header CRC and carry its own CRC** — the immutable header fields keep a CRC over a reduced range; the mutable sector `[X, X+512)` is self-CRC'd. (Same mutable-field-outside-the-CRC pattern as Kafka's `partitionLeaderEpoch`.)
- **Fencing atomicity preserved:** a 512 B sector overwrite is atomic at the device level, so the fence-epoch update stays atomic. The `force` to persist it flushes the `.chunk` fd (data too), but persists are infrequent (fence/create/seal/shutdown, not per-append), so the fsync coupling is negligible.
- **Net: open chunk = `.chunk`** (+ a share of the shared digest log). Removes the per-open-chunk `.meta` create + `force` + dirent-fsync (a real create-path / roll-churn saving), and one more file class.
- **Why optional / lower priority:** the open `.meta` set is bounded by active-write concurrency (thousands, not 100M), so its inode win is small; the value is the create-path saving and footprint uniformity. It also carries fencing-atomicity care that Lever 1 does not. Do it after Lever 1, if the open create-path cost is measured to matter.

---

## Options Considered

| Option | sealed files | open files | Assessment |
|--------|-------------|------------|------------|
| **Status quo** (keep `.meta`) | `.chunk` + `.meta` | `.chunk` + `.meta` (+ shared log) | Last per-chunk sidecar; ~100M inodes; create-path cost |
| **Lever 1 only** | **`.chunk`** | `.chunk` + `.meta` (+ shared log) | Kills the 100M-inode problem. **Recommended minimum.** |
| **Lever 1 + 2** | **`.chunk`** | **`.chunk`** (+ shared log) | Fully collapses per-chunk dedicated sidecars — the endgame footprint |
| Shared per-volume **state** log | — | — | **Rejected:** mutable-overwrite state (fence epoch) is the wrong shape for an append log (you'd compact it), and it complicates fencing durability. The digest log works because digests are append-once; state is overwrite. |

---

## Consequences

**Easier**
- Far fewer inodes/dirents (Lever 1: ~100M); fewer create/`force`/dirent ops per open chunk (Lever 2); a cold chunk is a single self-describing file.

**Harder**
- Recovery must read the trailer to classify SEALED (Lever 1 — one EOF read, cheap, recovery-only).
- The header gains a mutable, separately-CRC'd sector with its own atomicity + ordering contract (Lever 2).

**To revisit**
- **dirent-fsync discipline** on seal/delete — cross-ref the known *no-dirent-fsync* release-blocker (data-integrity review). Fewer files to dirent-sync is a simplification, but the seal-time `.meta` delete must itself be dirent-durable, else recovery's SEALED branch could re-find a stale `.meta`.

---

## Action Items

1. [ ] **Lever 1:** at seal, stop writing the SEALED `.meta` and delete it with the `.j` reclaim; make recovery's SEALED branch (`:1330-1334`) the primary classifier (trailer-driven).
2. [ ] **Lever 1:** audit that no live path reads a *sealed* chunk's `.meta` (state/length/epoch must all come from trailer + header).
3. [ ] **Lever 2 (optional):** define header layout v2 — immutable fields with header CRC over `[0, X)`; mutable-state sector `[X, X+512)` self-CRC'd and **excluded** from the header CRC; in-place atomic overwrite; remove the `.meta` file.
4. [ ] **Lever 2:** point recovery's OPEN branch (`:1750-1771`) at the header sector instead of `.meta`; preserve fence-epoch atomicity and the data-before-fence ordering.
5. [ ] **Both:** ensure create/delete **dirent fsync** (cross-ref data-integrity review) so a missing/stale sidecar can never be re-found after a crash.

---

## Footprint summary (composed with durability-v2)

| Stage | open | sealed |
|-------|------|--------|
| today | `.chunk` + `.meta` + `.j` | `.chunk` + `.meta` |
| + durability-v2 (shared digest log) | `.chunk` + `.meta` + *(shared log)* | `.chunk` + `.meta` |
| + Lever 1 | `.chunk` + `.meta` + *(shared log)* | **`.chunk`** |
| + Lever 2 | **`.chunk`** + *(shared log)* | **`.chunk`** |

The `.chunk` is irreducible (it is the data). After both ADRs, a node's file count is: one `.chunk` per chunk + one shared digest log per volume — down from three files per chunk.
