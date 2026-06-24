# Namespace in the Data Plane + Long FileId — Design

**Goal:** Make a chunk self-describe its namespace on the data node, change `FileId`
from a 128-bit UUID to a per-namespace owner-assigned `long`, and — building on
those — replace the central inventory push with owner-driven verification and
node-local orphan GC. The end state: the data plane carries `(namespace, fileId,
index)`, storage shards by directory, and the single-leader O(N) inventory push
is gone.

**Architecture:** A chunk's global identity becomes the triple `(namespace,
fileId, index)`. `fileId` is a `long`, unique *within a namespace*, assigned by
that namespace's authority (the owner for user namespaces; ZK for the system
`strata-meta` namespace). On disk, chunks live under
`chunks/<namespace>/<shard>/<fileId>.<index>` where `<shard>` is a low-bit split
of the long, so the namespace is the parent directory (free during any walk) and
storage fans out into a bounded tree. Durability/orphan reconciliation moves from
"every node pushes its full chunk list to the leader every 30s" to "each owner
pulls batched verification of its own namespaces' chunks" plus "each node GCs an
unclaimed chunk only after an authoritative owner confirm."

**Tech Stack:** Java 21, Maven multi-module. Touches `strata-common` (`FileId`,
`ChunkId`), `strata-proto` (RPCs, `ScpServer`), `strata-format` (`ChunkStore`,
`ChunkFormats`), `strata-meta` (records, manifest, meta-log codec, owner/repair),
`strata-node` (`ControlLoop`, `DataNode`), `strata-server`, `strata-it`.

## Global Constraints

- **Clean break** — project never ships to prod. Bump `FORMAT_VERSION`; **no**
  backward-compat shim, dual-read, or migration. `down -v` between old/new format.
- **No new global high-volume coordination** — the `/strata/ids` node-id
  allocator was just removed; do not reintroduce a per-file ZK allocator for the
  high-volume (user) path. User fileIds are owner-assigned; only the low-volume
  system namespace uses ZK.
- **Fail-safe deletes** — a node never deletes data on a timeout or on
  uncertainty; only on an explicit authoritative confirm. Unreachable ⇒ retry.
- **Single-writer-per-namespace** — the namespace owner is the sole writer of its
  metadata (manifest version-CAS fence); the `nextFileId` counter rides that
  existing fence (no new coordination).

## Locked decisions (from brainstorming)

1. Spec covers the **whole vision** (#1 data model + #2 owner verify + #3 orphan
   GC), implemented in 3 phases.
2. Namespace lives on disk as a **directory** (`chunks/<ns>/…`), not a header
   field — free ops (`du`/`ls`/`rm -rf`), namespace-for-free during walks, no
   on-disk record-format change for namespace.
3. `FileId` = **per-namespace owner-assigned `long`** (BookKeeper `ledgerId`
   model), not a UUID.
4. Directory shard split = **low bits first** (sequential ids spread evenly from
   file #1), not high-bits-first (which clusters early ids in `00/00/…`).

---

## Section 1 — Data model: `FileId` → per-namespace `long`

- `FileId` becomes a single `long` (8 bytes, was 128-bit UUID). A chunk's global
  identity becomes `(namespace, fileId, index)`. Everywhere `FileId` was treated
  as globally unique (chunk on-disk format, filename, descriptors, proto RPCs)
  must now carry or derive the namespace.
- **Two-layer fileId generation:**

  | Layer | Whose files | Generator | Volume |
  |---|---|---|---|
  | **User** | per-namespace user files | owner's `nextFileId` counter | high — no ZK |
  | **System** | `strata-meta` meta-log segments + snapshots | ZK-based / derived | low |

  - **User (owner) generation.** Each namespace's owner keeps a monotonic
    `nextFileId`; `createFile` assigns the next value. It rides the existing
    single-writer manifest fence — **no global coordination**.
    - Persistence: `nextFileId` is a **high-water field in the namespace snapshot**
      (`NamespaceMetadataSnapshot`, alongside `files`/`tombstones`/
      `nextLogStartOffset`) and in the in-memory `NamespaceMetadataState`. It is
      **not** a separate log (a separate log would just double appends) and it is
      **not** derived from `max(live files)` (a deleted file's swept tombstone
      would forget its id and risk reuse). It only ever increases.
    - Recovery on owner failover: load the snapshot's `nextFileId`, replay the
      tail. `assign-then-append` is safe: if the owner crashes after assigning N
      but before `FileCreated(N)` lands, no file exists at N, so a later reuse of
      N is harmless.
  - **System (ZK) generation.** A user namespace's `nextFileId` lives *in* its
    meta-log, which is itself stored as `strata-meta` strata files — so the system
    namespace cannot host its own counter (circular). Its files get ids from ZK:
    - **(b)** a small ZK CAS counter scoped to `strata-meta`, or
    - **(d)** derive deterministically from the per-namespace `generation` already
      in the manifest (manifest-fenced) — `(namespace, generation, log|snapshot)`
      → a collision-free system fileId, **no new ZK counter**.
    - **Preference: (d) if it yields collision-free ids; else (b).** Either way
      bounded/low-volume — does not reintroduce a high-volume ZK allocator.
- **Blast radius:** `strata-common` (`FileId`/`ChunkId` 16→8 bytes), `strata-proto`
  (every fileId-carrying RPC encode/decode), `strata-format` (chunk header
  `chunkId` `20→12` bytes, filename `baseName`), `strata-meta` (records, manifest,
  meta-log codec, snapshot). Bump `FORMAT_VERSION`.

## Section 2 — On-disk layout & low-bit shard

- Layout (replaces flat `chunks/<fileId-uuid>.<index>`):
  ```
  <dataDir>/chunks/<namespace>/<shard>/<fileId>.<index>.{chunk,meta,j}
  ```
- `<namespace>` — tenant dir. Gives free ops (`du -sh chunks/<ns>`, `rm -rf
  chunks/<ns>` for namespace deletion) and the namespace **for free during any
  directory walk** (parent dir), which powers Sections 3–4 routing.
- `<shard>` — **low-bit split** of the long, e.g. 2 levels of one byte each
  (256 fan-out): `L1 = fileId & 0xFF`, `L2 = (fileId>>8) & 0xFF`, path
  `…/<L1 hex>/<L2 hex>/…`. Sequential ids round-robin across L1 from file #1 (even
  spread); L2 increments every 256 files. Fan-out/levels are tunable (a namespace
  pushing tens of millions of files raises fan-out or adds a level). A file's
  chunks (index 0,1,…) share the same fileId → same shard dir (locality).
- `<fileId>.<index>` — filename. fileId formatted as **zero-padded hex (16
  digits)** so lexical sort == numeric sort.
- **Namespace → directory name:** constrain the namespace charset at the control
  plane (`[A-Za-z0-9._-]`, bounded length) so the dir name *is* the namespace
  verbatim — no escaping, case-collision-safe. (Fallback: reversible escape, if
  arbitrary namespaces are ever required.)
- **Header:** still carries `chunkId` for self-identification (now 12 bytes);
  namespace is **not** in the header (it's the directory). Sidecar/Footer
  unchanged. `open()` (create path) takes the namespace from `CREATE_CHUNK`,
  `mkdir -p chunks/<ns>/<shard>`, fsyncs the shard dir, then creates the file +
  writes the header — files are created on disk before any data (current behavior).
- **Recovery:** `recoverAll` walks `chunks/<ns>/<shard>/*`, derives `namespace`
  from the path and `(fileId,index)` from the filename, and can recover namespaces
  **in parallel** (helps the sequential-recoverAll scaling concern).

## Section 3 — Owner-driven reconcile (replaces the inventory push)

- Replace "node pushes full chunk list to leader every 30s" with **owner-pull
  batched verification**.
- **New RPC `VERIFY_CHUNKS` (owner → node):** owner iterates *its own namespace's*
  files (meta-log state); for each chunk's expected replica nodes, sends a batch
  `[(chunkId, expectedLen, expectedCrc), …]`. Node replies per chunk:
  `present-ok / missing / corrupt`. Owner reacts: `missing|corrupt` →
  `applyDeleteConfirmed` drop replica → the owner's under-replication repair
  re-replicates in its namespace (direct `EXEC_REPLICATE`).
- **Cadence:** rolling/incremental — a batch per tick, whole file-set swept over
  many ticks; samplable/prioritizable. Payload bounded by batch size, not by the
  node's total chunk count. Sharded across owners (scales with controller count).
- **Node-side scrub → rolling:** replace `scrubOnce` (full re-CRC every 5 min)
  with re-CRC of a fraction of sealed chunks per cycle; the verify reply reports
  the latest computed crc, so rot surfaces within a scrub cycle.
- **Responsibility split:** node-death under-replication stays on the reconcile
  scan (descriptor × live-nodes, unchanged); missing/corrupt moves to owner
  verify; orphan → Section 4.
- **Feeds Section 4:** the node records, per chunk, "last verified by which owner,
  when" — the orphan "claim" signal.
- Removes: `ControlLoop.inventoryLoop` push and `onInventory` missing/corrupt.

## Section 4 — Orphan GC (node-local, grace → owner confirm → fail-safe)

- **Nominate (cheap pre-filter):** a chunk not verified by any owner within a
  grace window becomes a *suspect* (not "delete").
- **Confirm (authoritative, required before delete):** node derives `namespace`
  from the chunk's directory path, routes to `ownerOf(namespace)`, and asks
  *"does chunk X's current descriptor still list me (nodeId=N) as a replica?"*:

  | owner answer | node action |
  |---|---|
  | no / file gone / no such namespace | **confirmed orphan → delete the 3 files** |
  | yes, you should have it | not orphan (verify hadn't reached it) → reset timer, keep |
  | unreachable / error | **do not delete; retry later** (fail-safe) |

  Worst case = delayed reclamation; never data loss.
- **Membership-round safety:** the node may trust "no owner verified" only after
  it has heard ≥1 verify from **every current owner**. Use a long grace +
  `membership epoch`; reset grace on owner-set change.
- **Crashed-write orphans** are covered: the crashed chunk file is already under
  `chunks/<ns>/…` (namespace known at `CREATE_CHUNK`), so the node routes the
  confirm; the owner answers "no such fileId" → orphan → GC. Whole-namespace
  deletion uses `rm -rf chunks/<ns>`.
- Mirrors BookKeeper's bookie-local GC (confirm against authoritative metadata
  before delete; do not delete on metadata unavailability). The confirm requires
  namespace-on-node — the unique justification for putting namespace in the data
  plane (owner-driven verify alone doesn't need it; safe node-local GC does).
- Removes: the `onInventory` orphan path. After this phase inventory is gone.

## Section 5 — Phasing, scope & migration

Clean break (bump `FORMAT_VERSION`, no migration). Implemented in 3 dependent,
independently-testable phases:

| Phase | What | Depends on | Inventory after |
|---|---|---|---|
| **1. Data model** | `FileId`→`long`; two-layer gen; `(ns,fileId,index)` through proto/format/metadata; on-disk dir + low-bit shard; recovery walk (per-ns parallel) | — | unchanged |
| **2. Owner verify** | `VERIFY_CHUNKS`; rolling scrub; missing/corrupt → drop+repair; remove inventory push + `onInventory` missing/corrupt | 1 | orphan-only |
| **3. Orphan GC** | node-local grace→confirm→fail-safe; confirm RPC routed by namespace; remove inventory orphan path | 1+2 | **removed** |

Ordering: Phase 1 is the prerequisite (namespace-on-node + identity); Phase 2 needs
Phase 1 routing; Phase 3 needs Phase 2's "last-verified". `1 → 2 → 3`.

Unchanged throughout: the reconcile scan for node-death under-replication.

## Section 6 — Testing & correctness

Two tests carry the correctness story:

- 🔴 **id-reuse-on-failover** (corruption guard, Phase 1): failure-injection —
  owner assigns ids and crashes at each window (after assign/before `FileCreated`
  append; after append/before snapshot; mid-compaction). New owner recovers
  `nextFileId` from snapshot + tail. **Assert no id is ever reassigned.**
- 🔴 **orphan false-positive fail-safe** (data-loss guard, Phase 3): owner
  unreachable during confirm → node **must not delete**; retries; on "you should
  have it" → keeps.

Phase 1: tombstone-sweep doesn't lower the high-water (create N → delete → sweep →
compact → next create = N+1); system (ZK) fileId gen collision-free under
concurrent owners; on-disk layout + recovery reconstructs `(ns,fileId,index)`,
parallel per-namespace recovery, low-bit shard spreads sequential ids evenly;
proto/format roundtrip with 8-byte fileId + golden corpus update.

Phase 2: `VERIFY_CHUNKS` detects missing/corrupt → drop → re-repair; rolling scrub
surfaces injected bit-rot; sharding (owner verifies only its namespaces; node no
longer sends `INVENTORY_REPORT`); **durability gap closed** — corrupt bytes on a
*live* node in a *non-leader-owned* namespace are caught + re-repaired.

Phase 3: confirmed orphan (crashed-write / dead-target leftover) GC'd only after
confirm; membership change mid-grace resets grace.

Integration: full-cluster churn perf — disk bounded, `created==deleted`,
`cleanupCandidates=0`, no orphan leak.

## Risks & open items

- **Highest risk: id reuse on owner failover** (Phase 1). Mitigated by the
  high-water-in-snapshot + tail-replay recovery and the dedicated failure-injection
  test. Any bug here corrupts data.
- **System fileId generation choice (b vs d)** — settle during Phase 1 planning;
  (d) preferred if collision-free.
- **Grace window tuning** for orphan GC — must exceed a full owner verify sweep +
  failover/partition; long is fine (orphans are rare).
- **Reverses the `namespace-scope-preference` principle** — namespace now lives in
  the data plane (directory). Intentional: justified by self-describing storage,
  ops visibility, and safe node-local GC. Update that memory when Phase 1 lands.
