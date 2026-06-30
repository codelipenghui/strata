# Strata — Technical Design

**Status:** Draft v0.3 (2026-06-30) · **Audience:** engineering · **Companion:** [strata-product-definition.md](strata-product-definition.md) (positioning, competitive landscape, value propositions — not repeated here)

This is the single source of truth for the design. It subsumes the former standalone design notes — metadata scaling (§4), the writer-origin per-record digest and the chunk-file footprint reduction (§11) — which are folded into the sections below; deferred/rejected variants of those are recorded in §17.

---

## 1. Purpose and scope

This document specifies how Strata is built: components, protocols, wire and on-disk formats, state machines, failure handling, and the seams between Kafka-derived code and new code. It records decisions at the level an implementing engineer needs, and explicitly marks what is still open (§17).

Scope: v1 plus the v1.x media work. Erasure coding, multi-protocol tenants, and geo-replication are out of scope (see product doc §6–§7 for why).

## 2. Architecture overview

Three services, three codebases, three deployment shapes:

| Service | Role | Codebase | Durable state | Deployment (K8s) |
|---|---|---|---|---|
| **Broker** | Kafka protocol; partition leadership; group/transaction coordinators | Kafka fork (storage stack replaced) | none | Deployment; no PVCs; optional ephemeral NVMe for read cache |
| **Data node** | chunk persistence and serving | new, no Kafka code; language TBD (storage-server-appropriate) | chunk files on local disks | StatefulSet; local PVs; topology labels (zone/rack/host) |
| **Metadata plane** | Strata MetadataStore: ZooKeeper consensus root (nodes, namespace ownership, manifests, IDs) + per-namespace metadata log owned by the namespace owner | new, no Kafka code | ZooKeeper ensemble; per-namespace metadata-log + snapshot system files (replicated Strata files) | controller processes (any node) + a ZooKeeper ensemble of 3 or 5 |

Communication paths (protocol per link — see §10.1):

```
Kafka clients ──(Kafka wire protocol)──> Broker
Broker        ──(SCP control: create/seal/lookup)──> Metadata plane
Broker        ──(SCP: append/read/fence/seal)──> Data nodes
Data node  ──(SCP control: register/heartbeat/verify)──> Metadata plane
Data node <──(SCP: fetch-chunk)──> Data node      [repair/relocation only]
Controller ──(ZooKeeper)──> consensus root              [cluster state, leadership, manifests]
```

**Codebase strategy.** Only the **broker** is derived from the Apache Kafka codebase — fork-and-replace, the path AutoMQ has proven in production — for compatibility economics: the group and transaction coordinators live inside the broker, so a clean-room broker would reimplement the hardest compatibility surface. The broker's storage stack is replaced with a Strata client; everything below the broker is new code. The **metadata plane and the data nodes contain no Kafka code**: the metadata plane is Strata's own `MetadataStore` over a ZooKeeper consensus root plus per-namespace metadata logs (§4), and the file service is a standalone storage engine (its language is an open decision, §17.5). Strata's broker-facing operations are SCP control opcodes (§4.3, §10.4), versioned behind a Strata feature level from day one so rolling upgrades stay safe.

Two load-bearing disciplines, stated once and assumed everywhere below:

1. **The data path never touches the metadata plane.** Metadata participates at chunk boundaries, leadership changes, and topology changes only.
2. **Data nodes never read cluster metadata.** They report their own state and execute commands. Their metadata footprint is O(own state).

## 3. Data model

```
Topic-partition ──> segments (Kafka roll policy) ──1:1──> files ──> chunk chain
                                                          │
                    checkpoint file (per open segment) ───┘  (same file machinery)
```

- **File** — bounded append-only byte stream; the storage layer's only abstraction. API: create / fenced-append / seal / read / delete. Types: `LOG` (a Kafka segment) and `CHECKPOINT` (index + producer-state journal for an open segment). One segment = one file is *policy*, not a model invariant (chunk chains have no inherent length limit).
- **File identity** — `fileId` is a **per-namespace, owner-assigned `long`** (the BookKeeper `ledgerId` model, rendered as 16-hex), not a UUID. The global identity is the triple `(namespace, fileId, index)`; a bare `(fileId, index)` is *not* globally unique (`fileId 0` exists in every namespace). The namespace lives on disk as the parent directory (§11.1), so a `fileId` is unique only within its namespace. Id generation is two-layer (§4.5): user-file ids from the owner's monotonic high-water, system-file ids from a small consensus-root counter.
- **Chunk** — the unit of replication and placement, **~1 GB nominal**, identified by `chunkId = (fileId, index)` (12 bytes on the wire/header: `u64 fileId, u32 index`). States: `OPEN → SEALED → DELETING`. Record batches cross the storage layer as opaque bytes; the address is the byte offset.
- **Chunk descriptor** (in the chunk map) — polymorphic by layout type:

```
ChunkDescriptor {
  chunkId        (fileId, index)
  state          OPEN | SEALED | DELETING
  layout         REPLICATED { replicas: [nodeId x replicationFactor], writeEpoch }   // v1: only layout
                 // future: EC { scheme, stripes... } — additive, never required
  length, crc    (set at seal; authoritative)
}
```

- **Sealed chunk footer** (self-describing; on every replica): sparse offset index, time index, producer snapshot, aborted-transaction index, per-range CRCs, stats. A sealed chunk is readable and verifiable with zero metadata access. Full layout: §11.2.

## 4. Metadata plane

The metadata plane is Strata's own — new code, no Kafka. It manages **storage metadata only**: data nodes, files, chunks, and namespaces. (Kafka topic/partition metadata, leadership, and the group/transaction coordinators are the broker layer's concern; a Kafka-derived broker is a *client* of this plane, not part of it.) Metadata is split across two tiers behind a single **`MetadataStore` SPI** (§4.4):

- **A ZooKeeper consensus root** for cluster-wide state — small, slow-changing, globally consistent.
- **A per-namespace metadata log** for file and chunk state — large, fast-changing, sharded across controllers and owned per namespace.

A single elected cluster leader (Curator `LeaderLatch`) coordinates global maintenance; namespace *ownership* — which controller serves which namespace — is computed by rendezvous hashing over the eligible controllers, independent of that latch.

### 4.1 Cluster/system state (ZooKeeper root)

Held directly in ZooKeeper under `/strata`, guarded by version-CAS:

- **Node registry** — `NodeRecord { nodeId, incarnationId, endpoints, topology{zone,rack,host}, capacityBytes, state: REGISTERED|FENCED|DRAINING|DEAD }`; registration, leases, and incarnation fencing.
- **Namespace ownership** — the eligible-controller set and the per-namespace assignment generation.
- **Per-namespace manifest** — the version-CAS pointer to a namespace's current metadata snapshot + open log (the linearizable barrier for metadata-log compaction, validated in `tla/MetadataManifestCAS.tla`).
- **Metadata epochs and ID allocation**, and the **descriptors of the metadata-log / snapshot system files** (which are themselves replicated Strata files — §4.2).

This state is small (cluster membership plus one manifest/epoch per namespace) and changes slowly, so a consensus root sized for coordination, not data volume, is the right tool.

### 4.2 Per-namespace file/chunk metadata (the namespace log)

Each namespace's file and chunk metadata — `FileRecord { fileId, namespace, path, state, … }` and the `ChunkDescriptor`s under it — lives in that namespace's own **metadata log**: an ordered, durable log of metadata mutations plus periodic snapshots, **stored as a replicated Strata file** (chunks on data nodes, the same machinery user data uses), **owned and served by the namespace's owner**. Recovery rebuilds the in-memory derived indexes (`file → chunks`, `node → chunks` for repair, per-node usage for placement) from the published snapshot + log tail. This state scales with retained data (~1M descriptors/PB), not with namespace count — which is exactly why it is sharded out of the global root and onto per-namespace owners. The log is kept bounded by a background compaction sweep (snapshot + roll once the open log passes a size threshold) and a generation-based GC of system files orphaned by a crash between file-create and manifest publish. Compaction may drop a record only after a sealed snapshot generation covering it is published in the consensus manifest **and** past the retention floor: a `FileDeleted` tombstone and the applied-opId idempotency state must outlive the window in which a stale create/seal/delete retry could resurrect the id or path, so the retention horizon — not just the snapshot cut — is the lower bound for safe log truncation (validated in `tla/MetadataTombstoneSweep.tla`, `tla/MetadataIdempotency.tla`).

The namespace's owner assigns each file's id from a monotonic `nextFileId` **high-water carried in the namespace snapshot** (not `max(live files)`, which a swept tombstone would forget and risk reusing). Assignment rides the existing single-writer manifest fence — no global allocator on the hot path. On failover the successor loads the snapshot's `nextFileId` and replays the tail; assign-then-append is safe because a crash after assigning N but before `FileCreated(N)` is durable leaves no file at N, so reissuing N is harmless — the "id reuse on failover" correctness anchor (§4.5, invariant §14.13).

### 4.3 RPC surface (SCP control)

Brokers and data nodes both reach the metadata plane over **SCP control opcodes** (§10.4) served by an SCP listener on each controller — one protocol stack for every client. A request for a namespace this controller does not own is answered `NOT_LEADER` carrying the owner's endpoint; the owner-aware client caches `namespace → owner` and routes directly, re-resolving only on that redirect.

| API | Caller | Notes |
|---|---|---|
| `CreateFile` / `DeleteFiles` | broker | the owner assigns the per-namespace FileId; retention is the caller's policy, the plane orchestrates physical deletion |
| `CreateChunk(fileId)` → `{chunkId, replicas[replicationFactor], writeEpoch}` | broker | placement decided here (§8); commit-before-write (§9.2 relies on this) |
| `SealChunk(chunkId, length, crc)` | broker | the metadata commit is the authoritative seal point |
| `LookupFile` / `LookupChunks(namespace, fileId, offsetRange)` | broker | served from the owner's in-memory state; clients cache aggressively (sealed descriptors are immutable) |
| `ListFiles(namespace, pageToken)` → `{paths[], nextPageToken}` | broker/tool | **always paged** — no unbounded response; listing stays local to the namespace owner |
| `REGISTER_NODE` / `NODE_HEARTBEAT` / `VERIFY_CHUNKS` | data node | heartbeat **responses** carry commands (`REPLICATE`, `DELETE`, `DRAIN`); durability reconciliation is owner-pull `VERIFY_CHUNKS`, not a node inventory push |

Availability: if a namespace's owner or the consensus root is briefly unavailable, open-chunk appends and all reads continue (no metadata on the data path); chunk creates/seals/ownership changes queue. Produce stalls only when an open chunk fills without a successor — minutes at typical per-partition rates. Heartbeat grace periods must exceed metadata failover time so a control-plane blip never triggers repair.

### 4.4 The `MetadataStore` SPI

Node-registry, namespace-assignment, and manifest logic sits behind a **`MetadataStore` SPI** so the consensus root is swappable and unit-testable. Three implementations run the SPI conformance suite (§16): the ZooKeeper-direct store (§4.1), the namespace-log store (the per-namespace owner over a ZooKeeper root, §4.2), and an independent in-memory reference. The SCP control surface (§10.4) and client behavior are identical regardless of which `MetadataStore` is bound, and commands always ride heartbeat responses — a backend's watch/notification mechanism is never exposed as protocol semantics, so a data node or broker cannot tell which root is running.

### 4.5 Scaling to 100M+ files (namespace sharding specifics)

The two-tier split (§4.1/§4.2) is what lets one Strata cluster hold **100M+ files across many tenants** without one consensus record per file, chunk, replica, or tombstone. The load-bearing specifics:

- **Static namespace assignment.** Ownership is rendezvous-hashed over the eligible-controller set and is **static after creation** — there is no automatic load balancer. It changes only when an operator explicitly moves a namespace or the assigned owner loses availability (failover re-derives ownership from the same hash). One namespace is served by exactly one owner; intra-namespace sharding is a deferred hook (§17.3).
- **Two-layer id generation.** User-file ids come from the owner's in-snapshot `nextFileId` high-water (§4.2) — no consensus round on the create path. The system `strata-meta` namespace cannot host its own counter (its `nextFileId` lives in meta-log files that are themselves `strata-meta` files — it would recurse), so system-file ids come from a small CAS counter in the consensus root scoped to `strata-meta` (low volume).
- **Per-namespace leader recovery barrier.** On acquiring a namespace, a controller CAS-increments its metadata epoch — fencing prior metadata-log writers at the storage layer, so two controllers that briefly open the same namespace during a membership settle are ordered by epoch (the later, higher-epoch opener wins) — then stays `RECOVERING`: load the manifest, recover/seal the open log tail to its durable end, replay the snapshot + tail, rebuild derived indexes, and re-verify the epoch is still current before serving (`ACTIVE`). The barrier is what makes a `LOOKUP_FILE`/`VERIFY_CHUNKS` answer authoritative — a `FILE_NOT_FOUND` is never a stale empty-state false positive — which the node-local orphan GC (§9.2) relies on to delete safely. (Validated in `tla/MetadataManifestCAS.tla`, `tla/MetadataTwoLeaderFencing.tla`.)
- **Stale-epoch re-acquire (converge, don't wedge).** A controller that still owns a namespace but holds a repository cached at a now-stale epoch would otherwise have every metadata-log append fenced (`FENCED_EPOCH`) and retry forever — a permanent wedge with the in-flight file never finalizing. Instead, a fenced append makes the owner evict the stale repository and re-open the namespace (fresh epoch + the barrier above), then replay the mutation once; the retry is bounded to a single attempt, so a genuine ownership disagreement surfaces as the fence rather than an epoch-thrash loop.
- **Derived indexes, not sources of truth.** `file → chunks`, `(namespace, path) → fileId`, and `node → chunks` are materialized from the metadata log for lookup/listing/repair and rebuilt on recovery; the log is the only authority.

## 5. Write path

### 5.1 Produce flow

1. Producer → leader broker (standard Kafka produce, unchanged on the wire).
2. Broker validates (epoch, producer state, transaction state) — in-memory, as Kafka does.
3. Broker appends the batch to the partition's open chunk: fan-out `APPEND` (§10.3) to all replicas selected by the file's `replicationFactor`.
4. Each replica: checks `writeEpoch ≥` its locally stored max epoch for the chunk (else `FENCED_EPOCH`), enforces contiguity (`baseOffset` must equal local end, else `OFFSET_GAP`; leader retries), appends payload bytes verbatim to the chunk file, appends an entry to the chunk's integrity ledger (§11.3) carrying the **writer-supplied per-record digest** (the frame's payload CRC, already verified by the frame decoder — the node stores it rather than originating its own), optionally fsyncs (§5.3), acks.
5. Broker acks the producer at the file's `ackQuorum` replica acks. With the default policy (`replicationFactor=3`, `ackQuorum=2`), latency = second-fastest replica.
6. Broker inserts the batch into its in-memory tail cache and advances the **durable offset (DO)** = highest contiguous byte acked by at least `ackQuorum` replicas. DO is piggybacked on the next `APPEND` (replicas learn DO with one-round lag — the BookKeeper LAC pattern). On idle partitions, an empty-payload `APPEND` serves as a DO beacon.

### 5.2 Chunk lifecycle driven by the leader

- **Roll** at ~1 GB, on segment roll, or on persistent replica failure (§7.2): seal current, `CreateChunk` next. Create-ahead (pre-allocate the successor when the current chunk passes ~80%) keeps chunk transitions off the produce latency path.
- **Seal:** write footer via `SEAL_CHUNK` to replicas (quorum-acked), then `SealChunk` to metadata with final length + CRC. A chunk is sealed when the *metadata commit* lands; replica footers are convergent state (§9.2 reconciles stragglers).
- **Checkpoint:** every ~16 MB of log, append to the segment's `CHECKPOINT` file: sparse index entries, producer-state delta, aborted-txn entries, current DO (format: §11.4). Checkpoints are accelerators; the log is the truth.

### 5.3 Durability policy

Per-file write policy: `replicationFactor`, `ackQuorum`, and `fsyncOnAck`. The default policy is `replicationFactor=3`, `ackQuorum=2`, `fsyncOnAck=false` (durability = 2 independent nodes, flushes async — Kafka's own stance). When `fsyncOnAck=true`, replicas fsync data + ledger before acking. The fsync choice is carried in `OPEN_CHUNK` and stored per chunk so replicas don't consult config. Benchmarks publish both fsync modes.

**Group commit (fsync mode).** Replicas never force per append: the write lands in the page cache and the ack defers until a per-chunk flusher's force covers it — one force amortizes across every append since the previous one, preceded by a short accumulation window (in-flight fsyncs throttle concurrent writes at the OS level; a clean gap lets a real batch land). Ack latency is ~accumulation + 1–2 force times regardless of pipeline depth. This requires deferred append acks at the server (§10.2 ordering note): validation and writes stay synchronous and in-order on the connection; only the response completes later, which is protocol-legal (correlation ids).

### 5.4 Fencing

Kafka's leader epoch is the only fencing token in the system. New leader (epoch E+1) fences before writing: `FENCE(chunkId, E+1)` to all reachable replicas; replicas persist the fence epoch (§11.3 sidecar) and reject lower-epoch appends permanently. A deposed leader's in-flight appends fail at the replicas; it cannot ack anything new. There is no window in which two writers can both achieve quorum: 2-of-3 ack sets for two epochs must intersect, and the intersecting replica is fenced.

## 6. Read path

| Read type | Served by | Mechanism |
|---|---|---|
| Tail fetch | leader broker | in-memory batch cache (data just flowed through); bounded by DO |
| Recent miss / lagging consumer | leader broker | `READ` from any chunk replica; optional local-NVMe read cache (pure cache, never correctness) |
| Historical / replay | broker (v1) | broker streams from storage replicas — sealed chunks: any replica, zero coordination, contiguous range, sendfile-friendly |
| Historical, broker-bypass | data node (v1.x option) | KIP-392 `PreferredReadReplica` redirect: leader points the consumer at a data node implementing a minimal Fetch subset for sealed data. Standard clients already honor this. Open chunks stay broker-served. |

Open-chunk reads are bounded by DO. The leader knows DO authoritatively; replicas know it with one-round lag (piggyback), which is sufficient for replica-served reads because anything ≤ piggybacked-DO is guaranteed quorum-durable. **Consumers never see un-quorum-acked bytes** — the analogue of Kafka's high-watermark rule, and the §17.1 item "DO propagation cadence" is about the *staleness bound*, not whether the rule exists.

## 7. Failure handling

### 7.1 Broker failure

The broker layer detects the failure (broker session expiry) and reassigns leadership of its partitions across surviving brokers — a Kafka-control operation, independent of Strata's metadata plane. Each new leader then, per partition: fence open chunks at epoch+1 → read latest checkpoint → replay ≤16 MB of open-chunk tail (batch headers only) to rebuild producer state and indexes → run seal-recovery if the open chunk needs it (§7.3) → accept produces. No data moves; cost independent of history size. Per-partition recovery is milliseconds; the fleet-wide work is parallel across all survivors.

### 7.2 Data-node failure

- **Detection:** missed heartbeats → `SUSPECT`; grace period (≥ several minutes, must exceed metadata failover and pod-reschedule time) → `DEAD`. Node identity = `incarnationId` persisted on its volumes: a pod rescheduled onto the same local PVs re-registers as the same node and cancels the clock. Repair of 100 TB must never be triggered by a 90-second reschedule.
- **Repair (sealed chunks):** the namespace's owner is the repair coordinator for its chunks (the cluster leader drives only cross-namespace orphan reconciliation). From its per-namespace `node → chunks` reverse index it enqueues every affected chunk, prioritized by exposure (chunks at 1 surviving replica jump the queue), picks new targets via standard placement (§8), and issues `REPLICATE` commands (in heartbeat responses) to the **new targets**, which pull via `FETCH_CHUNK` from surviving replicas (pull model: the target paces itself, owns retries, and dedups). Copies are checksum-verified, throttled per node; completion = atomic descriptor swap. Because placement scatters chunks, repair parallelism scales with pool size (100 TB node, 50-node pool ≈ ~2 TB per node ≈ ~3 h at 200 MB/s throttle).
- **Open chunks (fast path):** the partition leader, on losing a write-set replica past a short threshold, seals the chunk at DO (running seal-recovery if needed) and rolls to a fresh replica set — write path recovers in one chunk-create; the sealed remainder enters normal repair. There is no in-place ensemble patching; **roll is the ensemble change.**
- **Decommission** = the same machinery with the node as a willing copy source (`DRAINING`).

### 7.3 Seal recovery (open chunk, leader died or replica lost)

The recovering leader must establish the durable prefix without the old leader's in-memory DO:

1. `FENCE(chunkId, E+1)` on all reachable replicas — the response carries each replica's local end offset and last-known DO (need ≥2 reachable; with <2 the chunk is unavailable until a replica returns — same fault model as Kafka `min.insync.replicas=2`).
2. Start from `max(piggybacked DO)` across reachable replicas — everything below is known quorum-durable.
3. Scan forward batch-by-batch (`READ` + ledger verification): if a batch exists on **any** reachable replica (CRC-valid), re-replicate it to quorum and advance; stop at the first offset found on none.
4. Seal at the stop point; write footers; commit `SealChunk`.

Property: any producer-acked batch existed on at least `ackQuorum` replicas; with failures below the policy's tolerated threshold, at least one holder is reachable, so step 3 preserves it. With the default policy, this is the same tolerance as Kafka RF=3/acks=all/min.isr=2. Batches beyond the seal point were never acked; discarding them is correct.

### 7.4 Metadata quorum failure

Losing a single namespace owner or ZooKeeper member: failover in seconds, invisible to the data path — a successor controller re-acquires the namespace and re-opens its metadata log from the published manifest. Losing the ZooKeeper quorum: the data path continues (§4.3); chunk-boundary operations and ownership changes queue; recovery is standard ZooKeeper-ensemble recovery, after which each owner republishes from its manifest. Brokers and data nodes buffer/retry control operations idempotently (all control RPCs carry idempotency keys).

## 8. Placement

Inputs at `CreateChunk`: capacity-weighted random selection over `REGISTERED` nodes, filtered by (a) anti-affinity — no two replicas share the configured failure domain (default: host; recommended: rack/zone where topology allows), (b) exclusion of `SUSPECT/DRAINING/DEAD` nodes and nodes over a fullness watermark. Weights derive from free capacity, so new nodes absorb a proportionate share of *new* writes immediately — never a thundering herd, and never a rebalance. The same selector serves repair-target choice and operator relocation.

Anti-correlation is a first-order p99.9 concern (quorum latency is bounded by the second-slowest replica): placement must avoid co-locating replicas on shared hosts or shared burst-credit storage.

## 9. Background work

### 9.1 Retention

Leader evaluates retention (it owns the policy and the segment timeline) → `DeleteFiles` → metadata marks chunks `DELETING`, issues `DELETE` commands via heartbeats, removes records after replica confirmation (or reconciliation timeout). Space reclaim is file unlink: immediate and exact.

### 9.2 Reconciliation (scrub)

Reconciliation runs in both directions, owner-driven rather than as a node push. **Missing/corrupt replicas:** a namespace owner periodically pulls `VERIFY_CHUNKS` from each node holding one of its sealed replicas and diffs the node's report against the descriptor — a replica missing, short, or CRC-mismatched past a grace is dropped and re-repaired (the last live replica is never dropped). **Orphans on disk:** each node runs a local orphan GC — a sealed chunk that no owner has verified within a grace becomes a suspect, the node confirms it with the namespace owner (`LOOKUP_FILE`), and deletes only a chunk the owner no longer references (fail-safe: an unreachable owner never triggers a delete). The **commit-before-write invariant** (a chunk exists in metadata before any byte is sent — §4.3) is what makes orphan deletion safe: any on-disk chunk unknown to its owner and older than the create timeout was never live.

### 9.3 Compaction and relocation

Log compaction runs on the leader broker exactly as in Kafka — read, rewrite into a new file, swap, delete old — expressed entirely as ordinary file operations; the storage layer never knows. (Side effect: each cycle re-places output across current topology — compacted topics self-balance.) Operator relocation reuses the repair pull path: `REPLICATE` + descriptor swap; sealed-chunk immutability makes it coordination-free.

## 10. Wire protocol — SCP (Strata Chunk Protocol)

### 10.1 Protocol landscape and rationale

| Link | Protocol |
|---|---|
| Kafka clients ↔ broker | Kafka wire protocol (compatibility is the product) |
| Broker ↔ metadata plane | **SCP** control opcodes (the metadata plane is Strata-native; the broker is a Strata client) |
| Broker ↔ data node | **SCP** |
| Data node ↔ data node | **SCP** (`FETCH_CHUNK`) |
| Data node ↔ metadata plane | **SCP** control opcodes (metadata plane hosts an SCP listener) |

SCP is a purpose-built binary protocol rather than a reuse of Kafka framing, for three reasons: (1) the **payload-as-suffix invariant** — every frame ends with its opaque payload, so receivers can scatter headers into the heap and splice payload bytes directly between socket, aligned buffer pools, and disk with zero re-encoding; (2) the data node stays implementable in any language with **no Kafka dependency and exactly one protocol stack** (data, copy, and control all speak SCP); (3) independence from Kafka's API-key space and version cadence — the fork tracks upstream, and entangling storage APIs with Kafka's ApiKeys would couple data-node releases to broker merges. The alternative (Kafka framing everywhere) was considered and is workable; it was rejected on (1) and (3).

### 10.2 Conventions and framing

Conventions: big-endian; `varint` = unsigned LEB128 (zigzag where signed); `uuid` = 16 bytes; `string` = varint length + UTF-8; `bytes` = varint length + raw; all checksums CRC32C (Castagnoli).

**Frame layout (frameVersion = 1):**

```
u32  frameLength      // bytes after this field
u8   magic = 0x5C
u8   frameVersion = 1
u16  opcode
u16  apiVersion       // schema version of this opcode
u16  flags            // bit0 RESPONSE · bit1 PAYLOAD_CRC_PRESENT · bit2 reserved(compression) · rest must-be-0
u64  correlationId
u32  payloadLength
u32  payloadCrc32c    // valid iff PAYLOAD_CRC_PRESENT
u16  headerLength
[ header: fixed fields defined per (opcode, apiVersion), then a tagged-field block ]
[ payload: exactly payloadLength bytes — ALWAYS the trailing suffix of the frame ]
```

- **Tagged-field block** (every header ends with one): `varint count`, then per field `varint tag, varint size, bytes`. Unknown tags MUST be ignored. Tags are append-only per (opcode, direction) and never reused.
- **Responses** carry the request's opcode with the RESPONSE flag. Every response header begins `u16 errorCode` (0 = OK); on error: `string errorMessage` + tagged fields carrying typed detail (documented per opcode, e.g. `expectedEndOffset` on `OFFSET_GAP`, `currentFenceEpoch` on `FENCED_EPOCH`).
- **Payload CRC vs zero-copy:** the writer always sets PAYLOAD_CRC on `APPEND`. A server MAY omit it on `READ`/`FETCH_CHUNK` responses served via sendfile; the reader then relies on record-batch internal CRCs and footer range CRCs. Integrity is therefore end-to-end (content CRCs) with hop-level CRC as an optimization, not a dependency.
- **The APPEND payload CRC *is* the durable per-record digest (writer-origin).** For an `APPEND`, one append = one frame = one integrity-ledger record over the same bytes, so the frame decoder's verified payload CRC is stored verbatim as the chunk's per-record digest (§11.3) — the node never recomputes a competing value. This makes the stored digest the value the *writer* computed over its original bytes (true end-to-end), retains verify-on-ingest for free (the decoder already recomputed and compared it), and removes one redundant CRC pass on the hot path. The node still computes the running whole-chunk/per-4 MiB-range aggregate for the sealed footer (§11.2) and seal-divergence voting; deriving that aggregate from the per-record digests via CRC-combine (zero node byte-passes) is deferred (§17.13).
- **Connection lifecycle:** TCP, TLS per deployment policy. First frame MUST be `HELLO`. Pipelining is allowed after handshake, bounded by the negotiated in-flight byte cap. All `APPEND`s for a given chunk MUST use a single connection (ordering); after reconnect, the writer resyncs with `STAT_CHUNK`.
- **Implementation note (v0 finding):** on virtual-thread runtimes, never hold a monitor (`synchronized`) across blocking I/O or while response handlers may contend for it — blocked virtual threads inside monitors pin their carriers (JDK ≤23), and enough pinned carriers stall every virtual thread in the process. Use `ReentrantLock`, and keep blocking work/response callbacks off transport event-loop threads (a handler blocked on a lock must never stall frame dispatch for the very response its lock-holder is waiting on).

**Handshake.** `HELLO` request: `u16 frameVersionMin, u16 frameVersionMax, u8 clientKind (1 broker | 2 data-node | 3 metadata | 4 tool), u64 featureBits, string clientId`. Response: `u16 chosenFrameVersion, u64 featureBits (intersection), u32 nodeId (0 if n/a), uuid incarnationId, u32 maxFrameBytes, u64 maxInflightBytes, array{u16 opcode, u16 maxApiVersion}`. The per-opcode version map is the negotiation mechanism: a client uses `min(its max, advertised max)` per opcode and never sends an opcode absent from the map.

### 10.3 Data-plane opcodes

| Opcode | Name | Request header (v1 fixed fields) | Response (after errorCode) | Payload |
|---|---|---|---|---|
| 0x0001 | `HELLO` | see §10.2 | see §10.2 | — |
| 0x0010 | `OPEN_CHUNK` | chunkId{u64 fileId, u32 index}, i32 writeEpoch, u8 fsyncOnAck (0/1), u64 expectedMaxBytes, u64 createdAtMs | — | — |
| 0x0011 | `APPEND` | chunkId, i32 writeEpoch, u64 baseOffset, u64 durableOffset | u64 endOffset | log bytes (may be empty = DO beacon); the frame's payload CRC is stored as the per-record digest (§10.2) |
| 0x0012 | `READ` | chunkId, u64 offset, u32 maxBytes | u64 localEndOffset, u64 durableOffset | chunk bytes |
| 0x0013 | `FENCE` | chunkId, i32 fenceEpoch | i32 persistedFenceEpoch, u64 localEndOffset, u64 lastKnownDO, u8 state | — |
| 0x0014 | `STAT_CHUNK` | chunkId | u8 state, u64 localEndOffset, u64 lastKnownDO, i32 writeEpoch, i32 fenceEpoch, u64 sealedLength, u32 sealedCrc | — |
| 0x0015 | `SEAL_CHUNK` | chunkId, i32 writeEpoch, u64 dataLength | u64 finalLength, u32 chunkCrc | footer bytes (§11.2) |
| 0x0016 | `DELETE_CHUNKS` | varint n, chunkId×n | array{chunkId, u16 code} | — |
| 0x0017 | `FETCH_CHUNK` | chunkId, u64 offset, u32 maxBytes | u64 fileLength, u8 state | raw chunk-file bytes (header block + data + footer) |
| 0x0018 | `PING` | — | — | — |
| 0x0019 | `READ_LEDGER` | chunkId, u64 fromOffset | array{u64 endOffset, u32 payloadCrc, i32 writeEpoch} | — |

`READ_LEDGER` (v0 addition) exposes integrity-ledger entries above an offset: seal recovery (§7.3)
needs per-append boundaries without parsing the opaque payload. Empty for sealed chunks (the ledger
is deleted at seal; recovery never needs it then). `SEAL_CHUNK` semantics refined in v0: caller
footer sections are optional, and the node ALWAYS computes CRC_RANGES + STATS itself — recovery-
sealed chunks therefore stay byte-identical across replicas with no caller input.

Notes: `FETCH_CHUNK` is distinct from `READ` so it can run in a separate QoS/throttle class (repair must never starve foreground reads) and because it copies the *file* representation (header + footer included) — a repaired sealed replica is byte-identical, so whole-file CRCs are comparable across replicas. The node-local sidecar and ledger (§11.3) are never copied; the puller starts fresh ones.

### 10.4 Control-plane opcodes (data node ↔ metadata plane)

| Opcode | Name | Request (v1) | Response (after errorCode) |
|---|---|---|---|
| 0x0101 | `REGISTER_NODE` | uuid incarnationId, array endpoints, topology{string zone, rack, host}, array{u64 capacityBytes}, u32 onDiskFormatMax, u64 featureBits | u32 nodeId, u64 sessionEpoch, u32 heartbeatIntervalMs, u32 leaseMs |
| 0x0102 | `NODE_HEARTBEAT` | u32 nodeId, uuid incarnationId, u64 sessionEpoch, array{u64 usedBytes, u64 freeBytes}, u32 repairQueueDepth | u64 leaseValidUntilMs, array commands |

Durability reconciliation is the owner pulling `VERIFY_CHUNKS` (0x001C, a data-plane opcode served by the node — §9.2) plus the node's local orphan GC; there is no node-push inventory report.

v0 additions: `NODE_HEARTBEAT` requests carry tagged field 0 `completedCommands` (array{u64
commandId, u16 status}) so the repair coordinator learns command completion on the next heartbeat;
`REPLICATE` params gained `expectedLength`. The client↔metadata APIs ride SCP in the 0x02xx
range (`CREATE_FILE` 0x0201, `CREATE_CHUNK` 0x0202, `SEAL_CHUNK_META` 0x0203, `LOOKUP_FILE` 0x0204,
`DELETE_FILES` 0x0205, `SEAL_FILE` 0x0206, `ABORT_CHUNK_META` 0x0207, `LOOKUP_PATH` 0x0208) — this is the
broker-and-tool control surface to the metadata plane (§10.1).

Command encoding (in heartbeat responses): `u8 type` + tagged params. v1 types: `REPLICATE{chunkId, sources: array{nodeId, endpoint}, u8 priority, expectedCrc}` (pull via `FETCH_CHUNK`), `DELETE{chunkIds}`, `DRAIN{}` (node enters DRAINING; serve reads/copies, refuse `OPEN_CHUNK`). New command types are additive; a node MUST ignore unknown command types and report them in the next heartbeat (tagged field `unknownCommandTypes`) so operators see version skew instead of silent no-ops.

Node identity: the assigned `nodeId` + `incarnationId` are persisted in an identity file on every data volume; re-registration presents them, which is what makes pod rescheduling onto the same disks identity-preserving (§7.2).

### 10.5 Error codes (shared, append-only)

`0 OK · 1 UNKNOWN_OPCODE · 2 UNSUPPORTED_VERSION · 3 FENCED_EPOCH · 4 OFFSET_GAP (retriable) · 5 CHUNK_NOT_FOUND · 6 CHUNK_SEALED · 7 CHUNK_ALREADY_EXISTS · 8 OUT_OF_SPACE · 9 CRC_MISMATCH · 10 NOT_REGISTERED · 11 LEASE_EXPIRED · 12 THROTTLED (retriable; tagged retryAfterMs) · 13 CORRUPT_CHUNK · 14 INTERNAL (retriable) · 15 NOT_LEADER (retriable; v0) · 16 NO_CAPACITY (retriable; v0) · 17 FILE_NOT_FOUND (v0) · 18 FILE_SEALED (v0) · 19 PRECONDITION_FAILED (v0)`. Codes 0–999 reserved for protocol-level errors; 1000+ for future domain extensions. Codes are never renumbered or reused.

Error precedence (v0 finding, locked as protocol semantics): **the fence check dominates the state
check** — a deposed writer appending to a recovery-sealed chunk gets `FENCED_EPOCH` (permanent
death for the appender), never `CHUNK_SEALED` (which reads as "roll and continue").

### 10.6 Wire compatibility rules

1. The frame preamble is frozen for `frameVersion=1`. New cross-cutting needs use flag bits and tagged fields; a preamble layout change requires a frameVersion bump negotiated in `HELLO` (expected: never).
2. Fixed header fields for a given `(opcode, apiVersion)` are immutable — never reordered, retyped, or re-meant. Additive optional data goes in tagged fields *without* a version bump; new required fields or changed semantics require a new `apiVersion`.
3. Servers advertise `[0, maxApiVersion]` per opcode in `HELLO`; clients send the highest mutually supported version. Unknown opcodes are never sent; unknown tagged fields and unknown command types are ignored (and surfaced, per §10.4).
4. Error codes, opcodes, command types, and tags are append-only namespaces.
5. **Support window:** any two components within one minor release of each other MUST interoperate (rolling upgrades); a major release may drop wire versions only with an explicit migration note.
6. Feature bits in `HELLO` gate cross-cutting behaviors (e.g., future compression): active only when both sides advertise them.

## 11. On-disk formats

All persistent structures share three rules: every structure begins with `{u32 magic, u16 formatVersion}`; every structure or section carries a CRC32C; every format carries three feature masks — `compat` (readers may ignore), `roCompat` (readable, not writable, if unknown), `incompat` (must refuse if unknown) — the ext4 discipline that lets old software fail *predictably* on new data.

### 11.1 Chunk file

**On-disk path (namespace is the directory, not a header field):**

```
<dataDir>/chunks/<namespace>/<L1>/<L2>/<fileId-16hex>.<index>.{chunk,meta,j}
  <L1> = fileId & 0xFF · <L2> = (fileId>>8) & 0xFF   ← low-bit split, 256-way fan-out
  .chunk always · .meta + .j are open-chunk-only (§11.3) — a sealed chunk is just .chunk
```

The namespace is the parent directory — free `du -sh`/`rm -rf` per tenant, and the namespace falls out of any directory walk (which powers reconcile and orphan-GC routing, §9.2). The control plane constrains the namespace charset (`[A-Za-z0-9._-]`, bounded length) so the directory name *is* the namespace verbatim. The **low**-bit shard split makes sequential ids round-robin across `L1` from the first file rather than clustering early ids under `00/00/…`; a file's chunks share its `fileId` and so share the shard directory (locality). `<fileId-16hex>` is zero-padded hex so lexical sort equals numeric sort. Recovery walks `chunks/<ns>/<L1>/<L2>/*`, derives `(namespace, fileId, index)` from the path, and recovers namespaces in parallel.

```
[ header block — 4096 bytes, fixed ]
  u32 magic "SCHK" · u16 formatVersion=2 · u16 headerSize
  chunkId{ u64 fileId, u32 index } · u8 fsyncOnAck      // 12-byte chunkId; namespace is NOT here (it's the directory)
  i32 createWriteEpoch · u64 createdAtMs
  u32 compatFlags · u32 roCompatFlags · u32 incompatFlags
  tagged block · zero padding · u32 headerCrc (last 4 bytes)

[ data region ]
  raw logical bytes, verbatim (record batches for LOG; §11.4 entries for CHECKPOINT)
  logical chunk offset X = file offset 4096 + X        ← address arithmetic, preserved

[ footer — sealed chunks only, appended after data ]   (§11.2)
```

The data region contains **no storage-layer framing**: what the producer's batches look like on the broker is byte-for-byte what sits on disk and what the consumer receives. This is the zero-copy and offset-arithmetic invariant; all storage-layer bookkeeping lives in the header, footer, and (for open chunks) the sidecar and ledger.

### 11.2 Sealed footer

Sections, each `{u16 type, u16 version, u32 length, bytes, u32 crc}`:

| Type | Section | Content |
|---|---|---|
| 1 | OFFSET_INDEX | sparse: `{varint logicalOffsetDelta, varint chunkByteOffset}` per ~entry interval |
| 2 | TIME_INDEX | sparse: `{varint timestampDelta, varint chunkByteOffset}` |
| 3 | PRODUCER_SNAPSHOT | Kafka producer-snapshot bytes, opaque to the storage layer |
| 4 | ABORTED_TXN_INDEX | Kafka txn-index entries for this chunk's range |
| 5 | CRC_RANGES | crc32c per 4 MiB of data region (serves verified reads + scrub + sendfile-without-hop-CRC) |
| 6 | STATS | base/last logical offsets, min/max timestamps, batch count |

Fixed 64-byte trailer at EOF: `u64 dataLength · u64 footerStart · u32 sectionCount · u32 incompatFlags · u32 footerCrc · u32 dataCrc · ... · u32 magic "SFTR"`. (`dataCrc` is the crc32c over the whole data region — the whole-file checksum that makes repaired replicas byte-comparable.) Read path: read last 64 bytes → section directory → sections. New section types are additive (`compat` by default); readers skip unknown types.

**A valid trailer is the authoritative SEALED signal.** Recovery classifies a chunk SEALED from a valid trailer (`MAGIC_TRAILER` + `footerCrc` + `dataCrc`) alone — `writeEpoch` from the header, length from `trailer.dataLength`, `fenceEpoch` meaningless post-seal — so a sealed chunk needs **no `.meta` sidecar** (§11.3). The classifier wins even over a stale sidecar still reading `OPEN` in the pre-reclaim window. The one subtlety: in the default non-fsync mode a freshly sealed chunk briefly retains its ledger, and "valid trailer + ledger" is ambiguous against an OPEN chunk whose payload is crafted to *look* like a footer; a ledger-coverage disambiguator resolves it — a real sealed chunk's ledger ends exactly at `trailer.dataLength`, while a footer-shaped open chunk's ledger covers the whole appended payload (strictly past `dataLength`, since the footer physically occupies `[dataLength, EOF)`).

### 11.3 Sidecar and integrity ledger (per OPEN chunk, node-local, never replicated)

Both are **open-chunk-only** structures. A sealed chunk is fully self-describing from its own `.chunk` (header + footer + trailer, §11.2), so it carries **neither `.meta` nor `.j`** — a cold sealed chunk is a single file. At the target scale (~100M mostly-cold sealed chunks) this removes ~100M sidecar inodes and their per-chunk create + `force` + dirent-fsync.

- **Sidecar `<chunk>.meta`** — 512 bytes, single-sector atomic rewrite: `{magic "SMET", u16 ver, i32 writeEpoch, i32 fenceEpoch, u64 lastKnownDO (advisory), u8 state, u32 crc}`. Holds the fencing state that must survive restart (§5.4); `fenceEpoch` is recoverable *only* from here (a torn/lost fence epoch could resurrect a fenced writer), which is why open chunks keep it. Written only at state transitions (create/fence/seal/recovery/clean-shutdown), never on the append hot path. It is dropped when the chunk durably seals (sealed state is reconstructed from the trailer, §11.2); a clean-shutdown `.meta` for a sealed-but-not-yet-reclaimed chunk is a transient artifact the next recovery deletes.
- **Integrity ledger `<chunk>.j`** — append-only, one fixed 24-byte entry per `APPEND`: `{u64 endOffset, u32 payloadCrc, i32 writeEpoch, u32 reserved, u32 entryCrc}`. The `payloadCrc` is the **writer-origin per-record digest** — the client's frame payload CRC (§10.2), stored verbatim; the node does **not** originate a competing value, and verify-on-ingest is the frame decoder's existing check (free). Crash recovery scans the ledger, verifies the last entries' digests against the data file, and truncates the data file to the last verified `endOffset` — **the data node never parses payload bytes, even to recover.** When `fsyncOnAck=true`, data and ledger are fsynced before ack; otherwise both flush lazily. Overhead ≈ 24 B per multi-KB..MB append, written sequentially. The ledger is deleted once the chunk seals (the footer's CRC_RANGES supersede it).

The per-record digest is the **only** durable structure that gives record-granular torn-tail detection for an OPEN chunk: the running whole-chunk/range aggregate (§11.2 CRC_RANGES) is in-memory-only during the open window and 4 MiB-granular at seal, so it cannot find the exact last-intact-record boundary. "Remove server-side CRC" therefore means remove **origination** only — the node keeps re-verify (scrub, repair-import, recovery) and the structural CRCs (header, trailer, sidecar, per-entry `entryCrc`).

**Per-chunk file footprint:** open = `.chunk` + `.meta` + `.j`; **sealed = `.chunk`** (down from `.chunk` + `.meta`).

### 11.4 Checkpoint file content (broker-defined, rides ordinary CHECKPOINT files)

File-level header `{u32 magic "SCKP", u16 version}` then append-only entries `{u32 length, u16 type, u16 version, payload, u32 crc}`. v1 types: `SNAPSHOT` (full sparse index + producer state + open-txn state), `INDEX_DELTA`, `PRODUCER_DELTA`, `TXN_DELTA`, `DO_NOTE`, `ROLL_NOTE` (chunk chain advanced), `EPOCH_NOTE`. Replay = last `SNAPSHOT` + subsequent deltas; a fresh `SNAPSHOT` is written when deltas exceed a threshold, and the file is rotated via the normal create/swap/delete path. Unknown entry types: skip if flagged optional, abort replay if flagged required (1 bit in `type`'s high bit).

### 11.5 Format evolution rules

1. Magic + version + CRC on everything; feature masks decide ignore / read-only / refuse.
2. Fixed layouts are immutable per version; evolution is additive (tagged blocks, new sections, new entry types) until a version bump.
3. A node advertises its maximum on-disk format in `REGISTER_NODE`; the metadata plane will not place new-format-requiring chunks on old nodes (placement filter), which is what makes **mixed-version pools safe during rolling upgrades**.
4. Sealed chunks are immutable forever: format converters never rewrite in place — a format migration, if ever needed, is an explicit relocation (copy-as-new + descriptor swap), the same mechanism as repair.
5. Within a major version, read support for every prior format version is mandatory.

## 12. Strata client API

The client library the broker embeds (JVM, since the broker is Kafka-derived); the API is deliberately **Kafka-free** — files, bytes, offsets, epochs — preserving the tenant-agnostic discipline (product doc §5). Semantics below are normative for any future implementation.

```java
interface StrataClient extends AutoCloseable {
  static StrataClient connect(ClientConfig config);
  StrataFile create(FileSpec spec);
      // spec: StrataNamespace namespace, StrataPath path
  StrataFile open(StrataNamespace namespace, StrataPath path);
  StrataFile openById(FileId id);             // admin/internal escape hatch
  void delete(StrataNamespace namespace, StrataPath path);
  void delete(List<FilePath> paths);          // FilePath{StrataNamespace namespace, StrataPath path}
  void delete(StrataFile file);               // deletes by the handle's immutable FileId
  void deleteById(FileId id);                 // admin/internal escape hatch
  void deleteById(List<FileId> ids);
}

interface StrataFile {
  FileId id();
  StrataNamespace namespace();
  StrataPath path();
  Appender openForAppend();
  Reader openForRead();
  SealInfo recoverAndSeal();
      // fence all chunks -> seal-recover the open tail (§7.3) -> returns sealed length
}

interface Appender extends AutoCloseable {
  CompletableFuture<AppendAck> append(ByteBuffer data);
      // pipelined; completes on quorum ack; AppendAck{long endOffset, long durableOffset}
  long durableOffset();
  CompletableFuture<SealInfo> seal(FooterSections sections);   // caller supplies index/snapshot sections
}

interface Reader extends AutoCloseable {
  CompletableFuture<ReadResult> read(long offset, int maxBytes);
      // ReadResult{ByteBuffer data, long durableOffset, boolean endOfFile}
  CompletableFuture<FooterView> footer();                       // sealed files only
}
```

The user-facing logical name is `(StrataNamespace, StrataPath)`, not a local filesystem path.
`StrataNamespace` is a single identifier (for example one Kafka cluster or tenant) and is the future
ACL/quota root. `StrataPath` is absolute within that namespace (`/topic/partition/segment`),
canonical (no trailing slash, empty segment, `.`, or `..`), and unique only inside its namespace
while a file is live. Parent path segments are explicit namespace nodes so future ACLs can be
attached above individual files; file identity remains the immutable, globally unique `FileId`.

**Guarantees.**
- *Single writer:* at most one live `Appender` per file per epoch; a higher epoch anywhere kills lower-epoch appenders permanently (`FencedException`; the appender is dead, not retriable).
- *Ordering & durability:* appends complete in order; a completed append is on ≥2 independent nodes; `durableOffset` is monotonic.
- *Offset model:* offsets are **file-logical**; chunk boundaries are invisible to the caller. The client maps file offset → (chunk, chunk offset) from cached chunk metadata.
- *Reads:* `read` never returns bytes above the durable offset for open files; sealed files serve any replica, any range.

**Client-internal responsibilities** (so the broker code stays simple): chunk roll at ~1 GB + create-ahead; seal-and-roll on persistent replica failure (§7.2), where failure detection includes a **per-replica append timeout** — a black-holed connection (silent packet loss) must fail that one replica into the roll path, never stall the whole appender; DO tracking, piggybacking, and idle beacons; replica selection and hedged reads (hedge to a second replica after a p99-based timeout — directly exploits any-replica equivalence); metadata caching with invalidation on `CHUNK_NOT_FOUND`; retry/backoff with idempotency (append retries are safe: `OFFSET_GAP`/duplicate-suffix detection via `STAT_CHUNK` resync).

**Non-goals:** the client never inspects payload bytes; no Kafka types in the API surface; no buffering policy beyond pipelining (batching is the caller's concern — the broker already batches).

## 13. Kafka compatibility layer

| Kafka mechanism | Strata disposition |
|---|---|
| Wire protocol, all client APIs | kept (fork); compatibility tracked by running Kafka's client/system test suites against Strata in CI |
| `ReplicaManager`, `UnifiedLog`, ISR, truncation, follower fetch | **removed** — replaced by `StrataClient`/`StrataFile` (§12) + tail cache |
| Group coordinator, transaction coordinator | kept, run in brokers; their internal compacted topics are ordinary Strata files; coordinator failover = leadership move |
| Producer state / snapshots | per-segment checkpoint file (§11.4) + sealed footer; rebuilt on failover from checkpoint + bounded tail replay |
| Offset & time index, aborted-txn index | checkpoint file (open) / footer (sealed) |
| Leader-epoch cache (KIP-320 fencing for clients) | append-only per-partition epoch→offset map in the checkpoint; never truncated because logs never diverge |
| `metadata.version` feature gates | kept for the broker's own Kafka features; Strata's metadata-plane records and SCP APIs are versioned independently (§10.6), not via Kafka's `metadata.version` |

## 14. Invariants

1. One writer per chunk per epoch; data nodes enforce monotonic epoch fencing locally.
2. Producer-acked data exists on ≥2 replicas; tolerates any single data-node failure per replica set.
3. Consumers never read beyond the durable offset.
4. Chunk state transitions are authoritative only at metadata commit; physical state converges via reconciliation.
5. A chunk exists in metadata before any byte of it exists on any disk (commit-before-write).
6. Sealed content is immutable, checksummed, self-describing; all replicas are byte-identical including header and footer (whole-file CRCs comparable). A sealed chunk is a single self-describing file — no sidecar, no ledger; its SEALED state is reconstructed from a valid trailer (§11.2).
7. Brokers hold no durable state; any broker can lead any partition after bounded (≤ one checkpoint interval) replay.
8. No data-path operation requires a metadata-plane round trip.
9. The system never moves data except: repair, decommission, operator-invoked relocation. Never as a balancing policy.
10. Payload bytes are never re-encoded anywhere between producer and consumer: broker → SCP frame suffix → chunk data region → SCP frame suffix → consumer.
11. Every persistent structure and every frame carries magic, version, and CRC; readers ignore unknown-optional and refuse unknown-incompat.
12. The durable per-record digest is **writer-origin**: the node stores the writer's frame payload CRC verbatim and never originates a competing value; it retains only the right to re-verify (scrub/repair/recovery) and the structural CRCs of its own on-disk layout.
13. File identity is namespace-scoped: the global key is `(namespace, fileId, index)`; a per-namespace owner-assigned `long` `fileId` is unique only within its namespace, and ids are never reassigned (high-water recovery, §4.2).

## 15. Observability (v1 minimum)

Produce-path latency decomposed by stage (broker processing / storage append / quorum wait); per-replica append latency (feeds slow-node detection and hedging thresholds); chunk create/seal latency; under-replicated chunk count and repair backlog bytes (the durability-exposure gauges); DO lag; metadata commit latency and queue depth; heartbeat misses; tail-cache hit ratio; per-node fullness vs. weight; SCP-level: per-opcode rates, error-code counters, in-flight bytes vs. negotiated cap.

## 16. Testing strategy

- **Compatibility:** Apache Kafka's client matrix + system tests run against Strata in CI; per-API-version conformance tracked as a release gate.
- **MetadataStore SPI conformance:** one behavioral suite (chunk lifecycle, registration/lease, command delivery, repair orchestration, failover) run against all three backends — the ZooKeeper-direct store, the namespace-log store, and an independent in-memory reference (§4.4) — the parity harness that keeps the SPI honest and backend-swappable.
- **Chunk protocol:** deterministic simulation of the append/fence/seal/recovery state machine (single-process, virtual time, exhaustive fault schedules) — this is where seal-recovery edge cases are found cheaply.
- **Wire/format robustness:** frame fuzzing against the SCP server; golden-corpus tests (recorded frames, footers, checkpoints from every released version replayed against every newer version); cross-version interop matrix (client N±1 vs server N) as a CI gate, enforcing §10.6.5.
- **Crash safety:** torn-write injection on the data file + ledger (§11.3) across power-cut points; recovery must converge to a verified prefix.
- **Chaos:** Jepsen-style fault injection on real clusters: node kills mid-seal, metadata failover during repair, asymmetric partitions between broker and individual replicas.
- **Performance gates:** flat-p99 claim verified continuously (both durability modes); repair throughput vs. pool size; zero-copy path regression (no payload copies, asserted via allocation profiling).

## 17. Open questions

1. **DO staleness bound** — empty-`APPEND` beacons cover idle partitions; the cadence and the maximum staleness a direct reader may observe need a number.
2. **KIP-392 direct-read** — exact Fetch subset a data node must implement; session/quota handling without broker mediation. v1 ships broker-proxied; this is the v1.x decision.
3. **Intra-namespace metadata sharding** — sharding is by namespace from day one (rendezvous over controllers); at what per-namespace descriptor count / commit rate a single hot namespace's metadata log must itself be split across controllers is open (the split-mode hook exists, the threshold doesn't).
4. **Segment-roll policy defaults** — balancing chunk-count inflation (metadata sizing) against failover replay bound for low-throughput partition fleets.
5. **Data-node language/runtime** — decision owed before repo bootstrap; constraint: predictable tail latency under mixed read/append load; must implement SCP + §11 formats from this spec alone.
6. **Security baseline** — TLS posture per link (mandatory inter-node?), at-rest encryption (per-chunk envelope vs. volume-level), FIPS story — needs a design pass before the first regulated-industry conversation.
7. **Fork baseline** — which Kafka version to track first, and the merge cadence policy.
8. **Flow control** — v1 uses static per-connection caps from `HELLO`; whether repair traffic needs dynamic credit-based flow control to protect foreground p99 at scale is unproven either way.
9. **Compression** — reserved in the frame flags; whether `FETCH_CHUNK` (repair/relocation) benefits enough to justify it, given producers already compress batches.
10. **Metadata-log retention cadence** — the per-namespace open-log compaction threshold and the system-file orphan-GC sweep run on constants today; production tuning is open. (Non-blocking copy-on-write compaction — freeze under lock, encode/write off-lock, CAS the manifest — has landed, so large-namespace compaction no longer holds the namespace write lock across the encode.)
11. **Group commit for `fsyncOnAck`** — RESOLVED in v0 (§5.3): per-chunk coalesced forces with a 5 ms accumulation window and deferred append acks. Measured at window 256 on a shared-SSD laptop: p50 2,941 ms → 75 ms (39×), throughput 0.1 → 3.2 MB/s (32×). Remaining tunable: the accumulation window should become a config and be re-calibrated on production hardware (per-node devices interfere less than a shared laptop SSD).
12. **Shared per-volume digest log** — the open-chunk `.j` ledgers (§11.3) are per-chunk, so N actively-written chunks are N concurrently-fsync'd digest files (write fan-out 2N: data + ledger). A shared, append-only, digest-only log **per volume** (the bookie-journal shape, ~24 B/record, data stays in `.chunk`) would cut fan-out to N+1 and let one group-commit fsync amortize the digest barrier across all open chunks on the device. Deferred: it adds a WAL-shaped component, couples open-chunk recovery (replay the shared log), and needs shared-log compaction to drop sealed chunks' entries. Worth it only if the open-chunk fsync ceiling is measured to bind; the `.j` only ever covers the bounded open working set. (One log *per device* — sharding within a volume serializes barriers on the same hardware and erodes the amortization.)
13. **Aggregate digest via CRC-combine** — the node still makes one byte pass per append for the running whole-chunk/4 MiB-range aggregate (§11.2). Deriving it from the now-writer-origin per-record digests via GF(2) `crc32_combine` would let the node touch **no** bytes for CRC at all. Deferred for two real obstacles: arbitrary-length record CRCs don't combine to 4 MiB-range-aligned CRCs (the range-boundary problem), and combined whole-chunk CRCs would be identical across replicas, degenerating the seal-divergence vote (SealVotes, which relies on each replica CRC-ing its own bytes independently).
14. **Open-chunk single-file footprint (header mutable sector)** — sealed chunks are already one file (§11.3); the open `.meta` could be folded into a reserved, separately-CRC'd 512 B sector inside the 4096 B chunk header (in-place atomic single-sector overwrite), making an open chunk `.chunk` + `.j` only. Lower priority than the sealed win: the open `.meta` set is bounded by write concurrency (thousands, not 100M), so the value is create-path churn + footprint uniformity, and it adds fence-epoch atomicity care (the sector must sit *outside* the header CRC and carry its own) that the sealed-`.meta` removal does not.

---

## Appendix A — Design rationale: why this storage layer fits Kafka

The storage layer is not a general-purpose filesystem that happens to host Kafka — every design decision derives from a Kafka semantic:

| Kafka semantic | Storage-layer consequence |
|---|---|
| Single writer per partition, fenced by leader epoch | Epoch-fenced quorum append; no consensus protocol in the data path |
| Fetch = contiguous bytes from an offset | Byte-addressed chunks, batches stored as opaque bytes; zero-copy from media to consumer socket; catch-up reads can bypass the leader (sealed chunks unconditionally; open chunks behind the durable offset, §6) |
| Segments are immutable after roll | Sealed chunks are self-describing, any-replica readable, freely relocatable; retention = file delete, with immediate and exact space reclaim |
| Compaction is Kafka's own read-rewrite-swap | Expressed as ordinary file creates/deletes — the storage layer never learns compaction exists; compacted topics self-balance as a free side effect |
| Consecutive segments expire together | Background work is scheduled at seal time, cache-warm — never forced by a capacity watermark |
| Correctness state is bounded and derived | Checkpoint + bounded tail replay; recovery cost independent of partition size |

The general-purpose alternatives each fail one of these tests. Object stores fail latency and per-request economics for small, high-frequency appends — and an on-prem object store fails twice: it cannot serve the open-chunk path, and reaching its economics requires continuously moving data from disk to blob, rebuilding the tiering tax described in the product doc §1 (where one exists, it can be an optional sealed-chunk placement target — not the storage layer). HDFS fails tail latency and the metadata model. BookKeeper fails byte-contiguity, write amplification, and space reclaim — Appendix B.

## Appendix B — Why not BookKeeper?

The natural internal challenge: a quorum-replicated, disaggregated log store already exists (BookKeeper), with a Kafka protocol layer (KoP). The answer: three of BookKeeper's foundational decisions — each rational for its original workload (multi-tenant, millions of small entries, lowest-latency fsync) — invert into liabilities under Kafka's workload (single writer, large sequential batches, file-granularity lifecycle, contiguous-byte reads).

| | BookKeeper (under KoP) | Strata file service |
|---|---|---|
| **Addressing model** | (ledgerId, entryId) — record-oriented; every entry's location must be tracked | byte offset — "the address is the offset"; location is arithmetic |
| **Location index** | RocksDB (LSM) per bookie, written on the entry hot path | sparse index appended in-band (checkpoint file / sealed footer); no KV engine anywhere in the data path |
| **Local write path, per logical byte** | journal write + entry log write (2x), plus index write and its LSM compaction rewrites | single chunk append — the chunk *is* the WAL (integrity ledger ≈ 24 B/append, not a data copy) |
| **Space reclaim on delete** | entry-log GC must scan shared logs and **rewrite surviving entries**; reclaim is delayed and unpredictable | segment file unlink; reclaim is immediate, exact, and free |
| **Background IO taxes** | entry-log GC + RocksDB compaction, competing with foreground IO | none in steady state — sealed data ages in place |
| **Jitter sources on the write path** | journal fsync contention; RocksDB write stalls correlated with load | quorum absorbs single-node stalls; no compaction-coupled engine in the path |
| **Cold read path** | per-entry index lookups; bytes interleaved across ledgers — no contiguous range read, no sendfile | one contiguous byte range from one replica; zero-copy to socket |

Two of these are what operators meet in production:

**The RocksDB tax.** Record addressing forces an external entry→location index, and BookKeeper chose an LSM engine written synchronously with ingest. LSM compaction throttles writers when it falls behind — and the debt is correlated with load, so stalls land in your peak. Byte addressing removes the category: Strata's only index is a sparse offset map, itself append-only data, never compacted, never on the tail path.

**The deletion tax.** Bookies interleave all ledgers into shared entry logs, so reclaiming a deleted ledger's space requires GC that rewrites *other* ledgers' surviving entries; one long-lived ledger pins an entire entry log. For Kafka — where the dominant storage event is "a retention window of large segments expires every day" — this makes the system's cheapest operation its most expensive. Strata's one-segment-one-file mapping makes deletion a file unlink.

> **Net write path, per logical byte (before replication):** BookKeeper ≈ 2x at ingest (journal + entry log) + LSM index rewrites + GC-time rewrites at deletion — each amplification layer carrying its own jitter source and tuning surface. Strata = 1x + a KB-scale index append; deletion free.

None of this criticizes BookKeeper on its own terms — these mechanisms serve Pulsar's multi-tenant, small-entry, fsync-sensitive workload well. The argument is narrower: Kafka's workload renders each of them unnecessary while still charging full price for them.
