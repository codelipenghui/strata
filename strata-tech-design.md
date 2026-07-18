# Strata — Technical Design

**Status:** Draft v0.4 (2026-07-11) · **Audience:** engineering · **Companion:** [strata-product-definition.md](strata-product-definition.md) (positioning, competitive landscape, value propositions — not repeated here)

This is the single source of truth for the design. It subsumes the former standalone design notes — metadata scaling (§4), the writer-origin per-record digest and the chunk-file footprint reduction (§11) — which are folded into the sections below; deferred/rejected variants of those are recorded in §17.

The storage-engine v0 in this repository is implemented in Java 21. Sections that describe the Kafka-derived broker or later placement modes remain target v1/v1.x design; where the current v0 API or implementation differs, this document calls that out explicitly instead of presenting future work as already shipped.

---

## 1. Purpose and scope

This document specifies how Strata is built: components, protocols, wire and on-disk formats, state machines, failure handling, and the seams between Kafka-derived code and new code. It records decisions at the level an implementing engineer needs, and explicitly marks what is still open (§17).

Scope: v1 plus the v1.x media work. Erasure coding, multi-protocol tenants, and geo-replication are out of scope (see product doc §6–§7 for why).

## 2. Architecture overview

The target system has three services and deployment shapes:

| Service | Role | Codebase | Durable state | Deployment (K8s) |
|---|---|---|---|---|
| **Broker** | Kafka protocol; partition leadership; group/transaction coordinators | Kafka fork (storage stack replaced) | none | Deployment; no PVCs; optional ephemeral NVMe for read cache |
| **Data node** | chunk persistence and serving | Java 21, no Kafka code | chunk files on local disks | StatefulSet; local PVs; topology labels (zone/rack/host) |
| **Metadata plane** | Strata MetadataStore: ZooKeeper consensus root (nodes, namespace ownership, manifests, IDs) + per-namespace metadata log owned by the namespace owner | Java 21, no Kafka code | ZooKeeper ensemble; per-namespace metadata-log + snapshot system files (replicated Strata files) | controller processes (any node) + a ZooKeeper ensemble of 3 or 5 |

Communication paths (protocol per link — see §10.1):

```
Kafka clients ──(Kafka wire protocol)──> Broker
Broker        ──(SCP control: create/seal/lookup)──> Metadata plane
Broker        ──(SCP: append/read/fence/seal)──> Data nodes
Data node  ──(SCP control: register/heartbeat)──> Metadata plane
Metadata owner ──(SCP: verify-chunks)──> Data node
Data node <──(SCP: fetch-chunk)──> Data node      [repair/relocation only]
Controller ──(ZooKeeper)──> consensus root              [cluster state, leadership, manifests]
```

**Codebase strategy.** Only the **broker** is derived from the Apache Kafka codebase — fork-and-replace, the path AutoMQ has proven in production — for compatibility economics: the group and transaction coordinators live inside the broker, so a clean-room broker would reimplement the hardest compatibility surface. The broker's storage stack is replaced with a Strata client; everything below the broker is new code. The **metadata plane and the data nodes contain no Kafka code**: both are Java 21 services, with the metadata plane exposing Strata's own `MetadataStore` over a ZooKeeper consensus root plus per-namespace metadata logs (§4). Strata's broker-facing operations are SCP control opcodes (§4.3, §10.4). Current v0 uses a fixed frame/API version with golden compatibility coverage; feature-level and per-opcode negotiation remain target rolling-upgrade work (§10.6, §17.18).

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
- **File identity** — `fileId` is a `long` (the BookKeeper `ledgerId` shape, rendered as 16-hex), not a UUID. A file's global identity is `(namespace, fileId)`; a chunk's is `(namespace, fileId, index)`, because the 12-byte wire/header `ChunkId` carries only `fileId + index` and relies on its namespace context. The namespace-log backend assigns user-file ids from a per-namespace owner high-water, so the same numeric id may occur in different namespaces. The current flat ZooKeeper prototype instead uses a global CAS file-id counter. System metadata-file ids use their own small consensus-root counter (§4.5).
- **Chunk** — the unit of replication and placement, **2 GiB by default in v0 (configurable)**, identified by `chunkId = (fileId, index)` (12 bytes on the wire/header: `u64 fileId, u32 index`). States: `OPEN → SEALED → DELETING`. Record batches cross the storage layer as opaque bytes; the address is the byte offset.
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

- **Sealed chunk footer** (self-describing; on every replica): current nodes always synthesize per-range CRCs and storage stats. The section envelope also accepts optional caller-supplied offset/time indexes, producer snapshots, and aborted-transaction indexes, but the current public appender passes no such payload; those broker-defined sections remain an integration capability. Full layout: §11.2.

## 4. Metadata plane

The metadata plane is Strata's own — new code, no Kafka. It manages **storage metadata only**: data nodes, files, chunks, and namespaces. (Kafka topic/partition metadata, leadership, and the group/transaction coordinators are the broker layer's concern; a Kafka-derived broker is a *client* of this plane, not part of it.) Metadata is split across two tiers behind a single **`MetadataStore` SPI** (§4.4):

- **A ZooKeeper consensus root** for cluster-wide state — small, slow-changing, globally consistent.
- **A per-namespace metadata log** for file and chunk state — large, fast-changing, sharded across controllers and owned per namespace.

A single elected cluster leader (Curator `LeaderLatch`) coordinates global maintenance. When current v0 is
started with `STRATA_CONTROLLER_SHARDING=true`, namespace *ownership* is computed independently by rendezvous
hashing over the static `STRATA_CONTROLLER_ENDPOINTS` list. Without that opt-in, the configured ownership list
is empty and each serving controller follows the non-sharded/global-leader path. Persisted membership and
liveness-aware reassignment remain open (§4.5, §17.17).

### 4.1 Cluster/system state (ZooKeeper root)

Held directly in ZooKeeper under `/strata`, guarded by version-CAS:

- **Node registry** — `NodeRecord { nodeId, incarnationId, endpoints, topology{zone,rack,host}, capacityBytes, state: REGISTERED|DRAINING|DEAD }`; registration, leases, and incarnation fencing. `SUSPECT` is a derived in-memory lease state inside dead-grace, not a persisted `NodeRecord` value.
- **Shared cluster-liveness snapshot** — the elected cluster controller periodically publishes `ClusterLiveNodes { publishedAtMs, entries{NodeRecord, freeBytes} }` to the root. A sharded namespace owner, which has no data-node heartbeat channel of its own, merges that snapshot into its local placement and repair view; direct in-memory observations win, and snapshots older than one lease plus two dead-grace windows are ignored. This is placement/repair input, not persisted namespace-owner membership or automatic owner failover.
- **Namespace ownership schema** — assignment records and generations exist in the SPI/root codec, but current v0 serving uses statically configured controller endpoints and does not persist or fail over those assignments automatically (§4.5, §17.17).
- **Per-namespace manifest** — the version-CAS pointer to a namespace's current metadata snapshot + open log (the linearizable barrier for metadata-log compaction, modeled in `tla/MetadataManifestCAS.tla`).
- **Metadata epochs and ID allocation**, and the **descriptors of the metadata-log / snapshot system files** (which are themselves replicated Strata files — §4.2).

This state is small (cluster membership plus one manifest/epoch per namespace) and changes slowly, so a consensus root sized for coordination, not data volume, is the right tool.

### 4.2 Per-namespace file/chunk metadata (the namespace log)

Each namespace's file and chunk metadata — `FileRecord { fileId, namespace, path, state, … }` and the `ChunkDescriptor`s under it — lives in that namespace's own **metadata log**: an ordered, durable log of metadata mutations plus periodic snapshots, **stored as a replicated Strata file** (chunks on data nodes, the same machinery user data uses), **owned and served by the namespace's owner**. Recovery rebuilds the in-memory derived indexes (`file → chunks`, `node → chunks` for repair, per-node usage for placement) from the published snapshot + log tail. This state scales with retained data (~1M descriptors/PB), not with namespace count — which is exactly why it is sharded out of the global root and onto per-namespace owners. The log is kept bounded by a background compaction sweep (snapshot + roll once the open log passes a size threshold) and a generation-based GC of system files orphaned by a crash between file-create and manifest publish. The intended tombstone truncation condition is both a published snapshot covering the deletion and an elapsed retry-retention floor. Current `sweepOwnedNamespaceTombstones`, however, uses only a `System.currentTimeMillis()` cutoff before appending `TombstoneSwept`; it does not verify snapshot publication. The two-part rule is therefore a target safety condition, not a current guarantee (§17.20). `tla/MetadataTombstoneSweep.tla` models both rules, while `tla/MetadataIdempotency.tla` models the retained idempotency fence.

The namespace's owner assigns each file's id from a monotonic `nextFileId` **high-water carried in the namespace snapshot** (not `max(live files)`, which a swept tombstone would forget and risk reusing). Assignment rides the existing single-writer manifest fence — no global allocator on the hot path. On restart or repository reacquisition, the configured owner loads the snapshot's `nextFileId` and replays the tail; assign-then-append is safe because a crash after assigning N but before `FileCreated(N)` is durable leaves no file at N, so reissuing N is harmless — the "id reuse after recovery" correctness anchor (§4.5, invariant §14.13). Automatic reassignment to a successor controller is not implemented in v0 (§17.17).

The snapshot also carries each file's CAS version so a version token read before restart/reacquisition cannot alias a freshly recovered in-memory counter. Restoring from a snapshot must preserve the file's mutation lineage; otherwise a stale retry could compare against a reset counter and replay an old file update over an intervening owner mutation.

### 4.3 RPC surface (SCP control)

Brokers and data nodes both reach the metadata plane over **SCP control opcodes** (§10.4) served by an SCP listener on each controller — one protocol stack for every client. A request for a namespace this controller does not own is answered `NOT_LEADER` carrying the owner's endpoint; the owner-aware client caches `namespace → owner` and routes directly, re-resolving only on that redirect.

| API | Caller | Notes |
|---|---|---|
| `CreateFile` / `DeleteFiles` | broker | namespace-log owners assign from a per-namespace high-water; the flat ZooKeeper prototype uses a global CAS id; retention is caller policy and the plane orchestrates physical deletion |
| `CreateChunk(fileId)` → `{chunkId, replicas[replicationFactor], writeEpoch}` | broker | placement decided here (§8); commit-before-write (§9.2 relies on this) |
| `SealChunk(chunkId, length, crc)` | broker | the metadata commit is the authoritative seal point |
| `LookupFile` / `LookupPath` | broker/tool | served from current metadata state; clients cache file descriptors and sealed chunks are immutable |
| `REGISTER_NODE` / `NODE_HEARTBEAT` / `VERIFY_CHUNKS` | data node / namespace owner | data nodes register and heartbeat; heartbeat **responses** carry commands (`REPLICATE`, `DELETE`, `DRAIN`). The namespace owner initiates owner-pull `VERIFY_CHUNKS` against data nodes; there is no node inventory push |

Range-oriented `LookupChunks(namespace, fileId, offsetRange)` and paged `ListFiles(namespace, pageToken)` are target RPCs, not current opcodes. The internal `MetadataStore.listFiles()` method is an unpaged implementation/admin seam and must not be presented as that external contract.

Availability: if a namespace's owner or the consensus root is briefly unavailable, already-open data-path appends and reads can continue from cached descriptors; chunk creates/seals retry or wait. Produce stalls when an open chunk fills without a successor. There is no current ownership-change operation to queue: sharded service waits for its same configured owner to recover (§4.5). Heartbeat grace periods must exceed expected control-plane recovery time so a brief outage never triggers repair.

### 4.4 The `MetadataStore` SPI

Node-registry, namespace-assignment, and manifest logic sits behind a **`MetadataStore` SPI** so the consensus root is swappable and unit-testable. Three implementations run the SPI conformance suite (§16): the ZooKeeper-direct store (§4.1), the namespace-log store (the per-namespace owner over a ZooKeeper root, §4.2), and an independent in-memory reference. The SCP control surface (§10.4) and client behavior are identical regardless of which `MetadataStore` is bound; a backend's watch/notification mechanism is never exposed as protocol semantics. Delivery does depend on controller role: the global/non-sharded repair lane queues `REPLICATE`/`DELETE` commands on heartbeat responses, while a sharded namespace owner that lacks that channel calls `EXEC_REPLICATE`/`DELETE_CHUNKS` directly on the data node.

### 4.5 Scaling to 100M+ files (namespace sharding specifics)

The two-tier split (§4.1/§4.2) is what lets one Strata cluster hold **100M+ files across many tenants** without one consensus record per file, chunk, replica, or tombstone. The load-bearing specifics:

- **Current v0 configured ownership.** With `STRATA_CONTROLLER_SHARDING=true`, ownership is rendezvous-hashed over the immutable `STRATA_CONTROLLER_ENDPOINTS` configured in each process and `replicaSet[0]` is always selected. Without that flag, v0 retains non-sharded/global-leader behavior. There is no persisted production assignment, liveness-aware reassignment, operator move path, or automatic owner failover yet (§17.17). In sharded mode one namespace is served by exactly one configured owner while that endpoint is available; intra-namespace sharding is deferred (§17.3).
- **Two-layer id generation.** User-file ids come from the owner's in-snapshot `nextFileId` high-water (§4.2) — no consensus round on the create path. The system `strata-meta` namespace cannot host its own counter (its `nextFileId` lives in meta-log files that are themselves `strata-meta` files — it would recurse), so system-file ids come from a small CAS counter in the consensus root scoped to `strata-meta` (low volume).
- **Per-namespace leader recovery barrier.** On acquiring a namespace, a controller CAS-increments its metadata epoch — fencing prior metadata-log writers at the storage layer, so two controllers that briefly open the same namespace during a membership settle are ordered by epoch (the later, higher-epoch opener wins) — then stays `RECOVERING`: load the manifest, recover/seal the open log tail to its durable end, replay the snapshot + tail, rebuild derived indexes, and re-verify the epoch is still current before serving (`ACTIVE`). Ordinary reads may use the cached `ACTIVE` repository, but destructive orphan confirmation cannot: `CONFIRM_ORPHAN` synchronizes with the consensus root and requires the local manifest value, version, and epoch to match while the repository is locked. A superseded cache, missing manifest, or root-read failure therefore fails closed instead of authorizing deletion or auto-reacquiring a still-higher epoch. (The publication barrier is modeled in `tla/MetadataManifestCAS.tla` and `tla/MetadataTwoLeaderFencing.tla`; the destructive-read schedule is pinned by the two-owner `CONFIRM_ORPHAN` regressions.)
- **Stale-epoch re-acquire (converge, don't wedge).** A controller that still owns a namespace but holds a repository cached at a now-stale epoch would otherwise have every metadata-log append fenced (`FENCED_EPOCH`) and retry forever — a permanent wedge with the in-flight file never finalizing. Instead, a fenced append makes the owner evict the stale repository and re-open the namespace (fresh epoch + the barrier above), then replay the mutation once; the retry is bounded to a single attempt, so a genuine ownership disagreement surfaces as the fence rather than an epoch-thrash loop.
- **Derived indexes, not sources of truth.** `file → chunks`, `(namespace, path) → fileId`, and `node → chunks` are materialized from the metadata log for lookup/listing/repair and rebuilt on recovery; the log is the only authority.

## 5. Write path

### 5.1 Produce flow

1. Producer → leader broker (standard Kafka produce, unchanged on the wire).
2. Broker validates (epoch, producer state, transaction state) — in-memory, as Kafka does.
3. Broker appends the batch to the partition's open chunk: fan-out `APPEND` (§10.3) to all replicas selected by the file's `replicationFactor`.
4. Each replica: checks `writeEpoch ≥` its locally stored max epoch for the chunk (else `FENCED_EPOCH`), enforces contiguity (`baseOffset` must equal local end, else `OFFSET_GAP`; leader retries), appends payload bytes verbatim to the chunk file, appends an entry to the chunk's integrity ledger (§11.3) carrying the **writer-supplied per-record digest** (the frame's payload CRC, already verified by the frame decoder — the node stores it rather than originating its own), optionally fsyncs (§5.3), acks.
5. Broker acks the producer at the file's `ackQuorum` replica acks. With the default policy (`replicationFactor=3`, `ackQuorum=2`), latency = second-fastest replica.
6. Broker inserts the batch into its in-memory tail cache and advances the **durable offset (DO)** = highest contiguous byte acked by at least `ackQuorum` replicas. DO is piggybacked on the next `APPEND` (replicas learn DO with one-round lag — the BookKeeper LAC pattern). Each advance also debounces an automatic empty-payload `APPEND`: a later payload re-arms the timer, while a stream that stays idle for `durableBeaconIdleMs` (100 ms by default) publishes its final DO without another caller append or seal. On healthy pinned replica connections, DO staleness is therefore bounded by `durableBeaconIdleMs` plus one `APPEND` round trip (`callTimeoutMs` bounds that RPC attempt); the beacon carries no payload and adds no integrity-ledger entry.

### 5.2 Chunk lifecycle driven by the leader

- **Roll** at the configured chunk limit (2 GiB by default in v0), on the record-count guard, or on persistent replica failure (§7.2): seal current, `CreateChunk` next. Create-ahead near the roll threshold remains a target optimization; the current v0 appender creates the successor after sealing the current chunk.
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

- **Detection:** missed heartbeats → derived `SUSPECT`; dead grace expiry → persisted `DEAD`. Current defaults are a 10 s lease and 30 s dead grace. Production deployments should raise the grace above expected control-plane recovery and pod-reschedule time; the current defaults are development defaults, not a claim that a 90-second reschedule is safe. The persisted identity is the `nodeId + incarnationId` pair, so a process returning on the same local PVs re-registers as the same node and cancels the clock before expiry.
- **Repair (sealed chunks):** the namespace's owner is the repair coordinator for its chunks (the cluster leader drives only cross-namespace orphan reconciliation). From its per-namespace `node → chunks` reverse index it enqueues every affected chunk, prioritized by exposure (chunks at 1 surviving replica jump the queue), picks new targets via standard placement (§8), and asks the **new targets** to pull via `FETCH_CHUNK` from surviving replicas. The global/non-sharded lane delivers `REPLICATE` in heartbeat responses; a sharded non-global owner uses direct `EXEC_REPLICATE`. Copies are checksum-verified, throttled per node; completion = atomic descriptor swap. Because placement scatters chunks, repair parallelism scales with pool size (100 TB node, 50-node pool ≈ ~2 TB per node ≈ ~3 h at 200 MB/s throttle).
- **Open chunks (fast path):** the partition leader, on losing a write-set replica past a short threshold, seals the chunk at DO (running seal-recovery if needed) and rolls to a fresh replica set — write path recovers in one chunk-create; the sealed remainder enters normal repair. There is no in-place ensemble patching; **roll is the ensemble change.**
- **Decommission** = the same machinery with the node as a willing copy source (`DRAINING`).

### 7.3 Seal recovery (open chunk, leader died or replica lost)

The recovering leader must establish the durable prefix without the old leader's in-memory DO:

1. `FENCE(chunkId, E+1)` on all reachable replicas — the response carries each replica's local end offset and last-known DO. Recovery needs at least the file's configured `ackQuorum` of trustworthy responses; under the default AQ=2 policy fewer than two makes the chunk unavailable until a replica returns, while RF=1/AQ=1 legitimately needs one.
2. Start from `max(piggybacked DO)` across reachable replicas — everything below is known quorum-durable.
3. Scan forward batch-by-batch (`READ_LEDGER` boundaries + `READ_RECOVERY` bytes, both stamped with the exact persisted recovery fence epoch): if a batch exists on **any** reachable replica (CRC-valid), re-replicate it to quorum and advance; stop at the first offset found on none. A replica whose `FENCE` is transport-unreachable retains possible-holder credit (#29); this includes a call timeout or mid-call disconnect, because the replica may have applied the fence even though recovery received no response. An empty descriptor endpoint, `FENCE -> CHUNK_NOT_FOUND`, or otherwise untrusted/malformed FENCE outcome never supplies agreement for promoting a sub-quorum continuation. `CHUNK_NOT_FOUND` proves only that the node has no currently installed handle, not that it never acknowledged the tail. If an unresolved classification could combine with remaining claims to reach `ackQuorum` above the chosen seal point, or if a fenced replica claimed bytes there but its ledger/bytes cannot be verified, abort recovery with `SEAL_RECOVERY_BLOCKED` instead of choosing truncation or promotion; the chunk stays OPEN for a retry. Thread interruption is caller cancellation and aborts the entire recovery immediately; it is never classified as evidence about one replica. The same gate runs before accepting a lone SEALED replica when another reachable replica claims a higher end, protecting recovery from partial destructive seals left by an older attempt. Ordinary `READ` is not used here because it clamps an open chunk to the durable high watermark and therefore hides the tail recovery must re-prove.
4. Seal at the verified stop point; write footers; commit `SealChunk`.

Property: any producer-acked batch existed on at least `ackQuorum` replicas; with failures below the policy's tolerated threshold, at least one holder is reachable and verifiable, so step 3 preserves it. With the default policy, this is the same tolerance as Kafka RF=3/acks=all/min.isr=2. If holder evidence exists but is temporarily unreadable, corrupt, or inseparable from an unresolved descriptor/current-absence outcome, aborting is safer than floor-sealing because acked-data loss would be permanent while a later recovery can retry verification. A persistent local read fault or unresolved classification therefore remains visible as `SEAL_RECOVERY_BLOCKED`; an operator may explicitly accept the potential data loss for exactly one namespaced chunk by setting `strata.recovery.unsafeSealOverrideChunks`/`STRATA_RECOVERY_UNSAFE_SEAL_OVERRIDE_CHUNKS` to the exact `namespace:fileId.index` key from the error or log (the `fileId` is the 16-character hex form, e.g. `test:0000000000000030.0`). Recovery exclusively reserves the matching token at the ambiguity gate, evicts any unreadable blockers, and forces the remaining quorum to seal at the verified point without treating unresolved replicas as agreement. The token is consumed only after the chunk metadata seal commits; failure before that commit releases the reservation and logs that the configured token remains available for retry. Environment-based overrides are consumed only inside the running process, so operators must also remove the deployment env var after the targeted recovery completes.

### 7.4 Metadata quorum failure

Losing a ZooKeeper member while quorum remains is handled by the ensemble and is invisible to the data path. In non-sharded mode, the global leader latch can move service to another controller. In current sharded v0, however, losing a configured namespace owner makes that namespace's boundary operations unavailable until the same configured endpoint recovers; automatic successor reassignment is not implemented (§17.17). Losing the ZooKeeper quorum still leaves the already-open data path running (§4.3), while chunk-boundary operations wait for ensemble recovery. Control operations are retried idempotently where their opcode carries an operation id.

## 8. Placement

Current v0 `CreateChunk` placement is capacity-weighted random selection over nodes that remain registered inside the dead-grace window. `DEAD` and `DRAINING` nodes are excluded; a `SUSPECT` node inside dead-grace remains eligible so a metadata heartbeat stall does not prematurely remove a reachable data node, and the subsequent node RPC is the liveness probe. Replicas are host-anti-affine, nodes with no free bytes are excluded, and weights derive from free capacity. Configurable rack/zone failure domains and a fullness watermark remain target policy work.

Anti-correlation is a first-order p99.9 concern (quorum latency is bounded by the second-slowest replica): placement must avoid co-locating replicas on shared hosts or shared burst-credit storage.

## 9. Background work

### 9.1 Retention

Leader evaluates retention (it owns the policy and the segment timeline) → `DeleteFiles` → metadata marks chunks `DELETING`, then removes records after replica confirmation (or reconciliation timeout). The global leader queues `DELETE` commands via heartbeats; a sharded non-global namespace owner sends direct `DELETE_CHUNKS`. Space reclaim is file unlink: immediate and exact.

### 9.2 Reconciliation (scrub)

Reconciliation runs in both directions, owner-driven rather than as a node push. **Missing/corrupt replicas:** a namespace owner periodically pulls `VERIFY_CHUNKS` from each node holding one of its sealed replicas and diffs the node's report against the descriptor — a replica missing, short, or CRC-mismatched past a grace is dropped and re-repaired (the last live replica is never dropped). **Orphans on disk:** each node runs a local orphan GC — a sealed chunk that no owner has verified within a grace becomes a suspect, and the node walks configured metadata endpoints in order using `CONFIRM_ORPHAN` until one returns an authoritative verdict. The confirmation connection presents the `metadata` HELLO role only for the reserved `strata-meta` namespace; ordinary namespace checks continue to use the `tool` role. Unlike ordinary `LOOKUP_FILE`, this dedicated destructive lane returns no verdict until authority has been synchronized and checked: a namespace-log owner must exactly match the consensus manifest, while a root-backed responder must complete an authoritative root read and still hold global leadership. A stale endpoint is skipped and an unreachable/uncertain owner never triggers deletion. The response distinguishes a referenced chunk (keep), a present file that omits the chunk/node (orphan), and a missing file (which must repeat across GC passes); its positive owner epoch is durably recorded as a volume-bound floor before the verdict is accepted, so restart cannot make an older first response authoritative. A present-file orphan can delete in the same pass after the final confirm, while a missing-file pending key is discarded if the chunk leaves the suspect set. If confirmed-orphan volume crosses the namespace or node rolling-window threshold, or if process-lifetime cumulative confirmed deletes reach the namespace or node cap, orphan GC opens a latching breaker, emits error logs/metrics, and stops deleting in that scope until node restart; small ordinary orphan cleanup below the thresholds still drains. The cumulative caps assume orphan GC is a low-volume backstop while routine retention reclaim flows through owner-direct `DELETE_CHUNKS`. The **commit-before-write invariant** (a chunk exists in metadata before any byte is sent — §4.3) is what makes ordinary orphan deletion safe: an old on-disk chunk absent from the current consensus-validated live descriptor is either state that has since been deleted or data that was never committed as live; it is not part of current live state.

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
- **Connection lifecycle:** current SCP is plaintext, unauthenticated TCP; TLS must be supplied by an external deployment layer or future transport work. First frame MUST be `HELLO`. Pipelining is allowed after handshake and bounded by configured client/server request/byte limits; the server advertises `maxInflightBytes`, but the current client does not consume it as negotiated flow control. All `APPEND`s for a chunk are pinned to one connection generation for ordering. An ambiguous reconnect is not replayed or resynchronized: that replica fails out of the appender and the client seals/rolls according to its quorum policy.
- **Implementation note (v0 finding):** on virtual-thread runtimes, never hold a monitor (`synchronized`) across blocking I/O or while response handlers may contend for it — blocked virtual threads inside monitors pin their carriers (JDK ≤23), and enough pinned carriers stall every virtual thread in the process. Use `ReentrantLock`, and keep blocking work/response callbacks off transport event-loop threads (a handler blocked on a lock must never stall frame dispatch for the very response its lock-holder is waiting on).

**Handshake.** `HELLO` request: `u16 frameVersionMin, u16 frameVersionMax, u8 clientKind (1 broker | 2 data-node | 3 metadata | 4 tool), u64 featureBits, string clientId`. Response: `u16 chosenFrameVersion, u64 featureBits, u32 nodeId (0 if n/a), uuid incarnationId, u32 maxFrameBytes, u64 maxInflightBytes, array{u16 opcode, u16 maxApiVersion}`. Current v0 accepts frame version 1, emits feature bits `0`, advertises API version 1 for every opcode, and sends every request with API version 1; the client decodes but does not retain/use the opcode map for selection. Per-opcode and feature negotiation are target behavior (§10.6, §17.18). Controller operations addressing the reserved `strata-meta` namespace require the `metadata` HELLO role; every other role is rejected with non-retriable `PRECONDITION_FAILED`. This role is a typed protocol distinction, not authentication on the current unauthenticated SCP transport.

### 10.3 Data-plane opcodes

| Opcode | Name | Request header (v1 fixed fields) | Response (after errorCode) | Payload |
|---|---|---|---|---|
| 0x0001 | `HELLO` | see §10.2 | see §10.2 | — |
| 0x0010 | `OPEN_CHUNK` | chunkId{u64 fileId, u32 index}, i32 writeEpoch, u8 fsyncOnAck (0/1), u64 expectedMaxBytes, u64 createdAtMs, namespace | — | — |
| 0x0011 | `APPEND` | chunkId, i32 writeEpoch, u64 baseOffset, u64 durableOffset, namespace; tag 0: recovery append | u64 endOffset | log bytes (may be empty = DO beacon); the frame's payload CRC is stored as the per-record digest (§10.2) |
| 0x0012 | `READ` | chunkId, u64 offset, u32 maxBytes, namespace | u64 localEndOffset, u64 durableOffset | chunk bytes |
| 0x0013 | `FENCE` | chunkId, i32 fenceEpoch, namespace | i32 persistedFenceEpoch, u64 localEndOffset, u64 lastKnownDO, u8 state | — |
| 0x0014 | `STAT_CHUNK` | chunkId, namespace | u8 state, u64 localEndOffset, u64 lastKnownDO, i32 writeEpoch, i32 fenceEpoch, u64 sealedLength, u32 sealedCrc | — |
| 0x0015 | `SEAL_CHUNK` | chunkId, i32 writeEpoch, u64 dataLength, namespace; tag 0: u64 ownerEpoch | u64 finalLength, u32 chunkCrc | footer bytes (§11.2) |
| 0x0016 | `DELETE_CHUNKS` | varint n, chunkId×n, namespace; tag 0: u64 ownerEpoch | array{chunkId, u16 code} | — |
| 0x0017 | `FETCH_CHUNK` | chunkId, u64 offset, u32 maxBytes, namespace; tag 0: u64 ownerEpoch | u64 fileLength, u8 state | raw chunk-file bytes (header block + data + footer) |
| 0x0018 | `PING` | — | — | — |
| 0x0019 | `READ_LEDGER` | chunkId, u64 fromOffset, namespace; tag 1: i32 recoveryEpoch (required, >0) | array{u64 endOffset, u32 payloadCrc, i32 writeEpoch} | — |
| 0x001A | `READ_RECOVERY` | chunkId, u64 offset, u32 maxBytes, namespace; tag 1: i32 recoveryEpoch (required, >0) | u64 localEndOffset, u64 durableOffset | open-chunk bytes through local end, including the undurable tail |
| 0x001B | `EXEC_REPLICATE` | `REPLICATE` command{commandId, chunkId, sources, priority, expectedCrc, expectedLength, namespace}; tag 0: u64 ownerEpoch | — | — |
| 0x001C | `VERIFY_CHUNKS` | namespace, verifierEndpoint, array chunkIds; tag 0: u64 ownerEpoch | array{chunkId, present, state, length, crc} | — |

`READ_LEDGER` exposes integrity-ledger entries above an offset: seal recovery (§7.3) needs per-append
boundaries without parsing the opaque payload. `READ_LEDGER` and `READ_RECOVERY` require a positive
`recoveryEpoch` that exactly matches the chunk's persisted fence epoch; a lower epoch returns
`FENCED_EPOCH`, while a higher epoch returns `PRECONDITION_FAILED` until `FENCE` persists it. Empty
for sealed chunks (the ledger is deleted at seal; recovery never needs it then). `SEAL_CHUNK`
semantics refined in v0: caller
footer sections are optional, and the node ALWAYS computes CRC_RANGES + STATS itself — recovery-
sealed chunks therefore stay byte-identical across replicas with no caller input.

Notes: `FETCH_CHUNK` is distinct from `READ` so it can run in a separate QoS/throttle class (repair must never starve foreground reads) and because it copies the *file* representation (header + footer included) — repaired sealed replicas are byte-identical, their data-region CRCs are comparable, and tests may compare the full image bytes. It carries the namespace owner's epoch and participates in the source node's process-local owner watermark; epoch 0 is accepted only before the node observes a positive owner epoch for that namespace. Separately, only a consensus-validated `CONFIRM_ORPHAN` response may advance the volume-bound owner floor used across restart — persisting an epoch supplied by an arbitrary owner/tool RPC on today's unauthenticated SCP link would turn a process-scoped denial of service into a durable one. The node trusts that its configured endpoint addresses reach real controllers because it initiates those connections; SCP itself is plaintext and unauthenticated, so any transport security belongs to the deployment network. The node-local sidecar and ledger (§11.3) are never copied; the puller starts fresh ones.

### 10.4 Control-plane opcodes (data node ↔ metadata plane)

| Opcode | Name | Request (v1) | Response (after errorCode) |
|---|---|---|---|
| 0x0101 | `REGISTER_NODE` | u32 nodeId, uuid incarnationId, array endpoints, topology{string zone, rack, host}, array{u64 capacityBytes}, u32 onDiskFormatMax, u64 featureBits | u32 nodeId, u64 sessionEpoch, u32 heartbeatIntervalMs, u32 leaseMs |
| 0x0102 | `NODE_HEARTBEAT` | u32 nodeId, uuid incarnationId, u64 sessionEpoch, array{u64 usedBytes, u64 freeBytes}, u32 repairQueueDepth | u64 leaseValidUntilMs, array commands |
| 0x020B | `CONFIRM_ORPHAN` | namespace, chunkId, u32 nodeId | bool fileExists, bool referencedByNode, u64 ownerEpoch |

Durability reconciliation is the owner pulling `VERIFY_CHUNKS` (0x001C, a data-plane opcode served by the node — §9.2) plus the node's local orphan GC. Orphan GC obtains its destructive verdict through `CONFIRM_ORPHAN` (0x020B); 0x020A remains deliberately unassigned. There is no node-push inventory report.

v0 additions: `NODE_HEARTBEAT` requests carry tagged field 0 `completedCommands` (array{u64
commandId, u16 status}) so the repair coordinator learns command completion on the next heartbeat;
`REPLICATE` params gained `expectedLength`. The client↔metadata APIs ride SCP in the 0x02xx
range (`CREATE_FILE` 0x0201, `CREATE_CHUNK` 0x0202, `SEAL_CHUNK_META` 0x0203, `LOOKUP_FILE` 0x0204,
`DELETE_FILES` 0x0205, `SEAL_FILE` 0x0206, `ABORT_CHUNK_META` 0x0207, `LOOKUP_PATH` 0x0208,
`ALLOCATE_WRITER_EPOCH` 0x0209, `CONFIRM_ORPHAN` 0x020B) — this is the
broker, tool, and internal-metadata control surface to the metadata plane (§10.1).

Command encoding in heartbeat responses is `u64 commandId, u8 type, body`. v1 bodies are `REPLICATE{chunkId, sources: array{nodeId, endpoint}, u8 priority, expectedCrc, expectedLength, namespace}` (pull via `FETCH_CHUNK`), `DELETE{chunkIds, namespace}`, and `DRAIN{}` (node enters DRAINING; serve reads/copies, refuse `OPEN_CHUNK`). Per-command `ownerEpoch` values are carried separately in heartbeat-response tagged field 1 as `commandId → ownerEpoch`. Current v0 rejects an unknown command type during decode. Ignoring and reporting additive unknown commands is a target rolling-upgrade behavior, not a current compatibility guarantee (§17.18).

Node identity: the assigned `nodeId` + `incarnationId` are persisted in an identity file on every data volume; re-registration presents them, which is what makes pod rescheduling onto the same disks identity-preserving (§7.2).

### 10.5 Error codes (shared, append-only)

`0 OK · 1 UNKNOWN_OPCODE · 2 UNSUPPORTED_VERSION · 3 FENCED_EPOCH · 4 OFFSET_GAP (retriable) · 5 CHUNK_NOT_FOUND · 6 CHUNK_SEALED · 7 CHUNK_ALREADY_EXISTS · 8 OUT_OF_SPACE · 9 CRC_MISMATCH · 10 NOT_REGISTERED · 11 LEASE_EXPIRED · 12 THROTTLED (retriable; tag 0 is a generic numeric detail and current server overload responses use the applicable in-flight cap) · 13 CORRUPT_CHUNK · 14 INTERNAL (retriable) · 15 NOT_LEADER (retriable; v0) · 16 NO_CAPACITY (retriable; v0) · 17 FILE_NOT_FOUND (v0) · 18 FILE_SEALED (v0) · 19 PRECONDITION_FAILED (v0) · 20 METADATA_RECOVERING (retriable; v0) · 21 SEAL_RECOVERY_BLOCKED (retriable; v0)`. Codes 0–999 reserved for protocol-level errors; 1000+ for future domain extensions. Codes are never renumbered or reused.

Error precedence (v0 finding, locked as protocol semantics): **the fence check dominates the state
check** — a deposed writer appending to a recovery-sealed chunk gets `FENCED_EPOCH` (permanent
death for the appender), never `CHUNK_SEALED` (which reads as "roll and continue").

### 10.6 Wire compatibility rules

Current v0's enforceable contract is narrower than the target rolling-upgrade design:

1. The frame preamble is frozen at `frameVersion=1`, and requests use `apiVersion=1`.
2. Fixed header fields for a given opcode are immutable. Additive optional data uses tagged fields; decoders ignore unknown tags.
3. Error codes, opcodes, command types, and tags are append-only namespaces. Unknown command types are currently rejected, not ignored.
4. Golden-frame/corpus and released-artifact tests preserve the fixed-v1 bytes and behavior that are selected for compatibility runs.

The HELLO opcode map, mutually selected API versions, feature-bit activation, safe handling of unknown
commands, and an N±1 rolling support window remain target requirements (§17.18); they must not be claimed
until clients actually consume the negotiation result.

## 11. On-disk formats

Current v0 uses structure-specific guards rather than one universal envelope. Chunk headers and sidecars carry magic/version/CRC fields and strictly require format version 2; the chunk header also has feature masks. The fixed trailer carries magic and CRC fields but no independent version. Footer section envelopes carry a section-version field, but current readers do not enforce it. Integrity-ledger entries carry an entry CRC but no magic, format version, or feature masks. A uniform ext4-style `compat` / `roCompat` / `incompat` discipline and prior-version readers remain target evolution work (§11.5, §17.19).

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
| 1 | OFFSET_INDEX | optional caller section; intended sparse logical-offset → chunk-byte-offset entries |
| 2 | TIME_INDEX | optional caller section; intended sparse timestamp → chunk-byte-offset entries |
| 3 | PRODUCER_SNAPSHOT | optional caller section; Kafka producer-snapshot bytes, opaque to storage |
| 4 | ABORTED_TXN_INDEX | optional caller section; Kafka transaction-index entries |
| 5 | CRC_RANGES | node-generated: crc32c per 4 MiB of data region (verified reads + scrub) |
| 6 | STATS | node-generated 12 bytes: `{u64 dataLength, u32 ledgerEntryCount}` |

Fixed 64-byte trailer at EOF: `u64 dataLength · u64 footerStart · u32 sectionCount · u32 incompatFlags · u32 footerCrc · u32 dataCrc · ... · u32 magic "SFTR"`. (`dataCrc` is the crc32c over the whole data region; it is not a checksum of the header/footer image.) Read path: read last 64 bytes → section directory → sections. New section types are additive (`compat` by default); readers skip unknown types.

**A valid trailer is the authoritative SEALED signal.** Recovery classifies a chunk SEALED from a valid trailer (`MAGIC_TRAILER` + `footerCrc` + `dataCrc`) alone — `writeEpoch` from the header, length from `trailer.dataLength`, `fenceEpoch` meaningless post-seal — so a sealed chunk needs **no `.meta` sidecar** (§11.3). The classifier wins even over a stale sidecar still reading `OPEN` in the pre-reclaim window. The one subtlety: in the default non-fsync mode a freshly sealed chunk briefly retains its ledger, and "valid trailer + ledger" is ambiguous against an OPEN chunk whose payload is crafted to *look* like a footer; a ledger-coverage disambiguator resolves it — a real sealed chunk's ledger ends exactly at `trailer.dataLength`, while a footer-shaped open chunk's ledger covers the whole appended payload (strictly past `dataLength`, since the footer physically occupies `[dataLength, EOF)`).

### 11.3 Sidecar and integrity ledger (per OPEN chunk, node-local, never replicated)

Both are **open-chunk-only** structures. A sealed chunk is fully self-describing from its own `.chunk` (header + footer + trailer, §11.2), so it carries **neither `.meta` nor `.j`** — a cold sealed chunk is a single file. At the target scale (~100M mostly-cold sealed chunks) this removes ~100M sidecar inodes and their per-chunk create + `force` + dirent-fsync.

- **Sidecar `<chunk>.meta`** — 512 bytes: `{magic "SMET", u16 ver, i32 writeEpoch, i32 fenceEpoch, u64 lastKnownDO (advisory), u8 state, u32 crc}`. It is rewritten through a fresh temporary file, optionally forced, then replaced with an atomic move when the filesystem supports it; the implementation falls back to a normal replace when atomic move is unavailable and forces the directory for synchronous transitions. Holds the fencing state that must survive restart (§5.4); `fenceEpoch` is recoverable *only* from here (a torn/lost fence epoch could resurrect a fenced writer), which is why open chunks keep it. Written only at state transitions (create/fence/seal/recovery/clean-shutdown), never on the append hot path. It is dropped when the chunk durably seals (sealed state is reconstructed from the trailer, §11.2); a clean-shutdown `.meta` for a sealed-but-not-yet-reclaimed chunk is a transient artifact the next recovery deletes.
- **Integrity ledger `<chunk>.j`** — append-only, one fixed 24-byte entry per `APPEND`: `{u64 endOffset, u32 payloadCrc, i32 writeEpoch, u32 reserved, u32 entryCrc}`. The `payloadCrc` is the **writer-origin per-record digest** — the client's frame payload CRC (§10.2), stored verbatim; the node does **not** originate a competing value, and verify-on-ingest is the frame decoder's existing check (free). Crash recovery scans the ledger, verifies the last entries' digests against the data file, and truncates the data file to the last verified `endOffset` — **the data node never parses payload bytes, even to recover.** When `fsyncOnAck=true`, data and ledger are fsynced before ack; otherwise both flush lazily. Overhead ≈ 24 B per multi-KB..MB append, written sequentially. The ledger is deleted once the chunk seals (the footer's CRC_RANGES supersede it).

The per-record digest is the **only** durable structure that gives record-granular torn-tail detection for an OPEN chunk: the running whole-chunk/range aggregate (§11.2 CRC_RANGES) is in-memory-only during the open window and 4 MiB-granular at seal, so it cannot find the exact last-intact-record boundary. "Remove server-side CRC" therefore means remove **origination** only — the node keeps re-verify (scrub, repair-import, recovery) and the structural CRCs (header, trailer, sidecar, per-entry `entryCrc`).

**Per-chunk file footprint:** open = `.chunk` + `.meta` + `.j`; **sealed = `.chunk`** (down from `.chunk` + `.meta`).

### 11.4 Checkpoint file content (broker-defined, rides ordinary CHECKPOINT files)

File-level header `{u32 magic "SCKP", u16 version}` then append-only entries `{u32 length, u16 type, u16 version, payload, u32 crc}`. v1 types: `SNAPSHOT` (full sparse index + producer state + open-txn state), `INDEX_DELTA`, `PRODUCER_DELTA`, `TXN_DELTA`, `DO_NOTE`, `ROLL_NOTE` (chunk chain advanced), `EPOCH_NOTE`. Replay = last `SNAPSHOT` + subsequent deltas; a fresh `SNAPSHOT` is written when deltas exceed a threshold, and the file is rotated via the normal create/swap/delete path. Unknown entry types: skip if flagged optional, abort replay if flagged required (1 bit in `type`'s high bit).

### 11.5 Format evolution rules

1. Current readers strictly validate header/sidecar format version plus their structure-specific magic/CRC fields; trailer integrity is fixed-layout magic/CRC, footer section versions are currently ignored, and only the chunk header carries feature masks.
2. Fixed layouts are immutable per version; evolution should be additive (new sections or entry types) until a version bump.
3. **Target rolling-upgrade rule (not implemented in v0):** a node advertises its maximum on-disk format in `REGISTER_NODE`, and placement filters nodes that cannot write the required format. Until the controller persists and enforces that capability, mixed on-disk-format pools are not declared safe.
4. Sealed chunks are immutable forever: format converters never rewrite in place — a format migration, if ever needed, is an explicit relocation (copy-as-new + descriptor swap), the same mechanism as repair.
5. **Target, not implemented in v0:** define and enforce section-version compatibility, and support intentional read-old/write-new behavior. Current header/sidecar codecs reject versions other than 2, while footer section versions are not yet enforced.

## 12. Strata client API

The client library the broker embeds (JVM, since the broker is Kafka-derived); the API is deliberately **Kafka-free** — files, bytes, offsets, epochs — preserving the tenant-agnostic discipline (product doc §5). The signatures below are the current v0 contract in this repository; future asynchronous/footer APIs must be additive or explicitly versioned.

```java
interface StrataClient extends AutoCloseable {
  static StrataClient connect(ClientConfig config);
  StrataFile create(FileSpec spec);
      // spec: StrataNamespace namespace, StrataPath path
  StrataFile open(StrataNamespace namespace, StrataPath path);
  StrataFile openById(StrataNamespace namespace, FileId id);
  void delete(StrataNamespace namespace, StrataPath path);
  void delete(List<FilePath> paths);          // FilePath{StrataNamespace namespace, StrataPath path}
  void delete(StrataFile file);               // deletes by the handle's immutable FileId
  void deleteById(StrataNamespace namespace, FileId id);
  void deleteById(StrataNamespace namespace, List<FileId> ids);
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
  CompletableFuture<Long> append(ByteBuffer data);
      // pipelined; completes on quorum ack with this append's file-logical endOffset
  long durableOffset();   // sole durable query: current quorum-acked HWM (monotonic, weaker than recovery-durable)
  SealInfo seal();
}

interface Reader extends AutoCloseable {
  ReadResult read(long offset, int maxBytes);
      // ReadResult{ByteBuffer buffer, boolean endOfFile}; caller MUST close it
  void refresh();
}
```

The user-facing logical name is `(StrataNamespace, StrataPath)`, not a local filesystem path.
`StrataNamespace` is a single identifier (for example one Kafka cluster or tenant) and is the future
ACL/quota root. `StrataPath` is absolute within that namespace (`/topic/partition/segment`),
canonical (no trailing slash, empty segment, `.`, or `..`), and unique only inside its namespace
while a file is live. Parent path segments are explicit namespace nodes so future ACLs can be
attached above individual files; file identity is the immutable `(StrataNamespace, FileId)` tuple.

**Guarantees.**
- *Single writer:* at most one live `Appender` per file per epoch; a higher epoch anywhere kills lower-epoch appenders permanently (`FencedException`; the appender is dead, not retriable).
- *Ordering & durability:* appends complete in order; a completed append is acknowledged by the configured `ackQuorum` of distinct replicas; `durableOffset` is monotonic. The default RF=3/AQ=2 policy therefore places acknowledged data on at least two nodes, while valid RF=1/AQ=1 policies provide no single-node-failure tolerance.
- *Offset model:* offsets are **file-logical**; chunk boundaries are invisible to the caller. The client maps file offset → (chunk, chunk offset) from cached chunk metadata.
- *Reads:* `read` never returns bytes above the durable offset for open files; sealed files serve any replica, any range.

**Client-internal responsibilities** (so the broker code stays simple): roll at the configured byte/record limits; seal-and-roll on persistent replica failure (§7.2), where failure detection includes a **per-replica append timeout** — a black-holed connection (silent packet loss) must fail that one replica into the roll path, never stall the whole appender; DO tracking and piggybacking; replica selection; metadata caching and owner redirects; retry/backoff with idempotency. Create-ahead and p99-driven hedged reads remain target optimizations, not current v0 behavior.

**Non-goals:** the client never inspects payload bytes; no Kafka types in the API surface; no buffering policy beyond pipelining (batching is the caller's concern — the broker already batches).

## 13. Kafka compatibility layer

| Kafka mechanism | Strata disposition |
|---|---|
| Wire protocol, all client APIs | kept in the future broker fork; Kafka client/system-suite compatibility is a required target gate, but that broker and CI matrix are not part of this storage-engine repository (§16) |
| `ReplicaManager`, `UnifiedLog`, ISR, truncation, follower fetch | **removed** — replaced by `StrataClient`/`StrataFile` (§12) + tail cache |
| Group coordinator, transaction coordinator | kept, run in brokers; their internal compacted topics are ordinary Strata files; coordinator failover = leadership move |
| Producer state / snapshots | per-segment checkpoint file (§11.4) + sealed footer; rebuilt on failover from checkpoint + bounded tail replay |
| Offset & time index, aborted-txn index | checkpoint file (open) / footer (sealed) |
| Leader-epoch cache (KIP-320 fencing for clients) | append-only per-partition epoch→offset map in the checkpoint; never truncated because logs never diverge |
| `metadata.version` feature gates | kept for the broker's own Kafka features; Strata's metadata-plane records and SCP APIs are versioned independently (§10.6), not via Kafka's `metadata.version` |

## 14. Invariants

1. One writer per chunk per epoch; data nodes enforce monotonic epoch fencing locally.
2. Producer-acked data exists on at least the file's configured `ackQuorum` replicas. With the default RF=3/AQ=2 policy, one replica may be lost without losing the acknowledged quorum; RF=1/AQ=1 is valid but has no such tolerance.
3. Consumers never read beyond the durable offset.
4. Chunk state transitions are authoritative only at metadata commit; physical state converges via reconciliation.
5. A chunk exists in metadata before any byte of it exists on any disk (commit-before-write).
6. Sealed content is immutable, checksummed, self-describing; all replicas are byte-identical including header and footer. Data-region CRCs are comparable, while whole-image equality is established by comparing bytes rather than a stored whole-file CRC. A sealed chunk is a single self-describing file — no sidecar, no ledger; its SEALED state is reconstructed from a valid trailer (§11.2).
7. Brokers hold no durable state; any broker can lead any partition after bounded (≤ one checkpoint interval) replay.
8. No data-path operation requires a metadata-plane round trip.
9. The system never moves data except: repair, decommission, operator-invoked relocation. Never as a balancing policy.
10. Payload bytes are never re-encoded anywhere between producer and consumer: broker → SCP frame suffix → chunk data region → SCP frame suffix → consumer.
11. SCP frames and chunk-storage structures use the structure-specific version/integrity checks in §10–§11: frames and chunk/sidecar layouts are versioned and checksummed, while ledger entries carry an entry CRC. Current readers reject unsupported fixed versions; universal feature-mask and prior-format-read rules are targets, not current invariants.
12. The durable per-record digest is **writer-origin**: the node stores the writer's frame payload CRC verbatim and never originates a competing value; it retains only the right to re-verify (scrub/repair/recovery) and the structural CRCs of its own on-disk layout.
13. File identity is `(namespace, fileId)` and chunk identity is `(namespace, fileId, index)`. The namespace-log backend allocates user-file ids per namespace; the flat ZooKeeper prototype allocates numeric ids globally. Controller-assigned ids advance monotonically and are not reassigned (high-water/CAS-counter recovery, §4.2).

## 15. Observability (v1 minimum)

Produce-path latency decomposed by stage (broker processing / storage append / quorum wait); per-replica append latency (feeds slow-node detection and hedging thresholds); chunk create/seal latency; under-replicated chunk count and repair backlog bytes (the durability-exposure gauges); DO lag; metadata commit latency and queue depth; heartbeat misses; tail-cache hit ratio; per-node fullness vs. weight; SCP-level: per-opcode rates, error-code counters, in-flight bytes vs. negotiated cap.

## 16. Testing strategy

- **Compatibility (target broker integration):** Apache Kafka's client matrix + system tests must run against the future Strata broker; that broker and gate are not part of the current storage-engine repository.
- **MetadataStore SPI conformance:** the same file/path/node CRUD, namespace/listing, CAS, tombstone/sweep, reopen, stale-handle, and replacement-safety suite runs against the ZooKeeper-direct store, the namespace-log store, and an independent in-memory reference (§4.4). Chunk lifecycle, leases, command delivery, repair, and controller failover are covered by their focused unit/integration suites rather than this SPI harness.
- **Chunk protocol:** current unit, wire, recovery, and fault-injection tests exercise append/fence/seal/recovery schedules; `scripts/tlc.sh` automatically checks the `ChunkReplication` model. A single-process virtual-time exhaustive simulator remains target work.
- **Wire/format robustness:** current tests cover adversarial frames and a fixed-v1 golden corpus. Released-artifact replay runs when versions are explicitly selected. A client N±1/server N matrix is target work tied to real negotiation (§10.6, §17.18).
- **Crash safety:** targeted torn-tail and recovery injection covers the data file + ledger (§11.3), complemented by child-process restart tests; a systematic all-power-cut-point matrix remains target work.
- **Chaos:** current suites use containerized dependencies/Toxiproxy plus child-JVM process kills, restarts, and asymmetric link faults. A Jepsen-style external real-cluster harness remains target work.
- **Performance:** smoke tests and `StrataPerf` provide local evidence. Continuous flat-p99 gates in both durability modes, repair-throughput scaling gates, and allocation-profile assertions remain target work.

## 17. Open questions

1. **DO staleness bound — RESOLVED in v0:** `durableBeaconIdleMs` controls automatic empty-`APPEND` publication (100 ms by default); on healthy replica connections, direct-reader staleness is bounded by that idle interval plus one `APPEND` round trip.
2. **KIP-392 direct-read** — exact Fetch subset a data node must implement; session/quota handling without broker mediation. v1 ships broker-proxied; this is the v1.x decision.
3. **Intra-namespace metadata sharding** — sharding is by namespace in the opt-in sharded mode; the partitioning design and the descriptor-count/commit-rate threshold for splitting one hot namespace across controllers are both open.
4. **Segment-roll policy defaults** — balancing chunk-count inflation (metadata sizing) against failover replay bound for low-throughput partition fleets.
5. **Data-node language/runtime** — RESOLVED in v0: Java 21, Netty NIO transport, and serialized per-connection virtual-thread handler executors. The format and wire contracts remain language-neutral.
6. **Security baseline** — TLS posture per link (mandatory inter-node?), at-rest encryption (per-chunk envelope vs. volume-level), FIPS story — needs a design pass before the first regulated-industry conversation.
7. **Fork baseline** — which Kafka version to track first, and the merge cadence policy.
8. **Flow control** — v1 uses static per-connection caps from `HELLO`; whether repair traffic needs dynamic credit-based flow control to protect foreground p99 at scale is unproven either way.
9. **Compression** — reserved in the frame flags; whether `FETCH_CHUNK` (repair/relocation) benefits enough to justify it, given producers already compress batches.
10. **Metadata-log retention cadence** — IMPLEMENTED and configurable in v0: compaction bytes/cadence, system-file orphan GC, retention, read chunk size, and log roll size are exposed as `STRATA_CONTROLLER_LOG_*` settings. Remaining work is production calibration, not configuration plumbing. Non-blocking copy-on-write compaction (freeze under lock, encode/write off-lock, CAS the manifest) prevents large-namespace encoding from holding the namespace write lock.
11. **Group commit for `fsyncOnAck`** — RESOLVED in v0 (§5.3): per-chunk coalesced forces use a configurable adaptive accumulation window that tracks observed force duration, clamped to 1–50 ms by default, with deferred append acks. Remaining work is production-hardware calibration of those bounds.
12. **Shared per-volume digest log** — the open-chunk `.j` ledgers (§11.3) are per-chunk, so N actively-written chunks are N concurrently-fsync'd digest files (write fan-out 2N: data + ledger). A shared, append-only, digest-only log **per volume** (the bookie-journal shape, ~24 B/record, data stays in `.chunk`) would cut fan-out to N+1 and let one group-commit fsync amortize the digest barrier across all open chunks on the device. Deferred: it adds a WAL-shaped component, couples open-chunk recovery (replay the shared log), and needs shared-log compaction to drop sealed chunks' entries. Worth it only if the open-chunk fsync ceiling is measured to bind; the `.j` only ever covers the bounded open working set. (One log *per device* — sharding within a volume serializes barriers on the same hardware and erodes the amortization.)
13. **Aggregate digest via CRC-combine** — the node still makes one byte pass per append for the running whole-chunk/4 MiB-range aggregate (§11.2). Deriving it from the now-writer-origin per-record digests via GF(2) `crc32_combine` would let the node touch **no** bytes for CRC at all. Deferred for two real obstacles: arbitrary-length record CRCs don't combine to 4 MiB-range-aligned CRCs (the range-boundary problem), and combined whole-chunk CRCs would be identical across replicas, degenerating the seal-divergence vote (SealVotes, which relies on each replica CRC-ing its own bytes independently).
14. **Open-chunk single-file footprint (header mutable sector)** — sealed chunks are already one file (§11.3); the open `.meta` could be folded into a reserved, separately-CRC'd 512 B sector inside the 4096 B chunk header (in-place atomic single-sector overwrite), making an open chunk `.chunk` + `.j` only. Lower priority than the sealed win: the open `.meta` set is bounded by write concurrency (thousands, not 100M), so the value is create-path churn + footprint uniformity, and it adds fence-epoch atomicity care (the sector must sit *outside* the header CRC and carry its own) that the sealed-`.meta` removal does not.
15. **Mixed-version on-disk placement** — NOT IMPLEMENTED in v0 (§11.5.3): registration carries format/feature fields, but the controller does not yet persist or filter on them. The current node also reports format 1 while `ChunkFormats.FORMAT_VERSION` is 2. Resolve the advertisement and placement contract before using mixed-format rolling upgrades.
16. **Metadata-model automation** — the metadata TLA+ specs exist, but `scripts/tlc.sh` currently runs only `ChunkReplication.tla`. Add an explicit per-model matrix before treating the metadata specs as an automated gate; until then they are design models plus manually runnable evidence.
17. **Namespace-owner failover** — current sharded routing uses immutable configured endpoints and always selects `replicaSet[0]`; production serving does not persist assignments, evaluate owner liveness, or promote another replica. Add a durable assignment/membership lifecycle and an explicit handoff protocol before claiming automatic namespace failover.
18. **SCP rolling-version negotiation** — HELLO carries feature/opcode fields, but v0 advertises/sends fixed API version 1, feature bits are inactive, clients discard the opcode map, and unknown commands fail decode. Implement and test actual negotiation plus the N±1 matrix before declaring rolling wire compatibility.
19. **On-disk backward readers and feature masks** — current codecs accept only their current fixed version, and masks are not uniform across sidecars/ledgers. Define structure-by-structure migration and read-old/write-new behavior before mixed-format upgrades (§11.5, §17.15).
20. **Tombstone sweep publication fence** — current namespace-log sweeping uses a wall-clock TTL and appends `TombstoneSwept` without first proving a published snapshot covers the deletion. Add a monotonic retention basis plus a manifest/snapshot publication fence before claiming the two-part truncation rule in §4.2; `tla/MetadataTombstoneSweep.tla` exposes the difference between the current and target guards.

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
