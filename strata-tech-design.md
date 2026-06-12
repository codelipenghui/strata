# Strata — Technical Design

**Status:** Draft v0.2 (2026-06-10) · **Audience:** engineering · **Companion:** [strata-product-definition.md](strata-product-definition.md) (positioning, competitive landscape, value propositions — not repeated here)

---

## 1. Purpose and scope

This document specifies how Strata is built: components, protocols, wire and on-disk formats, state machines, failure handling, and the seams between Kafka-derived code and new code. It records decisions at the level an implementing engineer needs, and explicitly marks what is still open (§17).

Scope: v1 plus the v1.x media work. Erasure coding, multi-protocol tenants, and geo-replication are out of scope (see product doc §6–§7 for why).

## 2. Architecture overview

Three services, three codebases, three deployment shapes:

| Service | Role | Codebase | Durable state | Deployment (K8s) |
|---|---|---|---|---|
| **Broker** | Kafka protocol; partition leadership; group/transaction coordinators | Kafka fork (storage stack replaced) | none | Deployment; no PVCs; optional ephemeral NVMe for read cache |
| **Storage node** | chunk persistence and serving | new, no Kafka code; language TBD (storage-server-appropriate) | chunk files on local disks | StatefulSet; local PVs; topology labels (zone/rack/host) |
| **Metadata plane** | KRaft quorum: controller state machine + chunk-map state machine | Kafka fork (controller extended) | Raft logs + snapshots | StatefulSet of 3 or 5; NVMe |

Communication paths (protocol per link — see §10.1):

```
Kafka clients ──(Kafka wire protocol)──> Broker
Broker        ──(Kafka RPC framework, new API keys)──> Metadata plane
Broker        ──(SCP: append/read/fence/seal)──> Storage nodes
Storage node  ──(SCP control: register/heartbeat/inventory)──> Metadata plane
Storage node <──(SCP: fetch-chunk)──> Storage node      [repair/relocation only]
Metadata members ──(Raft)──> Metadata members           [quorum-internal only]
```

**Codebase strategy.** Broker and metadata plane are derived from the Apache Kafka codebase — fork-and-replace, the path AutoMQ has proven in production. Deriving the metadata plane is a *requirement* of the KRaft decision (using KRaft means running Kafka's controller code); deriving the broker is a *choice* made for compatibility economics — the group and transaction coordinators live inside the broker, so a clean-room broker would reimplement the hardest compatibility surface regardless of how it reached the metadata plane. KRaft's consensus core is consumed untouched; all extensions are additive (new record types, new RPC APIs, the chunk-map state machine as a new Raft-layer client — the same pattern AutoMQ used for its stream/object metadata). The single invasive change is removing ISR and replica assignment from the controller's partition model (§4.1) — the main source of upstream-merge friction. Strata's custom records and APIs sit behind their own feature-level lane (§4.3) so rolling upgrades stay safe. The Strata file service contains no Kafka code; its language is an open decision (§17.5). A v0 bootstrap path (§4.4) decouples the first implementation from the fork timeline entirely: the storage engine is built and validated against a ZooKeeper-backed metadata service behind the same interfaces, while the controller work lands in parallel.

Two load-bearing disciplines, stated once and assumed everywhere below:

1. **The data path never touches the metadata plane.** Metadata participates at chunk boundaries, leadership changes, and topology changes only.
2. **Storage nodes never read cluster metadata.** They report their own state and execute commands. Their metadata footprint is O(own state).

## 3. Data model

```
Topic-partition ──> segments (Kafka roll policy) ──1:1──> files ──> chunk chain
                                                          │
                    checkpoint file (per open segment) ───┘  (same file machinery)
```

- **File** — bounded append-only byte stream; the storage layer's only abstraction. API: create / fenced-append / seal / read / delete. Types: `LOG` (a Kafka segment) and `CHECKPOINT` (index + producer-state journal for an open segment). One segment = one file is *policy*, not a model invariant (chunk chains have no inherent length limit).
- **Chunk** — the unit of replication and placement, **~1 GB nominal**, identified by `(fileId, chunkIndex)`. States: `OPEN → SEALED → DELETING`. Record batches cross the storage layer as opaque bytes; the address is the byte offset.
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

Built on Kafka's KRaft layer: consensus core untouched; two state machines on two Raft logs (the RPC schemas already carry topic/partition arrays — the single-log assumption is wiring, not protocol).

### 4.1 Controller state machine (Kafka's, extended)

Keeps: topics, configs, ACLs, features, broker registration/heartbeats, leadership + leader epochs.
**Removed:** replica assignment, ISR state and shrink/expand logic, reassignment — Strata partitions are `{leaderBrokerId, leaderEpoch}` and nothing else. This deletion is the main upstream-merge friction point and gets the densest test coverage.
**Added record types:** `StorageNodeRecord { nodeId, incarnationId, endpoints, topology{zone,rack,host}, capacityBytes, state: REGISTERED|FENCED|DRAINING|DEAD }`.

### 4.2 Chunk-map state machine (new, second Raft log)

Holds `FileRecord { fileId, topicIdPartition, baseOffset, type }` and `ChunkDescriptor`s; maintains in-memory derived indexes: `file → chunks`, `node → chunks` (the repair reverse index), per-node usage for placement. Kept out of the broker-propagated metadata log deliberately: this state scales with retained data (~1M descriptors/PB), not partition count. Snapshotted like any KRaft state machine; target steady-state load is tens of commits/sec (chunk creates + seals + deletes), with bursts (node repair) batched.

### 4.3 RPC surface

Broker-facing APIs use Kafka's schema-generated framework (both ends are Kafka-derived); new API keys in the fork; versioned behind a Strata feature level (`metadata.version` discipline) from day one. Storage-node-facing APIs are SCP control opcodes (§10.4) served by an SCP listener on the metadata plane — so a storage node needs exactly one protocol stack.

| API | Caller | Transport | Notes |
|---|---|---|---|
| `CreateFile` / `DeleteFiles` | broker | Kafka RPC | retention is evaluated by the partition leader (it owns policy); metadata orchestrates physical deletion |
| `CreateChunk(fileId)` → `{chunkId, replicas[replicationFactor], writeEpoch}` | broker | Kafka RPC | placement decided here (§8); commit-before-write (§9.2 relies on this) |
| `SealChunk(chunkId, length, crc)` | broker | Kafka RPC | metadata commit is the authoritative seal point |
| `LookupChunks(topicIdPartition, offsetRange)` | broker | Kafka RPC | served from leader's in-memory state; brokers cache aggressively (sealed descriptors are immutable) |
| `REGISTER_NODE` / `NODE_HEARTBEAT` / `INVENTORY_REPORT` | storage node | SCP (§10.4) | heartbeat **responses** carry commands: `REPLICATE`, `DELETE`, `DRAIN` |

Availability semantics: if the quorum is unavailable, open-chunk appends and all reads continue (no metadata on the data path); chunk creates/seals/leadership changes queue. Produce stalls only when an open chunk fills without a successor — minutes at typical per-partition rates. Heartbeat grace periods must exceed metadata failover time so a control-plane blip never triggers repair.

### 4.4 Pluggable metadata backend and the v0 bootstrap path

The chunk-map and node-registry logic sits behind a **`MetadataStore` SPI** with two backends:

- **ZooKeeper (v0 — development and prototype only).** A thin active/standby metadata service (ZK leader election) persists chunk descriptors and the node registry to znodes and hosts the repair coordinator — the BookKeeper pattern (pluggable metadata drivers; elected auditor). For v0, the same service can assign partition leadership and epochs for the test harness or a minimal broker; the storage layer only ever sees monotonic epochs through the same client API.
- **KRaft (v1 — the product backend).** §4.1–§4.3.

Three rules make the backend swappable rather than load-bearing: (1) the SCP control surface (§10.4) and Strata client behavior are **identical across backends** — storage nodes and brokers cannot tell which is running; (2) commands ride heartbeat responses on both backends — ZK watches are never exposed as semantics; (3) ZooKeeper never appears in the Kafka fork — it exists only inside the v0 service, which is new code.

Why this exists: build order. The Strata file service, SCP, on-disk formats, seal recovery, and repair are buildable and testable before any controller surgery lands; the fork work proceeds in parallel and binds at v1. The named risk is calcification (Pravega shipped on its ZK scaffolding and never escaped). Exit criteria are explicit: v1 GA requires the SPI conformance suite green on **both** backends, KRaft at full parity, the ZK backend excluded from product builds, and **no metadata-migration promise for v0 clusters** — prototype data does not survive to v1, by design and by announcement.

## 5. Write path

### 5.1 Produce flow

1. Producer → leader broker (standard Kafka produce, unchanged on the wire).
2. Broker validates (epoch, producer state, transaction state) — in-memory, as Kafka does.
3. Broker appends the batch to the partition's open chunk: fan-out `APPEND` (§10.3) to all replicas selected by the file's `replicationFactor`.
4. Each replica: checks `writeEpoch ≥` its locally stored max epoch for the chunk (else `FENCED_EPOCH`), enforces contiguity (`baseOffset` must equal local end, else `OFFSET_GAP`; leader retries), appends payload bytes verbatim to the chunk file, appends an entry to the chunk's integrity ledger (§11.3), optionally fsyncs (§5.3), acks.
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
| Historical, broker-bypass | storage node (v1.x option) | KIP-392 `PreferredReadReplica` redirect: leader points the consumer at a storage node implementing a minimal Fetch subset for sealed data. Standard clients already honor this. Open chunks stay broker-served. |

Open-chunk reads are bounded by DO. The leader knows DO authoritatively; replicas know it with one-round lag (piggyback), which is sufficient for replica-served reads because anything ≤ piggybacked-DO is guaranteed quorum-durable. **Consumers never see un-quorum-acked bytes** — the analogue of Kafka's high-watermark rule, and the §17.1 item "DO propagation cadence" is about the *staleness bound*, not whether the rule exists.

## 7. Failure handling

### 7.1 Broker failure

Controller detects (broker session expiry) → reassigns leadership of its partitions across surviving brokers in one batch of `PartitionChange` records → each new leader, per partition: fence open chunks at epoch+1 → read latest checkpoint → replay ≤16 MB of open-chunk tail (batch headers only) to rebuild producer state and indexes → run seal-recovery if the open chunk needs it (§7.3) → accept produces. No data moves; cost independent of history size. Per-partition recovery is milliseconds; the fleet-wide work is parallel across all survivors.

### 7.2 Storage-node failure

- **Detection:** missed heartbeats → `SUSPECT`; grace period (≥ several minutes, must exceed metadata failover and pod-reschedule time) → `DEAD`. Node identity = `incarnationId` persisted on its volumes: a pod rescheduled onto the same local PVs re-registers as the same node and cancels the clock. Repair of 100 TB must never be triggered by a 90-second reschedule.
- **Repair (sealed chunks):** the chunk-map leader is the repair coordinator. From the `node → chunks` reverse index it enqueues every affected chunk, prioritized by exposure (chunks at 1 surviving replica jump the queue), picks new targets via standard placement (§8), and issues `REPLICATE` commands (in heartbeat responses) to the **new targets**, which pull via `FETCH_CHUNK` from surviving replicas (pull model: the target paces itself, owns retries, and dedups). Copies are checksum-verified, throttled per node; completion = atomic descriptor swap. Because placement scatters chunks, repair parallelism scales with pool size (100 TB node, 50-node pool ≈ ~2 TB per node ≈ ~3 h at 200 MB/s throttle).
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

Single-member loss: Raft failover, seconds, invisible to the data path. Quorum loss: data path continues (§4.3); chunk-boundary operations queue; recovery is standard KRaft disaster procedure. Brokers and storage nodes buffer/retry control operations idempotently (all control RPCs carry idempotency keys).

## 8. Placement

Inputs at `CreateChunk`: capacity-weighted random selection over `REGISTERED` nodes, filtered by (a) anti-affinity — no two replicas share the configured failure domain (default: host; recommended: rack/zone where topology allows), (b) exclusion of `SUSPECT/DRAINING/DEAD` nodes and nodes over a fullness watermark. Weights derive from free capacity, so new nodes absorb a proportionate share of *new* writes immediately — never a thundering herd, and never a rebalance. The same selector serves repair-target choice and operator relocation.

Anti-correlation is a first-order p99.9 concern (quorum latency is bounded by the second-slowest replica): placement must avoid co-locating replicas on shared hosts or shared burst-credit storage.

## 9. Background work

### 9.1 Retention

Leader evaluates retention (it owns the policy and the segment timeline) → `DeleteFiles` → metadata marks chunks `DELETING`, issues `DELETE` commands via heartbeats, removes records after replica confirmation (or reconciliation timeout). Space reclaim is file unlink: immediate and exact.

### 9.2 Reconciliation (scrub)

Periodic `INVENTORY_REPORT` diff against the chunk map, both directions: chunks in the map but missing on a replica → enqueue repair; chunks on disk but not in the map → orphans, deleted after a safety window. The **commit-before-write invariant** (chunk exists in metadata before any byte is sent — §4.3) is what makes orphan deletion safe: any on-disk chunk unknown to metadata and older than the create timeout was never live. Reports are sharded (`hash(chunkId) mod N` per cycle) so report size stays bounded; a full cycle completes daily.

### 9.3 Compaction and relocation

Log compaction runs on the leader broker exactly as in Kafka — read, rewrite into a new file, swap, delete old — expressed entirely as ordinary file operations; the storage layer never knows. (Side effect: each cycle re-places output across current topology — compacted topics self-balance.) Operator relocation reuses the repair pull path: `REPLICATE` + descriptor swap; sealed-chunk immutability makes it coordination-free.

## 10. Wire protocol — SCP (Strata Chunk Protocol)

### 10.1 Protocol landscape and rationale

| Link | Protocol |
|---|---|
| Kafka clients ↔ broker | Kafka wire protocol (compatibility is the product) |
| Broker ↔ metadata plane | Kafka RPC framework, new API keys (both ends Kafka-derived; rides existing machinery and feature gates) |
| Broker ↔ storage node | **SCP** |
| Storage node ↔ storage node | **SCP** (`FETCH_CHUNK`) |
| Storage node ↔ metadata plane | **SCP** control opcodes (metadata plane hosts an SCP listener) |

SCP is a purpose-built binary protocol rather than a reuse of Kafka framing, for three reasons: (1) the **payload-as-suffix invariant** — every frame ends with its opaque payload, so receivers can scatter headers into the heap and splice payload bytes directly between socket, aligned buffer pools, and disk with zero re-encoding; (2) the storage node stays implementable in any language with **no Kafka dependency and exactly one protocol stack** (data, copy, and control all speak SCP); (3) independence from Kafka's API-key space and version cadence — the fork tracks upstream, and entangling storage APIs with Kafka's ApiKeys would couple storage-node releases to broker merges. The alternative (Kafka framing everywhere) was considered and is workable; it was rejected on (1) and (3).

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
- **Connection lifecycle:** TCP, TLS per deployment policy. First frame MUST be `HELLO`. Pipelining is allowed after handshake, bounded by the negotiated in-flight byte cap. All `APPEND`s for a given chunk MUST use a single connection (ordering); after reconnect, the writer resyncs with `STAT_CHUNK`.
- **Implementation note (v0 finding):** on virtual-thread runtimes, never hold a monitor (`synchronized`) across blocking I/O or while response handlers may contend for it — blocked virtual threads inside monitors pin their carriers (JDK ≤23), and enough pinned carriers stall every virtual thread in the process. Use `ReentrantLock`, and keep blocking work/response callbacks off transport event-loop threads (a handler blocked on a lock must never stall frame dispatch for the very response its lock-holder is waiting on).

**Handshake.** `HELLO` request: `u16 frameVersionMin, u16 frameVersionMax, u8 clientKind (1 broker | 2 storage-node | 3 metadata | 4 tool), u64 featureBits, string clientId`. Response: `u16 chosenFrameVersion, u64 featureBits (intersection), u32 nodeId (0 if n/a), uuid incarnationId, u32 maxFrameBytes, u64 maxInflightBytes, array{u16 opcode, u16 maxApiVersion}`. The per-opcode version map is the negotiation mechanism: a client uses `min(its max, advertised max)` per opcode and never sends an opcode absent from the map.

### 10.3 Data-plane opcodes

| Opcode | Name | Request header (v1 fixed fields) | Response (after errorCode) | Payload |
|---|---|---|---|---|
| 0x0001 | `HELLO` | see §10.2 | see §10.2 | — |
| 0x0010 | `OPEN_CHUNK` | chunkId{uuid fileId, u32 index}, i32 writeEpoch, u8 fsyncOnAck (0/1), u64 expectedMaxBytes, u64 createdAtMs | — | — |
| 0x0011 | `APPEND` | chunkId, i32 writeEpoch, u64 baseOffset, u64 durableOffset | u64 endOffset | log bytes (may be empty = DO beacon) |
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

### 10.4 Control-plane opcodes (storage node ↔ metadata plane)

| Opcode | Name | Request (v1) | Response (after errorCode) |
|---|---|---|---|
| 0x0101 | `REGISTER_NODE` | uuid incarnationId, array endpoints, topology{string zone, rack, host}, array{u64 capacityBytes}, u32 onDiskFormatMax, u64 featureBits | u32 nodeId, u64 sessionEpoch, u32 heartbeatIntervalMs, u32 leaseMs |
| 0x0102 | `NODE_HEARTBEAT` | u32 nodeId, uuid incarnationId, u64 sessionEpoch, array{u64 usedBytes, u64 freeBytes}, u32 repairQueueDepth | u64 leaseValidUntilMs, array commands |
| 0x0103 | `INVENTORY_REPORT` | u32 nodeId, u32 shardIndex, u32 shardCount, array{chunkId, u8 state, u64 length, u32 crc} | u16 ack |

v0 additions: `NODE_HEARTBEAT` requests carry tagged field 0 `completedCommands` (array{u64
commandId, u16 status}) so the repair coordinator learns command completion on the next heartbeat;
`REPLICATE` params gained `expectedLength`. The v0 client↔metadata APIs ride SCP in the 0x02xx
range (`CREATE_FILE` 0x0201, `CREATE_CHUNK` 0x0202, `SEAL_CHUNK_META` 0x0203, `LOOKUP_FILE` 0x0204,
`DELETE_FILES` 0x0205, `SEAL_FILE` 0x0206, `ABORT_CHUNK_META` 0x0207, `LOOKUP_PATH` 0x0208) — v1 moves
broker-facing APIs to Kafka RPC per §10.1; the 0x02xx range stays reserved for tools.

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

```
[ header block — 4096 bytes, fixed ]
  u32 magic "SCHK" · u16 formatVersion=1 · u16 headerSize
  uuid fileId · u32 chunkIndex · u8 fsyncOnAck
  i32 createWriteEpoch · u64 createdAtMs
  u32 compatFlags · u32 roCompatFlags · u32 incompatFlags
  tagged block · zero padding · u32 headerCrc (last 4 bytes)

[ data region ]
  raw logical bytes, verbatim (record batches for LOG; §11.4 entries for CHECKPOINT)
  logical chunk offset X = file offset 4096 + X        ← address arithmetic, preserved

[ footer — sealed chunks only, appended after data ]   (§11.2)
```

The data region contains **no storage-layer framing**: what the producer's batches look like on the broker is byte-for-byte what sits on disk and what the consumer receives. This is the zero-copy and offset-arithmetic invariant; all storage-layer bookkeeping lives in the header, footer, sidecar, and ledger.

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

Fixed 64-byte trailer at EOF: `u64 dataLength · u64 footerStart · u32 sectionCount · u32 incompatFlags · u32 footerCrc · ... · u32 magic "SFTR"`. Read path: read last 64 bytes → section directory → sections. New section types are additive (`compat` by default); readers skip unknown types.

### 11.3 Sidecar and integrity ledger (per chunk, node-local, never replicated)

- **Sidecar `<chunk>.meta`** — 512 bytes, single-sector atomic rewrite: `{magic "SMET", u16 ver, i32 writeEpoch, i32 fenceEpoch, u64 lastKnownDO (advisory), u8 state, u32 crc}`. Holds the fencing state that must survive restart (§5.4).
- **Integrity ledger `<chunk>.j`** — append-only, one fixed 24-byte entry per `APPEND`: `{u64 endOffset, u32 payloadCrc, i32 writeEpoch, u32 reserved, u32 entryCrc}`. Crash recovery scans the ledger, verifies the last entries' CRCs against the data file, and truncates the data file to the last verified `endOffset` — **the storage node never parses payload bytes, even to recover.** When `fsyncOnAck=true`, data and ledger are fsynced before ack; otherwise both flush lazily. Overhead ≈ 24 B per multi-KB..MB append, written sequentially. The ledger is deleted once the chunk seals (the footer's CRC_RANGES supersede it).

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
  Appender openForAppend(int writeEpoch);
  Reader openForRead();
  SealInfo recoverAndSeal(int newEpoch);
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
| `metadata.version` feature gates | extended with a Strata feature lane for custom records/APIs |

## 14. Invariants

1. One writer per chunk per epoch; storage nodes enforce monotonic epoch fencing locally.
2. Producer-acked data exists on ≥2 replicas; tolerates any single storage-node failure per replica set.
3. Consumers never read beyond the durable offset.
4. Chunk state transitions are authoritative only at metadata commit; physical state converges via reconciliation.
5. A chunk exists in metadata before any byte of it exists on any disk (commit-before-write).
6. Sealed content is immutable, checksummed, self-describing; all replicas are byte-identical including header and footer (whole-file CRCs comparable).
7. Brokers hold no durable state; any broker can lead any partition after bounded (≤ one checkpoint interval) replay.
8. No data-path operation requires a metadata-plane round trip.
9. The system never moves data except: repair, decommission, operator-invoked relocation. Never as a balancing policy.
10. Payload bytes are never re-encoded anywhere between producer and consumer: broker → SCP frame suffix → chunk data region → SCP frame suffix → consumer.
11. Every persistent structure and every frame carries magic, version, and CRC; readers ignore unknown-optional and refuse unknown-incompat.

## 15. Observability (v1 minimum)

Produce-path latency decomposed by stage (broker processing / storage append / quorum wait); per-replica append latency (feeds slow-node detection and hedging thresholds); chunk create/seal latency; under-replicated chunk count and repair backlog bytes (the durability-exposure gauges); DO lag; metadata commit latency and queue depth; heartbeat misses; tail-cache hit ratio; per-node fullness vs. weight; SCP-level: per-opcode rates, error-code counters, in-flight bytes vs. negotiated cap.

## 16. Testing strategy

- **Compatibility:** Apache Kafka's client matrix + system tests run against Strata in CI; per-API-version conformance tracked as a release gate.
- **MetadataStore SPI conformance:** one behavioral suite (chunk lifecycle, registration/lease, command delivery, repair orchestration, failover) run against both backends (§4.4); KRaft-backend parity on this suite is a v1 GA gate.
- **Chunk protocol:** deterministic simulation of the append/fence/seal/recovery state machine (single-process, virtual time, exhaustive fault schedules) — this is where seal-recovery edge cases are found cheaply.
- **Wire/format robustness:** frame fuzzing against the SCP server; golden-corpus tests (recorded frames, footers, checkpoints from every released version replayed against every newer version); cross-version interop matrix (client N±1 vs server N) as a CI gate, enforcing §10.6.5.
- **Crash safety:** torn-write injection on the data file + ledger (§11.3) across power-cut points; recovery must converge to a verified prefix.
- **Chaos:** Jepsen-style fault injection on real clusters: node kills mid-seal, metadata failover during repair, asymmetric partitions between broker and individual replicas.
- **Performance gates:** flat-p99 claim verified continuously (both durability modes); repair throughput vs. pool size; zero-copy path regression (no payload copies, asserted via allocation profiling).

## 17. Open questions

1. **DO staleness bound** — empty-`APPEND` beacons cover idle partitions; the cadence and the maximum staleness a direct reader may observe need a number.
2. **KIP-392 direct-read** — exact Fetch subset a storage node must implement; session/quota handling without broker mediation. v1 ships broker-proxied; this is the v1.x decision.
3. **Chunk-map sharding trigger** — at what descriptor count / commit rate the second log splits (Northguard-style); the two-state-machine seam exists, the threshold doesn't.
4. **Segment-roll policy defaults** — balancing chunk-count inflation (metadata sizing) against failover replay bound for low-throughput partition fleets.
5. **Storage-node language/runtime** — decision owed before repo bootstrap; constraint: predictable tail latency under mixed read/append load; must implement SCP + §11 formats from this spec alone.
6. **Security baseline** — TLS posture per link (mandatory inter-node?), at-rest encryption (per-chunk envelope vs. volume-level), FIPS story — needs a design pass before the first regulated-industry conversation.
7. **Fork baseline** — which Kafka version to track first, and the merge cadence policy.
8. **Flow control** — v1 uses static per-connection caps from `HELLO`; whether repair traffic needs dynamic credit-based flow control to protect foreground p99 at scale is unproven either way.
9. **Compression** — reserved in the frame flags; whether `FETCH_CHUNK` (repair/relocation) benefits enough to justify it, given producers already compress batches.
10. **ZK backend retirement** — the v0 backend's cutoff point: which milestone flips the conformance gates, and confirmation that no v0 deployment needs its metadata to survive (current answer: none — no migration tooling will be built).
11. **Group commit for `fsyncOnAck`** — RESOLVED in v0 (§5.3): per-chunk coalesced forces with a 5 ms accumulation window and deferred append acks. Measured at window 256 on a shared-SSD laptop: p50 2,941 ms → 75 ms (39×), throughput 0.1 → 3.2 MB/s (32×). Remaining tunable: the accumulation window should become a config and be re-calibrated on production hardware (per-node devices interfere less than a shared laptop SSD).

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
