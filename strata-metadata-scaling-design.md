# Strata Metadata Scaling Design

**Status:** Draft
**Date:** 2026-06-16
**Companion:** [strata-tech-design.md](strata-tech-design.md) section 4

## 1. Summary

Strata needs to support a shared storage cluster with more than 100M files, many
Kafka clusters or tenants, high metadata throughput, and fast repair after
data-node failures. The v0 ZooKeeper-backed `MetadataStore` is useful for
bootstrap and correctness testing, but it must not become the production
metadata database.

The target design keeps consensus small and moves high-cardinality metadata into
Strata system metadata files:

```text
Consensus backend
  -> controller leader fencing
  -> namespace -> controller leader assignment
  -> manifest/root pointers for namespace metadata files
  -> physical root descriptors for current metadata snapshot/log files
  -> small node incarnation/fencing records

Strata system metadata files
  -> authoritative metadata change logs
  -> compacted file/chunk/path state snapshots
  -> materialized lookup and repair indexes
  -> sealed snapshots and metadata logs
```

User-file names, user-file ids, file state, chunk descriptors, placement,
tombstones, and replica membership changes live in Strata system metadata files.
They are not stored as per-user-file znodes or consensus records. Indexes such
as `file -> chunks`, `(namespace, path) -> fileId`, and `node -> chunks` are
materialized from the metadata log for fast lookup, recovery, and repair; they
are not independent sources of truth.

This is a two-layer metadata model:

```text
ZooKeeper/root MetadataStore
  -> controller leader election and fencing epoch
  -> namespace assignment records
  -> namespace metadata manifest records
  -> metadata-log system-file descriptors and chunk locations
  -> node registry (identity records; ids are externally supplied via STRATA_NODE_ID)

Strata metadata-log files
  -> user-file ids, names, lifecycle state, chunk descriptors, tombstones
  -> derived namespace-local lookup, listing, and repair indexes
```

The metadata-log system files use the root ZooKeeper-backed store only for their
own physical file descriptors. Their bytes are ordinary replicated Strata chunk
data stored on data nodes. User Strata files use the namespace-log metadata
store and do not create one root consensus record per user file.

`FileId` is a per-namespace owner-assigned `long` (a single 64-bit value rendered
`%016x`), assigned under the namespace owner's lock — **not** a UUID. This replaced
the earlier UUID-shaped id when the namespace moved into the data plane; see §20
for id generation (the owner's snapshot high-water for user files, a small
consensus-root counter for system files), the on-disk layout, and the
`FORMAT_VERSION` bump. File metadata operations route by namespace or by a
namespace-qualified file handle; `(namespace, fileId, index)` is the global identity.

Sharding is namespace-based in the first scalable version:

```text
namespace A -> namespace controller leader A
namespace B -> namespace controller leader B
namespace C -> namespace controller leader C
```

This removes the global single-metadata-leader ceiling while keeping
`ListFiles(namespace)` simple. One namespace is still served by one metadata
leader; that is an explicit first-version tradeoff.

## 2. Goals

- Support 100M+ Strata files in one cluster.
- Let one Strata storage cluster serve multiple Kafka clusters or tenants.
- Keep consensus traffic bounded by cluster roots, not by file/chunk count.
- Avoid one znode or consensus record per file, chunk, replica, tombstone, or
  reverse-index entry.
- Keep hot operations routed to one namespace controller leader.
- Keep namespace listing local to one namespace controller leader.
- Preserve the current data-path invariant: appends and reads do not call the
  metadata plane.
- Preserve existing correctness invariants: commit-before-write, fenced writers,
  idempotent retries, deletion tombstones, and repair based on authoritative
  metadata.

## 3. Non-goals

- No dependency on any Kafka cluster's controller quorum. Strata metadata is a
  Strata-native service because one Strata cluster can be shared by multiple
  Kafka clusters.
- No attempt to remove consensus entirely. Consensus remains the linearizable
  root for leadership, fencing, membership generations, and manifest publication.
- No intra-namespace sharding in the first version. Namespace-level sharding is
  the chosen complexity boundary for now.
- No automatic load-based metadata balancer in the first version. Namespace
  assignment is static after creation unless an operator explicitly moves it or
  the assigned replica set loses availability.
- `FileId` is a per-namespace owner-assigned `long` (changed when the namespace
  moved into the data plane — see §20; supersedes the original "no format change"
  goal). No ZooKeeper sequential ids or file-id ownership map — user ids come from
  the owner's snapshot high-water, system-file ids from a small consensus counter.
- No unbounded `ListFiles` response. Listing is always paged.
- No repair or retention workflow that depends on scanning every file in a
  namespace on a fixed cadence.
- No production migration plan for the current direct-ZooKeeper user-metadata
  layout. The project is not running in production, so the scalable backend can
  replace the prototype path without preserving old deployment state.

## 4. Current v0 constraints

The v0 implementation persists file/chunk metadata directly in ZooKeeper through
`ZkMetadataStore`:

```text
/strata/files/<fileId>
/strata/namespaces/<namespace>/paths/<path>/__file
/strata/nodes/<nodeId>
```

Node ids are no longer allocated in ZooKeeper: the `/strata/ids` sequential
allocator has been removed. Each data node supplies its own id via the
`STRATA_NODE_ID` environment variable (`DataNodeConfig.nodeId()`), validated
against the volume's `identity.properties` on startup; the controller validates
uniqueness at registration instead of minting ids. The metadata-log design does
not add a ZooKeeper-generated file-id sequence either.

This is acceptable for bootstrap, but prototype-scale at the intended target:

- `listFiles()` style scans scale with file/tombstone count.
- Repair reconciliation can reread stable records repeatedly.
- `node -> chunks` as consensus metadata would add a high-cardinality reverse
  index to the consensus service.
- File creation must not require one consensus write per file.
- A single controller leader becomes the throughput and recovery ceiling for all
  namespaces.

The production design keeps the same service semantics and SCP surface, but
changes where metadata lives and how it is sharded.

## 5. Target architecture

There are two metadata layers.

The consensus root is small, linearizable, and low-cardinality:

```text
/strata/meta/epoch
  latest allocated metadataEpoch

/strata/meta/namespaces/<namespace>
  assignmentGeneration
  replicaSet: [nodeId...]
  preferredLeader: nodeId
  leaderEndpoint
  metadataEpoch: latest published namespace metadata epoch
  manifestPointer:
    snapshotRootDescriptor
    indexRootDescriptors
    openLogRootDescriptor
    logStartOffset
  publishedLogOffset
  checksums
  state: ACTIVE | DRAINING | DELETED
  policy root: quota/retention/acl handles

/strata/meta/membership
  generation
  eligibleMetaNodes: [nodeId...]

/strata/nodes/<nodeId>
  incarnationId
  persistent endpoint identity
  state: REGISTERED | FENCED | DRAINING | DEAD
```

`/strata/meta/epoch` is a single compact consensus counter. A candidate
namespace controller leader CAS-increments it before activating leadership, then
uses that epoch when recovering and publishing namespace manifests. The counter
can have gaps; uniqueness and monotonic fencing matter, not density.

The system-file layer is high-cardinality and append/snapshot oriented. The
current implementation stores these physical metadata-log file descriptors under
the reserved `strata-meta` system namespace with paths prefixed by
`/metadata-log/`; the conceptual layout is:

```text
reserved system namespace: strata-meta

/metadata-log/<namespace>/log/<segment-file-id>
/metadata-log/<namespace>/snapshot/<snapshot-file-id>
/metadata-log/<namespace>/index/<index-file-id>
```

The reserved system namespace is internal and rejected for user-file creates.
The first implementation stores one Strata chunk per metadata system file; chunk
rolling for very large metadata logs/snapshots is a follow-up scaling item.

The consensus root stores enough physical information to open the current
metadata snapshot, indexes, and log tail without first consulting the metadata
log itself. That avoids a bootstrap cycle:

```text
need metadata log to resolve file metadata
need file metadata to open metadata log
```

For metadata system files, the root descriptor therefore includes the file id,
chunk descriptors needed to read the published snapshot/index/log range, replica
locations, lengths, and checksums. After the namespace leader loads those root
files, all normal user-file metadata is read from the metadata log and
snapshots.

Each namespace has one active metadata writer. The active writer appends ordered
metadata mutations to its namespace log, updates in-memory indexes, writes
sealed snapshots/index files, and CAS-publishes new manifest pointers through
the consensus root.

## 6. Namespace sharding

The first scalable version uses this ownership model:

```text
namespace -> active controller leader
```

A metadata-capable node may lead many namespaces. A large namespace still has
one leader until there is evidence that per-namespace throughput or recovery
time requires intra-namespace sharding.

Request routing:

| Operation | Route key | Metadata fan-out |
|---|---|---|
| `CreateFile(namespace, path)` | namespace | one namespace leader |
| `LookupPath(namespace, path)` | namespace | one namespace leader |
| `ListFiles(namespace, prefix, pageToken)` | namespace | one namespace leader |
| `CreateChunk(namespace, fileId)` | namespace | one namespace leader |
| `SealChunk(namespace, fileId, chunkIndex)` | namespace | one namespace leader |
| `LookupChunks(namespace, fileId, range)` | namespace | one namespace leader |
| `DeleteFiles(namespace, paths)` | namespace | one namespace leader |

This deliberately avoids fan-out listing for the common administrative API:

```text
ListFiles(namespace) -> one namespace leader
```

Batch APIs that carry namespace-qualified file handles must preserve the same
routing rule. A client or front-end gateway splits a multi-namespace request into
one RPC per namespace before contacting controller leaders. The namespace leader
then handles a local batch only; it is not responsible for fan-out to other
namespace leaders.

The tradeoff is also explicit:

```text
A single namespace is still bounded by one controller leader's CPU, memory,
metadata-log throughput, and recovery time.
```

That is acceptable for the first scalable version if each Kafka cluster or
tenant maps to one namespace. The design leaves a future split mode without
implementing it now:

```text
namespace -> splitGroup
splitMode = NONE | HASHED_PATH | FILE_ID
```

Only `NONE` is in scope for the first implementation.

### 6.1 Static assignment policy

Metadata placement is static in the first scalable version. Namespace creation
assigns a namespace to a small replica set of metadata-capable nodes using
rendezvous hashing:

```text
score = hash(namespace, membershipGeneration, nodeId)
replicaSet = top 3 eligible nodes by score
preferredLeader = replicaSet[0]
```

The result is persisted in the consensus root:

```text
/strata/meta/namespaces/<namespace>
  assignmentGeneration
  replicaSet
  preferredLeader
```

Every controller reads the same persisted assignment. Services do not
independently recompute ownership during request handling.

The assignment rules are:

- new namespaces use the current controller membership generation;
- existing namespaces keep their persisted assignment when nodes are added;
- existing namespaces do not move for load balancing automatically;
- if a preferred leader fails, leadership moves inside the persisted replica
  set;
- if the replica set loses enough availability, an operator or recovery workflow
  writes a new assignment generation through consensus;
- optional future rebalancing is explicit and generation-based, not an implicit
  side effect of membership churn.

For example, with 100 namespaces and 30 metadata-capable nodes, rendezvous
hashing should place roughly 3 or 4 preferred namespace leaders on each node.
The exact distribution can skew when namespaces have different sizes, but that
is accepted in the first version to keep the control plane simple.

## 7. File identity and routing

`FileId` is a per-namespace owner-assigned `long` (a single 64-bit value rendered
`%016x`); see §20 for id generation and the on-disk layout. A file id is unique
within its namespace and never reused there, but it does not encode a metadata
shard or routing hint — `(namespace, fileId, index)` is the global identity.

Authoritative file identity is stored in the namespace metadata log:

```text
FileCreated {
  namespace
  path
  fileId
  createOperationId
  filePolicy
}
```

Because the file id is not self-routing, scalable metadata APIs should route
file-scoped operations with namespace context:

```text
CreateFile(namespace, path) -> FileHandle(namespace, fileId)
CreateChunk(namespace, fileId)
SealChunk(namespace, fileId, chunkIndex)
LookupChunks(namespace, fileId, range)
```

The namespace can come from the request, a client-side file handle returned by
`CreateFile` or `LookupPath`, or a front-end metadata gateway cache. It must not
come from a per-file consensus lookup. The namespace leader verifies that the
file id belongs to the namespace before mutating or returning metadata.

Changing to single positive `long` file ids, ZooKeeper sequential id allocation,
or consensus-reserved file-id blocks is not part of this design. Those
mechanisms were only considered for the older model where high-cardinality
metadata stayed in ZooKeeper and file-id-only requests needed to be self-routing. Once
file identity and path bindings live in the namespace metadata log, the
namespace is the routing key and the existing `FileId` is sufficient.

## 8. Metadata log

Each namespace owns an append-only metadata log stored in Strata system files.
The log is the authoritative history for that namespace.

Record families in the authoritative metadata log:

| Record | Purpose |
|---|---|
| `NamespaceInitialized` | records namespace assignment generation and policy root |
| `FileCreated` | atomically reserves file id, initial file policy, and `(namespace, path)` binding |
| `PathBound` | idempotent path-binding validation/fence for split or replayed create flows |
| `PathUnbound` | removes a `(namespace, path)` lookup binding without changing file lifecycle state |
| `WriterEpochAllocated` | records a metadata-plane writer fencing epoch before append or recovery ownership changes |
| `ChunkCreated` | commits placement before any chunk byte is written |
| `ChunkSealed` | commits final length, CRC, and sealed replica set |
| `ChunkAborted` | removes the current open tail chunk for the same create operation before it becomes durable data |
| `ChunkDeleted` | removes an already-drained chunk descriptor from a `DELETING` file while other chunks may remain |
| `FileSealed` | closes the file |
| `FileDeleting` | prevents stale writers from resurrecting a file |
| `FileDeleted` | finalizes a drained `DELETING` file and records the tombstone deletion timestamp for retention |
| `ReplicaSwapped` | atomically replaces a failed sealed replica |
| `ReplicaDropped` | removes a missing/corrupt live sealed replica, or a deletion-confirmed replica from a `DELETING` file |
| `ReplicaAdded` | records a verified replacement replica for an under-replicated sealed chunk |
| `TombstoneSwept` | final metadata cleanup after the fencing window |

Every mutating client request carries an operation id. The namespace leader
records operation ids in the log or snapshot state so retries are idempotent
across leader failover.

The authoritative metadata log must not contain derived indexes or operational
delivery events. The following are explicitly excluded from the metadata log:

- `node -> chunks` reverse-index entries;
- path lookup or listing index rows;
- repair/delete command issuance or heartbeat completion records;
- inventory report pages or inventory checkpoint cursors;
- per-node placement weights and usage estimates.

If an operational event changes authoritative metadata, the log records only the
resulting metadata transition. For example, repair command completion may cause
a `ReplicaSwapped` record after the new replica is verified; the command
completion itself is not a metadata-log record.

### 8.1 Authoritative state and derived state

The metadata log is the source of truth for user-data metadata. Every
state-changing metadata operation must be represented as an append-only log
record before it is acknowledged.

Authoritative log-derived state:

- file identity: `fileId`, namespace, path binding, create operation id, and
  file policy;
- writer fencing state: the current file writer epoch;
- file lifecycle: `OPEN`, `SEALED`, `DELETING`, `DELETED` tombstone state,
  tombstone deletion timestamp, and tombstone sweep;
- chunk lifecycle: chunk index, placement, write epoch, open/sealed state,
  sealed length, CRC, and sealed replica set;
- replica membership changes such as repair-driven replica swaps, drops, and
  additions;
- bounded idempotency records for retried client operations.

For user data files, these records are the only durable source for file names,
file ids, file metadata, chunk descriptors, and tombstones. The root
ZooKeeper-backed store does not keep parallel user-file records. The root store
does keep metadata-log system-file records under the reserved system namespace,
because those records are needed to bootstrap and repair the metadata-log files
themselves.

Lookup maps, reverse indexes, and scheduling views may be materialized in memory
and in sealed snapshot/index files, but they must be rebuildable from the
authoritative log plus the latest compacted snapshot:

- `(namespace, path) -> fileId` lookup map;
- `fileId -> chunk descriptors` lookup map;
- `nodeId -> chunks owned by this namespace`;
- per-node usage and placement weights;
- namespace listing order and prefix indexes;
- repair candidates derived from node state and chunk replica sets;
- tombstone and deferred cleanup queues;
- inventory reconciliation progress.

`nodeId -> chunks` is specifically a derived reverse index. It is useful to
materialize because node failure repair must be bounded, but it can be rebuilt
by replaying `ChunkCreated`, `ChunkSealed`, `ReplicaSwapped`, `FileDeleting`,
`ReplicaDropped`, `ReplicaAdded`, `ChunkAborted`, `ChunkDeleted`, `FileDeleted`,
and `TombstoneSwept` records. File-level deletion drives chunk cleanup;
`ReplicaDropped` records each confirmed replica drain, and `ChunkDeleted` records
only the final removal of a descriptor whose replica set is already empty, not a
separate chunk-level `DELETING` lifecycle. No metadata correctness rule may
depend on a `node -> chunks` index update that does not correspond to an
authoritative log record. Tombstone cleanup is also index-driven: the leader
uses the retained `FileDeleted.deletedAtMs` map to find sweep candidates, not a
full scan of all live file records.

Current implementation foundation: `MetadataLogRecord` and
`NamespaceMetadataState` provide a package-internal replay core for one
namespace. `MetadataLogCodec` provides the versioned binary encoding for those
authoritative records, `MetadataLogSegmentCodec` provides internal segment
framing with monotonic offsets and CRC32C protection for record frames,
`MetadataLogSegment` provides the append/recover primitive for a single active
segment, `NamespaceMetadataSnapshotCodec` provides a CRC-protected compacted
snapshot envelope with the next metadata-log offset,
`NamespaceMetadataManifestCodec` provides the compact consensus-root descriptor
for physical snapshot/index/log system files, and `NamespaceMetadataRecovery`
enforces the startup barrier over snapshot plus the contiguous recovered durable log
segments. Together they apply encoded log records into file/path state, recover
only the valid prefix after a torn open tail, replay valid durable open-tail
records beyond the last published manifest offset, reject offset gaps before
activation, verify manifest-described sealed files, load a compacted file table,
and rebuild the derived `node -> chunks` index. `NamespaceMetadataLog` now owns
the active append/replay image for one namespace: it validates a candidate record
against a copied state, prepares the exact encoded frame, appends that frame to
the open metadata-log segment, and only then publishes the new in-memory
state/applied offset. `NamespaceMetadataFileStore` defines the physical
snapshot/index/log byte-store boundary, `NamespaceMetadataPersistentLog` wires
the active log to that store so a failed physical append cannot mutate active
state, and the test-only `TestNamespaceMetadataFileStore` fixture provides a
deterministic backend for recovery and append-ordering validation. The persistent log can
recover from manifest-described snapshot and log segment bytes, replay valid
durable records past the last published manifest offset, seal the recovered
open tail at exactly the replayed durable prefix, roll to a fresh open segment
at the recovered offset, and continue appending there. If the old tail has
already been sealed or the production system-file backend cannot truncate it in
place, recovery publishes a sealed copy of the replayed prefix instead of
referencing uncommitted tail bytes. Rolling the open segment on recovery is a
metadata-epoch fencing rule: a stale leader that still holds the old open system
file must not be able to append bytes that a successor can later publish without
replaying. `NamespaceMetadataLogRepository` now pairs
recovery, publish, rotation, and synchronous snapshot compaction with
`MetadataStore.putNamespaceManifest` CAS so a recovered leader publishes the new
open tail before accepting metadata writes, and later appends remain recoverable
from the compact manifest even before the next explicit publish. A background
sweep (`NamespaceLogBackend.startBackgroundCompaction`) bounds steady-state
open-log growth: on a fixed interval it runs the synchronous `compactAndPublish`
cycle for any owned namespace whose open log has passed a configured byte
threshold, so a stable long-lived leader's log is bounded by snapshot cadence
rather than only compacting at open/failover. The repository is deliberately
independent from the physical metadata-file implementation: tests can use an
in-memory store, while the service runtime uses `StrataSystemMetadataFileStore`
to write metadata-log bytes as replicated Strata chunks and publish only the
system-file descriptors in the root store. Non-blocking (copy-on-write)
compaction, retention-cutoff garbage collection of older superseded system files
(only the single just-superseded snapshot/log pair is reclaimed inline today),
and multi-chunk metadata system files remain separate hardening work.

`NamespaceLogMetadataStore` is the current bridge from the existing
`MetadataStore` SPI to the namespace-log backend. It delegates node registry,
namespace assignment, and manifest roots to the compact root store, while
converting file/path mutations into semantic namespace-log records.
Namespace-qualified reads, updates, and final deletes load only the named
namespace. File-id-only store helpers are retained only for internal full-scan
and system-file maintenance paths, not for client or metadata request routing.
The live `Controller` can be started against this bridge with
`STRATA_CONTROLLER_BACKEND=namespace-log`; the default remains
`STRATA_CONTROLLER_BACKEND=zk`. In namespace-log runtime mode, the meta-log
Strata files use the root `ZkMetadataStore` for their own descriptors and the
data-node SCP path for their bytes. User Strata files use
`NamespaceLogMetadataStore`, so their file names, file metadata, file ids, chunk
descriptors, tombstones, and path bindings are persisted only through the
namespace metadata log. ZooKeeper/root metadata contains bounded system state
plus metadata-log file descriptors, not one record per user file.

`NamespaceMetadataOperations` is the live service bridge: it moves client-facing
and repair-facing file mutations out of `Controller`/`RepairCoordinator`
and into semantic operations that correspond to the authoritative log records
(`FileCreated`, `WriterEpochAllocated`, `ChunkCreated`, `ChunkSealed`,
`PathUnbound`, `ChunkAborted`, `ChunkDeleted`, `FileSealed`, `FileDeleting`, `ReplicaSwapped`,
`ReplicaDropped`, `ReplicaAdded`, `FileDeleted`, and `TombstoneSwept`). The
current implementation still commits those operations through
`MetadataStore`/ZooKeeper, but the service no longer depends on
ZooKeeper's create-conflict exception or arbitrary file rewrites for request and
repair routing.

## 9. Snapshots and indexes

The active namespace leader derives these in-memory indexes from the log:

- `fileId -> FileRecord`;
- `(namespace, path) -> fileId`;
- `fileId -> chunk descriptors`;
- `nodeId -> chunk ids owned by this namespace`;
- `nodeId -> pending repair/delete commands`;
- tombstone and deferred cleanup queues;
- per-node usage estimates for placement.

The leader periodically writes sealed snapshot and index files:

```text
snapshot generation N:
  authoritative current-state tables:
    file table
    path binding table
    chunk table
    retained tombstones
    idempotency table watermark
  derived materialized indexes:
    path lookup index
    file -> chunks index
    node -> chunks reverse index
    listing/prefix index
    placement usage index
    deletion/tombstone queues
```

The materialized indexes are checkpoint accelerators. A namespace leader can
discard and rebuild any derived index from the authoritative current-state
tables plus log records after `logStartOffset`.

After writing and sealing the snapshot/index files, the leader CAS-publishes a
new manifest pointer in consensus:

```text
manifestPointer {
  namespace
  metadataEpoch
  generation
  snapshotRootDescriptor
  indexRootDescriptors
  openLogRootDescriptor
  logStartOffset
  publishedLogOffset
  checksums
}
```

Recovery loads the manifest, reads sealed snapshot/index files, recovers or
seals the open metadata-log tail, then replays records from `logStartOffset`
through the recovered durable end. `publishedLogOffset` is the manifest's
published lower bound for the listed log segments; it is not advanced through
consensus for every metadata mutation. A fenced successor may recover and apply
additional CRC-valid records already durable in the open metadata-log tail.

## 10. Metadata log compaction

Metadata log compaction is required because file metadata can be updated,
deleted, repaired, and swept. Compaction is snapshot plus log truncation, not
Kafka-style key compaction on the hot path.

The compaction cycle is:

```text
1. choose a compaction cut offset in the namespace metadata log
2. build current-state snapshot and derived index files through that offset
3. include retained tombstones and idempotency state in the snapshot
4. seal and verify the snapshot/index files
5. CAS-publish a manifest with the new snapshot generation and logStartOffset
6. delete old metadata-log segments below logStartOffset after a safety delay
```

The current implementation foundation supports the synchronous form of this
cycle for one namespace: `compactAndPublish` writes a new snapshot file at the
current applied offset, opens a new empty metadata-log segment at that same
offset, and CAS-publishes a manifest whose `logStartOffset` equals the snapshot
offset. If that CAS loses, the caller must discard the loaded log and recover
again; on a lost CAS the just-written snapshot/log are cleaned up and the old
manifest and its files are left intact, and on a winning CAS the superseded
snapshot/log pair is deleted only after the new manifest is durably published, so
exactly one prior generation is ever reclaimed inline. A background sweep
(`NamespaceLogBackend.startBackgroundCompaction` → `compactOversizedRepos`) drives
this cycle in steady state: on the `STRATA_CONTROLLER_LOG_COMPACT_INTERVAL_MS`
cadence it compacts any owned namespace whose open log exceeds
`STRATA_CONTROLLER_LOG_COMPACT_BYTES`, bounding open-log size for a stable leader.
A rotation-vs-compaction policy chooser, retention-cutoff deletion of older
superseded generations, non-blocking (copy-on-write) compaction, and multi-chunk
metadata system files are still future work; the service path is
already wired to a Strata-backed metadata file store for metadata-log bytes.

The compacted snapshot contains the latest state, not the full mutation history:

```text
authoritative current-state tables:
  current files
  current path bindings
  current chunk descriptors
  retained deletion tombstones with deletion timestamps
  retained idempotency records
derived materialized indexes:
  current path lookup/listing index
  current file -> chunks index
  current node -> chunks index
  pending repair/delete queues
```

Deletion and retry fencing define the retention boundary. A deleted file cannot
disappear from all metadata immediately: a `FileDeleted` tombstone, including its
deletion timestamp, must remain until stale create/seal/delete retries can no
longer resurrect the old file id or path binding. Only after the tombstone
retention window has elapsed, and after the tombstone state has been included in
a published snapshot, can a later compaction emit `TombstoneSwept` and remove the
tombstone from future snapshots. The sweep work is bounded by the tombstone
timestamp index, not by the total number of files in the namespace. Fixed-cadence
background sweeps must not load every published namespace; they may sweep
already-active namespaces and leave cold namespaces to explicit namespace-local
retention passes.

Idempotency records also need bounded retention. The namespace snapshot keeps
enough operation-id history to make client retries safe across leader failover
and network retries. Records older than the retry horizon can be dropped at
compaction time after they are below the published `logStartOffset`.

Compaction must not block the metadata write path. The active leader can build
snapshots from a stable in-memory view or a copy-on-write checkpoint while new
mutations continue appending to the current metadata log. Manifest publication
is the atomic cutover; readers and standby leaders either use the old manifest
or the new manifest, never a half-written snapshot.

Old log deletion is a garbage-collection step, not part of committing a
metadata mutation. If deletion fails or is delayed, correctness is unchanged;
only storage usage grows. A namespace remains recoverable as long as the
manifest's snapshot/index files and retained log range are available.

## 11. Node inventory and repair

Inventory remains useful, but only as reconciliation. It is not the
authoritative metadata source.

Data nodes send bounded inventory pages:

```text
INVENTORY_REPORT(nodeId, shardIndex, shardCount, entries[])
```

The `shardIndex` and `shardCount` fields here are inventory-page shards used to
bound report size. They are not metadata shard ids.

Internal maintenance APIs are namespace-qualified for the same reason as client
file APIs: the namespace is the metadata-sharding and ownership boundary. A
control gateway can collect cluster-wide events such as node failure or
inventory pages, but it must fan those events out into namespace-local
operations instead of issuing bare `FileId` or bare `ChunkId` metadata calls.

Inventory entries should carry the owning namespace or another owner hint that
was persisted in the chunk sidecar when the chunk was opened. If an entry only
carries `(fileId, chunkIndex)`, the receiving controller may consult a
derived `fileId -> namespace` routing index built from namespace metadata logs,
but it must not perform a per-file consensus lookup. The receiving metadata
service partitions bounded pages by owner namespace and forwards each page to
the correct namespace leader. Each namespace compares the report against only
the chunks it owns.

Repair ownership:

```text
node failure event -> all namespace leaders
each namespace leader reads its local node -> chunks index
each namespace leader schedules repair for chunks it owns
```

This avoids storing `node -> chunks` in ZooKeeper while keeping repair bounded.
A failed node can affect many namespaces, but the work is split across metadata
leaders and data nodes.

Repair and delete commands are owned by the namespace that owns the chunk.
Heartbeat command envelopes carry the owning namespace, or the gateway keeps a
bounded `commandId -> namespace` routing table until completion. A metadata
service may act as the control gateway for a data node heartbeat and
aggregate commands from multiple namespace work queues, but command delivery is
not authoritative metadata. After failover, the active namespace leader rebuilds
repair/delete work from authoritative file and chunk state and may reissue
idempotent commands. The metadata log records the final metadata transition
such as `ReplicaSwapped` or `TombstoneSwept`, not the command delivery event.

## 12. Listing semantics

Namespace listing is intentionally simple in the first version:

```text
ListFiles(namespace, prefix, limit, pageToken)
```

Because one namespace has one controller leader, the request goes to one metadata
leader. The response is bounded and carries a continuation token:

```text
pageToken {
  namespace
  namespaceGeneration
  prefix
  lastPath
  snapshotGeneration or readEpoch
}
```

Semantics:

- listing is paged;
- ordering is by canonical `StrataPath`;
- a token is valid only for the namespace generation it was issued against;
- the implementation may serve listing from a stable snapshot generation;
- list APIs are administrative/read APIs, not repair or retention primitives.

If intra-namespace sharding is added later, this API can evolve by adding
per-split cursors inside the token. That complexity is intentionally out of
scope now.

## 13. Leader lifecycle and recovery barrier

Each namespace controller leader has an explicit lifecycle:

```text
STANDBY -> RECOVERING -> ACTIVE -> FENCED
```

Only `ACTIVE` leaders serve normal metadata reads and writes. A leader candidate
that has won leadership but has not finished recovery must reject or defer
requests with a retryable response such as `METADATA_RECOVERING` or
`NOT_LEADER` with a leader hint when another active leader exists.

The recovery barrier is:

```text
1. win namespace leadership through consensus and CAS-increment `/strata/meta/epoch`
   to obtain a new metadataEpoch
2. publish/observe RECOVERING state for the namespace
3. load the namespace manifest from consensus
4. read and verify sealed snapshot/index root descriptors
5. fence previous metadata-log writers at the storage layer
6. recover or seal the open metadata-log tail and find its durable end offset
7. replay records from logStartOffset through the recovered durable end
8. rebuild derived indexes: path lookup, file -> chunks, node -> chunks, queues
9. restore retained idempotency state for retried operations
10. verify the leadership epoch is still current
11. publish/enter ACTIVE and begin serving metadata requests
```

The invariant is:

```text
ACTIVE implies:
  metadataEpoch is current
  manifest snapshot/index files are loaded and verified
  open metadata-log tail is recovered or sealed
  appliedOffset == recoveredDurableOffset
  derived indexes match authoritative current-state tables
  retained idempotency state is available
```

If any recovery step fails, the candidate stays non-active and does not serve
namespace metadata. It may retry recovery, resign leadership, or wait for an
operator action depending on the failure.

For the first scalable version, only the active namespace leader serves metadata
reads. Standby/follower reads are intentionally out of scope because they require
an `appliedOffset >= readOffset` rule and extra freshness semantics.

## 14. Failure handling

### Namespace controller leader failure

1. A standby wins leadership for the namespace through the consensus backend.
2. The new leader CAS-increments `/strata/meta/epoch` and obtains a higher
   `metadataEpoch`.
3. It completes the recovery barrier in section 13.
4. It resumes serving metadata operations only after entering `ACTIVE`.

Old leaders may still have open network connections, but they cannot publish a
new manifest or append to the metadata log after being fenced.

### Stale-epoch owner re-acquire

The recovery barrier (section 13) fences previous writers by CAS-incrementing the
epoch and republishing the manifest. Before per-namespace leadership is fully in
place, ownership is decided by the static assignment policy (section 6.1) and a
namespace's repository is opened lazily — opening allocates a fresh `metadataEpoch`
and republishes the manifest, which itself fences any prior writer. During a
membership settle two controllers can therefore briefly open the same namespace;
the later opener's higher epoch fences the earlier one's open-log writer.

A controller that still maps to the namespace under the assignment policy keeps a
cached repository at the now-stale epoch. Left alone, its every metadata-log append
fences (`FENCED_EPOCH`, append epoch < durable epoch) and it retries forever: the
in-flight file never finalizes and the owner repair pass skips it on each scan — a
permanent wedge. To converge instead of wedging, a fenced append makes the owner
**re-acquire**: it evicts the stale cached repository and re-opens the namespace,
which allocates a new epoch and re-runs the recovery barrier (recovering the latest
durable state and republishing the manifest), then replays the mutation once. The
retry is bounded to a single attempt, so a genuine ownership disagreement surfaces
as the fence rather than an epoch-thrash loop. Only a controller that still owns the
namespace reaches the append (owner-routed creates; ownership-gated repair), so
re-acquire reclaims the metadata log for the rightful owner rather than stealing it
from another.

### Consensus outage

Existing open-chunk appends and reads continue because they do not require
metadata. Metadata boundary operations that need leadership proof, manifest
publication, namespace assignment, or chunk creation/seal retry or queue until
the consensus root is available.

### Data-node failure

Node liveness transitions are cluster-level events. When a node becomes `DEAD`,
each namespace leader consults its local `node -> chunks` index and schedules
repair only for chunks it owns. Repair is parallel across namespaces and storage
nodes.

### Metadata system-file damage

System metadata files use the same Strata chunk replication, seal verification,
and checksum model as user files. A namespace leader cannot acknowledge a
metadata mutation until the corresponding metadata-log append is durable under
the configured policy.

## 15. Durability and availability rules

- A chunk descriptor is committed before any data byte is written.
- A metadata mutation is acknowledged only after it is appended to the owning
  namespace metadata log.
- Manifest publication is CAS-guarded by `metadataEpoch`.
- Snapshot and index files are immutable once sealed.
- Log records are replayable and idempotent.
- A namespace leader serves metadata requests only after the recovery barrier has
  loaded and verified snapshots, recovered the log tail, replayed all durable
  records, and rebuilt derived indexes.
- Deletion leaves a fencing tombstone until the configured cleanup window.
- Metadata-log compaction can delete old log segments only after a sealed
  snapshot/index generation is published in the consensus manifest.
- Tombstone and idempotency retention define the lower bound for safe log
  truncation.
- Consensus stores roots and epochs, not the high-cardinality metadata body.
- The data path does not depend on the availability of the controller leader.

## 16. Implementation sequence

### Step 1: lock the target root schema

Define the production root schema before adding more metadata features:

```text
controller membership generation
namespace -> replicaSet, preferredLeader, leader/epoch/manifest
```

The v0 ZooKeeper-backed `MetadataStore` and existing conformance suite remain
useful as a semantic reference while the new backend is built, but they are not
the target storage model.

### Step 2: persist namespace assignment root state

Add a compact, CAS-guarded namespace assignment record to the root metadata
store. This validates the static rendezvous assignment policy without moving
file/chunk descriptors yet, and it keeps request routing based on persisted
ownership rather than local recomputation.

### Step 3: build the system-file metadata backend for one namespace

Implement the system-file metadata backend for a single namespace first. Keep
the same `MetadataStore` conformance suite green against ZooKeeper, in-memory,
and the new namespace-log backend. The current bridge covers file/path semantics,
snapshot publication, recovery, and metadata-log compaction foundations; the
runtime controller can be wired to it explicitly with
`STRATA_CONTROLLER_BACKEND=namespace-log`. The current runtime uses the
ZooKeeper-backed root metadata store for metadata-log file descriptors and
`StrataSystemMetadataFileStore` for the metadata-log bytes. The remaining work
before production large-scale churn tests is hardening multi-chunk metadata
system files and background old-file garbage collection.

### Step 4: enable namespace sharding

Enable many namespaces, each with its own controller leader assignment. Route
operations by namespace or by a namespace-qualified file handle. Namespaces use
static rendezvous assignment over the current controller membership. Move
`node -> chunks`, path indexes, tombstones, and repair queues out of direct
ZooKeeper znodes.

### Step 5: remove direct-ZooKeeper high-cardinality metadata from the target path

ZooKeeper or another consensus backend remains for root state only. The target
runtime must not require direct znodes for file/chunk descriptors or reverse
indexes.

### Step 6: optional intra-namespace sharding

Implement only if measurements show one namespace needs more than one metadata
leader. Candidate split modes are `HASHED_PATH` or `FILE_ID`, but they require
fan-out listing and are intentionally deferred.

## 17. Success criteria

- No consensus parent has millions of children.
- No metadata operation requires one consensus write per file or chunk.
- No repair loop performs periodic global `listFiles()`.
- `node -> chunks` is derived from namespace metadata logs/snapshots, not stored
  in consensus.
- `ListFiles(namespace)` is routed to one namespace leader and is always paged.
- Namespace placement is deterministic, persisted, and stable across metadata
  process restarts.
- Adding metadata-capable nodes does not automatically move existing namespaces.
- Namespace metadata failover recovers from manifest + snapshot + log tail.
- Namespace metadata requests are rejected or retried while the leader is
  `RECOVERING`; only `ACTIVE` leaders serve reads and writes.
- Metadata-log storage is bounded by snapshot cadence plus tombstone and
  idempotency retention, not by total historical metadata mutations.
- Old metadata-log segments are deleted only after their compacted state is
  present in a manifest-published sealed snapshot/index generation.
- A node failure fans repair work out across namespace leaders.
- The data path continues through controller leader failover and consensus
  outages until an operation reaches a metadata boundary.
- The shared `MetadataStore` conformance suite passes against the scalable
  system-file backend.

## 18. Formal model

The recovery and log-authority core is modeled in
[tla/MetadataRecovery.tla](tla/MetadataRecovery.tla). The model covers one
namespace and checks:

- the metadata log contains only authoritative metadata records;
- compacted snapshots preserve the same authoritative state as full log replay;
- `node -> chunks` is a rebuildable derived index;
- a namespace leader becomes `ACTIVE` only after replaying the durable log tail
  and rebuilding derived indexes.

Run it with:

```bash
./scripts/tlc.sh MetadataRecovery
```

The full TLA gate runs all configured models:

```bash
./scripts/tlc.sh
```

## 19. Open questions

- Exact retryable error code or response shape for namespace leaders that are
  still `RECOVERING`.
- Exact system metadata file bootstrap sequence before the first manifest
  exists.
- Whether node heartbeat command aggregation is implemented as a gateway role,
  direct namespace polling, or another bounded command-outbox mechanism.
- Snapshot/index file format and compaction cadence.
- Operator-driven namespace movement between metadata nodes for repair or explicit
  rebalancing.
- Metrics that should trigger optional intra-namespace sharding.
- Security model for namespace root ACLs and system metadata namespace access.

## 20. Namespace in the data plane and long file ids

This section is the data-plane realization of the sharding model. A chunk's global
identity becomes the triple `(namespace, fileId, index)`: `fileId` is a `long`
unique *within a namespace*, assigned by that namespace's authority (the owner for
user namespaces; the consensus root for the system `strata-meta` namespace). On
disk the namespace is the parent directory, so storage shards by directory and a
walk yields the namespace for free; durability reconciliation moves from a central
inventory push to owner-driven verification plus node-local, confirm-before-delete
orphan GC. It is staged as three dependent phases (data model → owner verify →
orphan GC); **all three phases have landed**. The central inventory push has been
removed — durability reconciliation is now owner-pull `VERIFY_CHUNKS` plus
node-local, confirm-before-delete orphan GC.

**Locked decisions.**

1. The namespace lives on disk as a **directory** (`chunks/<ns>/…`), not a header
   field — free `du`/`ls`/`rm -rf`, namespace-for-free during walks, and no on-disk
   record-format change for the namespace.
2. `fileId` is a **per-namespace owner-assigned `long`** (BookKeeper `ledgerId`
   model), not a UUID. `(namespace, fileId, index)` is the global identity; a bare
   `(fileId, index)` is *not* globally unique (e.g. `fileId 0` exists in every
   namespace).
3. The directory shard splits **low bits first** so sequential ids spread evenly
   from the first file, rather than clustering early ids under `00/00/…`.
4. Clean break: bump `FORMAT_VERSION`, no migration or dual-read.

### 20.1 File id generation (two layers)

| Layer | Whose files | Generator |
|---|---|---|
| User | per-namespace user files | the owner's monotonic `nextFileId` (no consensus round) |
| System | `strata-meta` log segments + snapshots | a small consensus-root counter |

- **User (owner) generation.** Each namespace owner keeps a monotonic `nextFileId`
  and `createFile` assigns the next value, riding the existing single-writer
  manifest fence — no new global coordination. It is a **high-water field in the
  namespace snapshot** (and in-memory state), *not* a separate log (which would
  double appends) and *not* derived from `max(live files)` (a swept tombstone would
  forget its id and risk reuse). It only ever increases. On failover the successor
  loads the snapshot's `nextFileId` and replays the tail; assign-then-append is
  safe because if the owner crashes after assigning N but before `FileCreated(N)`
  is durable, no file exists at N and reissuing N is harmless.
- **System generation.** A user namespace's `nextFileId` lives inside its meta-log,
  which is itself stored as `strata-meta` files — so the system namespace cannot
  host its own counter (it would recurse). System file ids come from a small
  CAS counter in the consensus root scoped to `strata-meta` (low volume; does not
  reintroduce a high-volume id allocator on the user path).

### 20.2 On-disk layout and low-bit shard

```text
<dataDir>/chunks/<namespace>/<L1>/<L2>/<fileId-16hex>.<index>.{chunk,meta,j}
```

- `<namespace>` is the tenant directory — free `du -sh chunks/<ns>`, `rm -rf
  chunks/<ns>` for namespace deletion, and the namespace for free on any walk
  (which powers the reconcile and orphan-GC routing below). The namespace charset is
  constrained at the control plane (`[A-Za-z0-9._-]`, bounded length) so the
  directory name *is* the namespace verbatim — no escaping.
- `<L1>/<L2>` is a low-bit split of the long (`L1 = fileId & 0xFF`,
  `L2 = (fileId>>8) & 0xFF`; 256-way fan-out, tunable). Sequential ids round-robin
  across `L1` from the first file; a file's chunks share its `fileId` and so share
  the shard directory (locality).
- `<fileId-16hex>` is zero-padded hex so lexical sort equals numeric sort. The
  header still carries the 12-byte `chunkId` for self-identification; the namespace
  is **not** in the header (it is the directory). Recovery walks
  `chunks/<ns>/<L1>/<L2>/*`, derives `(namespace, fileId, index)` from the path, and
  recovers namespaces in parallel.

### 20.3 Owner-driven reconcile (replaces the inventory push) — *implemented*

Replaces "every node pushes its full chunk list to the leader every 30s" with
owner-pull batched verification: a `VERIFY_CHUNKS` RPC (opcode `0x001c`) where each
owner's verify thread (`RepairCoordinator.verifyPass`, settle-gated for a fresh
leader) iterates its own namespaces' files and asks each expected replica node, in
bounded batches, for the local state of each chunk; the owner judges
`present-ok / missing / corrupt` against its descriptor (reusing the grace +
`isRepairProtected` guards), `missing|corrupt` drops the replica, and the
under-replication scan re-replicates within its namespace. Fail-safe: an unreachable
node yields no verdict, never a drop. Node-death under-replication stays on the
reconcile scan; the node stamps `lastVerifiedAtMs` per chunk to feed orphan GC.
`INVENTORY_REPORT` and its messages are removed.

*Deferred (pure scale optimization):* node-side `scrubOnce` is still a full re-CRC
of all sealed chunks per cycle (it keeps its deterministic single-call behavior).
The rolling-fraction re-CRC remains a follow-up.

### 20.4 Orphan GC (node-local: grace → owner confirm → fail-safe) — *implemented*

A chunk not verified by any owner within a grace window becomes a *suspect* (not a
delete). Before deleting, the node derives the namespace from the chunk's directory,
routes to `ownerOf(namespace)`, and asks "does chunk X's descriptor still list me as
a replica?":

| owner answer | node action |
|---|---|
| no / file gone / no such namespace | confirmed orphan → delete the three files |
| yes, you should have it | not orphan → reset timer, keep |
| unreachable / error | do **not** delete; retry later (fail-safe) |

Worst case is delayed reclamation, never data loss. This confirm step is the unique
justification for putting the namespace on the node: owner-driven verify alone does
not need it; safe node-local GC does.

*Implementation note (deviation):* the spec's strict "trust no-owner-verified only
after hearing a verify from **every** owner" would deadlock when an owner has no
described chunks on a node (it never contacts the node) — exactly the all-orphan
case the test plants. Because the per-chunk `LOOKUP_FILE` confirm is **authoritative**
(the owner recovers its meta-log before answering, §14, so a `FILE_NOT_FOUND` is
never a stale empty-state false positive) and fail-safe (unreachable ⇒ keep),
`OrphanGc` instead uses a time-based **node-startup grace** plus that confirm —
deadlock-free and equally safe. A dynamic owner-set change resets the startup grace;
the v0 owner set is static.

### 20.5 Correctness anchors

- **Id reuse on owner failover** (corruption guard) — the highest risk. The
  high-water-in-snapshot plus tail-replay recovery, and a failure-injection test
  across the assign/append/snapshot/compaction crash windows, assert no file id is
  ever reassigned.
- **Orphan false-positive fail-safe** (data-loss guard) — an owner unreachable
  during confirm must not trigger a delete; the node retries and keeps the chunk on
  "you should have it".
- This design **reverses the earlier namespace-scope-preference** (namespace was
  metadata/control-plane only): the namespace now lives in the data plane,
  justified by self-describing storage, ops visibility, and safe node-local GC.
- For how an owner fenced on a stale epoch reclaims its meta-log rather than
  wedging, see §14 "Stale-epoch owner re-acquire".
