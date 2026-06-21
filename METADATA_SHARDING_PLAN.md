# Metadata namespace sharding — clean re-implementation plan

**Base:** `dfabc85e` (clean). Spec: `strata-metadata-scaling-design.md` + `tla/Metadata*.tla` (6 models).
This is a from-scratch rebuild on the current base (the earlier WIP was on a divergent base
`0311b09e` and could not be merged). Build with `mvn -o -pl strata-meta -am test` (`.m2` warm;
always `-am` to avoid the stale-proto trap).

Goal (user, 2026-06-19): shard the metadata plane by namespace; each namespace has exactly one
**Meta owner**; behind each owner is a set of **ZK-backed strata-meta-files** (the namespace
metadata log/snapshot — Strata files whose physical descriptors live in ZooKeeper, bytes are
replicated Strata chunks). User files are managed through the owner's metadata log.

## What the base already gives us
- `MetadataStore` SPI is partly namespace-aware: `resolvePath(ns,path)`, `deletePath(ns,…)`,
  `listFiles(ns)`, `listNamespaces()`, `sweepDeletedFiles(olderThanMs)`.
- `Placement.choose(StrataNamespace, NodeRegistry, count, exclNodes, exclHosts)` is namespace-aware.
- `Opcode` append-only, last = `ALLOCATE_WRITER_EPOCH(0x0209)`; data ends `READ_RECOVERY(0x001A)`.
- `ErrorCode.NOT_LEADER(15, retriable)` + `TAG_LEADER_HINT(1)` already drive client redirects.
- `ControlLoop.replicate(ReplicateCmd)` is a generic pull handler, reusable for direct repair.
- `AppenderImpl`/`Recovery` can store the metadata log itself as a StrataFile (unclamped
  `READ_RECOVERY` seals its open tail — see memory `recovery-read-bypasses-durable-clamp`).

## Milestones (each ends green: `mvn -o -pl strata-meta -am test`)

- [x] **M0 — Baseline.** Clean base green: 125 strata-meta tests, `mvn install` exit 0. Regression floor.
- [x] **M1 — Root schema + assignment + epoch (design §16 Step1/2).** DONE, 138 tests green.
  `NamespaceAssignmentPolicy` (rendezvous/HRW) + `NamespaceOwnership` resolver; `Records.NamespaceAssignment`;
  `MetadataStore` default methods `allocateMetadataEpoch`/`get|putNamespaceAssignment`/`listAssignedNamespaces`
  (root-store capabilities, throwing/empty defaults); `ZkMetadataStore` impl (`/strata/meta/epoch` CAS counter,
  `/strata/meta/namespaces/<ns>/assignment` CAS). Tests: policy(6), ownership(4), ZkRoot(3).
- [x] **M2 — Ownership routing (design §16 Step4 routing).** DONE, 140 tests green.
  `MetadataService.requireNamespaceOwner(ns)`/`requireNamespaceOwnerForFile(id)` gate each namespace-scoped
  op → NOT_LEADER+owner hint for non-owner; node-control ops keep the global latch; file-id ops resolve ns
  from the shared root store. Single-endpoint ⇒ ownsAll ⇒ unchanged. `MetaConfig.controllerEndpoints/
  controllerReplicaCount` + `withControllerEndpoints` (backward-compatible ctors). Client uses existing
  NOT_LEADER-hint following (per-namespace cache deferred — perf only). Test: `MetadataShardingRoutingTest`.
- [x] **M3 — Shared liveness (design §11).** DONE, 141 tests green. `NodeRegistry.publishClusterLiveNodes`
  (controller-only, hooked in `RepairCoordinator.scanOnce`) + `candidatesFor` merges the snapshot
  (`Records.ClusterLiveNodes` codec, `MetadataStore.get/putClusterLiveNodes`, `/strata/meta/live-nodes`).
  Test: `MetadataSharedLivenessTest` (non-controller owner places via the snapshot).

  **REORDER (2026-06-19):** M5 (backend) now precedes M4 (per-ns repair). Rationale: with the shared ZK
  store the controller already repairs every namespace correctly, so per-namespace repair is only *required*
  once metadata is partitioned into per-namespace logs (M5). The `namespace-log` backend ships default-off
  and single-node-correct first; M4 then makes its multi-node repair work.

- [~] **M5 — Strata-meta-file metadata-log backend (design §16 Step3, §8–§10, §13). THE CORE.** Built
  bottom-up, each sub-step green. **M5.1–M5.6 DONE (173 tests); the backend PASSES the full MetadataStore
  conformance suite — byte-exact SPI-equivalence to the ZK store.**
  - [x] **M5.1** `MetadataLogRecord` (14 families) + `MetadataLogCodec` (versioned, type-byte). 144 tests.
  - [x] **M5.2** `MetadataLogSegment` + `MetadataLogSegmentCodec` (CRC32C frames, torn-tail recovery). 149.
  - [x] **M5.3** `NamespaceMetadataState` (replay → file/path/chunk tables + derived node→chunks +
    tombstone-ts index). 155.
  - [x] **M5.4** `NamespaceMetadataState.Snapshot`+`NamespaceMetadataSnapshotCodec` (export/restore, CRC) +
    `Records.NamespaceManifest` + `MetadataStore.get|putNamespaceManifest` CAS (`/strata/meta/namespaces/<ns>/
    manifest`). 159.
  - [x] **M5.5** `NamespaceMetadataFileStore` (+ in-mem `TestNamespaceMetadataFileStore`),
    `NamespaceMetadataRecovery` (snapshot + open-log-tail replay), `NamespaceMetadataLogRepository`
    (durable append, compact, manifest version-CAS, recovery-time roll/fence). 162.
  - [x] **M5.6** `MetadataLogDiff` (wholesale-update → semantic records, createOp-aware) + `NamespaceLogBackend`
    (engine: per-ns repos + fileId→ns index + root delegation) + `NamespaceLogMetadataStore` (SPI facade) +
    `NamespaceLogMetadataStoreConformanceTest` — full conformance green. 173.
  - [x] **M5.7** DONE (175 tests). Durable `LocalNamespaceMetadataFileStore` (on-disk log/snapshot bytes) +
    restart-recovery test (`NamespaceLogBackendDurabilityTest`: fresh backend over same dir+ZK recovers
    files). Wired into `MetadataService` via a backend-factory constructor (`STRATA_CONTROLLER_BACKEND=
    namespace-log` + `STRATA_CONTROLLER_LOG_DIR`; default `zk` path byte-identical → no regression). E2E test
    (`MetadataServiceNamespaceLogBackendTest`: create/lookup/lookupPath/delete over SCP on the log backend).
    REMAINING HARDENING (not blocking the goal): replicated-chunk `StrataSystemMetadataFileStore` (bytes as
    Strata chunks for cross-node failover; bootstrap §19), fsync-on-append, eager namespace recovery on
    startup so `getFile(fileId)`-without-ns works pre-load.
- [x] **M5 — Strata-meta-file metadata-log backend. COMPLETE.** The user's core ask is delivered: each
  namespace's user-file metadata is stored as a per-namespace metadata log (a ZK-backed strata-meta-file:
  manifest/descriptors in ZooKeeper, bytes in the file store), byte-exact SPI-equivalent to the ZK store
  (full conformance green), durable across restart, and selectable in MetadataService.
- [x] **M5R — Replicated-chunk metadata-log store (REQUIRED, replaces the local-disk shortcut).** Per the
  user: user-namespace metadata (Files/Chunks) MUST be stored as Strata files, not local disk.
  `StrataSystemMetadataFileStore` stores the meta-log/snapshot bytes as **replicated Strata chunks** on the
  data nodes (descriptors in ZK), reusing the client `Appender`/`Reader`/`recoverAndSeal` machinery via
  an embedded `StrataClient` self-connected to this controller node's endpoint. Recursion-break + deadlock-safety:
  `NamespaceLogBackend` routes the reserved `strata-meta` system namespace to the ZK root **lock-free**
  (`getFile` is index-first then root-fallback, lock-free); MetadataService bypasses ownership for the
  system namespace. Wired as the default for `STRATA_CONTROLLER_BACKEND=namespace-log`
  (`STRATA_CONTROLLER_LOG_RF`/`_ACK`; settable via system property for tests). `strata-meta → strata-client`
  dep added (acyclic). ScpServer is per-connection single-thread vthread → the self-loop (separate
  connection) does not deadlock. IT: `strata-it/NamespaceLogMetadataBackendTest` — 3-node cluster, user
  file write/read + verifies the meta-log is replicated Strata chunks in the system namespace.
- [~] **M4 — Single-writer-safe repair + direct owner heal (design §11).**
  - [x] **M4a — orphan-delete safety (DATA-LOSS GUARD) DONE (176 tests).** `RepairCoordinator` takes an
    `ownsAll` BooleanSupplier (overloaded ctor, default `()->true` ⇒ existing 32 repair tests unchanged).
    `onInventory` only orphan-deletes a not-found chunk when non-sharded OR the file is present — so a
    sharded controller never deletes a chunk belonging to a namespace owned by another controller node. Wired
    `ownership::ownsAll` from MetadataService (ownership now built before the repair coordinator). Test:
    `RepairCoordinatorTest.shardedControllerDoesNotOrphanDeleteAChunkItDoesNotOwn`. Note: the
    under-replication scan is naturally scoped already — `allFileIds()` = `listNamespaces×listFiles`, and the
    namespace-log backend's `listNamespaces` returns only loaded/owned namespaces (zk backend returns all).
  - [x] **M4b — direct owner heal (`EXEC_REPLICATE`) DONE (177 tests).** Append-only `EXEC_REPLICATE(0x020A)`
    opcode (golden corpus opcode-table + HelloResp bytes updated via test capture); storage-node handler
    (`NodeHandlers` EXEC_REPLICATE case reusing the now-package-visible `ControlLoop.replicate()`; wired in
    `StorageNode`); `RepairCoordinator.ownerRepairPass()` runs on non-controllers — reads the controller's
    DEAD set from `store.listNodes()`, finds under-replicated sealed chunks in owned namespaces, picks a
    target from the M3 snapshot via `Placement.choose`, sends EXEC_REPLICATE, then CAS-writes the replica
    swap. `ownsNamespace` predicate wired from MetadataService (`openedOwnership::isOwner`). Test:
    `RepairCoordinatorTest.nonControllerOwnerHealsUnderReplicatedChunkViaExecReplicate` (fake storage server
    acks EXEC_REPLICATE; verifies the dead replica is swapped for the target).
- [x] **M4 — repair COMPLETE.** Multi-node namespace-log repair is now data-loss-safe (M4a) and functional
  (M4b owner-heal). Proto + node + meta all green (proto 83, node 47, meta 177).
- [ ] **M6 — Recovery lifecycle.** Synchronous-on-first-access STANDBY→ACTIVE (design's first version);
  explicit async RECOVERING barrier noted as follow-up.

## Test coverage for the new controller (namespace-log backend)
- **Unit/component:** codecs, segment torn-tail recovery, state replay, snapshot, manifest CAS, ownership,
  the diff bridge — `strata-meta` suite (179 tests).
- **Integration:** the full `MetadataStore` conformance suite on the namespace-log backend; manifest-CAS
  fencing (`NamespaceMetadataLogRepositoryTest`); routing/shared-liveness/owner-heal (`MetadataSharding*`,
  `RepairCoordinatorTest`); end-to-end over SCP (`MetadataServiceNamespaceLogBackendTest`); + cross-process
  `strata-it/NamespaceLogMetadataBackendTest` (3-node cluster — meta-log is replicated Strata chunks).
- **Failure injection:** `NamespaceMetadataLogFailureInjectionTest` — `FailureInjector` seams
  `meta.log.afterDurableAppend` (crash after durable append → successor recovers the record; byte-durability)
  and `meta.log.beforeManifestPublish` (crash mid-compaction → old manifest recoverable; compaction atomicity).
- **Chaos/failover:** `strata-it/NamespaceLogMetaFailoverTest` — lose+replace the controller node (fixed port);
  user metadata survives, recovered from the replicated log on data nodes. `NamespaceLogMetaLogRepairTest`
  — kill a data node holding metadata-log chunks; the controller re-replicates the meta-log back to RF
  (the system namespace is covered by repair; the metadata-of-metadata is not a SPOF).
- **Durability hardening:** meta-log `WritePolicy` is `fsyncOnAck=true` (power-loss safe). Eager
  ownership-scoped namespace recovery so `getFile(fileId)`/`openById` resolves right after a restart
  (`NamespaceLogBackendDurabilityTest.getFileByIdResolvesAnUntouchedNamespaceAfterRestart`).

## TLA invariants the impl must honour (spec gate)
- MetadataManifestCAS: only current-epoch ACTIVE leader publishes a manifest (version-CAS barrier).
- MetadataTwoLeaderFencing: epoch fence ⇒ at most one effective writer per namespace.
- MetadataRecovery: ACTIVE only after replay of durable log tail + derived-index rebuild.
- MetadataTombstoneSweep: TombstoneSwept only after retention window AND tombstone in published snapshot.
- MetadataByteDurability: a metadata mutation is acked only after its log append is quorum-durable.
- MetadataIdempotency: (namespace, opId) dedups retried mutations across failover.
