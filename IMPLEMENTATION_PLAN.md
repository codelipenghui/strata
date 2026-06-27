# Strata v0 — Implementation Plan & Status

Working state for the v0 build (tech design §4.4 bootstrap path: ZooKeeper-backed metadata, no Kafka fork).
This file is the durable source of truth for the development loop — update the checkboxes and decision log as work lands.

## Locked decisions

- **Stack:** Java 21 (Corretto), Maven multi-module (Maven 3.8.8 installed), JUnit 5, SLF4J-simple.
- **Networking:** Netty NIO transport for SCP; blocking metadata/storage handlers run off the event loop on serialized per-connection virtual-thread executors.
- **ZK access:** Apache Curator (framework + recipes + curator-test for embedded ZK in tests).
- **Chaos:** testcontainers + Toxiproxy (gated on Docker availability).
- **Model checking:** TLA+ (tla2tools.jar fetched on demand), spec under `tla/`.
- **v0 protocol additions** (additive per tech design §10.6; fold back into the design doc at finalize):
  - `READ_LEDGER` (0x0019): returns integrity-ledger entries from an offset — seal recovery needs per-append boundaries without parsing payload.
  - Client↔metadata over SCP, opcode range 0x02xx: `CREATE_FILE` 0x0201, `CREATE_CHUNK` 0x0202, `SEAL_CHUNK_META` 0x0203, `LOOKUP_FILE` 0x0204, `DELETE_FILES` 0x0205, `SEAL_FILE` 0x0206, `ABORT_CHUNK_META` 0x0207, `LOOKUP_PATH` 0x0208, `ALLOCATE_WRITER_EPOCH` 0x0209. (v1 moves broker-facing APIs to Kafka RPC; range stays for tools.)
  - `NODE_HEARTBEAT` request gains tagged field `completedCommands` (commandId → status) so the repair coordinator learns copy completion.
  - `SEAL_CHUNK` (0x0015): footer sections from caller are optional; the node always computes CRC_RANGES + STATS itself so recovery-sealed chunks are byte-identical across replicas.
  - Error codes added: 15 NOT_LEADER (tagged leader hint), 16 NO_CAPACITY (placement cannot find 3 nodes).
- **Epoch source:** metadata allocates a monotonic writer epoch per file; the storage layer is agnostic to how the controller sources it.
- **v0 controller:** single active instance in tests (Curator leader election implemented); leases in leader memory, registrations + file/chunk records in ZK (CAS via znode versions, leader-only writes).
- **File naming:** every file has a first-class `StrataNamespace` plus `StrataPath`. `FileId` is globally unique; `(namespace, path)` is unique only while a file is live. The v0 ZK backend stores path bindings under `/strata/namespaces/<namespace>/paths/<path>/__file`, leaving that namespace node as the future ACL/quota root.

## Module layout

```
strata-common   ids (FileId/ChunkId/NodeId), Varint, Crc32C, ErrorCode, exceptions
strata-proto    SCP frame codec, tagged fields, opcodes, message structs, Netty-backed ScpClient/ScpServer
strata-format   chunk file header/footer/trailer, sidecar .meta, integrity ledger .j, ChunkStore engine + crash recovery
strata-node     data node: SCP handlers → ChunkStore; register/heartbeat/inventory loop; REPLICATE executor (pull)
strata-meta     MetadataStore SPI, ZkMetadataStore, MetadataService (SCP listener, placement, leases, repair, retention)
strata-client   StrataClient/StrataFile/Appender/Reader per design §12 (quorum ack, DO, roll, create-ahead, recoverAndSeal)
strata-it       integration tests: in-process cluster + embedded ZK (primary correctness layer); chaos via testcontainers
tla/            ChunkReplication.tla + TLC config
scripts/        verify.sh (full pyramid), run helpers
```

## Stages (gate: tests green, then commit) — ALL COMPLETE

- [x] 1. Scaffold Maven multi-module; commit docs + plan + scaffold
- [x] 2. strata-common primitives + unit tests
- [x] 3. SCP framing + messages + roundtrip/golden tests
- [x] 4. Chunk store engine + crash recovery + torn-write tests
- [x] 5. Storage node server + single-node integration test
- [x] 6. Controller on ZK (embedded-ZK tests)
- [x] 7. Client library + 3-node end-to-end happy path
- [x] 8. Failure paths: fencing, seal-and-roll, seal recovery (§7.3) + fault-injection tests
- [x] 9. Repair + reconciliation + retention + tests
- [x] 10. Chaos suite (testcontainers: toxiproxy slow-replica/blackhole, ZK pause)
- [x] 11. TLA+ spec + TLC run (315K states, 4 invariants, negative control verified)
- [x] 12. verify.sh, design-doc updates (v0 additions), final commit

## Findings locked during implementation (also folded into the tech design doc)

- **Virtual-thread pinning:** never hold `synchronized` across blocking I/O or where response
  handlers contend; `ReentrantLock` + async callback dispatch off reader threads. (Manifested as a
  full-JVM stall: callbacks blocked on the appender monitor pinned every carrier thread.)
- **Per-replica append timeout** is required equipment: a black-holed connection must fail one
  replica into seal-and-roll, not starve the appender into quorum loss.
- **Fence check dominates state check** in `append` — a deposed writer must see FENCED_EPOCH
  (permanent death), never CHUNK_SEALED ("roll and continue").
- **DO clamping:** a piggybacked DO is clamped to the pre-append end — a DO claim can never cover
  the bytes of the append carrying it.
- Error codes 15–18 added (NOT_LEADER, NO_CAPACITY, FILE_NOT_FOUND, FILE_SEALED).

## Hardening pass (post-v0)

- [x] **Metadata failover test** (2 instances, leader kill mid-stream). Bugs found and fixed:
  - standby's NodeRegistry never learned of nodes registered after its boot → re-registration
    allocated NEW nodeIds; fix: registration falls back to the persistent store for incarnation
    matching (identity source of truth).
  - RepairCoordinator ran on standbys → would declare all nodes DEAD (no heartbeats route to a
    standby); fix: leadership gate + settle period (lease + grace) after acquiring leadership.
  - MetaClient double-rotated endpoints on connect failure → with 2 endpoints, every retry hit
    the dead instance; fix: rotation owned solely by call(), which now retries retriable errors
    with backoff up to a deadline (failover absorbed by ALL metadata operations).
- [x] **ack-on-fsync end-to-end**: policy verified in every replica's chunk header on disk;
  write/seal/read + seal recovery under fsync mode; mixed-mode files coexist.
- [x] **Perf smoke** (`PerfSmokeTest`, tag `perf`; `./scripts/verify.sh --perf`). First numbers
  (M-series laptop, 3 nodes in-process): replicate-mode write p50 0.3ms / p99 ~6ms @ window 256,
  ~21 MB/s at 1 KB records; sequential read ~280-400 MB/s; fsync-mode p50 ~184ms @ window 16 —
  per-append force with serial replica processing ⇒ group commit is required v1 work
  (tech design §17.11).
- [x] **Group commit for ack-on-fsync** (tech design §5.3, §17.11 resolved). Per-chunk flusher
  coalesces forces; append acks defer at the server (handleAsync — connection keeps processing
  while waiters await a covering force; out-of-order responses are protocol-legal). Two findings:
  - in-flight fsyncs throttle concurrent data/ledger writes at the OS level — without an
    accumulation gap, coalescing degraded to ~3 appends/force; a 5ms clean window before each
    force restored real batches (0ms→3, 1ms→6, 3ms→16, 5ms→44 appends/force on shared-SSD laptop).
  - regression guards: GroupCommitTest (engine), FsyncPipelineWireTest (wire),
    FsyncAppenderPipelineTest (full stack) — all assert forces << appends.
  Result @ window 256: fsync p50 2,941ms → 75ms (39×), throughput 0.1 → 3.2 MB/s (32×);
  replicate mode and read path unchanged.
- [x] **Review fixes (5 findings, all verified real, fixed with TDD)**:
  - P1 recovery seal divergence: Recovery.finishSeal now requires all successful seal responses
    to agree on length+crc (the appender's check) — never commits metadata over divergent bytes
    (`RecoveryDivergenceTest` constructs same-length-different-bytes replicas).
  - P1 stuck repair: in-flight commands are swept when the executing node is DEAD or the command
    aged past `repairCommandTimeoutMs` (new MetaConfig field) — the chunksBeingRepaired marker is
    released and the next scan re-issues (`RepairReliabilityTest`).
  - P2 SEAL_FILE validation: refuses unless every chunk is SEALED and lengths sum to the
    requested total (new error code 19 PRECONDITION_FAILED); also closes the no-epoch zombie
    file-seal hole, since a live appender implies an open chunk.
  - P2 seal idempotence: requires length AND crc match; same-length-different-crc is refused.
  - P2 inventory reconciliation: sealed entries must match descriptor length+crc; corrupt
    replicas are dropped from the descriptor (re-repaired; their copy becomes an orphan and is
    deleted by the existing orphan path).

## Full-repo review pass (4 parallel reviewers + adjudication; all fixes verified by tests)

Real bugs fixed (TDD where deterministically testable):
- **Repair could only SWAP dead replicas, never ADD** — a corrupt-replica drop left chunks
  stranded at RF=2 forever. Add-mode repair (deadNode=-1 sentinel); corrupt copies are now also
  physically deleted FIRST (FIFO before any re-replicate to the same node), and node-side
  replicate validates an existing local copy against expected len/crc instead of trusting it.
  (`RepairReliabilityTest` extended.)
- **Recovery could not catch up a replica behind the durable offset** — its re-replication
  APPEND at base=p hit OFFSET_GAP, was swallowed, and recovery "succeeded" leaving a short OPEN
  replica in the descriptor. Recovery now has a catch-up phase (also used for the found-sealed
  path), per-replica end tracking, and evicts-with-quorum-check any replica that cannot reach
  the seal point; finishSeal only includes caught-up replicas. (`RecoveryCatchUpTest`.)
- **Node-record writes were unconditional** — a deposed leader's expire scan could overwrite the
  new leader's REGISTERED with DEAD. putNode is now CAS by znode version; the registry tracks
  versions; a lost CAS adopts the store state. (`ZkMetadataStoreCasTest`.)
- **Sealed-chunk recovery never verified the footer CRC** — rotted footer metadata was served as
  healthy. Footer (KBs) now CRC-verified at recovery; mismatch quarantines the chunk, preserving
  evidence. Full data-region verification stays deferred to scrub. (`CrashRecoveryTest`.)
- **Protocol input validation**: negative payload length passed the frame length-equality check
  (26+1-1=26) and killed connections with unchecked exceptions; adversarial list counts hit
  ArrayList "Illegal Capacity"; incompatible HELLO got a silent close instead of
  UNSUPPORTED_VERSION; tagged-field count/size unbounded. All now typed protocol errors.
  (`AdversarialInputTest`.)
- ScpClient: orphaned-future race (send vs failAll between closed-check and registration) and
  close() not failing pending futures; appender sealChunkMeta failure now dies cleanly instead
  of leaking a half-sealed session; applyReplicaSwap skips a target that died before the swap;
  driveDeletion dead `allConfirmed` logic removed; GroupCommitter interrupt drains waiters and
  close() confirms flusher termination (stuck flusher fails the operation rather than racing
  file mutations); importSealed range-checks trailer dataLength; OPEN-chunk-without-ledger now
  logged loudly; HELLO announces the registered nodeId (was -1 until restart).

Adjudicated NOT bugs: duplicate command completions (deduped by inflight.remove), node
re-registration on a live connection (stateless RPC), one reviewer self-retraction.

## Second external review (5 findings, all verified real, fixed with TDD)

- **P1 seal commits only confirmed replicas**: SEAL_CHUNK_META gained a sealedReplicas list;
  the descriptor keeps the intersection (a client-failed-but-alive replica left listed would
  serve short reads forever — alive, so repair never fires). Add-mode repair restores RF after
  a subset seal. Inventory now also drops OPEN stragglers under SEALED descriptors (they never
  converge by themselves), and the reader rejects sealed-read responses shorter than the
  descriptor length (defense in depth). (`RepairReliabilityTest` x2 new tests.)
- **P1 live files never lose chunk records**: applyDeleteConfirmed keeps an empty-replica chunk
  record unless the file is DELETING — erasing it would silently shorten the file and shift
  reader offset accounting; total loss now surfaces as a hard read failure + ERROR log.
- **P2 negative wire values guarded at the engine boundary**: read/seal/fetch reject negative
  offsets/lengths — a negative seal length previously passed range checks and
  truncate(DATA_START-5) would destroy the chunk HEADER before throwing. (`ChunkStoreTest`.)
- **P2 correlation-entry leak**: every ScpClient future now removes its pending entry on ANY
  completion path (response, failure, caller timeout, orTimeout/cancel); callFrame completes
  the future exceptionally on timeout. (`ClientServerTest`.) Note: reusing the connection after
  a timeout is correct — correlation ids make late responses harmless.
- **P3 ControlLoop.close() interrupts and joins its virtual threads** (executeCommands no
  longer lingers in commandQueue.take()).

## Third external review (6 findings, all verified, fixed with TDD)

- **P1 CREATE_CHUNK rejected while the tail chunk is OPEN** — no legitimate flow creates over an
  open tail (appenders seal before rolling, recovery seals before new appenders open); racing
  same-epoch appenders could otherwise both claim file offset 0 in different chunks. Fence check
  keeps precedence. Same-epoch dual writers after a seal remain a caller-contract violation that
  v1 controller-issued unique epochs eliminate. (`MetadataServiceTest`.)
- **P1 DELETING files are immutable to writers** — seal-chunk and SEAL_FILE now reject DELETING
  (SEAL_FILE previously RESURRECTED a half-deleted file to SEALED, stopping deletion with chunks
  missing). (`MetadataServiceTest`.)
- **P2 read/fetch clamped server-side** at 8 MB per request (allocation was data-bounded, not
  attacker-controlled, but a >64 MB response would die at the frame layer anyway; callers loop).
- **P2 varint length validation before allocation** — a small frame advertising a huge/negative
  string/bytes length is a typed rejection, not heap pressure or NegativeArraySizeException.
- **P2 scrub** — nodes recompute sealed data CRCs (every 10th inventory cycle, and
  `ChunkStore.scrubOnce()` on demand); rot updates the reported CRC so the coordinator's
  existing corrupt-replica path drops and re-repairs the copy. Same-length silent corruption is
  now detected end-to-end without read-path cost. (`ChunkStoreTest`.)
- **P3 test-infra**: MiniCluster constructor cleans up on failure; all teardowns null-guarded
  (a setup flake previously leaked the half-built cluster and NPE'd teardown).

## Correctness invariants under test (tech design §14)

The release-gate map lives in `CORRECTNESS.md`. The short version:

1. Acked data survives storage failure, process crash, metadata failover, full cold restart, ZK
   restart, repair, and stress/fault schedules.
2. Fenced writers cannot ack again, and ambiguous storage reconnects roll instead of replaying
   appends on a replacement connection.
3. Readers never see bytes above the durable or sealed prefix.
4. Commit-before-write keeps orphan cleanup safe.
5. Seal recovery commits only a quorum-recoverable, CRC-verified prefix.
6. Sealed replicas are byte-identical, including raw chunk header, data, footer, and trailer.
7. Namespace/path lookup remains stable across metadata restart, and delete/recreate binds the path
   only to the replacement file id.

## How to verify

`scripts/verify.sh` — unit + integration (no Docker needed; embedded ZK).
`scripts/verify.sh --fault` — embedded stress/fault, process-crash, repair, and recovery gate.
`scripts/verify.sh --chaos` — Docker/Toxiproxy chaos gate.
`scripts/verify.sh --soak` — bounded multi-seed stress/fault soak; tune with `-Dstrata.soak.iterations`, `-Dstrata.soak.batches`, and `-Dstrata.soak.seed`.
`scripts/verify.sh --tlc` — TLA+ model checking.

`.github/workflows/ci.yml` runs the default, current compatibility/conformance, embedded fault,
and TLA+ model-checking gates on pull requests and pushes to `main`.
`.github/workflows/correctness.yml` runs the same gates plus Docker chaos and a larger bounded soak
on scheduled/manual deep-confidence runs.
Manual workflow runs can disable individual gates and can replay a soak with explicit
`soak_iterations`, `soak_batches`, and `soak_seed` inputs.
Exact stress/fault case replay is supported with
`-Dstrata.stress.case=<case> -Dstrata.stress.seed=<artifact-seed> -Dstrata.stress.batches=<artifact-batches>`.
Full stress/fault matrix replay, used by soak iteration artifacts, is supported with
`--stress-only -Dstrata.stress.seed=<artifact-seed> -Dstrata.stress.batches=<artifact-batches>`.
Targeted fault and chaos artifact replay is supported with `-Dtest=<TestClass#method>`;
`verify.sh` and `coverage.sh` both audit the artifacts produced by that one test without requiring
unrelated matrix artifacts.
CI uploads surefire reports and storage child-process logs from `strata-it/target/process-crash-logs`
so process-crash failures include the node-side evidence needed for replay and diagnosis.
The embedded fault gate also covers full service cold restart: clients, metadata, and storage
services are stopped, data nodes restart from the same data directories against the same ZK
state, and tests verify sealed-file readability plus recovery of an abandoned open chunk.
It also restarts the embedded ZooKeeper server itself before bringing metadata/storage back,
proving the v0 ZK metadata records survive a metadata-store process restart.
