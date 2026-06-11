# Strata v0 — Implementation Plan & Status

Working state for the v0 build (tech design §4.4 bootstrap path: ZooKeeper-backed metadata, no Kafka fork).
This file is the durable source of truth for the development loop — update the checkboxes and decision log as work lands.

## Locked decisions

- **Stack:** Java 21 (Corretto), Maven multi-module (Maven 3.8.8 installed), JUnit 5, SLF4J-simple.
- **Networking:** blocking IO on virtual threads (thread-per-connection); correctness first, NIO later if ever needed.
- **ZK access:** Apache Curator (framework + recipes + curator-test for embedded ZK in tests).
- **Chaos:** testcontainers + Toxiproxy (gated on Docker availability).
- **Model checking:** TLA+ (tla2tools.jar fetched on demand), spec under `tla/`.
- **v0 protocol additions** (additive per tech design §10.6; fold back into the design doc at finalize):
  - `READ_LEDGER` (0x0019): returns integrity-ledger entries from an offset — seal recovery needs per-append boundaries without parsing payload.
  - Client↔metadata over SCP, opcode range 0x02xx: `CREATE_FILE` 0x0201, `CREATE_CHUNK` 0x0202, `SEAL_CHUNK_META` 0x0203, `LOOKUP_FILE` 0x0204, `DELETE_FILES` 0x0205. (v1 moves broker-facing APIs to Kafka RPC; range stays for tools.)
  - `NODE_HEARTBEAT` request gains tagged field `completedCommands` (commandId → status) so the repair coordinator learns copy completion.
  - `SEAL_CHUNK` (0x0015): footer sections from caller are optional; the node always computes CRC_RANGES + STATS itself so recovery-sealed chunks are byte-identical across replicas.
  - Error codes added: 15 NOT_LEADER (tagged leader hint), 16 NO_CAPACITY (placement cannot find 3 nodes).
- **v0 epoch source:** the caller (test harness) supplies `writeEpoch` — monotonic per file. v1 replaces this with Kafka controller leader epochs; storage layer is agnostic.
- **v0 metadata service:** single active instance in tests (Curator leader election implemented); leases in leader memory, registrations + file/chunk records in ZK (CAS via znode versions, leader-only writes).

## Module layout

```
strata-common   ids (FileId/ChunkId/NodeId), Varint, Crc32C, ErrorCode, exceptions
strata-proto    SCP frame codec, tagged fields, opcodes, message structs, ScpClient/ScpServer (virtual threads)
strata-format   chunk file header/footer/trailer, sidecar .meta, integrity ledger .j, ChunkStore engine + crash recovery
strata-node     storage node: SCP handlers → ChunkStore; register/heartbeat/inventory loop; REPLICATE executor (pull)
strata-meta     MetadataStore SPI, ZkMetadataStore, MetadataService (SCP listener, placement, leases, repair, retention)
strata-client   SegmentStore/Appender/Reader per design §12 (quorum ack, DO, roll, create-ahead, recoverAndSeal)
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
- [x] 6. Metadata service on ZK (embedded-ZK tests)
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

## Correctness invariants under test (tech design §14)

1. Acked data (quorum 2-of-3) survives any single node failure through seal — verified by the recorder/verifier harness in every integration & chaos test.
2. Single writer per epoch: a fenced appender can never ack again.
3. Readers never see bytes above the durable offset.
4. Commit-before-write: chunk exists in metadata before any byte lands on a node.
5. Sealed replicas are byte-identical (whole-file CRC equality across replicas).
6. Crash recovery converges to a CRC-verified prefix without parsing payload.

## How to verify

`scripts/verify.sh` — unit + integration (no Docker needed; embedded ZK). Chaos: `mvn -pl strata-it test -Dchaos=true` (requires Docker). TLA+: `scripts/tlc.sh`.
