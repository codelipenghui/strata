# Expose hardcoded operational tuning knobs as configuration

Date: 2026-06-29
Status: Implemented

## Problem

An audit of `src/main` across all modules found ~28 hardcoded constants that are genuine
**operational tuning knobs** — values an operator/SRE would plausibly change per deployment or
workload — but that are not reachable through the documented config surface (`DataNodeConfig` /
`ControllerConfig` / `ClientConfig` records + `STRATA_*` env wired in `StrataServer`). Some are
plain literals; some are reachable only via `-D` JVM system properties, which sit outside the
`STRATA_*` env convention and are therefore inconsistent.

This work exposes those knobs as configuration, following the project's existing conventions and
its **no-backward-compat / clean-break** rule (replace literals and `-D` lookups in place; do not
leave dual paths or fallback chains).

Out of scope (audit-rejected as NOT config — kept hardcoded): wire/on-disk format invariants
(`MAX_REPAIR_FOOTER_BYTES`, CRC/header/offset constants), algorithmic constants (`CAS_RETRIES=5`,
`NodeRegistry` putNode CAS bound `3`, owner-direct `Math.max(30_000, …)` floor), pure diagnostics
(append-backpressure WARN cadence, shutdown `join(2_000)`), and the `StrataPerf` benchmark harness
internals.

## Conventions followed

- Add a field to the relevant record with a **production-sane default** and validation; wire a
  `STRATA_<UPPER_SNAKE>` env override in `StrataServer` via `intEnv/longEnv/boolEnv/env`.
- Env naming follows the **nearest existing sibling**: repair/verify cadences stay bare
  (`STRATA_VERIFY_*`, like `STRATA_REPAIR_*`); controller log/ZK/lifecycle knobs use
  `STRATA_CONTROLLER_*`; metrics use `STRATA_METRICS_*`; transport/codec use `STRATA_SCP_*`.
  Client knobs (`ClientConfig`) have no env entrypoint — they are library config fields set by the
  caller, not by `StrataServer`. Env defaults reproduce today's behavior exactly.
- Clean break: delete the old `Integer.getInteger`/`Long.getLong` `-D` lookups and bare literals;
  no env→`-D`→default fallback chain.

## Architecture — four tiers by how config reaches the site

### Tier A — domain config records (17 knobs)

Construction already has the record in scope; add a field, default, validation, and `StrataServer`
env wiring. No threading changes.

**`ControllerConfig` (8)**

| Site | Literal | Field | Env | Default | Notes |
|---|---|---|---|---|---|
| `RepairCoordinator.java:216` | `2_000` | `verifyIntervalMs` | `STRATA_VERIFY_INTERVAL_MS` | 2000 | sole proactive missing/corrupt-replica detection cadence; `>0` |
| `RepairCoordinator.java:217` | `256` | `verifyBatchSize` | `STRATA_VERIFY_BATCH_SIZE` | 256 | chunk-ids per `VERIFY_CHUNKS` RPC; `>0` |
| `RepairCoordinator.java:223` | `30_000` | `systemVerifyIntervalMs` | `STRATA_SYSTEM_VERIFY_INTERVAL_MS` | 30000 | slower cadence for the system (metadata-log) namespace; `>0`. Replace the existing `systemVerifyIntervalMsForTest` setter with config |
| `RepairCoordinator.java:121` | `600_000` | `deletedTombstoneTtlMs` | `STRATA_CONTROLLER_DELETED_TOMBSTONE_TTL_MS` | 600000 | fences delayed CREATE replay. Validate `> repairScanIntervalMs`; document cross-process coupling to the client retry deadline (max(15s, callTimeoutMs)) + clock skew |
| `NodeRegistry.java:34` | `16` | `maxCommandsPerHeartbeat` | `STRATA_CONTROLLER_MAX_COMMANDS_PER_HEARTBEAT` | 16 | command-plane throughput ceiling per node per heartbeat; `>0` |
| `ZkMetadataStore.java:74` | `100` | `zkRetryBaseMs` | `STRATA_CONTROLLER_ZK_RETRY_BASE_MS` | 100 | Curator `ExponentialBackoffRetry` base sleep; `>0` |
| `ZkMetadataStore.java:74` | `5` | `zkRetryMaxRetries` | `STRATA_CONTROLLER_ZK_RETRY_MAX` | 5 | Curator max retries; `>=0` |
| `StrataSystemMetadataFileStore.java:32` | `4*1024*1024` | `metadataReadChunkBytes` | `STRATA_CONTROLLER_LOG_READ_CHUNK_BYTES` | 4 MiB | metadata-log/snapshot recovery read buffer; `>0` |

**`DataNodeConfig` (6)**

| Site | Literal | Field | Env | Default | Notes |
|---|---|---|---|---|---|
| `OrphanGc.java:50` | `6_000` | `orphanGraceMs` | `STRATA_ORPHAN_GRACE_MS` | 6000 | replaces `DEFAULT_GRACE_MS`; `>0` |
| `OrphanGc.java:51` | `3_000` | `orphanScanIntervalMs` | `STRATA_ORPHAN_SCAN_INTERVAL_MS` | 3000 | replaces `DEFAULT_SCAN_INTERVAL_MS`; `>0` |
| `OrphanGc.java:52` | `6_000` | `orphanStartupGraceMs` | `STRATA_ORPHAN_STARTUP_GRACE_MS` | 6000 | replaces `DEFAULT_STARTUP_GRACE_MS`; `>0` |
| `OrphanGc.java:53` | `5_000` | `orphanConfirmTimeoutMs` | `STRATA_ORPHAN_CONFIRM_TIMEOUT_MS` | 5000 | replaces `CONFIRM_TIMEOUT_MS`; `>0` |
| `ControlLoop.java:40` | `10_000` | `controlCallTimeoutMs` | `STRATA_CONTROL_CALL_TIMEOUT_MS` | 10000 | node→controller RPC timeout (REGISTER/HEARTBEAT/FETCH_CHUNK); `>0` |
| `ControlLoop.java:41` | `4*1024*1024` | `repairFetchBytes` | `STRATA_REPAIR_FETCH_BYTES` | 4 MiB | repair pull granularity; `>0`. Keep the `MAX_REPAIR_FOOTER_BYTES` corruption guard hardcoded |

The `OrphanGc` default ctor already reads `DEFAULT_*` statics; `DataNode` will pass the configured
values through. `ControlLoop` already holds `DataNodeConfig` (`ControlLoop.java:45`).

**`ClientConfig` (3)**

The client has no `StrataServer` env entrypoint — these knobs are library config fields set by the
caller (e.g. the broker) via `ClientConfig` fields / `with*` setters. There are no `STRATA_CLIENT_*`
env variables; do not add them to docker-compose or other server-side config.

| Site | Literal | Field | Default | Notes |
|---|---|---|---|---|
| `ControllerClient.java:65` | `15_000` | `controllerRetryDeadlineMs` | 15000 | retry-deadline floor across owner redirects/failover; the floor still wins over a smaller `callTimeoutMs`; `>0` |
| `ControllerClient.java:102` | `200` | `controllerRetryBackoffMs` | 200 | flat backoff between metadata retries; `>0` |
| `Recovery.java:38` | `4*1024*1024` | `recoveryCopyChunkBytes` | 4 MiB | seal-recovery copy batch; `>0` |

`ControllerClient` / `Recovery` receive these from the `ClientConfig` already held by the client.

### Tier B — new `ChunkStoreConfig` record in `strata-format` (4 knobs)

`strata-format` does not depend on `strata-node`, and `ChunkStore` is constructed with only a
`Path` (`DataNode.java:66`). Introduce a small immutable `ChunkStoreConfig` record in
`strata-format` with sane defaults and validation. `ChunkStore` gains a
`ChunkStore(Path, ChunkStoreConfig)` constructor (keep a `ChunkStore(Path)` that uses
`ChunkStoreConfig.DEFAULT` for existing format-level tests). `DataNode` builds a `ChunkStoreConfig`
from `DataNodeConfig` fields (env-wired in `StrataServer`) and passes it in. `ChunkStore` forwards
the group-commit fields when it constructs `GroupCommitter` (`ChunkStore.java:553`).

| Site | Literal | `ChunkStoreConfig` field | Env (wired in StrataServer→DataNodeConfig) | Default | Notes |
|---|---|---|---|---|---|
| `ChunkStore.java:56` | `8*1024*1024` | `maxRequestBytes` | `STRATA_MAX_REQUEST_BYTES` | 8 MiB | per-request read/fetch cap; `>0` |
| `GroupCommitter.java:215` | `10_000` | `groupCommitDrainTimeoutMs` | `STRATA_GROUPCOMMIT_DRAIN_TIMEOUT_MS` | 10000 | fsync-force drain-before-interrupt window; `>0` |
| `GroupCommitter.java:94` (`-D`) | `1_000_000` | `groupCommitMinAccumulationNanos` | `STRATA_GROUPCOMMIT_MIN_ACCUMULATION_NANOS` | 1_000_000 | replaces `-Dstrata.groupcommit.minAccumulationNanos`; `>0` |
| `GroupCommitter.java:96` (`-D`) | `50_000_000` | `groupCommitMaxAccumulationNanos` | `STRATA_GROUPCOMMIT_MAX_ACCUMULATION_NANOS` | 50_000_000 | replaces `-Dstrata.groupcommit.maxAccumulationNanos`; validate `>= min` |

Delete the two `Long.getLong(...)` `-D` lookups in `GroupCommitter` (clean break).

### Tier C — `strata-proto` transport/codec env reads (6 knobs)

Transport/codec concerns are process-global and shared by both server roles, so they are NOT put in
the domain records. Replace each existing `-D`/static literal with a localized `STRATA_SCP_*`/
`STRATA_*` env read.

| Site | Literal | Env | Default | Mechanism |
|---|---|---|---|---|
| `ScpServer.java:43` (`-D`) | `1024` | `STRATA_SCP_MAX_INFLIGHT_REQUESTS` | 1024 | resolve in the public `ScpServer` ctor; keep the package-private injection ctor for tests. Delete the `Integer.getInteger` `-D` default |
| `ScpServer.java:45` (`-D`) | `1L<<30` | `STRATA_SCP_MAX_INFLIGHT_BYTES` | 1 GiB | same as above; delete the `Long.getLong` `-D` default |
| `ScpClient.java:39` (`-D`) | `1024` | `STRATA_SCP_MAX_PENDING_REQUESTS` | 1024 | resolve at `ScpClient` init; `AppenderImpl` pipeline depth keeps deriving from `maxPendingRequests()`. Delete the `Integer.getInteger` `-D` default |
| `NettyEventLoops.java:12` | `0` | `STRATA_SCP_SERVER_IO_THREADS` | 0 (=2×cores) | static-init env read for `SERVER_WORKER_GROUP` (and the matching client `CLIENT_GROUP`). `BOSS=1` stays fixed |
| `FrameIO.java:13` | `64*1024*1024` | `STRATA_MAX_FRAME_BYTES` | 64 MiB | static-init env read; the value flows to the `HelloResp` advertisement and the encode/decode guards unchanged |
| `ScpClient.java:53` / `ConnectionPolicy.java:13` | `5_000` | `STRATA_SCP_CONNECT_TIMEOUT_MS` | 5000 | route the direct `new ScpClient(host,port,kind,id)` callers (`RepairCoordinator.java:762,906`, `OrphanGc.java:142`, `ControlLoop.java:236`) through `ConnectionPolicy.connectTimeoutMs`; wire that one env in `StrataServer`. Removes both duplicate `5_000` literals |

Tier C env reads are localized in `strata-proto` (no proto→node/controller config dependency
introduced). Static-init reads (`NettyEventLoops`, `FrameIO`) are a one-time literal→env swap with
no fallback chain.

### Tier D — `strata-server` direct env (2 knobs)

`ServerMetrics` is registered from `StrataServer`, so env is directly in scope.

| Site | Literal | Env | Default | Notes |
|---|---|---|---|---|
| `ServerMetrics.java:145` (and `:206`) | `10` s | `STRATA_METRICS_NS_REFRESH_INTERVAL_MS` | 10000 | per-namespace metrics refresh period; `>0` |
| `ServerMetrics.java:268` | `{1,2,5,10,25,50,100,250,500,1000,2500,5000}` ms | `STRATA_METRICS_REQUEST_DURATION_BUCKETS_MS` | that list | CSV of ascending positive ms; parse → sorted, distinct, non-empty |

## Error handling & validation

- Every numeric knob is validated in its record/config constructor with the existing
  `IllegalArgumentException` style (`>0`, or `>=0` for retry counts). `groupCommitMaxAccumulationNanos
  >= groupCommitMinAccumulationNanos`. `deletedTombstoneTtlMs > repairScanIntervalMs`.
- Env parsing reuses `StrataServer`'s `intEnv/longEnv/env` helpers; the buckets env uses a new small
  CSV parser that rejects empty/non-ascending/non-positive input with a clear message.
- Defaults exactly reproduce current behavior, so an unset environment is a no-op change.

## Testing

- **Config records**: extend `ControllerConfigDefaultsTest`, `DataNodeConfigTest`, `ClientConfigTest`
  with the new defaults + validation (reject non-positive, ordering, tombstone-vs-scan coupling).
- **Env wiring**: extend `StrataServerStartupTest` to assert each `STRATA_*` override lands in the
  built config (set env/property, build config, assert field).
- **Behavioral**: targeted tests where behavior is observable — e.g. `maxCommandsPerHeartbeat` caps
  the drained command count; `verifyBatchSize` slices RPCs; `ChunkStore` honors `maxRequestBytes`;
  `GroupCommitter` honors drain timeout + accumulation bounds via `ChunkStoreConfig`.
- **`ChunkStoreConfig`**: new `strata-format` test for defaults/validation and that `ChunkStore(Path)`
  == `ChunkStore(Path, ChunkStoreConfig.DEFAULT)`.
- **Tier C**: use the existing `ScpServer` package-private injection ctor for inflight knobs; the
  static-init reads (`FrameIO`, `NettyEventLoops`) are plumbing-only — assert the default constants
  are unchanged when unset and document the static-init behavior.
- Full module test suites (`strata-meta`, `strata-node`, `strata-format`, `strata-client`,
  `strata-server`) green; no regression in `strata-it`.

## Rollout / compatibility

No production deployments and no compat guarantees: literals and `-D` lookups are replaced in place,
old names deleted. Docker-compose / deploy docs gain the new `STRATA_*` vars where an operator would
realistically set them (the medium-severity ones); the low-severity knobs are documented but left at
default.
