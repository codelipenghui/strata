# Strata — per-namespace observability & a namespace dashboard

Design doc · 2026-06-29 · **Implemented** (source-aligned 2026-07-24)

This document preserves the design rationale. Section 2 is the pre-change baseline. Source-level registry
tests verify selected metric registration and counter behavior; there is no end-to-end Prometheus scrape
test. `DashboardMetricsGuardTest` currently guards selected removed names and leader-view queries, not every
emitted metric/dashboard reference.

## 1. Goal

Make namespace the primary axis of Strata's observability. The implemented surface answers, per namespace:

- **Throughput** — client write/read bytes & ops.
- **Latency** — client data-op latency and controller request latency.
- **Controller request rate & latency** — by opcode.
- **Namespace-log activity** — write-log, read-log (replay), compaction, recovery, reacquisition.
- **Ownership view** — which controller currently exposes a loaded repository under the exact persisted
  assignment term, plus cold-open/restart counters. Automatic handoff is implemented; the counters remain an
  approximation of acquisitions rather than a precise end-to-end failover-duration measurement.

…plus a dedicated Grafana dashboard that keys every panel on a `$namespace` selector, and the
minimal migration of the existing dashboards so nothing regresses.

This reverses the historical "namespace stays control-plane" stance for *metrics* only — namespace
is already in the data plane (`ChunkStore` receives `m.namespace()` on every op; storage is
`chunks/<ns>/…`), so per-namespace data-plane counters are now natural, not a layering violation.

## 2. Pre-change baseline (verified before implementation)

Instrumentation lives in `strata-server/ServerMetrics.java` as periodic gauges over in-memory state
plus monotonic function-counters — no Micrometer in the data path (`strata-format`/`-proto`/`-common`
carry plain `LongAdder`s; the metrics layer wires Micrometer over their accessors). The `role`
(`controller|data-node|combined`) common tag is set in `StrataMetrics`; Prometheus adds `instance`.

| Concern | Today | Gap |
|---|---|---|
| Request latency | `strata_scp_request_duration_seconds{role,opcode,status}` — one histogram, **shared by both planes**, set via `ScpServer`'s `RequestObserver` (`opcode,duration,success`) | no `namespace` |
| Data throughput | `strata_data_node_{append,read}_{ops,bytes}_total` — **global** per node | no `namespace` |
| Namespace-log | `strata_controller_log_{append_records,append_bytes,compactions,recoveries,reacquisitions}_total` — **global**, `backend` tag only; no read-log | not per-namespace; no read/replay counter |
| Per-namespace | `strata_controller_namespace_files{namespace}`, `…_namespace_log_bytes{namespace}` (MultiGauge, 10s refresh, owned namespaces) | only file count + open bytes |
| Ownership | `NamespaceOwnership.ownerOf(ns)` computed (rendezvous), identical fleet-wide; `strata_controller_namespaces_loaded` = count | no namespace→owner mapping, no switch signal |

Existing dashboards (`deploy/grafana/dashboards/`): `strata-cluster.json` (uid `strata-overview`),
`strata-controller.json`, `strata-node.json`, `strata-zookeeper.json`. Conventions: datasource
`{"type":"prometheus","uid":"prometheus"}`, schemaVersion 39, `tags:["strata"]`, "Strata dashboards"
dropdown link, refresh `30s`, time `now-1h`; `label_values(...)` template vars matched with
`label=~"$var"`; stat banner `w4 h4`×6/row, timeseries `w12 h8` in pairs; legend table with
`["last","max","mean"]`, tooltip `multi`/`desc`, `fillOpacity` 20+stacking for additive breakdowns,
10 no-stacking for rates/latency. Provisioning is a single file provider — dropping a JSON into the
dashboards dir is auto-discovered; no `dashboards.yml` edit.

## 3. Design

### 3.1 Naming & exposition

Micrometer appends `_total` to counters and `_seconds_{bucket,count,sum}` to timers at exposition, so
**code** names omit those suffixes. New per-namespace counters join the existing
`strata_controller_namespace_*` family. The implementation made a clean metric-name break: the global
`strata_controller_log_*` counters were renamed/replaced rather than retained as aliases.

### 3.2 Request latency & rate by namespace — *unify, don't split*

Add a `namespace` tag to the **existing** `strata_scp_request_duration` timer rather than introducing
plane-specific metrics. Rationale (from the dashboard audit):

- The metric is plane-neutral; `role` (and the disjoint opcode sets) already separate controller vs
  data-node. The data node serves ~12 opcodes (APPEND, READ, OPEN_CHUNK, SEAL_CHUNK, FENCE,
  STAT_CHUNK, DELETE_CHUNKS, FETCH_CHUNK, READ_LEDGER, READ_RECOVERY, VERIFY_CHUNKS, EXEC_REPLICATE) —
  a split limited to APPEND/READ would drop the rest from the node dashboard.
- Every existing latency panel groups `by (opcode)`, which **sums the new `namespace` label away** →
  those panels keep working untouched. The new dashboard filters `{namespace=~"$namespace"}`.

Resulting series: `strata_scp_request_duration_seconds{role,namespace,opcode,status,le}`.
`namespace="-"` for ops with no namespace (PING, REGISTER_NODE, NODE_HEARTBEAT). SLO buckets unchanged.

**Plumbing the namespace into the transport.** `ScpServer` runs each request on a single-threaded
per-connection `requestExecutor`; the *synchronous* portion of `handleAsync` decodes the namespace
before any async (APPEND group-commit) wait, in order, on that thread. So:

- New `RequestContext` in `strata-proto` (no Micrometer dep): a `ThreadLocal<String>` with
  `setNamespace(String)` and `takeNamespace()` (returns `"-"` when unset, clears on read).
- Handlers set it right after decoding the request's namespace:
  - **Controller** — in `requireNamespaceOwner(ns)` (the single choke point every namespace-scoped
    opcode already calls): `RequestContext.setNamespace(ns.value())`.
  - **Data node** — in `DataNodeHandlers`, after each `var m = Messages.X.decode(h)`:
    `RequestContext.setNamespace(m.namespace().value())` (covers sync ops and the async APPEND, since
    decode is synchronous).
- `ScpServer.handleRequest`: after `respF = handler.handleAsync(req)` returns (sync portion done),
  capture `String ns = RequestContext.takeNamespace();` once, and pass it to `observeRequest(...)` in
  **both** the fast (sync) path and the async `whenComplete` closure. Defensive `take()` clears any
  stale value.
- `RequestObserver.observe(String opcode, String namespace, long durationNanos, boolean success)` —
  add the `namespace` param. `ServerMetrics.requestObserver` caches timers per `opcode:status:namespace`
  and adds `.tag("namespace", namespace)`.

> Note: in `docker-compose` `role=combined`, so split planes **by opcode**, not by role.

### 3.3 Data throughput by namespace (data plane)

`ChunkStore` (`strata-format`) uses `ConcurrentHashMap<String, LongAdder[]>`, with each four-element
array holding `{appendOps, appendBytes, readOps, readBytes}`. It is incremented in
`appendAsync(namespace, …)` and `readRegion(namespace, …)`. `namespaceIoStats()` exposes a
`Map<String, long[]>`; `ioNamespaces()` and `ioValue(namespace,index)` support allocation-free lazy
Micrometer registration. The original global `AtomicLong` counters/accessors remain as internal
aggregates, but the exported throughput meters are per namespace and the current fleet panels collapse all
labels with `sum(…)`.

New series (same names + `namespace`): `strata_data_node_append_ops_total{namespace}`,
`…_append_bytes_total{namespace}`, `…_read_ops_total{namespace}`, `…_read_bytes_total{namespace}`.

### 3.4 Namespace-log metrics by namespace

`NamespaceLogMetrics` is keyed by `ConcurrentHashMap<String, LongAdder[]>`, using a documented fixed
counter-index order. It remains held on `NamespaceLogBackend`, so counters survive an in-process
repository rebuild or reacquisition; a JVM/process restart creates a fresh metrics object. Every
`record*` accepts the namespace already known by its caller:

| method | call site (has namespace) |
|---|---|
| `recordAppend(ns, bytes)` | durable append in `NamespaceMetadataLogRepository` |
| `recordCompaction(ns)` | successful repository compaction |
| `recordRecovery(ns)` | repository open/replay |
| `recordLogRead(ns, records, bytes)` | recovery replay; counts segment records/bytes replayed |
| `recordReacquire(ns)` | fence-driven re-acquire only |
| `recordOwnerAcquired(ns)` | cold repository creation, not the explicit `reacquire()` path (see §3.5) |

New series (label `namespace`, exposed `_total`): `strata_controller_namespace_log_append_records`,
`…_append_bytes`, `…_read_records`, `…_read_bytes`, `…_compactions`, `…_recoveries`,
`…_reacquisitions`, and `…_snapshot_fallbacks`. These **replace** the global `strata_controller_log_*`. `strata_controller_log_reacquisitions`
had no panel anywhere — it gets a home on the new dashboard as an ownership-churn signal.

### 3.5 Namespace owner & switch timing

Two complementary signals have different lifecycles:

1. **Info gauge** `strata_controller_namespace_owner{namespace,owner}` = `1`, `owner =
   ownership.localEndpoint()`. Emitted via a MultiGauge in the existing 10s `registerPerNamespace`
   refresh (re-registered each tick, like the files/bytes gauges). It identifies a loaded repository whose
   opening term still exactly matches this process's current persisted assignment; automatic handoff makes
   its state timeline an owner-switch signal.
2. **Cold-open counter** `strata_controller_namespace_owner_changes_total{namespace}` — registered lazily
   and retained for the process lifetime, so it remains exposed with a frozen value if ownership later leaves
   this process. It is incremented
   when this process first opens a repository for a namespace it did not already hold. Process restart
   and initial load increment it too, so despite the historical metric name it is only an approximation
   for ownership handoffs. The explicit fence-driven `reacquire()` path is not counted here; that is
   tracked by `…_reacquisitions_total`.

`$namespace` for the dashboard sources from
`label_values(strata_scp_requests_total{namespace!="-"}, namespace)`. This works with both metadata backends
after a namespace-scoped request has been observed. It is not an authoritative namespace inventory: after a
fresh process restart the selector is empty until traffic arrives. The namespace-log-only
`strata_controller_namespace_files` gauge cannot serve as a backend-independent selector because the current
ZooKeeper backend returns no `namespaceStats()` rows.

### 3.6 Cardinality

The user opted for full opcode detail. Bounds: request timer = `roles × namespaces × opcodes × 2 ×
(buckets+2)`; data-plane counters = `namespaces × nodes`; controller log/owner = `namespaces`. All
bounded by the (small) opcode enum and the namespace count. `$namespace` is the primary filter on
every new-dashboard panel. The lazily-registered per-namespace counters are never deregistered when
ownership moves away (a counter must not vanish); the value freezes and resumes if the namespace
returns — correct counter semantics, cardinality bounded by namespaces ever owned on that instance.

## 4. Implementation surface

**`strata-proto`**
- `RequestObserver.observe(...)` — add `String namespace`.
- New `RequestContext` (ThreadLocal namespace holder; `set`/`take`).
- `ScpServer.handleRequest`/`observeRequest` — capture `RequestContext.takeNamespace()` after dispatch,
  thread through both sync and async observe calls.

**`strata-format`**
- `ChunkStore` — per-namespace `String -> LongAdder[4]` map; increment in `appendAsync`/`readRegion`;
  expose snapshot, namespace-set, and indexed-value accessors. Global aggregate accessors remain internal.

**`strata-node`**
- `DataNode` — expose `ioNamespaces()` and indexed `ioValue()` for lazy meter registration.
- `DataNodeHandlers` — `RequestContext.setNamespace(m.namespace().value())` after each decode.

**`strata-meta`**
- `NamespaceLogMetrics` — `String -> LongAdder[9]`; `record*(ns, …)`, snapshot, namespace-set, and
  indexed-value accessors.
- `NamespaceMetadataLogRepository` / `NamespaceLogBackend` / `NamespaceMetadataRecovery` — pass `ns` to
  `record*`; add owner-acquired increment in `repo()`; add replay-read counting.
- `Controller` — `requireNamespaceOwner` sets `RequestContext`; new `namespaceLogStats()` and
  `localControllerEndpoint()`; remove global `metadataLog*()` accessors.

**`strata-server`**
- `ServerMetrics` — remove global log + global data-node-io counters; register per-namespace
  function-counters lazily (track a registered-set in the 10s refresh) for the §3.3/§3.4 series + owner
  changes; add the owner MultiGauge; update `requestObserver` to tag `namespace` (cache key
  `opcode:status:namespace`).

**`deploy/grafana/dashboards`**
- New `strata-namespace.json` (§5).
- Migrate panels in `strata-cluster.json`, `strata-controller.json`, `strata-node.json` (§5.2).

## 5. Dashboards

### 5.1 New `strata-namespace.json` (uid `strata-namespace`, tag `strata`)

Skeleton cloned from `strata-controller.json` (closest template). Template var `namespace`
(`label_values(strata_scp_requests_total{namespace!="-"}, namespace)`, multi+includeAll, matched
`namespace=~"$namespace"`). Rows:

1. **Ownership** — namespace→owner table (`strata_controller_namespace_owner`), owner-over-time
   state-timeline, owner-change rate (`rate(strata_controller_namespace_owner_changes_total[…])`),
   namespaces-loaded per controller.
2. **Throughput** — write/read bytes (`Bps`) & ops (`ops`) per namespace from
   `strata_data_node_{append,read}_{bytes,ops}_total{namespace=~"$namespace"}`, `sum by (namespace)`,
   stacked.
3. **Latency** — client data latency
   (`histogram_quantile(0.99, sum by (le,namespace)(rate(strata_scp_request_duration_seconds_bucket{namespace=~"$namespace",opcode=~"APPEND|READ"}[…])))`)
   and controller request latency (opcode set CREATE_FILE|LOOKUP_*|SEAL_*|CREATE_CHUNK|…) p50/p95/p99.
4. **Controller requests** — rate by opcode and error rate
   (`status="error"`) for `$namespace`, from `strata_scp_requests_total`.
5. **Namespace log** — write-log records/bytes, read-log (replay) records/bytes, compactions,
   recoveries, reacquisitions per namespace; plus existing open-log bytes + live files filtered by
   `$namespace` (drill-down, not a copy of the fleet panels).

Conventions per the audit: `w4 h4` stat banner, `w12 h8` timeseries pairs, legend
`["last","max","mean"]`, tooltip multi/desc, units `Bps`/`ops`/`bytes`/`s`/`short`.

### 5.2 Existing-dashboard migration (source-aligned)

`strata-cluster.json`
- *Metadata ops/s* → `sum(rate(strata_controller_namespace_log_append_records_total[$__rate_interval]))`
- *Cluster write vs read throughput* (both series) → `sum(rate(strata_data_node_{append,read}_bytes_total[$__rate_interval]))`
- *Request rate by opcode*, *p99 latency by opcode* — **unchanged** (group by opcode).

`strata-controller.json`
- *Metadata mutation rate* → `sum(rate(strata_controller_namespace_log_append_records_total{instance=~"$instance"}[…]))`
- *Metadata-log write throughput* → `…namespace_log_append_bytes_total…`
- *Compaction & recovery rate* (2 series) → `…namespace_log_compactions_total…`, `…namespace_log_recoveries_total…`, each aggregated with `sum(...)`.

`strata-node.json`
- *Write throughput*, *Write vs read throughput* (2), *Write & read ops/s* (2) → `sum(rate(strata_data_node_{append,read}_{bytes,ops}_total{instance=~"$node"}[…]))`
- *p99 latency by opcode*, *Error rate by opcode* — **unchanged** (group by opcode).

`strata-zookeeper.json` — none (ZK-native metrics only).

## 6. Testing

Current coverage includes `NamespaceLogMetrics` per-namespace accumulation/reacquisition behavior,
`ChunkStore` namespace I/O accounting, `RequestContext` set/take behavior, and `ServerMetrics` request
and lazy data-node counter registration. `DashboardMetricsGuardTest` parses the provisioned dashboards,
rejects five removed controller-log names, and guards two leader-view queries.

Remaining coverage gaps are a multi-controller/multi-namespace scrape test that exercises owner movement,
plus a generated comparison of every emitted metric against every dashboard query. Until those exist,
the owner-gauge flip and exhaustive dashboard consistency are design expectations rather than verified
integration guarantees.

## 7. Out of scope / future (same idea, later)

- Per-namespace durability/repair (`strata_chunks_*`, `strata_repair_*` by namespace) — useful but the
  repair census is fleet-wide today; deferred.
- Recording rules / alerts (per-namespace SLOs, owner-flap alerts).
- Opcode-class grouping if full-opcode cardinality proves heavy in practice.
