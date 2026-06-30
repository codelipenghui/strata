# Strata — per-namespace observability & a namespace dashboard

Design doc · 2026-06-29 · branch `penghui/great-maxwell-0bc82d`

## 1. Goal

Make namespace the primary axis of Strata's observability. Today every throughput/latency
metric is either fleet-global or per-process; the namespace is only visible in two gauges
(`strata_controller_namespace_files`, `strata_controller_namespace_log_bytes`). We want to answer,
per namespace:

- **Throughput** — client write/read bytes & ops.
- **Latency** — client data-op latency and controller request latency.
- **Controller request rate & latency** — by opcode.
- **Namespace-log activity** — write-log, read-log (replay), compaction, recovery, reacquisition.
- **Ownership** — which controller currently owns the namespace, and *when ownership switched*.

…plus a dedicated Grafana dashboard that keys every panel on a `$namespace` selector, and the
minimal migration of the existing dashboards so nothing regresses.

This reverses the historical "namespace stays control-plane" stance for *metrics* only — namespace
is already in the data plane (`ChunkStore` receives `m.namespace()` on every op; storage is
`chunks/<ns>/…`), so per-namespace data-plane counters are now natural, not a layering violation.

## 2. Current state (verified)

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
`strata_controller_namespace_*` family. Per project policy (never ships to prod): **clean breaks, no
back-compat aliases** — the global `strata_controller_log_*` counters are renamed/replaced, not kept.

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

`ChunkStore` (`strata-format`) gains `ConcurrentHashMap<StrataNamespace, IoCounters>` where
`IoCounters` = `{appendOps, appendBytes, readOps, readBytes}` (`LongAdder`s, lock-free). Incremented in
`appendAsync(namespace, …)` and `readRegion(namespace, …)` — both already receive the namespace.
Exposed as `Map<StrataNamespace, long[]> namespaceIoStats()` (via `DataNode`). The current global
`appendOps()/appendBytes()/readOps()/readBytes()` accessors and their global function-counters are
**removed**; the fleet view becomes `sum without(namespace)(…)`.

New series (same names + `namespace`): `strata_data_node_append_ops_total{namespace}`,
`…_append_bytes_total{namespace}`, `…_read_ops_total{namespace}`, `…_read_bytes_total{namespace}`.

### 3.4 Namespace-log metrics by namespace

`NamespaceLogMetrics` becomes per-namespace keyed: `ConcurrentHashMap<StrataNamespace, Counters>`,
still held on `NamespaceLogBackend` (so counters survive a repository being rebuilt on
failover/restart — the existing invariant). Every `record*` gains a `StrataNamespace` arg; callers
already know it:

| method | call site (has namespace) |
|---|---|
| `recordAppend(ns, bytes)` | `NamespaceMetadataLogRepository:131` |
| `recordCompaction(ns)` | `NamespaceMetadataLogRepository:201` |
| `recordRecovery(ns)` | `NamespaceMetadataLogRepository:67` (open/replay) |
| `recordLogRead(ns, records, bytes)` **(new)** | replay loop in `NamespaceMetadataLogRepository.open` / `NamespaceMetadataRecovery` — counts segment records/bytes replayed = read-log throughput |
| `recordReacquire(ns)` | `NamespaceLogBackend:352` (fence-driven re-acquire only) |
| `recordOwnerAcquired(ns)` **(new)** | inside the `repos.computeIfAbsent` lambda on the **cold-acquisition** path in `NamespaceLogBackend.repo()` — NOT the `reacquire()` path (see §3.5) |

New series (label `namespace`, exposed `_total`): `strata_controller_namespace_log_append_records`,
`…_append_bytes`, `…_read_records`, `…_read_bytes`, `…_compactions`, `…_recoveries`,
`…_reacquisitions`. These **replace** the global `strata_controller_log_*`. `strata_controller_log_reacquisitions`
had no panel anywhere — it gets a home on the new dashboard as an ownership-churn signal.

### 3.5 Namespace owner & switch timing

Two complementary signals, both emitted **only by the current owner** (a controller has a live
repository in `repos` only for namespaces it owns; `namespaceStats().keySet()` = owned set):

1. **Info gauge** `strata_controller_namespace_owner{namespace,owner}` = `1`, `owner =
   ownership.localEndpoint()`. Emitted via a MultiGauge in the existing 10s `registerPerNamespace`
   refresh (re-registered each tick, like the files/bytes gauges). Exactly one series per namespace at
   steady state → a Grafana **state-timeline** shows the owner band and visibly flips on handoff (a
   brief 0/2-owner window during handoff is itself informative).
2. **Change counter** `strata_controller_namespace_owner_changes_total{namespace}` — incremented by
   `recordOwnerAcquired(ns)` inside the `computeIfAbsent` lambda on the **cold-acquisition** path of
   `NamespaceLogBackend.repo()`: it runs exactly once when this node first opens a repository for a
   namespace it had none for (a takeover/restart). The fence-driven `reacquire()` path (same node
   re-opening under a fresh epoch) is *not* counted here — that is what `…_reacquisitions_total`
   already tracks — so this counter approximates genuine ownership handoffs, not in-place churn. The
   **info gauge (#1) is the authoritative switch signal**; this counter drives a "switches" graph and
   dashboard annotations. Sum across the fleet ≈ total handoffs.

`$namespace` for the dashboard sources from `label_values(strata_controller_namespace_files, namespace)`
— stable today, so the selector is populated even before the new counters ship.

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
- `ChunkStore` — per-namespace `IoCounters` map; increment in `appendAsync`/`readRegion`; add
  `namespaceIoStats()`; remove global io accessors.

**`strata-node`**
- `DataNode` — expose `namespaceIoStats()`; drop global io accessors.
- `DataNodeHandlers` — `RequestContext.setNamespace(m.namespace().value())` after each decode.

**`strata-meta`**
- `NamespaceLogMetrics` — per-namespace keyed; `record*(ns, …)`; new `recordLogRead`,
  `recordOwnerAcquired`; snapshot accessor + `namespaces()`.
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
(`label_values(strata_controller_namespace_files, namespace)`, multi+includeAll, matched
`namespace=~"$namespace"`). Rows:

1. **Ownership** — namespace→owner table (`strata_controller_namespace_owner`), owner-over-time
   state-timeline, owner-change rate (`rate(strata_controller_namespace_owner_changes_total[…])`),
   namespaces-loaded per controller. Owner-change counter also feeds dashboard **annotations**.
2. **Throughput** — write/read bytes (`Bps`) & ops (`ops`) per namespace from
   `strata_data_node_{append,read}_{bytes,ops}_total{namespace=~"$namespace"}`, `sum by (namespace)`,
   stacked.
3. **Latency** — client data latency
   (`histogram_quantile(0.99, sum by (le)(rate(strata_scp_request_duration_seconds_bucket{namespace=~"$namespace",opcode=~"APPEND|READ"}[…])))`)
   and controller request latency (opcode set CREATE_FILE|LOOKUP_*|SEAL_*|CREATE_CHUNK|…) p50/p95/p99.
4. **Controller requests** — rate by opcode and error rate
   (`status="error"`) for `$namespace`, from `strata_scp_request_duration_seconds_count`.
5. **Namespace log** — write-log records/bytes, read-log (replay) records/bytes, compactions,
   recoveries, reacquisitions per namespace; plus existing open-log bytes + live files filtered by
   `$namespace` (drill-down, not a copy of the fleet panels).

Conventions per the audit: `w4 h4` stat banner, `w12 h8` timeseries pairs, legend
`["last","max","mean"]`, tooltip multi/desc, units `Bps`/`ops`/`bytes`/`s`/`short`.

### 5.2 Existing-dashboard migration (exact)

`strata-cluster.json`
- *Metadata ops/s* → `sum(rate(strata_controller_namespace_log_append_records_total[$__rate_interval]))`
- *Cluster write vs read throughput* (both series) → `sum without(namespace)(rate(strata_data_node_{append,read}_bytes_total[$__rate_interval]))`
- *Request rate by opcode*, *p99 latency by opcode* — **unchanged** (group by opcode).

`strata-controller.json`
- *Metadata mutation rate* → `sum without(namespace)(rate(strata_controller_namespace_log_append_records_total{instance=~"$instance"}[…]))`
- *Metadata-log write throughput* → `…namespace_log_append_bytes_total…`
- *Compaction & recovery rate* (2 series) → `…namespace_log_compactions_total…`, `…namespace_log_recoveries_total…`, each `sum without(namespace)`.

`strata-node.json`
- *Write throughput*, *Write vs read throughput* (2), *Write & read ops/s* (2) → `sum without(namespace)(rate(strata_data_node_{append,read}_{bytes,ops}_total{instance=~"$node"}[…]))`
- *p99 latency by opcode*, *Error rate by opcode* — **unchanged** (group by opcode).

`strata-zookeeper.json` — none (ZK-native metrics only).

## 6. Testing

- **Unit** — `NamespaceLogMetrics` per-namespace accumulation & survival across repo rebuild;
  `RequestContext` set/take threading (including the async path); owner gauge emits only on the owner;
  `ServerMetrics` scrape contains the new `namespace`-tagged series and no longer contains the removed
  global names.
- **Integration** — a sharded scenario (≥2 controllers, ≥2 namespaces): assert per-namespace request,
  throughput, and log series exist; assert `strata_controller_namespace_owner` maps each namespace to
  exactly one owner; trigger a failover and assert `strata_controller_namespace_owner_changes_total`
  increments and the owner gauge flips.
- **Dashboard** — all `deploy/grafana/dashboards/*.json` parse as JSON; a guard test asserts no
  dashboard references a removed metric name (`strata_controller_log_*`, the old global io counters).

## 7. Out of scope / future (same idea, later)

- Per-namespace durability/repair (`strata_chunks_*`, `strata_repair_*` by namespace) — useful but the
  repair census is fleet-wide today; deferred.
- Recording rules / alerts (per-namespace SLOs, owner-flap alerts).
- Opcode-class grouping if full-opcode cardinality proves heavy in practice.
