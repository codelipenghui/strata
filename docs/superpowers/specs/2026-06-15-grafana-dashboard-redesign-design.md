# Grafana Dashboard Redesign — Design Spec

**Date:** 2026-06-15
**Status:** Approved for planning
**Scope:** Replace the single `strata-overview.json` Grafana dashboard with three purpose-built dashboards (Cluster / Node / JVM), and add read-path byte/ops instrumentation so read throughput can be charted symmetrically with writes.

---

## 1. Goals & non-goals

**Goals**
- Split the current single overview into three focused dashboards:
  1. **Strata — Cluster**: fleet-wide health, the on-call landing page.
  2. **Strata — Node**: per-node deep-dive with a node selector (multi-select + All) and an opcode selector.
  3. **Strata — JVM & Runtime**: standard JVM/runtime dashboard with the same node selector.
- Cover both write **and read** paths (throughput, ops, latency, errors).
- Provide click-through drill-down: Cluster → Node (per-instance), and cross-links between all three carrying the `$node` selection.
- Follow RED (Rate/Errors/Duration) for the request path and USE (Utilization/Saturation/Errors) for resources.

**Non-goals**
- No Grafana alerting rules (dashboards only).
- No new Micrometer binders (we use exactly what's registered today).
- No changes to scrape topology, ports, or the datasource.

---

## 2. Environment (verified against source)

- **Topology:** 3 `combined` nodes (`node1`/`node2`/`node3`), each running meta + node in one JVM with the common tag `role=combined`. Queries are written **role-agnostic** (no hard `role=` filter) so they also work in split `node`/`meta` deployments.
- **Metrics endpoint:** `:9300` `/metrics`, scraped by Prometheus job `strata`. Per-node identity is the Prometheus `instance` label with values `node1:9300`, `node2:9300`, `node3:9300`. Every metric also carries `job` and `role`.
- **Grafana:** 11.2.0. Dashboards target `schemaVersion: 39`. Datasource uid is `prometheus` (provisioned, fixed) and is referenced directly in panels (matches the existing dashboard; no `$datasource` variable).
- **Provisioning:** the file provider loads every JSON in `deploy/grafana/dashboards/` (`allowUiUpdates: true`, `foldersFromFilesStructure: false`). Adding files needs no config change; dashboards are keyed by `uid`, so two files must never share a uid.

---

## 3. Verified metric inventory (the only metrics panels may use)

All custom + JVM metrics carry the global labels `role`, `instance`, `job`.

### Durability / replication (meta-plane gauges)
- `strata_chunks_unavailable` — SEALED chunks with zero live replicas (data-loss exposure; page).
- `strata_chunks_under_replicated` — SEALED chunks below replication factor.
- `strata_chunks_at_min_redundancy` — SEALED chunks down to a single live replica.

### Repair (meta-plane gauges)
- `strata_repair_inflight` — outstanding repair/delete commands.
- `strata_repair_backlog` — distinct chunks currently being repaired.

### Cluster membership / leadership / ZK (meta-plane gauges)
- `strata_nodes{state="alive|suspect|dead"}` — storage nodes by liveness state.
- `strata_meta_is_leader` — 1 if this instance is the active metadata leader (cluster sum should be 1).
- `strata_meta_zk_connected` — 1 if ZooKeeper is reachable.

### Node registration / capacity / chunks (node-plane gauges)
- `strata_node_registered` — 1 if the node holds a metadata registration.
- `strata_node_disk_used_bytes` — bytes occupied by chunk data.
- `strata_node_capacity_bytes` — configured node capacity.
- `strata_node_capacity_used_ratio` — disk used / capacity (0..1).
- `strata_node_chunks{state="open|sealed"}` — local chunks by state.

### Write path (node-plane FunctionCounters → `_total`)
- `strata_node_groupcommit_force_total` — group-commit force()/fsync calls.
- `strata_node_append_ops_total` — appended records.
- `strata_node_append_bytes_total` — appended payload bytes.

### Read path (NEW — added by this work; see §4)
- `strata_node_read_ops_total` — client READ operations served.
- `strata_node_read_bytes_total` — client READ payload bytes served.

### SCP request latency (Timer → Prometheus histogram, base unit seconds)
- `strata_scp_request_duration_seconds_bucket{opcode,status,le}`
- `strata_scp_request_duration_seconds_count{opcode,status}`
- `strata_scp_request_duration_seconds_sum{opcode,status}`
  - `status` ∈ {`ok`, `error`}.
  - `opcode` is the opcode name; read opcodes include `READ`, `FETCH_CHUNK`, `READ_LEDGER`; write/data opcodes include `APPEND`, `OPEN_CHUNK`, `SEAL_CHUNK`, etc.; metadata opcodes include `LOOKUP_FILE`, `CREATE_CHUNK`, `REGISTER_NODE`, `NODE_HEARTBEAT`, etc.
  - SLO buckets: 1, 2, 5, 10, 25, 50, 100, 250, 500 ms, 1, 2.5, 5 s.

### JVM / process (registered binders: JvmMemory, JvmGc, JvmThread, Processor, FileDescriptor, Uptime)
- Memory: `jvm_memory_used_bytes{area,id}`, `jvm_memory_committed_bytes{area,id}`, `jvm_memory_max_bytes{area,id}` (`area` ∈ heap|nonheap; `id` = pool name).
- GC: `jvm_gc_pause_seconds_{bucket,count,sum}{action,cause}`, `jvm_gc_memory_allocated_bytes_total`, `jvm_gc_memory_promoted_bytes_total`, `jvm_gc_live_data_size_bytes`, `jvm_gc_max_data_size_bytes`.
- Threads: `jvm_threads_live_threads`, `jvm_threads_daemon_threads`, `jvm_threads_peak_threads`, `jvm_threads_states_threads{state}`.
- CPU/OS: `process_cpu_usage`, `system_cpu_usage`, `system_cpu_count`, `system_load_average_1m`, `process_cpu_time_seconds_total`.
- FDs: `process_files_open_files`, `process_files_max_files`.
- Uptime: `process_uptime_seconds`.

---

## 4. Read-path instrumentation (code change)

The request observer records only `(opcode, duration, success)` — no bytes — and reads use a sendfile/`transferTo` path, so there is no read-byte metric today. We add two FunctionCounters mirroring the append counters (plain atomic counters; FunctionCounter sampling = zero hot-path cost, consistent with the module's "no timers on the hot path" design).

**Bytes-served source:** the READ handler already computes the served length — `NodeHandlers` `case READ` builds the response via `ScpServer.okFileRegion(req, header, r.channel(), r.filePosition(), r.length())`. `r.length()` is the read byte count.

**Changes**
1. **`ChunkStore`** — add `readOps`/`readBytes` atomic counters with `readOps()` / `readBytes()` accessors, mirroring the existing `appendOps()` / `appendBytes()`. Provide the increment site used by the read path (e.g. inside `readRegion(...)` or a `recordRead(long bytes)` called from the handler) so each served client READ adds 1 op and `length` bytes.
2. **`StorageNode`** — add `readOps()` / `readBytes()` observability accessors delegating to the store (next to `appendOps()` / `appendBytes()` at lines ~148–154).
3. **`ServerMetrics.registerNode`** — register:
   - `FunctionCounter.builder("strata_node_read_ops", n, StorageNode::readOps)` → `strata_node_read_ops_total`
   - `FunctionCounter.builder("strata_node_read_bytes", n, StorageNode::readBytes)` → `strata_node_read_bytes_total`
4. **Tests** — unit test that issuing reads increments `readOps`/`readBytes` by the expected amounts (mirror the existing append-counter test), so the metric is verified independent of Grafana.

**Scope boundary:** count **client `READ`** only. `FETCH_CHUNK` (replication/repair reads) and `READ_LEDGER` (metadata reads) remain represented through the SCP opcode latency/rate/error panels; they are not folded into `strata_node_read_bytes_total`.

---

## 5. Conventions applied to every panel

- `rate()` windows use `$__rate_interval` (adapts to scrape interval and zoom) — not the hardcoded `[1m]`/`[5m]` of the old dashboard.
- Units: `Bps` (byte throughput), `bytes` (sizes), `percentunit` (ratios 0..1), `s` (durations), `reqps` (request rates), `short` (counts), `dtdurations`/`s` (uptime).
- Health stats use threshold colors; timeseries use the classic palette with explicit overrides for state series (alive=green, suspect=orange, dead=red).
- Timeseries default: legend as a table with `last`/`max`/`mean` calcs where useful; tooltip mode `multi`, sorted descending.
- Dashboard refresh `30s`, default time range `now-1h`.

---

## 6. Dashboard 1 — `Strata — Cluster` (uid `strata-overview`)

Replaces the old file; reuses uid `strata-overview` to preserve existing links/bookmarks.

**Row — Health & SLOs** (stat tiles)
- Chunks unavailable — `sum(strata_chunks_unavailable)` — green → **red ≥1**.
- Under-replicated — `sum(strata_chunks_under_replicated)` — yellow ≥1.
- At min redundancy (1 replica) — `sum(strata_chunks_at_min_redundancy)` — yellow ≥1.
- Metadata leaders (want 1) — `sum(strata_meta_is_leader)` — thresholds `[red @null, green @1, red @2]` (0 = no leader, 1 = healthy, ≥2 = split-brain).
- ZK connected (min) — `min(strata_meta_zk_connected)` — red → green @1.
- Nodes alive — `sum(strata_nodes{state="alive"})`.
- Nodes suspect/dead — `sum(strata_nodes{state=~"suspect|dead"})` — yellow ≥1.
- Write throughput — `sum(rate(strata_node_append_bytes_total[$__rate_interval]))` — Bps.
- Read throughput — `sum(rate(strata_node_read_bytes_total[$__rate_interval]))` — Bps.
- Request rate — `sum(rate(strata_scp_request_duration_seconds_count[$__rate_interval]))` — reqps.
- Error ratio — `sum(rate(strata_scp_request_duration_seconds_count{status="error"}[$__rate_interval])) / clamp_min(sum(rate(strata_scp_request_duration_seconds_count[$__rate_interval])), 1)` — percentunit, red on rise.

**Row — Durability & repair**
- Durability census (timeseries): three series `sum(strata_chunks_unavailable)`, `sum(strata_chunks_under_replicated)`, `sum(strata_chunks_at_min_redundancy)`.
- Repair backlog & in-flight (timeseries): `sum(strata_repair_backlog)`, `sum(strata_repair_inflight)`.

**Row — Cluster membership**
- Nodes by liveness (timeseries, stacked): `sum by (state)(strata_nodes)`, overrides alive=green/suspect=orange/dead=red.
- **Per-node fleet table** (drill-down hub): one row per `instance`, columns: registered, capacity used %, disk used, capacity, open chunks, sealed chunks. Built from instant queries merged on the `instance` label:
  - `strata_node_registered`
  - `strata_node_capacity_used_ratio`
  - `strata_node_disk_used_bytes`
  - `strata_node_capacity_bytes`
  - `strata_node_chunks{state="open"}`
  - `strata_node_chunks{state="sealed"}`
  - Transformations: `labels to fields` / `merge` / `organize` keyed on `instance`; field overrides for units (percentunit, bytes) and a value-based color on capacity %.
  - **Data link** on the row → `/d/strata-node?var-node=${__data.fields.instance}&${__url_time_range}`.

**Row — Capacity**
- Capacity used ratio by node (timeseries): `strata_node_capacity_used_ratio` legend `{{instance}}`, percentunit, max 1, threshold lines at 0.8/0.9. Per-series data link → Node dashboard using `${__field.labels.instance}`.
- Disk used vs total capacity (timeseries): `sum(strata_node_disk_used_bytes)`, `sum(strata_node_capacity_bytes)` — bytes.

**Row — I/O throughput**
- Read vs write throughput (timeseries): `sum(rate(strata_node_append_bytes_total[$__rate_interval]))` (write), `sum(rate(strata_node_read_bytes_total[$__rate_interval]))` (read) — Bps.
- Write ops/s & read ops/s (timeseries): `sum(rate(strata_node_append_ops_total[$__rate_interval]))`, `sum(rate(strata_node_read_ops_total[$__rate_interval]))`.
- Group-commit force/s (fsync) by node (timeseries): `sum by (instance)(rate(strata_node_groupcommit_force_total[$__rate_interval]))`.
- Avg bytes per append (timeseries): `sum(rate(strata_node_append_bytes_total[$__rate_interval])) / clamp_min(sum(rate(strata_node_append_ops_total[$__rate_interval])), 1)` — bytes.

**Row — Requests (RED)**
- Request rate by opcode: `sum by (opcode)(rate(strata_scp_request_duration_seconds_count[$__rate_interval]))` — reqps.
- Error rate by opcode: `sum by (opcode)(rate(strata_scp_request_duration_seconds_count{status="error"}[$__rate_interval]))`.
- p99 latency by opcode: `histogram_quantile(0.99, sum by (le, opcode)(rate(strata_scp_request_duration_seconds_bucket[$__rate_interval])))` — s.
- Latency p50/p90/p99 (all opcodes): three `histogram_quantile(q, sum by (le)(rate(..._bucket[$__rate_interval])))` series — s.

---

## 7. Dashboard 2 — `Strata — Node` (uid `strata-node`)

**Template variables**
- `$node` — query `label_values(strata_node_disk_used_bytes, instance)`; multi-select; includeAll (All = `.*` regex); default All; refresh on time range change.
- `$opcode` — query `label_values(strata_scp_request_duration_seconds_count, opcode)`; multi-select; includeAll; default All; used only by the request panels.

Every panel filters `{instance=~"$node"}` (request panels also `opcode=~"$opcode"`).

**Row — Overview** (stat tiles, reflecting the selection)
- Nodes registered — `sum(strata_node_registered{instance=~"$node"})`.
- Capacity used (max) — `max(strata_node_capacity_used_ratio{instance=~"$node"})` — percentunit, thresholds 0.8/0.9.
- Disk used — `sum(strata_node_disk_used_bytes{instance=~"$node"})` — bytes.
- Open chunks — `sum(strata_node_chunks{instance=~"$node",state="open"})`.
- Sealed chunks — `sum(strata_node_chunks{instance=~"$node",state="sealed"})`.
- Write throughput — `sum(rate(strata_node_append_bytes_total{instance=~"$node"}[$__rate_interval]))` — Bps.
- Read throughput — `sum(rate(strata_node_read_bytes_total{instance=~"$node"}[$__rate_interval]))` — Bps.
- fsync/s — `sum(rate(strata_node_groupcommit_force_total{instance=~"$node"}[$__rate_interval]))`.

**Row — Capacity & storage**
- Capacity used ratio by node: `strata_node_capacity_used_ratio{instance=~"$node"}` — percentunit + threshold lines.
- Disk used vs capacity by node: `strata_node_disk_used_bytes{instance=~"$node"}`, `strata_node_capacity_bytes{instance=~"$node"}` — bytes.
- Local chunks by state: `sum by (instance, state)(strata_node_chunks{instance=~"$node"})`.

**Row — I/O throughput**
- Write throughput by node: `sum by (instance)(rate(strata_node_append_bytes_total{instance=~"$node"}[$__rate_interval]))` — Bps.
- Read throughput by node: `sum by (instance)(rate(strata_node_read_bytes_total{instance=~"$node"}[$__rate_interval]))` — Bps.
- Append ops/s & read ops/s by node: `sum by (instance)(rate(strata_node_append_ops_total{instance=~"$node"}[$__rate_interval]))`, `sum by (instance)(rate(strata_node_read_ops_total{instance=~"$node"}[$__rate_interval]))`.
- Group-commit force/s (fsync) by node: `sum by (instance)(rate(strata_node_groupcommit_force_total{instance=~"$node"}[$__rate_interval]))`.
- Avg bytes per append: `sum by (instance)(rate(strata_node_append_bytes_total{instance=~"$node"}[$__rate_interval])) / clamp_min(sum by (instance)(rate(strata_node_append_ops_total{instance=~"$node"}[$__rate_interval])), 1)` — bytes.
- Avg bytes per read: same shape with read counters — bytes.
- Appends per fsync (batch efficiency): `sum by (instance)(rate(strata_node_append_ops_total{instance=~"$node"}[$__rate_interval])) / clamp_min(sum by (instance)(rate(strata_node_groupcommit_force_total{instance=~"$node"}[$__rate_interval])), 1)` — short.

**Row — Requests on this node (RED)** (filtered `{instance=~"$node", opcode=~"$opcode"}`)
- Request rate by opcode.
- Error rate by opcode (`status="error"`).
- p99 latency by opcode.
- Latency p50/p90/p99 (selected opcodes).
- Latency heatmap: `sum by (le)(rate(strata_scp_request_duration_seconds_bucket{instance=~"$node",opcode=~"$opcode"}[$__rate_interval]))` — heatmap format, unit s.

**Row — JVM quicklook**
- Heap used by node: `sum by (instance)(jvm_memory_used_bytes{area="heap",instance=~"$node"})` — bytes.
- Process CPU by node: `process_cpu_usage{instance=~"$node"}` — percentunit.
- Panel/data link → JVM dashboard carrying `$node`.

---

## 8. Dashboard 3 — `Strata — JVM & Runtime` (uid `strata-jvm`)

**Template variable:** `$node` — identical definition to the Node dashboard. All panels filter `{instance=~"$node"}`.

**Row — Runtime overview** (stat tiles)
- Uptime (min): `min(process_uptime_seconds{instance=~"$node"})` — s (duration).
- Heap used %: `max(sum by (instance)(jvm_memory_used_bytes{area="heap",instance=~"$node"}) / sum by (instance)(jvm_memory_max_bytes{area="heap",instance=~"$node"}))` — percentunit, thresholds 0.8/0.9.
- Live threads: `sum(jvm_threads_live_threads{instance=~"$node"})`.
- Process CPU: `max(process_cpu_usage{instance=~"$node"})` — percentunit.
- System load (1m): `max(system_load_average_1m{instance=~"$node"})`.
- Open file descriptors: `max(process_files_open_files{instance=~"$node"})`.
- GC time/s: `sum(rate(jvm_gc_pause_seconds_sum{instance=~"$node"}[$__rate_interval]))` — s.

**Row — Memory**
- Heap used by node: `sum by (instance)(jvm_memory_used_bytes{area="heap",instance=~"$node"})` — bytes (+ `jvm_memory_max_bytes` heap as a dashed reference).
- Heap used % by node: used/max heap per instance — percentunit.
- Non-heap used by node: `sum by (instance)(jvm_memory_used_bytes{area="nonheap",instance=~"$node"})` — bytes.
- Memory pools: `sum by (id)(jvm_memory_used_bytes{instance=~"$node"})` — bytes (Eden/Survivor/Old/Metaspace/Code Cache).
- Heap committed vs used vs max: three series — bytes.

**Row — Garbage collection**
- GC time/s by node: `sum by (instance)(rate(jvm_gc_pause_seconds_sum{instance=~"$node"}[$__rate_interval]))` — s.
- GC count/s by action: `sum by (action)(rate(jvm_gc_pause_seconds_count{instance=~"$node"}[$__rate_interval]))`.
- Avg GC pause by action: `sum by (action)(rate(jvm_gc_pause_seconds_sum{instance=~"$node"}[$__rate_interval])) / clamp_min(sum by (action)(rate(jvm_gc_pause_seconds_count{instance=~"$node"}[$__rate_interval])), 1)` — s.
- Allocation rate: `sum(rate(jvm_gc_memory_allocated_bytes_total{instance=~"$node"}[$__rate_interval]))` — Bps.
- Promotion rate: `sum(rate(jvm_gc_memory_promoted_bytes_total{instance=~"$node"}[$__rate_interval]))` — Bps.
- Live data size after GC: `jvm_gc_live_data_size_bytes{instance=~"$node"}` — bytes.

**Row — Threads**
- Threads live/daemon/peak by node: `jvm_threads_live_threads`, `jvm_threads_daemon_threads`, `jvm_threads_peak_threads` filtered by `$node`.
- Threads by state: `sum by (state)(jvm_threads_states_threads{instance=~"$node"})`.

**Row — CPU & OS**
- Process vs system CPU: `process_cpu_usage{instance=~"$node"}`, `system_cpu_usage{instance=~"$node"}` — percentunit.
- System load (1m) + CPU count reference: `system_load_average_1m{instance=~"$node"}`, `system_cpu_count{instance=~"$node"}`.
- File descriptors open vs max: `process_files_open_files{instance=~"$node"}`, `process_files_max_files{instance=~"$node"}`.
- CPU time/s: `sum(rate(process_cpu_time_seconds_total{instance=~"$node"}[$__rate_interval]))`.

---

## 9. Cross-dashboard navigation

- **Dashboard-level links** (the nav dropdown) on all three dashboards pointing to the other two, with `keepTime: true` and `includeVars: true` so the active time range and `$node` selection propagate.
- **Cluster → Node drill-down:** the fleet table row link and per-node timeseries data links target `/d/strata-node?var-node=<instance>`.
- **Node → JVM:** the JVM quicklook panel links to `/d/strata-jvm?var-node=$node`.

---

## 10. File changes

- **Add** `deploy/grafana/dashboards/strata-cluster.json` (uid `strata-overview`, title `Strata — Cluster`).
- **Add** `deploy/grafana/dashboards/strata-node.json` (uid `strata-node`, title `Strata — Node`).
- **Add** `deploy/grafana/dashboards/strata-jvm.json` (uid `strata-jvm`, title `Strata — JVM & Runtime`).
- **Delete** `deploy/grafana/dashboards/strata-overview.json` (old single overview; uid is reused by the new Cluster dashboard, so the old file must be removed to avoid a uid clash).
- **Edit** `docker-compose.yml` — update the stale Grafana comment (line ~113) that points only at "Strata — Cluster Overview" to mention the three dashboards.
- **Code** — `ChunkStore`, `StorageNode`, `ServerMetrics` (+ tests) per §4.
- **Memory** — update `observability.md` to note the three dashboards and the new read counters (post-merge housekeeping).

---

## 11. Verification

1. **Read counters (TDD):** unit test asserting `readOps`/`readBytes` increment on reads — written before the implementation, must fail then pass.
2. **JSON validity:** each dashboard file parses (`python3 -m json.tool`).
3. **Metric/label lint:** assert every metric name and label referenced in the three dashboards exists in the §3 inventory (grep each `strata_*`/`jvm_*`/`process_*`/`system_*` token against the inventory list) — catches typos like a missing `_seconds`/`_total` suffix.
4. **Provisioning load:** start a throwaway Grafana 11.2.0 container mounting `deploy/grafana/provisioning` + `deploy/grafana/dashboards`, and assert the logs contain no `failed to load dashboard` / provisioning errors and that all three uids register. (Datasource resolution is per-query, so Prometheus need not be reachable for provisioning to succeed.)
5. **Build:** `./gradlew build` (or module build) green, including the new counter tests.

---

## 12. Risks / open considerations

- **`role` is always `combined` in the bundled compose**, but queries deliberately avoid `role=` so they hold for split deployments too. No functional risk.
- **`instance` carries the `:9300` port** (e.g. `node1:9300`); selector values and table rows show it verbatim. Acceptable; a regex strip is cosmetic and skipped (YAGNI).
- **Heatmap panel** depends on Grafana 11's `heatmap` panel reading native histogram buckets via the `le` label — standard and supported in 11.2.0.
- **Read-byte accounting** counts client `READ` served bytes only; replication (`FETCH_CHUNK`) and metadata (`READ_LEDGER`) reads remain visible via the SCP opcode panels but are excluded from `strata_node_read_bytes_total` by design.
