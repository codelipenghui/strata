# Grafana Dashboard Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single `strata-overview.json` Grafana dashboard with three focused dashboards (Cluster / Node / JVM) and add read-path byte/ops counters so read throughput charts symmetrically with writes.

**Architecture:** A small data-path change adds two FunctionCounters (`strata_node_read_bytes_total`, `strata_node_read_ops_total`) mirroring the existing append counters, incremented in `ChunkStore.readRegion` (the client-READ-only path). Three new provisioned Grafana JSON dashboards consume the full verified metric set, wired with `$node`/`$opcode` template variables and cross-dashboard drill-down links. The old overview file is deleted; its uid is reused by the new Cluster dashboard.

**Tech Stack:** Java 21, Maven (no wrapper — use `mvn`), Micrometer + Prometheus registry, JUnit 5, Grafana 11.2.0 (provisioned dashboards, schemaVersion 39), Prometheus, Docker Compose.

**Companion spec:** [docs/superpowers/specs/2026-06-15-grafana-dashboard-redesign-design.md](../specs/2026-06-15-grafana-dashboard-redesign-design.md) — §3 is the authoritative metric inventory; §6/§7/§8 list every panel's exact PromQL. This plan inlines the tricky queries and JSON shapes; consult the spec for the full per-panel query list.

---

## File Structure

**Code (read counters):**
- Modify `strata-format/src/main/java/io/strata/format/ChunkStore.java` — add `readOps`/`readBytes` AtomicLongs, accessors, and the increment in `readRegion`.
- Modify `strata-format/src/test/java/io/strata/format/ChunkStoreTest.java` — add the counter test.
- Modify `strata-node/src/main/java/io/strata/node/StorageNode.java` — add `readOps()`/`readBytes()` delegating accessors.
- Modify `strata-server/src/main/java/io/strata/server/ServerMetrics.java` — register the two FunctionCounters.

**Dashboards:**
- Create `deploy/grafana/dashboards/strata-cluster.json` (uid `strata-overview`).
- Create `deploy/grafana/dashboards/strata-node.json` (uid `strata-node`).
- Create `deploy/grafana/dashboards/strata-jvm.json` (uid `strata-jvm`).
- Delete `deploy/grafana/dashboards/strata-overview.json`.
- Modify `docker-compose.yml` — update the stale Grafana comment.

**Docs:**
- Update memory `observability.md` (post-merge housekeeping; see Task 9).

---

## Conventions for the dashboard JSON

Every panel uses `"datasource": { "type": "prometheus", "uid": "prometheus" }`. Sizes (Grafana grid is 24 columns wide; lay panels left-to-right within each row, Grafana wraps):
- **row** separator: `{ "type": "row", "title": "...", "gridPos": {"h":1,"w":24,"x":0,"y":Y}, "collapsed": false }`
- **stat**: `h:4, w:4`
- **timeseries**: `h:8, w:12`
- **table**: `h:10, w:24`
- **heatmap**: `h:8, w:24`

Reusable JSON fragments (copy and adapt per panel):

**Stat tile with thresholds**
```json
{
  "type": "stat", "title": "Chunks unavailable",
  "datasource": { "type": "prometheus", "uid": "prometheus" },
  "gridPos": { "h": 4, "w": 4, "x": 0, "y": 1 },
  "targets": [ { "expr": "sum(strata_chunks_unavailable)", "refId": "A", "instant": true } ],
  "fieldConfig": { "defaults": {
    "unit": "short",
    "color": { "mode": "thresholds" },
    "thresholds": { "mode": "absolute", "steps": [ { "color": "green", "value": null }, { "color": "red", "value": 1 } ] }
  } },
  "options": { "colorMode": "background", "graphMode": "area", "reduceOptions": { "calcs": ["lastNotNull"] } }
}
```

**Timeseries panel**
```json
{
  "type": "timeseries", "title": "Read vs write throughput",
  "datasource": { "type": "prometheus", "uid": "prometheus" },
  "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
  "targets": [
    { "expr": "sum(rate(strata_node_append_bytes_total[$__rate_interval]))", "refId": "A", "legendFormat": "write" },
    { "expr": "sum(rate(strata_node_read_bytes_total[$__rate_interval]))", "refId": "B", "legendFormat": "read" }
  ],
  "fieldConfig": { "defaults": { "unit": "Bps", "custom": { "fillOpacity": 10, "showPoints": "never" } } },
  "options": { "legend": { "displayMode": "table", "placement": "bottom", "calcs": ["last","max","mean"] }, "tooltip": { "mode": "multi", "sort": "desc" } }
}
```

**Template variable (`$node`)** — in `templating.list`:
```json
{
  "name": "node", "label": "Node", "type": "query",
  "datasource": { "type": "prometheus", "uid": "prometheus" },
  "query": { "query": "label_values(strata_node_disk_used_bytes, instance)", "refId": "StandardVariableQuery" },
  "includeAll": true, "multi": true, "allValue": ".*", "current": { "text": "All", "value": "$__all" },
  "refresh": 2, "sort": 1
}
```

**Template variable (`$opcode`)** — same shape, `"name": "opcode"`, query `label_values(strata_scp_request_duration_seconds_count, opcode)`.

**Dashboard nav links** — top-level `"links"`:
```json
[
  { "type": "dashboards", "title": "Strata dashboards", "asDropdown": true, "keepTime": true, "includeVars": true, "tags": ["strata"] }
]
```
(Tag all three dashboards `"tags": ["strata"]` so the dropdown lists them and `includeVars`/`keepTime` propagate `$node` + the time range.)

**Table data link (Cluster fleet table → Node dashboard)** — on the table panel:
```json
"fieldConfig": { "defaults": { "links": [
  { "title": "Open node dashboard", "url": "/d/strata-node?var-node=${__data.fields.instance}&${__url_time_range}" }
] } }
```

**Per-series data link (by-node timeseries → Node dashboard)**:
```json
"fieldConfig": { "defaults": { "links": [
  { "title": "Open node dashboard", "url": "/d/strata-node?var-node=${__field.labels.instance}&${__url_time_range}" }
] } }
```

---

## Task 1: Read-path counters in ChunkStore (TDD)

**Files:**
- Modify: `strata-format/src/main/java/io/strata/format/ChunkStore.java`
- Test: `strata-format/src/test/java/io/strata/format/ChunkStoreTest.java`

- [ ] **Step 1: Write the failing test**

Add this method to `ChunkStoreTest` (uses the existing `@TempDir Path dir`, `ChunkId id`, `newStore()`, `open(...)`, and `bytes(...)` helpers):

```java
@Test
void readCountersTrackServedReads() throws Exception {
    try (ChunkStore store = newStore()) {
        assertEquals(0, store.readOps());
        assertEquals(0, store.readBytes());

        open(store, id, 1);
        store.append(id, 1, 0, 0, bytes("hello world")); // 11 bytes, chunk still OPEN

        // OPEN-chunk read: bytes materialized under the lock, counted by served length
        var r1 = store.readRegion(id, 0, 1024);
        assertEquals(11, r1.length());
        assertEquals(1, store.readOps());
        assertEquals(11, store.readBytes());

        // partial read accrues
        var r2 = store.readRegion(id, 6, 1024); // "world" -> 5 bytes
        assertEquals(5, r2.length());
        assertEquals(2, store.readOps());
        assertEquals(16, store.readBytes());

        // read at/after end serves nothing and must NOT count
        var r3 = store.readRegion(id, 11, 1024);
        assertEquals(0, r3.length());
        assertEquals(2, store.readOps());
        assertEquals(16, store.readBytes());

        // SEALED read path (zero-copy region) also counts
        store.seal(id, 1, 11, null);
        var r4 = store.readRegion(id, 0, 1024);
        assertEquals(11, r4.length());
        assertEquals(3, store.readOps());
        assertEquals(27, store.readBytes());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl strata-format -am test -Dtest=ChunkStoreTest#readCountersTrackServedReads`
Expected: COMPILE FAILURE — `cannot find symbol: method readOps()` / `readBytes()`.

- [ ] **Step 3: Add the counter fields**

In `ChunkStore.java`, after the `appendBytes` field (currently lines 54–55), add:

```java
    private final java.util.concurrent.atomic.AtomicLong readOps =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong readBytes =
            new java.util.concurrent.atomic.AtomicLong();
```

- [ ] **Step 4: Add the accessors**

In `ChunkStore.java`, after `appendBytes()` (currently ends line 76), add:

```java
    /** Total client READ operations that served data since start (drives read-ops/sec via rate()). */
    public long readOps() {
        return readOps.get();
    }

    /** Total client READ payload bytes served since start (drives read throughput via rate()). */
    public long readBytes() {
        return readBytes.get();
    }
```

- [ ] **Step 5: Increment at the served-bytes site in `readRegion`**

In `ChunkStore.readRegion`, the `n == 0` early-return block is currently:

```java
            int n = (int) Math.min(Math.min(maxBytes, MAX_REQUEST_BYTES), end - offset);
            if (n == 0) {
                return new ReadRegionResult(null, 0, 0, null, end, h.lastKnownDO);
            }
            long filePos = checkedAdd(DATA_START, offset, "chunk file offset");
```

Insert the counter bump between the `n == 0` block and the `long filePos = ...` line — both remaining branches (OPEN materialized + SEALED region) serve exactly `n` bytes, so this single site counts every data-serving read once:

```java
            int n = (int) Math.min(Math.min(maxBytes, MAX_REQUEST_BYTES), end - offset);
            if (n == 0) {
                return new ReadRegionResult(null, 0, 0, null, end, h.lastKnownDO);
            }
            // observability: count client READ bytes served (mirrors append counters; drives read throughput)
            readOps.incrementAndGet();
            readBytes.addAndGet(n);
            long filePos = checkedAdd(DATA_START, offset, "chunk file offset");
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `mvn -q -pl strata-format -am test -Dtest=ChunkStoreTest#readCountersTrackServedReads`
Expected: PASS (Tests run: 1, Failures: 0).

- [ ] **Step 7: Commit**

```bash
git add strata-format/src/main/java/io/strata/format/ChunkStore.java \
        strata-format/src/test/java/io/strata/format/ChunkStoreTest.java
git commit -m "Add client READ byte/ops counters to ChunkStore"
```

---

## Task 2: Expose read accessors on StorageNode

**Files:**
- Modify: `strata-node/src/main/java/io/strata/node/StorageNode.java`

- [ ] **Step 1: Add delegating accessors**

In `StorageNode.java`, after `appendBytes()` (currently ends line 154), add:

```java
    public long readOps() {
        return store.readOps();
    }

    public long readBytes() {
        return store.readBytes();
    }
```

- [ ] **Step 2: Verify it compiles**

Run: `mvn -q -pl strata-node -am test-compile`
Expected: BUILD SUCCESS (no errors).

- [ ] **Step 3: Commit**

```bash
git add strata-node/src/main/java/io/strata/node/StorageNode.java
git commit -m "Expose read ops/bytes accessors on StorageNode"
```

---

## Task 3: Register read FunctionCounters in ServerMetrics

**Files:**
- Modify: `strata-server/src/main/java/io/strata/server/ServerMetrics.java`

- [ ] **Step 1: Register the two counters**

In `ServerMetrics.registerNode`, after the existing `strata_node_append_bytes` builder (currently lines 76–77), add:

```java
        FunctionCounter.builder("strata_node_read_ops", n, StorageNode::readOps)
                .description("client READ operations served (rate() = read ops/sec)").register(reg);
        FunctionCounter.builder("strata_node_read_bytes", n, StorageNode::readBytes)
                .description("client READ payload bytes served (rate() = read throughput)").register(reg);
```

- [ ] **Step 2: Build the server module**

Run: `mvn -q -pl strata-server -am test-compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Full build with tests**

Run: `mvn -q install`
Expected: BUILD SUCCESS, all module tests pass (includes the Task 1 counter test).

- [ ] **Step 4: Commit**

```bash
git add strata-server/src/main/java/io/strata/server/ServerMetrics.java
git commit -m "Export strata_node_read_bytes_total + strata_node_read_ops_total"
```

---

## Task 4: Cluster dashboard JSON

**Files:**
- Create: `deploy/grafana/dashboards/strata-cluster.json`

Top-level: `"uid": "strata-overview"`, `"title": "Strata — Cluster"`, `"tags": ["strata"]`, `"schemaVersion": 39`, `"version": 1`, `"refresh": "30s"`, `"time": {"from":"now-1h","to":"now"}`, `"templating": {"list": []}`, and the `"links"` dropdown fragment from Conventions.

- [ ] **Step 1: Build the panel array**

Add rows + panels in this order (full PromQL in spec §6; key queries inlined here). Assign `id` sequentially and `gridPos` per the size conventions.

Row **Health & SLOs** — stat tiles (`h:4,w:4`):
| Title | expr | unit | thresholds |
|---|---|---|---|
| Chunks unavailable | `sum(strata_chunks_unavailable)` | short | green, red@1 |
| Under-replicated | `sum(strata_chunks_under_replicated)` | short | green, yellow@1 |
| At min redundancy | `sum(strata_chunks_at_min_redundancy)` | short | green, yellow@1 |
| Metadata leaders (want 1) | `sum(strata_meta_is_leader)` | short | red@null, green@1, red@2 |
| ZK connected (min) | `min(strata_meta_zk_connected)` | short | red@null, green@1 |
| Nodes alive | `sum(strata_nodes{state="alive"})` | short | green |
| Nodes suspect/dead | `sum(strata_nodes{state=~"suspect\|dead"})` | short | green, yellow@1 |
| Write throughput | `sum(rate(strata_node_append_bytes_total[$__rate_interval]))` | Bps | — |
| Read throughput | `sum(rate(strata_node_read_bytes_total[$__rate_interval]))` | Bps | — |
| Request rate | `sum(rate(strata_scp_request_duration_seconds_count[$__rate_interval]))` | reqps | — |
| Error ratio | `sum(rate(strata_scp_request_duration_seconds_count{status="error"}[$__rate_interval])) / clamp_min(sum(rate(strata_scp_request_duration_seconds_count[$__rate_interval])), 1)` | percentunit | green, red@0.01 |

Row **Durability & repair** — timeseries (`h:8,w:12`):
- Durability census: 3 targets `sum(strata_chunks_unavailable)`, `sum(strata_chunks_under_replicated)`, `sum(strata_chunks_at_min_redundancy)`; unit short.
- Repair backlog & in-flight: `sum(strata_repair_backlog)`, `sum(strata_repair_inflight)`; unit short.

Row **Cluster membership**:
- Nodes by liveness (timeseries `h:8,w:12`, stacked): `sum by (state)(strata_nodes)`, `legendFormat "{{state}}"`; field overrides color alive=green / suspect=orange / dead=red; `custom.stacking.mode = "normal"`.
- Per-node fleet table (`h:10,w:24`, `"type":"table"`) — six **instant** targets:
  - A `strata_node_registered`
  - B `strata_node_capacity_used_ratio`
  - C `strata_node_disk_used_bytes`
  - D `strata_node_capacity_bytes`
  - E `strata_node_chunks{state="open"}`
  - F `strata_node_chunks{state="sealed"}`
  - `transformations`: `[{ "id": "merge" }, { "id": "organize", "options": { ... rename Value #A..F to Registered / Capacity % / Disk used / Capacity / Open chunks / Sealed chunks, keep instance } }]`
  - field overrides: Capacity % → `unit percentunit` + value thresholds (green / yellow@0.8 / red@0.9); Disk used & Capacity → `unit bytes`.
  - data link (table fragment from Conventions).

Row **Capacity** — timeseries (`h:8,w:12`):
- Capacity used ratio by node: `strata_node_capacity_used_ratio`, `legendFormat "{{instance}}"`, unit percentunit, `min 0 max 1`, threshold lines at 0.8/0.9; per-series data link fragment.
- Disk used vs total capacity: `sum(strata_node_disk_used_bytes)`, `sum(strata_node_capacity_bytes)`; unit bytes.

Row **I/O throughput** — timeseries (`h:8,w:12`):
- Read vs write throughput: `sum(rate(strata_node_append_bytes_total[$__rate_interval]))` (write), `sum(rate(strata_node_read_bytes_total[$__rate_interval]))` (read); unit Bps.
- Write & read ops/s: `sum(rate(strata_node_append_ops_total[$__rate_interval]))`, `sum(rate(strata_node_read_ops_total[$__rate_interval]))`; unit ops (use `"short"` or `"ops"`).
- Group-commit force/s (fsync) by node: `sum by (instance)(rate(strata_node_groupcommit_force_total[$__rate_interval]))`; unit short.
- Avg bytes per append: `sum(rate(strata_node_append_bytes_total[$__rate_interval])) / clamp_min(sum(rate(strata_node_append_ops_total[$__rate_interval])), 1)`; unit bytes.

Row **Requests (RED)** — timeseries (`h:8,w:12`):
- Request rate by opcode: `sum by (opcode)(rate(strata_scp_request_duration_seconds_count[$__rate_interval]))`, `legendFormat "{{opcode}}"`, unit reqps.
- Error rate by opcode: `sum by (opcode)(rate(strata_scp_request_duration_seconds_count{status="error"}[$__rate_interval]))`, unit reqps.
- p99 latency by opcode: `histogram_quantile(0.99, sum by (le, opcode)(rate(strata_scp_request_duration_seconds_bucket[$__rate_interval])))`, unit s.
- Latency p50/p90/p99 (all opcodes): three targets `histogram_quantile(0.5|0.9|0.99, sum by (le)(rate(strata_scp_request_duration_seconds_bucket[$__rate_interval])))`, unit s.

- [ ] **Step 2: Validate JSON parses**

Run: `python3 -m json.tool deploy/grafana/dashboards/strata-cluster.json > /dev/null && echo OK`
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add deploy/grafana/dashboards/strata-cluster.json
git commit -m "Add Strata Cluster dashboard"
```

---

## Task 5: Node dashboard JSON

**Files:**
- Create: `deploy/grafana/dashboards/strata-node.json`

Top-level: `"uid": "strata-node"`, `"title": "Strata — Node"`, `"tags": ["strata"]`, schemaVersion 39, refresh 30s, time now-1h, the `"links"` dropdown, and `templating.list` containing the `$node` and `$opcode` variables (fragments from Conventions). Every panel target filters `{instance=~"$node"}` (request panels also `opcode=~"$opcode"`). Full PromQL in spec §7.

- [ ] **Step 1: Build the panel array**

Row **Overview** — stat tiles (`h:4,w:4`): nodes registered `sum(strata_node_registered{instance=~"$node"})`; capacity used (max) `max(strata_node_capacity_used_ratio{instance=~"$node"})` percentunit thresholds 0.8/0.9; disk used `sum(strata_node_disk_used_bytes{instance=~"$node"})` bytes; open chunks `sum(strata_node_chunks{instance=~"$node",state="open"})`; sealed chunks `sum(strata_node_chunks{instance=~"$node",state="sealed"})`; write throughput `sum(rate(strata_node_append_bytes_total{instance=~"$node"}[$__rate_interval]))` Bps; read throughput `sum(rate(strata_node_read_bytes_total{instance=~"$node"}[$__rate_interval]))` Bps; fsync/s `sum(rate(strata_node_groupcommit_force_total{instance=~"$node"}[$__rate_interval]))`.

Row **Capacity & storage** — timeseries (`h:8,w:12`): capacity used ratio `strata_node_capacity_used_ratio{instance=~"$node"}` `{{instance}}` percentunit + threshold lines; disk used vs capacity `strata_node_disk_used_bytes{instance=~"$node"}` + `strata_node_capacity_bytes{instance=~"$node"}` bytes; local chunks by state `sum by (instance, state)(strata_node_chunks{instance=~"$node"})` `{{instance}} {{state}}`.

Row **I/O throughput** — timeseries (`h:8,w:12`):
- Write throughput by node: `sum by (instance)(rate(strata_node_append_bytes_total{instance=~"$node"}[$__rate_interval]))` Bps.
- Read throughput by node: `sum by (instance)(rate(strata_node_read_bytes_total{instance=~"$node"}[$__rate_interval]))` Bps.
- Append & read ops/s by node: `sum by (instance)(rate(strata_node_append_ops_total{instance=~"$node"}[$__rate_interval]))`, `sum by (instance)(rate(strata_node_read_ops_total{instance=~"$node"}[$__rate_interval]))`.
- Group-commit force/s by node: `sum by (instance)(rate(strata_node_groupcommit_force_total{instance=~"$node"}[$__rate_interval]))`.
- Avg bytes per append: `sum by (instance)(rate(strata_node_append_bytes_total{instance=~"$node"}[$__rate_interval])) / clamp_min(sum by (instance)(rate(strata_node_append_ops_total{instance=~"$node"}[$__rate_interval])), 1)` bytes.
- Avg bytes per read: same shape with `read_bytes`/`read_ops` bytes.
- Appends per fsync: `sum by (instance)(rate(strata_node_append_ops_total{instance=~"$node"}[$__rate_interval])) / clamp_min(sum by (instance)(rate(strata_node_groupcommit_force_total{instance=~"$node"}[$__rate_interval])), 1)` short.

Row **Requests on this node (RED)** — filter `{instance=~"$node", opcode=~"$opcode"}`:
- Request rate by opcode (timeseries `h:8,w:12`): `sum by (opcode)(rate(strata_scp_request_duration_seconds_count{instance=~"$node",opcode=~"$opcode"}[$__rate_interval]))` reqps.
- Error rate by opcode: add `,status="error"` reqps.
- p99 latency by opcode: `histogram_quantile(0.99, sum by (le, opcode)(rate(strata_scp_request_duration_seconds_bucket{instance=~"$node",opcode=~"$opcode"}[$__rate_interval])))` s.
- Latency p50/p90/p99 (selected opcodes): three `histogram_quantile(q, sum by (le)(rate(strata_scp_request_duration_seconds_bucket{instance=~"$node",opcode=~"$opcode"}[$__rate_interval])))` s.
- Latency heatmap (`"type":"heatmap"`, `h:8,w:24`): target `sum by (le)(rate(strata_scp_request_duration_seconds_bucket{instance=~"$node",opcode=~"$opcode"}[$__rate_interval]))` with `"format": "heatmap"` and `"legendFormat": "{{le}}"`; panel `options.calculate=false`, `yAxis.unit="s"`.

Row **JVM quicklook** — timeseries (`h:8,w:12`): heap used by node `sum by (instance)(jvm_memory_used_bytes{area="heap",instance=~"$node"})` bytes; process CPU `process_cpu_usage{instance=~"$node"}` `{{instance}}` percentunit. Add a panel data link on either to `/d/strata-jvm?var-node=$node&${__url_time_range}`.

- [ ] **Step 2: Validate JSON parses**

Run: `python3 -m json.tool deploy/grafana/dashboards/strata-node.json > /dev/null && echo OK`
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add deploy/grafana/dashboards/strata-node.json
git commit -m "Add Strata Node dashboard with node/opcode selectors"
```

---

## Task 6: JVM dashboard JSON

**Files:**
- Create: `deploy/grafana/dashboards/strata-jvm.json`

Top-level: `"uid": "strata-jvm"`, `"title": "Strata — JVM & Runtime"`, `"tags": ["strata"]`, schemaVersion 39, refresh 30s, time now-1h, the `"links"` dropdown, and `templating.list` containing the `$node` variable only. Every panel filters `{instance=~"$node"}`. Full PromQL in spec §8.

- [ ] **Step 1: Build the panel array**

Row **Runtime overview** — stat tiles (`h:4,w:4`): uptime (min) `min(process_uptime_seconds{instance=~"$node"})` unit `s` (`"decimals":0`); heap used % `max(sum by (instance)(jvm_memory_used_bytes{area="heap",instance=~"$node"}) / sum by (instance)(jvm_memory_max_bytes{area="heap",instance=~"$node"}))` percentunit thresholds 0.8/0.9; live threads `sum(jvm_threads_live_threads{instance=~"$node"})`; process CPU `max(process_cpu_usage{instance=~"$node"})` percentunit; system load 1m `max(system_load_average_1m{instance=~"$node"})`; open FDs `max(process_files_open_files{instance=~"$node"})`; GC time/s `sum(rate(jvm_gc_pause_seconds_sum{instance=~"$node"}[$__rate_interval]))` s.

Row **Memory** — timeseries (`h:8,w:12`):
- Heap used by node: `sum by (instance)(jvm_memory_used_bytes{area="heap",instance=~"$node"})` + `sum by (instance)(jvm_memory_max_bytes{area="heap",instance=~"$node"})` (max as reference) bytes.
- Heap used % by node: `sum by (instance)(jvm_memory_used_bytes{area="heap",instance=~"$node"}) / sum by (instance)(jvm_memory_max_bytes{area="heap",instance=~"$node"})` percentunit.
- Non-heap used by node: `sum by (instance)(jvm_memory_used_bytes{area="nonheap",instance=~"$node"})` bytes.
- Memory pools: `sum by (id)(jvm_memory_used_bytes{instance=~"$node"})` `{{id}}` bytes.
- Heap committed/used/max: `sum(jvm_memory_committed_bytes{area="heap",instance=~"$node"})`, `sum(jvm_memory_used_bytes{area="heap",instance=~"$node"})`, `sum(jvm_memory_max_bytes{area="heap",instance=~"$node"})` bytes.

Row **Garbage collection** — timeseries (`h:8,w:12`):
- GC time/s by node: `sum by (instance)(rate(jvm_gc_pause_seconds_sum{instance=~"$node"}[$__rate_interval]))` s.
- GC count/s by action: `sum by (action)(rate(jvm_gc_pause_seconds_count{instance=~"$node"}[$__rate_interval]))` short.
- Avg GC pause by action: `sum by (action)(rate(jvm_gc_pause_seconds_sum{instance=~"$node"}[$__rate_interval])) / clamp_min(sum by (action)(rate(jvm_gc_pause_seconds_count{instance=~"$node"}[$__rate_interval])), 1)` s.
- Allocation rate: `sum(rate(jvm_gc_memory_allocated_bytes_total{instance=~"$node"}[$__rate_interval]))` Bps.
- Promotion rate: `sum(rate(jvm_gc_memory_promoted_bytes_total{instance=~"$node"}[$__rate_interval]))` Bps.
- Live data size after GC: `jvm_gc_live_data_size_bytes{instance=~"$node"}` `{{instance}}` bytes.

Row **Threads** — timeseries (`h:8,w:12`):
- Threads live/daemon/peak by node: `jvm_threads_live_threads{instance=~"$node"}`, `jvm_threads_daemon_threads{instance=~"$node"}`, `jvm_threads_peak_threads{instance=~"$node"}`.
- Threads by state: `sum by (state)(jvm_threads_states_threads{instance=~"$node"})` `{{state}}`.

Row **CPU & OS** — timeseries (`h:8,w:12`):
- Process vs system CPU: `process_cpu_usage{instance=~"$node"}`, `system_cpu_usage{instance=~"$node"}` percentunit.
- System load 1m + CPU count: `system_load_average_1m{instance=~"$node"}`, `system_cpu_count{instance=~"$node"}`.
- File descriptors open vs max: `process_files_open_files{instance=~"$node"}`, `process_files_max_files{instance=~"$node"}` short.
- CPU time/s: `sum(rate(process_cpu_time_seconds_total{instance=~"$node"}[$__rate_interval]))` short.

- [ ] **Step 2: Validate JSON parses**

Run: `python3 -m json.tool deploy/grafana/dashboards/strata-jvm.json > /dev/null && echo OK`
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add deploy/grafana/dashboards/strata-jvm.json
git commit -m "Add Strata JVM & Runtime dashboard"
```

---

## Task 7: Remove old overview + fix compose comment

**Files:**
- Delete: `deploy/grafana/dashboards/strata-overview.json`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Delete the old dashboard**

```bash
git rm deploy/grafana/dashboards/strata-overview.json
```

- [ ] **Step 2: Update the Grafana comment**

In `docker-compose.yml`, change the grafana `ports` comment (currently `# open http://localhost:3000 -> "Strata — Cluster Overview"`) to:

```yaml
      - "3000:3000"   # open http://localhost:3000 -> Strata dashboards: Cluster / Node / JVM
```

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml deploy/grafana/dashboards/strata-overview.json
git commit -m "Replace single overview with Cluster/Node/JVM dashboards"
```

---

## Task 8: Verify dashboards (lint + provisioning load)

**Files:** none (verification only).

- [ ] **Step 1: JSON validity for all three**

Run:
```bash
for f in deploy/grafana/dashboards/strata-*.json; do
  python3 -m json.tool "$f" >/dev/null && echo "OK $f" || { echo "BAD $f"; exit 1; }
done
```
Expected: three `OK` lines.

- [ ] **Step 2: Metric/label lint (no typos, no metric outside the inventory)**

Run this script — it extracts every `strata_*`/`jvm_*`/`process_*`/`system_*` token from the dashboards and checks each against the verified inventory:

```bash
python3 - <<'PY'
import json, re, glob, sys
known = {
 "strata_chunks_unavailable","strata_chunks_under_replicated","strata_chunks_at_min_redundancy",
 "strata_repair_inflight","strata_repair_backlog","strata_nodes","strata_meta_is_leader",
 "strata_meta_zk_connected","strata_node_registered","strata_node_disk_used_bytes",
 "strata_node_capacity_bytes","strata_node_capacity_used_ratio","strata_node_chunks",
 "strata_node_groupcommit_force_total","strata_node_append_ops_total","strata_node_append_bytes_total",
 "strata_node_read_ops_total","strata_node_read_bytes_total",
 "strata_scp_request_duration_seconds_bucket","strata_scp_request_duration_seconds_count",
 "strata_scp_request_duration_seconds_sum",
 "jvm_memory_used_bytes","jvm_memory_committed_bytes","jvm_memory_max_bytes",
 "jvm_gc_pause_seconds_bucket","jvm_gc_pause_seconds_count","jvm_gc_pause_seconds_sum",
 "jvm_gc_memory_allocated_bytes_total","jvm_gc_memory_promoted_bytes_total",
 "jvm_gc_live_data_size_bytes","jvm_gc_max_data_size_bytes",
 "jvm_threads_live_threads","jvm_threads_daemon_threads","jvm_threads_peak_threads",
 "jvm_threads_states_threads","process_cpu_usage","system_cpu_usage","system_cpu_count",
 "system_load_average_1m","process_cpu_time_seconds_total","process_files_open_files",
 "process_files_max_files","process_uptime_seconds",
}
tok = re.compile(r'\b(?:strata|jvm|process|system)_[a-z0-9_]+\b')
bad = False
for f in glob.glob("deploy/grafana/dashboards/strata-*.json"):
    d = json.load(open(f))
    exprs = []
    def walk(o):
        if isinstance(o, dict):
            for k,v in o.items():
                if k in ("expr","query") and isinstance(v,str): exprs.append(v)
                elif k=="query" and isinstance(v,dict) and isinstance(v.get("query"),str): exprs.append(v["query"])
                else: walk(v)
        elif isinstance(o, list):
            for x in o: walk(x)
    walk(d)
    for e in exprs:
        for m in tok.findall(e):
            if m not in known:
                print(f"UNKNOWN METRIC {m} in {f}: {e}"); bad = True
print("METRIC LINT FAIL" if bad else "METRIC LINT OK")
sys.exit(1 if bad else 0)
PY
```
Expected: `METRIC LINT OK`. (If a name like `strata_scp_request_duration_bucket` — missing `_seconds` — slips in, this fails.)

- [ ] **Step 3: Provisioning load into a throwaway Grafana**

Run:
```bash
docker run --rm -d --name strata-graf-verify \
  -v "$PWD/deploy/grafana/provisioning:/etc/grafana/provisioning:ro" \
  -v "$PWD/deploy/grafana/dashboards:/var/lib/grafana/dashboards:ro" \
  -p 3001:3000 grafana/grafana:11.2.0 >/dev/null
# give provisioning time to run
for i in $(seq 1 20); do sleep 2; docker logs strata-graf-verify 2>&1 | grep -q "HTTP Server Listen" && break; done
echo "--- provisioning errors (should be none) ---"
docker logs strata-graf-verify 2>&1 | grep -iE "failed to load dashboard|provisioning error|invalid" || echo "none"
echo "--- dashboards registered ---"
docker logs strata-graf-verify 2>&1 | grep -iE "finished to provision dashboards|dashboard.*strata" || true
docker rm -f strata-graf-verify >/dev/null
```
Expected: the errors section prints `none`. (Datasource resolution is per-query, so an unreachable Prometheus does not block provisioning.)

- [ ] **Step 4 (optional full-stack smoke): confirm read metrics export**

Only if Docker + a built image are available:
```bash
./scripts/build-image.sh
docker compose up -d
sleep 30
docker compose run --rm --no-deps loadgen perf --meta node1:9100 --workload write --duration 20 || true
docker compose exec node1 sh -c 'wget -qO- localhost:9300/metrics | grep -E "strata_node_read_(bytes|ops)_total|strata_node_append_bytes_total"'
docker compose down
```
Expected: lines for `strata_node_read_bytes_total` and `strata_node_read_ops_total` appear.

- [ ] **Step 5: Commit (if any fixes were needed)**

```bash
git add deploy/grafana/dashboards/
git commit -m "Fix dashboard lint/provisioning issues" || echo "nothing to fix"
```

---

## Task 9: Update observability memory note

**Files:** none in-repo (updates the user's memory store).

- [ ] **Step 1: Refresh the memory note**

Update `observability.md` in the memory directory so it reflects three dashboards (Cluster / Node with `$node`+`$opcode` selectors / JVM) and the new `strata_node_read_bytes_total` + `strata_node_read_ops_total` counters. (Do this via the memory Write tool, not a repo commit.)

---

## Self-Review

**Spec coverage:**
- §4 read counters → Tasks 1–3 (ChunkStore counter + test, StorageNode accessors, ServerMetrics registration). ✅
- §6 Cluster dashboard → Task 4. ✅
- §7 Node dashboard (incl. `$node`/`$opcode`, heatmap) → Task 5. ✅
- §8 JVM dashboard → Task 6. ✅
- §9 cross-dashboard nav/drill-down → Conventions fragments, applied in Tasks 4–6. ✅
- §10 file changes (delete old, compose comment) → Task 7. ✅
- §11 verification (TDD, JSON lint, metric lint, provisioning load, build) → Tasks 1/3/8. ✅

**Placeholder scan:** code steps show full code; dashboard steps give exact PromQL + JSON fragment templates + sizes; verification gives runnable scripts with expected output. No TBD/TODO. ✅

**Type/name consistency:** `readOps()`/`readBytes()` used identically across ChunkStore (Task 1) → StorageNode (Task 2) → `StorageNode::readOps`/`::readBytes` (Task 3). Prometheus names `strata_node_read_ops_total`/`strata_node_read_bytes_total` match the metric lint allowlist (Task 8) and the dashboard queries (Tasks 4–5). uid `strata-overview` (Task 4) matches the deleted file's uid (Task 7) and the drill-down targets `strata-node`/`strata-jvm`. ✅
