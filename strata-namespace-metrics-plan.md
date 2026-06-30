# Per-namespace observability — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make namespace the primary axis of Strata metrics — per-namespace request latency, data throughput, namespace-log activity, and owner/switch tracking — plus a `strata-namespace` Grafana dashboard, with the existing dashboards migrated.

**Architecture:** Add a `namespace` tag to the existing `strata_scp_request_duration` timer (threaded through a tiny `RequestContext` ThreadLocal that handlers set and `ScpServer` reads after dispatch); add per-namespace `LongAdder`/`AtomicLong` counters in `ChunkStore` (data plane) and `NamespaceLogMetrics` (control plane), wired to Micrometer in `ServerMetrics` as lazily-registered per-namespace function-counters + MultiGauges. No Micrometer in `strata-format`/`-proto`/`-common`.

**Tech Stack:** Java 21, Micrometer + Prometheus, Netty (SCP transport), JUnit 5, Grafana (schemaVersion 39, Prometheus datasource uid `prometheus`).

Design doc: `strata-namespace-metrics-design.md` (§5 has the full dashboard panel/expr spec).

## Global Constraints

- Naming taxonomy: metrics `strata_controller_*` / `strata_data_node_*`; `strata_scp_request_duration` stays plane-neutral (`role` common tag separates planes).
- Micrometer appends `_total` (counters) and `_seconds_{bucket,count,sum}` (timers) at exposition — **code names omit those suffixes**.
- Clean breaks, no back-compat aliases (project never ships to prod): rename/replace in place, delete old names.
- No Micrometer dependency in `strata-format`, `strata-proto`, `strata-common` — plain `LongAdder`/`AtomicLong`, exposed via accessors; Micrometer wiring only in `strata-server`.
- `StrataNamespace.value()` returns the namespace `String`. The no-namespace sentinel tag value is `"-"`.
- Build: `mvn -q -pl <module> -am test` (use `-am` so upstream modules build — stale `.m2` proto trap per project memory). The Docker image is built from the worktree.

---

### Task 1: Request latency & rate by namespace

**Files:**
- Create: `strata-proto/src/main/java/io/strata/proto/RequestContext.java`
- Modify: `strata-proto/src/main/java/io/strata/proto/RequestObserver.java`
- Modify: `strata-proto/src/main/java/io/strata/proto/ScpServer.java:318-326` (observeRequest) and `:299-318` (handleRequest capture)
- Modify: `strata-meta/src/main/java/io/strata/meta/Controller.java:444` (requireNamespaceOwner)
- Modify: `strata-node/src/main/java/io/strata/node/DataNodeHandlers.java` (set namespace per op)
- Modify: `strata-server/src/main/java/io/strata/server/ServerMetrics.java:192-208` (requestObserver)
- Test: `strata-proto/src/test/java/io/strata/proto/RequestContextTest.java` (new)

**Interfaces:**
- Produces: `RequestContext.setNamespace(String ns)`, `RequestContext.takeNamespace()` → `String` (`"-"` when unset, clears on read). `RequestObserver.observe(String opcode, String namespace, long durationNanos, boolean success)`.
- Consumes: nothing from later tasks.

- [ ] **Step 1: Write the failing test** `RequestContextTest.java`

```java
package io.strata.proto;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RequestContextTest {
    @Test
    void takeReturnsSetValueThenClears() {
        RequestContext.setNamespace("orders");
        assertEquals("orders", RequestContext.takeNamespace());
        assertEquals("-", RequestContext.takeNamespace(), "take must clear; default is \"-\"");
    }

    @Test
    void takeDefaultsToDashWhenUnset() {
        assertEquals("-", RequestContext.takeNamespace());
    }
}
```

- [ ] **Step 2: Run it, verify it fails**

Run: `mvn -q -pl strata-proto -am test -Dtest=RequestContextTest`
Expected: FAIL — `RequestContext` does not exist.

- [ ] **Step 3: Create `RequestContext.java`**

```java
package io.strata.proto;

/**
 * Per-request namespace carrier from the SCP handler to {@link ScpServer}'s request observer, without a
 * metrics dependency in the transport. A handler sets the namespace synchronously while decoding the
 * request (the synchronous portion of {@code handleAsync} runs in-order on the single-threaded
 * per-connection executor); {@code ScpServer} reads it with {@link #takeNamespace()} right after dispatch
 * — valid for both sync and async (group-committed APPEND) paths, since the namespace is known at decode
 * time, not at completion time. {@code take} clears so a value never leaks to the next request.
 */
public final class RequestContext {
    private static final ThreadLocal<String> NAMESPACE = new ThreadLocal<>();

    private RequestContext() {
    }

    public static void setNamespace(String namespace) {
        NAMESPACE.set(namespace);
    }

    /** Returns the namespace set for the current request and clears it; {@code "-"} when unset. */
    public static String takeNamespace() {
        String ns = NAMESPACE.get();
        NAMESPACE.remove();
        return ns == null ? "-" : ns;
    }
}
```

- [ ] **Step 4: Run it, verify it passes**

Run: `mvn -q -pl strata-proto -am test -Dtest=RequestContextTest`
Expected: PASS.

- [ ] **Step 5: Add `namespace` to `RequestObserver.observe`**

In `RequestObserver.java`, change the method to:

```java
    void observe(String opcode, String namespace, long durationNanos, boolean success);
```

Update the javadoc `@param` list to add `@param namespace the request's namespace, or "-" for cluster-scope ops`.

- [ ] **Step 6: Thread namespace through `ScpServer`**

In `ScpServer.handleRequest`, after `respF = handler.handleAsync(req);` completes its synchronous portion (i.e. immediately after the try/catch that assigns `respF`, before the `if (respF.isDone()...)` branch), capture:

```java
            String ns = RequestContext.takeNamespace();
```

Change both `observeRequest(req, startNanos, ...)` calls to pass `ns`. Update `observeRequest` (currently `:318`):

```java
        private void observeRequest(Frame req, long startNanos, boolean success, String namespace) {
            RequestObserver obs = requestObserver;
            if (obs == null) {
                return;
            }
            Opcode op = Opcode.fromCode(req.opcode());
            obs.observe(op != null ? op.name() : "unknown", namespace, System.nanoTime() - startNanos, success);
        }
```

The async branch's `whenComplete` closure captures the local `ns` (effectively final). Add `import io.strata.proto.RequestContext;` is unnecessary (same package).

- [ ] **Step 7: Set namespace in the controller handler**

In `Controller.requireNamespaceOwner` (`:444`), add as the FIRST line (so every namespace-scoped opcode tags its namespace, including the system namespace early-return):

```java
        io.strata.proto.RequestContext.setNamespace(namespace.value());
```

- [ ] **Step 8: Set namespace in the data-node handlers**

In `DataNodeHandlers.java`: in `handleAsync` for APPEND, after `var m = Messages.Append.decode(req.headerSlice());` add `io.strata.proto.RequestContext.setNamespace(m.namespace().value());`. In `handle`'s switch, after each `var m = Messages.X.decode(h);` that has a `m.namespace()` (OPEN_CHUNK, READ, READ_RECOVERY, FENCE, STAT_CHUNK, SEAL_CHUNK, DELETE_CHUNKS, FETCH_CHUNK, READ_LEDGER, VERIFY_CHUNKS) add `io.strata.proto.RequestContext.setNamespace(m.namespace().value());`. PING and EXEC_REPLICATE have no namespace — leave unset (defaults to `"-"`).

- [ ] **Step 9: Tag the namespace in `ServerMetrics.requestObserver`**

Replace the lambda (`:192-208`) so the cache key and tag include namespace:

```java
    static RequestObserver requestObserver(MeterRegistry reg) {
        Map<String, Timer> timers = new ConcurrentHashMap<>();
        return (opcode, namespace, durationNanos, success) -> {
            String status = success ? "ok" : "error";
            timers.computeIfAbsent(opcode + ':' + status + ':' + namespace, k -> Timer.builder("strata_scp_request_duration")
                            .description("request handler latency by opcode + namespace (incl. async durability wait)")
                            .tag("opcode", opcode)
                            .tag("status", status)
                            .tag("namespace", namespace)
                            .serviceLevelObjectives(
                                    Duration.ofMillis(1), Duration.ofMillis(2), Duration.ofMillis(5),
                                    Duration.ofMillis(10), Duration.ofMillis(25), Duration.ofMillis(50),
                                    Duration.ofMillis(100), Duration.ofMillis(250), Duration.ofMillis(500),
                                    Duration.ofSeconds(1), Duration.ofMillis(2500), Duration.ofSeconds(5))
                            .register(reg))
                    .record(durationNanos, TimeUnit.NANOSECONDS);
        };
    }
```

- [ ] **Step 10: Add a `ServerMetrics` test for the namespace tag**

Append to `ServerMetricsTest.java`:

```java
    @Test
    void requestObserverTagsNamespace() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        var obs = ServerMetrics.requestObserver(registry);
        obs.observe("READ", "orders", 1_000_000L, true);
        assertNotNull(registry.find("strata_scp_request_duration").tag("namespace", "orders")
                .tag("opcode", "READ").tag("status", "ok").timer(),
                "request timer must carry a namespace tag");
    }
```

(Add `import io.strata.proto.RequestObserver;` if needed.)

- [ ] **Step 11: Build the affected modules + run tests**

Run: `mvn -q -pl strata-proto,strata-node,strata-meta,strata-server -am test`
Expected: PASS (all existing `RequestObserver` callers updated; no other implementors exist).

- [ ] **Step 12: Commit**

```bash
git add strata-proto strata-meta/src/main/java/io/strata/meta/Controller.java strata-node/src/main/java/io/strata/node/DataNodeHandlers.java strata-server/src/main/java/io/strata/server/ServerMetrics.java strata-server/src/test/java/io/strata/server/ServerMetricsTest.java
git commit -m "feat(metrics): tag scp_request_duration by namespace via RequestContext seam"
```

---

### Task 2: Per-namespace data throughput (data plane)

**Files:**
- Modify: `strata-format/src/main/java/io/strata/format/ChunkStore.java` (counters at `:67-73`, `:801-802`, `:919-920`; accessors `:223-239`)
- Modify: `strata-node/src/main/java/io/strata/node/DataNode.java:176-189`
- Modify: `strata-server/src/main/java/io/strata/server/ServerMetrics.java` (registerDataNode `:137-180`)
- Test: `strata-format/src/test/java/io/strata/format/ChunkStoreTest.java` (extend), `ServerMetricsTest.java` (extend)

**Interfaces:**
- Produces: `ChunkStore.namespaceIoStats()` → `Map<String, long[]>` keyed by `namespace.value()`, value `[appendOps, appendBytes, readOps, readBytes]`. `DataNode.namespaceIoStats()` delegates. Series `strata_data_node_{append,read}_{ops,bytes}_total{namespace}`.
- Consumes: nothing.

- [ ] **Step 1: Write the failing ChunkStore test**

Add to `ChunkStoreTest.java` (follow the file's existing open/append helpers; namespace `StrataNamespace.of("ns1")`):

```java
    @Test
    void perNamespaceIoCountersAccumulate() throws Exception {
        // open a chunk in ns1, append, read; assert namespaceIoStats reflects ns1
        // (use the test's existing helpers to open + append a known byte count, then readRegion)
        var stats = store.namespaceIoStats().get("ns1");
        assertNotNull(stats, "ns1 must appear after I/O");
        assertTrue(stats[0] >= 1, "appendOps");
        assertTrue(stats[1] >= 1, "appendBytes");
    }
```

(Mirror the existing append/read test in the file for the setup lines.)

- [ ] **Step 2: Run it, verify it fails**

Run: `mvn -q -pl strata-format -am test -Dtest=ChunkStoreTest#perNamespaceIoCountersAccumulate`
Expected: FAIL — `namespaceIoStats` undefined.

- [ ] **Step 3: Add per-namespace counters to `ChunkStore`**

Near the existing `appendOps`/`appendBytes`/`readOps`/`readBytes` fields (`:67-73`), add:

```java
    // Per-namespace I/O for the namespace dashboard. ConcurrentHashMap of plain LongAdder quads — the
    // data-path cost is one map lookup + adder increment; the global accessors above remain the cluster
    // rollup but are no longer exported (the namespace dashboard's fleet view is sum without(namespace)).
    private final java.util.concurrent.ConcurrentHashMap<String, long[]> nsIo =
            new java.util.concurrent.ConcurrentHashMap<>();

    private long[] nsIoFor(StrataNamespace ns) {
        return nsIo.computeIfAbsent(ns.value(), k -> new long[4]);
    }
```

Wait — `long[]` is not safe to mutate concurrently. Use `java.util.concurrent.atomic.LongAdder[]`:

```java
    private final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.LongAdder[]> nsIo =
            new java.util.concurrent.ConcurrentHashMap<>();

    private java.util.concurrent.atomic.LongAdder[] nsIoFor(StrataNamespace ns) {
        return nsIo.computeIfAbsent(ns.value(), k -> new java.util.concurrent.atomic.LongAdder[]{
                new java.util.concurrent.atomic.LongAdder(), new java.util.concurrent.atomic.LongAdder(),
                new java.util.concurrent.atomic.LongAdder(), new java.util.concurrent.atomic.LongAdder()});
    }

    /** Per-namespace [appendOps, appendBytes, readOps, readBytes] snapshot for the namespace dashboard. */
    public java.util.Map<String, long[]> namespaceIoStats() {
        java.util.Map<String, long[]> out = new java.util.HashMap<>(nsIo.size());
        nsIo.forEach((ns, a) -> out.put(ns, new long[]{a[0].sum(), a[1].sum(), a[2].sum(), a[3].sum()}));
        return out;
    }
```

In `appendAsync` at `:801-802` (after the existing `appendOps`/`appendBytes` increments), add:

```java
            var a = nsIoFor(ns);
            a[0].increment();
            a[1].add(len);
```

In `readRegion(... )` at `:919-920` (after the existing `readOps`/`readBytes` increments), add:

```java
                var a = nsIoFor(ns);
                a[2].increment();
                a[3].add(n);
```

- [ ] **Step 4: Run the ChunkStore test, verify it passes**

Run: `mvn -q -pl strata-format -am test -Dtest=ChunkStoreTest#perNamespaceIoCountersAccumulate`
Expected: PASS.

- [ ] **Step 5: Expose on `DataNode`**

In `DataNode.java`, add after the existing io accessors (`:189`):

```java
    public java.util.Map<String, long[]> namespaceIoStats() {
        return store.namespaceIoStats();
    }
```

- [ ] **Step 6: Register per-namespace data-io meters; drop the global io counters**

In `ServerMetrics.registerDataNode`, REMOVE the four global `FunctionCounter.builder("strata_data_node_append_ops"/"_append_bytes"/"_read_ops"/"_read_bytes", ...)` registrations (`:156-163`). Add a lazily-refreshing per-namespace registration (extract a shared helper so Task 3/4 reuse it):

```java
    // Lazily registers per-namespace function-counters as namespaces first appear in `stats`. A counter is
    // never deregistered when its namespace goes idle (a counter must not vanish); the value freezes and
    // resumes — correct cumulative semantics, cardinality bounded by namespaces seen on this instance.
    private static void refreshNsCounters(MeterRegistry reg, java.util.Set<String> seen,
            java.util.Map<String, long[]> stats, String[] names) {
        for (var e : stats.entrySet()) {
            String ns = e.getKey();
            for (int i = 0; i < names.length; i++) {
                String key = names[i] + '|' + ns;
                if (seen.add(key)) {
                    final int idx = i;
                    FunctionCounter.builder(names[idx], ns, n -> {
                        long[] v = stats.get(n);   // note: stats is re-read live below; see scheduler
                        return v == null ? 0.0 : v[idx];
                    }).tag("namespace", ns).register(reg);
                }
            }
        }
    }
```

> The `FunctionCounter` must read a LIVE value each scrape, not the snapshot. Bind it to the source instead: register with a supplier that calls `node.namespaceIoStats().getOrDefault(ns, EMPTY)[idx]`. Implement as:

```java
    private static final long[] EMPTY4 = new long[4];

    private static void registerDataNodeNsCounter(MeterRegistry reg, DataNode n, String ns, String name, int idx) {
        FunctionCounter.builder(name, n, x -> x.namespaceIoStats().getOrDefault(ns, EMPTY4)[idx])
                .tag("namespace", ns).register(reg);
    }
```

Drive registration from the same daemon scheduler `registerPerNamespace` already runs (Task 3 consolidates this). For Task 2 standalone, add a small scheduler in `registerDataNode`:

```java
        var seen = java.util.concurrent.ConcurrentHashMap.<String>newKeySet();
        var refresh = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "data-node-ns-metrics"); t.setDaemon(true); return t;
        });
        String[] names = {"strata_data_node_append_ops", "strata_data_node_append_bytes",
                "strata_data_node_read_ops", "strata_data_node_read_bytes"};
        refresh.scheduleAtFixedRate(() -> {
            for (String ns : n.namespaceIoStats().keySet()) {
                for (int i = 0; i < names.length; i++) {
                    if (seen.add(names[i] + '|' + ns)) registerDataNodeNsCounter(reg, n, ns, names[i], i);
                }
            }
        }, 0, 10, TimeUnit.SECONDS);
```

- [ ] **Step 7: Update `ServerMetricsTest` for per-namespace io**

Replace `registerNodeExposesReadCounterMeters` with a version that drives I/O then forces a refresh. Since the scheduler is internal, expose a package-private `static void refreshDataNodeNsCounters(...)` OR test through a real DataNode that has served I/O and `await` up to ~11s. Prefer extracting the refresh body into a package-private method `boolean registerNewNamespaces(...)` callable from the test:

```java
    @Test
    void registerNodeExposesPerNamespaceIoCounters() throws Exception {
        try (DataNode node = new DataNode(DataNodeConfig.standalone(dir))) {
            // drive an append+read in namespace "ns1" via the node's store (use existing test helpers),
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            ServerMetrics.registerDataNode(registry, node);
            ServerMetrics.registerNewDataNodeNamespaces(registry, node); // package-private, idempotent
            assertNotNull(registry.find("strata_data_node_append_bytes").tag("namespace", "ns1").functionCounter());
        }
    }
```

Refactor Step 6's scheduler body into `static void registerNewDataNodeNamespaces(MeterRegistry reg, DataNode n)` (tracking `seen` in a field-less way via a registry lookup: skip if `reg.find(name).tag("namespace", ns).functionCounter() != null`). This removes the need for a `seen` set and is naturally idempotent:

```java
    static void registerNewDataNodeNamespaces(MeterRegistry reg, DataNode n) {
        String[] names = {"strata_data_node_append_ops", "strata_data_node_append_bytes",
                "strata_data_node_read_ops", "strata_data_node_read_bytes"};
        for (String ns : n.namespaceIoStats().keySet()) {
            for (int i = 0; i < names.length; i++) {
                if (reg.find(names[i]).tag("namespace", ns).functionCounter() == null) {
                    registerDataNodeNsCounter(reg, n, ns, names[i], i);
                }
            }
        }
    }
```

The scheduler in `registerDataNode` just calls `registerNewDataNodeNamespaces(reg, n)` each tick.

- [ ] **Step 8: Run tests**

Run: `mvn -q -pl strata-format,strata-node,strata-server -am test`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add strata-format strata-node/src/main/java/io/strata/node/DataNode.java strata-server/src/main/java/io/strata/server/ServerMetrics.java strata-server/src/test/java/io/strata/server/ServerMetricsTest.java
git commit -m "feat(metrics): per-namespace data-node throughput (append/read ops+bytes)"
```

---

### Task 3: Per-namespace namespace-log metrics

**Files:**
- Modify: `strata-meta/.../NamespaceLogMetrics.java` (per-namespace keyed; new methods)
- Modify: `strata-meta/.../NamespaceMetadataLogRepository.java` (`:67`, `:131`, `:201`, `:260` recordLogRead; thread `namespace`)
- Modify: `strata-meta/.../NamespaceMetadataRecovery.java` (`Recovered` gains `recordsReplayed`,`bytesRead`)
- Modify: `strata-meta/.../NamespaceLogBackend.java:352` (recordReacquire(ns))
- Modify: `strata-meta/.../Controller.java` (`:313-335` replace global accessors with `namespaceLogStats()`)
- Modify: `strata-server/.../ServerMetrics.java` (registerController: drop global `strata_controller_log_*`, register per-namespace `strata_controller_namespace_log_*`)
- Test: `strata-meta/.../NamespaceLogMetricsTest.java` (extend), `ServerMetricsTest`/controller metrics

**Interfaces:**
- Produces: `NamespaceLogMetrics.recordAppend(StrataNamespace,long)`, `recordLogRead(StrataNamespace,long records,long bytes)`, `recordCompaction(StrataNamespace)`, `recordRecovery(StrataNamespace)`, `recordReacquire(StrataNamespace)`, `recordOwnerAcquired(StrataNamespace)` (Task 4); snapshot `Map<String,long[]> stats()` → per-ns `[appendRecords, appendBytes, readRecords, readBytes, compactions, recoveries, reacquisitions, ownerChanges]`. `Controller.namespaceLogStats()` → same map. Series `strata_controller_namespace_log_{append_records,append_bytes,read_records,read_bytes,compactions,recoveries,reacquisitions}_total{namespace}`.
- Consumes: nothing.

- [ ] **Step 1: Write the failing `NamespaceLogMetrics` test**

Extend `NamespaceLogMetricsTest.java`:

```java
    @Test
    void countersAreKeyedByNamespace() {
        NamespaceLogMetrics m = new NamespaceLogMetrics();
        StrataNamespace a = StrataNamespace.of("a"), b = StrataNamespace.of("b");
        m.recordAppend(a, 100);
        m.recordAppend(a, 50);
        m.recordCompaction(b);
        long[] sa = m.stats().get("a");
        long[] sb = m.stats().get("b");
        assertEquals(2, sa[0], "a appendRecords");
        assertEquals(150, sa[1], "a appendBytes");
        assertEquals(1, sb[4], "b compactions");
    }
```

- [ ] **Step 2: Run it, verify it fails** — `mvn -q -pl strata-meta -am test -Dtest=NamespaceLogMetricsTest#countersAreKeyedByNamespace` → FAIL.

- [ ] **Step 3: Rewrite `NamespaceLogMetrics` per-namespace**

```java
package io.strata.meta;

import io.strata.common.StrataNamespace;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Per-namespace counters for the namespace-log metadata backend, surfaced through {@code Controller} /
 * {@code ServerMetrics}. Lock-free {@link LongAdder}s held on the BACKEND (not per-repository) so they
 * survive a namespace's repository being rebuilt on failover/restart. Index order in {@link #stats()}:
 * 0 appendRecords, 1 appendBytes, 2 readRecords, 3 readBytes, 4 compactions, 5 recoveries,
 * 6 reacquisitions, 7 ownerChanges.
 */
final class NamespaceLogMetrics {
    private static final int N = 8;
    private final ConcurrentHashMap<String, LongAdder[]> byNs = new ConcurrentHashMap<>();

    private LongAdder[] of(StrataNamespace ns) {
        return byNs.computeIfAbsent(ns.value(), k -> {
            LongAdder[] a = new LongAdder[N];
            for (int i = 0; i < N; i++) a[i] = new LongAdder();
            return a;
        });
    }

    void recordAppend(StrataNamespace ns, long bytes) { var a = of(ns); a[0].increment(); a[1].add(bytes); }
    void recordLogRead(StrataNamespace ns, long records, long bytes) { var a = of(ns); a[2].add(records); a[3].add(bytes); }
    void recordCompaction(StrataNamespace ns) { of(ns)[4].increment(); }
    void recordRecovery(StrataNamespace ns) { of(ns)[5].increment(); }
    void recordReacquire(StrataNamespace ns) { of(ns)[6].increment(); }
    void recordOwnerAcquired(StrataNamespace ns) { of(ns)[7].increment(); }

    /** Per-namespace snapshot: namespace -> [appendRecords, appendBytes, readRecords, readBytes,
     *  compactions, recoveries, reacquisitions, ownerChanges]. */
    Map<String, long[]> stats() {
        Map<String, long[]> out = new HashMap<>(byNs.size());
        byNs.forEach((ns, a) -> {
            long[] v = new long[N];
            for (int i = 0; i < N; i++) v[i] = a[i].sum();
            out.put(ns, v);
        });
        return out;
    }
}
```

- [ ] **Step 4: Run it, verify it passes** — same command → PASS.

- [ ] **Step 5: Thread `namespace` through the record sites + add read-log counting**

`NamespaceMetadataRecovery.java`: change `record Recovered(NamespaceMetadataState state, long durableEndOffset)` to `record Recovered(NamespaceMetadataState state, long durableEndOffset, long recordsReplayed, long bytesRead)`. In `recover(...)`, accumulate the open-log replay: capture `byte[] logBytes = fileStore.readLog(m.logFileId().get());` into a variable, replay from `MetadataLogSegmentCodec.recoverPrefix(logBytes)`, and return `new Recovered(state, durableEndOffset, prefix.records().size(), logBytes.length)` (0/0 when there is no open log to read). Update all other `new Recovered(...)` call sites in that file accordingly.

`NamespaceMetadataLogRepository.java`:
- `:131` → `metrics.recordAppend(namespace, frame.length);`
- `:201` → `metrics.recordCompaction(namespace);`
- `:67` (in `open`) → `metrics.recordRecovery(namespace);`
- In `recoverAndRepublish` (`:260`), after `NamespaceMetadataRecovery.Recovered recovered = ...`, add:
  `metrics.recordLogRead(namespace, recovered.recordsReplayed(), recovered.bytesRead());`

`NamespaceLogBackend.java:352` → `metrics.recordReacquire(namespace);`

- [ ] **Step 6: Replace `Controller`'s global accessors**

In `Controller.java`, delete `metadataLogAppendRecords/Bytes/Compactions/Recoveries/Reacquisitions()` (`:313-335`) and add:

```java
    /** Per-namespace namespace-log counters for owned namespaces; empty under the ZK backend. */
    public java.util.Map<String, long[]> namespaceLogStats() {
        return store instanceof NamespaceLogMetadataStore log ? log.metrics().stats() : java.util.Map.of();
    }
```

Expose `stats()` up the chain: `NamespaceLogMetadataStore.metrics()` already returns the `NamespaceLogMetrics` (package-private) — `Controller` is in the same package, so `log.metrics().stats()` compiles.

- [ ] **Step 7: Swap the global log counters for per-namespace in `ServerMetrics.registerController`**

Delete the five `FunctionCounter.builder("strata_controller_log_*", ...)` registrations (`:94-103`). In `registerPerNamespace`'s scheduler (`:125-133`), after the existing files/logBytes MultiGauge refresh, add per-namespace log counter registration (idempotent, like Task 2):

```java
        String[] logNames = {"strata_controller_namespace_log_append_records",
                "strata_controller_namespace_log_append_bytes", "strata_controller_namespace_log_read_records",
                "strata_controller_namespace_log_read_bytes", "strata_controller_namespace_log_compactions",
                "strata_controller_namespace_log_recoveries", "strata_controller_namespace_log_reacquisitions"};
        // inside the scheduled lambda:
        for (String ns : s.namespaceLogStats().keySet()) {
            for (int i = 0; i < logNames.length; i++) {
                final int idx = i;
                if (reg.find(logNames[i]).tag("namespace", ns).functionCounter() == null) {
                    FunctionCounter.builder(logNames[i], s, m -> m.namespaceLogStats().getOrDefault(ns, EMPTY8)[idx])
                            .tag("namespace", ns).register(reg);
                }
            }
        }
```

Add `private static final long[] EMPTY8 = new long[8];` to `ServerMetrics`.

- [ ] **Step 8: Test the controller per-namespace log registration**

Add to a controller-facing metrics test (or `ServerMetricsTest` with a namespace-log-backed Controller fixture if one exists; otherwise assert at the `NamespaceLogMetrics`/`Controller.namespaceLogStats()` level):

```java
    @Test
    void controllerExposesPerNamespaceLogStats() throws Exception {
        // with a namespace-log Controller that has appended metadata in "ns1",
        // assertTrue(controller.namespaceLogStats().get("ns1")[0] >= 1);
    }
```

- [ ] **Step 9: Run the meta + server suites**

Run: `mvn -q -pl strata-meta,strata-server -am test`
Expected: PASS (no remaining references to the deleted `metadataLog*` accessors or `strata_controller_log_*` builders).

- [ ] **Step 10: Commit**

```bash
git add strata-meta strata-server/src/main/java/io/strata/server/ServerMetrics.java strata-server/src/test/java/io/strata/server/ServerMetricsTest.java
git commit -m "feat(metrics): per-namespace namespace-log counters (write/read/compaction/recovery/reacquire)"
```

---

### Task 4: Namespace owner gauge + change counter

**Files:**
- Modify: `strata-meta/.../NamespaceLogBackend.java:310-314` (recordOwnerAcquired on cold acquisition)
- Modify: `strata-meta/.../Controller.java` (expose `localControllerEndpoint()`)
- Modify: `strata-server/.../ServerMetrics.java` (registerPerNamespace: owner MultiGauge + owner_changes FunctionCounter)
- Test: `strata-meta/.../NamespaceLogBackendTest` or `NamespaceLogMetricsTest`; `ServerMetricsTest`

**Interfaces:**
- Produces: `strata_controller_namespace_owner{namespace,owner} = 1` (owner-only MultiGauge), `strata_controller_namespace_owner_changes_total{namespace}`. `Controller.localControllerEndpoint()` → `String`.
- Consumes: `NamespaceLogMetrics.recordOwnerAcquired` (Task 3), `Controller.namespaceStats()` keyset, `Controller.namespaceLogStats()` (Task 3).

- [ ] **Step 1: Write the failing acquisition test**

In `NamespaceLogMetricsTest` (or a backend test that opens a repo): assert that opening a repository for a namespace bumps `ownerChanges` (index 7) exactly once, and a second op on the same repo does not.

```java
    @Test
    void coldAcquisitionCountsOwnerChangeOnce() {
        NamespaceLogMetrics m = new NamespaceLogMetrics();
        StrataNamespace a = StrataNamespace.of("a");
        m.recordOwnerAcquired(a);
        assertEquals(1, m.stats().get("a")[7]);
    }
```

(An integration-level assertion through `NamespaceLogBackend.repo()` is added in Step 4's test where a backend fixture exists.)

- [ ] **Step 2: Run it, verify it fails / passes** — `recordOwnerAcquired` exists from Task 3, so this unit passes; the behavioral wiring is the real change. Proceed.

- [ ] **Step 3: Increment on cold acquisition in `NamespaceLogBackend.repo()`**

Inside the `if (r == null)` block (`:310-314`), after `repos.put(namespace, r);` add:

```java
                metrics.recordOwnerAcquired(namespace);   // cold acquisition = ownership handoff to this node
```

(The `reacquire()` path is deliberately NOT counted here — it bumps `recordReacquire` only.)

- [ ] **Step 4: Expose the local controller endpoint**

In `Controller.java`, add:

```java
    /** This controller's rendezvous endpoint identity — the owner label for the namespace-owner gauge. */
    public String localControllerEndpoint() {
        return ownership.localEndpoint();
    }
```

- [ ] **Step 5: Register the owner MultiGauge + change counter in `ServerMetrics.registerPerNamespace`**

Add a `MultiGauge owner = MultiGauge.builder("strata_controller_namespace_owner").description("current owner (=1) of each namespace this controller owns").register(reg);` and, in the scheduled lambda, after computing `stats = s.namespaceStats();`:

```java
            String self = s.localControllerEndpoint();
            owner.register(stats.keySet().stream()
                    .map(ns -> MultiGauge.Row.of(Tags.of("namespace", ns, "owner", self), 1))
                    .collect(java.util.stream.Collectors.toList()), true);
```

And register `strata_controller_namespace_owner_changes` as one more per-namespace FunctionCounter (idempotent), sourced from `namespaceLogStats()[7]`:

```java
            for (String ns : s.namespaceLogStats().keySet()) {
                if (reg.find("strata_controller_namespace_owner_changes").tag("namespace", ns).functionCounter() == null) {
                    FunctionCounter.builder("strata_controller_namespace_owner_changes", s,
                                    m -> m.namespaceLogStats().getOrDefault(ns, EMPTY8)[7])
                            .tag("namespace", ns).register(reg);
                }
            }
```

- [ ] **Step 6: Test the owner gauge emits only for owned namespaces**

```java
    @Test
    void ownerGaugeLabelsNamespaceAndOwner() {
        // with a Controller owning "ns1" at endpoint "ctrl-1:9301":
        // SimpleMeterRegistry r = ...; ServerMetrics.registerController(r, controller);
        // (trigger the refresh) assertEquals(1.0, r.find("strata_controller_namespace_owner")
        //     .tag("namespace","ns1").tag("owner","ctrl-1:9301").gauge().value());
    }
```

- [ ] **Step 7: Run the suites** — `mvn -q -pl strata-meta,strata-server -am test` → PASS.

- [ ] **Step 8: Commit**

```bash
git add strata-meta strata-server/src/main/java/io/strata/server/ServerMetrics.java strata-server/src/test/java/io/strata/server/ServerMetricsTest.java
git commit -m "feat(metrics): namespace owner gauge + owner-change counter"
```

---

### Task 5: New `strata-namespace.json` dashboard

**Files:**
- Create: `deploy/grafana/dashboards/strata-namespace.json`
- Test: `deploy/grafana/dashboards/` JSON-parse guard (Task 7)

**Interfaces:** consumes all metrics from Tasks 1–4. No code interfaces.

- [ ] **Step 1: Author the dashboard** per design §5.1 and the audit's authoring reference: uid `strata-namespace`, title `Strata — Namespace`, `tags:["strata"]`, schemaVersion 39, datasource `{"type":"prometheus","uid":"prometheus"}` on every panel, refresh `30s`, time `now-1h`, the "Strata dashboards" dropdown link, and the `namespace` template var:

```json
{ "name": "namespace", "type": "query", "datasource": {"type":"prometheus","uid":"prometheus"},
  "query": "label_values(strata_controller_namespace_files, namespace)",
  "refresh": 2, "includeAll": true, "multi": true,
  "current": {"text":"All","value":"$__all"}, "sort": 1, "label": "namespace" }
```

Rows + key panel exprs (all filtered `namespace=~"$namespace"`):
- **Ownership** — table `strata_controller_namespace_owner` (show `namespace`,`owner`); state-timeline `strata_controller_namespace_owner`; `rate(strata_controller_namespace_owner_changes_total[$__rate_interval])`; `strata_controller_namespaces_loaded`.
- **Throughput** — `sum by (namespace)(rate(strata_data_node_append_bytes_total{namespace=~"$namespace"}[$__rate_interval]))` (+ read, + ops), unit `Bps`/`ops`, stacked, fillOpacity 20.
- **Latency** — `histogram_quantile(0.99, sum by (le,namespace)(rate(strata_scp_request_duration_seconds_bucket{namespace=~"$namespace",opcode=~"APPEND|READ"}[$__rate_interval])))`; controller op set `opcode=~"CREATE_FILE|CREATE_CHUNK|LOOKUP_FILE|LOOKUP_PATH|SEAL_FILE|SEAL_CHUNK_META|ABORT_CHUNK_META|ALLOCATE_WRITER_EPOCH"`. p50/p95/p99 lines, unit `s`, fillOpacity 10.
- **Controller requests** — `sum by (opcode)(rate(strata_scp_request_duration_seconds_count{namespace=~"$namespace"}[$__rate_interval]))` and the `status="error"` variant, unit `ops`.
- **Namespace log** — `sum by (namespace)(rate(strata_controller_namespace_log_append_records_total{namespace=~"$namespace"}[$__rate_interval]))` and `_append_bytes` (write log), `_read_records`/`_read_bytes` (read log), `_compactions`, `_recoveries`, `_reacquisitions`; plus `strata_controller_namespace_files{namespace=~"$namespace"}` and `strata_controller_namespace_log_bytes{namespace=~"$namespace"}`.

Panel scaffolds (stat/timeseries fieldConfig+options) per design §5.1 conventions: stat `w4 h4`, timeseries `w12 h8` pairs, legend table `["last","max","mean"]`, tooltip multi/desc.

- [ ] **Step 2: Validate JSON parses**

Run: `python3 -c "import json,sys; json.load(open('deploy/grafana/dashboards/strata-namespace.json')); print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add deploy/grafana/dashboards/strata-namespace.json
git commit -m "feat(dashboards): add Strata — Namespace dashboard"
```

---

### Task 6: Migrate existing dashboards

**Files:** Modify `deploy/grafana/dashboards/strata-cluster.json`, `strata-controller.json`, `strata-node.json` — exact panel rewrites from design §5.2 / the audit impact list.

- [ ] **Step 1: Apply the rewrites** (each is a single `expr` string swap):

`strata-cluster.json`
- *Metadata ops/s* → `sum(rate(strata_controller_namespace_log_append_records_total[$__rate_interval]))`
- *Cluster write vs read throughput* (2 targets) → `sum without(namespace)(rate(strata_data_node_append_bytes_total[$__rate_interval]))` and `…read_bytes_total…`

`strata-controller.json`
- *Metadata mutation rate* → `sum without(namespace)(rate(strata_controller_namespace_log_append_records_total{instance=~"$instance"}[$__rate_interval]))`
- *Metadata-log write throughput* → `…namespace_log_append_bytes_total…`
- *Compaction & recovery rate* (2 targets) → `…namespace_log_compactions_total…` / `…namespace_log_recoveries_total…`, each `sum without(namespace)`

`strata-node.json`
- *Write throughput*, *Write vs read throughput* (2), *Write & read ops/s* (2) → wrap each in `sum without(namespace)(...)` (metric names unchanged).

> Request-rate / p99-by-opcode panels on cluster + node are UNCHANGED (they group by opcode; the namespace label sums away).

- [ ] **Step 2: Validate all dashboards parse**

Run: `for f in deploy/grafana/dashboards/*.json; do python3 -c "import json,sys; json.load(open('$f'))" || echo "BAD $f"; done; echo done`
Expected: `done` with no `BAD`.

- [ ] **Step 3: Commit**

```bash
git add deploy/grafana/dashboards/strata-cluster.json deploy/grafana/dashboards/strata-controller.json deploy/grafana/dashboards/strata-node.json
git commit -m "fix(dashboards): migrate panels to per-namespace metric names + aggregate away namespace"
```

---

### Task 7: Guard test — no stale metric references

**Files:** Create `strata-server/src/test/java/io/strata/server/DashboardMetricsGuardTest.java` (or a shell check in CI).

- [ ] **Step 1: Write the guard test**

```java
package io.strata.server;

import org.junit.jupiter.api.Test;
import java.nio.file.*;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertFalse;

class DashboardMetricsGuardTest {
    @Test
    void noDashboardReferencesRemovedMetrics() throws Exception {
        Path dir = Path.of(System.getProperty("user.dir")).resolve("../deploy/grafana/dashboards").normalize();
        List<String> removed = List.of("strata_controller_log_append_records_total",
                "strata_controller_log_append_bytes_total", "strata_controller_log_compactions_total",
                "strata_controller_log_recoveries_total", "strata_controller_log_reacquisitions_total");
        try (var files = Files.newDirectoryStream(dir, "*.json")) {
            for (Path f : files) {
                String body = Files.readString(f);
                for (String m : removed) {
                    assertFalse(body.contains(m), f.getFileName() + " still references removed metric " + m);
                }
            }
        }
    }
}
```

> Resolve the dashboards path relative to the module's working dir; adjust the `../` prefix to the repo layout if the test runs from a different CWD. If a path-based test is brittle in CI, implement this as a `scripts/check-dashboards.sh` grep gate instead.

- [ ] **Step 2: Run it** — `mvn -q -pl strata-server -am test -Dtest=DashboardMetricsGuardTest` → PASS.

- [ ] **Step 3: Full build + Docker image rebuild sanity**

Run: `mvn -q -T1C install -DskipTests=false` (full suite), then rebuild the image per the project's build-image script before any docker-compose run.

- [ ] **Step 4: Commit**

```bash
git add strata-server/src/test/java/io/strata/server/DashboardMetricsGuardTest.java
git commit -m "test(dashboards): guard against references to removed global log metrics"
```

---

## Self-Review

**Spec coverage:** Controller request rate/latency by namespace → T1. Data throughput by namespace → T2. Namespace-log write/read/compaction/recovery/reacquire by namespace → T3. Owner mapping + switch → T4. New dashboard → T5. Existing-dashboard migration → T6 + guard T7. All design §3/§5 items map to a task.

**Placeholders:** Dashboard JSON (T5) is specified by conventions + exact exprs rather than 900 literal lines — acceptable for a config artifact; every expr is given. T2 Step 3 corrects the unsafe `long[]` to `LongAdder[]` inline.

**Type consistency:** `stats()`/`namespaceLogStats()` index order `[appendRecords, appendBytes, readRecords, readBytes, compactions, recoveries, reacquisitions, ownerChanges]` used identically in T3 (registration idx) and T4 (idx 7). `namespaceIoStats()` `[appendOps, appendBytes, readOps, readBytes]` consistent T2. `observe(opcode, namespace, durationNanos, success)` consistent across T1.

**Risks:** lazy per-namespace registration means a metric is absent until its namespace sees traffic — fleet panels show "No data" until then (expected); tests force a refresh. `$namespace` sources from the pre-existing `strata_controller_namespace_files` so the selector populates regardless of the new counters.
