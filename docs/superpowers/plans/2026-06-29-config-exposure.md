# Expose Hardcoded Tuning Knobs as Configuration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose ~28 hardcoded operational tuning knobs (timeouts, intervals, buffer/batch sizes, watermarks, thread counts) as configuration — config-record fields wired to `STRATA_*` env in `StrataServer`, a new `strata-format` `ChunkStoreConfig`, and localized `strata-proto`/`strata-common` env reads — replacing all literals and `-D` system-property lookups in place.

**Architecture:** Four tiers by how config reaches the site. (A) Domain records `ControllerConfig`/`DataNodeConfig`/`ClientConfig` gain fields, consumed by classes that already hold the record. (B) A new immutable `ChunkStoreConfig` record in `strata-format` carries format-layer knobs, built by `DataNode` and passed into `ChunkStore`→`GroupCommitter`. (C) `strata-proto`/`strata-common` transport/codec statics read `STRATA_*` env at init via a new `EnvConfig` helper. (D) `strata-server` `ServerMetrics` reads env directly.

**Tech Stack:** Java 21 records, JUnit 5 (`org.junit.jupiter`), Netty (proto), Curator (ZK), Micrometer (metrics), Maven multi-module.

## Global Constraints

- **No backward compat / no prod:** replace literals and `-D` lookups in place; delete the old name; NO env→`-D`→default fallback chains, NO aliases. (Project never ships to prod.)
- **Defaults reproduce current behavior exactly:** an unset environment is a no-op change. Every default below equals today's literal.
- **Env naming follows the nearest sibling:** repair/verify cadences bare `STRATA_VERIFY_*`; controller log/ZK/lifecycle `STRATA_CONTROLLER_*`; client `STRATA_CLIENT_*`; metrics `STRATA_METRICS_*`; transport/codec `STRATA_SCP_*`.
- **Validation style:** `IllegalArgumentException` with `<field> must be ...` messages, matching existing records. Positive (`> 0`) for sizes/intervals/timeouts; `>= 0` for retry counts.
- **Record churn pattern:** new numeric fields are appended to the canonical record component list; the compact constructor normalizes a non-positive sentinel (`<= 0`, or `null` for the nested record) to the production default — matching the existing `controllerReplicaCount <= 0 → 3` precedent. Existing convenience constructors / `forTests` pass the sentinel; `StrataServer` sets real values via new `with*` methods.
- **Build:** use `mvn -q -pl <module> -am test` (or `-Dtest=<Class>` for a single test). Build proto/common changes with `-am` so dependents recompile.

---

## File Structure

**New files**
- `strata-common/src/main/java/io/strata/common/EnvConfig.java` — tiny `intEnv/longEnv` env reader for static-init config in common/proto.
- `strata-format/src/main/java/io/strata/format/ChunkStoreConfig.java` — format-layer tuning record.
- `strata-format/src/test/java/io/strata/format/ChunkStoreConfigTest.java` — defaults + validation.
- `strata-common/src/test/java/io/strata/common/EnvConfigTest.java` — default-path parsing.

**Modified (by tier)**
- A: `ControllerConfig.java`, `RepairCoordinator.java`, `NodeRegistry.java`, `ZkMetadataStore.java`, `Controller.java`, `StrataSystemMetadataFileStore.java`, `DataNodeConfig.java`, `OrphanGc.java`, `ControlLoop.java`, `ClientConfig.java`, `ControllerClient.java`, `Recovery.java` + their config tests.
- B: `ChunkStore.java`, `GroupCommitter.java`, `DataNode.java`.
- C: `ScpServer.java`, `ScpClient.java`, `NettyEventLoops.java`, `FrameIO.java`, `ConnectionPolicy.java`.
- D: `ServerMetrics.java`, `StrataServer.java` (env wiring lives in Tasks 1/8/9/14).

**Knob → home → env (full table is in the spec `docs/superpowers/specs/2026-06-29-config-exposure-design.md`).** Each task below repeats the exact subset it implements.

---

## TIER A — domain records + consumers

### Task 1: Extend `ControllerConfig` with 7 tuning fields + env wiring

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/ControllerConfig.java`
- Modify: `strata-server/src/main/java/io/strata/server/StrataServer.java` (runController, runCombined)
- Test: `strata-meta/src/test/java/io/strata/meta/ControllerConfigDefaultsTest.java`

**Interfaces — Produces** (new accessors other tasks consume):
`int verifyIntervalMs()`, `int verifyBatchSize()`, `int systemVerifyIntervalMs()`, `long deletedTombstoneTtlMs()`, `int maxCommandsPerHeartbeat()`, `int zkRetryBaseMs()`, `int zkRetryMaxRetries()`, and `with*` setters for each. Defaults: `2000, 256, 30000, 600000L, 16, 100, 5`.

- [ ] **Step 1: Write failing test** — append to `ControllerConfigDefaultsTest`:

```java
    @Test
    void newTuningFieldsDefault() {
        ControllerConfig c = new ControllerConfig("zk:2181", 9100, 3000, 60000, 300000, 5000, 30000);
        assertEquals(2_000, c.verifyIntervalMs());
        assertEquals(256, c.verifyBatchSize());
        assertEquals(30_000, c.systemVerifyIntervalMs());
        assertEquals(600_000L, c.deletedTombstoneTtlMs());
        assertEquals(16, c.maxCommandsPerHeartbeat());
        assertEquals(100, c.zkRetryBaseMs());
        assertEquals(5, c.zkRetryMaxRetries());
    }

    @Test
    void withSettersOverrideTuning() {
        ControllerConfig c = new ControllerConfig("zk:2181", 9100, 3000, 60000, 300000, 5000, 30000)
                .withVerifyIntervalMs(500).withVerifyBatchSize(64).withSystemVerifyIntervalMs(10_000)
                .withDeletedTombstoneTtlMs(120_000).withMaxCommandsPerHeartbeat(64)
                .withZkRetryBaseMs(50).withZkRetryMaxRetries(9);
        assertEquals(500, c.verifyIntervalMs());
        assertEquals(64, c.verifyBatchSize());
        assertEquals(10_000, c.systemVerifyIntervalMs());
        assertEquals(120_000L, c.deletedTombstoneTtlMs());
        assertEquals(64, c.maxCommandsPerHeartbeat());
        assertEquals(50, c.zkRetryBaseMs());
        assertEquals(9, c.zkRetryMaxRetries());
    }

    @Test
    void rejectsTombstoneTtlNotExceedingRepairScan() {
        // deletedTombstoneTtlMs (1000) must exceed repairScanIntervalMs (5000) -> invalid
        assertThrows(IllegalArgumentException.class,
                () -> new ControllerConfig("zk:2181", 9100, 3000, 60000, 300000, 5000, 30000)
                        .withDeletedTombstoneTtlMs(1_000));
    }
```

- [ ] **Step 2: Run, verify fail** — `mvn -q -pl strata-meta test -Dtest=ControllerConfigDefaultsTest` → FAIL (`verifyIntervalMs()` not found).

- [ ] **Step 3: Implement** — in `ControllerConfig.java`:
  1. Append 7 components to the record header (after `int controllerReplicaCount`):
```java
        int controllerReplicaCount,  // metadata replica-set size per namespace (design §6.1)
        int verifyIntervalMs,        // owner-pull VERIFY_CHUNKS cadence (RepairCoordinator)
        int verifyBatchSize,         // chunk-ids per VERIFY_CHUNKS RPC
        int systemVerifyIntervalMs,  // slower verify cadence for the system (metadata-log) namespace
        long deletedTombstoneTtlMs,  // DELETED tombstone retention before reap
        int maxCommandsPerHeartbeat, // commands drained per node heartbeat
        int zkRetryBaseMs,           // Curator ExponentialBackoffRetry base sleep
        int zkRetryMaxRetries        // Curator ExponentialBackoffRetry max retries
```
  2. In the compact ctor (after the `controllerReplicaCount` default), normalize sentinels to defaults and validate the coupling:
```java
        if (controllerReplicaCount <= 0) {
            controllerReplicaCount = 3;
        }
        if (verifyIntervalMs <= 0) {
            verifyIntervalMs = 2_000;
        }
        if (verifyBatchSize <= 0) {
            verifyBatchSize = 256;
        }
        if (systemVerifyIntervalMs <= 0) {
            systemVerifyIntervalMs = 30_000;
        }
        if (deletedTombstoneTtlMs <= 0) {
            deletedTombstoneTtlMs = 600_000;
        }
        if (maxCommandsPerHeartbeat <= 0) {
            maxCommandsPerHeartbeat = 16;
        }
        if (zkRetryBaseMs <= 0) {
            zkRetryBaseMs = 100;
        }
        if (zkRetryMaxRetries < 0) {
            zkRetryMaxRetries = 5;
        }
        // A DELETED tombstone fences a delayed CREATE replay; it must outlive the reconcile sweep cadence.
        if (deletedTombstoneTtlMs <= repairScanIntervalMs) {
            throw new IllegalArgumentException("deletedTombstoneTtlMs (" + deletedTombstoneTtlMs
                    + ") must exceed repairScanIntervalMs (" + repairScanIntervalMs + ")");
        }
```
  3. Append `, 0, 0, 0, 0, 0, 0, 0` to the tail of the two convenience constructors (7-arg at lines 51-55, 11-arg at 58-65) and `forTests` (lines 99-102) — i.e. each existing `this(..., List.of(), 3)` becomes `this(..., List.of(), 3, 0, 0, 0, 0, 0, 0, 0)`.
  4. In each existing `with*` method (`withAdvertisedHost`, `withReplicaMissingGraceMs`, `withReconcileIntervalMs`, `withControllerEndpoints`) append the 7 fields, carrying current values, to its `new ControllerConfig(...)` call: `..., controllerReplicaCount, verifyIntervalMs, verifyBatchSize, systemVerifyIntervalMs, deletedTombstoneTtlMs, maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries)`.
  5. Add 7 new `with*` methods (each reconstructs the full record, overriding one field). Example for the first; the rest are identical in shape with the overridden field swapped:
```java
    public ControllerConfig withVerifyIntervalMs(int v) {
        return new ControllerConfig(zkConnect, listenPort, heartbeatIntervalMs, leaseMs, deadGraceMs,
                repairScanIntervalMs, repairCommandTimeoutMs, reconcileIntervalMs, zkSessionTimeoutMs,
                zkConnectionTimeoutMs, advertisedHost, replicaMissingGraceMs, controllerEndpoints,
                controllerReplicaCount, v, verifyBatchSize, systemVerifyIntervalMs, deletedTombstoneTtlMs,
                maxCommandsPerHeartbeat, zkRetryBaseMs, zkRetryMaxRetries);
    }
```
  Add `withVerifyBatchSize(int)`, `withSystemVerifyIntervalMs(int)`, `withDeletedTombstoneTtlMs(long)`, `withMaxCommandsPerHeartbeat(int)`, `withZkRetryBaseMs(int)`, `withZkRetryMaxRetries(int)` the same way (override the matching positional arg, pass the rest from `this`).

- [ ] **Step 4: Run, verify pass** — `mvn -q -pl strata-meta test -Dtest=ControllerConfigDefaultsTest` → PASS.

- [ ] **Step 5: Wire env in `StrataServer`** — in BOTH `runController()` (after `.withReconcileIntervalMs(...)`, line ~68) and `runCombined()` (the `controllerConfig` builder, line ~147) append:
```java
                .withVerifyIntervalMs(intEnv("STRATA_VERIFY_INTERVAL_MS", 2_000))
                .withVerifyBatchSize(intEnv("STRATA_VERIFY_BATCH_SIZE", 256))
                .withSystemVerifyIntervalMs(intEnv("STRATA_SYSTEM_VERIFY_INTERVAL_MS", 30_000))
                .withDeletedTombstoneTtlMs(longEnv("STRATA_CONTROLLER_DELETED_TOMBSTONE_TTL_MS", 600_000))
                .withMaxCommandsPerHeartbeat(intEnv("STRATA_CONTROLLER_MAX_COMMANDS_PER_HEARTBEAT", 16))
                .withZkRetryBaseMs(intEnv("STRATA_CONTROLLER_ZK_RETRY_BASE_MS", 100))
                .withZkRetryMaxRetries(intEnv("STRATA_CONTROLLER_ZK_RETRY_MAX", 5))
```

- [ ] **Step 6: Compile the module + server** — `mvn -q -pl strata-meta,strata-server -am test-compile` → success.

- [ ] **Step 7: Commit**

```bash
git add strata-meta/src/main/java/io/strata/meta/ControllerConfig.java \
        strata-meta/src/test/java/io/strata/meta/ControllerConfigDefaultsTest.java \
        strata-server/src/main/java/io/strata/server/StrataServer.java
git commit -m "feat(config): add ControllerConfig verify/tombstone/heartbeat/zk-retry knobs"
```

---

### Task 2: Consume verify + tombstone knobs in `RepairCoordinator`

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/RepairCoordinator.java`
- Test: `strata-meta/src/test/java/io/strata/meta/RepairCoordinatorVerifyTest.java` (existing verify tests — locate the class that drives `verifyPass()`/`systemVerifyIntervalMsForTest`; add to it)

**Interfaces — Consumes:** `config.verifyIntervalMs()`, `config.verifyBatchSize()`, `config.systemVerifyIntervalMs()`, `config.deletedTombstoneTtlMs()` from Task 1. `config` is the existing `private final ControllerConfig config` field.

- [ ] **Step 1: Write failing test** — assert the configured system-verify cadence is honored (replacing the `systemVerifyIntervalMsForTest` seam). Find the existing test that calls `systemVerifyIntervalMsForTest`; replace that call with a `ControllerConfig.forTests(...).withSystemVerifyIntervalMs(...)` passed at construction, and assert the throttle still applies (system namespace not re-verified within the window). If no such test exists, add:

```java
    @Test
    void systemVerifyCadenceComesFromConfig() throws Exception {
        // build a RepairCoordinator with a tiny system-verify window via config (not the *ForTest setter)
        ControllerConfig cfg = ControllerConfig.forTests("zk:1").withSystemVerifyIntervalMs(50);
        // ... existing harness builds RepairCoordinator(store, registry, cfg, () -> true) ...
        // first verifyPass() stamps lastSystemVerifyMs; an immediate second pass must skip the system ns
        // (assert via a spy/counter on execVerify for the system namespace, mirroring the existing test).
    }
```
  (Use the existing test's mocking style — the meta module's verify tests already construct a `RepairCoordinator` and observe verify RPCs.)

- [ ] **Step 2: Run, verify fail** — `mvn -q -pl strata-meta test -Dtest=RepairCoordinator*Test` → FAIL or compile error (config setter not used yet).

- [ ] **Step 3: Implement** — in `RepairCoordinator.java`:
  - Delete `private static final long VERIFY_INTERVAL_MS = 2_000;` (line 216); in `verifyLoop()` replace `Thread.sleep(VERIFY_INTERVAL_MS);` with `Thread.sleep(config.verifyIntervalMs());`.
  - Delete `private static final int VERIFY_BATCH_SIZE = 256;` (line 217); in `verifyFile()` replace both `VERIFY_BATCH_SIZE` uses (loop step + `Math.min`) with `config.verifyBatchSize()` (capture once: `int batchSize = config.verifyBatchSize();` above the `for` loop, use `batchSize`).
  - Delete `private static final long SYSTEM_VERIFY_INTERVAL_MS = 30_000;` (line 223) and change the field initializer `private volatile long systemVerifyIntervalMs = SYSTEM_VERIFY_INTERVAL_MS;` → `private volatile long systemVerifyIntervalMs;` and set it in EVERY constructor body: `this.systemVerifyIntervalMs = config.systemVerifyIntervalMs();` (in the canonical 6-arg ctor at line 187). Keep the `systemVerifyIntervalMsForTest` setter (still useful) OR delete it and update its callers to the config path; prefer keeping it.
  - Delete `private static final long DELETED_TOMBSTONE_TTL_MS = 600_000;` (line 121); in `sweepTombstones()` replace both `DELETED_TOMBSTONE_TTL_MS` with `config.deletedTombstoneTtlMs()`.

- [ ] **Step 4: Run, verify pass** — `mvn -q -pl strata-meta test -Dtest=RepairCoordinator*Test` → PASS.

- [ ] **Step 5: Run the meta suite** — `mvn -q -pl strata-meta test` → green.

- [ ] **Step 6: Commit**

```bash
git add strata-meta/src/main/java/io/strata/meta/RepairCoordinator.java strata-meta/src/test/java/io/strata/meta/RepairCoordinator*Test.java
git commit -m "feat(config): source verify cadence/batch + tombstone TTL from ControllerConfig"
```

---

### Task 3: Consume `maxCommandsPerHeartbeat` in `NodeRegistry`

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/NodeRegistry.java`
- Test: the existing NodeRegistry heartbeat test (locate the class exercising `heartbeat(...)` command draining).

**Interfaces — Consumes:** `config.maxCommandsPerHeartbeat()`. `config` is the existing `private final ControllerConfig config` field (NodeRegistry.java:57).

- [ ] **Step 1: Write failing test** — queue >N commands for a node, configure `maxCommandsPerHeartbeat` to a small value, assert exactly that many drain per heartbeat:

```java
    @Test
    void heartbeatDrainsAtMostConfiguredCommands() throws Exception {
        ControllerConfig cfg = ControllerConfig.forTests("zk:1").withMaxCommandsPerHeartbeat(2);
        // build NodeRegistry(store, cfg); register a node; enqueue 5 commands;
        // call heartbeat(...) and assert resp.commands().size() == 2 (mirror the existing heartbeat test harness).
    }
```

- [ ] **Step 2: Run, verify fail** — `mvn -q -pl strata-meta test -Dtest=NodeRegistry*Test` → FAIL.

- [ ] **Step 3: Implement** — in `NodeRegistry.java`: delete `private static final int MAX_COMMANDS_PER_HEARTBEAT = 16;` (line 34); in `heartbeat()` replace `for (int i = 0; i < MAX_COMMANDS_PER_HEARTBEAT; i++)` with `int maxCommands = config.maxCommandsPerHeartbeat();` (declared just above the lock) and `for (int i = 0; i < maxCommands; i++)`.

- [ ] **Step 4: Run, verify pass** — `mvn -q -pl strata-meta test -Dtest=NodeRegistry*Test` → PASS.

- [ ] **Step 5: Commit**

```bash
git add strata-meta/src/main/java/io/strata/meta/NodeRegistry.java strata-meta/src/test/java/io/strata/meta/NodeRegistry*Test.java
git commit -m "feat(config): source command-per-heartbeat cap from ControllerConfig"
```

---

### Task 4: Thread ZK retry policy from config into `ZkMetadataStore`

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/ZkMetadataStore.java`
- Modify: `strata-meta/src/main/java/io/strata/meta/Controller.java` (construction at line 79-80)
- Test: `strata-meta/src/test/java/io/strata/meta/ZkMetadataStoreRetryTest.java` (new)

**Interfaces — Consumes:** `config.zkRetryBaseMs()`, `config.zkRetryMaxRetries()`. **Produces:** new ctor `ZkMetadataStore(String zkConnect, int sessionTimeoutMs, int connectionTimeoutMs, int retryBaseMs, int retryMaxRetries)`.

- [ ] **Step 1: Write failing test** — the retry policy isn't observable post-construction, so test the new ctor exists and the existing 3-arg ctor delegates with `(100, 5)`. Use a no-ZK construction guard test that the new 5-arg ctor is present (compile-level) plus assert the 3-arg path is unchanged. Minimal:

```java
    @Test
    void retryConstructorIsAvailable() throws Exception {
        // Reflection: assert the 5-arg constructor exists with (String,int,int,int,int)
        ZkMetadataStore.class.getDeclaredConstructor(String.class, int.class, int.class, int.class, int.class);
    }
```

- [ ] **Step 2: Run, verify fail** — `mvn -q -pl strata-meta test -Dtest=ZkMetadataStoreRetryTest` → FAIL (NoSuchMethodException).

- [ ] **Step 3: Implement** — in `ZkMetadataStore.java`:
  - Change `public ZkMetadataStore(String zkConnect, int sessionTimeoutMs, int connectionTimeoutMs)` to delegate: its body builds Curator with `.retryPolicy(new ExponentialBackoffRetry(100, 5))`. Replace with a new 5-arg public ctor that takes `retryBaseMs, retryMaxRetries`, and make the 3-arg ctor call it:
```java
    public ZkMetadataStore(String zkConnect, int sessionTimeoutMs, int connectionTimeoutMs) {
        this(zkConnect, sessionTimeoutMs, connectionTimeoutMs, 100, 5);
    }

    public ZkMetadataStore(String zkConnect, int sessionTimeoutMs, int connectionTimeoutMs,
                           int retryBaseMs, int retryMaxRetries) {
        this(CuratorFrameworkFactory.builder()
                .connectString(zkConnect)
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                .retryPolicy(new ExponentialBackoffRetry(retryBaseMs, retryMaxRetries))
                .build(), true, connectionTimeoutMs);
    }
```

- [ ] **Step 4: Run, verify pass** — `mvn -q -pl strata-meta test -Dtest=ZkMetadataStoreRetryTest` → PASS.

- [ ] **Step 5: Thread from Controller** — in `Controller.java:79-80` change:
```java
            openedStore = new ZkMetadataStore(config.zkConnect(),
                    config.zkSessionTimeoutMs(), config.zkConnectionTimeoutMs(),
                    config.zkRetryBaseMs(), config.zkRetryMaxRetries());
```

- [ ] **Step 6: Run meta suite** — `mvn -q -pl strata-meta test` → green.

- [ ] **Step 7: Commit**

```bash
git add strata-meta/src/main/java/io/strata/meta/ZkMetadataStore.java \
        strata-meta/src/main/java/io/strata/meta/Controller.java \
        strata-meta/src/test/java/io/strata/meta/ZkMetadataStoreRetryTest.java
git commit -m "feat(config): thread ZK retry base/max from ControllerConfig"
```

---

### Task 5: Make `metadataReadChunkBytes` a `STRATA_CONTROLLER_LOG_*` setting

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/StrataSystemMetadataFileStore.java`
- Modify: `strata-meta/src/main/java/io/strata/meta/Controller.java` (`defaultBackendFactory`, the `intSetting` block ~183-210)

**Interfaces — Produces:** `StrataSystemMetadataFileStore(Supplier<String>, int replicationFactor, int ackQuorum, boolean fsyncOnAck, int readChunkBytes)`.

- [ ] **Step 1: Write failing test** — `strata-meta/src/test/java/io/strata/meta/StrataSystemMetadataFileStoreTest.java` (new) reflection check the 5-arg ctor exists:

```java
    @Test
    void readChunkConstructorIsAvailable() throws Exception {
        StrataSystemMetadataFileStore.class.getDeclaredConstructor(
                java.util.function.Supplier.class, int.class, int.class, boolean.class, int.class);
    }
```

- [ ] **Step 2: Run, verify fail** — `mvn -q -pl strata-meta test -Dtest=StrataSystemMetadataFileStoreTest` → FAIL.

- [ ] **Step 3: Implement** — in `StrataSystemMetadataFileStore.java`:
  - Delete `private static final int READ_CHUNK = 4 * 1024 * 1024;` (line 32); add instance field `private final int readChunkBytes;`.
  - Change the constructor to accept and validate it:
```java
    StrataSystemMetadataFileStore(Supplier<String> metaEndpoint, int replicationFactor, int ackQuorum,
                                  boolean fsyncOnAck, int readChunkBytes) {
        this.metaEndpoint = metaEndpoint;
        if (readChunkBytes <= 0) {
            throw new IllegalArgumentException("readChunkBytes must be positive: " + readChunkBytes);
        }
        this.readChunkBytes = readChunkBytes;
        this.policy = new StrataClient.WritePolicy(replicationFactor, ackQuorum, fsyncOnAck);
    }
```
  - In `readAll()` replace `reader.read(offset, READ_CHUNK)` with `reader.read(offset, readChunkBytes)`.

- [ ] **Step 4: Thread the setting** — in `Controller.java` `defaultBackendFactory()`: add near the other `intSetting` reads (~line 187):
```java
        int readChunkBytes = intSetting("STRATA_CONTROLLER_LOG_READ_CHUNK_BYTES",
                "strata.controller.log.read.chunk.bytes", 4 * 1024 * 1024);
```
  and pass it to the store construction (line ~210):
```java
                    new StrataSystemMetadataFileStore(() -> endpoint, replicationFactor, ackQuorum, logFsync,
                            readChunkBytes), true);
```

- [ ] **Step 5: Run, verify pass + suite** — `mvn -q -pl strata-meta test` → green.

- [ ] **Step 6: Commit**

```bash
git add strata-meta/src/main/java/io/strata/meta/StrataSystemMetadataFileStore.java \
        strata-meta/src/main/java/io/strata/meta/Controller.java \
        strata-meta/src/test/java/io/strata/meta/StrataSystemMetadataFileStoreTest.java
git commit -m "feat(config): expose metadata-log read-chunk bytes via STRATA_CONTROLLER_LOG_READ_CHUNK_BYTES"
```

---

### Task 6: Create `ChunkStoreConfig` record (strata-format)

**Files:**
- Create: `strata-format/src/main/java/io/strata/format/ChunkStoreConfig.java`
- Test: `strata-format/src/test/java/io/strata/format/ChunkStoreConfigTest.java`

**Interfaces — Produces:** `record ChunkStoreConfig(int maxRequestBytes, long groupCommitDrainTimeoutMs, long groupCommitMinAccumulationNanos, long groupCommitMaxAccumulationNanos)` with `ChunkStoreConfig.DEFAULT` (`8*1024*1024, 10_000L, 1_000_000L, 50_000_000L`) and `with*` setters.

- [ ] **Step 1: Write failing test** — `ChunkStoreConfigTest.java`:

```java
package io.strata.format;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChunkStoreConfigTest {
    @Test
    void defaultsMatchHistoricalConstants() {
        ChunkStoreConfig c = ChunkStoreConfig.DEFAULT;
        assertEquals(8 * 1024 * 1024, c.maxRequestBytes());
        assertEquals(10_000L, c.groupCommitDrainTimeoutMs());
        assertEquals(1_000_000L, c.groupCommitMinAccumulationNanos());
        assertEquals(50_000_000L, c.groupCommitMaxAccumulationNanos());
    }

    @Test
    void rejectsNonPositiveAndBadAccumulationOrder() {
        assertThrows(IllegalArgumentException.class, () -> new ChunkStoreConfig(0, 10_000L, 1_000_000L, 50_000_000L));
        assertThrows(IllegalArgumentException.class, () -> new ChunkStoreConfig(8 << 20, 0L, 1_000_000L, 50_000_000L));
        // max < min -> invalid
        assertThrows(IllegalArgumentException.class, () -> new ChunkStoreConfig(8 << 20, 10_000L, 50_000_000L, 1_000_000L));
    }

    @Test
    void withSettersOverride() {
        ChunkStoreConfig c = ChunkStoreConfig.DEFAULT.withMaxRequestBytes(4 << 20)
                .withGroupCommitDrainTimeoutMs(20_000L)
                .withGroupCommitAccumulationNanos(2_000_000L, 80_000_000L);
        assertEquals(4 << 20, c.maxRequestBytes());
        assertEquals(20_000L, c.groupCommitDrainTimeoutMs());
        assertEquals(2_000_000L, c.groupCommitMinAccumulationNanos());
        assertEquals(80_000_000L, c.groupCommitMaxAccumulationNanos());
    }
}
```

- [ ] **Step 2: Run, verify fail** — `mvn -q -pl strata-format test -Dtest=ChunkStoreConfigTest` → FAIL (class missing).

- [ ] **Step 3: Implement** — `ChunkStoreConfig.java`:

```java
package io.strata.format;

/** Tuning knobs for {@link ChunkStore}'s read cap and group-commit fsync batching. */
public record ChunkStoreConfig(
        int maxRequestBytes,
        long groupCommitDrainTimeoutMs,
        long groupCommitMinAccumulationNanos,
        long groupCommitMaxAccumulationNanos) {

    public static final ChunkStoreConfig DEFAULT =
            new ChunkStoreConfig(8 * 1024 * 1024, 10_000L, 1_000_000L, 50_000_000L);

    public ChunkStoreConfig {
        if (maxRequestBytes <= 0) {
            throw new IllegalArgumentException("maxRequestBytes must be positive: " + maxRequestBytes);
        }
        if (groupCommitDrainTimeoutMs <= 0) {
            throw new IllegalArgumentException("groupCommitDrainTimeoutMs must be positive: " + groupCommitDrainTimeoutMs);
        }
        if (groupCommitMinAccumulationNanos <= 0) {
            throw new IllegalArgumentException("groupCommitMinAccumulationNanos must be positive: " + groupCommitMinAccumulationNanos);
        }
        if (groupCommitMaxAccumulationNanos < groupCommitMinAccumulationNanos) {
            throw new IllegalArgumentException("groupCommitMaxAccumulationNanos (" + groupCommitMaxAccumulationNanos
                    + ") must be >= groupCommitMinAccumulationNanos (" + groupCommitMinAccumulationNanos + ")");
        }
    }

    public ChunkStoreConfig withMaxRequestBytes(int v) {
        return new ChunkStoreConfig(v, groupCommitDrainTimeoutMs, groupCommitMinAccumulationNanos,
                groupCommitMaxAccumulationNanos);
    }

    public ChunkStoreConfig withGroupCommitDrainTimeoutMs(long v) {
        return new ChunkStoreConfig(maxRequestBytes, v, groupCommitMinAccumulationNanos,
                groupCommitMaxAccumulationNanos);
    }

    public ChunkStoreConfig withGroupCommitAccumulationNanos(long min, long max) {
        return new ChunkStoreConfig(maxRequestBytes, groupCommitDrainTimeoutMs, min, max);
    }
}
```

- [ ] **Step 4: Run, verify pass** — `mvn -q -pl strata-format test -Dtest=ChunkStoreConfigTest` → PASS.

- [ ] **Step 5: Commit**

```bash
git add strata-format/src/main/java/io/strata/format/ChunkStoreConfig.java \
        strata-format/src/test/java/io/strata/format/ChunkStoreConfigTest.java
git commit -m "feat(config): add ChunkStoreConfig record for format-layer tuning knobs"
```

---

### Task 7: Consume `ChunkStoreConfig` in `ChunkStore` + `GroupCommitter`

**Files:**
- Modify: `strata-format/src/main/java/io/strata/format/ChunkStore.java`
- Modify: `strata-format/src/main/java/io/strata/format/GroupCommitter.java`
- Test: `strata-format/src/test/java/io/strata/format/ChunkStoreConfigWiringTest.java` (new)

**Interfaces — Consumes:** `ChunkStoreConfig` (Task 6). **Produces:** `ChunkStore(Path, ChunkStoreConfig)` ctor (and `ChunkStore(Path)` keeps using `ChunkStoreConfig.DEFAULT`); `GroupCommitter(String, Syncer, AtomicLong, long drainTimeoutMs, long minAccNanos, long maxAccNanos)`.

- [ ] **Step 1: Write failing test** — assert `ChunkStore(Path)` equals `ChunkStore(Path, DEFAULT)` behaviorally and that a custom `maxRequestBytes` caps a read. Minimal compile-first test:

```java
    @Test
    void chunkStoreAcceptsConfig() throws Exception {
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("cs");
        try (ChunkStore s = new ChunkStore(dir, ChunkStoreConfig.DEFAULT.withMaxRequestBytes(4096))) {
            // write a sealed chunk > 4096 bytes, read from offset 0 with a large maxBytes,
            // assert the returned slice length <= 4096 (the configured cap). Mirror ChunkStoreTest read helpers.
        }
    }
```

- [ ] **Step 2: Run, verify fail** — `mvn -q -pl strata-format test -Dtest=ChunkStoreConfigWiringTest` → FAIL (ctor missing).

- [ ] **Step 3: Implement `ChunkStore`** —
  - Delete `public static final int MAX_REQUEST_BYTES = 8 * 1024 * 1024;` (line 56); add instance field `private final ChunkStoreConfig csConfig;`.
  - Add a config-bearing ctor and route the existing ones through it. Current chain is `(Path) → (Path, boolean) → (Path, boolean, int)`. Add a `ChunkStoreConfig` param to the innermost ctor and default it in the outer ones:
```java
    public ChunkStore(Path dir) throws IOException {
        this(dir, SEAL_FSYNC_DEFAULT);
    }

    public ChunkStore(Path dir, ChunkStoreConfig csConfig) throws IOException {
        this(dir, SEAL_FSYNC_DEFAULT, (int) CHANNEL_CACHE_MAX_SIZE, csConfig);
    }

    ChunkStore(Path dir, boolean sealFsync) throws IOException {
        this(dir, sealFsync, (int) CHANNEL_CACHE_MAX_SIZE, ChunkStoreConfig.DEFAULT);
    }

    ChunkStore(Path dir, boolean sealFsync, int channelCacheCapacity) throws IOException {
        this(dir, sealFsync, channelCacheCapacity, ChunkStoreConfig.DEFAULT);
    }

    ChunkStore(Path dir, boolean sealFsync, int channelCacheCapacity, ChunkStoreConfig csConfig) throws IOException {
        this.csConfig = csConfig;
        // ... existing body unchanged ...
    }
```
  - Replace the 3 `MAX_REQUEST_BYTES` read-cap uses (lines 899, 910, 946) with `csConfig.maxRequestBytes()`.
  - In `Handle.startCommitterIfFsync` (line 553) pass the group-commit knobs to `GroupCommitter`:
```java
                committer = new GroupCommitter(id.toString(), () -> {
                    data.force(false);
                    ledger.force();
                }, counter,
                csConfig.groupCommitDrainTimeoutMs(),
                csConfig.groupCommitMinAccumulationNanos(),
                csConfig.groupCommitMaxAccumulationNanos());
```
  (Note: `Handle` is an inner class of `ChunkStore`, so `csConfig` is reachable via the enclosing instance.)

- [ ] **Step 4: Implement `GroupCommitter`** —
  - Delete the two `Long.getLong(...)` static fields (lines 94-97). Add instance fields:
```java
    private final long drainTimeoutMs;
    private final long minAccumulationNanos;
    private final long maxAccumulationNanos;
    private long accumulationNanos;
```
  - New constructor:
```java
    GroupCommitter(String name, Syncer syncer, AtomicLong forceCounter,
                   long drainTimeoutMs, long minAccumulationNanos, long maxAccumulationNanos) {
        this.syncer = syncer;
        this.forceCounter = forceCounter;
        this.drainTimeoutMs = drainTimeoutMs;
        this.minAccumulationNanos = minAccumulationNanos;
        this.maxAccumulationNanos = maxAccumulationNanos;
        this.accumulationNanos = minAccumulationNanos;
        this.flusher = Thread.ofVirtual().name("group-commit-" + name).start(this::run);
    }
```
  - In `run()` (lines 145-148) replace `MAX_ACCUMULATION_NANOS`/`MIN_ACCUMULATION_NANOS` with `maxAccumulationNanos`/`minAccumulationNanos`.
  - In `closeAndConfirm()` replace `flusher.join(10_000);` (line 215) with `flusher.join(drainTimeoutMs);`.
  - Update the existing `GroupCommitTest`/`ChunkStoreTest` direct `new GroupCommitter(name, syncer, forces)` calls (GroupCommitTest lines 134,156,172,208,226; ChunkStoreTest 643,684,737) to the 6-arg form passing `10_000L, 1_000_000L, 50_000_000L`.

- [ ] **Step 5: Run, verify pass** — `mvn -q -pl strata-format test` → green (config wiring + existing format tests).

- [ ] **Step 6: Commit**

```bash
git add strata-format/src/main/java/io/strata/format/ChunkStore.java \
        strata-format/src/main/java/io/strata/format/GroupCommitter.java \
        strata-format/src/test/java/io/strata/format/
git commit -m "feat(config): ChunkStore/GroupCommitter honor ChunkStoreConfig (read cap, drain, accumulation)"
```

---

### Task 8: Extend `DataNodeConfig` (6 node fields + nested `ChunkStoreConfig`) + env wiring

**Files:**
- Modify: `strata-node/src/main/java/io/strata/node/DataNodeConfig.java`
- Modify: `strata-server/src/main/java/io/strata/server/StrataServer.java` (runDataNode, runCombined)
- Test: `strata-node/src/test/java/io/strata/node/DataNodeConfigTest.java`

**Interfaces — Produces:** accessors `orphanGraceMs()`, `orphanScanIntervalMs()`, `orphanStartupGraceMs()`, `orphanConfirmTimeoutMs()`, `controlCallTimeoutMs()`, `repairFetchBytes()`, `chunkStoreConfig()` + matching `with*`. Defaults: `6000, 3000, 6000, 5000, 10000, 4*1024*1024, ChunkStoreConfig.DEFAULT`. (`DataNodeConfig` imports `io.strata.format.ChunkStoreConfig` — `strata-node` already depends on `strata-format`.)

- [ ] **Step 1: Write failing test** — append to `DataNodeConfigTest`:

```java
    @Test
    void newNodeTuningFieldsDefault() {
        DataNodeConfig c = new DataNodeConfig(Path.of("data"), 0, "10.0.0.1", null,
                List.of("meta:9000"), "zone", "rack", "host", 1234, 55);
        assertEquals(6_000, c.orphanGraceMs());
        assertEquals(3_000, c.orphanScanIntervalMs());
        assertEquals(6_000, c.orphanStartupGraceMs());
        assertEquals(5_000, c.orphanConfirmTimeoutMs());
        assertEquals(10_000, c.controlCallTimeoutMs());
        assertEquals(4 * 1024 * 1024, c.repairFetchBytes());
        assertEquals(io.strata.format.ChunkStoreConfig.DEFAULT, c.chunkStoreConfig());
    }

    @Test
    void withSettersOverrideNodeTuning() {
        DataNodeConfig c = new DataNodeConfig(Path.of("data"), 0, "10.0.0.1", null,
                List.of("meta:9000"), "zone", "rack", "host", 1234, 55)
                .withOrphanGraceMs(1000).withOrphanScanIntervalMs(500).withOrphanStartupGraceMs(1500)
                .withOrphanConfirmTimeoutMs(2500).withControlCallTimeoutMs(8000).withRepairFetchBytes(1 << 20)
                .withChunkStoreConfig(io.strata.format.ChunkStoreConfig.DEFAULT.withMaxRequestBytes(4096));
        assertEquals(1000, c.orphanGraceMs());
        assertEquals(500, c.orphanScanIntervalMs());
        assertEquals(1500, c.orphanStartupGraceMs());
        assertEquals(2500, c.orphanConfirmTimeoutMs());
        assertEquals(8000, c.controlCallTimeoutMs());
        assertEquals(1 << 20, c.repairFetchBytes());
        assertEquals(4096, c.chunkStoreConfig().maxRequestBytes());
    }
```

- [ ] **Step 2: Run, verify fail** — `mvn -q -pl strata-node test -Dtest=DataNodeConfigTest` → FAIL.

- [ ] **Step 3: Implement** — in `DataNodeConfig.java`:
  - Add import `import io.strata.format.ChunkStoreConfig;`.
  - Append 7 components to the record header (after `int nodeId`):
```java
        int nodeId,                  // -1 = standalone/unregistered; otherwise >= 1
        long orphanGraceMs,
        long orphanScanIntervalMs,
        long orphanStartupGraceMs,
        int orphanConfirmTimeoutMs,
        int controlCallTimeoutMs,
        int repairFetchBytes,
        ChunkStoreConfig chunkStoreConfig
```
  - In the compact ctor, after the existing validation, normalize sentinels:
```java
        if (orphanGraceMs <= 0) { orphanGraceMs = 6_000; }
        if (orphanScanIntervalMs <= 0) { orphanScanIntervalMs = 3_000; }
        if (orphanStartupGraceMs <= 0) { orphanStartupGraceMs = 6_000; }
        if (orphanConfirmTimeoutMs <= 0) { orphanConfirmTimeoutMs = 5_000; }
        if (controlCallTimeoutMs <= 0) { controlCallTimeoutMs = 10_000; }
        if (repairFetchBytes <= 0) { repairFetchBytes = 4 * 1024 * 1024; }
        if (chunkStoreConfig == null) { chunkStoreConfig = ChunkStoreConfig.DEFAULT; }
```
  - Update the 10-arg convenience ctor (the one that delegates with `ConnectionPolicy.DEFAULT, -1`) to append the 7 sentinels: `..., ConnectionPolicy.DEFAULT, -1, 0L, 0L, 0L, 0, 0, 0, null`.
  - Update `standalone(...)` and `withMetadata(...)` factories: their `new DataNodeConfig(...)` calls use the 10-arg convenience ctor, so no change IF they call the 10-arg form; if they call the full ctor, append the 7 sentinels.
  - Update the 4 existing `with*` methods (`withListenPort`, `withAdvertisedEndpoint`, `withConnectionPolicy`, `withNodeId`) to carry the 7 new fields: append `, orphanGraceMs, orphanScanIntervalMs, orphanStartupGraceMs, orphanConfirmTimeoutMs, controlCallTimeoutMs, repairFetchBytes, chunkStoreConfig` to each `new DataNodeConfig(...)`.
  - Add 7 new `with*` methods following the same full-reconstruction pattern (override one field). Example:
```java
    public DataNodeConfig withOrphanGraceMs(long v) {
        return new DataNodeConfig(dataDir, listenPort, advertisedHost, advertisedEndpointOverride,
                controllerEndpoints, zone, rack, host, capacityBytes, scrubIntervalMs, connectionPolicy, nodeId,
                v, orphanScanIntervalMs, orphanStartupGraceMs, orphanConfirmTimeoutMs, controlCallTimeoutMs,
                repairFetchBytes, chunkStoreConfig);
    }
```
  Add `withOrphanScanIntervalMs(long)`, `withOrphanStartupGraceMs(long)`, `withOrphanConfirmTimeoutMs(int)`, `withControlCallTimeoutMs(int)`, `withRepairFetchBytes(int)`, `withChunkStoreConfig(ChunkStoreConfig)` identically (override the matching arg).
  - Note: the existing `NullPointerException` test (`..., null, -1`) calls the 12-arg ctor; that ctor no longer exists as a distinct arity — update that one test line to append 7 sentinels or switch it to assert NPE another way. Adjust the test if it fails to compile.

- [ ] **Step 4: Run, verify pass** — `mvn -q -pl strata-node test -Dtest=DataNodeConfigTest` → PASS.

- [ ] **Step 5: Wire env in `StrataServer`** — in BOTH `runDataNode()` (after the `DataNodeConfig` is built, before `.withNodeId(...)`, OR chained after it) and `runCombined()` (the `nodeConfig` builder) chain:
```java
                .withOrphanGraceMs(longEnv("STRATA_ORPHAN_GRACE_MS", 6_000))
                .withOrphanScanIntervalMs(longEnv("STRATA_ORPHAN_SCAN_INTERVAL_MS", 3_000))
                .withOrphanStartupGraceMs(longEnv("STRATA_ORPHAN_STARTUP_GRACE_MS", 6_000))
                .withOrphanConfirmTimeoutMs(intEnv("STRATA_ORPHAN_CONFIRM_TIMEOUT_MS", 5_000))
                .withControlCallTimeoutMs(intEnv("STRATA_CONTROL_CALL_TIMEOUT_MS", 10_000))
                .withRepairFetchBytes(intEnv("STRATA_REPAIR_FETCH_BYTES", 4 * 1024 * 1024))
                .withChunkStoreConfig(new io.strata.format.ChunkStoreConfig(
                        intEnv("STRATA_MAX_REQUEST_BYTES", 8 * 1024 * 1024),
                        longEnv("STRATA_GROUPCOMMIT_DRAIN_TIMEOUT_MS", 10_000),
                        longEnv("STRATA_GROUPCOMMIT_MIN_ACCUMULATION_NANOS", 1_000_000),
                        longEnv("STRATA_GROUPCOMMIT_MAX_ACCUMULATION_NANOS", 50_000_000)))
```
  (Place the chain so it applies to the same `config`/`nodeConfig` local; ensure `.withNodeId(requiredIntEnv("STRATA_NODE_ID"))` stays in the chain.)

- [ ] **Step 6: Compile node + server** — `mvn -q -pl strata-node,strata-server -am test-compile` → success.

- [ ] **Step 7: Commit**

```bash
git add strata-node/src/main/java/io/strata/node/DataNodeConfig.java \
        strata-node/src/test/java/io/strata/node/DataNodeConfigTest.java \
        strata-server/src/main/java/io/strata/server/StrataServer.java
git commit -m "feat(config): add DataNodeConfig orphan/control/chunk-store knobs + env wiring"
```

---

### Task 9: Consume orphan knobs in `OrphanGc` + wire from `DataNode`

**Files:**
- Modify: `strata-node/src/main/java/io/strata/node/OrphanGc.java`
- Modify: `strata-node/src/main/java/io/strata/node/DataNode.java` (line 83 construction)
- Test: existing `OrphanGc` test class (locate; add a confirm-timeout/grace wiring assertion)

**Interfaces — Consumes:** `config.orphanGraceMs/orphanScanIntervalMs/orphanStartupGraceMs/orphanConfirmTimeoutMs()`. **Produces:** `OrphanGc(ChunkStore, int, List<String>, long graceMs, long scanIntervalMs, long startupGraceMs, int confirmTimeoutMs)`.

- [ ] **Step 1: Write failing test** — assert the 7-arg ctor exists and `confirmTimeoutMs` is honored (reflection or behavioral). Minimal:

```java
    @Test
    void sevenArgConstructorIsAvailable() throws Exception {
        OrphanGc.class.getDeclaredConstructor(ChunkStore.class, int.class, java.util.List.class,
                long.class, long.class, long.class, int.class);
    }
```

- [ ] **Step 2: Run, verify fail** — `mvn -q -pl strata-node test -Dtest=OrphanGc*Test` → FAIL.

- [ ] **Step 3: Implement** — in `OrphanGc.java`:
  - Delete `private static final int CONFIRM_TIMEOUT_MS = 5_000;` (line 53); add field `private final int confirmTimeoutMs;`.
  - Change the 6-arg ctor (line 70) to a 7-arg ctor adding `int confirmTimeoutMs` and `this.confirmTimeoutMs = confirmTimeoutMs;`. Keep the 3-arg convenience ctor but make it delegate with all 4 defaults: `this(store, nodeId, controllerEndpoints, DEFAULT_GRACE_MS, DEFAULT_SCAN_INTERVAL_MS, DEFAULT_STARTUP_GRACE_MS, 5_000);` — and add a `private static final int DEFAULT_CONFIRM_TIMEOUT_MS = 5_000;` to name it (replacing the deleted constant), or inline `5_000`.
  - In `confirm(...)` replace `CONFIRM_TIMEOUT_MS` (line 144) with `confirmTimeoutMs`.

- [ ] **Step 4: Wire from `DataNode`** — change line 83:
```java
                startedGc = new OrphanGc(openedStore, nodeId, config.controllerEndpoints(),
                        config.orphanGraceMs(), config.orphanScanIntervalMs(), config.orphanStartupGraceMs(),
                        config.orphanConfirmTimeoutMs());
```

- [ ] **Step 5: Run, verify pass + node suite** — `mvn -q -pl strata-node test` → green.

- [ ] **Step 6: Commit**

```bash
git add strata-node/src/main/java/io/strata/node/OrphanGc.java strata-node/src/main/java/io/strata/node/DataNode.java strata-node/src/test/java/io/strata/node/OrphanGc*Test.java
git commit -m "feat(config): OrphanGc grace/scan/startup/confirm-timeout from DataNodeConfig"
```

---

### Task 10: Consume control-loop knobs in `ControlLoop` + chunk-store config in `DataNode`

**Files:**
- Modify: `strata-node/src/main/java/io/strata/node/ControlLoop.java`
- Modify: `strata-node/src/main/java/io/strata/node/DataNode.java` (line 66 ChunkStore construction)
- Test: existing ControlLoop test / a focused new test

**Interfaces — Consumes:** `config.controlCallTimeoutMs()`, `config.repairFetchBytes()`, `config.chunkStoreConfig()`.

- [ ] **Step 1: Write failing test** — assert `DataNode` builds its `ChunkStore` from `config.chunkStoreConfig()` (behavioral: a custom `maxRequestBytes` caps a read through the node) OR a compile-level check that `ControlLoop` no longer references the deleted constants. Use the existing data-plane test harness to drive a read and assert the cap. Minimal compile-first:

```java
    @Test
    void controlLoopUsesConfiguredCallTimeout() {
        // build a DataNodeConfig with controlCallTimeoutMs=8000; construct ControlLoop;
        // assert no NoSuchFieldError and (if observable) the call timeout passed to a stubbed connection is 8000.
    }
```

- [ ] **Step 2: Run, verify fail** — `mvn -q -pl strata-node test -Dtest=ControlLoop*Test` → FAIL/compile error.

- [ ] **Step 3: Implement `ControlLoop`** —
  - Delete `private static final int CALL_TIMEOUT_MS = 10_000;` (line 40) and `private static final int FETCH_CHUNK_BYTES = 4 * 1024 * 1024;` (line 41). Keep `MAX_REPAIR_FOOTER_BYTES` (line 42) — it stays hardcoded (format invariant).
  - Replace the 3 `CALL_TIMEOUT_MS` uses (lines 147, 171, 270) with `config.controlCallTimeoutMs()`.
  - Replace the 3 `FETCH_CHUNK_BYTES` uses (lines 269, 298, 300) with `config.repairFetchBytes()` (capture once: `int fetchBytes = config.repairFetchBytes();` at the top of `fetchWholeFile()` and use `fetchBytes`).
  - Note: `config` may be `null` in standalone tests (the `config != null ? ... : DEFAULT` guard already exists for `connectionPolicy`). `ControlLoop` is only constructed when `controllerEndpoints` is non-empty (DataNode.java:75), so `config` is non-null in that path; no extra guard needed.

- [ ] **Step 4: Wire `ChunkStore` config in `DataNode`** — change line 66:
```java
            openedStore = new ChunkStore(config.dataDir().resolve("chunks"), config.chunkStoreConfig());
```

- [ ] **Step 5: Run, verify pass + node suite** — `mvn -q -pl strata-node test` → green.

- [ ] **Step 6: Commit**

```bash
git add strata-node/src/main/java/io/strata/node/ControlLoop.java strata-node/src/main/java/io/strata/node/DataNode.java strata-node/src/test/java/io/strata/node/
git commit -m "feat(config): ControlLoop call-timeout/fetch-bytes + DataNode ChunkStoreConfig wiring"
```

---

### Task 11: Extend `ClientConfig` + consume in `ControllerClient` / `Recovery`

**Files:**
- Modify: `strata-client/src/main/java/io/strata/client/ClientConfig.java`
- Modify: `strata-client/src/main/java/io/strata/client/ControllerClient.java`
- Modify: `strata-client/src/main/java/io/strata/client/Recovery.java`
- Test: `strata-client/src/test/java/io/strata/client/ClientConfigTest.java`

**Interfaces — Produces:** accessors `controllerRetryDeadlineMs()`, `controllerRetryBackoffMs()`, `recoveryCopyChunkBytes()` + `with*`. Defaults `15000L, 200, 4*1024*1024`. **Consumes:** `config` already held by `ControllerClient` (line 35) and `Recovery` (line 45).

- [ ] **Step 1: Write failing test** — append to `ClientConfigTest`:

```java
    @Test
    void newClientTuningFieldsDefault() {
        ClientConfig c = ClientConfig.of("host:123");
        assertEquals(15_000L, c.controllerRetryDeadlineMs());
        assertEquals(200, c.controllerRetryBackoffMs());
        assertEquals(4 * 1024 * 1024, c.recoveryCopyChunkBytes());
    }

    @Test
    void withSettersOverrideClientTuning() {
        ClientConfig c = ClientConfig.of("host:123")
                .withControllerRetryDeadlineMs(30_000L).withControllerRetryBackoffMs(50)
                .withRecoveryCopyChunkBytes(1 << 20);
        assertEquals(30_000L, c.controllerRetryDeadlineMs());
        assertEquals(50, c.controllerRetryBackoffMs());
        assertEquals(1 << 20, c.recoveryCopyChunkBytes());
    }
```

- [ ] **Step 2: Run, verify fail** — `mvn -q -pl strata-client test -Dtest=ClientConfigTest` → FAIL.

- [ ] **Step 3: Implement `ClientConfig`** —
  - Append 3 components to the record header (after `int dataNodeConnectionsPerEndpoint`):
```java
                           int dataNodeConnectionsPerEndpoint,
                           long controllerRetryDeadlineMs,
                           int controllerRetryBackoffMs,
                           int recoveryCopyChunkBytes) {
```
  - In the compact ctor, normalize sentinels + validate:
```java
        if (controllerRetryDeadlineMs <= 0) { controllerRetryDeadlineMs = 15_000L; }
        if (controllerRetryBackoffMs <= 0) { controllerRetryBackoffMs = 200; }
        if (recoveryCopyChunkBytes <= 0) { recoveryCopyChunkBytes = 4 * 1024 * 1024; }
```
  - Update the two secondary ctors (3-arg, 4-arg) and `of(...)` to append `, 0L, 0, 0` to their `this(...)` tails.
  - Update the 3 existing `with*` methods (`withChunkRollBytes`, `withConnectionPolicy`, `withDataNodeConnectionsPerEndpoint`) to carry the 3 new fields.
  - Add `withControllerRetryDeadlineMs(long)`, `withControllerRetryBackoffMs(int)`, `withRecoveryCopyChunkBytes(int)`.
  - Update the `NullPointerException` test in `ClientConfigTest` (`new ClientConfig(List.of("host:123"), 1, 1, null)`) — that calls the 4-arg ctor which still exists, so no change needed; but the `dataNodeConnectionsPerEndpoint == 0/-1` tests call the 5-arg ctor `(..., ConnectionPolicy.DEFAULT, 0)` — that arity is now 8; update those two `assertThrows` lines to append `, 0L, 0, 0`.

- [ ] **Step 4: Run, verify pass** — `mvn -q -pl strata-client test -Dtest=ClientConfigTest` → PASS.

- [ ] **Step 5: Consume in `ControllerClient`** — line 65 `long deadline = System.currentTimeMillis() + Math.max(15_000, config.callTimeoutMs());` → `Math.max(config.controllerRetryDeadlineMs(), config.callTimeoutMs());`. In `sleep()` line 102 `Thread.sleep(200);` → `Thread.sleep(config.controllerRetryBackoffMs());`.

- [ ] **Step 6: Consume in `Recovery`** — delete `private static final int COPY_CHUNK_BYTES = 4 * 1024 * 1024;` (line 38); in `catchUp()` (line 315) `Math.min(COPY_CHUNK_BYTES, target - rs.end)` → `Math.min(config.recoveryCopyChunkBytes(), target - rs.end)`.

- [ ] **Step 7: Run client suite** — `mvn -q -pl strata-client test` → green.

- [ ] **Step 8: Commit**

```bash
git add strata-client/src/main/java/io/strata/client/ClientConfig.java \
        strata-client/src/main/java/io/strata/client/ControllerClient.java \
        strata-client/src/main/java/io/strata/client/Recovery.java \
        strata-client/src/test/java/io/strata/client/ClientConfigTest.java
git commit -m "feat(config): add ClientConfig retry-deadline/backoff + recovery copy bytes"
```

---

## TIER C — strata-common / strata-proto transport & codec env reads

### Task 12: Add `EnvConfig` helper (strata-common)

**Files:**
- Create: `strata-common/src/main/java/io/strata/common/EnvConfig.java`
- Test: `strata-common/src/test/java/io/strata/common/EnvConfigTest.java`

**Interfaces — Produces:** `static int EnvConfig.intEnv(String name, int def)`, `static long EnvConfig.longEnv(String name, long def)`. Reads `System.getenv(name)`; blank/unset → default; parse failure throws `NumberFormatException` (fail-loud, no silent fallback).

- [ ] **Step 1: Write failing test** — `EnvConfigTest.java`:

```java
package io.strata.common;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class EnvConfigTest {
    @Test
    void unsetReturnsDefault() {
        assertEquals(7, EnvConfig.intEnv("STRATA_DEFINITELY_UNSET_XYZ_INT", 7));
        assertEquals(9L, EnvConfig.longEnv("STRATA_DEFINITELY_UNSET_XYZ_LONG", 9L));
    }
}
```

- [ ] **Step 2: Run, verify fail** — `mvn -q -pl strata-common test -Dtest=EnvConfigTest` → FAIL (class missing).

- [ ] **Step 3: Implement** — `EnvConfig.java`:

```java
package io.strata.common;

/** Reads STRATA_* environment variables for static-init config in the common/proto transport layer. */
public final class EnvConfig {
    private EnvConfig() {}

    public static int intEnv(String name, int def) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? def : Integer.parseInt(v.trim());
    }

    public static long longEnv(String name, long def) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? def : Long.parseLong(v.trim());
    }
}
```

- [ ] **Step 4: Run, verify pass** — `mvn -q -pl strata-common test -Dtest=EnvConfigTest` → PASS.

- [ ] **Step 5: Commit**

```bash
git add strata-common/src/main/java/io/strata/common/EnvConfig.java strata-common/src/test/java/io/strata/common/EnvConfigTest.java
git commit -m "feat(config): add EnvConfig STRATA_* env reader for transport-layer static config"
```

---

### Task 13: Env-source `ScpServer` inflight watermarks + `ScpClient` max-pending

**Files:**
- Modify: `strata-proto/src/main/java/io/strata/proto/ScpServer.java`
- Modify: `strata-proto/src/main/java/io/strata/proto/ScpClient.java`
- Test: existing `ClientServerTest` continues to pass (admission test uses the injection ctor).

**Interfaces — Consumes:** `EnvConfig` (Task 12). Clean break: delete the `Integer.getInteger`/`Long.getLong` `-D` defaults.

- [ ] **Step 1: Write failing test** — assert defaults still hold when env unset (covers the parse + default path without env manipulation):

```java
    // in a new ScpTransportConfigTest (strata-proto test)
    @Test
    void inflightDefaultsUnchanged() throws Exception {
        // ScpServer with the 5-arg public ctor still admits up to the default; reuse ClientServerTest helpers.
        // assert ScpClient.maxPendingRequests() == 1024 by default.
        org.junit.jupiter.api.Assertions.assertEquals(1024, ScpClient.maxPendingRequests());
    }
```

- [ ] **Step 2: Run, verify fail** — first add the import/usage so it compiles against the new code; before implementation this passes trivially (1024 today). To make it a real RED, instead assert the source no longer contains `Integer.getInteger` — simpler: skip RED here and treat this as a refactor guarded by the full `strata-proto` suite. Run `mvn -q -pl strata-proto test` as the baseline (GREEN now).

- [ ] **Step 3: Implement `ScpServer`** — replace lines 43-46:
```java
    private static final int DEFAULT_MAX_INFLIGHT_REQUESTS =
            io.strata.common.EnvConfig.intEnv("STRATA_SCP_MAX_INFLIGHT_REQUESTS", 1024);
    private static final long DEFAULT_MAX_INFLIGHT_BYTES =
            io.strata.common.EnvConfig.longEnv("STRATA_SCP_MAX_INFLIGHT_BYTES", 1L << 30);
```

- [ ] **Step 4: Implement `ScpClient`** — replace lines 39-40:
```java
    private static final int MAX_PENDING_REQUESTS =
            io.strata.common.EnvConfig.intEnv("STRATA_SCP_MAX_PENDING_REQUESTS", 1024);
```

- [ ] **Step 5: Run proto suite** — `mvn -q -pl strata-proto test` → green (incl. `ClientServerTest` admission test, which injects via the 7-arg ctor unaffected by the default change).

- [ ] **Step 6: Commit**

```bash
git add strata-proto/src/main/java/io/strata/proto/ScpServer.java strata-proto/src/main/java/io/strata/proto/ScpClient.java strata-proto/src/test/java/io/strata/proto/
git commit -m "feat(config): source SCP inflight + max-pending watermarks from STRATA_SCP_* env"
```

---

### Task 14: Unify connect-timeout via `STRATA_SCP_CONNECT_TIMEOUT_MS`

**Files:**
- Modify: `strata-common/src/main/java/io/strata/common/ConnectionPolicy.java`
- Modify: `strata-proto/src/main/java/io/strata/proto/ScpClient.java`
- Test: `strata-common/src/test/java/io/strata/common/ConnectionPolicyTest.java` (add/extend)

**Interfaces — Consumes:** `EnvConfig`. The single env sources both `ConnectionPolicy.DEFAULT.connectTimeoutMs` and `ScpClient`'s 4-arg ctor; removes both `5_000` literals.

- [ ] **Step 1: Write failing test** — assert `ConnectionPolicy.DEFAULT.connectTimeoutMs()` is `5000` by default:

```java
    @Test
    void defaultConnectTimeoutIsFiveSeconds() {
        assertEquals(5_000, ConnectionPolicy.DEFAULT.connectTimeoutMs());
    }
```
  (If `ConnectionPolicyTest` doesn't exist, create it with the JUnit5 imports.)

- [ ] **Step 2: Run** — `mvn -q -pl strata-common test -Dtest=ConnectionPolicyTest` → PASS today (guards against regression). Proceed as a refactor.

- [ ] **Step 3: Implement `ConnectionPolicy`** — change the `DEFAULT` first component:
```java
    public static final ConnectionPolicy DEFAULT = new ConnectionPolicy(
            EnvConfig.intEnv("STRATA_SCP_CONNECT_TIMEOUT_MS", 5_000),
            10_000,
            2_000,
            60_000,
            100,
            5_000);
```
  (Add `import io.strata.common.EnvConfig;` if needed — same package, so no import required.)

- [ ] **Step 4: Implement `ScpClient`** — change the 4-arg ctor (lines 52-54) to delegate to the policy default instead of a literal:
```java
    public ScpClient(String host, int port, byte clientKind, String clientId) throws IOException {
        this(host, port, clientKind, clientId, io.strata.common.ConnectionPolicy.DEFAULT.connectTimeoutMs());
    }
```

- [ ] **Step 5: Run common + proto suites** — `mvn -q -pl strata-common,strata-proto -am test` → green.

- [ ] **Step 6: Commit**

```bash
git add strata-common/src/main/java/io/strata/common/ConnectionPolicy.java \
        strata-proto/src/main/java/io/strata/proto/ScpClient.java \
        strata-common/src/test/java/io/strata/common/ConnectionPolicyTest.java
git commit -m "feat(config): unify SCP connect timeout under STRATA_SCP_CONNECT_TIMEOUT_MS"
```

---

### Task 15: Env-source `NettyEventLoops` I/O threads + `FrameIO` max-frame

**Files:**
- Modify: `strata-proto/src/main/java/io/strata/proto/NettyEventLoops.java`
- Modify: `strata-proto/src/main/java/io/strata/proto/FrameIO.java`
- Test: proto suite (plumbing; default-path assertion)

- [ ] **Step 1: Write failing test** — assert `FrameIO.MAX_FRAME_BYTES == 64*1024*1024` by default (regression guard):

```java
    @Test
    void maxFrameBytesDefaultUnchanged() {
        org.junit.jupiter.api.Assertions.assertEquals(64 * 1024 * 1024, FrameIO.MAX_FRAME_BYTES);
    }
```

- [ ] **Step 2: Run** — `mvn -q -pl strata-proto test -Dtest=ScpTransportConfigTest` → PASS today. Proceed as refactor.

- [ ] **Step 3: Implement `NettyEventLoops`** — replace the worker-group `0` (line 12) and the client-group `0` (line 7) with an env-sourced count:
```java
    static final NioEventLoopGroup CLIENT_GROUP =
            new NioEventLoopGroup(io.strata.common.EnvConfig.intEnv("STRATA_SCP_SERVER_IO_THREADS", 0),
                    new DefaultThreadFactory("scp-client-io", true));
    static final NioEventLoopGroup SERVER_BOSS_GROUP =
            new NioEventLoopGroup(1, new DefaultThreadFactory("scp-server-boss", true));
    static final NioEventLoopGroup SERVER_WORKER_GROUP =
            new NioEventLoopGroup(io.strata.common.EnvConfig.intEnv("STRATA_SCP_SERVER_IO_THREADS", 0),
                    new DefaultThreadFactory("scp-server-io", true));
```
  (Both shared I/O groups read the same knob; BOSS stays fixed at 1. `0` keeps the Netty default of 2×cores.)

- [ ] **Step 4: Implement `FrameIO`** — replace line 13:
```java
    public static final int MAX_FRAME_BYTES =
            io.strata.common.EnvConfig.intEnv("STRATA_MAX_FRAME_BYTES", 64 * 1024 * 1024);
```
  (The encode/decode guards and the `HelloResp` advertisement consume the static, so they pick up the env value automatically.)

- [ ] **Step 5: Run proto suite** — `mvn -q -pl strata-proto test` → green.

- [ ] **Step 6: Commit**

```bash
git add strata-proto/src/main/java/io/strata/proto/NettyEventLoops.java strata-proto/src/main/java/io/strata/proto/FrameIO.java strata-proto/src/test/java/io/strata/proto/
git commit -m "feat(config): source SCP I/O thread count + max frame bytes from STRATA_* env"
```

---

## TIER D — strata-server metrics

### Task 16: Env-source `ServerMetrics` ns-refresh interval + SLO buckets

**Files:**
- Modify: `strata-server/src/main/java/io/strata/server/ServerMetrics.java`
- Modify: `strata-server/src/main/java/io/strata/server/StrataServer.java` (pass env-derived values into the registrar; add a CSV-of-ms parser helper)
- Test: `strata-server/src/test/java/io/strata/server/ServerMetricsConfigTest.java` (new — the SLO-bucket CSV parser)

**Interfaces — Produces:** `StrataServer.durationBucketsMsEnv(String name, long[] def)` returning a sorted, distinct, positive `Duration[]`/`long[]`; `ServerMetrics.registerController`/`registerDataNode`/`requestObserver` gain an interval + buckets parameter.

- [ ] **Step 1: Write failing test** — `ServerMetricsConfigTest` for the CSV parser (the parser lives in `StrataServer` but is package-visible to the test in the same package):

```java
package io.strata.server;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServerMetricsConfigTest {
    @Test
    void parsesAscendingDistinctPositiveCsv() {
        assertArrayEquals(new long[]{1, 5, 10, 250}, StrataServer.parseBucketsMs("1,5,10,250", new long[]{1}));
    }

    @Test
    void unsetUsesDefault() {
        assertArrayEquals(new long[]{1, 2, 5}, StrataServer.parseBucketsMs(null, new long[]{1, 2, 5}));
    }

    @Test
    void rejectsNonAscendingOrNonPositive() {
        assertThrows(IllegalArgumentException.class, () -> StrataServer.parseBucketsMs("5,5", new long[]{1}));
        assertThrows(IllegalArgumentException.class, () -> StrataServer.parseBucketsMs("0,5", new long[]{1}));
        assertThrows(IllegalArgumentException.class, () -> StrataServer.parseBucketsMs("10,5", new long[]{1}));
    }
}
```

- [ ] **Step 2: Run, verify fail** — `mvn -q -pl strata-server test -Dtest=ServerMetricsConfigTest` → FAIL (`parseBucketsMs` missing).

- [ ] **Step 3: Implement parser in `StrataServer`** — add (package-private for the test):

```java
    static long[] parseBucketsMs(String csv, long[] def) {
        if (csv == null || csv.isBlank()) {
            return def;
        }
        String[] parts = csv.split(",");
        long[] out = new long[parts.length];
        long prev = 0;
        for (int i = 0; i < parts.length; i++) {
            long v = Long.parseLong(parts[i].trim());
            if (v <= 0) {
                throw new IllegalArgumentException("bucket must be positive ms: " + v);
            }
            if (v <= prev) {
                throw new IllegalArgumentException("buckets must be strictly ascending: " + csv);
            }
            out[i] = v;
            prev = v;
        }
        return out;
    }
```

- [ ] **Step 4: Run, verify pass** — `mvn -q -pl strata-server test -Dtest=ServerMetricsConfigTest` → PASS.

- [ ] **Step 5: Thread interval + buckets into `ServerMetrics`** —
  - `registerController(MeterRegistry, Controller, long refreshIntervalMs)` and `registerDataNode(MeterRegistry, DataNode, long refreshIntervalMs)`: replace the two `scheduleAtFixedRate(..., 0, 10, TimeUnit.SECONDS)` (lines 145, 206) with `scheduleAtFixedRate(..., 0, refreshIntervalMs, TimeUnit.MILLISECONDS)`.
  - `requestObserver(MeterRegistry reg, long[] bucketsMs)`: build the SLO list from `bucketsMs` instead of the inline 12-`Duration` list:
```java
            Timer.Builder b = Timer.builder("strata_scp_request_duration")
                    .description("request handler latency by opcode + namespace (incl. async durability wait)")
                    .tag("opcode", opcode).tag("status", status).tag("namespace", namespace);
            Duration[] slos = new Duration[bucketsMs.length];
            for (int i = 0; i < bucketsMs.length; i++) {
                slos[i] = Duration.ofMillis(bucketsMs[i]);
            }
            b.serviceLevelObjectives(slos);
            return b.register(reg);
```
  - Update the 3 `startMetrics(...)` registrar lambdas in `StrataServer` (runController 83-86, runDataNode 114-117, startCombined 197-203) to pass the env-derived values:
```java
            long nsRefreshMs = intEnv("STRATA_METRICS_NS_REFRESH_INTERVAL_MS", 10_000);
            long[] buckets = parseBucketsMs(env("STRATA_METRICS_REQUEST_DURATION_BUCKETS_MS", null),
                    new long[]{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000});
            // ... ServerMetrics.registerController(reg, service, nsRefreshMs);
            //     service.setRequestObserver(ServerMetrics.requestObserver(reg, buckets));
```
  (Hoist `nsRefreshMs`/`buckets` so all three lambdas can capture them, or compute inside each role method.)

- [ ] **Step 6: Run server suite** — `mvn -q -pl strata-server -am test` → green.

- [ ] **Step 7: Commit**

```bash
git add strata-server/src/main/java/io/strata/server/ServerMetrics.java \
        strata-server/src/main/java/io/strata/server/StrataServer.java \
        strata-server/src/test/java/io/strata/server/ServerMetricsConfigTest.java
git commit -m "feat(config): expose metrics ns-refresh interval + SLO buckets via STRATA_METRICS_* env"
```

---

## Final verification

### Task 17: Full build, integration suite, docs

**Files:**
- Modify: `deploy/` env docs / `docker-compose.yml` comments where operators realistically set the medium-severity knobs (verify interval, max-commands-per-heartbeat, control-call-timeout, max-frame-bytes, io-threads, scp inflight) — add the new `STRATA_*` vars as commented examples.
- Modify: `docs/superpowers/specs/2026-06-29-config-exposure-design.md` — flip Status to "Implemented".

- [ ] **Step 1: Full reactor build** — `mvn -q -T1C -DskipTests install` → success (all modules compile with the new ctor arities).

- [ ] **Step 2: Full test suite** — `mvn -q test` (or per-module) across `strata-common, strata-proto, strata-format, strata-meta, strata-node, strata-client, strata-server` → all green.

- [ ] **Step 3: Integration smoke** — run the `strata-it` suite (single JVM, watch for disk-flake per the known IT batching hazard): `mvn -q -pl strata-it test`. If a heavy IT class ERRORs on `SEAL_CHUNK_META` timeout, re-run that class alone to confirm it is disk saturation, not a regression.

- [ ] **Step 4: Grep for stragglers** — confirm the clean break removed every old path:
```bash
grep -rn 'Integer.getInteger("strata.scp\|Long.getLong("strata.groupcommit\|MAX_REQUEST_BYTES\|FETCH_CHUNK_BYTES\|CALL_TIMEOUT_MS\|VERIFY_INTERVAL_MS\|MAX_COMMANDS_PER_HEARTBEAT\|COPY_CHUNK_BYTES' --include='*.java' strata-*/src/main
```
Expected: no matches in `src/main` (all replaced by config accessors / `EnvConfig`).

- [ ] **Step 5: Update docs + commit**

```bash
git add deploy docker-compose.yml docs/superpowers/specs/2026-06-29-config-exposure-design.md
git commit -m "docs(config): document new STRATA_* tuning env vars; mark spec implemented"
```

---

## Self-Review notes

- **Spec coverage:** all 28 knobs / 29 fields from the spec map to Tasks 1–16 (Tier A: 1–11, Tier C: 12–15, Tier D: 16). The spec's `metadataReadChunkBytes` was re-homed from `ControllerConfig` to a `STRATA_CONTROLLER_LOG_*` setting (Task 5) to match its static `defaultBackendFactory` construction site; the connect-timeout was unified to one env shared by `ConnectionPolicy.DEFAULT` + `ScpClient` (Task 14). Both refinements are noted in the chat design summary and should be reflected back into the spec on Task 17.
- **Type consistency:** record accessor names match between producer (Tasks 1, 6, 8, 11) and consumer (Tasks 2–5, 7, 9–11) tasks; `ChunkStoreConfig` field/`with*` names are identical across Tasks 6, 7, 8.
- **Test-seam reality:** ZK/static-init knobs (Tasks 4, 5, 13, 15) use reflection/default-path assertions because env/`-D` can't be reset in-process; behavioral coverage is via the injection ctors that already exist (`ScpServer` 7-arg, `ChunkStore` config ctor, config records' `with*`). End-to-end env wiring can additionally be exercised with the `StrataServerStartupTest` subprocess pattern if desired.
