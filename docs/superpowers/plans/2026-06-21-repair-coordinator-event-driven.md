# Event-Driven Repair + Owner-Local Reconcile — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut the `RepairCoordinator`'s steady-state ZooKeeper read load (~24/s scan-driven `/strata` reads, growing with cluster size) by making repair event-driven + owner-local, without changing durability semantics.

**Architecture:** Split the single 5s repair loop into three lanes — an in-memory liveness/housekeeping tick (`STRATA_REPAIR_SCAN_INTERVAL_MS`, 5s), event-driven repair off the node-death signal `NodeRegistry.expireScan()` already returns, and a slow full reconcile (`STRATA_REPAIR_RECONCILE_INTERVAL_MS`, 60s) that is the correctness backstop. Unify the leader's `scanOnce` and the owners' `ownerRepairPass` into one owner-local `repairOwned()` (in-memory, no ZK) plus a leader-only `repairSystem()` (the meta-log files in ZK).

**Tech Stack:** Java 21, Maven multi-module, JUnit 5. Module: `strata-meta`. Build offline: `mvn -o -pl strata-meta -am test`.

## Global Constraints

- Durability semantics MUST NOT change: RF, ack quorum, target selection, the `REPLICATE`/fence path, and the `chunksBeingRepaired` dedup are reused unchanged. Worst-case repair time is gated by `leaseMs + deadGraceMs` (~6 min) and must stay so.
- Reuse, do not reimplement, the existing per-chunk repair helpers (`ownerRepairChunk`, `applyOwnerRepair`) and the per-file scan body.
- Project never ships to prod (see naming taxonomy): clean breaks, no back-compat aliases or env fallbacks.
- All identifiers stay on the controller/data-node taxonomy. Config keys: env `STRATA_*`, system prop `strata.*`.
- Build/run offline: `mvn -o`. `timeout` is unavailable on macOS — use background tasks for long runs.
- Each task ends green: `mvn -o -pl strata-meta -am test` passes before commit.

---

### Task 1: Add `reconcileIntervalMs` to `ControllerConfig` + env wiring

Adds the slow-reconcile cadence config. Unused this task (wired into the loop in Task 3); shippable on its own.

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/ControllerConfig.java` (record field at `:6-13`, convenience constructors at `:37-49`)
- Modify: `strata-server/src/main/java/io/strata/server/StrataServer.java` (where `STRATA_REPAIR_SCAN_INTERVAL_MS` is read with `intEnv(...)`)
- Test: `strata-meta/src/test/java/io/strata/meta/ControllerConfigDefaultsTest.java` (new)

**Interfaces:**
- Produces: `int ControllerConfig.reconcileIntervalMs()` (record accessor); default `60_000`.

- [ ] **Step 1: Write the failing test**

Create `strata-meta/src/test/java/io/strata/meta/ControllerConfigDefaultsTest.java`:

```java
package io.strata.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

class ControllerConfigDefaultsTest {
    @Test
    void reconcileIntervalDefaultsToSixtySeconds() {
        ControllerConfig c = new ControllerConfig("zk:2181", 9100, 3000, 60000, 300000, 5000, 30000);
        assertEquals(60_000, c.reconcileIntervalMs());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o -pl strata-meta -am test -Dtest=ControllerConfigDefaultsTest`
Expected: COMPILE FAILURE — `reconcileIntervalMs()` and the 7-arg constructor's defaulting do not exist yet.

- [ ] **Step 3: Add the field + default**

In `ControllerConfig.java`, add `int reconcileIntervalMs` to the record component list immediately after `int repairCommandTimeoutMs` (line `:13`). In the convenience constructor at `:37-40` (the 7-arg one used by tests/`StrataServer`), pass `60_000` for the new component in its delegating `this(...)` call. Do the same in any other convenience constructor (`:44-49`) and every `with*` method (`:53`, `:60`, `:71`, …) — thread the existing `reconcileIntervalMs` through unchanged. The canonical (all-args) constructor needs no body change beyond accepting the component.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -o -pl strata-meta -am test -Dtest=ControllerConfigDefaultsTest`
Expected: PASS.

- [ ] **Step 5: Wire the env var in `StrataServer`**

Where the controller config is built and `repairScanIntervalMs` is read via `intEnv("STRATA_REPAIR_SCAN_INTERVAL_MS", 5_000)`, the new component is supplied by the 7-arg convenience constructor's default (60_000) — but expose an override: if `StrataServer` constructs `ControllerConfig` via the all-args path, add `intEnv("STRATA_REPAIR_RECONCILE_INTERVAL_MS", 60_000)`. If it uses the 7-arg convenience constructor, add `.withReconcileIntervalMs(intEnv("STRATA_REPAIR_RECONCILE_INTERVAL_MS", 60_000))` and add that wither to `ControllerConfig` (mirror `withReplicaMissingGraceMs` at `:60-66`).

- [ ] **Step 6: Verify full module compiles + tests green, then commit**

Run: `mvn -o -pl strata-meta,strata-server -am test`
Expected: BUILD SUCCESS.

```bash
git add strata-meta/src/main/java/io/strata/meta/ControllerConfig.java \
        strata-server/src/main/java/io/strata/server/StrataServer.java \
        strata-meta/src/test/java/io/strata/meta/ControllerConfigDefaultsTest.java
git commit -m "feat(repair): add STRATA_REPAIR_RECONCILE_INTERVAL_MS config (default 60s)"
```

---

### Task 2: Leader repairs from in-memory liveness; cache `listNamespaces` per pass — SUPERSEDED (no-op)

**Status: verified already-satisfied during execution; no code change.** The leader's `scanOnce` already decides liveness via in-memory `registry.isDead()` (`RepairCoordinator.java:234`), never `store.listNodes()`, and `listNamespaces()` is already called once per pass (`:202`, `:344`). The only `store.listNodes()` (ZK) is in `ownerRepairPass` (`:339`), where it builds the **DEAD** set — which the published cluster-live-nodes snapshot is alive-only and structurally cannot supply, so it cannot be removed standalone. The owner-liveness ZK win is realized instead by Task 3 (the owner reconcile drops 5s→60s, 12× fewer reads) and Task 5 (event-driven owner repair via the alive-snapshot delta — a node leaving the alive set is the death signal — removing the per-pass DEAD-set read for the common case). Skipped; proceed to Task 3.

_Original (no longer applicable):_ Removes the per-pass ZK `listNodes` read from the leader's repair decision and the repeated `listNamespaces` ZK call. Behavior-preserving.

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/RepairCoordinator.java` (`ownerRepairPass` `:334-360`, `allFileIds` `:200-206`)
- Test: `strata-meta/src/test/java/io/strata/meta/RepairCoordinatorTest.java` (add one test)

**Interfaces:**
- Consumes: `NodeRegistry.aliveNodes()` (existing, `:402`) and `NodeRegistry.candidatesFor(StrataNamespace)` (existing) for the in-memory live view.
- Produces: no new public surface; `ownerRepairPass`/`scanOnce` use `registry.aliveNodes()` for the leader instead of `store.listNodes()`.

- [ ] **Step 1: Write the failing test (repair still fires using in-memory liveness)**

In `RepairCoordinatorTest.java`, mirror the existing under-replication test pattern (`:55-65`) but assert the repair is issued without the test's store recording a `listNodes` call. Use the existing `register(registry, id, host)` + a `CountingStore` spy if present, else assert behavior:

```java
@Test
void leaderRepairsUnderReplicatedChunkWithoutReadingNodeRegistryFromStore() throws Exception {
    Registered source = register(registry, 1010, "source");
    Registered target = register(registry, 1011, "target");
    FileId fileId = createSealedFileWithReplicasOn(source);   // existing helper pattern in this test
    killNode(registry, source);                                // lease-expire + expireScan -> DEAD
    long storeListNodesBefore = store.listNodesCallCount();    // add a counter to the test store (Step 3)
    new RepairCoordinator(store, registry, config(), () -> true).scanOnce();
    assertReplicateIssuedTo(target);                           // existing assertion pattern
    assertEquals(storeListNodesBefore, store.listNodesCallCount());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o -pl strata-meta -am test -Dtest=RepairCoordinatorTest#leaderRepairsUnderReplicatedChunkWithoutReadingNodeRegistryFromStore`
Expected: FAIL — `store.listNodesCallCount()` increases (the pass still reads `listNodes` from the store) or helper missing.

- [ ] **Step 3: Add a `listNodes` counter to the test store**

In the in-memory `MetadataStore` test double used by `RepairCoordinatorTest`, increment a counter in `listNodes()` and expose `long listNodesCallCount()`. (If the test uses `ZkMetadataStore` against a test ZK, instead assert via a wrapping spy `MetadataStore` that delegates and counts.)

- [ ] **Step 4: Switch the leader path to in-memory liveness**

In `ownerRepairPass` (`:334-360`) replace the live-node source `store.listNodes()` with `registry.aliveNodes()` (in-memory; `:402`). In `scanOnce` and `ownerRepairPass`, call `store.listNamespaces()` **once** at the top of the pass and reuse the result. Leave non-leader owners reading `candidatesFor(ns)` (which already falls back to the cached snapshot) unchanged.

- [ ] **Step 5: Run the focused test + full repair suite**

Run: `mvn -o -pl strata-meta -am test -Dtest=RepairCoordinatorTest,RepairReliabilityTest`
Expected: PASS (all existing repair tests stay green).

- [ ] **Step 6: Commit**

```bash
git add strata-meta/src/main/java/io/strata/meta/RepairCoordinator.java \
        strata-meta/src/test/java/io/strata/meta/RepairCoordinatorTest.java
git commit -m "perf(repair): leader repairs from in-memory NodeRegistry; cache listNamespaces per pass"
```

---

### Task 3: Split the loop into a light tick + a slow reconcile

Introduces Lane A (in-memory housekeeping at `repairScanIntervalMs`) and Lane C (full reconcile at `reconcileIntervalMs`). Repair correctness still rests entirely on the (now slower) reconcile — no event path yet.

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/RepairCoordinator.java` (`scanLoop` `:158-194`)
- Test: `strata-meta/src/test/java/io/strata/meta/RepairCoordinatorLoopTest.java` (new)

**Interfaces:**
- Produces: `void RepairCoordinator.tick()` (in-memory housekeeping: `publishClusterLiveNodes` + `expireScan` + `sweepStuckCommands`, leader-gated) and the existing `scanOnce()` repurposed as the reconcile body. `scanLoop` runs `tick()` every `repairScanIntervalMs` and `scanOnce()`/`ownerRepairPass()` every `reconcileIntervalMs`.

- [ ] **Step 1: Write the failing test (reconcile runs on the slow cadence, tick on the fast one)**

Create `strata-meta/src/test/java/io/strata/meta/RepairCoordinatorLoopTest.java`:

```java
package io.strata.meta;

import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RepairCoordinatorLoopTest {
    @Test
    void tickRunsEachIntervalReconcileRunsOnReconcileInterval() throws Exception {
        AtomicInteger ticks = new AtomicInteger();
        AtomicInteger reconciles = new AtomicInteger();
        // Subclass/seam: count tick() vs reconcile() invocations over ~3 fast intervals
        // with reconcileIntervalMs = 3 * repairScanIntervalMs.
        RepairCoordinator coord = newCountingCoordinator(ticks, reconciles, /*scanMs*/ 20, /*reconcileMs*/ 60);
        coord.start();
        Thread.sleep(140);                  // ~7 ticks, ~2 reconciles
        coord.close();
        assertTrue(ticks.get() >= 5, "ticks=" + ticks.get());
        assertTrue(reconciles.get() >= 1 && reconciles.get() <= 3, "reconciles=" + reconciles.get());
    }
}
```

Add a protected seam in `RepairCoordinator` so the test can count: extract `tick()` and `reconcile()` as package-private methods the loop calls, and let `newCountingCoordinator` wrap them (or count via a `MultiGauge`/counter the test reads).

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o -pl strata-meta -am test -Dtest=RepairCoordinatorLoopTest`
Expected: FAIL — `tick()`/`reconcile()` seam does not exist; loop still runs everything every `repairScanIntervalMs`.

- [ ] **Step 3: Restructure `scanLoop`**

Rewrite `scanLoop` (`:158-194`) to:

```java
private void scanLoop() {
    long lastReconcile = 0;
    while (!closed.get()) {
        try {
            Thread.sleep(config.repairScanIntervalMs());
            tick();                                   // Lane A: in-memory housekeeping (leader-gated inside)
            long now = System.currentTimeMillis();
            if (now - lastReconcile >= config.reconcileIntervalMs()) {
                lastReconcile = now;
                reconcile();                          // Lane C: full backstop pass
            }
        } catch (InterruptedException e) {
            return;
        } catch (Exception e) {
            if (!closed.get()) log.warn("repair loop failed", e);
        }
    }
}

void tick() {
    if (!isLeader.getAsBoolean()) { leaderSince = 0; return; }
    if (leaderSince == 0) leaderSince = System.currentTimeMillis();
    registry.publishClusterLiveNodes();
    if (System.currentTimeMillis() - leaderSince < config.leaseMs() + config.deadGraceMs()) return;
    registry.expireScan();
    sweepStuckCommands();
}

void reconcile() throws Exception {
    if (isLeader.getAsBoolean()) {
        if (System.currentTimeMillis() - leaderSince < config.leaseMs() + config.deadGraceMs()) return;
        scanOnce();
        store.sweepDeletedFiles(DELETED_TOMBSTONE_TTL_MS);
    } else {
        ownerRepairPass();
    }
}
```

Move `sweepStuckCommands()` out of `scanOnce` into `tick()` (it is in-memory; keep it on the fast cadence). Leave `scanOnce`/`ownerRepairPass` bodies otherwise intact for now.

- [ ] **Step 4: Run the loop test + repair regression**

Run: `mvn -o -pl strata-meta -am test -Dtest=RepairCoordinatorLoopTest,RepairCoordinatorTest,RepairReliabilityTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add strata-meta/src/main/java/io/strata/meta/RepairCoordinator.java \
        strata-meta/src/test/java/io/strata/meta/RepairCoordinatorLoopTest.java
git commit -m "refactor(repair): split loop into in-memory tick + slow reconcile backstop"
```

---

### Task 4: Lane B — leader event-driven repair off `expireScan()` deaths

`expireScan()` already returns newly-dead node ids. Feed them to a targeted repair so a node death triggers repair immediately instead of waiting for the 60s reconcile.

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/RepairCoordinator.java` (`tick()` from Task 3; add `repairForDeadNode(int)` + a single-thread `repairEventExecutor`)
- Test: `strata-meta/src/test/java/io/strata/meta/RepairCoordinatorEventTest.java` (new)

**Interfaces:**
- Consumes: `List<Integer> NodeRegistry.expireScan()` (existing, `:324`).
- Produces: `void repairForDeadNode(int deadNodeId)` — enumerates owned namespaces' files in memory (+ system files via ZK when leader), finds chunks with a replica on `deadNodeId`, and issues `REPLICATE` via the existing per-chunk path, deduped against `chunksBeingRepaired`.

- [ ] **Step 1: Write the failing test (node death triggers repair without a reconcile)**

Create `strata-meta/src/test/java/io/strata/meta/RepairCoordinatorEventTest.java`:

```java
package io.strata.meta;

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class RepairCoordinatorEventTest {
    @Test
    void nodeDeathTriggersTargetedRepairBeforeAnyReconcile() throws Exception {
        // Build a sealed file with replicas on {source, other}; register a spare target.
        NodeRegistry registry = newRegistry();
        var store = newStore();
        Registered source = register(registry, 2010, "source");
        Registered other  = register(registry, 2011, "other");
        Registered target = register(registry, 2012, "target");
        FileId fileId = createSealedFile(store, source, other);
        RepairCoordinator coord = new RepairCoordinator(store, registry, config(), () -> true);
        coord.becomeLeaderForTest();                 // sets leaderSince past settle for the test
        expire(registry, source);                    // lease-expire source
        // Drive ONE tick: expireScan() returns [source], event repair fires; no reconcile() called.
        coord.tick();
        assertTrue(replicateIssuedFor(store, fileId, target.nodeId()),
                "expected event-driven REPLICATE to the spare target");
    }
}
```

(Reuse the file/replica/assertion helpers already in `RepairCoordinatorTest`; extract them to a shared `RepairTestSupport` if needed.)

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o -pl strata-meta -am test -Dtest=RepairCoordinatorEventTest`
Expected: FAIL — `tick()` calls `expireScan()` but discards its return; no event repair happens.

- [ ] **Step 3: Consume the death list in `tick()` and add `repairForDeadNode`**

In `tick()`, capture the return: `List<Integer> dead = registry.expireScan(); for (int id : dead) repairEventExecutor.submit(() -> repairForDeadNodeSafe(id));`. Add:

```java
private final ExecutorService repairEventExecutor = Executors.newSingleThreadExecutor(
        Thread.ofVirtual().name("repair-event-", 0).factory());

private void repairForDeadNodeSafe(int deadNodeId) {
    try { repairForDeadNode(deadNodeId); }
    catch (Exception e) { log.warn("event repair for dead node {} failed — reconcile backstops", deadNodeId, e); }
}

void repairForDeadNode(int deadNodeId) throws Exception {
    if (!isLeader.getAsBoolean()) return;                        // owners handled in Task 5
    if (System.currentTimeMillis() - leaderSince < config.leaseMs() + config.deadGraceMs()) return;
    for (StrataNamespace ns : store.listNamespaces()) {          // loaded (owned) + system
        if (!ownsAll.getAsBoolean() && !ownsNamespace.test(ns) && !NamespaceLogBackend.isSystem(ns)) continue;
        for (FileId fileId : store.listFiles(ns)) {
            var opt = store.getFile(fileId);
            if (opt.isEmpty()) continue;
            repairFileChunksOnNode(opt.get(), deadNodeId);       // reuse the per-chunk repair body
        }
    }
}
```

`repairFileChunksOnNode` extracts the existing per-chunk under-replication branch from `scanOnce`/`ownerRepairChunk` (`:363`), filtered to chunks whose replica set contains `deadNodeId`. Reuse `chunksBeingRepaired` and `applyOwnerRepair` verbatim. Shut down `repairEventExecutor` in `close()` (mirror `awaitTermination` at `:864`).

- [ ] **Step 4: Run the event test + repair regression**

Run: `mvn -o -pl strata-meta -am test -Dtest=RepairCoordinatorEventTest,RepairCoordinatorTest,RepairReliabilityTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add strata-meta/src/main/java/io/strata/meta/RepairCoordinator.java \
        strata-meta/src/test/java/io/strata/meta/RepairCoordinatorEventTest.java
git commit -m "feat(repair): event-driven repair on node death (leader), reconcile backstops"
```

---

### Task 5: Lane B — owner event repair from the live-nodes snapshot delta

Non-leader owners have no heartbeat channel; they detect death by a node disappearing from the published cluster-live-nodes snapshot they already read.

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/RepairCoordinator.java` (`tick()` owner branch)
- Modify: `strata-meta/src/main/java/io/strata/meta/NodeRegistry.java` (expose the current snapshot node-id set if not already reachable; `currentSnapshot` `:467`)
- Test: `strata-meta/src/test/java/io/strata/meta/RepairCoordinatorEventTest.java` (add a non-leader case)

**Interfaces:**
- Consumes: the snapshot node-id set via `registry.snapshotNodeIds()` (new thin accessor over `currentSnapshot`).
- Produces: owner `tick()` computes the set-difference vs the previous tick's snapshot ids; each removed id → `repairForDeadNodeOwned(id)` (owned user namespaces only, in-memory).

- [ ] **Step 1: Write the failing test (owner repairs when a node leaves the snapshot)**

Add to `RepairCoordinatorEventTest`:

```java
@Test
void ownerRepairsWhenNodeLeavesPublishedSnapshot() throws Exception {
    NodeRegistry registry = newRegistry();
    var store = newStore();
    Registered source = register(registry, 2110, "source");
    Registered other  = register(registry, 2111, "other");
    Registered target = register(registry, 2112, "target");
    FileId fileId = createSealedFileInOwnedNamespace(store, source, other);   // ns owned by THIS controller
    // Non-leader owner: isLeader=false, ownsNamespace=true for the file's namespace.
    RepairCoordinator owner = new RepairCoordinator(store, registry, config(),
            () -> false, () -> false, ns -> ns.equals(ownedNamespace()));
    publishSnapshotWith(store, source, other, target);   // tick 1 sees all three
    owner.tick();
    publishSnapshotWith(store, other, target);           // source gone from snapshot
    owner.tick();                                         // tick 2 sees source removed -> repair
    assertTrue(replicateIssuedFor(store, fileId, target.nodeId()));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o -pl strata-meta -am test -Dtest=RepairCoordinatorEventTest#ownerRepairsWhenNodeLeavesPublishedSnapshot`
Expected: FAIL — owner `tick()` currently returns early for non-leaders and does no delta detection.

- [ ] **Step 3: Add `snapshotNodeIds()` and owner delta logic**

In `NodeRegistry`, add `Set<Integer> snapshotNodeIds()` returning the node ids in `currentSnapshot(now)` (empty if none). In `RepairCoordinator.tick()`, replace the non-leader early-return with:

```java
if (!isLeader.getAsBoolean()) {
    leaderSince = 0;
    Set<Integer> nowIds = registry.snapshotNodeIds();
    if (prevSnapshotIds != null) {
        for (int gone : prevSnapshotIds) {
            if (!nowIds.contains(gone)) repairEventExecutor.submit(() -> repairForDeadNodeOwnedSafe(gone));
        }
    }
    prevSnapshotIds = nowIds;
    return;
}
prevSnapshotIds = null;   // reset when we become leader
```

Add `repairForDeadNodeOwned(int)` = the same body as `repairForDeadNode` but skipping the system namespace (owners never repair system files) and gated by `ownsNamespace` only. Add the `private volatile Set<Integer> prevSnapshotIds;` field.

- [ ] **Step 4: Run the event test (both leader + owner) + regression**

Run: `mvn -o -pl strata-meta -am test -Dtest=RepairCoordinatorEventTest,RepairCoordinatorTest,RepairReliabilityTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add strata-meta/src/main/java/io/strata/meta/RepairCoordinator.java \
        strata-meta/src/main/java/io/strata/meta/NodeRegistry.java \
        strata-meta/src/test/java/io/strata/meta/RepairCoordinatorEventTest.java
git commit -m "feat(repair): owner event repair from live-nodes snapshot delta"
```

---

### Task 6: Unify `scanOnce`/`ownerRepairPass` into `repairOwned()` + `repairSystem()`

Collapse the two reconcile bodies so each controller reconciles only the namespaces it owns (in-memory), and the leader additionally reconciles the system meta-log files (ZK). Removes the leader's redundant re-scan of its own user namespaces.

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/RepairCoordinator.java` (`reconcile` from Task 3, `scanOnce` `:209`, `ownerRepairPass` `:334`)
- Test: `strata-meta/src/test/java/io/strata/meta/RepairCoordinatorTest.java` + `RepairReliabilityTest.java` (regression only)

**Interfaces:**
- Produces: `void repairOwned()` (in-memory reconcile over `store.listNamespaces()` filtered by `ownsAll`/`ownsNamespace`, excluding system) and `void repairSystem()` (leader-only, reconciles `NamespaceLogBackend.isSystem(ns)` namespaces via ZK). `reconcile()` calls `repairOwned()` always and `repairSystem()` when leader.

- [ ] **Step 1: Write the failing test (a non-owned namespace is not reconciled by this controller)**

```java
@Test
void reconcileSkipsNamespacesThisControllerDoesNotOwn() throws Exception {
    // Two namespaces A (owned) and B (not owned); both have an under-replicated sealed chunk.
    RepairCoordinator coord = new RepairCoordinator(store, registry, config(),
            () -> true, () -> false, ns -> ns.equals(nsA()));   // leader, but only owns A
    coord.becomeLeaderForTest();
    coord.reconcile();
    assertTrue(replicateIssuedForNamespace(store, nsA()));
    assertFalse(replicateIssuedForNamespace(store, nsB()));     // B left to its owner
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o -pl strata-meta -am test -Dtest=RepairCoordinatorTest#reconcileSkipsNamespacesThisControllerDoesNotOwn`
Expected: FAIL — current `scanOnce` (leader) reconciles every namespace it can enumerate, not just owned ones.

- [ ] **Step 3: Split the reconcile body**

Extract from `scanOnce` (`:209-330`): the per-file/per-chunk under-replication census + repair into a helper `reconcileNamespace(StrataNamespace ns)`. Define:

```java
void repairOwned() throws Exception {
    for (StrataNamespace ns : store.listNamespaces()) {
        if (NamespaceLogBackend.isSystem(ns)) continue;
        if (!ownsAll.getAsBoolean() && !ownsNamespace.test(ns)) continue;
        reconcileNamespace(ns);                 // in-memory: listFiles/getFile resolve from the meta-log
    }
    publishDurabilityGauges();                  // census aggregated across reconciled namespaces
}

void repairSystem() throws Exception {          // leader only
    for (StrataNamespace ns : store.listNamespaces()) {
        if (NamespaceLogBackend.isSystem(ns)) reconcileNamespace(ns);   // ZK-backed meta-log files
    }
}
```

Update `reconcile()` (Task 3) to call `repairOwned()` always, and `repairSystem()` + `store.sweepDeletedFiles(...)` when leader. Delete the now-dead `scanOnce`/`ownerRepairPass`/`allFileIds` once nothing references them (update `RepairCoordinatorTest` call sites that used `scanOnce()` to call `reconcile()` or `repairOwned()` as appropriate — they are leader+owns-all in those tests, so `reconcile()` preserves behavior).

- [ ] **Step 4: Run the full repair suite (all existing tests must stay green)**

Run: `mvn -o -pl strata-meta -am test`
Expected: BUILD SUCCESS — `RepairCoordinatorTest`, `RepairReliabilityTest`, `ControllerTest`, `NamespaceLogBackendDurabilityTest` all pass.

- [ ] **Step 5: Commit**

```bash
git add strata-meta/src/main/java/io/strata/meta/RepairCoordinator.java \
        strata-meta/src/test/java/io/strata/meta/RepairCoordinatorTest.java
git commit -m "refactor(repair): unify scanOnce/ownerRepairPass into repairOwned + repairSystem"
```

---

### Task 7: Trigger metrics + end-to-end verification

Adds observability (repairs by trigger) and proves the whole change end to end against the live cluster.

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/RepairCoordinator.java` (counters)
- Modify: `strata-server/src/main/java/io/strata/server/ServerMetrics.java` (expose the counters)
- Test: `strata-meta/src/test/java/io/strata/meta/RepairCoordinatorEventTest.java` (assert counter increments)

**Interfaces:**
- Produces: `long RepairCoordinator.eventRepairs()` and `long reconcileRepairs()` — monotonic counters incremented where a `REPLICATE` is issued via the event path vs the reconcile path.

- [ ] **Step 1: Write the failing test**

```java
@Test
void eventRepairIncrementsEventCounterNotReconcileCounter() throws Exception {
    RepairCoordinator coord = /* leader, as in nodeDeathTriggersTargetedRepair */;
    long before = coord.eventRepairs();
    coord.tick();                                  // drives the node-death event repair
    assertTrue(coord.eventRepairs() > before);
    assertEquals(0, coord.reconcileRepairs());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o -pl strata-meta -am test -Dtest=RepairCoordinatorEventTest#eventRepairIncrementsEventCounterNotReconcileCounter`
Expected: FAIL — counters do not exist.

- [ ] **Step 3: Add the counters + metric registration**

Add `private final AtomicLong eventRepairs = new AtomicLong(); private final AtomicLong reconcileRepairs = new AtomicLong();` and accessors. Increment the right counter where `applyOwnerRepair`/`REPLICATE` is issued (pass a `trigger` flag into the shared repair helper). In `ServerMetrics.java` register `FunctionCounter.builder("strata_repair_actions", s, Controller::eventRepairs).tag("trigger","event")...` and a matching `reconcile` series (mirror the existing `strata_repair_*` registrations).

- [ ] **Step 4: Run the test + full module**

Run: `mvn -o -pl strata-meta,strata-server -am test`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add strata-meta/src/main/java/io/strata/meta/RepairCoordinator.java \
        strata-server/src/main/java/io/strata/server/ServerMetrics.java \
        strata-meta/src/test/java/io/strata/meta/RepairCoordinatorEventTest.java
git commit -m "feat(repair): metrics for event vs reconcile repairs"
```

- [ ] **Step 6: End-to-end verification on the live cluster**

Rebuild + redeploy fresh (the disk-fill hazard means use bounded runs):

```bash
mvn -o -q -pl strata-server -am -DskipTests package && docker build -t strata-server:local .
docker compose down -v --remove-orphans && docker compose up -d
```

Wait for readiness, then measure idle ZK reads (expect ~24/s → ~2–3/s):

```bash
curl -s 'http://localhost:9090/api/v1/query?query=rate(read_per_namespace_count%7Bkey%3D%22strata%22%7D%5B1m%5D)'
```

Then kill a data node and confirm replicas restore (event-driven) while ZK reads stay bounded:

```bash
docker compose stop node3
# poll strata_chunks_under_replicated -> returns to 0; read_per_namespace_count stays low
```

Expected: idle `/strata` reads drop to ~2–3/s; under-replication clears after a node death without a ZK read spike. Record results; no commit (verification only).

---

## Notes for the implementer

- The Bash tool's text output garbles long identifiers in this repo (renders them as `n`); use the Read tool to view exact code, line numbers from grep are reliable.
- `NamespaceLogBackend.isSystem(StrataNamespace)` already exists (`Controller.java:387` calls it) — use it to distinguish the meta-log system namespace from user namespaces.
- Keep the settle gate (`leaseMs + deadGraceMs` after acquiring leadership) on every repair entry point (tick event path, reconcile) — it prevents spurious repairs while nodes re-register after a leader change.
- Durability backstop invariant: at every stage, `reconcile()` alone must restore full RF even if the event path does nothing. Do not let any task make the event path load-bearing for correctness.
