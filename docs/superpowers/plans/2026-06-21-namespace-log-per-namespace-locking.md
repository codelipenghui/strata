# Per-Namespace Meta-Log Locking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove cross-namespace head-of-line blocking in the namespace-log write path so a slow meta-log append in one namespace no longer stalls mutations in the controller's other owned namespaces.

**Architecture:** Replace `NamespaceLogBackend`'s single global `ReentrantLock` (held across the blocking durable append) with: a thread-safe lazy `repo()` (ConcurrentHashMap `repos` + a small `repoCreateLock`), and a per-namespace `ReentrantLock` living on `NamespaceMetadataLogRepository` that each mutation/correctness-read acquires. Within a namespace, ops still serialize under that namespace's lock across the durable append — single-writer ordering and apply-after-durable are unchanged.

**Tech Stack:** Java 21, Maven multi-module, JUnit 5. Module: `strata-meta`. Build offline: `mvn -o -pl strata-meta -am test`.

## Global Constraints

- **No change to durability semantics:** a mutation is acked only after its record is durable on the ack quorum; recovery replays the durable log; single-writer-per-namespace ordering preserved (TLA `MetadataByteDurability`/`MetadataManifestCAS` unaffected). No speculative apply.
- Within a namespace, ops MUST still serialize under that namespace's lock held **across** the durable append (`repo.append`) — same as today, just no longer a *global* lock.
- The **system namespace** path MUST stay lock-free (a user mutation's meta-log append self-loops into this engine for the system file; taking any namespace lock there would deadlock). Do not add namespace locking to the `isSystem(...) → root.*` branches.
- The Bash tool garbles long identifiers in this repo's output (renders as `n`); use the Read tool for real code, grep line numbers are reliable.
- Each task ends green: `mvn -o -pl strata-meta -am test`.

---

### Task 1: Thread-safe lazy `repo()` (decouple repo creation from the global lock)

Make `repo()` self-thread-safe so repo lifecycle no longer depends on the global `lock`. Behavior is unchanged this task — the mutation/read sites still hold the global `lock` (which still serializes everything); this only changes *how repos are created*. Regression must stay green.

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/NamespaceLogBackend.java` (`repos` field `:53`, `repo()` `:105-116`, `warmOwnedNamespaces()` `:186-201`)

**Interfaces:**
- Produces: `private NamespaceMetadataLogRepository repo(StrataNamespace)` is now safe to call without holding any other lock (ConcurrentHashMap + `repoCreateLock` double-checked). Task 2 relies on this.

- [ ] **Step 1: Make `repos` concurrent + add `repoCreateLock`**

In `NamespaceLogBackend.java`, change the `repos` field (`:53`) and add a create-lock next to it:

```java
private final java.util.concurrent.ConcurrentHashMap<StrataNamespace, NamespaceMetadataLogRepository> repos =
        new java.util.concurrent.ConcurrentHashMap<>();
private final ReentrantLock repoCreateLock = new ReentrantLock();
```

(Remove the old `private final Map<StrataNamespace, NamespaceMetadataLogRepository> repos = new HashMap<>();` and the now-unused `HashMap`/`Map` imports if nothing else needs them — leave `Map` if other methods still reference the type.)

- [ ] **Step 2: Rewrite `repo()` as thread-safe double-checked lazy create**

Replace `repo()` (`:105-116`) with:

```java
private NamespaceMetadataLogRepository repo(StrataNamespace namespace) throws Exception {
    NamespaceMetadataLogRepository r = repos.get(namespace);   // fast path, lock-free
    if (r != null) {
        return r;
    }
    repoCreateLock.lock();
    try {
        r = repos.get(namespace);
        if (r == null) {
            long epoch = root.allocateMetadataEpoch();
            r = NamespaceMetadataLogRepository.open(namespace, fileStore, root, epoch, metrics);
            for (FileId id : r.state().liveFiles()) {
                fileIndex.put(id, namespace);
            }
            repos.put(namespace, r);   // publish only after recovery + fileIndex populated
        }
        return r;
    } finally {
        repoCreateLock.unlock();
    }
}
```

Rationale to keep in mind: recovery (`open` → recover/compact/CAS-publish) is heavy but runs once per namespace; serializing it on `repoCreateLock` keeps it out of a `computeIfAbsent` bin lock, and the system-file self-loop inside `open` routes to `root` (lock-free), so it cannot deadlock on `repoCreateLock`.

- [ ] **Step 3: Make `warmOwnedNamespaces()` use `repoCreateLock`**

Replace the `lock.lock()/unlock()` in `warmOwnedNamespaces()` (`:187`,`:199`) with `repoCreateLock.lock()/unlock()` (the body — the double-checked `ownedNamespacesWarmed` guard + the `repo(ns)` loop — is unchanged). This keeps the one-time bulk recover on the create-lock rather than the global mutation lock.

- [ ] **Step 4: Build + run the namespace-log suites (behavior unchanged → all green)**

Run: `mvn -o -pl strata-meta -am test -Dtest=NamespaceLogMetadataStoreConformanceTest,NamespaceMetadataLogRepositoryTest,NamespaceLogBackendDurabilityTest,ControllerNamespaceLogBackendTest`
Expected: PASS (no behavior change — the global `lock` still serializes the sites; only repo creation moved to `repoCreateLock`).

- [ ] **Step 5: Commit**

```bash
git add strata-meta/src/main/java/io/strata/meta/NamespaceLogBackend.java
git commit -m "refactor(meta): thread-safe lazy repo() via ConcurrentHashMap + repoCreateLock"
```

---

### Task 2: Per-namespace mutation lock (the head-of-line fix)

Add a `ReentrantLock` to `NamespaceMetadataLogRepository`; switch every user-namespace mutation/correctness-read site from the global `lock` to that namespace's `repo.lock()`; make the best-effort metric reads lock-free over the ConcurrentHashMap; remove the global `lock`.

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/NamespaceMetadataLogRepository.java` (add the lock + accessors near `:23-46`)
- Modify: `strata-meta/src/main/java/io/strata/meta/NamespaceLogBackend.java` (all `lock.lock()/unlock()` sites; remove the `lock` field `:52`)
- Test: `strata-meta/src/test/java/io/strata/meta/NamespaceLogPerNamespaceLockTest.java` (new)

**Interfaces:**
- Consumes: the thread-safe `repo(StrataNamespace)` from Task 1.
- Produces: `void NamespaceMetadataLogRepository.lock()` / `void unlock()` (delegate to a private `ReentrantLock`). The `NamespaceLogBackend.lock` field is removed.

- [ ] **Step 1: Write the failing concurrency test**

Create `strata-meta/src/test/java/io/strata/meta/NamespaceLogPerNamespaceLockTest.java`. It wraps a real in-memory file store with a latch that blocks ONE namespace's log append, and asserts a mutation on a DIFFERENT namespace still completes:

```java
package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

class NamespaceLogPerNamespaceLockTest {

    // Wraps a delegate file store; blocks appendLog for exactly one log fileId until released.
    static final class BlockingFileStore implements NamespaceMetadataFileStore {
        final NamespaceMetadataFileStore delegate;
        final CountDownLatch block = new CountDownLatch(1);
        final CountDownLatch entered = new CountDownLatch(1);
        volatile FileId blockLogId;            // set to namespace A's log file id
        BlockingFileStore(NamespaceMetadataFileStore d) { this.delegate = d; }
        public FileId createLogFile() throws Exception { return delegate.createLogFile(); }
        public void appendLog(FileId logFileId, byte[] frame) throws Exception {
            if (logFileId.equals(blockLogId)) { entered.countDown(); block.await(); }
            delegate.appendLog(logFileId, frame);
        }
        public byte[] readLog(FileId id) throws Exception { return delegate.readLog(id); }
        public FileId writeSnapshot(byte[] bytes) throws Exception { return delegate.writeSnapshot(bytes); }
        public byte[] readSnapshot(FileId id) throws Exception { return delegate.readSnapshot(id); }
        public void deleteFile(FileId id) throws Exception { delegate.deleteFile(id); }
        public void close() throws Exception { delegate.close(); }
    }

    @Test
    void slowAppendInOneNamespaceDoesNotBlockAnother() throws Exception {
        StrataNamespace nsA = StrataNamespace.of("ns-a");
        StrataNamespace nsB = StrataNamespace.of("ns-b");
        var root = NamespaceLogTestSupport.inMemoryRoot();          // existing helper used by the conformance test
        var blocking = new BlockingFileStore(NamespaceLogTestSupport.inMemoryFileStore(root));
        try (var backend = new NamespaceLogBackend(root, blocking, true)) {
            // Warm A so we know its log file id, then arm the block on A's log.
            FileId fA = FileId.random();
            // first create on A creates A's repo+log; capture the log id via the backend's stats/repo
            blocking.blockLogId = NamespaceLogTestSupport.openLogFileId(backend, nsA); // helper: ensures repo, returns its logFileId

            CompletableFuture<Void> aCreate = CompletableFuture.runAsync(() -> sneaky(() ->
                backend.createFile(NamespaceLogTestSupport.fileRecord(fA, nsA, StrataPath.of("/a")))));
            assertTrue(blocking.entered.await(2, TimeUnit.SECONDS), "A's append should be in flight");

            // While A is blocked mid-append (holding A's lock), B must proceed.
            FileId fB = FileId.random();
            CompletableFuture<Void> bCreate = CompletableFuture.runAsync(() -> sneaky(() ->
                backend.createFile(NamespaceLogTestSupport.fileRecord(fB, nsB, StrataPath.of("/b")))));
            bCreate.get(3, TimeUnit.SECONDS);   // FAILS (times out) under the global lock; PASSES per-namespace

            blocking.block.countDown();
            aCreate.get(5, TimeUnit.SECONDS);
        }
    }

    interface ThrowingRunnable { void run() throws Exception; }
    static void sneaky(ThrowingRunnable r) { try { r.run(); } catch (Exception e) { throw new RuntimeException(e); } }
}
```

If the helpers (`NamespaceLogTestSupport.inMemoryRoot/inMemoryFileStore/fileRecord/openLogFileId`) do not already exist, add a small package-private `NamespaceLogTestSupport` mirroring how `NamespaceLogMetadataStoreConformanceTest` constructs its root + file store + file records (Read that test first and reuse its exact construction). Keep the test deterministic — it must block on the latch, never sleep.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o -pl strata-meta -am test -Dtest=NamespaceLogPerNamespaceLockTest`
Expected: FAIL — `bCreate.get(3s)` times out, because namespace A holds the single global `lock` while blocked in `appendLog`, so B cannot acquire it.

- [ ] **Step 3: Add the per-namespace lock to `NamespaceMetadataLogRepository`**

In `NamespaceMetadataLogRepository.java`, add the field (near `:28`) and accessors:

```java
private final java.util.concurrent.locks.ReentrantLock lock = new java.util.concurrent.locks.ReentrantLock();

/** Acquires this namespace's mutation lock (held across the durable append; single-writer ordering). */
void lock() { lock.lock(); }
void unlock() { lock.unlock(); }
```

- [ ] **Step 4: Switch the mutation/correctness-read sites to the per-namespace lock**

In `NamespaceLogBackend.java`, for each method below, replace the `lock.lock(); try { ... repo(ns) ... } finally { lock.unlock(); }` with: resolve the repo first, then lock it. The uniform pattern:

```java
// BEFORE:
lock.lock();
try {
    NamespaceMetadataLogRepository repo = repo(record.namespace());
    ... work using repo / repo.state() ...
} finally {
    lock.unlock();
}
// AFTER:
NamespaceMetadataLogRepository repo = repo(record.namespace());
repo.lock();
try {
    ... work using repo / repo.state() ...   // body unchanged
} finally {
    repo.unlock();
}
```

Apply to (resolve the repo before locking, body unchanged): **`createFile`** (`:123`), **`lockedFile`** (`:172`, repo is `repo(ns)`), **`resolvePath`** (`:207`), **`updateFile`** (`:219`), **`deletePath`** (`:240`), **`deleteFile`** (`:263`, repo is `repo(ns)`), **`listFiles`** (`:289`).

- [ ] **Step 5: Make the best-effort metric reads lock-free, and sweep per-repo**

`loadedNamespaceCount` (`:77-84`), `namespaceStats` (`:87-98`), `listNamespaces` (`:297-311`): remove the `lock.lock()/unlock()` and iterate `repos` directly — `repos` is a `ConcurrentHashMap`, so `.size()` / `entrySet()` iteration is safe; these are best-effort gauges/listings where a momentarily-stale per-namespace read is acceptable.

`sweepDeletedFiles` (`:313-329`): iterate `repos.values()` (lock-free), but lock **each** repo around its own sweep so the tombstone append stays under that namespace's lock:

```java
int sweepDeletedFiles(long olderThanMs) throws Exception {
    int reaped = root.sweepDeletedFiles(olderThanMs);
    long cutoff = System.currentTimeMillis() - olderThanMs;
    for (NamespaceMetadataLogRepository repo : repos.values()) {
        repo.lock();
        try {
            for (FileId id : repo.state().tombstonesDeletedAtOrBefore(cutoff)) {
                repo.append(new MetadataLogRecord.TombstoneSwept(id));
                fileIndex.remove(id);
                reaped++;
            }
        } finally {
            repo.unlock();
        }
    }
    return reaped;
}
```

- [ ] **Step 6: Fix `close()` and remove the global `lock` field**

`close()` (`:331-349`): replace its `lock.lock()/unlock()` with `repoCreateLock.lock()/unlock()` (it only guards the `closed` flag + `fileStore.close()`/`root.close()` shutdown; no namespace work). Then delete the now-unused `private final ReentrantLock lock = new ReentrantLock();` field (`:52`). Compile to confirm no remaining `lock.` references in the file (only `repoCreateLock.` and `repo.lock()/unlock()` should remain).

- [ ] **Step 7: Run the concurrency test + full namespace-log/controller suites**

Run: `mvn -o -pl strata-meta -am test -Dtest=NamespaceLogPerNamespaceLockTest,NamespaceLogMetadataStoreConformanceTest,NamespaceMetadataLogRepositoryTest,NamespaceLogBackendDurabilityTest,ControllerNamespaceLogBackendTest,ControllerTest,RepairReliabilityTest`
Expected: PASS — the concurrency test now passes (B completes while A is blocked), and all regression suites stay green (within-namespace semantics unchanged).

- [ ] **Step 8: Full module green, then commit**

Run: `mvn -o -pl strata-meta -am test`
Expected: BUILD SUCCESS.

```bash
git add strata-meta/src/main/java/io/strata/meta/NamespaceMetadataLogRepository.java \
        strata-meta/src/main/java/io/strata/meta/NamespaceLogBackend.java \
        strata-meta/src/test/java/io/strata/meta/NamespaceLogPerNamespaceLockTest.java
git commit -m "perf(meta): per-namespace mutation lock — remove cross-namespace head-of-line blocking"
```

---

## Notes for the implementer

- The exact `NamespaceMetadataFileStore` interface methods (for the test's `BlockingFileStore`) must match the real interface — Read `strata-meta/src/main/java/io/strata/meta/NamespaceMetadataFileStore.java` and implement exactly its methods (the set shown in Task 2 Step 1 is indicative; correct it to the real signatures).
- Do NOT touch the `isSystem(...) → root.*` early-return branches in any method — the system path must stay lock-free (deadlock-avoidance, see Global Constraints).
- After Task 2, grep the file for `lock.` (Read tool) and confirm only `repoCreateLock.` and `repo.lock()`/`repo.unlock()` remain — no stray global-`lock` reference.
