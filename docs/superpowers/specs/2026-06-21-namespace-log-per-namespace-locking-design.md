# NamespaceLogBackend: per-namespace mutation locking

**Status:** Design (approved 2026-06-21)
**Scope:** `strata-meta` — `NamespaceLogBackend`, `NamespaceMetadataLogRepository`.
**Goal:** Remove the cross-namespace head-of-line blocking in the namespace-log metadata write path so a slow meta-log append in one namespace does not stall mutations in the controller's other owned namespaces. Cut metadata-op tail latency without changing durability semantics.

## 1. Problem (measured)

In namespace-log/sharded mode, metadata-op tail latency spikes under the data flood: `SEAL_CHUNK_META` p99 ~214ms, `DELETE_FILES` p99 ~452ms, while `APPEND` (data) is ~2ms and the median metadata op is sub-ms. Confirmed not fsync (`0 fsync/s`, env off), not compaction (`0/s`), not the repair refactor (bisect-neutral), not host load (the ZK backend is p50 0.44ms on the same host).

Root cause: `NamespaceLogBackend` uses a **single `ReentrantLock` for all user-namespace mutations** ([NamespaceLogBackend.java:36](../../../strata-meta/src/main/java/io/strata/meta/NamespaceLogBackend.java)), held across the **blocking quorum meta-log append** (`NamespaceMetadataLogRepository.append` → `fileStore.appendLog(...)` is a synchronous `.get()`). That append writes a chunk on the **same data nodes** carrying 200 MB/s of data, so under contention it spikes to hundreds of ms — and because the lock is global and held across that I/O, one slow append blocks every other namespace's metadata op on the controller, propagating the tail.

## 2. Goals / non-goals

**Goals**
- A slow append in namespace A must not block a mutation in namespace B on the same controller (per-namespace concurrency).
- **No change to durability semantics**: a mutation is acked only after its record is durable on the ack quorum; recovery replays the durable log; the single-writer-per-namespace ordering is preserved (TLA `MetadataByteDurability` unaffected).

**Non-goals**
- No within-namespace pipelining / speculative apply (that was "#2"; explicitly deferred — its benefit is marginal at ~1 writer/namespace and it would touch the apply-after-durable ordering). Within a namespace, ops stay serialized across the durable append exactly as today.
- No change to the system (meta-log file) namespace path, which already bypasses this lock in the ZK root.
- No change to `getFile`/`fileIndex` lock-free reads, or to the append/recovery/compaction machinery itself.

## 3. Design — two-tier locking

Replace the single global `lock` with:

### Hot path: per-namespace lock
Each namespace gets its own lock guarding its mutation/read sequence (validate → `repo.append` → apply, and per-namespace state reads). Implementation: add a `final ReentrantLock lock` to `NamespaceMetadataLogRepository` (the natural per-namespace unit; it already holds that namespace's `state`, `appliedOffset`, appender). Every backend mutation/read that today does `lock.lock()/unlock()` for a user namespace instead acquires `repo(ns).lock()`.

- Different namespaces proceed fully concurrently; a slow append in A holds only A's lock.
- Within a namespace, ops still serialize under that namespace's lock, held across the durable append — single-writer ordering and apply-after-durable are unchanged.

### Cold path: repo lifecycle
`repos` becomes a `ConcurrentHashMap<StrataNamespace, NamespaceMetadataLogRepository>`. Lazy create+recover of a repo is guarded by a single small `repoCreateLock` (a `ReentrantLock`) via double-checked lookup:

```
NamespaceMetadataLogRepository repo(StrataNamespace ns) {
    NamespaceMetadataLogRepository r = repos.get(ns);   // fast, lock-free
    if (r != null) return r;
    repoCreateLock.lock();
    try {
        r = repos.get(ns);
        if (r == null) { r = createAndOpen(ns); repos.put(ns, r); }
        return r;
    } finally { repoCreateLock.unlock(); }
}
```

Recovery (manifest read + `recoverAndSeal` + compact + CAS-publish) is heavy but happens **once per namespace** (cold), so serializing it on `repoCreateLock` is fine — and keeps it OUT of a `computeIfAbsent` bin lock (which would block unrelated namespaces hashing to the same bin during the heavy recovery).

### What each lock protects
- `repoCreateLock`: the `repos` map insert + a namespace's one-time create/recover. Not held during mutations.
- per-namespace `repo.lock`: that namespace's validate/append/apply and state reads (`listFiles`, `resolvePath`, `lockedFile`).
- `fileIndex` (user file→namespace): stays a lock-free `ConcurrentHashMap`; entries are written under the owning namespace's lock (per-file, no cross-namespace conflict).
- System namespace: unaffected — `root.createFile`/etc. already run lock-free against the ZK root.

## 4. Correctness

- **Within-namespace ordering preserved:** all of a namespace's mutations and its state reads serialize on that namespace's single lock, in the same order as today. The durable append is still inside that lock, so apply-after-durable and single-writer semantics are byte-for-byte the prior behavior.
- **No cross-namespace invariant is dropped:** the namespace-log mutation path has no cross-namespace dependency (each namespace's log/state/manifest is independent; the shared root/system path is separate and unchanged). `listNamespaces()` iterates the `ConcurrentHashMap` and reads each repo's `state().liveFiles()` under that repo's lock (best-effort listing; a per-repo lock gives a consistent per-namespace view).
- **Recovery atomicity preserved:** `repoCreateLock` makes create+recover happen exactly once per namespace before any mutation on it.
- **Durability invariant unchanged:** acks remain gated on durable quorum append; no speculative apply.

## 5. Testing

- **New concurrency test** (`strata-meta` test): with two user namespaces A and B owned by one backend, block A's append mid-flight (via the existing `FailureInjector.point("meta.log.afterDurableAppend")` held open, or a test `NamespaceMetadataFileStore` whose `appendLog` blocks on a latch for namespace A's log file) and assert that a `createFile`/`updateFile` on B **completes while A is still blocked** (e.g. B's future resolves within a tight timeout). Then release A and assert A completes. This directly verifies the head-of-line fix.
- **Regression (must stay green):** `NamespaceLogMetadataStoreConformanceTest`, `NamespaceMetadataLogRepositoryTest`, `NamespaceLogBackendDurabilityTest`, `ControllerNamespaceLogBackendTest`, `ControllerTest`, `RepairReliabilityTest` — they exercise the mutation/read paths whose locking changes.
- **No new flakiness:** the concurrency test must use a deterministic latch/injection, not timing sleeps, for the "B proceeds while A blocked" assertion.

## 6. Out of scope / follow-ups

- Within-namespace pipelining (speculative apply / group-commit) — deferred; revisit if a higher-concurrency-per-namespace workload needs it.
- The systemic ChunkStore `synchronized`-across-FileChannel-I/O / carrier-pinning that makes the underlying append slow in the first place — separate, larger effort (already in the perf-review backlog).
