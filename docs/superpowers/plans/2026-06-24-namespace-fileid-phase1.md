# Namespace + Long FileId — Phase 1 (Data Model) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change `FileId` from a 128-bit UUID to a per-namespace owner-assigned `long`, make a chunk's identity `(namespace, fileId, index)`, and store chunks on disk under `chunks/<namespace>/<low-bit shard>/<fileId-hex>.<index>` — the foundation for owner-driven verify (Phase 2) and orphan GC (Phase 3).

**Architecture:** `FileId` becomes `record FileId(long id)`. The namespace owner assigns ids from a monotonic `nextFileId` counter persisted as a high-water field in the namespace meta-log snapshot (recovered as `max+1` on failover); the `strata-meta` system namespace derives its file ids from the per-namespace manifest `generation`. Create-idempotency moves from fileId-keyed to opId-keyed because the client no longer mints the id. On disk, namespace is the parent directory and the long's low bytes form a bounded directory tree.

**Tech Stack:** Java 21, Maven multi-module (`mvn -o` offline; build with `-am` for upstream modules; `-Dsurefire.failIfNoSpecifiedTests=false` with `-Dtest`). JUnit 5. Existing `FailureInjector` framework for crash-window tests.

## Global Constraints

- **Clean break** — no prod, no backward-compat, no migration. Bump `ChunkFormats.FORMAT_VERSION`. `down -v` between old/new on-disk format.
- **No new high-volume ZK coordination** — user fileIds are owner-assigned (no ZK); only the low-volume `strata-meta` namespace derives ids (from the manifest generation, no new ZK counter).
- **fileId is namespace-scoped** — `(namespace, fileId, index)` is the global identity; `fileId` alone is NOT globally unique.
- **`nextFileId` is a high-water counter** — only ever increases; never derived from `max(live files)` (tombstone-sweep would forget+reuse); recovered from snapshot + tail replay.
- Build/test the affected modules with `mvn -o` and `-am`. Read files with the Read tool (Bash garbles long Java identifiers).

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `strata-common/.../FileId.java` | the id type | UUID→`long` |
| `strata-common/.../StrataNamespace.java` | namespace identity + validation | ensure filesystem-safe charset |
| `strata-common/.../ChunkId.java` | `(FileId,index)` | encoding shrinks with FileId |
| `strata-format/.../ChunkFormats.java` | on-disk header/format constants, `baseName` | header `chunkId` 20→12B, `baseName` hex, `FORMAT_VERSION` bump, path helper |
| `strata-format/.../ChunkStore.java` | chunk files on disk | namespace-scoped path, `open(namespace,…)`, recovery walk |
| `strata-meta/.../NamespaceMetadataState.java` | per-namespace in-mem state + snapshot | `nextFileId` high-water + assign + recover |
| `strata-meta/.../NamespaceMetadataSnapshotCodec.java` | snapshot serialization | encode/decode `nextFileId` |
| `strata-meta/.../NamespaceLogBackend.java` | meta-log backend | `createFile` assigns id from owner counter |
| `strata-meta/.../Controller.java` | createFile/createChunk handlers | owner-assign id; opId-keyed idempotency |
| `strata-meta/.../Records.java` | `NamespaceManifest` etc. | system-file id derivation input |
| `strata-proto/.../Messages.java` | RPCs carrying FileId | `CreateFile` drops client fileId; golden corpus |
| `strata-client/.../ControllerClient.java` | client createFile | stops minting fileId; reads it from resp |

---

### Task 1: `FileId` becomes a `long` (type change + repo-wide compile sweep)

This is the foundational, atomic change: `FileId` is referenced across every module, so the deliverable is **the new type + its unit test + the whole repo compiling again**. The sweep is mechanical (compiler-driven).

**Files:**
- Modify: `strata-common/src/main/java/io/strata/common/FileId.java`
- Test: `strata-common/src/test/java/io/strata/common/FileIdTest.java` (create)
- Sweep: all `new FileId(msb, lsb)`, `.msb()`, `.lsb()`, `FileId.random()`, `FileId.fromString(...)` call sites across all modules (compiler lists them).

**Interfaces:**
- Produces: `record FileId(long id)` with `void writeTo(ByteBuffer)` (8 bytes), `static FileId readFrom(ByteBuffer)`, `String toString()` (16-digit zero-padded lowercase hex), `static FileId fromHex(String)`, `int compareTo(FileId)` (unsigned-long), and `static FileId of(long id)`. `random()` is **removed** (ids are owner-assigned, not minted at call sites). Tests that need an ad-hoc id use `FileId.of(<n>)`.

- [ ] **Step 1: Write the failing test** — `FileIdTest.java`:

```java
package io.strata.common;

import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import static org.junit.jupiter.api.Assertions.*;

class FileIdTest {
    @Test void roundTripsAsEightBytes() {
        FileId id = FileId.of(0x0123_4567_89ab_cdefL);
        ByteBuffer b = ByteBuffer.allocate(8);
        id.writeTo(b);
        assertEquals(8, b.position());
        b.flip();
        assertEquals(id, FileId.readFrom(b));
    }
    @Test void toStringIsZeroPaddedHexAndSortsNumerically() {
        assertEquals("0000000000000001", FileId.of(1).toString());
        assertEquals("00000000000000ff", FileId.of(255).toString());
        // lexical order == numeric order
        assertTrue(FileId.of(1).toString().compareTo(FileId.of(2).toString()) < 0);
        assertEquals(FileId.of(0x1234L), FileId.fromHex("0000000000001234"));
    }
    @Test void compareIsUnsigned() {
        assertTrue(FileId.of(1).compareTo(FileId.of(-1L)) < 0); // -1 == max unsigned
    }
}
```

- [ ] **Step 2: Run it — expect FAIL** (compile error / method not found):
Run: `mvn -o -q -pl strata-common test -Dtest=FileIdTest`
Expected: FAIL (FileId.of/fromHex/8-byte writeTo not present).

- [ ] **Step 3: Rewrite `FileId.java`:**

```java
package io.strata.common;

import java.nio.ByteBuffer;

/** Identifier of a storage-layer file, unique WITHIN a namespace (owner-assigned long). */
public record FileId(long id) implements Comparable<FileId> {
    public static FileId of(long id) { return new FileId(id); }

    public static FileId fromHex(String s) { return new FileId(Long.parseUnsignedLong(s, 16)); }

    public void writeTo(ByteBuffer buf) { buf.putLong(id); }

    public static FileId readFrom(ByteBuffer buf) { return new FileId(buf.getLong()); }

    @Override public String toString() { return String.format("%016x", id); }

    @Override public int compareTo(FileId o) { return Long.compareUnsigned(id, o.id); }
}
```

- [ ] **Step 4: Compiler-driven sweep.** Repeatedly run `mvn -o -q compile` then `mvn -o -q test-compile` and fix each error:
  - `new FileId(msb, lsb)` → `FileId.of(<value>)`. In tests using two arbitrary longs, collapse to one (`FileId.of(1)`), or derive (`FileId.of(((long)msb<<32)|lsb)`) where a test needs two distinct components — but prefer a single literal.
  - `.msb()`/`.lsb()` → `.id()` (and any code combining them).
  - `FileId.random()` → `FileId.of(<n>)` in tests (each call site picks a distinct constant); production `random()` callers are replaced by owner-assignment in Task 3/5 — for now stub with `FileId.of(0)` ONLY inside code that Task 3/5 rewrites, and leave a `// TASK3` marker.
  - `FileId.fromString` → `FileId.fromHex`.
  - Any `%s`/UUID formatting of a fileId now prints 16-hex — fine.

- [ ] **Step 5: Verify** — `mvn -o -q clean test-compile` green; `mvn -o -q -pl strata-common test -Dtest=FileIdTest` PASS.

- [ ] **Step 6: Commit** — `git commit -am "feat: FileId is a long (was UUID)"`.

---

### Task 2: `StrataNamespace` is a filesystem-safe directory name

Namespace becomes a directory component (Task 6), so it must be a safe single path segment.

**Files:**
- Modify: `strata-common/src/main/java/io/strata/common/StrataNamespace.java` (`validate`, ~line 31-50)
- Test: `strata-common/src/test/java/io/strata/common/StrataNamespaceTest.java` (create or extend)

**Interfaces:**
- Consumes: nothing new.
- Produces: `StrataNamespace.of(String)` rejects any namespace that is not a safe directory name. Allowed: `[A-Za-z0-9._-]`, length 1..255 bytes, not `.`/`..`, not the reserved system value. (Read the existing `validate` first — it already checks length + reserved + a per-char loop; tighten the per-char allowance to exactly this set and add the `.`/`..` rejection.)

- [ ] **Step 1: Write failing tests:**

```java
@Test void rejectsPathUnsafeNamespaces() {
    for (String bad : new String[]{"a/b", "..", ".", "a b", "with space", ""}) {
        assertThrows(IllegalArgumentException.class, () -> StrataNamespace.of(bad), bad);
    }
}
@Test void acceptsSafeNamespaces() {
    for (String ok : new String[]{"perf-0", "tenant_A", "ns.1", "a"}) {
        assertEquals(ok, StrataNamespace.of(ok).value());
    }
}
```

- [ ] **Step 2: Run — expect FAIL** (`a/b`, `.`, `..` currently slip through or the charset differs):
Run: `mvn -o -q -pl strata-common test -Dtest=StrataNamespaceTest`

- [ ] **Step 3: Tighten `validate`** (read current code; adjust the per-char loop to allow only `A-Za-z0-9._-`, and reject `.`/`..`):

```java
// after the length + reserved checks:
if (raw.equals(".") || raw.equals("..")) {
    throw new IllegalArgumentException("namespace must not be . or ..");
}
for (int i = 0; i < raw.length(); i++) {
    char c = raw.charAt(i);
    boolean ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
            || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
    if (!ok) throw new IllegalArgumentException("namespace has unsafe char '" + c + "': " + raw);
}
```

- [ ] **Step 4: Verify** — `mvn -o -q -pl strata-common test -Dtest=StrataNamespaceTest` PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat: constrain namespace to a filesystem-safe directory name"`.

---

### Task 3: Owner-assigned `nextFileId` (counter + snapshot high-water + recovery)

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/NamespaceMetadataState.java` (`Snapshot` ~line 52, `exportSnapshot` ~60, `apply(FileCreated)` ~82, add `nextFileId` field + `assignFileId()`)
- Modify: `strata-meta/src/main/java/io/strata/meta/NamespaceMetadataSnapshotCodec.java` (encode/decode `nextFileId`)
- Test: `strata-meta/src/test/java/io/strata/meta/NamespaceMetadataStateTest.java` (create or extend)

**Interfaces:**
- Produces: `NamespaceMetadataState.assignFileId()` → `FileId` (returns current `nextFileId`, then increments). `Snapshot` gains `long nextFileId` as its **first** component: `record Snapshot(long nextFileId, long nextLogStartOffset, List<FileRecord> files, Map<FileId,Long> tombstones)`. `apply(FileCreated r)` advances `nextFileId = max(nextFileId, r.fileId().id() + 1)` (so a replayed/imported create from the tail keeps the high-water correct).
- Consumes (Task 8/4): `assignFileId()` from `NamespaceLogBackend.createFile`.

- [ ] **Step 1: Write failing test:**

```java
@Test void nextFileIdIsMonotonicAndSurvivesTombstoneSweepViaSnapshot() {
    NamespaceMetadataState s = new NamespaceMetadataState();
    FileId a = s.assignFileId();   // 0
    FileId b = s.assignFileId();   // 1
    assertEquals(FileId.of(0), a);
    assertEquals(FileId.of(1), b);
    // simulate: create+delete+sweep file 1, then snapshot/restore; next id must still be 2, never reuse 1
    var snap = s.exportSnapshot(123);
    assertEquals(2, snap.nextFileId());
    NamespaceMetadataState restored = NamespaceMetadataState.fromSnapshot(snap);
    assertEquals(FileId.of(2), restored.assignFileId());
}
@Test void applyAdvancesHighWaterFromReplayedCreate() {
    NamespaceMetadataState s = new NamespaceMetadataState();
    s.apply(new MetadataLogRecord.FileCreated(FileId.of(41), StrataNamespace.of("ns"),
            StrataPath.of("/p"), /* …other FileCreated fields per the record… */));
    assertEquals(FileId.of(42), s.assignFileId());
}
```
(Read `MetadataLogRecord.FileCreated`'s full field list and `NamespaceMetadataState.fromSnapshot`/constructor before finalizing the test args.)

- [ ] **Step 2: Run — expect FAIL** (`assignFileId`/`nextFileId` absent):
Run: `mvn -o -q -pl strata-meta -am test -Dtest=NamespaceMetadataStateTest -Dsurefire.failIfNoSpecifiedTests=false`

- [ ] **Step 3: Implement.** In `NamespaceMetadataState`: add `private long nextFileId = 0;`. Add:
```java
FileId assignFileId() { return new FileId(nextFileId++); }
```
In `apply(MetadataLogRecord.FileCreated r)`: add `nextFileId = Math.max(nextFileId, r.fileId().id() + 1);`. Add `nextFileId` to the `Snapshot` record (first field) and `exportSnapshot` (`new Snapshot(nextFileId, nextLogStartOffset, …)`). In `fromSnapshot`/the snapshot-restore path, set `nextFileId = snap.nextFileId()`. Update `NamespaceMetadataSnapshotCodec` to write/read the leading `long nextFileId`.

- [ ] **Step 4: Verify** — the two tests PASS; existing `NamespaceMetadataSnapshotCodec` roundtrip tests updated and PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat: per-namespace nextFileId high-water in state + snapshot"`.

---

### Task 4: System namespace (`strata-meta`) fileId derivation

The `strata-meta` system files (meta-log segments + snapshots) cannot use the owner counter (it lives *in* a meta-log). Derive their ids deterministically from the per-namespace manifest `generation`.

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/NamespaceMetadataLogRepository.java` (where it creates the log/snapshot strata files — `createLogFile`/`writeSnapshot` paths)
- Modify (maybe): `strata-meta/src/main/java/io/strata/meta/StrataSystemMetadataFileStore.java`
- Test: `strata-meta/src/test/java/io/strata/meta/SystemFileIdTest.java` (create)

**Interfaces:**
- Produces: a pure helper `static FileId systemFileId(StrataNamespace ns, long generation, int kind)` where `kind` 0=log, 1=snapshot, returning a collision-free `long` derived from `(ns, generation, kind)` — e.g. `FileId.of(mix(ns.hashCode64(), generation, kind))` using a reversible/low-collision mix (document the scheme; a 64-bit hash of `ns||generation||kind` is acceptable given the low cardinality). On a `generation` bump (compaction/roll), the derived ids change → no reuse of an in-use system file.
- Consumes: the manifest `generation` (already on `Records.NamespaceManifest`).

- [ ] **Step 1: Write failing test:**

```java
@Test void systemFileIdsAreCollisionFreeAcrossNsGenerationKind() {
    Set<FileId> seen = new HashSet<>();
    for (String ns : new String[]{"perf-0","perf-1","tenant.x"}) {
        for (long g = 0; g < 1000; g++) {
            assertTrue(seen.add(SystemFileIds.of(StrataNamespace.of(ns), g, 0)));
            assertTrue(seen.add(SystemFileIds.of(StrataNamespace.of(ns), g, 1)));
        }
    }
}
```

- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement** `SystemFileIds.of(ns, generation, kind)` with a documented 64-bit mix; wire `NamespaceMetadataLogRepository` to use it when creating the log segment / snapshot strata files (instead of `FileId.random()`).
- [ ] **Step 4: Verify** — test PASS; meta-log open/compaction still works (run `NamespaceMetadataLogRepositoryTest`).
- [ ] **Step 5: Commit** — `git commit -am "feat: derive strata-meta system file ids from manifest generation"`.

---

### Task 5: `CreateFile` becomes owner-assigned + opId-keyed idempotency

Today the client mints a random fileId and create is idempotent by that fileId. The owner now assigns the id, so idempotency must key on the client's **opId**.

**Files:**
- Modify: `strata-proto/.../Messages.java` (`CreateFile` ~646: drop the `fileId` field; keep `namespace, path, writePolicy, opIdMsb, opIdLsb`. `CreateFileResp` ~695 already returns `fileId` — keep). Update golden corpus.
- Modify: `strata-client/.../ControllerClient.java` (~119): build `CreateFile` without a fileId; still read `fileId` from the resp.
- Modify: `strata-meta/.../Controller.java` (`createFile` ~598): assign `fileId = store.assignFileId(namespace)`; idempotency via opId.
- Modify: `strata-meta/.../NamespaceLogBackend.java` (`createFile` ~131) + interface: add `assignFileId(namespace)` and an opId→fileId lookup.
- Test: `strata-meta/.../ControllerTest.java` (the createFile cases) + `MessageGoldenCorpusTest`.

**Interfaces:**
- Produces: `MetadataStore.assignFileId(StrataNamespace) → FileId` (delegates to the owner's `NamespaceMetadataState.assignFileId`); `Controller.createFile` returns the assigned id; a retried create with the same `(namespace, opId)` returns the **same** fileId (idempotent), implemented by recording `opId→fileId` in the namespace state/log and checking it before assigning.
- Consumes: `NamespaceMetadataState.assignFileId()` (Task 3).

- [ ] **Step 1: Write failing tests** (in `ControllerTest`): create returns an id starting at 0 for a fresh namespace; a second create with the **same opId** returns the **same** id; a create with a **new opId** returns the **next** id. (Adapt the existing createFile test harness — it currently passes a client fileId; remove that.)
- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement.** Drop `fileId` from `CreateFile` (proto encode/decode + the `CreateFile` constructors that minted it). In `NamespaceLogBackend.createFile`: look up the opId; if present return its fileId; else `id = state.assignFileId()`, append `FileCreated(id, …)` (the record already carries the id) + record `opId→id`, return `id`. In `Controller.createFile`: build the `FileRecord` with the assigned id; remove the fileId-keyed NodeExists idempotency, replace with opId-keyed. Regenerate the golden corpus bytes for `CreateFile`.
- [ ] **Step 4: Verify** — ControllerTest createFile cases PASS; golden corpus PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat: owner-assigned fileId + opId-keyed create idempotency"`.

---

### Task 6: On-disk namespace directory + low-bit shard + `open(namespace)`

**Files:**
- Modify: `strata-format/.../ChunkFormats.java` (`baseName` ~221 → hex; add `chunkRelativePath(StrataNamespace, ChunkId)`; bump `FORMAT_VERSION`; header `chunkId` encoding shrinks via FileId 8B)
- Modify: `strata-format/.../ChunkStore.java` (`open(...)` ~500 takes a `StrataNamespace`; `Handle` paths use the relative path; `mkdir -p` + `forceDirectory` on the shard dir)
- Modify: the node-facing chunk-open call site so the namespace reaches `ChunkStore.open` (`CreateChunk` already carries namespace at the controller; thread it to the data node's open RPC + `DataNodeHandlers`).
- Test: `strata-format/.../ChunkStoreTest.java` (path placement) + `strata-node` open path.

**Interfaces:**
- Produces: `ChunkFormats.chunkRelativePath(ns, chunkId)` → `"<ns>/<L1 2hex>/<L2 2hex>/<fileId 16hex>.<index>"` where `L1 = fileId.id() & 0xFF`, `L2 = (fileId.id() >> 8) & 0xFF`. `ChunkStore.open(StrataNamespace ns, ChunkId id, …)`.
- Consumes: `StrataNamespace` (Task 2), `FileId` (Task 1).

- [ ] **Step 1: Write failing test:**

```java
@Test void chunkPathIsNamespaceShardedLowBitsFirst() {
    var ns = StrataNamespace.of("perf-0");
    assertEquals("perf-0/00/00/0000000000000000.0",
            ChunkFormats.chunkRelativePath(ns, new ChunkId(FileId.of(0), 0)));
    assertEquals("perf-0/ff/00/00000000000000ff.0",
            ChunkFormats.chunkRelativePath(ns, new ChunkId(FileId.of(255), 0)));
    assertEquals("perf-0/00/01/0000000000000100.2",
            ChunkFormats.chunkRelativePath(ns, new ChunkId(FileId.of(256), 2)));
}
```

- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement** `chunkRelativePath`; change `baseName` to `fileId.toString() (hex) + "." + index`; in `ChunkStore.open(ns, …)` compute the dir from `chunkRelativePath`, `Files.createDirectories(shardDir)`, `forceDirectory(shardDir)` (mirror the existing dir-fsync), then the existing `.chunk/.meta/.j` create. Thread `namespace` through the data node's open RPC and `DataNodeHandlers` so `open` receives it. Bump `FORMAT_VERSION`.
- [ ] **Step 4: Verify** — path test PASS; an `open(ns, …)` creates files at `chunks/perf-0/00/00/0000000000000000.0.chunk`.
- [ ] **Step 5: Commit** — `git commit -am "feat: namespace-sharded chunk on-disk layout (chunks/<ns>/<shard>/<fileId>.<index>)"`.

---

### Task 7: Recovery walk reconstructs `(namespace, fileId, index)`, per-namespace parallel

**Files:**
- Modify: `strata-format/.../ChunkStore.java` (`recoverAll` / the startup scan)
- Test: `strata-format/.../ChunkStoreTest.java`

**Interfaces:**
- Consumes: `chunkRelativePath` layout (Task 6).
- Produces: on startup, the in-memory chunk index is rebuilt by walking `chunks/<ns>/<L1>/<L2>/*.chunk`, parsing `namespace` from the `<ns>` path component and `(fileId,index)` from the filename; namespaces recovered in parallel (one task per top-level `<ns>` dir).

- [ ] **Step 1: Write failing test:** create chunks in two namespaces, close the store, reopen, assert all chunks recovered with correct `(namespace, fileId, index)` and that a known chunk reads back.
- [ ] **Step 2: Run — expect FAIL** (old flat recovery can't parse the new tree).
- [ ] **Step 3: Implement** the two-level (ns, shard) walk; derive namespace from the path; parallelize across top-level `<ns>` dirs (a bounded executor / parallel stream). Keep the per-chunk recovery body (header read, state) unchanged.
- [ ] **Step 4: Verify** — reopen test PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat: namespace-aware parallel chunk recovery"`.

---

### Task 8: 🔴 id-reuse-on-owner-failover (correctness anchor)

**Files:**
- Test: `strata-meta/.../NamespaceFileIdRecoveryInjectionTest.java` (create), using the existing `FailureInjector`.

**Interfaces:**
- Consumes: `assignFileId` (Task 3), the createFile path + snapshot/recovery (Tasks 3,5), `FailureInjector` points already present in `NamespaceMetadataLogRepository` (e.g. `meta.log.afterDurableAppend`) plus any new point needed around assign/snapshot.

- [ ] **Step 1: Write the test.** For each crash window — (a) after `assignFileId` but before the `FileCreated` append, (b) after append but before the next snapshot, (c) mid-compaction (snapshot written, manifest not yet CAS-published) — drive: assign several ids, inject the crash, recover the namespace into a fresh `NamespaceMetadataState`/owner, then assign more ids. **Assert every id returned across the whole run is distinct (no value ever reappears)** and that any id that reached a durable `FileCreated` is never reassigned. Use the existing failure-injection harness pattern from `NamespaceMetadataLogFailureInjectionTest`.
- [ ] **Step 2: Run — expect FAIL** if any window reuses (it should pass if Tasks 3/5 are correct; the test is the guard).
- [ ] **Step 3: Fix** any reuse found (e.g. ensure the snapshot captures `nextFileId` before the manifest CAS; ensure recovery takes `max(snapshot.nextFileId, max tail FileCreated id + 1)`).
- [ ] **Step 4: Verify** — test PASS across all three windows.
- [ ] **Step 5: Commit** — `git commit -am "test: no fileId reuse across owner-failover crash windows"`.

---

### Task 9: Full build green + integration (mini-cluster create/restart/recover)

**Files:**
- Modify: any remaining test fixtures using UUID-shaped fileIds (golden corpora, conformance tests, `strata-it`).
- Test: `strata-it/.../` an end-to-end test that creates files across ≥2 namespaces, writes+seals chunks, restarts the data node, and verifies recovery + readback with the long-id layout.

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1:** `mvn -o clean test -pl '!strata-it'` — fix every remaining failure (golden corpora, conformance, proto roundtrips) until green.
- [ ] **Step 2:** Update/add a `strata-it` end-to-end test (extend `EndToEndTest` or `MiniCluster`): create files in `perf-0` and `perf-1`, append+seal, assert ids are `0,1,…` per namespace, restart a node, assert chunks recover at `chunks/<ns>/<shard>/…` and read back.
- [ ] **Step 3:** `mvn -o -q -pl strata-it -am test -Dtest=<that test> -Dsurefire.failIfNoSpecifiedTests=false` PASS.
- [ ] **Step 4: Verify** — `mvn -o clean test -pl '!strata-it'` green; the new it test green.
- [ ] **Step 5: Commit** — `git commit -am "test: end-to-end long-fileId create/recover across namespaces; full build green"`.

---

## Self-Review

**Spec coverage (Phase 1 scope of the spec §1, §2, §5, §6):**
- §1 FileId→long → Task 1. Two-layer gen: owner counter → Task 3, system derive → Task 4. opId idempotency (surfaced) → Task 5. ✓
- §2 directory + low-bit shard → Task 6; namespace charset → Task 2; recovery walk + parallel → Task 7. ✓
- §5 clean break / FORMAT_VERSION bump → Tasks 1/6. ✓
- §6 id-reuse-on-failover → Task 8; tombstone-sweep high-water → Task 3; system collision-free → Task 4; layout/recovery → Tasks 6/7; proto roundtrip+golden → Tasks 1/5; full build + it → Task 9. ✓ (Phase 2/3 tests are out of scope.)

**Placeholder scan:** Tasks 3/5 say "read the full FileCreated field list / fromSnapshot before finalizing" — this is a *grounding instruction to the implementer*, not a vague requirement; the surrounding code/signatures are concrete. The compiler-driven sweep (Task 1 Step 4) is a strategy, not a placeholder — the deliverable (compile green) is verifiable.

**Type consistency:** `FileId.of(long)`/`.id()`/`fromHex`/`writeTo`(8B) used consistently across Tasks 1,3,6,8. `assignFileId()` defined in Task 3, consumed in Task 5. `Snapshot(nextFileId, …)` defined Task 3, used in Task 8. `chunkRelativePath(ns, chunkId)` defined Task 6, used Task 7.

## Notes / risks carried from the spec

- **Biggest risk: id reuse on failover** — Task 8 is the guard; do not weaken it.
- **System fileId derivation (Task 4)** — if the deterministic `(ns,generation,kind)` mix risks collisions at the cardinality, fall back to a small ZK CAS counter scoped to `strata-meta` (spec option (b)).
- **opId idempotency (Task 5)** — confirm whether an `opId→fileId` map must persist in the meta-log (durable idempotency across owner restart) or only in memory (best-effort); persist it (append-or-snapshot) for correctness across failover.
