# Namespace-Routed Metadata — Eliminate the Per-File ZK Owner-Index — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry `namespace` on every file-scoped metadata RPC and route by `ownerOf(namespace)`, then delete the per-file ZooKeeper `fileId → namespace` owner-index (`META_FIDX`).

**Architecture:** The metadata plane is sharded — each namespace has one controller owner computed by rendezvous over the namespace string. Today file-scoped ops carry only a random-UUID `FileId`, so a non-owner controller resolves `fileId → namespace` via a shared ZK index before redirecting. This change makes the caller supply the namespace on every op (the client always holds it via the open file handle), so the owner is computed directly and the ZK index is removed. The `FileId` stays a random UUID.

**Tech Stack:** Java 21, Maven multi-module (`strata-common`, `strata-proto`, `strata-client`, `strata-meta`, `strata-server`, `strata-it`), SCP binary RPC, ZooKeeper/Curator, JUnit 5.

**Spec:** `docs/superpowers/specs/2026-06-22-namespace-routing-eliminate-zk-fidx-design.md`

## Global Constraints

- **No production, no backward compatibility** — clean break. Remove `META_FIDX` outright; no dual-read, no migration, no compatibility overloads.
- **Namespace is mandatory on every file operation, including lookups.** No bare-`FileId` access path remains after this change.
- **Routing/addressing change only** — meta-log durability, rendezvous ownership, and recovery semantics are untouched.
- **Wire field placement rule:** add `namespace` as the **first** encoded field of each message — `w.string(namespace.toString())` first on encode; `StrataNamespace.of(Varint.readString(b))` first on decode. (Mirror the existing `CreateFile`/`LookupPath` pattern.)
- Build offline: `mvn -o -pl <module> -am test`. When `-am` builds dependency modules with `-Dtest=`, add `-Dsurefire.failIfNoSpecifiedTests=false`.
- Commit after each task. Branch is `penghui/confident-cray-7a9fd1` (do not push/merge unless asked).

---

## File Structure / Decomposition

| File | Responsibility | Tasks |
|---|---|---|
| `strata-meta/.../Diag.java` + 4 instrumented files | TEMP perf scaffolding — removed for a clean baseline | T1 |
| `strata-proto/.../Messages.java` | Add `namespace` to 7 file-scoped request records | T2 |
| `strata-client/.../ControllerClient.java` | Take + route by `namespace`; drop `fileId→owner` cache | T2 |
| `strata-client/.../StrataFileImpl.java`, `AppenderImpl.java`, `ReaderImpl.java`, `Recovery.java` | Hold + forward `namespace` | T2 |
| `strata-client/.../StrataClient.java`, `StrataClientImpl.java` | Public API: `openById`/`deleteById`/`delete` take namespace | T2 |
| `strata-meta/.../StrataSystemMetadataFileStore.java`, `strata-server/.../StrataPerf.java` | Pass `SYSTEM_NAMESPACE` / file namespace to the new API | T2 |
| `strata-meta/.../Controller.java` | Handlers gate by `requireNamespaceOwner(m.namespace())`; delete `requireNamespaceOwnerForFile`/`fileNamespace` | T3 |
| `strata-meta/.../Controller.java` (index call sites) + `ZkMetadataStore.java` | Delete `indexFileNamespace`/`unindexFileNamespace` + the 3 fidx methods + `META_FIDX` | T4 |
| `strata-meta/.../MetadataShardingRoutingTest.java`, `strata-it/...` | Update routing test; assert zero `META_FIDX` znodes | T5 |

Build/dependency ordering forces coarse tasks: adding a required field to a wire record breaks all construction sites until updated, so **T2 bundles proto + the entire client chain + the two in-tree API callers** (the smallest unit that compiles). T3 (server gate) and T4 (index deletion) then layer on a compiling base.

---

## Task 1: Remove the temporary Diag instrumentation (clean baseline)

The working tree carries temporary perf-investigation scaffolding (`-Dstrata.diag.metalog` timers) that must be removed so later tasks edit clean code. It is uncommitted and flag-gated; the post-fix validation uses the perf tool's built-in DELETE p50 and T5's "zero `META_FIDX` znodes" assertion instead.

**Files:**
- Delete: `strata-meta/src/main/java/io/strata/meta/Diag.java`
- Modify: `strata-meta/src/main/java/io/strata/meta/Controller.java` (handler wrappers in `CREATE_CHUNK`, `SEAL_CHUNK_META`, `DELETE_FILES`)
- Modify: `strata-meta/src/main/java/io/strata/meta/ZkMetadataStore.java` (`createFile`, `getFile`, `updateFile`, `putClusterLiveNodes`, `getClusterLiveNodes`, and the 3 `*FileNamespace` wrappers)
- Modify: `strata-meta/src/main/java/io/strata/meta/NamespaceMetadataLogRepository.java` (`lock()`)
- Modify: `strata-meta/src/main/java/io/strata/meta/StrataSystemMetadataFileStore.java` (`appendLog`)

- [ ] **Step 1: Find every Diag reference**

Run: `grep -rn 'Diag' strata-meta/src/main/java/io/strata/meta/`
Expected: references in the files listed above (a `long t0 = Diag.ON ? System.nanoTime() : 0;` line + a `try { ... } finally { if (Diag.ON) Diag.<TIMER>.record(System.nanoTime() - t0); }` wrapper, or a guarded `record` call, in each).

- [ ] **Step 2: Restore each wrapped method to its pre-instrumentation body**

Remove every `Diag.ON` guard, `Diag.<TIMER>.record(...)` call, and the `try/finally` added solely for timing, so each method calls its body directly. Example — `NamespaceMetadataLogRepository.lock()` becomes exactly:

```java
    /** Acquires this namespace's mutation lock (held across the durable append; single-writer ordering). */
    void lock() {
        lock.lock();
    }
```

And `StrataSystemMetadataFileStore.appendLog` becomes exactly:

```java
    @Override
    public void appendLog(FileId logFileId, byte[] frameBytes) throws Exception {
        appenderFor(logFileId).append(ByteBuffer.wrap(frameBytes)).get(); // durable on ack quorum
    }
```

For the `Controller` handlers (`CREATE_CHUNK`, `SEAL_CHUNK_META`, `DELETE_FILES`) and the `ZkMetadataStore` methods, delete the `long t0 = ...` line and unwrap the `try/finally`, leaving the original statements. (The `ZkMetadataStore` `*FileNamespace` methods are deleted entirely in Task 4; restoring them here is fine — Task 4 removes them.)

- [ ] **Step 3: Delete `Diag.java`**

```bash
rm strata-meta/src/main/java/io/strata/meta/Diag.java
```

- [ ] **Step 4: Verify no Diag references remain and the module compiles**

Run: `grep -rn 'Diag' strata-meta/src/main/java/ ; mvn -o -q -pl strata-meta -am compile`
Expected: grep prints nothing; compile succeeds.

- [ ] **Step 5: Commit**

```bash
git add -A strata-meta/src/main/java/io/strata/meta/
git commit -m "chore: remove temporary Diag perf instrumentation"
```

---

## Task 2: Carry namespace through proto + the client chain

Add `namespace` to the 7 file-scoped request messages and thread it from the `StrataFile` handle (which holds it) through `ControllerClient` to the wire, routing by namespace. Update the public API and the two in-tree callers so the build stays green. The server still reads `m.fileId()` via `requireNamespaceOwnerForFile` (it ignores the new field) until Task 3 — that compiles and behaves as today.

**Files:**
- Modify: `strata-proto/src/main/java/io/strata/proto/Messages.java` (records `CreateChunk`, `AllocateWriterEpoch`, `SealChunkMeta`, `AbortChunkMeta`, `SealFile`, `LookupFile`, `DeleteFiles`)
- Modify: `strata-client/src/main/java/io/strata/client/ControllerClient.java`
- Modify: `strata-client/src/main/java/io/strata/client/StrataFileImpl.java`, `AppenderImpl.java`, `ReaderImpl.java`, `Recovery.java`
- Modify: `strata-client/src/main/java/io/strata/client/StrataClient.java`, `StrataClientImpl.java`
- Modify: `strata-meta/src/main/java/io/strata/meta/StrataSystemMetadataFileStore.java` (embedded client `openById` calls)
- Modify: `strata-server/src/main/java/io/strata/server/StrataPerf.java` (`openById` calls)
- Test: `strata-proto/src/test/java/io/strata/proto/MessagesNamespaceRoundTripTest.java` (new), `strata-client/src/test/java/io/strata/client/ControllerClientTest.java`

**Interfaces produced (used by Task 3):**
- Each request message exposes `StrataNamespace namespace()`.
- `ControllerClient` file-scoped methods all take a leading `StrataNamespace namespace` and route by it:
  - `createChunk(StrataNamespace, FileId, int, long, long)` and `...(…, Set<Integer>)`
  - `allocateWriterEpochForAppend(StrataNamespace, FileId)`, `allocateWriterEpochForRecovery(StrataNamespace, FileId)`
  - `sealChunkMeta(StrataNamespace, ChunkId, int, long, int, List<Integer>)`
  - `abortChunkMeta(StrataNamespace, ChunkId, int, long, long)`
  - `sealFile(StrataNamespace, FileId, long)`, `lookupFile(StrataNamespace, FileId)`
  - `deleteFiles(StrataNamespace, List<FileId>)`
- `StrataClient.openById(StrataNamespace, FileId)`, `deleteById(StrataNamespace, FileId)`, `deleteById(StrataNamespace, List<FileId>)`.

### 2a. Proto messages

- [ ] **Step 1: Write the failing round-trip test**

Create `strata-proto/src/test/java/io/strata/proto/MessagesNamespaceRoundTripTest.java`:

```java
package io.strata.proto;

import io.strata.common.ChunkId;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MessagesNamespaceRoundTripTest {
    private static final StrataNamespace NS = StrataNamespace.of("tenant-a");
    private static final FileId FID = FileId.random();
    private static final ChunkId CID = new ChunkId(FID, 3);

    @Test
    void createChunkCarriesNamespace() {
        var m = new Messages.CreateChunk(NS, FID, 7, 1L, 2L, List.of(5, 9));
        var d = Messages.CreateChunk.decode(ByteBuffer.wrap(m.encode()));
        assertEquals(NS, d.namespace());
        assertEquals(FID, d.fileId());
        assertEquals(7, d.writeEpoch());
        assertEquals(List.of(5, 9), d.excludedNodeIds());
    }

    @Test
    void sealChunkMetaCarriesNamespace() {
        var m = new Messages.SealChunkMeta(NS, CID, 7, 100L, 42, List.of(1, 2));
        var d = Messages.SealChunkMeta.decode(ByteBuffer.wrap(m.encode()));
        assertEquals(NS, d.namespace());
        assertEquals(CID, d.chunkId());
        assertEquals(List.of(1, 2), d.sealedReplicas());
    }

    @Test
    void allocateWriterEpochCarriesNamespace() {
        var m = Messages.AllocateWriterEpoch.forAppend(NS, FID);
        var d = Messages.AllocateWriterEpoch.decode(ByteBuffer.wrap(m.encode()));
        assertEquals(NS, d.namespace());
        assertEquals(FID, d.fileId());
        assertEquals(Messages.AllocateWriterEpoch.FOR_APPEND, d.purpose());
    }

    @Test
    void abortSealLookupDeleteCarryNamespace() {
        var abort = Messages.AbortChunkMeta.decode(
                ByteBuffer.wrap(new Messages.AbortChunkMeta(NS, CID, 7, 1L, 2L).encode()));
        assertEquals(NS, abort.namespace());

        var seal = Messages.SealFile.decode(ByteBuffer.wrap(new Messages.SealFile(NS, FID, 64L).encode()));
        assertEquals(NS, seal.namespace());

        var lf = Messages.LookupFile.decode(ByteBuffer.wrap(new Messages.LookupFile(NS, FID).encode()));
        assertEquals(NS, lf.namespace());

        var del = Messages.DeleteFiles.decode(
                ByteBuffer.wrap(new Messages.DeleteFiles(NS, List.of(FID)).encode()));
        assertEquals(NS, del.namespace());
        assertEquals(List.of(FID), del.fileIds());
    }
}
```

- [ ] **Step 2: Run it to confirm it fails to compile**

Run: `mvn -o -q -pl strata-proto -am test-compile -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compile failure — the new constructors (`CreateChunk(NS, …)`, etc.) and `namespace()` accessors do not exist yet.

- [ ] **Step 3: Add `namespace` as the first field to each of the 7 records**

Edit `strata-proto/src/main/java/io/strata/proto/Messages.java`. For each record, prepend `StrataNamespace namespace` to the component list, prepend `w.string(namespace.toString())` in `encode()`, and read it first in `decode()`. Ensure `import io.strata.common.StrataNamespace;` is present.

**`CreateChunk`** — record + all constructors + encode + decode:

```java
    public record CreateChunk(StrataNamespace namespace, FileId fileId, int writeEpoch, long opIdMsb, long opIdLsb,
                              List<Integer> excludedNodeIds) {
        public static final int TAG_EXCLUDED_NODE_IDS = 0;

        public CreateChunk {
            excludedNodeIds = List.copyOf(excludedNodeIds);
        }

        public CreateChunk(StrataNamespace namespace, FileId fileId, int writeEpoch) {
            this(namespace, fileId, writeEpoch, UUID.randomUUID());
        }

        public CreateChunk(StrataNamespace namespace, FileId fileId, int writeEpoch, long opIdMsb, long opIdLsb) {
            this(namespace, fileId, writeEpoch, opIdMsb, opIdLsb, List.of());
        }

        private CreateChunk(StrataNamespace namespace, FileId fileId, int writeEpoch, UUID opId) {
            this(namespace, fileId, writeEpoch, opId.getMostSignificantBits(), opId.getLeastSignificantBits(), List.of());
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.string(namespace.toString()).fileId(fileId).i32(writeEpoch).u64(opIdMsb).u64(opIdLsb);
            if (excludedNodeIds.isEmpty()) {
                w.noTags();
            } else {
                BufWriter excluded = new BufWriter();
                excluded.varint(excludedNodeIds.size());
                for (int nodeId : excludedNodeIds) excluded.u32(nodeId);
                TaggedFields.of(Map.of(TAG_EXCLUDED_NODE_IDS, excluded.toBytes())).writeTo(w);
            }
            return w.toBytes();
        }

        public static CreateChunk decode(ByteBuffer b) {
            StrataNamespace namespace = StrataNamespace.of(Varint.readString(b));
            FileId fileId = FileId.readFrom(b);
            int writeEpoch = b.getInt();
            long opIdMsb = b.getLong();
            long opIdLsb = b.getLong();
            TaggedFields tags = TaggedFields.readFrom(b);
            byte[] rawExcluded = tags.get(TAG_EXCLUDED_NODE_IDS);
            List<Integer> excluded = List.of();
            if (rawExcluded != null) {
                ByteBuffer excludedBuf = ByteBuffer.wrap(rawExcluded);
                int n = count(excludedBuf);
                List<Integer> ids = new ArrayList<>(n);
                for (int i = 0; i < n; i++) ids.add(excludedBuf.getInt());
                if (excludedBuf.hasRemaining()) {
                    throw new IllegalArgumentException(
                            "trailing bytes in CreateChunk excluded-node tag: " + excludedBuf.remaining());
                }
                excluded = ids;
            }
            return new CreateChunk(namespace, fileId, writeEpoch, opIdMsb, opIdLsb, excluded);
        }
```

**`AllocateWriterEpoch`** — note the factories gain the namespace param:

```java
    public record AllocateWriterEpoch(StrataNamespace namespace, FileId fileId, byte purpose) {
        public static final byte FOR_APPEND = 1;
        public static final byte FOR_RECOVERY = 2;

        public AllocateWriterEpoch {
            fileId = Objects.requireNonNull(fileId, "fileId");
            if (purpose != FOR_APPEND && purpose != FOR_RECOVERY) {
                throw new IllegalArgumentException("unknown writer epoch purpose " + (purpose & 0xFF));
            }
        }

        public static AllocateWriterEpoch forAppend(StrataNamespace namespace, FileId fileId) {
            return new AllocateWriterEpoch(namespace, fileId, FOR_APPEND);
        }

        public static AllocateWriterEpoch forRecovery(StrataNamespace namespace, FileId fileId) {
            return new AllocateWriterEpoch(namespace, fileId, FOR_RECOVERY);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.string(namespace.toString()).fileId(fileId).u8(purpose).noTags();
            return w.toBytes();
        }

        public static AllocateWriterEpoch decode(ByteBuffer b) {
            AllocateWriterEpoch m = new AllocateWriterEpoch(
                    StrataNamespace.of(Varint.readString(b)), FileId.readFrom(b), b.get());
            TaggedFields.readFrom(b);
            return m;
        }
    }
```

**`SealChunkMeta`** (preserve the javadoc above it):

```java
    public record SealChunkMeta(StrataNamespace namespace, ChunkId chunkId, int writeEpoch, long length, int crc,
                                List<Integer> sealedReplicas) {
        public SealChunkMeta {
            sealedReplicas = List.copyOf(sealedReplicas);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.string(namespace.toString()).chunkId(chunkId).i32(writeEpoch).u64(length).u32(crc);
            w.varint(sealedReplicas.size());
            for (int id : sealedReplicas) w.u32(id);
            w.noTags();
            return w.toBytes();
        }

        public static SealChunkMeta decode(ByteBuffer b) {
            StrataNamespace namespace = StrataNamespace.of(Varint.readString(b));
            ChunkId id = ChunkId.readFrom(b);
            int epoch = b.getInt();
            long length = b.getLong();
            int crc = b.getInt();
            int n = count(b);
            List<Integer> sealed = new ArrayList<>(n);
            for (int i = 0; i < n; i++) sealed.add(b.getInt());
            TaggedFields.readFrom(b);
            return new SealChunkMeta(namespace, id, epoch, length, crc, sealed);
        }
    }
```

**`AbortChunkMeta`**:

```java
    public record AbortChunkMeta(StrataNamespace namespace, ChunkId chunkId, int writeEpoch, long opIdMsb, long opIdLsb) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.string(namespace.toString()).chunkId(chunkId).i32(writeEpoch).u64(opIdMsb).u64(opIdLsb).noTags();
            return w.toBytes();
        }

        public static AbortChunkMeta decode(ByteBuffer b) {
            AbortChunkMeta m = new AbortChunkMeta(
                    StrataNamespace.of(Varint.readString(b)), ChunkId.readFrom(b), b.getInt(), b.getLong(), b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }
```

**`SealFile`**:

```java
    public record SealFile(StrataNamespace namespace, FileId fileId, long totalLength) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.string(namespace.toString()).fileId(fileId).u64(totalLength).noTags();
            return w.toBytes();
        }

        public static SealFile decode(ByteBuffer b) {
            SealFile m = new SealFile(StrataNamespace.of(Varint.readString(b)), FileId.readFrom(b), b.getLong());
            TaggedFields.readFrom(b);
            return m;
        }
    }
```

**`LookupFile`**:

```java
    public record LookupFile(StrataNamespace namespace, FileId fileId) {
        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.string(namespace.toString()).fileId(fileId).noTags();
            return w.toBytes();
        }

        public static LookupFile decode(ByteBuffer b) {
            LookupFile m = new LookupFile(StrataNamespace.of(Varint.readString(b)), FileId.readFrom(b));
            TaggedFields.readFrom(b);
            return m;
        }
    }
```

**`DeleteFiles`**:

```java
    public record DeleteFiles(StrataNamespace namespace, List<FileId> fileIds) {
        public DeleteFiles {
            fileIds = List.copyOf(fileIds);
        }

        public byte[] encode() {
            BufWriter w = new BufWriter();
            w.string(namespace.toString());
            w.varint(fileIds.size());
            for (FileId f : fileIds) w.fileId(f);
            w.noTags();
            return w.toBytes();
        }

        public static DeleteFiles decode(ByteBuffer b) {
            StrataNamespace namespace = StrataNamespace.of(Varint.readString(b));
            int n = count(b);
            List<FileId> ids = new ArrayList<>(n);
            for (int i = 0; i < n; i++) ids.add(FileId.readFrom(b));
            TaggedFields.readFrom(b);
            return new DeleteFiles(namespace, ids);
        }
    }
```

- [ ] **Step 4: Run the round-trip test**

Run: `mvn -o -q -pl strata-proto -am test -Dtest=MessagesNamespaceRoundTripTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS. (Other modules will not compile yet — that's fixed in the rest of Task 2; only the proto module is built here via `-pl strata-proto`.)

### 2b. Client chain + public API

- [ ] **Step 5: Update `ControllerClient` — take + route by namespace**

In `strata-client/src/main/java/io/strata/client/ControllerClient.java`: each file-scoped method gains a leading `StrataNamespace namespace`, passes it as the `routingKey`, and includes it in the message. Delete `inheritOwner` and its two call sites in `createFile`/`lookupPath` (a created/looked-up file now routes by its namespace, which is already the routing key for those calls). Replace the methods:

```java
    Messages.CreateChunkResp createChunk(StrataNamespace namespace, FileId fileId, int writeEpoch,
                                         long opIdMsb, long opIdLsb) {
        return createChunk(namespace, fileId, writeEpoch, opIdMsb, opIdLsb, Set.of());
    }

    Messages.CreateChunkResp createChunk(StrataNamespace namespace, FileId fileId, int writeEpoch,
                                         long opIdMsb, long opIdLsb, Set<Integer> excludedNodeIds) {
        var resp = call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(namespace, fileId, writeEpoch, opIdMsb, opIdLsb,
                        List.copyOf(excludedNodeIds)).encode(), namespace);
        return decode(Opcode.CREATE_CHUNK, resp, Messages.CreateChunkResp::decode);
    }

    int allocateWriterEpochForAppend(StrataNamespace namespace, FileId fileId) {
        var resp = call(Opcode.ALLOCATE_WRITER_EPOCH,
                Messages.AllocateWriterEpoch.forAppend(namespace, fileId).encode(), namespace);
        return decode(Opcode.ALLOCATE_WRITER_EPOCH, resp, Messages.AllocateWriterEpochResp::decode).writerEpoch();
    }

    int allocateWriterEpochForRecovery(StrataNamespace namespace, FileId fileId) {
        var resp = call(Opcode.ALLOCATE_WRITER_EPOCH,
                Messages.AllocateWriterEpoch.forRecovery(namespace, fileId).encode(), namespace);
        return decode(Opcode.ALLOCATE_WRITER_EPOCH, resp, Messages.AllocateWriterEpochResp::decode).writerEpoch();
    }

    void sealChunkMeta(StrataNamespace namespace, io.strata.common.ChunkId chunkId, int writeEpoch, long length,
                       int crc, java.util.List<Integer> sealedReplicas) {
        call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(namespace, chunkId, writeEpoch, length, crc, sealedReplicas).encode(),
                namespace);
    }

    void abortChunkMeta(StrataNamespace namespace, io.strata.common.ChunkId chunkId, int writeEpoch,
                        long opIdMsb, long opIdLsb) {
        call(Opcode.ABORT_CHUNK_META,
                new Messages.AbortChunkMeta(namespace, chunkId, writeEpoch, opIdMsb, opIdLsb).encode(),
                namespace);
    }

    Messages.LookupFileResp lookupFile(StrataNamespace namespace, FileId fileId) {
        var resp = call(Opcode.LOOKUP_FILE, new Messages.LookupFile(namespace, fileId).encode(), namespace);
        return decode(Opcode.LOOKUP_FILE, resp, Messages.LookupFileResp::decode);
    }

    void sealFile(StrataNamespace namespace, FileId fileId, long totalLength) {
        call(Opcode.SEAL_FILE, new Messages.SealFile(namespace, fileId, totalLength).encode(), namespace);
    }

    Messages.DeleteFilesResp deleteFiles(StrataNamespace namespace, List<FileId> ids) {
        var resp = call(Opcode.DELETE_FILES, new Messages.DeleteFiles(namespace, ids).encode(), namespace);
        return decode(Opcode.DELETE_FILES, resp, Messages.DeleteFilesResp::decode);
    }
```

In `createFile` and `lookupPath`, delete the `inheritOwner(id, namespace)` line (and delete the private `inheritOwner` method). Add `import io.strata.common.StrataNamespace;` if not present.

- [ ] **Step 6: Thread namespace through `StrataFileImpl`**

In `StrataFileImpl.openForAppend()` pass `namespace` to the lookup, epoch, and `AppenderImpl`; in `openForRead()` and `recoverAndSeal()` pass `namespace` to `ReaderImpl`/`Recovery`:

```java
    @Override
    public Appender openForAppend() {
        Messages.LookupFileResp file = controller.lookupFile(namespace, fileId);
        if (file.fileState() != FileState.OPEN.value) {
            throw new ScpException(ErrorCode.FILE_SEALED, "file state " + file.fileState());
        }
        long length = 0;
        for (Messages.ChunkInfo c : file.chunks()) {
            if (c.state() != ChunkState.SEALED) {
                throw new ScpException(ErrorCode.INTERNAL,
                        "file has an open chunk — run recoverAndSeal or resume the owning appender");
            }
            length = addChunkLength(length, c.length());
        }
        int writeEpoch = controller.allocateWriterEpochForAppend(namespace, fileId);
        return new AppenderImpl(controller, appendPool, config, fileId, namespace, writeEpoch, file.writePolicy(), length);
    }

    @Override
    public Reader openForRead() {
        return new ReaderImpl(controller, readPool, config, fileId, namespace);
    }

    @Override
    public SealInfo recoverAndSeal() {
        return new Recovery(controller, appendPool, readPool, config, namespace).recoverAndSeal(fileId);
    }
```

- [ ] **Step 7: Add a namespace field to `AppenderImpl`, `ReaderImpl`, `Recovery` and use it at each call site**

`AppenderImpl`: add `private final StrataNamespace namespace;` (import `io.strata.common.StrataNamespace`), add the constructor param after `fileId`:

```java
    AppenderImpl(ControllerClient controller, NodePool pool, ClientConfig config, io.strata.common.FileId fileId,
                 io.strata.common.StrataNamespace namespace, int epoch, Messages.WritePolicy writePolicy,
                 long existingFileLength) {
        ...
        this.fileId = fileId;
        this.namespace = namespace;
        this.epoch = epoch;
        ...
    }
```

Update its four call sites to pass `namespace` first:
- line ~497: `controller.sealChunkMeta(namespace, id, epoch, fl, fcrc, sealedReplicas);`
- line ~544/~554: `created = controller.createChunk(namespace, fileId, epoch, createOp.getMostSignificantBits(), createOp.getLeastSignificantBits(), placementExclusions);` (and the retry overload without exclusions: `controller.createChunk(namespace, fileId, epoch, createOp.getMostSignificantBits(), createOp.getLeastSignificantBits());`)
- line ~659: `controller.abortChunkMeta(namespace, chunkId, epoch, createOp.getMostSignificantBits(), createOp.getLeastSignificantBits());`
- line ~754: `controller.sealFile(namespace, fileId, t);`

`ReaderImpl`: add `private final StrataNamespace namespace;`, add the constructor param after `fileId`, and change the `refresh()`/constructor lookup to `this.file = controller.lookupFile(namespace, fileId);` (and any other `controller.lookupFile(fileId)` in the file).

`Recovery`: add `private final StrataNamespace namespace;`, add a trailing `StrataNamespace namespace` param to **both** constructors (the 2-arg delegates to the 4-arg, forwarding `namespace`), and pass `namespace` first at each call site: `controller.lookupFile(namespace, fileId)` (lines ~72, ~77, ~83), `controller.allocateWriterEpochForRecovery(namespace, fileId)` (line ~76), `controller.sealFile(namespace, fileId, total)` (line ~111), `controller.sealChunkMeta(namespace, chunkId, epoch, …)` (line ~482).

- [ ] **Step 8: Update the public API in `StrataClient` + `StrataClientImpl`**

`StrataClient` (interface): change `openById` and the delete methods to require a namespace:

```java
    StrataFile openById(StrataNamespace namespace, FileId fileId);

    default void delete(StrataFile file) {
        Objects.requireNonNull(file, "file");
        deleteById(file.namespace(), file.id());
    }

    default void deleteById(StrataNamespace namespace, FileId fileId) {
        deleteById(namespace, List.of(Objects.requireNonNull(fileId, "fileId")));
    }

    void deleteById(StrataNamespace namespace, List<FileId> fileIds);
```

`StrataClientImpl`:

```java
    @Override
    public StrataFile openById(StrataNamespace namespace, FileId fileId) {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(fileId, "fileId");
        Messages.LookupFileResp file = controller.lookupFile(namespace, fileId);
        return new StrataFileImpl(controller, appendPool, readPool, config, fileId, namespace, file.path());
    }

    @Override
    public void delete(List<FilePath> paths) {
        Objects.requireNonNull(paths, "paths");
        for (FilePath p : paths) {
            Objects.requireNonNull(p, "path");
            FileId id = controller.lookupPath(p.namespace(), p.path());
            deleteById(p.namespace(), List.of(id));
        }
    }

    @Override
    public void deleteById(StrataNamespace namespace, List<FileId> fileIds) {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(fileIds, "fileIds");
        fileIds = fileIds.stream().map(id -> Objects.requireNonNull(id, "fileId")).toList();
        Messages.DeleteFilesResp resp = controller.deleteFiles(namespace, fileIds);
        if (!resp.fileIds().equals(fileIds) || resp.codes().size() != fileIds.size()) {
            throw new ScpException(ErrorCode.INTERNAL, "metadata delete response did not match request");
        }
        for (int i = 0; i < resp.fileIds().size(); i++) {
            short code = resp.codes().get(i);
            if (code != ErrorCode.OK.code) {
                throw new ScpException(ErrorCode.fromCode(code),
                        "delete " + resp.fileIds().get(i) + " failed with " + code);
            }
        }
    }
```

- [ ] **Step 9: Update the two in-tree API callers**

`strata-meta/.../StrataSystemMetadataFileStore.java` — its embedded `openById` calls (lines ~76, ~95, ~117) pass the system namespace `NamespaceLogBackend.SYSTEM_NAMESPACE`:

```java
        StrataFile file = client().openById(NamespaceLogBackend.SYSTEM_NAMESPACE, logFileId);
```

(Apply to all three `openById` call sites.)

`strata-server/.../StrataPerf.java` — its `openById` calls (lines ~265, ~406) pass the file's namespace. The perf already tracks the namespace each file was created in; pass that value (e.g. `client.openById(ns, id)` where `ns` is the namespace used to create the file). The delete call (`client.deleteById(List.of(file.id))`) becomes `client.deleteById(ns, file.id)`.

- [ ] **Step 9b: Update every test caller of the changed public API**

Changing `openById`/`deleteById` breaks existing test callers in multiple modules. Find them:

Run: `grep -rn 'openById\|\.deleteById(' strata-client/src/test strata-it/src/test strata-meta/src/test strata-server/src/test`

Known call sites to fix (pass the namespace the file was created in — each test creates its file via `FileSpec.log("<ns>", "/path")`, so use that same `StrataNamespace.of("<ns>")`):
- `strata-client/src/test/java/io/strata/client/StrataClientBehaviorTest.java` — `client.openById(fileId)` → `client.openById(ns, fileId)`; `client.deleteById(List.of(...))` → `client.deleteById(ns, List.of(...))`.
- `strata-it/src/test/java/io/strata/it/EndToEndTest.java` — `client.openById(fileId)` → `client.openById(ns, fileId)`.
- `strata-it/src/test/java/io/strata/it/RepairAndRetentionTest.java` — `client.openById(fileId)` → `client.openById(ns, fileId)`; `client.deleteById(List.of(fileId))` → `client.deleteById(ns, fileId)`.

Transformation rule: the test already knows the namespace (it created or looked up the file in one). Hoist it into a local `StrataNamespace ns = StrataNamespace.of("...")` if not already present, and thread it. Do NOT invent a fidx-style lookup.

- [ ] **Step 10: Update `ControllerClientTest` to route file-scoped ops by namespace**

In `strata-client/src/test/java/io/strata/client/ControllerClientTest.java`, any test that calls a file-scoped `ControllerClient` method must pass a namespace. Add a routing test:

```java
    @Test
    void fileScopedOpRoutesByNamespaceAndFollowsRedirect() throws Exception {
        StrataNamespace ns = StrataNamespace.of("tenant-x");
        FileId id = FileId.random();
        AtomicInteger standbyCalls = new AtomicInteger();
        try (ScpServer standby = new ScpServer(0, 1, 0, 0, req -> {
                standbyCalls.incrementAndGet();
                throw new ScpException(ErrorCode.NOT_LEADER, "standby"); // null hint → client rotates to next seed
             });
             ScpServer owner = new ScpServer(0, 1, 0, 0,
                     req -> ScpServer.ok(req, new Messages.AllocateWriterEpochResp(7).encode(), null));
             ControllerClient cc = new ControllerClient(new ClientConfig(
                     List.of(endpoint(standby), endpoint(owner)), 1024, 100))) {
            assertEquals(7, cc.allocateWriterEpochForAppend(ns, id));
            assertEquals(1, standbyCalls.get());
        }
    }
```

- [ ] **Step 11: Compile the whole reactor (catches all callers) and run the unit tests**

Run: `mvn -o -q test-compile -Dsurefire.failIfNoSpecifiedTests=false` — this builds every module's main + test sources, so any missed caller of the changed public API (including `strata-it`) fails here.
Then: `mvn -o -q -pl strata-client,strata-proto -am test -Dtest=ControllerClientTest,MessagesNamespaceRoundTripTest,StrataClientBehaviorTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: whole-reactor test-compile succeeds; the listed test classes PASS. (Server behavior is unchanged this task — it still gates via `requireNamespaceOwnerForFile`, ignoring the new namespace field.)

- [ ] **Step 12: Commit**

```bash
git add -A
git commit -m "feat: carry namespace on file-scoped metadata RPCs (client + proto)"
```

---

## Task 3: Route by the supplied namespace on the server

Switch the file-scoped handlers to gate on the message's namespace, and delete the now-unused fileId→namespace resolution helpers.

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/Controller.java`
- Test: `strata-meta/src/test/java/io/strata/meta/MetadataShardingRoutingTest.java`

**Interfaces consumed:** each request message exposes `namespace()` (Task 2).

- [ ] **Step 1: Update `MetadataShardingRoutingTest` to assert namespace-based file-op routing**

In `ownerServesItsNamespaceAndNonOwnerRedirectsToTheOwner`, replace the `LookupFile` assertions (which currently rely on the fidx) so the message carries the owning namespace and a non-owner redirects:

```java
        StrataNamespace owningNs = StrataNamespace.of(nsOf0);
        assertEquals(nsOf0, Messages.LookupFileResp.decode(client0.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(owningNs, created0.fileId()).encode(), null, 5_000)).namespace().value());
        ScpException wrongNode = assertThrows(ScpException.class, () -> client1.call(Opcode.LOOKUP_FILE,
                new Messages.LookupFile(owningNs, created0.fileId()).encode(), null, 5_000));
        assertEquals(ErrorCode.NOT_LEADER, wrongNode.code());
        assertEquals(ep0, wrongNode.leaderHint(), "file-id op redirects to the supplied namespace's owner");
```

(Update the comment that referenced the "global fileId→namespace index" — routing is now by the supplied namespace.)

- [ ] **Step 2: Run it to confirm it fails**

Run: `mvn -o -q -pl strata-meta -am test -Dtest=MetadataShardingRoutingTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL to compile (`Messages.LookupFile` now needs a namespace) until the handler change — and the call sites compile only after Step 3.

- [ ] **Step 3: Switch the handlers to `requireNamespaceOwner(m.namespace())`**

In `Controller.handle`, change each file-scoped case to gate on the message namespace:

- `CREATE_CHUNK`: `requireNamespaceOwner(m.namespace());`
- `ALLOCATE_WRITER_EPOCH`: `requireNamespaceOwner(m.namespace());`
- `SEAL_CHUNK_META`: `requireNamespaceOwner(m.namespace());`
- `ABORT_CHUNK_META`: `requireNamespaceOwner(m.namespace());`
- `SEAL_FILE`: `requireNamespaceOwner(m.namespace());`
- `LOOKUP_FILE`: `requireNamespaceOwner(m.namespace());`

For `DELETE_FILES`, the batch is single-namespace — gate once before the loop and drop the per-entry gate:

```java
            case DELETE_FILES -> {
                var m = Messages.DeleteFiles.decode(h);
                requireNamespaceOwner(m.namespace());
                List<Short> codes = new ArrayList<>();
                for (FileId id : m.fileIds()) {
                    try {
                        markDeleting(id);
                        repair.driveDeletionSoon(id); // prompt reclaim, but don't block metadata delete responses
                        codes.add(ErrorCode.OK.code);
                    } catch (ScpException e) {
                        codes.add(e.code().code);
                    }
                }
                yield ScpServer.ok(req, new Messages.DeleteFilesResp(m.fileIds(), codes).encode(), null);
            }
```

(`requireNamespaceOwner` already collapses to `requireLeader()` when `ownsAll()`, so the old `if (ownership.ownsAll()) requireLeader();` special-case for DELETE is no longer needed.)

- [ ] **Step 4: Delete `requireNamespaceOwnerForFile` and `fileNamespace`**

Remove both methods (Controller.java ~430-453). `fileNamespace` was the only caller of `rootZk.getFileNamespace` — that ZK method becomes uncalled and is removed in Task 4.

- [ ] **Step 5: Run the routing test**

Run: `mvn -o -q -pl strata-meta -am test -Dtest=MetadataShardingRoutingTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS — owner serves, non-owner redirects to the supplied namespace's owner.

- [ ] **Step 6: Commit**

```bash
git add -A strata-meta
git commit -m "feat: gate file-scoped metadata ops by the supplied namespace"
```

---

## Task 4: Delete the ZK owner-index

With the server gating by namespace, nothing reads or writes `META_FIDX`. Remove the index end to end.

**Files:**
- Modify: `strata-meta/src/main/java/io/strata/meta/Controller.java` (`indexFileNamespace`/`unindexFileNamespace` + call sites)
- Modify: `strata-meta/src/main/java/io/strata/meta/ZkMetadataStore.java` (`putFileNamespace`/`getFileNamespace`/`deleteFileNamespace` + `META_FIDX`)

- [ ] **Step 1: Remove the index call sites and helper methods in `Controller`**

- In the `CREATE_FILE` path, delete both `indexFileNamespace(m.fileId(), m.namespace())` calls (the create and the idempotent-retry site, ~lines 651/659).
- In `markDeleting`, delete the `unindexFileNamespace(id)` line (~line 912).
- Delete the private `indexFileNamespace(FileId, StrataNamespace)` and `unindexFileNamespace(FileId)` methods (~lines 923-933).

- [ ] **Step 2: Remove the fidx methods and constant in `ZkMetadataStore`**

Delete `putFileNamespace`, `getFileNamespace`, `deleteFileNamespace`, and the `private static final String META_FIDX = META + "/fidx";` line. Verify nothing else references `META_FIDX`:

Run: `grep -rn 'META_FIDX\|FileNamespace' strata-meta/src/main`
Expected: no matches.

- [ ] **Step 3: Build and run the meta suite**

Run: `mvn -o -q -pl strata-meta -am test -Dsurefire.failIfNoSpecifiedTests=false`
Expected: the strata-meta test suite passes (including `MetadataShardingRoutingTest`, `ControllerTest`, `NamespaceOwnershipTest`).

- [ ] **Step 4: Commit**

```bash
git add -A strata-meta
git commit -m "feat: delete the per-file ZK owner-index (META_FIDX)"
```

---

## Task 5: Sharded no-fidx regression assertion

Prove that on the **sharded** path (the only path that ever wrote the index), a create + delete leaves no `META_FIDX` znode. This belongs in `MetadataShardingRoutingTest`, which already stands up a 2-controller sharded cluster with a `TestingServer` ZK and exercises `CREATE_FILE`/`DELETE_FILES` as pure metadata ops (no data nodes needed). A single-controller `MiniCluster` could not test this — with `ownsAll()` the index write was already a no-op, so the assertion would pass even on the old code.

This test would FAIL on the pre-change code (sharded `CREATE_FILE` wrote `/strata/meta/fidx/<fileId>`) and PASS after Task 4.

**Files:**
- Test: `strata-meta/src/test/java/io/strata/meta/MetadataShardingRoutingTest.java` (add one test method)

**Interfaces consumed:** `Messages.DeleteFiles(StrataNamespace, List<FileId>)` (Task 2); the test's existing `zk` (`TestingServer`), `client0`/`client1`, `ep0`/`ep1`, and `namespaceOwnedBy(endpoint)` helpers (Task 3 / existing).

- [ ] **Step 1: Add the failing assertion**

Add to `MetadataShardingRoutingTest` (imports: `org.apache.curator.framework.CuratorFramework`, `org.apache.curator.framework.CuratorFrameworkFactory`, `org.apache.curator.retry.RetryOneTime`, `io.strata.common.FileId`):

```java
    @Test
    void shardedCreateAndDeleteLeaveNoOwnerIndexZnode() throws Exception {
        StrataNamespace ns = StrataNamespace.of(namespaceOwnedBy(ep0)); // route to node0
        // create then delete on the owner — both are metadata-only ops (no data nodes needed)
        FileId id = Messages.CreateFileResp.decode(client0.call(Opcode.CREATE_FILE,
                new Messages.CreateFile(ns, "/fidx-check").encode(), null, 5_000)).fileId();
        Messages.DeleteFilesResp del = Messages.DeleteFilesResp.decode(client0.call(Opcode.DELETE_FILES,
                new Messages.DeleteFiles(ns, java.util.List.of(id)).encode(), null, 5_000));
        assertEquals(ErrorCode.OK.code, del.codes().get(0));

        try (CuratorFramework zkClient = CuratorFrameworkFactory.newClient(
                zk.getConnectString(), new RetryOneTime(100))) {
            zkClient.start();
            assertNull(zkClient.checkExists().forPath("/strata/meta/fidx"),
                    "no per-file owner-index znode should exist after a sharded create+delete");
        }
    }
```

(If `CreateFile`'s convenience constructor `new Messages.CreateFile(StrataNamespace, String)` does not exist, use the form `MetadataShardingRoutingTest` already uses elsewhere — match the existing `new Messages.CreateFile(nsOf0, "/seg-0")` call.)

- [ ] **Step 2: Run it**

Run: `mvn -o -q -pl strata-meta -am test -Dtest=MetadataShardingRoutingTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS — after Task 4 the index code is gone, so `/strata/meta/fidx` never appears.

- [ ] **Step 3: Commit**

```bash
git add -A strata-meta
git commit -m "test: sharded create+delete writes no per-file owner-index"
```

---

## Validation (after all tasks)

- Full build: `mvn -o -q -DskipITs=false verify` (or the project's standard suite) is green.
- Optional perf A/B on the dev cluster (3 dedicated controllers, `forceSync=no`): sharding-on DELETE p50 should drop toward the ~2ms sharding-off baseline now that the per-delete ZK owner-index write is gone (compare against the pre-fix A-B-A numbers). The perf tool reports DELETE p50 directly — no extra instrumentation needed.
