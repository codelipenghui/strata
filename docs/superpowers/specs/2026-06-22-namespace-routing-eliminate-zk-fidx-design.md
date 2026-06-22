# Namespace-Routed Metadata — Eliminate the Per-File ZK Owner-Index — Design

**Goal:** Make every file-scoped metadata RPC carry its `namespace`, so a controller routes by `ownerOf(namespace)` computed fresh from rendezvous — and delete the per-file ZooKeeper `fileId → namespace` owner-index (`META_FIDX`) entirely.

**Architecture:** The metadata plane is sharded: each namespace has one controller owner, computed (not stored) by rendezvous hash over the namespace string ([NamespaceAssignmentPolicy](../../../strata-meta/src/main/java/io/strata/meta/NamespaceAssignmentPolicy.java)). Today, file-scoped ops carry only a random-UUID `FileId`, so a non-owner controller must resolve `fileId → namespace` via a shared ZK index before it can redirect. This design removes that lookup by requiring the caller to supply the namespace on every op; the `FileId` stays a random UUID.

**Tech Stack:** Java 21, Maven multi-module (`strata-common`, `strata-proto`, `strata-client`, `strata-meta`, `strata-server`), SCP binary RPC, ZooKeeper/Curator for the root store.

## Global Constraints

- **No production, no backward compatibility** — clean break. Remove `META_FIDX` outright; no dual-read, no migration shim, no env fallback. Rename/replace in place.
- **Namespace is mandatory on every file operation, including lookups.** There is no bare-`FileId` access path anywhere after this change.
- **Durability/ownership invariants unchanged.** This is a routing/addressing change only. The meta-log durability model, rendezvous ownership computation, and recovery are untouched.
- Build offline: `mvn -o -pl <module> -am test` (use `-Dsurefire.failIfNoSpecifiedTests=false` when `-am` builds dependency modules).

---

## Background: the problem this solves

Measured on the sharded dev cluster (3 dedicated controllers, `forceSync=no`, A-B-A to rule out disk drift): a DELETE op costs ~8–10ms with sharding on vs ~2ms off. Component attribution (`Diag` per-op timers) showed:

- **`ZK_FIDX` ≈ 4–5ms per create AND per delete** — the `fileId → namespace` znode write (`putFileNamespace` on create, `deleteFileNamespace` on delete). `n=0` when sharding is off. This is ~half the delete latency.
- The extra ZK write load also indirectly slows the meta-log quorum appends (shared SSD contention), ~1ms → ~5ms.
- Lock contention is **not** a factor (`REPO_LOCK_WAIT ≈ 0`).

Why the index exists: `ownerOf` is computed from the **namespace string**, but file-scoped ops (`CREATE_CHUNK`, `SEAL_CHUNK_META`, `DELETE_FILES`, …) carry only a `FileId`. The owning controller holds `fileId → namespace` locally (in-memory `fileIndex`), but a **non-owner** that receives a misrouted op (cold client cache, or a client that didn't create the file) has no local copy — so the mapping was kept in shared ZK. The code already flags the fix: [ZkMetadataStore.java](../../../strata-meta/src/main/java/io/strata/meta/ZkMetadataStore.java) — *"for very large fleets this should become owner bits encoded in the FileId."*

We rejected encoding a **hash of the namespace** into the FileId: it bakes a derived value, so changing the hash/rendezvous derivation strands old FileIds (their baked token no longer matches the namespace's freshly-computed owner). Instead we route by the **stable namespace identity itself**, supplied on each op, and compute the owner fresh every time — algorithm changes then move file-routing and ns-log ownership together, always consistent.

---

## Design

Every file-scoped metadata RPC gains a `namespace` field. The client (which always holds the namespace via the open file handle) supplies it; the controller's ownership gate uses it directly; the ZK owner-index is deleted.

### 1. Wire protocol (`strata-proto`)

Add a `StrataNamespace namespace` field to the file-scoped request messages (and their encode/decode):

- `CreateChunk`, `AllocateWriterEpoch`, `SealChunkMeta`, `AbortChunkMeta`, `SealFile`, `LookupFile`, `DeleteFiles`.

Unchanged (already namespace-scoped): `CreateFile`, `LookupPath`.

`DeleteFiles` carries a single `namespace` for the whole batch; a batch is single-namespace (see §2).

### 2. Client (`strata-client`)

- **`ControllerClient`** routes every op by **namespace**. Drop the `fileId → owner` cache and `inheritOwner`; keep only `namespace → owner` (`ownerByKey` keyed by namespace). Each file-scoped method takes a `StrataNamespace` and passes it as both the routing key and the message field.
- A `DeleteFiles` call is scoped to one namespace; callers that delete across namespaces issue one call per namespace. (The data path deletes one file at a time, so the common case is a single id.)
- **`StrataFileImpl` / `AppenderImpl` / `ReaderImpl` / `Recovery`** thread the file's namespace into every `ControllerClient` call. The handle already knows its namespace (it was created/opened in one).
- **`StrataClient.openById(FileId)` → `openById(StrataNamespace, FileId)`** ([StrataClient.java](../../../strata-client/src/main/java/io/strata/client/StrataClient.java), [StrataClientImpl.java](../../../strata-client/src/main/java/io/strata/client/StrataClientImpl.java)). No bare-FileId open remains.

### 3. Server (`strata-meta` / `Controller`)

- Replace `requireNamespaceOwnerForFile(FileId)` with `requireNamespaceOwner(StrataNamespace)` (the latter already exists, [Controller.java:406](../../../strata-meta/src/main/java/io/strata/meta/Controller.java)) at each file-scoped handler (`CREATE_CHUNK`, `ALLOCATE_WRITER_EPOCH`, `SEAL_CHUNK_META`, `ABORT_CHUNK_META`, `SEAL_FILE`, `LOOKUP_FILE`, `DELETE_FILES`), using the namespace from the decoded message.
- **Delete** `fileNamespace(FileId)`, `requireNamespaceOwnerForFile(FileId)`, `indexFileNamespace(...)`, `unindexFileNamespace(...)` and their call sites (the `indexFileNamespace` calls in the `CREATE_FILE` path, the `unindexFileNamespace` call in `markDeleting`).
- The owner's local `store.getFile(fileId)` resolution is **unchanged** — it uses the in-memory `fileIndex` + `warmOwnedNamespaces` ([NamespaceLogBackend.java:152-173](../../../strata-meta/src/main/java/io/strata/meta/NamespaceLogBackend.java)) and **never** consulted the ZK fidx. Deleting the fidx therefore cannot break it.

### 4. ZK root store (`ZkMetadataStore`)

- Remove `putFileNamespace`, `getFileNamespace`, `deleteFileNamespace`, the `META_FIDX` path constant, and any now-dead helpers. No reads or writes to that subtree remain.

### 5. Embedded meta-log client (`StrataSystemMetadataFileStore`)

- Meta-log/snapshot files live in the reserved `NamespaceLogBackend.SYSTEM_NAMESPACE` (`"strata-meta"`). Pass that namespace on its embedded-client `openById`/append/seal calls so they satisfy the new mandatory-namespace API. System files route locally regardless (`isSystem` short-circuits the ownership gate), so this is purely making the namespace explicit.

### 6. Load generator (`StrataPerf`)

- `openById` call sites ([StrataPerf.java:265,406](../../../strata-server/src/main/java/io/strata/server/StrataPerf.java)) pass the file's namespace (the perf creates files in known namespaces).

---

## Correctness notes

- **Membership-change stability (the reason for Method B):** both the file-scoped routing and the ns-log ownership derive the owner from the **same namespace string** via the **current** rendezvous algorithm. Changing the algorithm moves both together — no file is ever stranded. Nothing derived is persisted in the FileId.
- **Owner-side resolution unchanged:** `getFile(fileId)` → in-memory `fileIndex` → `repo(ns)`; cold/owned-but-unloaded namespaces are handled by `warmOwnedNamespaces`. This path predates and is independent of the ZK fidx.
- **Trust model (no extra verification):** the client supplies the namespace. A wrong namespace routes the op to the wrong owner, which does not hold the file → `FILE_NOT_FOUND`. It cannot affect a file in another namespace, because the owner resolves the target by `FileId` only within its own ns-logs. No server-side namespace/fileId cross-check is required.
- **System files:** `isSystem(namespace)` continues to short-circuit the ownership gate to the local ZK root; behavior unchanged.

---

## Testing

- **Unit (`strata-client`):** `ControllerClient` routes each file-scoped op by the supplied namespace; a `NOT_LEADER` redirect retargets and updates the `namespace → owner` cache; no `fileId`-keyed cache entries are created.
- **Unit (`strata-meta`):** each file-scoped handler accepts the op when this node owns the supplied namespace and redirects (`NOT_LEADER` + owner hint) when it does not — driven purely by the message namespace, with no ZK fidx present.
- **Integration (`strata-it`):** on a 3-controller sharded cluster, create/append/seal/delete/lookup across multiple namespaces all succeed and **zero** `META_FIDX` znodes are ever created (assert the subtree is absent). Existing sharding suites (`ChaosTest`, `ConsistencyVerifier`) still pass.
- **Perf (A/B):** re-run sharding-on vs off and compare DELETE p50 against the pre-fix A-B-A numbers. Expectation: with the fidx code path gone (no `META_FIDX` writes on create/delete), sharding-on DELETE p50 drops toward the ~2ms sharding-off baseline. (The `ZK_FIDX` `Diag` timer is removed along with the fidx methods; the robust assertion is the integration test's "zero `META_FIDX` znodes" check.)
- **Regression:** every existing `openById` / file-scoped test threads a namespace (clean break — update call sites, no compatibility overload).

---

## Out of scope (YAGNI)

- **The indirect meta-log slowdown** (3 controllers' meta-log streams contending on the shared SSD). This change removes the ZK write load that aggravates it, but does not restructure the meta-log append path. Re-measure after; address separately if still material.
- **Encoding owner/nsId bits in the FileId** (the rejected alternatives). Not needed once the namespace travels with every op.
- **A namespace registry / numeric namespace IDs.** Not required by Method B.

## Clean break

`META_FIDX` and all `*FileNamespace` methods are removed outright. No dual-read window, no migration of existing znodes, no compatibility overloads — consistent with the project's no-prod / no-compat policy.
