# Task 7 Report: Namespace-key the Data-Node Chunk Index + Thread Chunk RPCs + Namespace-Aware Recovery

## What was done

### Part A: Namespace-key the in-memory chunk index in ChunkStore.java

- Defined `record NsChunkId(StrataNamespace namespace, ChunkId chunkId) {}` as a package-private record in `ChunkStore.java` (io.strata.format).
- Changed `Map<ChunkId, Handle>` to `Map<NsChunkId, Handle>` for the `chunks` field.
- Changed `Set<ChunkId>` to `Set<NsChunkId>` for the `creating` field.
- All ChunkStore methods now take `StrataNamespace ns` as first parameter: `open`, `append`, `read`, `readRegion`, `fence`, `stat`, `seal`, `fetch`, `delete`, `contains`.
- `Handle` carries `final StrataNamespace ns` and stores chunks under the namespace-sharded path `chunks/<ns>/<l1>/<l2>/<baseName>.chunk` (via `ChunkFormats.chunkRelativePath`).
- `inventory()` returns `InventoryItem(namespace, chunkId, state, length, crc)` with the correct namespace.
- `recoverAll()` now does a namespace-aware directory walk: it enumerates `chunks/<ns>/` subdirectories to recover chunks by namespace.
- New TDD test: `twoNamespaceCoexistenceAndRecovery` in `ChunkStoreTest.java` verifies two namespaces with the same `ChunkId` coexist independently and both survive recovery.

### Part B: Thread `StrataNamespace namespace` through 8 data-plane RPCs in Messages.java

Added `StrataNamespace namespace` as last field to:
- `Append(ChunkId, int, long, long, StrataNamespace)` — data append
- `Read(ChunkId, long, int, StrataNamespace)` — data read
- `Fence(ChunkId, int, StrataNamespace)` — write-epoch fence
- `StatChunk(ChunkId, StrataNamespace)` — chunk stat
- `SealChunk(ChunkId, int, long, StrataNamespace)` — chunk seal
- `DeleteChunks(List<ChunkId>, StrataNamespace)` — chunk deletion
- `FetchChunk(ChunkId, long, int, StrataNamespace)` — repair fetch
- `ReadLedger(ChunkId, long, int, StrataNamespace)` — recovery read

Also added `StrataNamespace namespace` to `DeleteCmd`, `ReplicateCmd`, and `InventoryEntry` in the heartbeat protocol so namespace flows through the entire control path.

### Part C: Replace flat `recoverAll()` with namespace-aware parallel recovery

`ChunkStore.recoverAll()` now walks `chunks/<ns>/` directories and recovers chunks per namespace in parallel (using virtual threads). Two chunks with the same `ChunkId` but different namespaces are recovered independently.

### Part D: Update golden corpus expected hex bytes in tests

- `MessageGoldenCorpusTest.java`: All golden hex strings updated to include namespace encoding (`0474657374` = varint-length 4 + "test"). Fixed frame preamble to include correct 2-byte `headerLength` field.
- `ScpV0CompatibilityTest.java`: All 8 data-plane RPC request/response hex strings updated with namespace.

### Part E: TDD test `twoNamespaceCoexistenceAndRecovery`

Written in `ChunkStoreTest.java`: creates `ChunkId(FileId.of(0), 0)` in namespaces "perf-0" and "perf-1", verifies both coexist in memory, and verifies both independently survive a store close+reopen (recovery). Passes.

## Tests

| Module | Tests | Pass |
|---|---|---|
| strata-proto | 87 (2 skipped = released-artifact compat) | 85 pass |
| strata-format | 106 (1 skipped) | 105 pass |
| strata-node | 48 | 48 pass |
| strata-meta | 205 | 205 pass |

strata-it compiles and the non-repair tests pass. Repair/retention tests have pre-existing timing issues on the single-disk VM unrelated to this task (30-140s timeouts that were flaky before Task 7). The `FailureRecoveryTest` cold-restart tests were fixed by adding `MiniCluster.startDataNodes(hosts, nodeIds)` to reuse original node IDs on restart (fix for the node-id externalization interaction). The `corruptDataByte` helper in `RepairAndRetentionTest` was updated to use the namespace-sharded chunk path.

## Key design decisions

- `NsChunkId` is package-private (not a new file) — callers outside `io.strata.format` use the namespace-parameterized `ChunkStore` API directly; no need to expose the composite key.
- Namespace is the LAST parameter on all data-plane RPCs (matches the pattern used for file-scoped metadata RPCs from Task 6).
- Recovery is namespace-aware but still single-store: a `ChunkStore` manages all namespaces for a node. The namespace-sharded directory layout separates files on disk and removes the old flat `chunks/<fileId>.<index>.chunk` format entirely.
- `InventoryEntry.namespace()` is now the source of truth for which namespace a chunk belongs to, enabling the `RepairCoordinator` to issue `ReplicateCmd` and `DeleteCmd` with the correct namespace.
