# Strata v0 — Implementation Plan & Status

Working state for the v0 build (tech design §4.4 bootstrap path: ZooKeeper-backed metadata, no Kafka fork).
This file is the durable source of truth for the development loop — update the checkboxes and decision log as work lands.

## Locked decisions

- **Stack:** Java 21 (Corretto), Maven multi-module (Maven 3.8.8 installed), JUnit 5, SLF4J-simple.
- **Networking:** blocking IO on virtual threads (thread-per-connection); correctness first, NIO later if ever needed.
- **ZK access:** Apache Curator (framework + recipes + curator-test for embedded ZK in tests).
- **Chaos:** testcontainers + Toxiproxy (gated on Docker availability).
- **Model checking:** TLA+ (tla2tools.jar fetched on demand), spec under `tla/`.
- **v0 protocol additions** (additive per tech design §10.6; fold back into the design doc at finalize):
  - `READ_LEDGER` (0x0019): returns integrity-ledger entries from an offset — seal recovery needs per-append boundaries without parsing payload.
  - Client↔metadata over SCP, opcode range 0x02xx: `CREATE_FILE` 0x0201, `CREATE_CHUNK` 0x0202, `SEAL_CHUNK_META` 0x0203, `LOOKUP_FILE` 0x0204, `DELETE_FILES` 0x0205. (v1 moves broker-facing APIs to Kafka RPC; range stays for tools.)
  - `NODE_HEARTBEAT` request gains tagged field `completedCommands` (commandId → status) so the repair coordinator learns copy completion.
  - `SEAL_CHUNK` (0x0015): footer sections from caller are optional; the node always computes CRC_RANGES + STATS itself so recovery-sealed chunks are byte-identical across replicas.
  - Error codes added: 15 NOT_LEADER (tagged leader hint), 16 NO_CAPACITY (placement cannot find 3 nodes).
- **v0 epoch source:** the caller (test harness) supplies `writeEpoch` — monotonic per file. v1 replaces this with Kafka controller leader epochs; storage layer is agnostic.
- **v0 metadata service:** single active instance in tests (Curator leader election implemented); leases in leader memory, registrations + file/chunk records in ZK (CAS via znode versions, leader-only writes).

## Module layout

```
strata-common   ids (FileId/ChunkId/NodeId), Varint, Crc32C, ErrorCode, exceptions
strata-proto    SCP frame codec, tagged fields, opcodes, message structs, ScpClient/ScpServer (virtual threads)
strata-format   chunk file header/footer/trailer, sidecar .meta, integrity ledger .j, ChunkStore engine + crash recovery
strata-node     storage node: SCP handlers → ChunkStore; register/heartbeat/inventory loop; REPLICATE executor (pull)
strata-meta     MetadataStore SPI, ZkMetadataStore, MetadataService (SCP listener, placement, leases, repair, retention)
strata-client   SegmentStore/Appender/Reader per design §12 (quorum ack, DO, roll, create-ahead, recoverAndSeal)
strata-it       integration tests: in-process cluster + embedded ZK (primary correctness layer); chaos via testcontainers
tla/            ChunkReplication.tla + TLC config
scripts/        verify.sh (full pyramid), run helpers
```

## Stages (gate: tests green, then commit)

- [ ] 1. Scaffold Maven multi-module; commit docs + plan + scaffold
- [ ] 2. strata-common primitives + unit tests
- [ ] 3. SCP framing + messages + roundtrip/golden tests
- [ ] 4. Chunk store engine + crash recovery + torn-write tests
- [ ] 5. Storage node server + single-node integration test
- [ ] 6. Metadata service on ZK (embedded-ZK tests)
- [ ] 7. Client library + 3-node end-to-end happy path
- [ ] 8. Failure paths: fencing, seal-and-roll, seal recovery (§7.3) + fault-injection tests
- [ ] 9. Repair + reconciliation + retention + tests
- [ ] 10. Chaos suite (testcontainers; start Docker first)
- [ ] 11. TLA+ spec + TLC run
- [ ] 12. verify.sh, design-doc updates (v0 additions), final commit

## Correctness invariants under test (tech design §14)

1. Acked data (quorum 2-of-3) survives any single node failure through seal — verified by the recorder/verifier harness in every integration & chaos test.
2. Single writer per epoch: a fenced appender can never ack again.
3. Readers never see bytes above the durable offset.
4. Commit-before-write: chunk exists in metadata before any byte lands on a node.
5. Sealed replicas are byte-identical (whole-file CRC equality across replicas).
6. Crash recovery converges to a CRC-verified prefix without parsing payload.

## How to verify

`scripts/verify.sh` — unit + integration (no Docker needed; embedded ZK). Chaos: `mvn -pl strata-it test -Dchaos=true` (requires Docker). TLA+: `scripts/tlc.sh`.
