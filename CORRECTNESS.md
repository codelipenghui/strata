# Strata Correctness Contract

This file is the release-gate map for stress and fault correctness. Metrics are intentionally
out of scope here; the only purpose is to make safety claims traceable to deterministic tests,
fault gates, and model checking.

## Core Invariants

| Invariant | Required evidence |
|---|---|
| Acked bytes are never lost. | `Workload`, `BinaryWorkload`, and `ConsistencyVerifier` compare every acknowledged prefix after normal runs, metadata failover, storage restart, process crash, repair, and stress/fault schedules. |
| Unacknowledged tails are never exposed as committed data. | `EndToEndTest`, `FailureRecoveryTest.openReaderDoesNotExposeSingleReplicaUncommittedTail`, `sealRecoveryDoesNotCommitSingleReplicaUncommittedTail`, and recovery tests assert reads are bounded by durable or sealed length. |
| A fenced writer cannot acknowledge new bytes. | `FailureRecoveryTest` fencing cases and `AppenderImplTest` cover epoch replacement, replica failure, and permanent appender death after fencing. |
| Append replay is never attempted after an ambiguous storage reconnect. | `AppenderImplTest`, `NodePoolTest`, and pooled fault cases pin appends to the connection generation used to open a chunk and roll on generation replacement. |
| Storage connection pooling preserves per-session ordering. | `AppenderImplTest.appendPinsReplicaConnectionWhenEndpointHasMultipleConnections` proves an appender keeps its per-replica pinned connection for the open chunk and fails/rolls on generation replacement. `ReaderImplTest.readerPinsEndpointConnectionAcrossReads` proves one reader keeps a stable endpoint connection, while `independentReadersCanUseDifferentEndpointPoolConnections` proves separate readers can still spread over the endpoint pool. |
| Partial chunk open cannot strand unsafe metadata. | `OpenQuorumFailureTest` covers both below-quorum open abort and quorum-open with a short replica set; the latter seals only successfully opened replicas and repairs back to full RF after the failed node returns. |
| Live file descriptors remain structurally safe during faults. | `ConsistencyVerifier.assertLiveFileDescriptorConsistent` is called by deterministic stress/fault and child-JVM process fault schedules before seal or recovery. It requires valid write policy, contiguous chunk indexes, sealed-prefix/open-tail ordering, open tails with no advertised sealed length/CRC, quorum-sized non-duplicate replica sets, and reachable ack quorum for each live chunk. |
| Metadata commit precedes physical writes for live chunks. | `RecoveryCatchUpTest`, `RepairAndRetentionTest`, and the TLA+ model cover recovery and orphan handling under commit-before-write assumptions. |
| Seal recovery commits only a quorum-recoverable, CRC-verified prefix. | `RecoveryTest`, `RecoveryCatchUpTest`, `RecoveryDivergenceTest`, `FailureRecoveryTest`, and `MetadataFailoverTest` cover divergent replicas, unavailable replicas, single-replica dirty tails, all-open-replica restarts, fsync recovery, a writer dying after replica seal but before metadata commit, a writer dying after chunk metadata commit but before file seal, idempotent retry after final file seal commit, and controller leader failover across the replica-seal boundary. |
| Sealed replicas are byte-identical and self-verifying. | `ConsistencyVerifier.assertSealedFileConsistent` validates metadata length/CRC plus raw `FETCH_CHUNK` header, data, footer, and whole-image consistency across replicas. |
| Repair never promotes corrupt, partial, or stale replicas. | `RepairAndRetentionTest`, `RepairCoordinatorTest`, `RepairReliabilityTest`, deterministic stress/fault artifacts, and process-crash repair cases cover copy verification, replicate/delete command retry, stale inventory, scrubbed sealed-replica bit rot, lost completion acknowledgements, and convergence back to full readable RF after a killed data node rejoins. |
| Storage-node command completions are retried until a controller leader accepts them. | `ControlLoopTest.failedHeartbeatCompletionIsResentAfterEndpointRotationAndReregistration` drains a completed command into a heartbeat that fails with `NOT_LEADER`, rotates to another controller endpoint, re-registers, and proves the same completion is resent on the new leader heartbeat. |
| A healed storage-network partition cannot preserve a stale open replica. | `ChaosTest.healedReplicaPartitionRepairsStaleOpenCopyToSealedRf` partitions all client data traffic to one required RF=4 replica, continues writes and seal through AQ=3, heals the link, then requires repair to replace the stale local copy and converge to four byte-identical sealed replicas. `ChaosTest.processStoragePartitionHealsAndRepairsStaleOpenCopy` repeats the same property with data nodes running as child JVMs and only the data endpoint routed through Toxiproxy. |
| Stale inventory cannot erase healthy replicas after metadata failover. | `MetadataFailoverTest.staleInventoryAfterLeaderFailoverCannotDropHealthyReplica` kills the controller leader, sends a stale destructive inventory report to the new leader, and verifies the sealed replica set and acked data remain intact. |
| Metadata leader endpoint failures cannot strand boundary operations. | `ChaosTest.metadataLeaderProxyBlackholeDuringChunkRollRetriesOnNewLeader` routes the client's active controller leader connection through Toxiproxy, black-holes that endpoint, kills the leader, forces a chunk roll, and verifies retry on the new leader preserves acknowledged bytes and sealed replica consistency. |
| Deletion cannot be undone by stale writers or stale create replays. | `RepairAndRetentionTest.deletingOpenFileCannotBeResurrectedByStaleAppender` deletes an open file, requires the stale appender's seal to fail, and verifies metadata plus physical chunks converge to deletion. `ControllerTest.deletingFileCannotBeSealedOrResurrected` rejects stale chunk seal, chunk create, chunk abort, and file seal requests once a file is `DELETING`. `ControllerTest.createFileReplayAfterDeleteCannotReturnOldFileOrClearReplacementPath` rejects old `CREATE_FILE` operation-id replays after deletion and after same-path replacement. |
| Metadata failover cannot interrupt deletion convergence. | `MetadataFailoverTest.leaderFailoverDuringOpenFileDeletionConvergesAndDoesNotResurrect` deletes an open file, kills the controller leader, verifies the stale appender cannot resurrect the file, and waits for all physical chunks to disappear. |
| Real process crashes cannot strand appends at metadata or storage boundaries. | `MetadataProcessFailoverTest.clientAndDataNodesRecoverAfterMetadataLeaderProcessCrashDuringChunkRoll` runs two controllers as child JVMs, kills the active metadata SCP endpoint while an appender is open, forces a post-crash chunk roll, then verifies acked data and sealed replica consistency through the surviving metadata process. `clientSurvivesMetadataLeaderAndDataNodeReplicaProcessCrashInSameOpenSession` adds child-JVM data nodes and kills both the controller leader process and an open-chunk storage replica process in one write session. `deterministicProcessFaultSchedulePreservesAckedBytes` repeatedly appends under a deterministic child-JVM schedule that kills an open-chunk storage process, fails over metadata, restarts storage on the same disk, verifies open reads remain acknowledged prefixes, then seals and verifies replica consistency. `fullProcessClusterRestartRecoversAckedOpenChunkFromSameDisks` kills every Strata metadata and storage child JVM, restarts them on the same ZooKeeper namespace and storage directories, verifies node identity and endpoint refresh, then recovers and seals the acknowledged open chunk. |
| External-style multi-process nemesis schedules preserve acknowledged bytes. | `ExternalNemesisTest.childProcessStoragePartitionAndMetadataLeaderLossPreserveAckedBytes` runs metadata and storage as child JVMs, advertises one storage data endpoint through Toxiproxy, black-holes that storage link, black-holes the client's controller leader endpoint, kills the actual leader process, seals through the surviving metadata process and storage quorum, then heals the data endpoint, requires repair to converge to full RF, restarts the healed storage process from the same disk, and re-verifies the sealed bytes. `concurrentClientFilesSurviveSharedStorageAndMetadataFaults` keeps two independent client/file/appender sessions and two long-lived cross-client readers active through the same storage partition plus controller leader loss, verifies each reader only returns acknowledged prefixes, seals both files, restarts the healed storage process, and requires both files to converge to full RF. `storageControlPartitionDelaysRepairUntilHeartbeatPathHeals` starts a spare storage process whose metadata heartbeat/control path is routed through Toxiproxy, partitions that control path, kills an existing sealed replica, verifies the file remains readable at ack quorum but below full readable RF, then heals the control path and requires repair to restore full readable RF before re-verifying the sealed bytes. |
| Metadata backend outages do not corrupt acknowledged data. | `ChaosTest.zookeeperPauseDoesNotAffectDataPath` pauses containerized ZooKeeper while appends continue inside the current chunk. `ChaosTest.zookeeperPauseDuringChunkRollIsRetriedWithoutAckedLoss` and `zookeeperPauseDuringFinalSealIsRetriedWithoutAckedLoss` pause ZooKeeper at metadata boundaries, then require metadata retry to finish without losing acknowledged bytes. `zookeeperLongPauseDuringChunkRollFailsCleanlyAndRecoversAckedPrefix` and `zookeeperLongPauseDuringFinalSealFailsCleanlyAndRecoversAckedPrefix` leave ZooKeeper paused past the metadata retry deadline, require the live appender to fail cleanly, then recover and seal the acknowledged prefix after ZooKeeper returns. |
| File namespace/path binding survives metadata restart, delete/recreate, and deferred old-file cleanup. | `FailureRecoveryTest.fullClusterColdRestartAfterZooKeeperRestartPreservesMetadataAndData` and `pathDeleteAndRecreateAfterZooKeeperRestartBindsOnlyNewFile` cover ZK restart, lookup stability, deletion, and same-path replacement with a new file id. `MetadataStoreConformanceTest.finalDeleteOfOldFileDoesNotRemoveReplacementPathBinding` requires final cleanup of an old `DELETING` file to preserve a replacement path marker. |
| Node identity survives restart when the same disks return. | `FailureRecoveryTest`, `StressFaultTest`, and `ProcessCrashRecoveryTest` assert restarted data nodes keep their node ids and controller endpoints refresh before recovery proceeds. |
| Metadata backend semantics are stable across implementations. | `MetadataStoreConformanceTest` now runs against both `ZkMetadataStoreConformanceTest` and `InMemoryMetadataStoreConformanceTest`, covering file/path/node lifecycle, namespace scoping, list visibility, CAS wins/losses, failed duplicate path creation without orphan records, replacement-safe final deletes, permanent deleted-FileId reservation, idempotent missing deletes, store-handle close/reopen preservation of state, versions, and node-id allocation, stale leader handles being unable to update, delete, or replay-create an old file after another handle deletes it and reuses the path for a replacement file, and repeated delete/recreate histories where every prior `FileId` remains permanently reserved even while the path is temporarily free. |
| SCP v0 wire encodings remain stable and current peers remain v0-compatible. | `MessageGoldenCorpusTest` locks exact bytes for opcodes, error codes, all current message families, response headers, and a complete frame. `ScpV0CompatibilityTest` drives frozen raw v0 client frames through the current Netty server and frozen raw v0 server responses through the current Netty client across data, node-control, and client-metadata opcodes. `ScpReleasedArtifactCompatibilityTest` plus `scripts/scp-compat.sh` run the current checkout against released `io.strata:strata-proto` artifacts in isolated JVMs when release versions are supplied. |

## Required Gates

Run these before treating a correctness-sensitive change as ready:

```bash
./scripts/verify.sh
./scripts/verify.sh --skip-default --compat
./scripts/verify.sh --skip-default --fault
./scripts/verify.sh --skip-default --tlc
```

Run these for scheduled/manual confidence and before major storage changes:

```bash
./scripts/verify.sh --skip-default --chaos
./scripts/verify.sh --skip-default --soak
./scripts/verify.sh --skip-default --compat --compat-version=<released-version>
```

The stress/fault and soak gates write replay artifacts under
`strata-it/target/correctness-artifacts/`, including runtime context, seed, exact replay command,
policy, fault events, live descriptor verification points, acknowledged byte count/hash, and final
descriptors. Artifact-producing tests validate this evidence before passing, and `verify.sh`
audits real gate artifacts to reject missing or empty replay identity/runtime fields, missing pass
status, recorded failures, missing acknowledged-byte/hash evidence, missing descriptor evidence,
descriptor/write-policy mismatches, final descriptors whose replica count is below the declared
replication factor or repeat a replica node ID, final descriptors whose chunk lengths do not sum to
the acknowledged byte count, missing scenario-specific replay selectors, or missing
scenario-specific safety markers. The fault gate also requires all five deterministic stress/fault
policy cases, all four storage-process crash recovery cases, the child-JVM process-stress schedule,
and the full-process-restart artifact. The chaos gate requires all external nemesis artifacts, and
the soak gate requires the soak replay artifact. Before any artifact-producing gate trusts real
artifacts, it runs `scripts/correctness-artifacts.sh --self-test` against synthetic positive and
negative fixtures so the verifier itself fails closed.
The soak gate records the full soak replay command plus one `iterationReplayCommand` per iteration.
Re-run a full soak iteration with:

```bash
./scripts/verify.sh --skip-default --fault --stress-only -Dstrata.stress.seed=<iteration-seed> -Dstrata.stress.batches=<batches>
```

Use the same `--stress-only -Dstrata.stress.seed=<iteration-seed>
-Dstrata.stress.batches=<batches>` form with `scripts/coverage.sh --skip-default --fault` when
the full stress/fault matrix replay needs an aggregate coverage report.

Re-run one exact stress/fault case from an artifact with:

```bash
./scripts/verify.sh --skip-default --fault -Dstrata.stress.case=<case> -Dstrata.stress.seed=<seed> -Dstrata.stress.batches=<batches>
```

When `strata.stress.case` is set to a non-`all` value, the fault gate runs `StressFaultTest` only
so the artifact seed and case map to one deterministic replay.

Re-run one targeted fault artifact, such as a process-crash recovery artifact, with:

```bash
./scripts/verify.sh --skip-default --fault -Dtest=<TestClass#method>
```

Use the same `-Dtest=<TestClass#method>` form with `scripts/coverage.sh --skip-default --fault`
when the replay needs an aggregate coverage report.

Re-run the external child-JVM nemesis schedules from an artifact seed with:

```bash
./scripts/verify.sh --skip-default --chaos -Dstrata.external.seed=<seed>
```

Re-run one targeted external nemesis artifact with:

```bash
./scripts/verify.sh --skip-default --chaos -Dtest=<TestClass#method> -Dstrata.external.seed=<seed>
```

Use the same `-Dtest=<TestClass#method>` form with `scripts/coverage.sh --skip-default --chaos`
when the external replay needs an aggregate coverage report.

## CI Policy

`.github/workflows/ci.yml` is the pull-request and `main` push merge gate. It runs the default,
current compatibility/conformance, embedded fault, and TLA+ model-checking gates, then uploads
test reports, process-crash logs, metadata-process logs, and replayable correctness artifacts.

`.github/workflows/correctness.yml` is the scheduled/manual deep-confidence workflow. Scheduled
runs cover the CI gates plus Docker/Toxiproxy chaos and bounded stress/fault soak. Manual runs can
toggle individual gates and supply `compat_versions` to add the SCP released-artifact
compatibility matrix to the compatibility job.

## Known Remaining Gaps

- The metadata plane is the Strata `MetadataStore` over a ZooKeeper consensus root (plus per-namespace
  metadata logs); there is no other consensus backend planned. The shared `MetadataStore` conformance
  suite runs against three implementations — the ZooKeeper-direct store, the namespace-log store, and
  an independent in-memory reference — which keeps the SPI honest and backend-swappable.
- Current SCP compatibility and metadata-store conformance run as a CI gate. Released-artifact SCP
  compatibility is additive when a version is supplied, but no historical release version has been
  exercised yet because the project has not published a stable protocol artifact.
- Jepsen-style multi-host partitions are approximated by embedded, child-JVM, and Toxiproxy tests,
  including a reusable external-style child-process nemesis harness. A full external multi-host
  deployment nemesis is still future work.
- Metrics and alerting are intentionally deferred.
