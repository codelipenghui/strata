# RepairCoordinator: event-driven repair + owner-local reconcile

**Status:** Design (approved 2026-06-21)
**Scope:** `strata-meta` — `RepairCoordinator`, `NodeRegistry`, `NamespaceLogBackend`/`NamespaceLogMetadataStore`, `Controller` config wiring.
**Goal:** Cut the repair scan's steady-state ZooKeeper read load (~24/s of `/strata` reads on the 3-node dev cluster, growing with cluster size) **without changing durability semantics**, and make repair cost scale with what *changed* rather than total cluster size.

## 1. Problem

In namespace-log / sharded mode the `RepairCoordinator` runs one loop every `STRATA_REPAIR_SCAN_INTERVAL_MS` (default **5s**):
- the global leader runs `scanOnce()` — a full reconcile over `allFileIds()` (every namespace × its files/chunks) plus cluster housekeeping;
- non-leader namespace owners run `ownerRepairPass()` — repair under-replication in the namespaces they own.

Measured behaviour (idle, no client load): **~29 ZK reads/s on `/strata`**, ~0.4 writes/s. Empirically confirmed the repair scan is the driver (slowing it 6× dropped reads 29→9/s); decomposition ≈ **120 ZK reads per pass** across the 3 controllers + ~5/s constant background.

Two facts make this wasteful:
1. **User-namespace metadata is already in-memory.** `NamespaceLogBackend.getFile`/`listFiles` resolve user namespaces from the in-memory meta-log state (`repo(ns).state()`); only the **system meta-log namespace** (`strata-meta`, the replicated Strata files that hold the logs) and the **node registry** hit ZK root. So the ZK reads are control-plane (meta-log file replica health + liveness), **not** user file metadata.
2. **The scan is ~72× more frequent than it can act.** A node death produces a repair only after `leaseMs + deadGraceMs` (~6 min default). The 5s cadence re-reads everything but the action is gated by dead-grace, so the frequent full enumeration buys almost nothing for the common case.

## 2. Goals / non-goals

**Goals**
- Repair becomes **event-driven** (off the in-memory node-death signal) plus a **slow full reconcile** as the correctness backstop.
- Repair is **owner-local**: each controller repairs only the namespaces it owns, from in-memory state; ZK is read only for the **system meta-log namespace** and only by the leader.
- Steady-state ZK reads drop from ~24/s (scan-driven) to roughly snapshot-poll + `O(#namespaces)`/reconcile, and **stop scaling with #files**.
- **No change to durability semantics** (RF, ack quorum, which chunks repair, the `REPLICATE` mechanism).

**Non-goals**
- No ZooKeeper watches (explicitly rejected in design — adds watch-lifecycle complexity; event + reconcile is simpler and sufficient).
- No change to the heartbeat/liveness *routing* (heartbeats still go to the leader; owners still learn liveness from the published cluster-live-nodes snapshot).
- No change to the per-chunk repair mechanics (`REPLICATE`, fencing, `chunksBeingRepaired` dedup) — they are reused verbatim.
- Not making the in-memory user reconcile itself incremental (possible later; out of scope).

## 3. Target architecture — three lanes

The single 5s loop splits into three lanes.

### Lane A — liveness / housekeeping tick (~5s, in-memory, role-specific)
Keeps the cadence of `STRATA_REPAIR_SCAN_INTERVAL_MS` but does **only in-memory / cached** work, differing by role:
- **Leader:** `publishClusterLiveNodes()` (one ZK *write*, not a per-file enumeration); `expireScan()` (in-memory lease/grace expiry — the leader's node-death event source, §4); `sweepStuckCommands()`.
- **Non-leader owners:** read the *cached* cluster-live-nodes snapshot; a node disappearing from it is the owner's node-death event source (§4). Owners have no heartbeat channel, so the published snapshot is how they learn of death.

No per-file ZK reads in this lane: the leader's publish is one write, an owner's snapshot read is one cached `getData`.

### Lane B — event-driven repair (every controller)
On a node ALIVE→DEAD transition (detected in `expireScan`), fire a **targeted** repair for the chunks that node held:
- **User namespaces:** each owning controller scans the namespaces it owns *in memory*, finds chunks with a replica on the dead node, and issues `REPLICATE` — reusing today's per-chunk repair logic and the `chunksBeingRepaired` dedup. **No ZK.**
- **System meta-log namespace:** the global leader repairs the affected meta-log chunks. This reads the system file records from ZK root — but **only on a real node death**, not every 5s.
- Gated by the existing settle period (`now - leaderSince < leaseMs + deadGraceMs` ⇒ skip), so re-registering nodes after a leader change don't trigger spurious repairs.

### Lane C — slow full reconcile (configurable, default 60s)
The correctness backstop: the equivalent of today's `scanOnce()`/`ownerRepairPass()`, run rarely.
- **User namespaces:** reconciled in memory (no ZK), per owner.
- **System meta-log namespace:** the leader enumerates the meta-log files from ZK — `O(#namespaces)`, every 60s instead of every 5s (~12× rarer).
- Refreshes the durability gauges (under-replicated / unavailable / at-min) and runs `sweepDeletedFiles()`.

### Unification
`scanOnce` (leader) and `ownerRepairPass` (owners) collapse into one `repairOwned()` that **every** controller runs for the namespaces it owns (in-memory, no ZK). The leader **additionally** runs `repairSystem()` (the meta-log files in ZK) and Lane A housekeeping. The leader no longer scans its own user namespaces twice.

Liveness in the repair decision: the **leader** uses the in-memory `NodeRegistry` (authoritative from heartbeats) instead of re-reading `listNodes` from ZK; **non-leader owners** use the cached cluster-live-nodes snapshot (they have no heartbeat channel). Optionally raise the snapshot reload TTL (`SNAPSHOT_RELOAD_MS` 100ms → 1s) to trim the constant — gated as a separate, low-risk step.

## 4. Node-death event flow

1. Node-death detection is **role-specific** (owners have no heartbeat channel):
   - **Leader:** `NodeRegistry.expireScan()` transitions nodes to DEAD on lease+grace expiry; a hook on the ALIVE→DEAD edge invokes `onNodeDead(nodeId)`.
   - **Non-leader owners:** detect a node leaving the published cluster-live-nodes snapshot during their Lane-A liveness read, and invoke `onNodeDead(nodeId)` for their owned namespaces.
2. `RepairCoordinator` enqueues a repair task for `nodeId` onto a single-thread worker (same pattern as the existing `deleteDispatchExecutor`/`completionExecutor`).
3. The worker, for the owning controller:
   - iterates the in-memory state of the namespaces it owns, collecting chunks that have a replica on `nodeId`;
   - if leader, also enumerates the system meta-log files (ZK) for chunks on `nodeId`;
   - issues `REPLICATE` for each under-replicated chunk via the existing repair path; dedups against `chunksBeingRepaired`; respects the settle gate.

Events are best-effort. Correctness does **not** depend on any single event being delivered — Lane C reconciles regardless (§5).

## 5. Durability preservation

- **Ownership invariant:** every namespace has exactly one owner (rendezvous hash) = its sole repairer; the system namespace's sole repairer is the global leader. ⇒ every chunk has exactly one repairer — **no gaps, no double-repair**. This is the same ownership model already used for request routing.
- **Reconcile is the backstop; events are optimization.** A missed event (node death during a leader/ownership change, or a controller restart that drops the queue) is caught by the next reconcile (≤ reconcile interval).
- **End-to-end repair time unchanged.** Worst-case *detection* latency rises 5s → 60s, but the repair *action* is gated by dead-grace (~6 min), which dominates — so time-to-repair is effectively identical.
- **Semantics untouched:** RF, ack quorum, target selection, `REPLICATE`/fencing are reused unchanged.
- **Transitions & restarts:** on ownership change the new owner's reconcile picks up its namespaces; the settle period suppresses spurious repairs while nodes re-register; a restarted controller loses its event queue but the reconcile rebuilds the full picture well within dead-grace.

## 6. ZK cost / scalability

| | today | after |
|---|---|---|
| Lane A tick (5s) | full enumeration, ~tens of ZK reads/pass | in-memory only, ~0 ZK reads |
| liveness | `listNodes` from ZK each pass × 3 controllers | leader in-memory; owners read the cached snapshot |
| node-death repair | folded into the 5s scan | ZK only on a real death, `O(chunks on dead node)` |
| full reconcile | every 5s, `O(all files)` | every 60s, system files only = `O(#namespaces)` ZK |

Net on the current cluster: scan-driven **~24/s → ~2–3/s**. Scaling: repair cost tracks what changed (a node death) rather than total cluster size; the only periodic ZK enumeration is `O(#namespaces)` every 60s.

## 7. Config

- **New** `STRATA_REPAIR_RECONCILE_INTERVAL_MS` (system prop `strata.repair.reconcile.interval.ms`), default `60000` — Lane C cadence. Added to the repair config object the `RepairCoordinator` already receives (alongside `repairScanIntervalMs()`/`leaseMs()`/`deadGraceMs()`), read at startup the same way the existing repair settings are.
- `STRATA_REPAIR_SCAN_INTERVAL_MS` (default 5000) — repurposed as the Lane A / liveness-publish cadence; meaning documented.
- **Optional, separately gated** `SNAPSHOT_RELOAD_MS` 100ms → 1s for the live-nodes snapshot cache.

## 8. Observability

- New counters: repairs by trigger (`event` vs `reconcile`), node-death events processed, reconcile duration/last-run timestamp.
- Keep the durability gauges (`strata_chunks_under_replicated` / `_unavailable` / `_at_min_redundancy`), now refreshed on reconcile and on event-driven repair.

## 9. Testing

- **Unit:** `onNodeDead` fires targeted owner-local repair (and system repair when leader); the reconcile catches a *deliberately dropped* event; ownership-transition handoff repairs the moved namespace; the settle gate suppresses spurious repairs while nodes re-register.
- **Regression (must stay green):** `RepairCoordinatorTest`, `RepairReliabilityTest`, `NamespaceLogBackendDurabilityTest`, `ControllerTest` — they exercise the per-chunk repair logic reused here.
- **IT / chaos:** `ChaosTest` and the process-crash suites still restore full RF (now via the event path); add an assertion that the ZK read-rate stays bounded while replicas recover.

## 10. Staging (preserve durability at each step)

1. **In-memory liveness for the leader + cache `listNamespaces`/snapshot** in the existing loop (no structural change). Independently shippable; trims ZK reads; durability unchanged.
2. **Introduce Lane C (slow reconcile) + Lane A (light tick)** by splitting the loop; the reconcile keeps doing the full pass. Repair correctness still entirely on the (now slower) reconcile.
3. **Add Lane B (event-driven repair)** off `onNodeDead`; reconcile remains the backstop. This is where the latency win lands while the backstop guarantees safety.
4. **Unify** `scanOnce`/`ownerRepairPass` into `repairOwned()` + `repairSystem()` and remove the redundant leader user-namespace scan.

Each step is independently testable and leaves durability intact, so we can stop or roll back at any stage.
