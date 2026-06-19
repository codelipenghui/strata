# Bounded LRU FileChannel cache for sealed chunks

- **Date:** 2026-06-19
- **Status:** Approved design — ready for implementation planning
- **Area:** `strata-format` (`ChunkStore`), with touchpoints in `strata-proto` (read/sendfile path) and `strata-server`/`strata-metrics` (observability)

## 1. Problem

The storage node opens **one persistent `FileChannel` per resident chunk** and holds it for the chunk's
entire lifetime. The descriptor is opened in `ChunkStore.open()`
([ChunkStore.java:522](../../../strata-format/src/main/java/io/strata/format/ChunkStore.java)),
`recoverOne()` (~:1458), and `importSealed()` (~:1186), and closed only in `delete()` (~:1281) and
`close()` (~:1842). OPEN chunks hold a **second** descriptor for the `.j` integrity ledger.

Two consequences:

1. **Startup FD storm.** `recoverAll()` scans the data directory and opens a persistent channel for every
   `.chunk` on disk. A node with N resident chunks holds ≥N descriptors before serving a single request.
2. **The ~1000-chunk gate.** With the common default soft `RLIMIT_NOFILE` of 1024, the node exhausts file
   descriptors at roughly a thousand resident chunks — far below the scale target of ~1 GB chunks and
   ~1M chunk descriptors per PB of retained data (tech design §3/§4.2).

A second, smaller FD source is the read path: `readRegion()` opens a **fresh transient** READ-only channel
per client read for the zero-copy/sendfile transfer (~:809 OPEN-clamped, ~:828 SEALED), closed when the
async write completes. This is already bounded by `strata.scp.server.maxInflightRequests` (default 1024)
per connection, so it is not the gate — but it is additive and worth collapsing.

### FD model summary

| Source | Scope | Bound today |
| --- | --- | --- |
| Persistent `h.data` | 1 per resident chunk (OPEN + SEALED) | resident chunk count — **the gate** |
| Persistent ledger | 1 per OPEN chunk | OPEN chunk count |
| Transient read | 1 per in-flight client read | `maxInflightRequests`/conn |

## 2. Goals / non-goals

**Goals**

- Decouple open-FD count from resident-chunk count. After this change:
  `open FDs ≈ (OPEN + pending-reclaim sealed, pinned) + (cache capacity) + (leased-but-evicted channels held by in-flight transfers)` —
  all bounded, configurable, independent of total sealed-chunk count.
- Remove the `recoverAll()` startup FD storm for sealed chunks.
- Collapse transient per-read FDs into shared cached channels.
- Preserve every existing durability, recovery, and concurrency invariant.

**Non-goals**

- Caching/eviction of OPEN-chunk channels or ledgers (they stay pinned — see §10 assumption).
- Touching the other scalability blockers (`usedBytes()` heartbeat stat-hammer, `scrubOnce()` full re-CRC,
  sequential `recoverAll`, inventory sharding). The cache must not regress them; it does not fix them.
- Any change to the on-disk chunk format or the wire protocol.

## 3. Decisions

| Decision | Choice | Rationale |
| --- | --- | --- |
| Cache scope | **Sealed-only; pin OPEN** | OPEN chunks are few (only tail chunks under active write) and carry every hazard (committer forces, seal truncate, ledger ordering, identity checks). SEALED chunks are immutable, read-only, and the bulk at scale. Caching only sealed channels sidesteps the concurrency risk. |
| Read/sendfile path | **Routed through the cache via ref-counted leases** | Collapses transient read FDs; hot sealed chunks share one FD across concurrent sendfiles (positional `transferTo` is stateless, so sharing is safe). |
| Capacity | **Auto-sized from the OS FD limit, with a knob override** | Operator does not have to hand-tune against `ulimit`; also yields a live open-FD metric for free. |

## 4. Architecture

### 4.1 `ChannelCache` (new class, `strata-format`)

A small, purpose-built, reference-counted LRU. No new dependency (the module declares only slf4j today;
no Caffeine/Guava). ~150 lines.

State:

- `Map<ChunkId, Entry>` in **access order** (LRU). `Entry` holds:
  - `FileChannel channel` — opened READ-only.
  - `int refCount` — number of outstanding leases.
  - `boolean evicted` — removed from the map but kept alive until the last lease releases.
- A single `ReentrantLock` guarding **only** the map / LRU pointer mutations.

API:

```java
final class ChannelCache implements AutoCloseable {
    Lease acquire(ChunkId id, Path dataPath) throws IOException; // open-on-miss
    void invalidate(ChunkId id);   // delete/quarantine: close idle now, or defer to last lease
    void close();                  // closeAll: close idle channels (no leases expected at shutdown)
    long hits(); long misses(); long evictions(); int size(); // observability
}

interface Lease extends AutoCloseable {
    FileChannel channel();
    void close();                  // refCount--; idempotent; physical close iff evicted && last lease
}
```

### 4.2 Concurrency protocol (no I/O under the lock)

The lock is **never** held across `FileChannel.open()`, `close()`, or `force()` — this avoids virtual-thread
carrier pinning (the codebase's known `synchronized`-across-blocking-I/O hazard) and keeps cache hits cheap.

- **`acquire` hit:** lock → entry present → `refCount++`, move to MRU → unlock → return lease. (`hits++`)
- **`acquire` miss:** lock → absent → unlock → `FileChannel.open(dataPath, READ)` **outside lock** →
  re-lock → resolve race: if another thread inserted meanwhile, `refCount++` the winner and remember to
  close our loser **outside the lock**; else insert with `refCount=1`. Then, still under lock, collect
  eviction victims from the LRU tail (entries with `refCount==0`), remove them from the map → unlock →
  close the loser and victims **outside the lock**. (`misses++`, `evictions += victims`)
- **Eviction is a soft cap.** Only `refCount==0` entries are closed. If the cache is over capacity but all
  entries are leased (more concurrent **distinct** sealed reads than capacity), the cache temporarily
  exceeds capacity and meters it; it **never** closes a leased FD. Worst case is bounded by concurrent
  distinct reads.
- **`Lease.close`:** lock → `refCount--`; if `evicted && refCount==0`, take the channel out and unlock →
  `channel.close()` outside the lock. A `closed` flag on the lease makes this idempotent (downstream may
  call close more than once via the Netty/Frame path).
- **`invalidate`:** lock → remove entry. If `refCount==0`, take channel → unlock → close. Else mark
  `evicted` (last lease closes it). Unlinking the file while leases hold FDs is safe — the inode stays
  alive until the last descriptor closes.

### 4.3 `ChunkStore` integration — pinned vs. evictable lifecycle

`Handle.data` keeps today's meaning **only while pinned**. A chunk is **pinned** (owns a persistent
writable `h.data`, possibly a ledger, untouched by the cache) when:

- state is OPEN, **or**
- state is SEALED but the ledger has not yet been reclaimed (`sealedLedgerPending == true`).

A chunk is **evictable** (`h.data == null`, all access via the cache) once it is SEALED **and** durable:

| Path | pinned → evictable transition |
| --- | --- |
| `seal()` with `SEAL_FSYNC=true` | end of `seal()` (data forced, ledger dropped) → close + null `h.data` |
| `seal()` with `SEAL_FSYNC=false` (default) | end of `reclaimSealedLedgersOnce()` for that chunk (after it forces data+sidecar durable and drops the ledger) → close + null `h.data` |
| `importSealed()` | right after install (already durable) → close + null `h.data` |
| `recoverOne()` SEALED branch | **never opens a persistent `h.data`** — validate header/trailer/footer via a transient channel, install with `data == null` |

`recoverOne()` OPEN branch is unchanged (pinned writable channel + ledger). Removing the persistent
sealed-channel open in recovery is what eliminates the startup FD storm.

The close+null of `h.data` at a transition runs under the chunk monitor and re-checks
`chunks.get(id)==h && h.data==snapshot && state==SEALED` before acting, so it cannot race a concurrent
`delete()` (which sets `DELETING` and closes under the same monitor) or the existing background-thread
identity checks.

### 4.4 Sealed access paths → cache lease

For each sealed-serving path, **snapshot the sealed-immutable fields under the monitor**
(`sealedLength`, `sealedRangeCrcs`, `dataPath`, `lastKnownDO`), release the monitor, then acquire a lease
and do I/O **outside** the monitor. This is safe because sealed data never changes, and it also reduces the
existing carrier pinning on the verified-read and scrub paths.

- **`readRegion()` SEALED branch (zero-copy):** acquire a lease outside the monitor; carry it in
  `ReadRegionResult`. See §4.5.
- **Verified `read()` → `readSealedVerified`:** acquire lease, verify range CRCs and copy out, release.
- **`fetch()` (sealed-only repair):** acquire lease, read into the byte buffer, release.
- **`scrubOnce()`:** for each SEALED chunk, snapshot `sealedLength`, acquire lease, re-CRC outside the
  monitor, release; update `h.dataCrc` under the monitor on mismatch (unchanged semantics).

OPEN-chunk paths are **completely untouched**: `appendAsync` write + ledger append under the monitor, the
`GroupCommitter` force closure, `seal()` `truncate` after `stopCommitter()`, `backgroundFlushOnce()` /
`reclaimSealedLedgersOnce()` snapshot-then-force-outside-monitor with `h.data==data` identity checks. All of
these operate on the pinned writable channel exactly as today.

### 4.5 Read/sendfile lease path (cross-module, `strata-proto`)

`ReadRegionResult` carries the `Lease` (for sealed chunks); its `close()` **releases the lease instead of
closing the FD**. The cached channel is shared across concurrent sendfiles.

The one subtlety: Netty's `DefaultFileRegion` closes its backing channel on deallocate, which would close
a shared cached channel out from under other readers. The plan: a `LeasedFileRegion` (subclass overriding
`deallocate()` to release the lease and **not** close the channel), or otherwise disable Netty's
auto-close, so that **the lease is the single owner** of "done with this channel" and the physical
`close()` happens only via cache eviction at `refCount==0`. Exact wiring against
`ScpServer` (`DefaultFileRegion` construction ~:376, write listener ~:390) and `Frame.FilePayload`
(`close()`) is to be pinned down during planning; the requirement is invariant: **neither Netty nor Frame
may close the underlying cached FD — they release the lease.**

OPEN-clamped client reads (`readRegion` OPEN branch) and recovery reads keep their current behavior (OPEN
chunks are pinned; recovery materializes/verifies the undurable tail under the monitor as a heap snapshot).

### 4.6 Delete and shutdown

- **`delete()`:** call `channelCache.invalidate(id)`. For pinned chunks (`h.data != null`) also close as
  today; for evictable chunks (`h.data == null`) there is nothing to close beyond the cache entry.
  Unlinking is safe with leased FDs (inode kept alive until leases drain).
- **`close()` (store shutdown):** after the existing pinned-handle close loop, call `channelCache.close()`.
  Flusher and cleanup executors are already stopped first.

## 5. Capacity auto-sizing + config

- Knob: `strata.fileChannelCache.maxSize` / `STRATA_FILE_CHANNEL_CACHE_MAX_SIZE` via the existing
  `longConf(property, env, default)` convention.
- Default (auto): from `com.sun.management.UnixOperatingSystemMXBean.getMaxFileDescriptorCount()` (the soft
  `RLIMIT_NOFILE`), minus headroom reserved for pinned OPEN channels, ledgers, sockets, and in-flight
  transient FDs. Headroom = `max(256, 25% of the soft limit)`; capacity floored at 128. Accessed via
  `instanceof com.sun.management.UnixOperatingSystemMXBean` so non-Unix/non-HotSpot JVMs fall back to a
  fixed default of 1024. An explicit knob value always overrides the auto-size.
- Optional secondary knob `strata.fileChannelCache.headroom` if a deployment needs to tune the reserve.

## 6. Metrics

New `ChunkStore` accessors (mirroring `fsyncForceCount()`, `openChunks()`, …):
`channelCacheHits()`, `channelCacheMisses()`, `channelCacheEvictions()`, `cachedChannels()` (current size),
`channelCacheCapacity()`, and `openFds()` (from
`UnixOperatingSystemMXBean.getOpenFileDescriptorCount()`, best-effort `-1` when unavailable).

Wire into `ServerMetrics.registerNode` (Micrometer):
`strata_node_filechannel_cache{event=hit|miss|eviction}` (FunctionCounters),
`strata_node_filechannel_cache_size`, `strata_node_filechannel_cache_capacity`, `strata_node_open_fds`
(Gauges). Add panels to the Grafana dashboard alongside the existing operator metrics.

## 7. Concurrency & correctness invariants preserved

- **OPEN-chunk hazards untouched.** Committer force-after-close, seal truncate-floor, ledger ordering, and
  the `h.data==data` / `chunks.get(id)==h` identity checks all operate on the pinned writable channel; the
  cache never manages OPEN channels.
- **Sealed immutability.** The cache serves only SEALED chunks whose data region never changes — no
  truncate/force races; concurrent sharing of one read FD is safe (stateless positional reads).
- **Delete-while-leased.** The leased FD keeps the inode alive across `unlink`; new `acquire` calls fail
  (chunk gone from `chunks`); in-flight transfers complete correctly.
- **Durability.** Evicting an idle FD is correct because `fsync` is per-inode, not per-FD (a later force on
  any FD to the same file flushes the dirty pages). Stronger here: only durable, reclaimed sealed chunks are
  ever evictable.
- **No I/O under a global lock.** `open`/`close`/`force` always run outside the cache lock and (for sealed
  reads) outside the chunk monitor — no new carrier pinning.
- **Soft capacity.** A leased FD is never closed; over-capacity is metered and bounded by concurrent
  distinct reads.

## 8. Testing strategy

**`ChannelCache` unit tests**

- miss → opens; hit → reuses (no second open).
- LRU eviction order; eviction closes outside the lock (no deadlock).
- refcount blocks eviction of a leased channel; all-pinned-over-capacity does not close in-use FDs (and
  meters the overshoot).
- `invalidate` closes an idle channel immediately; defers a leased one to the last release.
- idempotent `Lease.close` (double close safe).
- concurrent `acquire` race for the same id → exactly one channel survives, the loser is closed.
- `close()` closes all idle channels.

**`ChunkStore` tests**

- FD/cache-size stays bounded as sealed-chunk count ≫ capacity (assert via `cachedChannels()` /
  `openFds()`).
- `recoverAll()` of many sealed chunks opens **zero** persistent data FDs (assert `cachedChannels()==0`
  immediately after construction; channels open lazily on first read).
- seal pinned→evictable correctness: after seal (`SEAL_FSYNC=true`) and after `reclaimSealedLedgersOnce()`
  (`SEAL_FSYNC=false`), `h.data` is null and reads succeed through the cache.
- `delete()` invalidates the cache; a delete during an in-flight lease keeps the read correct (inode alive).
- N concurrent readers of one sealed chunk share a single FD (assert cache size == 1 with N leases).
- Existing behavior preserved: `ChunkStoreTest` (sealed zero-copy + verified reads, open-clamp),
  `CrashRecoveryTest` (sealed recovery now via transient channel — same outcomes), `ChunkStoreCrcTest`,
  `RepairAndRetentionTest` (concurrent reads + delete + repair).

**Perf**

- `PerfSmokeTest` read path stays zero-copy on a cache hit (no CRC, no chunk lock on the hot path). The
  ref-count is a lock + counter per read — negligible against I/O. Verify empirically (build + benchmark
  the commit), not by reading the diff.

## 9. Files

**New**

- `strata-format/src/main/java/io/strata/format/ChannelCache.java`
- `strata-format/src/test/java/io/strata/format/ChannelCacheTest.java`
- (likely) `strata-proto/.../LeasedFileRegion.java` or an equivalent autoclose-off mechanism

**Modified**

- `strata-format/.../ChunkStore.java` — lifecycle transitions, sealed access via lease, recovery, delete,
  close, metrics accessors, config knob
- `strata-format/.../ChunkStore.java` `ReadRegionResult` — carry the lease, release on close
- `strata-proto/.../ScpServer.java`, `Frame.java` — lease release instead of FD close on the sendfile path
- `strata-server/.../ServerMetrics.java` — register the new gauges/counters
- Grafana dashboard JSON (strata-metrics) — panels
- Tests listed in §8

## 10. Assumptions, risks, out of scope

- **Load-bearing assumption:** the number of simultaneously-OPEN (actively-written) chunks per node stays
  well under the FD budget. This design bounds FDs to *active-write fan-out + cache size*. If a node can
  hold more OPEN chunks than the budget, the uniform-cache variant (caching OPEN channels too, with pinning
  while a committer/active write/truncate is in flight) would be required — explicitly **out of scope**.
- **`UnixOperatingSystemMXBean` availability:** present on HotSpot/Unix; the design degrades to a fixed
  default elsewhere. Accessed reflectively/via `instanceof` to avoid a hard `com.sun.*` compile coupling.
- **Netty close semantics:** the exact `DefaultFileRegion`/`Frame` wiring is confirmed during planning; the
  invariant (lease is the sole owner of close) is fixed here.
