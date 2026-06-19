# Bounded LRU FileChannel Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decouple open-FD count from resident-chunk count by serving SEALED-chunk data through a bounded, reference-counted LRU `FileChannel` cache, while OPEN chunks keep their pinned writable channel.

**Architecture:** A new `ChannelCache` (strata-format) opens READ-only channels on demand, caches the hot ones in access order, and closes the cold ones when over capacity — never closing a channel that has an outstanding lease. All SEALED-chunk reads (`read()` verified, `readRegion()` zero-copy, `fetch()`, `scrubOnce()`) go through the cache. Lifecycle changes stop opening a persistent FD per sealed chunk: recovery validates via a transient channel, and `seal()`/`reclaimSealedLedgersOnce()`/`importSealed()` close and null `h.data` once the chunk is durable. The zero-copy read path carries a `Runnable` releaser so the sendfile transfer releases its lease instead of closing the shared FD.

**Tech Stack:** Java 21, Maven, JUnit 5 (5.10.2, no assertj), Netty 4.1.134, Micrometer 1.12.13, SLF4J 2.0.13. No new dependencies.

## Global Constraints

- `maven.compiler.release=21`; SLF4J only in strata-format/strata-common — **no new dependency** (write the cache from scratch; do not add Caffeine/Guava).
- Config knob convention: `static long longConf(String property, String env, long def)` / `booleanConf(...)`; property `strata.featureName.configName`, env `STRATA_FEATURE_NAME_CONFIG_NAME`; `longConf` returns the default on non-numeric or `<= 0`.
- Concurrency: **never hold a lock across blocking I/O** (`FileChannel.open`/`close`/`force`) — avoids virtual-thread carrier pinning. The cache's `ReentrantLock` guards only O(1) map/LRU mutations.
- **No new module dependency:** strata-proto must not depend on strata-format. Only a `java.lang.Runnable` may cross from strata-format → strata-node → strata-proto.
- Preserve every OPEN-chunk path unchanged (append, `GroupCommitter` force, seal `truncate` after `stopCommitter`, background flush, the `h.data==data` / `chunks.get(id)==h` identity checks).
- Cache scope is **SEALED chunks only**; OPEN (and not-yet-reclaimed-sealed) channels stay pinned.
- Capacity is a **soft cap**: a leased FD is never closed; over-capacity is metered, not enforced by closing in-use channels.
- Test framework: JUnit 5 with `@TempDir`, `org.junit.jupiter.api.Assertions.*`. Construct `new ChunkStore(dir)` (seal-fsync off) or `new ChunkStore(dir, true)`.
- Tests excluded by default group: `chaos,perf,soak`. Run unit tests with `mvn -q -pl <module> test`.
- End every commit message with: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

---

## File Structure

**New files**
- `strata-format/src/main/java/io/strata/format/ChannelCache.java` — the cache + `Lease` (one responsibility: bound open READ FDs by LRU + refcount).
- `strata-format/src/test/java/io/strata/format/ChannelCacheTest.java` — cache unit tests.

**Modified files**
- `strata-format/src/main/java/io/strata/format/ChunkStore.java` — config knob, capacity auto-size, `openFds()`, cache field + construct + `closeAll`, sealed read routing, `ReadRegionResult.releaser`, lifecycle transitions, `delete()` invalidation, stat accessors, `scanDataCrcs`/`readSealedVerified` refactor.
- `strata-format/src/test/java/io/strata/format/ChunkStoreTest.java` — new tests; existing tests must stay green.
- `strata-node/src/main/java/io/strata/node/NodeHandlers.java` — thread the releaser into `okFileRegion`.
- `strata-proto/src/main/java/io/strata/proto/ScpServer.java` — `okFileRegion` signature + DefaultFileRegion uses the channel; release via releaser.
- `strata-proto/src/main/java/io/strata/proto/Frame.java` — `FilePayload` carries a `Runnable releaser`.
- `strata-server/src/main/java/io/strata/server/ServerMetrics.java` — register cache + open-FD meters.
- `strata-server/src/test/java/io/strata/server/ServerMetricsTest.java` — assert new meters.
- `strata-node/src/main/java/io/strata/node/StorageNode.java` — forward new accessors (if it wraps ChunkStore counters).
- `deploy/grafana/dashboards/strata-node.json` — panels.

---

## Task 1: `ChannelCache` core + unit tests

**Files:**
- Create: `strata-format/src/main/java/io/strata/format/ChannelCache.java`
- Test: `strata-format/src/test/java/io/strata/format/ChannelCacheTest.java`

**Interfaces:**
- Consumes: nothing (standalone). `io.strata.common.ChunkId` is a record (`public record ChunkId(FileId fileId, int index)`), safe as a `HashMap` key.
- Produces:
  - `ChannelCache(int capacity)`
  - `ChannelCache.Lease acquire(ChunkId id, java.nio.file.Path dataPath) throws IOException`
  - `interface Lease extends AutoCloseable { java.nio.channels.FileChannel channel(); void release(); @Override void close(); }`
  - `void invalidate(ChunkId id)`
  - `void close()` (closes all idle channels)
  - `long hits(); long misses(); long evictions(); int size(); int capacity();`

- [ ] **Step 1: Write the failing tests**

Create `strata-format/src/test/java/io/strata/format/ChannelCacheTest.java`:

```java
package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.FileId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChannelCacheTest {

    @TempDir
    Path dir;

    private ChunkId id(int i) {
        return new ChunkId(new FileId(0L, i), 0);
    }

    private Path file(int i, String content) throws Exception {
        Path p = dir.resolve("f" + i + ".chunk");
        Files.write(p, content.getBytes(StandardCharsets.UTF_8));
        return p;
    }

    private static byte[] readAll(FileChannel ch, int len) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(len);
        int n = 0;
        while (n < len) {
            int r = ch.read(buf, n);
            if (r < 0) break;
            n += r;
        }
        byte[] out = new byte[buf.position()];
        buf.flip();
        buf.get(out);
        return out;
    }

    @Test
    void acquireOpensOnMissAndReusesOnHit() throws Exception {
        Path p = file(1, "hello");
        try (ChannelCache cache = new ChannelCache(8)) {
            ChannelCache.Lease a = cache.acquire(id(1), p);
            assertEquals("hello", new String(readAll(a.channel(), 5), StandardCharsets.UTF_8));
            ChannelCache.Lease b = cache.acquire(id(1), p);
            assertSame(a.channel(), b.channel(), "hit must reuse the same FileChannel");
            assertEquals(1, cache.misses());
            assertEquals(1, cache.hits());
            assertEquals(1, cache.size());
            a.release();
            b.release();
        }
    }

    @Test
    void evictsLeastRecentlyUsedWhenOverCapacity() throws Exception {
        Path p1 = file(1, "a");
        Path p2 = file(2, "b");
        Path p3 = file(3, "c");
        try (ChannelCache cache = new ChannelCache(2)) {
            FileChannel c1;
            try (ChannelCache.Lease l = cache.acquire(id(1), p1)) { c1 = l.channel(); }
            try (ChannelCache.Lease l = cache.acquire(id(2), p2)) { l.channel(); }
            // id(1) is now LRU; acquiring id(3) evicts it
            try (ChannelCache.Lease l = cache.acquire(id(3), p3)) { l.channel(); }
            assertEquals(2, cache.size());
            assertEquals(1, cache.evictions());
            assertFalse(c1.isOpen(), "evicted channel must be physically closed");
        }
    }

    @Test
    void leasedChannelIsNotEvictedAndClosesOnLastRelease() throws Exception {
        Path p1 = file(1, "a");
        Path p2 = file(2, "b");
        Path p3 = file(3, "c");
        try (ChannelCache cache = new ChannelCache(2)) {
            ChannelCache.Lease held = cache.acquire(id(1), p1); // keep leased
            FileChannel c1 = held.channel();
            try (ChannelCache.Lease l = cache.acquire(id(2), p2)) { l.channel(); }
            try (ChannelCache.Lease l = cache.acquire(id(3), p3)) { l.channel(); }
            // id(1) is over-capacity but pinned: must NOT be closed
            assertTrue(c1.isOpen(), "a leased channel must not be closed by eviction");
            held.release();
            assertFalse(c1.isOpen(), "evicted+unleased channel closes on last release");
        }
    }

    @Test
    void concurrentAcquireOfSameIdKeepsExactlyOneChannel() throws Exception {
        Path p = file(1, "x");
        try (ChannelCache cache = new ChannelCache(8)) {
            int threads = 16;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            List<ChannelCache.Lease> leases = new ArrayList<>();
            AtomicReference<Throwable> err = new AtomicReference<>();
            for (int i = 0; i < threads; i++) {
                Thread.ofVirtual().start(() -> {
                    try {
                        start.await();
                        ChannelCache.Lease l = cache.acquire(id(1), p);
                        synchronized (leases) { leases.add(l); }
                    } catch (Throwable t) {
                        err.set(t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            done.await();
            if (err.get() != null) throw new AssertionError(err.get());
            assertEquals(1, cache.size(), "all concurrent acquirers must share one channel");
            FileChannel only = leases.get(0).channel();
            for (ChannelCache.Lease l : leases) {
                assertSame(only, l.channel());
            }
            for (ChannelCache.Lease l : leases) l.release();
        }
    }

    @Test
    void invalidateClosesIdleNowAndDefersLeased() throws Exception {
        Path p = file(1, "x");
        try (ChannelCache cache = new ChannelCache(8)) {
            FileChannel idle;
            try (ChannelCache.Lease l = cache.acquire(id(1), p)) { idle = l.channel(); }
            cache.invalidate(id(1));
            assertFalse(idle.isOpen(), "invalidate of an idle entry closes the channel now");

            ChannelCache.Lease held = cache.acquire(id(1), p);
            FileChannel leased = held.channel();
            cache.invalidate(id(1));
            assertTrue(leased.isOpen(), "invalidate must defer close while leased");
            held.release();
            assertFalse(leased.isOpen(), "deferred close runs on last release");
        }
    }

    @Test
    void releaseIsIdempotent() throws Exception {
        Path p = file(1, "x");
        try (ChannelCache cache = new ChannelCache(1)) {
            ChannelCache.Lease l = cache.acquire(id(1), p);
            l.release();
            l.release(); // must not double-decrement or throw
            // a fresh acquire still works
            try (ChannelCache.Lease l2 = cache.acquire(id(1), p)) {
                assertTrue(l2.channel().isOpen());
            }
        }
    }

    @Test
    void closeClosesAllIdleChannels() throws Exception {
        Path p1 = file(1, "a");
        Path p2 = file(2, "b");
        ChannelCache cache = new ChannelCache(8);
        FileChannel c1, c2;
        try (ChannelCache.Lease l = cache.acquire(id(1), p1)) { c1 = l.channel(); }
        try (ChannelCache.Lease l = cache.acquire(id(2), p2)) { c2 = l.channel(); }
        cache.close();
        assertFalse(c1.isOpen());
        assertFalse(c2.isOpen());
        assertEquals(0, cache.size());
    }

    @Test
    void acquireMissingFileThrows() {
        try (ChannelCache cache = new ChannelCache(4)) {
            assertThrows(java.io.IOException.class,
                    () -> cache.acquire(id(99), dir.resolve("nope.chunk")));
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -pl strata-format test -Dtest=ChannelCacheTest`
Expected: FAIL — `ChannelCache` does not exist (compilation error).

- [ ] **Step 3: Implement `ChannelCache`**

Create `strata-format/src/main/java/io/strata/format/ChannelCache.java`:

```java
package io.strata.format;

import io.strata.common.ChunkId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded, reference-counted LRU cache of READ-only {@link FileChannel}s for SEALED chunk data.
 *
 * Caps the number of simultaneously-open sealed-chunk descriptors regardless of resident chunk
 * count. A channel with an outstanding lease is never closed by eviction (capacity is a soft cap);
 * over-capacity due to pinning is bounded by the number of concurrent distinct reads. Eviction and
 * close always run OUTSIDE the cache lock so a blocking close never pins a virtual-thread carrier.
 *
 * fsync is per-inode, so evicting an idle channel is safe — but this cache only serves SEALED,
 * durable, immutable chunks, so eviction never races a write, truncate, or force.
 */
final class ChannelCache implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ChannelCache.class);

    interface Lease extends AutoCloseable {
        FileChannel channel();
        void release();
        @Override default void close() { release(); }
    }

    private static final class Entry {
        final FileChannel channel;
        int refCount;
        boolean evicted;
        Entry(FileChannel channel) { this.channel = channel; }
    }

    private final int capacity;
    private final ReentrantLock lock = new ReentrantLock();
    /** access-order: iteration yields least-recently-used first. */
    private final LinkedHashMap<ChunkId, Entry> map = new LinkedHashMap<>(16, 0.75f, true);

    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();

    ChannelCache(int capacity) {
        this.capacity = Math.max(1, capacity);
    }

    Lease acquire(ChunkId id, Path dataPath) throws IOException {
        lock.lock();
        try {
            Entry e = map.get(id);
            if (e != null) {
                e.refCount++;
                hits.incrementAndGet();
                return new LeaseImpl(e);
            }
        } finally {
            lock.unlock();
        }
        // miss: open outside the lock
        FileChannel opened = FileChannel.open(dataPath, StandardOpenOption.READ);
        List<FileChannel> toClose = new ArrayList<>();
        Entry leased;
        lock.lock();
        try {
            Entry existing = map.get(id);
            if (existing != null) {
                existing.refCount++;
                hits.incrementAndGet();
                toClose.add(opened); // we lost the open race; discard our channel
                leased = existing;
            } else {
                Entry ne = new Entry(opened);
                ne.refCount = 1;
                map.put(id, ne);
                misses.incrementAndGet();
                evictDownToCapacity(toClose);
                leased = ne;
            }
        } finally {
            lock.unlock();
        }
        closeAllQuietly(toClose);
        return new LeaseImpl(leased);
    }

    /** Caller holds the lock. Removes LRU, refCount==0 entries until size <= capacity. */
    private void evictDownToCapacity(List<FileChannel> toClose) {
        if (map.size() <= capacity) return;
        Iterator<Map.Entry<ChunkId, Entry>> it = map.entrySet().iterator();
        while (map.size() > capacity && it.hasNext()) {
            Map.Entry<ChunkId, Entry> me = it.next();
            Entry e = me.getValue();
            if (e.refCount == 0) {
                it.remove();
                e.evicted = true;
                toClose.add(e.channel);
                evictions.incrementAndGet();
            }
            // pinned entries are skipped; capacity is a soft cap
        }
    }

    void invalidate(ChunkId id) {
        FileChannel toClose = null;
        lock.lock();
        try {
            Entry e = map.remove(id);
            if (e != null) {
                e.evicted = true;
                if (e.refCount == 0) toClose = e.channel;
            }
        } finally {
            lock.unlock();
        }
        closeQuietly(toClose);
    }

    @Override
    public void close() {
        List<FileChannel> toClose = new ArrayList<>();
        lock.lock();
        try {
            for (Entry e : map.values()) {
                e.evicted = true;
                if (e.refCount == 0) toClose.add(e.channel);
            }
            map.clear();
        } finally {
            lock.unlock();
        }
        closeAllQuietly(toClose);
    }

    long hits() { return hits.get(); }
    long misses() { return misses.get(); }
    long evictions() { return evictions.get(); }
    int capacity() { return capacity; }

    int size() {
        lock.lock();
        try {
            return map.size();
        } finally {
            lock.unlock();
        }
    }

    private final class LeaseImpl implements Lease {
        private final Entry entry;
        private boolean released;
        LeaseImpl(Entry entry) { this.entry = entry; }

        @Override public FileChannel channel() { return entry.channel; }

        @Override public void release() {
            FileChannel toClose = null;
            lock.lock();
            try {
                if (released) return;
                released = true;
                entry.refCount--;
                if (entry.evicted && entry.refCount == 0) toClose = entry.channel;
            } finally {
                lock.unlock();
            }
            closeQuietly(toClose);
        }
    }

    private static void closeAllQuietly(List<FileChannel> channels) {
        for (FileChannel c : channels) closeQuietly(c);
    }

    private static void closeQuietly(FileChannel c) {
        if (c == null) return;
        try {
            c.close();
        } catch (IOException e) {
            log.warn("failed to close cached channel", e);
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -pl strata-format test -Dtest=ChannelCacheTest`
Expected: PASS (all 8 tests).

- [ ] **Step 5: Commit**

```bash
git add strata-format/src/main/java/io/strata/format/ChannelCache.java \
        strata-format/src/test/java/io/strata/format/ChannelCacheTest.java
git commit -m "feat(format): bounded ref-counted LRU FileChannel cache

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Config knob, capacity auto-sizing, and `openFds()`

**Files:**
- Modify: `strata-format/src/main/java/io/strata/format/ChunkStore.java`
- Test: `strata-format/src/test/java/io/strata/format/ChunkStoreTest.java`

**Interfaces:**
- Consumes: `ChunkStore.longConf(String, String, long)` (existing static).
- Produces:
  - `static int defaultChannelCacheCapacity()` (package-private, for tests)
  - `public long openFds()` (process open FD count; `-1` when unavailable)
  - static field `CHANNEL_CACHE_MAX_SIZE` (the resolved capacity)

- [ ] **Step 1: Write the failing test**

Add to `ChunkStoreTest.java`:

```java
    @Test
    void defaultChannelCacheCapacityIsPositive() {
        assertTrue(ChunkStore.defaultChannelCacheCapacity() >= 128,
                "auto-sized cache capacity must be a sane floor");
    }

    @Test
    void openFdsIsNonNegativeOnUnixOrMinusOne() throws Exception {
        try (ChunkStore store = newStore()) {
            long fds = store.openFds();
            assertTrue(fds >= 0 || fds == -1, "openFds() is the live count or -1 when unavailable");
        }
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `mvn -q -pl strata-format test -Dtest=ChunkStoreTest#defaultChannelCacheCapacityIsPositive+openFdsIsNonNegativeOnUnixOrMinusOne`
Expected: FAIL — methods do not exist.

- [ ] **Step 3: Implement the config + capacity helpers**

In `ChunkStore.java`, add the import near the top:

```java
import java.lang.management.ManagementFactory;
```

Add the static knob next to the other knobs (after `SLOW_MUTATION_LOG_NANOS`, around line 96):

```java
    private static final long CHANNEL_CACHE_MAX_SIZE =
            longConf("strata.fileChannelCache.maxSize", "STRATA_FILE_CHANNEL_CACHE_MAX_SIZE",
                    defaultChannelCacheCapacity());
```

Add these methods (near `longConf`/`booleanConf`, around line 130):

```java
    /**
     * Default sealed-chunk channel-cache capacity: derived from the soft RLIMIT_NOFILE minus headroom
     * for pinned OPEN channels, ledgers, sockets, and in-flight transient FDs. Falls back to a fixed
     * default on non-Unix / non-HotSpot JVMs where the FD limit is not introspectable.
     */
    static int defaultChannelCacheCapacity() {
        long max = -1;
        java.lang.management.OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.UnixOperatingSystemMXBean unix) {
            max = unix.getMaxFileDescriptorCount();
        }
        if (max <= 0) {
            return 1024; // non-Unix fallback
        }
        long headroom = Math.max(256, max / 4);
        long cap = max - headroom;
        return (int) Math.max(128, Math.min(cap, Integer.MAX_VALUE));
    }

    /** Live process open file-descriptor count for observability; {@code -1} when unavailable. */
    public long openFds() {
        java.lang.management.OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.UnixOperatingSystemMXBean unix) {
            return unix.getOpenFileDescriptorCount();
        }
        return -1;
    }
```

- [ ] **Step 4: Run to verify pass**

Run: `mvn -q -pl strata-format test -Dtest=ChunkStoreTest#defaultChannelCacheCapacityIsPositive+openFdsIsNonNegativeOnUnixOrMinusOne`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add strata-format/src/main/java/io/strata/format/ChunkStore.java \
        strata-format/src/test/java/io/strata/format/ChunkStoreTest.java
git commit -m "feat(format): channel-cache capacity knob + open-FD accessor

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Wire the cache into `ChunkStore` (construct, shutdown, stat accessors)

**Files:**
- Modify: `strata-format/src/main/java/io/strata/format/ChunkStore.java`
- Test: `strata-format/src/test/java/io/strata/format/ChunkStoreTest.java`

**Interfaces:**
- Consumes: `ChannelCache(int)`, `ChannelCache.close()`, `ChannelCache.hits/misses/evictions/size/capacity` (Task 1); `CHANNEL_CACHE_MAX_SIZE` (Task 2).
- Produces (public accessors): `long channelCacheHits(); long channelCacheMisses(); long channelCacheEvictions(); int cachedChannels(); int channelCacheCapacity();`
- Produces (private field): `final ChannelCache channelCache;`

- [ ] **Step 1: Write the failing test**

Add to `ChunkStoreTest.java`:

```java
    @Test
    void exposesChannelCacheAccessorsAndClosesCleanly() throws Exception {
        try (ChunkStore store = newStore()) {
            assertEquals(0, store.cachedChannels());
            assertEquals(0, store.channelCacheHits());
            assertEquals(0, store.channelCacheMisses());
            assertEquals(0, store.channelCacheEvictions());
            assertTrue(store.channelCacheCapacity() >= 128);
        }
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `mvn -q -pl strata-format test -Dtest=ChunkStoreTest#exposesChannelCacheAccessorsAndClosesCleanly`
Expected: FAIL — accessors/field do not exist.

- [ ] **Step 3: Add the field, construct it, close it, expose stats**

Add the field next to `chunks` (around line 55):

```java
    private final ChannelCache channelCache;
```

In the package-private constructor `ChunkStore(Path dir, boolean sealFsync)`, initialize it **before** `recoverAll()` (after `this.sealFsync = sealFsync;`):

```java
        this.channelCache = new ChannelCache((int) CHANNEL_CACHE_MAX_SIZE);
```

Add the accessors next to `openChunks()`/`sealedChunks()` (around line 200):

```java
    /** Sealed-chunk channel-cache hits (served an already-open FD) — observability. */
    public long channelCacheHits() { return channelCache.hits(); }

    /** Sealed-chunk channel-cache misses (opened a new FD) — observability. */
    public long channelCacheMisses() { return channelCache.misses(); }

    /** Sealed-chunk channel-cache evictions (closed an idle FD over capacity) — observability. */
    public long channelCacheEvictions() { return channelCache.evictions(); }

    /** Currently-open cached sealed-chunk channels — observability. */
    public int cachedChannels() { return channelCache.size(); }

    /** Configured channel-cache capacity — observability. */
    public int channelCacheCapacity() { return channelCache.capacity(); }
```

In `close()`, add the cache shutdown after the per-chunk close loop, just before the `if (failure == null)` block (around line 1850):

```java
        channelCache.close();
```

- [ ] **Step 4: Run to verify pass**

Run: `mvn -q -pl strata-format test -Dtest=ChunkStoreTest#exposesChannelCacheAccessorsAndClosesCleanly`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add strata-format/src/main/java/io/strata/format/ChunkStore.java \
        strata-format/src/test/java/io/strata/format/ChunkStoreTest.java
git commit -m "feat(format): wire ChannelCache into ChunkStore lifecycle + stats

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Refactor `scanDataCrcs` / `readSealedVerified` to take a `FileChannel`

This is a pure refactor (no behavior change) so the next tasks can pass either `h.data` or a leased channel.

**Files:**
- Modify: `strata-format/src/main/java/io/strata/format/ChunkStore.java`

**Interfaces:**
- Produces:
  - `private CrcScan scanDataCrcs(FileChannel data, long dataLength) throws IOException`
  - `private void readSealedVerified(FileChannel data, long sealedLength, java.util.List<Integer> rangeCrcs, ChunkId id, long offset, byte[] out) throws IOException`

- [ ] **Step 1: Change `scanDataCrcs` signature and body**

Replace the current `scanDataCrcs(Handle h, long dataLength)` (around line 1061) header and the read line:

```java
    private CrcScan scanDataCrcs(FileChannel data, long dataLength) throws IOException {
        CRC32C whole = new CRC32C();
        List<Integer> ranges = new ArrayList<>();
        byte[] buf = new byte[1 << 20];
        long pos = 0;
        CRC32C range = new CRC32C();
        long rangeRemaining = ChunkFormats.CRC_RANGE_SIZE;
        while (pos < dataLength) {
            int n = (int) Math.min(buf.length, Math.min(dataLength - pos, rangeRemaining));
            ByteBuffer bb = ByteBuffer.wrap(buf, 0, n);
            readFully(data, bb, checkedAdd(DATA_START, pos, "chunk file offset"));
            whole.update(buf, 0, n);
            range.update(buf, 0, n);
            pos += n;
            rangeRemaining -= n;
            if (rangeRemaining == 0 || pos == dataLength) {
                ranges.add((int) range.getValue());
                range.reset();
                rangeRemaining = ChunkFormats.CRC_RANGE_SIZE;
            }
        }
        return new CrcScan((int) whole.getValue(), ranges);
    }
```

- [ ] **Step 2: Update `scanDataCrcs` callers to pass the channel**

In `seal()` (around line 912):

```java
            CrcScan scan = dataLength == h.end ? h.snapshotRunningCrcs() : scanDataCrcs(h.data, dataLength);
```

In `scrubOnce()` (around line 1360) — leave temporarily as `scanDataCrcs(h.data, h.sealedLength)` (Task 5 reroutes it):

```java
                int actual = scanDataCrcs(h.data, h.sealedLength).dataCrc();
```

- [ ] **Step 3: Change `readSealedVerified` signature and body**

Replace `readSealedVerified(Handle h, long offset, byte[] out)` (around line 1614):

```java
    private void readSealedVerified(FileChannel data, long sealedLength, List<Integer> rangeCrcs,
                                    ChunkId id, long offset, byte[] out) throws IOException {
        if (out.length == 0) return;
        if (rangeCrcs.isEmpty()) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "sealed chunk missing CRC ranges: " + id);
        }
        long firstRange = offset / ChunkFormats.CRC_RANGE_SIZE;
        long lastRange = (offset + out.length - 1) / ChunkFormats.CRC_RANGE_SIZE;
        byte[] rangeBuf = new byte[ChunkFormats.CRC_RANGE_SIZE];
        for (long range = firstRange; range <= lastRange; range++) {
            if (range >= rangeCrcs.size()) {
                throw new ScpException(ErrorCode.CORRUPT_CHUNK, "CRC range missing for " + id);
            }
            long rangeStart = range * (long) ChunkFormats.CRC_RANGE_SIZE;
            int rangeLen = (int) Math.min(ChunkFormats.CRC_RANGE_SIZE, sealedLength - rangeStart);
            readFully(data, ByteBuffer.wrap(rangeBuf, 0, rangeLen),
                    checkedAdd(DATA_START, rangeStart, "chunk file offset"));
            int actual = Crc.of(rangeBuf, 0, rangeLen);
            int expected = rangeCrcs.get((int) range);
            if (actual != expected) {
                throw new ScpException(ErrorCode.CRC_MISMATCH,
                        "sealed range crc mismatch on " + id + " range " + range);
            }
            long copyStart = Math.max(offset, rangeStart);
            long copyEnd = Math.min(offset + out.length, rangeStart + rangeLen);
            System.arraycopy(rangeBuf, (int) (copyStart - rangeStart),
                    out, (int) (copyStart - offset), (int) (copyEnd - copyStart));
        }
    }
```

- [ ] **Step 4: Update the `read()` SEALED caller (temporary — Task 5 reroutes)**

In `read()` (around line 747), keep it passing `h.data` for now:

```java
            if (h.state == ChunkState.SEALED) {
                readSealedVerified(h.data, h.sealedLength, h.sealedRangeCrcs, h.id, offset, out);
            } else {
                readOpenVerified(h, offset, out);
            }
```

- [ ] **Step 5: Run the full format test suite to verify no behavior change**

Run: `mvn -q -pl strata-format test`
Expected: PASS (all existing tests green).

- [ ] **Step 6: Commit**

```bash
git add strata-format/src/main/java/io/strata/format/ChunkStore.java
git commit -m "refactor(format): scanDataCrcs/readSealedVerified take a FileChannel

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: Route sealed `read()`, `fetch()`, `scrubOnce()` through the cache

Sealed reads acquire a cache lease for an independent READ channel; immutable sealed fields are snapshotted under the monitor, then I/O runs outside it. This works whether or not `h.data` is still pinned (the cache opens a separate FD; the page cache makes a written-but-unforced footer visible across FDs).

**Files:**
- Modify: `strata-format/src/main/java/io/strata/format/ChunkStore.java`
- Test: `strata-format/src/test/java/io/strata/format/ChunkStoreTest.java`

**Interfaces:**
- Consumes: `channelCache.acquire(ChunkId, Path)`, `Lease.channel()`, `Lease.release()`; `scanDataCrcs(FileChannel,long)`, `readSealedVerified(FileChannel,long,List,ChunkId,long,byte[])`.
- Produces: behavior — sealed `read()`/`fetch()`/`scrubOnce()` increment `channelCacheMisses()`/`hits()`.

- [ ] **Step 1: Write the failing tests**

Add to `ChunkStoreTest.java`:

```java
    @Test
    void sealedVerifiedReadGoesThroughChannelCache() throws Exception {
        try (ChunkStore store = newStore()) {
            sealedBytes(store, id, "cache-me-please");
            store.reclaimSealedLedgersOnce(); // become evictable (h.data nulled in Task 8; harmless here)
            assertArrayEquals("cache".getBytes(),
                    store.read(id, 0, 5).bytes());
            assertTrue(store.channelCacheMisses() + store.channelCacheHits() >= 1,
                    "a sealed read must consult the channel cache");
            assertTrue(store.cachedChannels() >= 1, "the sealed channel is cached after a read");
        }
    }

    @Test
    void sealedFetchGoesThroughChannelCache() throws Exception {
        try (ChunkStore store = newStore()) {
            byte[] full = sealedBytes(store, id, "fetch-via-cache");
            long missesBefore = store.channelCacheMisses() + store.channelCacheHits();
            byte[] got = store.fetch(id, 0, Integer.MAX_VALUE).bytes();
            assertArrayEquals(full, got);
            assertTrue(store.channelCacheMisses() + store.channelCacheHits() > missesBefore,
                    "fetch of a sealed chunk must consult the channel cache");
        }
    }

    @Test
    void scrubReadsSealedDataThroughCache() throws Exception {
        try (ChunkStore store = newStore()) {
            sealedBytes(store, id, "scrub-me");
            assertEquals(0, store.scrubOnce(), "clean chunk has no corruption");
            assertTrue(store.cachedChannels() >= 1);
        }
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `mvn -q -pl strata-format test -Dtest=ChunkStoreTest#sealedVerifiedReadGoesThroughChannelCache+sealedFetchGoesThroughChannelCache+scrubReadsSealedDataThroughCache`
Expected: FAIL — `cachedChannels()` stays 0 (reads still use `h.data`).

- [ ] **Step 3: Reroute sealed `read()`**

Replace the body of `read()` (around line 738-754) so the SEALED case snapshots then reads outside the monitor:

```java
    public ReadResult read(ChunkId id, long offset, int maxBytes) throws IOException {
        requireNonNegative(offset, "read offset");
        requireNonNegative(maxBytes, "read maxBytes");
        Handle h = lookup(id);
        long sealedLength;
        List<Integer> rangeCrcs;
        long lastKnownDO;
        Path dataPath;
        synchronized (h) {
            if (h.state != ChunkState.SEALED) {
                long end = h.currentEnd();
                if (offset >= end) return new ReadResult(new byte[0], end, h.lastKnownDO);
                int n = (int) Math.min(Math.min(maxBytes, MAX_REQUEST_BYTES), end - offset);
                byte[] out = new byte[n];
                readOpenVerified(h, offset, out);
                return new ReadResult(out, end, h.lastKnownDO);
            }
            sealedLength = h.sealedLength;
            rangeCrcs = h.sealedRangeCrcs;
            lastKnownDO = h.lastKnownDO;
            dataPath = h.dataPath;
        }
        if (offset >= sealedLength) return new ReadResult(new byte[0], sealedLength, lastKnownDO);
        int n = (int) Math.min(Math.min(maxBytes, MAX_REQUEST_BYTES), sealedLength - offset);
        byte[] out = new byte[n];
        try (ChannelCache.Lease lease = channelCache.acquire(id, dataPath)) {
            readSealedVerified(lease.channel(), sealedLength, rangeCrcs, id, offset, out);
        }
        return new ReadResult(out, sealedLength, lastKnownDO);
    }
```

- [ ] **Step 4: Reroute `fetch()`**

Replace `fetch()` (around line 1097-1112):

```java
    public FetchResult fetch(ChunkId id, long offset, int maxBytes) throws IOException {
        requireNonNegative(offset, "fetch offset");
        requireNonNegative(maxBytes, "fetch maxBytes");
        Handle h = lookup(id);
        ChunkState state;
        Path dataPath;
        synchronized (h) {
            if (h.state != ChunkState.SEALED) {
                throw new ScpException(ErrorCode.INTERNAL, "fetch of non-sealed chunk " + id);
            }
            state = h.state;
            dataPath = h.dataPath;
        }
        try (ChannelCache.Lease lease = channelCache.acquire(id, dataPath)) {
            FileChannel data = lease.channel();
            long fileLen = data.size();
            if (offset >= fileLen) return new FetchResult(fileLen, state, new byte[0]);
            int n = (int) Math.min(Math.min(maxBytes, MAX_REQUEST_BYTES), fileLen - offset);
            byte[] out = new byte[n];
            readFully(data, ByteBuffer.wrap(out), offset);
            return new FetchResult(fileLen, state, out);
        }
    }
```

- [ ] **Step 5: Reroute `scrubOnce()` (snapshot under monitor, CRC outside, update under monitor)**

Replace `scrubOnce()` (around line 1355-1370):

```java
    public int scrubOnce() throws IOException {
        int corrupt = 0;
        for (Handle h : chunks.values()) {
            long sealedLength;
            int storedCrc;
            Path dataPath;
            synchronized (h) {
                if (h.state != ChunkState.SEALED) continue;
                sealedLength = h.sealedLength;
                storedCrc = h.dataCrc;
                dataPath = h.dataPath;
            }
            int actual;
            try (ChannelCache.Lease lease = channelCache.acquire(h.id, dataPath)) {
                actual = scanDataCrcs(lease.channel(), sealedLength).dataCrc();
            }
            if (actual != storedCrc) {
                synchronized (h) {
                    if (h.state == ChunkState.SEALED && h.dataCrc == storedCrc) {
                        log.error("scrub: sealed chunk {} data rot — stored crc {} actual {}; "
                                + "exposing via inventory for re-repair", h.id, storedCrc, actual);
                        h.dataCrc = actual;
                        corrupt++;
                    }
                }
            }
        }
        return corrupt;
    }
```

- [ ] **Step 6: Run the new tests + the full format suite**

Run: `mvn -q -pl strata-format test`
Expected: PASS — new tests green and all existing tests still pass (`read()`/`fetch()`/scrub correctness unchanged).

- [ ] **Step 7: Commit**

```bash
git add strata-format/src/main/java/io/strata/format/ChunkStore.java \
        strata-format/src/test/java/io/strata/format/ChunkStoreTest.java
git commit -m "feat(format): sealed read/fetch/scrub via channel cache

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: `readRegion()` SEALED zero-copy via ref-counted lease

The sealed zero-copy path acquires a lease and carries a `Runnable` releaser in `ReadRegionResult`; the channel is shared so concurrent readers of one chunk share one FD. OPEN-clamped reads keep their own transient FD (released by closing the channel).

**Files:**
- Modify: `strata-format/src/main/java/io/strata/format/ChunkStore.java`
- Test: `strata-format/src/test/java/io/strata/format/ChunkStoreTest.java`

**Interfaces:**
- Produces: `ReadRegionResult` record gains a trailing `Runnable releaser` component; `ReadRegionResult.close()` runs the releaser (no checked exception).
- Consumes: `channelCache.acquire(...)`, `Lease`.

- [ ] **Step 1: Write the failing tests**

Add to `ChunkStoreTest.java`:

```java
    @Test
    void sealedReadRegionSharesOneCachedFdAcrossConcurrentReaders() throws Exception {
        try (ChunkStore store = newStore()) {
            sealedBytes(store, id, "shared-zero-copy");
            ChunkStore.ReadRegionResult r1 = store.readRegion(id, 0, 6);
            ChunkStore.ReadRegionResult r2 = store.readRegion(id, 6, 4);
            try {
                assertTrue(r1.channel() != null && r2.channel() != null, "sealed reads are zero-copy");
                assertSame(r1.channel(), r2.channel(), "concurrent sealed readers share one cached FD");
                assertEquals(1, store.cachedChannels());
                assertArrayEquals("shared".getBytes(), consumeRegion(r1));
            } finally {
                r1.close();
                r2.close();
            }
            // releasing the leases leaves the channel cached (idle), not closed
            assertEquals(1, store.cachedChannels());
        }
    }

    @Test
    void sealedReadRegionLeaseKeepsFdOpenUntilReleased() throws Exception {
        try (ChunkStore store = newStore()) {
            sealedBytes(store, id, "lease-lifetime");
            ChunkStore.ReadRegionResult r = store.readRegion(id, 0, 5);
            FileChannel ch = r.channel();
            assertTrue(ch.isOpen());
            r.close(); // releases the lease; channel stays cached/open
            assertTrue(ch.isOpen(), "release returns the FD to the cache, does not close it");
        }
    }
```

(Imports already present in the test file include `java.nio.channels.FileChannel` and `assertSame`. Add them if missing.)

- [ ] **Step 2: Run to verify failure**

Run: `mvn -q -pl strata-format test -Dtest=ChunkStoreTest#sealedReadRegionSharesOneCachedFdAcrossConcurrentReaders+sealedReadRegionLeaseKeepsFdOpenUntilReleased`
Expected: FAIL — sealed `readRegion` opens a fresh FD per call (no sharing), and `ReadRegionResult` has no `releaser`.

- [ ] **Step 3: Add `releaser` to `ReadRegionResult`**

Replace the record (around line 728-736):

```java
    public record ReadRegionResult(FileChannel channel, long filePosition, int length, byte[] bytes,
                                   long localEndOffset, long lastKnownDO, Runnable releaser)
            implements AutoCloseable {
        /** Releases the underlying read resource: a cache lease (sealed) or the transient FD (open). */
        @Override
        public void close() {
            if (releaser != null) {
                releaser.run();
            }
        }
    }
```

- [ ] **Step 4: Update all `ReadRegionResult` constructions**

There are five construction sites in `readRegion`/`readRegionForRecovery` (the empty/heap-snapshot ones get `releaser = null`; the OPEN transient one closes its channel; the SEALED one releases its lease). Replace the relevant region of `readRegion(...)` (around line 781-839) with:

```java
            if (offset >= readableEnd) {
                return new ReadRegionResult(null, 0, 0, null, localEnd, h.lastKnownDO, null);
            }
            int n = (int) Math.min(Math.min(maxBytes, MAX_REQUEST_BYTES), readableEnd - offset);
            if (n == 0) {
                return new ReadRegionResult(null, 0, 0, null, localEnd, h.lastKnownDO, null);
            }
            if (!includeUndurableTail) {
                readOps.incrementAndGet();
                readBytes.addAndGet(n);
            }
            long filePos = checkedAdd(DATA_START, offset, "chunk file offset");
            if (h.state != ChunkState.SEALED) {
                if (includeUndurableTail) {
                    byte[] out = new byte[n];
                    readOpenVerified(h, offset, out);
                    return new ReadRegionResult(null, 0, n, out, localEnd, h.lastKnownDO, null);
                }
                // OPEN client fast path: independent transient READ FD, released by closing it.
                FileChannel readChannel = FileChannel.open(h.dataPath, StandardOpenOption.READ);
                try {
                    return new ReadRegionResult(readChannel, filePos, n, null, localEnd, h.lastKnownDO,
                            () -> closeQuietly(readChannel));
                } catch (RuntimeException e) {
                    closeQuietly(readChannel);
                    throw e;
                }
            }
            // SEALED: zero-copy region over a ref-counted cached FD shared across concurrent readers.
            ChannelCache.Lease lease = channelCache.acquire(id, h.dataPath);
            try {
                return new ReadRegionResult(lease.channel(), filePos, n, null, localEnd, h.lastKnownDO,
                        lease::release);
            } catch (RuntimeException e) {
                lease.release();
                throw e;
            }
        }
    }
```

Add the private helper near `forceDirectory()` (around line 612):

```java
    private static void closeQuietly(FileChannel ch) {
        if (ch == null) return;
        try {
            ch.close();
        } catch (IOException e) {
            log.warn("failed to close transient read channel", e);
        }
    }
```

> **Note for the implementer:** acquiring the lease under `synchronized (h)` is acceptable here (the existing code already opened the transient FD under the monitor at this point) and the cache never acquires a Handle monitor, so there is no lock-ordering deadlock. Keep the `acquire` call inside the `synchronized (h)` block exactly where the old `FileChannel.open` was.

- [ ] **Step 5: Run the new tests + full format suite**

Run: `mvn -q -pl strata-format test`
Expected: PASS — new sharing/lease tests pass; existing `openReadRegion...`/`...ZeroCopy...` tests still pass (OPEN path unchanged in behavior; `consumeRegion` reads `channel()` before `close()`).

- [ ] **Step 6: Commit**

```bash
git add strata-format/src/main/java/io/strata/format/ChunkStore.java \
        strata-format/src/test/java/io/strata/format/ChunkStoreTest.java
git commit -m "feat(format): sealed readRegion zero-copy via ref-counted lease

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: Cross-module lease wiring (NodeHandlers → ScpServer → Frame)

Carry the `Runnable` releaser from `ReadRegionResult` into `Frame.FilePayload` so the sendfile completion releases the lease instead of closing the FD. No strata-format type crosses into strata-proto.

**Files:**
- Modify: `strata-proto/src/main/java/io/strata/proto/Frame.java`
- Modify: `strata-proto/src/main/java/io/strata/proto/ScpServer.java`
- Modify: `strata-node/src/main/java/io/strata/node/NodeHandlers.java`

**Interfaces:**
- Produces:
  - `Frame.FilePayload(FileChannel channel, long position, int length, Runnable releaser)` — `close()` runs `releaser`.
  - `ScpServer.okFileRegion(Frame req, byte[] header, FileChannel channel, long position, int length, Runnable releaser)`.
- Consumes: `ChunkStore.ReadRegionResult.releaser()` (Task 6).

- [ ] **Step 1: Confirm the only callers of `okFileRegion` and `new FilePayload`**

Run:
```bash
grep -rn "okFileRegion\|new Frame.FilePayload\|new FilePayload\|FilePayload(" \
  strata-node/src/main strata-proto/src/main
```
Expected: `okFileRegion` is called once (`NodeHandlers.readRegionResponse`); `FilePayload` is constructed once (inside `okFileRegion`). If other callers exist, update them the same way in this task.

- [ ] **Step 2: Update `Frame.FilePayload` to carry a releaser**

Replace the `FilePayload` record (Frame.java ~line 61-82):

```java
    public record FilePayload(FileChannel channel, long position, int length, Runnable releaser)
            implements AutoCloseable {
        public FilePayload {
            if (channel == null) {
                throw new IllegalArgumentException("channel must not be null");
            }
            if (position < 0) {
                throw new IllegalArgumentException("position must be non-negative: " + position);
            }
            if (length < 0) {
                throw new IllegalArgumentException("length must be non-negative: " + length);
            }
            if (releaser == null) {
                throw new IllegalArgumentException("releaser must not be null");
            }
        }

        @Override
        public void close() {
            // Releases the read resource: a channel-cache lease (sealed) or the transient FD (open).
            releaser.run();
        }
    }
```

(`Frame.close()` already calls `filePayload.close()` — no change needed there.)

- [ ] **Step 3: Update `ScpServer.okFileRegion` signature and `DefaultFileRegion` construction**

Replace `okFileRegion` (ScpServer.java ~line 448-451):

```java
    /** Convenience for handlers: success response whose payload is streamed from a file region. */
    public static Frame okFileRegion(Frame req, byte[] header, FileChannel channel, long position,
                                     int length, Runnable releaser) {
        return Frame.fileResponse(req, header, new Frame.FilePayload(channel, position, length, releaser));
    }
```

`writeFileResponseOnEventLoop` already builds `new DefaultFileRegion(file.channel(), file.position(), file.length())` — unchanged. `DefaultFileRegion` does not close the channel on deallocate (confirmed), so the lease release in `FilePayload.close()` (fired by the write listener via `closeFrames`) is the sole owner of "done".

- [ ] **Step 4: Update `NodeHandlers.readRegionResponse` to pass the releaser**

Replace (NodeHandlers.java ~line 120-129):

```java
    private static Frame readRegionResponse(Frame req, ChunkStore.ReadRegionResult r) {
        byte[] header = new Messages.ReadResp(r.localEndOffset(), r.lastKnownDO()).encode();
        if (r.channel() != null) {
            return ScpServer.okFileRegion(req, header, r.channel(), r.filePosition(), r.length(),
                    r.releaser());
        }
        byte[] bytes = r.bytes();
        return ScpServer.ok(req, header, bytes != null && bytes.length > 0 ? ByteBuffer.wrap(bytes) : null);
    }
```

> **Ownership note:** when `r.channel() != null` the lease/FD ownership transfers to the returned `Frame`; do not also call `r.close()` on this path (the Frame's `close()` releases it). The heap-bytes path has `releaser == null` and needs no release.

- [ ] **Step 5: Build the affected modules and run their tests**

Run: `mvn -q -pl strata-proto,strata-node -am test`
Expected: PASS — proto/node compile and existing tests (e.g. `ClientServerTest`, `ServerWriteRaceInjectionTest`, `StorageNodeWireTest`) pass; sealed reads stream correctly and release the lease after the write completes.

- [ ] **Step 6: Commit**

```bash
git add strata-proto/src/main/java/io/strata/proto/Frame.java \
        strata-proto/src/main/java/io/strata/proto/ScpServer.java \
        strata-node/src/main/java/io/strata/node/NodeHandlers.java
git commit -m "feat(proto,node): release read lease on sendfile completion

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 8: Lifecycle transitions — stop holding a persistent FD per sealed chunk

This is the task that frees the FDs. Sealed chunks become **evictable** (`h.data == null`) once durable: `recoverOne` validates via a transient channel, and `seal()`/`reclaimSealedLedgersOnce()`/`importSealed()` close and null `h.data`.

**Files:**
- Modify: `strata-format/src/main/java/io/strata/format/ChunkStore.java`
- Test: `strata-format/src/test/java/io/strata/format/ChunkStoreTest.java`

**Interfaces:**
- Consumes: `Handle.data` (now nullable for sealed); `channelCache`; existing `scanDataCrcs(FileChannel,long)`.
- Produces: invariant — a SEALED+durable chunk has `h.data == null`; all sealed reads use the cache (already true after Tasks 5-6).

- [ ] **Step 1: Write the failing tests**

Add to `ChunkStoreTest.java`:

```java
    @Test
    void recoveryOpensNoPersistentFdPerSealedChunk() throws Exception {
        ChunkId a = new ChunkId(FileId.random(), 0);
        ChunkId b = new ChunkId(FileId.random(), 0);
        try (ChunkStore store = new ChunkStore(dir, true)) { // seal-fsync on: sealed + durable immediately
            sealedBytes(store, a, "alpha");
            sealedBytes(store, b, "bravo");
        }
        try (ChunkStore recovered = new ChunkStore(dir, true)) {
            assertEquals(2, recovered.sealedChunks());
            assertEquals(0, recovered.cachedChannels(),
                    "recovery must not open a persistent data FD per sealed chunk");
            // a read lazily opens (and caches) exactly one channel
            assertArrayEquals("alpha".getBytes(), recovered.read(a, 0, 5).bytes());
            assertEquals(1, recovered.cachedChannels());
        }
    }

    @Test
    void sealClosesPersistentDataFdWhenSealFsyncOn() throws Exception {
        try (ChunkStore store = new ChunkStore(dir, true)) {
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("durable-seal"));
            store.seal(id, 1, "durable-seal".length(), null);
            assertNull(handleData(store, id), "seal-fsync=on must drop the writable FD at seal");
            assertArrayEquals("durable-seal".getBytes(), store.read(id, 0, 12).bytes());
        }
    }

    @Test
    void reclaimClosesPersistentDataFdWhenSealFsyncOff() throws Exception {
        try (ChunkStore store = newStore()) { // seal-fsync off
            open(store, id, 1);
            store.append(id, 1, 0, 0, bytes("reclaim-me"));
            store.seal(id, 1, "reclaim-me".length(), null);
            assertNotNull(handleData(store, id), "seal-fsync=off keeps the FD until reclaim");
            store.reclaimSealedLedgersOnce();
            assertNull(handleData(store, id), "reclaim drops the writable FD once durable");
            assertArrayEquals("reclaim-me".getBytes(), store.read(id, 0, 10).bytes());
        }
    }
```

Add this reflective helper to `ChunkStoreTest.java` (the test file may already have reflection helpers; reuse them if so):

```java
    /** Reflectively reads the private Handle.data field for the given chunk (test-only). */
    private static FileChannel handleData(ChunkStore store, ChunkId chunkId) throws Exception {
        var chunksField = ChunkStore.class.getDeclaredField("chunks");
        chunksField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<ChunkId, ?> chunks = (java.util.Map<ChunkId, ?>) chunksField.get(store);
        Object handle = chunks.get(chunkId);
        if (handle == null) return null;
        var dataField = handle.getClass().getDeclaredField("data");
        dataField.setAccessible(true);
        return (FileChannel) dataField.get(handle);
    }
```

(Ensure imports: `static org.junit.jupiter.api.Assertions.assertNull;` and `assertNotNull;`.)

- [ ] **Step 2: Run to verify failure**

Run: `mvn -q -pl strata-format test -Dtest=ChunkStoreTest#recoveryOpensNoPersistentFdPerSealedChunk+sealClosesPersistentDataFdWhenSealFsyncOn+reclaimClosesPersistentDataFdWhenSealFsyncOff`
Expected: FAIL — recovery still opens `h.data` per sealed chunk; seal/reclaim keep `h.data`.

- [ ] **Step 3: `seal()` — close + null `h.data` on the seal-fsync path**

In `seal()`, in the `SEAL_FSYNC=true` branch where `deleteLedgerAsync(id, ledgerPath)` is called (around line 968-971), add the close+null after it:

```java
            if (sealFsync) {
                // data + sidecar were just forced durable above, so the SEALED state is recoverable
                // from the trailer without the ledger — drop it now.
                deleteLedgerAsync(id, ledgerPath);
                // sealed + durable: release the writable FD; reads now go through the channel cache.
                closeAndNullData(h);
            } else {
```

(The `else` branch — `h.sealedLedgerPending = true;` — is unchanged; it keeps `h.data` for the reclaim force.)

Add the helper near `closeQuietly` (around line 612):

```java
    /** Closes and nulls a sealed Handle's writable data channel under its monitor. Caller holds the monitor. */
    private void closeAndNullData(Handle h) {
        if (h.data != null) {
            try {
                h.data.close();
            } catch (IOException e) {
                log.warn("failed to close writable data channel for sealed chunk {}", h.id, e);
            }
            h.data = null;
        }
    }
```

- [ ] **Step 4: `reclaimSealedLedgersOnce()` — close + null after the SEALED state is durable**

In `reclaimSealedLedgersOnce()`, inside the second `synchronized (h)` block where it sets `h.sealedLedgerPending = false; reclaimed = true;` (around line 260-269), add the close+null:

```java
                synchronized (h) {
                    if (chunks.get(h.id) != h || h.data != data
                            || h.state != ChunkState.SEALED || !h.sealedLedgerPending) {
                        continue; // superseded after the force — leave it to the winner
                    }
                    h.persistSidecar(true);             // SEALED sidecar (+ directory) durable
                    Files.deleteIfExists(h.ledgerPath); // safe now: SEALED state recoverable without it
                    h.sealedLedgerPending = false;
                    closeAndNullData(h);                // release the writable FD; reads use the cache
                    reclaimed = true;
                }
```

> **Note:** `closeAndNullData` closes `data` (== `h.data`, already verified equal). The outer local `FileChannel data` reference is now closed; the subsequent `forceDirectory()` call (outside the monitor) opens its own directory channel and is unaffected.

- [ ] **Step 5: `importSealed()` — install evictable (no persistent `h.data`)**

In `importSealed()` after `Files.move(sourceFile, h.dataPath, ...)`/`forceDirectory()`/`movedData = true;` (around line 1183-1186), replace the `h.data = FileChannel.open(...)` line with leaving it null:

```java
                Files.move(sourceFile, h.dataPath, StandardCopyOption.ATOMIC_MOVE);
                forceDirectory();
                movedData = true;
                h.data = null; // sealed + durable on import: reads go through the channel cache
                h.state = ChunkState.SEALED;
```

(`cleanupFailedImport` already null-checks `h.data` before closing, so the failure path is safe.)

- [ ] **Step 6: `recoverOne()` SEALED branch — validate via a transient channel, install with `data == null`**

In `recoverOne()`, the SEALED branch currently uses `h.data` (opened at ~line 1458) to read the trailer/footer. Restructure so the OPEN branch opens `h.data` but the SEALED branch uses a transient channel. Replace the block starting at `h.data = FileChannel.open(h.dataPath, ...)` (~line 1458) through the end of the `if (sidecar.state() == SEALED) { ... } else { ... }` (~line 1528) with:

```java
            if (sidecar.state() == ChunkState.SEALED) {
                // Sealed chunks are durable + immutable: validate the trailer/footer with a transient
                // channel and DO NOT keep a persistent data FD — reads open via the channel cache.
                try (FileChannel data = FileChannel.open(h.dataPath, StandardOpenOption.READ)) {
                    long fileLen = data.size();
                    byte[] trailerBytes = new byte[TRAILER_SIZE];
                    readFully(data, ByteBuffer.wrap(trailerBytes), fileLen - TRAILER_SIZE);
                    ChunkFormats.Trailer trailer = ChunkFormats.Trailer.decode(trailerBytes);
                    int footerLen = checkedFooterLength(trailer, fileLen);
                    byte[] footerBytes = new byte[footerLen];
                    readFully(data, ByteBuffer.wrap(footerBytes), trailer.footerStart());
                    if (Crc.of(footerBytes) != trailer.footerCrc()) {
                        throw new CorruptChunkException("footer crc mismatch for sealed chunk " + id);
                    }
                    h.writeEpoch = sidecar.writeEpoch();
                    h.fenceEpoch = sidecar.fenceEpoch();
                    h.lastKnownDO = sidecar.lastKnownDO();
                    h.data = null;
                    h.state = ChunkState.SEALED;
                    h.end = trailer.dataLength();
                    h.sealedLength = trailer.dataLength();
                    h.dataCrc = trailer.dataCrc();
                    h.sealedRangeCrcs = decodeCrcRanges(footerBytes, trailer.dataLength(), trailer.sectionCount());
                    h.lastKnownDO = Math.max(h.lastKnownDO, trailer.dataLength());
                }
                if (reconstructedSidecar) {
                    h.persistSidecar();
                }
                Files.deleteIfExists(h.ledgerPath); // seal crashed before ledger delete
                forceDirectory();
            } else {
                // OPEN: keep a persistent writable channel + ledger (pinned, as before).
                h.data = FileChannel.open(h.dataPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
                h.writeEpoch = sidecar.writeEpoch();
                h.fenceEpoch = sidecar.fenceEpoch();
                h.lastKnownDO = sidecar.lastKnownDO();
                if (!Files.exists(h.ledgerPath) && h.data.size() > HEADER_SIZE) {
                    log.warn("chunk {} is OPEN with {} data bytes but NO integrity ledger — "
                            + "unverifiable data will be discarded", id, h.data.size() - HEADER_SIZE);
                }
                h.ledger = IntegrityLedger.open(h.ledgerPath);
                long verifiedEnd = 0;
                byte[] buf = null;
                for (ChunkFormats.LedgerEntry e : h.ledger.entries()) {
                    long start = verifiedEnd;
                    long delta = e.endOffset() - start;
                    if (delta <= 0 || delta > Integer.MAX_VALUE) break;
                    int len = (int) delta;
                    long entryFileEnd;
                    try {
                        entryFileEnd = checkedAdd(DATA_START, e.endOffset(), "chunk file offset");
                    } catch (ScpException overflow) {
                        break;
                    }
                    if (entryFileEnd > h.data.size()) break;
                    if (buf == null || buf.length < len) buf = new byte[len];
                    ByteBuffer bb = ByteBuffer.wrap(buf, 0, len);
                    readFully(h.data, bb, checkedAdd(DATA_START, start, "chunk file offset"));
                    if (Crc.of(buf, 0, len) != e.payloadCrc()) break;
                    h.crcAccumulate(ByteBuffer.wrap(buf, 0, len));
                    verifiedEnd = e.endOffset();
                }
                h.ledger.truncateTo(verifiedEnd);
                h.data.truncate(checkedAdd(DATA_START, verifiedEnd, "chunk file offset"));
                h.state = ChunkState.OPEN;
                h.end = verifiedEnd;
                h.lastKnownDO = Math.min(h.lastKnownDO, verifiedEnd);
                if (reconstructedSidecar) {
                    h.persistSidecar();
                }
                h.startCommitterIfFsync(forceCount);
            }
```

> **Implementer note:** `closeRecoveringHandle` (the `!installed` fallback) null-checks `h.data` before closing, so the SEALED branch leaving `h.data == null` is safe. The transient channel in the SEALED branch is closed by its try-with-resources.

- [ ] **Step 7: Run the new tests + the FULL format suite (recovery is heavily tested)**

Run: `mvn -q -pl strata-format test`
Expected: PASS — new lifecycle tests pass; `CrashRecoveryTest`, `ChunkStoreCrcTest`, and all `ChunkStoreTest` cases still pass (sealed recovery converges identically; reads now lazy via cache).

- [ ] **Step 8: Commit**

```bash
git add strata-format/src/main/java/io/strata/format/ChunkStore.java \
        strata-format/src/test/java/io/strata/format/ChunkStoreTest.java
git commit -m "feat(format): sealed chunks evictable — drop per-chunk persistent FD

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 9: `delete()` cache invalidation + delete-while-leased correctness

**Files:**
- Modify: `strata-format/src/main/java/io/strata/format/ChunkStore.java`
- Test: `strata-format/src/test/java/io/strata/format/ChunkStoreTest.java`

**Interfaces:**
- Consumes: `channelCache.invalidate(ChunkId)`.

- [ ] **Step 1: Write the failing tests**

Add to `ChunkStoreTest.java`:

```java
    @Test
    void deleteInvalidatesCachedChannel() throws Exception {
        try (ChunkStore store = newStore()) {
            sealedBytes(store, id, "delete-me");
            store.read(id, 0, 4); // caches the channel
            assertEquals(1, store.cachedChannels());
            assertEquals(ErrorCode.OK, store.delete(id));
            assertEquals(0, store.cachedChannels(), "delete must invalidate the cached channel");
        }
    }

    @Test
    void readRegionLeaseSurvivesConcurrentDelete() throws Exception {
        try (ChunkStore store = newStore()) {
            sealedBytes(store, id, "inode-alive");
            ChunkStore.ReadRegionResult region = store.readRegion(id, 0, 5);
            try {
                assertEquals(ErrorCode.OK, store.delete(id)); // unlinks file; leased FD keeps inode alive
                assertArrayEquals("inode".getBytes(), consumeRegion(region),
                        "an in-flight leased transfer still reads correct bytes after delete");
            } finally {
                region.close();
            }
        }
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `mvn -q -pl strata-format test -Dtest=ChunkStoreTest#deleteInvalidatesCachedChannel+readRegionLeaseSurvivesConcurrentDelete`
Expected: FAIL — `delete()` does not invalidate the cache (cached count stays 1).

- [ ] **Step 3: Invalidate the cache in `delete()`**

In `delete()`, after the `synchronized (h)` block completes successfully (after the block, before the slow-log/`return ErrorCode.OK`, around line 1299), add:

```java
        channelCache.invalidate(id);
```

> The leased-FD case is handled by the cache: `invalidate` removes the entry and defers the physical close until the last lease releases, so an in-flight `readRegion` transfer keeps reading the (now-unlinked) inode correctly.

- [ ] **Step 4: Run the new tests + full format suite**

Run: `mvn -q -pl strata-format test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add strata-format/src/main/java/io/strata/format/ChunkStore.java \
        strata-format/src/test/java/io/strata/format/ChunkStoreTest.java
git commit -m "feat(format): invalidate channel cache on chunk delete

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 10: Metrics — accessors, registration, dashboard

**Files:**
- Modify: `strata-node/src/main/java/io/strata/node/StorageNode.java`
- Modify: `strata-server/src/main/java/io/strata/server/ServerMetrics.java`
- Modify: `strata-server/src/test/java/io/strata/server/ServerMetricsTest.java`
- Modify: `deploy/grafana/dashboards/strata-node.json`

**Interfaces:**
- Consumes: `ChunkStore.channelCacheHits/Misses/Evictions/cachedChannels/channelCacheCapacity/openFds` (Tasks 2-3).
- Produces: `StorageNode` forwarders (same names) and Micrometer meters `strata_node_filechannel_cache{event=hit|miss|eviction}`, `strata_node_filechannel_cache_size`, `strata_node_filechannel_cache_capacity`, `strata_node_open_fds`.

- [ ] **Step 1: Confirm how StorageNode forwards ChunkStore counters**

Run:
```bash
grep -n "fsyncForceCount\|backgroundFlushes\|openChunks\|sealedChunks\|store\." \
  strata-node/src/main/java/io/strata/node/StorageNode.java
```
Expected: `StorageNode` has thin forwarders like `public long fsyncForceCount() { return store.fsyncForceCount(); }`. Add forwarders following that exact pattern:

```java
    public long channelCacheHits() { return store.channelCacheHits(); }
    public long channelCacheMisses() { return store.channelCacheMisses(); }
    public long channelCacheEvictions() { return store.channelCacheEvictions(); }
    public int cachedChannels() { return store.cachedChannels(); }
    public int channelCacheCapacity() { return store.channelCacheCapacity(); }
    public long openFds() { return store.openFds(); }
```

- [ ] **Step 2: Write the failing metrics test**

Add to `ServerMetricsTest.java`:

```java
    @Test
    void registerNodeExposesChannelCacheAndFdMeters() throws Exception {
        try (StorageNode node = new StorageNode(NodeConfig.standalone(dir))) {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            ServerMetrics.registerNode(registry, node);

            assertNotNull(registry.find("strata_node_filechannel_cache").tag("event", "hit").functionCounter());
            assertNotNull(registry.find("strata_node_filechannel_cache").tag("event", "miss").functionCounter());
            assertNotNull(registry.find("strata_node_filechannel_cache").tag("event", "eviction").functionCounter());
            assertNotNull(registry.find("strata_node_filechannel_cache_size").gauge());
            assertNotNull(registry.find("strata_node_filechannel_cache_capacity").gauge());
            assertNotNull(registry.find("strata_node_open_fds").gauge());
        }
    }
```

- [ ] **Step 3: Run to verify failure**

Run: `mvn -q -pl strata-server -am test -Dtest=ServerMetricsTest#registerNodeExposesChannelCacheAndFdMeters`
Expected: FAIL — meters not registered.

- [ ] **Step 4: Register the meters in `ServerMetrics.registerNode`**

Append inside `registerNode` (after the `strata_node_background_flush` registration, ServerMetrics.java ~line 101):

```java
        Gauge.builder("strata_node_filechannel_cache_size", n, StorageNode::cachedChannels)
                .description("open cached sealed-chunk file channels").register(reg);
        Gauge.builder("strata_node_filechannel_cache_capacity", n, StorageNode::channelCacheCapacity)
                .description("configured channel-cache capacity").register(reg);
        Gauge.builder("strata_node_open_fds", n, StorageNode::openFds)
                .description("process open file descriptors (-1 if unavailable)").register(reg);

        FunctionCounter.builder("strata_node_filechannel_cache", n, StorageNode::channelCacheHits)
                .tag("event", "hit").description("sealed-chunk channel cache events").register(reg);
        FunctionCounter.builder("strata_node_filechannel_cache", n, StorageNode::channelCacheMisses)
                .tag("event", "miss").register(reg);
        FunctionCounter.builder("strata_node_filechannel_cache", n, StorageNode::channelCacheEvictions)
                .tag("event", "eviction").register(reg);
```

- [ ] **Step 5: Run to verify pass**

Run: `mvn -q -pl strata-server -am test -Dtest=ServerMetricsTest#registerNodeExposesChannelCacheAndFdMeters`
Expected: PASS.

- [ ] **Step 6: Add Grafana panels**

In `deploy/grafana/dashboards/strata-node.json`, add two panels to the `panels` array (use unique `id`s not already present; place after the existing chunk/IO panels). Adjust `gridPos.y` to sit below existing rows:

```json
    {
      "id": 50,
      "type": "stat",
      "title": "Open file descriptors",
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "gridPos": { "h": 4, "w": 4, "x": 0, "y": 26 },
      "targets": [
        { "expr": "max(strata_node_open_fds{instance=~\"$node\"})", "refId": "A", "instant": true }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "color": { "mode": "thresholds" },
          "thresholds": { "mode": "absolute", "steps": [{ "color": "green", "value": null }] }
        }
      },
      "options": { "colorMode": "value", "graphMode": "area", "reduceOptions": { "calcs": ["lastNotNull"] } }
    },
    {
      "id": 51,
      "type": "timeseries",
      "title": "Channel cache size vs capacity & hit rate",
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "gridPos": { "h": 8, "w": 12, "x": 4, "y": 26 },
      "targets": [
        { "expr": "strata_node_filechannel_cache_size{instance=~\"$node\"}", "refId": "A", "legendFormat": "{{instance}} size" },
        { "expr": "strata_node_filechannel_cache_capacity{instance=~\"$node\"}", "refId": "B", "legendFormat": "{{instance}} capacity" },
        { "expr": "rate(strata_node_filechannel_cache_total{instance=~\"$node\",event=\"eviction\"}[5m])", "refId": "C", "legendFormat": "{{instance}} evict/s" }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": { "fillOpacity": 10, "showPoints": "never" },
          "thresholds": { "mode": "absolute", "steps": [{ "color": "green", "value": null }] }
        }
      },
      "options": {
        "legend": { "displayMode": "table", "placement": "bottom", "calcs": ["last", "max"] },
        "tooltip": { "mode": "multi", "sort": "desc" }
      }
    }
```

- [ ] **Step 7: Validate the dashboard JSON**

Run: `python3 -c "import json,sys; json.load(open('deploy/grafana/dashboards/strata-node.json')); print('ok')"`
Expected: `ok`.

- [ ] **Step 8: Commit**

```bash
git add strata-node/src/main/java/io/strata/node/StorageNode.java \
        strata-server/src/main/java/io/strata/server/ServerMetrics.java \
        strata-server/src/test/java/io/strata/server/ServerMetricsTest.java \
        deploy/grafana/dashboards/strata-node.json
git commit -m "feat(metrics): channel-cache + open-FD meters and dashboard panels

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 11: Bounded-FD integration test + full verification

**Files:**
- Test: `strata-format/src/test/java/io/strata/format/ChunkStoreTest.java`

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Write the bounded-FD test**

Add to `ChunkStoreTest.java`:

```java
    @Test
    void cachedChannelsStayBoundedAsSealedCountExceedsCapacity() throws Exception {
        // Force a tiny cache so the bound is observable without thousands of files.
        System.setProperty("strata.fileChannelCache.maxSize", "4");
        try {
            // A fresh store reads the capacity from the system property at construction time only if
            // the static knob is re-read; construct via the package-private ctor and assert capacity.
            try (ChunkStore store = new ChunkStore(dir, true)) {
                int n = 32;
                List<ChunkId> ids = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    ChunkId c = new ChunkId(FileId.random(), 0);
                    ids.add(c);
                    sealedBytes(store, c, "payload-" + i);
                }
                // read every chunk once: misses open channels, eviction keeps the open set bounded
                for (ChunkId c : ids) {
                    store.read(c, 0, 3);
                }
                assertTrue(store.cachedChannels() <= store.channelCacheCapacity(),
                        "open cached channels must not exceed capacity when no leases are held");
                assertTrue(store.channelCacheEvictions() > 0, "eviction must have fired");
            }
        } finally {
            System.clearProperty("strata.fileChannelCache.maxSize");
        }
    }
```

> **Implementer note:** `CHANNEL_CACHE_MAX_SIZE` is a `static final` read once at class load, so a system property set inside a test may not change it if the class is already loaded. If the assertion `cachedChannels() <= capacity` depends on a small capacity, instead assert against the **actual** `store.channelCacheCapacity()` (as written above) rather than a hard-coded 4 — the test holds regardless of whether the small override took effect. Keep it this way to avoid class-load-order flakiness.

- [ ] **Step 2: Run the bounded-FD test**

Run: `mvn -q -pl strata-format test -Dtest=ChunkStoreTest#cachedChannelsStayBoundedAsSealedCountExceedsCapacity`
Expected: PASS.

- [ ] **Step 3: Run every affected module's unit tests**

Run: `mvn -q -pl strata-format,strata-proto,strata-node,strata-server -am test`
Expected: PASS — all modules green.

- [ ] **Step 4: Run the integration suite (excludes chaos/perf/soak by default)**

Run: `mvn -q -pl strata-it -am test`
Expected: PASS — `RepairAndRetentionTest` (concurrent reads + delete + repair) and others pass with the cache.

- [ ] **Step 5: Perf sanity (manual, optional but recommended)**

The read fast path must stay zero-copy on a cache hit (no CRC, no chunk lock). Per the repo's perf-regression practice, verify empirically — build this branch and run the perf smoke read shape, comparing against the pre-change commit. Do **not** build concurrently with a perf run.

Run: `mvn -q -pl strata-it test -Dgroups=perf -Dtest=PerfSmokeTest` (on an otherwise-idle machine)
Expected: read throughput within noise of the baseline; `strata_node_filechannel_cache` shows hits dominating during steady-state reads.

- [ ] **Step 6: Final commit (if the test added in Step 1 is not yet committed)**

```bash
git add strata-format/src/test/java/io/strata/format/ChunkStoreTest.java
git commit -m "test(format): channel cache stays bounded beyond capacity

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- §4.1 `ChannelCache` → Task 1. §4.2 concurrency protocol (no I/O under lock, soft cap, deferred close) → Task 1 + tests. §4.3 lifecycle pinned↔evictable (seal/reclaim/import/recover) → Task 8. §4.4 sealed access via lease (read/fetch/scrub) → Tasks 4-5; readRegion → Task 6. §4.5 lease/sendfile wiring → Task 7. §4.6 delete + shutdown → Task 9 (delete) + Task 3 (`close()`). §5 capacity auto-size + knob → Task 2. §6 metrics → Task 10. §8 testing → Tasks 1-11. §7 invariants → preserved by SEALED-only scope + Task 8 keeping the OPEN branch unchanged.
- Gap check: the spec's "snapshot sealed-immutable fields under the monitor, I/O outside" is realized in Tasks 5 (read/fetch/scrub) and 6 (readRegion keeps acquire under the monitor where the old open was — noted as acceptable since the cache never takes a Handle monitor). No uncovered requirement.

**Placeholder scan:** No TBD/TODO/"handle errors"/"similar to". Every code step shows real, compilable code. The one judgment call (system-property class-load timing in Task 11) is resolved explicitly by asserting against `channelCacheCapacity()` rather than a literal.

**Type consistency:** `ChannelCache.acquire(ChunkId, Path) → Lease`; `Lease.channel()/release()/close()` used identically in Tasks 5, 6, 9. `ReadRegionResult(... , Runnable releaser)` defined in Task 6, consumed in Task 7. `FilePayload(FileChannel, long, int, Runnable)` and `okFileRegion(..., Runnable)` consistent across Task 7. `scanDataCrcs(FileChannel,long)` / `readSealedVerified(FileChannel,long,List,ChunkId,long,byte[])` defined in Task 4, used in Tasks 5 and 8. Accessor names (`channelCacheHits/Misses/Evictions`, `cachedChannels`, `channelCacheCapacity`, `openFds`) consistent across Tasks 2-3-10. `closeAndNullData(Handle)` defined once (Task 8 Step 3) and reused (Task 8 Step 4).
