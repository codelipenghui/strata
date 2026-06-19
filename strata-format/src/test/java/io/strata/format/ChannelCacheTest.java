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
    void leasedChannelIsNeverEvictedAndReclaimedAfterRelease() throws Exception {
        Path p1 = file(1, "a");
        Path p2 = file(2, "b");
        Path p3 = file(3, "c");
        Path p4 = file(4, "d");
        try (ChannelCache cache = new ChannelCache(2)) {
            ChannelCache.Lease held = cache.acquire(id(1), p1); // keep id(1) leased
            FileChannel c1 = held.channel();
            try (ChannelCache.Lease l = cache.acquire(id(2), p2)) { l.channel(); }
            try (ChannelCache.Lease l = cache.acquire(id(3), p3)) { l.channel(); }
            // id(1) is the LRU but pinned: the soft cap keeps it open AND cached
            assertTrue(c1.isOpen(), "a leased channel is never closed by eviction");
            assertTrue(cache.size() >= 2, "pinned over-capacity entry stays resident");
            held.release();
            // release alone does not close a soft-cap-retained channel
            assertTrue(c1.isOpen(), "release returns the FD to the cache, does not close it");
            // the now-idle, over-capacity channel is reclaimed on the next eviction
            try (ChannelCache.Lease l = cache.acquire(id(4), p4)) { l.channel(); }
            assertFalse(c1.isOpen(), "the now-idle over-capacity channel is reclaimed on the next miss");
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
