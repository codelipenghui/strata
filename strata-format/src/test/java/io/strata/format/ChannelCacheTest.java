package io.strata.format;

import io.strata.common.ChunkId;
import io.strata.common.FileId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    void acquireReusesAPooledChannelAfterRelease() throws Exception {
        Path p = file(1, "hello");
        try (ChannelCache cache = new ChannelCache(8)) {
            ChannelCache.Lease a = cache.acquire(id(1), p);
            FileChannel chA = a.channel();
            assertEquals("hello", new String(readAll(chA, 5), StandardCharsets.UTF_8));
            a.release(); // returns chA to the idle pool
            ChannelCache.Lease b = cache.acquire(id(1), p);
            assertSame(chA, b.channel(), "a released channel is reused from the pool on the next acquire");
            assertEquals(1, cache.misses());
            assertEquals(1, cache.hits());
            assertEquals(0, cache.size(), "the reused channel is leased again, not idle");
            b.release();
            assertEquals(1, cache.size(), "released channel is back in the idle pool");
        }
    }

    @Test
    void concurrentLeasesOfSameChunkGetExclusiveChannels() throws Exception {
        Path p = file(1, "x");
        try (ChannelCache cache = new ChannelCache(8)) {
            ChannelCache.Lease a = cache.acquire(id(1), p);
            ChannelCache.Lease b = cache.acquire(id(1), p);
            assertNotSame(a.channel(), b.channel(),
                    "concurrent leases of one chunk must NOT share a FileChannel (interrupt-safety)");
            assertTrue(a.channel().isOpen() && b.channel().isOpen());
            a.release();
            b.release();
        }
    }

    @Test
    void interruptClosingOneLeasedChannelDoesNotAffectAnother() throws Exception {
        // Closing one lease's channel (as a thread interrupt on an InterruptibleChannel would) must
        // not break a concurrent lease — that is the whole point of exclusive channels.
        Path p = file(1, "payload");
        try (ChannelCache cache = new ChannelCache(8)) {
            ChannelCache.Lease a = cache.acquire(id(1), p);
            ChannelCache.Lease b = cache.acquire(id(1), p);
            a.channel().close(); // simulate an interrupt closing channel A
            assertFalse(a.channel().isOpen());
            assertTrue(b.channel().isOpen(), "channel B is independent of A");
            assertEquals("payload", new String(readAll(b.channel(), 7), StandardCharsets.UTF_8));
            a.release(); // the closed channel is discarded, not pooled
            b.release();
            assertEquals(1, cache.size(), "only the still-open channel returned to the pool");
        }
    }

    @Test
    void releasingClosedChannelDoesNotPoolIt() throws Exception {
        Path p = file(1, "x");
        try (ChannelCache cache = new ChannelCache(8)) {
            ChannelCache.Lease a = cache.acquire(id(1), p);
            a.channel().close();
            a.release();
            assertEquals(0, cache.size(), "a closed channel is discarded on release");
            ChannelCache.Lease b = cache.acquire(id(1), p);
            assertTrue(b.channel().isOpen());
            assertEquals(2, cache.misses(), "no pooled channel to reuse -> a fresh open");
            b.release();
        }
    }

    @Test
    void idlePoolIsBoundedByCapacityAcrossChunks() throws Exception {
        try (ChannelCache cache = new ChannelCache(2)) {
            ChannelCache.Lease l1 = cache.acquire(id(1), file(1, "a"));
            FileChannel first = l1.channel();
            l1.release();
            ChannelCache.Lease l2 = cache.acquire(id(2), file(2, "b"));
            l2.release();
            ChannelCache.Lease l3 = cache.acquire(id(3), file(3, "c"));
            l3.release();
            assertEquals(2, cache.size(), "idle pool bounded by capacity");
            assertEquals(1, cache.evictions());
            assertFalse(first.isOpen(), "the least-recently-used idle channel (id1) was evicted and closed");
        }
    }

    @Test
    void concurrentAcquireSameChunkAllExclusiveThenPoolBounded() throws Exception {
        Path p = file(1, "x");
        try (ChannelCache cache = new ChannelCache(4)) {
            int threads = 16;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch acquired = new CountDownLatch(threads);
            CountDownLatch mayRelease = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            Set<FileChannel> channels = ConcurrentHashMap.newKeySet();
            AtomicReference<Throwable> err = new AtomicReference<>();
            for (int i = 0; i < threads; i++) {
                Thread.ofVirtual().start(() -> {
                    try {
                        start.await();
                        ChannelCache.Lease l = cache.acquire(id(1), p);
                        channels.add(l.channel());
                        acquired.countDown();
                        mayRelease.await();
                        l.release();
                    } catch (Throwable t) {
                        err.set(t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            acquired.await(); // all 16 hold leases simultaneously
            assertEquals(16, channels.size(), "16 simultaneous leases of one chunk -> 16 exclusive channels");
            mayRelease.countDown();
            done.await();
            if (err.get() != null) throw new AssertionError(err.get());
            assertTrue(cache.size() <= cache.capacity(), "idle pool bounded after all leases released");
        }
    }

    @Test
    void invalidateClosesPooledIdleChannels() throws Exception {
        Path p = file(1, "x");
        try (ChannelCache cache = new ChannelCache(8)) {
            ChannelCache.Lease a = cache.acquire(id(1), p);
            FileChannel ch = a.channel();
            a.release(); // pooled idle
            assertEquals(1, cache.size());
            cache.invalidate(id(1));
            assertEquals(0, cache.size());
            assertFalse(ch.isOpen(), "invalidate closes a chunk's pooled idle channels");
        }
    }

    @Test
    void invalidateLeavesAConcurrentLeaseUsable() throws Exception {
        Path p = file(1, "inode-alive");
        try (ChannelCache cache = new ChannelCache(8)) {
            ChannelCache.Lease held = cache.acquire(id(1), p); // exclusive, not in the idle pool
            cache.invalidate(id(1)); // closes idle (none); the leased channel is untouched
            assertTrue(held.channel().isOpen(), "a leased channel survives invalidate (delete-while-reading)");
            assertEquals("inode", new String(readAll(held.channel(), 5), StandardCharsets.UTF_8));
            held.release();
        }
    }

    @Test
    void releaseIsIdempotent() throws Exception {
        Path p = file(1, "x");
        try (ChannelCache cache = new ChannelCache(8)) {
            ChannelCache.Lease l = cache.acquire(id(1), p);
            l.release();
            assertEquals(1, cache.size());
            l.release(); // must not double-pool
            assertEquals(1, cache.size());
        }
    }

    @Test
    void closeClosesAllIdleChannels() throws Exception {
        Path p1 = file(1, "a");
        Path p2 = file(2, "b");
        ChannelCache cache = new ChannelCache(8);
        ChannelCache.Lease l1 = cache.acquire(id(1), p1);
        FileChannel c1 = l1.channel();
        l1.release();
        ChannelCache.Lease l2 = cache.acquire(id(2), p2);
        FileChannel c2 = l2.channel();
        l2.release();
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
