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

    /**
     * Caller holds the lock. Removes least-recently-used, idle (refCount==0) entries and schedules
     * their channels for close until size <= capacity. Pinned (refCount>0) entries are SKIPPED and
     * left in the map: capacity is a SOFT cap — a leased channel is never closed by eviction, so the
     * resident map may temporarily exceed capacity while many distinct chunks are concurrently leased
     * (bounded by the number of concurrent distinct reads). Such over-capacity idle entries are
     * reclaimed on a later eviction once their leases release.
     */
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
