package io.strata.format;

import io.strata.common.NsChunkId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded LRU pool of READ-only {@link FileChannel}s for chunk data.
 *
 * Each acquire hands out an EXCLUSIVE channel; a channel is never shared by two concurrent leases.
 * This matters because {@link FileChannel} is an {@link java.nio.channels.InterruptibleChannel}: if a
 * thread blocked on I/O is interrupted (connection cancel, replica fault, pool shutdown), the JVM
 * closes that channel. A shared channel would then be closed out from under every other concurrent
 * reader; exclusive channels confine that damage to the interrupted operation.
 *
 * Released channels return to a per-chunk idle pool bounded by {@code capacity} (LRU-evicted across
 * chunks); a channel that comes back closed (interrupted) or stale after an invalidate is discarded,
 * not pooled.
 */
final class ChannelCache implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ChannelCache.class);
    private static final int MAX_POOLED_LEASES_PER_THREAD = 64;
    private static final ThreadLocal<ArrayDeque<LeaseImpl>> LEASES =
            ThreadLocal.withInitial(ArrayDeque::new);

    interface Lease extends AutoCloseable {
        FileChannel channel();
        void release();
        @Override default void close() { release(); }
    }

    private final int capacity;
    private final ReentrantLock lock = new ReentrantLock();
    /** Per-chunk entries; access-order so iteration yields least-recently-used chunks first. */
    private final LinkedHashMap<NsChunkId, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);
    private int idleCount;
    private boolean closed;

    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();

    ChannelCache(int capacity) {
        this.capacity = Math.max(1, capacity);
    }

    /** Borrow an exclusive READ channel for {@code id}, reusing a pooled idle one or opening a new one. */
    Lease acquire(NsChunkId id, Path dataPath) throws IOException {
        Entry entry = null;
        long generation = 0;
        lock.lock();
        try {
            if (!closed) {
                entry = entries.computeIfAbsent(id, Entry::new); // access-order touch -> MRU
                entry.leased++;
                generation = entry.generation;
                while (!entry.idle.isEmpty()) {
                    FileChannel ch = entry.idle.pollFirst();
                    idleCount--;
                    if (ch.isOpen()) {
                        hits.incrementAndGet();
                        return LeaseImpl.acquire(this, entry, ch, generation);
                    }
                    closeQuietly(ch); // a pooled channel should never be closed; never hand one out
                }
            }
        } finally {
            lock.unlock();
        }

        FileChannel ch;
        try {
            ch = FileChannel.open(dataPath, StandardOpenOption.READ);
        } catch (IOException | RuntimeException e) {
            if (entry != null) {
                cancelAcquire(entry);
            }
            throw e;
        }
        misses.incrementAndGet();
        return LeaseImpl.acquire(this, entry, ch, generation);
    }

    private void cancelAcquire(Entry entry) {
        lock.lock();
        try {
            entry.leased--;
            removeUnusedEntry(entry);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Drop a chunk's pooled idle channels (on delete/quarantine/replacement). Leased channels are
     * exclusive and unaffected: a read in flight keeps the old inode alive and completes. The generation
     * bump prevents that stale lease from returning to the idle pool after it is released.
     */
    void invalidate(NsChunkId id) {
        List<FileChannel> toClose = List.of();
        lock.lock();
        try {
            Entry entry = entries.get(id);
            if (entry == null) {
                return;
            }
            entry.generation++;
            if (!entry.idle.isEmpty()) {
                idleCount -= entry.idle.size();
                toClose = new ArrayList<>(entry.idle);
                entry.idle.clear();
            }
            removeUnusedEntry(entry);
        } finally {
            lock.unlock();
        }
        closeAllQuietly(toClose);
    }

    @Override
    public void close() {
        List<FileChannel> toClose = new ArrayList<>();
        lock.lock();
        try {
            closed = true;
            for (Entry entry : entries.values()) {
                toClose.addAll(entry.idle);
                entry.idle.clear();
            }
            entries.clear();
            idleCount = 0;
        } finally {
            lock.unlock();
        }
        closeAllQuietly(toClose);
    }

    long hits() { return hits.get(); }
    long misses() { return misses.get(); }
    long evictions() { return evictions.get(); }
    int capacity() { return capacity; }

    /** Number of pooled idle channels currently held (bounded by capacity). */
    int size() {
        lock.lock();
        try {
            return idleCount;
        } finally {
            lock.unlock();
        }
    }

    /** Caller holds the lock. Evicts least-recently-used idle channels until idleCount <= capacity. */
    private List<FileChannel> evictDownToCapacity() {
        if (idleCount <= capacity) {
            return null;
        }
        List<FileChannel> evicted = new ArrayList<>();
        Iterator<Map.Entry<NsChunkId, Entry>> it = entries.entrySet().iterator();
        while (idleCount > capacity && it.hasNext()) {
            Entry entry = it.next().getValue();
            while (idleCount > capacity && !entry.idle.isEmpty()) {
                evicted.add(entry.idle.pollFirst());
                idleCount--;
                evictions.incrementAndGet();
            }
            if (entry.leased == 0 && entry.idle.isEmpty()) {
                it.remove();
            }
        }
        return evicted;
    }

    private void removeUnusedEntry(Entry entry) {
        if (entry.leased == 0 && entry.idle.isEmpty()) {
            entries.remove(entry.id, entry);
        }
    }

    private static final class Entry {
        private final NsChunkId id;
        private final Deque<FileChannel> idle = new ArrayDeque<>();
        private int leased;
        private long generation;

        private Entry(NsChunkId id) {
            this.id = id;
        }
    }

    private static final class LeaseImpl implements Lease {
        private ChannelCache owner;
        private Entry entry;
        private FileChannel channel;
        private long generation;
        private boolean released;

        private static LeaseImpl acquire(ChannelCache owner, Entry entry, FileChannel channel, long generation) {
            ArrayDeque<LeaseImpl> leases = LEASES.get();
            LeaseImpl lease = leases.pollFirst();
            if (lease == null) {
                lease = new LeaseImpl();
            }
            lease.owner = owner;
            lease.entry = entry;
            lease.channel = channel;
            lease.generation = generation;
            lease.released = false;
            return lease;
        }

        @Override public FileChannel channel() { return channel; }

        @Override public void release() {
            if (released) {
                return;
            }
            ChannelCache cache = owner;
            Entry leasedEntry = entry;
            FileChannel leasedChannel = channel;
            long leasedGeneration = generation;
            released = true;
            owner = null;
            entry = null;
            channel = null;
            generation = 0;

            FileChannel closeNow = null;
            List<FileChannel> evicted = null;
            if (leasedEntry == null) {
                closeNow = leasedChannel;
            } else {
                cache.lock.lock();
                try {
                    leasedEntry.leased--;
                    if (cache.closed || !leasedChannel.isOpen() || leasedEntry.generation != leasedGeneration) {
                        closeNow = leasedChannel; // shutting down, interrupted, or invalidated while leased
                    } else {
                        leasedEntry.idle.addLast(leasedChannel);
                        cache.idleCount++;
                        evicted = cache.evictDownToCapacity();
                    }
                    cache.removeUnusedEntry(leasedEntry);
                } finally {
                    cache.lock.unlock();
                }
            }
            closeQuietly(closeNow);
            if (evicted != null) {
                closeAllQuietly(evicted);
            }
            recycle(this);
        }

        private static void recycle(LeaseImpl lease) {
            ArrayDeque<LeaseImpl> leases = LEASES.get();
            if (leases.size() < MAX_POOLED_LEASES_PER_THREAD) {
                leases.addFirst(lease);
            }
        }
    }

    private static void closeAllQuietly(List<FileChannel> channels) {
        for (FileChannel c : channels) {
            closeQuietly(c);
        }
    }

    private static void closeQuietly(FileChannel c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (IOException e) {
            log.warn("failed to close cached channel", e);
        }
    }
}
