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
 * Bounded LRU pool of READ-only {@link FileChannel}s for SEALED chunk data.
 *
 * Each acquire hands out an EXCLUSIVE channel — a channel is never shared by two concurrent leases.
 * This matters because {@link FileChannel} is an {@link java.nio.channels.InterruptibleChannel}: if a
 * thread blocked on I/O is interrupted (connection cancel, replica fault, pool shutdown), the JVM
 * closes that channel. A shared channel would then be closed out from under every other concurrent
 * reader; exclusive channels confine that damage to the interrupted operation.
 *
 * Released channels return to a per-chunk idle pool bounded by {@code capacity} (LRU-evicted across
 * chunks); a channel that comes back closed (interrupted) is discarded, not pooled. Open/close run
 * OUTSIDE the cache lock so a blocking close never pins a virtual-thread carrier. fsync is per-inode,
 * and this pool only serves SEALED, durable, immutable chunks, so reopening a chunk's channel later
 * is always safe.
 */
final class ChannelCache implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ChannelCache.class);

    interface Lease extends AutoCloseable {
        FileChannel channel();
        void release();
        @Override default void close() { release(); }
    }

    private final int capacity;
    private final ReentrantLock lock = new ReentrantLock();
    /** Idle (not-leased) channels per chunk; access-order so iteration yields least-recently-used chunks first. */
    private final LinkedHashMap<NsChunkId, Deque<FileChannel>> idle = new LinkedHashMap<>(16, 0.75f, true);
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
        lock.lock();
        try {
            if (!closed) {
                Deque<FileChannel> dq = idle.get(id); // access-order touch -> MRU
                if (dq != null) {
                    while (!dq.isEmpty()) {
                        FileChannel ch = dq.pollFirst();
                        idleCount--;
                        if (dq.isEmpty()) {
                            idle.remove(id);
                        }
                        if (ch.isOpen()) {
                            hits.incrementAndGet();
                            return new LeaseImpl(id, ch);
                        }
                        closeQuietly(ch); // a pooled channel should never be closed; never hand one out
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        // miss (or cache closed): open a fresh exclusive channel outside the lock
        FileChannel ch = FileChannel.open(dataPath, StandardOpenOption.READ);
        misses.incrementAndGet();
        return new LeaseImpl(id, ch);
    }

    /**
     * Drop a chunk's pooled idle channels (on delete/quarantine). Leased channels are exclusive and
     * unaffected — a read in flight keeps the unlinked inode alive and completes; when released the
     * channel pools again under a fresh entry and is reclaimed later by capacity eviction or close().
     */
    void invalidate(NsChunkId id) {
        List<FileChannel> toClose;
        lock.lock();
        try {
            Deque<FileChannel> dq = idle.remove(id);
            if (dq == null || dq.isEmpty()) {
                return;
            }
            idleCount -= dq.size();
            toClose = new ArrayList<>(dq);
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
            for (Deque<FileChannel> dq : idle.values()) {
                toClose.addAll(dq);
            }
            idle.clear();
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
        Iterator<Map.Entry<NsChunkId, Deque<FileChannel>>> it = idle.entrySet().iterator();
        while (idleCount > capacity && it.hasNext()) {
            Map.Entry<NsChunkId, Deque<FileChannel>> e = it.next();
            Deque<FileChannel> dq = e.getValue();
            while (idleCount > capacity && !dq.isEmpty()) {
                evicted.add(dq.pollFirst());
                idleCount--;
                evictions.incrementAndGet();
            }
            if (dq.isEmpty()) {
                it.remove();
            }
        }
        return evicted;
    }

    private final class LeaseImpl implements Lease {
        private final NsChunkId id;
        private final FileChannel channel;
        private boolean released;
        LeaseImpl(NsChunkId id, FileChannel channel) {
            this.id = id;
            this.channel = channel;
        }

        @Override public FileChannel channel() { return channel; }

        @Override public void release() {
            FileChannel closeNow = null;
            List<FileChannel> evicted = null;
            lock.lock();
            try {
                if (released) {
                    return;
                }
                released = true;
                if (closed || !channel.isOpen()) {
                    closeNow = channel; // shutting down, or interrupted/closed mid-use: do not pool
                } else {
                    Deque<FileChannel> dq = idle.get(id); // access-order touch -> MRU
                    if (dq == null) {
                        dq = new ArrayDeque<>();
                        idle.put(id, dq);
                    }
                    dq.addLast(channel);
                    idleCount++;
                    evicted = evictDownToCapacity();
                }
            } finally {
                lock.unlock();
            }
            closeQuietly(closeNow);
            if (evicted != null) {
                closeAllQuietly(evicted);
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
