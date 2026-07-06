package io.strata.node;

import io.strata.common.ChunkId;
import io.strata.common.Endpoint;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.format.ChunkStore;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Node-local orphan GC (design §9.2). A sealed chunk no owner has verified within {@code graceMs}
 * becomes a <em>suspect</em>; before deleting it the node asks the namespace's owner whether the
 * descriptor still lists this node for the chunk:
 *
 * <ul>
 *   <li>file gone / no such chunk / not this node &rarr; confirmed orphan &rarr; delete the three files;</li>
 *   <li>yes, this node is a replica &rarr; keep (the owner-pull verify pass re-stamps it);</li>
 *   <li>owner unreachable / only redirects &rarr; keep and retry later (fail-safe; never delete).</li>
 * </ul>
 *
 * <p>Two graces guard against false positives. A per-chunk grace (a freshly-known chunk is verified
 * before it can become a suspect) covers an in-flight create whose {@code FileCreated} is not yet
 * durable. A node-startup grace keeps a just-started node from GC'ing before the owner-pull verify
 * mechanism has had a cycle to attest its chunks. Owner-confirm is the ordinary cleanup signal, and
 * delete breakers below halt a large confirmed-orphan wave so a metadata-plane loss cannot turn many
 * stale descriptors into cascading physical deletes. A tripped breaker latches in memory: no further
 * orphan deletes run in its scope until the node restarts, trading reclaim latency for a hard stop.
 *
 * <p>Design note: the original orphan-GC spec required hearing a verify from <em>every</em>
 * owner, which would deadlock when an owner has no described chunks on this node
 * (so it never contacts the node) — exactly the all-orphan case. A time-based startup grace plus
 * owner-confirm and halting mass-delete breakers are sufficient for ordinary cleanup without deadlocking.
 * A dynamic owner-set change would reset the startup grace; the v0 owner set is static.
 */
final class OrphanGc implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(OrphanGc.class);

    // Constants for now (the project does not ship to prod). The grace comfortably exceeds several
    // owner-pull verify cycles, so a legitimately-held chunk is always re-attested before it could
    // be suspected; only a truly unreferenced chunk ever reaches the owner-confirm step.
    static final long DEFAULT_GRACE_MS = 6_000;
    static final long DEFAULT_SCAN_INTERVAL_MS = 3_000;
    static final long DEFAULT_STARTUP_GRACE_MS = 6_000;
    static final int DEFAULT_MAX_CONFIRMED_DELETES_PER_NAMESPACE_PER_PASS = 64;
    static final int DEFAULT_MAX_CONFIRMED_DELETE_PERCENT_PER_NAMESPACE_PER_PASS = 25;
    static final int DEFAULT_MAX_CONFIRMED_DELETES_PER_NODE_PASS = 256;
    private static final long DEFAULT_BREAKER_WINDOW_MS = 60_000;
    private static final long OPEN_BREAKER_WARN_INTERVAL_MS = 60_000;
    private static final int DEFAULT_CONFIRM_TIMEOUT_MS = 5_000;

    private final ChunkStore store;
    private final ChunkDeleteService deletes;
    private final int nodeId;
    private final List<String> controllerEndpoints;
    private final long graceMs;
    private final long scanIntervalMs;
    private final long startupGraceMs;
    private final int confirmTimeoutMs;
    private final int maxConfirmedDeletesPerNamespacePerPass;
    private final int maxConfirmedDeletePercentPerNamespacePerPass;
    private final int maxConfirmedDeletesPerNodePass;
    private final long breakerWindowMs;
    private final Set<StrataNamespace> openNamespaceBreakers = ConcurrentHashMap.newKeySet();
    private final Map<StrataNamespace, RollingCounter> namespaceConfirmedWindows = new ConcurrentHashMap<>();
    private final RollingCounter nodeConfirmedWindow = new RollingCounter();
    private final AtomicBoolean nodeBreakerOpen = new AtomicBoolean();
    private final AtomicLong breakerTrips = new AtomicLong();
    private final AtomicLong breakerSkippedChunkTotal = new AtomicLong();
    private final long startedAtMs = System.currentTimeMillis();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile int breakerHaltedNamespaces;
    private volatile int breakerHaltedChunks;
    private volatile long firstBreakerOpenedAtMs;
    private volatile long lastOpenBreakerWarnMs;
    private volatile Thread thread;

    OrphanGc(ChunkStore store, ChunkDeleteService deletes, int nodeId, List<String> controllerEndpoints,
             long graceMs, long scanIntervalMs, long startupGraceMs, int confirmTimeoutMs,
             int maxConfirmedDeletesPerNamespacePerPass, int maxConfirmedDeletePercentPerNamespacePerPass,
             int maxConfirmedDeletesPerNodePass) {
        this.store = Objects.requireNonNull(store, "store");
        this.deletes = Objects.requireNonNull(deletes, "deletes");
        this.nodeId = nodeId;
        this.controllerEndpoints = List.copyOf(controllerEndpoints);
        this.graceMs = graceMs;
        this.scanIntervalMs = scanIntervalMs;
        this.startupGraceMs = startupGraceMs;
        this.confirmTimeoutMs = confirmTimeoutMs;
        this.maxConfirmedDeletesPerNamespacePerPass = maxConfirmedDeletesPerNamespacePerPass;
        this.maxConfirmedDeletePercentPerNamespacePerPass = maxConfirmedDeletePercentPerNamespacePerPass;
        this.maxConfirmedDeletesPerNodePass = maxConfirmedDeletesPerNodePass;
        this.breakerWindowMs = Math.max(DEFAULT_BREAKER_WINDOW_MS, scanIntervalMs);
    }

    void start() {
        thread = Thread.ofVirtual().name("node-orphan-gc-" + nodeId).start(this::loop);
    }

    private void loop() {
        while (!closed.get()) {
            try {
                Thread.sleep(scanIntervalMs);
                if (System.currentTimeMillis() - startedAtMs < startupGraceMs) {
                    continue; // warm-up: let the owner-pull verify attest our chunks first
                }
                gcOnce();
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                if (!closed.get()) {
                    log.warn("orphan GC pass failed", e);
                }
            }
        }
    }

    /** One GC pass: confirm suspects, open latching breakers for mass waves, and delete only safe orphans. */
    void gcOnce() throws InterruptedException {
        long now = System.currentTimeMillis();
        List<ChunkStore.SuspectChunk> suspects = store.orphanSuspects(graceMs, now);
        if (nodeBreakerOpen.get()) {
            recordNodeBreakerBacklog(suspects);
            maybeWarnOpenBreakers(now);
            return;
        }
        Map<StrataNamespace, List<ChunkId>> confirmed = new LinkedHashMap<>();
        Map<StrataNamespace, Integer> sealedByNamespace = store.sealedChunksByNamespace();
        Map<StrataNamespace, Integer> haltedByOpenNamespaceBreaker = new LinkedHashMap<>();
        for (ChunkStore.SuspectChunk s : suspects) {
            if (closed.get()) {
                return;
            }
            if (openNamespaceBreakers.contains(s.namespace())) {
                haltedByOpenNamespaceBreaker.merge(s.namespace(), 1, Integer::sum);
                continue;
            }
            switch (confirm(s.namespace(), s.chunkId())) {
                case ORPHAN -> confirmed.computeIfAbsent(s.namespace(), ignored -> new ArrayList<>()).add(s.chunkId());
                case KEEP, UNREACHABLE -> { /* fail-safe: never delete a kept or unconfirmed suspect */ }
            }
        }

        int haltedNamespaces = haltedByOpenNamespaceBreaker.size();
        int haltedChunks = haltedByOpenNamespaceBreaker.values().stream().mapToInt(Integer::intValue).sum();
        Map<StrataNamespace, List<ChunkId>> deleteCandidates = new LinkedHashMap<>();
        for (Map.Entry<StrataNamespace, List<ChunkId>> e : confirmed.entrySet()) {
            List<ChunkId> chunks = e.getValue();
            if (chunks.isEmpty()) {
                continue;
            }
            int namespaceBudget = namespaceBudget(sealedByNamespace.getOrDefault(e.getKey(), 0));
            long windowConfirmed = namespaceConfirmedWindows
                    .computeIfAbsent(e.getKey(), ignored -> new RollingCounter())
                    .add(now, chunks.size(), breakerWindowMs);
            if (namespaceBreakerShouldTrip(windowConfirmed, namespaceBudget)) {
                tripNamespaceBreaker(e.getKey(), windowConfirmed, namespaceBudget, chunks.size(), now);
                haltedNamespaces++;
                haltedChunks += chunks.size();
                continue;
            }
            deleteCandidates.put(e.getKey(), chunks);
        }

        int residualConfirmed = deleteCandidates.values().stream().mapToInt(List::size).sum();
        if (residualConfirmed > 0) {
            long windowConfirmed = nodeConfirmedWindow.add(now, residualConfirmed, breakerWindowMs);
            if (nodeBreakerShouldTrip(windowConfirmed)) {
                tripNodeBreaker(windowConfirmed, deleteCandidates.size(), residualConfirmed, now);
                haltedNamespaces += deleteCandidates.size();
                haltedChunks += residualConfirmed;
                recordBreakerHalted(haltedNamespaces, haltedChunks);
                maybeWarnOpenBreakers(now);
                return;
            }
        }

        for (Map.Entry<StrataNamespace, List<ChunkId>> e : deleteCandidates.entrySet()) {
            for (ChunkId chunkId : e.getValue()) {
                if (confirm(e.getKey(), chunkId) == Verdict.ORPHAN) {
                    deleteConfirmed(e.getKey(), chunkId);
                }
            }
        }
        recordBreakerHalted(haltedNamespaces, haltedChunks);
        maybeWarnOpenBreakers(now);
    }

    private boolean namespaceBreakerShouldTrip(long confirmedChunks, int namespaceBudget) {
        return namespaceBudget != Integer.MAX_VALUE && confirmedChunks > namespaceBudget;
    }

    private boolean nodeBreakerShouldTrip(long confirmedChunks) {
        return maxConfirmedDeletesPerNodePass > 0 && confirmedChunks > maxConfirmedDeletesPerNodePass;
    }

    private void tripNamespaceBreaker(StrataNamespace namespace, long windowConfirmed, int namespaceBudget,
                                      int skippedChunks, long now) {
        if (openNamespaceBreakers.add(namespace)) {
            noteBreakerOpened(now);
            breakerTrips.incrementAndGet();
            breakerSkippedChunkTotal.addAndGet(skippedChunks);
            log.error("orphan GC breaker opened for namespace {}: {} confirmed orphans in {}ms exceeds budget {}; "
                            + "skipped {} deletes and halted this namespace until node restart",
                    namespace, windowConfirmed, breakerWindowMs, namespaceBudget, skippedChunks);
        }
    }

    private void tripNodeBreaker(long windowConfirmed, int namespaces, int skippedChunks, long now) {
        if (nodeBreakerOpen.compareAndSet(false, true)) {
            noteBreakerOpened(now);
            breakerTrips.incrementAndGet();
            breakerSkippedChunkTotal.addAndGet(skippedChunks);
            log.error("orphan GC node-wide breaker opened: {} confirmed orphans in {}ms across {} namespaces "
                            + "exceeds node budget {}; skipped {} deletes and halted all orphan GC until node restart",
                    windowConfirmed, breakerWindowMs, namespaces, maxConfirmedDeletesPerNodePass, skippedChunks);
        }
    }

    private void noteBreakerOpened(long now) {
        if (firstBreakerOpenedAtMs == 0) {
            firstBreakerOpenedAtMs = now;
        }
    }

    private int namespaceBudget(int sealedChunkCount) {
        int budget = Integer.MAX_VALUE;
        if (maxConfirmedDeletesPerNamespacePerPass > 0) {
            budget = Math.min(budget, maxConfirmedDeletesPerNamespacePerPass);
        }
        if (maxConfirmedDeletePercentPerNamespacePerPass > 0) {
            int percentBudget = sealedChunkCount <= 0
                    ? 0
                    : Math.max(1, (int) ((sealedChunkCount * (long) maxConfirmedDeletePercentPerNamespacePerPass) / 100));
            budget = Math.min(budget, percentBudget);
        }
        return budget;
    }

    private void recordNodeBreakerBacklog(List<ChunkStore.SuspectChunk> suspects) {
        Map<StrataNamespace, Integer> byNamespace = new LinkedHashMap<>();
        for (ChunkStore.SuspectChunk s : suspects) {
            byNamespace.merge(s.namespace(), 1, Integer::sum);
        }
        recordBreakerHalted(byNamespace.size(), suspects.size());
    }

    private void recordBreakerHalted(int haltedNamespaces, int haltedChunks) {
        this.breakerHaltedNamespaces = haltedNamespaces;
        this.breakerHaltedChunks = haltedChunks;
    }

    private void maybeWarnOpenBreakers(long now) {
        if (!nodeBreakerOpen.get() && openNamespaceBreakers.isEmpty()) {
            return;
        }
        long last = lastOpenBreakerWarnMs;
        if (last != 0 && now - last < OPEN_BREAKER_WARN_INTERVAL_MS) {
            return;
        }
        lastOpenBreakerWarnMs = now;
        long openForMs = firstBreakerOpenedAtMs == 0 ? 0 : now - firstBreakerOpenedAtMs;
        log.warn("orphan GC halted: nodeBreakerOpen={} namespaceBreakers={} haltedNamespaces={} "
                        + "haltedChunks={} openForMs={}; verify metadata/chunk state and restart node to resume",
                nodeBreakerOpen.get(), openNamespaceBreakers.size(), breakerHaltedNamespaces,
                breakerHaltedChunks, openForMs);
    }

    int breakerOpenNamespaces() {
        return openNamespaceBreakers.size();
    }

    boolean namespaceBreakerOpen(StrataNamespace namespace) {
        return openNamespaceBreakers.contains(namespace);
    }

    boolean nodeBreakerOpen() {
        return nodeBreakerOpen.get();
    }

    long breakerTrips() {
        return breakerTrips.get();
    }

    long breakerSkippedChunkTotal() {
        return breakerSkippedChunkTotal.get();
    }

    int breakerHaltedNamespaces() {
        return breakerHaltedNamespaces;
    }

    int breakerHaltedChunks() {
        return breakerHaltedChunks;
    }

    private void deleteConfirmed(StrataNamespace namespace, ChunkId chunkId) throws InterruptedException {
        ErrorCode result = deletes.delete(namespace, chunkId);
        if (result == ErrorCode.OK) {
            log.info("orphan GC: deleted unreferenced sealed chunk {} in ns={}", chunkId, namespace);
        } else {
            log.warn("orphan GC: confirmed orphan {} in ns={} failed to delete: {}",
                    chunkId, namespace, result);
        }
    }

    private enum Verdict { ORPHAN, KEEP, UNREACHABLE }

    private static final class RollingCounter {
        private long windowStartedAtMs = Long.MIN_VALUE;
        private long count;

        synchronized long add(long now, int delta, long windowMs) {
            if (delta <= 0) {
                return count;
            }
            if (windowStartedAtMs == Long.MIN_VALUE || now - windowStartedAtMs > windowMs) {
                windowStartedAtMs = now;
                count = 0;
            }
            count += delta;
            return count;
        }
    }

    /**
     * Asks the namespace's owner whether its descriptor still lists this node for {@code chunkId}. Only
     * a definitive answer from the authoritative owner deletes; an unreachable owner (or only NOT_LEADER
     * redirects, or any other error) keeps the chunk.
     */
    private Verdict confirm(StrataNamespace ns, ChunkId chunkId) {
        FileId fileId = chunkId.fileId();
        byte[] req = new Messages.LookupFile(ns, fileId).encode();
        for (String ep : controllerEndpoints) {
            Endpoint endpoint;
            try {
                endpoint = Endpoint.parse(ep, "controller endpoint", ErrorCode.INTERNAL);
            } catch (Exception e) {
                continue;
            }
            try (ScpClient client = new ScpClient(endpoint.host(), endpoint.port(),
                    ScpClient.KIND_TOOL, "orphan-confirm")) {
                ByteBuffer resp = client.call(Opcode.LOOKUP_FILE, req, null, confirmTimeoutMs);
                Messages.LookupFileResp r = Messages.LookupFileResp.decode(resp);
                for (Messages.ChunkInfo ci : r.chunks()) {
                    if (ci.chunkId().equals(chunkId)) {
                        for (Messages.Replica rep : ci.replicas()) {
                            if (rep.nodeId() == nodeId) {
                                return Verdict.KEEP; // the owner still lists us — not an orphan
                            }
                        }
                        return Verdict.ORPHAN; // chunk exists but the descriptor no longer lists us
                    }
                }
                return Verdict.ORPHAN; // file exists but has no such chunk — orphan
            } catch (ScpException se) {
                if (se.code() == ErrorCode.FILE_NOT_FOUND) {
                    return Verdict.ORPHAN; // ordinary cleanup signal; breakers still halt mass waves
                }
                if (se.code() == ErrorCode.NOT_LEADER) {
                    continue; // this controller is not the owner — try the next endpoint
                }
                // any other error: do not trust it as a delete signal — fall through to the next endpoint
            } catch (Exception e) {
                // connection failure: try the next endpoint
            }
        }
        return Verdict.UNREACHABLE; // no owner gave a definitive answer — keep (fail-safe)
    }

    @Override
    public void close() {
        closed.set(true);
        Thread t = thread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
