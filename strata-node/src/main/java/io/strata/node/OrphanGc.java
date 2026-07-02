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

/**
 * Node-local orphan GC (design §20.4). A sealed chunk no owner has verified within {@code graceMs}
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
 * mechanism has had a cycle to attest its chunks. A single owner-confirm is enough for ordinary cleanup,
 * but a metadata-plane loss can make many old chunks look absent at once, so the mass-delete circuit below
 * refuses a large confirmed-orphan wave without an operator override.
 *
 * <p>Design note (§20.4 deviation): the spec's strict "trust no-owner-verified only after hearing a
 * verify from <em>every</em> owner" would deadlock when an owner has no described chunks on this node
 * (so it never contacts the node) — exactly the all-orphan case. Because the per-chunk confirm is
 * authoritative and fail-safe, a time-based startup grace plus that confirm is sufficient and cannot
 * deadlock. A dynamic owner-set change would reset the startup grace; the v0 owner set is static.
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
    private final Set<StrataNamespace> deleteCircuitsOpen = ConcurrentHashMap.newKeySet();
    private final long startedAtMs = System.currentTimeMillis();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Thread thread;

    OrphanGc(ChunkStore store, ChunkDeleteService deletes, int nodeId, List<String> controllerEndpoints,
             long graceMs, long scanIntervalMs, long startupGraceMs, int confirmTimeoutMs,
             int maxConfirmedDeletesPerNamespacePerPass) {
        if (maxConfirmedDeletesPerNamespacePerPass < 0) {
            throw new IllegalArgumentException("maxConfirmedDeletesPerNamespacePerPass must be non-negative: "
                    + maxConfirmedDeletesPerNamespacePerPass);
        }
        this.store = Objects.requireNonNull(store, "store");
        this.deletes = Objects.requireNonNull(deletes, "deletes");
        this.nodeId = nodeId;
        this.controllerEndpoints = List.copyOf(controllerEndpoints);
        this.graceMs = graceMs;
        this.scanIntervalMs = scanIntervalMs;
        this.startupGraceMs = startupGraceMs;
        this.confirmTimeoutMs = confirmTimeoutMs;
        this.maxConfirmedDeletesPerNamespacePerPass = maxConfirmedDeletesPerNamespacePerPass;
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

    /** One GC pass: confirm each suspect with its owner and delete only confirmed orphans. */
    void gcOnce() throws InterruptedException {
        long now = System.currentTimeMillis();
        Map<StrataNamespace, List<ChunkId>> confirmed = new LinkedHashMap<>();
        for (ChunkStore.SuspectChunk s : store.orphanSuspects(graceMs, now)) {
            if (closed.get()) {
                return;
            }
            if (deleteCircuitsOpen.contains(s.namespace())) {
                continue;
            }
            switch (confirm(s.namespace(), s.chunkId())) {
                case ORPHAN -> recordConfirmedOrphan(confirmed, s.namespace(), s.chunkId());
                case KEEP, UNREACHABLE -> { /* fail-safe: never delete a kept or unconfirmed suspect */ }
            }
        }
        for (Map.Entry<StrataNamespace, List<ChunkId>> e : confirmed.entrySet()) {
            if (deleteCircuitsOpen.contains(e.getKey())) {
                continue;
            }
            for (ChunkId chunkId : e.getValue()) {
                deleteConfirmed(e.getKey(), chunkId);
            }
        }
    }

    private void recordConfirmedOrphan(Map<StrataNamespace, List<ChunkId>> confirmed,
                                       StrataNamespace namespace, ChunkId chunkId) {
        List<ChunkId> nsConfirmed = confirmed.computeIfAbsent(namespace, ignored -> new ArrayList<>());
        nsConfirmed.add(chunkId);
        if (maxConfirmedDeletesPerNamespacePerPass == 0
                || nsConfirmed.size() <= maxConfirmedDeletesPerNamespacePerPass) {
            return;
        }
        confirmed.remove(namespace);
        if (deleteCircuitsOpen.add(namespace)) {
            log.error("orphan GC: mass-delete circuit opened for ns={} after {} confirmed orphans in one pass "
                            + "(limit {}); refusing deletes for this namespace until restart or operator override",
                    namespace, nsConfirmed.size(), maxConfirmedDeletesPerNamespacePerPass);
        }
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
                    return Verdict.ORPHAN; // authoritative: the owner recovered its log; the file is gone
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
