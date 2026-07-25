package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.FailureInjector;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * The per-process engine behind {@link NamespaceLogMetadataStore} (tech design §4.2). Each USER namespace's
 * file metadata lives in a {@link NamespaceMetadataLogRepository} — the ZK-backed strata-meta-file: a
 * metadata log whose bytes are stored by the {@link NamespaceMetadataFileStore} and whose physical
 * descriptors live in the consensus root.
 *
 * <p><b>System namespace routing (tech design §4.1–§4.2; recursion constraint §4.5).</b> The
 * metadata-log files themselves are stored as Strata
 * files in the reserved {@link #SYSTEM_NAMESPACE}; their own descriptors live directly in the ZK root
 * store (otherwise the log would recurse into itself). So every op for the system namespace is routed
 * straight to {@code root}, and — crucially — <b>without taking any namespace lock</b>: a user-namespace
 * mutation holds <em>that namespace's</em> lock while it writes its metadata-log file, which (with a
 * replicated-chunk file store) self-loops back into this engine for the system file; if that re-took a
 * namespace lock it would deadlock. Namespace-scoped file lookup therefore sends the system namespace
 * directly to the root without a repo lock, while user namespaces read their own repo under its lock.
 *
 * <p>A single engine is shared across {@link NamespaceLogMetadataStore} handles so multiple in-process
 * leaders observe one consistent log per namespace; cross-process single-writer is enforced by configured
 * ownership routing plus the consensus metadata-epoch and manifest-CAS fences. Each
 * {@link NamespaceMetadataLogRepository} owns a per-namespace {@link ReentrantLock}
 * (not {@code synchronized}) held across that namespace's durable append, so the blocking root/file-store
 * I/O inside a critical section never pins a virtual-thread carrier — and a slow append in one namespace
     * never head-of-line-blocks a mutation in another. Repo creation/recovery is guarded by that namespace's
     * leadership handle; best-effort metric reads ({@code loadedNamespaceCount}/{@code namespaceStats}/
     * {@code listNamespaces}) run lock-free over the {@link ConcurrentHashMap} of namespace handles.
 */
final class NamespaceLogBackend implements AutoCloseable, NamespaceLeadership {

    private static final Logger log = LoggerFactory.getLogger(NamespaceLogBackend.class);
    private static final NamespaceOwnership.AuthorityTerm STATIC_ASSIGNMENT_TERM =
            new NamespaceOwnership.AuthorityTerm(-1, -1);

    /** A destructive orphan-GC verdict bound atomically to the owner epoch that authorized it. */
    record OrphanConfirmation(boolean fileExists, boolean referencedByNode, long ownerEpoch) {
        OrphanConfirmation {
            if (ownerEpoch <= 0) {
                throw new IllegalArgumentException("ownerEpoch must be positive");
            }
            if (!fileExists && referencedByNode) {
                throw new IllegalArgumentException("a missing file cannot reference a replica");
            }
        }
    }

    /** Reserved namespace holding the metadata-log/snapshot system files; routed to the ZK root. */
    static final StrataNamespace SYSTEM_NAMESPACE = StrataNamespace.of("strata-meta");
    private static final long SYSTEM_NAMESPACE_ACTIVE_SINCE_MS = 1L;

    static boolean isSystem(StrataNamespace namespace) {
        return SYSTEM_NAMESPACE.equals(namespace);
    }

    private final MetadataStore root;
    private final NamespaceMetadataFileStore fileStore;
    private final boolean ownsRoot;
    private final NamespaceLogMetrics metrics = new NamespaceLogMetrics();
    private final ConcurrentHashMap<StrataNamespace, NamespaceLeadershipHandle> namespaces =
            new ConcurrentHashMap<>();
    // Only namespaces this node OWNS may be opened here — opening another owner's namespace would
    // republish its manifest and fence the real owner. Default ns->true for single-node / tests.
    private volatile Predicate<StrataNamespace> ownsNamespace = ns -> true;
    // Non-null in production sharded mode. The exact persisted assignment revision is then part of
    // repository authority; the Predicate-only seam remains for focused single-process unit tests.
    private volatile NamespaceOwnership namespaceOwnership;
    private volatile AutoCloseable ownershipListener;
    private volatile boolean closed;
    private volatile Thread compactionThread;

    private static final class NamespaceLeadershipHandle {
        private final StrataNamespace namespace;
        private final ReentrantLock openLock = new ReentrantLock();
        private final ReentrantLock reconcileLock = new ReentrantLock();
        private volatile NamespaceLeaderState state = NamespaceLeaderState.STANDBY;
        private volatile NamespaceMetadataLogRepository repo;
        private volatile long metadataEpoch;
        private volatile NamespaceOwnership.AuthorityTerm assignmentTerm = STATIC_ASSIGNMENT_TERM;
        private volatile long activeSinceMs;
        private boolean reacquirePending;

        private NamespaceLeadershipHandle(StrataNamespace namespace) {
            this.namespace = namespace;
        }

        private NamespaceMetadataLogRepository activeRepo() {
            NamespaceMetadataLogRepository r = repo;
            return state == NamespaceLeaderState.ACTIVE && r != null ? r : null;
        }

        private void recovering(long epoch, NamespaceOwnership.AuthorityTerm term) {
            metadataEpoch = epoch;
            assignmentTerm = term;
            activeSinceMs = 0;
            state = NamespaceLeaderState.RECOVERING;
        }

        private void activate(NamespaceMetadataLogRepository opened, NamespaceOwnership.AuthorityTerm term) {
            repo = opened;
            metadataEpoch = opened.metadataEpoch();
            assignmentTerm = term;
            activeSinceMs = System.currentTimeMillis();
            state = NamespaceLeaderState.ACTIVE;
        }

        private void fenceIfCurrent(NamespaceMetadataLogRepository stale) {
            if (repo == stale) {
                repo = null;
                activeSinceMs = 0;
                state = NamespaceLeaderState.FENCED;
            }
        }

        private void restore(NamespaceMetadataLogRepository stale, NamespaceOwnership.AuthorityTerm term) {
            repo = stale;
            if (stale != null) {
                metadataEpoch = stale.metadataEpoch();
                assignmentTerm = term;
                activeSinceMs = System.currentTimeMillis();
                state = NamespaceLeaderState.ACTIVE;
            } else {
                metadataEpoch = 0;
                assignmentTerm = STATIC_ASSIGNMENT_TERM;
                activeSinceMs = 0;
                state = NamespaceLeaderState.STANDBY;
            }
        }
    }

    NamespaceLogBackend(MetadataStore root, NamespaceMetadataFileStore fileStore, boolean ownsRoot) {
        this.root = root;
        this.fileStore = fileStore;
        this.ownsRoot = ownsRoot;
    }

    MetadataStore root() {
        return root;
    }

    /** Process-wide metadata-log counters (append/compaction/recovery), for Prometheus export. */
    NamespaceLogMetrics metrics() {
        return metrics;
    }

    /** Namespaces with a live owner repository on this instance — the sharding load this node carries. */
    int loadedNamespaceCount() {
        int count = 0;
        for (NamespaceLeadershipHandle handle : namespaces.values()) {
            if (activeRepoForCurrentAssignment(handle) != null) {
                count++;
            }
        }
        return count; // best-effort gauge over the ConcurrentHashMap (lock-free)
    }

    /** Per-namespace stats for the namespaces this node owns: {@code namespace -> [liveFiles, openLogBytes]}. */
    Map<StrataNamespace, long[]> namespaceStats() {
        // Best-effort gauges over the ConcurrentHashMap; each per-namespace read is taken under that repo's
        // lock — liveFileCount() iterates a plain HashMap that append() mutates under the same lock, so a
        // lock-free read would race into a ConcurrentModificationException.
        Map<StrataNamespace, long[]> out = new HashMap<>(namespaces.size());
        for (Map.Entry<StrataNamespace, NamespaceLeadershipHandle> e : namespaces.entrySet()) {
            NamespaceMetadataLogRepository repo = activeRepoForCurrentAssignment(e.getValue());
            if (repo == null) {
                continue;
            }
            repo.lock();
            try {
                out.put(e.getKey(), new long[]{repo.liveFileCount(), repo.openLogBytes()});
            } finally {
                repo.unlock();
            }
        }
        return out;
    }

    /** Restricts eager recovery to the namespaces this node owns (wired from Controller). */
    void setOwnership(Predicate<StrataNamespace> ownsNamespace) {
        this.ownsNamespace = ownsNamespace;
        this.namespaceOwnership = null;
        closeOwnershipListener();
    }

    /**
     * Binds repository authority to the exact persisted namespace-assignment revision. A session suspension
     * or assignment change fences the matching local handle immediately; a delayed LOST callback for an old
     * revision cannot fence a repository opened after a later legitimate acquisition.
     */
    void setOwnership(NamespaceOwnership ownership) {
        this.namespaceOwnership = java.util.Objects.requireNonNull(ownership, "ownership");
        this.ownsNamespace = ownership::isOwner;
        closeOwnershipListener();
        this.ownershipListener = ownership.addListener(new NamespaceOwnership.Listener() {
            @Override
            public void onLost(StrataNamespace namespace, NamespaceOwnership.AuthorityTerm term) {
                fenceLostOwnership(namespace, term);
            }
        });
    }

    private void fenceLostOwnership(StrataNamespace namespace, NamespaceOwnership.AuthorityTerm lostTerm) {
        NamespaceLeadershipHandle handle = namespaces.get(namespace);
        if (handle == null) {
            return;
        }
        handle.openLock.lock();
        try {
            NamespaceMetadataLogRepository active = handle.activeRepo();
            if (handle.assignmentTerm.equals(lostTerm)) {
                handle.reacquirePending = false;
                if (active != null) {
                    handle.fenceIfCurrent(active);
                }
                log.info("namespace {} local repository fenced after assignment term {} was lost",
                        namespace, lostTerm);
            }
        } finally {
            handle.openLock.unlock();
        }
    }

    private void closeOwnershipListener() {
        AutoCloseable listener = ownershipListener;
        ownershipListener = null;
        if (listener == null) {
            return;
        }
        try {
            listener.close();
        } catch (Exception e) {
            log.warn("failed to close namespace ownership listener", e);
        }
    }

    // Safety delay (tech design §4.2 / issue #8): a superseded metadata-log generation is retained for this
    // many ms after it is superseded before the sweep reclaims it — a rollback margin against a bad newest
    // generation. 0 disables the window (reap as soon as the sweep sees the orphan). Set from Controller env.
    private volatile long logRetentionMs;

    /** Configures the superseded-generation retention window (STRATA_CONTROLLER_LOG_RETENTION_MS). */
    void setLogRetentionMs(long retentionMs) {
        this.logRetentionMs = Math.max(0, retentionMs);
    }

    /**
     * Starts the periodic open-log compaction sweep (tech design §4.2 bounded-storage maintenance). Without
     * it, a per-namespace repo compacts only at open/failover ({@link NamespaceMetadataLogRepository#open}),
     * so a stable long-lived owner's open log grows unbounded between failovers. The sweep snapshot+rolls
     * every owned namespace whose open log has passed {@code thresholdBytes}, bounding steady-state storage
     * by snapshot cadence rather than by total historical mutations. The same thread also runs the
     * SYSTEM-namespace retention sweep ({@link #gcOrphanedSystemFiles}) each tick when {@code runOrphanGc} is
     * set: it reclaims superseded snapshot/log generations once their retention window has elapsed and reaps
     * snapshot/log files left behind by a crash between file-create and manifest CAS. It runs every tick (not
     * throttled) because, with the inline delete removed (issue #8), it is the sole reclamation path for
     * superseded generations, not just a rare crash backstop.
     * No-op if already started, if either compaction bound is non-positive (compaction disabled — the
     * default for unit tests), or if already closed.
     */
    synchronized void startBackgroundCompaction(long thresholdBytes, long intervalMs, boolean runOrphanGc) {
        if (compactionThread != null || thresholdBytes <= 0 || intervalMs <= 0 || closed) {
            return;
        }
        compactionThread = Thread.ofVirtual().name("meta-log-compaction")
                .start(() -> compactionLoop(thresholdBytes, intervalMs, runOrphanGc));
    }

    private void compactionLoop(long thresholdBytes, long intervalMs, boolean runOrphanGc) {
        while (!closed) {
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            compactOversizedRepos(thresholdBytes);
            if (runOrphanGc) {
                try {
                    gcOrphanedSystemFiles();
                } catch (Exception e) {
                    if (!closed) {
                        log.warn("metadata system-file retention sweep failed; will retry next sweep", e);
                    }
                }
            }
        }
    }

    /** A superseded/orphaned system file the sweep may reclaim, with the generation it belongs to. */
    private record ReapCandidate(FileId id, StrataNamespace namespace, long generation, StrataPath path) {}

    /**
     * Reclaims metadata SYSTEM-namespace files (snapshot/log) that are no longer referenced by a published
     * manifest (tech design §4.2, issue #8): a generation superseded by a later compaction, plus snapshot/log
     * files orphaned by a crash between writing a new generation's files and the manifest CAS. Since the
     * inline delete was removed from {@code publishCompacted}, this is the SOLE reclamation path for
     * superseded generations — not just a crash backstop.
     *
     * <p><b>What is reapable — safe BY CONSTRUCTION on generation.</b> {@code publishCompacted} writes the
     * files for generation {@code G+1} BEFORE publishing the manifest at {@code G+1}, so an in-flight
     * (not-yet-published) file is always at a generation STRICTLY GREATER than its namespace's currently
     * published generation. A file is therefore a candidate only when it is (a) referenced by no current
     * manifest AND (b) its path-encoded generation is {@code <=} the published generation — which proves a
     * higher generation has already been published, so it can never become referenced. A file whose path
     * cannot be parsed, or whose namespace has no published manifest yet, is KEPT (fail-safe).
     *
     * <p><b>When a candidate is reclaimed — the retention window (issue #8).</b> A just-superseded generation
     * is retained for {@code retentionMs} as a rollback margin against a bad newest generation, rather than
     * reclaimed the instant the new manifest is durable. The window is timed off the SUCCESSOR generation's
     * {@code createdAtMs} — the durable instant the candidate was superseded (generation {@code G} was
     * superseded when generation {@code G+1} was created/published). That instant lives in the successor's
     * {@link Records.FileRecord} (consensus root), so the window is honored ACROSS FAILOVER with no in-memory
     * queue: a new owner recomputes it from durable state. If the successor's files are already gone, the
     * candidate was superseded long ago and is reclaimed immediately. {@code retentionMs <= 0} disables the
     * window (reap as soon as a candidate is seen — crash orphans always satisfy this since their successor
     * generation typically does not exist). Returns the number reaped. Package-private + the
     * ({@code nowMs}, {@code retentionMs}) overload so a test can drive a sweep deterministically.
     */
    int gcOrphanedSystemFiles() throws Exception {
        return gcOrphanedSystemFiles(System.currentTimeMillis(), logRetentionMs);
    }

    int gcOrphanedSystemFiles(long nowMs, long retentionMs) throws Exception {
        Map<StrataNamespace, Long> publishedGen = new HashMap<>();
        Set<FileId> referenced = new LinkedHashSet<>();
        // Enumerate by METADATA namespaces (every namespace with a manifest), NOT listNamespaces() (which
        // only reports namespaces that still have live USER files): a namespace whose user files were all
        // deleted keeps its manifest + system files, so missing it here would wrongly reap a live snapshot/log.
        for (StrataNamespace ns : root.listAssignedNamespaces()) {
            root.getNamespaceManifest(ns).map(MetadataStore.Versioned::value).ifPresent(m -> {
                publishedGen.put(ns, m.generation());
                m.snapshotFileId().ifPresent(referenced::add);
                m.logFileId().ifPresent(referenced::add);
                m.previous().ifPresent(previous -> {
                    previous.snapshotFileId().ifPresent(referenced::add);
                    previous.logFileId().ifPresent(referenced::add);
                });
            });
        }
        // First pass: record each generation's creation time (keyed (namespace, generation)) and collect the
        // reap candidates. A candidate's SUPERSESSION instant is the creation time of its successor generation,
        // so we must observe creation times for live (referenced) files too — hence we scan every file here.
        Map<StrataNamespace, Map<Long, Long>> genCreatedAt = new HashMap<>();
        List<ReapCandidate> candidates = new ArrayList<>();
        for (FileId id : root.listFileIds(SYSTEM_NAMESPACE)) {
            Optional<MetadataStore.Versioned<Records.FileRecord>> rec = root.getFile(SYSTEM_NAMESPACE, id);
            if (rec.isEmpty()) {
                continue; // already gone
            }
            Records.FileRecord file = rec.get().value();
            StrataSystemMetadataFileStore.SystemFileCoord coord =
                    StrataSystemMetadataFileStore.parseSystemFilePath(file.path());
            if (coord == null) {
                continue; // unrecognized path — cannot judge its generation, so never reap (fail-safe)
            }
            // Snapshot and log of one generation are created within ms of each other; min() is a stable
            // representative of when that generation became live (== when the prior generation was superseded).
            genCreatedAt.computeIfAbsent(coord.namespace(), k -> new HashMap<>())
                    .merge(coord.generation(), file.createdAtMs(), Math::min);
            if (referenced.contains(id)) {
                continue; // referenced by a currently-published manifest — live
            }
            if (file.state() == FileState.DELETING || file.state() == FileState.DELETED) {
                continue; // a prior sweep already started reclaiming it
            }
            Long published = publishedGen.get(coord.namespace());
            if (published == null || coord.generation() > published) {
                continue; // no manifest yet, or a not-yet-published in-flight/future generation — keep
            }
            candidates.add(new ReapCandidate(id, coord.namespace(), coord.generation(), file.path()));
        }
        int reaped = 0;
        for (ReapCandidate c : candidates) {
            Long supersededAt = genCreatedAt.getOrDefault(c.namespace(), Map.of()).get(c.generation() + 1);
            if (retentionMs > 0 && supersededAt != null && nowMs - supersededAt < retentionMs) {
                continue; // inside the retention window — keep this superseded generation as a rollback margin
            }
            try {
                fileStore.deleteFile(c.id());
                reaped++;
                log.info("system-file GC: reclaimed superseded/orphaned metadata file {} (ns={}, gen={})",
                        c.id(), c.namespace(), c.generation());
            } catch (Exception e) {
                // isolate a poison file — the rest of the sweep proceeds; this one retries next pass
                log.warn("system-file GC: failed to delete {} ({}); will retry next sweep",
                        c.id(), c.path(), e);
            }
        }
        return reaped;
    }

    /**
     * Compacts every owned repository whose open log has grown past {@code thresholdBytes}. Compaction is
     * non-blocking ({@link NamespaceMetadataLogRepository#compact} — copy-on-write, tech design §4.2): it manages
     * its own locking so the snapshot encode + write happen off the namespace's mutation lock, and a
     * namespace keeps accepting writes for the duration of its own compaction. {@code compact()} skips a
     * repo (returns {@code false}) that is under threshold or already compacting, so this sweep does NOT
     * hold the lock across the call. A fenced manifest CAS means another node now owns the namespace, so
     * the stale repo is evicted and the next op re-acquires under a fresh epoch rather than fencing forever.
     * Returns the number of namespaces compacted. Package-private so a test can drive a sweep deterministically.
     */
    int compactOversizedRepos(long thresholdBytes) {
        int compacted = 0;
        for (Map.Entry<StrataNamespace, NamespaceLeadershipHandle> e : namespaces.entrySet()) {
            NamespaceLeadershipHandle handle = e.getValue();
            NamespaceMetadataLogRepository repo = handle.activeRepo();
            if (repo == null) {
                continue;
            }
            try {
                if (!validateLocalAuthority(e.getKey(), handle.assignmentTerm)) {
                    handle.fenceIfCurrent(repo);
                    continue;
                }
                if (repo.compact(thresholdBytes)) {
                    compacted++;
                }
            } catch (IllegalStateException fenced) {
                // The only IllegalStateException compact() raises in steady state is a lost manifest CAS —
                // another node owns this namespace now; drop the stale repo so the next op re-acquires.
                e.getValue().fenceIfCurrent(repo);
                if (!closed) {
                    log.warn("namespace {} open-log compaction fenced — evicting stale repo", e.getKey(), fenced);
                }
            } catch (Exception ex) {
                // transient (file store / I/O), or the backend was closed mid-sweep — leave the repo and
                // retry on the next sweep; suppress the warning once closed (shutdown is not a failure).
                if (!closed) {
                    log.warn("namespace {} open-log compaction failed; will retry next sweep", e.getKey(), ex);
                }
            }
        }
        return compacted;
    }

    private NamespaceLeadershipHandle namespaceHandle(StrataNamespace namespace) {
        return namespaces.computeIfAbsent(namespace, NamespaceLeadershipHandle::new);
    }

    private NamespaceMetadataLogRepository repo(StrataNamespace namespace) throws Exception {
        NamespaceOwnership.AuthorityTerm currentTerm = requireCurrentLocalTerm(namespace);
        NamespaceLeadershipHandle handle = namespaceHandle(namespace);
        NamespaceMetadataLogRepository r = handle.activeRepo();   // fast path, lock-free
        if (r != null && handle.assignmentTerm.equals(currentTerm)) {
            return r;
        }
        handle.openLock.lock();
        try {
            currentTerm = requireCurrentLocalTerm(namespace);
            r = handle.activeRepo();
            if (r != null && handle.assignmentTerm.equals(currentTerm)) {
                return r;
            }
            boolean assignmentChanged = r != null;
            if (assignmentChanged) {
                // LOST delivery is deliberately asynchronous. The cached assignment term is the correctness
                // gate: never expose a repository recovered under an earlier owner term while waiting for its
                // callback to acquire this lock.
                handle.fenceIfCurrent(r);
                handle.reacquirePending = false;
            }
            // Cold open = this process has no cached repository for the namespace. This includes initial
            // load and process restart, so ownerChanges is only an approximation of an ownership handoff.
            boolean retryingReacquire = handle.reacquirePending;
            if (retryingReacquire) {
                metrics.recordReacquire(namespace);
            }
            NamespaceMetadataLogRepository opened =
                    openLocked(handle, assignmentChanged || !retryingReacquire);
            handle.reacquirePending = false;
            return opened;
        } finally {
            handle.openLock.unlock();
        }
    }

    /**
     * Opens (and recovers) this node's repository for {@code namespace} if not already cached; caller holds
     * the namespace handle's open lock. {@code countAsAcquisition} distinguishes a cold open (counted in
     * {@code ownerChanges}, including initial load/restart) from the fence-driven in-place {@link #reacquire} — an epoch bump on
     * a namespace this node already owns, which must NOT register as an owner change. If another thread won
     * the open race the existing repo is returned and nothing is counted.
     */
    private NamespaceMetadataLogRepository openLocked(NamespaceLeadershipHandle handle, boolean countAsAcquisition)
            throws Exception {
        StrataNamespace namespace = handle.namespace;
        requireOwnedNamespace(namespace);
        NamespaceMetadataLogRepository r = handle.activeRepo();
        if (r != null) {
            return r;
        }
        NamespaceOwnership.AuthorityTerm assignmentTerm = requireAuthoritativeLocalTerm(namespace);
        long epoch = root.allocateMetadataEpoch();
        handle.recovering(epoch, assignmentTerm);
        try {
            r = NamespaceMetadataLogRepository.open(namespace, fileStore, root, epoch, metrics);
            assertRecoveredBeforeActive(handle, r);
            if (!validateLocalAuthority(namespace, assignmentTerm)) {
                throw notLocalAuthority(namespace, assignmentTerm);
            }
            handle.activate(r, assignmentTerm);   // publish only after recovery + assignment revalidation
            if (countAsAcquisition) {
                metrics.recordOwnerAcquired(namespace);
            }
            return r;
        } catch (Exception e) {
            if (handle.repo == null) {
                handle.restore(null, STATIC_ASSIGNMENT_TERM);
            }
            throw e;
        }
    }

    private static void assertRecoveredBeforeActive(NamespaceLeadershipHandle handle,
                                                    NamespaceMetadataLogRepository repo) {
        if (repo.metadataEpoch() != handle.metadataEpoch) {
            throw new IllegalStateException("namespace " + handle.namespace
                    + " recovered epoch " + repo.metadataEpoch() + " but handle expected " + handle.metadataEpoch);
        }
        if (repo.state() == null) {
            throw new IllegalStateException("namespace " + handle.namespace + " recovered without metadata state");
        }
        if (repo.openLogBytes() < 0) {
            throw new IllegalStateException("namespace " + handle.namespace + " recovered with a negative open log");
        }
    }

    @FunctionalInterface
    private interface RepoTxn<T> {
        T run(NamespaceMetadataLogRepository repo) throws Exception;
    }

    /**
     * Runs a mutating transaction against this node's repository for {@code namespace}, re-acquiring the
     * meta-log and retrying ONCE if the append is fenced ({@link ErrorCode#FENCED_EPOCH}) because another
     * opener republished the manifest at a higher epoch, or if a previous ambiguous append failure poisoned
     * the cached repo.
     *
     * <p>Without this, a controller that briefly co-owned a namespace during a membership settle keeps a
     * cached repository at the stale epoch, so every meta-log append fences forever — the file never
     * finalizes and {@code ownerRepairPass} skips it every scan (the observed permanent wedge). The retry
     * evicts the stale repo and re-opens it ({@link #reacquire}), which allocates a fresh epoch and
     * recovers the latest durable state, then replays the transaction against it. Callers reach this only
     * for namespaces this node owns (owner-routed creates; ownsNamespace-gated repair), so re-acquiring
     * reclaims the meta-log for the rightful owner rather than stealing it; the retry is bounded to one
     * attempt, so a genuine ownership disagreement surfaces as the fence rather than looping.
     */
    private <T> T withRepoReacquiringOnFence(StrataNamespace namespace, RepoTxn<T> txn) throws Exception {
        NamespaceMetadataLogRepository repo = repo(namespace);
        try {
            return runLockedMutation(repo, txn);
        } catch (PoisonedMetadataLogRepositoryException e) {
            log.warn("namespace {} meta-log repository poisoned after append failure ({}) — re-acquiring and retrying once",
                    namespace, causeSummary(e.getCause()));
            metrics.recordReacquire(namespace);
        } catch (Exception e) {
            if (!isFencedEpoch(e)) {
                throw e;
            }
            log.warn("namespace {} meta-log append fenced (stale epoch) — re-acquiring and retrying once",
                    namespace, e);
            metrics.recordReacquire(namespace);
        }
        try {
            return runLockedMutation(reacquire(namespace, repo), txn);
        } catch (PoisonedMetadataLogRepositoryException e) {
            throw poisonFailure(e);
        }
    }

    private void requireOwnedNamespace(StrataNamespace namespace) {
        if (!ownsNamespace.test(namespace)) {
            throw new ScpException(ErrorCode.NOT_LEADER,
                    "namespace " + namespace + " is not owned by this controller");
        }
    }

    /** Returns the current locally-owned cached assignment term without a consensus round trip. */
    private NamespaceOwnership.AuthorityTerm requireCurrentLocalTerm(StrataNamespace namespace) {
        requireOwnedNamespace(namespace);
        NamespaceOwnership ownership = namespaceOwnership;
        if (ownership == null) {
            return STATIC_ASSIGNMENT_TERM;
        }
        return ownership.cachedLocalAuthorityTerm(namespace).orElseThrow(
                () -> new ScpException(ErrorCode.NOT_LEADER,
                        "namespace " + namespace + " has no current local assignment term"));
    }

    /** Returns the exact persisted assignment term owned by this process, or fails closed. */
    private NamespaceOwnership.AuthorityTerm requireAuthoritativeLocalTerm(
            StrataNamespace namespace) throws Exception {
        NamespaceOwnership.AuthorityTerm term = requireCurrentLocalTerm(namespace);
        NamespaceOwnership ownership = namespaceOwnership;
        if (ownership == null) {
            return STATIC_ASSIGNMENT_TERM;
        }
        if (!ownership.validateLocalAuthority(namespace, term)) {
            throw notLocalAuthority(namespace, term);
        }
        return term;
    }

    /**
     * Returns an ACTIVE repository only when its opening term is still the locally-owned cached assignment
     * term. This is intentionally local and lock-free so repair scheduling and metrics do not add ZooKeeper
     * traffic, while an actual reopen still goes through authoritative validation in {@link #openLocked}.
     */
    private NamespaceMetadataLogRepository activeRepoForCurrentAssignment(
            NamespaceLeadershipHandle handle) {
        NamespaceMetadataLogRepository active = handle.activeRepo();
        if (active == null) {
            return null;
        }
        NamespaceOwnership ownership = namespaceOwnership;
        if (ownership == null) {
            return ownsNamespace.test(handle.namespace) ? active : null;
        }
        return ownership.cachedLocalAuthorityTerm(handle.namespace)
                .filter(handle.assignmentTerm::equals)
                .map(ignored -> active)
                .orElse(null);
    }

    private boolean validateLocalAuthority(
            StrataNamespace namespace,
            NamespaceOwnership.AuthorityTerm expectedTerm) throws Exception {
        NamespaceOwnership ownership = namespaceOwnership;
        if (ownership == null) {
            requireOwnedNamespace(namespace);
            return expectedTerm.equals(STATIC_ASSIGNMENT_TERM);
        }
        return ownership.validateLocalAuthority(namespace, expectedTerm);
    }

    private static ScpException notLocalAuthority(
            StrataNamespace namespace,
            NamespaceOwnership.AuthorityTerm term) {
        return new ScpException(ErrorCode.NOT_LEADER,
                "namespace " + namespace + " assignment term " + term
                        + " is no longer owned by this controller");
    }

    private static <T> T runLocked(NamespaceMetadataLogRepository repo, RepoTxn<T> txn) throws Exception {
        repo.lock();
        try {
            return txn.run(repo);
        } finally {
            repo.unlock();
        }
    }

    private static <T> T runLockedMutation(NamespaceMetadataLogRepository repo, RepoTxn<T> txn) throws Exception {
        return runLocked(repo, locked -> {
            if (locked.poisoned()) {
                throw new PoisonedMetadataLogRepositoryException(locked.poisonCause());
            }
            return txn.run(locked);
        });
    }

    /**
     * Runs a read against the already-active repository only after an authoritative consensus-root read
     * proves that repository's exact published manifest and znode version are still current. This path is
     * intentionally separate from {@link #repo} / {@link #withRepoReacquiringOnFence}: a stale owner asking
     * to authorize destructive work must fail closed, never reclaim the namespace by allocating a newer
     * epoch. The consensus round trip deliberately happens before taking either local lock; the open lock
     * then keeps the selected handle stable while the repo lock binds manifest validation, metadata state,
     * verdict, and returned epoch to one repository image. A local publish/replacement during the fetch is
     * detected by the exact manifest/version and active-repo checks and fails closed.
     */
    private <T> T withAuthoritativeRepo(StrataNamespace namespace, RepoTxn<T> read) throws Exception {
        requireOwnedNamespace(namespace);
        NamespaceLeadershipHandle handle = namespaces.get(namespace);
        if (handle == null) {
            throw new ScpException(ErrorCode.FENCED_EPOCH,
                    "namespace " + namespace + " has no active local authority");
        }

        NamespaceMetadataLogRepository selected = handle.activeRepo();
        if (selected == null) {
            throw new ScpException(ErrorCode.FENCED_EPOCH,
                    "namespace " + namespace + " local authority is " + handle.state,
                    handle.metadataEpoch);
        }
        NamespaceOwnership.AuthorityTerm selectedAssignmentTerm = handle.assignmentTerm;
        if (!validateLocalAuthority(namespace, selectedAssignmentTerm)) {
            handle.fenceIfCurrent(selected);
            throw notLocalAuthority(namespace, selectedAssignmentTerm);
        }
        Optional<MetadataStore.Versioned<Records.NamespaceManifest>> current =
                root.getNamespaceManifestAuthoritative(namespace);
        if (current.isEmpty()) {
            throw new ScpException(ErrorCode.FENCED_EPOCH,
                    "namespace " + namespace + " has no authoritative manifest");
        }

        handle.openLock.lock();
        try {
            NamespaceMetadataLogRepository active = handle.activeRepo();
            if (active == null || active != selected) {
                throw new ScpException(ErrorCode.FENCED_EPOCH,
                        "namespace " + namespace + " local authority changed during validation",
                        handle.metadataEpoch);
            }
            if (!validateLocalAuthority(namespace, selectedAssignmentTerm)) {
                handle.fenceIfCurrent(active);
                throw notLocalAuthority(namespace, selectedAssignmentTerm);
            }
            return runLocked(active, repo -> {
                if (repo.poisoned()) {
                    throw new ScpException(ErrorCode.INTERNAL,
                            "namespace " + namespace + " metadata state is uncertain after an append failure",
                            repo.poisonCause());
                }
                MetadataStore.Versioned<Records.NamespaceManifest> authoritative = current.get();
                if (!repo.matchesPublishedManifest(authoritative) || handle.activeRepo() != repo) {
                    throw new ScpException(ErrorCode.FENCED_EPOCH,
                            "namespace " + namespace + " local manifest is no longer authoritative",
                            authoritative.value().metadataEpoch());
                }
                return read.run(repo);
            });
        } finally {
            handle.openLock.unlock();
        }
    }

    /**
     * Authoritatively confirms whether {@code chunkId} still belongs on {@code nodeId}. Missing files are
     * represented in the response rather than as FILE_NOT_FOUND so only this consensus-validated path can
     * produce a destructive orphan verdict.
     */
    OrphanConfirmation confirmOrphan(StrataNamespace namespace, ChunkId chunkId, int nodeId) throws Exception {
        if (isSystem(namespace)) {
            throw new IllegalArgumentException("system namespace orphan confirms use the root leader path");
        }
        return withAuthoritativeRepo(namespace, repo -> {
            Optional<Records.FileRecord> file = repo.state().file(chunkId.fileId());
            boolean referenced = file.stream().flatMap(f -> f.chunks().stream())
                    .anyMatch(chunk -> chunk.index() == chunkId.index() && chunk.replicas().contains(nodeId));
            return new OrphanConfirmation(file.isPresent(), referenced, repo.metadataEpoch());
        });
    }

    private static Exception poisonFailure(PoisonedMetadataLogRepositoryException e) {
        Throwable cause = e.getCause();
        if (cause instanceof Exception ex) {
            return ex;
        }
        return new ScpException(ErrorCode.INTERNAL,
                "metadata-log repository poisoned by append failure", e);
    }

    private static String causeSummary(Throwable cause) {
        return cause == null ? "unknown" : cause.toString();
    }

    private static final class PoisonedMetadataLogRepositoryException extends Exception {
        private PoisonedMetadataLogRepositoryException(Throwable cause) {
            super("metadata-log repository poisoned by append failure", cause, false, false);
        }
    }

    /**
     * Evicts {@code stale} (the repo we were fenced on) and re-opens this namespace at a fresh epoch.
     * The conditional remove lets a concurrent re-acquire win: if another thread already replaced the
     * cache entry, its fresh repo is reused rather than churning yet another epoch.
     */
    private NamespaceMetadataLogRepository reacquire(StrataNamespace namespace,
            NamespaceMetadataLogRepository stale) throws Exception {
        NamespaceLeadershipHandle handle = namespaceHandle(namespace);
        handle.openLock.lock();
        try {
            NamespaceMetadataLogRepository current = handle.activeRepo();
            if (current != null && current != stale) {
                return current; // another thread already re-acquired
            }
            NamespaceOwnership.AuthorityTerm staleAssignmentTerm = handle.assignmentTerm;
            if (!validateLocalAuthority(namespace, staleAssignmentTerm)) {
                handle.fenceIfCurrent(stale);
                throw notLocalAuthority(namespace, staleAssignmentTerm);
            }
            handle.reacquirePending = true;
            handle.fenceIfCurrent(stale);
            try {
                // In-place epoch bump on a namespace this node already owns — NOT an ownership handoff, so it
                // must not increment ownerChanges (recordReacquire already counts this churn separately).
                NamespaceMetadataLogRepository opened = openLocked(handle, false);
                handle.reacquirePending = false;
                return opened;
            } catch (Exception e) {
                // Never resurrect the stale repository. openLocked may already have published a new manifest,
                // partially installed a higher data-node floor, or discovered that this assignment revision
                // was lost. A later request may retry from FENCED/STANDBY, but old authority cannot become
                // ACTIVE again merely because recovery failed.
                throw e;
            }
        } finally {
            handle.openLock.unlock();
        }
    }

    /** True if {@code FENCED_EPOCH} appears anywhere in the cause chain — the append wraps it in an
     *  {@code ExecutionException} on the real Strata-file store, raw on the in-memory test store. */
    private static boolean isFencedEpoch(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof ScpException se && se.code() == ErrorCode.FENCED_EPOCH) {
                return true;
            }
        }
        return false;
    }

    void createFile(Records.FileRecord record) throws Exception {
        if (isSystem(record.namespace())) {
            root.createFile(record); // metadata-log system file — lives in the ZK root (lock-free)
            return;
        }
        withRepoReacquiringOnFence(record.namespace(), repo -> {
            NamespaceMetadataState state = repo.state();
            if (state.hasTombstone(record.fileId()) || state.file(record.fileId()).isPresent()) {
                throw new KeeperException.NodeExistsException("file " + record.fileId());
            }
            if (state.resolvePath(record.path()).isPresent()) {
                throw new KeeperException.NodeExistsException(
                        "path " + record.namespace() + ":" + record.path());
            }
            repo.append(new MetadataLogRecord.FileCreated(record.fileId(), record.namespace(),
                    record.path(), record.replicationFactor(), record.ackQuorum(), record.fsyncOnAck(),
                    record.createdAtMs(), record.createOpMsb(), record.createOpLsb()));
            Records.FileRecord created = state.file(record.fileId()).orElseThrow();
            for (MetadataLogRecord r : MetadataLogDiff.diff(created, record)) {
                repo.append(r);
            }
            return null;
        });
    }

    /**
     * Owner-assigned create: the server assigns the file id, with opId-keyed idempotency.
     *
     * <p>For the system namespace, id is allocated via the ZK root's {@code nextSystemFileId()} counter
     * (low-volume; lives in the consensus root). For user namespaces, id is read from the owner's
     * {@link NamespaceMetadataState#peekNextFileId()} (no-advance peek; {@code apply(FileCreated)} inside
     * {@code repo.append} advances the counter, so a crash before the append leaves nextFileId unchanged
     * in durable state and the successor re-issues the same id safely — no reuse of a durable file id).
     *
     * <p>Idempotency: if a live (non-DELETING) file already carries the same {@code (namespace, opId)},
     * the existing file id is returned immediately — {@code sameCreateRequest} guards that the path and
     * policy match; mismatch is PRECONDITION_FAILED. A swept-tombstone opId is treated as fresh.
     *
     * @return the server-assigned (or previously assigned) {@link FileId}
     * @throws io.strata.common.ScpException with PRECONDITION_FAILED if the opId is already in use
     *         for a different path or policy, or if the path is bound to a different file
     */
    FileId createFileOwnerAssigned(Records.FileRecord template) throws Exception {
        if (isSystem(template.namespace())) {
            // System files live in the ZK root, keyed for idempotency on PATH (each system path carries a
            // unique UUID leaf, so the path identifies the logical create). A lost-response retry re-sends the
            // identical path, so if it is already bound, return the committed id instead of allocating a fresh
            // system-file id and colliding on the existing path marker (which would fail the retry rather than
            // returning the already-committed id).
            Optional<FileId> bound = root.resolvePath(template.namespace(), template.path());
            if (bound.isPresent()) {
                return bound.get();
            }
            long sysId = root.nextSystemFileId();
            Records.FileRecord sysRecord = new Records.FileRecord(
                    FileId.of(sysId), template.namespace(), template.path(),
                    template.replicationFactor(), template.ackQuorum(), template.fsyncOnAck(),
                    template.state(), template.createdAtMs(), template.chunks(),
                    template.createOpMsb(), template.createOpLsb());
            try {
                root.createFile(sysRecord);
            } catch (KeeperException.NodeExistsException e) {
                // Raced with a concurrent/retried create that bound the path between resolve and create:
                // return the now-committed id rather than failing the retry (one system-file id is burned).
                Optional<FileId> raced = root.resolvePath(template.namespace(), template.path());
                if (raced.isPresent()) {
                    return raced.get();
                }
                throw e;
            }
            return sysRecord.fileId();
        }
        return withRepoReacquiringOnFence(template.namespace(), repo -> {
            NamespaceMetadataState state = repo.state();
            // --- opId-keyed idempotency (O(1) index lookup) ---
            Optional<FileId> existing = state.fileIdForOpId(template.createOpMsb(), template.createOpLsb());
            if (existing.isPresent()) {
                FileId existingId = existing.get();
                Optional<Records.FileRecord> existingFile = state.file(existingId);
                if (existingFile.isPresent()) {
                    Records.FileRecord f = existingFile.get();
                    if (f.state() == FileState.DELETING) {
                        // The file has been deleted; this opId is consumed — reject the replay.
                        throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                                "create opId already consumed by a deleted file: " + existingId);
                    }
                    if (!f.path().equals(template.path()) || !samePolicyAs(f, template)) {
                        throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                                "create opId already used with a different path or policy: " + existingId);
                    }
                    return existingId; // idempotent: same opId, same request
                }
                // Tombstone present but not yet swept — the opId is consumed; reject.
                throw new ScpException(ErrorCode.PRECONDITION_FAILED,
                        "create opId already consumed by a deleted file");
            }
            // --- path conflict check ---
            if (state.resolvePath(template.path()).isPresent()) {
                throw new KeeperException.NodeExistsException(
                        "path " + template.namespace() + ":" + template.path());
            }
            // --- assign id and append ---
            // Peek (no advance): nextFileId is advanced only by apply(FileCreated) inside repo.append(),
            // so a crash between peek and append leaves nextFileId unchanged in durable state and the
            // successor re-issues the same id — which is safe because no FileCreated record existed.
            // See NamespaceFileIdRecoveryInjectionTest window (a).
            FileId id = state.peekNextFileId();
            FailureInjector.point("meta.log.afterAssignBeforeAppend");
            repo.append(new MetadataLogRecord.FileCreated(id, template.namespace(),
                    template.path(), template.replicationFactor(), template.ackQuorum(), template.fsyncOnAck(),
                    template.createdAtMs(), template.createOpMsb(), template.createOpLsb()));
            return id;
        });
    }

    /** True if the existing file's write policy matches the template. */
    private static boolean samePolicyAs(Records.FileRecord existing, Records.FileRecord template) {
        return existing.replicationFactor() == template.replicationFactor()
                && existing.ackQuorum() == template.ackQuorum()
                && existing.fsyncOnAck() == template.fsyncOnAck();
    }

    /**
     * Namespace-scoped file lookup: routes directly to {@code namespace}'s repo. Required because
     * file ids are per-namespace (each namespace's owner assigns 0, 1, 2, …), so a bare FileId
     * is not globally unique across namespaces.
     *
     * <p>Routes to the ZK root for system-namespace files (which use a global counter and are
     * never ambiguous).
     */
    Optional<MetadataStore.Versioned<Records.FileRecord>> getFile(
            StrataNamespace namespace, FileId id) throws Exception {
        if (isSystem(namespace)) {
            return root.getFile(namespace, id);
        }
        return lockedFile(namespace, id);
    }

    private Optional<MetadataStore.Versioned<Records.FileRecord>> lockedFile(StrataNamespace ns, FileId id)
            throws Exception {
        return runLocked(repo(ns), repo -> {
            NamespaceMetadataState state = repo.state();
            return state.file(id).map(f -> new MetadataStore.Versioned<>(f, state.version(id)));
        });
    }

    Optional<FileId> resolvePath(StrataNamespace namespace, StrataPath path) throws Exception {
        if (isSystem(namespace)) {
            return root.resolvePath(namespace, path);
        }
        return runLocked(repo(namespace), repo -> repo.state().resolvePath(path));
    }

    boolean updateFile(Records.FileRecord record, int expectedVersion) throws Exception {
        if (isSystem(record.namespace())) {
            return root.updateFile(record, expectedVersion);
        }
        return withRepoReacquiringOnFence(record.namespace(), repo -> {
            NamespaceMetadataState state = repo.state();
            Optional<Records.FileRecord> current = state.file(record.fileId());
            if (current.isEmpty() || !current.get().namespace().equals(record.namespace())
                    || state.version(record.fileId()) != expectedVersion) {
                return false;
            }
            for (MetadataLogRecord r : MetadataLogDiff.diff(current.get(), record)) {
                repo.append(r);
            }
            return true;
        });
    }

    boolean deletePath(StrataNamespace namespace, StrataPath path, FileId expectedFileId) throws Exception {
        if (isSystem(namespace)) {
            return root.deletePath(namespace, path, expectedFileId);
        }
        return withRepoReacquiringOnFence(namespace, repo -> {
            Optional<FileId> bound = repo.state().resolvePath(path);
            if (bound.isEmpty()) {
                return true; // already unbound — idempotent
            }
            if (!bound.get().equals(expectedFileId)) {
                return false; // bound to a different (replacement) file
            }
            repo.append(new MetadataLogRecord.PathUnbound(namespace, path, expectedFileId));
            return true;
        });
    }

    boolean deleteFile(StrataNamespace namespace, FileId id, int expectedVersion) throws Exception {
        if (isSystem(namespace)) {
            // metadata-log system file lives in the root — lock-free
            return root.deleteFile(namespace, id, expectedVersion);
        }
        return withRepoReacquiringOnFence(namespace, repo -> {
            NamespaceMetadataState state = repo.state();
            Optional<Records.FileRecord> current = state.file(id);
            if (current.isEmpty()) {
                return true; // already a DELETED tombstone — idempotent
            }
            if (state.version(id) != expectedVersion) {
                return false;
            }
            Records.FileRecord file = current.get();
            if (state.resolvePath(file.path()).map(id::equals).orElse(false)) {
                repo.append(new MetadataLogRecord.PathUnbound(namespace, file.path(), id));
            }
            repo.append(new MetadataLogRecord.FileDeleted(id, System.currentTimeMillis()));
            return true;
        });
    }

    List<FileId> listFiles(StrataNamespace namespace) throws Exception {
        if (isSystem(namespace)) {
            return root.listFiles(namespace);
        }
        return runLocked(repo(namespace), repo -> repo.state().liveFiles());
    }

    /**
     * Children-only enumeration for the reconcile/verify sweep (see {@link MetadataStore#listFileIds}). For
     * the system namespace the metadata-log segment descriptors live in the consensus root, so this routes
     * to the root's children-only listing — the sweep no longer reads every segment record just to enumerate
     * ids. User namespaces already serve their listing from the in-memory repo (no consensus-root read).
     */
    List<FileId> listFileIds(StrataNamespace namespace) throws Exception {
        if (isSystem(namespace)) {
            return root.listFileIds(namespace);
        }
        return runLocked(repo(namespace), repo -> repo.state().liveFiles());
    }

    List<StrataNamespace> listNamespaces() throws Exception {
        // System (metadata-log) namespaces come from the root; user namespaces from active namespace handles.
        // The per-repo live check is taken under that repo's lock: state().hasLiveFiles() iterates a plain
        // HashMap that append() mutates under the same lock, so a lock-free read would race into a CME.
        Set<StrataNamespace> out = new LinkedHashSet<>(root.listNamespaces());
        for (Map.Entry<StrataNamespace, NamespaceLeadershipHandle> e : namespaces.entrySet()) {
            NamespaceMetadataLogRepository repo = activeRepoForCurrentAssignment(e.getValue());
            if (repo != null && runLocked(repo, active -> active.state().hasLiveFiles())) {
                out.add(e.getKey());
            }
        }
        return new ArrayList<>(out);
    }

    int sweepDeletedFiles(long olderThanMs) throws Exception {
        int reaped = root.sweepDeletedFiles(olderThanMs); // metadata-log system-file tombstones (shared root)
        return reaped + sweepOwnedNamespaceTombstones(olderThanMs);
    }

    /**
     * Reaps tombstones only in the active owned namespace handles — the system-file tombstones in the
     * shared root are NOT touched here (the leader sweeps those globally via {@link #sweepDeletedFiles}).
     * Each namespace owner calls this so its own namespaces' tombstones are reaped even when it does not
     * hold the global leader latch.
     */
    int sweepOwnedNamespaceTombstones(long olderThanMs) throws Exception {
        long cutoff = System.currentTimeMillis() - olderThanMs;
        int reaped = 0;
        // Iterate namespace handles lock-free, but lock EACH repo around its own sweep so the tombstone append
        // stays under that namespace's mutation lock (no cross-namespace head-of-line blocking).
        for (Map.Entry<StrataNamespace, NamespaceLeadershipHandle> e : namespaces.entrySet()) {
            StrataNamespace namespace = e.getKey();
            if (e.getValue().activeRepo() == null) {
                continue;
            }
            try {
                reaped += withRepoReacquiringOnFence(namespace, repo -> {
                    int swept = 0;
                    for (FileId id : repo.state().tombstonesDeletedAtOrBefore(cutoff)) {
                        repo.append(new MetadataLogRecord.TombstoneSwept(id));
                        swept++;
                    }
                    return swept;
                });
            } catch (Exception ex) {
                if (!closed) {
                    log.warn("namespace {} tombstone sweep failed; will retry next sweep", namespace, ex);
                }
            }
        }
        return reaped;
    }

    @Override
    public NamespaceLeaderState leaderState(StrataNamespace namespace) {
        if (isSystem(namespace)) {
            return NamespaceLeaderState.ACTIVE;
        }
        NamespaceLeadershipHandle handle = namespaces.get(namespace);
        if (handle == null) {
            return NamespaceLeaderState.STANDBY;
        }
        return handle.state == NamespaceLeaderState.ACTIVE
                && activeRepoForCurrentAssignment(handle) == null
                ? NamespaceLeaderState.FENCED
                : handle.state;
    }

    @Override
    public boolean isNamespaceActive(StrataNamespace namespace) {
        return leaderState(namespace) == NamespaceLeaderState.ACTIVE;
    }

    @Override
    public long namespaceActiveSinceMs(StrataNamespace namespace) {
        if (isSystem(namespace)) {
            return SYSTEM_NAMESPACE_ACTIVE_SINCE_MS;
        }
        NamespaceLeadershipHandle handle = namespaces.get(namespace);
        return handle == null || activeRepoForCurrentAssignment(handle) == null
                ? 0
                : handle.activeSinceMs;
    }

    @Override
    public long namespaceOwnerEpoch(StrataNamespace namespace) {
        if (isSystem(namespace)) {
            return 0;
        }
        NamespaceLeadershipHandle handle = namespaces.get(namespace);
        return handle == null || activeRepoForCurrentAssignment(handle) == null
                ? 0
                : handle.metadataEpoch;
    }

    @Override
    public long authoritativeOwnerEpoch(StrataNamespace namespace) throws Exception {
        if (isSystem(namespace)) {
            return 0;
        }
        return withAuthoritativeRepo(namespace, NamespaceMetadataLogRepository::metadataEpoch);
    }

    @Override
    public boolean requiresDurableOwnerEpochFence(StrataNamespace namespace) {
        NamespaceOwnership ownership = namespaceOwnership;
        return ownership != null && !ownership.ownsAll() && !isSystem(namespace);
    }

    @Override
    public ReentrantLock namespaceReconcileLock(StrataNamespace namespace) {
        NamespaceLeadershipHandle handle = namespaces.get(namespace);
        if (handle == null) {
            throw new IllegalStateException("namespace has no local leadership handle: " + namespace);
        }
        return handle.reconcileLock;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        closeOwnershipListener();
        Thread sweeper = compactionThread;
        if (sweeper != null) {
            sweeper.interrupt(); // best-effort; the loop also self-exits on the closed flag
        }
        try {
            fileStore.close();
        } catch (RuntimeException ignored) {
            // best-effort — file-store close releases an embedded client; never block shutdown
        }
        if (ownsRoot) {
            root.close();
        }
    }
}
