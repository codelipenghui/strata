package io.strata.meta;

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
 * The per-process engine behind {@link NamespaceLogMetadataStore} (design §8). Each USER namespace's
 * file metadata lives in a {@link NamespaceMetadataLogRepository} — the ZK-backed strata-meta-file: a
 * metadata log whose bytes are stored by the {@link NamespaceMetadataFileStore} and whose physical
 * descriptors live in the consensus root.
 *
 * <p><b>System namespace routing (design §5).</b> The metadata-log files themselves are stored as Strata
 * files in the reserved {@link #SYSTEM_NAMESPACE}; their own descriptors live directly in the ZK root
 * store (otherwise the log would recurse into itself). So every op for the system namespace is routed
 * straight to {@code root}, and — crucially — <b>without taking any namespace lock</b>: a user-namespace
 * mutation holds <em>that namespace's</em> lock while it writes its metadata-log file, which (with a
 * replicated-chunk file store) self-loops back into this engine for the system file; if that re-took a
 * namespace lock it would deadlock. {@code getFile(fileId)} has no namespace, so it checks the in-memory
 * user index lock-free, then falls back to the root (system files are only ever in the root).
 *
 * <p>A single engine is shared across {@link NamespaceLogMetadataStore} handles so multiple in-process
 * leaders observe one consistent log per namespace; cross-process single-writer is enforced by ownership
 * routing (M2). Each {@link NamespaceMetadataLogRepository} owns a per-namespace {@link ReentrantLock}
 * (not {@code synchronized}) held across that namespace's durable append, so the blocking root/file-store
 * I/O inside a critical section never pins a virtual-thread carrier — and a slow append in one namespace
 * never head-of-line-blocks a mutation in another. Repo creation/warm/close is guarded by
 * {@code repoCreateLock}; best-effort metric reads ({@code loadedNamespaceCount}/{@code namespaceStats}/
 * {@code listNamespaces}) run lock-free over the {@link ConcurrentHashMap} of repos.
 */
final class NamespaceLogBackend implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(NamespaceLogBackend.class);

    /** Reserved namespace holding the metadata-log/snapshot system files; routed to the ZK root. */
    static final StrataNamespace SYSTEM_NAMESPACE = StrataNamespace.of("strata-meta");

    static boolean isSystem(StrataNamespace namespace) {
        return SYSTEM_NAMESPACE.equals(namespace);
    }

    private final MetadataStore root;
    private final NamespaceMetadataFileStore fileStore;
    private final boolean ownsRoot;
    private final NamespaceLogMetrics metrics = new NamespaceLogMetrics();
    private final ConcurrentHashMap<StrataNamespace, NamespaceMetadataLogRepository> repos =
            new ConcurrentHashMap<>();
    private final ReentrantLock repoCreateLock = new ReentrantLock();
    // Only namespaces this node OWNS may be opened here — opening another owner's namespace would
    // republish its manifest and fence the real owner. Default ns->true for single-node / tests.
    private volatile Predicate<StrataNamespace> ownsNamespace = ns -> true;
    private volatile boolean closed;
    private volatile Thread compactionThread;

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
        return repos.size(); // best-effort gauge over the ConcurrentHashMap (lock-free)
    }

    /** Per-namespace stats for the namespaces this node owns: {@code namespace -> [liveFiles, openLogBytes]}. */
    Map<StrataNamespace, long[]> namespaceStats() {
        // Best-effort gauges over the ConcurrentHashMap; each per-namespace read is taken under that repo's
        // lock — liveFileCount() iterates a plain HashMap that append() mutates under the same lock, so a
        // lock-free read would race into a ConcurrentModificationException.
        Map<StrataNamespace, long[]> out = new HashMap<>(repos.size());
        for (Map.Entry<StrataNamespace, NamespaceMetadataLogRepository> e : repos.entrySet()) {
            NamespaceMetadataLogRepository repo = e.getValue();
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
    }

    // Safety delay (design §10 step 6 / issue #8): a superseded metadata-log generation is retained for this
    // many ms after it is superseded before the sweep reclaims it — a rollback margin against a bad newest
    // generation. 0 disables the window (reap as soon as the sweep sees the orphan). Set from Controller env.
    private volatile long logRetentionMs;

    /** Configures the superseded-generation retention window (STRATA_CONTROLLER_LOG_RETENTION_MS). */
    void setLogRetentionMs(long retentionMs) {
        this.logRetentionMs = Math.max(0, retentionMs);
    }

    /**
     * Starts the periodic open-log compaction sweep (design §8/§10 bounded-storage maintenance). Without
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
     * manifest (design §8/§10, issue #8): a generation superseded by a later compaction, plus snapshot/log
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
            });
        }
        // First pass: record each generation's creation time (keyed (namespace, generation)) and collect the
        // reap candidates. A candidate's SUPERSESSION instant is the creation time of its successor generation,
        // so we must observe creation times for live (referenced) files too — hence we scan every file here.
        Map<StrataNamespace, Map<Long, Long>> genCreatedAt = new HashMap<>();
        List<ReapCandidate> candidates = new ArrayList<>();
        for (FileId id : root.listFiles(SYSTEM_NAMESPACE)) {
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
     * Compacts every owned repository whose open log has grown past {@code thresholdBytes}. Each compaction
     * runs under that namespace's mutation lock (same as open-time compaction; non-blocking COW compaction
     * is design-deferred). A fenced manifest CAS means another node now owns the namespace, so the stale
     * repo is evicted and the next op re-acquires under a fresh epoch rather than fencing forever. Returns
     * the number of namespaces compacted. Package-private so a test can drive a sweep deterministically.
     */
    int compactOversizedRepos(long thresholdBytes) {
        int compacted = 0;
        for (Map.Entry<StrataNamespace, NamespaceMetadataLogRepository> e : repos.entrySet()) {
            NamespaceMetadataLogRepository repo = e.getValue();
            repo.lock();
            try {
                if (repo.openLogBytes() < thresholdBytes) {
                    continue;
                }
                repo.compactAndPublish();
                compacted++;
            } catch (IllegalStateException fenced) {
                // The only IllegalStateException compactAndPublish raises in steady state is a lost manifest
                // CAS — another node owns this namespace now; drop the stale repo so the next op re-acquires.
                repos.remove(e.getKey(), repo);
                if (!closed) {
                    log.warn("namespace {} open-log compaction fenced — evicting stale repo", e.getKey(), fenced);
                }
            } catch (Exception ex) {
                // transient (file store / I/O), or the backend was closed mid-sweep — leave the repo and
                // retry on the next sweep; suppress the warning once closed (shutdown is not a failure).
                if (!closed) {
                    log.warn("namespace {} open-log compaction failed; will retry next sweep", e.getKey(), ex);
                }
            } finally {
                repo.unlock();
            }
        }
        return compacted;
    }

    private NamespaceMetadataLogRepository repo(StrataNamespace namespace) throws Exception {
        NamespaceMetadataLogRepository r = repos.get(namespace);   // fast path, lock-free
        if (r != null) {
            return r;
        }
        repoCreateLock.lock();
        try {
            r = repos.get(namespace);
            if (r == null) {
                long epoch = root.allocateMetadataEpoch();
                r = NamespaceMetadataLogRepository.open(namespace, fileStore, root, epoch, metrics);
                repos.put(namespace, r);   // publish only after recovery
            }
            return r;
        } finally {
            repoCreateLock.unlock();
        }
    }

    @FunctionalInterface
    private interface RepoTxn<T> {
        T run(NamespaceMetadataLogRepository repo) throws Exception;
    }

    /**
     * Runs a mutating transaction against this node's repository for {@code namespace}, re-acquiring the
     * meta-log and retrying ONCE if the append is fenced ({@link ErrorCode#FENCED_EPOCH}) because another
     * opener republished the manifest at a higher epoch.
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
            return runLocked(repo, txn);
        } catch (Exception e) {
            if (!isFencedEpoch(e)) {
                throw e;
            }
            log.warn("namespace {} meta-log append fenced (stale epoch) — re-acquiring and retrying once",
                    namespace, e);
            metrics.recordReacquire();
        }
        return runLocked(reacquire(namespace, repo), txn);
    }

    private static <T> T runLocked(NamespaceMetadataLogRepository repo, RepoTxn<T> txn) throws Exception {
        repo.lock();
        try {
            return txn.run(repo);
        } finally {
            repo.unlock();
        }
    }

    /**
     * Evicts {@code stale} (the repo we were fenced on) and re-opens this namespace at a fresh epoch.
     * The conditional remove lets a concurrent re-acquire win: if another thread already replaced the
     * cache entry, its fresh repo is reused rather than churning yet another epoch.
     */
    private NamespaceMetadataLogRepository reacquire(StrataNamespace namespace,
            NamespaceMetadataLogRepository stale) throws Exception {
        repoCreateLock.lock();
        try {
            repos.remove(namespace, stale); // no-op if another thread already re-acquired
            return repo(namespace);          // re-opens at a fresh epoch (recovers latest state) if we evicted
        } finally {
            repoCreateLock.unlock();
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
            if (current.isEmpty() || state.version(record.fileId()) != expectedVersion) {
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

    List<StrataNamespace> listNamespaces() throws Exception {
        // System (metadata-log) namespaces come from the root; user namespaces from the loaded repos.
        // The per-repo live check is taken under that repo's lock: state().hasLiveFiles() iterates a plain
        // HashMap that append() mutates under the same lock, so a lock-free read would race into a CME.
        Set<StrataNamespace> out = new LinkedHashSet<>(root.listNamespaces());
        for (Map.Entry<StrataNamespace, NamespaceMetadataLogRepository> e : repos.entrySet()) {
            if (runLocked(e.getValue(), repo -> repo.state().hasLiveFiles())) {
                out.add(e.getKey());
            }
        }
        return new ArrayList<>(out);
    }

    int sweepDeletedFiles(long olderThanMs) throws Exception {
        int reaped = root.sweepDeletedFiles(olderThanMs); // metadata-log system-file tombstones
        long cutoff = System.currentTimeMillis() - olderThanMs;
        // Iterate the repos lock-free, but lock EACH repo around its own sweep so the tombstone append
        // stays under that namespace's mutation lock (no cross-namespace head-of-line blocking).
        for (NamespaceMetadataLogRepository repo : repos.values()) {
            repo.lock();
            try {
                for (FileId id : repo.state().tombstonesDeletedAtOrBefore(cutoff)) {
                    repo.append(new MetadataLogRecord.TombstoneSwept(id));
                    reaped++;
                }
            } finally {
                repo.unlock();
            }
        }
        return reaped;
    }

    @Override
    public void close() {
        repoCreateLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            Thread sweeper = compactionThread;
            if (sweeper != null) {
                sweeper.interrupt(); // best-effort; the loop also self-exits on the closed flag
            }
            try {
                fileStore.close();
            } catch (RuntimeException ignore) {
                // best-effort — file-store close releases an embedded client; never block shutdown
            }
            if (ownsRoot) {
                root.close();
            }
        } finally {
            repoCreateLock.unlock();
        }
    }
}
