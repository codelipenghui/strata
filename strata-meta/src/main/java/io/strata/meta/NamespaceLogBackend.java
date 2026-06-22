package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.zookeeper.KeeperException;

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
    private final Map<FileId, StrataNamespace> fileIndex = new ConcurrentHashMap<>();
    // Only namespaces this node OWNS may be opened here — opening another owner's namespace would
    // republish its manifest and fence the real owner. Default ns->true for single-node / tests.
    private volatile Predicate<StrataNamespace> ownsNamespace = ns -> true;
    private volatile boolean ownedNamespacesWarmed;
    private boolean closed;

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
        // Best-effort gauges over the ConcurrentHashMap; a momentarily-stale per-namespace read is fine.
        Map<StrataNamespace, long[]> out = new HashMap<>(repos.size());
        for (Map.Entry<StrataNamespace, NamespaceMetadataLogRepository> e : repos.entrySet()) {
            out.put(e.getKey(), new long[]{e.getValue().liveFileCount(), e.getValue().openLogBytes()});
        }
        return out;
    }

    /** Restricts eager recovery to the namespaces this node owns (wired from Controller). */
    void setOwnership(Predicate<StrataNamespace> ownsNamespace) {
        this.ownsNamespace = ownsNamespace;
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
                for (FileId id : r.state().liveFiles()) {
                    fileIndex.put(id, namespace);
                }
                repos.put(namespace, r);   // publish only after recovery + fileIndex populated
            }
            return r;
        } finally {
            repoCreateLock.unlock();
        }
    }

    void createFile(Records.FileRecord record) throws Exception {
        if (isSystem(record.namespace())) {
            root.createFile(record); // metadata-log system file — lives in the ZK root (lock-free)
            return;
        }
        NamespaceMetadataLogRepository repo = repo(record.namespace());
        repo.lock();
        try {
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
            fileIndex.put(record.fileId(), record.namespace());
        } finally {
            repo.unlock();
        }
    }

    Optional<MetadataStore.Versioned<Records.FileRecord>> getFile(FileId id) throws Exception {
        StrataNamespace ns = fileIndex.get(id); // lock-free
        if (ns != null) {
            return lockedFile(ns, id);
        }
        // A metadata-log system file lives in the root; this read is lock-free, so a self-looping
        // system-file lookup never blocks on a user op that holds its namespace's lock.
        Optional<MetadataStore.Versioned<Records.FileRecord>> system = root.getFile(id);
        if (system.isPresent()) {
            return system;
        }
        // A file-id op (e.g. openById right after a restart) for an owned namespace not yet loaded:
        // warm the owned namespaces once, then re-resolve.
        if (!ownedNamespacesWarmed) {
            warmOwnedNamespaces();
            ns = fileIndex.get(id);
            if (ns != null) {
                return lockedFile(ns, id);
            }
        }
        return Optional.empty();
    }

    private Optional<MetadataStore.Versioned<Records.FileRecord>> lockedFile(StrataNamespace ns, FileId id)
            throws Exception {
        NamespaceMetadataLogRepository repo = repo(ns);
        repo.lock();
        try {
            NamespaceMetadataState state = repo.state();
            return state.file(id).map(f -> new MetadataStore.Versioned<>(f, state.version(id)));
        } finally {
            repo.unlock();
        }
    }

    /**
     * Opens (recovers) every namespace this node owns that has a published manifest, once — so a
     * file-id op can resolve a file whose namespace has not yet been touched by a path-scoped op.
     * Ownership-scoped: opening a non-owned namespace would republish its manifest and fence the owner.
     */
    private void warmOwnedNamespaces() throws Exception {
        repoCreateLock.lock();
        try {
            if (ownedNamespacesWarmed) {
                return;
            }
            for (StrataNamespace ns : root.listAssignedNamespaces()) {
                if (!isSystem(ns) && ownsNamespace.test(ns)) {
                    repo(ns); // recover + populate fileIndex
                }
            }
            ownedNamespacesWarmed = true;
        } finally {
            repoCreateLock.unlock();
        }
    }

    Optional<FileId> resolvePath(StrataNamespace namespace, StrataPath path) throws Exception {
        if (isSystem(namespace)) {
            return root.resolvePath(namespace, path);
        }
        NamespaceMetadataLogRepository repo = repo(namespace);
        repo.lock();
        try {
            return repo.state().resolvePath(path);
        } finally {
            repo.unlock();
        }
    }

    boolean updateFile(Records.FileRecord record, int expectedVersion) throws Exception {
        if (isSystem(record.namespace())) {
            return root.updateFile(record, expectedVersion);
        }
        NamespaceMetadataLogRepository repo = repo(record.namespace());
        repo.lock();
        try {
            NamespaceMetadataState state = repo.state();
            Optional<Records.FileRecord> current = state.file(record.fileId());
            if (current.isEmpty() || state.version(record.fileId()) != expectedVersion) {
                return false;
            }
            for (MetadataLogRecord r : MetadataLogDiff.diff(current.get(), record)) {
                repo.append(r);
            }
            return true;
        } finally {
            repo.unlock();
        }
    }

    boolean deletePath(StrataNamespace namespace, StrataPath path, FileId expectedFileId) throws Exception {
        if (isSystem(namespace)) {
            return root.deletePath(namespace, path, expectedFileId);
        }
        NamespaceMetadataLogRepository repo = repo(namespace);
        repo.lock();
        try {
            Optional<FileId> bound = repo.state().resolvePath(path);
            if (bound.isEmpty()) {
                return true; // already unbound — idempotent
            }
            if (!bound.get().equals(expectedFileId)) {
                return false; // bound to a different (replacement) file
            }
            repo.append(new MetadataLogRecord.PathUnbound(namespace, path, expectedFileId));
            return true;
        } finally {
            repo.unlock();
        }
    }

    boolean deleteFile(FileId id, int expectedVersion) throws Exception {
        StrataNamespace ns = fileIndex.get(id); // lock-free
        if (ns == null) {
            // a metadata-log system file (root), or unknown/already-swept — root delete is lock-free
            return root.deleteFile(id, expectedVersion);
        }
        NamespaceMetadataLogRepository repo = repo(ns);
        repo.lock();
        try {
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
                repo.append(new MetadataLogRecord.PathUnbound(ns, file.path(), id));
            }
            repo.append(new MetadataLogRecord.FileDeleted(id, System.currentTimeMillis()));
            return true;
        } finally {
            repo.unlock();
        }
    }

    List<FileId> listFiles(StrataNamespace namespace) throws Exception {
        if (isSystem(namespace)) {
            return root.listFiles(namespace);
        }
        NamespaceMetadataLogRepository repo = repo(namespace);
        repo.lock();
        try {
            return repo.state().liveFiles();
        } finally {
            repo.unlock();
        }
    }

    List<StrataNamespace> listNamespaces() throws Exception {
        // System (metadata-log) namespaces come from the root; user namespaces from the loaded repos.
        // Best-effort listing over the ConcurrentHashMap (lock-free); a stale live-files snapshot is fine.
        Set<StrataNamespace> out = new LinkedHashSet<>(root.listNamespaces());
        for (Map.Entry<StrataNamespace, NamespaceMetadataLogRepository> e : repos.entrySet()) {
            if (!e.getValue().state().liveFiles().isEmpty()) {
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
                    fileIndex.remove(id);
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
