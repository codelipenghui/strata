package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * The namespace-log {@link MetadataStore} backend (design §8, §16 Step 3). User file/path metadata is
 * persisted through a per-namespace metadata log (a ZK-backed strata-meta-file); node-registry, epoch,
 * assignment, manifest, and liveness are the consensus root's responsibility and delegate to it. A thin
 * facade over the shared {@link NamespaceLogBackend}, so the existing SCP surface and Controller
 * run unchanged — only where metadata lives changes (design §1, §4).
 */
public final class NamespaceLogMetadataStore implements MetadataStore {
    private final NamespaceLogBackend backend;

    NamespaceLogMetadataStore(NamespaceLogBackend backend) {
        this.backend = backend;
    }

    /** Restricts eager namespace recovery to namespaces this node owns (wired from Controller). */
    void setOwnership(java.util.function.Predicate<StrataNamespace> ownsNamespace) {
        backend.setOwnership(ownsNamespace);
    }

    /** Process-wide metadata-log counters (append/compaction/recovery), surfaced as Prometheus metrics. */
    NamespaceLogMetrics metrics() {
        return backend.metrics();
    }

    /** Namespaces with a live owner repository on this instance — the sharding load this node carries. */
    int loadedNamespaceCount() {
        return backend.loadedNamespaceCount();
    }

    /** Per-namespace stats {@code namespace -> [liveFiles, openLogBytes]} for owned namespaces. */
    java.util.Map<StrataNamespace, long[]> namespaceStats() {
        return backend.namespaceStats();
    }

    /* ---- file / path metadata: the namespace metadata log ---- */

    @Override
    public void createFile(Records.FileRecord record) throws Exception {
        backend.createFile(record);
    }

    /**
     * Owner-assigned create: delegates to {@link NamespaceLogBackend#createFileOwnerAssigned}.
     * The server assigns the file id; opId-keyed idempotency is enforced in the namespace log.
     */
    FileId createFileOwnerAssigned(Records.FileRecord template) throws Exception {
        return backend.createFileOwnerAssigned(template);
    }

    @Override
    public Optional<Versioned<Records.FileRecord>> getFile(StrataNamespace namespace, FileId id) throws Exception {
        return backend.getFile(namespace, id);
    }

    @Override
    public Optional<FileId> resolvePath(StrataNamespace namespace, StrataPath path) throws Exception {
        return backend.resolvePath(namespace, path);
    }

    @Override
    public boolean updateFile(Records.FileRecord record, int expectedVersion) throws Exception {
        return backend.updateFile(record, expectedVersion);
    }

    @Override
    public boolean deletePath(StrataNamespace namespace, StrataPath path, FileId expectedFileId)
            throws Exception {
        return backend.deletePath(namespace, path, expectedFileId);
    }

    @Override
    public boolean deleteFile(StrataNamespace namespace, FileId id, int expectedVersion) throws Exception {
        return backend.deleteFile(namespace, id, expectedVersion);
    }

    @Override
    public List<FileId> listFiles(StrataNamespace namespace) throws Exception {
        return backend.listFiles(namespace);
    }

    @Override
    public List<FileId> listFileIds(StrataNamespace namespace) throws Exception {
        return backend.listFileIds(namespace);
    }

    @Override
    public List<StrataNamespace> listNamespaces() throws Exception {
        return backend.listNamespaces();
    }

    @Override
    public int sweepDeletedFiles(long olderThanMs) throws Exception {
        return backend.sweepDeletedFiles(olderThanMs);
    }

    @Override
    public int sweepOwnedNamespaceTombstones(long olderThanMs) throws Exception {
        return backend.sweepOwnedNamespaceTombstones(olderThanMs);
    }

    /* ---- node registry + sharding root: delegated to the consensus root store ---- */

    @Override
    public boolean putNode(Records.NodeRecord record, int expectedVersion) throws Exception {
        return backend.root().putNode(record, expectedVersion);
    }

    @Override
    public Optional<Versioned<Records.NodeRecord>> getNode(int nodeId) throws Exception {
        return backend.root().getNode(nodeId);
    }

    @Override
    public List<Versioned<Records.NodeRecord>> listNodes() throws Exception {
        return backend.root().listNodes();
    }

    @Override
    public long allocateMetadataEpoch() throws Exception {
        return backend.root().allocateMetadataEpoch();
    }

    @Override
    public Optional<Versioned<Records.NamespaceAssignment>> getNamespaceAssignment(StrataNamespace namespace)
            throws Exception {
        return backend.root().getNamespaceAssignment(namespace);
    }

    @Override
    public boolean putNamespaceAssignment(Records.NamespaceAssignment assignment, int expectedVersion)
            throws Exception {
        return backend.root().putNamespaceAssignment(assignment, expectedVersion);
    }

    @Override
    public List<StrataNamespace> listAssignedNamespaces() throws Exception {
        return backend.root().listAssignedNamespaces();
    }

    @Override
    public Optional<Versioned<Records.NamespaceManifest>> getNamespaceManifest(StrataNamespace namespace)
            throws Exception {
        return backend.root().getNamespaceManifest(namespace);
    }

    @Override
    public OptionalInt putNamespaceManifest(Records.NamespaceManifest manifest, int expectedVersion)
            throws Exception {
        return backend.root().putNamespaceManifest(manifest, expectedVersion);
    }

    @Override
    public void putClusterLiveNodes(byte[] snapshot) throws Exception {
        backend.root().putClusterLiveNodes(snapshot);
    }

    @Override
    public Optional<byte[]> getClusterLiveNodes() throws Exception {
        return backend.root().getClusterLiveNodes();
    }

    @Override
    public void close() {
        backend.close();
    }
}
