package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;

import java.util.List;
import java.util.Optional;

/**
 * MetadataStore SPI (tech design §4.4): persistence behind the controller. v0 backend is
 * ZooKeeper; v1 is the KRaft chunk-map state machine. The SCP surface and service semantics are
 * identical across backends — this interface is the swap line.
 *
 * Versioned reads + compare-and-set writes; the service leader is the only writer, CAS guards
 * against stale leaders racing across failover.
 */
public interface MetadataStore extends AutoCloseable {

    record Versioned<T>(T value, int version) {}

    /* ---- file / chunk records ---- */

    /**
     * Creates a file. A deleted FileId must not become creatable again until its DELETED tombstone
     * is swept ({@link #sweepDeletedFiles}); otherwise a delayed CREATE replay could resurrect a
     * deleted file. The create fails while the tombstone is present.
     */
    void createFile(Records.FileRecord record) throws Exception;

    Optional<Versioned<Records.FileRecord>> getFile(FileId id) throws Exception;

    Optional<FileId> resolvePath(StrataNamespace namespace, StrataPath path) throws Exception;

    /** CAS update; returns false on version conflict. */
    boolean updateFile(Records.FileRecord record, int expectedVersion) throws Exception;

    boolean deletePath(StrataNamespace namespace, StrataPath path, FileId expectedFileId) throws Exception;

    /** CAS delete; returns false on version conflict. */
    boolean deleteFile(FileId id, int expectedVersion) throws Exception;

    /** Live file ids within one namespace (excludes DELETED tombstones awaiting sweep). */
    List<FileId> listFiles(StrataNamespace namespace) throws Exception;

    /**
     * Namespaces that currently have at least one live file — the roots files are organized under.
     * A cluster-wide file sweep is {@code listNamespaces()} composed with {@link #listFiles(StrataNamespace)};
     * there is intentionally no global flat file listing.
     */
    List<StrataNamespace> listNamespaces() throws Exception;

    /**
     * Reaps DELETED file tombstones whose deletion is older than {@code olderThanMs}. Until reaped,
     * a tombstone keeps its FileId un-creatable (fencing a delayed CREATE replay); the window must
     * exceed the longest possible create-retry delay. Returns the number reaped.
     */
    int sweepDeletedFiles(long olderThanMs) throws Exception;

    /* ---- node registry ---- */

    int nextNodeId() throws Exception;

    /**
     * CAS write of a node record: expectedVersion -1 creates (fails if present), otherwise
     * updates only if the stored version matches. A deposed leader's stale write must lose —
     * unconditional node writes allowed a dead leader's expire-scan to overwrite the new
     * leader's REGISTERED state with DEAD.
     */
    boolean putNode(Records.NodeRecord record, int expectedVersion) throws Exception;

    Optional<Versioned<Records.NodeRecord>> getNode(int nodeId) throws Exception;

    List<Versioned<Records.NodeRecord>> listNodes() throws Exception;

    /* ---- namespace sharding root (design §5, §6.1) ----
     * Consensus-root capabilities: the global metadata-epoch fence and per-namespace assignment
     * records. The namespace-log backend delegates these to the underlying root store, so they are
     * not part of the backend-neutral file/path/node contract — backends that are not a consensus
     * root inherit the safe defaults below. */

    /**
     * CAS-increments the global metadata epoch counter and returns the new value (design §5, §13
     * step 2). Values are unique and strictly monotonic; gaps are allowed. A namespace leader
     * allocates an epoch before activating and fences manifest publication with it.
     */
    default long allocateMetadataEpoch() throws Exception {
        throw new UnsupportedOperationException("metadata epoch allocation requires a consensus-root backend");
    }

    /** The persisted rendezvous assignment for {@code namespace}, if one has been written (design §6.1). */
    default Optional<Versioned<Records.NamespaceAssignment>> getNamespaceAssignment(StrataNamespace namespace)
            throws Exception {
        return Optional.empty();
    }

    /**
     * CAS write of a namespace assignment: {@code expectedVersion -1} creates (fails if present),
     * otherwise updates only if the stored version matches. Returns false on a version conflict.
     */
    default boolean putNamespaceAssignment(Records.NamespaceAssignment assignment, int expectedVersion)
            throws Exception {
        throw new UnsupportedOperationException("namespace assignment requires a consensus-root backend");
    }

    /** Namespaces that currently have a persisted assignment record (design §6.1). */
    default List<StrataNamespace> listAssignedNamespaces() throws Exception {
        return List.of();
    }

    /** The published metadata-log manifest for {@code namespace}, if any (design §5, §9). */
    default Optional<Versioned<Records.NamespaceManifest>> getNamespaceManifest(StrataNamespace namespace)
            throws Exception {
        return Optional.empty();
    }

    /**
     * CAS-publishes a namespace metadata-log manifest — the linearizable cutover barrier (design §9).
     * {@code expectedVersion -1} creates (fails if present), otherwise updates only if the stored
     * version matches; returns false on a version conflict so a fenced leader's publish loses.
     */
    default boolean putNamespaceManifest(Records.NamespaceManifest manifest, int expectedVersion)
            throws Exception {
        throw new UnsupportedOperationException("namespace manifest requires a consensus-root backend");
    }

    /* ---- shared cluster liveness (design §11) ---- */

    /**
     * Publishes a compact snapshot of currently-live data nodes to the consensus root so a
     * non-controller namespace owner — which has no heartbeat channel — can still place and repair
     * replicas. Last-write-wins (the controller is the single publisher); root-store capability with
     * a no-op default for non-root backends.
     */
    default void putClusterLiveNodes(byte[] snapshot) throws Exception {
        // no-op for non-root backends / test doubles
    }

    /** The latest published live-node snapshot, if any (design §11). */
    default Optional<byte[]> getClusterLiveNodes() throws Exception {
        return Optional.empty();
    }

    @Override
    void close();
}
