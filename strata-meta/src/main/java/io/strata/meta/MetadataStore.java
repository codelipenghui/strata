package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;

import java.util.List;
import java.util.Optional;

/**
 * MetadataStore SPI (tech design §4.4): persistence behind the metadata service. v0 backend is
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

    @Override
    void close();
}
