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
     * Creates a file and permanently reserves its FileId. A deleted FileId must not become
     * creatable again; otherwise a delayed CREATE replay can resurrect a deleted file.
     */
    void createFile(Records.FileRecord record) throws Exception;

    Optional<Versioned<Records.FileRecord>> getFile(FileId id) throws Exception;

    Optional<FileId> resolvePath(StrataNamespace namespace, StrataPath path) throws Exception;

    /** CAS update; returns false on version conflict. */
    boolean updateFile(Records.FileRecord record, int expectedVersion) throws Exception;

    boolean deletePath(StrataNamespace namespace, StrataPath path, FileId expectedFileId) throws Exception;

    /** CAS delete; returns false on version conflict. */
    boolean deleteFile(FileId id, int expectedVersion) throws Exception;

    List<FileId> listFiles() throws Exception;

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
