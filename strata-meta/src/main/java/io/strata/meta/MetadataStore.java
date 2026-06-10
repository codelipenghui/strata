package io.strata.meta;

import io.strata.common.FileId;

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

    void createFile(Records.FileRecord record) throws Exception;

    Optional<Versioned<Records.FileRecord>> getFile(FileId id) throws Exception;

    /** CAS update; returns false on version conflict. */
    boolean updateFile(Records.FileRecord record, int expectedVersion) throws Exception;

    void deleteFile(FileId id, int expectedVersion) throws Exception;

    List<FileId> listFiles() throws Exception;

    /* ---- node registry ---- */

    int nextNodeId() throws Exception;

    void putNode(Records.NodeRecord record) throws Exception;

    Optional<Records.NodeRecord> getNode(int nodeId) throws Exception;

    List<Records.NodeRecord> listNodes() throws Exception;

    @Override
    void close();
}
