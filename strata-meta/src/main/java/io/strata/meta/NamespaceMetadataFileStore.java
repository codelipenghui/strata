package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;

/**
 * The physical byte-store boundary for one namespace's metadata system files (design §8). The metadata
 * log and snapshots are stored as named blobs; the runtime backend ({@code StrataSystemMetadataFileStore})
 * writes them as replicated Strata chunks with their descriptors in the consensus root, while tests use
 * an in-memory double. The repository depends only on this interface, so it is decoupled from the
 * physical metadata-file implementation.
 */
interface NamespaceMetadataFileStore {

    /**
     * Creates a fresh, empty open-log file and returns its id.
     *
     * @param ns         the user namespace whose metadata this log belongs to
     * @param generation the manifest generation that this log file will be published under
     */
    FileId createLogFile(StrataNamespace ns, long generation) throws Exception;

    /** Durably appends pre-framed bytes to the open log file {@code logFileId}. */
    void appendLog(FileId logFileId, byte[] frameBytes) throws Exception;

    /** All durable bytes of {@code logFileId} (empty if the file has no bytes yet). */
    byte[] readLog(FileId logFileId) throws Exception;

    /**
     * Writes {@code snapshotBytes} as a new immutable snapshot file and returns its id.
     *
     * @param ns         the user namespace whose metadata this snapshot captures
     * @param generation the manifest generation that this snapshot will be published under
     */
    FileId writeSnapshot(StrataNamespace ns, long generation, byte[] snapshotBytes) throws Exception;

    /** All bytes of the snapshot file {@code snapshotFileId}. */
    byte[] readSnapshot(FileId snapshotFileId) throws Exception;

    /** Best-effort delete of an unreferenced system file (compaction GC). */
    void deleteFile(FileId fileId) throws Exception;

    /** Releases any resources (e.g. an embedded Strata client). No-op for in-memory/disk stores. */
    default void close() {
    }
}
