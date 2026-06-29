package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;

import java.util.List;

/**
 * Authoritative metadata-log records (design §8). Every mutating metadata operation is represented as
 * exactly one append-only record; replaying the log ({@link NamespaceMetadataState}) reconstructs all
 * file identity, path bindings, chunk descriptors, tombstones, and the derived {@code node -> chunks}
 * index. The log NEVER contains derived indexes or command/delivery events — only the resulting
 * metadata transition (design §8, §8.1).
 */
public sealed interface MetadataLogRecord {

    /**
     * Reserves a file id, its {@code (namespace, path)} binding, and the file policy, plus the
     * create-operation id used to dedup retried creates across failover (idempotency).
     */
    record FileCreated(FileId fileId, StrataNamespace namespace, StrataPath path,
                       int replicationFactor, int ackQuorum, boolean fsyncOnAck,
                       long createdAtMs, long createOpMsb, long createOpLsb) implements MetadataLogRecord {}

    /** Records a writer fencing epoch before append or recovery ownership changes (design §8). */
    record WriterEpochAllocated(FileId fileId, int writerEpoch) implements MetadataLogRecord {}

    /** Commits chunk placement before any chunk byte is written (commit-before-write, design §15). */
    record ChunkCreated(FileId fileId, int chunkIndex, int writeEpoch, List<Integer> replicas,
                        long createOpMsb, long createOpLsb) implements MetadataLogRecord {
        public ChunkCreated {
            replicas = List.copyOf(replicas);
        }
    }

    /** Commits the final length, CRC, and sealed replica set of a chunk. */
    record ChunkSealed(FileId fileId, int chunkIndex, long length, int crc, int writeEpoch,
                       List<Integer> sealedReplicas) implements MetadataLogRecord {
        public ChunkSealed {
            sealedReplicas = List.copyOf(sealedReplicas);
        }
    }

    /** Removes the current open tail chunk for the same create operation before it becomes durable data. */
    record ChunkAborted(FileId fileId, int chunkIndex) implements MetadataLogRecord {}

    /** Removes an already-drained chunk descriptor from a DELETING file while other chunks may remain. */
    record ChunkDeleted(FileId fileId, int chunkIndex) implements MetadataLogRecord {}

    /** Closes the file (OPEN -> SEALED). */
    record FileSealed(FileId fileId) implements MetadataLogRecord {}

    /** Marks a file DELETING so stale writers cannot resurrect it. */
    record FileDeleting(FileId fileId) implements MetadataLogRecord {}

    /** Finalizes a drained DELETING file and records the tombstone deletion timestamp for retention. */
    record FileDeleted(FileId fileId, long deletedAtMs) implements MetadataLogRecord {}

    /** Removes a {@code (namespace, path)} lookup binding without changing file lifecycle state. */
    record PathUnbound(StrataNamespace namespace, StrataPath path, FileId expectedFileId)
            implements MetadataLogRecord {}

    /** Atomically replaces a failed sealed replica (repair). */
    record ReplicaSwapped(FileId fileId, int chunkIndex, int fromNode, int toNode)
            implements MetadataLogRecord {}

    /** Removes a missing/corrupt live sealed replica, or a deletion-confirmed replica from a DELETING file. */
    record ReplicaDropped(FileId fileId, int chunkIndex, int nodeId) implements MetadataLogRecord {}

    /** Records a verified replacement replica for an under-replicated sealed chunk. */
    record ReplicaAdded(FileId fileId, int chunkIndex, int nodeId) implements MetadataLogRecord {}

    /** Final metadata cleanup of a deletion tombstone after the fencing window (design §10). */
    record TombstoneSwept(FileId fileId) implements MetadataLogRecord {}
}
