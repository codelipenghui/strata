package io.strata.meta;

import io.strata.common.ChunkId;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * The in-memory replay image of one namespace's metadata log (design §8.1, §9). Applying the
 * authoritative {@link MetadataLogRecord}s in order reconstructs the current file table, path
 * bindings, chunk descriptors, deletion tombstones (with timestamps for bounded sweep), and the
 * derived {@code node -> chunks} reverse index. All derived state is rebuildable from the log plus the
 * latest snapshot — it is never an independent source of truth.
 *
 * <p>Replay trusts the log: the operation layer ({@code NamespaceMetadataOperations}) enforces the
 * invariants (single binding per path, tombstone fencing, quorum on seal) before a record is appended,
 * so {@code apply} is a deterministic state transition with no validation.
 */
final class NamespaceMetadataState {
    /** Packs two longs into a single long[] key for the opId index map. */
    private record OpIdKey(long msb, long lsb) {}

    private final StrataNamespace namespace;
    private long nextFileId = 0;
    private final Map<FileId, Records.FileRecord> files = new HashMap<>();
    private final Map<FileId, Long> tombstones = new HashMap<>();        // DELETED fileId -> deletedAtMs
    private final Map<StrataPath, FileId> pathBindings = new HashMap<>();
    private final Map<Integer, Set<ChunkId>> nodeChunks = new HashMap<>(); // derived reverse index
    private final Map<FileId, Integer> versions = new HashMap<>();        // per-file CAS version (SPI)
    // opId → fileId index: rebuilt from log/snapshot on recovery via apply(FileCreated).
    // Entry is removed on TombstoneSwept so the opId may be reused after the tombstone is reaped.
    private final Map<OpIdKey, FileId> opIdIndex = new HashMap<>();

    NamespaceMetadataState(StrataNamespace namespace) {
        this.namespace = Objects.requireNonNull(namespace, "namespace");
    }

    StrataNamespace namespace() {
        return namespace;
    }

    /**
     * Returns the next file id to be assigned WITHOUT advancing the high-water mark. The caller must
     * follow this immediately with an {@code append(FileCreated(id, ...))} whose {@link #apply} will
     * advance {@code nextFileId} via {@code max(nextFileId, id+1)}. This ordering ensures that a crash
     * between peek and append does NOT leave {@code nextFileId} advanced in persistent state, so a
     * successor recovers with the same id available — the id is only "consumed" once its
     * {@code FileCreated} record lands durably in the log. The test
     * {@code NamespaceFileIdRecoveryInjectionTest} verifies window (a): crash after peek before append.
     */
    FileId peekNextFileId() {
        return FileId.of(nextFileId);
    }

    /**
     * The compacted serialized form of this state at a log cut offset (design §9, §10). Path bindings
     * and {@code node -> chunks} are derived on restore (a path binds exactly to its OPEN/SEALED file),
     * so only the file table and tombstone deletion timestamps are kept.
     */
    record Snapshot(long nextFileId, long nextLogStartOffset, List<Records.FileRecord> files, Map<FileId, Long> tombstones) {
        Snapshot {
            files = List.copyOf(files);
            tombstones = Map.copyOf(tombstones);
        }
    }

    /** Captures the current state for a compaction snapshot cut at {@code nextLogStartOffset}. */
    Snapshot exportSnapshot(long nextLogStartOffset) {
        return new Snapshot(nextFileId, nextLogStartOffset, new ArrayList<>(files.values()), new HashMap<>(tombstones));
    }

    /** Replaces this state with a snapshot's tables, re-deriving path bindings and the node index. */
    void restore(Snapshot snapshot) {
        nextFileId = snapshot.nextFileId();
        files.clear();
        tombstones.clear();
        pathBindings.clear();
        nodeChunks.clear();
        versions.clear();
        opIdIndex.clear();
        for (Records.FileRecord f : snapshot.files()) {
            files.put(f.fileId(), f);
            addToNodeChunks(f);
            versions.put(f.fileId(), 0);
            if (f.state() == FileState.OPEN || f.state() == FileState.SEALED) {
                pathBindings.put(f.path(), f.fileId());
            }
            opIdIndex.put(new OpIdKey(f.createOpMsb(), f.createOpLsb()), f.fileId());
        }
        tombstones.putAll(snapshot.tombstones());
    }

    void apply(MetadataLogRecord record) {
        switch (record) {
            case MetadataLogRecord.FileCreated r -> {
                nextFileId = Math.max(nextFileId, r.fileId().id() + 1);
                putFile(new Records.FileRecord(r.fileId(), r.namespace(), r.path(),
                        r.replicationFactor(), r.ackQuorum(), r.fsyncOnAck(), FileState.OPEN,
                        r.createdAtMs(), List.of(), r.createOpMsb(), r.createOpLsb()));
                tombstones.remove(r.fileId());
                pathBindings.put(r.path(), r.fileId());
                opIdIndex.put(new OpIdKey(r.createOpMsb(), r.createOpLsb()), r.fileId());
            }
            case MetadataLogRecord.WriterEpochAllocated r ->
                    mutate(r.fileId(), f -> f.withWriterEpoch(r.writerEpoch()));
            case MetadataLogRecord.ChunkCreated r -> mutate(r.fileId(), f -> {
                List<Records.ChunkRecord> chunks = new ArrayList<>(f.chunks());
                chunks.add(new Records.ChunkRecord(r.chunkIndex(), ChunkState.OPEN, 0, 0,
                        r.writeEpoch(), r.replicas(), r.createOpMsb(), r.createOpLsb()));
                return f.withChunks(chunks);
            });
            case MetadataLogRecord.ChunkSealed r -> mutate(r.fileId(), f -> updateChunk(f, r.chunkIndex(),
                    c -> c.sealed(r.length(), r.crc(), r.writeEpoch()).withReplicas(r.sealedReplicas())));
            case MetadataLogRecord.ChunkAborted r -> mutate(r.fileId(), f -> removeChunk(f, r.chunkIndex()));
            case MetadataLogRecord.ChunkDeleted r -> mutate(r.fileId(), f -> removeChunk(f, r.chunkIndex()));
            case MetadataLogRecord.FileSealed r -> mutate(r.fileId(), f -> f.withState(FileState.SEALED));
            case MetadataLogRecord.FileDeleting r -> mutate(r.fileId(), f -> f.withState(FileState.DELETING));
            case MetadataLogRecord.FileDeleted r -> {
                Records.FileRecord f = files.get(r.fileId());
                if (f != null) {
                    putFile(f.withState(FileState.DELETED));
                    tombstones.put(r.fileId(), r.deletedAtMs());
                }
            }
            case MetadataLogRecord.PathUnbound r -> {
                FileId bound = pathBindings.get(r.path());
                if (bound != null && bound.equals(r.expectedFileId())) {
                    pathBindings.remove(r.path());
                }
            }
            case MetadataLogRecord.ReplicaSwapped r -> mutate(r.fileId(), f -> updateChunk(f, r.chunkIndex(),
                    c -> c.withReplicaSwapped(r.fromNode(), r.toNode())));
            case MetadataLogRecord.ReplicaDropped r -> mutate(r.fileId(), f -> updateChunk(f, r.chunkIndex(),
                    c -> withoutReplica(c, r.nodeId())));
            case MetadataLogRecord.ReplicaAdded r -> mutate(r.fileId(), f -> updateChunk(f, r.chunkIndex(),
                    c -> withReplica(c, r.nodeId())));
            case MetadataLogRecord.TombstoneSwept r -> {
                Records.FileRecord removed = files.remove(r.fileId());
                if (removed != null) {
                    removeFromNodeChunks(removed);
                    opIdIndex.remove(new OpIdKey(removed.createOpMsb(), removed.createOpLsb()));
                }
                tombstones.remove(r.fileId());
                versions.remove(r.fileId());
            }
        }
    }

    /* ---- queries ---- */

    /** The live file record (OPEN/SEALED/DELETING); empty for a DELETED tombstone or an absent id. */
    Optional<Records.FileRecord> file(FileId id) {
        Records.FileRecord f = files.get(id);
        if (f == null || f.state() == FileState.DELETED) {
            return Optional.empty();
        }
        return Optional.of(f);
    }

    Optional<FileId> resolvePath(StrataPath path) {
        return Optional.ofNullable(pathBindings.get(path));
    }

    /** Live (non-DELETED) file ids in this namespace. */
    List<FileId> liveFiles() {
        List<FileId> out = new ArrayList<>();
        for (Map.Entry<FileId, Records.FileRecord> e : files.entrySet()) {
            if (e.getValue().state() != FileState.DELETED) {
                out.add(e.getKey());
            }
        }
        return out;
    }

    /**
     * Returns the file id previously created by this opId, if any live (non-swept) record carries it.
     * Used for opId-keyed idempotency: a retried create with the same opId returns the same file id
     * without assigning a new one. Empty means no live record; the opId is fresh or tombstone-swept.
     */
    Optional<FileId> fileIdForOpId(long opIdMsb, long opIdLsb) {
        return Optional.ofNullable(opIdIndex.get(new OpIdKey(opIdMsb, opIdLsb)));
    }

    /** Whether a DELETED tombstone for {@code id} is still present (fences a recreate of the id). */
    boolean hasTombstone(FileId id) {
        Records.FileRecord f = files.get(id);
        return f != null && f.state() == FileState.DELETED;
    }

    long tombstoneDeletedAt(FileId id) {
        return tombstones.getOrDefault(id, -1L);
    }

    /** The per-file CAS version (0 at create, +1 per mutating record); -1 if the file is absent. */
    int version(FileId id) {
        return versions.getOrDefault(id, -1);
    }

    /** Tombstoned file ids whose deletion is at or before {@code cutoffMs} — bounded sweep candidates. */
    List<FileId> tombstonesDeletedAtOrBefore(long cutoffMs) {
        List<FileId> out = new ArrayList<>();
        for (Map.Entry<FileId, Long> e : tombstones.entrySet()) {
            if (e.getValue() <= cutoffMs) {
                out.add(e.getKey());
            }
        }
        return out;
    }

    /** Chunks (across this namespace's files) that have a replica on {@code nodeId} — derived index. */
    Set<ChunkId> chunksOn(int nodeId) {
        return Set.copyOf(nodeChunks.getOrDefault(nodeId, Set.of()));
    }

    int fileCount() {
        return files.size();
    }

    /* ---- internals ---- */

    private void mutate(FileId id, UnaryOperator<Records.FileRecord> fn) {
        Records.FileRecord f = files.get(id);
        if (f == null) {
            return; // a record for an absent file; the log is well-formed, but stay defensive
        }
        putFile(fn.apply(f));
    }

    private void putFile(Records.FileRecord updated) {
        Records.FileRecord old = files.put(updated.fileId(), updated);
        if (old != null) {
            removeFromNodeChunks(old);
        }
        addToNodeChunks(updated);
        Integer prior = versions.get(updated.fileId());
        versions.put(updated.fileId(), prior == null ? 0 : prior + 1);
    }

    // Open/sealed files contribute to node -> chunks; DELETING/DELETED files do not (their chunks are
    // being reclaimed and must not be repaired).
    private void addToNodeChunks(Records.FileRecord f) {
        if (f.state() == FileState.DELETING || f.state() == FileState.DELETED) {
            return;
        }
        for (Records.ChunkRecord c : f.chunks()) {
            ChunkId cid = f.chunkId(c.index());
            for (int nodeId : c.replicas()) {
                nodeChunks.computeIfAbsent(nodeId, k -> new HashSet<>()).add(cid);
            }
        }
    }

    private void removeFromNodeChunks(Records.FileRecord f) {
        for (Records.ChunkRecord c : f.chunks()) {
            ChunkId cid = f.chunkId(c.index());
            for (int nodeId : c.replicas()) {
                Set<ChunkId> s = nodeChunks.get(nodeId);
                if (s != null) {
                    s.remove(cid);
                    if (s.isEmpty()) {
                        nodeChunks.remove(nodeId);
                    }
                }
            }
        }
    }

    private static Records.FileRecord updateChunk(Records.FileRecord f, int index,
                                                  UnaryOperator<Records.ChunkRecord> fn) {
        List<Records.ChunkRecord> chunks = new ArrayList<>(f.chunks());
        for (int i = 0; i < chunks.size(); i++) {
            if (chunks.get(i).index() == index) {
                chunks.set(i, fn.apply(chunks.get(i)));
                break;
            }
        }
        return f.withChunks(chunks);
    }

    private static Records.FileRecord removeChunk(Records.FileRecord f, int index) {
        List<Records.ChunkRecord> chunks = new ArrayList<>(f.chunks());
        chunks.removeIf(c -> c.index() == index);
        return f.withChunks(chunks);
    }

    private static Records.ChunkRecord withoutReplica(Records.ChunkRecord c, int nodeId) {
        List<Integer> replicas = new ArrayList<>(c.replicas());
        replicas.remove((Integer) nodeId);
        return c.withReplicas(replicas);
    }

    private static Records.ChunkRecord withReplica(Records.ChunkRecord c, int nodeId) {
        if (c.replicas().contains(nodeId)) {
            return c;
        }
        List<Integer> replicas = new ArrayList<>(c.replicas());
        replicas.add(nodeId);
        return c.withReplicas(replicas);
    }
}
