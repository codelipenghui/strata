package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory {@link NamespaceMetadataFileStore} for tests (design §8 "TestNamespaceMetadataFileStore"):
 * a deterministic backend for recovery and append-ordering validation, with no Strata data nodes.
 */
final class TestNamespaceMetadataFileStore implements NamespaceMetadataFileStore {
    private final Map<FileId, ByteArrayOutputStream> logs = new ConcurrentHashMap<>();
    private final Map<FileId, byte[]> snapshots = new ConcurrentHashMap<>();
    private final AtomicLong ids = new AtomicLong(1);

    @Override
    public FileId createLogFile(StrataNamespace ns, long generation) {
        FileId id = nextId();
        logs.put(id, new ByteArrayOutputStream());
        return id;
    }

    @Override
    public synchronized void appendLog(FileId logFileId, byte[] frameBytes) {
        logs.computeIfAbsent(logFileId, k -> new ByteArrayOutputStream()).writeBytes(frameBytes);
    }

    @Override
    public synchronized byte[] readLog(FileId logFileId) {
        ByteArrayOutputStream b = logs.get(logFileId);
        return b == null ? new byte[0] : b.toByteArray();
    }

    @Override
    public FileId writeSnapshot(StrataNamespace ns, long generation, byte[] snapshotBytes) {
        FileId id = nextId();
        snapshots.put(id, snapshotBytes.clone());
        return id;
    }

    @Override
    public byte[] readSnapshot(FileId snapshotFileId) {
        byte[] b = snapshots.get(snapshotFileId);
        if (b == null) {
            throw new IllegalStateException("no snapshot file " + snapshotFileId);
        }
        return b.clone();
    }

    @Override
    public void deleteFile(FileId fileId) {
        logs.remove(fileId);
        snapshots.remove(fileId);
    }

    /** Corrupts the tail of a log file (simulates a torn append) for recovery tests. */
    synchronized void appendRawToLog(FileId logFileId, byte[] raw) {
        logs.computeIfAbsent(logFileId, k -> new ByteArrayOutputStream()).writeBytes(raw);
    }

    int liveFileCount() {
        return logs.size() + snapshots.size();
    }

    private FileId nextId() {
        // High bits (0x5354524D45544100L = "STRMETA\0") keep system-file ids visually distinct from
        // user-file ids (low positive longs) in test output; the low byte is a unique counter.
        return FileId.of(0x5354524D45544100L | ids.getAndIncrement());
    }
}
