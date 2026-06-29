package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * A durable, on-disk {@link NamespaceMetadataFileStore}: metadata-log and snapshot bytes are stored as
 * local files under {@code dir}, keyed by a {@link FileId}. This makes the namespace-log backend durable
 * across a single meta-node restart — recovery reads the manifest from the consensus root, then the
 * snapshot/log bytes from disk.
 *
 * <p>Cross-node failover requires the replicated-chunk store ({@code StrataSystemMetadataFileStore}),
 * which stores the same bytes as Strata chunks so a successor on another node can read them; that is the
 * remaining hardening (design §5, §16 Step 3, and the bootstrap open question §19). Durability here
 * relies on the page cache; an fsync-on-append pass is a follow-up for crash-consistency.
 */
final class LocalNamespaceMetadataFileStore implements NamespaceMetadataFileStore {
    private final Path dir;

    LocalNamespaceMetadataFileStore(Path dir) throws IOException {
        this.dir = dir;
        Files.createDirectories(dir);
    }

    @Override
    public FileId createLogFile(StrataNamespace ns, long generation) throws IOException {
        FileId id = SystemFileIds.of(ns, generation, 0);
        Files.write(logPath(id), new byte[0], StandardOpenOption.CREATE_NEW);
        return id;
    }

    @Override
    public void appendLog(FileId logFileId, byte[] frameBytes) throws IOException {
        Files.write(logPath(logFileId), frameBytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    @Override
    public byte[] readLog(FileId logFileId) throws IOException {
        Path p = logPath(logFileId);
        return Files.exists(p) ? Files.readAllBytes(p) : new byte[0];
    }

    @Override
    public FileId writeSnapshot(StrataNamespace ns, long generation, byte[] snapshotBytes) throws IOException {
        FileId id = SystemFileIds.of(ns, generation, 1);
        Files.write(snapPath(id), snapshotBytes, StandardOpenOption.CREATE_NEW);
        return id;
    }

    @Override
    public byte[] readSnapshot(FileId snapshotFileId) throws IOException {
        return Files.readAllBytes(snapPath(snapshotFileId));
    }

    @Override
    public void deleteFile(FileId fileId) throws IOException {
        Files.deleteIfExists(logPath(fileId));
        Files.deleteIfExists(snapPath(fileId));
    }

    private Path logPath(FileId id) {
        return dir.resolve(id + ".log");
    }

    private Path snapPath(FileId id) {
        return dir.resolve(id + ".snap");
    }
}
