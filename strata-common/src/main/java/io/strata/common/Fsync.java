package io.strata.common;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/** Directory-level durability for create, rename, and unlink operations. */
public final class Fsync {
    private Fsync() {}

    @FunctionalInterface
    interface DirectorySyncer {
        void force(Path dir) throws IOException;
    }

    /**
     * Ensures one directory exists and forces its parent so the directory entry is crash-durable.
     * The parent must already exist and be durably anchored; callers must not silently create an
     * unanchored ancestor chain. Existing directories have their parent forced again for retry safety.
     */
    public static void ensureDirectoryDurable(Path directory) throws IOException {
        ensureDirectoryDurable(directory, Fsync::forceDirectory);
    }

    static void ensureDirectoryDurable(Path directory, DirectorySyncer directorySyncer) throws IOException {
        Path normalized = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
        Objects.requireNonNull(directorySyncer, "directorySyncer");
        try {
            Files.createDirectory(normalized);
        } catch (FileAlreadyExistsException e) {
            if (!Files.isDirectory(normalized)) {
                throw e;
            }
        }
        Path parent = normalized.getParent();
        if (parent != null) {
            directorySyncer.force(parent);
        }
    }

    /** Opens {@code dir} read-only and forces its directory metadata to stable storage. */
    public static void forceDirectory(Path dir) throws IOException {
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        }
    }
}
