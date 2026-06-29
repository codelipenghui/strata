package io.strata.common;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Directory-level fsync: makes a rename/unlink within {@code dir} durable across a crash. */
public final class Fsync {
    private Fsync() {}

    /** Opens {@code dir} read-only and forces its directory metadata to stable storage. */
    public static void forceDirectory(Path dir) throws IOException {
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        }
    }
}
