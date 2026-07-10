package io.strata.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FsyncTest {

    @TempDir
    Path dir;

    @Test
    void ensureDirectoryDurableCreatesLeafAndForcesItsParent() throws Exception {
        Path child = dir.resolve("node-1");
        Path expectedParent = dir.toAbsolutePath().normalize();
        List<Path> forced = new ArrayList<>();

        Fsync.ensureDirectoryDurable(child, path -> {
            assertTrue(Files.isDirectory(child), "the child must exist before its parent is forced");
            forced.add(path.toAbsolutePath().normalize());
        });

        assertTrue(Files.isDirectory(child));
        assertEquals(List.of(expectedParent), forced);
    }

    @Test
    void ensureDirectoryDurableRequiresAnExistingParent() {
        Path missingParent = dir.resolve("missing");
        Path child = missingParent.resolve("node-1");

        assertThrows(IOException.class,
                () -> Fsync.ensureDirectoryDurable(child, ignored -> {}));

        assertFalse(Files.exists(missingParent),
                "durable creation must not silently create an unanchored ancestor chain");
    }

    @Test
    void ensureDirectoryDurableRetriesParentForceForAnExistingDirectory() throws Exception {
        Path child = dir.resolve("node-1");
        Path expectedParent = dir.toAbsolutePath().normalize();
        AtomicInteger attempts = new AtomicInteger();
        Fsync.DirectorySyncer syncer = path -> {
            assertEquals(expectedParent, path.toAbsolutePath().normalize());
            if (attempts.incrementAndGet() == 1) {
                throw new IOException("injected parent fsync failure");
            }
        };

        assertThrows(IOException.class, () -> Fsync.ensureDirectoryDurable(child, syncer));
        assertTrue(Files.isDirectory(child), "the failed first attempt may leave the child in place");

        Fsync.ensureDirectoryDurable(child, syncer);
        assertEquals(2, attempts.get());
    }
}
