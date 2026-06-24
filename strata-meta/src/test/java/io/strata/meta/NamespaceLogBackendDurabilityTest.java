package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The namespace-log backend over a durable on-disk file store survives a meta-node restart: a fresh
 * backend over the same data directory and ZooKeeper root recovers each namespace's files from the
 * published manifest plus the on-disk snapshot and open-log tail (design §13).
 */
class NamespaceLogBackendDurabilityTest {

    @TempDir
    Path dir;

    @Test
    void backendRecoversFilesFromDiskAndManifestAfterRestart() throws Exception {
        StrataNamespace ns = StrataNamespace.of("tenant-a");
        StrataPath path = StrataPath.of("/logs/seg-0");
        FileId fileId = FileId.of(7);

        try (TestingServer zk = new TestingServer(true)) {
            // session 1: create a file through the namespace-log backend (durable to disk + ZK manifest)
            try (ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
                LocalNamespaceMetadataFileStore fileStore = new LocalNamespaceMetadataFileStore(dir);
                NamespaceLogBackend backend = new NamespaceLogBackend(root, fileStore, false);
                NamespaceLogMetadataStore store = new NamespaceLogMetadataStore(backend);
                store.createFile(new Records.FileRecord(fileId, ns, path, 3, 2, true,
                        FileState.OPEN, 1_000, List.of(), 1, 1));
                assertEquals(fileId, store.resolvePath(ns, path).orElseThrow());
                backend.close();
            }

            // session 2: a brand-new backend over the SAME dir + ZK recovers the file.
            try (ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
                LocalNamespaceMetadataFileStore fileStore = new LocalNamespaceMetadataFileStore(dir);
                NamespaceLogBackend backend = new NamespaceLogBackend(root, fileStore, false);
                NamespaceLogMetadataStore store = new NamespaceLogMetadataStore(backend);
                // an ns-scoped read recovers the repo from the persisted manifest/snapshot/log.
                assertEquals(fileId, store.resolvePath(ns, path).orElseThrow(),
                        "path binding survived restart via the on-disk log + ZK manifest");
                assertEquals(List.of(fileId), store.listFiles(ns));
                assertTrue(store.getFile(fileId).isPresent(), "file record recovered after restart");
                backend.close();
            }
        }
    }

    @Test
    void getFileByIdResolvesAnUntouchedNamespaceAfterRestart() throws Exception {
        // openById right after a restart hits no namespace-scoped op first, so the file's namespace has
        // not been loaded. Eager recovery must still resolve it (without it, getFile would return empty).
        StrataNamespace ns = StrataNamespace.of("tenant-b");
        StrataPath path = StrataPath.of("/logs/seg-9");
        FileId fileId = FileId.of(9);

        try (TestingServer zk = new TestingServer(true)) {
            try (ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
                LocalNamespaceMetadataFileStore fileStore = new LocalNamespaceMetadataFileStore(dir);
                NamespaceLogBackend backend = new NamespaceLogBackend(root, fileStore, false);
                backend.createFile(new Records.FileRecord(fileId, ns, path, 3, 2, true,
                        FileState.OPEN, 1_000, List.of(), 1, 1));
                backend.close();
            }

            try (ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {
                LocalNamespaceMetadataFileStore fileStore = new LocalNamespaceMetadataFileStore(dir);
                NamespaceLogBackend backend = new NamespaceLogBackend(root, fileStore, false);
                assertTrue(backend.getFile(fileId).isPresent(),
                        "getFile(fileId)/openById resolves after restart via eager namespace recovery");
                backend.close();
            }
        }
    }
}
