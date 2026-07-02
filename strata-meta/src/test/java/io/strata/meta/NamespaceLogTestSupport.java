package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.test.TestingServer;

import java.util.List;

/**
 * Shared construction helpers for namespace-log backend tests, mirroring how
 * {@link NamespaceLogMetadataStoreConformanceTest} and {@link NamespaceMetadataLogRepositoryTest} build
 * their root store ({@link ZkMetadataStore} over a {@link org.apache.curator.test.TestingServer}) and
 * in-memory metadata file store ({@link TestNamespaceMetadataFileStore}). Keeping it in one place lets
 * the concurrency test reuse the proven, manifest-capable construction.
 */
final class NamespaceLogTestSupport {

    private NamespaceLogTestSupport() {
    }

    /** A fresh ZooKeeper-backed root store (manifest/epoch capable). Caller closes the returned server. */
    static TestingServer testingServer() throws Exception {
        return new TestingServer(true);
    }

    /** The root {@link MetadataStore} over {@code zk}, exactly as the conformance/repository tests build it. */
    static ZkMetadataStore inMemoryRoot(TestingServer zk) throws Exception {
        return new ZkMetadataStore(zk.getConnectString());
    }

    /** The in-memory namespace-metadata file store the conformance/repository tests use. */
    static TestNamespaceMetadataFileStore inMemoryFileStore() {
        return new TestNamespaceMetadataFileStore();
    }

    /**
     * A unique OPEN {@link Records.FileRecord} for {@code (fileId, ns, path)} with distinct idempotency
     * keys derived from the file id, so two records in different namespaces never collide.
     */
    static Records.FileRecord fileRecord(FileId fileId, StrataNamespace ns, StrataPath path) {
        return new Records.FileRecord(fileId, ns, path, 3, 2, true,
                FileState.OPEN, 1_000, List.of(), fileId.id(), 0L);
    }
}
