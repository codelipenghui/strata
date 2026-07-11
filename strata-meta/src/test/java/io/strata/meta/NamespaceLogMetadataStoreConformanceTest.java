package io.strata.meta;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Runs the backend-neutral {@link MetadataStoreConformanceTest} against the namespace-log backend
 * (tech design §4.2 and §16): user file/path metadata flows through per-namespace metadata logs while the
 * node registry and sharding root delegate to a ZooKeeper root store. Multiple store handles share one
 * {@link NamespaceLogBackend}, modelling one metadata process serving the namespace. Handles are
 * non-owning views; the fixture closes the shared process backend exactly once.
 */
class NamespaceLogMetadataStoreConformanceTest extends MetadataStoreConformanceTest {

    @Test
    void facadeOwnershipClosesTheProcessBackendExactlyOnce() throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString());
            try {
                CloseCountingFileStore sharedFiles = new CloseCountingFileStore();
                NamespaceLogBackend shared = new NamespaceLogBackend(root, sharedFiles, false);
                NamespaceLogMetadataStore first = NamespaceLogMetadataStore.nonOwningView(shared);
                NamespaceLogMetadataStore second = NamespaceLogMetadataStore.nonOwningView(shared);

                first.close();
                second.close();
                assertEquals(0, sharedFiles.closeCount.get(),
                        "closing a request-scoped facade must not close the process backend");

                shared.close();
                shared.close();
                assertEquals(1, sharedFiles.closeCount.get(),
                        "the process owner closes its backend exactly once");

                CloseCountingFileStore ownedFiles = new CloseCountingFileStore();
                NamespaceLogBackend owned = new NamespaceLogBackend(root, ownedFiles, false);
                NamespaceLogMetadataStore owner = new NamespaceLogMetadataStore(owned);
                owner.close();
                owner.close();
                assertEquals(1, ownedFiles.closeCount.get(),
                        "the default facade owns and closes its backend exactly once");
            } finally {
                root.close();
            }
        }
    }

    @Override
    protected Backend startBackend() throws Exception {
        TestingServer zk = new TestingServer(true);
        ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString());
        TestNamespaceMetadataFileStore fileStore = new TestNamespaceMetadataFileStore();
        NamespaceLogBackend shared = new NamespaceLogBackend(root, fileStore, false);
        return new Backend() {
            @Override
            public MetadataStore openStore() {
                return NamespaceLogMetadataStore.nonOwningView(shared);
            }

            @Override
            public void close() throws Exception {
                shared.close();
                root.close();
                zk.close();
            }
        };
    }

    private static final class CloseCountingFileStore extends TestNamespaceMetadataFileStore {
        private final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }
    }
}
