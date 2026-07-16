package io.strata.meta;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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

    @Test
    void backendCloseStillClosesItsRootWhenFileStoreCloseFails() {
        AtomicInteger rootCloseCount = new AtomicInteger();
        MetadataStore root = closeCountingStore(rootCloseCount);
        CloseCountingFileStore files = new CloseCountingFileStore(true);
        NamespaceLogBackend backend = new NamespaceLogBackend(root, files, true);

        assertDoesNotThrow(backend::close,
                "a file-store close failure must not prevent the owned root from closing");
        assertDoesNotThrow(backend::close, "backend close remains idempotent after the failure path");
        assertEquals(1, files.closeCount.get(), "the file store is closed exactly once");
        assertEquals(1, rootCloseCount.get(), "the owned root is closed exactly once");
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

    private static MetadataStore closeCountingStore(AtomicInteger closeCount) {
        return (MetadataStore) Proxy.newProxyInstance(
                MetadataStore.class.getClassLoader(),
                new Class<?>[]{MetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        closeCount.incrementAndGet();
                        return null;
                    }
                    throw new AssertionError("unexpected root operation during backend close: " + method.getName());
                });
    }

    private static final class CloseCountingFileStore extends TestNamespaceMetadataFileStore {
        private final AtomicInteger closeCount = new AtomicInteger();
        private final boolean failOnClose;

        private CloseCountingFileStore() {
            this(false);
        }

        private CloseCountingFileStore(boolean failOnClose) {
            this.failOnClose = failOnClose;
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
            if (failOnClose) {
                throw new IllegalStateException("injected file-store close failure");
            }
        }
    }
}
