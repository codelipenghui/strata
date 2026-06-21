package io.strata.meta;

import org.apache.curator.test.TestingServer;

/**
 * Runs the backend-neutral {@link MetadataStoreConformanceTest} against the namespace-log backend
 * (design §8, §16 Step 3): user file/path metadata flows through per-namespace metadata logs while the
 * node registry and sharding root delegate to a ZooKeeper root store. Multiple store handles share one
 * {@link NamespaceLogBackend}, modelling one metadata process serving the namespace.
 */
class NamespaceLogMetadataStoreConformanceTest extends MetadataStoreConformanceTest {

    @Override
    protected Backend startBackend() throws Exception {
        TestingServer zk = new TestingServer(true);
        ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString());
        TestNamespaceMetadataFileStore fileStore = new TestNamespaceMetadataFileStore();
        NamespaceLogBackend shared = new NamespaceLogBackend(root, fileStore, false);
        return new Backend() {
            @Override
            public MetadataStore openStore() {
                return new NamespaceLogMetadataStore(shared);
            }

            @Override
            public void close() throws Exception {
                shared.close();
                root.close();
                zk.close();
            }
        };
    }
}
