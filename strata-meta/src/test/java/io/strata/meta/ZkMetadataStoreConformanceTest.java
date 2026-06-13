package io.strata.meta;

import org.apache.curator.test.TestingServer;

class ZkMetadataStoreConformanceTest extends MetadataStoreConformanceTest {

    @Override
    protected Backend startBackend() throws Exception {
        TestingServer zk = new TestingServer(true);
        return new Backend() {
            @Override
            public MetadataStore openStore() {
                return new ZkMetadataStore(zk.getConnectString());
            }

            @Override
            public void close() throws Exception {
                zk.close();
            }
        };
    }
}
