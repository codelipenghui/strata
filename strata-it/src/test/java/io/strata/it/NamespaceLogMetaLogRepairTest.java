package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ChunkState;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.meta.Records;
import io.strata.meta.ZkMetadataStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Chaos: the metadata log is itself replicated Strata data, so when a data node holding metadata-log
 * chunks dies, those chunks must be re-replicated back to the replication factor — exactly like user
 * data. This proves the reserved system namespace is covered by the repair engine (the metadata of the
 * metadata is not a single point of failure). A 4-node cluster (RF 3 + one spare) lets repair restore RF.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NamespaceLogMetaLogRepairTest {

    private static final StrataNamespace SYSTEM = StrataNamespace.of("strata-meta");

    private MiniCluster cluster;

    @BeforeAll
    void setup() throws Exception {
        System.setProperty("strata.controller.backend", "namespace-log");
        System.setProperty("strata.controller.log.rf", "3");
        System.setProperty("strata.controller.log.ack", "2");
        cluster = new MiniCluster(4);
    }

    @AfterAll
    void teardown() throws Exception {
        try {
            if (cluster != null) {
                cluster.close();
            }
        } finally {
            System.clearProperty("strata.controller.backend");
            System.clearProperty("strata.controller.log.rf");
            System.clearProperty("strata.controller.log.ack");
        }
    }

    @Test
    void metadataLogChunksAreRepairedWhenADataNodeDies() throws Exception {
        // write a user file — its metadata is stored as replicated (RF 3) metadata-log Strata chunks
        try (StrataClient client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()))) {
            FileId fileId = client.create(StrataClient.FileSpec.log("tenant-a", "/topic-0")).id();
            try (StrataFile.Appender appender = client.openById(StrataNamespace.of("tenant-a"), fileId).openForAppend()) {
                appender.append(ByteBuffer.wrap(new byte[8_192])).get();
                appender.seal();
            }
        }

        // kill a data node that holds a sealed metadata-log chunk
        int deadNodeId;
        try (ZkMetadataStore root = new ZkMetadataStore(cluster.zk.getConnectString())) {
            deadNodeId = aSealedMetaLogReplica(root);
        }
        cluster.killNode(nodeIndexOf(deadNodeId));

        // the controller re-replicates the metadata-log chunks back to RF, off the dead node
        awaitMetaLogRepairedOff(deadNodeId);

        // the user file's metadata is still served by the cluster
        try (StrataClient client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()))) {
            assertEquals("tenant-a", client.open("tenant-a", "/topic-0").namespace().value());
        }
    }

    private int aSealedMetaLogReplica(ZkMetadataStore root) throws Exception {
        for (FileId sys : root.listFiles(SYSTEM)) {
            Records.FileRecord record = root.getFile(SYSTEM, sys).orElseThrow().value();
            for (Records.ChunkRecord chunk : record.chunks()) {
                if (chunk.state() == ChunkState.SEALED && !chunk.replicas().isEmpty()) {
                    return chunk.replicas().get(0);
                }
            }
        }
        throw new IllegalStateException("no sealed metadata-log chunk found to fault");
    }

    private int nodeIndexOf(int nodeId) {
        for (int i = 0; i < cluster.nodes.size(); i++) {
            if (cluster.nodes.get(i).nodeId() == nodeId) {
                return i;
            }
        }
        throw new IllegalStateException("node " + nodeId + " not in cluster");
    }

    private void awaitMetaLogRepairedOff(int deadNodeId) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            boolean healthy = true;
            try (ZkMetadataStore root = new ZkMetadataStore(cluster.zk.getConnectString())) {
                for (FileId sys : root.listFiles(SYSTEM)) {
                    var record = root.getFile(SYSTEM, sys);
                    if (record.isEmpty()) {
                        continue; // compacted/deleted system file
                    }
                    for (Records.ChunkRecord chunk : record.get().value().chunks()) {
                        if (chunk.state() == ChunkState.SEALED
                                && (chunk.replicas().contains(deadNodeId) || chunk.replicas().size() < 3)) {
                            healthy = false;
                        }
                    }
                }
            }
            if (healthy) {
                return;
            }
            Thread.sleep(250);
        }
        fail("metadata-log chunks were not repaired off the dead node within 30s");
    }
}
