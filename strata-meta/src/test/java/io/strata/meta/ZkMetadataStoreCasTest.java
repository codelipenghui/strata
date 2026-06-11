package io.strata.meta;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Node-record writes are CAS (split-brain guard): a deposed leader holding a stale version must
 * lose against the new leader's write — unconditional node writes let a dead leader's expire
 * scan overwrite REGISTERED with DEAD.
 */
class ZkMetadataStoreCasTest {

    @Test
    void staleNodeWriteLosesCas() throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            // two store handles = two metadata service instances
            try (ZkMetadataStore leaderA = new ZkMetadataStore(zk.getConnectString());
                 ZkMetadataStore leaderB = new ZkMetadataStore(zk.getConnectString())) {

                Records.NodeRecord node = new Records.NodeRecord(7, 1L, 2L, List.of("h:9000"),
                        "z", "r", "host7", (byte) 0, 1L << 30, Records.NodeState.REGISTERED);
                assertTrue(leaderA.putNode(node, -1), "create must succeed");
                assertFalse(leaderA.putNode(node, -1), "double-create must fail");

                int v0 = leaderA.getNode(7).orElseThrow().version();

                // new leader B re-registers the node (bumps the version)
                assertTrue(leaderB.putNode(node.withState(Records.NodeState.REGISTERED), v0));

                // deposed leader A, still holding v0, tries to mark it DEAD — must lose
                assertFalse(leaderA.putNode(node.withState(Records.NodeState.DEAD), v0),
                        "stale-version write must be rejected");

                assertEquals(Records.NodeState.REGISTERED,
                        leaderA.getNode(7).orElseThrow().value().state(),
                        "the new leader's state must survive");
            }
        }
    }
}
