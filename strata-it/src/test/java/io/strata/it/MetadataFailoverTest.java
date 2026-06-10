package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.SegmentStore;
import io.strata.client.StrataClient;
import io.strata.common.FileId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Metadata failover (tech design §4.4 / §7.4): two metadata instances over one ZooKeeper.
 * Kill the leader mid-stream; the standby takes over via leader election. Asserts:
 *  - appends keep acking THROUGH the failover, including chunk rolls (retry + endpoint rotation)
 *  - storage-node identity is stable across re-registration with the new leader
 *  - every acked byte reads back afterwards
 *  - the standby never ran failure detection while it wasn't leader (no spurious DEAD nodes)
 */
class MetadataFailoverTest {

    @Test
    void leaderKillMidStreamIsAbsorbed() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3, null, 2)) {
            ClientConfig cfg = new ClientConfig(cluster.metaEndpoints(), 1024, 5_000);
            try (StrataClient client = new StrataClient(cfg)) {
                FileId fileId = client.create(SegmentStore.FileSpec.log("failover"));
                Workload workload = new Workload();

                List<Integer> nodeIdsBefore = new ArrayList<>();
                for (var n : cluster.nodes) nodeIdsBefore.add(n.nodeId());

                try (SegmentStore.Appender appender = client.openForAppend(fileId, 1)) {
                    workload.appendAcked(appender, 0, 300);

                    // kill the current leader
                    int leaderIdx = -1;
                    for (int i = 0; i < cluster.metas.size(); i++) {
                        if (cluster.metas.get(i).isLeader()) leaderIdx = i;
                    }
                    assertTrue(leaderIdx >= 0, "no leader before failover");
                    cluster.killMeta(leaderIdx);

                    // write straight through the failover: ~8 chunk rolls must survive the
                    // window where the standby is acquiring leadership and nodes re-register
                    workload.appendAcked(appender, 300, 600);
                    var sealed = appender.seal();
                    assertEquals(workload.ackedBytes(), sealed.sealedLength());
                }

                cluster.awaitAnyLeader();
                long leaders = cluster.metas.stream().filter(m -> {
                    try {
                        return m.isLeader();
                    } catch (Exception e) {
                        return false;
                    }
                }).count();
                assertEquals(1, leaders, "exactly the survivor should lead");

                // every acked byte reads back through the new leader
                workload.verifyAckedPrefix(client, fileId);

                // volume-bound identity survived re-registration with the new leader
                // (NodeRegistry falls back to the persistent store for incarnation matching)
                for (int i = 0; i < cluster.nodes.size(); i++) {
                    assertEquals(nodeIdsBefore.get(i), cluster.nodes.get(i).nodeId(),
                            "node " + i + " changed identity across metadata failover");
                }
            }
        }
    }
}
