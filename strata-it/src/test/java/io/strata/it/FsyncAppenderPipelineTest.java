package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Appender-level group-commit diagnostic: forces per node must be << appends. */
class FsyncAppenderPipelineTest {

    @Test
    void appenderPipelineCoalescesForcesOnAllReplicas() throws Exception {
        int appends = 400;
        try (MiniCluster cluster = new MiniCluster(3);
             StrataClient client = StrataClient.connect(
                     ClientConfig.of(cluster.metaEndpoint()).withChunkRollBytes(64L << 20))) {

            FileId fileId = client.create(new StrataClient.FileSpec("test", "/diag", (byte) 0, (byte) 0, (byte) 1)).id();
            byte[] payload = new byte[512];

            try (StrataFile.Appender appender = client.openById(fileId).openForAppend(1)) {
                long start = System.nanoTime();
                List<CompletableFuture<StrataFile.AppendAck>> futures = new ArrayList<>(appends);
                for (int i = 0; i < appends; i++) {
                    futures.add(appender.append(ByteBuffer.wrap(payload)));
                }
                CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(60, TimeUnit.SECONDS);
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;

                StringBuilder sb = new StringBuilder();
                long maxForces = 0;
                for (var node : cluster.nodes) {
                    long f = node.store().fsyncForceCount();
                    maxForces = Math.max(maxForces, f);
                    sb.append("node").append(node.nodeId()).append("=").append(f).append(" ");
                }
                System.out.printf("appender pipeline: %d fsync appends in %d ms, forces: %s%n",
                        appends, elapsedMs, sb);
                appender.seal();

                assertTrue(maxForces < appends / 2,
                        "appender path defeats group commit: " + sb + " for " + appends + " appends");
            }
        }
    }
}
