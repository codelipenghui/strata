package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.common.FileId;
import io.strata.meta.MetadataService;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * With multiple metadata replicas, a client configured with ONLY a standby endpoint must still
 * reach the leader: the standby returns the leader's endpoint as a {@code NOT_LEADER} redirect hint
 * and the client follows it. Without the hint the client would blind-rotate within its single
 * configured endpoint (the standby) and could never reach a leader that isn't in its list — so a
 * successful create proves the redirect path end to end.
 */
class LeaderRedirectTest {

    @Test
    void clientWithOnlyAStandbyReachesLeaderViaHint() throws Exception {
        try (MiniCluster cluster = new MiniCluster(0, null, 3)) {
            cluster.awaitAnyLeader();

            MetadataService standby = cluster.metas.stream()
                    .filter(m -> !m.isLeader())
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("expected a standby among 3 metas"));

            ClientConfig cfg = new ClientConfig(List.of(standby.endpoint()), 1L << 20, 10_000);
            try (StrataClient client = StrataClient.connect(cfg)) {
                // Both creates are leader-only mutations served through the redirected connection.
                FileId a = client.create(StrataClient.FileSpec.log("redirect", "/via-hint-a")).id();
                FileId b = client.create(StrataClient.FileSpec.log("redirect", "/via-hint-b")).id();
                assertNotNull(a);
                assertNotNull(b);
                assertNotEquals(a, b);
            }
        }
    }
}
