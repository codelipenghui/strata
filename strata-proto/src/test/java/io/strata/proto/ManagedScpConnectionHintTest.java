package io.strata.proto;

import io.strata.common.ConnectionPolicy;
import io.strata.common.ScpException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A malformed leader-redirect hint (from a misconfigured peer) must be dropped so the next connect
 * falls back to the configured endpoint list — not stick and get re-parsed on every retry until the
 * call deadline.
 */
class ManagedScpConnectionHintTest {

    @Test
    void malformedLeaderHintFallsBackToConfiguredEndpoint() {
        try (ManagedScpConnection conn = new ManagedScpConnection(
                List.of("127.0.0.1:1"), ConnectionPolicy.DEFAULT, ScpClient.KIND_BROKER,
                "test", "controller endpoint", true, false)) {
            byte[] header = new BufWriter().noTags().toBytes();
            conn.preferEndpoint("bad-host:notaport");  // unparseable hint from a misconfigured peer

            // First attempt: the bad hint fails to parse but degrades to a retriable error.
            ScpException first = assertThrows(ScpException.class,
                    () -> conn.call(Opcode.PING, header, null, 300));
            assertTrue(first.retriable(), "a malformed hint must be retriable, got " + first.code());

            // Second attempt: the preference was dropped, so we fall back to the configured endpoint
            // (127.0.0.1:1) rather than re-parsing the same garbage.
            ScpException second = assertThrows(ScpException.class,
                    () -> conn.call(Opcode.PING, header, null, 300));
            assertTrue(second.getMessage().contains("127.0.0.1:1"),
                    "after a bad hint the next connect must use the configured endpoint, got: "
                            + second.getMessage());
        }
    }
}
