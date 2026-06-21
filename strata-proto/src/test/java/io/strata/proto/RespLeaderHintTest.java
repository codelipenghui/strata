package io.strata.proto;

import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The error wire format carries an optional leader-endpoint hint (tagged field) so a client that
 * lands on a standby can redirect straight to the leader instead of blind-rotating its endpoints.
 */
class RespLeaderHintTest {

    private static ScpException roundTrip(byte[] errorBytes) {
        try {
            Resp.check(ByteBuffer.wrap(errorBytes));
            throw new AssertionError("Resp.check did not throw on an error header");
        } catch (ScpException e) {
            return e;
        }
    }

    @Test
    void carriesLeaderHintOnNotLeader() {
        ScpException e = roundTrip(
                Resp.error(ErrorCode.NOT_LEADER, "not the controller leader", 0, "10.0.0.5:9200"));
        assertEquals(ErrorCode.NOT_LEADER, e.code());
        assertEquals("10.0.0.5:9200", e.leaderHint());
        assertEquals(0, e.detail());
    }

    @Test
    void carriesBothDetailAndLeaderHint() {
        ScpException e = roundTrip(Resp.error(ErrorCode.NOT_LEADER, "x", 42, "host-7:9200"));
        assertEquals(42, e.detail());
        assertEquals("host-7:9200", e.leaderHint());
    }

    @Test
    void nullOrBlankHintLeavesItNull() {
        assertNull(roundTrip(Resp.error(ErrorCode.NOT_LEADER, "x", 0, null)).leaderHint());
        assertNull(roundTrip(Resp.error(ErrorCode.NOT_LEADER, "x", 0, "  ")).leaderHint());
    }

    @Test
    void legacyThreeArgErrorHasNoHint() {
        ScpException e = roundTrip(Resp.error(ErrorCode.INTERNAL, "boom", 7));
        assertEquals(7, e.detail());
        assertNull(e.leaderHint());
    }
}
