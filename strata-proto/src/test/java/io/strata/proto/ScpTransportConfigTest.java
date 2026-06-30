package io.strata.proto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression guard: confirms that STRATA_SCP_* env defaults are unchanged when the env is unset.
 * These are static-init constants; the assertions verify the defaults held at class-load time.
 */
class ScpTransportConfigTest {

    @Test
    void inflightDefaultsUnchanged() {
        // MAX_PENDING_REQUESTS default must remain 1024 when STRATA_SCP_MAX_PENDING_REQUESTS is unset.
        assertEquals(1024, ScpClient.maxPendingRequests());
    }

    @Test
    void maxFrameBytesDefaultUnchanged() {
        // MAX_FRAME_BYTES default must remain 64 MiB when STRATA_MAX_FRAME_BYTES is unset.
        assertEquals(64 * 1024 * 1024, FrameIO.MAX_FRAME_BYTES);
    }
}
