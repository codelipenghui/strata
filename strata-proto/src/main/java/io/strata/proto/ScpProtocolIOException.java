package io.strata.proto;

import java.io.IOException;

/** Internal checked marker for corrupt SCP frame bytes received from a peer. */
final class ScpProtocolIOException extends IOException {
    ScpProtocolIOException(String message) {
        super(message);
    }
}
