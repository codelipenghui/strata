package io.strata.common;

/** Local SCP transport or connection-lifecycle failure, distinct from server-returned INTERNAL. */
public final class ScpConnectionException extends ScpException {
    public ScpConnectionException(String message) {
        super(ErrorCode.INTERNAL, message);
    }

    public ScpConnectionException(String message, Throwable cause) {
        super(ErrorCode.INTERNAL, message, cause);
    }
}
