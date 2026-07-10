package io.strata.common;

/** Local SCP response-decoding failure, distinct from transport unreachability. */
public final class ScpProtocolException extends ScpException {
    public ScpProtocolException(String message) {
        super(ErrorCode.INTERNAL, message);
    }

    public ScpProtocolException(String message, Throwable cause) {
        super(ErrorCode.INTERNAL, message, cause);
    }
}
