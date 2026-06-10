package io.strata.common;

/** Carries an SCP error code across layers; thrown by servers, reconstructed by clients. */
public class ScpException extends RuntimeException {
    private final ErrorCode code;
    private final long detail; // opcode-documented numeric detail (e.g. expectedEndOffset, currentFenceEpoch)

    public ScpException(ErrorCode code, String message) {
        this(code, message, 0);
    }

    public ScpException(ErrorCode code, String message, long detail) {
        super(code + ": " + message);
        this.code = code;
        this.detail = detail;
    }

    public ErrorCode code() {
        return code;
    }

    public long detail() {
        return detail;
    }

    public boolean retriable() {
        return code.retriable;
    }
}
