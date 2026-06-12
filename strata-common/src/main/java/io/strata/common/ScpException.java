package io.strata.common;

/** Carries an SCP error code across layers; thrown by servers, reconstructed by clients. */
public class ScpException extends RuntimeException {
    private final ErrorCode code;
    private final long detail; // opcode-documented numeric detail (e.g. expectedEndOffset, currentFenceEpoch)

    public ScpException(ErrorCode code, String message) {
        this(code, message, 0);
    }

    public ScpException(ErrorCode code, String message, long detail) {
        this(code, message, detail, null);
    }

    public ScpException(ErrorCode code, String message, Throwable cause) {
        this(code, message, 0, cause);
    }

    public ScpException(ErrorCode code, String message, long detail, Throwable cause) {
        super(code + ": " + message, cause);
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
