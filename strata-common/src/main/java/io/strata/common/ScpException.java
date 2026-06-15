package io.strata.common;

import java.util.concurrent.CompletionException;

/** Carries an SCP error code across layers; thrown by servers, reconstructed by clients. */
public class ScpException extends RuntimeException {
    private final ErrorCode code;
    private final long detail; // opcode-documented numeric detail (e.g. expectedEndOffset, currentFenceEpoch)
    private final String leaderHint; // NOT_LEADER: the current leader's endpoint, so the client can redirect

    public ScpException(ErrorCode code, String message) {
        this(code, message, 0, null, null);
    }

    public ScpException(ErrorCode code, String message, long detail) {
        this(code, message, detail, null, null);
    }

    public ScpException(ErrorCode code, String message, Throwable cause) {
        this(code, message, 0, null, cause);
    }

    public ScpException(ErrorCode code, String message, long detail, Throwable cause) {
        this(code, message, detail, null, cause);
    }

    public ScpException(ErrorCode code, String message, long detail, String leaderHint) {
        this(code, message, detail, leaderHint, null);
    }

    public ScpException(ErrorCode code, String message, long detail, String leaderHint, Throwable cause) {
        super(code + ": " + message, cause);
        this.code = code;
        this.detail = detail;
        this.leaderHint = leaderHint;
    }

    public ErrorCode code() {
        return code;
    }

    public long detail() {
        return detail;
    }

    /** For NOT_LEADER: the current leader's endpoint the client should redirect to, or null if unknown. */
    public String leaderHint() {
        return leaderHint;
    }

    public boolean retriable() {
        return code.retriable;
    }

    /** Strips the CompletionException wrappers a CompletableFuture adds, returning the real cause. */
    public static Throwable rootCause(Throwable t) {
        Throwable cause = t;
        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }
}
