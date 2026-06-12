package io.strata.common;

/** Overflow-checked arithmetic over untrusted wire/disk values; failures are typed SCP errors. */
public final class Checks {
    private Checks() {}

    /** {@code left + right}, rejecting overflow as CORRUPT_CHUNK ("&lt;what&gt; overflow"). */
    public static long checkedAdd(long left, long right, String what) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException e) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, what + " overflow");
        }
    }

    /** Folds an untrusted chunk length into a running file length, rejecting negatives and overflow. */
    public static long addChunkLength(long total, long length) {
        if (length < 0) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "negative chunk length " + length);
        }
        return checkedAdd(total, length, "file length");
    }
}
