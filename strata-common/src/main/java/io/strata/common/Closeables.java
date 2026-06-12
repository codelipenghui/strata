package io.strata.common;

import java.io.IOException;

/** Accumulating close-failure plumbing shared by multi-resource close() paths. */
public final class Closeables {
    private Closeables() {}

    /** Folds a close failure into the accumulator: the first failure wins, later ones are suppressed. */
    public static Throwable suppress(Throwable failure, Throwable next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
    }

    /** Rethrows an accumulated close failure; IOException and RuntimeException pass through unwrapped. */
    public static void throwIfFailed(Throwable failure) throws IOException {
        if (failure == null) {
            return;
        }
        if (failure instanceof IOException e) {
            throw e;
        }
        if (failure instanceof RuntimeException e) {
            throw e;
        }
        throw new IOException(failure);
    }
}
