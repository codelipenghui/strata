package io.strata.meta;

/** Signals that a namespace-manifest CAS was lost or its commit outcome is ambiguous. */
final class ManifestCasLostException extends IllegalStateException {
    ManifestCasLostException(String message) {
        super(message);
    }
}
