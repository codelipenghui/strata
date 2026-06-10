package io.strata.format;

/** Raised when an on-disk structure fails magic/version/CRC validation. */
public class CorruptChunkException extends RuntimeException {
    public CorruptChunkException(String message) {
        super(message);
    }
}
