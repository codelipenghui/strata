package io.strata.common;

/** SCP error codes — append-only namespace (tech design §10.5 + v0 additions). */
public enum ErrorCode {
    OK(0, false),
    UNKNOWN_OPCODE(1, false),
    UNSUPPORTED_VERSION(2, false),
    FENCED_EPOCH(3, false),
    OFFSET_GAP(4, true),
    CHUNK_NOT_FOUND(5, false),
    CHUNK_SEALED(6, false),
    CHUNK_ALREADY_EXISTS(7, false),
    OUT_OF_SPACE(8, false),
    CRC_MISMATCH(9, false),
    NOT_REGISTERED(10, false),
    LEASE_EXPIRED(11, false),
    THROTTLED(12, true),
    CORRUPT_CHUNK(13, false),
    INTERNAL(14, true),
    // v0 additions (append-only)
    NOT_LEADER(15, true),
    NO_CAPACITY(16, true),
    FILE_NOT_FOUND(17, false),
    FILE_SEALED(18, false),
    PRECONDITION_FAILED(19, false);

    public final short code;
    public final boolean retriable;

    ErrorCode(int code, boolean retriable) {
        this.code = (short) code;
        this.retriable = retriable;
    }

    private static final ErrorCode[] BY_CODE;
    static {
        int max = 0;
        for (ErrorCode e : values()) max = Math.max(max, e.code);
        BY_CODE = new ErrorCode[max + 1];
        for (ErrorCode e : values()) BY_CODE[e.code] = e;
    }

    public static ErrorCode fromCode(short code) {
        if (code < 0 || code >= BY_CODE.length || BY_CODE[code] == null) {
            return INTERNAL; // unknown future code: treat as retriable internal
        }
        return BY_CODE[code];
    }
}
