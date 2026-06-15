package io.strata.common;

/** File lifecycle states (tech design §3). Wire/store representation is the byte value. */
public enum FileState {
    OPEN(0), SEALED(1), DELETING(2), DELETED(3);

    public final byte value;

    FileState(int value) {
        this.value = (byte) value;
    }

    public static FileState fromValue(byte v) {
        return switch (v) {
            case 0 -> OPEN;
            case 1 -> SEALED;
            case 2 -> DELETING;
            case 3 -> DELETED;
            default -> throw new IllegalArgumentException("unknown file state " + v);
        };
    }
}
