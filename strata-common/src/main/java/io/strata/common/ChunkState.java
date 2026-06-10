package io.strata.common;

/** Chunk lifecycle states (tech design §3). Wire/disk representation is the byte value. */
public enum ChunkState {
    OPEN(0), SEALED(1), DELETING(2);

    public final byte value;

    ChunkState(int value) {
        this.value = (byte) value;
    }

    public static ChunkState fromValue(byte v) {
        return switch (v) {
            case 0 -> OPEN;
            case 1 -> SEALED;
            case 2 -> DELETING;
            default -> throw new IllegalArgumentException("unknown chunk state " + v);
        };
    }
}
