package io.strata.proto;

/** SCP opcodes — append-only namespace (tech design §10.3/§10.4 + v0 additions per IMPLEMENTATION_PLAN.md). */
public enum Opcode {
    HELLO(0x0001),
    // data plane
    OPEN_CHUNK(0x0010),
    APPEND(0x0011),
    READ(0x0012),
    FENCE(0x0013),
    STAT_CHUNK(0x0014),
    SEAL_CHUNK(0x0015),
    DELETE_CHUNKS(0x0016),
    FETCH_CHUNK(0x0017),
    PING(0x0018),
    READ_LEDGER(0x0019),
    // control plane (storage node -> metadata)
    REGISTER_NODE(0x0101),
    NODE_HEARTBEAT(0x0102),
    INVENTORY_REPORT(0x0103),
    // v0 client -> metadata (v1 moves broker-facing APIs to Kafka RPC)
    CREATE_FILE(0x0201),
    CREATE_CHUNK(0x0202),
    SEAL_CHUNK_META(0x0203),
    LOOKUP_FILE(0x0204),
    DELETE_FILES(0x0205),
    SEAL_FILE(0x0206),
    ABORT_CHUNK_META(0x0207),
    LOOKUP_PATH(0x0208),
    ALLOCATE_WRITER_EPOCH(0x0209);

    public final short code;

    Opcode(int code) {
        this.code = (short) code;
    }

    private static final Opcode[] BY_CODE;
    static {
        int max = 0;
        for (Opcode o : values()) max = Math.max(max, o.code);
        BY_CODE = new Opcode[max + 1];
        for (Opcode o : values()) BY_CODE[o.code] = o;
    }

    public static Opcode fromCode(short code) {
        if (code < 0 || code >= BY_CODE.length) return null;
        return BY_CODE[code];
    }
}
