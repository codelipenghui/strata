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
    // recovery-scoped ranged read: serves locally-present bytes up to localEndOffset, including the
    // never-acked tail above the durable high watermark that the client READ path clamps away.
    READ_RECOVERY(0x001A),
    // control plane (data node -> metadata)
    REGISTER_NODE(0x0101),
    NODE_HEARTBEAT(0x0102),
    // (0x0103 INVENTORY_REPORT removed: durability reconciliation is owner-pull VERIFY_CHUNKS, §9.2)
    // v0 client -> metadata (v1 moves broker-facing APIs to Kafka RPC)
    CREATE_FILE(0x0201),
    CREATE_CHUNK(0x0202),
    SEAL_CHUNK_META(0x0203),
    LOOKUP_FILE(0x0204),
    DELETE_FILES(0x0205),
    SEAL_FILE(0x0206),
    ABORT_CHUNK_META(0x0207),
    LOOKUP_PATH(0x0208),
    ALLOCATE_WRITER_EPOCH(0x0209),
    // direct metadata-owner -> data-node repair: a non-controller namespace owner, which has no heartbeat
    // command channel, tells a target node to pull a chunk from a live source (reuses the proven
    // ControlLoop.replicate path). Data-plane opcode (< 0x0100) so the combined-node router sends it to
    // DataNodeHandlers, not the Controller. Append-only assignment (tech design §7.2 and §10.3).
    EXEC_REPLICATE(0x001B),
    // owner-pull durability verification (tech design §9.2): a namespace owner asks a data node, in bounded
    // batches, for the local state of the chunks it expects that node to hold (present/missing/corrupt).
    // Replaces the central inventory push. Owner -> node, so a data-plane opcode (< 0x0100) routed to
    // DataNodeHandlers. Append-only; currently the highest assigned data-plane opcode.
    VERIFY_CHUNKS(0x001C),
    // data-node orphan GC -> metadata owner: a dedicated destructive confirmation lane. 0x020a stays
    // deliberately unassigned. An older metadata owner rejects this unknown opcode, which the caller
    // treats as no verdict so mixed-version orphan GC fails closed.
    CONFIRM_ORPHAN(0x020B),
    // Metadata owner -> data node: durably raises the namespace owner-epoch floor before any destructive
    // owner RPC is issued. 0x001D is reserved by the quarantine lane; keep this assignment append-only.
    INSTALL_OWNER_EPOCH(0x001E);

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
