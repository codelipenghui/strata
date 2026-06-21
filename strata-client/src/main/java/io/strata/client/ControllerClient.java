package io.strata.client;

import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import io.strata.proto.Messages;
import io.strata.proto.ManagedScpConnection;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Owner-aware client-side metadata access (v0: SCP 0x02xx opcodes). The metadata plane is sharded — each
 * namespace has exactly one controller owner — so this client connects to ANY controller and routes each
 * op directly to the namespace's owner, caching {@code namespace -> owner} and {@code fileId -> owner}.
 * A miss (cold cache, or an ownership change) surfaces as a {@code NOT_LEADER} redirect carrying the owner
 * endpoint; the client updates its cache and retries there. This keeps one connection per owner the client
 * actually talks to (lazily opened from the seed endpoints + learned redirect hints), so concurrent ops
 * across owners never thrash a single connection.
 *
 * <p>Routing key per op: namespace for namespace-scoped ops (CREATE_FILE, LOOKUP_PATH), fileId for
 * file-scoped ops (CREATE_CHUNK, SEAL, LOOKUP_FILE, ...). A created file inherits its namespace's owner.
 */
final class ControllerClient implements AutoCloseable {
    private final ClientConfig config;
    private final List<String> seeds;                       // configured controllers used to bootstrap/round-robin
    private final Map<String, ManagedScpConnection> conns = new ConcurrentHashMap<>();
    private final Map<Object, String> ownerByKey = new ConcurrentHashMap<>(); // namespace|fileId -> owner endpoint
    private final AtomicInteger seedCursor = new AtomicInteger();

    ControllerClient(ClientConfig config) {
        this.config = config;
        this.seeds = List.copyOf(config.controllerEndpoints());
        if (seeds.isEmpty()) {
            throw new IllegalArgumentException("at least one controller endpoint is required");
        }
    }

    /** A lazily-opened, single-endpoint connection to one controller (auto-reconnecting, no rotation). */
    private ManagedScpConnection connFor(String endpoint) {
        return conns.computeIfAbsent(endpoint, e -> new ManagedScpConnection(List.of(e),
                config.connectionPolicy(), ScpClient.KIND_BROKER, "strata-client", "controller " + e, false, true));
    }

    private String nextSeed() {
        return seeds.get(Math.floorMod(seedCursor.getAndIncrement(), seeds.size()));
    }

    /**
     * Routes {@code op} to the owner of {@code routingKey} (cached, else a seed) and caches whoever serves
     * it. On a NOT_LEADER redirect, retargets to the owner hint and updates the cache; other retriable
     * errors back off and retry. {@code routingKey} null = un-routed (no owner concept).
     */
    private ByteBuffer call(Opcode op, byte[] header, Object routingKey) {
        long deadline = System.currentTimeMillis() + Math.max(15_000, config.callTimeoutMs());
        String target = routingKey == null ? null : ownerByKey.get(routingKey);
        if (target == null) {
            target = nextSeed();
        }
        ScpException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                ByteBuffer resp = connFor(target).call(op, header, null, config.callTimeoutMs());
                if (routingKey != null) {
                    ownerByKey.put(routingKey, target); // remember who serves this namespace/file
                }
                return resp;
            } catch (ScpException e) {
                last = e;
                if (!e.retriable()) {
                    throw e;
                }
                if (e.code() == ErrorCode.NOT_LEADER && e.leaderHint() != null && !e.leaderHint().isBlank()) {
                    target = e.leaderHint();                  // redirect straight to the namespace owner
                    if (routingKey != null) {
                        ownerByKey.put(routingKey, target);
                    }
                } else {
                    // Owner unknown (election), or a transport/NO_CAPACITY failure on the current target:
                    // advance to another controller so a dead/unreachable cached owner can't pin us. The
                    // cache is left intact; a live owner just serves again and re-confirms on success.
                    target = nextSeed();
                }
                sleep();
            }
        }
        throw last != null ? last : new ScpException(ErrorCode.INTERNAL, "no reachable controller");
    }

    private void sleep() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ScpException(ErrorCode.INTERNAL, "interrupted");
        }
    }

    private <T> T decode(Opcode op, ByteBuffer response, Function<ByteBuffer, T> decoder) {
        try {
            return decoder.apply(response);
        } catch (ScpException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new ScpException(ErrorCode.INTERNAL, "malformed metadata response for " + op + ": " + e);
        }
    }

    FileId createFile(StrataClient.FileSpec spec) {
        StrataClient.WritePolicy policy = spec.writePolicy();
        var resp = call(Opcode.CREATE_FILE, new Messages.CreateFile(spec.namespace(), spec.path(),
                new Messages.WritePolicy(policy.replicationFactor(), policy.ackQuorum(), policy.fsyncOnAck()))
                .encode(), spec.namespace());
        FileId id = decode(Opcode.CREATE_FILE, resp, Messages.CreateFileResp::decode).fileId();
        inheritOwner(id, spec.namespace()); // the file is served by its namespace's owner
        return id;
    }

    Messages.CreateChunkResp createChunk(FileId fileId, int writeEpoch, long opIdMsb, long opIdLsb) {
        return createChunk(fileId, writeEpoch, opIdMsb, opIdLsb, Set.of());
    }

    Messages.CreateChunkResp createChunk(FileId fileId, int writeEpoch, long opIdMsb, long opIdLsb,
                                         Set<Integer> excludedNodeIds) {
        var resp = call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(fileId, writeEpoch, opIdMsb, opIdLsb,
                        List.copyOf(excludedNodeIds)).encode(), fileId);
        return decode(Opcode.CREATE_CHUNK, resp, Messages.CreateChunkResp::decode);
    }

    int allocateWriterEpochForAppend(FileId fileId) {
        var resp = call(Opcode.ALLOCATE_WRITER_EPOCH, Messages.AllocateWriterEpoch.forAppend(fileId).encode(), fileId);
        return decode(Opcode.ALLOCATE_WRITER_EPOCH, resp, Messages.AllocateWriterEpochResp::decode).writerEpoch();
    }

    int allocateWriterEpochForRecovery(FileId fileId) {
        var resp = call(Opcode.ALLOCATE_WRITER_EPOCH, Messages.AllocateWriterEpoch.forRecovery(fileId).encode(), fileId);
        return decode(Opcode.ALLOCATE_WRITER_EPOCH, resp, Messages.AllocateWriterEpochResp::decode).writerEpoch();
    }

    void sealChunkMeta(io.strata.common.ChunkId chunkId, int writeEpoch, long length, int crc,
                       java.util.List<Integer> sealedReplicas) {
        call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunkId, writeEpoch, length, crc, sealedReplicas).encode(),
                chunkId.fileId());
    }

    void abortChunkMeta(io.strata.common.ChunkId chunkId, int writeEpoch, long opIdMsb, long opIdLsb) {
        call(Opcode.ABORT_CHUNK_META,
                new Messages.AbortChunkMeta(chunkId, writeEpoch, opIdMsb, opIdLsb).encode(),
                chunkId.fileId());
    }

    Messages.LookupFileResp lookupFile(FileId fileId) {
        var resp = call(Opcode.LOOKUP_FILE, new Messages.LookupFile(fileId).encode(), fileId);
        return decode(Opcode.LOOKUP_FILE, resp, Messages.LookupFileResp::decode);
    }

    FileId lookupPath(StrataNamespace namespace, StrataPath path) {
        var resp = call(Opcode.LOOKUP_PATH, new Messages.LookupPath(namespace, path).encode(), namespace);
        FileId id = decode(Opcode.LOOKUP_PATH, resp, Messages.LookupPathResp::decode).fileId();
        inheritOwner(id, namespace);
        return id;
    }

    void sealFile(FileId fileId, long totalLength) {
        call(Opcode.SEAL_FILE, new Messages.SealFile(fileId, totalLength).encode(), fileId);
    }

    Messages.DeleteFilesResp deleteFiles(List<FileId> ids) {
        // One batch, routed by the first file's owner (so the per-file response codes map positionally to
        // the request, as the caller expects). The common case is a single-file delete; a batch that spans
        // owners would see per-file NOT_LEADER codes for files owned elsewhere (rare — the data path deletes
        // one file at a time).
        Object key = ids.isEmpty() ? null : ids.get(0);
        var resp = call(Opcode.DELETE_FILES, new Messages.DeleteFiles(ids).encode(), key);
        return decode(Opcode.DELETE_FILES, resp, Messages.DeleteFilesResp::decode);
    }

    /** A file is owned by its namespace's owner; mirror that mapping so file-scoped ops route directly. */
    private void inheritOwner(FileId fileId, StrataNamespace namespace) {
        String owner = ownerByKey.get(namespace);
        if (owner != null) {
            ownerByKey.put(fileId, owner);
        }
    }

    @Override
    public void close() {
        for (ManagedScpConnection c : conns.values()) {
            try {
                c.close();
            } catch (RuntimeException ignore) {
                // best-effort
            }
        }
        conns.clear();
    }
}
