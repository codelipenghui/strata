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
 * op directly to the namespace's owner, caching {@code namespace -> owner}. A miss (cold cache, or an
 * ownership change) surfaces as a {@code NOT_LEADER} redirect carrying the owner endpoint; the client
 * updates its cache and retries there. This keeps one connection per owner the client actually talks to
 * (lazily opened from the seed endpoints + learned redirect hints), so concurrent ops across owners never
 * thrash a single connection.
 *
 * <p>Routing key per op: the <b>namespace</b>, for every op. File-scoped ops (CREATE_CHUNK, SEAL,
 * LOOKUP_FILE, ...) carry their namespace explicitly — the per-file {@code fileId -> namespace} ZK index
 * was removed, so a file is routed by the namespace it was created/opened in.
 */
final class ControllerClient implements AutoCloseable {
    private final ClientConfig config;
    private final List<String> seeds;                       // configured controllers used to bootstrap/round-robin
    private final Map<String, ManagedScpConnection> conns = new ConcurrentHashMap<>();
    private final Map<Object, String> ownerByKey = new ConcurrentHashMap<>(); // namespace -> owner endpoint
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
        return decode(Opcode.CREATE_FILE, resp, Messages.CreateFileResp::decode).fileId();
    }

    Messages.CreateChunkResp createChunk(StrataNamespace namespace, FileId fileId, int writeEpoch,
                                         long opIdMsb, long opIdLsb) {
        return createChunk(namespace, fileId, writeEpoch, opIdMsb, opIdLsb, Set.of());
    }

    Messages.CreateChunkResp createChunk(StrataNamespace namespace, FileId fileId, int writeEpoch,
                                         long opIdMsb, long opIdLsb, Set<Integer> excludedNodeIds) {
        var resp = call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(namespace, fileId, writeEpoch, opIdMsb, opIdLsb,
                        List.copyOf(excludedNodeIds)).encode(), namespace);
        return decode(Opcode.CREATE_CHUNK, resp, Messages.CreateChunkResp::decode);
    }

    int allocateWriterEpochForAppend(StrataNamespace namespace, FileId fileId) {
        var resp = call(Opcode.ALLOCATE_WRITER_EPOCH,
                Messages.AllocateWriterEpoch.forAppend(namespace, fileId).encode(), namespace);
        return decode(Opcode.ALLOCATE_WRITER_EPOCH, resp, Messages.AllocateWriterEpochResp::decode).writerEpoch();
    }

    int allocateWriterEpochForRecovery(StrataNamespace namespace, FileId fileId) {
        var resp = call(Opcode.ALLOCATE_WRITER_EPOCH,
                Messages.AllocateWriterEpoch.forRecovery(namespace, fileId).encode(), namespace);
        return decode(Opcode.ALLOCATE_WRITER_EPOCH, resp, Messages.AllocateWriterEpochResp::decode).writerEpoch();
    }

    void sealChunkMeta(StrataNamespace namespace, io.strata.common.ChunkId chunkId, int writeEpoch, long length,
                       int crc, java.util.List<Integer> sealedReplicas, long opIdMsb, long opIdLsb) {
        call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(namespace, chunkId, writeEpoch, length, crc, sealedReplicas,
                        opIdMsb, opIdLsb).encode(),
                namespace);
    }

    void abortChunkMeta(StrataNamespace namespace, io.strata.common.ChunkId chunkId, int writeEpoch,
                        long opIdMsb, long opIdLsb) {
        call(Opcode.ABORT_CHUNK_META,
                new Messages.AbortChunkMeta(namespace, chunkId, writeEpoch, opIdMsb, opIdLsb).encode(),
                namespace);
    }

    Messages.LookupFileResp lookupFile(StrataNamespace namespace, FileId fileId) {
        var resp = call(Opcode.LOOKUP_FILE, new Messages.LookupFile(namespace, fileId).encode(), namespace);
        return decode(Opcode.LOOKUP_FILE, resp, Messages.LookupFileResp::decode);
    }

    FileId lookupPath(StrataNamespace namespace, StrataPath path) {
        var resp = call(Opcode.LOOKUP_PATH, new Messages.LookupPath(namespace, path).encode(), namespace);
        return decode(Opcode.LOOKUP_PATH, resp, Messages.LookupPathResp::decode).fileId();
    }

    void sealFile(StrataNamespace namespace, FileId fileId, long totalLength) {
        call(Opcode.SEAL_FILE, new Messages.SealFile(namespace, fileId, totalLength).encode(), namespace);
    }

    Messages.DeleteFilesResp deleteFiles(StrataNamespace namespace, List<FileId> ids) {
        var resp = call(Opcode.DELETE_FILES, new Messages.DeleteFiles(namespace, ids).encode(), namespace);
        return decode(Opcode.DELETE_FILES, resp, Messages.DeleteFilesResp::decode);
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
