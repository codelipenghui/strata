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
import java.util.Set;
import java.util.function.Function;

/** Client-side metadata access (v0: SCP 0x02xx opcodes; v1: Kafka RPC). Rotates endpoints on failure. */
final class MetaClient implements AutoCloseable {
    private final ClientConfig config;
    private final ManagedScpConnection connection;

    MetaClient(ClientConfig config) {
        this.config = config;
        this.connection = new ManagedScpConnection(config.metadataEndpoints(), config.connectionPolicy(),
                ScpClient.KIND_BROKER, "strata-client", "metadata endpoint", true, true);
    }

    /**
     * Metadata calls are chunk-boundary operations: a leader failover mid-call must be absorbed,
     * not surfaced. Retries retriable errors (NOT_LEADER while the standby acquires leadership,
     * connection failures, NO_CAPACITY while nodes re-register) with backoff up to a deadline.
     */
    private ByteBuffer call(Opcode op, byte[] header) {
        long deadline = System.currentTimeMillis() + Math.max(15_000, config.callTimeoutMs());
        ScpException last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                return connection.call(op, header, null, config.callTimeoutMs());
            } catch (ScpException e) {
                last = e;
                if (!e.retriable()) {
                    throw e;
                }
                if (e.code() == ErrorCode.NOT_LEADER) {
                    String leader = e.leaderHint();
                    if (leader != null && !leader.isBlank()) {
                        connection.preferEndpoint(leader);  // jump straight to the leader
                    } else {
                        connection.rotateEndpoint();        // unknown leader (election) — round-robin
                    }
                }
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
        throw last != null ? last : new ScpException(ErrorCode.INTERNAL, "no reachable metadata leader");
    }

    private <T> T decode(Opcode op, ByteBuffer response, Function<ByteBuffer, T> decoder) {
        try {
            return decoder.apply(response);
        } catch (ScpException e) {
            throw e;
        } catch (RuntimeException e) {
            connection.disconnect();
            throw new ScpException(ErrorCode.INTERNAL, "malformed metadata response for " + op + ": " + e);
        }
    }

    FileId createFile(StrataClient.FileSpec spec) {
        StrataClient.WritePolicy policy = spec.writePolicy();
        var resp = call(Opcode.CREATE_FILE, new Messages.CreateFile(spec.namespace(), spec.path(),
                new Messages.WritePolicy(policy.replicationFactor(), policy.ackQuorum(), policy.fsyncOnAck()))
                .encode());
        return decode(Opcode.CREATE_FILE, resp, Messages.CreateFileResp::decode).fileId();
    }

    Messages.CreateChunkResp createChunk(FileId fileId, int writeEpoch, long opIdMsb, long opIdLsb) {
        return createChunk(fileId, writeEpoch, opIdMsb, opIdLsb, Set.of());
    }

    Messages.CreateChunkResp createChunk(FileId fileId, int writeEpoch, long opIdMsb, long opIdLsb,
                                         Set<Integer> excludedNodeIds) {
        var resp = call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(fileId, writeEpoch, opIdMsb, opIdLsb,
                        List.copyOf(excludedNodeIds)).encode());
        return decode(Opcode.CREATE_CHUNK, resp, Messages.CreateChunkResp::decode);
    }

    int allocateWriterEpochForAppend(FileId fileId) {
        var resp = call(Opcode.ALLOCATE_WRITER_EPOCH, Messages.AllocateWriterEpoch.forAppend(fileId).encode());
        return decode(Opcode.ALLOCATE_WRITER_EPOCH, resp, Messages.AllocateWriterEpochResp::decode).writerEpoch();
    }

    int allocateWriterEpochForRecovery(FileId fileId) {
        var resp = call(Opcode.ALLOCATE_WRITER_EPOCH, Messages.AllocateWriterEpoch.forRecovery(fileId).encode());
        return decode(Opcode.ALLOCATE_WRITER_EPOCH, resp, Messages.AllocateWriterEpochResp::decode).writerEpoch();
    }

    void sealChunkMeta(io.strata.common.ChunkId chunkId, int writeEpoch, long length, int crc,
                       java.util.List<Integer> sealedReplicas) {
        call(Opcode.SEAL_CHUNK_META,
                new Messages.SealChunkMeta(chunkId, writeEpoch, length, crc, sealedReplicas).encode());
    }

    void abortChunkMeta(io.strata.common.ChunkId chunkId, int writeEpoch, long opIdMsb, long opIdLsb) {
        call(Opcode.ABORT_CHUNK_META,
                new Messages.AbortChunkMeta(chunkId, writeEpoch, opIdMsb, opIdLsb).encode());
    }

    Messages.LookupFileResp lookupFile(FileId fileId) {
        var resp = call(Opcode.LOOKUP_FILE, new Messages.LookupFile(fileId).encode());
        return decode(Opcode.LOOKUP_FILE, resp, Messages.LookupFileResp::decode);
    }

    FileId lookupPath(StrataNamespace namespace, StrataPath path) {
        var resp = call(Opcode.LOOKUP_PATH, new Messages.LookupPath(namespace, path).encode());
        return decode(Opcode.LOOKUP_PATH, resp, Messages.LookupPathResp::decode).fileId();
    }

    void sealFile(FileId fileId, long totalLength) {
        call(Opcode.SEAL_FILE, new Messages.SealFile(fileId, totalLength).encode());
    }

    Messages.DeleteFilesResp deleteFiles(List<FileId> ids) {
        var resp = call(Opcode.DELETE_FILES, new Messages.DeleteFiles(ids).encode());
        return decode(Opcode.DELETE_FILES, resp, Messages.DeleteFilesResp::decode);
    }

    @Override
    public void close() {
        connection.close();
    }
}
