package io.strata.client;

import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

/** Client-side metadata access (v0: SCP 0x02xx opcodes; v1: Kafka RPC). Rotates endpoints on failure. */
final class MetaClient implements AutoCloseable {
    // ReentrantLock, not synchronized: connect() blocks, and virtual threads must not pin carriers
    private final java.util.concurrent.locks.ReentrantLock lock =
            new java.util.concurrent.locks.ReentrantLock();
    private final ClientConfig config;
    private ScpClient client;
    private int endpointIndex;

    MetaClient(ClientConfig config) {
        this.config = config;
    }

    private ScpClient conn() {
        lock.lock();
        try {
            if (client == null || client.isClosed()) {
                String ep = config.metadataEndpoints().get(endpointIndex % config.metadataEndpoints().size());
                HostPort hp = parseEndpoint(ep);
                try {
                    client = new ScpClient(hp.host(), hp.port(), ScpClient.KIND_BROKER, "strata-client");
                } catch (IOException e) {
                    // rotation is owned by call(): incrementing here too would double-rotate and,
                    // with two endpoints, pin every retry onto the same dead instance
                    throw new ScpException(ErrorCode.INTERNAL, "metadata unreachable at " + ep + ": " + e);
                }
            }
            return client;
        } finally {
            lock.unlock();
        }
    }

    private record HostPort(String host, int port) {}

    private static HostPort parseEndpoint(String endpoint) {
        int colon = endpoint == null ? -1 : endpoint.lastIndexOf(':');
        if (colon <= 0 || colon == endpoint.length() - 1) {
            throw new ScpException(ErrorCode.INTERNAL, "invalid metadata endpoint: " + endpoint);
        }
        String host = endpoint.substring(0, colon);
        if (host.startsWith("[") != host.endsWith("]")) {
            throw new ScpException(ErrorCode.INTERNAL, "invalid metadata endpoint brackets: " + endpoint);
        }
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        int port;
        try {
            port = Integer.parseInt(endpoint.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new ScpException(ErrorCode.INTERNAL, "invalid metadata endpoint port: " + endpoint);
        }
        if (host.isBlank() || !host.equals(host.trim()) || port <= 0 || port > 65_535) {
            throw new ScpException(ErrorCode.INTERNAL, "invalid metadata endpoint: " + endpoint);
        }
        return new HostPort(host, port);
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
                return conn().call(op, header, null, config.callTimeoutMs());
            } catch (ScpException e) {
                last = e;
                boolean rotate = e.code() == ErrorCode.NOT_LEADER
                        || (e.code() == ErrorCode.INTERNAL && e.retriable());
                if (!rotate && !e.retriable()) {
                    throw e;
                }
                if (rotate) {
                    lock.lock();
                    try {
                        endpointIndex++;
                        if (client != null) client.close();
                        client = null;
                    } finally {
                        lock.unlock();
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
            lock.lock();
            try {
                if (client != null) client.close();
                client = null;
            } finally {
                lock.unlock();
            }
            throw new ScpException(ErrorCode.INTERNAL, "malformed metadata response for " + op + ": " + e);
        }
    }

    FileId createFile(StrataClient.FileSpec spec) {
        var resp = call(Opcode.CREATE_FILE, new Messages.CreateFile(spec.fileKind(), spec.mediaClass(),
                spec.ackPolicy(), spec.ownerTag()).encode());
        return decode(Opcode.CREATE_FILE, resp, Messages.CreateFileResp::decode).fileId();
    }

    Messages.CreateChunkResp createChunk(FileId fileId, int writeEpoch, long opIdMsb, long opIdLsb) {
        var resp = call(Opcode.CREATE_CHUNK,
                new Messages.CreateChunk(fileId, writeEpoch, (byte) 0xFF, opIdMsb, opIdLsb).encode());
        return decode(Opcode.CREATE_CHUNK, resp, Messages.CreateChunkResp::decode);
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

    void sealFile(FileId fileId, long totalLength) {
        call(Opcode.SEAL_FILE, new Messages.SealFile(fileId, totalLength).encode());
    }

    Messages.DeleteFilesResp deleteFiles(List<FileId> ids) {
        var resp = call(Opcode.DELETE_FILES, new Messages.DeleteFiles(ids).encode());
        return decode(Opcode.DELETE_FILES, resp, Messages.DeleteFilesResp::decode);
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (client != null) client.close();
        } finally {
            lock.unlock();
        }
    }
}
