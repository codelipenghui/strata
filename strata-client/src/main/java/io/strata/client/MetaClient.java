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
                String[] hp = ep.split(":");
                try {
                    client = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_BROKER, "strata-client");
                } catch (IOException e) {
                    endpointIndex++;
                    throw new ScpException(ErrorCode.INTERNAL, "metadata unreachable at " + ep + ": " + e);
                }
            }
            return client;
        } finally {
            lock.unlock();
        }
    }

    private ByteBuffer call(Opcode op, byte[] header) {
        for (int attempt = 0; attempt < config.metadataEndpoints().size() + 1; attempt++) {
            try {
                return conn().call(op, header, null, config.callTimeoutMs());
            } catch (ScpException e) {
                if (e.code() == ErrorCode.NOT_LEADER || (e.code() == ErrorCode.INTERNAL && e.retriable())) {
                    lock.lock();
                    try {
                        endpointIndex++;
                        if (client != null) client.close();
                        client = null;
                    } finally {
                        lock.unlock();
                    }
                    continue;
                }
                throw e;
            }
        }
        throw new ScpException(ErrorCode.INTERNAL, "no reachable metadata leader");
    }

    FileId createFile(SegmentStore.FileSpec spec) {
        var resp = call(Opcode.CREATE_FILE, new Messages.CreateFile(spec.fileKind(), spec.mediaClass(),
                spec.ackPolicy(), spec.ownerTag()).encode());
        return Messages.CreateFileResp.decode(resp).fileId();
    }

    Messages.CreateChunkResp createChunk(FileId fileId, int writeEpoch) {
        var resp = call(Opcode.CREATE_CHUNK, new Messages.CreateChunk(fileId, writeEpoch, (byte) 0xFF).encode());
        return Messages.CreateChunkResp.decode(resp);
    }

    void sealChunkMeta(io.strata.common.ChunkId chunkId, int writeEpoch, long length, int crc) {
        call(Opcode.SEAL_CHUNK_META, new Messages.SealChunkMeta(chunkId, writeEpoch, length, crc).encode());
    }

    Messages.LookupFileResp lookupFile(FileId fileId) {
        var resp = call(Opcode.LOOKUP_FILE, new Messages.LookupFile(fileId).encode());
        return Messages.LookupFileResp.decode(resp);
    }

    void sealFile(FileId fileId, long totalLength) {
        call(Opcode.SEAL_FILE, new Messages.SealFile(fileId, totalLength).encode());
    }

    Messages.DeleteFilesResp deleteFiles(List<FileId> ids) {
        var resp = call(Opcode.DELETE_FILES, new Messages.DeleteFiles(ids).encode());
        return Messages.DeleteFilesResp.decode(resp);
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
