package io.strata.client;

import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.proto.Messages;

import java.util.List;

/** Entry point: SegmentStore implementation over SCP + the v0 metadata service. */
public final class StrataClient implements SegmentStore {
    private final ClientConfig config;
    private final MetaClient meta;
    private final NodePool pool = new NodePool();

    public StrataClient(ClientConfig config) {
        this.config = config;
        this.meta = new MetaClient(config);
    }

    @Override
    public FileId create(FileSpec spec) {
        return meta.createFile(spec);
    }

    @Override
    public Appender openForAppend(FileId fileId, int writeEpoch) {
        Messages.LookupFileResp file = meta.lookupFile(fileId);
        if (file.fileState() != 0) {
            throw new ScpException(ErrorCode.FILE_SEALED, "file state " + file.fileState());
        }
        long length = 0;
        for (Messages.ChunkInfo c : file.chunks()) {
            if (c.state() != ChunkState.SEALED) {
                throw new ScpException(ErrorCode.INTERNAL,
                        "file has an open chunk — run recoverAndSeal or resume the owning appender");
            }
            if (c.writeEpoch() > writeEpoch) {
                throw new ScpException(ErrorCode.FENCED_EPOCH, "file epoch " + c.writeEpoch(), c.writeEpoch());
            }
            if (c.length() < 0) {
                throw new ScpException(ErrorCode.CORRUPT_CHUNK, "negative chunk length " + c.length());
            }
            try {
                length = Math.addExact(length, c.length());
            } catch (ArithmeticException e) {
                throw new ScpException(ErrorCode.CORRUPT_CHUNK, "file length overflow");
            }
        }
        return new AppenderImpl(meta, pool, config, fileId, writeEpoch, file.fileKind(),
                file.ackPolicy(), length);
    }

    @Override
    public Reader openForRead(FileId fileId) {
        return new ReaderImpl(meta, pool, config, fileId);
    }

    @Override
    public SealInfo recoverAndSeal(FileId fileId, int newEpoch) {
        return new Recovery(meta, pool, config).recoverAndSeal(fileId, newEpoch);
    }

    @Override
    public void delete(List<FileId> fileIds) {
        Messages.DeleteFilesResp resp = meta.deleteFiles(fileIds);
        if (!resp.fileIds().equals(fileIds) || resp.codes().size() != fileIds.size()) {
            throw new ScpException(ErrorCode.INTERNAL, "metadata delete response did not match request");
        }
        for (int i = 0; i < resp.fileIds().size(); i++) {
            short code = resp.codes().get(i);
            if (code != ErrorCode.OK.code) {
                throw new ScpException(ErrorCode.fromCode(code),
                        "delete " + resp.fileIds().get(i) + " failed with " + code);
            }
        }
    }

    @Override
    public void close() {
        pool.close();
        meta.close();
    }
}
