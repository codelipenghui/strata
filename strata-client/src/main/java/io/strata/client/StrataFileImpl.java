package io.strata.client;

import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.proto.Messages;

import java.util.Objects;

/** SCP-backed StrataFile handle over one logical file id. */
final class StrataFileImpl implements StrataFile {
    private final MetaClient meta;
    private final NodePool pool;
    private final ClientConfig config;
    private final FileId fileId;

    StrataFileImpl(MetaClient meta, NodePool pool, ClientConfig config, FileId fileId) {
        this.meta = Objects.requireNonNull(meta, "meta");
        this.pool = Objects.requireNonNull(pool, "pool");
        this.config = Objects.requireNonNull(config, "config");
        this.fileId = Objects.requireNonNull(fileId, "fileId");
    }

    @Override
    public FileId id() {
        return fileId;
    }

    @Override
    public Appender openForAppend(int writeEpoch) {
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
    public Reader openForRead() {
        return new ReaderImpl(meta, pool, config, fileId);
    }

    @Override
    public SealInfo recoverAndSeal(int newEpoch) {
        return new Recovery(meta, pool, config).recoverAndSeal(fileId, newEpoch);
    }
}
