package io.strata.client;

import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import io.strata.proto.Messages;

import java.util.Objects;

import static io.strata.common.Checks.addChunkLength;

/** SCP-backed StrataFile handle over one logical file id. */
final class StrataFileImpl implements StrataFile {
    private final MetaClient meta;
    private final NodePool pool;
    private final ClientConfig config;
    private final FileId fileId;
    private final StrataNamespace namespace;
    private final StrataPath path;

    StrataFileImpl(MetaClient meta, NodePool pool, ClientConfig config, FileId fileId,
                   StrataNamespace namespace, StrataPath path) {
        this.meta = Objects.requireNonNull(meta, "meta");
        this.pool = Objects.requireNonNull(pool, "pool");
        this.config = Objects.requireNonNull(config, "config");
        this.fileId = Objects.requireNonNull(fileId, "fileId");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.path = Objects.requireNonNull(path, "path");
    }

    @Override
    public FileId id() {
        return fileId;
    }

    @Override
    public StrataNamespace namespace() {
        return namespace;
    }

    @Override
    public StrataPath path() {
        return path;
    }

    @Override
    public Appender openForAppend() {
        Messages.LookupFileResp file = meta.lookupFile(fileId);
        if (file.fileState() != FileState.OPEN.value) {
            throw new ScpException(ErrorCode.FILE_SEALED, "file state " + file.fileState());
        }
        long length = 0;
        for (Messages.ChunkInfo c : file.chunks()) {
            if (c.state() != ChunkState.SEALED) {
                throw new ScpException(ErrorCode.INTERNAL,
                        "file has an open chunk — run recoverAndSeal or resume the owning appender");
            }
            length = addChunkLength(length, c.length());
        }
        int writeEpoch = meta.allocateWriterEpochForAppend(fileId);
        return new AppenderImpl(meta, pool, config, fileId, writeEpoch, file.writePolicy(), length);
    }

    @Override
    public Reader openForRead() {
        return new ReaderImpl(meta, pool, config, fileId);
    }

    @Override
    public SealInfo recoverAndSeal() {
        return new Recovery(meta, pool, config).recoverAndSeal(fileId);
    }
}
