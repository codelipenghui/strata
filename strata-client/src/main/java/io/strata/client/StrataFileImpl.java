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
    private final ControllerClient controller;
    private final NodePool appendPool;
    private final NodePool readPool;
    private final ClientConfig config;
    private final FileId fileId;
    private final StrataNamespace namespace;
    private final StrataPath path;

    StrataFileImpl(ControllerClient controller, NodePool appendPool, NodePool readPool, ClientConfig config,
                   FileId fileId, StrataNamespace namespace, StrataPath path) {
        this.controller = Objects.requireNonNull(controller, "controller");
        this.appendPool = Objects.requireNonNull(appendPool, "appendPool");
        this.readPool = Objects.requireNonNull(readPool, "readPool");
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
        Messages.LookupFileResp file = controller.lookupFile(fileId);
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
        int writeEpoch = controller.allocateWriterEpochForAppend(fileId);
        return new AppenderImpl(controller, appendPool, config, fileId, writeEpoch, file.writePolicy(), length);
    }

    @Override
    public Reader openForRead() {
        return new ReaderImpl(controller, readPool, config, fileId);
    }

    @Override
    public SealInfo recoverAndSeal() {
        return new Recovery(controller, appendPool, readPool, config).recoverAndSeal(fileId);
    }
}
