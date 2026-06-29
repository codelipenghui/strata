package io.strata.client;

import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import io.strata.proto.Messages;

import java.util.List;
import java.util.Objects;

/** SCP-backed StrataClient implementation over the v0 controller. */
final class StrataClientImpl implements StrataClient {
    private final ClientConfig config;
    private final ControllerClient controller;
    private final NodePool appendPool;
    private final NodePool readPool;

    StrataClientImpl(ClientConfig config) {
        this.config = config;
        this.controller = new ControllerClient(config);
        this.appendPool = new NodePool(config, "strata-client-append");
        this.readPool = new NodePool(config, "strata-client-read");
    }

    @Override
    public StrataFile create(FileSpec spec) {
        Objects.requireNonNull(spec, "spec");
        return new StrataFileImpl(controller, appendPool, readPool, config,
                controller.createFile(spec), spec.namespace(), spec.path());
    }

    @Override
    public StrataFile open(StrataNamespace namespace, StrataPath path) {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(path, "path");
        FileId fileId = controller.lookupPath(namespace, path);
        return new StrataFileImpl(controller, appendPool, readPool, config, fileId, namespace, path);
    }

    @Override
    public StrataFile openById(StrataNamespace namespace, FileId fileId) {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(fileId, "fileId");
        Messages.LookupFileResp file = controller.lookupFile(namespace, fileId);
        return new StrataFileImpl(controller, appendPool, readPool, config, fileId, namespace, file.path());
    }

    @Override
    public void delete(List<FilePath> paths) {
        Objects.requireNonNull(paths, "paths");
        for (FilePath p : paths) {
            Objects.requireNonNull(p, "path");
            FileId id = controller.lookupPath(p.namespace(), p.path());
            deleteById(p.namespace(), List.of(id));
        }
    }

    @Override
    public void deleteById(StrataNamespace namespace, List<FileId> fileIds) {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(fileIds, "fileIds");
        fileIds = fileIds.stream().map(id -> Objects.requireNonNull(id, "fileId")).toList();
        Messages.DeleteFilesResp resp = controller.deleteFiles(namespace, fileIds);
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
        appendPool.close();
        readPool.close();
        controller.close();
    }
}
