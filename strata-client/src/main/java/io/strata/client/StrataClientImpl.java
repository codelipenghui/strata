package io.strata.client;

import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import io.strata.proto.Messages;

import java.util.List;
import java.util.Objects;

/** SCP-backed StrataClient implementation over the v0 metadata service. */
final class StrataClientImpl implements StrataClient {
    private final ClientConfig config;
    private final MetaClient meta;
    private final NodePool pool;

    StrataClientImpl(ClientConfig config) {
        this.config = config;
        this.meta = new MetaClient(config);
        this.pool = new NodePool(config);
    }

    @Override
    public StrataFile create(FileSpec spec) {
        Objects.requireNonNull(spec, "spec");
        return new StrataFileImpl(meta, pool, config, meta.createFile(spec), spec.namespace(), spec.path());
    }

    @Override
    public StrataFile open(StrataNamespace namespace, StrataPath path) {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(path, "path");
        FileId fileId = meta.lookupPath(namespace, path);
        return new StrataFileImpl(meta, pool, config, fileId, namespace, path);
    }

    @Override
    public StrataFile openById(FileId fileId) {
        Objects.requireNonNull(fileId, "fileId");
        Messages.LookupFileResp file = meta.lookupFile(fileId);
        return new StrataFileImpl(meta, pool, config, fileId, file.namespace(), file.path());
    }

    @Override
    public void delete(List<FilePath> paths) {
        Objects.requireNonNull(paths, "paths");
        deleteById(paths.stream()
                .map(path -> {
                    Objects.requireNonNull(path, "path");
                    return meta.lookupPath(path.namespace(), path.path());
                })
                .toList());
    }

    @Override
    public void deleteById(List<FileId> fileIds) {
        Objects.requireNonNull(fileIds, "fileIds");
        fileIds = fileIds.stream().map(id -> Objects.requireNonNull(id, "fileId")).toList();
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
