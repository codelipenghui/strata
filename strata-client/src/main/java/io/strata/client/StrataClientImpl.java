package io.strata.client;

import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.proto.Messages;

import java.util.List;

/** SCP-backed StrataClient implementation over the v0 metadata service. */
final class StrataClientImpl implements StrataClient {
    private final ClientConfig config;
    private final MetaClient meta;
    private final NodePool pool = new NodePool();

    StrataClientImpl(ClientConfig config) {
        this.config = config;
        this.meta = new MetaClient(config);
    }

    @Override
    public StrataFile create(FileSpec spec) {
        return open(meta.createFile(spec));
    }

    @Override
    public StrataFile open(FileId fileId) {
        return new StrataFileImpl(meta, pool, config, fileId);
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
