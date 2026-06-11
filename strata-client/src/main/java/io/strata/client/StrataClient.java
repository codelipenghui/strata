package io.strata.client;

import io.strata.common.FileId;

import java.util.List;
import java.util.Objects;

/**
 * Strata client API for managing append-only logical files.
 * File-scoped read, append, and recovery operations live on StrataFile handles.
 */
public interface StrataClient extends AutoCloseable {

    static StrataClient connect(ClientConfig config) {
        return new StrataClientImpl(Objects.requireNonNull(config, "config"));
    }

    record FileSpec(byte fileKind, byte mediaClass, byte ackPolicy, String ownerTag) {
        public FileSpec {
            if (ackPolicy != 0 && ackPolicy != 1) {
                throw new IllegalArgumentException("unsupported ack policy " + (ackPolicy & 0xFF));
            }
            if (ownerTag == null) {
                throw new IllegalArgumentException("ownerTag must not be null");
            }
        }

        public static FileSpec log(String ownerTag) {
            return new FileSpec((byte) 0, (byte) 0, (byte) 0, ownerTag);
        }
    }

    StrataFile create(FileSpec spec);

    StrataFile open(FileId fileId);

    default void delete(FileId fileId) {
        delete(List.of(Objects.requireNonNull(fileId, "fileId")));
    }

    default void delete(StrataFile file) {
        delete(Objects.requireNonNull(file, "file").id());
    }

    void delete(List<FileId> fileIds);

    @Override
    void close();
}
