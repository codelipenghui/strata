package io.strata.client;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;

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

    record FileSpec(StrataNamespace namespace, StrataPath path, byte fileKind, byte mediaClass, byte ackPolicy) {
        public FileSpec {
            namespace = Objects.requireNonNull(namespace, "namespace");
            path = Objects.requireNonNull(path, "path");
            if (ackPolicy != 0 && ackPolicy != 1) {
                throw new IllegalArgumentException("unsupported ack policy " + (ackPolicy & 0xFF));
            }
        }

        public FileSpec(String namespace, String path, byte fileKind, byte mediaClass, byte ackPolicy) {
            this(StrataNamespace.of(namespace), StrataPath.of(path), fileKind, mediaClass, ackPolicy);
        }

        public static FileSpec log(String namespace, String path) {
            return new FileSpec(namespace, path, (byte) 0, (byte) 0, (byte) 0);
        }
    }

    record FilePath(StrataNamespace namespace, StrataPath path) {
        public FilePath {
            namespace = Objects.requireNonNull(namespace, "namespace");
            path = Objects.requireNonNull(path, "path");
        }

        public FilePath(String namespace, String path) {
            this(StrataNamespace.of(namespace), StrataPath.of(path));
        }
    }

    StrataFile create(FileSpec spec);

    StrataFile open(StrataNamespace namespace, StrataPath path);

    StrataFile openById(FileId fileId);

    default StrataFile open(String namespace, String path) {
        return open(StrataNamespace.of(namespace), StrataPath.of(path));
    }

    default void delete(StrataNamespace namespace, StrataPath path) {
        delete(List.of(new FilePath(namespace, path)));
    }

    default void delete(StrataFile file) {
        deleteById(Objects.requireNonNull(file, "file").id());
    }

    default void delete(String namespace, String path) {
        delete(StrataNamespace.of(namespace), StrataPath.of(path));
    }

    void delete(List<FilePath> paths);

    default void deleteById(FileId fileId) {
        deleteById(List.of(Objects.requireNonNull(fileId, "fileId")));
    }

    void deleteById(List<FileId> fileIds);

    @Override
    void close();
}
