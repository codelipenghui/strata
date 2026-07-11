package io.strata.client;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import io.strata.common.WritePolicyChecks;

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

    record WritePolicy(int replicationFactor, int ackQuorum, boolean fsyncOnAck) {
        public static final WritePolicy DEFAULT = new WritePolicy(3, 2, false);

        public WritePolicy {
            WritePolicyChecks.validate(replicationFactor, ackQuorum);
        }

        public static WritePolicy replicated(int replicationFactor, int ackQuorum) {
            return new WritePolicy(replicationFactor, ackQuorum, false);
        }

        public static WritePolicy fsync(int replicationFactor, int ackQuorum) {
            return new WritePolicy(replicationFactor, ackQuorum, true);
        }
    }

    record FileSpec(StrataNamespace namespace, StrataPath path, WritePolicy writePolicy) {
        public FileSpec {
            namespace = Objects.requireNonNull(namespace, "namespace");
            path = Objects.requireNonNull(path, "path");
            writePolicy = Objects.requireNonNull(writePolicy, "writePolicy");
        }

        public FileSpec(String namespace, String path) {
            this(StrataNamespace.of(namespace), StrataPath.of(path), WritePolicy.DEFAULT);
        }

        public FileSpec(String namespace, String path, WritePolicy writePolicy) {
            this(StrataNamespace.of(namespace), StrataPath.of(path), writePolicy);
        }

        public static FileSpec log(String namespace, String path) {
            return new FileSpec(namespace, path);
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

    StrataFile openById(StrataNamespace namespace, FileId fileId);

    default StrataFile open(String namespace, String path) {
        return open(StrataNamespace.of(namespace), StrataPath.of(path));
    }

    default void delete(StrataNamespace namespace, StrataPath path) {
        delete(List.of(new FilePath(namespace, path)));
    }

    default void delete(StrataFile file) {
        Objects.requireNonNull(file, "file");
        deleteById(file.namespace(), file.id());
    }

    default void delete(String namespace, String path) {
        delete(StrataNamespace.of(namespace), StrataPath.of(path));
    }

    void delete(List<FilePath> paths);

    default void deleteById(StrataNamespace namespace, FileId fileId) {
        deleteById(namespace, List.of(Objects.requireNonNull(fileId, "fileId")));
    }

    void deleteById(StrataNamespace namespace, List<FileId> fileIds);

    @Override
    void close();
}
