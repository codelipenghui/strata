package io.strata.common;

import java.util.Objects;

/**
 * Composite key for namespace-qualified chunk identity. Two namespaces may have files with the
 * same per-namespace long fileId; this key prevents collisions.
 */
public record NsChunkId(StrataNamespace namespace, ChunkId chunkId) {
    public static int hash(StrataNamespace namespace, ChunkId chunkId) {
        int result = 1;
        result = 31 * result + Objects.hashCode(namespace);
        result = 31 * result + Objects.hashCode(chunkId);
        return result;
    }

    @Override
    public int hashCode() {
        return hash(namespace, chunkId);
    }
}
