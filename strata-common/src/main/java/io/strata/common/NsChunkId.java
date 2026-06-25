package io.strata.common;

/**
 * Composite key for namespace-qualified chunk identity. Two namespaces may have files with the
 * same per-namespace long fileId; this key prevents collisions. Java records provide correct
 * equals/hashCode automatically.
 */
public record NsChunkId(StrataNamespace namespace, ChunkId chunkId) {}
