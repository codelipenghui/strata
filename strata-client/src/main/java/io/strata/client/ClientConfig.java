package io.strata.client;

import java.util.List;

/** Client configuration. chunkRollBytes is ~1 GB nominal in production; tests use small values. */
public record ClientConfig(List<String> metadataEndpoints, long chunkRollBytes, long callTimeoutMs) {
    public static ClientConfig of(String metadataEndpoint) {
        return new ClientConfig(List.of(metadataEndpoint), 1L << 30, 10_000);
    }

    public ClientConfig withChunkRollBytes(long bytes) {
        return new ClientConfig(metadataEndpoints, bytes, callTimeoutMs);
    }
}
