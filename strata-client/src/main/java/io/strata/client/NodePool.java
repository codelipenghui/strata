package io.strata.client;

import io.strata.common.ErrorCode;
import io.strata.common.Endpoint;
import io.strata.common.ScpException;
import io.strata.proto.ScpClient;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One SCP connection per storage-node endpoint, lazily created, replaced when closed.
 * All appends for a chunk flow through one connection (§10.2 ordering rule) — guaranteed
 * here because an endpoint maps to exactly one connection.
 */
final class NodePool implements AutoCloseable {
    private final Map<String, ScpClient> conns = new ConcurrentHashMap<>();

    ScpClient get(String endpoint) {
        if (endpoint == null) {
            throw new ScpException(ErrorCode.INTERNAL, "invalid storage endpoint: null");
        }
        // lock-free fast path: every append/read resolves a connection, and compute() takes the
        // bin lock even when the cached connection is healthy (worse, a connect() for one dead
        // endpoint would block lookups of unrelated endpoints hashed to the same bin)
        ScpClient cached = conns.get(endpoint);
        if (cached != null && !cached.isClosed()) {
            return cached;
        }
        ScpClient c = conns.compute(endpoint, (ep, existing) -> {
            if (existing != null && !existing.isClosed()) return existing;
            Endpoint hp = Endpoint.parse(ep, "storage endpoint", ErrorCode.INTERNAL);
            try {
                return new ScpClient(hp.host(), hp.port(), ScpClient.KIND_BROKER, "strata-client");
            } catch (IOException e) {
                return null;
            }
        });
        if (c == null) throw new ScpException(ErrorCode.INTERNAL, "storage node unreachable: " + endpoint);
        return c;
    }

    @Override
    public void close() {
        conns.values().forEach(ScpClient::close);
        conns.clear();
    }
}
