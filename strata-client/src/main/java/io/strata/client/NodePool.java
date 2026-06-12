package io.strata.client;

import io.strata.common.ErrorCode;
import io.strata.common.Endpoint;
import io.strata.common.ScpException;
import io.strata.proto.ManagedScpConnection;
import io.strata.proto.ScpClient;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One SCP connection per storage-node endpoint, lazily created, replaced when closed.
 * All appends for a chunk flow through one connection (§10.2 ordering rule) — guaranteed
 * here because an endpoint maps to exactly one connection.
 */
final class NodePool implements AutoCloseable {
    private final ClientConfig config;
    private final Map<String, ManagedScpConnection> conns = new ConcurrentHashMap<>();

    NodePool(ClientConfig config) {
        this.config = config;
    }

    NodePool() {
        this(new ClientConfig(List.of("127.0.0.1:1"), 1, 1));
    }

    ManagedScpConnection get(String endpoint) {
        if (endpoint == null) {
            throw new ScpException(ErrorCode.INTERNAL, "invalid storage endpoint: null");
        }
        Endpoint.parse(endpoint, "storage endpoint", ErrorCode.INTERNAL);
        return conns.computeIfAbsent(endpoint, ep -> new ManagedScpConnection(
                List.of(ep), config.connectionPolicy(), ScpClient.KIND_BROKER,
                "strata-client", "storage endpoint", false, true));
    }

    @Override
    public void close() {
        conns.values().forEach(ManagedScpConnection::close);
        conns.clear();
    }
}
