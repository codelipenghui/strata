package io.strata.client;

import io.strata.common.ErrorCode;
import io.strata.common.Endpoint;
import io.strata.common.ScpException;
import io.strata.proto.ManagedScpConnection;
import io.strata.proto.ScpClient;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lazily-created SCP connection pools for storage-node endpoints. {@link #get} hands out a
 * round-robin connection within an endpoint; callers that need per-replica ordering (appenders and
 * readers) cache the returned connection for the lifetime of an open chunk.
 */
final class NodePool implements AutoCloseable {
    private final ClientConfig config;
    private final Map<String, EndpointPool> conns = new ConcurrentHashMap<>();

    NodePool(ClientConfig config) {
        this.config = config;
    }

    NodePool() {
        this(new ClientConfig(List.of("127.0.0.1:1"), 1, 1));
    }

    ManagedScpConnection get(String endpoint) {
        return endpointPool(endpoint).next();
    }

    private EndpointPool endpointPool(String endpoint) {
        if (endpoint == null) {
            throw new ScpException(ErrorCode.INTERNAL, "invalid storage endpoint: null");
        }
        Endpoint.parse(endpoint, "storage endpoint", ErrorCode.INTERNAL);
        return conns.computeIfAbsent(endpoint, EndpointPool::new);
    }

    @Override
    public void close() {
        conns.values().forEach(EndpointPool::close);
        conns.clear();
    }

    private final class EndpointPool implements AutoCloseable {
        private final ManagedScpConnection[] connections;
        private final AtomicInteger next = new AtomicInteger();

        EndpointPool(String endpoint) {
            connections = new ManagedScpConnection[config.storageConnectionsPerEndpoint()];
            for (int i = 0; i < connections.length; i++) {
                connections[i] = new ManagedScpConnection(
                        List.of(endpoint), config.connectionPolicy(), ScpClient.KIND_BROKER,
                        "strata-client-storage-" + i, "storage endpoint", false, true);
            }
        }

        ManagedScpConnection next() {
            int index = Math.floorMod(next.getAndIncrement(), connections.length);
            return connections[index];
        }

        @Override
        public void close() {
            for (ManagedScpConnection connection : connections) {
                connection.close();
            }
        }
    }
}
