package io.strata.client;

import io.strata.common.ErrorCode;
import io.strata.common.Endpoint;
import io.strata.common.ScpException;
import io.strata.proto.ManagedScpConnection;
import io.strata.proto.ScpClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lazily-created SCP connection pools for storage-node endpoints. {@link #get} hands out a
 * round-robin connection within an endpoint; callers that need per-replica ordering (appenders and
 * readers) cache the returned connection for the lifetime of an open chunk.
 */
final class NodePool implements AutoCloseable {
    private final ClientConfig config;
    private final String clientIdPrefix;
    private final Map<String, EndpointPool> conns = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    NodePool(ClientConfig config) {
        this(config, "strata-client-storage");
    }

    NodePool(ClientConfig config, String clientIdPrefix) {
        this.config = config;
        this.clientIdPrefix = Objects.requireNonNull(clientIdPrefix, "clientIdPrefix");
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
        if (closed.get()) {
            throw new ScpException(ErrorCode.INTERNAL, "node pool is closed");
        }
        EndpointPool pool = conns.computeIfAbsent(endpoint, EndpointPool::new);
        if (closed.get()) {
            // raced with close(): if our insert slipped past the close sweep, close it here so its
            // connections and monitor threads do not leak; either way reject rather than hand back
            // a pool that close() may have already torn down
            if (conns.remove(endpoint, pool)) {
                pool.close();
            }
            throw new ScpException(ErrorCode.INTERNAL, "node pool is closed");
        }
        return pool;
    }

    @Override
    public void close() {
        // set closed BEFORE the sweep so a racing get() observes it and closes its own late insert;
        // remove-and-close each pool (clear() alone would drop references without closing them)
        closed.set(true);
        for (String endpoint : new ArrayList<>(conns.keySet())) {
            EndpointPool pool = conns.remove(endpoint);
            if (pool != null) {
                pool.close();
            }
        }
    }

    private final class EndpointPool implements AutoCloseable {
        private final ManagedScpConnection[] connections;
        private final AtomicInteger next = new AtomicInteger();

        EndpointPool(String endpoint) {
            connections = new ManagedScpConnection[config.storageConnectionsPerEndpoint()];
            for (int i = 0; i < connections.length; i++) {
                connections[i] = new ManagedScpConnection(
                        List.of(endpoint), config.connectionPolicy(), ScpClient.KIND_BROKER,
                        clientIdPrefix + "-" + i, "storage endpoint", false, true);
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
