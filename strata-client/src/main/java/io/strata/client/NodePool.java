package io.strata.client;

import io.strata.common.ErrorCode;
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
        ScpClient c = conns.compute(endpoint, (ep, existing) -> {
            if (existing != null && !existing.isClosed()) return existing;
            HostPort hp = parseEndpoint(ep);
            try {
                return new ScpClient(hp.host(), hp.port(), ScpClient.KIND_BROKER, "strata-client");
            } catch (IOException e) {
                return null;
            }
        });
        if (c == null) throw new ScpException(ErrorCode.INTERNAL, "storage node unreachable: " + endpoint);
        return c;
    }

    private record HostPort(String host, int port) {}

    private static HostPort parseEndpoint(String endpoint) {
        int colon = endpoint == null ? -1 : endpoint.lastIndexOf(':');
        if (colon <= 0 || colon == endpoint.length() - 1) {
            throw new ScpException(ErrorCode.INTERNAL, "invalid storage endpoint: " + endpoint);
        }
        String host = endpoint.substring(0, colon);
        if (host.startsWith("[") != host.endsWith("]")) {
            throw new ScpException(ErrorCode.INTERNAL, "invalid storage endpoint brackets: " + endpoint);
        }
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        int port;
        try {
            port = Integer.parseInt(endpoint.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new ScpException(ErrorCode.INTERNAL, "invalid storage endpoint port: " + endpoint);
        }
        if (host.isBlank() || !host.equals(host.trim()) || port <= 0 || port > 65_535) {
            throw new ScpException(ErrorCode.INTERNAL, "invalid storage endpoint: " + endpoint);
        }
        return new HostPort(host, port);
    }

    @Override
    public void close() {
        conns.values().forEach(ScpClient::close);
        conns.clear();
    }
}
