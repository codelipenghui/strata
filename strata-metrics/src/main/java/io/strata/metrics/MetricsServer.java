package io.strata.metrics;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;

/**
 * Tiny HTTP server exposing Prometheus metrics at {@code /metrics}, a {@code /healthz} liveness
 * probe, and a {@code /readyz} readiness probe, backed by the JDK's built-in {@link HttpServer} —
 * no web framework dependency. A single daemon thread is plenty: scrapes are infrequent and cheap.
 * This same server is the natural foundation for future read-only admin/status endpoints.
 */
public final class MetricsServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);

    private final HttpServer server;

    private MetricsServer(HttpServer server) {
        this.server = server;
    }

    public static MetricsServer start(int port, StrataMetrics metrics) throws IOException {
        return start(port, metrics, () -> true);
    }

    public static MetricsServer start(int port, StrataMetrics metrics, BooleanSupplier ready) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = metrics.scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.createContext("/healthz", exchange -> {
            byte[] body = "ok\n".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.createContext("/readyz", exchange -> {
            boolean isReady;
            try {
                isReady = ready.getAsBoolean();
            } catch (RuntimeException e) {
                log.warn("readiness check failed", e);
                isReady = false;
            }
            byte[] body = (isReady ? "ready\n" : "not ready\n").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(isReady ? 200 : 503, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.setExecutor(Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "strata-metrics-http");
            t.setDaemon(true);
            return t;
        }));
        server.start();
        log.info("metrics endpoint listening on :{}/metrics", port);
        return new MetricsServer(server);
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
