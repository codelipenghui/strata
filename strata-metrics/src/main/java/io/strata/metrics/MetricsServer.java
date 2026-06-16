package io.strata.metrics;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Tiny HTTP server exposing Prometheus metrics at {@code /metrics} (and a {@code /healthz} liveness
 * probe), backed by the JDK's built-in {@link HttpServer} — no web framework dependency. A single
 * daemon thread is plenty: scrapes are infrequent and cheap. This same server is the natural
 * foundation for the future read-only admin/status endpoints.
 */
public final class MetricsServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);

    private final HttpServer server;
    private final ExecutorService executor;

    private MetricsServer(HttpServer server, ExecutorService executor) {
        this.server = server;
        this.executor = executor;
    }

    public static MetricsServer start(int port, StrataMetrics metrics) throws IOException {
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
        ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "strata-metrics-http");
            t.setDaemon(true);
            return t;
        });
        server.setExecutor(executor);
        server.start();
        log.info("metrics endpoint listening on :{}/metrics", port);
        return new MetricsServer(server, executor);
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }
}
