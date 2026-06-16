package io.strata.metrics;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsServerTest {

    @Test
    void closeStopsExecutorThreadAfterServingRequest() throws Exception {
        long before = metricsThreads();
        StrataMetrics metrics = new StrataMetrics("test");
        MetricsServer endpoint = MetricsServer.start(0, metrics);
        try {
            int port = port(endpoint);
            assertEquals(200, status(port, "/healthz"));
            waitFor(() -> metricsThreads() > before, "metrics executor thread never started");
        } finally {
            endpoint.close();
            metrics.close();
        }

        waitFor(() -> metricsThreads() <= before, "metrics executor thread leaked after close()");
    }

    @Test
    void readinessEndpointIsSeparateFromLiveness() throws Exception {
        int port = freePort();
        try (StrataMetrics metrics = new StrataMetrics("test");
             MetricsServer ignored = MetricsServer.start(port, metrics)) {
            assertEquals(200, status(port, "/healthz"));
            assertEquals(200, status(port, "/readyz"),
                    "operators need a readiness probe distinct from process liveness");
        }
    }

    @Test
    void readinessEndpointReflectsReadinessSupplier() throws Exception {
        int port = freePort();
        AtomicBoolean ready = new AtomicBoolean(false);
        try (StrataMetrics metrics = new StrataMetrics("test");
             MetricsServer ignored = MetricsServer.start(port, metrics, ready::get)) {
            assertEquals(200, status(port, "/healthz"));
            assertEquals(503, status(port, "/readyz"),
                    "readiness must stay false while the role cannot safely receive traffic");

            ready.set(true);

            assertEquals(200, status(port, "/readyz"));
        }
    }

    private static int port(MetricsServer endpoint) throws Exception {
        Field field = MetricsServer.class.getDeclaredField("server");
        field.setAccessible(true);
        HttpServer server = (HttpServer) field.get(endpoint);
        return server.getAddress().getPort();
    }

    private static int status(int port, String path) throws Exception {
        HttpURLConnection connection = (HttpURLConnection) URI.create("http://127.0.0.1:" + port + path)
                .toURL()
                .openConnection();
        connection.setConnectTimeout(2_000);
        connection.setReadTimeout(2_000);
        connection.setRequestMethod("GET");
        try {
            return connection.getResponseCode();
        } finally {
            connection.disconnect();
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static long metricsThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .filter(t -> t.getName().equals("strata-metrics-http"))
                .count();
    }

    private static void waitFor(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), message);
    }
}
