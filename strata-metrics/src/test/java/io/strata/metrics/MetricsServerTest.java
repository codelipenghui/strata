package io.strata.metrics;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;
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
            HttpResponse<String> response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/healthz")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            waitFor(() -> metricsThreads() > before, "metrics executor thread never started");
        } finally {
            endpoint.close();
            metrics.close();
        }

        waitFor(() -> metricsThreads() <= before, "metrics executor thread leaked after close()");
    }

    private static int port(MetricsServer endpoint) throws Exception {
        Field field = MetricsServer.class.getDeclaredField("server");
        field.setAccessible(true);
        HttpServer server = (HttpServer) field.get(endpoint);
        return server.getAddress().getPort();
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
