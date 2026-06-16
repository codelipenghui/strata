package io.strata.metrics;

import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetricsServerTest {

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
}
