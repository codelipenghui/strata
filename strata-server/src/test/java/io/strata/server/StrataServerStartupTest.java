package io.strata.server;

import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StrataServerStartupTest {

    @TempDir
    Path dir;

    @Test
    void standaloneNodeClosesListenerWhenMetricsBindFails() throws Exception {
        try (ServerSocket metricsBlocker = new ServerSocket(0)) {
            Path output = dir.resolve("strata-node-startup.log");
            int nodePort = freePort();
            ProcessBuilder builder = new ProcessBuilder(
                    javaBinary(),
                    "-cp",
                    System.getProperty("java.class.path"),
                    NodeMetricsBindFailureProbe.class.getName());
            builder.redirectErrorStream(true);
            builder.redirectOutput(output.toFile());
            builder.environment().put("STRATA_DATA_DIR", dir.resolve("node-data").toString());
            builder.environment().put("STRATA_NODE_ID", "1");
            builder.environment().put("STRATA_LISTEN_PORT", Integer.toString(nodePort));
            builder.environment().put("STRATA_CONTROLLER_ENDPOINTS", "127.0.0.1:1");
            builder.environment().put("STRATA_METRICS_PORT", Integer.toString(metricsBlocker.getLocalPort()));
            builder.environment().put("STRATA_METRICS_ENABLED", "true");

            Process process = builder.start();
            try {
                boolean exited = process.waitFor(3, TimeUnit.SECONDS);
                String log = readOutput(output);
                assertTrue(exited,
                        () -> "metrics bind failure should not leak a live standalone node process\n" + log);
                assertEquals(0, process.exitValue(), () -> log);
                assertTrue(log.contains("BindException") || log.contains("Address already in use"),
                        () -> "child JVM did not fail for the intended metrics bind reason\n" + log);
                assertTrue(log.contains("LISTENER_STILL_OPEN=false"), () -> log);
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                    process.waitFor(3, TimeUnit.SECONDS);
                }
            }
        }
    }

    private static String javaBinary() {
        return Path.of(System.getProperty("java.home"), "bin", "java").toString();
    }

    private static String readOutput(Path output) throws Exception {
        return Files.exists(output) ? Files.readString(output) : "";
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    public static final class NodeMetricsBindFailureProbe {
        public static void main(String[] args) throws Exception {
            try {
                StrataServer.main(new String[] {"data-node"});
            } catch (Throwable t) {
                t.printStackTrace(System.out);
            }
            boolean listenerStillOpen = false;
            int nodePort = Integer.parseInt(System.getenv("STRATA_LISTEN_PORT"));
            try (ScpClient client = new ScpClient("127.0.0.1", nodePort, ScpClient.KIND_TOOL, "startup-probe")) {
                client.call(Opcode.PING, Messages.okHeader(), null, 1_000);
                listenerStillOpen = true;
            } catch (Throwable ignored) {
                // Expected once startup cleanup closes the partially-started node.
            }
            System.out.println("LISTENER_STILL_OPEN=" + listenerStillOpen);
            System.exit(listenerStillOpen ? 7 : 0);
        }
    }
}
