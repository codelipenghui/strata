package io.strata.proto;

import io.strata.common.ErrorCode;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Optional compatibility matrix against a resolved released strata-proto artifact. Enable with
 * scripts/scp-compat.sh so the reference artifact runs in a separate JVM/classpath.
 */
class ScpReleasedArtifactCompatibilityTest {
    private static final String REFERENCE_CP_PROPERTY = "strata.compat.referenceClasspath";
    private static final String REFERENCE_VERSION_PROPERTY = "strata.compat.referenceVersion";

    @Test
    void releasedClientCanCallCurrentServer() throws Exception {
        String referenceCp = referenceClasspathOrSkip();
        String version = System.getProperty(REFERENCE_VERSION_PROPERTY, "unknown");
        ArrayDeque<ScpCompatCorpus.Exchange> expected = new ArrayDeque<>(ScpCompatCorpus.exchanges());
        AtomicReference<Throwable> serverFailure = new AtomicReference<>();

        try (ScpServer server = new ScpServer(0, 42, 0x1111222233334444L, 0x5555666677778888L, req -> {
            try {
                ScpCompatCorpus.Exchange exchange = expected.removeFirst();
                assertEquals(Opcode.valueOf(exchange.opcodeName()).code, req.opcode(), exchange.name());
                assertEquals(exchange.requestHeaderHex(), ScpCompatCorpus.hex(req.headerSlice()), exchange.name());
                assertEquals(exchange.requestPayloadHex(), ScpCompatCorpus.hex(req.payloadSlice()), exchange.name());
                return ScpServer.ok(req, exchange.responseHeader(),
                        ScpCompatCorpus.payloadBuffer(exchange.responsePayload()));
            } catch (Throwable t) {
                serverFailure.compareAndSet(null, t);
                return Frame.response(req, Resp.error(ErrorCode.INTERNAL, t.toString(), 0), null);
            }
        })) {
            Process process = startReferencePeer(referenceCp, "client", "127.0.0.1",
                    Integer.toString(server.port()));
            assertProcessExit(process, "released client " + version);
            assertTrue(expected.isEmpty(), "released client did not send all expected exchanges");
            if (serverFailure.get() != null) {
                throw new AssertionError("current server rejected released client " + version,
                        serverFailure.get());
            }
        }
    }

    @Test
    void currentClientCanCallReleasedServer() throws Exception {
        String referenceCp = referenceClasspathOrSkip();
        String version = System.getProperty(REFERENCE_VERSION_PROPERTY, "unknown");
        Path readyFile = Files.createTempDirectory("strata-scp-compat").resolve("server.ready");
        Process process = startReferencePeer(referenceCp, "server", readyFile.toString());
        int port = awaitReadyPort(readyFile);

        try (ScpClient client = new ScpClient("127.0.0.1", port,
                ScpClient.KIND_BROKER, "scp-compat-current-client")) {
            for (ScpCompatCorpus.Exchange exchange : ScpCompatCorpus.exchanges()) {
                Frame response = client.callFrame(Opcode.valueOf(exchange.opcodeName()),
                        exchange.requestHeader(),
                        ScpCompatCorpus.payloadBuffer(exchange.requestPayload()),
                        5_000);
                assertEquals(exchange.responseHeaderHex(), ScpCompatCorpus.hex(response.headerSlice()),
                        exchange.name());
                assertEquals(exchange.responsePayloadHex(), ScpCompatCorpus.hex(response.payloadSlice()),
                        exchange.name());
            }
        }
        assertProcessExit(process, "released server " + version);
    }

    private static String referenceClasspathOrSkip() {
        String cp = System.getProperty(REFERENCE_CP_PROPERTY);
        Assumptions.assumeTrue(cp != null && !cp.isBlank(),
                "set " + REFERENCE_CP_PROPERTY + " to run released-artifact compatibility");
        return cp;
    }

    private static Process startReferencePeer(String referenceCp, String... peerArgs) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Dorg.slf4j.simpleLogger.defaultLogLevel=warn");
        command.add("-cp");
        command.add(Path.of("target", "test-classes") + File.pathSeparator + referenceCp);
        command.add(ScpCompatPeerMain.class.getName());
        command.addAll(List.of(peerArgs));
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        return builder.start();
    }

    private static int awaitReadyPort(Path readyFile) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.exists(readyFile)) {
                String value = Files.readString(readyFile).trim();
                if (!value.isBlank()) {
                    return Integer.parseInt(value);
                }
            }
            Thread.sleep(50);
        }
        throw new AssertionError("released server did not become ready: " + readyFile);
    }

    private static void assertProcessExit(Process process, String description) throws Exception {
        assertTrue(process.waitFor(15, TimeUnit.SECONDS), description + " did not exit");
        assertEquals(0, process.exitValue(), description + " exited with failure");
    }
}
