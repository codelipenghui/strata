package io.strata.it;

import io.strata.meta.ControllerConfig;
import io.strata.meta.Controller;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/**
 * Test-only child process entry point for OS-level metadata-service crash tests.
 */
final class ControllerProcessMain {
    private ControllerProcessMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "usage: ControllerProcessMain <zkConnect> <listenPort> <readyFile>");
        }
        String zkConnect = args[0];
        int listenPort = Integer.parseInt(args[1]);
        Path readyFile = Path.of(args[2]);

        ControllerConfig config = new ControllerConfig(zkConnect, listenPort, 200, 1_000, 1_500, 300, 3_000,
                2_000, 2_000, "127.0.0.1", 90_000);
        try (Controller service = new Controller(config)) {
            Files.createDirectories(readyFile.getParent());
            Files.writeString(readyFile, service.endpoint() + System.lineSeparator());
            new CountDownLatch(1).await();
        }
    }
}
