package io.strata.it;

import io.strata.meta.MetaConfig;
import io.strata.meta.MetadataService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/**
 * Test-only child process entry point for OS-level metadata-service crash tests.
 */
final class MetadataServiceProcessMain {
    private MetadataServiceProcessMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "usage: MetadataServiceProcessMain <zkConnect> <listenPort> <readyFile>");
        }
        String zkConnect = args[0];
        int listenPort = Integer.parseInt(args[1]);
        Path readyFile = Path.of(args[2]);

        MetaConfig config = new MetaConfig(zkConnect, listenPort, 200, 1_000, 1_500, 300, 3_000,
                2_000, 2_000, "127.0.0.1");
        try (MetadataService service = new MetadataService(config)) {
            Files.createDirectories(readyFile.getParent());
            Files.writeString(readyFile, service.endpoint() + System.lineSeparator());
            new CountDownLatch(1).await();
        }
    }
}
