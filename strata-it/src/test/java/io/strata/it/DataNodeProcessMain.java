package io.strata.it;

import io.strata.node.DataNodeConfig;
import io.strata.node.DataNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Test-only child process entry point for OS-level data-node crash tests.
 */
final class DataNodeProcessMain {
    private DataNodeProcessMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4 && args.length != 6) {
            throw new IllegalArgumentException(
                    "usage: DataNodeProcessMain <dataDir> <controllerEndpointsCsv> <host> <readyFile>"
                            + " [<listenPort> <advertisedEndpoint>]");
        }
        Path dataDir = Path.of(args[0]);
        List<String> controllerEndpoints = Arrays.stream(args[1].split(","))
                .filter(s -> !s.isBlank())
                .toList();
        String host = args[2];
        Path readyFile = Path.of(args[3]);
        DataNodeConfig config = DataNodeConfig.withMetadata(dataDir, controllerEndpoints, host);
        if (args.length == 6) {
            config = config.withListenPort(Integer.parseInt(args[4]))
                    .withAdvertisedEndpoint(args[5]);
        }

        try (DataNode node = new DataNode(config)) {
            long deadline = System.currentTimeMillis() + 15_000;
            while (node.nodeId() < 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            if (node.nodeId() < 0) {
                throw new IllegalStateException("data node did not register before deadline");
            }
            Files.createDirectories(readyFile.getParent());
            Files.writeString(readyFile, node.nodeId() + " " + node.endpoint() + System.lineSeparator());
            new CountDownLatch(1).await();
        }
    }
}
