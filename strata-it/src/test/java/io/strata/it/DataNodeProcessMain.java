package io.strata.it;

import io.strata.node.NodeConfig;
import io.strata.node.StorageNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Test-only child process entry point for OS-level storage-node crash tests.
 */
final class StorageNodeProcessMain {
    private StorageNodeProcessMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4 && args.length != 6) {
            throw new IllegalArgumentException(
                    "usage: StorageNodeProcessMain <dataDir> <metadataEndpointsCsv> <host> <readyFile>"
                            + " [<listenPort> <advertisedEndpoint>]");
        }
        Path dataDir = Path.of(args[0]);
        List<String> metadataEndpoints = Arrays.stream(args[1].split(","))
                .filter(s -> !s.isBlank())
                .toList();
        String host = args[2];
        Path readyFile = Path.of(args[3]);
        NodeConfig config = NodeConfig.withMetadata(dataDir, metadataEndpoints, host);
        if (args.length == 6) {
            config = config.withListenPort(Integer.parseInt(args[4]))
                    .withAdvertisedEndpoint(args[5]);
        }

        try (StorageNode node = new StorageNode(config)) {
            long deadline = System.currentTimeMillis() + 15_000;
            while (node.nodeId() < 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            if (node.nodeId() < 0) {
                throw new IllegalStateException("storage node did not register before deadline");
            }
            Files.createDirectories(readyFile.getParent());
            Files.writeString(readyFile, node.nodeId() + " " + node.endpoint() + System.lineSeparator());
            new CountDownLatch(1).await();
        }
    }
}
