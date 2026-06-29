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
 *
 * <p>The node id is supplied by the launcher (the ZK allocator was removed): it must be a positive,
 * volume-bound identity that is stable across restarts on the same dataDir.
 */
final class DataNodeProcessMain {
    private DataNodeProcessMain() {
    }

    /**
     * Derives a stable, positive node id from a host name for process-launched nodes: deterministic so a
     * respawn on the same host/dataDir reuses the same id (volume-bound identity), and host-scoped so
     * distinct process hosts get distinct ids. In-process harnesses (MiniCluster) hand out counter ids instead.
     */
    static int nodeIdForHost(String host) {
        return (host.hashCode() & 0x7fff_ffff) % 1_000_000 + 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5 && args.length != 7) {
            throw new IllegalArgumentException(
                    "usage: DataNodeProcessMain <dataDir> <controllerEndpointsCsv> <host> <nodeId>"
                            + " <readyFile> [<listenPort> <advertisedEndpoint>]");
        }
        Path dataDir = Path.of(args[0]);
        List<String> controllerEndpoints = Arrays.stream(args[1].split(","))
                .filter(s -> !s.isBlank())
                .toList();
        String host = args[2];
        int nodeId = Integer.parseInt(args[3]);
        Path readyFile = Path.of(args[4]);
        DataNodeConfig config = DataNodeConfig.withMetadata(dataDir, controllerEndpoints, host)
                .withNodeId(nodeId);
        if (args.length == 7) {
            config = config.withListenPort(Integer.parseInt(args[5]))
                    .withAdvertisedEndpoint(args[6]);
        }

        try (DataNode node = new DataNode(config)) {
            // node.nodeId() is resolved at construction (no allocator round-trip), so there is no
            // registration wait: the id is valid immediately. Just publish the ready marker.
            assert node.nodeId() >= 1 : "data node id must be positive: " + node.nodeId();
            Files.createDirectories(readyFile.getParent());
            Files.writeString(readyFile, node.nodeId() + " " + node.endpoint() + System.lineSeparator());
            new CountDownLatch(1).await();
        }
    }
}
