package io.strata.node;

import io.strata.format.ChunkStore;
import io.strata.proto.ScpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Storage node process: ChunkStore engine + SCP server + control loop (register/heartbeat/
 * inventory/commands) against the metadata plane. Identity is bound to the data volume
 * (tech design §10.4): nodeId + incarnationId persist in an identity file.
 */
public final class StorageNode implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(StorageNode.class);

    private final NodeConfig config;
    private final ChunkStore store;
    private final ScpServer server;
    private final ControlLoop controlLoop;
    private final AtomicBoolean draining = new AtomicBoolean(false);

    private volatile int nodeId;
    private final UUID incarnation;

    public StorageNode(NodeConfig config) throws IOException {
        this.config = config;
        Files.createDirectories(config.dataDir());
        Identity identity = loadIdentity(config.dataDir());
        this.nodeId = identity.nodeId;
        this.incarnation = identity.incarnation;
        this.store = new ChunkStore(config.dataDir().resolve("chunks"));
        this.server = new ScpServer(config.listenPort(), nodeId,
                incarnation.getMostSignificantBits(), incarnation.getLeastSignificantBits(),
                new NodeHandlers(store, this));
        if (config.metadataEndpoints().isEmpty()) {
            this.controlLoop = null; // standalone (data-plane tests)
        } else {
            this.controlLoop = new ControlLoop(this, config, store);
            this.controlLoop.start();
        }
        log.info("storage node started: port={} incarnation={} nodeId={}", server.port(), incarnation, nodeId);
    }

    public int port() {
        return server.port();
    }

    public String endpoint() {
        return config.advertisedHost() + ":" + port();
    }

    public int nodeId() {
        return nodeId;
    }

    public UUID incarnation() {
        return incarnation;
    }

    public ChunkStore store() {
        return store;
    }

    public NodeConfig config() {
        return config;
    }

    public boolean isDraining() {
        return draining.get();
    }

    void setDraining(boolean v) {
        draining.set(v);
    }

    /** Called by the control loop once the metadata plane assigns/confirms our node id. */
    void nodeIdAssigned(int id) throws IOException {
        if (this.nodeId != id) {
            this.nodeId = id;
            persistIdentity(config.dataDir(), new Identity(id, incarnation));
        }
    }

    /* ---------------- volume-bound identity ---------------- */

    record Identity(int nodeId, UUID incarnation) {}

    private static Identity loadIdentity(Path dataDir) throws IOException {
        Path f = dataDir.resolve("identity.properties");
        if (Files.exists(f)) {
            Properties p = new Properties();
            try (var in = Files.newInputStream(f)) {
                p.load(in);
            }
            return new Identity(Integer.parseInt(p.getProperty("nodeId", "-1")),
                    UUID.fromString(p.getProperty("incarnation")));
        }
        Identity fresh = new Identity(-1, UUID.randomUUID());
        persistIdentity(dataDir, fresh);
        return fresh;
    }

    private static void persistIdentity(Path dataDir, Identity id) throws IOException {
        Properties p = new Properties();
        p.setProperty("nodeId", String.valueOf(id.nodeId));
        p.setProperty("incarnation", id.incarnation.toString());
        Path f = dataDir.resolve("identity.properties");
        Path tmp = dataDir.resolve("identity.properties.tmp");
        try (var out = Files.newOutputStream(tmp)) {
            p.store(out, "strata storage node identity — bound to this volume");
        }
        Files.move(tmp, f, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public void close() throws IOException {
        if (controlLoop != null) controlLoop.close();
        server.close();
        store.close();
    }
}
