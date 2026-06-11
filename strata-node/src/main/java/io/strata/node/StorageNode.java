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
        ChunkStore openedStore = null;
        ScpServer openedServer = null;
        ControlLoop startedLoop = null;
        try {
            openedStore = new ChunkStore(config.dataDir().resolve("chunks"));
            openedServer = new ScpServer(config.listenPort(), nodeId,
                    incarnation.getMostSignificantBits(), incarnation.getLeastSignificantBits(),
                    new NodeHandlers(openedStore, this));
            if (!config.metadataEndpoints().isEmpty()) {
                startedLoop = new ControlLoop(this, config, openedStore);
                startedLoop.start();
            }
            this.store = openedStore;
            this.server = openedServer;
            this.controlLoop = startedLoop; // null in standalone data-plane tests
        } catch (IOException | RuntimeException e) {
            cleanupFailedStart(startedLoop, openedServer, openedStore, e);
            throw e;
        }
        log.info("storage node started: port={} incarnation={} nodeId={}", server.port(), incarnation, nodeId);
    }

    private static void cleanupFailedStart(ControlLoop loop, ScpServer server, ChunkStore store, Throwable failure) {
        if (loop != null) {
            try {
                loop.close();
            } catch (RuntimeException e) {
                failure.addSuppressed(e);
            }
        }
        if (server != null) {
            try {
                server.close();
            } catch (RuntimeException e) {
                failure.addSuppressed(e);
            }
        }
        if (store != null) {
            try {
                store.close();
            } catch (IOException | RuntimeException e) {
                failure.addSuppressed(e);
            }
        }
    }

    private static Throwable suppress(Throwable failure, Throwable next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
    }

    private static void throwCloseFailure(Throwable failure) throws IOException {
        if (failure == null) {
            return;
        }
        if (failure instanceof IOException e) {
            throw e;
        }
        if (failure instanceof RuntimeException e) {
            throw e;
        }
        throw new IOException(failure);
    }

    public int port() {
        return server.port();
    }

    public String endpoint() {
        if (config.advertisedEndpointOverride() != null) {
            return config.advertisedEndpointOverride();
        }
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
            server.setNodeId(id); // HELLO responses must announce the real id, not -1
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
            String nodeIdText = p.getProperty("nodeId");
            String incarnationText = p.getProperty("incarnation");
            if (nodeIdText == null || incarnationText == null) {
                throw new IOException("storage node identity is missing nodeId or incarnation: " + f);
            }
            try {
                int nodeId = Integer.parseInt(nodeIdText);
                if (nodeId < -1) {
                    throw new IllegalArgumentException("nodeId " + nodeId + " < -1");
                }
                return new Identity(nodeId, UUID.fromString(incarnationText));
            } catch (IllegalArgumentException e) {
                throw new IOException("invalid storage node identity: " + f, e);
            }
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
        Throwable failure = null;
        if (controlLoop != null) {
            try {
                controlLoop.close();
            } catch (RuntimeException e) {
                failure = suppress(failure, e);
            }
        }
        try {
            server.close();
        } catch (RuntimeException e) {
            failure = suppress(failure, e);
        }
        try {
            store.close();
        } catch (IOException | RuntimeException e) {
            failure = suppress(failure, e);
        }
        throwCloseFailure(failure);
    }
}
