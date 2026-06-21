package io.strata.node;

import io.strata.common.Closeables;
import io.strata.format.ChunkStore;
import io.strata.proto.ScpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Data node process: ChunkStore engine + SCP server + control loop (register/heartbeat/
 * inventory/commands) against the metadata plane. Identity is bound to the data volume
 * (tech design §10.4): nodeId + incarnationId persist in an identity file.
 */
public final class DataNode implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DataNode.class);

    private final DataNodeConfig config;
    private final ChunkStore store;
    private final ScpServer server;
    private final ControlLoop controlLoop;
    private final AtomicBoolean draining = new AtomicBoolean(false);

    private volatile int nodeId;
    private final UUID incarnation;

    public DataNode(DataNodeConfig config) throws IOException {
        this(config, null);
    }

    /**
     * With a non-null {@code controllerHandler}, this node's single SCP listener also serves the
     * control-plane/metadata opcodes by routing them to that handler — combined mode, where a
     * co-resident controller shares the node's port instead of binding its own.
     */
    public DataNode(DataNodeConfig config, ScpServer.Handler controllerHandler) throws IOException {
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
            DataNodeHandlers dataHandler = new DataNodeHandlers(openedStore, this);
            ScpServer.Handler handler = controllerHandler == null
                    ? dataHandler
                    : ScpServer.Handler.route(dataHandler, controllerHandler);
            openedServer = new ScpServer(config.listenPort(), nodeId,
                    incarnation.getMostSignificantBits(), incarnation.getLeastSignificantBits(), handler);
            this.store = openedStore;
            this.server = openedServer;
            if (!config.controllerEndpoints().isEmpty()) {
                startedLoop = new ControlLoop(this, config, openedStore);
                this.controlLoop = startedLoop;
                dataHandler.controlLoop(startedLoop); // serve direct owner-repair EXEC_REPLICATE
                startedLoop.start();
            } else {
                this.controlLoop = null; // null in standalone data-plane tests
            }
        } catch (IOException | RuntimeException e) {
            Throwable closeFailure = closeAll(startedLoop, openedServer, openedStore);
            if (closeFailure != null) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
        log.info("data node started: port={} incarnation={} nodeId={}", server.port(), incarnation, nodeId);
    }

    /** Closes whatever subset of the node's resources exists; returns the accumulated failure. */
    private static Throwable closeAll(ControlLoop loop, ScpServer server, ChunkStore store) {
        Throwable failure = null;
        if (loop != null) {
            try {
                loop.close();
            } catch (RuntimeException e) {
                failure = Closeables.suppress(failure, e);
            }
        }
        if (server != null) {
            try {
                server.close();
            } catch (RuntimeException e) {
                failure = Closeables.suppress(failure, e);
            }
        }
        if (store != null) {
            try {
                store.close();
            } catch (IOException | RuntimeException e) {
                failure = Closeables.suppress(failure, e);
            }
        }
        return failure;
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

    // --- observability accessors (read-only; consumed by the metrics layer in strata-server) ---

    /** Whether this node currently holds a metadata registration (false in standalone mode). */
    public boolean registered() {
        return controlLoop != null && controlLoop.registered();
    }

    public long diskUsedBytes() {
        return store.usedBytes();
    }

    public long capacityBytes() {
        return config.capacityBytes();
    }

    public int openChunks() {
        return store.openChunks();
    }

    public int sealedChunks() {
        return store.sealedChunks();
    }

    public long fsyncForceCount() {
        return store.fsyncForceCount();
    }

    public long appendOps() {
        return store.appendOps();
    }

    public long appendBytes() {
        return store.appendBytes();
    }

    public long readOps() {
        return store.readOps();
    }

    public long readBytes() {
        return store.readBytes();
    }

    public long backgroundFlushes() {
        return store.backgroundFlushes();
    }

    /** Installs a per-request latency observer on the data-plane server (used by the metrics layer). */
    public void setRequestObserver(io.strata.proto.RequestObserver observer) {
        server.setRequestObserver(observer);
    }

    public UUID incarnation() {
        return incarnation;
    }

    public ChunkStore store() {
        return store;
    }

    public DataNodeConfig config() {
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
            persistIdentity(config.dataDir(), new Identity(id, incarnation));
            this.nodeId = id;
            server.setNodeId(id); // HELLO responses must announce the real id, not -1
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
                throw new IOException("data node identity is missing nodeId or incarnation: " + f);
            }
            try {
                int nodeId = Integer.parseInt(nodeIdText);
                if (nodeId < -1) {
                    throw new IllegalArgumentException("nodeId " + nodeId + " < -1");
                }
                return new Identity(nodeId, UUID.fromString(incarnationText));
            } catch (IllegalArgumentException e) {
                throw new IOException("invalid data node identity: " + f, e);
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
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
             var out = Channels.newOutputStream(ch)) {
            p.store(out, "strata data node identity — bound to this volume");
            out.flush();
            ch.force(true);
        }
        Files.move(tmp, f, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        forceDirectory(dataDir);
    }

    private static void forceDirectory(Path dir) throws IOException {
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        }
    }

    @Override
    public void close() throws IOException {
        Closeables.throwIfFailed(closeAll(controlLoop, server, store));
    }
}
