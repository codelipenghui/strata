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

import static io.strata.common.Fsync.forceDirectory;

/**
 * Data node process: ChunkStore engine + SCP server + control loop (register/heartbeat/
 * scrub/commands; durability via owner-pull VERIFY_CHUNKS) against the metadata plane.
 * Identity is bound to the data volume
 * (tech design §10.4): nodeId + incarnationId persist in an identity file.
 */
public final class DataNode implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DataNode.class);

    private final DataNodeConfig config;
    private final ChunkStore store;
    private final ScpServer server;
    private final ControlLoop controlLoop;
    private final OrphanGc orphanGc; // node-local orphan GC (design §20.4); null in standalone mode
    private final AtomicBoolean draining = new AtomicBoolean(false);

    private final int nodeId;
    private final UUID incarnation;
    // Owners (by advertised endpoint) this node has heard a VERIFY_CHUNKS from. Node-local orphan GC
    // (design §20.4) only trusts "no owner verified" once it has heard from every current owner, so it
    // never deletes a chunk whose owner simply has not gotten around to verifying it yet. Populated here
    // by the VERIFY_CHUNKS handler; consumed by the orphan-GC loop.
    private final java.util.Set<String> verifiersHeardFrom = java.util.concurrent.ConcurrentHashMap.newKeySet();

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
        Identity identity = loadOrCreateIdentity(config.dataDir(), config.nodeId());
        this.nodeId = identity.nodeId;
        this.incarnation = identity.incarnation;
        ChunkStore openedStore = null;
        ScpServer openedServer = null;
        ControlLoop startedLoop = null;
        OrphanGc startedGc = null;
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
                // Node-local orphan GC (design §20.4): reclaim sealed chunks no owner references, after
                // confirming with the namespace owner. Only a registered node runs it (it needs a nodeId
                // to recognise itself in a descriptor and controller endpoints to ask).
                startedGc = new OrphanGc(openedStore, nodeId, config.controllerEndpoints());
                this.orphanGc = startedGc;
                startedGc.start();
            } else {
                this.controlLoop = null; // null in standalone data-plane tests
                this.orphanGc = null;
            }
        } catch (IOException | RuntimeException e) {
            Throwable closeFailure = closeAll(startedLoop, startedGc, openedServer, openedStore);
            if (closeFailure != null) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
        log.info("data node started: port={} incarnation={} nodeId={}", server.port(), incarnation, nodeId);
    }

    /** Closes whatever subset of the node's resources exists; returns the accumulated failure. */
    private static Throwable closeAll(ControlLoop loop, OrphanGc orphanGc, ScpServer server, ChunkStore store) {
        Throwable failure = null;
        if (loop != null) {
            try {
                loop.close();
            } catch (RuntimeException e) {
                failure = Closeables.suppress(failure, e);
            }
        }
        if (orphanGc != null) {
            try {
                orphanGc.close();
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

    public long channelCacheHits() { return store.channelCacheHits(); }
    public long channelCacheMisses() { return store.channelCacheMisses(); }
    public long channelCacheEvictions() { return store.channelCacheEvictions(); }
    public int cachedChannels() { return store.cachedChannels(); }
    public int channelCacheCapacity() { return store.channelCacheCapacity(); }
    public long openFds() { return store.openFds(); }

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

    /** Records that owner {@code verifierEndpoint} issued a VERIFY_CHUNKS to this node (design §20.4). */
    void noteVerifiedBy(String verifierEndpoint) {
        verifiersHeardFrom.add(verifierEndpoint);
    }

    /** The set of owner endpoints this node has heard a VERIFY_CHUNKS from (orphan-GC membership grace). */
    java.util.Set<String> verifiersHeardFrom() {
        return verifiersHeardFrom;
    }

    void setDraining(boolean v) {
        draining.set(v);
    }

    /* ---------------- volume-bound identity ---------------- */

    record Identity(int nodeId, UUID incarnation) {}

    /**
     * Resolves this node's identity, binding the externally-supplied {@code configuredNodeId}
     * ({@code STRATA_NODE_ID}, or -1 for standalone/data-plane tests) to the volume:
     * <ul>
     *   <li>existing volume: the recorded id must match the configured id, else we refuse to start —
     *       a misconfigured id would let this process impersonate another node and corrupt placement;
     *       a previously-standalone volume (recorded -1) adopts the configured id, keeping its incarnation;</li>
     *   <li>fresh volume: persist {@code (configuredNodeId, new incarnation)}.</li>
     * </ul>
     * The incarnation is minted once and stays volume-bound across restarts.
     */
    private static Identity loadOrCreateIdentity(Path dataDir, int configuredNodeId) throws IOException {
        Path f = dataDir.resolve("identity.properties");
        if (Files.exists(f)) {
            Identity onVolume = readIdentity(f);
            if (configuredNodeId >= 1 && onVolume.nodeId() != configuredNodeId) {
                if (onVolume.nodeId() != -1) {
                    throw new IOException("configured STRATA_NODE_ID " + configuredNodeId
                            + " does not match this volume's recorded node id " + onVolume.nodeId()
                            + " — refusing to start (this data volume belongs to node " + onVolume.nodeId() + ")");
                }
                // a previously-standalone volume adopts the configured id, keeping its incarnation
                Identity adopted = new Identity(configuredNodeId, onVolume.incarnation());
                persistIdentity(dataDir, adopted);
                return adopted;
            }
            return onVolume;
        }
        // configuredNodeId is already validated to be -1 (standalone) or >= 1 by DataNodeConfig.
        Identity fresh = new Identity(configuredNodeId, UUID.randomUUID());
        persistIdentity(dataDir, fresh);
        return fresh;
    }

    private static Identity readIdentity(Path f) throws IOException {
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

    @Override
    public void close() throws IOException {
        Closeables.throwIfFailed(closeAll(controlLoop, orphanGc, server, store));
    }
}
