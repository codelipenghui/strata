package io.strata.node;

import io.strata.common.Closeables;
import io.strata.common.ChunkId;
import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.format.ChunkStore;
import io.strata.proto.RequestObserver;
import io.strata.proto.RequestContext;
import io.strata.proto.ScpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.strata.common.Fsync.ensureDirectoryDurable;
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
    private final ChunkDeleteService deleteService;
    private final ScpServer server;
    private final ControlLoop controlLoop;
    private final OrphanGc orphanGc; // node-local orphan GC (tech design §9.2); null in standalone mode
    private final AtomicBoolean draining = new AtomicBoolean(false);
    private final AtomicLong ownerEpochFenceRejects = new AtomicLong();
    private final AtomicLong ownerEpochPersistenceRejects = new AtomicLong();
    private final AtomicLong ownerEpochDeleteClaimRejects = new AtomicLong();
    private final Map<StrataNamespace, Object> ownerEpochLocks = new ConcurrentHashMap<>();
    private final Map<StrataNamespace, MutationClaim> activeDestructiveMutations = new ConcurrentHashMap<>();
    private final Object ownerEpochPersistenceLock = new Object();
    // Process-local observations still fence ordinary owner RPCs, but only a dedicated orphan-confirm
    // response from a configured controller endpoint, after that server validates consensus authority,
    // may raise the volume-bound floor. This trusts that configured endpoint addresses reach real
    // controllers (the node initiates the connection); SCP itself is plaintext and unauthenticated, so any
    // transport security must come from the deployment network. Persisting epochs supplied by arbitrary
    // inbound callers would turn a process-lifetime DoS into a permanent one.
    private final Map<StrataNamespace, Long> highestOwnerEpochByNamespace = new ConcurrentHashMap<>();
    private final Map<StrataNamespace, Long> durableOwnerEpochByNamespace = new HashMap<>();
    private volatile Exception ownerEpochPersistenceFailure;

    private final int nodeId;
    private final UUID incarnation;
    // Owners (by advertised endpoint) this node has heard a VERIFY_CHUNKS from. Retained as a
    // diagnostic trace of owner-pull verification activity; orphan GC now relies on per-chunk
    // owner-confirm plus latching mass-delete breakers instead of a global "heard from every owner" gate.
    private final Set<String> verifiersHeardFrom = ConcurrentHashMap.newKeySet();

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
        ensureDirectoryDurable(config.dataDir());
        durableOwnerEpochByNamespace.putAll(loadOwnerEpochs(config.dataDir()));
        highestOwnerEpochByNamespace.putAll(durableOwnerEpochByNamespace);
        Identity identity = loadOrCreateIdentity(config.dataDir(), config.nodeId());
        this.nodeId = identity.nodeId;
        this.incarnation = identity.incarnation;
        ChunkStore openedStore = null;
        ScpServer openedServer = null;
        ControlLoop startedLoop = null;
        OrphanGc startedGc = null;
        try {
            openedStore = new ChunkStore(config.dataDir().resolve("chunks"), config.chunkStoreConfig());
            ChunkDeleteService deletes = new ChunkDeleteService(openedStore,
                    config.deleteMaxConcurrent(), config.deleteMinIntervalMs());
            DataNodeHandlers dataHandler = new DataNodeHandlers(openedStore, this, deletes);
            ScpServer.Handler handler = controllerHandler == null
                    ? dataHandler
                    : ScpServer.Handler.route(dataHandler, controllerHandler);
            openedServer = new ScpServer(config.listenPort(), nodeId,
                    incarnation.getMostSignificantBits(), incarnation.getLeastSignificantBits(), handler);
            this.store = openedStore;
            this.deleteService = deletes;
            this.server = openedServer;
            if (!config.controllerEndpoints().isEmpty()) {
                startedLoop = new ControlLoop(this, config, openedStore, deletes);
                this.controlLoop = startedLoop;
                dataHandler.controlLoop(startedLoop); // serve direct owner-repair EXEC_REPLICATE
                startedLoop.start();
                // Node-local orphan GC (tech design §9.2): reclaim sealed chunks no owner references, after
                // confirming with the namespace owner. Only a registered node runs it (it needs a nodeId
                // to recognise itself in a descriptor and controller endpoints to ask).
                startedGc = new OrphanGc(openedStore, nodeId, config.controllerEndpoints(),
                        config.orphanGraceMs(), config.orphanScanIntervalMs(), config.orphanStartupGraceMs(),
                        config.orphanConfirmTimeoutMs(), config.orphanDeleteMaxConfirmedPerNamespacePerPass(),
                        config.orphanDeleteMaxNamespacePercentPerPass(),
                        config.orphanDeleteMaxConfirmedPerNodePass(),
                        config.orphanDeleteMaxCumulativePerNamespace(),
                        config.orphanDeleteMaxCumulativePerNode(), this::acceptAuthoritativeOwnerEpoch,
                        this::deleteConfirmedOrphan);
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

    /** Namespaces that have seen I/O — drives lazy per-namespace meter registration. */
    public Set<String> ioNamespaces() {
        return store.ioNamespaces();
    }

    /** One per-namespace I/O counter by index (0 appendOps, 1 appendBytes, 2 readOps, 3 readBytes); O(1). */
    public long ioValue(String namespace, int index) {
        return store.ioValue(namespace, index);
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

    public int deleteWaiting() { return deleteService.waitingDeletes(); }
    public int deleteInFlight() { return deleteService.inFlightDeletes(); }
    public long deleteOkCount() { return deleteService.okDeletes(); }
    public long deleteNotFoundCount() { return deleteService.notFoundDeletes(); }
    public long deleteFailedCount() { return deleteService.failedDeletes(); }
    public int orphanGcBreakerOpenNamespaces() {
        return orphanGc == null ? 0 : orphanGc.breakerOpenNamespaces();
    }
    public int orphanGcNodeBreakerOpen() {
        return orphanGc != null && orphanGc.nodeBreakerOpen() ? 1 : 0;
    }
    public long orphanGcBreakerTrips() {
        return orphanGc == null ? 0 : orphanGc.breakerTrips();
    }
    public long orphanGcCumulativeBreakerTrips() {
        return orphanGc == null ? 0 : orphanGc.cumulativeBreakerTrips();
    }
    public long orphanGcBreakerSkippedChunkTotal() {
        return orphanGc == null ? 0 : orphanGc.breakerSkippedChunkTotal();
    }
    public long orphanGcAlreadyDeletedTotal() {
        return orphanGc == null ? 0 : orphanGc.alreadyDeletedTotal();
    }
    public int orphanGcBreakerHaltedNamespaces() {
        return orphanGc == null ? 0 : orphanGc.breakerHaltedNamespaces();
    }
    public int orphanGcBreakerHaltedChunks() {
        return orphanGc == null ? 0 : orphanGc.breakerHaltedChunks();
    }
    public long ownerEpochFenceRejects() { return ownerEpochFenceRejects.get(); }
    public int ownerEpochPersistencePoisoned() { return ownerEpochPersistenceFailure == null ? 0 : 1; }
    public long ownerEpochPersistenceRejects() { return ownerEpochPersistenceRejects.get(); }
    public long ownerEpochDeleteClaimRejects() { return ownerEpochDeleteClaimRejects.get(); }
    public long orphanGcOwnerEpochConfirmRejects() {
        return orphanGc == null ? 0 : orphanGc.ownerEpochConfirmRejects();
    }
    public long orphanGcPersistencePoisonConfirmRejects() {
        return orphanGc == null ? 0 : orphanGc.persistencePoisonConfirmRejects();
    }

    /** Installs a per-request latency observer on the data-plane server (used by the metrics layer). */
    public void setRequestObserver(RequestObserver observer) {
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

    /** Records that owner {@code verifierEndpoint} issued a VERIFY_CHUNKS to this node (tech design §9.2). */
    void noteVerifiedBy(String verifierEndpoint) {
        verifiersHeardFrom.add(verifierEndpoint);
    }

    void acceptOwnerEpoch(StrataNamespace namespace, long ownerEpoch) {
        acceptOwnerEpoch(namespace, ownerEpoch, false);
    }

    /**
     * Raises or checks the node-local owner watermark for owner-scoped lanes. The
     * allowUnstampedAfterSeen escape is only for broker data-plane requests that predate owner fencing and
     * never carry an owner epoch: writer SEAL_CHUNK and broker-owned chunk cleanup. Owner/tool lanes must
     * stamp a nonzero epoch once any owner epoch has been observed for the namespace.
     */
    void acceptOwnerEpoch(StrataNamespace namespace, long ownerEpoch, boolean allowUnstampedAfterSeen) {
        if (ownerEpoch < 0) {
            throw new IllegalArgumentException("ownerEpoch must be non-negative: " + ownerEpoch);
        }
        // Owner epochs fence stale namespace owners; they are not an auth boundary on today's unauthenticated
        // SCP links. A client that can issue owner-only opcodes can still raise this process-local watermark.
        synchronized (ownerEpochLock(namespace)) {
            long seen = highestOwnerEpochByNamespace.getOrDefault(namespace, 0L);
            if (ownerEpoch == 0) {
                if (seen > 0) {
                    if (allowUnstampedAfterSeen) {
                        rejectOwnerRpcDuringCommittedMutation(namespace, ownerEpoch);
                        return;
                    }
                    throw fencedOwnerEpoch(namespace, ownerEpoch, seen);
                }
                rejectOwnerRpcDuringCommittedMutation(namespace, ownerEpoch);
                log.debug("accepting unstamped owner RPC before watermark is established namespace={} "
                                + "clientKind={} clientId={}",
                        namespace, RequestContext.clientKind(), RequestContext.clientId());
                return;
            }
            if (ownerEpoch < seen) {
                throw fencedOwnerEpoch(namespace, ownerEpoch, seen);
            }
            rejectOwnerRpcDuringCommittedMutation(namespace, ownerEpoch);
            if (ownerEpoch > seen) {
                highestOwnerEpochByNamespace.put(namespace, ownerEpoch);
                log.info("raising owner epoch watermark namespace={} previousOwnerEpoch={} acceptedOwnerEpoch={} "
                                + "clientKind={} clientId={}",
                        namespace, seen, ownerEpoch, RequestContext.clientKind(), RequestContext.clientId());
            }
        }
    }

    /**
     * Accepts an epoch from the dedicated orphan-confirm response returned by a configured controller
     * endpoint after server-side consensus validation. Unlike ordinary owner RPCs, this trusted response
     * may raise the volume-bound floor that survives node restart; the node does not verify a cryptographic
     * consensus proof itself. The node trusts that its configured endpoint addresses reach real controllers;
     * SCP is plaintext and unauthenticated, so any transport security must come from the deployment network.
     */
    void acceptAuthoritativeOwnerEpoch(StrataNamespace namespace, long ownerEpoch) {
        if (ownerEpoch <= 0) {
            throw new IllegalArgumentException("authoritative ownerEpoch must be positive: " + ownerEpoch);
        }
        synchronized (ownerEpochLock(namespace)) {
            long seen = highestOwnerEpochByNamespace.getOrDefault(namespace, 0L);
            if (ownerEpoch < seen) {
                throw fencedOwnerEpoch(namespace, ownerEpoch, seen);
            }
            rejectOwnerRpcDuringCommittedMutation(namespace, ownerEpoch);
            if (ownerEpochPersistenceFailure != null) {
                throw ownerEpochPersistenceFailed(namespace, ownerEpoch, ownerEpochPersistenceFailure);
            }
            synchronized (ownerEpochPersistenceLock) {
                if (ownerEpochPersistenceFailure != null) {
                    throw ownerEpochPersistenceFailed(namespace, ownerEpoch, ownerEpochPersistenceFailure);
                }
                long durableSeen = durableOwnerEpochByNamespace.getOrDefault(namespace, 0L);
                if (ownerEpoch <= durableSeen) {
                    highestOwnerEpochByNamespace.put(namespace, Math.max(seen, ownerEpoch));
                    return;
                }

                Map<StrataNamespace, Long> raised = new HashMap<>(durableOwnerEpochByNamespace);
                raised.put(namespace, ownerEpoch);
                try {
                    persistOwnerEpochs(config.dataDir(), raised);
                } catch (IOException | RuntimeException e) {
                    // The failure may have happened after rename but before the directory fsync. Poison the
                    // durable-confirm/delete lane for this process so a later request cannot overwrite an
                    // uncertain higher floor. Keep the accepted authoritative epoch as the volatile floor so
                    // ordinary owner RPCs remain available without letting an older epoch through this process.
                    highestOwnerEpochByNamespace.put(namespace, Math.max(seen, ownerEpoch));
                    ownerEpochPersistenceFailure = e;
                    log.error("failed to persist authoritative owner epoch floor namespace={} "
                                    + "previousOwnerEpoch={} offeredOwnerEpoch={} floorFile={}; authoritative "
                                    + "floor raises and orphan deletes are poisoned fail-closed, while ordinary "
                                    + "owner RPCs remain enabled. Repair the data volume, verify the floor file, "
                                    + "and restart the node",
                            namespace, durableSeen, ownerEpoch, ownerEpochFloorFile(), e);
                    throw ownerEpochPersistenceFailed(namespace, ownerEpoch, e);
                }
                durableOwnerEpochByNamespace.put(namespace, ownerEpoch);
                highestOwnerEpochByNamespace.put(namespace, ownerEpoch);
                log.info("raising durable owner epoch floor namespace={} previousOwnerEpoch={} acceptedOwnerEpoch={}",
                        namespace, durableSeen, ownerEpoch);
            }
        }
    }

    /**
     * Waits for the shared delete throttle before committing a short namespace-local delete claim. The
     * claim binds the final epoch check to the unlink without holding the namespace monitor across either
     * the throttle wait or physical I/O. Owner RPCs that arrive after the claim commit fail fast with a
     * retriable error and can retry once the unlink finishes.
     */
    ErrorCode deleteConfirmedOrphan(StrataNamespace namespace, ChunkId chunkId, long confirmedOwnerEpoch)
            throws InterruptedException {
        try (ChunkDeleteService.PreparedDelete prepared = deleteService.prepare()) {
            return deleteConfirmedOrphan(namespace, chunkId, confirmedOwnerEpoch,
                    () -> prepared.delete(namespace, chunkId));
        }
    }

    @FunctionalInterface
    interface PhysicalDelete {
        ErrorCode delete() throws InterruptedException;
    }

    ErrorCode deleteConfirmedOrphan(StrataNamespace namespace, ChunkId chunkId, long confirmedOwnerEpoch,
                                     PhysicalDelete physicalDelete) throws InterruptedException {
        if (confirmedOwnerEpoch <= 0) {
            throw new IllegalArgumentException("confirmedOwnerEpoch must be positive: " + confirmedOwnerEpoch);
        }
        if (physicalDelete == null) {
            throw new IllegalArgumentException("physicalDelete must be non-null");
        }
        MutationClaim claim = new MutationClaim(chunkId, confirmedOwnerEpoch, "orphan unlink");
        synchronized (ownerEpochLock(namespace)) {
            if (ownerEpochPersistenceFailure != null) {
                throw ownerEpochPersistenceFailed(namespace, confirmedOwnerEpoch, ownerEpochPersistenceFailure);
            }
            long seen = highestOwnerEpochByNamespace.getOrDefault(namespace, 0L);
            if (confirmedOwnerEpoch < seen) {
                throw fencedOwnerEpoch(namespace, confirmedOwnerEpoch, seen);
            }
            long durableSeen;
            synchronized (ownerEpochPersistenceLock) {
                if (ownerEpochPersistenceFailure != null) {
                    throw ownerEpochPersistenceFailed(
                            namespace, confirmedOwnerEpoch, ownerEpochPersistenceFailure);
                }
                durableSeen = durableOwnerEpochByNamespace.getOrDefault(namespace, 0L);
            }
            if (confirmedOwnerEpoch != seen || durableSeen < confirmedOwnerEpoch) {
                throw new ScpException(ErrorCode.INTERNAL,
                        "orphan confirmation epoch " + confirmedOwnerEpoch + " is not the committed current floor "
                                + "for namespace " + namespace + " (seen=" + seen + ", durable=" + durableSeen + ")");
            }
            MutationClaim existing = activeDestructiveMutations.putIfAbsent(namespace, claim);
            if (existing != null) {
                throw new ScpException(ErrorCode.INTERNAL,
                        "orphan delete already committed for namespace " + namespace + " chunk "
                                + existing.chunkId() + "; retry chunk " + chunkId);
            }
        }
        try {
            return physicalDelete.delete();
        } finally {
            synchronized (ownerEpochLock(namespace)) {
                activeDestructiveMutations.remove(namespace, claim);
            }
        }
    }

    /**
     * Quarantines a verify-rejected replica only after the shared delete throttle has been acquired and
     * the request epoch has been rechecked under the namespace claim. A newer owner can advance the epoch
     * while this request waits for QoS; that stale request must not rename a freshly repaired chunk.
     */
    ErrorCode quarantineVerifiedReplica(StrataNamespace namespace, ChunkId chunkId, long ownerEpoch)
            throws InterruptedException {
        acceptOwnerEpoch(namespace, ownerEpoch);
        try (ChunkDeleteService.PreparedDelete prepared = deleteService.prepare()) {
            return quarantineVerifiedReplicaAfterThrottle(namespace, chunkId, ownerEpoch,
                    () -> prepared.quarantine(namespace, chunkId));
        }
    }

    ErrorCode quarantineVerifiedReplica(StrataNamespace namespace, ChunkId chunkId, long ownerEpoch,
                                        PhysicalDelete physicalDelete) throws InterruptedException {
        acceptOwnerEpoch(namespace, ownerEpoch);
        try (ChunkDeleteService.PreparedDelete ignored = deleteService.prepare()) {
            return quarantineVerifiedReplicaAfterThrottle(namespace, chunkId, ownerEpoch, physicalDelete);
        }
    }

    private ErrorCode quarantineVerifiedReplicaAfterThrottle(StrataNamespace namespace, ChunkId chunkId,
                                                              long ownerEpoch, PhysicalDelete physicalDelete)
            throws InterruptedException {
        if (ownerEpoch <= 0) {
            throw new IllegalArgumentException("quarantine ownerEpoch must be positive: " + ownerEpoch);
        }
        if (physicalDelete == null) {
            throw new IllegalArgumentException("physicalDelete must be non-null");
        }
        MutationClaim claim = new MutationClaim(chunkId, ownerEpoch, "verify quarantine");
        synchronized (ownerEpochLock(namespace)) {
            long seen = highestOwnerEpochByNamespace.getOrDefault(namespace, 0L);
            if (ownerEpoch < seen) {
                throw fencedOwnerEpoch(namespace, ownerEpoch, seen);
            }
            if (ownerEpoch != seen) {
                throw new ScpException(ErrorCode.INTERNAL,
                        "quarantine epoch " + ownerEpoch + " was not accepted for namespace " + namespace
                                + " (current=" + seen + ")");
            }
            rejectOwnerRpcDuringCommittedMutation(namespace, ownerEpoch);
            MutationClaim existing = activeDestructiveMutations.putIfAbsent(namespace, claim);
            if (existing != null) {
                throw new ScpException(ErrorCode.INTERNAL,
                        existing.operation() + " already committed for namespace " + namespace + " chunk "
                                + existing.chunkId() + "; retry quarantine for chunk " + chunkId);
            }
        }
        try {
            return physicalDelete.delete();
        } finally {
            synchronized (ownerEpochLock(namespace)) {
                activeDestructiveMutations.remove(namespace, claim);
            }
        }
    }

    private record MutationClaim(ChunkId chunkId, long ownerEpoch, String operation) {}

    private Object ownerEpochLock(StrataNamespace namespace) {
        return ownerEpochLocks.computeIfAbsent(namespace, ignored -> new Object());
    }

    private void rejectOwnerRpcDuringCommittedMutation(StrataNamespace namespace, long offered) {
        MutationClaim claim = activeDestructiveMutations.get(namespace);
        if (claim == null) {
            return;
        }
        ownerEpochDeleteClaimRejects.incrementAndGet();
        log.info("rejecting owner RPC during committed {} namespace={} chunkId={} "
                        + "mutationOwnerEpoch={} offeredOwnerEpoch={} clientKind={} clientId={}; caller may retry",
                claim.operation(), namespace, claim.chunkId(), claim.ownerEpoch(), offered,
                RequestContext.clientKind(), RequestContext.clientId());
        throw new ScpException(ErrorCode.INTERNAL,
                claim.operation() + " is committed for namespace " + namespace + " chunk " + claim.chunkId()
                        + " at owner epoch " + claim.ownerEpoch() + "; retry owner RPC");
    }

    private OwnerEpochPersistenceException ownerEpochPersistenceFailed(
            StrataNamespace namespace, long offered, Exception cause) {
        ownerEpochPersistenceRejects.incrementAndGet();
        return new OwnerEpochPersistenceException(
                "authoritative owner epoch floor is unavailable for namespace " + namespace
                        + "; rejecting epoch " + offered + " and orphan delete fail-closed; floorFile="
                        + ownerEpochFloorFile() + "; repair the data volume, verify the floor file, and restart",
                cause);
    }

    static final class OwnerEpochPersistenceException extends ScpException {
        OwnerEpochPersistenceException(String message, Throwable cause) {
            super(ErrorCode.INTERNAL, message, cause);
        }
    }

    private ScpException fencedOwnerEpoch(StrataNamespace namespace, long offered, long required) {
        ownerEpochFenceRejects.incrementAndGet();
        long durableRequired;
        synchronized (ownerEpochPersistenceLock) {
            durableRequired = durableOwnerEpochByNamespace.getOrDefault(namespace, 0L);
        }
        log.warn("rejecting stale owner RPC namespace={} offeredOwnerEpoch={} requiredOwnerEpoch={} "
                        + "durableOwnerEpoch={} floorFile={} clientKind={} clientId={}; if the durable floor is "
                        + "unexpected, stop the node and reconcile this volume file with authoritative metadata "
                        + "before restart",
                namespace, offered, required, durableRequired, ownerEpochFloorFile(),
                RequestContext.clientKind(), RequestContext.clientId());
        return new ScpException(ErrorCode.FENCED_EPOCH,
                "stale owner epoch " + offered + " for namespace " + namespace + " (required >= " + required + ")",
                required);
    }

    private Path ownerEpochFloorFile() {
        return config.dataDir().resolve("owner-epochs.properties").toAbsolutePath();
    }

    /** The set of owner endpoints this node has heard a VERIFY_CHUNKS from (orphan-GC membership grace). */
    Set<String> verifiersHeardFrom() {
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
        Files.move(tmp, f, StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
        forceDirectory(dataDir);
    }

    /* ---------------- volume-bound owner epoch floors ---------------- */

    private static Map<StrataNamespace, Long> loadOwnerEpochs(Path dataDir) throws IOException {
        Path file = dataDir.resolve("owner-epochs.properties");
        if (!Files.exists(file)) {
            return Map.of();
        }
        Properties properties = new StrictProperties();
        try (var in = Files.newInputStream(file)) {
            properties.load(in);
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid data node owner epoch floor file: " + file, e);
        }
        if (properties.isEmpty()) {
            throw new IOException("data node owner epoch floor file is empty: " + file);
        }
        Map<StrataNamespace, Long> loaded = new HashMap<>();
        for (String namespaceText : properties.stringPropertyNames()) {
            String epochText = properties.getProperty(namespaceText);
            try {
                StrataNamespace namespace = StrataNamespace.of(namespaceText);
                long epoch = Long.parseLong(epochText);
                if (epoch <= 0) {
                    throw new IllegalArgumentException("owner epoch " + epoch + " <= 0");
                }
                loaded.put(namespace, epoch);
            } catch (IllegalArgumentException e) {
                throw new IOException("invalid data node owner epoch floor entry for namespace '"
                        + namespaceText + "' in " + file, e);
            }
        }
        return loaded;
    }

    /** Properties parser that treats duplicate namespace floors as corruption instead of taking the last one. */
    private static final class StrictProperties extends Properties {
        @Override
        public synchronized Object put(Object key, Object value) {
            if (containsKey(key)) {
                throw new IllegalArgumentException("duplicate property: " + key);
            }
            return super.put(key, value);
        }
    }

    private static void persistOwnerEpochs(Path dataDir, Map<StrataNamespace, Long> epochs) throws IOException {
        Properties properties = new Properties();
        for (Map.Entry<StrataNamespace, Long> entry : epochs.entrySet()) {
            properties.setProperty(entry.getKey().value(), Long.toString(entry.getValue()));
        }
        Path file = dataDir.resolve("owner-epochs.properties");
        Path tmp = dataDir.resolve("owner-epochs.properties.tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
             var out = Channels.newOutputStream(ch)) {
            properties.store(out, "strata data node owner epoch floors — bound to this volume");
            out.flush();
            ch.force(true);
        }
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
        forceDirectory(dataDir);
    }

    @Override
    public void close() throws IOException {
        Closeables.throwIfFailed(closeAll(controlLoop, orphanGc, server, store));
    }
}
