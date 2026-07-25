package io.strata.meta;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Controller-process membership backed by the controller's existing Curator session.
 *
 * <p>Each advertised endpoint owns one ephemeral znode whose payload includes a process incarnation.
 * A second process cannot claim the same endpoint until ZooKeeper has expired the first process's session.
 * Connection suspension is a local serving fence: callers must stop treating this controller as an owner
 * immediately, while other controllers wait for the ephemeral node to disappear before promoting a
 * successor. A session LOST event creates a fresh incarnation for the eventual re-registration.
 */
final class ControllerMembership implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ControllerMembership.class);
    private static final byte RECORD_VERSION = 1;
    private static final int AUTHORITATIVE_REFRESH_RETRIES = 3;

    record LiveController(String endpoint, UUID incarnation) {
        LiveController {
            Objects.requireNonNull(endpoint, "endpoint");
            Objects.requireNonNull(incarnation, "incarnation");
            if (endpoint.isBlank()) {
                throw new IllegalArgumentException("controller endpoint must not be blank");
            }
        }

        byte[] encode() {
            byte[] endpointBytes = endpoint.getBytes(StandardCharsets.UTF_8);
            return ByteBuffer.allocate(1 + Long.BYTES * 2 + Integer.BYTES + endpointBytes.length)
                    .put(RECORD_VERSION)
                    .putLong(incarnation.getMostSignificantBits())
                    .putLong(incarnation.getLeastSignificantBits())
                    .putInt(endpointBytes.length)
                    .put(endpointBytes)
                    .array();
        }

        static LiveController decode(byte[] bytes) {
            if (bytes == null || bytes.length < 1 + Long.BYTES * 2 + Integer.BYTES) {
                throw new IllegalArgumentException("truncated controller membership record");
            }
            ByteBuffer b = ByteBuffer.wrap(bytes);
            byte version = b.get();
            if (version != RECORD_VERSION) {
                throw new IllegalArgumentException("controller membership record version " + version);
            }
            UUID incarnation = new UUID(b.getLong(), b.getLong());
            int endpointLength = b.getInt();
            if (endpointLength <= 0 || endpointLength > b.remaining()) {
                throw new IllegalArgumentException("invalid controller endpoint length " + endpointLength);
            }
            byte[] endpointBytes = new byte[endpointLength];
            b.get(endpointBytes);
            if (b.hasRemaining()) {
                throw new IllegalArgumentException("trailing bytes in controller membership record");
            }
            return new LiveController(new String(endpointBytes, StandardCharsets.UTF_8), incarnation);
        }
    }

    private final CuratorFramework curator;
    private final String localEndpoint;
    private final String localLivePath;
    private final AtomicReference<UUID> localIncarnation = new AtomicReference<>(UUID.randomUUID());
    private final AtomicBoolean sessionReady = new AtomicBoolean();
    private final AtomicLong sessionStateVersion = new AtomicLong();
    private final AtomicLong connectionGeneration = new AtomicLong();
    private final AtomicBoolean connectionUsable = new AtomicBoolean(true);
    private final Object readinessLock = new Object();
    private final AtomicBoolean registrationRequired = new AtomicBoolean(true);
    private final Object registrationLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Map<String, LiveController>> liveView = new AtomicReference<>(Map.of());
    private final Object liveViewLock = new Object();
    private final AtomicLong membershipEventGeneration = new AtomicLong();
    private final List<Runnable> listeners = new CopyOnWriteArrayList<>();
    private final CuratorCache liveCache;
    private final ConnectionStateListener connectionStateListener;
    private volatile Runnable beforeReadinessPublication = () -> {};
    private volatile Runnable beforeRefreshPublication = () -> {};
    private volatile Runnable beforeCacheEventPublication = () -> {};

    ControllerMembership(ZkMetadataStore root, String localEndpoint) throws Exception {
        this.curator = Objects.requireNonNull(root, "root").curator();
        this.localEndpoint = requireEndpoint(localEndpoint);
        this.localLivePath = livePath(localEndpoint);

        liveCache = CuratorCache.build(curator, ZkMetadataStore.META_CONTROLLER_LIVE);
        liveCache.listenable().addListener(this::onCacheEvent);
        liveCache.start();

        connectionStateListener = (ignored, state) -> handleConnectionState(state);
        curator.getConnectionStateListenable().addListener(connectionStateListener);
        try {
            ensureRegisteredAndRefreshed();
        } catch (Exception e) {
            curator.getConnectionStateListenable().removeListener(connectionStateListener);
            liveCache.close();
            throw e;
        }
    }

    String localEndpoint() {
        return localEndpoint;
    }

    UUID localIncarnation() {
        return localIncarnation.get();
    }

    boolean sessionReady() {
        return !closed.get() && connectionUsable.get() && sessionReady.get() && localIncarnationIsLive();
    }

    long sessionStateVersion() {
        return sessionStateVersion.get();
    }

    boolean isLive(String endpoint) {
        return liveView.get().containsKey(endpoint);
    }

    LiveController liveController(String endpoint) {
        return liveView.get().get(endpoint);
    }

    boolean localIncarnationIsLive() {
        LiveController live = liveView.get().get(localEndpoint);
        return live != null && live.incarnation().equals(localIncarnation.get());
    }

    Map<String, LiveController> liveControllers() {
        return liveView.get();
    }

    AutoCloseable addListener(Runnable listener) {
        Objects.requireNonNull(listener, "listener");
        listeners.add(listener);
        return () -> listeners.remove(listener);
    }

    /**
     * Rebuilds the live map after a ZooKeeper sync. Watches reduce latency, while this authoritative refresh
     * is the periodic reconciliation path and the only view used before a dead-owner promotion.
     */
    void refreshAuthoritative() throws Exception {
        synchronized (registrationLock) {
            refreshAuthoritativeLocked();
        }
    }

    private void refreshAuthoritativeLocked() throws Exception {
        for (int attempt = 0; attempt < AUTHORITATIVE_REFRESH_RETRIES; attempt++) {
            long expectedEventGeneration = membershipEventGeneration.get();
            Map<String, LiveController> refreshed = readAuthoritative();
            beforeRefreshPublication.run();
            boolean changed;
            synchronized (liveViewLock) {
                if (membershipEventGeneration.get() != expectedEventGeneration) {
                    continue;
                }
                Map<String, LiveController> immutableRefreshed = Map.copyOf(refreshed);
                changed = !immutableRefreshed.equals(liveView.get());
                liveView.set(immutableRefreshed);
            }
            if (!localIncarnationIsLive()) {
                invalidateLocalIncarnation();
            }
            if (changed) {
                notifyListeners();
            }
            return;
        }
        throw new IllegalStateException("controller membership changed during authoritative refresh");
    }

    private Map<String, LiveController> readAuthoritative() throws Exception {
        ZkMetadataStore.awaitSync(curator.sync(), ZkMetadataStore.META_CONTROLLER_LIVE);
        List<String> children;
        try {
            children = curator.getChildren().forPath(ZkMetadataStore.META_CONTROLLER_LIVE);
        } catch (KeeperException.NoNodeException e) {
            children = List.of();
        }
        Map<String, LiveController> refreshed = new HashMap<>();
        for (String child : children) {
            String path = ZkMetadataStore.META_CONTROLLER_LIVE + "/" + child;
            try {
                LiveController live = LiveController.decode(curator.getData().forPath(path));
                if (!path.equals(livePath(live.endpoint()))) {
                    log.warn("ignoring controller membership whose path does not match payload endpoint: path={} endpoint={}",
                            path, live.endpoint());
                    continue;
                }
                refreshed.put(live.endpoint(), live);
            } catch (KeeperException.NoNodeException ignored) {
                // The controller session expired between getChildren and getData.
            } catch (IllegalArgumentException badRecord) {
                log.warn("ignoring invalid controller membership at {}", path, badRecord);
            }
        }
        return refreshed;
    }

    /**
     * Retries a registration that failed transiently during CONNECTED/RECONNECTED. Connection-state
     * callbacks are edge-triggered; without this periodic retry a single failed write could leave a
     * healthy controller fenced forever.
     */
    void ensureRegisteredAndRefreshed() throws Exception {
        synchronized (registrationLock) {
            long generation = connectionGeneration.get();
            if (closed.get()) {
                throw new IllegalStateException("controller membership is closed");
            }
            if (registrationRequired.get()) {
                registerCurrentIncarnation();
            }
            refreshAuthoritativeLocked();
            if (!localIncarnationIsLive()) {
                setSessionReady(false);
                throw new IllegalStateException("local controller incarnation is not live after refresh");
            }
            if (connectionGeneration.get() != generation) {
                setSessionReady(false);
                throw new IllegalStateException("controller session changed during membership refresh");
            }
            beforeReadinessPublication.run();
            publishSessionReady(generation);
        }
    }

    private void onCacheEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
        ChildData selected = data != null ? data : oldData;
        if (selected == null || selected.getPath().equals(ZkMetadataStore.META_CONTROLLER_LIVE)) {
            return;
        }
        boolean invalidateLocal = false;
        try {
            LiveController live = LiveController.decode(selected.getData());
            synchronized (liveViewLock) {
                membershipEventGeneration.incrementAndGet();
                if (!selected.getPath().equals(livePath(live.endpoint()))) {
                    log.warn("ignoring controller membership event whose path does not match endpoint: path={} endpoint={}",
                            selected.getPath(), live.endpoint());
                    invalidateLocal = selected.getPath().equals(localLivePath);
                } else {
                    Map<String, LiveController> updated = new HashMap<>(liveView.get());
                    if (type == CuratorCacheListener.Type.NODE_DELETED) {
                        updated.remove(live.endpoint(), live);
                    } else {
                        updated.put(live.endpoint(), live);
                    }
                    if (live.endpoint().equals(localEndpoint)) {
                        LiveController local = updated.get(localEndpoint);
                        invalidateLocal = local == null
                                || !local.incarnation().equals(localIncarnation.get());
                    }
                    beforeCacheEventPublication.run();
                    liveView.set(Map.copyOf(updated));
                }
            }
            if (invalidateLocal) {
                invalidateLocalIncarnation();
            }
            notifyListeners();
        } catch (IllegalArgumentException badRecord) {
            log.warn("ignoring invalid controller membership cache event at {}", selected.getPath(), badRecord);
            synchronized (liveViewLock) {
                membershipEventGeneration.incrementAndGet();
                invalidateLocal = selected.getPath().equals(localLivePath);
            }
            if (invalidateLocal) {
                invalidateLocalIncarnation();
            }
            notifyListeners();
        }
    }

    private void handleConnectionState(ConnectionState state) {
        if (closed.get()) {
            return;
        }
        switch (state) {
            case CONNECTED, RECONNECTED -> {
                synchronized (readinessLock) {
                    connectionUsable.set(true);
                }
                registrationRequired.set(true);
                try {
                    ensureRegisteredAndRefreshed();
                } catch (Exception e) {
                    setSessionReady(false);
                    log.warn("controller {} failed to re-register membership after {}", localEndpoint, state, e);
                }
            }
            case SUSPENDED, READ_ONLY -> {
                fenceConnection();
            }
            case LOST -> {
                fenceConnection();
                synchronized (registrationLock) {
                    localIncarnation.set(UUID.randomUUID());
                    registrationRequired.set(true);
                    removeLive(localEndpoint);
                    setSessionReady(false);
                }
                notifyListeners();
            }
        }
    }

    private void invalidateLocalIncarnation() {
        synchronized (readinessLock) {
            connectionGeneration.incrementAndGet();
            setSessionReady(false);
        }
        synchronized (registrationLock) {
            LiveController local = liveView.get().get(localEndpoint);
            if ((local == null || !local.incarnation().equals(localIncarnation.get()))
                    && !registrationRequired.get()) {
                localIncarnation.set(UUID.randomUUID());
                registrationRequired.set(true);
                removeLive(localEndpoint);
            }
            setSessionReady(false);
        }
    }

    /** Package-private deterministic hook for connection-state unit tests. */
    void handleConnectionStateForTest(ConnectionState state) {
        handleConnectionState(state);
    }

    /** Package-private deterministic hook immediately before generation-bound readiness publication. */
    void beforeReadinessPublicationForTest(Runnable hook) {
        beforeReadinessPublication = Objects.requireNonNull(hook, "hook");
    }

    /** Package-private deterministic hook after the authoritative scan and before live-view publication. */
    void beforeRefreshPublicationForTest(Runnable hook) {
        beforeRefreshPublication = Objects.requireNonNull(hook, "hook");
    }

    /** Package-private deterministic hook immediately before a cache event atomically publishes its view. */
    void beforeCacheEventPublicationForTest(Runnable hook) {
        beforeCacheEventPublication = Objects.requireNonNull(hook, "hook");
    }

    /** Package-private cache-generation visibility for deterministic lifecycle race tests. */
    long membershipEventGenerationForTest() {
        return membershipEventGeneration.get();
    }

    /** Package-private deterministic stale-cache injection for owner-promotion tests. */
    void forgetLiveControllerForTest(String endpoint) {
        synchronized (liveViewLock) {
            Map<String, LiveController> stale = new HashMap<>(liveView.get());
            stale.remove(endpoint);
            liveView.set(Map.copyOf(stale));
        }
    }

    private void fenceConnection() {
        synchronized (readinessLock) {
            connectionUsable.set(false);
            connectionGeneration.incrementAndGet();
            setSessionReady(false);
        }
    }

    private void publishSessionReady(long expectedGeneration) {
        synchronized (readinessLock) {
            if (closed.get() || !connectionUsable.get()
                    || connectionGeneration.get() != expectedGeneration) {
                setSessionReady(false);
                throw new IllegalStateException("controller session changed during membership refresh");
            }
            setSessionReady(true);
        }
    }

    private void registerCurrentIncarnation() throws Exception {
        LiveController local = new LiveController(localEndpoint, localIncarnation.get());
        byte[] encoded = local.encode();
        try {
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                    .forPath(localLivePath, encoded);
        } catch (KeeperException.NodeExistsException e) {
            LiveController existing = LiveController.decode(curator.getData().forPath(localLivePath));
            if (!existing.equals(local)) {
                throw new IllegalStateException("controller endpoint " + localEndpoint
                        + " is already held by incarnation " + existing.incarnation());
            }
        }
        persistMemberRecord(encoded);
        putLive(localEndpoint, local);
        registrationRequired.set(false);
    }

    private void putLive(String endpoint, LiveController live) {
        synchronized (liveViewLock) {
            Map<String, LiveController> updated = new HashMap<>(liveView.get());
            updated.put(endpoint, live);
            liveView.set(Map.copyOf(updated));
        }
    }

    private void removeLive(String endpoint) {
        synchronized (liveViewLock) {
            Map<String, LiveController> updated = new HashMap<>(liveView.get());
            updated.remove(endpoint);
            liveView.set(Map.copyOf(updated));
        }
    }

    private void persistMemberRecord(byte[] encoded) throws Exception {
        String path = memberPath(localEndpoint);
        try {
            curator.create().creatingParentsIfNeeded().forPath(path, encoded);
        } catch (KeeperException.NodeExistsException e) {
            curator.setData().forPath(path, encoded);
        }
    }

    private void setSessionReady(boolean ready) {
        if (sessionReady.getAndSet(ready) != ready) {
            sessionStateVersion.incrementAndGet();
            notifyListeners();
        }
    }

    private void notifyListeners() {
        for (Runnable listener : new ArrayList<>(listeners)) {
            try {
                listener.run();
            } catch (RuntimeException e) {
                log.warn("controller membership listener failed", e);
            }
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        setSessionReady(false);
        curator.getConnectionStateListenable().removeListener(connectionStateListener);
        liveCache.close();
        // Do not explicitly delete the shared endpoint path. No read/version/delete sequence can prevent
        // an old session from deleting a replacement znode after delete/recreate resets dataVersion to zero.
        // Controller closes the owning Curator session immediately after ownership, which removes this
        // ephemeral atomically with that session and cannot affect a replacement incarnation.
        removeLive(localEndpoint);
        notifyListeners();
        listeners.clear();
    }

    private static String requireEndpoint(String endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        if (endpoint.isBlank()) {
            throw new IllegalArgumentException("controller endpoint must not be blank");
        }
        return endpoint;
    }

    private static String livePath(String endpoint) {
        return ZkMetadataStore.META_CONTROLLER_LIVE + "/" + pathKey(endpoint);
    }

    private static String memberPath(String endpoint) {
        return ZkMetadataStore.META_CONTROLLER_MEMBERS + "/" + pathKey(endpoint);
    }

    private static String pathKey(String endpoint) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(endpoint.getBytes(StandardCharsets.UTF_8));
    }
}
