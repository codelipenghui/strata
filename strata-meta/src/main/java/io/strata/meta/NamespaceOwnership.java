package io.strata.meta;

import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import org.apache.curator.framework.recipes.watch.PersistentWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Resolves and fences the controller owner of a namespace.
 *
 * <p>The local constructor is only for non-sharded deployments. Sharded serving must use
 * {@link #persistent}: configured endpoints are consulted exactly once to create the namespace's initial
 * persisted replica set. Every subsequent
 * owner decision comes from the versioned ZooKeeper assignment. The assignment znode data version is the
 * ownership revision consumed by namespace recovery, so a stale local owner can be rejected before it
 * re-opens a repository.
 */
public final class NamespaceOwnership implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(NamespaceOwnership.class);
    private static final long RECONCILE_WARN_INTERVAL_MS = 30_000;
    private static final int OWNERSHIP_TRANSITION_LANES = 32;

    /** Stable identity of one assignment znode incarnation and data revision. */
    public record AuthorityTerm(long creationZxid, int revision) {
        public AuthorityTerm {
            if (creationZxid < -1 || revision < -1) {
                throw new IllegalArgumentException(
                        "invalid assignment authority term " + creationZxid + "/" + revision);
            }
        }
    }

    /** Persisted assignment plus the ZooKeeper identity that revisions this owner term. */
    public record Assignment(
            StrataNamespace namespace,
            int generation,
            List<String> replicaSet,
            UUID leaderIncarnation,
            long creationZxid,
            long modifiedZxid,
            int revision) {
        public Assignment {
            namespace = Objects.requireNonNull(namespace, "namespace");
            replicaSet = List.copyOf(replicaSet);
            leaderIncarnation = Objects.requireNonNull(leaderIncarnation, "leaderIncarnation");
            if (replicaSet.isEmpty()) {
                throw new IllegalArgumentException("namespace assignment must contain at least one replica");
            }
            if (replicaSet.stream().anyMatch(endpoint -> endpoint == null || endpoint.isBlank())
                    || replicaSet.stream().distinct().count() != replicaSet.size()) {
                throw new IllegalArgumentException(
                        "namespace assignment replicas must be non-blank and distinct");
            }
            if (revision < -1) {
                throw new IllegalArgumentException("invalid namespace assignment revision " + revision);
            }
            if (creationZxid < -1 || modifiedZxid < -1) {
                throw new IllegalArgumentException("invalid namespace assignment zxid");
            }
        }

        public String preferredLeader() {
            return replicaSet.get(0);
        }

        public boolean hasBoundLeader() {
            return !Records.NamespaceAssignment.UNBOUND_LEADER_INCARNATION.equals(leaderIncarnation);
        }

        public AuthorityTerm authorityTerm() {
            return new AuthorityTerm(creationZxid, revision);
        }
    }

    /** Local ownership transitions. A revision change is LOST(old) followed by ACQUIRED(new). */
    public interface Listener {
        default void onAcquired(StrataNamespace namespace, AuthorityTerm term) {
        }

        default void onLost(StrataNamespace namespace, AuthorityTerm term) {
        }
    }

    private static final class ListenerRegistration {
        private final Listener listener;
        private final Object callbackLock = new Object();
        private boolean active = true;
        private int callbacksInFlight;

        private ListenerRegistration(Listener listener) {
            this.listener = listener;
        }

        private boolean beginCallback() {
            synchronized (callbackLock) {
                if (!active) {
                    return false;
                }
                callbacksInFlight++;
                return true;
            }
        }

        private void endCallback() {
            synchronized (callbackLock) {
                callbacksInFlight--;
                callbackLock.notifyAll();
            }
        }

        private boolean deactivate() {
            synchronized (callbackLock) {
                if (!active) {
                    return false;
                }
                active = false;
                callbackLock.notifyAll();
                return true;
            }
        }

        private void awaitQuiescent(boolean calledFromThisRegistration) {
            boolean interrupted = false;
            synchronized (callbackLock) {
                int allowedInFlight = calledFromThisRegistration ? 1 : 0;
                while (callbacksInFlight > allowedInFlight) {
                    try {
                        callbackLock.wait();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private final String localEndpoint;
    private final List<String> eligibleEndpoints;
    private final int generation;
    private final int replicaCount;
    private final ZkMetadataStore root;
    private final ControllerMembership membership;
    private final BooleanSupplier coordinatesFailover;
    private final long reconcileIntervalMs;
    private final Map<StrataNamespace, Assignment> assignmentCache = new ConcurrentHashMap<>();
    private final Map<StrataNamespace, Object> assignmentLocks = new ConcurrentHashMap<>();
    private final Set<StrataNamespace> dirtyAssignments = ConcurrentHashMap.newKeySet();
    private final Map<StrataNamespace, Long> assignmentEventVersions = new ConcurrentHashMap<>();
    private final Map<StrataNamespace, AuthorityTerm> acquiredTerms = new ConcurrentHashMap<>();
    private final List<ListenerRegistration> listeners = new ArrayList<>();
    /**
     * Serializes cache/term decisions. Listener callbacks never run while this lock (or
     * {@link #reconcileSignal}) is held: a namespace-log callback takes its repository open lock, while an
     * opener calls back into {@link #isOwner}; invoking it inline would invert those locks.
     */
    private final Object ownershipStateLock = new Object();
    private final ThreadLocal<ListenerRegistration> currentListenerCallback = new ThreadLocal<>();
    private final List<BlockingQueue<OwnershipTransition>> ownershipTransitionQueues;
    private final AtomicBoolean authorityReady = new AtomicBoolean();
    private final AtomicLong authoritySessionStateVersion = new AtomicLong(Long.MIN_VALUE);
    private final AtomicBoolean fullReconcileRequested = new AtomicBoolean(true);
    private final AtomicLong watchResetVersion = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong lastReconcileWarnMs = new AtomicLong();
    private final Object reconcileSignal = new Object();
    /**
     * Linearizes a promotion CAS with lifecycle close. Either the CAS completes before close publishes
     * {@link #closed}, or close wins and every later promotion observes it before writing ZooKeeper.
     */
    private final Object promotionLifecycleLock = new Object();
    private volatile Runnable beforePromotionCas = () -> {};
    private volatile Runnable beforeAuthorityPublication = () -> {};
    private volatile Runnable afterAuthorityRead = () -> {};
    private final PersistentWatcher assignmentWatch;
    private final AutoCloseable membershipListener;
    private final List<Thread> ownershipTransitionThreads;
    private final Thread reconcileThread;

    private record OwnershipTransition(
            StrataNamespace namespace,
            AuthorityTerm term,
            boolean acquired,
            List<ListenerRegistration> targets,
            CountDownLatch barrier) {

        static OwnershipTransition changed(
                StrataNamespace namespace,
                AuthorityTerm term,
                boolean acquired,
                List<ListenerRegistration> targets) {
            return new OwnershipTransition(namespace, term, acquired, targets, null);
        }

        static OwnershipTransition barrier(CountDownLatch barrier) {
            return new OwnershipTransition(null, null, false, List.of(), barrier);
        }
    }

    /**
     * Non-sharded resolver. Multi-endpoint construction is rejected so production cannot accidentally fall
     * back to static HRW serving authority; sharded deployments must use {@link #persistent}.
     */
    public NamespaceOwnership(String localEndpoint, List<String> eligibleEndpoints,
                              int generation, int replicaCount) {
        this.localEndpoint = requireEndpoint(localEndpoint);
        this.eligibleEndpoints = normalizedEndpoints(eligibleEndpoints);
        validateSingleEndpoint(this.localEndpoint, this.eligibleEndpoints);
        if (this.eligibleEndpoints.size() > 1) {
            throw new IllegalArgumentException(
                    "multi-controller namespace ownership requires persisted assignments");
        }
        this.generation = generation;
        this.replicaCount = replicaCount;
        this.root = null;
        this.membership = null;
        this.coordinatesFailover = () -> false;
        this.reconcileIntervalMs = 0;
        this.assignmentWatch = null;
        this.membershipListener = null;
        this.ownershipTransitionQueues = List.of();
        this.ownershipTransitionThreads = List.of();
        this.reconcileThread = null;
    }

    private NamespaceOwnership(ZkMetadataStore root, String localEndpoint, List<String> eligibleEndpoints,
                               int generation, int replicaCount, BooleanSupplier coordinatesFailover,
                               long reconcileIntervalMs) throws Exception {
        this.root = Objects.requireNonNull(root, "root");
        this.localEndpoint = requireEndpoint(localEndpoint);
        this.eligibleEndpoints = normalizedEndpoints(eligibleEndpoints);
        validateSingleEndpoint(this.localEndpoint, this.eligibleEndpoints);
        if (this.eligibleEndpoints.size() <= 1) {
            throw new IllegalArgumentException("persistent namespace ownership requires at least two endpoints");
        }
        if (!this.eligibleEndpoints.contains(this.localEndpoint)) {
            throw new IllegalArgumentException("controller endpoints must include local endpoint "
                    + this.localEndpoint);
        }
        if (replicaCount < 2 || replicaCount > this.eligibleEndpoints.size()) {
            throw new IllegalArgumentException("persistent replicaCount must be in [2, "
                    + this.eligibleEndpoints.size() + "]: " + replicaCount);
        }
        if (reconcileIntervalMs <= 0) {
            throw new IllegalArgumentException("reconcileIntervalMs must be positive: " + reconcileIntervalMs);
        }
        this.generation = generation;
        this.replicaCount = replicaCount;
        this.coordinatesFailover = Objects.requireNonNull(coordinatesFailover, "coordinatesFailover");
        this.reconcileIntervalMs = reconcileIntervalMs;

        ControllerMembership openedMembership = null;
        PersistentWatcher openedWatch = null;
        AutoCloseable openedMembershipListener = null;
        List<BlockingQueue<OwnershipTransition>> transitionQueues =
                new ArrayList<>(OWNERSHIP_TRANSITION_LANES);
        for (int lane = 0; lane < OWNERSHIP_TRANSITION_LANES; lane++) {
            transitionQueues.add(new LinkedBlockingQueue<>());
        }
        this.ownershipTransitionQueues = List.copyOf(transitionQueues);
        List<Thread> openedTransitionThreads = new ArrayList<>(OWNERSHIP_TRANSITION_LANES);
        Thread openedReconcileThread = null;
        try {
            openedMembership = new ControllerMembership(root, localEndpoint);
            this.membership = openedMembership;
            openedMembershipListener = openedMembership.addListener(this::membershipChanged);

            openedWatch = new PersistentWatcher(root.curator(), ZkMetadataStore.META_NAMESPACES, true);
            openedWatch.getListenable().addListener(this::assignmentChanged);
            openedWatch.getResetListenable().addListener(this::assignmentWatchReset);
            openedWatch.start();

            for (int lane = 0; lane < OWNERSHIP_TRANSITION_LANES; lane++) {
                BlockingQueue<OwnershipTransition> queue = transitionQueues.get(lane);
                openedTransitionThreads.add(Thread.ofVirtual()
                        .name("namespace-ownership-transitions-" + localEndpoint + "-" + lane)
                        .start(() -> ownershipTransitionLoop(queue)));
            }
            openedReconcileThread = Thread.ofVirtual()
                    .name("namespace-ownership-reconcile-" + localEndpoint)
                    .start(this::reconcileLoop);
        } catch (Exception e) {
            for (Thread transitionThread : openedTransitionThreads) {
                transitionThread.interrupt();
            }
            closeQuietly(openedMembershipListener);
            if (openedWatch != null) {
                openedWatch.close();
            }
            if (openedMembership != null) {
                openedMembership.close();
            }
            throw e;
        }
        this.assignmentWatch = openedWatch;
        this.membershipListener = openedMembershipListener;
        this.ownershipTransitionThreads = List.copyOf(openedTransitionThreads);
        this.reconcileThread = openedReconcileThread;
    }

    /**
     * Starts the sharded, persisted owner authority. The configured endpoints seed only a namespace's first
     * HRW replica set. Thereafter owner changes are assignment CAS rotations over live controller sessions.
     */
    public static NamespaceOwnership persistent(
            ZkMetadataStore root,
            String localEndpoint,
            List<String> initialEligibleEndpoints,
            int membershipGeneration,
            int replicaCount,
            BooleanSupplier coordinatesFailover,
            long reconcileIntervalMs) throws Exception {
        return new NamespaceOwnership(root, localEndpoint, initialEligibleEndpoints, membershipGeneration,
                replicaCount, coordinatesFailover, reconcileIntervalMs);
    }

    /** Single-endpoint / empty membership: this node owns every namespace (no sharding). */
    public boolean ownsAll() {
        return eligibleEndpoints.size() <= 1;
    }

    /** The persisted endpoint currently assigned to lead {@code namespace}. */
    public String ownerOf(StrataNamespace namespace) {
        if (root != null && !isAuthorityReady()) {
            throw unavailable(namespace,
                    new IllegalStateException("controller assignment view is not reconciled"));
        }
        return assignmentOf(namespace).preferredLeader();
    }

    /**
     * Whether this process may serve {@code namespace}. Persistent ownership additionally requires the
     * local ephemeral membership session to be ready; SUSPENDED/LOST therefore fails closed immediately.
     */
    public boolean isOwner(StrataNamespace namespace) {
        if (closed.get()) {
            return false;
        }
        if (root == null) {
            return ownsAll() || ownerOf(namespace).equals(localEndpoint);
        }
        try {
            assignmentOf(namespace);
        } catch (ScpException unavailable) {
            return false;
        }
        return cachedLocalAuthorityTerm(namespace).isPresent();
    }

    /**
     * Returns the exact locally-owned cached term without doing ZooKeeper I/O. Consumers use this to ensure
     * an ACTIVE resource opened under one assignment term is never reused merely because an asynchronous
     * LOST callback has not run yet.
     */
    Optional<AuthorityTerm> cachedLocalAuthorityTerm(StrataNamespace namespace) {
        Objects.requireNonNull(namespace, "namespace");
        if (closed.get()) {
            return Optional.empty();
        }
        if (root == null) {
            return ownsAll()
                    ? Optional.of(new AuthorityTerm(-1, -1))
                    : Optional.empty();
        }
        if (!isAuthorityReady()) {
            return Optional.empty();
        }
        long sessionStateVersion = membership.sessionStateVersion();
        long observedAssignmentEvents = assignmentEventVersion(namespace);
        synchronized (reconcileSignal) {
            synchronized (ownershipStateLock) {
                Assignment assignment = assignmentCache.get(namespace);
                if (assignment == null
                        || assignmentEventVersion(namespace) != observedAssignmentEvents
                        || assignmentCache.get(namespace) != assignment
                        || !assignment.preferredLeader().equals(localEndpoint)
                        || !assignment.leaderIncarnation().equals(membership.localIncarnation())
                        || !membership.sessionReady()
                        || !isAuthorityReady()
                        || membership.sessionStateVersion() != sessionStateVersion) {
                    return Optional.empty();
                }
                return Optional.of(assignment.authorityTerm());
            }
        }
    }

    public boolean isSessionReady() {
        return membership == null || membership.sessionReady();
    }

    /**
     * Whether this process has reconciled its assignment cache after the current ZooKeeper session became
     * ready. This is stricter than {@link #isSessionReady()} and is included in {@link #isOwner}.
     */
    public boolean isAuthorityReady() {
        return root == null || (authorityReady.get()
                && membership.sessionReady()
                && authoritySessionStateVersion.get() == membership.sessionStateVersion());
    }

    /**
     * Returns the assignment view used for routing/recovery. On a cold cache this synchronously reads or
     * creates the persisted assignment. If ZooKeeper cannot establish that authority, this fails with the
     * retriable METADATA_RECOVERING code; it never falls back to a freshly computed static owner.
     */
    public Assignment assignmentOf(StrataNamespace namespace) {
        Objects.requireNonNull(namespace, "namespace");
        if (ownsAll()) {
            return new Assignment(namespace, generation, List.of(localEndpoint),
                    Records.NamespaceAssignment.UNBOUND_LEADER_INCARNATION, -1, -1, -1);
        }
        if (root == null) {
            throw new IllegalStateException("non-sharded ownership cannot resolve a multi-controller assignment");
        }
        Assignment cached = assignmentCache.get(namespace);
        if (cached != null) {
            return cached;
        }
        Object lock = assignmentLocks.computeIfAbsent(namespace, ignored -> new Object());
        synchronized (lock) {
            cached = assignmentCache.get(namespace);
            if (cached != null) {
                return cached;
            }
            try {
                for (int attempt = 0; attempt < 3; attempt++) {
                    if (!isAuthorityReady()) {
                        throw new IllegalStateException("controller authority is not ready");
                    }
                    long sessionStateVersion = membership.sessionStateVersion();
                    long observedWatchReset = watchResetVersion.get();
                    long observedAssignmentEvents = assignmentEventVersion(namespace);
                    Optional<ZkMetadataStore.NamespaceAssignmentState> existing =
                            root.getNamespaceAssignmentAuthoritativeState(namespace);
                    if (existing.isEmpty()) {
                        membership.refreshAuthoritative();
                        Records.NamespaceAssignment initial = initialAssignment(namespace);
                        root.putNamespaceAssignment(initial, -1);
                        existing = root.getNamespaceAssignmentAuthoritativeState(namespace);
                    }
                    ZkMetadataStore.NamespaceAssignmentState persisted = existing.orElseThrow(
                            () -> new IllegalStateException(
                                    "namespace assignment create/read race left no record"));
                    synchronized (reconcileSignal) {
                        if (membership.sessionReady()
                                && !closed.get()
                                && isAuthorityReady()
                                && membership.sessionStateVersion() == sessionStateVersion
                                && watchResetVersion.get() == observedWatchReset
                                && assignmentEventVersion(namespace) == observedAssignmentEvents) {
                            return publishAssignment(persisted);
                        }
                    }
                }
                throw new IllegalStateException("namespace assignment changed during authoritative read");
            } catch (Exception e) {
                throw unavailable(namespace, e);
            }
        }
    }

    private Records.NamespaceAssignment initialAssignment(StrataNamespace namespace) {
        NamespaceAssignmentPolicy.Assignment initial =
                NamespaceAssignmentPolicy.assign(namespace, generation, eligibleEndpoints, replicaCount);
        List<String> replicas = initial.replicaSet();
        ControllerMembership.LiveController firstLive = null;
        for (String endpoint : replicas) {
            firstLive = membership.liveController(endpoint);
            if (firstLive != null) {
                replicas = rotateTo(replicas, endpoint);
                break;
            }
        }
        UUID incarnation = firstLive == null
                ? Records.NamespaceAssignment.UNBOUND_LEADER_INCARNATION
                : firstLive.incarnation();
        return new Records.NamespaceAssignment(
                namespace, initial.generation(), replicas, incarnation);
    }

    public int assignmentRevision(StrataNamespace namespace) {
        return assignmentOf(namespace).revision();
    }

    public AuthorityTerm assignmentTerm(StrataNamespace namespace) {
        return assignmentOf(namespace).authorityTerm();
    }

    /**
     * Consensus-revalidates the exact local owner revision captured by a namespace repository. This method
     * deliberately checks session readiness both before and after the authoritative read.
     */
    public boolean validateLocalAuthority(StrataNamespace namespace, int expectedRevision) throws Exception {
        Assignment expected = assignmentOf(namespace);
        if (expected.revision() != expectedRevision) {
            return false;
        }
        return validateLocalAuthority(namespace, expected.authorityTerm());
    }

    public boolean validateLocalAuthority(StrataNamespace namespace, AuthorityTerm expectedTerm) throws Exception {
        if (root == null) {
            return expectedTerm.equals(new AuthorityTerm(-1, -1)) && isOwner(namespace);
        }
        if (!isAuthorityReady()) {
            return false;
        }
        long sessionStateVersion = membership.sessionStateVersion();
        long observedWatchReset = watchResetVersion.get();
        long observedAssignmentEvents = assignmentEventVersion(namespace);
        Optional<ZkMetadataStore.NamespaceAssignmentState> current =
                root.getNamespaceAssignmentAuthoritativeState(namespace);
        afterAuthorityRead.run();
        ZkMetadataStore.NamespaceAssignmentState versioned;
        Assignment published;
        synchronized (reconcileSignal) {
            if (!membership.sessionReady()
                    || closed.get()
                    || !isAuthorityReady()
                    || membership.sessionStateVersion() != sessionStateVersion
                    || watchResetVersion.get() != observedWatchReset
                    || assignmentEventVersion(namespace) != observedAssignmentEvents
                    || current.isEmpty()) {
                return false;
            }
            versioned = current.get();
            published = publishAssignment(versioned);
        }
        return published.revision() == versioned.version()
                && published.creationZxid() == versioned.creationZxid()
                && published.modifiedZxid() == versioned.modifiedZxid()
                && published.authorityTerm().equals(expectedTerm)
                && published.preferredLeader().equals(localEndpoint)
                && published.leaderIncarnation().equals(membership.localIncarnation())
                && watchResetVersion.get() == observedWatchReset
                && assignmentEventVersion(namespace) == observedAssignmentEvents
                && membership.sessionReady()
                && isAuthorityReady()
                && membership.sessionStateVersion() == sessionStateVersion
                && isOwner(namespace);
    }

    public List<String> eligibleEndpoints() {
        return eligibleEndpoints;
    }

    public String localEndpoint() {
        return localEndpoint;
    }

    public AutoCloseable addListener(Listener listener) {
        Objects.requireNonNull(listener, "listener");
        ListenerRegistration registration = new ListenerRegistration(listener);
        synchronized (ownershipStateLock) {
            if (closed.get()) {
                throw new IllegalStateException("namespace ownership is closed");
            }
            listeners.add(registration);
            for (Map.Entry<StrataNamespace, AuthorityTerm> acquired : acquiredTerms.entrySet()) {
                transitionQueue(acquired.getKey()).add(OwnershipTransition.changed(
                        acquired.getKey(), acquired.getValue(), true, List.of(registration)));
            }
        }
        return () -> removeListener(registration);
    }

    private void removeListener(ListenerRegistration registration) {
        if (registration.deactivate()) {
            synchronized (ownershipStateLock) {
                listeners.remove(registration);
            }
        }
        // A callback that already entered this registration may run on any namespace lane. Waiting on a
        // per-registration count gives external removals a precise no-callback-after-return contract. A
        // self-removing callback waits for callbacks on other lanes but not for its own stack frame.
        registration.awaitQuiescent(currentListenerCallback.get() == registration);
    }

    /** Package-private deterministic hook for connection-state tests. */
    void handleConnectionStateForTest(ConnectionState state) {
        if (membership == null) {
            throw new IllegalStateException("static ownership has no controller session");
        }
        membership.handleConnectionStateForTest(state);
    }

    /** Package-private immediate reconciliation hook for tests and later Controller diagnostics. */
    void reconcileNow() throws Exception {
        fullReconcileRequested.set(false);
        reconcileAllAssignments();
    }

    /** Wakes the coordinator for an immediate full failover audit (for example, on LeaderLatch acquisition). */
    void requestFullReconcile() {
        fullReconcileRequested.set(true);
        wakeReconciler();
    }

    void beforePromotionCasForTest(Runnable hook) {
        beforePromotionCas = Objects.requireNonNull(hook, "hook");
    }

    /** Package-private deterministic hook for session-transition/publication races. */
    void beforeAuthorityPublicationForTest(Runnable hook) {
        beforeAuthorityPublication = Objects.requireNonNull(hook, "hook");
    }

    /** Package-private deterministic hook after an authority read and before session revalidation. */
    void afterAuthorityReadForTest(Runnable hook) {
        afterAuthorityRead = Objects.requireNonNull(hook, "hook");
    }

    /** Package-private deterministic invalidation hook for lock-order and stale-publication tests. */
    void invalidateAssignmentForTest(StrataNamespace namespace) {
        invalidateAssignment(namespace);
    }

    /** Package-private deterministic dirty-path hook that does not run a full reconciliation first. */
    void reconcileDirtyNowForTest() throws Exception {
        reconcileDirtyAssignments();
    }

    /** Package-private deterministic stale membership-view injection for failover tests. */
    void forgetLiveControllerForTest(String endpoint) {
        if (membership == null) {
            throw new IllegalStateException("static ownership has no controller membership");
        }
        membership.forgetLiveControllerForTest(endpoint);
    }

    boolean controllerIsLiveForTest(String endpoint) {
        return membership != null && membership.isLive(endpoint);
    }

    long assignmentEventVersionForTest(StrataNamespace namespace) {
        return assignmentEventVersion(namespace);
    }

    private void reconcileAllAssignments() throws Exception {
        if (root == null || closed.get()) {
            return;
        }
        if (!membership.sessionReady()) {
            try {
                membership.ensureRegisteredAndRefreshed();
            } catch (Exception e) {
                fenceLocalOwnership();
                throw e;
            }
            if (!membership.sessionReady()) {
                fenceLocalOwnership();
                return;
            }
        }
        long sessionStateVersion = membership.sessionStateVersion();
        long observedWatchReset;
        Map<StrataNamespace, Long> observedAssignmentEvents;
        synchronized (reconcileSignal) {
            observedWatchReset = watchResetVersion.get();
            observedAssignmentEvents = new java.util.HashMap<>(assignmentEventVersions);
        }
        membership.ensureRegisteredAndRefreshed();
        if (!membership.sessionReady() || membership.sessionStateVersion() != sessionStateVersion) {
            fenceLocalOwnership();
            return;
        }

        List<StrataNamespace> namespaces = root.listAssignedNamespaces();
        Map<StrataNamespace, ZkMetadataStore.NamespaceAssignmentState> snapshot = new ConcurrentHashMap<>();
        Set<StrataNamespace> corruptAssignments = new HashSet<>();
        for (StrataNamespace namespace : namespaces) {
            try {
                root.getNamespaceAssignmentState(namespace)
                        .ifPresent(state -> snapshot.put(namespace, state));
            } catch (IllegalArgumentException corrupt) {
                // One damaged assignment must fence that namespace, not every healthy namespace on this
                // controller. Transport/session failures still escape and fail the entire authority view
                // closed because they make the snapshot itself untrustworthy.
                corruptAssignments.add(namespace);
                log.error("unreadable namespace assignment; fencing namespace={}", namespace, corrupt);
            }
        }

        if (!membership.sessionReady() || membership.sessionStateVersion() != sessionStateVersion) {
            fenceLocalOwnership();
            return;
        }
        beforeAuthorityPublication.run();
        synchronized (reconcileSignal) {
            if (watchResetVersion.get() != observedWatchReset) {
                fullReconcileRequested.set(true);
                return;
            }
            if (!membership.sessionReady()
                    || closed.get()
                    || membership.sessionStateVersion() != sessionStateVersion) {
                synchronized (ownershipStateLock) {
                    authorityReady.set(false);
                    reevaluateAllLocalOwnershipLocked();
                }
                fullReconcileRequested.set(true);
                return;
            }
            Set<StrataNamespace> persistedNamespaces = new HashSet<>(snapshot.keySet());
            persistedNamespaces.addAll(corruptAssignments);
            for (StrataNamespace cached : new ArrayList<>(assignmentCache.keySet())) {
                long before = observedAssignmentEvents.getOrDefault(cached, 0L);
                if (!persistedNamespaces.contains(cached)
                        && assignmentEventVersion(cached) == before) {
                    removeAssignment(cached);
                    dirtyAssignments.remove(cached);
                }
            }
            for (StrataNamespace corrupt : corruptAssignments) {
                long before = observedAssignmentEvents.getOrDefault(corrupt, 0L);
                if (assignmentEventVersion(corrupt) == before) {
                    removeAssignment(corrupt);
                    dirtyAssignments.remove(corrupt);
                }
            }
            for (Map.Entry<StrataNamespace, ZkMetadataStore.NamespaceAssignmentState> entry
                    : snapshot.entrySet()) {
                StrataNamespace namespace = entry.getKey();
                long before = observedAssignmentEvents.getOrDefault(namespace, 0L);
                if (assignmentEventVersion(namespace) == before) {
                    publishAssignment(entry.getValue());
                    dirtyAssignments.remove(namespace);
                }
            }
            // Bind the reconciled snapshot to the exact membership-session state. If a connection event
            // races after this check, the monotonically increasing session version makes isAuthorityReady()
            // false even if that event and this publication reorder their boolean stores.
            synchronized (ownershipStateLock) {
                authoritySessionStateVersion.set(sessionStateVersion);
                authorityReady.set(true);
                reevaluateAllLocalOwnershipLocked();
            }
        }

        if (coordinatesFailover.getAsBoolean() && membership.sessionReady()) {
            for (StrataNamespace namespace : snapshot.keySet()) {
                Assignment assignment = assignmentCache.get(namespace);
                if (assignment != null) {
                    promoteIfOwnerDead(assignment);
                }
            }
        }
    }

    private void reconcileDirtyAssignments() throws Exception {
        if (closed.get() || !isAuthorityReady()) {
            fullReconcileRequested.set(true);
            return;
        }
        boolean coordinateFailover = coordinatesFailover.getAsBoolean() && membership.sessionReady();
        if (coordinateFailover) {
            // Curator cache events are latency hints, not proof that an owner is dead. Refresh the complete
            // ephemeral membership set after a ZooKeeper sync before any dirty-path promotion CAS.
            membership.refreshAuthoritative();
            if (closed.get() || !membership.sessionReady() || !isAuthorityReady()) {
                fullReconcileRequested.set(true);
                return;
            }
        }
        List<StrataNamespace> dirty = new ArrayList<>(dirtyAssignments);
        for (StrataNamespace namespace : dirty) {
            long sessionStateVersion = membership.sessionStateVersion();
            long observedWatchReset = watchResetVersion.get();
            long observedAssignmentEvents = assignmentEventVersion(namespace);
            Optional<ZkMetadataStore.NamespaceAssignmentState> current;
            try {
                current = root.getNamespaceAssignmentAuthoritativeState(namespace);
            } catch (IllegalArgumentException corrupt) {
                synchronized (reconcileSignal) {
                    if (watchResetVersion.get() != observedWatchReset
                            || assignmentEventVersion(namespace) != observedAssignmentEvents) {
                        continue;
                    }
                    dirtyAssignments.remove(namespace);
                    removeAssignment(namespace);
                }
                log.error("unreadable namespace assignment; fencing namespace={}", namespace, corrupt);
                continue;
            }
            synchronized (reconcileSignal) {
                if (watchResetVersion.get() != observedWatchReset
                        || assignmentEventVersion(namespace) != observedAssignmentEvents
                        || closed.get()
                        || !isAuthorityReady()
                        || membership.sessionStateVersion() != sessionStateVersion) {
                    continue;
                }
                dirtyAssignments.remove(namespace);
                if (current.isEmpty()) {
                    removeAssignment(namespace);
                    continue;
                }
                publishAssignment(current.get());
            }
            Assignment assignment = assignmentCache.get(namespace);
            if (assignment != null && coordinateFailover && membership.sessionReady()) {
                promoteIfOwnerDead(assignment);
            }
        }
    }

    private void promoteIfOwnerDead(Assignment observed) throws Exception {
        long sessionStateVersion = membership.sessionStateVersion();
        if (closed.get() || !membership.sessionReady() || !isAuthorityReady()) {
            return;
        }
        ControllerMembership.LiveController currentLeader =
                membership.liveController(observed.preferredLeader());
        if (currentLeader != null
                && observed.hasBoundLeader()
                && currentLeader.incarnation().equals(observed.leaderIncarnation())) {
            return;
        }

        // A v1/unbound record is migrated in place when its preferred endpoint is live. A bound record
        // whose endpoint has a different incarnation represents a genuinely expired owner term: skip that
        // endpoint so a restart cannot cause automatic failback while this CAS is in flight.
        int firstCandidate = observed.hasBoundLeader() ? 1 : 0;
        ControllerMembership.LiveController successor = null;
        for (int i = firstCandidate; i < observed.replicaSet().size(); i++) {
            String candidate = observed.replicaSet().get(i);
            ControllerMembership.LiveController live = membership.liveController(candidate);
            if (live != null) {
                successor = live;
                break;
            }
        }
        if (successor == null && observed.hasBoundLeader() && currentLeader != null) {
            // No standby is live. Rebinding the restarted endpoint under a fresh CAS revision is safe
            // because the old incarnation can never match the new term; this is the availability fallback,
            // not failback while a successor is available.
            successor = currentLeader;
        }
        if (successor == null) {
            return; // unavailable is safer than selecting a controller whose session is not live
        }

        List<String> rotated = rotateTo(observed.replicaSet(), successor.endpoint());
        Records.NamespaceAssignment replacement =
                new Records.NamespaceAssignment(
                        observed.namespace(), observed.generation(), rotated, successor.incarnation());
        beforePromotionCas.run();
        boolean committed;
        synchronized (promotionLifecycleLock) {
            if (closed.get()
                    || !membership.sessionReady()
                    || !isAuthorityReady()
                    || membership.sessionStateVersion() != sessionStateVersion) {
                return;
            }
            committed = root.putNamespaceAssignment(replacement, observed.revision());
        }
        if (committed) {
            Optional<Assignment> committedAssignment = refreshAssignmentGuarded(observed.namespace());
            if (committedAssignment.isPresent()) {
                Assignment updated = committedAssignment.get();
                log.info("namespace owner failover namespace={} oldOwner={} newOwner={} revision={}",
                        observed.namespace(), observed.preferredLeader(), updated.preferredLeader(), updated.revision());
            }
        } else {
            refreshAssignmentGuarded(observed.namespace());
        }
    }

    /**
     * Reads back an assignment after a promotion CAS without allowing a delayed pre-watch value to
     * resurrect a cache entry. A watch that races the read invalidates this attempt; the next attempt (or
     * the dirty reconciler) reads the newer state.
     */
    private Optional<Assignment> refreshAssignmentGuarded(StrataNamespace namespace) throws Exception {
        for (int attempt = 0; attempt < 3; attempt++) {
            if (!isAuthorityReady()) {
                return Optional.empty();
            }
            long sessionStateVersion = membership.sessionStateVersion();
            long observedWatchReset = watchResetVersion.get();
            long observedAssignmentEvents = assignmentEventVersion(namespace);
            Optional<ZkMetadataStore.NamespaceAssignmentState> current =
                    root.getNamespaceAssignmentAuthoritativeState(namespace);
            synchronized (reconcileSignal) {
                if (!membership.sessionReady()
                        || closed.get()
                        || !isAuthorityReady()
                        || membership.sessionStateVersion() != sessionStateVersion
                        || watchResetVersion.get() != observedWatchReset
                        || assignmentEventVersion(namespace) != observedAssignmentEvents) {
                    continue;
                }
                if (current.isEmpty()) {
                    removeAssignment(namespace);
                    return Optional.empty();
                }
                return Optional.of(publishAssignment(current.get()));
            }
        }
        dirtyAssignments.add(namespace);
        wakeReconciler();
        return Optional.empty();
    }

    private void reconcileLoop() {
        long nextFullReconcileMs = 0;
        while (!closed.get()) {
            boolean failed = false;
            try {
                long now = System.currentTimeMillis();
                if (fullReconcileRequested.getAndSet(false) || now >= nextFullReconcileMs) {
                    reconcileAllAssignments();
                    nextFullReconcileMs = System.currentTimeMillis() + reconcileIntervalMs;
                } else {
                    reconcileDirtyAssignments();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                fullReconcileRequested.set(true);
                failed = true;
                maybeWarnReconcile(e);
            }
            synchronized (reconcileSignal) {
                if (closed.get()) {
                    return;
                }
                try {
                    long waitMs = Math.max(1, nextFullReconcileMs - System.currentTimeMillis());
                    if (failed) {
                        reconcileSignal.wait(Math.min(250, reconcileIntervalMs));
                    } else if (dirtyAssignments.isEmpty() && !fullReconcileRequested.get()) {
                        reconcileSignal.wait(waitMs);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void maybeWarnReconcile(Exception e) {
        long now = System.currentTimeMillis();
        long last = lastReconcileWarnMs.get();
        if ((last == 0 || now - last >= RECONCILE_WARN_INTERVAL_MS)
                && lastReconcileWarnMs.compareAndSet(last, now)) {
            log.warn("namespace ownership reconciliation failed; cached assignments remain fail-closed "
                    + "for local serving until the controller session is ready", e);
        }
    }

    private void membershipChanged() {
        if (closed.get()) {
            return;
        }
        synchronized (ownershipStateLock) {
            if (!membership.sessionReady()) {
                authorityReady.set(false);
            }
            reevaluateAllLocalOwnershipLocked();
        }
        fullReconcileRequested.set(true);
        wakeReconciler();
    }

    private void assignmentChanged(WatchedEvent event) {
        if (closed.get()) {
            return;
        }
        StrataNamespace namespace = namespaceFromAssignmentPath(event.getPath());
        if (namespace == null) {
            return;
        }
        invalidateAssignment(namespace);
    }

    private void invalidateAssignment(StrataNamespace namespace) {
        synchronized (reconcileSignal) {
            assignmentEventVersions.merge(namespace, 1L, Long::sum);
            dirtyAssignments.add(namespace);
            // A watch is only an invalidation signal. Never publish its payload/dataVersion: a delayed
            // pre-delete event could otherwise resurrect an assignment after delete/recreate reset version.
            removeAssignment(namespace);
            reconcileSignal.notifyAll();
        }
    }

    private void assignmentWatchReset() {
        synchronized (reconcileSignal) {
            synchronized (ownershipStateLock) {
                authorityReady.set(false);
                reevaluateAllLocalOwnershipLocked();
            }
            fullReconcileRequested.set(true);
            watchResetVersion.incrementAndGet();
            reconcileSignal.notifyAll();
        }
    }

    private Assignment publishAssignment(ZkMetadataStore.NamespaceAssignmentState versioned) {
        Records.NamespaceAssignment value = versioned.value();
        Assignment updated = new Assignment(
                value.namespace(),
                value.generation(),
                value.replicaSet(),
                value.leaderIncarnation(),
                versioned.creationZxid(),
                versioned.modifiedZxid(),
                versioned.version());
        synchronized (ownershipStateLock) {
            assignmentCache.compute(value.namespace(), (namespace, previous) -> {
                if (previous == null || updated.modifiedZxid() >= previous.modifiedZxid()) {
                    return updated;
                }
                return previous; // a racing authoritative read must not regress a newer ZooKeeper zxid
            });
            Assignment current = assignmentCache.get(value.namespace());
            reevaluateLocalOwnershipLocked(value.namespace(), current);
            return current;
        }
    }

    private void removeAssignment(StrataNamespace namespace) {
        synchronized (ownershipStateLock) {
            assignmentCache.remove(namespace);
            AuthorityTerm prior = acquiredTerms.remove(namespace);
            if (prior != null) {
                enqueueTransitionLocked(namespace, prior, false);
            }
        }
    }

    private void reevaluateAllLocalOwnershipLocked() {
        for (Map.Entry<StrataNamespace, Assignment> e : assignmentCache.entrySet()) {
            reevaluateLocalOwnershipLocked(e.getKey(), e.getValue());
        }
    }

    private void reevaluateLocalOwnershipLocked(StrataNamespace namespace, Assignment assignment) {
        boolean shouldOwn = membership != null
                && membership.sessionReady()
                && isAuthorityReady()
                && assignmentCache.get(namespace) == assignment
                && assignment.preferredLeader().equals(localEndpoint)
                && assignment.leaderIncarnation().equals(membership.localIncarnation());
        AuthorityTerm updatedTerm = assignment.authorityTerm();
        AuthorityTerm previous = acquiredTerms.get(namespace);
        if (!shouldOwn) {
            if (previous != null && acquiredTerms.remove(namespace, previous)) {
                enqueueTransitionLocked(namespace, previous, false);
            }
            return;
        }
        if (previous == null) {
            if (acquiredTerms.putIfAbsent(namespace, updatedTerm) == null) {
                enqueueTransitionLocked(namespace, updatedTerm, true);
            }
        } else if (!previous.equals(updatedTerm)
                && acquiredTerms.replace(namespace, previous, updatedTerm)) {
            enqueueTransitionLocked(namespace, previous, false);
            enqueueTransitionLocked(namespace, updatedTerm, true);
        }
    }

    private void enqueueTransitionLocked(
            StrataNamespace namespace, AuthorityTerm term, boolean acquired) {
        if (!listeners.isEmpty()) {
            transitionQueue(namespace).add(OwnershipTransition.changed(
                    namespace, term, acquired, List.copyOf(listeners)));
        }
    }

    private BlockingQueue<OwnershipTransition> transitionQueue(StrataNamespace namespace) {
        int lane = (namespace.hashCode() & Integer.MAX_VALUE) % ownershipTransitionQueues.size();
        return ownershipTransitionQueues.get(lane);
    }

    private void ownershipTransitionLoop(BlockingQueue<OwnershipTransition> queue) {
        try {
            while (true) {
                OwnershipTransition transition = queue.take();
                if (transition.barrier() != null) {
                    transition.barrier().countDown();
                    continue;
                }
                notifyTransition(transition);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void notifyTransition(OwnershipTransition transition) {
        for (ListenerRegistration target : transition.targets()) {
            if (!target.beginCallback()) {
                continue;
            }
            ListenerRegistration previous = currentListenerCallback.get();
            currentListenerCallback.set(target);
            try {
                if (transition.acquired()) {
                    target.listener.onAcquired(transition.namespace(), transition.term());
                } else {
                    target.listener.onLost(transition.namespace(), transition.term());
                }
            } catch (Throwable e) {
                log.warn("namespace ownership {} listener failed namespace={} term={}",
                        transition.acquired() ? "acquired" : "lost",
                        transition.namespace(), transition.term(), e);
            } finally {
                if (previous == null) {
                    currentListenerCallback.remove();
                } else {
                    currentListenerCallback.set(previous);
                }
                target.endCallback();
            }
        }
    }

    private void awaitOwnershipTransitions() {
        if (ownershipTransitionThreads.isEmpty()) {
            return;
        }
        CountDownLatch drained = new CountDownLatch(ownershipTransitionQueues.size());
        for (BlockingQueue<OwnershipTransition> queue : ownershipTransitionQueues) {
            queue.add(OwnershipTransition.barrier(drained));
        }
        try {
            if (!drained.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
                log.warn("timed out draining namespace ownership callbacks during close");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void fenceLocalOwnership() {
        synchronized (ownershipStateLock) {
            authorityReady.set(false);
            reevaluateAllLocalOwnershipLocked();
        }
    }

    private void wakeReconciler() {
        synchronized (reconcileSignal) {
            reconcileSignal.notifyAll();
        }
    }

    private long assignmentEventVersion(StrataNamespace namespace) {
        return assignmentEventVersions.getOrDefault(namespace, 0L);
    }

    @Override
    public void close() {
        synchronized (promotionLifecycleLock) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
        }
        fenceLocalOwnership();
        wakeReconciler();
        if (reconcileThread != null) {
            reconcileThread.interrupt();
        }
        if (assignmentWatch != null) {
            assignmentWatch.close();
        }
        closeQuietly(membershipListener);
        if (membership != null) {
            membership.close();
        }
        // A scan that started before close is forbidden to publish by the closed checks above. Fence once
        // more after every producer/listener is stopped, then drain LOST while the namespace-log listener
        // is still registered.
        fenceLocalOwnership();
        // Drain every LOST transition before shutdown while the
        // namespace-log listener is still registered, then stop the isolated callback worker.
        awaitOwnershipTransitions();
        for (Thread transitionThread : ownershipTransitionThreads) {
            transitionThread.interrupt();
        }
        synchronized (ownershipStateLock) {
            for (ListenerRegistration listener : listeners) {
                listener.deactivate();
            }
            listeners.clear();
        }
    }

    private static List<String> rotateTo(List<String> replicas, String successor) {
        int index = replicas.indexOf(successor);
        if (index <= 0) {
            return replicas;
        }
        List<String> rotated = new ArrayList<>(replicas.size());
        rotated.addAll(replicas.subList(index, replicas.size()));
        rotated.addAll(replicas.subList(0, index));
        return List.copyOf(rotated);
    }

    private static StrataNamespace namespaceFromAssignmentPath(String path) {
        if (path == null) {
            return null;
        }
        String prefix = ZkMetadataStore.META_NAMESPACES + "/";
        String suffix = "/assignment";
        if (!path.startsWith(prefix) || !path.endsWith(suffix)) {
            return null;
        }
        String value = path.substring(prefix.length(), path.length() - suffix.length());
        if (value.isEmpty() || value.indexOf('/') >= 0) {
            return null;
        }
        try {
            return StrataNamespace.of(value);
        } catch (IllegalArgumentException invalidNamespace) {
            log.warn("ignoring malformed namespace assignment watch path {}", path);
            return null;
        }
    }

    private static ScpException unavailable(StrataNamespace namespace, Exception cause) {
        return new ScpException(ErrorCode.METADATA_RECOVERING,
                "namespace " + namespace + " has no readable persisted owner assignment; retry", cause);
    }

    private static String requireEndpoint(String endpoint) {
        Objects.requireNonNull(endpoint, "localEndpoint");
        if (endpoint.isBlank()) {
            throw new IllegalArgumentException("localEndpoint must not be blank");
        }
        return endpoint;
    }

    private static List<String> normalizedEndpoints(List<String> endpoints) {
        if (endpoints == null) {
            return List.of();
        }
        List<String> normalized = endpoints.stream()
                .map(endpoint -> Objects.requireNonNull(endpoint, "controller endpoint"))
                .map(String::trim)
                .filter(endpoint -> !endpoint.isEmpty())
                .distinct()
                .toList();
        if (normalized.size() != endpoints.size()) {
            throw new IllegalArgumentException("controller endpoints must be non-blank and distinct");
        }
        return normalized;
    }

    private static void validateSingleEndpoint(String localEndpoint, List<String> endpoints) {
        if (endpoints.size() == 1 && !endpoints.get(0).equals(localEndpoint)) {
            throw new IllegalArgumentException("single-endpoint membership must name this node ("
                    + localEndpoint + "); got " + endpoints.get(0));
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // best effort during constructor rollback / shutdown
        }
    }
}
