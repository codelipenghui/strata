package io.strata.meta;

import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.watch.PersistentWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PersistentNamespaceOwnershipTest {
    private static final List<String> ENDPOINTS = List.of("m1:9301", "m2:9301", "m3:9301");
    private static final BooleanSupplier NOT_COORDINATOR = () -> false;

    @Test
    void firstLookupPersistsHrwAssignmentAndAllServingReadsUseItsRevision() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root3 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), NOT_COORDINATOR);
             NamespaceOwnership m3 = persistent(root3, ENDPOINTS.get(2), NOT_COORDINATOR)) {
            StrataNamespace namespace = StrataNamespace.of("tenant-a");
            NamespaceAssignmentPolicy.Assignment expected =
                    NamespaceAssignmentPolicy.assign(namespace, 0, ENDPOINTS, 3);

            NamespaceOwnership.Assignment first = m1.assignmentOf(namespace);
            assertEquals(expected.replicaSet(), first.replicaSet());
            assertEquals(0, first.revision(), "fresh assignment znode dataVersion");

            await(() -> m2.assignmentRevision(namespace) == 0 && m3.assignmentRevision(namespace) == 0);
            assertEquals(first.preferredLeader(), m2.ownerOf(namespace));
            assertEquals(first.preferredLeader(), m3.ownerOf(namespace));
            int owners = (m1.isOwner(namespace) ? 1 : 0)
                    + (m2.isOwner(namespace) ? 1 : 0)
                    + (m3.isOwner(namespace) ? 1 : 0);
            assertEquals(1, owners);

            MetadataStore.Versioned<Records.NamespaceAssignment> persisted =
                    root1.getNamespaceAssignment(namespace).orElseThrow();
            assertEquals(first.replicaSet(), persisted.value().replicaSet());
            assertEquals(first.revision(), persisted.version());
        }
    }

    @Test
    void firstLookupSkipsDeadPreferredReplicaWithoutAnUnboundRevision() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root3 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), () -> true);
             NamespaceOwnership m3 = persistent(root3, ENDPOINTS.get(2), NOT_COORDINATOR)) {
            StrataNamespace namespace = namespaceWithReplicaOrder(ENDPOINTS);

            NamespaceOwnership.Assignment first = m2.assignmentOf(namespace);
            assertEquals(ENDPOINTS.get(1), first.preferredLeader(),
                    "initial creation must rotate directly to the first live standby");
            assertEquals(liveIncarnation(root2.curator(), ENDPOINTS.get(1)),
                    first.leaderIncarnation());
            assertEquals(0, first.revision(),
                    "first touch must not require a second coordinator CAS to become usable");

            m2.reconcileNow();
            assertEquals(0, root2.getNamespaceAssignment(namespace).orElseThrow().version());
            await(() -> m2.isOwner(namespace) && !m3.isOwner(namespace));
        }
    }

    @Test
    void realSessionPartitionWaitsForExpiryThenPromotesExactlyOneSuccessor() throws Exception {
        TestingCluster cluster = new TestingCluster(3);
        cluster.start();
        List<InstanceSpec> servers = new ArrayList<>(cluster.getInstances());
        InstanceSpec isolatedServer = servers.get(0);
        try (cluster;
             CuratorFramework isolated = newIsolatedCurator(isolatedServer.getConnectString());
             CuratorFramework survivor2 = newIsolatedCurator(servers.get(1).getConnectString());
             CuratorFramework survivor3 = newIsolatedCurator(servers.get(2).getConnectString());
             ZkMetadataStore root1 = new ZkMetadataStore(isolated);
             ZkMetadataStore root2 = new ZkMetadataStore(survivor2);
             ZkMetadataStore root3 = new ZkMetadataStore(survivor3);
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), () -> true);
             NamespaceOwnership m3 = persistent(root3, ENDPOINTS.get(2), NOT_COORDINATOR)) {
            StrataNamespace namespace = namespaceWithReplicaOrder(ENDPOINTS);
            NamespaceOwnership.Assignment original = m1.assignmentOf(namespace);
            m2.assignmentOf(namespace);
            m3.assignmentOf(namespace);
            await(() -> m1.isOwner(namespace));
            UUID oldIncarnation = original.leaderIncarnation();
            long oldSessionId = isolated.getZookeeperClient().getZooKeeper().getSessionId();
            String oldLivePath = liveControllerPath(ENDPOINTS.get(0));
            CountDownLatch suspended = new CountDownLatch(1);
            CountDownLatch lost = new CountDownLatch(1);
            isolated.getConnectionStateListenable().addListener((ignored, state) -> {
                if (state == ConnectionState.SUSPENDED) {
                    suspended.countDown();
                } else if (state == ConnectionState.LOST) {
                    lost.countDown();
                }
            });

            assertTrue(cluster.killServer(isolatedServer));
            assertTrue(suspended.await(5, TimeUnit.SECONDS),
                    "isolated controller never observed SUSPENDED");
            assertEquals(1, lost.getCount(),
                    "the pre-expiry assertion must run before ZooKeeper declares the session LOST");
            await(() -> !m1.isSessionReady() && m2.isAuthorityReady());
            assertNotNull(root2.curator().checkExists().forPath(oldLivePath),
                    "SUSPENDED is only uncertainty: the old ephemeral still exists before timeout");

            m2.reconcileNow();
            assertNotNull(root2.curator().checkExists().forPath(oldLivePath));
            NamespaceOwnership.Assignment beforeExpiry = m2.assignmentOf(namespace);
            assertEquals(original.authorityTerm(), beforeExpiry.authorityTerm(),
                    "the coordinator must not promote while the old session can still be alive");
            assertEquals(ENDPOINTS.get(0), beforeExpiry.preferredLeader());
            assertFalse(m1.isOwner(namespace),
                    "the partitioned owner must fail closed before ZooKeeper expires its session");

            await(() -> {
                try {
                    return root2.curator().checkExists().forPath(oldLivePath) == null;
                } catch (Exception e) {
                    return false;
                }
            });
            assertTrue(lost.await(5, TimeUnit.SECONDS),
                    "the isolated controller session never reached LOST");
            await(() -> m2.assignmentRevision(namespace) == original.revision() + 1);
            NamespaceOwnership.Assignment promoted = m2.assignmentOf(namespace);
            assertEquals(ENDPOINTS.get(1), promoted.preferredLeader());
            assertEquals(liveIncarnation(root2.curator(), ENDPOINTS.get(1)),
                    promoted.leaderIncarnation());
            await(() -> (m1.isOwner(namespace) ? 1 : 0)
                    + (m2.isOwner(namespace) ? 1 : 0)
                    + (m3.isOwner(namespace) ? 1 : 0) == 1);
            assertTrue(m2.isOwner(namespace));

            assertTrue(cluster.restartServer(isolatedServer));
            await(() -> m1.isSessionReady() && m1.isAuthorityReady());
            await(() -> {
                try {
                    return isolated.getZookeeperClient().getZooKeeper().getSessionId() != oldSessionId;
                } catch (Exception e) {
                    return false;
                }
            });
            await(() -> {
                try {
                    return !oldIncarnation.equals(
                            liveIncarnation(root2.curator(), ENDPOINTS.get(0)));
                } catch (Exception e) {
                    return false;
                }
            });
            assertEquals(ENDPOINTS.get(1), m1.ownerOf(namespace),
                    "the old endpoint rejoins as a standby under a new incarnation");
            assertFalse(m1.isOwner(namespace));
            assertEquals(promoted.authorityTerm(), m1.assignmentTerm(namespace));
        }
    }

    @Test
    void globalCoordinatorRotatesDeadOwnerOnceAndDoesNotFailBackOnRejoin() throws Exception {
        AtomicBoolean m2Coordinates = new AtomicBoolean(true);
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root3 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), m2Coordinates::get);
             NamespaceOwnership m3 = persistent(root3, ENDPOINTS.get(2), NOT_COORDINATOR)) {
            StrataNamespace namespace = namespaceOwnedBy(ENDPOINTS.get(0));
            NamespaceOwnership.Assignment original = m1.assignmentOf(namespace);
            assertEquals(ENDPOINTS.get(0), original.preferredLeader());
            m2.assignmentOf(namespace);
            m3.assignmentOf(namespace);

            m1.close();
            assertEquals(original.revision(), m2.assignmentRevision(namespace),
                    "closing ownership alone must not delete a live session's replacement-capable znode");
            root1.close(); // session close atomically removes only this incarnation's ephemeral membership

            await(() -> m2.assignmentRevision(namespace) == original.revision() + 1);
            NamespaceOwnership.Assignment promoted = m2.assignmentOf(namespace);
            assertNotEquals(ENDPOINTS.get(0), promoted.preferredLeader());
            assertTrue(List.of(ENDPOINTS.get(1), ENDPOINTS.get(2)).contains(promoted.preferredLeader()));
            assertTrue(promoted.preferredLeader().equals(ENDPOINTS.get(1))
                    ? m2.isOwner(namespace) : m3.isOwner(namespace));

            try (ZkMetadataStore restartedRoot = new ZkMetadataStore(zk.getConnectString());
                 NamespaceOwnership restarted =
                         persistent(restartedRoot, ENDPOINTS.get(0), NOT_COORDINATOR)) {
                await(restarted::isSessionReady);
                restarted.reconcileNow();
                assertEquals(promoted.preferredLeader(), restarted.ownerOf(namespace),
                        "a recovered old endpoint rejoins as standby; it does not auto-failback");
                assertFalse(restarted.isOwner(namespace));
                assertEquals(promoted.revision(), root2.getNamespaceAssignment(namespace).orElseThrow().version());
            }
        }
    }

    @Test
    void closeLinearizesBeforeAPausedPromotionCanCommit() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), () -> true)) {
            StrataNamespace namespace = namespaceOwnedBy(ENDPOINTS.get(0));
            NamespaceOwnership.Assignment original = m1.assignmentOf(namespace);
            m2.assignmentOf(namespace);
            await(() -> m1.isOwner(namespace) && m2.isAuthorityReady());

            CountDownLatch beforeCommit = new CountDownLatch(1);
            CountDownLatch releaseCommit = new CountDownLatch(1);
            m2.beforePromotionCasForTest(() -> {
                beforeCommit.countDown();
                awaitDespiteInterrupt(releaseCommit);
            });

            m1.close();
            root1.close();
            assertTrue(beforeCommit.await(10, TimeUnit.SECONDS),
                    "coordinator did not pause immediately before the promotion lifecycle gate");

            Thread coordinatorThread = reconcileThread(m2);
            m2.close();
            releaseCommit.countDown();
            coordinatorThread.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse(coordinatorThread.isAlive(), "closed ownership reconciler did not stop");

            assertEquals(original.revision(),
                    root2.getNamespaceAssignment(namespace).orElseThrow().version(),
                    "a promotion paused before close must not write after close returns");
        }
    }

    @Test
    void dirtyReconcileRefreshesAStaleDeadOwnerViewBeforePromotion() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), () -> true)) {
            StrataNamespace namespace = namespaceOwnedBy(ENDPOINTS.get(0));
            NamespaceOwnership.Assignment original = m1.assignmentOf(namespace);
            m2.assignmentOf(namespace);
            m2.reconcileNow();

            Thread coordinatorThread = reconcileThread(m2);
            coordinatorThread.interrupt();
            coordinatorThread.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse(coordinatorThread.isAlive(), "test must control the dirty reconciliation");

            assertTrue(m2.controllerIsLiveForTest(ENDPOINTS.get(0)));
            m2.forgetLiveControllerForTest(ENDPOINTS.get(0));
            assertFalse(m2.controllerIsLiveForTest(ENDPOINTS.get(0)),
                    "test precondition requires a stale cache that falsely reports the owner dead");
            m2.beforePromotionCasForTest(() -> {
                throw new AssertionError("a live owner must not reach the promotion CAS");
            });

            m2.invalidateAssignmentForTest(namespace);
            m2.reconcileDirtyNowForTest();

            assertTrue(m2.controllerIsLiveForTest(ENDPOINTS.get(0)),
                    "dirty reconciliation must refresh membership from ZooKeeper");
            assertEquals(original.revision(),
                    root2.getNamespaceAssignment(namespace).orElseThrow().version());
            assertEquals(ENDPOINTS.get(0), m2.ownerOf(namespace));
        }
    }

    @Test
    void suspendedSessionFailsClosedAndNotifiesLostBeforeReconnectReacquires() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), NOT_COORDINATOR)) {
            StrataNamespace namespace = namespaceOwnedBy(ENDPOINTS.get(0));
            await(m1::isAuthorityReady);
            int revision = m1.assignmentRevision(namespace);
            List<String> transitions = new CopyOnWriteArrayList<>();
            AutoCloseable listener = m1.addListener(new NamespaceOwnership.Listener() {
                @Override
                public void onAcquired(StrataNamespace ns, NamespaceOwnership.AuthorityTerm term) {
                    if (namespace.equals(ns)) {
                        transitions.add("acquired:" + term.revision());
                    }
                }

                @Override
                public void onLost(StrataNamespace ns, NamespaceOwnership.AuthorityTerm term) {
                    if (namespace.equals(ns)) {
                        transitions.add("lost:" + term.revision());
                    }
                }
            });
            try {
                await(() -> transitions.contains("acquired:" + revision));
                assertTrue(m1.isOwner(namespace));

                m1.handleConnectionStateForTest(ConnectionState.SUSPENDED);
                await(() -> transitions.contains("lost:" + revision));
                assertFalse(m1.isSessionReady());
                assertFalse(m1.isOwner(namespace));

                m1.handleConnectionStateForTest(ConnectionState.RECONNECTED);
                await(() -> count(transitions, "acquired:" + revision) == 2);
                assertTrue(m1.isSessionReady());
                assertTrue(m1.isOwner(namespace));
                assertEquals(List.of(
                                "acquired:" + revision,
                                "lost:" + revision,
                                "acquired:" + revision),
                        transitions,
                        "one namespace's callbacks must retain FIFO order across reconnect");
            } finally {
                listener.close();
            }
        }
    }

    @Test
    void authoritativeRevisionRejectsOwnerAfterAssignmentCasMoves() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), NOT_COORDINATOR)) {
            StrataNamespace namespace = namespaceOwnedBy(ENDPOINTS.get(0));
            await(m1::isAuthorityReady);
            NamespaceOwnership.Assignment original = m1.assignmentOf(namespace);
            assertTrue(m1.validateLocalAuthority(namespace, original.revision()));

            List<String> rotated = new ArrayList<>(original.replicaSet());
            rotated.remove(ENDPOINTS.get(1));
            rotated.add(0, ENDPOINTS.get(1));
            assertTrue(root2.putNamespaceAssignment(new Records.NamespaceAssignment(
                    namespace, original.generation(), rotated,
                    liveIncarnation(root2.curator(), ENDPOINTS.get(1))), original.revision()));

            await(() -> m1.assignmentRevision(namespace) == original.revision() + 1);
            assertFalse(m1.validateLocalAuthority(namespace, original.revision()));
            assertFalse(m1.isOwner(namespace));
            assertTrue(m2.isOwner(namespace));
        }
    }

    @Test
    void authoritativeReadCannotResurrectTermInvalidatedByANewerWatch() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), NOT_COORDINATOR)) {
            StrataNamespace namespace = namespaceOwnedBy(ENDPOINTS.get(0));
            NamespaceOwnership.Assignment old = m1.assignmentOf(namespace);
            await(() -> m1.isOwner(namespace));
            long beforeEvent = m1.assignmentEventVersionForTest(namespace);
            AtomicBoolean moved = new AtomicBoolean();
            m1.afterAuthorityReadForTest(() -> {
                if (!moved.compareAndSet(false, true)) {
                    return;
                }
                try {
                    List<String> rotated = rotateTo(old.replicaSet(), ENDPOINTS.get(1));
                    assertTrue(root2.putNamespaceAssignment(new Records.NamespaceAssignment(
                            namespace, old.generation(), rotated,
                            liveIncarnation(root2.curator(), ENDPOINTS.get(1))), old.revision()));
                    awaitUnchecked(() ->
                            m1.assignmentEventVersionForTest(namespace) > beforeEvent);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });

            assertFalse(m1.validateLocalAuthority(namespace, old.authorityTerm()),
                    "a read invalidated before publication must never authorize the old term");
            assertTrue(moved.get());
            await(() -> m2.isOwner(namespace));
            assertFalse(m1.isOwner(namespace));
            assertEquals(ENDPOINTS.get(1), m1.ownerOf(namespace));
            assertNotEquals(old.authorityTerm(), m1.assignmentTerm(namespace));
        }
    }

    @Test
    void ownershipCallbackCannotInvertRepositoryAndReconcileLocks() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership ownership =
                     persistent(root, ENDPOINTS.get(0), NOT_COORDINATOR)) {
            StrataNamespace namespace = namespaceOwnedBy(ENDPOINTS.get(0));
            ownership.assignmentOf(namespace);
            await(() -> ownership.isOwner(namespace));
            ReentrantLock repositoryOpenLock = new ReentrantLock();
            CountDownLatch lostCallbackEntered = new CountDownLatch(1);
            CountDownLatch lostCallbackCompleted = new CountDownLatch(1);
            AutoCloseable listener = ownership.addListener(new NamespaceOwnership.Listener() {
                @Override
                public void onLost(StrataNamespace ns, NamespaceOwnership.AuthorityTerm term) {
                    if (!namespace.equals(ns)) {
                        return;
                    }
                    lostCallbackEntered.countDown();
                    repositoryOpenLock.lock();
                    try {
                        lostCallbackCompleted.countDown();
                    } finally {
                        repositoryOpenLock.unlock();
                    }
                }
            });
            repositoryOpenLock.lock();
            try {
                Thread invalidator = Thread.ofVirtual().start(
                        () -> ownership.invalidateAssignmentForTest(namespace));
                assertTrue(lostCallbackEntered.await(5, TimeUnit.SECONDS));
                assertTimeoutPreemptively(Duration.ofSeconds(2),
                        () -> ownership.isOwner(namespace),
                        "isOwner must not wait behind a callback that is blocked on a repository lock");
                invalidator.join(2_000);
                assertFalse(invalidator.isAlive(),
                        "assignment invalidation must not invoke repository callbacks under reconcileSignal");
            } finally {
                repositoryOpenLock.unlock();
                listener.close();
            }
            assertTrue(lostCallbackCompleted.await(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void blockedNamespaceCallbackDoesNotStarveAnotherNamespace() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership ownership =
                     persistent(root, ENDPOINTS.get(0), NOT_COORDINATOR)) {
            List<StrataNamespace> pair = ownedNamespacesOnDifferentTransitionLanes();
            StrataNamespace blockedNamespace = pair.get(0);
            StrataNamespace independentNamespace = pair.get(1);
            ownership.assignmentOf(blockedNamespace);
            ownership.assignmentOf(independentNamespace);
            await(() -> ownership.isOwner(blockedNamespace)
                    && ownership.isOwner(independentNamespace));

            CountDownLatch blockedEntered = new CountDownLatch(1);
            CountDownLatch releaseBlocked = new CountDownLatch(1);
            CountDownLatch independentLost = new CountDownLatch(1);
            AutoCloseable listener = ownership.addListener(new NamespaceOwnership.Listener() {
                @Override
                public void onLost(StrataNamespace namespace, NamespaceOwnership.AuthorityTerm term) {
                    if (blockedNamespace.equals(namespace)) {
                        blockedEntered.countDown();
                        awaitUnchecked(releaseBlocked);
                    } else if (independentNamespace.equals(namespace)) {
                        independentLost.countDown();
                    }
                }
            });
            try {
                ownership.invalidateAssignmentForTest(blockedNamespace);
                assertTrue(blockedEntered.await(5, TimeUnit.SECONDS));
                ownership.invalidateAssignmentForTest(independentNamespace);
                assertTrue(independentLost.await(2, TimeUnit.SECONDS),
                        "a slow namespace callback must not block fencing on an independent lane");
            } finally {
                releaseBlocked.countDown();
                listener.close();
            }
        }
    }

    @Test
    void selfRemovingListenerWaitsForItsOtherLaneWithoutDeadlocking() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership ownership =
                     persistent(root, ENDPOINTS.get(0), NOT_COORDINATOR)) {
            List<StrataNamespace> pair = ownedNamespacesOnDifferentTransitionLanes();
            StrataNamespace selfRemoving = pair.get(0);
            StrataNamespace blocked = pair.get(1);
            ownership.assignmentOf(selfRemoving);
            ownership.assignmentOf(blocked);
            await(() -> ownership.isOwner(selfRemoving) && ownership.isOwner(blocked));

            CountDownLatch blockedEntered = new CountDownLatch(1);
            CountDownLatch releaseBlocked = new CountDownLatch(1);
            CountDownLatch selfCloseEntered = new CountDownLatch(1);
            CountDownLatch selfCloseReturned = new CountDownLatch(1);
            AtomicReference<AutoCloseable> registration = new AtomicReference<>();
            registration.set(ownership.addListener(new NamespaceOwnership.Listener() {
                @Override
                public void onLost(StrataNamespace namespace, NamespaceOwnership.AuthorityTerm term) {
                    if (blocked.equals(namespace)) {
                        blockedEntered.countDown();
                        awaitUnchecked(releaseBlocked);
                    } else if (selfRemoving.equals(namespace)) {
                        selfCloseEntered.countDown();
                        try {
                            registration.get().close();
                            selfCloseReturned.countDown();
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    }
                }
            }));

            ownership.invalidateAssignmentForTest(blocked);
            assertTrue(blockedEntered.await(5, TimeUnit.SECONDS));
            ownership.invalidateAssignmentForTest(selfRemoving);
            assertTrue(selfCloseEntered.await(5, TimeUnit.SECONDS));
            assertFalse(selfCloseReturned.await(200, TimeUnit.MILLISECONDS),
                    "self-removal must wait for the same registration already running on another lane");
            releaseBlocked.countDown();
            assertTrue(selfCloseReturned.await(5, TimeUnit.SECONDS),
                    "self-removal must not wait for its own callback frame");
        }
    }

    @Test
    void listenerErrorCannotKillTheNamespaceTransitionLane() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership ownership =
                     persistent(root, ENDPOINTS.get(0), NOT_COORDINATOR)) {
            StrataNamespace namespace = namespaceOwnedBy(ENDPOINTS.get(0));
            ownership.assignmentOf(namespace);
            await(() -> ownership.isOwner(namespace));
            CountDownLatch survivorCalled = new CountDownLatch(1);
            AutoCloseable failing = ownership.addListener(new NamespaceOwnership.Listener() {
                @Override
                public void onLost(StrataNamespace ns, NamespaceOwnership.AuthorityTerm term) {
                    if (namespace.equals(ns)) {
                        throw new AssertionError("injected listener error");
                    }
                }
            });
            AutoCloseable survivor = ownership.addListener(new NamespaceOwnership.Listener() {
                @Override
                public void onLost(StrataNamespace ns, NamespaceOwnership.AuthorityTerm term) {
                    if (namespace.equals(ns)) {
                        survivorCalled.countDown();
                    }
                }
            });
            try {
                ownership.invalidateAssignmentForTest(namespace);
                assertTrue(survivorCalled.await(5, TimeUnit.SECONDS),
                        "one listener Error must not kill the lane or suppress later listeners");
            } finally {
                failing.close();
                survivor.close();
            }
        }
    }

    @Test
    void backendReopensCurrentTermBeforeDelayedLostCallbackCanRun() throws Exception {
        CountDownLatch releaseLost = new CountDownLatch(1);
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), NOT_COORDINATOR)) {
            TestNamespaceMetadataFileStore sharedFileStore = new TestNamespaceMetadataFileStore();
            try (NamespaceLogBackend ownerA = new NamespaceLogBackend(root1, sharedFileStore, false);
                 NamespaceLogBackend ownerB = new NamespaceLogBackend(root2, sharedFileStore, false)) {
                StrataNamespace namespace = namespaceOwnedBy(ENDPOINTS.get(0));
                CountDownLatch lostEntered = new CountDownLatch(1);
                AutoCloseable blocker = m1.addListener(new NamespaceOwnership.Listener() {
                    @Override
                    public void onLost(StrataNamespace ns, NamespaceOwnership.AuthorityTerm term) {
                        if (namespace.equals(ns)) {
                            lostEntered.countDown();
                            awaitUnchecked(releaseLost);
                        }
                    }
                });
                try {
                    ownerA.setOwnership(m1);
                    ownerB.setOwnership(m2);
                    NamespaceOwnership.Assignment original = m1.assignmentOf(namespace);
                    await(() -> m1.isOwner(namespace));
                    FileId first = ownerA.createFileOwnerAssigned(fileTemplate(namespace, "/a", 1));
                    assertEquals(FileId.of(0), first);
                    long epochA = ownerA.namespaceOwnerEpoch(namespace);

                    List<String> toB = rotateTo(original.replicaSet(), ENDPOINTS.get(1));
                    assertTrue(root2.putNamespaceAssignment(new Records.NamespaceAssignment(
                            namespace, original.generation(), toB,
                            liveIncarnation(root2.curator(), ENDPOINTS.get(1))), original.revision()));
                    assertTrue(lostEntered.await(5, TimeUnit.SECONDS),
                            "the test did not delay A's backend LOST callback");
                    await(() -> m2.isOwner(namespace));

                    FileId second = ownerB.createFileOwnerAssigned(fileTemplate(namespace, "/b", 2));
                    assertEquals(FileId.of(1), second);
                    long epochB = ownerB.namespaceOwnerEpoch(namespace);
                    assertTrue(epochB > epochA);

                    NamespaceOwnership.Assignment assignedToB = m2.assignmentOf(namespace);
                    List<String> backToA = rotateTo(assignedToB.replicaSet(), ENDPOINTS.get(0));
                    assertTrue(root2.putNamespaceAssignment(new Records.NamespaceAssignment(
                            namespace, assignedToB.generation(), backToA,
                            liveIncarnation(root2.curator(), ENDPOINTS.get(0))),
                            assignedToB.revision()));
                    await(() -> m1.isOwner(namespace));

                    assertEquals(NamespaceLeaderState.FENCED, ownerA.leaderState(namespace),
                            "a stale ACTIVE handle must be hidden before its delayed LOST callback runs");
                    assertEquals(0, ownerA.namespaceActiveSinceMs(namespace));
                    assertEquals(0, ownerA.namespaceOwnerEpoch(namespace),
                            "repair must not capture the old owner epoch under the new assignment term");

                    assertTrue(ownerA.getFile(namespace, second).isPresent(),
                            "the A->B->A read must reopen and recover B's acknowledged write");
                    assertTrue(ownerA.namespaceOwnerEpoch(namespace) > epochB,
                            "the new assignment term must recover under a fresh metadata epoch");

                    releaseLost.countDown();
                    await(() -> ownerA.isNamespaceActive(namespace));
                    assertTrue(ownerA.getFile(namespace, second).isPresent(),
                            "the delayed old-term LOST callback must not fence the reopened repository");
                } finally {
                    releaseLost.countDown();
                    blocker.close();
                }
            }
        }
    }

    @Test
    void reconnectReconcilesAssignmentsBeforeItCanReacquireOwnership() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), NOT_COORDINATOR)) {
            StrataNamespace namespace = namespaceOwnedBy(ENDPOINTS.get(0));
            NamespaceOwnership.Assignment original = m1.assignmentOf(namespace);
            await(() -> m1.isOwner(namespace));
            List<String> transitions = new CopyOnWriteArrayList<>();
            AutoCloseable listener = m1.addListener(new NamespaceOwnership.Listener() {
                @Override
                public void onAcquired(StrataNamespace ns, NamespaceOwnership.AuthorityTerm term) {
                    if (namespace.equals(ns)) {
                        transitions.add("acquired:" + term.revision());
                    }
                }

                @Override
                public void onLost(StrataNamespace ns, NamespaceOwnership.AuthorityTerm term) {
                    if (namespace.equals(ns)) {
                        transitions.add("lost:" + term.revision());
                    }
                }
            });
            try {
                m1.handleConnectionStateForTest(ConnectionState.SUSPENDED);
                await(() -> transitions.contains("lost:" + original.revision()));

                List<String> rotated = new ArrayList<>(original.replicaSet());
                rotated.remove(ENDPOINTS.get(1));
                rotated.add(0, ENDPOINTS.get(1));
                assertTrue(root2.putNamespaceAssignment(new Records.NamespaceAssignment(
                        namespace, original.generation(), rotated,
                        liveIncarnation(root2.curator(), ENDPOINTS.get(1))), original.revision()));

                m1.handleConnectionStateForTest(ConnectionState.RECONNECTED);
                await(() -> m1.isAuthorityReady()
                        && m1.assignmentRevision(namespace) == original.revision() + 1);
                assertFalse(m1.isOwner(namespace));
                assertEquals(1, count(transitions, "acquired:" + original.revision()),
                        "the prior session's cached revision must not be reacquired");
                assertTrue(m2.isOwner(namespace));
            } finally {
                listener.close();
            }
        }
    }

    @Test
    void reconnectUsesCreationZxidToRejectQuiescedRestoreDataVersionAba() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
            NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), NOT_COORDINATOR)) {
            StrataNamespace namespace = namespaceOwnedBy(ENDPOINTS.get(0));
            await(m1::isAuthorityReady);
            NamespaceOwnership.Assignment stale = m1.assignmentOf(namespace);
            await(() -> m1.isOwner(namespace));
            assertEquals(0, stale.revision());
            assertTrue(m1.validateLocalAuthority(namespace, stale.authorityTerm()));

            m1.handleConnectionStateForTest(ConnectionState.SUSPENDED);
            await(() -> !m1.isSessionReady() && !m1.isAuthorityReady());
            closeAssignmentWatch(m1); // model delete/create events missed while disconnected

            // Assignment znodes are never deleted during online operation. This models an explicitly
            // quiesced disaster restore and verifies that a reconnect cannot accept its stale cache.
            root2.curator().delete().forPath(ZkMetadataStore.assignmentPath(namespace));
            assertTrue(root2.putNamespaceAssignment(new Records.NamespaceAssignment(
                    namespace, stale.generation(), stale.replicaSet(),
                    stale.leaderIncarnation()), -1));
            assertEquals(0, root2.getNamespaceAssignment(namespace).orElseThrow().version());
            assertEquals(stale.revision(), m1.assignmentRevision(namespace),
                    "the disconnected controller deliberately retains its missed-watch cache");

            m1.handleConnectionStateForTest(ConnectionState.RECONNECTED);
            m1.reconcileNow();

            assertTrue(m1.isAuthorityReady());
            assertEquals(0, m1.assignmentRevision(namespace),
                    "post-session reconciliation must accept a recreated znode whose dataVersion reset");
            NamespaceOwnership.Assignment recreated = m1.assignmentOf(namespace);
            assertNotEquals(stale.creationZxid(), recreated.creationZxid());
            assertEquals(stale.preferredLeader(), recreated.preferredLeader());
            assertTrue(m1.isOwner(namespace));
            assertFalse(m1.validateLocalAuthority(namespace, stale.authorityTerm()),
                    "same endpoint/incarnation/revision cannot validate across znode recreation");
            assertTrue(m1.validateLocalAuthority(namespace, recreated.authorityTerm()));
        }
    }

    @Test
    void authoritativeValidationRejectsSessionTransitionDuringConsensusRead() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), NOT_COORDINATOR)) {
            StrataNamespace namespace = namespaceOwnedBy(ENDPOINTS.get(0));
            NamespaceOwnership.Assignment assignment = m1.assignmentOf(namespace);
            await(() -> m1.isAuthorityReady() && m1.isOwner(namespace));
            AtomicBoolean transitioned = new AtomicBoolean();
            m1.afterAuthorityReadForTest(() -> {
                if (transitioned.compareAndSet(false, true)) {
                    m1.handleConnectionStateForTest(ConnectionState.SUSPENDED);
                    m1.handleConnectionStateForTest(ConnectionState.RECONNECTED);
                    try {
                        m1.reconcileNow();
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
            });

            boolean valid = m1.validateLocalAuthority(namespace, assignment.authorityTerm());
            assertFalse(valid,
                    "a session transition after the ZK read must fence the result");
            assertTrue(transitioned.get(), "the test did not inject the session transition");
            assertTrue(m1.isSessionReady());
            assertTrue(m1.isAuthorityReady());
        }
    }

    @Test
    void corruptAssignmentFencesOnlyItsNamespace() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), NOT_COORDINATOR)) {
            StrataNamespace healthy = namespaceOwnedBy(ENDPOINTS.get(0));
            StrataNamespace corrupt = StrataNamespace.of("corrupt-assignment");
            await(m1::isAuthorityReady);
            m1.assignmentOf(healthy);
            m1.assignmentOf(corrupt);
            await(() -> m1.isOwner(healthy));

            root2.curator().setData().forPath(
                    ZkMetadataStore.assignmentPath(corrupt), new byte[]{1, 2, 3});
            root2.curator().create().creatingParentsIfNeeded()
                    .forPath(ZkMetadataStore.META_NAMESPACES + "/invalid namespace/assignment",
                            new byte[]{1, 2, 3});
            m1.reconcileNow();

            await(() -> m1.isAuthorityReady() && m1.isOwner(healthy));
            ScpException failure = assertThrows(ScpException.class, () -> m1.assignmentOf(corrupt));
            assertEquals(ErrorCode.METADATA_RECOVERING, failure.code());
            assertFalse(m1.isOwner(corrupt));
        }
    }

    @Test
    void assignmentPathPayloadMismatchCannotPoisonAnotherNamespace() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), NOT_COORDINATOR)) {
            StrataNamespace victim = namespaceOwnedBy(ENDPOINTS.get(0));
            StrataNamespace forgedPath = StrataNamespace.of("forged-path");
            await(m1::isAuthorityReady);
            NamespaceOwnership.Assignment victimBefore = m1.assignmentOf(victim);
            m1.assignmentOf(forgedPath);
            await(() -> m1.isOwner(victim));

            Records.NamespaceAssignment forgedPayload = new Records.NamespaceAssignment(
                    victim,
                    victimBefore.generation(),
                    victimBefore.replicaSet(),
                    victimBefore.leaderIncarnation());
            root2.curator().setData().forPath(
                    ZkMetadataStore.assignmentPath(forgedPath), forgedPayload.encode());
            m1.reconcileNow();

            await(() -> m1.isAuthorityReady() && m1.isOwner(victim));
            assertEquals(victimBefore.authorityTerm(), m1.assignmentTerm(victim),
                    "a payload stored at another namespace path must not overwrite the victim cache");
            ScpException failure = assertThrows(
                    ScpException.class, () -> m1.assignmentOf(forgedPath));
            assertEquals(ErrorCode.METADATA_RECOVERING, failure.code());
            assertFalse(m1.isOwner(forgedPath));
        }
    }

    @Test
    void fullReconcileCannotPublishAuthorityAcrossSessionTransition() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership ownership =
                     persistent(root, ENDPOINTS.get(0), NOT_COORDINATOR)) {
            StrataNamespace namespace = namespaceOwnedBy(ENDPOINTS.get(0));
            await(ownership::isAuthorityReady);
            ownership.assignmentOf(namespace);
            await(() -> ownership.isOwner(namespace));

            AtomicBoolean injected = new AtomicBoolean();
            ownership.beforeAuthorityPublicationForTest(() -> {
                if (injected.compareAndSet(false, true)) {
                    ownership.handleConnectionStateForTest(ConnectionState.SUSPENDED);
                }
            });
            ownership.reconcileNow();

            assertTrue(injected.get());
            assertFalse(ownership.isSessionReady());
            assertFalse(ownership.isAuthorityReady(),
                    "a scan from the prior session state cannot republish readiness after SUSPENDED");
            assertFalse(ownership.isOwner(namespace));
        }
    }

    @Test
    void rejoiningEndpointCannotServeBetweenDeadSnapshotAndPromotionCas() throws Exception {
        AtomicBoolean m2Coordinates = new AtomicBoolean(true);
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root3 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), m2Coordinates::get);
             NamespaceOwnership m3 = persistent(root3, ENDPOINTS.get(2), NOT_COORDINATOR)) {
            StrataNamespace namespace = namespaceOwnedBy(ENDPOINTS.get(0));
            NamespaceOwnership.Assignment original = m1.assignmentOf(namespace);
            m2.assignmentOf(namespace);
            m3.assignmentOf(namespace);
            await(() -> m1.isOwner(namespace) && m2.isAuthorityReady() && m3.isAuthorityReady());

            CountDownLatch selectedSuccessor = new CountDownLatch(1);
            CountDownLatch releaseCas = new CountDownLatch(1);
            AtomicBoolean blockOnce = new AtomicBoolean();
            m2.beforePromotionCasForTest(() -> {
                if (blockOnce.compareAndSet(false, true)) {
                    selectedSuccessor.countDown();
                    awaitUnchecked(releaseCas);
                }
            });

            m1.handleConnectionStateForTest(ConnectionState.LOST);
            root3.curator().delete().forPath(liveControllerPath(ENDPOINTS.get(0)));
            assertTrue(selectedSuccessor.await(10, TimeUnit.SECONDS),
                    "coordinator did not reach the post-membership-snapshot CAS hook");
            try {
                m1.handleConnectionStateForTest(ConnectionState.RECONNECTED);
                m1.reconcileNow();
                assertTrue(m1.isSessionReady());
                assertTrue(m1.isAuthorityReady());
                assertFalse(m1.isOwner(namespace),
                        "a new endpoint incarnation must not resume the old assignment term");
                assertFalse(m1.validateLocalAuthority(namespace, original.authorityTerm()));
            } finally {
                releaseCas.countDown();
            }

            await(() -> m2.assignmentRevision(namespace) == original.revision() + 1);
            NamespaceOwnership.Assignment promoted = m2.assignmentOf(namespace);
            assertNotEquals(ENDPOINTS.get(0), promoted.preferredLeader());
            assertEquals(liveIncarnation(root2.curator(), promoted.preferredLeader()),
                    promoted.leaderIncarnation());
            await(() -> (m1.isOwner(namespace) ? 1 : 0)
                    + (m2.isOwner(namespace) ? 1 : 0)
                    + (m3.isOwner(namespace) ? 1 : 0) == 1);
        }
    }

    @Test
    void deletingLocalLiveMembershipFencesOldIncarnationBeforeItCanReRegister() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership m1 = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR);
             NamespaceOwnership m2 = persistent(root2, ENDPOINTS.get(1), NOT_COORDINATOR)) {
            StrataNamespace namespace = namespaceOwnedBy(ENDPOINTS.get(0));
            await(m1::isAuthorityReady);
            NamespaceOwnership.Assignment original = m1.assignmentOf(namespace);
            await(() -> m1.isOwner(namespace));

            root2.curator().delete().forPath(liveControllerPath(ENDPOINTS.get(0)));

            await(() -> !m1.isOwner(namespace));
            assertFalse(m1.validateLocalAuthority(namespace, original.authorityTerm()));
            assertEquals(original.revision(), root2.getNamespaceAssignment(namespace).orElseThrow().version(),
                    "without a coordinator the namespace stays unavailable instead of inventing an owner");
        }
    }

    @Test
    void membershipCloseNeverDeletesAReplacementIncarnationAtSameEndpoint() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             CuratorFramework ownerSession = newCurator(zk.getConnectString());
             CuratorFramework replacementSession = newCurator(zk.getConnectString())) {
            try (ZkMetadataStore root = new ZkMetadataStore(ownerSession);
                 ControllerMembership membership = new ControllerMembership(root, ENDPOINTS.get(0))) {
                String livePath = liveControllerPath(ENDPOINTS.get(0));
                ControllerMembership.LiveController original = ControllerMembership.LiveController.decode(
                        ownerSession.getData().forPath(livePath));

                membership.close();
                assertEquals(original, ControllerMembership.LiveController.decode(
                        ownerSession.getData().forPath(livePath)),
                        "component close must leave cleanup to the owning ZooKeeper session");
                ownerSession.close();

                await(() -> {
                    try {
                        return replacementSession.checkExists().forPath(livePath) == null;
                    } catch (Exception e) {
                        return false;
                    }
                });
                ControllerMembership.LiveController replacement =
                        new ControllerMembership.LiveController(ENDPOINTS.get(0), UUID.randomUUID());
                replacementSession.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                        .forPath(livePath, replacement.encode());
                ControllerMembership.LiveController remaining = ControllerMembership.LiveController.decode(
                        replacementSession.getData().forPath(livePath));
                assertEquals(replacement, remaining,
                        "the expired old session cannot delete a replacement incarnation");
            }
        }
    }

    @Test
    void coldAssignmentReadFailsClosedWhenConsensusIsUnavailable() throws Exception {
        try (TestingServer zk = new TestingServer(true)) {
            ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString());
            NamespaceOwnership ownership = persistent(root, ENDPOINTS.get(0), NOT_COORDINATOR);
            try {
                root.close();
                ScpException failure = assertThrows(ScpException.class,
                        () -> ownership.ownerOf(StrataNamespace.of("cold-after-root-close")));
                assertEquals(ErrorCode.METADATA_RECOVERING, failure.code());
            } finally {
                ownership.close();
            }
        }
    }

    @Test
    void duplicateLiveEndpointIncarnationIsRejected() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root1 = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore root2 = new ZkMetadataStore(zk.getConnectString());
             NamespaceOwnership ignored = persistent(root1, ENDPOINTS.get(0), NOT_COORDINATOR)) {
            assertThrows(IllegalStateException.class,
                    () -> persistent(root2, ENDPOINTS.get(0), NOT_COORDINATOR));
        }
    }

    private static NamespaceOwnership persistent(
            ZkMetadataStore root, String endpoint, BooleanSupplier coordinator) throws Exception {
        NamespaceOwnership ownership =
                NamespaceOwnership.persistent(root, endpoint, ENDPOINTS, 0, 3, coordinator, 25);
        await(ownership::isAuthorityReady);
        return ownership;
    }

    private static CuratorFramework newCurator(String connectString) throws Exception {
        CuratorFramework curator = CuratorFrameworkFactory.newClient(
                connectString, new ExponentialBackoffRetry(10, 3));
        curator.start();
        assertTrue(curator.blockUntilConnected(10, TimeUnit.SECONDS));
        return curator;
    }

    private static CuratorFramework newIsolatedCurator(String connectString) throws Exception {
        CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(6_000)
                .connectionTimeoutMs(2_000)
                .retryPolicy(new ExponentialBackoffRetry(20, 10))
                .ensembleTracker(false)
                .build();
        curator.start();
        assertTrue(curator.blockUntilConnected(10, TimeUnit.SECONDS));
        return curator;
    }

    private static void closeAssignmentWatch(NamespaceOwnership ownership) throws Exception {
        var field = NamespaceOwnership.class.getDeclaredField("assignmentWatch");
        field.setAccessible(true);
        ((PersistentWatcher) field.get(ownership)).close();
    }

    private static Thread reconcileThread(NamespaceOwnership ownership) throws Exception {
        var field = NamespaceOwnership.class.getDeclaredField("reconcileThread");
        field.setAccessible(true);
        return (Thread) field.get(ownership);
    }

    private static UUID liveIncarnation(CuratorFramework curator, String endpoint) throws Exception {
        return ControllerMembership.LiveController.decode(
                curator.getData().forPath(liveControllerPath(endpoint))).incarnation();
    }

    private static void awaitUnchecked(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for test latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for test latch", e);
        }
    }

    private static void awaitDespiteInterrupt(CountDownLatch latch) {
        boolean interrupted = false;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        try {
            while (true) {
                try {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0 || !latch.await(remaining, TimeUnit.NANOSECONDS)) {
                        throw new AssertionError("timed out waiting for test latch");
                    }
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static String liveControllerPath(String endpoint) {
        return ZkMetadataStore.META_CONTROLLER_LIVE + "/"
                + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(endpoint.getBytes(StandardCharsets.UTF_8));
    }

    private static StrataNamespace namespaceOwnedBy(String endpoint) {
        for (int i = 0; i < 10_000; i++) {
            StrataNamespace namespace = StrataNamespace.of("owner-" + i);
            if (NamespaceAssignmentPolicy.assign(namespace, 0, ENDPOINTS, 3)
                    .preferredLeader().equals(endpoint)) {
                return namespace;
            }
        }
        throw new AssertionError("no namespace owned by " + endpoint);
    }

    private static StrataNamespace namespaceWithReplicaOrder(List<String> expectedOrder) {
        for (int i = 0; i < 10_000; i++) {
            StrataNamespace namespace = StrataNamespace.of("ordered-" + i);
            if (NamespaceAssignmentPolicy.assign(namespace, 0, ENDPOINTS, 3)
                    .replicaSet().equals(expectedOrder)) {
                return namespace;
            }
        }
        throw new AssertionError("no namespace with replica order " + expectedOrder);
    }

    private static List<StrataNamespace> ownedNamespacesOnDifferentTransitionLanes() {
        StrataNamespace first = null;
        int firstLane = -1;
        for (int i = 0; i < 10_000; i++) {
            StrataNamespace namespace = StrataNamespace.of("lane-" + i);
            if (!NamespaceAssignmentPolicy.assign(namespace, 0, ENDPOINTS, 3)
                    .preferredLeader().equals(ENDPOINTS.get(0))) {
                continue;
            }
            int lane = (namespace.hashCode() & Integer.MAX_VALUE) % 32;
            if (first == null) {
                first = namespace;
                firstLane = lane;
            } else if (lane != firstLane) {
                return List.of(first, namespace);
            }
        }
        throw new AssertionError("could not find owned namespaces on distinct transition lanes");
    }

    private static List<String> rotateTo(List<String> replicas, String endpoint) {
        int index = replicas.indexOf(endpoint);
        List<String> rotated = new ArrayList<>(replicas.size());
        rotated.addAll(replicas.subList(index, replicas.size()));
        rotated.addAll(replicas.subList(0, index));
        return rotated;
    }

    private static Records.FileRecord fileTemplate(
            StrataNamespace namespace, String path, long operationId) {
        return new Records.FileRecord(
                FileId.of(0),
                namespace,
                StrataPath.of(path),
                3,
                2,
                true,
                FileState.OPEN,
                100L,
                List.of(),
                operationId,
                operationId);
    }

    private static int count(List<String> values, String expected) {
        int count = 0;
        for (String value : values) {
            if (expected.equals(value)) {
                count++;
            }
        }
        return count;
    }

    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), "condition was not met before timeout");
    }

    private static void awaitUnchecked(BooleanSupplier condition) {
        try {
            await(condition);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
