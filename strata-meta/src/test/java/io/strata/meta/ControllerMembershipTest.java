package io.strata.meta;

import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControllerMembershipTest {
    private static final String LOCAL = "controller-1:9100";
    private static final String REMOTE = "controller-2:9100";

    @Test
    void suspendedImmediatelyBeforeReadinessPublicationCannotBeOverwritten() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString());
             ControllerMembership membership = new ControllerMembership(root, LOCAL)) {
            assertTrue(membership.sessionReady());
            long readyVersion = membership.sessionStateVersion();
            AtomicBoolean injected = new AtomicBoolean();
            membership.beforeReadinessPublicationForTest(() -> {
                if (injected.compareAndSet(false, true)) {
                    membership.handleConnectionStateForTest(ConnectionState.SUSPENDED);
                }
            });

            assertThrows(IllegalStateException.class, membership::ensureRegisteredAndRefreshed);

            assertTrue(injected.get());
            assertFalse(membership.sessionReady(),
                    "a SUSPENDED event after refresh validation must win over readiness publication");
            assertTrue(membership.sessionStateVersion() > readyVersion,
                    "the serving fence must remain externally observable");
        }
    }

    @Test
    void deleteObservedAfterScanCannotBeResurrectedByRefreshPublication() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore localRoot = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore remoteRoot = new ZkMetadataStore(zk.getConnectString());
             ControllerMembership local = new ControllerMembership(localRoot, LOCAL);
             ControllerMembership remote = new ControllerMembership(remoteRoot, REMOTE)) {
            local.refreshAuthoritative();
            assertTrue(local.isLive(REMOTE));

            CountDownLatch deleteObserved = new CountDownLatch(1);
            try (AutoCloseable ignored = local.addListener(() -> {
                if (!local.isLive(REMOTE)) {
                    deleteObserved.countDown();
                }
            })) {
                AtomicBoolean injected = new AtomicBoolean();
                local.beforeRefreshPublicationForTest(() -> {
                    if (!injected.compareAndSet(false, true)) {
                        return;
                    }
                    try {
                        localRoot.curator().delete().forPath(livePath(REMOTE));
                        assertTrue(deleteObserved.await(5, TimeUnit.SECONDS),
                                "the cache delete must land before the stale scan attempts publication");
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                });

                local.refreshAuthoritative();

                assertTrue(injected.get());
                assertFalse(local.isLive(REMOTE),
                        "a scan containing the remote member must retry instead of resurrecting its deletion");
            }
        }
    }

    @Test
    void refreshScanIsSerializedBeforeLostAndReRegistration() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString());
             ControllerMembership membership = new ControllerMembership(root, LOCAL)) {
            UUID originalIncarnation = membership.localIncarnation();
            CountDownLatch scanComplete = new CountDownLatch(1);
            CountDownLatch releasePublication = new CountDownLatch(1);
            AtomicBoolean blocked = new AtomicBoolean();
            membership.beforeRefreshPublicationForTest(() -> {
                if (!blocked.compareAndSet(false, true)) {
                    return;
                }
                scanComplete.countDown();
                try {
                    assertTrue(releasePublication.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });

            CompletableFuture<Void> oldRefresh =
                    CompletableFuture.runAsync(() -> run(membership::refreshAuthoritative));
            assertTrue(scanComplete.await(5, TimeUnit.SECONDS));
            CompletableFuture<Void> reconnect = CompletableFuture.runAsync(() -> run(() -> {
                membership.handleConnectionStateForTest(ConnectionState.LOST);
                long beforeDelete = membership.membershipEventGenerationForTest();
                root.curator().delete().forPath(livePath(LOCAL));
                await(() -> membership.membershipEventGenerationForTest() > beforeDelete);
                membership.handleConnectionStateForTest(ConnectionState.RECONNECTED);
            }));
            await(() -> !membership.sessionReady());

            releasePublication.countDown();
            oldRefresh.get(5, TimeUnit.SECONDS);
            reconnect.get(5, TimeUnit.SECONDS);

            assertNotEquals(originalIncarnation, membership.localIncarnation());
            assertEquals(membership.localIncarnation(),
                    membership.liveController(LOCAL).incarnation());
            assertTrue(membership.sessionReady(),
                    "the old direct refresh must linearize before, not overwrite, the new registration");
        }
    }

    @Test
    void cacheEventPublishesOneCompleteImmutableSnapshot() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore localRoot = new ZkMetadataStore(zk.getConnectString());
             ZkMetadataStore remoteRoot = new ZkMetadataStore(zk.getConnectString());
             ControllerMembership local = new ControllerMembership(localRoot, LOCAL);
             ControllerMembership remote = new ControllerMembership(remoteRoot, REMOTE)) {
            local.refreshAuthoritative();
            Map<String, ControllerMembership.LiveController> before = local.liveControllers();
            assertEquals(2, before.size());

            CountDownLatch publicationBlocked = new CountDownLatch(1);
            CountDownLatch releasePublication = new CountDownLatch(1);
            CountDownLatch deleteObserved = new CountDownLatch(1);
            AtomicBoolean blocked = new AtomicBoolean();
            local.beforeCacheEventPublicationForTest(() -> {
                if (!blocked.compareAndSet(false, true)) {
                    return;
                }
                publicationBlocked.countDown();
                try {
                    assertTrue(releasePublication.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });
            try (AutoCloseable ignored = local.addListener(() -> {
                if (!local.isLive(REMOTE)) {
                    deleteObserved.countDown();
                }
            })) {
                localRoot.curator().delete().forPath(livePath(REMOTE));
                assertTrue(publicationBlocked.await(5, TimeUnit.SECONDS));
                assertEquals(before, local.liveControllers(),
                        "lock-free readers must retain the complete old snapshot while publication is blocked");

                releasePublication.countDown();
                assertTrue(deleteObserved.await(5, TimeUnit.SECONDS));
                Map<String, ControllerMembership.LiveController> after = local.liveControllers();
                assertEquals(1, after.size());
                assertTrue(after.containsKey(LOCAL));
                assertFalse(after.containsKey(REMOTE));
                assertThrows(UnsupportedOperationException.class,
                        () -> after.put(REMOTE, before.get(REMOTE)));
            } finally {
                releasePublication.countDown();
            }
        }
    }

    private static void await(Check check) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!check.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("condition did not become true");
            }
            Thread.sleep(5);
        }
    }

    private static void run(CheckedRunnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static String livePath(String endpoint) {
        String key = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(endpoint.getBytes(StandardCharsets.UTF_8));
        return ZkMetadataStore.META_CONTROLLER_LIVE + "/" + key;
    }

    @FunctionalInterface
    private interface Check {
        boolean getAsBoolean();
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run() throws Exception;
    }
}
