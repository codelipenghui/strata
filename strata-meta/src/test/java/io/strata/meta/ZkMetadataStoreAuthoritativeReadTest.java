package io.strata.meta;

import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.ErrorListenerPathable;
import org.apache.curator.framework.api.SyncBuilder;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkMetadataStoreAuthoritativeReadTest {

    @Test
    void authoritativeSyncWaitsForSuccessfulBackgroundCallback() throws Exception {
        ControlledSync sync = new ControlledSync();
        CompletableFuture<Void> operation = invokeAwaitSync(sync);

        assertTrue(sync.submitted.await(2, TimeUnit.SECONDS));
        assertFalse(operation.isDone(), "authoritative read must not overtake the queued sync callback");

        sync.complete(KeeperException.Code.OK);
        operation.get(2, TimeUnit.SECONDS);
    }

    @Test
    void authoritativeSyncPropagatesNonOkBackgroundResult() throws Exception {
        ControlledSync sync = new ControlledSync();
        CompletableFuture<Void> operation = invokeAwaitSync(sync);

        assertTrue(sync.submitted.await(2, TimeUnit.SECONDS));
        sync.complete(KeeperException.Code.CONNECTIONLOSS);

        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> operation.get(2, TimeUnit.SECONDS));
        assertInstanceOf(KeeperException.ConnectionLossException.class, failure.getCause());
    }

    @Test
    void authoritativeSyncFailsClosedWhenCallbackNeverArrives() {
        ControlledSync sync = new ControlledSync();

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> ZkMetadataStore.awaitSync(sync.builder, "/strata/test", 25));

        assertTrue(failure.getMessage().contains("timed out"));
    }

    private static CompletableFuture<Void> invokeAwaitSync(ControlledSync sync) {
        return CompletableFuture.runAsync(() -> {
            try {
                ZkMetadataStore.awaitSync(sync.builder, "/strata/test");
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }

    /** Deterministic model of Curator's background-only SyncBuilder. */
    private static final class ControlledSync {
        private final CountDownLatch submitted = new CountDownLatch(1);
        private final AtomicReference<BackgroundCallback> callback = new AtomicReference<>();
        private final SyncBuilder builder = (SyncBuilder) Proxy.newProxyInstance(
                SyncBuilder.class.getClassLoader(), new Class<?>[] {SyncBuilder.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("inBackground")
                            && args != null && args.length > 0 && args[0] instanceof BackgroundCallback cb) {
                        callback.set(cb);
                        return pathable();
                    }
                    if (method.getName().equals("forPath")) {
                        throw new AssertionError("authoritative sync must install a result callback");
                    }
                    return objectMethod(proxy, method.getName());
                });

        @SuppressWarnings("unchecked")
        private ErrorListenerPathable<Void> pathable() {
            return (ErrorListenerPathable<Void>) Proxy.newProxyInstance(
                    ErrorListenerPathable.class.getClassLoader(), new Class<?>[] {ErrorListenerPathable.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("forPath")) {
                            submitted.countDown();
                            return null;
                        }
                        if (method.getName().equals("withUnhandledErrorListener")) {
                            return proxy;
                        }
                        return objectMethod(proxy, method.getName());
                    });
        }

        private void complete(KeeperException.Code code) throws Exception {
            CuratorEvent event = (CuratorEvent) Proxy.newProxyInstance(
                    CuratorEvent.class.getClassLoader(), new Class<?>[] {CuratorEvent.class},
                    (proxy, method, args) -> method.getName().equals("getResultCode")
                            ? code.intValue() : objectMethod(proxy, method.getName()));
            callback.get().processResult(null, event);
        }

        private static Object objectMethod(Object proxy, String name) {
            return switch (name) {
                case "toString" -> "ControlledSync";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> false;
                default -> null;
            };
        }
    }
}
