package io.strata.proto;

import io.strata.common.Backoff;
import io.strata.common.ConnectionPolicy;
import io.strata.common.Endpoint;
import io.strata.common.ErrorCode;
import io.strata.common.ScpConnectionException;
import io.strata.common.ScpException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Lifecycle manager for one logical SCP connection. It reconnects with bounded backoff,
 * probes idle live connections with PING, and exposes a generation number so callers can
 * detect when an open-session connection was replaced.
 */
public final class ManagedScpConnection implements AutoCloseable {
    private static final byte[] EMPTY_HEADER = new BufWriter(4).noTags().toBytes();

    public record CallResult(ByteBuffer header, long generation) {}

    private record Ref(ScpClient client, long generation) {}

    private final List<String> endpoints;
    private final ConnectionPolicy policy;
    private final byte clientKind;
    private final String clientId;
    private final String endpointLabel;
    private final boolean rotateOnFailure;
    private final boolean genericHeartbeat;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger activeApplicationCalls = new AtomicInteger();
    private final Thread monitorThread;

    private volatile ScpClient client;
    private volatile long generation;
    private volatile long lastApplicationActivityMs = System.currentTimeMillis();
    private volatile boolean maintainConnection;

    private int endpointIndex;
    private final Backoff backoff;

    public ManagedScpConnection(List<String> endpoints, ConnectionPolicy policy, byte clientKind,
                                String clientId, String endpointLabel, boolean rotateOnFailure,
                                boolean genericHeartbeat) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("at least one endpoint is required");
        }
        this.endpoints = List.copyOf(endpoints);
        this.policy = Objects.requireNonNull(policy, "policy");
        this.clientKind = clientKind;
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.endpointLabel = Objects.requireNonNull(endpointLabel, "endpointLabel");
        this.rotateOnFailure = rotateOnFailure;
        this.genericHeartbeat = genericHeartbeat;
        this.backoff = new Backoff(policy.reconnectInitialBackoffMs(), policy.reconnectMaxBackoffMs());
        this.monitorThread = Thread.ofVirtual().name("scp-conn-monitor-" + clientId + "-", 0)
                .start(this::monitorLoop);
    }

    public long generation() {
        return generation;
    }

    boolean monitorAliveForTests() {
        return monitorThread.isAlive();
    }

    public ByteBuffer call(Opcode op, byte[] header, ByteBuffer payload, long timeoutMs) {
        return callWithGeneration(op, header, payload, timeoutMs).header();
    }

    public CallResult callWithGeneration(Opcode op, byte[] header, ByteBuffer payload, long timeoutMs) {
        Ref ref = acquire(null);
        return callWithRef(ref, op, header, payload, timeoutMs);
    }

    public CallResult callWithGeneration(Opcode op, byte[] header, ByteBuffer payload,
                                         long timeoutMs, long expectedGeneration) {
        Ref ref = acquire(expectedGeneration);
        return callWithRef(ref, op, header, payload, timeoutMs);
    }

    private CallResult callWithRef(Ref ref, Opcode op, byte[] header, ByteBuffer payload, long timeoutMs) {
        try {
            return new CallResult(ref.client().call(op, header, payload, timeoutMs), ref.generation());
        } catch (RuntimeException e) {
            throw classifyFailure(ref.client(), e);
        } finally {
            release();
        }
    }

    public Frame callFrame(Opcode op, byte[] header, ByteBuffer payload, long timeoutMs) {
        Ref ref = acquire(null);
        try {
            return ref.client().callFrame(op, header, payload, timeoutMs);
        } catch (RuntimeException e) {
            throw classifyFailure(ref.client(), e);
        } finally {
            release();
        }
    }

    public CompletableFuture<Frame> sendWithTimeout(Opcode op, byte[] header, ByteBuffer payload,
                                                    long timeoutMs, long expectedGeneration) {
        Ref ref = acquire(expectedGeneration);
        return sendWithRef(ref, op, header, payload, timeoutMs);
    }

    public CompletableFuture<Frame> sendWithTimeout(Opcode op, byte[] header, ByteBuffer payload,
                                                    long timeoutMs) {
        Ref ref = acquire(null);
        return sendWithRef(ref, op, header, payload, timeoutMs);
    }

    private CompletableFuture<Frame> sendWithRef(Ref ref, Opcode op, byte[] header, ByteBuffer payload,
                                                 long timeoutMs) {
        CompletableFuture<Frame> future;
        try {
            future = ref.client().sendWithTimeout(op, header, payload, timeoutMs);
        } catch (RuntimeException e) {
            release();
            throw classifyFailure(ref.client(), e);
        }
        future.whenComplete((frame, error) -> {
            release();
            invalidateClosed(ref.client());
        });
        return future;
    }

    public void rotateEndpoint() {
        lock.lock();
        try {
            endpointIndex++;
            closeCurrentLocked();
            maintainConnection = true;
        } finally {
            lock.unlock();
        }
    }

    public void disconnect() {
        lock.lock();
        try {
            closeCurrentLocked();
            maintainConnection = false;
        } finally {
            lock.unlock();
        }
    }

    private Ref acquire(Long expectedGeneration) {
        if (closed.get()) {
            throw new ScpConnectionException(endpointLabel + " connection manager is closed");
        }
        activeApplicationCalls.incrementAndGet();
        touchApplication();
        lock.lock();
        try {
            maintainConnection = true;
            if (expectedGeneration != null && generation != expectedGeneration) {
                throw new ScpConnectionException(
                        endpointLabel + " connection generation changed: "
                                + expectedGeneration + " -> " + generation);
            }
            ScpClient c = connectLocked();
            long g = generation;
            if (expectedGeneration != null && g != expectedGeneration) {
                throw new ScpConnectionException(
                        endpointLabel + " connection generation changed: "
                                + expectedGeneration + " -> " + g);
            }
            return new Ref(c, g);
        } catch (RuntimeException e) {
            activeApplicationCalls.decrementAndGet();
            throw e;
        } finally {
            lock.unlock();
        }
    }

    private void release() {
        touchApplication();
        activeApplicationCalls.decrementAndGet();
    }

    private void touchApplication() {
        lastApplicationActivityMs = System.currentTimeMillis();
    }

    private ScpClient connectLocked() {
        if (client != null && !client.isClosed()) {
            return client;
        }
        closeCurrentLocked();
        String endpoint = endpoints.get(Math.floorMod(endpointIndex, endpoints.size()));
        Endpoint hp = Endpoint.parse(endpoint, endpointLabel, ErrorCode.INTERNAL);
        try {
            client = new ScpClient(hp.host(), hp.port(), clientKind, clientId, policy.connectTimeoutMs());
            generation++;
            backoff.reset();
            return client;
        } catch (IOException e) {
            if (rotateOnFailure && endpoints.size() > 1) {
                endpointIndex++;
            }
            throw new ScpConnectionException(endpointLabel + " unreachable at " + endpoint + ": " + e, e);
        }
    }

    private RuntimeException classifyFailure(ScpClient seen, RuntimeException e) {
        boolean connectionClosed = seen != null && seen.isClosed();
        invalidateClosed(seen);
        if (connectionClosed && !(e instanceof ScpConnectionException)) {
            return new ScpConnectionException(endpointLabel + " connection failed: " + e.getMessage(), e);
        }
        return e;
    }

    private void invalidateClosed(ScpClient seen) {
        if (seen == null || !seen.isClosed()) {
            return;
        }
        lock.lock();
        try {
            if (client == seen) {
                if (rotateOnFailure && endpoints.size() > 1) {
                    endpointIndex++;
                }
                closeCurrentLocked();
            }
        } finally {
            lock.unlock();
        }
    }

    private void closeCurrentLocked() {
        ScpClient c = client;
        client = null;
        if (c != null) {
            generation++;
            c.close();
        }
    }

    private void monitorLoop() {
        long delayMs = policy.heartbeatIntervalMs();
        while (!closed.get()) {
            sleepQuiet(delayMs);
            if (closed.get()) {
                return;
            }
            delayMs = policy.heartbeatIntervalMs();
            ScpClient current = client;
            long idleMs = System.currentTimeMillis() - lastApplicationActivityMs;
            if (idleMs >= policy.idleTimeoutMs()
                    && activeApplicationCalls.get() == 0
                    && (current == null || current.pendingCount() == 0)) {
                lock.lock();
                try {
                    closeCurrentLocked();
                    maintainConnection = false;
                } finally {
                    lock.unlock();
                }
                continue;
            }

            if (current == null || current.isClosed()) {
                if (maintainConnection) {
                    if (tryReconnect()) {
                        delayMs = policy.heartbeatIntervalMs();
                    } else {
                        delayMs = backoff.nextMs();
                    }
                }
                continue;
            }
            if (!genericHeartbeat || activeApplicationCalls.get() != 0 || current.pendingCount() != 0) {
                continue;
            }
            try {
                current.call(Opcode.PING, EMPTY_HEADER, null, policy.heartbeatTimeoutMs());
                backoff.reset();
            } catch (RuntimeException e) {
                lock.lock();
                try {
                    if (client == current) {
                        if (rotateOnFailure && endpoints.size() > 1) {
                            endpointIndex++;
                        }
                        closeCurrentLocked();
                    }
                } finally {
                    lock.unlock();
                }
                delayMs = backoff.nextMs();
            }
        }
    }

    private boolean tryReconnect() {
        lock.lock();
        try {
            if (closed.get() || !maintainConnection) {
                return false;
            }
            connectLocked();
            return true;
        } catch (RuntimeException e) {
            return false;
        } finally {
            lock.unlock();
        }
    }

    private static void sleepQuiet(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        monitorThread.interrupt();
        lock.lock();
        try {
            closeCurrentLocked();
            maintainConnection = false;
        } finally {
            lock.unlock();
        }
        try {
            monitorThread.join(Math.max(2_000L, policy.connectTimeoutMs() + 1_000L));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
