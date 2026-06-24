package io.strata.node;

import io.strata.common.Backoff;
import io.strata.common.Checks;
import io.strata.common.ChunkState;
import io.strata.common.ConnectionPolicy;
import io.strata.common.Endpoint;
import io.strata.common.ErrorCode;
import io.strata.common.ScpConnectionException;
import io.strata.common.ScpException;
import io.strata.format.ChunkFormats;
import io.strata.format.ChunkStore;
import io.strata.proto.Messages;
import io.strata.proto.ManagedScpConnection;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Data-node control plane (tech design §10.4): register, heartbeat (commands ride the
 * responses), inventory reports, and the REPLICATE executor (pull via FETCH_CHUNK).
 * Data nodes never read cluster metadata — everything here is self-reporting and obedience.
 */
final class ControlLoop implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ControlLoop.class);
    private static final int CALL_TIMEOUT_MS = 10_000;
    private static final int FETCH_CHUNK_BYTES = 4 * 1024 * 1024;
    private static final long MAX_REPAIR_FOOTER_BYTES = 64L * 1024 * 1024;

    private final DataNode node;
    private final DataNodeConfig config;
    private final ChunkStore store;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final LinkedBlockingQueue<Messages.Command> commandQueue = new LinkedBlockingQueue<>();
    private final ConcurrentLinkedQueue<Messages.CompletedCommand> completed = new ConcurrentLinkedQueue<>();

    private volatile ManagedScpConnection controller;
    private volatile long sessionEpoch = -1;
    private volatile int heartbeatIntervalMs = 1_000;
    // confined to the control (run) thread today; volatile guards the lock-discipline asymmetry —
    // read under connLock in ensureRegisteredLocked, written lock-free in rotateEndpoint
    private volatile int endpointIndex = 0;
    private final Backoff backoff;

    ControlLoop(DataNode node, DataNodeConfig config, ChunkStore store) {
        this.node = node;
        this.config = config;
        this.store = store;
        ConnectionPolicy policy = config != null ? config.connectionPolicy() : ConnectionPolicy.DEFAULT;
        this.backoff = new Backoff(policy.reconnectInitialBackoffMs(), policy.reconnectMaxBackoffMs());
    }

    private final java.util.List<Thread> threads = new java.util.ArrayList<>(3);

    /** Whether the node currently holds a metadata registration (lease session). */
    boolean registered() {
        return sessionEpoch >= 0;
    }

    void start() {
        threads.add(Thread.ofVirtual().name("node-control-" + node.incarnation()).start(this::run));
        threads.add(Thread.ofVirtual().name("node-cmd-exec-" + node.incarnation()).start(this::executeCommands));
        threads.add(Thread.ofVirtual().name("node-inventory-" + node.incarnation()).start(this::inventoryLoop));
    }

    private void run() {
        while (!closed.get()) {
            try {
                ensureRegistered();
                if (closed.get()) {
                    continue; // raced with close(): skip heartbeat (controller may be absent) and exit
                }
                heartbeatOnce();
                backoff.reset();
                Thread.sleep(heartbeatIntervalMs);
            } catch (InterruptedException e) {
                return;
            } catch (ScpConnectionException e) {
                log.warn("metadata connection problem: {} — reconnecting", e.toString());
                rotateEndpoint();
                sleepQuiet(backoff.nextMs());
            } catch (ScpException e) {
                if (e.code() == ErrorCode.LEASE_EXPIRED || e.code() == ErrorCode.NOT_REGISTERED) {
                    log.warn("session invalid ({}), re-registering", e.code());
                    sessionEpoch = -1;
                } else if (e.code() == ErrorCode.NOT_LEADER) {
                    rotateEndpoint();
                } else {
                    log.warn("control-loop error: {}", e.getMessage());
                }
                sleepQuiet(backoff.nextMs());
            } catch (Exception e) {
                log.warn("metadata connection problem: {} — reconnecting", e.toString());
                rotateEndpoint();
                sleepQuiet(backoff.nextMs());
            }
        }
    }

    private final java.util.concurrent.locks.ReentrantLock connLock =
            new java.util.concurrent.locks.ReentrantLock();

    private void ensureRegistered() throws IOException {
        connLock.lock();
        try {
            ensureRegisteredLocked();
        } finally {
            connLock.unlock();
        }
    }

    private void ensureRegisteredLocked() throws IOException {
        if (closed.get()) {
            // shutting down: do not create a fresh connection that close() might race past
            return;
        }
        if (controller == null) {
            disconnect();
            String ep = config.controllerEndpoints().get(endpointIndex % config.controllerEndpoints().size());
            Endpoint.parse(ep, "endpoint", ErrorCode.INTERNAL);
            controller = new ManagedScpConnection(List.of(ep), config.connectionPolicy(),
                    ScpClient.KIND_DATA_NODE, "node-" + node.incarnation(),
                    "controller endpoint", false, false);
            sessionEpoch = -1;
        }
        if (sessionEpoch < 0) {
            var reg = new Messages.RegisterNode(
                    node.nodeId(),
                    node.incarnation().getMostSignificantBits(), node.incarnation().getLeastSignificantBits(),
                    List.of(node.endpoint()), config.zone(), config.rack(), config.host(),
                    List.of(new Messages.StorageCapacity(config.capacityBytes())),
                    1, 0);
            ByteBuffer resp = controller.call(Opcode.REGISTER_NODE, reg.encode(), null, CALL_TIMEOUT_MS);
            var r = Messages.RegisterResp.decode(resp);
            if (r.nodeId() != node.nodeId()) {
                // the node owns its id (volume-bound, STRATA_NODE_ID); the leader only echoes it back
                log.warn("controller echoed nodeId {} but this node is {}", r.nodeId(), node.nodeId());
            }
            sessionEpoch = r.sessionEpoch();
            heartbeatIntervalMs = Math.max(100, r.heartbeatIntervalMs());
            log.info("registered: nodeId={} session={} hb={}ms", node.nodeId(), r.sessionEpoch(), r.heartbeatIntervalMs());
        }
    }

    private void heartbeatOnce() {
        List<Messages.CompletedCommand> done = new ArrayList<>();
        Messages.CompletedCommand c;
        while ((c = completed.poll()) != null) done.add(c);

        long used = store.usedBytes();
        var hb = new Messages.NodeHeartbeat(node.nodeId(),
                node.incarnation().getMostSignificantBits(), node.incarnation().getLeastSignificantBits(),
                sessionEpoch,
                List.of(new Messages.StorageUsage(used, Math.max(0, config.capacityBytes() - used))),
                commandQueue.size(), done);
        try {
            ByteBuffer resp = controller.call(Opcode.NODE_HEARTBEAT, hb.encode(), null, CALL_TIMEOUT_MS);
            var r = Messages.HeartbeatResp.decode(resp);
            commandQueue.addAll(r.commands());
        } catch (RuntimeException e) {
            // re-queue completions so they are reported on the next successful heartbeat
            completed.addAll(done);
            throw e;
        }
    }

    private void executeCommands() {
        while (!closed.get()) {
            Messages.Command cmd;
            try {
                cmd = commandQueue.take();
            } catch (InterruptedException e) {
                return;
            }
            short status = 0;
            try {
                switch (cmd) {
                    case Messages.ReplicateCmd r -> replicate(r);
                    case Messages.DeleteCmd d -> {
                        for (var id : d.chunkIds()) {
                            ErrorCode result = store.delete(id);
                            if (result != ErrorCode.OK && result != ErrorCode.CHUNK_NOT_FOUND) {
                                throw new ScpException(result, "delete " + id + " failed");
                            }
                        }
                    }
                    case Messages.DrainCmd dr -> node.setDraining(true);
                }
            } catch (ScpException e) {
                log.warn("command {} failed: {}", cmd.commandId(), e.getMessage());
                status = e.code().code;
            } catch (Exception e) {
                log.warn("command {} failed", cmd.commandId(), e);
                status = ErrorCode.INTERNAL.code;
            }
            completed.add(new Messages.CompletedCommand(cmd.commandId(), status));
        }
    }

    void replicate(Messages.ReplicateCmd cmd) throws IOException {
        if (store.contains(cmd.chunkId())) {
            // command replay — but only a VALID copy counts: a local chunk whose seal state or
            // crc/length mismatch the descriptor is corrupt and must be replaced, not trusted
            var stat = store.stat(cmd.chunkId());
            if (stat.state() == io.strata.common.ChunkState.SEALED
                    && stat.sealedLength() == cmd.expectedLength()
                    && stat.dataCrc() == cmd.expectedCrc()) {
                return;
            }
            log.warn("local copy of {} mismatches descriptor (state={} len={} crc={}) — re-pulling",
                    cmd.chunkId(), stat.state(), stat.sealedLength(), stat.dataCrc());
            ErrorCode deleteResult = store.delete(cmd.chunkId());
            if (deleteResult != ErrorCode.OK && deleteResult != ErrorCode.CHUNK_NOT_FOUND) {
                throw new ScpException(deleteResult, "delete stale local copy of " + cmd.chunkId() + " failed");
            }
        }
        ScpException last = null;
        for (Messages.Replica source : cmd.sources()) {
            if (source.nodeId() == node.nodeId()) continue;
            try {
                Endpoint hp = Endpoint.parse(source.endpoint(), "endpoint", ErrorCode.INTERNAL);
                try (ScpClient src = new ScpClient(hp.host(), hp.port(), ScpClient.KIND_DATA_NODE,
                        "repair-" + node.nodeId())) {
                    Path tmp = store.createImportTemp(cmd.chunkId());
                    try {
                        long fileLength = fetchWholeFile(src, cmd, tmp);
                        store.importSealed(cmd.chunkId(), tmp, cmd.expectedLength(), cmd.expectedCrc());
                        tmp = null; // importSealed consumes the temp file by moving it into place.
                        log.info("replicated {} from node {} ({} bytes)", cmd.chunkId(), source.nodeId(), fileLength);
                    } finally {
                        if (tmp != null) {
                            Files.deleteIfExists(tmp);
                        }
                    }
                    return;
                }
            } catch (ScpException e) {
                last = e;
                log.warn("replicate {} from {} failed: {}", cmd.chunkId(), source.endpoint(), e.getMessage());
            } catch (IOException e) {
                last = new ScpException(ErrorCode.INTERNAL, "source " + source.endpoint() + ": " + e);
                log.warn("replicate {} from {} failed: {}", cmd.chunkId(), source.endpoint(), e.toString());
            }
        }
        throw last != null ? last : new ScpException(ErrorCode.INTERNAL, "no usable source");
    }

    long fetchWholeFile(ScpClient src, Messages.ReplicateCmd cmd, Path output) throws IOException {
        long offset = 0;
        long fileLength = -1;
        long maxFileLength = maxRepairFileLength(cmd.expectedLength());
        try (FileChannel out = FileChannel.open(output,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            while (fileLength < 0 || offset < fileLength) {
                var fetch = new Messages.FetchChunk(cmd.chunkId(), offset, FETCH_CHUNK_BYTES);
                var frame = src.callFrame(Opcode.FETCH_CHUNK, fetch.encode(), null, CALL_TIMEOUT_MS);
                Messages.FetchResp resp;
                try {
                    ByteBuffer hb = frame.headerSlice();
                    io.strata.proto.Resp.check(hb);
                    resp = Messages.FetchResp.decode(hb);
                } catch (ScpException e) {
                    throw e;
                } catch (RuntimeException e) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                            "malformed fetch response from repair source: " + e);
                }
                if (resp.state() != ChunkState.SEALED) {
                    throw new ScpException(ErrorCode.INTERNAL, "source chunk not sealed");
                }
                long reportedLength = resp.fileLength();
                if (reportedLength < ChunkFormats.HEADER_SIZE + ChunkFormats.TRAILER_SIZE
                        || reportedLength > maxFileLength) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                            "source file length out of bounds: " + reportedLength);
                }
                if (fileLength < 0) {
                    fileLength = reportedLength;
                } else if (reportedLength != fileLength) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                            "source file length changed: " + fileLength + " -> " + reportedLength);
                }
                int n = frame.payloadLength();
                if (n > FETCH_CHUNK_BYTES) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK,
                            "source sent " + n + " bytes for fetch limit " + FETCH_CHUNK_BYTES);
                }
                if (n > fileLength - offset) {
                    throw new ScpException(ErrorCode.CORRUPT_CHUNK, "source sent bytes past EOF");
                }
                if (n == 0 && offset < fileLength) {
                    throw new ScpException(ErrorCode.INTERNAL, "short fetch at " + offset);
                }
                ByteBuffer payload = frame.payloadSlice();
                long written = 0;
                while (payload.hasRemaining()) {
                    int w = out.write(payload, offset + written);
                    if (w <= 0) {
                        throw new ScpException(ErrorCode.INTERNAL, "repair fetch made no write progress");
                    }
                    written += w;
                }
                if (written != n) {
                    throw new ScpException(ErrorCode.INTERNAL, "repair fetch wrote " + written + " != " + n);
                }
                offset += n;
            }
            out.force(true);
        }
        return fileLength;
    }

    private static long maxRepairFileLength(long expectedLength) {
        if (expectedLength < 0) {
            throw new ScpException(ErrorCode.CORRUPT_CHUNK, "negative expected repair length");
        }
        long max = Checks.checkedAdd(ChunkFormats.HEADER_SIZE, expectedLength, "repair length");
        max = Checks.checkedAdd(max, ChunkFormats.TRAILER_SIZE, "repair length");
        max = Checks.checkedAdd(max, MAX_REPAIR_FOOTER_BYTES, "repair length");
        return max;
    }

    private void inventoryLoop() {
        int cycle = 0;
        while (!closed.get()) {
            sleepQuiet(config.inventoryIntervalMs());
            if (closed.get()) return;
            try {
                // periodic scrub (every 10th cycle): recompute sealed data CRCs so rot shows up
                // in the NEXT report's crc and the coordinator drops + re-repairs the replica
                if (++cycle % 10 == 0) {
                    store.scrubOnce();
                }
                ManagedScpConnection m = controller;
                if (m == null || sessionEpoch < 0) continue;
                List<Messages.InventoryEntry> entries = new ArrayList<>();
                for (var item : store.inventory()) {
                    entries.add(new Messages.InventoryEntry(item.chunkId(), item.state(), item.length(), item.crc()));
                }
                var report = new Messages.InventoryReport(node.nodeId(),
                        node.incarnation().getMostSignificantBits(), node.incarnation().getLeastSignificantBits(),
                        sessionEpoch, 0, 1, entries);
                m.call(Opcode.INVENTORY_REPORT, report.encode(), null, CALL_TIMEOUT_MS);
            } catch (Exception e) {
                log.debug("inventory report failed: {}", e.toString());
            }
        }
    }

    private void rotateEndpoint() {
        endpointIndex++;
        disconnect();
    }

    private void disconnect() {
        ManagedScpConnection m = controller;
        controller = null;
        sessionEpoch = -1;
        if (m != null) m.close();
    }

    private static void sleepQuiet(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        closed.set(true);
        // executeCommands blocks in commandQueue.take() and the loops in sleep — interrupt so
        // the virtual threads exit promptly instead of lingering per closed node
        for (Thread t : threads) {
            t.interrupt();
        }
        // Join BEFORE disconnecting: the control thread can create a fresh metadata connection
        // (and its monitor thread) in the window between observing !closed and here, which a
        // disconnect placed earlier would miss — leaking the connection. Once the threads are
        // joined nobody else creates connections, so this disconnect closes the last one for good.
        for (Thread t : threads) {
            try {
                t.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        disconnect();
    }
}
