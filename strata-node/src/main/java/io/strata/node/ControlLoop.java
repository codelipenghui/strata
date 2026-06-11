package io.strata.node;

import io.strata.common.ChunkState;
import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import io.strata.format.ChunkStore;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Storage-node control plane (tech design §10.4): register, heartbeat (commands ride the
 * responses), inventory reports, and the REPLICATE executor (pull via FETCH_CHUNK).
 * Storage nodes never read cluster metadata — everything here is self-reporting and obedience.
 */
final class ControlLoop implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ControlLoop.class);
    private static final int CALL_TIMEOUT_MS = 10_000;
    private static final int FETCH_CHUNK_BYTES = 4 * 1024 * 1024;

    private final StorageNode node;
    private final NodeConfig config;
    private final ChunkStore store;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final LinkedBlockingQueue<Messages.Command> commandQueue = new LinkedBlockingQueue<>();
    private final ConcurrentLinkedQueue<Messages.CompletedCommand> completed = new ConcurrentLinkedQueue<>();

    private volatile ScpClient meta;
    private volatile long sessionEpoch = -1;
    private volatile int heartbeatIntervalMs = 1_000;
    private int endpointIndex = 0;

    ControlLoop(StorageNode node, NodeConfig config, ChunkStore store) {
        this.node = node;
        this.config = config;
        this.store = store;
    }

    private final java.util.List<Thread> threads = new java.util.ArrayList<>(3);

    void start() {
        threads.add(Thread.ofVirtual().name("node-control-" + node.incarnation()).start(this::run));
        threads.add(Thread.ofVirtual().name("node-cmd-exec-" + node.incarnation()).start(this::executeCommands));
        threads.add(Thread.ofVirtual().name("node-inventory-" + node.incarnation()).start(this::inventoryLoop));
    }

    private void run() {
        while (!closed.get()) {
            try {
                ensureRegistered();
                heartbeatOnce();
                Thread.sleep(heartbeatIntervalMs);
            } catch (InterruptedException e) {
                return;
            } catch (ScpException e) {
                if (e.code() == ErrorCode.LEASE_EXPIRED || e.code() == ErrorCode.NOT_REGISTERED) {
                    log.warn("session invalid ({}), re-registering", e.code());
                    sessionEpoch = -1;
                } else if (e.code() == ErrorCode.NOT_LEADER) {
                    rotateEndpoint();
                } else {
                    log.warn("control-loop error: {}", e.getMessage());
                }
                sleepQuiet(200);
            } catch (Exception e) {
                log.warn("metadata connection problem: {} — reconnecting", e.toString());
                disconnect();
                rotateEndpoint();
                sleepQuiet(200);
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
        if (meta == null || meta.isClosed()) {
            disconnect();
            String ep = config.metadataEndpoints().get(endpointIndex % config.metadataEndpoints().size());
            String[] hp = ep.split(":");
            meta = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_STORAGE_NODE,
                    "node-" + node.incarnation());
            sessionEpoch = -1;
        }
        if (sessionEpoch < 0) {
            var reg = new Messages.RegisterNode(
                    node.incarnation().getMostSignificantBits(), node.incarnation().getLeastSignificantBits(),
                    List.of(node.endpoint()), config.zone(), config.rack(), config.host(),
                    List.of(new Messages.MediaCapacity(config.mediaClass(), config.capacityBytes())),
                    1, 0);
            ByteBuffer resp = meta.call(Opcode.REGISTER_NODE, reg.encode(), null, CALL_TIMEOUT_MS);
            var r = Messages.RegisterResp.decode(resp);
            node.nodeIdAssigned(r.nodeId());
            sessionEpoch = r.sessionEpoch();
            heartbeatIntervalMs = Math.max(100, r.heartbeatIntervalMs());
            log.info("registered: nodeId={} session={} hb={}ms", r.nodeId(), r.sessionEpoch(), r.heartbeatIntervalMs());
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
                List.of(new Messages.MediaUsage(config.mediaClass(), used,
                        Math.max(0, config.capacityBytes() - used))),
                commandQueue.size(), done);
        try {
            ByteBuffer resp = meta.call(Opcode.NODE_HEARTBEAT, hb.encode(), null, CALL_TIMEOUT_MS);
            var r = Messages.HeartbeatResp.decode(resp);
            commandQueue.addAll(r.commands());
        } catch (ScpException e) {
            // re-queue completions so they are reported on the next successful heartbeat
            completed.addAll(done);
            throw e;
        } catch (RuntimeException e) {
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
                        for (var id : d.chunkIds()) store.delete(id); // CHUNK_NOT_FOUND is idempotent-ok
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

    private void replicate(Messages.ReplicateCmd cmd) throws IOException {
        if (store.contains(cmd.chunkId())) {
            // command replay — but only a VALID copy counts: a local chunk whose seal state or
            // crc/length mismatch the descriptor is corrupt and must be replaced, not trusted
            var stat = store.stat(cmd.chunkId());
            if (stat.state() == io.strata.common.ChunkState.SEALED
                    && stat.sealedLength() == cmd.expectedLength()
                    && (cmd.expectedCrc() == 0 || stat.dataCrc() == cmd.expectedCrc())) {
                return;
            }
            log.warn("local copy of {} mismatches descriptor (state={} len={} crc={}) — re-pulling",
                    cmd.chunkId(), stat.state(), stat.sealedLength(), stat.dataCrc());
            store.delete(cmd.chunkId());
        }
        ScpException last = null;
        for (Messages.Replica source : cmd.sources()) {
            if (source.nodeId() == node.nodeId()) continue;
            String[] hp = source.endpoint().split(":");
            try (ScpClient src = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_STORAGE_NODE,
                    "repair-" + node.nodeId())) {
                byte[] file = fetchWholeFile(src, cmd);
                store.importSealed(cmd.chunkId(), file, cmd.expectedLength(), cmd.expectedCrc());
                log.info("replicated {} from node {} ({} bytes)", cmd.chunkId(), source.nodeId(), file.length);
                return;
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

    private byte[] fetchWholeFile(ScpClient src, Messages.ReplicateCmd cmd) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long offset = 0;
        long fileLength = Long.MAX_VALUE;
        while (offset < fileLength) {
            var fetch = new Messages.FetchChunk(cmd.chunkId(), offset, FETCH_CHUNK_BYTES);
            var frame = src.callFrame(Opcode.FETCH_CHUNK, fetch.encode(), null, CALL_TIMEOUT_MS);
            ByteBuffer hb = frame.headerSlice();
            io.strata.proto.Resp.check(hb);
            var resp = Messages.FetchResp.decode(hb);
            if (resp.state() != ChunkState.SEALED) {
                throw new ScpException(ErrorCode.INTERNAL, "source chunk not sealed");
            }
            fileLength = resp.fileLength();
            int n = frame.payloadLength();
            if (n == 0 && offset < fileLength) {
                throw new ScpException(ErrorCode.INTERNAL, "short fetch at " + offset);
            }
            byte[] part = new byte[n];
            frame.payloadSlice().get(part);
            out.write(part);
            offset += n;
        }
        return out.toByteArray();
    }

    private void inventoryLoop() {
        while (!closed.get()) {
            sleepQuiet(config.inventoryIntervalMs());
            if (closed.get()) return;
            try {
                ScpClient m = meta;
                if (m == null || m.isClosed() || sessionEpoch < 0) continue;
                List<Messages.InventoryEntry> entries = new ArrayList<>();
                for (var item : store.inventory()) {
                    entries.add(new Messages.InventoryEntry(item.chunkId(), item.state(), item.length(), item.crc()));
                }
                var report = new Messages.InventoryReport(node.nodeId(), 0, 1, entries);
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
        ScpClient m = meta;
        meta = null;
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
        disconnect();
        // executeCommands blocks in commandQueue.take() and the loops in sleep — interrupt so
        // the virtual threads exit promptly instead of lingering per closed node
        for (Thread t : threads) {
            t.interrupt();
        }
        for (Thread t : threads) {
            try {
                t.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
