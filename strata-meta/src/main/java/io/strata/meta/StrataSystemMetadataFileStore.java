package io.strata.meta;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * The production {@link NamespaceMetadataFileStore} (design §5, §8): each namespace's metadata-log and
 * snapshot bytes are stored as <b>replicated Strata files</b> in the reserved {@code strata-meta} system
 * namespace — their descriptors in the ZooKeeper root, their bytes as replicated chunks on data
 * nodes. This reuses the proven client append/seal/recovery machinery; recovery of an open log
 * {@code recoverAndSeal}s it — fencing any previous writer and sealing at the committed (quorum-durable)
 * prefix (§7.3, §13). A successor on another controller node can therefore read a failed owner's metadata log
 * from the data-node replicas, which the local-disk store could not — this is what makes cross-node
 * metadata failover work.
 *
 * <p>The {@link StrataClient} is created lazily and points at this controller node's own SCP endpoint; the
 * controller routes the system namespace straight to the ZK root (see {@link NamespaceLogBackend}), so there
 * is no infinite recursion and no engine-lock re-entry.
 */
final class StrataSystemMetadataFileStore implements NamespaceMetadataFileStore {
    private static final int READ_CHUNK = 4 * 1024 * 1024;

    private final Supplier<String> metaEndpoint;
    private final StrataClient.WritePolicy policy;
    private final Object initLock = new Object();
    private volatile StrataClient client;
    private final Map<FileId, StrataFile.Appender> openLogAppenders = new ConcurrentHashMap<>();

    StrataSystemMetadataFileStore(Supplier<String> metaEndpoint, int replicationFactor, int ackQuorum,
                                  boolean fsyncOnAck) {
        this.metaEndpoint = metaEndpoint;
        // Durability is by REPLICATION, not single-node fsync: the metadata log is a Strata file with RF
        // replicas, acked once the ack quorum holds the record. fsyncOnAck=false (default) keeps it
        // page-cache durable on the quorum — fully safe against process/JVM/container crashes (the page
        // cache survives), and the integrity ledger + background writeback make it disk-durable shortly
        // after. This matches the data plane (STRATA_SEAL_FSYNC=false). fsyncOnAck=true
        // (STRATA_CONTROLLER_LOG_FSYNC) fsyncs every append on the ack quorum (design §15), which only
        // adds durability against a *correlated* power loss of the whole ack quorum before writeback — at
        // a large throughput cost when the log shares a disk with the data plane.
        this.policy = new StrataClient.WritePolicy(replicationFactor, ackQuorum, fsyncOnAck);
    }

    @Override
    public FileId createLogFile(StrataNamespace ns, long generation) {
        StrataFile file = client().create(systemFileSpec(ns, generation, "log"));
        openLogAppenders.put(file.id(), file.openForAppend());
        return file.id();
    }

    @Override
    public void appendLog(FileId logFileId, byte[] frameBytes) throws Exception {
        appenderFor(logFileId).append(ByteBuffer.wrap(frameBytes)).get(); // durable on ack quorum
    }

    @Override
    public byte[] readLog(FileId logFileId) throws Exception {
        // Recovery read: fence any previous writer and seal the open tail at the committed prefix (§7.3),
        // then read the sealed content. Drop our own appender first if we still hold one.
        closeQuietly(openLogAppenders.remove(logFileId));
        StrataFile file = client().openById(NamespaceLogBackend.SYSTEM_NAMESPACE, logFileId);
        file.recoverAndSeal();
        return readAll(file);
    }

    @Override
    public FileId writeSnapshot(StrataNamespace ns, long generation, byte[] snapshotBytes) throws Exception {
        StrataFile file = client().create(systemFileSpec(ns, generation, "snapshot"));
        try (StrataFile.Appender appender = file.openForAppend()) {
            if (snapshotBytes.length > 0) {
                appender.append(ByteBuffer.wrap(snapshotBytes)).get();
            }
            appender.seal();
        }
        return file.id();
    }

    @Override
    public byte[] readSnapshot(FileId snapshotFileId) throws Exception {
        return readAll(client().openById(NamespaceLogBackend.SYSTEM_NAMESPACE, snapshotFileId)); // sealed and immutable — no fencing needed
    }

    @Override
    public void deleteFile(FileId fileId) throws Exception {
        closeQuietly(openLogAppenders.remove(fileId));
        client().deleteById(NamespaceLogBackend.SYSTEM_NAMESPACE, fileId);
    }

    @Override
    public void close() {
        for (StrataFile.Appender appender : openLogAppenders.values()) {
            closeQuietly(appender);
        }
        openLogAppenders.clear();
        StrataClient c = client;
        if (c != null) {
            c.close();
        }
    }

    private StrataFile.Appender appenderFor(FileId logFileId) {
        return openLogAppenders.computeIfAbsent(logFileId,
                id -> client().openById(NamespaceLogBackend.SYSTEM_NAMESPACE, id).openForAppend());
    }

    private byte[] readAll(StrataFile file) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (StrataFile.Reader reader = file.openForRead()) {
            long offset = 0;
            while (true) {
                try (StrataFile.ReadResult result = reader.read(offset, READ_CHUNK)) {
                    int n = result.length();
                    if (n > 0) {
                        byte[] bytes = new byte[n];
                        result.buffer().get(bytes);
                        out.writeBytes(bytes);
                        offset += n;
                    }
                    if (n == 0 || result.endOfFile()) {
                        break;
                    }
                }
            }
        }
        return out.toByteArray();
    }

    private StrataClient.FileSpec systemFileSpec(StrataNamespace ns, long generation, String kind) {
        // Path encodes (ns, generation, kind) for human-readable traceability; the server assigns the FileId.
        // A unique token (UUID) is appended to the leaf so every create attempt uses a fresh path — retried
        // or raced compactions (same ns/generation/kind) therefore never collide at the ZK path level and
        // cannot trigger NodeExistsException. Readers always look up files by FileId via openById(), not by
        // path, so the unique leaf does not affect correctness. A retried/raced compaction leaves at most one
        // orphan snapshot+log; the failed-CAS case is cleaned up in-process by deleteQuietly, but an orphan
        // from a crash between file-create and manifest-CAS is not yet GC'd — deferred to orphan-GC work.
        return new StrataClient.FileSpec(NamespaceLogBackend.SYSTEM_NAMESPACE,
                StrataPath.of("/metadata-log/" + ns + "/gen-" + generation + "/" + kind + "-"
                        + UUID.randomUUID()), policy);
    }

    private StrataClient client() {
        StrataClient c = client;
        if (c == null) {
            synchronized (initLock) {
                if (client == null) {
                    client = StrataClient.connect(ClientConfig.of(metaEndpoint.get()));
                }
                c = client;
            }
        }
        return c;
    }

    private static void closeQuietly(StrataFile.Appender appender) {
        if (appender != null) {
            try {
                appender.close();
            } catch (RuntimeException ignore) {
                // best-effort
            }
        }
    }
}
