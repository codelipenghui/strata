package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * ZooKeeper backend (tech design §4.4, v0 only — development/prototype; retired at v1 GA).
 * Layout: /strata/files/<fileId> (FileRecord, CAS by znode version; a deleted file is left as a
 * DELETED tombstone that fences a replayed CREATE until the sweeper reaps it),
 * /strata/namespaces/<namespace>/paths/<path segments>/__file (sealed FileId binding or sealed tombstone),
 * /strata/namespaces/<namespace>/files/<fileId> (per-namespace file index for namespace-scoped
 * enumeration without a global /strata/files scan),
 * /strata/nodes/<nodeId> (node identity record; ids are externally supplied via STRATA_NODE_ID).
 */
public final class ZkMetadataStore implements MetadataStore {
    private static final Logger log = LoggerFactory.getLogger(ZkMetadataStore.class);
    private static final String FILES = "/strata/files";
    private static final String NAMESPACES = "/strata/namespaces";
    private static final String NODES = "/strata/nodes";
    private static final String FILE_MARKER = "__file";
    private static final byte PATH_MARKER_TOMBSTONE = 0;
    private static final byte PATH_MARKER_FILE_ID = 1;
    private static final byte[] DELETED_FILE_MARKER = pathMarkerBytes(Optional.empty());
    // Namespace-sharding consensus root (design §5, §6.1): a single global metadata-epoch counter
    // and one assignment record per namespace. Distinct from /strata/namespaces (user path/file
    // bindings) so the two never collide.
    private static final String META = "/strata/meta";
    private static final String META_NAMESPACES = META + "/namespaces";
    private static final String META_EPOCH = META + "/epoch";
    private static final String META_LIVE_NODES = META + "/live-nodes";
    // Low-volume CAS counter for the system namespace's file ids (a handful of meta-log segments).
    private static final String META_SYS_FILE_ID = META + "/sys-file-id";
    // Global monotonic file-id counter for the ZK backend. File ids are globally unique in ZK
    // because all file records live under a shared /strata/files/<id> flat space.
    private static final String META_GLOBAL_FILE_ID = META + "/global-file-id";
    /** The next-level znodes under {@code /strata}, used to tag per-subtree ZK request metrics. */
    public static final List<String> SUBTREES = List.of("files", "namespaces", "nodes");

    private final CuratorFramework curator;
    private final boolean ownsCurator;
    private final int connectionTimeoutMs;
    // Per-subtree ZK request/byte counters (read vs write), populated on the I/O path and read back
    // by ServerMetrics as monotonic function-counters — pure accounting, never affects control flow.
    private final Map<String, Counters> zkStats = newStats();

    public ZkMetadataStore(String zkConnect) {
        this(zkConnect, 60_000, 15_000);
    }

    public ZkMetadataStore(String zkConnect, int sessionTimeoutMs, int connectionTimeoutMs) {
        this(zkConnect, sessionTimeoutMs, connectionTimeoutMs, 100, 5);
    }

    public ZkMetadataStore(String zkConnect, int sessionTimeoutMs, int connectionTimeoutMs,
                           int retryBaseMs, int retryMaxRetries) {
        this(CuratorFrameworkFactory.builder()
                .connectString(zkConnect)
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                .retryPolicy(new ExponentialBackoffRetry(retryBaseMs, retryMaxRetries))
                .build(), true, connectionTimeoutMs);
    }

    public ZkMetadataStore(CuratorFramework curator) {
        this(curator, false, 10_000);
    }

    private ZkMetadataStore(CuratorFramework curator, boolean ownsCurator, int connectionTimeoutMs) {
        this.curator = curator;
        this.ownsCurator = ownsCurator;
        this.connectionTimeoutMs = connectionTimeoutMs;
        boolean initialized = false;
        try {
            if (ownsCurator) {
                this.curator.start();
            }
            awaitConnected();
            init();
            initialized = true;
        } finally {
            if (ownsCurator && !initialized) {
                this.curator.close();
            }
        }
    }

    private void awaitConnected() {
        try {
            if (!curator.blockUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("zk connection timed out after "
                        + connectionTimeoutMs + "ms");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while connecting to zk", e);
        }
    }

    private void init() {
        try {
            for (String path : new String[]{FILES, NAMESPACES, NODES, META, META_NAMESPACES}) {
                try {
                    if (curator.checkExists().forPath(path) == null) {
                        curator.create().creatingParentsIfNeeded().forPath(path);
                    }
                } catch (KeeperException.NodeExistsException ignored) {
                    // Another controller won the create race for this root; continue
                    // initializing the rest of the namespace.
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("zk init failed", e);
        }
    }

    public CuratorFramework curator() {
        return curator;
    }

    /** Whether the ZooKeeper connection is currently live (a LOST connection freezes the control plane). */
    public boolean isConnected() {
        return curator.getZookeeperClient().isConnected();
    }

    @Override
    public void createFile(Records.FileRecord record) throws Exception {
        ensureNamespaceDir(record.namespace(), record.path());
        ensureNamespaceFilesDir(record.namespace());
        String markerPath = fileMarkerPath(record.namespace(), record.path());
        Optional<PathMarker> marker = readPathMarker(record.namespace(), record.path());
        if (marker.isPresent() && marker.get().fileId().isPresent()) {
            throw new KeeperException.NodeExistsException(markerPath);
        }
        byte[] enc = record.encode();
        byte[] idBytes = fileIdBytes(record.fileId());
        List<CuratorOp> ops = new ArrayList<>();
        ops.add(curator.transactionOp().create().forPath(FILES + "/" + record.fileId(), enc));
        if (marker.isPresent()) {
            ops.add(curator.transactionOp().setData().withVersion(marker.get().version())
                    .forPath(markerPath, idBytes));
        } else {
            ops.add(curator.transactionOp().create().forPath(markerPath, idBytes));
        }
        // Per-namespace file index, atomic with the file create, so files can be enumerated per
        // namespace without a global /strata/files scan.
        ops.add(curator.transactionOp().create()
                .forPath(namespaceFileIndexPath(record.namespace(), record.fileId())));
        try {
            curator.transaction().forOperations(ops);
            record(FILES, true, enc.length);
            record(markerPath, true, idBytes.length);
            record(NAMESPACES, true, 0);
        } catch (KeeperException.BadVersionException e) {
            throw new KeeperException.NodeExistsException(markerPath);
        }
    }

    @Override
    public Optional<Versioned<Records.FileRecord>> getFile(StrataNamespace namespace, FileId id) throws Exception {
        // ZK backend: file ids are globally unique (all records live under /strata/files/<id>);
        // the namespace arg satisfies the interface contract but does not change the lookup.
        try {
            Stat stat = new Stat();
            byte[] data = curator.getData().storingStatIn(stat).forPath(FILES + "/" + id);
            record(FILES, false, data.length);
            Records.FileRecord record = Records.FileRecord.decode(data);
            if (record.state() == FileState.DELETED) {
                return Optional.empty();  // a swept-pending tombstone is logically gone
            }
            return Optional.of(new Versioned<>(record, stat.getVersion()));
        } catch (KeeperException.NoNodeException e) {
            record(FILES, false, 0);
            return Optional.empty();
        }
    }

    @Override
    public Optional<FileId> resolvePath(StrataNamespace namespace, StrataPath path) throws Exception {
        Optional<PathMarker> marker = readPathMarker(namespace, path);
        if (marker.isEmpty() || marker.get().fileId().isEmpty()) {
            return Optional.empty();
        }
        return marker.get().fileId();
    }

    @Override
    public boolean updateFile(Records.FileRecord record, int expectedVersion) throws Exception {
        try {
            byte[] enc = record.encode();
            curator.setData().withVersion(expectedVersion).forPath(FILES + "/" + record.fileId(), enc);
            record(FILES, true, enc.length);
            return true;
        } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
            return false;
        }
    }

    @Override
    public boolean deletePath(StrataNamespace namespace, StrataPath path, FileId expectedFileId) throws Exception {
        Optional<PathMarker> marker = readPathMarker(namespace, path);
        if (marker.isEmpty() || marker.get().fileId().isEmpty()) {
            return true;
        }
        if (!marker.get().fileId().get().equals(expectedFileId)) {
            return false;
        }
        try {
            curator.setData().withVersion(marker.get().version())
                .forPath(fileMarkerPath(namespace, path), DELETED_FILE_MARKER);
            record(NAMESPACES, true, DELETED_FILE_MARKER.length);
            return true;
        } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
            return pathNoLongerBinds(namespace, path, expectedFileId);
        }
    }

    @Override
    public boolean deleteFile(StrataNamespace namespace, FileId id, int expectedVersion) throws Exception {
        // ZK backend: file ids are globally unique; the namespace arg satisfies the interface
        // contract but does not change the lookup — the record's own namespace() is used below.
        String filePath = FILES + "/" + id;
        try {
            Optional<Versioned<Records.FileRecord>> current = getFile(namespace, id);
            if (current.isEmpty()) {
                return true;
            }
            if (current.get().version() != expectedVersion) {
                return false;
            }
            Records.FileRecord record = current.get().value();
            // Leave a DELETED tombstone in place of the record (a swept-later id reservation) so a
            // replayed CREATE for this id still collides on the existing znode; the sweeper reaps it.
            byte[] tombstone = record.withState(FileState.DELETED).encode();
            Optional<PathMarker> marker = readPathMarker(record.namespace(), record.path());
            if (marker.isPresent() && marker.get().fileId().map(id::equals).orElse(false)) {
                List<CuratorOp> ops = List.of(
                        curator.transactionOp().setData().withVersion(expectedVersion).forPath(filePath, tombstone),
                        curator.transactionOp().setData().withVersion(marker.get().version())
                                .forPath(fileMarkerPath(record.namespace(), record.path()), DELETED_FILE_MARKER)
                );
                curator.transaction().forOperations(ops);
                record(FILES, true, tombstone.length);
                record(NAMESPACES, true, DELETED_FILE_MARKER.length);
            } else {
                curator.setData().withVersion(expectedVersion).forPath(filePath, tombstone);
                record(FILES, true, tombstone.length);
            }
            // Keep the per-namespace index entry until the sweeper reaps the tombstone (sweepDeletedFiles
            // drops it): listFilesIncludingTombstones — the controller's opId-reuse scan — reads that index,
            // so an entry dropped here would let a stale CREATE replay miss the unswept tombstone and
            // resurrect the deleted file under a fresh id. listFiles filters the tombstone via getFile, so the
            // retained entry never leaks into a normal namespace listing.
            return true;
        } catch (KeeperException.BadVersionException e) {
            return false;
        } catch (KeeperException.NoNodeException ignored) {
            return getFile(namespace, id).isEmpty();
        }
    }

    @Override
    public List<FileId> listFiles(StrataNamespace namespace) throws Exception {
        List<FileId> out = new ArrayList<>();
        List<String> children;
        try {
            children = curator.getChildren().forPath(namespaceFilesDir(namespace));
        } catch (KeeperException.NoNodeException e) {
            return out;  // namespace never had any files
        }
        record(NAMESPACES, false, 0);
        for (String child : children) {
            FileId id = FileId.fromHex(child);
            if (getFile(namespace, id).isPresent()) {  // skip DELETED tombstones (index entry not yet swept)
                out.add(id);
            }
        }
        return out;
    }

    /**
     * All file ids for the namespace, including DELETED tombstone records that have not yet been swept.
     * Used by the controller to detect opId reuse after deletion (prevents stale-replay resurrection).
     */
    List<FileId> listFilesIncludingTombstones(StrataNamespace namespace) throws Exception {
        List<String> children;
        try {
            children = curator.getChildren().forPath(namespaceFilesDir(namespace));
        } catch (KeeperException.NoNodeException e) {
            return List.of();
        }
        record(NAMESPACES, false, 0);
        List<FileId> out = new ArrayList<>(children.size());
        for (String child : children) {
            out.add(FileId.fromHex(child));
        }
        return out;
    }

    /**
     * Children-only enumeration for the per-namespace reconcile/verify sweep: returns every index entry
     * (including not-yet-swept DELETED tombstones) WITHOUT reading a single file record. The repair loops
     * re-read each id via {@link #getFile} (which hides tombstones) and skip the empties, so this avoids
     * the read amplification of {@link #listFiles} (one record read per file to filter tombstones) followed
     * by the loop's second read of each survivor — the dominant idle {@code /strata/files} read load.
     */
    @Override
    public List<FileId> listFileIds(StrataNamespace namespace) throws Exception {
        return listFilesIncludingTombstones(namespace);
    }

    /**
     * True if {@code namespace} has at least one live (non-tombstone) file. Short-circuits on the first
     * live record so {@link #listNamespaces}'s non-emptiness probe reads at most one record per namespace
     * instead of every record — it runs on every controller on every repair/verify tick.
     */
    private boolean hasLiveFile(StrataNamespace namespace) throws Exception {
        List<String> children;
        try {
            children = curator.getChildren().forPath(namespaceFilesDir(namespace));
        } catch (KeeperException.NoNodeException e) {
            return false;
        }
        record(NAMESPACES, false, 0);
        for (String child : children) {
            if (getFile(namespace, FileId.fromHex(child)).isPresent()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Fetches a file record including DELETED tombstone records. Used by the controller's opId-reuse
     * scan to detect tombstoned files carrying a previously-used opId.
     */
    Optional<Records.FileRecord> getFileIncludingTombstone(FileId id) throws Exception {
        try {
            byte[] data = curator.getData().forPath(FILES + "/" + id);
            record(FILES, false, data.length);
            return Optional.of(Records.FileRecord.decode(data));
        } catch (KeeperException.NoNodeException e) {
            record(FILES, false, 0);
            return Optional.empty();
        }
    }

    @Override
    public List<StrataNamespace> listNamespaces() throws Exception {
        List<StrataNamespace> out = new ArrayList<>();
        List<String> children;
        try {
            children = curator.getChildren().forPath(NAMESPACES);
        } catch (KeeperException.NoNodeException e) {
            return out;
        }
        record(NAMESPACES, false, 0);
        for (String child : children) {
            StrataNamespace ns = StrataNamespace.of(child);
            // a namespace znode persists after its files are gone; only report namespaces with live files
            if (hasLiveFile(ns)) {
                out.add(ns);
            }
        }
        return out;
    }

    @Override
    public int sweepDeletedFiles(long olderThanMs) throws Exception {
        // Ages tombstones by the znode mtime (set when deleteFile wrote DELETED). Relies on a DELETED
        // znode never being mutated again (every writer goes through getFile, which hides it), so
        // mtime == delete time. The controller clock (now) is compared to the ZK-server mtime, so olderThanMs
        // must absorb that skew — see ControllerConfig.deletedTombstoneTtlMs.
        long now = System.currentTimeMillis();
        int reaped = 0;
        List<String> children = curator.getChildren().forPath(FILES);
        record(FILES, false, 0);
        for (String child : children) {
            String path = FILES + "/" + child;
            Stat stat = new Stat();
            byte[] data;
            try {
                data = curator.getData().storingStatIn(stat).forPath(path);
                record(FILES, false, data.length);
            } catch (KeeperException.NoNodeException e) {
                continue;  // reaped concurrently
            }
            Records.FileRecord decoded = Records.FileRecord.decode(data);
            if (decoded.state() != FileState.DELETED
                    || now - stat.getMtime() < olderThanMs) {
                continue;  // live/DELETING, or still inside the fencing window
            }
            try {
                curator.delete().withVersion(stat.getVersion()).forPath(path);
                record(FILES, true, 0);
                // also drop any lingering per-namespace index entry (e.g. if deleteFile's best-effort
                // cleanup didn't run); harmless if already gone
                dropNamespaceFileIndex(decoded.namespace(), FileId.fromHex(child));
                reaped++;
            } catch (KeeperException.BadVersionException | KeeperException.NoNodeException ignored) {
                // raced with another mutation/sweeper; leave it for the next pass
            }
        }
        return reaped;
    }

    private void ensureNamespaceDir(StrataNamespace namespace, StrataPath path) throws Exception {
        try {
            curator.create().creatingParentsIfNeeded().forPath(namespaceDirPath(namespace, path));
            record(NAMESPACES, true, 0);
        } catch (KeeperException.NodeExistsException ignored) {
            // The directory already exists, possibly because another file is in the same parent.
        }
    }

    private static String namespaceDirPath(StrataNamespace namespace, StrataPath path) {
        return NAMESPACES + "/" + namespace + "/paths" + path;
    }

    private static String fileMarkerPath(StrataNamespace namespace, StrataPath path) {
        return namespaceDirPath(namespace, path) + "/" + FILE_MARKER;
    }

    private static String namespaceFilesDir(StrataNamespace namespace) {
        return NAMESPACES + "/" + namespace + "/files";
    }

    private static String namespaceFileIndexPath(StrataNamespace namespace, FileId id) {
        return namespaceFilesDir(namespace) + "/" + id;
    }

    private void ensureNamespaceFilesDir(StrataNamespace namespace) throws Exception {
        try {
            curator.create().creatingParentsIfNeeded().forPath(namespaceFilesDir(namespace));
            record(NAMESPACES, true, 0);
        } catch (KeeperException.NodeExistsException ignored) {
            // already present (another file in this namespace created it)
        }
    }

    /**
     * Best-effort removal of the per-namespace file index entry. The entry is only an enumeration
     * optimization — {@link #listFiles(StrataNamespace)} filters tombstones via {@link #getFile} — so a
     * transient failure self-heals on the next delete/sweep rather than blocking the logical delete.
     */
    private void dropNamespaceFileIndex(StrataNamespace namespace, FileId id) {
        try {
            curator.delete().forPath(namespaceFileIndexPath(namespace, id));
            record(NAMESPACES, true, 0);
        } catch (KeeperException.NoNodeException ignored) {
            // already gone
        } catch (Exception e) {
            log.warn("failed to drop namespace file index {}/{} (will self-heal on sweep)", namespace, id, e);
        }
    }

    private static byte[] fileIdBytes(FileId id) {
        return pathMarkerBytes(Optional.of(id));
    }

    private static byte[] pathMarkerBytes(Optional<FileId> id) {
        if (id.isEmpty()) {
            return Records.sealRecord(new byte[] {PATH_MARKER_TOMBSTONE});
        }
        ByteBuffer buf = ByteBuffer.allocate(1 + 8);
        buf.put(PATH_MARKER_FILE_ID);
        id.get().writeTo(buf);
        return Records.sealRecord(buf.array());
    }

    private static Optional<FileId> readFileId(byte[] bytes) {
        byte[] body = Records.openRecord(bytes);
        if (body.length == 1 && body[0] == PATH_MARKER_TOMBSTONE) {
            return Optional.empty();
        }
        if (body.length != 1 + 8 || body[0] != PATH_MARKER_FILE_ID) {
            throw new IllegalArgumentException("bad namespace file marker envelope length " + body.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(body);
        buf.get();
        return Optional.of(FileId.readFrom(buf));
    }

    private static byte[] zkCounterBytes(long value) {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(value);
        return Records.sealRecord(buf.array());
    }

    private static long readZkCounter(String zkPath, byte[] bytes) {
        byte[] body = Records.openRecord(bytes);
        if (body.length != 8) {
            throw new IllegalArgumentException("bad zk counter envelope length " + body.length + " at " + zkPath);
        }
        return ByteBuffer.wrap(body).getLong();
    }

    private Optional<PathMarker> readPathMarker(StrataNamespace namespace, StrataPath path) throws Exception {
        try {
            Stat stat = new Stat();
            byte[] data = curator.getData().storingStatIn(stat).forPath(fileMarkerPath(namespace, path));
            record(NAMESPACES, false, data.length);
            return Optional.of(new PathMarker(readFileId(data), stat.getVersion()));
        } catch (KeeperException.NoNodeException e) {
            record(NAMESPACES, false, 0);
            return Optional.empty();
        }
    }

    private boolean pathNoLongerBinds(StrataNamespace namespace, StrataPath path, FileId expectedFileId)
            throws Exception {
        Optional<PathMarker> latest = readPathMarker(namespace, path);
        return latest.isEmpty()
                || latest.get().fileId().isEmpty()
                || !latest.get().fileId().get().equals(expectedFileId);
    }

    private record PathMarker(Optional<FileId> fileId, int version) {
    }

    @Override
    public boolean putNode(Records.NodeRecord record, int expectedVersion) throws Exception {
        String path = NODES + "/" + record.nodeId();
        try {
            byte[] enc = record.encode();
            if (expectedVersion < 0) {
                curator.create().forPath(path, enc);
            } else {
                curator.setData().withVersion(expectedVersion).forPath(path, enc);
            }
            record(NODES, true, enc.length);
            return true;
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException
                 | KeeperException.NoNodeException e) {
            return false;
        }
    }

    @Override
    public Optional<Versioned<Records.NodeRecord>> getNode(int nodeId) throws Exception {
        try {
            Stat stat = new Stat();
            byte[] data = curator.getData().storingStatIn(stat).forPath(NODES + "/" + nodeId);
            record(NODES, false, data.length);
            return Optional.of(new Versioned<>(Records.NodeRecord.decode(data), stat.getVersion()));
        } catch (KeeperException.NoNodeException e) {
            record(NODES, false, 0);
            return Optional.empty();
        }
    }

    @Override
    public List<Versioned<Records.NodeRecord>> listNodes() throws Exception {
        List<Versioned<Records.NodeRecord>> out = new ArrayList<>();
        List<String> children = curator.getChildren().forPath(NODES);
        record(NODES, false, 0);
        for (String child : children) {
            getNode(Integer.parseInt(child)).ifPresent(out::add);
        }
        return out;
    }

    /**
     * Atomically increments the long counter stored at {@code zkPath}, creating the znode at 1 if
     * it does not yet exist. Uses a CAS-with-retry loop: on BadVersion the read-modify-write is
     * retried; on NoNode the znode is created at 1 (and if another writer wins the create race,
     * the loop retries the CAS).
     */
    private long casIncrementZkCounter(String zkPath) throws Exception {
        while (true) {
            try {
                Stat stat = new Stat();
                byte[] data = curator.getData().storingStatIn(stat).forPath(zkPath);
                long current = readZkCounter(zkPath, data);
                long next = current + 1;
                try {
                    curator.setData().withVersion(stat.getVersion())
                            .forPath(zkPath, zkCounterBytes(next));
                    return next;
                } catch (KeeperException.BadVersionException retry) {
                    // lost the CAS race; re-read and try again
                }
            } catch (KeeperException.NoNodeException e) {
                try {
                    curator.create().creatingParentsIfNeeded()
                            .forPath(zkPath, zkCounterBytes(1L));
                    return 1L;
                } catch (KeeperException.NodeExistsException created) {
                    // another writer created it first; loop to CAS-increment off its value
                }
            }
        }
    }

    @Override
    public long allocateMetadataEpoch() throws Exception {
        return casIncrementZkCounter(META_EPOCH);
    }

    @Override
    public long nextSystemFileId() throws Exception {
        return casIncrementZkCounter(META_SYS_FILE_ID);
    }

    @Override
    public Optional<Versioned<Records.NamespaceAssignment>> getNamespaceAssignment(StrataNamespace namespace)
            throws Exception {
        try {
            Stat stat = new Stat();
            byte[] data = curator.getData().storingStatIn(stat).forPath(assignmentPath(namespace));
            return Optional.of(new Versioned<>(Records.NamespaceAssignment.decode(data), stat.getVersion()));
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        }
    }

    @Override
    public boolean putNamespaceAssignment(Records.NamespaceAssignment assignment, int expectedVersion)
            throws Exception {
        return casCreateOrSet(assignmentPath(assignment.namespace()), assignment.encode(), expectedVersion)
                .isPresent();
    }

    /**
     * Creates the znode ({@code expectedVersion < 0}) or CAS-sets it at the expected version; returns the
     * new znode data-version on success, or empty on a create/version conflict (node already exists, stale
     * version, or vanished). Returning the version lets the caller chain the next CAS without a read-back.
     */
    private OptionalInt casCreateOrSet(String path, byte[] enc, int expectedVersion) throws Exception {
        try {
            if (expectedVersion < 0) {
                curator.create().creatingParentsIfNeeded().forPath(path, enc);
                return OptionalInt.of(0); // a freshly created znode has data version 0
            }
            Stat stat = curator.setData().withVersion(expectedVersion).forPath(path, enc);
            return OptionalInt.of(stat.getVersion());
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException
                 | KeeperException.NoNodeException e) {
            return OptionalInt.empty();
        }
    }

    @Override
    public List<StrataNamespace> listAssignedNamespaces() throws Exception {
        try {
            List<String> children = curator.getChildren().forPath(META_NAMESPACES);
            List<StrataNamespace> out = new ArrayList<>(children.size());
            for (String child : children) {
                out.add(StrataNamespace.of(child));
            }
            return out;
        } catch (KeeperException.NoNodeException e) {
            return List.of();
        }
    }

    private static String assignmentPath(StrataNamespace namespace) {
        return META_NAMESPACES + "/" + namespace + "/assignment";
    }

    @Override
    public Optional<Versioned<Records.NamespaceManifest>> getNamespaceManifest(StrataNamespace namespace)
            throws Exception {
        try {
            Stat stat = new Stat();
            byte[] data = curator.getData().storingStatIn(stat).forPath(manifestPath(namespace));
            return Optional.of(new Versioned<>(Records.NamespaceManifest.decode(data), stat.getVersion()));
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        }
    }

    @Override
    public OptionalInt putNamespaceManifest(Records.NamespaceManifest manifest, int expectedVersion)
            throws Exception {
        return casCreateOrSet(manifestPath(manifest.namespace()), manifest.encode(), expectedVersion);
    }

    private static String manifestPath(StrataNamespace namespace) {
        return META_NAMESPACES + "/" + namespace + "/manifest";
    }

    @Override
    public void putClusterLiveNodes(byte[] snapshot) throws Exception {
        try {
            curator.setData().forPath(META_LIVE_NODES, snapshot);
        } catch (KeeperException.NoNodeException e) {
            try {
                curator.create().creatingParentsIfNeeded().forPath(META_LIVE_NODES, snapshot);
            } catch (KeeperException.NodeExistsException created) {
                curator.setData().forPath(META_LIVE_NODES, snapshot);
            }
        }
    }

    @Override
    public Optional<byte[]> getClusterLiveNodes() throws Exception {
        try {
            return Optional.of(curator.getData().forPath(META_LIVE_NODES));
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        }
    }

    /** Read-only: ZooKeeper requests issued against a {@code /strata} subtree, by op kind. */
    public long zkOps(String subtree, boolean write) {
        Counters c = zkStats.get(subtree);
        return c == null ? 0 : (write ? c.writeOps : c.readOps).sum();
    }

    /** Read-only: ZooKeeper payload bytes read/written against a {@code /strata} subtree, by op kind. */
    public long zkBytes(String subtree, boolean write) {
        Counters c = zkStats.get(subtree);
        return c == null ? 0 : (write ? c.writeBytes : c.readBytes).sum();
    }

    /** Attribute one ZK op (and its payload bytes) to the {@code /strata} subtree of {@code path}. */
    private void record(String path, boolean write, int bytes) {
        Counters c = zkStats.get(subtreeOf(path));
        if (c == null) {
            return;
        }
        if (write) {
            c.writeOps.increment();
            if (bytes > 0) {
                c.writeBytes.add(bytes);
            }
        } else {
            c.readOps.increment();
            if (bytes > 0) {
                c.readBytes.add(bytes);
            }
        }
    }

    private static String subtreeOf(String path) {
        if (path.startsWith(FILES)) {
            return "files";
        }
        if (path.startsWith(NAMESPACES)) {
            return "namespaces";
        }
        if (path.startsWith(NODES)) {
            return "nodes";
        }
        return "other";
    }

    private static Map<String, Counters> newStats() {
        Map<String, Counters> m = new ConcurrentHashMap<>();
        for (String s : SUBTREES) {
            m.put(s, new Counters());
        }
        m.put("other", new Counters());
        return m;
    }

    private static final class Counters {
        final LongAdder readOps = new LongAdder();
        final LongAdder readBytes = new LongAdder();
        final LongAdder writeOps = new LongAdder();
        final LongAdder writeBytes = new LongAdder();
    }

    /**
     * Assigns a globally-unique file id for the ZK backend. The ZK file record space is flat
     * ({@code /strata/files/<id>}), so ids must be globally unique across all namespaces.
     * Uses a single global CAS counter rather than per-namespace counters.
     */
    @Override
    public FileId assignFileId(StrataNamespace namespace) throws Exception {
        return FileId.of(nextGlobalFileId());
    }

    /**
     * Global monotonic file-id counter. The ZK backend stores all file records under the shared
     * {@code /strata/files/<id>} flat space, so ids must be globally unique regardless of namespace.
     */
    private long nextGlobalFileId() throws Exception {
        return casIncrementZkCounter(META_GLOBAL_FILE_ID);
    }

    @Override
    public void close() {
        if (ownsCurator) {
            curator.close();
        }
    }
}
