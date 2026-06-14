package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * ZooKeeper backend (tech design §4.4, v0 only — development/prototype; retired at v1 GA).
 * Layout: /strata/files/<fileId> (FileRecord, CAS by znode version),
 * /strata/file-ids/<fileId> (permanent FileId reservation),
 * /strata/namespaces/<namespace>/paths/<path segments>/__file (FileId binding or empty tombstone),
 * /strata/nodes/<nodeId>, /strata/ids/seq- (sequential, node-id allocation).
 */
public final class ZkMetadataStore implements MetadataStore {
    private static final String FILES = "/strata/files";
    private static final String FILE_IDS = "/strata/file-ids";
    private static final String NAMESPACES = "/strata/namespaces";
    private static final String NODES = "/strata/nodes";
    private static final String IDS = "/strata/ids";
    private static final String FILE_MARKER = "__file";
    private static final byte[] DELETED_FILE_MARKER = new byte[0];

    private final CuratorFramework curator;
    private final boolean ownsCurator;
    private final int connectionTimeoutMs;

    public ZkMetadataStore(String zkConnect) {
        this(zkConnect, 60_000, 15_000);
    }

    public ZkMetadataStore(String zkConnect, int sessionTimeoutMs, int connectionTimeoutMs) {
        this(CuratorFrameworkFactory.builder()
                .connectString(zkConnect)
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                .retryPolicy(new ExponentialBackoffRetry(100, 5))
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
            for (String path : new String[]{FILES, FILE_IDS, NAMESPACES, NODES, IDS}) {
                try {
                    if (curator.checkExists().forPath(path) == null) {
                        curator.create().creatingParentsIfNeeded().forPath(path);
                    }
                } catch (KeeperException.NodeExistsException ignored) {
                    // Another metadata service won the create race for this root; continue
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
        String markerPath = fileMarkerPath(record.namespace(), record.path());
        Optional<PathMarker> marker = readPathMarker(record.namespace(), record.path());
        if (marker.isPresent() && marker.get().fileId().isPresent()) {
            throw new KeeperException.NodeExistsException(markerPath);
        }
        List<CuratorOp> ops = new ArrayList<>();
        ops.add(curator.transactionOp().create().forPath(FILE_IDS + "/" + record.fileId(), new byte[0]));
        ops.add(curator.transactionOp().create().forPath(FILES + "/" + record.fileId(), record.encode()));
        if (marker.isPresent()) {
            ops.add(curator.transactionOp().setData().withVersion(marker.get().version())
                    .forPath(markerPath, fileIdBytes(record.fileId())));
        } else {
            ops.add(curator.transactionOp().create().forPath(markerPath, fileIdBytes(record.fileId())));
        }
        try {
            curator.transaction().forOperations(ops);
        } catch (KeeperException.BadVersionException e) {
            throw new KeeperException.NodeExistsException(markerPath);
        }
    }

    @Override
    public Optional<Versioned<Records.FileRecord>> getFile(FileId id) throws Exception {
        try {
            Stat stat = new Stat();
            byte[] data = curator.getData().storingStatIn(stat).forPath(FILES + "/" + id);
            return Optional.of(new Versioned<>(Records.FileRecord.decode(data), stat.getVersion()));
        } catch (KeeperException.NoNodeException e) {
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
            curator.setData().withVersion(expectedVersion).forPath(FILES + "/" + record.fileId(), record.encode());
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
            return true;
        } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
            return pathNoLongerBinds(namespace, path, expectedFileId);
        }
    }

    @Override
    public boolean deleteFile(FileId id, int expectedVersion) throws Exception {
        String filePath = FILES + "/" + id;
        try {
            Optional<Versioned<Records.FileRecord>> current = getFile(id);
            if (current.isEmpty()) {
                return true;
            }
            if (current.get().version() != expectedVersion) {
                return false;
            }
            Records.FileRecord record = current.get().value();
            Optional<PathMarker> marker = readPathMarker(record.namespace(), record.path());
            if (marker.isPresent() && marker.get().fileId().map(id::equals).orElse(false)) {
                List<CuratorOp> ops = List.of(
                        curator.transactionOp().delete().withVersion(expectedVersion).forPath(filePath),
                        curator.transactionOp().setData().withVersion(marker.get().version())
                                .forPath(fileMarkerPath(record.namespace(), record.path()), DELETED_FILE_MARKER)
                );
                curator.transaction().forOperations(ops);
            } else {
                curator.delete().withVersion(expectedVersion).forPath(filePath);
            }
            return true;
        } catch (KeeperException.BadVersionException e) {
            return false;
        } catch (KeeperException.NoNodeException ignored) {
            return getFile(id).isEmpty();
        }
    }

    @Override
    public List<FileId> listFiles() throws Exception {
        List<FileId> out = new ArrayList<>();
        for (String child : curator.getChildren().forPath(FILES)) {
            out.add(FileId.fromString(child));
        }
        return out;
    }

    private void ensureNamespaceDir(StrataNamespace namespace, StrataPath path) throws Exception {
        try {
            curator.create().creatingParentsIfNeeded().forPath(namespaceDirPath(namespace, path));
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

    private static byte[] fileIdBytes(FileId id) {
        ByteBuffer buf = ByteBuffer.allocate(16);
        id.writeTo(buf);
        return buf.array();
    }

    private static Optional<FileId> readFileId(byte[] bytes) {
        if (bytes.length == 0) {
            return Optional.empty();
        }
        if (bytes.length != 16) {
            throw new IllegalArgumentException("bad namespace file id length " + bytes.length);
        }
        return Optional.of(FileId.readFrom(ByteBuffer.wrap(bytes)));
    }

    private Optional<PathMarker> readPathMarker(StrataNamespace namespace, StrataPath path) throws Exception {
        try {
            Stat stat = new Stat();
            byte[] data = curator.getData().storingStatIn(stat).forPath(fileMarkerPath(namespace, path));
            return Optional.of(new PathMarker(readFileId(data), stat.getVersion()));
        } catch (KeeperException.NoNodeException e) {
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
    public int nextNodeId() throws Exception {
        String path = curator.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(IDS + "/seq-");
        String seq = path.substring(path.lastIndexOf('-') + 1);
        return Integer.parseInt(seq) + 1; // node ids start at 1
    }

    @Override
    public boolean putNode(Records.NodeRecord record, int expectedVersion) throws Exception {
        String path = NODES + "/" + record.nodeId();
        try {
            if (expectedVersion < 0) {
                curator.create().forPath(path, record.encode());
            } else {
                curator.setData().withVersion(expectedVersion).forPath(path, record.encode());
            }
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
            return Optional.of(new Versioned<>(Records.NodeRecord.decode(data), stat.getVersion()));
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<Versioned<Records.NodeRecord>> listNodes() throws Exception {
        List<Versioned<Records.NodeRecord>> out = new ArrayList<>();
        for (String child : curator.getChildren().forPath(NODES)) {
            getNode(Integer.parseInt(child)).ifPresent(out::add);
        }
        return out;
    }

    @Override
    public void close() {
        if (ownsCurator) {
            curator.close();
        }
    }
}
