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
 * /strata/namespaces/<namespace>/paths/<path segments>/__file (FileId binding),
 * /strata/nodes/<nodeId>, /strata/ids/seq- (sequential, node-id allocation).
 */
public final class ZkMetadataStore implements MetadataStore {
    private static final String FILES = "/strata/files";
    private static final String NAMESPACES = "/strata/namespaces";
    private static final String NODES = "/strata/nodes";
    private static final String IDS = "/strata/ids";
    private static final String FILE_MARKER = "__file";

    private final CuratorFramework curator;
    private final boolean ownsCurator;

    public ZkMetadataStore(String zkConnect) {
        this(CuratorFrameworkFactory.newClient(zkConnect, new ExponentialBackoffRetry(100, 5)), true);
    }

    public ZkMetadataStore(CuratorFramework curator) {
        this(curator, false);
    }

    private ZkMetadataStore(CuratorFramework curator, boolean ownsCurator) {
        this.curator = curator;
        this.ownsCurator = ownsCurator;
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
            if (!curator.blockUntilConnected(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("zk connection timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while connecting to zk", e);
        }
    }

    private void init() {
        try {
            for (String path : new String[]{FILES, NAMESPACES, NODES, IDS}) {
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

    @Override
    public void createFile(Records.FileRecord record) throws Exception {
        ensureNamespaceDir(record.namespace(), record.path());
        List<CuratorOp> ops = List.of(
                curator.transactionOp().create().forPath(FILES + "/" + record.fileId(), record.encode()),
                curator.transactionOp().create()
                        .forPath(fileMarkerPath(record.namespace(), record.path()), fileIdBytes(record.fileId()))
        );
        curator.transaction().forOperations(ops);
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
        try {
            byte[] data = curator.getData().forPath(fileMarkerPath(namespace, path));
            return Optional.of(readFileId(data));
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        }
    }

    @Override
    public boolean updateFile(Records.FileRecord record, int expectedVersion) throws Exception {
        try {
            curator.setData().withVersion(expectedVersion).forPath(FILES + "/" + record.fileId(), record.encode());
            return true;
        } catch (KeeperException.BadVersionException e) {
            return false;
        }
    }

    @Override
    public boolean deletePath(StrataNamespace namespace, StrataPath path, FileId expectedFileId) throws Exception {
        try {
            byte[] data = curator.getData().forPath(fileMarkerPath(namespace, path));
            if (!readFileId(data).equals(expectedFileId)) {
                return false;
            }
            curator.delete().forPath(fileMarkerPath(namespace, path));
            return true;
        } catch (KeeperException.NoNodeException ignored) {
            return true;
        }
    }

    @Override
    public boolean deleteFile(FileId id, int expectedVersion) throws Exception {
        try {
            Optional<Versioned<Records.FileRecord>> current = getFile(id);
            if (current.isEmpty()) {
                return true;
            }
            if (current.get().version() != expectedVersion) {
                return false;
            }
            curator.delete().withVersion(expectedVersion).forPath(FILES + "/" + id);
            deletePath(current.get().value().namespace(), current.get().value().path(), id);
            return true;
        } catch (KeeperException.BadVersionException e) {
            return false;
        } catch (KeeperException.NoNodeException ignored) {
            return true;
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

    private static FileId readFileId(byte[] bytes) {
        if (bytes.length != 16) {
            throw new IllegalArgumentException("bad namespace file id length " + bytes.length);
        }
        return FileId.readFrom(ByteBuffer.wrap(bytes));
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
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException e) {
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
