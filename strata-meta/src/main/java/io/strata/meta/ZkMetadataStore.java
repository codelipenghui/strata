package io.strata.meta;

import io.strata.common.FileId;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * ZooKeeper backend (tech design §4.4, v0 only — development/prototype; retired at v1 GA).
 * Layout: /strata/files/<fileId> (FileRecord, CAS by znode version), /strata/nodes/<nodeId>,
 * /strata/ids/seq- (sequential, node-id allocation).
 */
public final class ZkMetadataStore implements MetadataStore {
    private static final String FILES = "/strata/files";
    private static final String NODES = "/strata/nodes";
    private static final String IDS = "/strata/ids";

    private final CuratorFramework curator;
    private final boolean ownsCurator;

    public ZkMetadataStore(String zkConnect) {
        this.curator = CuratorFrameworkFactory.newClient(zkConnect, new ExponentialBackoffRetry(100, 5));
        this.curator.start();
        this.ownsCurator = true;
        init();
    }

    public ZkMetadataStore(CuratorFramework curator) {
        this.curator = curator;
        this.ownsCurator = false;
        init();
    }

    private void init() {
        try {
            for (String path : new String[]{FILES, NODES, IDS}) {
                if (curator.checkExists().forPath(path) == null) {
                    curator.create().creatingParentsIfNeeded().forPath(path);
                }
            }
        } catch (KeeperException.NodeExistsException ignored) {
        } catch (Exception e) {
            throw new IllegalStateException("zk init failed", e);
        }
    }

    public CuratorFramework curator() {
        return curator;
    }

    @Override
    public void createFile(Records.FileRecord record) throws Exception {
        curator.create().forPath(FILES + "/" + record.fileId(), record.encode());
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
    public boolean updateFile(Records.FileRecord record, int expectedVersion) throws Exception {
        try {
            curator.setData().withVersion(expectedVersion).forPath(FILES + "/" + record.fileId(), record.encode());
            return true;
        } catch (KeeperException.BadVersionException e) {
            return false;
        }
    }

    @Override
    public void deleteFile(FileId id, int expectedVersion) throws Exception {
        try {
            curator.delete().withVersion(expectedVersion).forPath(FILES + "/" + id);
        } catch (KeeperException.NoNodeException ignored) {
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

    @Override
    public int nextNodeId() throws Exception {
        String path = curator.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(IDS + "/seq-");
        String seq = path.substring(path.lastIndexOf('-') + 1);
        return Integer.parseInt(seq) + 1; // node ids start at 1
    }

    @Override
    public void putNode(Records.NodeRecord record) throws Exception {
        String path = NODES + "/" + record.nodeId();
        if (curator.checkExists().forPath(path) == null) {
            curator.create().forPath(path, record.encode());
        } else {
            curator.setData().forPath(path, record.encode());
        }
    }

    @Override
    public Optional<Records.NodeRecord> getNode(int nodeId) throws Exception {
        try {
            return Optional.of(Records.NodeRecord.decode(curator.getData().forPath(NODES + "/" + nodeId)));
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<Records.NodeRecord> listNodes() throws Exception {
        List<Records.NodeRecord> out = new ArrayList<>();
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
