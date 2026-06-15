package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class InMemoryMetadataStoreConformanceTest extends MetadataStoreConformanceTest {

    @Override
    protected Backend startBackend() {
        InMemoryMetadataStore.State state = new InMemoryMetadataStore.State();
        return new Backend() {
            @Override
            public MetadataStore openStore() {
                return new InMemoryMetadataStore(state);
            }

            @Override
            public void close() {
            }
        };
    }

    private static final class InMemoryMetadataStore implements MetadataStore {
        private final State state;
        private boolean closed;

        private InMemoryMetadataStore(State state) {
            this.state = state;
        }

        @Override
        public void createFile(Records.FileRecord record) {
            synchronized (state) {
                ensureOpen();
                PathKey pathKey = new PathKey(record.namespace(), record.path());
                PathMarker marker = state.paths.get(pathKey);
                // files.containsKey covers both a live file and an unswept DELETED tombstone — a
                // replayed CREATE for a deleted-but-unswept id must fail.
                if (state.files.containsKey(record.fileId())
                        || marker != null && marker.fileId().isPresent()) {
                    throw new IllegalStateException("file already exists: " + record.fileId());
                }
                state.files.put(record.fileId(), new Versioned<>(record, 0));
                state.paths.put(pathKey, marker == null
                        ? new PathMarker(Optional.of(record.fileId()), 0)
                        : marker.withFileId(Optional.of(record.fileId())));
            }
        }

        @Override
        public Optional<Versioned<Records.FileRecord>> getFile(FileId id) {
            synchronized (state) {
                ensureOpen();
                Versioned<Records.FileRecord> v = state.files.get(id);
                if (v == null || v.value().state() == FileState.DELETED) {
                    return Optional.empty();  // a swept-pending tombstone is logically gone
                }
                return Optional.of(v);
            }
        }

        @Override
        public Optional<FileId> resolvePath(StrataNamespace namespace, StrataPath path) {
            synchronized (state) {
                ensureOpen();
                PathMarker marker = state.paths.get(new PathKey(namespace, path));
                return marker == null ? Optional.empty() : marker.fileId();
            }
        }

        @Override
        public boolean updateFile(Records.FileRecord record, int expectedVersion) {
            synchronized (state) {
                ensureOpen();
                Versioned<Records.FileRecord> current = state.files.get(record.fileId());
                if (current == null || current.version() != expectedVersion) {
                    return false;
                }
                state.files.put(record.fileId(), new Versioned<>(record, current.version() + 1));
                return true;
            }
        }

        @Override
        public boolean deletePath(StrataNamespace namespace, StrataPath path, FileId expectedFileId) {
            synchronized (state) {
                ensureOpen();
                PathKey pathKey = new PathKey(namespace, path);
                PathMarker marker = state.paths.get(pathKey);
                if (marker == null || marker.fileId().isEmpty()) {
                    return true;
                }
                if (!marker.fileId().get().equals(expectedFileId)) {
                    return false;
                }
                state.paths.put(pathKey, marker.withFileId(Optional.empty()));
                return true;
            }
        }

        @Override
        public boolean deleteFile(FileId id, int expectedVersion) {
            synchronized (state) {
                ensureOpen();
                Versioned<Records.FileRecord> current = state.files.get(id);
                if (current == null || current.value().state() == FileState.DELETED) {
                    return true;
                }
                if (current.version() != expectedVersion) {
                    return false;
                }
                Records.FileRecord record = current.value();
                PathKey pathKey = new PathKey(record.namespace(), record.path());
                PathMarker marker = state.paths.get(pathKey);
                if (marker != null && marker.fileId().map(id::equals).orElse(false)) {
                    state.paths.put(pathKey, marker.withFileId(Optional.empty()));
                }
                // Tombstone the record (fences a replayed CREATE) until the sweeper reaps it.
                state.files.put(id, new Versioned<>(record.withState(FileState.DELETED), current.version() + 1));
                state.deletedAtMs.put(id, System.currentTimeMillis());
                return true;
            }
        }

        @Override
        public List<FileId> listFiles() {
            synchronized (state) {
                ensureOpen();
                ArrayList<FileId> files = new ArrayList<>();
                for (Map.Entry<FileId, Versioned<Records.FileRecord>> e : state.files.entrySet()) {
                    if (e.getValue().value().state() != FileState.DELETED) {  // skip tombstones
                        files.add(e.getKey());
                    }
                }
                files.sort(FileId::compareTo);
                return files;
            }
        }

        @Override
        public int sweepDeletedFiles(long olderThanMs) {
            synchronized (state) {
                ensureOpen();
                long now = System.currentTimeMillis();
                int reaped = 0;
                for (FileId id : new ArrayList<>(state.files.keySet())) {
                    Long deletedAt = state.deletedAtMs.get(id);
                    if (state.files.get(id).value().state() == FileState.DELETED
                            && deletedAt != null && now - deletedAt >= olderThanMs) {
                        state.files.remove(id);
                        state.deletedAtMs.remove(id);
                        reaped++;
                    }
                }
                return reaped;
            }
        }

        @Override
        public int nextNodeId() {
            synchronized (state) {
                ensureOpen();
                return state.nextNodeId++;
            }
        }

        @Override
        public boolean putNode(Records.NodeRecord record, int expectedVersion) {
            synchronized (state) {
                ensureOpen();
                Versioned<Records.NodeRecord> current = state.nodes.get(record.nodeId());
                if (expectedVersion < 0) {
                    if (current != null) {
                        return false;
                    }
                    state.nodes.put(record.nodeId(), new Versioned<>(record, 0));
                    return true;
                }
                if (current == null || current.version() != expectedVersion) {
                    return false;
                }
                state.nodes.put(record.nodeId(), new Versioned<>(record, current.version() + 1));
                return true;
            }
        }

        @Override
        public Optional<Versioned<Records.NodeRecord>> getNode(int nodeId) {
            synchronized (state) {
                ensureOpen();
                return Optional.ofNullable(state.nodes.get(nodeId));
            }
        }

        @Override
        public List<Versioned<Records.NodeRecord>> listNodes() {
            synchronized (state) {
                ensureOpen();
                ArrayList<Integer> nodeIds = new ArrayList<>(state.nodes.keySet());
                nodeIds.sort(Integer::compareTo);
                ArrayList<Versioned<Records.NodeRecord>> nodes = new ArrayList<>(nodeIds.size());
                for (Integer nodeId : nodeIds) {
                    nodes.add(state.nodes.get(nodeId));
                }
                return nodes;
            }
        }

        @Override
        public void close() {
            closed = true;
        }

        private void ensureOpen() {
            if (closed) {
                throw new IllegalStateException("metadata store is closed");
            }
        }

        private static final class State {
            private final Map<FileId, Versioned<Records.FileRecord>> files = new HashMap<>();
            private final Map<FileId, Long> deletedAtMs = new HashMap<>();
            private final Map<PathKey, PathMarker> paths = new HashMap<>();
            private final Map<Integer, Versioned<Records.NodeRecord>> nodes = new HashMap<>();
            private int nextNodeId = 1;
        }

        private record PathKey(StrataNamespace namespace, StrataPath path) {
        }

        private record PathMarker(Optional<FileId> fileId, int version) {
            private PathMarker withFileId(Optional<FileId> newFileId) {
                return new PathMarker(newFileId, version + 1);
            }
        }
    }
}
