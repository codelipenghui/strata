package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The namespace-log store routes SYSTEM-namespace file enumeration — the metadata-log segment
 * descriptors, which live in the consensus root — through the children-only
 * {@link MetadataStore#listFileIds}, so the per-tick reconcile/verify sweep enumerates segment ids
 * WITHOUT reading every segment record (the dominant idle {@code /strata/files} read load). User
 * namespaces serve their listing from the in-memory repo and never touched the root regardless.
 */
class NamespaceLogListFileIdsTest {

    private static final StrataNamespace SYS = NamespaceLogBackend.SYSTEM_NAMESPACE;

    @Test
    void listFileIdsForSystemNamespaceReadsNoSegmentRecords() throws Exception {
        try (TestingServer zk = NamespaceLogTestSupport.testingServer();
             ZkMetadataStore root = NamespaceLogTestSupport.inMemoryRoot(zk)) {
            NamespaceLogBackend backend =
                    new NamespaceLogBackend(root, NamespaceLogTestSupport.inMemoryFileStore(), false);
            MetadataStore store = new NamespaceLogMetadataStore(backend);
            for (int i = 1; i <= 3; i++) {
                StrataPath path = StrataPath.of("/metadata-log/owner/gen-1/log-" + i);
                root.createFile(new Records.FileRecord(FileId.of(i), SYS, path, 3, 2, false,
                        FileState.OPEN, 1_000L, List.of(), 0, 0));
            }

            long filesReadBefore = root.zkOps("files", false);
            List<FileId> ids = store.listFileIds(SYS);

            assertEquals(Set.of(FileId.of(1), FileId.of(2), FileId.of(3)), Set.copyOf(ids),
                    "every system segment id must be enumerated");
            assertEquals(filesReadBefore, root.zkOps("files", false),
                    "system-namespace listFileIds must not read any segment record (children-only)");
            backend.close();
        }
    }
}
