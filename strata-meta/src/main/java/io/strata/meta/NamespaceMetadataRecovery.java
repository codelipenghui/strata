package io.strata.meta;

import io.strata.common.StrataNamespace;

import java.util.Optional;

/**
 * The metadata-log recovery barrier for one namespace (design §13): load the manifest's snapshot, then
 * replay the durable (CRC-valid) prefix of the open log file past the snapshot cut. A torn tail append
 * is discarded (see {@link MetadataLogSegmentCodec}). Returns the rebuilt state and the durable end
 * offset — the highest offset a successor may treat as committed.
 */
final class NamespaceMetadataRecovery {
    private NamespaceMetadataRecovery() {}

    record Recovered(NamespaceMetadataState state, long durableEndOffset, long recordsReplayed, long bytesRead) {}

    static Recovered recover(StrataNamespace namespace, NamespaceMetadataFileStore fileStore,
                             Optional<Records.NamespaceManifest> manifest) throws Exception {
        NamespaceMetadataState state = new NamespaceMetadataState(namespace);
        if (manifest.isEmpty()) {
            return new Recovered(state, 0L, 0L, 0L); // never published — a fresh namespace
        }
        Records.NamespaceManifest m = manifest.get();
        if (m.snapshotFileId().isPresent()) {
            NamespaceMetadataState.Snapshot snapshot =
                    NamespaceMetadataSnapshotCodec.decode(fileStore.readSnapshot(m.snapshotFileId().get()));
            state.restore(snapshot);
        }
        long durableEnd = m.logStartOffset();
        long recordsReplayed = 0;
        long bytesRead = 0;
        if (m.logFileId().isPresent()) {
            // Replay the WHOLE open log file, not just up to publishedLogOffset: a fenced successor may
            // apply additional CRC-valid records already durable in the open tail (design §9).
            MetadataLogSegmentCodec.Prefix prefix =
                    MetadataLogSegmentCodec.recoverPrefix(fileStore.readLog(m.logFileId().get()));
            for (MetadataLogRecord r : prefix.records()) {
                state.apply(r);
            }
            durableEnd = m.logStartOffset() + prefix.validBytes();
            recordsReplayed = prefix.records().size();
            bytesRead = prefix.validBytes();
        }
        return new Recovered(state, durableEnd, recordsReplayed, bytesRead);
    }
}
