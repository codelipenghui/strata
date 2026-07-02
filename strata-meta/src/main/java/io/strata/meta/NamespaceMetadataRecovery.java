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

    record Recovered(NamespaceMetadataState state, long durableEndOffset, long recordsReplayed, long bytesRead,
                     Optional<Records.NamespaceManifest> sourceManifest) {}

    static Recovered recover(StrataNamespace namespace, NamespaceMetadataFileStore fileStore,
                             Optional<Records.NamespaceManifest> manifest) throws Exception {
        if (manifest.isEmpty()) {
            return new Recovered(new NamespaceMetadataState(namespace), 0L, 0L, 0L, Optional.empty());
        }
        return recoverFromManifest(namespace, fileStore, manifest.get(), true);
    }

    private static Recovered recoverFromManifest(StrataNamespace namespace, NamespaceMetadataFileStore fileStore,
                                                 Records.NamespaceManifest m, boolean allowPreviousFallback)
            throws Exception {
        NamespaceMetadataState state = new NamespaceMetadataState(namespace);
        if (m.snapshotFileId().isPresent()) {
            try {
                NamespaceMetadataState.Snapshot snapshot =
                        NamespaceMetadataSnapshotCodec.decode(fileStore.readSnapshot(m.snapshotFileId().get()));
                state.restore(snapshot);
            } catch (IllegalArgumentException snapshotFailure) {
                if (!allowPreviousFallback || m.previous().isEmpty()) {
                    throw snapshotFailure;
                }
                Records.NamespaceManifest previous =
                        m.previous().get().toManifest(namespace, m.metadataEpoch());
                try {
                    return recoverFromManifest(namespace, fileStore, previous, false);
                } catch (Exception fallbackFailure) {
                    snapshotFailure.addSuppressed(fallbackFailure);
                    throw snapshotFailure;
                }
            }
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
        return new Recovered(state, durableEnd, recordsReplayed, bytesRead, Optional.of(m));
    }
}
