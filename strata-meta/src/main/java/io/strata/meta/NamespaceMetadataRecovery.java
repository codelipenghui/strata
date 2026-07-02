package io.strata.meta;

import io.strata.common.FileId;
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
                     Optional<Records.NamespaceManifest> sourceManifest, boolean usedSnapshotFallback) {}

    static Recovered recover(StrataNamespace namespace, NamespaceMetadataFileStore fileStore,
                             Optional<Records.NamespaceManifest> manifest) throws Exception {
        if (manifest.isEmpty()) {
            return new Recovered(new NamespaceMetadataState(namespace), 0L, 0L, 0L, Optional.empty(), false);
        }
        return recoverFromManifest(namespace, fileStore, manifest.get(), true);
    }

    private static Recovered recoverFromManifest(StrataNamespace namespace, NamespaceMetadataFileStore fileStore,
                                                 Records.NamespaceManifest m, boolean allowPreviousFallback)
            throws Exception {
        NamespaceMetadataState state = new NamespaceMetadataState(namespace);
        if (m.snapshotFileId().isPresent()) {
            try {
                state.restore(readSnapshotWithRetry(fileStore, m.snapshotFileId().get()));
            } catch (IllegalArgumentException snapshotFailure) {
                if (!allowPreviousFallback || m.previous().isEmpty()) {
                    throw snapshotFailure;
                }
                return recoverFromPreviousThenCurrentLog(namespace, fileStore, m, snapshotFailure);
            }
        }
        LogReplay replay = replayLog(state, fileStore, m, m.logStartOffset());
        return new Recovered(state, replay.durableEndOffset(), replay.recordsReplayed(), replay.bytesRead(),
                Optional.of(m), false);
    }

    private static NamespaceMetadataState.Snapshot readSnapshotWithRetry(NamespaceMetadataFileStore fileStore,
                                                                        FileId snapshotFileId)
            throws Exception {
        try {
            return NamespaceMetadataSnapshotCodec.decode(fileStore.readSnapshot(snapshotFileId));
        } catch (IllegalArgumentException firstFailure) {
            // Only codec failures (CRC/version/parse) are retried or allowed to fall back. Transient I/O
            // exceptions must stay loud and must not trigger a rollback to an older generation.
            try {
                return NamespaceMetadataSnapshotCodec.decode(fileStore.readSnapshot(snapshotFileId));
            } catch (IllegalArgumentException retryFailure) {
                firstFailure.addSuppressed(retryFailure);
                throw firstFailure;
            }
        }
    }

    private static Recovered recoverFromPreviousThenCurrentLog(StrataNamespace namespace,
                                                               NamespaceMetadataFileStore fileStore,
                                                               Records.NamespaceManifest current,
                                                               IllegalArgumentException snapshotFailure)
            throws Exception {
        Records.NamespaceManifest previous =
                current.previous().get().toManifest(namespace, current.metadataEpoch());
        Recovered base;
        try {
            base = recoverFromManifest(namespace, fileStore, previous, false);
        } catch (Exception fallbackFailure) {
            snapshotFailure.addSuppressed(fallbackFailure);
            throw snapshotFailure;
        }

        if (current.logFileId().isPresent() && base.durableEndOffset() < current.logStartOffset()) {
            snapshotFailure.addSuppressed(new IllegalStateException(
                    "fallback chain gap: previous generation recovers to " + base.durableEndOffset()
                            + " but current log starts at " + current.logStartOffset()));
            throw snapshotFailure;
        }

        try {
            LogReplay currentReplay = replayLog(base.state(), fileStore, current, base.durableEndOffset());
            long durableEnd = Math.max(base.durableEndOffset(), currentReplay.durableEndOffset());
            return new Recovered(base.state(), durableEnd,
                    base.recordsReplayed() + currentReplay.recordsReplayed(),
                    base.bytesRead() + currentReplay.bytesRead(), base.sourceManifest(), true);
        } catch (Exception currentLogFailure) {
            snapshotFailure.addSuppressed(currentLogFailure);
            throw snapshotFailure;
        }
    }

    private record LogReplay(long durableEndOffset, long recordsReplayed, long bytesRead) {}

    private static LogReplay replayLog(NamespaceMetadataState state, NamespaceMetadataFileStore fileStore,
                                       Records.NamespaceManifest m, long alreadyRecoveredOffset) throws Exception {
        long skipBytes = Math.max(0L, alreadyRecoveredOffset - m.logStartOffset());
        if (m.logFileId().isPresent()) {
            // Replay the WHOLE open log file, not just up to publishedLogOffset: a fenced successor may
            // apply additional CRC-valid records already durable in the open tail (design §9).
            MetadataLogSegmentCodec.Prefix prefix =
                    MetadataLogSegmentCodec.recoverPrefix(fileStore.readLog(m.logFileId().get()),
                            Math.toIntExact(Math.min(skipBytes, Integer.MAX_VALUE)));
            for (MetadataLogRecord r : prefix.records()) {
                state.apply(r);
            }
            return new LogReplay(m.logStartOffset() + prefix.validBytes(), prefix.records().size(),
                    prefix.validBytes());
        }
        return new LogReplay(m.logStartOffset(), 0L, 0L);
    }
}
