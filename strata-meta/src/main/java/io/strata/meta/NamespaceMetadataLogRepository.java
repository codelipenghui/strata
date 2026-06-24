package io.strata.meta;

import io.strata.common.FailureInjector;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;

import java.util.Optional;
import java.util.OptionalInt;

/**
 * Owns one namespace's metadata log: recovery, durable append, snapshot compaction, and manifest
 * version-CAS publication (design §8–§10, §13).
 *
 * <p>On {@link #open} it recovers state from the published manifest, then compacts (writes a fresh
 * snapshot, rolls a new empty open-log file) and CAS-publishes a new manifest BEFORE accepting writes —
 * the metadata-epoch fencing rule: a stale leader holding the old open file cannot append bytes that a
 * successor would later publish (design §8, §13 step 6). A lost manifest CAS means another leader has
 * advanced the namespace, so the caller must re-recover under a new epoch.
 *
 * <p>Durability ordering (design §15): an append writes to the file store first; only a durable append
 * mutates in-memory state, so a failed physical append never advances visible metadata. Manifest
 * publication is the linearizable barrier validated in {@code tla/MetadataManifestCAS.tla}.
 */
final class NamespaceMetadataLogRepository {
    private final StrataNamespace namespace;
    private final NamespaceMetadataFileStore fileStore;
    private final MetadataStore rootStore;
    private final long metadataEpoch;
    private final NamespaceLogMetrics metrics;
    private final java.util.concurrent.locks.ReentrantLock lock =
            new java.util.concurrent.locks.ReentrantLock();

    private NamespaceMetadataState state;
    private FileId logFileId;
    private FileId snapshotFileId;
    private long logStartOffset; // base offset of the open log file (== the snapshot cut)
    private long appliedOffset;  // durable end offset
    private long generation;
    private int manifestVersion; // znode version for the next CAS publish

    private NamespaceMetadataLogRepository(StrataNamespace namespace, NamespaceMetadataFileStore fileStore,
                                           MetadataStore rootStore, long metadataEpoch,
                                           NamespaceLogMetrics metrics) {
        this.namespace = namespace;
        this.fileStore = fileStore;
        this.rootStore = rootStore;
        this.metadataEpoch = metadataEpoch;
        this.metrics = metrics;
    }

    /**
     * Recovers the namespace under {@code metadataEpoch} and republishes a fresh manifest (compact +
     * roll) so the recovered prefix is durable in a new open log before any write is accepted.
     */
    static NamespaceMetadataLogRepository open(StrataNamespace namespace,
            NamespaceMetadataFileStore fileStore, MetadataStore rootStore, long metadataEpoch,
            NamespaceLogMetrics metrics)
            throws Exception {
        NamespaceMetadataLogRepository repo =
                new NamespaceMetadataLogRepository(namespace, fileStore, rootStore, metadataEpoch, metrics);
        repo.recoverAndRepublish();
        metrics.recordRecovery();
        return repo;
    }

    /** Convenience overload (fresh throwaway metrics) for unit tests that don't assert on counters. */
    static NamespaceMetadataLogRepository open(StrataNamespace namespace,
            NamespaceMetadataFileStore fileStore, MetadataStore rootStore, long metadataEpoch)
            throws Exception {
        return open(namespace, fileStore, rootStore, metadataEpoch, new NamespaceLogMetrics());
    }

    NamespaceMetadataState state() {
        return state;
    }

    long appliedOffset() {
        return appliedOffset;
    }

    /** Live (non-deleted) files in this namespace — for the per-namespace dashboard gauge. */
    int liveFileCount() {
        return state.liveFiles().size();
    }

    /** Open-log size in bytes (snapshot cut → durable end): the live metadata-log size for this namespace. */
    long openLogBytes() {
        return appliedOffset - logStartOffset;
    }

    long generation() {
        return generation;
    }

    long metadataEpoch() {
        return metadataEpoch;
    }

    StrataNamespace namespace() {
        return namespace;
    }

    /** Acquires this namespace's mutation lock (held across the durable append; single-writer ordering). */
    void lock() {
        lock.lock();
    }

    void unlock() {
        lock.unlock();
    }

    /** Durably appends a record (file store first), applies it to state, returns the new applied offset. */
    long append(MetadataLogRecord record) throws Exception {
        byte[] frame = MetadataLogSegmentCodec.frame(MetadataLogCodec.encode(record));
        fileStore.appendLog(logFileId, frame); // durable first
        // Crash window: the record is durable in the log but not yet applied/acked. A successor must
        // still recover it (byte-durability, tla/MetadataByteDurability) — see the failure-injection test.
        FailureInjector.point("meta.log.afterDurableAppend");
        state.apply(record);                    // then visible
        appliedOffset += frame.length;
        metrics.recordAppend(frame.length);
        return appliedOffset;
    }

    /** Compacts the log: snapshot the current state, roll a new open log, and CAS-publish the manifest. */
    void compactAndPublish() throws Exception {
        publishCompacted(appliedOffset, manifestVersion);
        metrics.recordCompaction();
    }

    private void recoverAndRepublish() throws Exception {
        Optional<MetadataStore.Versioned<Records.NamespaceManifest>> current =
                rootStore.getNamespaceManifest(namespace);
        Optional<Records.NamespaceManifest> manifest = current.map(MetadataStore.Versioned::value);
        NamespaceMetadataRecovery.Recovered recovered =
                NamespaceMetadataRecovery.recover(namespace, fileStore, manifest);
        this.state = recovered.state();
        this.generation = manifest.map(Records.NamespaceManifest::generation).orElse(0L);
        int expectedVersion = current.map(MetadataStore.Versioned::version).orElse(-1);
        publishCompacted(recovered.durableEndOffset(), expectedVersion);
    }

    private void publishCompacted(long cut, int expectedVersion) throws Exception {
        FileId newSnapshot = fileStore.writeSnapshot(
                NamespaceMetadataSnapshotCodec.encode(state.exportSnapshot(cut)));
        FileId newLog = fileStore.createLogFile();
        long newGeneration = generation + 1;
        Records.NamespaceManifest published = new Records.NamespaceManifest(namespace, metadataEpoch,
                newGeneration, cut, cut, Optional.of(newSnapshot), Optional.of(newLog));
        // Crash window: the new snapshot/log files exist but the manifest still points at the old
        // generation. A crash here must leave the OLD manifest fully recoverable — compaction is atomic
        // at the manifest CAS (design §10) — see the failure-injection test.
        FailureInjector.point("meta.log.beforeManifestPublish");
        OptionalInt newVersion = rootStore.putNamespaceManifest(published, expectedVersion);
        if (newVersion.isEmpty()) {
            // Best-effort cleanup of the files we just wrote but could not publish.
            deleteQuietly(newSnapshot);
            deleteQuietly(newLog);
            throw new IllegalStateException("manifest CAS lost for namespace " + namespace
                    + " — fenced; recover again under a new epoch");
        }
        FileId oldSnapshot = this.snapshotFileId;
        FileId oldLog = this.logFileId;
        this.snapshotFileId = newSnapshot;
        this.logFileId = newLog;
        this.logStartOffset = cut;
        this.appliedOffset = cut;
        this.generation = newGeneration;
        // The CAS returns the new znode version directly — no read-back round-trip, and no window where a
        // transient read failure leaves manifestVersion stale (which would fence the namespace on next CAS).
        this.manifestVersion = newVersion.getAsInt();
        deleteQuietly(oldSnapshot);
        deleteQuietly(oldLog);
    }

    private void deleteQuietly(FileId id) {
        if (id == null) {
            return;
        }
        try {
            fileStore.deleteFile(id);
        } catch (Exception ignore) {
            // compaction GC is best-effort; an undeleted old file only wastes space (design §10)
        }
    }
}
