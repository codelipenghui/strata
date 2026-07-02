package io.strata.meta;

import io.strata.common.FailureInjector;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.locks.ReentrantLock;

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
    private static final Logger log = LoggerFactory.getLogger(NamespaceMetadataLogRepository.class);

    private final StrataNamespace namespace;
    private final NamespaceMetadataFileStore fileStore;
    private final MetadataStore rootStore;
    private final long metadataEpoch;
    private final NamespaceLogMetrics metrics;
    private final ReentrantLock lock =
            new ReentrantLock();

    private NamespaceMetadataState state;
    private FileId logFileId;
    private FileId snapshotFileId;
    private long logStartOffset; // base offset of the open log file (== the snapshot cut)
    private long appliedOffset;  // durable end offset
    private long generation;
    private int manifestVersion; // znode version for the next CAS publish
    private Records.NamespaceManifest publishedManifest; // last manifest this repo successfully CAS-published
    private boolean compacting;  // guarded by lock: one compaction in flight, blocks an overlapping one
    // Set after an append throws: the log may contain a durable frame this repo has not applied.
    private volatile boolean poisoned;
    private volatile Exception poisonCause;
    // Guarded by lock: while a compaction is in flight, append() buffers each frame it writes here so the
    // publish phase can carry the freeze→CAS-window tail into the new segment WITHOUT reading (and, on the
    // production store, destructively sealing) the live open log. Cleared at each freeze and on completion.
    private final List<byte[]> compactionTail = new ArrayList<>();

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
        metrics.recordRecovery(namespace);
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

    boolean poisoned() {
        return poisoned;
    }

    Exception poisonCause() {
        return poisonCause;
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
        try {
            fileStore.appendLog(logFileId, frame); // durable first
            // Crash window: the record is durable in the log but not yet applied/acked. A successor must
            // still recover it (byte-durability, tla/MetadataByteDurability) — see the failure-injection test.
            FailureInjector.point("meta.log.afterDurableAppend");
            state.apply(record);                    // then visible
            appliedOffset += frame.length;
            if (compacting) {
                // A compaction froze its snapshot at an earlier cut; this frame is in its freeze→CAS window.
                // Buffer it so publishFrozen can carry it into the new segment without re-reading the open log.
                compactionTail.add(frame);
            }
            metrics.recordAppend(namespace, frame.length);
            return appliedOffset;
        } catch (Exception e) {
            poisoned = true;
            poisonCause = e;
            throw e;
        }
    }

    /** Always-compact entry point for the threshold-free call sites (tests); see {@link #compact}. */
    void compactAndPublish() throws Exception {
        compact(0);
    }

    /**
     * Non-blocking (copy-on-write) open-log compaction (design §10). Splits compaction into a short locked
     * freeze, a long UNLOCKED snapshot encode + write, and a short locked manifest CAS + pointer swap, so
     * the namespace keeps accepting metadata writes for the (size-proportional) duration of its own
     * compaction. Returns {@code true} if a manifest was published, {@code false} if the call was skipped
     * (open log under {@code thresholdBytes}, or a compaction is already in flight for this repo).
     *
     * <p><b>Carry the tail, do not reset.</b> Appends that land in the freeze→CAS window go to the OLD open
     * log (the currently-published one — so they survive a fencing failover, whose recovery replays the
     * whole old open log). {@code append()} also buffers each such frame in {@code compactionTail}; the
     * locked publish phase writes that buffered tail into the new segment so it physically starts at the
     * snapshot cut (recovery reads the whole log file assuming {@code byte 0 == logStartOffset == cut}).
     * Buffering — rather than reading the old log — is deliberate: the production store's
     * {@code readLog} recover-and-seals the file, which would fence the still-live open log. {@code
     * appliedOffset} is preserved (NOT reset to the cut — that would drop the carried tail's offsets).
     *
     * @throws IllegalStateException if the manifest CAS is lost — another node owns this namespace now
     */
    boolean compact(long thresholdBytes) throws Exception {
        Frozen frozen;
        lock();
        try {
            if (poisoned || compacting || openLogBytes() < thresholdBytes) {
                return false;
            }
            compacting = true;
            compactionTail.clear();
            frozen = freeze();
        } catch (RuntimeException | Error t) {
            // freeze() (exportSnapshot) does no I/O, but if it throws (e.g. OOM on a huge state) release the
            // compaction claim so it is not stuck `true` forever, disabling all future compaction.
            compacting = false;
            throw t;
        } finally {
            unlock();
        }
        boolean published = false;
        try {
            // Off-lock: the expensive snapshot encode + durable (replicated) snapshot write, plus rolling
            // the (empty) new log. Appends to this namespace run concurrently against the still-current open
            // log during this phase and are buffered into compactionTail.
            byte[] snapshotBytes = NamespaceMetadataSnapshotCodec.encode(frozen.snapshot());
            FileId newSnapshot = fileStore.writeSnapshot(namespace, frozen.newGeneration(), snapshotBytes);
            FileId newLog = fileStore.createLogFile(namespace, frozen.newGeneration());
            lock();
            try {
                published = publishFrozen(frozen, newSnapshot, newLog);
            } finally {
                unlock();
            }
        } finally {
            lock();
            try {
                compacting = false;
                compactionTail.clear();
            } finally {
                unlock();
            }
        }
        if (published) {
            metrics.recordCompaction(namespace);
        }
        return published;
    }

    /** Immutable freeze of the state to compact, captured under the mutation lock (the short locked phase). */
    private record Frozen(long cut, int expectedVersion, long newGeneration,
                          NamespaceMetadataState.Snapshot snapshot) {
    }

    /** Captures the cut, the manifest version to CAS against, and an immutable snapshot of the state. */
    private Frozen freeze() {
        long cut = appliedOffset;
        return new Frozen(cut, manifestVersion, generation + 1, state.exportSnapshot(cut));
    }

    /**
     * The short locked publish phase: carry the post-freeze tail into the (already-rolled) new open log,
     * CAS-publish the manifest, then swap the in-memory pointers. Mirrors the crash-window invariants of
     * {@link #publishCompacted}: the new files are written before the CAS, a lost CAS cleans up the new files
     * and throws (fence), and the just-superseded generation is NOT deleted inline (issue #8) — it is retained
     * and reclaimed by the retention-gated sweep. It never reads or seals the old open log, so a publish
     * failure leaves that log writable for the next op / re-acquire.
     */
    private boolean publishFrozen(Frozen frozen, FileId newSnapshot, FileId newLog) throws Exception {
        if (poisoned) {
            // An append may have durably reached the old open log but failed before apply/compactionTail.
            // Publishing this frozen cut would make the durable frame unreachable from the manifest.
            deleteQuietly(newSnapshot);
            deleteQuietly(newLog);
            return false;
        }
        // Carry the freeze→CAS-window appends [cut, appliedOffset) — buffered by append() into
        // compactionTail — into the new segment so it physically starts at the snapshot cut. The frames are
        // already framed and quorum-durable in the old log; we re-append the bytes (a small, bounded write).
        for (byte[] frame : compactionTail) {
            fileStore.appendLog(newLog, frame);
        }
        Optional<Records.NamespaceManifest.NamespaceManifestRef> previous =
                Optional.ofNullable(publishedManifest).map(Records.NamespaceManifest.NamespaceManifestRef::from);
        Records.NamespaceManifest published = new Records.NamespaceManifest(namespace, metadataEpoch,
                frozen.newGeneration(), frozen.cut(), appliedOffset, Optional.of(newSnapshot), Optional.of(newLog),
                previous);
        // Crash window (same as publishCompacted): the new snapshot/log exist but the manifest still points
        // at the old generation; a crash here must leave the OLD manifest fully recoverable.
        FailureInjector.point("meta.log.beforeManifestPublish");
        OptionalInt newVersion = rootStore.putNamespaceManifest(published, frozen.expectedVersion());
        if (newVersion.isEmpty()) {
            // Best-effort cleanup of the files we just wrote but could not publish.
            deleteQuietly(newSnapshot);
            deleteQuietly(newLog);
            throw new IllegalStateException("manifest CAS lost for namespace " + namespace
                    + " — fenced; recover again under a new epoch");
        }
        this.snapshotFileId = newSnapshot;
        this.logFileId = newLog;
        this.logStartOffset = frozen.cut();
        // appliedOffset is preserved: the tail [cut, appliedOffset) was carried into the new log, so the
        // durable end is unchanged. (Do NOT reset to cut — that would drop the carried tail.)
        this.generation = frozen.newGeneration();
        this.manifestVersion = newVersion.getAsInt();
        this.publishedManifest = published;
        // The just-superseded generation (old snapshot/log) is NOT deleted inline (issue #8, design §10 step
        // 6): it is retained as a rollback margin and reclaimed by the retention-gated sweep
        // (NamespaceLogBackend.gcOrphanedSystemFiles) once STRATA_CONTROLLER_LOG_RETENTION_MS has elapsed.
        return true;
    }

    private void recoverAndRepublish() throws Exception {
        Optional<MetadataStore.Versioned<Records.NamespaceManifest>> current =
                rootStore.getNamespaceManifest(namespace);
        Optional<Records.NamespaceManifest> manifest = current.map(MetadataStore.Versioned::value);
        NamespaceMetadataRecovery.Recovered recovered =
                NamespaceMetadataRecovery.recover(namespace, fileStore, manifest);
        metrics.recordLogRead(namespace, recovered.recordsReplayed(), recovered.bytesRead());
        if (recovered.usedSnapshotFallback()) {
            Records.NamespaceManifest currentManifest = manifest.orElseThrow();
            Records.NamespaceManifest sourceManifest = recovered.sourceManifest().orElseThrow();
            metrics.recordSnapshotFallback(namespace);
            log.error("namespace metadata snapshot fallback recovered namespace={} badGeneration={} "
                            + "sourceGeneration={} currentLogFile={} sourceSnapshotFile={} durableEnd={}",
                    namespace, currentManifest.generation(), sourceManifest.generation(),
                    currentManifest.logFileId().map(FileId::toString).orElse("<none>"),
                    sourceManifest.snapshotFileId().map(FileId::toString).orElse("<none>"),
                    recovered.durableEndOffset());
        }
        this.state = recovered.state();
        this.generation = manifest.map(Records.NamespaceManifest::generation).orElse(0L);
        int expectedVersion = current.map(MetadataStore.Versioned::version).orElse(-1);
        publishCompacted(recovered.durableEndOffset(), expectedVersion, recovered.sourceManifest());
    }

    private void publishCompacted(long cut, int expectedVersion,
                                  Optional<Records.NamespaceManifest> previousManifest) throws Exception {
        long newGeneration = generation + 1;
        FileId newSnapshot = fileStore.writeSnapshot(namespace, newGeneration,
                NamespaceMetadataSnapshotCodec.encode(state.exportSnapshot(cut)));
        FileId newLog = fileStore.createLogFile(namespace, newGeneration);
        Optional<Records.NamespaceManifest.NamespaceManifestRef> previous =
                previousManifest.map(Records.NamespaceManifest.NamespaceManifestRef::from);
        Records.NamespaceManifest published = new Records.NamespaceManifest(namespace, metadataEpoch,
                newGeneration, cut, cut, Optional.of(newSnapshot), Optional.of(newLog), previous);
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
        this.snapshotFileId = newSnapshot;
        this.logFileId = newLog;
        this.logStartOffset = cut;
        this.appliedOffset = cut;
        this.generation = newGeneration;
        // The CAS returns the new znode version directly — no read-back round-trip, and no window where a
        // transient read failure leaves manifestVersion stale (which would fence the namespace on next CAS).
        this.manifestVersion = newVersion.getAsInt();
        this.publishedManifest = published;
        // The just-superseded generation (old snapshot/log) is NOT deleted inline (issue #8, design §10 step
        // 6): it is retained as a rollback margin and reclaimed by the retention-gated sweep
        // (NamespaceLogBackend.gcOrphanedSystemFiles) once STRATA_CONTROLLER_LOG_RETENTION_MS has elapsed
        // since this publish. Deferring to that sweep keeps reclamation on ONE durable, failover-safe path
        // rather than an inline best-effort delete that no retention window could honor.
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
