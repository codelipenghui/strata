package io.strata.meta;

import io.strata.common.ChunkState;
import io.strata.common.FileId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Translates a wholesale {@link Records.FileRecord} mutation (the generic {@code MetadataStore.updateFile}
 * the service issues) into the authoritative semantic log records that reproduce it on replay
 * ({@link NamespaceMetadataState}). This is the bridge from the CAS-replace SPI to the append-only log
 * (design §8.1): the immutable identity fields (id, namespace, path, policy, createdAt) come from the
 * original {@code FileCreated}; only writer-epoch, chunk, replica, and lifecycle deltas are emitted.
 */
final class MetadataLogDiff {
    private MetadataLogDiff() {}

    static List<MetadataLogRecord> diff(Records.FileRecord old, Records.FileRecord updated) {
        FileId id = updated.fileId();
        List<MetadataLogRecord> records = new ArrayList<>();

        if (updated.writerEpoch() != old.writerEpoch()) {
            records.add(new MetadataLogRecord.WriterEpochAllocated(id, updated.writerEpoch()));
        }

        Map<Integer, Records.ChunkRecord> oldByIndex = byIndex(old.chunks());
        Map<Integer, Records.ChunkRecord> newByIndex = byIndex(updated.chunks());

        for (Records.ChunkRecord nc : updated.chunks()) {
            Records.ChunkRecord oc = oldByIndex.get(nc.index());
            if (oc == null || differentCreate(oc, nc)) {
                // A brand-new chunk, or a reused index claimed by a different create op: (re)create it.
                if (oc != null) {
                    records.add(removeChunkRecord(id, oc));
                }
                records.add(new MetadataLogRecord.ChunkCreated(id, nc.index(), nc.writeEpoch(),
                        nc.replicas(), nc.createOpMsb(), nc.createOpLsb()));
                if (nc.state() == ChunkState.SEALED) {
                    records.add(new MetadataLogRecord.ChunkSealed(id, nc.index(), nc.length(), nc.crc(),
                            nc.writeEpoch(), nc.replicas()));
                }
            } else if (oc.state() != ChunkState.SEALED && nc.state() == ChunkState.SEALED) {
                records.add(new MetadataLogRecord.ChunkSealed(id, nc.index(), nc.length(), nc.crc(),
                        nc.writeEpoch(), nc.replicas()));
            } else if (!oc.replicas().equals(nc.replicas())) {
                // Replica membership change (repair): drop the departed, add the new.
                for (int r : oc.replicas()) {
                    if (!nc.replicas().contains(r)) {
                        records.add(new MetadataLogRecord.ReplicaDropped(id, nc.index(), r));
                    }
                }
                for (int r : nc.replicas()) {
                    if (!oc.replicas().contains(r)) {
                        records.add(new MetadataLogRecord.ReplicaAdded(id, nc.index(), r));
                    }
                }
            }
        }

        for (Records.ChunkRecord oc : old.chunks()) {
            if (!newByIndex.containsKey(oc.index())) {
                records.add(removeChunkRecord(id, oc));
            }
        }

        if (old.state() != updated.state()) {
            switch (updated.state()) {
                case SEALED -> records.add(new MetadataLogRecord.FileSealed(id));
                case DELETING -> records.add(new MetadataLogRecord.FileDeleting(id));
                case OPEN, DELETED -> { /* OPEN: no transition record; DELETED: via deleteFile */ }
            }
        }
        return records;
    }

    private static MetadataLogRecord removeChunkRecord(FileId id, Records.ChunkRecord oc) {
        return oc.state() == ChunkState.SEALED
                ? new MetadataLogRecord.ChunkDeleted(id, oc.index())
                : new MetadataLogRecord.ChunkAborted(id, oc.index());
    }

    private static boolean differentCreate(Records.ChunkRecord oc, Records.ChunkRecord nc) {
        return oc.createOpMsb() != nc.createOpMsb() || oc.createOpLsb() != nc.createOpLsb();
    }

    private static Map<Integer, Records.ChunkRecord> byIndex(List<Records.ChunkRecord> chunks) {
        Map<Integer, Records.ChunkRecord> m = new HashMap<>();
        for (Records.ChunkRecord c : chunks) {
            m.put(c.index(), c);
        }
        return m;
    }
}
