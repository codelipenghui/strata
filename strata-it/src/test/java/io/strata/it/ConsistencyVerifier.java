package io.strata.it;

import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ChunkState;
import io.strata.common.Crc;
import io.strata.common.FileId;
import io.strata.common.FileState;
import io.strata.format.ChunkFormats;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.Resp;
import io.strata.proto.ScpClient;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Shared black-box consistency checks for integration and fault tests. */
final class ConsistencyVerifier {
    private static final int CALL_TIMEOUT_MS = 5_000;
    private static final int FETCH_CHUNK_BYTES = 4 * 1024 * 1024;

    private ConsistencyVerifier() {
    }

    static Messages.LookupFileResp lookupFile(MiniCluster cluster, FileId fileId) throws Exception {
        return lookupFile(cluster.metaEndpoints(), fileId);
    }

    static Messages.LookupFileResp lookupFile(List<String> metadataEndpoints, FileId fileId) throws Exception {
        AssertionError failure = new AssertionError("no metadata endpoint could serve lookup for " + fileId);
        for (String endpoint : metadataEndpoints) {
            try {
                String[] hp = endpoint.split(":");
                try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]),
                        ScpClient.KIND_TOOL, "consistency-lookup")) {
                    ByteBuffer h = direct.call(Opcode.LOOKUP_FILE,
                            new Messages.LookupFile(fileId).encode(), null, CALL_TIMEOUT_MS);
                    return Messages.LookupFileResp.decode(h);
                }
            } catch (Exception e) {
                failure.addSuppressed(e);
            }
        }
        throw failure;
    }

    static Messages.LookupFileResp assertSealedFileConsistent(MiniCluster cluster, StrataClient client,
                                                              FileId fileId, long sealedLength) throws Exception {
        return assertSealedFileConsistent(cluster.metaEndpoints(), client, fileId, sealedLength);
    }

    static Messages.LookupFileResp waitForFullSealedFileConsistent(MiniCluster cluster,
                                                                    StrataClient client,
                                                                    FileId fileId,
                                                                    long sealedLength) throws Exception {
        return waitForFullSealedFileConsistent(cluster.metaEndpoints(), client, fileId, sealedLength);
    }

    static Messages.LookupFileResp waitForFullSealedFileConsistent(List<String> metadataEndpoints,
                                                                    StrataClient client,
                                                                    FileId fileId,
                                                                    long sealedLength) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        Throwable lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                Messages.LookupFileResp lookup = assertSealedFileConsistent(metadataEndpoints, client,
                        fileId, sealedLength);
                assertEverySealedChunkAtFullReadableRf(lookup);
                return lookup;
            } catch (Exception | AssertionError e) {
                lastFailure = e;
            }
            Thread.sleep(250);
        }
        if (lastFailure instanceof AssertionError assertion) {
            throw assertion;
        }
        AssertionError failure = new AssertionError(
                "sealed file did not converge to full readable RF for " + fileId);
        if (lastFailure != null) {
            failure.addSuppressed(lastFailure);
        }
        throw failure;
    }

    static Messages.LookupFileResp assertSealedFileConsistent(List<String> metadataEndpoints,
                                                              StrataClient client,
                                                              FileId fileId,
                                                              long sealedLength) throws Exception {
        Messages.LookupFileResp lookup = assertSealedDescriptorConsistent(metadataEndpoints, fileId, sealedLength);
        assertClientReadsExactlySealedLength(client, fileId, sealedLength);
        assertSealedReplicasAreByteIdentical(lookup);
        return lookup;
    }

    static Messages.LookupFileResp assertSealedDescriptorConsistent(MiniCluster cluster,
                                                                    FileId fileId,
                                                                    long sealedLength) throws Exception {
        return assertSealedDescriptorConsistent(cluster.metaEndpoints(), fileId, sealedLength);
    }

    static Messages.LookupFileResp assertSealedDescriptorConsistent(List<String> metadataEndpoints,
                                                                    FileId fileId,
                                                                    long sealedLength) throws Exception {
        Messages.LookupFileResp lookup = lookupFile(metadataEndpoints, fileId);
        assertLiveDescriptorInvariants(fileId, lookup);
        assertEquals(FileState.SEALED.value, lookup.fileState(),
                "file metadata must be sealed for " + fileId);

        long descriptorLength = 0;
        for (Messages.ChunkInfo chunk : lookup.chunks()) {
            assertEquals(ChunkState.SEALED, chunk.state(),
                    "chunk metadata must be sealed for " + chunk.chunkId());
            assertTrue(chunk.length() >= 0, "negative chunk length for " + chunk.chunkId());
            assertTrue(chunk.replicas().size() >= lookup.writePolicy().ackQuorum(),
                    "sealed chunk " + chunk.chunkId() + " has fewer replicas than ack quorum");

            Set<Integer> nodeIds = new HashSet<>();
            List<Messages.Replica> readable = new ArrayList<>();
            AssertionError replicaFailures = new AssertionError(
                    "unreadable or inconsistent replicas for " + chunk.chunkId());
            for (Messages.Replica replica : chunk.replicas()) {
                assertTrue(nodeIds.add(replica.nodeId()),
                        "duplicate replica node " + replica.nodeId() + " for " + chunk.chunkId());
                try {
                    validateReplicaStat(replica, chunk);
                    readable.add(replica);
                } catch (Exception | AssertionError e) {
                    replicaFailures.addSuppressed(e);
                }
            }
            if (readable.size() < lookup.writePolicy().ackQuorum()) {
                AssertionError quorumFailure = new AssertionError("sealed chunk " + chunk.chunkId()
                        + " has " + readable.size() + " readable replicas, below ack quorum "
                        + lookup.writePolicy().ackQuorum());
                quorumFailure.addSuppressed(replicaFailures);
                throw quorumFailure;
            }
            descriptorLength += chunk.length();
        }

        assertEquals(sealedLength, descriptorLength,
                "sealed descriptor length must match file seal length");
        return lookup;
    }

    static Messages.LookupFileResp assertLiveFileDescriptorConsistent(MiniCluster cluster,
                                                                      FileId fileId) throws Exception {
        return assertLiveFileDescriptorConsistent(cluster.metaEndpoints(), fileId);
    }

    static Messages.LookupFileResp assertLiveFileDescriptorConsistent(List<String> metadataEndpoints,
                                                                      FileId fileId) throws Exception {
        Messages.LookupFileResp lookup = lookupFile(metadataEndpoints, fileId);
        assertLiveDescriptorInvariants(fileId, lookup);

        for (Messages.ChunkInfo chunk : lookup.chunks()) {
            AssertionError replicaFailures = new AssertionError(
                    "chunk " + chunk.chunkId() + " has no reachable quorum");
            int reachable = 0;
            for (Messages.Replica replica : chunk.replicas()) {
                try {
                    Messages.StatResp stat = statReplica(replica, chunk);
                    if (statUsableForDescriptor(chunk, stat)) {
                        reachable++;
                    }
                } catch (Exception | AssertionError e) {
                    replicaFailures.addSuppressed(e);
                }
            }
            if (reachable < lookup.writePolicy().ackQuorum()) {
                AssertionError quorumFailure = new AssertionError("chunk " + chunk.chunkId()
                        + " has " + reachable + " reachable replicas, below ack quorum "
                        + lookup.writePolicy().ackQuorum());
                quorumFailure.addSuppressed(replicaFailures);
                throw quorumFailure;
            }
        }
        return lookup;
    }

    static void assertLiveDescriptorInvariants(FileId fileId, Messages.LookupFileResp lookup) {
        FileState fileState = FileState.fromValue(lookup.fileState());
        assertTrue(fileState == FileState.OPEN || fileState == FileState.SEALED,
                "live descriptor check requires OPEN or SEALED file state for " + fileId
                        + ", got " + fileState);
        assertTrue(lookup.writePolicy().replicationFactor() > 0,
                "replicationFactor must be positive for " + fileId);
        assertTrue(lookup.writePolicy().ackQuorum() > 0,
                "ackQuorum must be positive for " + fileId);
        assertTrue(lookup.writePolicy().ackQuorum() <= lookup.writePolicy().replicationFactor(),
                "ackQuorum must not exceed replicationFactor for " + fileId);
        assertTrue(lookup.writePolicy().ackQuorum() > lookup.writePolicy().replicationFactor() / 2,
                "ackQuorum must intersect any other quorum for " + fileId);

        long descriptorLength = 0;
        boolean sawOpen = false;
        for (int i = 0; i < lookup.chunks().size(); i++) {
            Messages.ChunkInfo chunk = lookup.chunks().get(i);
            assertEquals(fileId, chunk.chunkId().fileId(),
                    "chunk " + chunk.chunkId() + " belongs to a different file");
            assertEquals(i, chunk.chunkId().index(),
                    "chunk index must be contiguous for " + fileId);
            assertTrue(chunk.length() >= 0, "negative chunk length for " + chunk.chunkId());
            assertTrue(chunk.writeEpoch() >= 0, "negative write epoch for " + chunk.chunkId());

            if (chunk.state() == ChunkState.SEALED) {
                assertTrue(!sawOpen, "sealed chunk appears after open chunk for " + fileId);
                descriptorLength = addDescriptorLength(descriptorLength, chunk);
            } else if (chunk.state() == ChunkState.OPEN) {
                sawOpen = true;
                assertEquals(FileState.OPEN, fileState,
                        "sealed file must not contain open chunk " + chunk.chunkId());
                assertEquals(lookup.chunks().size() - 1, i,
                        "open chunk must be the tail for " + fileId);
                assertEquals(0, chunk.length(),
                        "open chunk must not advertise committed length for " + chunk.chunkId());
                assertEquals(0, chunk.crc(),
                        "open chunk must not advertise sealed crc for " + chunk.chunkId());
            } else {
                throw new AssertionError("live descriptor contains deleting chunk " + chunk.chunkId());
            }

            assertReplicaSetShape(lookup, chunk);
        }

        if (fileState == FileState.SEALED) {
            assertTrue(!sawOpen, "sealed file has an open chunk for " + fileId);
        }
        assertTrue(descriptorLength >= 0, "descriptor length overflowed for " + fileId);
    }

    static Messages.Replica waitForOpenReplicaEndAtLeast(Messages.ChunkInfo chunk, long minEnd) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        AssertionError last = null;
        while (System.currentTimeMillis() < deadline) {
            for (Messages.Replica replica : chunk.replicas()) {
                try {
                    Messages.StatResp stat = statReplica(replica, chunk);
                    if (stat.state() == ChunkState.OPEN && stat.localEndOffset() >= minEnd) {
                        return replica;
                    }
                } catch (AssertionError e) {
                    last = e;
                }
            }
            Thread.sleep(50);
        }
        if (last != null) {
            throw last;
        }
        throw new AssertionError("no open replica reached end offset " + minEnd + " for " + chunk.chunkId());
    }

    static Messages.SealResp sealReplicaOnly(Messages.Replica replica, Messages.ChunkInfo chunk,
                                             long dataLength) throws Exception {
        String[] hp = replica.endpoint().split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]),
                ScpClient.KIND_TOOL, "fault-seal")) {
            ByteBuffer h = direct.call(Opcode.SEAL_CHUNK,
                    new Messages.SealChunk(chunk.chunkId(), chunk.writeEpoch(), dataLength).encode(),
                    null, CALL_TIMEOUT_MS);
            return Messages.SealResp.decode(h);
        }
    }

    private static void assertClientReadsExactlySealedLength(StrataClient client,
                                                             FileId fileId,
                                                             long sealedLength) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (StrataFile.Reader reader = client.openById(fileId).openForRead()) {
            long offset = 0;
            for (int idle = 0; idle < 3; ) {
                StrataFile.ReadResult result = reader.read(offset, 1 << 20);
                if (result.data().length > 0) {
                    out.write(result.data(), 0, result.data().length);
                    offset += result.data().length;
                    idle = 0;
                } else if (result.endOfFile()) {
                    break;
                } else {
                    idle++;
                    reader.refresh();
                }
            }
        }
        assertEquals(sealedLength, out.size(),
                "client-visible sealed length must match metadata descriptor length");
    }

    private static void validateReplicaStat(Messages.Replica replica, Messages.ChunkInfo chunk) throws Exception {
        if (replica.endpoint().isBlank()) {
            throw new AssertionError("descriptor replica " + replica.nodeId()
                    + " has no endpoint for " + chunk.chunkId());
        }
        Messages.StatResp stat = statReplica(replica, chunk);
        assertEquals(ChunkState.SEALED, stat.state(),
                "replica " + replica.nodeId() + " is not sealed for " + chunk.chunkId());
        assertEquals(chunk.length(), stat.sealedLength(),
                "replica " + replica.nodeId() + " sealed length differs for " + chunk.chunkId());
        assertTrue(stat.localEndOffset() >= chunk.length(),
                "replica " + replica.nodeId() + " local end is short for " + chunk.chunkId());
        assertTrue(Math.max(stat.writeEpoch(), stat.fenceEpoch()) >= chunk.writeEpoch(),
                "replica " + replica.nodeId() + " epoch floor is below metadata epoch for "
                        + chunk.chunkId());
        assertEquals(chunk.crc(), stat.sealedCrc(),
                "replica " + replica.nodeId() + " sealed crc differs for " + chunk.chunkId());
    }

    private static boolean statUsableForDescriptor(Messages.ChunkInfo chunk, Messages.StatResp stat) {
        if (chunk.state() == ChunkState.SEALED) {
            return stat.state() == ChunkState.SEALED
                    && stat.sealedLength() == chunk.length()
                    && stat.sealedCrc() == chunk.crc()
                    && stat.localEndOffset() >= chunk.length()
                    && Math.max(stat.writeEpoch(), stat.fenceEpoch()) >= chunk.writeEpoch();
        }
        return (stat.state() == ChunkState.OPEN || stat.state() == ChunkState.SEALED)
                && stat.localEndOffset() >= 0
                && Math.max(stat.writeEpoch(), stat.fenceEpoch()) >= chunk.writeEpoch();
    }

    private static long addDescriptorLength(long current, Messages.ChunkInfo chunk) {
        try {
            return Math.addExact(current, chunk.length());
        } catch (ArithmeticException e) {
            throw new AssertionError("descriptor length overflow at " + chunk.chunkId(), e);
        }
    }

    private static void assertReplicaSetShape(Messages.LookupFileResp lookup, Messages.ChunkInfo chunk) {
        assertTrue(chunk.replicas().size() >= lookup.writePolicy().ackQuorum(),
                "chunk " + chunk.chunkId() + " has fewer replicas than ack quorum");
        assertTrue(chunk.replicas().size() <= lookup.writePolicy().replicationFactor(),
                "chunk " + chunk.chunkId() + " has more replicas than replicationFactor");

        Set<Integer> nodeIds = new HashSet<>();
        Set<String> endpoints = new HashSet<>();
        for (Messages.Replica replica : chunk.replicas()) {
            assertTrue(replica.nodeId() > 0,
                    "replica node id must be positive for " + chunk.chunkId());
            assertTrue(nodeIds.add(replica.nodeId()),
                    "duplicate replica node " + replica.nodeId() + " for " + chunk.chunkId());
            assertTrue(!replica.endpoint().isBlank(),
                    "replica endpoint is blank for node " + replica.nodeId() + " on " + chunk.chunkId());
            assertTrue(replica.endpoint().contains(":"),
                    "replica endpoint must include host and port for node " + replica.nodeId()
                            + " on " + chunk.chunkId() + ": " + replica.endpoint());
            assertTrue(endpoints.add(replica.endpoint()),
                    "duplicate replica endpoint " + replica.endpoint() + " for " + chunk.chunkId());
        }
    }

    static Messages.StatResp statReplica(Messages.Replica replica, Messages.ChunkInfo chunk) throws Exception {
        String[] hp = replica.endpoint().split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]),
                ScpClient.KIND_TOOL, "consistency-stat")) {
            ByteBuffer h = direct.call(Opcode.STAT_CHUNK,
                    new Messages.StatChunk(chunk.chunkId()).encode(), null, CALL_TIMEOUT_MS);
            return Messages.StatResp.decode(h);
        }
    }

    static int readableSealedReplicaCount(Messages.ChunkInfo chunk) {
        int readable = 0;
        for (Messages.Replica replica : chunk.replicas()) {
            try {
                Messages.StatResp stat = statReplica(replica, chunk);
                if (stat.state() == ChunkState.SEALED
                        && stat.sealedLength() == chunk.length()
                        && stat.sealedCrc() == chunk.crc()) {
                    readable++;
                }
            } catch (Exception | AssertionError ignored) {
            }
        }
        return readable;
    }

    private static void assertEverySealedChunkAtFullReadableRf(Messages.LookupFileResp lookup) {
        int replicationFactor = lookup.writePolicy().replicationFactor();
        for (Messages.ChunkInfo chunk : lookup.chunks()) {
            assertEquals(replicationFactor, chunk.replicas().size(),
                    "sealed chunk " + chunk.chunkId() + " did not converge to full RF");
            assertEquals(replicationFactor, readableSealedReplicaCount(chunk),
                    "sealed chunk " + chunk.chunkId() + " did not converge to full readable RF");
        }
    }

    private static void assertSealedReplicasAreByteIdentical(Messages.LookupFileResp lookup) throws Exception {
        for (Messages.ChunkInfo chunk : lookup.chunks()) {
            Set<String> hashes = new HashSet<>();
            int readable = 0;
            AssertionError fetchFailures = new AssertionError("unable to fetch replicas for " + chunk.chunkId());
            for (Messages.Replica replica : chunk.replicas()) {
                try {
                    hashes.add(sha256(fetchWholeChunk(replica, chunk)));
                    readable++;
                } catch (Exception | AssertionError e) {
                    fetchFailures.addSuppressed(e);
                }
            }
            if (readable < lookup.writePolicy().ackQuorum()) {
                AssertionError quorumFailure = new AssertionError("sealed chunk " + chunk.chunkId()
                        + " has " + readable + " fetchable replicas, below ack quorum "
                        + lookup.writePolicy().ackQuorum());
                quorumFailure.addSuppressed(fetchFailures);
                throw quorumFailure;
            }
            assertEquals(1, hashes.size(), "replicas of " + chunk.chunkId() + " diverge");
        }
    }

    private static byte[] fetchWholeChunk(Messages.Replica replica, Messages.ChunkInfo chunk) throws Exception {
        String[] hp = replica.endpoint().split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]),
                ScpClient.KIND_TOOL, "consistency-fetch")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            long expectedFileLength = -1;
            long offset = 0;
            while (expectedFileLength < 0 || offset < expectedFileLength) {
                var frame = direct.callFrame(Opcode.FETCH_CHUNK,
                        new Messages.FetchChunk(chunk.chunkId(), offset, FETCH_CHUNK_BYTES).encode(), null,
                        CALL_TIMEOUT_MS);
                ByteBuffer h = frame.headerSlice();
                Resp.check(h);
                Messages.FetchResp resp = Messages.FetchResp.decode(h);
                assertEquals(ChunkState.SEALED, resp.state(),
                        "fetch source " + replica.nodeId() + " is not sealed for " + chunk.chunkId());
                if (expectedFileLength < 0) {
                    expectedFileLength = resp.fileLength();
                    assertTrue(expectedFileLength >= ChunkFormats.HEADER_SIZE + ChunkFormats.TRAILER_SIZE,
                            "raw chunk file too short for " + chunk.chunkId());
                    assertTrue(expectedFileLength <= Integer.MAX_VALUE,
                            "raw chunk file too large for verifier: " + expectedFileLength);
                } else {
                    assertEquals(expectedFileLength, resp.fileLength(),
                            "raw chunk file length changed while fetching " + chunk.chunkId());
                }
                int n = frame.payloadLength();
                assertTrue(n <= FETCH_CHUNK_BYTES,
                        "fetch returned " + n + " bytes above requested limit for " + chunk.chunkId());
                assertTrue(n <= expectedFileLength - offset,
                        "fetch returned bytes past EOF for " + chunk.chunkId());
                if (n == 0 && offset < expectedFileLength) {
                    throw new AssertionError("short fetch at " + offset + " for " + chunk.chunkId());
                }
                byte[] bytes = new byte[n];
                frame.payloadSlice().get(bytes);
                out.write(bytes, 0, bytes.length);
                offset += n;
            }
            byte[] raw = out.toByteArray();
            assertEquals(expectedFileLength, raw.length,
                    "fetched raw chunk length differs for " + chunk.chunkId());
            assertRawChunkImageMatchesMetadata(raw, chunk);
            return raw;
        }
    }

    private static void assertRawChunkImageMatchesMetadata(byte[] raw, Messages.ChunkInfo chunk) {
        byte[] header = java.util.Arrays.copyOfRange(raw, 0, ChunkFormats.HEADER_SIZE);
        assertEquals(chunk.chunkId(), ChunkFormats.Header.decode(header).chunkId(),
                "raw chunk header id differs for " + chunk.chunkId());

        int trailerOffset = raw.length - ChunkFormats.TRAILER_SIZE;
        byte[] trailerBytes = java.util.Arrays.copyOfRange(raw, trailerOffset, raw.length);
        ChunkFormats.Trailer trailer = ChunkFormats.Trailer.decode(trailerBytes);
        assertEquals(chunk.length(), trailer.dataLength(),
                "raw chunk trailer length differs for " + chunk.chunkId());
        assertEquals(chunk.crc(), Crc.of(raw, ChunkFormats.HEADER_SIZE, Math.toIntExact(chunk.length())),
                "raw chunk data crc differs for " + chunk.chunkId());
        assertEquals(chunk.crc(), trailer.dataCrc(),
                "raw chunk trailer crc differs for " + chunk.chunkId());

        long expectedFooterStart = ChunkFormats.HEADER_SIZE + chunk.length();
        assertEquals(expectedFooterStart, trailer.footerStart(),
                "raw chunk footer start differs for " + chunk.chunkId());
        assertTrue(trailer.footerStart() <= trailerOffset,
                "raw chunk footer overlaps trailer for " + chunk.chunkId());
        int footerLength = Math.toIntExact(trailerOffset - trailer.footerStart());
        assertEquals(trailer.footerCrc(), Crc.of(raw, Math.toIntExact(trailer.footerStart()), footerLength),
                "raw chunk footer crc differs for " + chunk.chunkId());
    }

    private static String sha256(byte[] bytes) throws Exception {
        return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}
