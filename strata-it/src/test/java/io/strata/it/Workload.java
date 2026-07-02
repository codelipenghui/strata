package io.strata.it;

import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The acked-data recorder/verifier (IMPLEMENTATION_PLAN invariant 1): every payload whose append
 * future completed is recorded; verification reads the file back and asserts the acked prefix
 * matches byte-for-byte. Unacked tails may or may not survive — acked data must.
 */
final class Workload {
    private final List<byte[]> ackedPayloads = new ArrayList<>();
    private final StringBuilder expected = new StringBuilder();

    static byte[] payload(int i) {
        return ("record-%06d|".formatted(i)).getBytes(StandardCharsets.UTF_8);
    }

    /** Appends n records, waiting for each ack (call from a virtual thread for pipelining tests). */
    long appendAcked(StrataFile.Appender appender, int from, int n) {
        long lastEnd = -1;
        List<CompletableFuture<Long>> futures = new ArrayList<>(n);
        List<byte[]> payloads = new ArrayList<>(n);
        for (int i = from; i < from + n; i++) {
            byte[] p = payload(i);
            payloads.add(p);
            futures.add(appender.append(ByteBuffer.wrap(p)));
        }
        for (int i = 0; i < n; i++) {
            long ack = futures.get(i).join();
            recordAcked(payloads.get(i));
            lastEnd = ack;
        }
        return lastEnd;
    }

    synchronized void recordAcked(byte[] payload) {
        ackedPayloads.add(payload);
        expected.append(new String(payload, StandardCharsets.UTF_8));
    }

    synchronized long ackedBytes() {
        return expected.length();
    }

    synchronized byte[] expectedBytes() {
        return expected.toString().getBytes(StandardCharsets.UTF_8);
    }

    synchronized String ackedSha256() {
        try {
            byte[] expectedBytes = expected.toString().getBytes(StandardCharsets.UTF_8);
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(expectedBytes));
        } catch (Exception e) {
            throw new IllegalStateException("sha256 unavailable", e);
        }
    }

    synchronized int ackedCount() {
        return ackedPayloads.size();
    }

    long appendAckedAndVerifyOffsets(StrataFile.Appender appender, int from, int n) {
        long expectedEnd = ackedBytes();
        long lastEnd = -1;
        List<CompletableFuture<Long>> futures = new ArrayList<>(n);
        List<byte[]> payloads = new ArrayList<>(n);
        for (int i = from; i < from + n; i++) {
            byte[] p = payload(i);
            payloads.add(p);
            futures.add(appender.append(ByteBuffer.wrap(p)));
        }
        for (int i = 0; i < n; i++) {
            byte[] payload = payloads.get(i);
            expectedEnd += payload.length;
            long ack = futures.get(i).join();
            if (ack != expectedEnd) {
                throw new AssertionError("append returned end offset " + ack
                        + " but expected " + expectedEnd + " for record " + (from + i));
            }
            recordAcked(payload);
            lastEnd = ack;
        }
        return lastEnd;
    }

    /** Asserts the file's prefix equals every acked byte, in order. */
    synchronized void verifyAckedPrefix(StrataClient store, StrataNamespace namespace, FileId fileId) {
        byte[] expectedBytes = expected.toString().getBytes(StandardCharsets.UTF_8);
        byte[] actual = readAll(store, namespace, fileId, expectedBytes.length);
        if (actual.length < expectedBytes.length) {
            throw new AssertionError("ACKED DATA LOST: acked " + expectedBytes.length
                    + " bytes but only " + actual.length + " readable");
        }
        for (int i = 0; i < expectedBytes.length; i++) {
            if (expectedBytes[i] != actual[i]) {
                throw new AssertionError("ACKED DATA CORRUPT at offset " + i);
            }
        }
    }

    static byte[] readAll(StrataClient store, StrataNamespace namespace, FileId fileId, int atLeast) {
        try (StrataFile.Reader reader = store.openById(namespace, fileId).openForRead()) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            long offset = 0;
            int idleRounds = 0;
            while (idleRounds < 3) {
                try (StrataFile.ReadResult r = reader.read(offset, 1 << 20)) {
                    int n = r.length();
                    if (n > 0) {
                        byte[] tmp = new byte[n];
                        r.buffer().get(tmp);
                        out.write(tmp, 0, n);
                        offset += n;
                        idleRounds = 0;
                    } else if (r.endOfFile() || out.size() >= atLeast) {
                        break;
                    } else {
                        idleRounds++;
                        reader.refresh();
                    }
                }
            }
            return out.toByteArray();
        }
    }
}
