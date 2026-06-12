package io.strata.it;

import io.strata.client.StrataClient;
import io.strata.client.StrataFile;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
        List<CompletableFuture<StrataFile.AppendAck>> futures = new ArrayList<>(n);
        List<byte[]> payloads = new ArrayList<>(n);
        for (int i = from; i < from + n; i++) {
            byte[] p = payload(i);
            payloads.add(p);
            futures.add(appender.append(ByteBuffer.wrap(p)));
        }
        for (int i = 0; i < n; i++) {
            StrataFile.AppendAck ack = futures.get(i).join();
            recordAcked(payloads.get(i));
            lastEnd = ack.endOffset();
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

    synchronized int ackedCount() {
        return ackedPayloads.size();
    }

    /** Asserts the file's prefix equals every acked byte, in order. */
    synchronized void verifyAckedPrefix(StrataClient store, io.strata.common.FileId fileId) {
        byte[] expectedBytes = expected.toString().getBytes(StandardCharsets.UTF_8);
        byte[] actual = readAll(store, fileId, expectedBytes.length);
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

    static byte[] readAll(StrataClient store, io.strata.common.FileId fileId, int atLeast) {
        try (StrataFile.Reader reader = store.openById(fileId).openForRead()) {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            long offset = 0;
            int idleRounds = 0;
            while (idleRounds < 3) {
                StrataFile.ReadResult r = reader.read(offset, 1 << 20);
                if (r.data().length > 0) {
                    out.write(r.data(), 0, r.data().length);
                    offset += r.data().length;
                    idleRounds = 0;
                } else if (r.endOfFile() || out.size() >= atLeast) {
                    break;
                } else {
                    idleRounds++;
                    reader.refresh();
                }
            }
            return out.toByteArray();
        }
    }
}
