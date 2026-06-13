package io.strata.it;

import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Byte-oriented acked-prefix verifier for randomized integration/fault tests. */
final class BinaryWorkload {
    private final ByteArrayOutputStream expected = new ByteArrayOutputStream();
    private int nextRecord;

    int appendRandomBatch(StrataFile.Appender appender, Random random, int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            byte[] payload = randomPayload(random, nextRecord++);
            appender.append(ByteBuffer.wrap(payload)).join();
            expected.write(payload, 0, payload.length);
        }
        return nextRecord;
    }

    int ackedBytes() {
        return expected.size();
    }

    String ackedSha256() {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(expected.toByteArray()));
        } catch (Exception e) {
            throw new IllegalStateException("sha256 unavailable", e);
        }
    }

    void verifyOpenReadIsAckedPrefix(StrataClient client, FileId fileId, String context) {
        try (StrataFile.Reader reader = client.openById(fileId).openForRead()) {
            verifyReaderReadIsAckedPrefix(reader, context + " open");
        }
    }

    void verifyReaderReadIsAckedPrefix(StrataFile.Reader reader, String context) {
        byte[] expectedBytes = expected.toByteArray();
        StrataFile.ReadResult result;
        try {
            result = reader.read(0, 1 << 20);
        } catch (RuntimeException e) {
            throw new AssertionError(context + " read failed", e);
        }
        assertTrue(result.data().length <= expectedBytes.length,
                context + " read exposed " + result.data().length
                        + " bytes above acked " + expectedBytes.length);
        assertArrayEquals(Arrays.copyOf(expectedBytes, result.data().length), result.data(),
                context + " read returned a non-prefix");
    }

    void verifySealedAckedPrefix(StrataClient client, FileId fileId, String context) {
        byte[] expectedBytes = expected.toByteArray();
        byte[] actual = Workload.readAll(client, fileId, expectedBytes.length);
        assertTrue(actual.length >= expectedBytes.length,
                context + " read " + actual.length + " < acked " + expectedBytes.length);
        assertArrayEquals(expectedBytes, Arrays.copyOf(actual, expectedBytes.length),
                context + " acked prefix changed after recovery");
    }

    private byte[] randomPayload(Random random, int record) {
        byte[] payload = new byte[16 + random.nextInt(96)];
        random.nextBytes(payload);
        ByteBuffer.wrap(payload).putInt(record);
        return payload;
    }
}
