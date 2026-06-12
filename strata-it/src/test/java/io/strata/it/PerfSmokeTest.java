package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Perf smoke (tech design §16 performance gates, first data point — not a rigorous benchmark):
 * write-path ack latency (p50/p95/p99/max) and throughput for BOTH durability modes (§5.3 says
 * benchmarks publish both), plus sequential read throughput. In-process 3-node cluster on local
 * disk; numbers are machine-relative. Assertions are loose pathology guards only.
 *
 * Run: mvn -pl strata-it test -Dtest=PerfSmokeTest "-DexcludedGroups="
 * Tunables: -Dperf.records=8000 -Dperf.recordSize=1024 -Dperf.window=256
 */
@Tag("perf")
class PerfSmokeTest {

    private static final int RECORDS = Integer.getInteger("perf.records", 8_000);
    private static final int RECORD_SIZE = Integer.getInteger("perf.recordSize", 1_024);
    private static final int WINDOW = Integer.getInteger("perf.window", 256);
    // group commit (§5.3): replicas coalesce forces, so fsync mode sustains the same deep
    // pipeline as replicate mode — ack latency ~1-2 force times regardless of window depth
    private static final int FSYNC_WINDOW = Integer.getInteger("perf.fsyncWindow", WINDOW);
    private static final int FSYNC_RECORDS = Integer.getInteger("perf.fsyncRecords", 4_000);
    private static final int WARMUP = 1_000;
    private static final int READ_SIZE = 64 * 1024;

    record WriteResult(long[] latenciesNanos, double throughputMBs) {}

    record ReadResult(long[] latenciesNanos, double throughputMBs) {}

    @Test
    void writeAndReadSmoke() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3)) {
            ClientConfig cfg = ClientConfig.of(cluster.metaEndpoint()).withChunkRollBytes(64L << 20);
            try (StrataClient client = StrataClient.connect(cfg)) {

                System.out.printf(Locale.ROOT,
                        "%n=== Strata v0 perf smoke: %d records x %d B, window %d, 3 nodes in-process ===%n",
                        RECORDS, RECORD_SIZE, WINDOW);

                var replicate = runWrite(client,
                        new StrataClient.FileSpec("test", "/perf-rep", (byte) 0, (byte) 0, (byte) 0), RECORDS, WINDOW);
                printWrite("write ack-on-replicate (window " + WINDOW + ") ", replicate.result());
                var fsync = runWrite(client,
                        new StrataClient.FileSpec("test", "/perf-fsync", (byte) 0, (byte) 0, (byte) 1), FSYNC_RECORDS, FSYNC_WINDOW);
                printWrite("write ack-on-fsync     (window " + FSYNC_WINDOW + ")  ", fsync.result());

                ReadResult read = runRead(client, replicate.fileId());
                printRead("read  sequential 64KB ", read);

                // pathology guards, deliberately loose (CI machines vary wildly)
                assertTrue(p(replicate.result().latenciesNanos(), 99) < 1_000_000_000L,
                        "replicate-mode write p99 above 1s — pathological");
                assertTrue(p(fsync.result().latenciesNanos(), 99) < 2_000_000_000L,
                        "fsync-mode write p99 above 2s — pathological");
                assertTrue(read.throughputMBs() > 5.0,
                        "sequential read below 5 MB/s — pathological");
            }
        }
    }

    private record WriteRun(FileId fileId, WriteResult result) {}

    private WriteRun runWrite(StrataClient client, StrataClient.FileSpec spec,
                              int records, int window) throws Exception {
        FileId fileId = client.create(spec).id();
        byte[] payload = new byte[RECORD_SIZE];
        java.util.concurrent.ThreadLocalRandom.current().nextBytes(payload);

        try (StrataFile.Appender appender = client.openById(fileId).openForAppend(1)) {
            // warmup (not measured)
            await(appendWindowed(appender, payload, Math.min(WARMUP, records), null, window));

            long[] latencies = new long[records];
            long start = System.nanoTime();
            await(appendWindowed(appender, payload, records, latencies, window));
            long elapsed = System.nanoTime() - start;
            appender.seal();

            double mbs = ((double) records * RECORD_SIZE / (1 << 20)) / (elapsed / 1e9);
            return new WriteRun(fileId, new WriteResult(latencies, mbs));
        }
    }

    /** Pipelined appends with a bounded in-flight window; per-append ack latency captured. */
    private CompletableFuture<?>[] appendWindowed(StrataFile.Appender appender, byte[] payload,
                                                  int count, long[] latenciesOut,
                                                  int windowSize) throws InterruptedException {
        Semaphore window = new Semaphore(windowSize);
        CompletableFuture<?>[] futures = new CompletableFuture[count];
        for (int i = 0; i < count; i++) {
            window.acquire();
            final int idx = i;
            final long submitted = System.nanoTime();
            futures[i] = appender.append(ByteBuffer.wrap(payload)).whenComplete((ack, err) -> {
                if (latenciesOut != null) latenciesOut[idx] = System.nanoTime() - submitted;
                window.release();
                if (err != null) throw new RuntimeException(err);
            });
        }
        return futures;
    }

    private void await(CompletableFuture<?>[] futures) throws Exception {
        CompletableFuture.allOf(futures).get(10, TimeUnit.MINUTES);
    }

    private ReadResult runRead(StrataClient client, FileId fileId) {
        try (StrataFile.Reader reader = client.openById(fileId).openForRead()) {
            long[] latencies = new long[4096];
            int reads = 0;
            long bytes = 0;
            long start = System.nanoTime();
            long offset = 0;
            while (true) {
                long t0 = System.nanoTime();
                StrataFile.ReadResult r = reader.read(offset, READ_SIZE);
                if (r.data().length == 0) break;
                if (reads < latencies.length) latencies[reads] = System.nanoTime() - t0;
                reads++;
                bytes += r.data().length;
                offset += r.data().length;
                if (r.endOfFile()) break;
            }
            long elapsed = System.nanoTime() - start;
            double mbs = ((double) bytes / (1 << 20)) / (elapsed / 1e9);
            return new ReadResult(Arrays.copyOf(latencies, Math.min(reads, latencies.length)), mbs);
        }
    }

    private static long p(long[] latencies, int percentile) {
        long[] sorted = latencies.clone();
        Arrays.sort(sorted);
        int idx = (int) Math.ceil(percentile / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }

    private static void printWrite(String label, WriteResult r) {
        System.out.printf(Locale.ROOT,
                "%s  p50=%6.2fms  p95=%6.2fms  p99=%6.2fms  max=%7.2fms  throughput=%7.1f MB/s%n",
                label, ms(p(r.latenciesNanos(), 50)), ms(p(r.latenciesNanos(), 95)),
                ms(p(r.latenciesNanos(), 99)), ms(p(r.latenciesNanos(), 100)), r.throughputMBs());
    }

    private static void printRead(String label, ReadResult r) {
        System.out.printf(Locale.ROOT,
                "%s  p50=%6.2fms  p95=%6.2fms  p99=%6.2fms  max=%7.2fms  throughput=%7.1f MB/s%n",
                label, ms(p(r.latenciesNanos(), 50)), ms(p(r.latenciesNanos(), 95)),
                ms(p(r.latenciesNanos(), 99)), ms(p(r.latenciesNanos(), 100)), r.throughputMBs());
    }

    private static double ms(long nanos) {
        return nanos / 1e6;
    }
}
