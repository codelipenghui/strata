package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
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

    // Parallel coverage (the real shapes the design supports): writes fan out across many files
    // (single writer per file), reads fan out both across files AND across many readers on ONE
    // file (no single-reader-per-file limit). Defaults are modest so the smoke run stays quick;
    // raise perf.par.* for a real saturation sweep.
    private static final int PAR_FILES = Integer.getInteger("perf.par.files", 8);
    private static final int PAR_RECORDS = Integer.getInteger("perf.par.records", 8_000);
    private static final int PAR_READERS = Integer.getInteger("perf.par.readers", 8);
    private static final int PAR_READ_PASSES = Integer.getInteger("perf.par.readPasses", 4);

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

                var replicate = runWrite(client, new StrataClient.FileSpec("test", "/perf-rep"), RECORDS, WINDOW);
                printWrite("write ack-on-replicate (window " + WINDOW + ") ", replicate.result());

                ReadResult read = runRead(client, replicate.fileId());
                printRead("read  sequential 64KB ", read);

                // pathology guards, deliberately loose (CI machines vary wildly)
                assertTrue(p(replicate.result().latenciesNanos(), 99) < 1_000_000_000L,
                        "replicate-mode write p99 above 1s — pathological");
                assertTrue(read.throughputMBs() > 5.0,
                        "sequential read below 5 MB/s — pathological");
            }
        }

        try (MiniCluster cluster = new MiniCluster(3)) {
            ClientConfig cfg = ClientConfig.of(cluster.metaEndpoint()).withChunkRollBytes(64L << 20);
            try (StrataClient client = StrataClient.connect(cfg)) {
                var fsync = runWrite(client, new StrataClient.FileSpec("test", "/perf-fsync",
                                StrataClient.WritePolicy.fsync(3, 2)),
                        FSYNC_RECORDS, FSYNC_WINDOW);
                printWrite("write ack-on-fsync     (window " + FSYNC_WINDOW + ")  ", fsync.result());
                assertTrue(p(fsync.result().latenciesNanos(), 99) < 2_000_000_000L,
                        "fsync-mode write p99 above 2s — pathological");
            }
        }
    }

    /**
     * Parallel coverage. The single-stream numbers above are latency-bound (one op in flight) and
     * do NOT reflect what the design sustains: writes scale by fanning out across files (single
     * writer per file), reads scale by fanning out streams — across files AND across many readers
     * on the SAME file (no single-reader-per-file limit). These aggregate-throughput numbers are
     * the ones to compare against hardware bandwidth.
     */
    @Test
    void parallelWriteAndRead() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3)) {
            ClientConfig cfg = ClientConfig.of(cluster.metaEndpoint()).withChunkRollBytes(64L << 20);
            try (StrataClient client = StrataClient.connect(cfg)) {
                System.out.printf(Locale.ROOT,
                        "%n=== Strata v0 perf parallel: %d files x %d records x %d B, window %d, "
                                + "%d readers, %d read passes, 3 nodes in-process ===%n",
                        PAR_FILES, PAR_RECORDS, RECORD_SIZE, WINDOW, PAR_READERS, PAR_READ_PASSES);

                // Writes fan out across files; one appender per file (single-writer-per-file).
                ParallelWriteResult w = runParallelWrite(client, PAR_FILES, PAR_RECORDS, WINDOW);
                printWrite("parallel write " + PAR_FILES + " files (1 writer each)   ",
                        new WriteResult(w.latencies(), w.throughputMBs()));

                // Many readers on ONE file — the no-single-reader-per-file property, concurrent
                // zero-copy region transfer from a single sealed chunk.
                ReadResult single = runParallelRead(client, List.of(w.fileIds().get(0)),
                        PAR_READERS, PAR_READ_PASSES, w.perFileBytes());
                printRead("parallel read  1 file  x " + PAR_READERS + " readers     ", single);

                // One reader per file across all files — independent chunks/streams in parallel.
                ReadResult multi = runParallelRead(client, w.fileIds(),
                        w.fileIds().size(), PAR_READ_PASSES, w.perFileBytes());
                printRead("parallel read  " + PAR_FILES + " files x " + PAR_FILES + " readers     ", multi);

                // pathology guards, deliberately loose (machine + contention vary wildly)
                assertTrue(w.throughputMBs() > 10.0,
                        "parallel multi-file write below 10 MB/s — pathological");
                assertTrue(single.throughputMBs() > 50.0,
                        "parallel single-file read below 50 MB/s — pathological");
                assertTrue(multi.throughputMBs() > 50.0,
                        "parallel multi-file read below 50 MB/s — pathological");
            }
        }
    }

    private record ParallelWriteResult(List<FileId> fileIds, long[] latencies,
                                       double throughputMBs, long perFileBytes) {}

    /** F files, one appender each, all driven concurrently; aggregate ack throughput. */
    private ParallelWriteResult runParallelWrite(StrataClient client, int files,
                                                 int recordsPerFile, int window) throws Exception {
        byte[] payload = new byte[RECORD_SIZE];
        ThreadLocalRandom.current().nextBytes(payload);

        List<FileId> ids = new ArrayList<>();
        List<StrataFile.Appender> appenders = new ArrayList<>();
        for (int f = 0; f < files; f++) {
            FileId id = client.create(new StrataClient.FileSpec("test", "/par-write-" + f)).id();
            ids.add(id);
            appenders.add(client.openById(id).openForAppend());
        }
        try {
            int warm = Math.min(WARMUP, recordsPerFile);
            runConcurrent(files, f ->
                    await(appendWindowed(appenders.get(f), payload, warm, null, window)));

            long[][] perFile = new long[files][];
            long start = System.nanoTime();
            runConcurrent(files, f -> {
                long[] lat = new long[recordsPerFile];
                perFile[f] = lat;
                await(appendWindowed(appenders.get(f), payload, recordsPerFile, lat, window));
            });
            long elapsed = System.nanoTime() - start;

            long sealedLen = 0;
            for (StrataFile.Appender ap : appenders) {
                sealedLen = ap.seal().sealedLength();
            }
            double mbs = ((double) files * recordsPerFile * RECORD_SIZE / (1 << 20)) / (elapsed / 1e9);
            return new ParallelWriteResult(ids, merge(perFile), mbs, sealedLen);
        } finally {
            for (StrataFile.Appender ap : appenders) {
                try {
                    ap.close();
                } catch (RuntimeException ignored) {
                }
            }
        }
    }

    /**
     * {@code readers} reader streams driven concurrently, each assigned a file round-robin and
     * reading it fully {@code passes} times. Readers are pre-opened so connection setup is excluded
     * from the timed window; aggregate throughput = all bytes read / wall-clock.
     */
    private ReadResult runParallelRead(StrataClient client, List<FileId> files, int readers,
                                       int passes, long perFileBytes) throws Exception {
        List<StrataFile.Reader> rds = new ArrayList<>();
        for (int r = 0; r < readers; r++) {
            rds.add(client.openById(files.get(r % files.size())).openForRead());
        }
        try {
            long[][] perReader = new long[readers][];
            long[] bytesPerReader = new long[readers];
            int cap = (int) (passes * (perFileBytes / READ_SIZE + 2)) + 2;
            long start = System.nanoTime();
            runConcurrent(readers, r -> {
                StrataFile.Reader reader = rds.get(r);
                long[] lat = new long[cap];
                int n = 0;
                long bytes = 0;
                for (int pass = 0; pass < passes; pass++) {
                    long offset = 0;
                    while (true) {
                        long t0 = System.nanoTime();
                        StrataFile.ReadResult rr = reader.read(offset, READ_SIZE);
                        if (rr.data().length == 0) break;
                        if (n < lat.length) lat[n++] = System.nanoTime() - t0;
                        bytes += rr.data().length;
                        offset += rr.data().length;
                        if (rr.endOfFile()) break;
                    }
                }
                perReader[r] = Arrays.copyOf(lat, n);
                bytesPerReader[r] = bytes;
            });
            long elapsed = System.nanoTime() - start;
            double mbs = ((double) Arrays.stream(bytesPerReader).sum() / (1 << 20)) / (elapsed / 1e9);
            return new ReadResult(merge(perReader), mbs);
        } finally {
            for (StrataFile.Reader reader : rds) {
                try {
                    reader.close();
                } catch (RuntimeException ignored) {
                }
            }
        }
    }

    /** Runs body(0..n-1) concurrently on virtual threads; rethrows the first failure. */
    private static void runConcurrent(int n, ConcurrentBody body) throws Exception {
        try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                final int idx = i;
                futures.add(exec.submit(() -> {
                    body.run(idx);
                    return null;
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
        }
    }

    @FunctionalInterface
    private interface ConcurrentBody {
        void run(int index) throws Exception;
    }

    private static long[] merge(long[][] parts) {
        int total = 0;
        for (long[] part : parts) {
            if (part != null) total += part.length;
        }
        long[] out = new long[total];
        int i = 0;
        for (long[] part : parts) {
            if (part != null) {
                System.arraycopy(part, 0, out, i, part.length);
                i += part.length;
            }
        }
        return out;
    }

    private record WriteRun(FileId fileId, WriteResult result) {}

    private WriteRun runWrite(StrataClient client, StrataClient.FileSpec spec,
                              int records, int window) throws Exception {
        FileId fileId = client.create(spec).id();
        byte[] payload = new byte[RECORD_SIZE];
        java.util.concurrent.ThreadLocalRandom.current().nextBytes(payload);

        try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
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
