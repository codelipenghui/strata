package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
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
    // Files use FileSpec's default WritePolicy (RF=3, ackQuorum=2). The "total throughput with
    // replicas" — logical x RF — is what should approach the disk's raw write bandwidth, since
    // all replicas land on disk. Measure the disk ceiling separately (e.g. a sequential
    // FileChannel write+fsync micro-benchmark) and compare against the physical number below.
    private static final int REPLICATION_FACTOR = 3;
    // Latency profile measures each op with ONE op in flight — the latency FLOOR (no queueing),
    // which is what a latency-sensitive caller sees. Swept across durability mode, record size,
    // read size, and sealed (zero-copy) vs open (materialized) read paths.
    private static final int LAT_SAMPLES = Integer.getInteger("perf.lat.samples", 1_000);

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
                // Measure BOTH durability modes. Only ack-on-fsync forces the bytes to disk
                // BEFORE acking, so only its physical number is apples-to-apples with a forced
                // disk write ceiling. Ack-on-replicate acks on page-cache quorum and can briefly
                // run ABOVE the sustained disk rate by buffering — do not compare it to a forced
                // ceiling. The replicate files also feed the read test below.
                ParallelWriteResult wRep = runParallelWrite(client, PAR_FILES, PAR_RECORDS, WINDOW,
                        StrataClient.WritePolicy.DEFAULT, "/par-rep-");
                printParallelWrite("parallel write " + PAR_FILES + " files, ack-on-replicate", wRep);

                ParallelWriteResult wFsync = runParallelWrite(client, PAR_FILES, PAR_RECORDS, WINDOW,
                        StrataClient.WritePolicy.fsync(REPLICATION_FACTOR, 2), "/par-fsync-");
                printParallelWrite("parallel write " + PAR_FILES + " files, ack-on-fsync   ", wFsync);

                // Many readers on ONE file — the no-single-reader-per-file property, concurrent
                // zero-copy region transfer from a single sealed chunk.
                ReadResult single = runParallelRead(client, List.of(wRep.fileIds().get(0)),
                        PAR_READERS, PAR_READ_PASSES, wRep.perFileBytes());
                printRead("parallel read  1 file  x " + PAR_READERS + " readers     ", single);

                // One reader per file across all files — independent chunks/streams in parallel.
                ReadResult multi = runParallelRead(client, wRep.fileIds(),
                        wRep.fileIds().size(), PAR_READ_PASSES, wRep.perFileBytes());
                printRead("parallel read  " + PAR_FILES + " files x " + PAR_FILES + " readers     ", multi);

                // pathology guards, deliberately loose (machine + contention vary wildly)
                assertTrue(wRep.throughputMBs() > 10.0,
                        "parallel ack-on-replicate write below 10 MB/s — pathological");
                assertTrue(wFsync.throughputMBs() > 5.0,
                        "parallel ack-on-fsync write below 5 MB/s — pathological");
                assertTrue(single.throughputMBs() > 50.0,
                        "parallel single-file read below 50 MB/s — pathological");
                assertTrue(multi.throughputMBs() > 50.0,
                        "parallel multi-file read below 50 MB/s — pathological");
            }
        }
    }

    /**
     * Latency profile across workloads, each measured with ONE op in flight (the floor a
     * latency-sensitive caller sees, with no pipeline queueing). Write latency is swept over
     * {durability mode} x {record size}; read latency over {read size} on a sealed (zero-copy)
     * chunk, plus an open-chunk tail read (materialized under the chunk lock). Throughput tests
     * above show the loaded/pipelined latency; this isolates the unloaded per-op cost.
     */
    @Test
    void latencyProfile() throws Exception {
        int[] writeSizes = {1_024, 65_536, 1 << 20};
        int[] readSizes = {4_096, 65_536, 262_144, 1 << 20};
        try (MiniCluster cluster = new MiniCluster(3)) {
            ClientConfig cfg = ClientConfig.of(cluster.metaEndpoint()).withChunkRollBytes(64L << 20);
            try (StrataClient client = StrataClient.connect(cfg)) {
                System.out.printf(Locale.ROOT,
                        "%n=== Strata v0 latency profile (1 op in flight = floor), 3 nodes in-process ===%n");

                System.out.println("-- write ack latency: durability mode x record size --");
                for (int size : writeSizes) {
                    printLat(String.format(Locale.ROOT, "  write replicate %7dB", size),
                            writeLatency(client, "/lat-rep-" + size, StrataClient.WritePolicy.DEFAULT, size));
                }
                for (int size : writeSizes) {
                    printLat(String.format(Locale.ROOT, "  write fsync     %7dB", size),
                            writeLatency(client, "/lat-fsync-" + size,
                                    StrataClient.WritePolicy.fsync(REPLICATION_FACTOR, 2), size));
                }

                System.out.println("-- read latency: read size (sealed chunk, zero-copy region) --");
                long sealedBytes = 60L << 20;
                FileId sealed = buildSealedFile(client, "/lat-read", 1 << 20, 60);
                long[] read64k = null;
                for (int size : readSizes) {
                    long[] lat = readLatency(client, sealed, size, sealedBytes);
                    if (size == 65_536) read64k = lat;
                    printLat(String.format(Locale.ROOT, "  read %9dB", size), lat);
                }

                System.out.println("-- read latency: open-chunk tail (materialized under lock) --");
                printLat("  read     65536B (open) ", openTailReadLatency(client, "/lat-open", 65_536));

                // pathology guard, deliberately loose — asserts on the 64KB run above, not a re-measure
                assertTrue(p(read64k, 99) < 200_000_000L,
                        "sealed 64KB read p99 above 200ms — pathological");
            }
        }
    }

    /** Per-append ack latency with one append in flight (no queueing), then seal. */
    private long[] writeLatency(StrataClient client, String path, StrataClient.WritePolicy policy,
                                int recordSize) throws Exception {
        int samples = Math.min(LAT_SAMPLES, Math.max(200, (256 << 20) / recordSize));
        int warm = Math.min(samples / 5, 200);
        byte[] payload = new byte[recordSize];
        ThreadLocalRandom.current().nextBytes(payload);
        FileId id = client.create(new StrataClient.FileSpec("test", path, policy)).id();
        try (StrataFile.Appender ap = client.openById(StrataNamespace.of("test"), id).openForAppend()) {
            await(appendWindowed(ap, payload, warm, null, 1));
            long[] lat = new long[samples];
            await(appendWindowed(ap, payload, samples, lat, 1));
            ap.seal();
            return lat;
        }
    }

    private FileId buildSealedFile(StrataClient client, String path, int recordSize, int records)
            throws Exception {
        byte[] payload = new byte[recordSize];
        ThreadLocalRandom.current().nextBytes(payload);
        FileId id = client.create(new StrataClient.FileSpec("test", path)).id();
        try (StrataFile.Appender ap = client.openById(StrataNamespace.of("test"), id).openForAppend()) {
            await(appendWindowed(ap, payload, records, null, 16));
            ap.seal();
        }
        return id;
    }

    /** Single-read latency at a fixed read size, cycling offsets across the file. */
    private long[] readLatency(StrataClient client, FileId fileId, int readSize, long fileBytes) {
        int warm = Math.min(LAT_SAMPLES / 5, 200);
        long span = Math.max(1, fileBytes - readSize);
        try (StrataFile.Reader reader = client.openById(StrataNamespace.of("test"), fileId).openForRead()) {
            for (int i = 0; i < warm; i++) {
                reader.read((long) i * readSize % span, readSize);
            }
            long[] lat = new long[LAT_SAMPLES];
            for (int i = 0; i < lat.length; i++) {
                long off = (long) i * readSize % span;
                long t0 = System.nanoTime();
                reader.read(off, readSize);
                lat[i] = System.nanoTime() - t0;
            }
            return lat;
        }
    }

    /** Read latency on an OPEN (unsealed) chunk — exercises the materialize-under-lock path. */
    private long[] openTailReadLatency(StrataClient client, String path, int readSize) throws Exception {
        byte[] payload = new byte[readSize];
        ThreadLocalRandom.current().nextBytes(payload);
        FileId id = client.create(new StrataClient.FileSpec("test", path)).id();
        StrataFile.Appender ap = client.openById(StrataNamespace.of("test"), id).openForAppend();
        try {
            await(appendWindowed(ap, payload, 256, null, 32)); // fill a durable tail, do NOT seal
            long durable = ap.durableOffset();
            return readLatency(client, id, readSize, durable);
        } finally {
            ap.close();
        }
    }

    private static void printLat(String label, long[] lat) {
        System.out.printf(Locale.ROOT,
                "%s  p50=%7.3fms  p95=%7.3fms  p99=%7.3fms  max=%8.3fms  (n=%d)%n",
                label, ms(p(lat, 50)), ms(p(lat, 95)), ms(p(lat, 99)), ms(p(lat, 100)), lat.length);
    }

    private record ParallelWriteResult(List<FileId> fileIds, long[] latencies,
                                       double throughputMBs, long perFileBytes) {}

    /** F files, one appender each, all driven concurrently; aggregate ack throughput. */
    private ParallelWriteResult runParallelWrite(StrataClient client, int files, int recordsPerFile,
                                                 int window, StrataClient.WritePolicy policy,
                                                 String pathPrefix) throws Exception {
        byte[] payload = new byte[RECORD_SIZE];
        ThreadLocalRandom.current().nextBytes(payload);

        List<FileId> ids = new ArrayList<>();
        List<StrataFile.Appender> appenders = new ArrayList<>();
        for (int f = 0; f < files; f++) {
            FileId id = client.create(new StrataClient.FileSpec("test", pathPrefix + f, policy)).id();
            ids.add(id);
            appenders.add(client.openById(StrataNamespace.of("test"), id).openForAppend());
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
            rds.add(client.openById(StrataNamespace.of("test"), files.get(r % files.size())).openForRead());
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
                    ReadPass p = readToEnd(reader, lat, n);
                    bytes += p.bytes();
                    n = p.latencyCount();
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
        ThreadLocalRandom.current().nextBytes(payload);

        try (StrataFile.Appender appender = client.openById(StrataNamespace.of("test"), fileId).openForAppend()) {
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

    private record ReadPass(long bytes, int latencyCount) {}

    /**
     * Reads one reader from 0 to EOF in READ_SIZE chunks, timing each read into {@code latOut}
     * starting at {@code latStart} (until the array fills). Returns bytes read and the next free
     * latency index. Shared by the throughput readers ({@link #runRead}, {@link #runParallelRead}).
     */
    private static ReadPass readToEnd(StrataFile.Reader reader, long[] latOut, int latStart) {
        long bytes = 0;
        long offset = 0;
        int i = latStart;
        while (true) {
            long t0 = System.nanoTime();
            int n;
            boolean eof;
            long elapsedNanos;
            try (StrataFile.ReadResult rr = reader.read(offset, READ_SIZE)) {
                elapsedNanos = System.nanoTime() - t0;
                n = rr.length();
                eof = rr.endOfFile();
            }
            if (n == 0) break;
            if (i < latOut.length) latOut[i++] = elapsedNanos;
            bytes += n;
            offset += n;
            if (eof) break;
        }
        return new ReadPass(bytes, i);
    }

    private ReadResult runRead(StrataClient client, FileId fileId) {
        try (StrataFile.Reader reader = client.openById(StrataNamespace.of("test"), fileId).openForRead()) {
            long[] latencies = new long[4096];
            long start = System.nanoTime();
            ReadPass pass = readToEnd(reader, latencies, 0);
            long elapsed = System.nanoTime() - start;
            double mbs = ((double) pass.bytes() / (1 << 20)) / (elapsed / 1e9);
            return new ReadResult(Arrays.copyOf(latencies, pass.latencyCount()), mbs);
        }
    }

    private static long p(long[] latencies, int percentile) {
        long[] sorted = latencies.clone();
        Arrays.sort(sorted);
        int idx = (int) Math.ceil(percentile / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }

    /** Percentile + throughput line shared by every write/read print path. */
    private static void printStats(String label, long[] latenciesNanos, double throughputMBs) {
        System.out.printf(Locale.ROOT,
                "%s  p50=%6.2fms  p95=%6.2fms  p99=%6.2fms  max=%7.2fms  throughput=%7.1f MB/s%n",
                label, ms(p(latenciesNanos, 50)), ms(p(latenciesNanos, 95)),
                ms(p(latenciesNanos, 99)), ms(p(latenciesNanos, 100)), throughputMBs);
    }

    private static void printWrite(String label, WriteResult r) {
        printStats(label, r.latenciesNanos(), r.throughputMBs());
    }

    /** Write line plus the physical (with-replicas) throughput — the disk-ceiling comparison. */
    private static void printParallelWrite(String label, ParallelWriteResult w) {
        printStats(label + " ", w.latencies(), w.throughputMBs());
        System.out.printf(Locale.ROOT,
                "  -> physical write incl. replicas (x%d) = %7.1f MB/s%n",
                REPLICATION_FACTOR, w.throughputMBs() * REPLICATION_FACTOR);
    }

    private static void printRead(String label, ReadResult r) {
        printStats(label, r.latenciesNanos(), r.throughputMBs());
    }

    private static double ms(long nanos) {
        return nanos / 1e6;
    }
}
