package io.strata.server;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * A load generator that drives a RUNNING Strata cluster through {@link StrataClient} (it is not the
 * in-process PerfSmokeTest). Point it at the metadata endpoint, pick a workload, and it sustains
 * load for a duration while printing client-side throughput + latency — so you can watch the
 * cluster react in Grafana.
 *
 * Must run where the node advertised hosts resolve: inside the compose network
 * (`docker compose run --rm loadgen perf ...` / `scripts/perf.sh`), not from the host.
 *
 * Flags (all optional): --meta host:port  --workload write|read|churn  --record-size BYTES  --files N
 *   --window N  --duration SECONDS (0 = until Ctrl-C)  --fsync  --readers N  --read-size BYTES  --keep
 *   --connections N (storage connections per endpoint, default 8)
 */
final class StrataPerf {
    private static final Logger log = LoggerFactory.getLogger(StrataPerf.class);
    private static volatile boolean stop;

    static void run(String[] argv) throws Exception {
        Map<String, String> a = parse(argv);
        String meta = a.getOrDefault("meta",
                StrataServer.env("STRATA_META_ENDPOINTS", "localhost:9100")).split(",")[0].trim();
        String workload = a.getOrDefault("workload", "write");
        int recordSize = intArg(a, "record-size", 64 * 1024);
        int files = intArg(a, "files", 8);
        int window = intArg(a, "window", 64);
        int durationSec = intArg(a, "duration", 60);
        boolean fsync = a.containsKey("fsync");
        int readers = intArg(a, "readers", 8);
        int readSize = intArg(a, "read-size", 64 * 1024);
        boolean keep = a.containsKey("keep");

        // storage connections per endpoint: a single connection serializes concurrent reads on one
        // pipe, capping read throughput; 8 lets the readers spread across the sendfile path (measured
        // ~10x churn-read throughput vs 1). Override with --connections N.
        int connections = intArg(a, "connections", 8);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> stop = true, "perf-stop"));
        log.info("perf: meta={} workload={} recordSize={}B files={} window={} duration={}s fsync={} connections={}",
                meta, workload, recordSize, files, window, durationSec, fsync, connections);

        long chunkRoll = longArg(a, "chunk-roll", 2L << 30); // 2 GiB, matches ClientConfig default
        try (StrataClient client = StrataClient.connect(ClientConfig.of(meta)
                .withChunkRollBytes(chunkRoll).withStorageConnectionsPerEndpoint(connections))) {
            if (workload.equals("read")) {
                runRead(client, readers, readSize, durationSec, recordSize, keep);
            } else if (workload.equals("churn")) {
                runChurn(client, files, recordSize, longArg(a, "file-size", 256L << 20),
                        readers, intArg(a, "write-window", 64), intArg(a, "writers", 1), durationSec);
            } else {
                runWrite(client, files, recordSize, window, durationSec, fsync, keep);
            }
        }
    }

    private static void runWrite(StrataClient client, int files, int recordSize, int window,
                                 int durationSec, boolean fsync, boolean keep) throws Exception {
        byte[] payload = new byte[recordSize];
        ThreadLocalRandom.current().nextBytes(payload);
        StrataClient.WritePolicy policy = fsync
                ? StrataClient.WritePolicy.fsync(3, 2) : StrataClient.WritePolicy.DEFAULT;
        long ts = System.currentTimeMillis();
        List<FileId> ids = new ArrayList<>();
        List<StrataFile.Appender> appenders = new ArrayList<>();
        for (int i = 0; i < files; i++) {
            FileId id = client.create(new StrataClient.FileSpec("perf", "/perf/run-" + ts + "-" + i, policy)).id();
            ids.add(id);
            appenders.add(client.openById(id).openForAppend());
        }
        Stats stats = new Stats();
        long deadline = deadline(durationSec);
        Thread reporter = startReporter(stats, recordSize, "write");
        try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> fs = new ArrayList<>();
            for (StrataFile.Appender ap : appenders) {
                fs.add(exec.submit(() -> {
                    writeLoop(ap, payload, recordSize, window, deadline, stats);
                    return null;
                }));
            }
            for (Future<?> f : fs) {
                f.get();
            }
        }
        reporter.interrupt();
        for (StrataFile.Appender ap : appenders) {
            try {
                ap.seal();
            } catch (RuntimeException ignored) {
            }
            try {
                ap.close();
            } catch (RuntimeException ignored) {
            }
        }
        stats.printFinal(recordSize, "write");
        cleanup(client, ids, keep);
    }

    private static void writeLoop(StrataFile.Appender ap, byte[] payload, int recordSize, int window,
                                  long deadline, Stats stats) {
        Semaphore permits = new Semaphore(window);
        try {
            while (!stop && System.nanoTime() < deadline) {
                permits.acquire();
                long t0 = System.nanoTime();
                stats.inflight.incrementAndGet();
                ap.append(ByteBuffer.wrap(payload)).whenComplete((ack, err) -> {
                    stats.record(System.nanoTime() - t0);
                    stats.inflight.decrementAndGet();
                    if (err != null) {
                        stats.errors.increment();
                    } else {
                        stats.ops.increment();
                        stats.bytes.add(recordSize);
                    }
                    permits.release();
                });
            }
            permits.acquire(window); // drain in-flight
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void runRead(StrataClient client, int readers, int readSize, int durationSec,
                                int recordSize, boolean keep) throws Exception {
        // seed a file to read (~256 MB), sealed so reads hit the zero-copy path
        byte[] payload = new byte[recordSize];
        ThreadLocalRandom.current().nextBytes(payload);
        long seedBytes = 256L << 20;
        long ts = System.currentTimeMillis();
        FileId id = client.create(new StrataClient.FileSpec("perf", "/perf/read-" + ts)).id();
        log.info("seeding {} MB read file...", seedBytes >> 20);
        try (StrataFile.Appender ap = client.openById(id).openForAppend()) {
            Semaphore permits = new Semaphore(64);
            long written = 0;
            while (written < seedBytes) {
                permits.acquire();
                ap.append(ByteBuffer.wrap(payload)).whenComplete((ack, err) -> permits.release());
                written += recordSize;
            }
            permits.acquire(64);
            ap.seal();
        }
        long fileBytes = (seedBytes / recordSize) * recordSize;
        Stats stats = new Stats();
        long deadline = deadline(durationSec);
        Thread reporter = startReporter(stats, readSize, "read");
        try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> fs = new ArrayList<>();
            for (int r = 0; r < readers; r++) {
                fs.add(exec.submit(() -> {
                    readLoop(client, id, readSize, fileBytes, deadline, stats);
                    return null;
                }));
            }
            for (Future<?> f : fs) {
                f.get();
            }
        }
        reporter.interrupt();
        stats.printFinal(readSize, "read");
        cleanup(client, List.of(id), keep);
    }

    private static void readLoop(StrataClient client, FileId id, int readSize, long fileBytes,
                                 long deadline, Stats stats) {
        long span = Math.max(1, fileBytes - readSize);
        try (StrataFile.Reader reader = client.openById(id).openForRead()) {
            long off = 0;
            while (!stop && System.nanoTime() < deadline) {
                long t0 = System.nanoTime();
                StrataFile.ReadResult r = reader.read(off, readSize);
                stats.record(System.nanoTime() - t0);
                int n = r.data().length;
                if (n == 0) {
                    off = 0;
                    continue;
                }
                stats.ops.increment();
                stats.bytes.add(n);
                off = (off + n) % span;
            }
        }
    }

    /**
     * Continuous mixed workload: a single writer creates -> writes {@code fileSize} -> seals a new
     * file, then deletes the OLDEST once the live set exceeds {@code targetFiles}; {@code readers}
     * concurrently read random ranges of random live (sealed) files. The live set is bounded to
     * {@code targetFiles}, so on-disk footprint is ~ targetFiles * fileSize * replicationFactor and
     * cannot grow with duration — safe for long soaks. Exercises append+seal (writeback), sealed
     * reads (range-CRC verify), and delete, all at once.
     */
    private static void runChurn(StrataClient client, int targetFiles, int recordSize, long fileSize,
                                 int readers, int writeWindow, int writers, int durationSec) throws Exception {
        byte[] payload = new byte[recordSize];
        ThreadLocalRandom.current().nextBytes(payload);
        long deadline = deadline(durationSec);
        ConcurrentLinkedDeque<FileId> live = new ConcurrentLinkedDeque<>();
        AtomicInteger liveCount = new AtomicInteger();
        LongAdder filesCreated = new LongAdder();
        LongAdder filesDeleted = new LongAdder();
        AtomicInteger fileSeq = new AtomicInteger();
        long ts = System.currentTimeMillis();
        Stats writeStats = new Stats();
        Stats readStats = new Stats();
        Thread wr = startReporter(writeStats, recordSize, "churn-write");
        Thread rr = startReporter(readStats, recordSize, "churn-read");
        try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> fs = new ArrayList<>();
            for (int w = 0; w < writers; w++) fs.add(exec.submit(() -> {
                while (!stop && System.nanoTime() < deadline) {
                  try {
                    FileId id = client.create(new StrataClient.FileSpec(
                            "perf", "/perf/churn-" + ts + "-" + fileSeq.incrementAndGet())).id();
                    try (StrataFile.Appender ap = client.openById(id).openForAppend()) {
                        Semaphore permits = new Semaphore(writeWindow);
                        long written = 0;
                        while (written < fileSize && !stop && System.nanoTime() < deadline) {
                            permits.acquire();
                            long t0 = System.nanoTime();
                            writeStats.inflight.incrementAndGet();
                            ap.append(ByteBuffer.wrap(payload)).whenComplete((ack, err) -> {
                                writeStats.record(System.nanoTime() - t0);
                                writeStats.inflight.decrementAndGet();
                                if (err != null) {
                                    writeStats.errors.increment();
                                } else {
                                    writeStats.ops.increment();
                                    writeStats.bytes.add(recordSize);
                                }
                                permits.release();
                            });
                            written += recordSize;
                        }
                        permits.acquire(writeWindow);
                        ap.seal();
                    }
                    live.addLast(id);
                    liveCount.incrementAndGet();
                    filesCreated.increment();
                    // trim the oldest beyond the target -> bounds the on-disk footprint
                    while (liveCount.get() > targetFiles) {
                        FileId old = live.pollFirst();
                        if (old == null) {
                            break;
                        }
                        liveCount.decrementAndGet();
                        try {
                            client.deleteById(List.of(old));
                            filesDeleted.increment();
                        } catch (RuntimeException e) {
                            writeStats.errors.increment();
                            live.addFirst(old);            // delete failed (meta busy) — keep it tracked to
                            liveCount.incrementAndGet();   // retry next round instead of leaking it on disk
                            break;
                        }
                    }
                  } catch (RuntimeException e) {
                      writeStats.errors.increment(); // transient meta/repair blip — back off and retry next file
                      try { Thread.sleep(200); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                  }
                }
                return null;
            }));
            long span = Math.max(1, fileSize - recordSize);
            for (int r = 0; r < readers; r++) {
                fs.add(exec.submit(() -> {
                    while (!stop && System.nanoTime() < deadline) {
                        FileId id = randomLive(live);
                        if (id == null) {
                            Thread.sleep(50); // no sealed file published yet
                            continue;
                        }
                        // Open ONCE and read many random ranges before rotating to another live file —
                        // amortizes the metadata lookup + reader setup (open-per-read otherwise caps
                        // throughput well below the data path). A deleted file throws mid-loop; we catch,
                        // count it, and re-pick. ~1024 reads ≈ 64 MB, well inside a file's ~10s lifetime.
                        try (StrataFile.Reader reader = client.openById(id).openForRead()) {
                            for (int i = 0; i < 1024 && !stop && System.nanoTime() < deadline; i++) {
                                long off = ThreadLocalRandom.current().nextLong(span);
                                long t0 = System.nanoTime();
                                StrataFile.ReadResult res = reader.read(off, recordSize);
                                readStats.record(System.nanoTime() - t0);
                                int n = res.data().length;
                                if (n > 0) {
                                    readStats.ops.increment();
                                    readStats.bytes.add(n);
                                }
                            }
                        } catch (RuntimeException e) {
                            readStats.errors.increment(); // file deleted mid-read — pick another
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> f : fs) {
                f.get();
            }
        }
        wr.interrupt();
        rr.interrupt();
        writeStats.printFinal(recordSize, "churn-write");
        readStats.printFinal(recordSize, "churn-read");
        log.info("churn: files created={} deleted={} live-at-end={}",
                filesCreated.sum(), filesDeleted.sum(), live.size());
        cleanup(client, new ArrayList<>(live), false);
    }

    /** Picks a random currently-live file id, or null if none are published yet. */
    private static FileId randomLive(ConcurrentLinkedDeque<FileId> live) {
        Object[] arr = live.toArray();
        if (arr.length == 0) {
            return null;
        }
        return (FileId) arr[ThreadLocalRandom.current().nextInt(arr.length)];
    }

    private static void cleanup(StrataClient client, List<FileId> ids, boolean keep) {
        if (keep) {
            log.info("kept {} perf file(s)", ids.size());
            return;
        }
        try {
            client.deleteById(ids);
            log.info("deleted {} perf file(s)", ids.size());
        } catch (RuntimeException e) {
            log.warn("perf file cleanup failed (delete manually): {}", e.toString());
        }
    }

    private static long deadline(int durationSec) {
        return durationSec <= 0 ? Long.MAX_VALUE : System.nanoTime() + durationSec * 1_000_000_000L;
    }

    /** Periodic console reporter: interval throughput + a sampled latency snapshot. */
    private static Thread startReporter(Stats stats, int unitBytes, String label) {
        Thread t = new Thread(() -> {
            long startMs = System.currentTimeMillis();
            long lastMs = startMs, lastOps = 0, lastBytes = 0;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(5_000);
                    long now = System.currentTimeMillis();
                    long ops = stats.ops.sum(), bytes = stats.bytes.sum();
                    double secs = (now - lastMs) / 1000.0;
                    double mbs = ((bytes - lastBytes) / (1 << 20)) / secs;
                    double ops_s = (ops - lastOps) / secs;
                    long[] s = stats.snapshotSorted();
                    log.info(String.format(Locale.ROOT,
                            "[t=%2ds] %s: %7.1f MB/s  %8.0f ops/s  inflight=%d  errors=%d  p50=%.2fms p99=%.2fms",
                            (now - startMs) / 1000, label, mbs, ops_s, stats.inflight.get(),
                            stats.errors.sum(), ms(pct(s, 50)), ms(pct(s, 99))));
                    lastMs = now;
                    lastOps = ops;
                    lastBytes = bytes;
                }
            } catch (InterruptedException ignored) {
            }
        }, "perf-reporter");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static final class Stats {
        static final int CAP = 1 << 16; // latency sample ring (racy writes — fine for an estimate)
        final LongAdder ops = new LongAdder();
        final LongAdder bytes = new LongAdder();
        final LongAdder errors = new LongAdder();
        final AtomicInteger inflight = new AtomicInteger();
        final long[] sample = new long[CAP];
        final AtomicInteger idx = new AtomicInteger();
        long startNanos = System.nanoTime();

        void record(long latNanos) {
            sample[idx.getAndIncrement() & (CAP - 1)] = latNanos;
        }

        long[] snapshotSorted() {
            int filled = Math.min(idx.get(), CAP);
            long[] c = Arrays.copyOf(sample, filled);
            Arrays.sort(c);
            return c;
        }

        void printFinal(int unitBytes, String label) {
            double secs = (System.nanoTime() - startNanos) / 1e9;
            long[] s = snapshotSorted();
            log.info(String.format(Locale.ROOT,
                    "DONE %s: %d ops, %.1f MB total, avg %.1f MB/s, %.0f ops/s, errors=%d, "
                            + "p50=%.2fms p95=%.2fms p99=%.2fms max=%.2fms",
                    label, ops.sum(), bytes.sum() / (double) (1 << 20),
                    (bytes.sum() / (double) (1 << 20)) / secs, ops.sum() / secs, errors.sum(),
                    ms(pct(s, 50)), ms(pct(s, 95)), ms(pct(s, 99)), ms(pct(s, 100))));
        }
    }

    private static long pct(long[] sorted, int p) {
        if (sorted.length == 0) {
            return 0;
        }
        int i = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(i, sorted.length - 1))];
    }

    private static double ms(long nanos) {
        return nanos / 1e6;
    }

    /** Parses --key value and boolean --flag args into a map. */
    private static Map<String, String> parse(String[] argv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < argv.length; i++) {
            if (!argv[i].startsWith("--")) {
                continue;
            }
            String key = argv[i].substring(2);
            if (i + 1 < argv.length && !argv[i + 1].startsWith("--")) {
                m.put(key, argv[++i]);
            } else {
                m.put(key, "true"); // boolean flag
            }
        }
        return m;
    }

    private static int intArg(Map<String, String> a, String key, int def) {
        String v = a.get(key);
        return v == null ? def : Integer.parseInt(v);
    }

    private static long longArg(Map<String, String> a, String key, long def) {
        String v = a.get(key);
        return v == null ? def : Long.parseLong(v);
    }

    private StrataPerf() {
    }
}
