package io.strata.server;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Docker-compose load generator for the stable Strata file lifecycle perf scenario.
 *
 * Default run:
 * - 8 files, 8 writers, 8 readers
 * - 256 MiB per file, 128 MiB chunk roll
 * - 8 data-node connections per endpoint
 * - readers tail open files and read each file's bytes exactly once before delete
 *
 * Useful flags:
 *   --controller host:port
 *   --duration SECONDS (default 300, 0 = until Ctrl-C)
 *   --files N (also the writer count; default 8)
 *   --readers N (readers per file; default 1, 0 = write/seal/delete only)
 *   --namespaces N (distinct namespaces to round-robin files across; default 8 — with the
 *                   namespace-log backend these shard across controller owners by rendezvous hash;
 *                   pass 1 for the single-owner baseline)
 *   --read-sealed (read each file once after it is sealed instead of while it is open)
 *   --file-size BYTES (default 256 MiB)
 *   --chunk-roll BYTES (default 128 MiB)
 *   --record-size BYTES (default 32 KiB)
 *   --read-size BYTES (default 1 MiB)
 *   --write-window N (default 64)
 *   --connections N (default 8)
 *   --target-write-mbps MBPS (aggregate target, default 200; <=0 disables pacing)
 */
final class StrataPerf {
    private static final Logger log = LoggerFactory.getLogger(StrataPerf.class);

    private static final int DEFAULT_FILES = 8;
    private static final int DEFAULT_RECORD_SIZE = 32 * 1024;
    private static final int DEFAULT_READ_SIZE = 1 << 20;
    private static final int DEFAULT_READERS_PER_FILE = 1;
    private static final int DEFAULT_WRITE_WINDOW = 64;
    private static final int DEFAULT_DURATION_SEC = 300;
    private static final int DEFAULT_DATA_NODE_CONNECTIONS_PER_ENDPOINT = 8;
    private static final long DEFAULT_FILE_SIZE_BYTES = 256L << 20;
    private static final long DEFAULT_CHUNK_ROLL_BYTES = 128L << 20;
    private static final double DEFAULT_TARGET_WRITE_MBPS = 200.0;
    private static final long DEFAULT_TARGET_WRITE_BURST_MS = 2_000;
    private static final long REPORT_INTERVAL_MS = 5_000;

    private static volatile boolean stop;

    static void run(String[] argv) throws Exception {
        stop = false;
        Map<String, String> a = parse(argv);
        String controller = a.getOrDefault("controller",
                StrataServer.env("STRATA_CONTROLLER_ENDPOINTS", "localhost:9100")).split(",")[0].trim();

        int files = intArg(a, "files", DEFAULT_FILES);
        int readersPerFile = intArg(a, "readers", DEFAULT_READERS_PER_FILE);
        int recordSize = intArg(a, "record-size", DEFAULT_RECORD_SIZE);
        int readSize = intArg(a, "read-size", DEFAULT_READ_SIZE);
        int writeWindow = intArg(a, "write-window", DEFAULT_WRITE_WINDOW);
        int durationSec = intArg(a, "duration", DEFAULT_DURATION_SEC);
        int connections = intArg(a, "connections", DEFAULT_DATA_NODE_CONNECTIONS_PER_ENDPOINT);
        long fileSize = longArg(a, "file-size", DEFAULT_FILE_SIZE_BYTES);
        long chunkRoll = longArg(a, "chunk-roll", DEFAULT_CHUNK_ROLL_BYTES);
        boolean readSealed = a.containsKey("read-sealed");
        double targetWriteMbps = doubleArg(a, "target-write-mbps", DEFAULT_TARGET_WRITE_MBPS);
        long targetWriteBurstMs = longArg(a, "target-write-burst-ms", DEFAULT_TARGET_WRITE_BURST_MS);
        // Default to 8 namespaces so the load actually exercises sharding: distinct namespaces shard
        // across controller owners by rendezvous hash, spreading metadata/control-plane load. Pass
        // --namespaces 1 for the single-owner baseline (all metadata on one controller).
        int namespaces = intArg(a, "namespaces", 8);

        validate(files, readersPerFile, recordSize, readSize, writeWindow, fileSize, chunkRoll, connections);
        if (namespaces < 1) {
            throw new IllegalArgumentException("--namespaces must be positive");
        }

        ThroughputLimiter writeLimiter = targetWriteMbps > 0
                ? new ThroughputLimiter(targetWriteMbps, targetWriteBurstMs) : null;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> stop = true, "perf-stop"));
        String readMode = readersPerFile == 0 ? "none" : readSealed ? "sealed" : "open";

        log.info("perf: controller={} files={} writers={} readersPerFile={} totalReaders={} namespaces={} readMode={} "
                        + "fileSize={}B chunkRoll={}B "
                        + "recordSize={}B readSize={}B writeWindow={} duration={}s connections={} targetWrite={}",
                controller, files, files, readersPerFile, files * readersPerFile, namespaces, readMode,
                fileSize, chunkRoll, recordSize, readSize, writeWindow, durationSec, connections,
                writeLimiter == null ? "unlimited" : targetWriteMbps + "MiB/s");

        PerfConfig config = new PerfConfig(files, readersPerFile, recordSize, readSize, writeWindow,
                durationSec, fileSize, readSealed, writeLimiter, namespaces);
        try (StrataClient client = StrataClient.connect(ClientConfig.of(controller)
                .withChunkRollBytes(chunkRoll)
                .withDataNodeConnectionsPerEndpoint(connections))) {
            runFileLifecycle(client, config);
        }
    }

    private static void validate(int files, int readersPerFile, int recordSize, int readSize, int writeWindow,
                                 long fileSize, long chunkRoll, int connections) {
        if (files <= 0) {
            throw new IllegalArgumentException("--files must be positive");
        }
        if (readersPerFile < 0) {
            throw new IllegalArgumentException("--readers is readers per file and must be non-negative");
        }
        if (recordSize <= 0 || readSize <= 0) {
            throw new IllegalArgumentException("--record-size and --read-size must be positive");
        }
        if (writeWindow <= 0) {
            throw new IllegalArgumentException("--write-window must be positive");
        }
        if (fileSize <= 0) {
            throw new IllegalArgumentException("--file-size must be positive");
        }
        if (chunkRoll <= 0) {
            throw new IllegalArgumentException("--chunk-roll must be positive");
        }
        if (connections <= 0) {
            throw new IllegalArgumentException("--connections must be positive");
        }
    }

    private record PerfConfig(int files, int readersPerFile, int recordSize, int readSize, int writeWindow,
                              int durationSec, long fileSize, boolean readSealed,
                              ThroughputLimiter writeLimiter, int namespaces) {
        int totalReaders() {
            return files * readersPerFile;
        }

        /** Namespace for a file by sequence number — round-robins across {@code namespaces} (sharding). */
        String namespaceFor(int seq) {
            return namespaces <= 1 ? "perf" : "perf-" + Math.floorMod(seq, namespaces);
        }
    }

    private static void runFileLifecycle(StrataClient client, PerfConfig config) throws Exception {
        byte[] payload = new byte[config.recordSize()];
        ThreadLocalRandom.current().nextBytes(payload);
        long deadline = deadline(config.durationSec());
        long runId = System.currentTimeMillis();

        FileLane[] lanes = buildLanes(config.files(), config.readersPerFile());
        Stats writeStats = new Stats();
        Stats readStats = new Stats();
        Stats deleteStats = new Stats();
        LongAdder filesCreated = new LongAdder();
        LongAdder filesDeleted = new LongAdder();
        AtomicInteger activeWriters = new AtomicInteger(config.files());
        AtomicInteger fileSeq = new AtomicInteger();
        List<PerfFile> cleanupFiles = java.util.Collections.synchronizedList(new ArrayList<>());

        logReaderDistribution(lanes);
        Thread writeReporter = startReporter(writeStats, config.recordSize(), "write");
        Thread readReporter = config.totalReaders() > 0
                ? startReporter(readStats, config.readSize(), "read") : null;
        Thread deleteReporter = startReporter(deleteStats, config.recordSize(), "delete");

        try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < config.files(); i++) {
                final int fileIndex = i;
                futures.add(exec.submit(() -> {
                    writerLoop(client, config, fileIndex, lanes[fileIndex], payload, deadline, runId,
                            fileSeq, filesCreated, filesDeleted, cleanupFiles, writeStats, deleteStats,
                            activeWriters);
                    return null;
                }));
            }
            for (int r = 0; r < config.totalReaders(); r++) {
                FileLane lane = lanes[r % config.files()];
                futures.add(exec.submit(() -> {
                    readerLoop(client, config, lane, activeWriters, filesDeleted, cleanupFiles,
                            readStats, deleteStats);
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            writeStats.finish();
            readStats.finish();
            deleteStats.finish();
            writeReporter.interrupt();
            if (readReporter != null) {
                readReporter.interrupt();
            }
            deleteReporter.interrupt();
        }

        writeStats.printFinal(config.recordSize(), "write");
        if (config.totalReaders() > 0) {
            readStats.printFinal(config.readSize(), "read");
        }
        deleteStats.printFinal(config.recordSize(), "delete");
        log.info("perf: files created={} deleted={} cleanupCandidates={}",
                filesCreated.sum(), filesDeleted.sum(), cleanupFiles.size());
        cleanup(client, cleanupFiles);
    }

    private static FileLane[] buildLanes(int files, int readersPerFile) {
        FileLane[] lanes = new FileLane[files];
        for (int i = 0; i < files; i++) {
            lanes[i] = new FileLane(i, readersPerFile);
        }
        return lanes;
    }

    private static void logReaderDistribution(FileLane[] lanes) {
        StringBuilder out = new StringBuilder();
        for (FileLane lane : lanes) {
            if (!out.isEmpty()) {
                out.append(", ");
            }
            out.append(lane.fileIndex).append(":").append(lane.readerCount);
        }
        log.info("perf: reader distribution per file lane [{}]", out);
    }

    private static void writerLoop(StrataClient client, PerfConfig config, int fileIndex, FileLane lane,
                                   byte[] payload, long deadline, long runId, AtomicInteger fileSeq,
                                   LongAdder filesCreated, LongAdder filesDeleted, List<PerfFile> cleanupFiles,
                                   Stats writeStats, Stats deleteStats, AtomicInteger activeWriters) {
        try {
            while (!stop && System.nanoTime() < deadline) {
                PerfFile file = null;
                StrataFile.Appender appender = null;
                boolean published = false;
                try {
                    int seq = fileSeq.incrementAndGet();
                    StrataNamespace ns = StrataNamespace.of(config.namespaceFor(seq));
                    FileId id = client.create(new StrataClient.FileSpec(
                            ns.toString(), "/perf/run-" + runId + "-" + seq)).id();
                    filesCreated.increment();
                    file = new PerfFile(id, ns, lane.readerCount);
                    appender = client.openById(ns, id).openForAppend();
                    if (lane.readerCount > 0 && !config.readSealed()) {
                        lane.publish(file);
                        published = true;
                    }

                    writeOneFile(config, payload, appender, file, writeStats);
                    StrataFile.SealInfo seal = appender.seal();
                    file.markSealed(seal.sealedLength());
                    appender.close();
                    appender = null;

                    if (lane.readerCount == 0) {
                        deleteFile(client, file, filesDeleted, cleanupFiles, deleteStats);
                        file.completed.complete(null);
                    } else if (config.readSealed()) {
                        lane.publish(file);
                        published = true;
                        awaitCompletion(file);
                    } else {
                        awaitCompletion(file);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    stop = true;
                    abortOrDelete(client, file, published, cleanupFiles);
                    return;
                } catch (RuntimeException e) {
                    writeStats.recordError("perf writer lane " + fileIndex, e);
                    abortOrDelete(client, file, published, cleanupFiles);
                    sleepQuietly(200);
                } finally {
                    if (appender != null) {
                        try {
                            appender.close();
                        } catch (RuntimeException ignored) {
                        }
                    }
                }
            }
        } finally {
            activeWriters.decrementAndGet();
        }
    }

    private static void writeOneFile(PerfConfig config, byte[] payload, StrataFile.Appender appender,
                                     PerfFile file, Stats writeStats) throws InterruptedException {
        Semaphore permits = new Semaphore(config.writeWindow());
        AtomicBoolean appendFailed = new AtomicBoolean();
        long written = 0;
        while (written < config.fileSize() && !stop) {
            int bytes = (int) Math.min(config.recordSize(), config.fileSize() - written);
            pace(config.writeLimiter(), bytes);
            permits.acquire();
            appendTracked(appender, payload, bytes, permits, writeStats,
                    ack -> file.publishDurable(ack.durableOffset()),
                    err -> appendFailed.set(true));
            written += bytes;
        }
        permits.acquire(config.writeWindow());
        file.publishDurable(appender.durableOffset());
        if (appendFailed.get()) {
            throw new ScpException(ErrorCode.INTERNAL, "append failed while writing " + file.id);
        }
    }

    private static void appendTracked(StrataFile.Appender appender, byte[] payload, int bytes,
                                      Semaphore permits, Stats stats,
                                      Consumer<StrataFile.AppendAck> onSuccess,
                                      Consumer<Throwable> onFailure) {
        long t0 = System.nanoTime();
        stats.inflight.incrementAndGet();
        CompletableFuture<StrataFile.AppendAck> future;
        try {
            future = appender.append(ByteBuffer.wrap(payload, 0, bytes));
        } catch (RuntimeException e) {
            finishAppend(stats, bytes, permits, t0, null, e, onSuccess, onFailure);
            throw e;
        }
        future.whenComplete((ack, err) -> finishAppend(stats, bytes, permits, t0,
                ack, err, onSuccess, onFailure));
    }

    private static void finishAppend(Stats stats, int bytes, Semaphore permits, long startNanos,
                                     StrataFile.AppendAck ack, Throwable err,
                                     Consumer<StrataFile.AppendAck> onSuccess,
                                     Consumer<Throwable> onFailure) {
        try {
            stats.record(System.nanoTime() - startNanos);
            if (err != null) {
                stats.recordError("append", err);
                if (onFailure != null) {
                    onFailure.accept(err);
                }
            } else {
                stats.ops.increment();
                stats.bytes.add(bytes);
                if (onSuccess != null) {
                    onSuccess.accept(ack);
                }
            }
        } finally {
            stats.inflight.decrementAndGet();
            permits.release();
        }
    }

    private static void readerLoop(StrataClient client, PerfConfig config, FileLane lane,
                                   AtomicInteger activeWriters, LongAdder filesDeleted,
                                   List<PerfFile> cleanupFiles, Stats readStats, Stats deleteStats) {
        while (true) {
            if (stop && lane.queue.isEmpty()) {
                return;
            }
            if (activeWriters.get() == 0 && lane.queue.isEmpty()) {
                return;
            }
            PerfFile file;
            try {
                file = lane.queue.poll(250, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stop = true;
                return;
            }
            if (file == null) {
                continue;
            }
            try {
                readFileOnce(client, config, file, readStats);
            } finally {
                completeReader(client, file, filesDeleted, cleanupFiles, deleteStats);
            }
        }
    }

    private static void readFileOnce(StrataClient client, PerfConfig config, PerfFile file,
                                     Stats readStats) {
        if (file.aborted.get()) {
            return;
        }
        try (StrataFile.Reader reader = client.openById(file.namespace, file.id).openForRead()) {
            long reads = 0;
            while (!file.aborted.get()) {
                if (stop) {
                    file.abort();
                    return;
                }
                ReadClaim claim = file.claim(config.readSize());
                if (claim.done()) {
                    return;
                }
                if (claim.waitForMore()) {
                    reader.refresh();
                    sleepInterruptibly(5);
                    continue;
                }
                reads = readClaim(reader, file, claim, reads, readStats);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stop = true;
            file.abort();
        } catch (RuntimeException e) {
            readStats.recordError("perf read " + file.id, e);
            file.abort();
        }
    }

    private static long readClaim(StrataFile.Reader reader, PerfFile file, ReadClaim claim,
                                  long reads, Stats readStats) throws InterruptedException {
        long offset = claim.offset();
        int remaining = claim.length();
        int retries = 0;
        while (remaining > 0 && !file.aborted.get()) {
            if (stop) {
                file.abort();
                return reads;
            }
            if ((reads++ & 0xff) == 0) {
                reader.refresh();
            }
            long t0 = System.nanoTime();
            int n;
            long elapsedNanos;
            try (StrataFile.ReadResult result = reader.read(offset, remaining)) {
                elapsedNanos = System.nanoTime() - t0;
                n = result.length();
            } catch (RuntimeException e) {
                if (isTransientReadUnavailable(e) && retries++ < 20) {
                    reader.refresh();
                    sleepInterruptibly(100);
                    continue;
                }
                readStats.recordError("perf read range", e);
                file.abort();
                return reads;
            }
            readStats.record(elapsedNanos);
            if (n == 0) {
                reader.refresh();
                sleepInterruptibly(5);
                continue;
            }
            retries = 0;
            readStats.ops.increment();
            readStats.bytes.add(n);
            offset += n;
            remaining -= n;
        }
        return reads;
    }

    private static void completeReader(StrataClient client, PerfFile file, LongAdder filesDeleted,
                                       List<PerfFile> cleanupFiles, Stats deleteStats) {
        if (file.remainingReaders.decrementAndGet() != 0) {
            return;
        }
        deleteFile(client, file, filesDeleted, cleanupFiles, deleteStats);
        file.completed.complete(null);
    }

    private static void deleteFile(StrataClient client, PerfFile file, LongAdder filesDeleted,
                                   List<PerfFile> cleanupFiles, Stats deleteStats) {
        long t0 = System.nanoTime();
        try {
            client.deleteById(file.namespace, file.id);
            deleteStats.record(System.nanoTime() - t0);
            deleteStats.ops.increment();
            deleteStats.bytes.add(file.length());
            filesDeleted.increment();
        } catch (RuntimeException e) {
            deleteStats.recordError("perf delete " + file.id, e);
            cleanupFiles.add(file);
        }
    }

    private static void abortOrDelete(StrataClient client, PerfFile file, boolean published,
                                      List<PerfFile> cleanupFiles) {
        if (file == null) {
            return;
        }
        file.abort();
        if (published) {
            awaitCompletionQuietly(file);
            return;
        }
        try {
            client.deleteById(file.namespace, file.id);
        } catch (RuntimeException e) {
            cleanupFiles.add(file);
        } finally {
            file.completed.complete(null);
        }
    }

    private static void awaitCompletion(PerfFile file) throws InterruptedException {
        try {
            file.completed.get();
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    private static void awaitCompletionQuietly(PerfFile file) {
        try {
            file.completed.get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | java.util.concurrent.TimeoutException ignored) {
        }
    }

    private static void pace(ThroughputLimiter limiter, int bytes) throws InterruptedException {
        if (limiter != null) {
            limiter.acquire(bytes);
        }
    }

    private static void sleepInterruptibly(long millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stop = true;
        }
    }

    private static boolean isTransientReadUnavailable(Throwable err) {
        Throwable root = ScpException.rootCause(err);
        if (root instanceof ScpException se) {
            return se.code() == ErrorCode.INTERNAL
                    && se.getMessage() != null
                    && se.getMessage().contains("no readable replica");
        }
        return false;
    }

    private static final class FileLane {
        final int fileIndex;
        final int readerCount;
        final BlockingQueue<PerfFile> queue = new LinkedBlockingQueue<>();

        FileLane(int fileIndex, int readerCount) {
            this.fileIndex = fileIndex;
            this.readerCount = readerCount;
        }

        void publish(PerfFile file) {
            for (int i = 0; i < readerCount; i++) {
                queue.add(file);
            }
        }
    }

    private static final class PerfFile {
        final FileId id;
        final StrataNamespace namespace;
        final AtomicLong durableOffset = new AtomicLong();
        final AtomicLong finalLength = new AtomicLong(-1);
        final AtomicLong nextReadOffset = new AtomicLong();
        final AtomicInteger remainingReaders;
        final AtomicBoolean aborted = new AtomicBoolean();
        final CompletableFuture<Void> completed = new CompletableFuture<>();

        PerfFile(FileId id, StrataNamespace namespace, int readers) {
            this.id = id;
            this.namespace = namespace;
            this.remainingReaders = new AtomicInteger(readers);
        }

        void publishDurable(long offset) {
            durableOffset.accumulateAndGet(offset, Math::max);
        }

        void markSealed(long length) {
            finalLength.set(length);
            publishDurable(length);
        }

        void abort() {
            aborted.set(true);
        }

        long length() {
            long sealed = finalLength.get();
            return sealed >= 0 ? sealed : Math.max(0, durableOffset.get());
        }

        ReadClaim claim(int maxBytes) {
            while (true) {
                long offset = nextReadOffset.get();
                long sealed = finalLength.get();
                long readable = sealed >= 0 ? sealed : durableOffset.get();
                if (offset >= readable) {
                    return sealed >= 0 ? ReadClaim.DONE : ReadClaim.WAIT;
                }
                int len = (int) Math.min(maxBytes, readable - offset);
                if (nextReadOffset.compareAndSet(offset, offset + len)) {
                    return new ReadClaim(offset, len, false);
                }
            }
        }
    }

    private record ReadClaim(long offset, int length, boolean done) {
        static final ReadClaim WAIT = new ReadClaim(-1, 0, false);
        static final ReadClaim DONE = new ReadClaim(-1, 0, true);

        boolean waitForMore() {
            return !done && length == 0;
        }
    }

    private static void cleanup(StrataClient client, List<PerfFile> files) {
        if (files.isEmpty()) {
            return;
        }
        int deleted = 0;
        int failed = 0;
        for (PerfFile f : files) {
            try {
                client.deleteById(f.namespace, f.id);
                deleted++;
            } catch (RuntimeException e) {
                failed++;
                log.warn("perf file cleanup failed for {} (delete manually): {}", f.id, e.toString());
            }
        }
        log.info("cleanup: deleted={} failed={}", deleted, failed);
    }

    private static long deadline(int durationSec) {
        return durationSec <= 0 ? Long.MAX_VALUE : System.nanoTime() + durationSec * 1_000_000_000L;
    }

    private static Thread startReporter(Stats stats, int unitBytes, String label) {
        Thread t = new Thread(() -> {
            long startMs = System.currentTimeMillis();
            long lastMs = startMs;
            long lastOps = 0;
            long lastBytes = 0;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(REPORT_INTERVAL_MS);
                    long now = System.currentTimeMillis();
                    long ops = stats.ops.sum();
                    long bytes = stats.bytes.sum();
                    double secs = (now - lastMs) / 1000.0;
                    double mbs = ((bytes - lastBytes) / (double) (1 << 20)) / secs;
                    double opsPerSecond = (ops - lastOps) / secs;
                    long[] sample = stats.snapshotSorted();
                    log.info(String.format(Locale.ROOT,
                            "[t=%2ds] %s: %7.1f MB/s  %8.0f ops/s  inflight=%d  errors=%d  "
                                    + "p50=%.2fms p99=%.2fms",
                            (now - startMs) / 1000, label, mbs, opsPerSecond, stats.inflight.get(),
                            stats.errors.sum(), ms(pct(sample, 50)), ms(pct(sample, 99))));
                    lastMs = now;
                    lastOps = ops;
                    lastBytes = bytes;
                }
            } catch (InterruptedException ignored) {
            }
        }, "perf-reporter-" + label);
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static final class Stats {
        private static final int CAP = 1 << 16;

        final LongAdder ops = new LongAdder();
        final LongAdder bytes = new LongAdder();
        final LongAdder errors = new LongAdder();
        final AtomicLong errorEvents = new AtomicLong();
        final AtomicInteger inflight = new AtomicInteger();
        final long[] sample = new long[CAP];
        final AtomicInteger idx = new AtomicInteger();
        final long startNanos = System.nanoTime();
        volatile long endNanos;

        void record(long latNanos) {
            sample[idx.getAndIncrement() & (CAP - 1)] = latNanos;
        }

        void recordError(String context, Throwable err) {
            errors.increment();
            long count = errorEvents.incrementAndGet();
            if (count <= 20 || (count & (count - 1)) == 0) {
                log.warn("{} error #{}: {}", context, count, summarize(err));
            }
        }

        void finish() {
            if (endNanos == 0) {
                endNanos = System.nanoTime();
            }
        }

        long[] snapshotSorted() {
            int filled = Math.min(idx.get(), CAP);
            long[] copy = Arrays.copyOf(sample, filled);
            Arrays.sort(copy);
            return copy;
        }

        void printFinal(int unitBytes, String label) {
            long end = endNanos == 0 ? System.nanoTime() : endNanos;
            double secs = Math.max(1e-9, (end - startNanos) / 1e9);
            long[] sorted = snapshotSorted();
            log.info(String.format(Locale.ROOT,
                    "DONE %s: %d ops, %.1f MB total, avg %.1f MB/s, %.0f ops/s, errors=%d, "
                            + "p50=%.2fms p95=%.2fms p99=%.2fms max=%.2fms",
                    label, ops.sum(), bytes.sum() / (double) (1 << 20),
                    (bytes.sum() / (double) (1 << 20)) / secs, ops.sum() / secs, errors.sum(),
                    ms(pct(sorted, 50)), ms(pct(sorted, 95)), ms(pct(sorted, 99)), ms(pct(sorted, 100))));
        }
    }

    private static String summarize(Throwable err) {
        if (err == null) {
            return "unknown";
        }
        Throwable root = ScpException.rootCause(err);
        if (root instanceof ScpException se) {
            return se.code() + ": " + se.getMessage();
        }
        String message = root.getMessage();
        return root.getClass().getSimpleName() + (message == null || message.isBlank() ? "" : ": " + message);
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

    private static final class ThroughputLimiter {
        private final double nanosPerByte;
        private final long maxBurstNanos;
        private long nextNanos;

        ThroughputLimiter(double mibPerSecond, long maxBurstMs) {
            if (mibPerSecond <= 0 || !Double.isFinite(mibPerSecond)) {
                throw new IllegalArgumentException("mibPerSecond must be finite and positive");
            }
            this.nanosPerByte = 1_000_000_000.0 / (mibPerSecond * (1 << 20));
            this.maxBurstNanos = Math.max(0L, TimeUnit.MILLISECONDS.toNanos(maxBurstMs));
            this.nextNanos = System.nanoTime();
        }

        void acquire(int bytes) throws InterruptedException {
            long dueNanos;
            synchronized (this) {
                long now = System.nanoTime();
                long earliest = now - maxBurstNanos;
                if (nextNanos < earliest) {
                    nextNanos = earliest;
                }
                dueNanos = nextNanos;
                long intervalNanos = Math.max(1L, Math.round(bytes * nanosPerByte));
                nextNanos = Math.addExact(nextNanos, intervalNanos);
            }
            while (true) {
                long delayNanos = dueNanos - System.nanoTime();
                if (delayNanos <= 0) {
                    return;
                }
                TimeUnit.NANOSECONDS.sleep(Math.min(delayNanos, TimeUnit.MILLISECONDS.toNanos(10)));
            }
        }
    }

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
                m.put(key, "true");
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

    private static double doubleArg(Map<String, String> a, String key, double def) {
        String v = a.get(key);
        return v == null ? def : Double.parseDouble(v);
    }

    private StrataPerf() {
    }
}
