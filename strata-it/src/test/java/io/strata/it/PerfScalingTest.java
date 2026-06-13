package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.FileId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Concurrency-SCALING smoke (does aggregate throughput use the hardware, or plateau on a global
 * code bottleneck?). Runs N concurrent appenders (each its own file/chunk, so they are mutually
 * independent and CAN run in parallel) and N concurrent readers, sweeping connections-per-endpoint,
 * and prints aggregate MB/s per concurrency level. Not an assertion gate — a diagnostic.
 *
 * Run: mvn -pl strata-it test -Dtest=PerfScalingTest "-DexcludedGroups="
 * Tunables: -Dscale.recordsPerWriter=20000 -Dscale.recordSize=1024 -Dscale.window=64
 */
@Tag("perf")
class PerfScalingTest {

    private static final int RECORDS_PER_WRITER = Integer.getInteger("scale.recordsPerWriter", 20_000);
    private static final int RECORD_SIZE = Integer.getInteger("scale.recordSize", 1_024);
    private static final int WINDOW = Integer.getInteger("scale.window", 64);
    private static final int[] CONCURRENCY = {1, 2, 4, 8};

    /**
     * Sustained 8-writer write load for profiling (run with -Dscale.profile=true and JFR attached).
     * No reads, no sweep — a long, clean write window to locate the aggregate-write ceiling.
     */
    @Test
    void sustainedWriteForProfiling() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(Boolean.getBoolean("scale.profile"));
        int writers = Integer.getInteger("scale.writers", 8);
        int records = Integer.getInteger("scale.recordsPerWriter", 120_000);
        try (MiniCluster cluster = new MiniCluster(3)) {
            ClientConfig cfg = ClientConfig.of(cluster.metaEndpoint())
                    .withChunkRollBytes(1L << 30)
                    .withStorageConnectionsPerEndpoint(Integer.getInteger("scale.conns", 1));
            try (StrataClient client = StrataClient.connect(cfg)) {
                int saved = RECORDS_PER_WRITER_OVERRIDE.getAndSet(records);
                System.out.printf(Locale.ROOT, "%n=== sustained write: %d writers x %d records ===%n",
                        writers, records);
                double mbs = runWriteScaling(client, writers);
                System.out.printf(Locale.ROOT, "  sustained aggregate write = %7.1f MB/s%n", mbs);
                RECORDS_PER_WRITER_OVERRIDE.set(saved);
            }
        }
    }

    private static final java.util.concurrent.atomic.AtomicInteger RECORDS_PER_WRITER_OVERRIDE =
            new java.util.concurrent.atomic.AtomicInteger(0);

    private int recordsPerWriter() {
        int o = RECORDS_PER_WRITER_OVERRIDE.get();
        return o > 0 ? o : RECORDS_PER_WRITER;
    }

    @Test
    void writeAndReadScaling() throws Exception {
        System.out.printf(Locale.ROOT, "%n=== Strata scaling smoke: %d records x %d B per writer, "
                + "window %d, 3 nodes in-process, %d cores ===%n",
                RECORDS_PER_WRITER, RECORD_SIZE, WINDOW, Runtime.getRuntime().availableProcessors());

        // conns=1: every writer multiplexes over ONE shared connection per node (one server thread
        // per node). conns=8: writers spread across 8 connections per node (up to 8 server threads).
        for (int conns : new int[]{1, 8}) {
            System.out.printf(Locale.ROOT, "%n-- storageConnectionsPerEndpoint=%d --%n", conns);
            try (MiniCluster cluster = new MiniCluster(3)) {
                ClientConfig cfg = ClientConfig.of(cluster.metaEndpoint())
                        .withChunkRollBytes(256L << 20)
                        .withStorageConnectionsPerEndpoint(conns);
                try (StrataClient client = StrataClient.connect(cfg)) {
                    double prevWrite = 0, prevRead = 0;
                    for (int n : CONCURRENCY) {
                        double wMBs = runWriteScaling(client, n);
                        double scaleW = prevWrite > 0 ? wMBs / prevWrite : 1.0;
                        System.out.printf(Locale.ROOT,
                                "  writers=%-2d  aggregate write = %7.1f MB/s   (x%.2f vs prev)%n",
                                n, wMBs, scaleW);
                        prevWrite = wMBs;
                    }
                    for (int n : CONCURRENCY) {
                        double rMBs = runReadScaling(client, n);
                        double scaleR = prevRead > 0 ? rMBs / prevRead : 1.0;
                        System.out.printf(Locale.ROOT,
                                "  readers=%-2d  aggregate read  = %7.1f MB/s   (x%.2f vs prev)%n",
                                n, rMBs, scaleR);
                        prevRead = rMBs;
                    }
                }
            }
        }
    }

    /** N independent appenders write concurrently; returns aggregate MB/s. */
    private double runWriteScaling(StrataClient client, int writers) throws Exception {
        final int records = recordsPerWriter();
        byte[] payload = new byte[RECORD_SIZE];
        java.util.concurrent.ThreadLocalRandom.current().nextBytes(payload);
        List<FileId> files = new ArrayList<>();
        for (int i = 0; i < writers; i++) {
            files.add(client.create(new StrataClient.FileSpec("test", "/scale-w-" + writers + "-" + i
                    + "-" + System.nanoTime())).id());
        }
        CountDownLatch ready = new CountDownLatch(writers);
        CountDownLatch go = new CountDownLatch(1);
        AtomicLong totalBytes = new AtomicLong();
        List<Thread> threads = new ArrayList<>();
        AtomicLong[] elapsed = new AtomicLong[writers];
        for (int i = 0; i < writers; i++) {
            final FileId fileId = files.get(i);
            final int idx = i;
            elapsed[i] = new AtomicLong();
            Thread t = Thread.ofVirtual().start(() -> {
                try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
                    Semaphore win = new Semaphore(WINDOW);
                    CompletableFuture<?>[] fs = new CompletableFuture[records];
                    ready.countDown();
                    go.await();
                    long start = System.nanoTime();
                    for (int r = 0; r < records; r++) {
                        win.acquire();
                        fs[r] = appender.append(ByteBuffer.wrap(payload)).whenComplete((a, e) -> win.release());
                    }
                    CompletableFuture.allOf(fs).get(5, TimeUnit.MINUTES);
                    elapsed[idx].set(System.nanoTime() - start);
                    totalBytes.addAndGet((long) records * RECORD_SIZE);
                    appender.seal();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            threads.add(t);
        }
        ready.await();
        long wall0 = System.nanoTime();
        go.countDown();
        for (Thread t : threads) t.join();
        long wall = System.nanoTime() - wall0;
        return ((double) totalBytes.get() / (1 << 20)) / (wall / 1e9);
    }

    /** N independent readers read pre-written sealed files concurrently; returns aggregate MB/s. */
    private double runReadScaling(StrataClient client, int readers) throws Exception {
        byte[] payload = new byte[RECORD_SIZE];
        java.util.concurrent.ThreadLocalRandom.current().nextBytes(payload);
        // each reader gets its own sealed file of RECORDS_PER_WRITER*RECORD_SIZE bytes
        List<FileId> files = new ArrayList<>();
        for (int i = 0; i < readers; i++) {
            FileId fileId = client.create(new StrataClient.FileSpec("test", "/scale-r-" + readers + "-" + i
                    + "-" + System.nanoTime())).id();
            try (StrataFile.Appender appender = client.openById(fileId).openForAppend()) {
                Semaphore win = new Semaphore(WINDOW);
                CompletableFuture<?>[] fs = new CompletableFuture[RECORDS_PER_WRITER];
                for (int r = 0; r < RECORDS_PER_WRITER; r++) {
                    win.acquire();
                    fs[r] = appender.append(ByteBuffer.wrap(payload)).whenComplete((a, e) -> win.release());
                }
                CompletableFuture.allOf(fs).get(5, TimeUnit.MINUTES);
                appender.seal();
            }
            files.add(fileId);
        }
        CountDownLatch ready = new CountDownLatch(readers);
        CountDownLatch go = new CountDownLatch(1);
        AtomicLong totalBytes = new AtomicLong();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < readers; i++) {
            final FileId fileId = files.get(i);
            Thread t = Thread.ofVirtual().start(() -> {
                try (StrataFile.Reader reader = client.openById(fileId).openForRead()) {
                    ready.countDown();
                    go.await();
                    long offset = 0, bytes = 0;
                    while (true) {
                        StrataFile.ReadResult rr = reader.read(offset, 64 * 1024);
                        if (rr.data().length == 0) break;
                        bytes += rr.data().length;
                        offset += rr.data().length;
                        if (rr.endOfFile()) break;
                    }
                    totalBytes.addAndGet(bytes);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            threads.add(t);
        }
        ready.await();
        long wall0 = System.nanoTime();
        go.countDown();
        for (Thread t : threads) t.join();
        long wall = System.nanoTime() - wall0;
        return ((double) totalBytes.get() / (1 << 20)) / (wall / 1e9);
    }
}
