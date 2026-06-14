package io.strata.server;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.strata.meta.MetadataService;
import io.strata.node.StorageNode;
import io.strata.proto.RequestObserver;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Registers Strata's domain metrics on the meter registry by wiring Micrometer gauges/counters to
 * the read-only accessors on {@link MetadataService} / {@link StorageNode}. All of these are either
 * periodic gauges over existing in-memory state (zero data-path cost) or monotonic function-counters
 * over plain atomic counters — no timers on the hot path. The {@code role} common tag is set by
 * {@code StrataMetrics}, so a single Prometheus job can scrape both process kinds.
 */
final class ServerMetrics {
    private ServerMetrics() {
    }

    /** Control-plane: durability census, repair progress, cluster liveness, leadership, ZK. */
    static void registerMeta(MeterRegistry reg, MetadataService s) {
        Gauge.builder("strata_meta_is_leader", s, m -> m.isLeader() ? 1 : 0)
                .description("1 if this instance is the active metadata leader (cluster sum should be 1)").register(reg);
        Gauge.builder("strata_meta_zk_connected", s, m -> m.zkConnected() ? 1 : 0)
                .description("1 if ZooKeeper is reachable; 0 freezes the control plane").register(reg);

        Gauge.builder("strata_chunks_unavailable", s, MetadataService::unavailableChunks)
                .description("SEALED chunks with zero live replicas — data-loss exposure (PAGE)").register(reg);
        Gauge.builder("strata_chunks_under_replicated", s, MetadataService::underReplicatedChunks)
                .description("SEALED chunks below their replication factor").register(reg);
        Gauge.builder("strata_chunks_at_min_redundancy", s, MetadataService::chunksAtMinRedundancy)
                .description("SEALED chunks down to a single live replica — one failure from loss").register(reg);

        Gauge.builder("strata_repair_inflight", s, MetadataService::repairInflight)
                .description("outstanding repair/delete commands").register(reg);
        Gauge.builder("strata_repair_backlog", s, MetadataService::repairBacklog)
                .description("distinct chunks currently being repaired").register(reg);

        Gauge.builder("strata_nodes", s, MetadataService::aliveNodes)
                .tag("state", "alive").description("storage nodes by liveness state").register(reg);
        Gauge.builder("strata_nodes", s, MetadataService::suspectNodes)
                .tag("state", "suspect").register(reg);
        Gauge.builder("strata_nodes", s, MetadataService::deadNodes)
                .tag("state", "dead").register(reg);
    }

    /** Data plane: capacity, chunk state, write throughput, fsync force rate, registration. */
    static void registerNode(MeterRegistry reg, StorageNode n) {
        Gauge.builder("strata_node_registered", n, x -> x.registered() ? 1 : 0)
                .description("1 if the node holds a metadata registration").register(reg);

        Gauge.builder("strata_node_disk_used_bytes", n, StorageNode::diskUsedBytes)
                .description("bytes occupied by chunk data").register(reg);
        Gauge.builder("strata_node_capacity_bytes", n, StorageNode::capacityBytes)
                .description("configured node capacity").register(reg);
        Gauge.builder("strata_node_capacity_used_ratio", n,
                        x -> x.capacityBytes() > 0 ? (double) x.diskUsedBytes() / x.capacityBytes() : 0.0)
                .description("disk used / capacity").register(reg);

        Gauge.builder("strata_node_chunks", n, StorageNode::openChunks)
                .tag("state", "open").description("local chunks by state").register(reg);
        Gauge.builder("strata_node_chunks", n, StorageNode::sealedChunks)
                .tag("state", "sealed").register(reg);

        FunctionCounter.builder("strata_node_groupcommit_force", n, StorageNode::fsyncForceCount)
                .description("group-commit force()/fsync calls").register(reg);
        FunctionCounter.builder("strata_node_append_ops", n, StorageNode::appendOps)
                .description("appended records (rate() = write ops/sec)").register(reg);
        FunctionCounter.builder("strata_node_append_bytes", n, StorageNode::appendBytes)
                .description("appended payload bytes (rate() = write throughput)").register(reg);
    }

    /**
     * A per-request latency observer recording into a {@code strata_scp_request_duration} timer
     * tagged by opcode + status. Emits a Prometheus HISTOGRAM (cumulative {@code _bucket{le}}
     * series) rather than client-side quantiles, so percentiles can be aggregated correctly across
     * the node fleet at query time (histogram_quantile over summed buckets) and re-quantiled at any
     * window — pre-computed per-instance quantiles cannot be averaged across instances. Explicit SLO
     * buckets (1ms..5s) bound the cardinality and pick boundaries meaningful for SCP request latency.
     * Timers are cached per opcode+status, so the per-request cost is a map lookup + a histogram
     * record. For an async APPEND in fsync mode this latency includes the group-commit/fsync wait.
     */
    static RequestObserver requestObserver(MeterRegistry reg) {
        Map<String, Timer> timers = new ConcurrentHashMap<>();
        return (opcode, durationNanos, success) -> {
            String status = success ? "ok" : "error";
            timers.computeIfAbsent(opcode + ':' + status, k -> Timer.builder("strata_scp_request_duration")
                            .description("request handler latency by opcode (incl. async durability wait)")
                            .tag("opcode", opcode)
                            .tag("status", status)
                            .serviceLevelObjectives(
                                    Duration.ofMillis(1), Duration.ofMillis(2), Duration.ofMillis(5),
                                    Duration.ofMillis(10), Duration.ofMillis(25), Duration.ofMillis(50),
                                    Duration.ofMillis(100), Duration.ofMillis(250), Duration.ofMillis(500),
                                    Duration.ofSeconds(1), Duration.ofMillis(2500), Duration.ofSeconds(5))
                            .register(reg))
                    .record(durationNanos, TimeUnit.NANOSECONDS);
        };
    }
}
