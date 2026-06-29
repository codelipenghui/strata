package io.strata.server;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.strata.meta.Controller;
import io.strata.meta.ZkMetadataStore;
import io.strata.node.DataNode;
import io.strata.proto.RequestObserver;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Registers Strata's domain metrics on the meter registry by wiring Micrometer gauges/counters to
 * the read-only accessors on {@link Controller} / {@link DataNode}. All of these are either
 * periodic gauges over existing in-memory state (zero data-path cost) or monotonic function-counters
 * over plain atomic counters — no timers on the hot path. The {@code role} common tag is set by
 * {@code StrataMetrics}, so a single Prometheus job can scrape both process kinds.
 */
final class ServerMetrics {
    private ServerMetrics() {
    }

    /** Control-plane: durability census, repair progress, cluster liveness, leadership, ZK. */
    static void registerController(MeterRegistry reg, Controller s) {
        Gauge.builder("strata_controller_is_leader", s, m -> m.isLeader() ? 1 : 0)
                .description("1 if this instance is the active controller leader (cluster sum should be 1)").register(reg);
        Gauge.builder("strata_controller_zk_connected", s, m -> m.zkConnected() ? 1 : 0)
                .description("1 if ZooKeeper is reachable; 0 freezes the control plane").register(reg);

        Gauge.builder("strata_chunks_unavailable", s, Controller::unavailableChunks)
                .description("SEALED chunks with zero live replicas — data-loss exposure (PAGE)").register(reg);
        Gauge.builder("strata_chunks_under_replicated", s, Controller::underReplicatedChunks)
                .description("SEALED chunks below their replication factor").register(reg);
        Gauge.builder("strata_chunks_at_min_redundancy", s, Controller::chunksAtMinRedundancy)
                .description("SEALED chunks down to a single live replica — one failure from loss").register(reg);

        Gauge.builder("strata_repair_inflight", s, Controller::repairInflight)
                .description("outstanding repair/delete commands").register(reg);
        Gauge.builder("strata_repair_backlog", s, Controller::repairBacklog)
                .description("distinct chunks currently being repaired").register(reg);
        FunctionCounter.builder("strata_repair_actions", s, Controller::eventRepairs)
                .tag("trigger", "event")
                .description("repairs issued, by trigger lane (event = node-death driven, reconcile = backstop scan)").register(reg);
        FunctionCounter.builder("strata_repair_actions", s, Controller::reconcileRepairs)
                .tag("trigger", "reconcile")
                .description("repairs issued, by trigger lane (event = node-death driven, reconcile = backstop scan)").register(reg);
        FunctionCounter.builder("strata_controller_reconcile_skipped_files", s, Controller::reconcileSkippedFiles)
                .description("files skipped in the reconcile pass due to per-file errors (rate() = error frequency)").register(reg);

        Gauge.builder("strata_data_nodes", s, Controller::aliveNodes)
                .tag("state", "alive").description("data nodes by liveness state").register(reg);
        Gauge.builder("strata_data_nodes", s, Controller::suspectNodes)
                .tag("state", "suspect").register(reg);
        Gauge.builder("strata_data_nodes", s, Controller::deadNodes)
                .tag("state", "dead").register(reg);

        // Per-subtree metadata-store request load: rate(strata_metadata_store_ops_total) = requests/s and
        // rate(strata_metadata_store_bytes_total) = throughput, tagged by `backend` (e.g. zk), the /strata
        // child the op touched (files/namespaces/nodes), and op=read|write. Backend-neutral name +
        // label so a future non-ZK metadata store surfaces on the same Cluster/Node dashboard panels.
        String backend = s.metadataBackend();
        for (String subtree : ZkMetadataStore.SUBTREES) {
            for (boolean write : new boolean[]{false, true}) {
                String op = write ? "write" : "read";
                FunctionCounter.builder("strata_metadata_store_ops", s, m -> m.metadataStoreOps(subtree, write))
                        .tag("backend", backend).tag("subtree", subtree).tag("op", op)
                        .description("metadata-store requests issued, by backend, /strata subtree, and op").register(reg);
                FunctionCounter.builder("strata_metadata_store_bytes", s, m -> m.metadataStoreBytes(subtree, write))
                        .tag("backend", backend).tag("subtree", subtree).tag("op", op)
                        .description("metadata-store payload bytes read/written, by backend, /strata subtree, and op").register(reg);
            }
        }

        // Namespace-log backend: user file/path metadata is stored as replicated Strata files, sharded
        // one owner per namespace. These read 0 under the ZK backend, so the same panels work for both;
        // the metadata log's OWN chunk durability is already counted in strata_chunks_* (the reserved
        // strata-meta namespace is in the repair scan). The `backend` tag matches strata_metadata_store_*.
        Gauge.builder("strata_controller_namespace_log_active", s, m -> m.namespaceLogActive() ? 1 : 0)
                .description("1 if the namespace-log backend is active (metadata stored as Strata files)").register(reg);
        Gauge.builder("strata_controller_namespaces_loaded", s, Controller::loadedNamespaces)
                .description("namespaces this instance owns a live metadata-log repository for (sharding load)").register(reg);
        Gauge.builder("strata_controller_endpoints_configured", s, Controller::controllerEndpointsConfigured)
                .description("configured controller-endpoint membership = controllers sharing the namespaces; max() = fleet count").register(reg);
        Gauge.builder("strata_controller_sharding_active", s, m -> m.shardingActive() ? 1 : 0)
                .description("1 if namespaces are sharded across multiple controllers; 0 = single global leader").register(reg);
        FunctionCounter.builder("strata_controller_log_append_records", s, Controller::metadataLogAppendRecords)
                .tag("backend", backend).description("metadata-log records appended (rate() = metadata ops/s)").register(reg);
        FunctionCounter.builder("strata_controller_log_append_bytes", s, Controller::metadataLogAppendBytes)
                .tag("backend", backend).description("metadata-log bytes appended (rate() = metadata write throughput)").register(reg);
        FunctionCounter.builder("strata_controller_log_compactions", s, Controller::metadataLogCompactions)
                .tag("backend", backend).description("metadata-log snapshot+roll compactions").register(reg);
        FunctionCounter.builder("strata_controller_log_recoveries", s, Controller::metadataLogRecoveries)
                .tag("backend", backend).description("namespace repositories (re)opened from a manifest (failover/restart churn)").register(reg);
        FunctionCounter.builder("strata_controller_log_reacquisitions", s, Controller::metadataLogReacquisitions)
                .tag("backend", backend).description("stale-epoch meta-log re-acquisitions (ownership contention / membership churn)").register(reg);

        registerPerNamespace(reg, s);
    }

    /**
     * Per-namespace gauges (namespace-stacked dashboard panels): live files + open metadata-log bytes,
     * labelled by {@code namespace}, for the namespaces THIS controller owns. Refreshed off a daemon
     * timer because the namespace set changes at runtime (a {@link MultiGauge} must be re-registered, not
     * supplier-bound). Cardinality grows with the namespace count — namespace stays control-plane, so
     * these live only on the controller (data nodes never see namespaces).
     */
    private static void registerPerNamespace(MeterRegistry reg, Controller s) {
        MultiGauge files = MultiGauge.builder("strata_controller_namespace_files")
                .description("live files per namespace owned by this controller").register(reg);
        MultiGauge logBytes = MultiGauge.builder("strata_controller_namespace_log_bytes")
                .description("open metadata-log bytes per namespace owned by this controller").register(reg);
        var refresh = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "controller-ns-metrics");
            t.setDaemon(true);
            return t;
        });
        refresh.scheduleAtFixedRate(() -> {
            Map<String, long[]> stats = s.namespaceStats();
            files.register(stats.entrySet().stream()
                    .map(e -> MultiGauge.Row.of(Tags.of("namespace", e.getKey()), e.getValue()[0]))
                    .collect(java.util.stream.Collectors.toList()), true);
            logBytes.register(stats.entrySet().stream()
                    .map(e -> MultiGauge.Row.of(Tags.of("namespace", e.getKey()), e.getValue()[1]))
                    .collect(java.util.stream.Collectors.toList()), true);
        }, 0, 10, TimeUnit.SECONDS);
    }

    /** Data plane: capacity, chunk state, write throughput, fsync force rate, registration. */
    static void registerDataNode(MeterRegistry reg, DataNode n) {
        Gauge.builder("strata_data_node_registered", n, x -> x.registered() ? 1 : 0)
                .description("1 if the node holds a metadata registration").register(reg);

        Gauge.builder("strata_data_node_disk_used_bytes", n, DataNode::diskUsedBytes)
                .description("bytes occupied by chunk data").register(reg);
        Gauge.builder("strata_data_node_capacity_bytes", n, DataNode::capacityBytes)
                .description("configured node capacity").register(reg);
        Gauge.builder("strata_data_node_capacity_used_ratio", n,
                        x -> x.capacityBytes() > 0 ? (double) x.diskUsedBytes() / x.capacityBytes() : 0.0)
                .description("disk used / capacity").register(reg);

        Gauge.builder("strata_data_node_chunks", n, DataNode::openChunks)
                .tag("state", "open").description("local chunks by state").register(reg);
        Gauge.builder("strata_data_node_chunks", n, DataNode::sealedChunks)
                .tag("state", "sealed").register(reg);

        FunctionCounter.builder("strata_data_node_groupcommit_force", n, DataNode::fsyncForceCount)
                .description("group-commit force()/fsync calls").register(reg);
        FunctionCounter.builder("strata_data_node_append_ops", n, DataNode::appendOps)
                .description("appended records (rate() = write ops/sec)").register(reg);
        FunctionCounter.builder("strata_data_node_append_bytes", n, DataNode::appendBytes)
                .description("appended payload bytes (rate() = write throughput)").register(reg);
        FunctionCounter.builder("strata_data_node_read_ops", n, DataNode::readOps)
                .description("client READ operations served (rate() = read ops/sec)").register(reg);
        FunctionCounter.builder("strata_data_node_read_bytes", n, DataNode::readBytes)
                .description("client READ payload bytes served (rate() = read throughput)").register(reg);
        FunctionCounter.builder("strata_data_node_background_flush", n, DataNode::backgroundFlushes)
                .description("background-writeback fsyncs of open chunks").register(reg);

        Gauge.builder("strata_data_node_filechannel_cache_size", n, DataNode::cachedChannels)
                .description("open cached sealed-chunk file channels").register(reg);
        Gauge.builder("strata_data_node_filechannel_cache_capacity", n, DataNode::channelCacheCapacity)
                .description("configured channel-cache capacity").register(reg);
        Gauge.builder("strata_data_node_open_fds", n, DataNode::openFds)
                .description("process open file descriptors (-1 if unavailable)").register(reg);

        FunctionCounter.builder("strata_data_node_filechannel_cache", n, DataNode::channelCacheHits)
                .tag("event", "hit").description("sealed-chunk channel cache events").register(reg);
        FunctionCounter.builder("strata_data_node_filechannel_cache", n, DataNode::channelCacheMisses)
                .tag("event", "miss").register(reg);
        FunctionCounter.builder("strata_data_node_filechannel_cache", n, DataNode::channelCacheEvictions)
                .tag("event", "eviction").register(reg);
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
