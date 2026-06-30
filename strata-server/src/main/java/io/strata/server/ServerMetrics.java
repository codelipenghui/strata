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
    static void registerController(MeterRegistry reg, Controller s, long refreshIntervalMs) {
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
        // Namespace-log activity (append/read/compaction/recovery/reacquisition) is now per-namespace —
        // registered lazily in registerPerNamespace below as strata_controller_namespace_log_*{namespace}.
        // The global controller view is sum without(namespace)(...). (design §3.4)

        registerPerNamespace(reg, s, refreshIntervalMs);
    }

    // Per-namespace controller counter names, index-aligned with NamespaceLogMetrics.stats(): 0 appendRecords,
    // 1 appendBytes, 2 readRecords, 3 readBytes, 4 compactions, 5 recoveries, 6 reacquisitions, 7 ownerChanges
    // (owner_changes is just index 7 — registered uniformly with the rest, no special case).
    private static final String[] CONTROLLER_NS_COUNTERS = {
            "strata_controller_namespace_log_append_records", "strata_controller_namespace_log_append_bytes",
            "strata_controller_namespace_log_read_records", "strata_controller_namespace_log_read_bytes",
            "strata_controller_namespace_log_compactions", "strata_controller_namespace_log_recoveries",
            "strata_controller_namespace_log_reacquisitions", "strata_controller_namespace_owner_changes"};

    /**
     * Per-namespace gauges (namespace-stacked dashboard panels): live files + open metadata-log bytes,
     * labelled by {@code namespace}, for the namespaces THIS controller owns. Refreshed off a daemon
     * timer because the namespace set changes at runtime (a {@link MultiGauge} must be re-registered, not
     * supplier-bound). Cardinality grows with the namespace count — namespace stays control-plane, so
     * these live only on the controller (data nodes never see namespaces).
     */
    private static void registerPerNamespace(MeterRegistry reg, Controller s, long refreshIntervalMs) {
        MultiGauge files = MultiGauge.builder("strata_controller_namespace_files")
                .description("live files per namespace owned by this controller").register(reg);
        MultiGauge logBytes = MultiGauge.builder("strata_controller_namespace_log_bytes")
                .description("open metadata-log bytes per namespace owned by this controller").register(reg);
        // The current owner of each namespace: emitted as 1 ONLY by the controller that owns it (the rows
        // come from namespaceStats(), which only lists owned namespaces), tagged with this controller's
        // endpoint. A state-timeline over this series shows the owner and visibly flips on handoff (design §3.5).
        MultiGauge owner = MultiGauge.builder("strata_controller_namespace_owner")
                .description("=1 from the controller that currently owns the namespace (owner = its endpoint)").register(reg);
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
            String self = s.localControllerEndpoint();
            owner.register(stats.keySet().stream()
                    .map(ns -> MultiGauge.Row.of(Tags.of("namespace", ns, "owner", self), 1))
                    .collect(java.util.stream.Collectors.toList()), true);
            registerNewControllerNamespaceCounters(reg, s);
        }, 0, refreshIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Idempotently registers the per-namespace namespace-log + owner-change function-counters for any
     * namespace this controller now owns but hasn't registered yet. Called by the refresh timer; also
     * callable directly (tests) to register without waiting for a tick.
     */
    static void registerNewControllerNamespaceCounters(MeterRegistry reg, Controller s) {
        registerLazyNsCounters(reg, s, s.namespaceLogNamespaces(), CONTROLLER_NS_COUNTERS,
                "per-namespace metadata-log activity / ownership handoffs (rate() = ops/s or bytes/s)",
                Controller::namespaceLogValue);
    }

    /** Data plane: capacity, chunk state, write throughput, fsync force rate, registration. */
    static void registerDataNode(MeterRegistry reg, DataNode n, long refreshIntervalMs) {
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
        // append/read ops+bytes are now per-namespace (strata_data_node_{append,read}_{ops,bytes}_total
        // carry a {namespace} tag) — registered lazily below as namespaces first see I/O. The fleet rollup
        // is sum without(namespace)(...). Namespace is the data plane's primary metrics axis (design §3.3).
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

        // Per-namespace data throughput: register a function-counter per namespace as it first appears
        // (via ioNamespaces()). Refreshed off a daemon timer because the namespace set changes at runtime.
        var refresh = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "data-node-ns-metrics");
            t.setDaemon(true);
            return t;
        });
        refresh.scheduleAtFixedRate(() -> registerNewDataNodeNamespaces(reg, n), 0, refreshIntervalMs, TimeUnit.MILLISECONDS);
    }

    private static final String[] DATA_NODE_NS_COUNTERS = {
            "strata_data_node_append_ops", "strata_data_node_append_bytes",
            "strata_data_node_read_ops", "strata_data_node_read_bytes"};

    /** Reads one per-namespace counter value (index into the source's namespace-counter array). */
    @FunctionalInterface
    interface NsCounterReader<T> {
        double valueOf(T source, String namespace, int index);
    }

    /**
     * Idempotently registers, per namespace, one monotonic function-counter per name — each bound to read a
     * SINGLE counter via {@code reader} (O(1) per scrape, no per-scrape map allocation). A counter is never
     * deregistered once its namespace goes idle (its value freezes), so cardinality is bounded by namespaces
     * ever seen and counter semantics stay monotonic.
     */
    private static <T> void registerLazyNsCounters(MeterRegistry reg, T source, Iterable<String> namespaces,
            String[] names, String description, NsCounterReader<T> reader) {
        for (String ns : namespaces) {
            for (int i = 0; i < names.length; i++) {
                if (reg.find(names[i]).tag("namespace", ns).functionCounter() == null) {
                    final int idx = i;
                    final String namespace = ns;
                    FunctionCounter.builder(names[i], source, src -> reader.valueOf(src, namespace, idx))
                            .tag("namespace", namespace).description(description).register(reg);
                }
            }
        }
    }

    /**
     * Idempotently registers the per-namespace data-throughput function-counters for any namespace that has
     * seen I/O. Called by the refresh timer; also callable directly (tests) to register without a tick.
     */
    static void registerNewDataNodeNamespaces(MeterRegistry reg, DataNode n) {
        registerLazyNsCounters(reg, n, n.ioNamespaces(), DATA_NODE_NS_COUNTERS,
                "per-namespace data throughput (rate() = ops/s or bytes/s)", DataNode::ioValue);
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
    static RequestObserver requestObserver(MeterRegistry reg, long[] bucketsMs) {
        Duration[] slos = new Duration[bucketsMs.length];
        for (int i = 0; i < bucketsMs.length; i++) {
            slos[i] = Duration.ofMillis(bucketsMs[i]);
        }
        Map<String, Timer> timers = new ConcurrentHashMap<>();
        return (opcode, namespace, durationNanos, success) -> {
            String status = success ? "ok" : "error";
            timers.computeIfAbsent(opcode + ':' + status + ':' + namespace, k -> {
                Timer.Builder b = Timer.builder("strata_scp_request_duration")
                        .description("request handler latency by opcode + namespace (incl. async durability wait)")
                        .tag("opcode", opcode)
                        .tag("status", status)
                        .tag("namespace", namespace);
                b.serviceLevelObjectives(slos);
                return b.register(reg);
            }).record(durationNanos, TimeUnit.NANOSECONDS);
        };
    }
}
