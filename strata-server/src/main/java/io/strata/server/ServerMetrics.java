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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * Registers Strata's domain metrics on the meter registry by wiring Micrometer gauges/counters to
 * the read-only accessors on {@link Controller} / {@link DataNode}. All of these are either
 * periodic gauges over existing in-memory state (zero data-path cost), monotonic function-counters
 * over plain atomic counters, or sampled request timers. The {@code role} common tag is set by
 * {@code StrataMetrics}, so a single Prometheus job can scrape both process kinds.
 */
final class ServerMetrics {
    static final int DEFAULT_REQUEST_LATENCY_SAMPLE_RATE = 16;
    private static final String STATUS_OK = "ok";
    private static final String STATUS_ERROR = "error";

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
        FunctionCounter.builder("strata_controller_authority_revalidation_skips", s,
                        Controller::authorityRevalidationSkips)
                .description("destructive namespace passes skipped because owner authority could not be revalidated")
                .register(reg);
        FunctionCounter.builder("strata_controller_cluster_live_nodes_read_failures", s,
                        Controller::clusterLiveNodesReadFailures)
                .description("published cluster live-node snapshots that failed to read or decode on placement readers")
                .register(reg);
        FunctionCounter.builder("strata_controller_metadata_store_namespace_contract_violations_total", s,
                        Controller::metadataStoreNamespaceContractViolations)
                .description("metadata-store records rejected because their embedded namespace differed from the request")
                .register(reg);

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
        // Namespace-log activity (append/read/compaction/recovery/reacquisition/fallback) is now
        // per-namespace — registered lazily in registerPerNamespace below as
        // strata_controller_namespace_log_*{namespace}.
        // The global controller view is sum without(namespace)(...). (design §3.4)

        registerPerNamespace(reg, s, refreshIntervalMs);
    }

    // Per-namespace controller counter names, index-aligned with NamespaceLogMetrics.stats(): 0 appendRecords,
    // 1 appendBytes, 2 readRecords, 3 readBytes, 4 compactions, 5 recoveries, 6 reacquisitions,
    // 7 ownerChanges, 8 snapshotFallbacks (owner_changes is just index 7 — registered uniformly with
    // the rest, no special case).
    private static final String[] CONTROLLER_NS_COUNTERS = {
            "strata_controller_namespace_log_append_records", "strata_controller_namespace_log_append_bytes",
            "strata_controller_namespace_log_read_records", "strata_controller_namespace_log_read_bytes",
            "strata_controller_namespace_log_compactions", "strata_controller_namespace_log_recoveries",
            "strata_controller_namespace_log_reacquisitions", "strata_controller_namespace_owner_changes",
            "strata_controller_namespace_log_snapshot_fallbacks"};

    /**
     * Per-namespace gauges (namespace-stacked dashboard panels): live files + open metadata-log bytes,
     * labelled by {@code namespace}, for the namespaces THIS controller owns. Refreshed off a daemon
     * timer because the namespace set changes at runtime (a {@link MultiGauge} must be re-registered, not
     * supplier-bound). Cardinality grows with the namespace count; these metadata-log gauges live only on
     * the controller, while data nodes expose their separate per-namespace I/O counters.
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
        NsCounterRegistrationState nsCounters = new NsCounterRegistrationState();
        var refresh = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "controller-ns-metrics");
            t.setDaemon(true);
            return t;
        });
        refresh.scheduleAtFixedRate(() -> {
            Map<String, long[]> stats = s.namespaceStats();
            files.register(stats.entrySet().stream()
                    .map(e -> MultiGauge.Row.of(Tags.of("namespace", e.getKey()), e.getValue()[0]))
                    .collect(Collectors.toList()), true);
            logBytes.register(stats.entrySet().stream()
                    .map(e -> MultiGauge.Row.of(Tags.of("namespace", e.getKey()), e.getValue()[1]))
                    .collect(Collectors.toList()), true);
            String self = s.localControllerEndpoint();
            owner.register(stats.keySet().stream()
                    .map(ns -> MultiGauge.Row.of(Tags.of("namespace", ns, "owner", self), 1))
                    .collect(Collectors.toList()), true);
            registerNewControllerNamespaceCounters(reg, s, nsCounters);
        }, 0, refreshIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Idempotently registers the per-namespace namespace-log + owner-change function-counters for any
     * namespace this controller now owns but hasn't registered yet. Called by the refresh timer; also
     * callable directly (tests) to register without waiting for a tick.
     */
    static void registerNewControllerNamespaceCounters(MeterRegistry reg, Controller s) {
        registerNewControllerNamespaceCounters(reg, s, new NsCounterRegistrationState());
    }

    private static void registerNewControllerNamespaceCounters(MeterRegistry reg, Controller s,
            NsCounterRegistrationState registrations) {
        registerLazyNsCounters(reg, s, s.namespaceLogNamespaces(), CONTROLLER_NS_COUNTERS,
                "per-namespace metadata-log activity / ownership handoffs (rate() = ops/s or bytes/s)",
                Controller::namespaceLogValue, registrations);
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
        Gauge.builder("strata_data_node_delete_waiting", n, DataNode::deleteWaiting)
                .description("delete callers waiting for the physical-delete QoS gate").register(reg);
        Gauge.builder("strata_data_node_delete_inflight", n, DataNode::deleteInFlight)
                .description("physical chunk deletes currently running").register(reg);

        FunctionCounter.builder("strata_data_node_filechannel_cache", n, DataNode::channelCacheHits)
                .tag("event", "hit").description("sealed-chunk channel cache events").register(reg);
        FunctionCounter.builder("strata_data_node_filechannel_cache", n, DataNode::channelCacheMisses)
                .tag("event", "miss").register(reg);
        FunctionCounter.builder("strata_data_node_filechannel_cache", n, DataNode::channelCacheEvictions)
                .tag("event", "eviction").register(reg);
        FunctionCounter.builder("strata_data_node_delete", n, DataNode::deleteOkCount)
                .tag("result", "ok").description("physical chunk delete completions").register(reg);
        FunctionCounter.builder("strata_data_node_delete", n, DataNode::deleteNotFoundCount)
                .tag("result", "not_found").register(reg);
        FunctionCounter.builder("strata_data_node_delete", n, DataNode::deleteFailedCount)
                .tag("result", "failed").register(reg);
        FunctionCounter.builder("strata_data_node_owner_epoch_fence_rejects", n,
                        DataNode::ownerEpochFenceRejects)
                .description("owner RPCs rejected because their owner epoch is stale").register(reg);
        Gauge.builder("strata_data_node_owner_epoch_persistence_poisoned", n,
                        DataNode::ownerEpochPersistencePoisoned)
                .description("1 when durable orphan-confirm epoch persistence is poisoned until restart")
                .register(reg);
        FunctionCounter.builder("strata_data_node_owner_epoch_persistence_rejects_total", n,
                        DataNode::ownerEpochPersistenceRejects)
                .description("durable owner-epoch raises or orphan deletes rejected by persistence poison")
                .register(reg);
        FunctionCounter.builder("strata_data_node_owner_epoch_delete_claim_rejects_total", n,
                        DataNode::ownerEpochDeleteClaimRejects)
                .description("owner RPCs retriably rejected while a committed orphan unlink is in progress")
                .register(reg);
        FunctionCounter.builder("strata_data_node_orphan_gc_owner_epoch_confirm_rejects_total", n,
                        DataNode::orphanGcOwnerEpochConfirmRejects)
                .description("controller orphan-confirm responses rejected by the node owner-epoch gate")
                .register(reg);
        FunctionCounter.builder("strata_data_node_orphan_gc_persistence_poison_confirm_rejects_total", n,
                        DataNode::orphanGcPersistencePoisonConfirmRejects)
                .description("orphan-confirm responses rejected because the durable epoch floor is poisoned")
                .register(reg);
        Gauge.builder("strata_data_node_orphan_gc_breaker_open_namespaces", n,
                        DataNode::orphanGcBreakerOpenNamespaces)
                .description("namespaces whose orphan-GC breaker is open").register(reg);
        Gauge.builder("strata_data_node_orphan_gc_node_breaker_open", n,
                        DataNode::orphanGcNodeBreakerOpen)
                .description("1 when the node-wide orphan-GC breaker is open").register(reg);
        Gauge.builder("strata_data_node_orphan_gc_breaker_halted_namespaces", n,
                        DataNode::orphanGcBreakerHaltedNamespaces)
                .description("namespaces whose suspect chunks were withheld by open orphan-GC breakers "
                        + "during the last pass").register(reg);
        Gauge.builder("strata_data_node_orphan_gc_breaker_halted_chunks", n,
                        DataNode::orphanGcBreakerHaltedChunks)
                .description("suspect chunks withheld by open orphan-GC breakers during the last pass").register(reg);
        FunctionCounter.builder("strata_data_node_orphan_gc_breaker_trips_total", n,
                        DataNode::orphanGcBreakerTrips)
                .description("orphan-GC breaker openings").register(reg);
        FunctionCounter.builder("strata_data_node_orphan_gc_cumulative_breaker_trips_total", n,
                        DataNode::orphanGcCumulativeBreakerTrips)
                .description("orphan-GC process-lifetime cumulative breaker openings").register(reg);
        FunctionCounter.builder("strata_data_node_orphan_gc_breaker_skipped_chunk_total", n,
                        DataNode::orphanGcBreakerSkippedChunkTotal)
                .description("confirmed orphan deletes skipped when opening orphan-GC breakers").register(reg);
        FunctionCounter.builder("strata_data_node_orphan_gc_already_deleted_total", n,
                        DataNode::orphanGcAlreadyDeletedTotal)
                .description("confirmed orphan deletes where another delete lane had already removed the chunk")
                .register(reg);

        // Per-namespace data throughput: register a function-counter per namespace as it first appears
        // (via ioNamespaces()). Refreshed off a daemon timer because the namespace set changes at runtime.
        var refresh = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "data-node-ns-metrics");
            t.setDaemon(true);
            return t;
        });
        NsCounterRegistrationState nsCounters = new NsCounterRegistrationState();
        refresh.scheduleAtFixedRate(() -> registerNewDataNodeNamespaces(reg, n, nsCounters),
                0, refreshIntervalMs, TimeUnit.MILLISECONDS);
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
            String[] names, String description, NsCounterReader<T> reader,
            NsCounterRegistrationState registrations) {
        for (String ns : namespaces) {
            for (int i = 0; i < names.length; i++) {
                final int idx = i;
                final String namespace = ns;
                String name = names[i];
                if (registrations.mark(name, namespace)) {
                    boolean registered = false;
                    try {
                        if (reg.find(name).tag("namespace", namespace).functionCounter() == null) {
                            FunctionCounter.builder(name, source, src -> reader.valueOf(src, namespace, idx))
                                    .tag("namespace", namespace).description(description).register(reg);
                        }
                        registered = true;
                    } finally {
                        if (!registered) {
                            registrations.unmark(name, namespace);
                        }
                    }
                }
            }
        }
    }

    private static final class NsCounterRegistrationState {
        private final Set<NsCounterKey> registered = ConcurrentHashMap.newKeySet();

        boolean mark(String name, String namespace) {
            return registered.add(new NsCounterKey(name, namespace));
        }

        void unmark(String name, String namespace) {
            registered.remove(new NsCounterKey(name, namespace));
        }
    }

    private record NsCounterKey(String name, String namespace) {
    }

    /**
     * Idempotently registers the per-namespace data-throughput function-counters for any namespace that has
     * seen I/O. Called by the refresh timer; also callable directly (tests) to register without a tick.
     */
    static void registerNewDataNodeNamespaces(MeterRegistry reg, DataNode n) {
        registerNewDataNodeNamespaces(reg, n, new NsCounterRegistrationState());
    }

    private static void registerNewDataNodeNamespaces(MeterRegistry reg, DataNode n,
            NsCounterRegistrationState registrations) {
        registerLazyNsCounters(reg, n, n.ioNamespaces(), DATA_NODE_NS_COUNTERS,
                "per-namespace data throughput (rate() = ops/s or bytes/s)", DataNode::ioValue, registrations);
    }

    /**
     * A per-request observer that keeps exact request counts in {@code strata_scp_requests} and samples
     * successful latency observations into {@code strata_scp_request_duration}; error latency is always
     * recorded. The duration timer emits a Prometheus HISTOGRAM (cumulative {@code _bucket{le}} series)
     * rather than client-side quantiles, so percentiles can be aggregated across the node fleet at query
     * time. Explicit SLO buckets (1ms..5s) bound cardinality and pick boundaries meaningful for SCP
     * request latency. For an async APPEND in fsync mode this latency includes the group-commit/fsync wait.
     */
    static RequestObserver requestObserver(MeterRegistry reg, long[] bucketsMs) {
        return requestObserver(reg, bucketsMs, DEFAULT_REQUEST_LATENCY_SAMPLE_RATE);
    }

    static RequestObserver requestObserver(MeterRegistry reg, long[] bucketsMs, int successLatencySampleRate) {
        if (successLatencySampleRate <= 0) {
            throw new IllegalArgumentException("successLatencySampleRate must be positive: "
                    + successLatencySampleRate);
        }
        Duration[] slos = new Duration[bucketsMs.length];
        for (int i = 0; i < bucketsMs.length; i++) {
            slos[i] = Duration.ofMillis(bucketsMs[i]);
        }
        ConcurrentHashMap<String, RequestMetricFamily> metrics = new ConcurrentHashMap<>();
        return (opcode, namespace, durationNanos, success) -> {
            RequestMetricFamily family = metrics.get(opcode);
            if (family == null) {
                RequestMetricFamily created = new RequestMetricFamily();
                RequestMetricFamily existing = metrics.putIfAbsent(opcode, created);
                family = existing == null ? created : existing;
            }
            RequestMetric metric = family.metric(reg, slos, opcode, namespace, success);
            metric.requests.increment();
            if (!success || successLatencySampleRate == 1
                    || ThreadLocalRandom.current().nextInt(successLatencySampleRate) == 0) {
                metric.latency.record(durationNanos, TimeUnit.NANOSECONDS);
            }
        };
    }

    private static final class RequestMetricFamily {
        private final ConcurrentHashMap<String, RequestMetric> ok = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, RequestMetric> error = new ConcurrentHashMap<>();

        RequestMetric metric(MeterRegistry reg, Duration[] slos, String opcode, String namespace, boolean success) {
            ConcurrentHashMap<String, RequestMetric> byNamespace = success ? ok : error;
            String namespaceTag = String.valueOf(namespace);
            RequestMetric metric = byNamespace.get(namespaceTag);
            if (metric != null) {
                return metric;
            }
            synchronized (byNamespace) {
                metric = byNamespace.get(namespaceTag);
                if (metric == null) {
                    metric = registerMetric(reg, slos, opcode, namespaceTag, success ? STATUS_OK : STATUS_ERROR);
                    byNamespace.put(namespaceTag, metric);
                }
                return metric;
            }
        }

        private static RequestMetric registerMetric(MeterRegistry reg, Duration[] slos, String opcode,
                                                    String namespace, String status) {
            Timer.Builder b = Timer.builder("strata_scp_request_duration")
                    .description("sampled request handler latency by opcode + namespace "
                            + "(errors always recorded; includes async durability wait)")
                    .tag("opcode", opcode)
                    .tag("status", status)
                    .tag("namespace", namespace);
            b.serviceLevelObjectives(slos);
            RequestMetric created = new RequestMetric(b.register(reg));
            FunctionCounter.builder("strata_scp_requests", created.requests, LongAdder::sum)
                    .description("exact SCP request count by opcode + namespace + status")
                    .tag("opcode", opcode)
                    .tag("status", status)
                    .tag("namespace", namespace)
                    .register(reg);
            return created;
        }
    }

    private static final class RequestMetric {
        final LongAdder requests = new LongAdder();
        final Timer latency;

        RequestMetric(Timer latency) {
            this.latency = latency;
        }
    }
}
