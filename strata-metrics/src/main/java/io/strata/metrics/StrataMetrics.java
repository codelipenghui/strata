package io.strata.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

/**
 * Owns the process-wide Prometheus meter registry and registers the standard JVM/process binders.
 * A {@code role} (controller|data-node) common tag is applied to every meter so one Prometheus job can scrape
 * both kinds of process and disambiguate by label.
 *
 * <p>Server-side only: strata data-node/controller/server depend on this module; the client/proto/format/
 * common modules deliberately do not, so Micrometer never enters the client API or the data-path
 * hot loops. Instrumentation is done at the data-node/controller layer as periodic gauges over existing state
 * plus a few cheap function-counters — see {@code StrataServer} for the wiring.
 */
public final class StrataMetrics implements AutoCloseable {
    private final PrometheusMeterRegistry registry;

    public StrataMetrics(String role) {
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.config().commonTags("role", role);
        // Free and operator-essential: GC pauses, heap, threads, CPU, file descriptors, uptime.
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new FileDescriptorMetrics().bindTo(registry);
        new UptimeMetrics().bindTo(registry);
    }

    /** The registry to register gauges/counters on. */
    public MeterRegistry registry() {
        return registry;
    }

    /** Prometheus text exposition for the {@code /metrics} endpoint. */
    public String scrape() {
        return registry.scrape();
    }

    @Override
    public void close() {
        registry.close();
    }
}
