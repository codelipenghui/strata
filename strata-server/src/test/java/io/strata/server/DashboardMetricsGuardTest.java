package io.strata.server;

import org.junit.jupiter.api.Test;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the provisioned Grafana dashboards against drift from the metric surface: no dashboard may
 * reference a metric name that was removed/renamed (here, the global {@code strata_controller_log_*}
 * counters that became per-namespace {@code strata_controller_namespace_log_*}). A stale reference would
 * render a silently-empty panel in production, so fail the build instead.
 */
class DashboardMetricsGuardTest {

    private static final List<String> REMOVED_METRICS = List.of(
            "strata_controller_log_append_records",
            "strata_controller_log_append_bytes",
            "strata_controller_log_compactions",
            "strata_controller_log_recoveries",
            "strata_controller_log_reacquisitions");

    @Test
    void noDashboardReferencesRemovedMetrics() throws Exception {
        Path dir = locateDashboards();
        int checked = 0;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*.json")) {
            for (Path f : files) {
                String body = Files.readString(f);
                for (String metric : REMOVED_METRICS) {
                    assertFalse(body.contains(metric),
                            f.getFileName() + " references removed metric " + metric
                                    + " (renamed to strata_controller_namespace_log_*)");
                }
                checked++;
            }
        }
        assertTrue(checked >= 4, "expected to scan the provisioned dashboards, found " + checked);
    }

    /** Walks up from the test working directory to the repo's {@code deploy/grafana/dashboards}. */
    private static Path locateDashboards() {
        Path p = Path.of("").toAbsolutePath();
        for (int i = 0; i < 6 && p != null; i++) {
            Path candidate = p.resolve("deploy/grafana/dashboards");
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            p = p.getParent();
        }
        throw new IllegalStateException(
                "could not locate deploy/grafana/dashboards from " + Path.of("").toAbsolutePath());
    }
}
