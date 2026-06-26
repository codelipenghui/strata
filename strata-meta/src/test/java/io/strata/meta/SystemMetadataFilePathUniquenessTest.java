package io.strata.meta;

import io.strata.client.StrataClient;
import io.strata.common.FailureInjector;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression for the "deterministic system-file path" wedge (perf-run bug A):
 *
 * <p>The crash window: an owner calls {@code publishCompacted}, which creates snapshot + log files
 * for {@code gen=N} and then CAS-publishes the manifest. If the owner crashes BEFORE the CAS,
 * those files exist (in ZK / on disk) but the manifest still points at gen=N-1. The successor
 * recovers, computes {@code newGeneration = generation + 1 = N} again, and tries to create files at
 * the SAME path. With the old deterministic path ({@code /metadata-log/<ns>/gen-<N>/<kind>}) ZK
 * throws {@code NodeExistsException} → the namespace is permanently wedged.
 *
 * <h3>Tests</h3>
 * <ol>
 *   <li>{@link #systemFileSpecPathIsUniquePerCallForSameNsGenerationKind()} — directly asserts that
 *       two successive calls to {@link StrataSystemMetadataFileStore#systemFileSpec} (exposed via
 *       reflection) with identical {@code (ns, generation, kind)} return paths with DISTINCT leaf
 *       tokens. <b>FAILS before the UUID-suffix fix</b> (both paths are identical), passes after.
 *   <li>{@link #successorAfterPreCasCrashSucceedsWithUniqueIdStore()} — integration: the
 *       crash+retry scenario completes without error when the file store assigns unique ids per
 *       create call (mimicking the UUID-path production fix). Provides coverage for the post-fix
 *       scenario.
 * </ol>
 */
class SystemMetadataFilePathUniquenessTest {

    private static final StrataNamespace NS = StrataNamespace.of("perf-7");

    @AfterEach
    void disarm() {
        FailureInjector.reset();
    }

    /**
     * <b>Primary regression test (FAILS pre-fix):</b> calling {@code systemFileSpec} twice with
     * the same {@code (ns, generation, kind)} must return paths that differ in the leaf so that a
     * retry or race never creates the same ZK node twice.
     *
     * <p>Pre-fix the method returned {@code /metadata-log/<ns>/gen-<N>/<kind>} — identical on both
     * calls → any retry would throw {@code NodeExistsException}. Post-fix the method appends a fresh
     * UUID on each call → the two paths always differ.
     */
    @Test
    void systemFileSpecPathIsUniquePerCallForSameNsGenerationKind() throws Exception {
        StrataSystemMetadataFileStore store = new StrataSystemMetadataFileStore(
                () -> "127.0.0.1:0", // endpoint unused — systemFileSpec doesn't connect
                3, 2, false);

        Method systemFileSpec = StrataSystemMetadataFileStore.class.getDeclaredMethod(
                "systemFileSpec", StrataNamespace.class, long.class, String.class);
        systemFileSpec.setAccessible(true);

        // The orphan GC reaps by generation, so its path parser MUST invert systemFileSpec. If the path
        // format ever changes, this round-trip fails — forcing parseSystemFilePath to be updated in lockstep
        // (otherwise the GC silently fails safe and stops reaping). A non-system path must parse to null.
        StrataClient.FileSpec genSpec = (StrataClient.FileSpec) systemFileSpec.invoke(store, NS, 42L, "snapshot");
        StrataSystemMetadataFileStore.SystemFileCoord coord =
                StrataSystemMetadataFileStore.parseSystemFilePath(genSpec.path());
        assertNotNull(coord, "the parser must recognize a path the builder produced");
        assertEquals(NS, coord.namespace(), "round-trip namespace");
        assertEquals(42L, coord.generation(), "round-trip generation");
        assertNull(StrataSystemMetadataFileStore.parseSystemFilePath(StrataPath.of("/some/user/file")),
                "a non-system path must parse to null so the GC keeps it (fail-safe)");
        // ADVERSARIAL namespace names that collide with the path literals ("metadata-log", "gen-N") are
        // legal (only "strata-meta"/"__"-prefixed are reserved) and MUST parse positionally, not by scanning
        // — a misparse here would let the generation GC reap a live in-flight file for that namespace.
        for (String adversarial : new String[]{"metadata-log", "gen-8"}) {
            StrataNamespace advNs = StrataNamespace.of(adversarial);
            StrataClient.FileSpec advSpec = (StrataClient.FileSpec) systemFileSpec.invoke(store, advNs, 5L, "log");
            StrataSystemMetadataFileStore.SystemFileCoord advCoord =
                    StrataSystemMetadataFileStore.parseSystemFilePath(advSpec.path());
            assertNotNull(advCoord, "must parse a path for namespace '" + adversarial + "'");
            assertEquals(advNs, advCoord.namespace(), "namespace '" + adversarial + "' must not be misparsed");
            assertEquals(5L, advCoord.generation(), "generation for namespace '" + adversarial + "'");
        }

        StrataClient.FileSpec spec1 = (StrataClient.FileSpec) systemFileSpec.invoke(store, NS, 2L, "log");
        StrataClient.FileSpec spec2 = (StrataClient.FileSpec) systemFileSpec.invoke(store, NS, 2L, "log");

        assertNotNull(spec1.path(), "spec1 must have a path");
        assertNotNull(spec2.path(), "spec2 must have a path");
        assertNotEquals(spec1.path(), spec2.path(),
                "two successive systemFileSpec calls for the same (ns, generation, kind) must produce "
                        + "DISTINCT paths — pre-fix both were identical, causing NodeExistsException on retry; "
                        + "post-fix each call appends a fresh UUID. Got: " + spec1.path() + " vs " + spec2.path());

        // Sanity: both paths must still embed (ns, generation, kind) for human traceability.
        String p1 = spec1.path().toString();
        assertTrue(p1.contains(NS.value()), "path must contain namespace");
        assertTrue(p1.contains("gen-2"),    "path must contain generation");
        assertTrue(p1.contains("log"),      "path must contain kind");
    }

    /**
     * Integration: the crash+retry scenario (manifest CAS crash, then successor open) completes
     * without error when the file store assigns unique ids per create call — matching production
     * post-fix behaviour.
     */
    @Test
    void successorAfterPreCasCrashSucceedsWithUniqueIdStore() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkMetadataStore root = new ZkMetadataStore(zk.getConnectString())) {

            TestNamespaceMetadataFileStore fs = new TestNamespaceMetadataFileStore();

            NamespaceMetadataLogRepository owner = NamespaceMetadataLogRepository.open(NS, fs, root, 1);

            // Crash before manifest CAS for gen=2 — orphaned files created but not published.
            FailureInjector.arm("meta.log.beforeManifestPublish",
                    p -> { throw new RuntimeException("simulated pre-CAS crash"); });
            try {
                owner.compactAndPublish();
            } catch (RuntimeException expected) {
                // expected — crash point fired
            }
            FailureInjector.reset();

            // Successor must open successfully at gen=2 (same as crashed attempt) with no collision.
            NamespaceMetadataLogRepository successor = NamespaceMetadataLogRepository.open(NS, fs, root, 2);
            assertNotNull(successor, "successor must open without error after a pre-CAS crash");
            assertTrue(successor.generation() >= 2,
                    "successor published a new generation (>= 2), got: " + successor.generation());
        }
    }


}
