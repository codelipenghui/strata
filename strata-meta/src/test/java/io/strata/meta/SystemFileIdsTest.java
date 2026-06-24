package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies that {@link SystemFileIds} produces no collisions over the expected cardinality. */
class SystemFileIdsTest {

    @Test
    void systemFileIdsAreCollisionFreeAcrossNsGenerationKind() {
        Set<FileId> seen = new HashSet<>();
        for (String ns : new String[]{"perf-0", "perf-1", "tenant.x"}) {
            for (long g = 0; g < 1000; g++) {
                assertTrue(seen.add(SystemFileIds.of(StrataNamespace.of(ns), g, 0)),
                        "collision at ns=" + ns + " gen=" + g + " kind=0");
                assertTrue(seen.add(SystemFileIds.of(StrataNamespace.of(ns), g, 1)),
                        "collision at ns=" + ns + " gen=" + g + " kind=1");
            }
        }
        // 3 namespaces × 1000 generations × 2 kinds = 6000 distinct ids
    }
}
