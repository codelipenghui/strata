package io.strata.common;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StrataPathTest {

    @Test
    void acceptsCanonicalAbsolutePaths() {
        StrataPath path = StrataPath.of("/kafka/cluster-a/topic_A.1/00000000000000000000");

        assertEquals("/kafka/cluster-a/topic_A.1/00000000000000000000", path.toString());
    }

    @Test
    void rejectsAmbiguousOrReservedPaths() {
        assertThrows(NullPointerException.class, () -> StrataPath.of(null));
        assertThrows(IllegalArgumentException.class, () -> StrataPath.of(""));
        assertThrows(IllegalArgumentException.class, () -> StrataPath.of("relative"));
        assertThrows(IllegalArgumentException.class, () -> StrataPath.of("/"));
        assertThrows(IllegalArgumentException.class, () -> StrataPath.of("/a/"));
        assertThrows(IllegalArgumentException.class, () -> StrataPath.of("/a//b"));
        assertThrows(IllegalArgumentException.class, () -> StrataPath.of("/a/./b"));
        assertThrows(IllegalArgumentException.class, () -> StrataPath.of("/a/../b"));
        assertThrows(IllegalArgumentException.class, () -> StrataPath.of("/a/__file"));
        assertThrows(IllegalArgumentException.class, () -> StrataPath.of("/a/b c"));
        assertThrows(IllegalArgumentException.class, () -> StrataPath.of("/a/b~c"));
    }

    @Test
    void ordersPathsAndRejectsLengthLimits() {
        assertTrue(StrataPath.of("/b").compareTo(StrataPath.of("/a")) > 0);

        assertThrows(IllegalArgumentException.class,
                () -> StrataPath.of("/" + "a".repeat(StrataPath.MAX_PATH_BYTES)));
        assertThrows(IllegalArgumentException.class,
                () -> StrataPath.of("/" + "a".repeat(StrataPath.MAX_SEGMENT_BYTES + 1)));

        StringBuilder tooManySegments = new StringBuilder();
        for (int i = 0; i < StrataPath.MAX_SEGMENTS + 1; i++) {
            tooManySegments.append("/a");
        }
        assertThrows(IllegalArgumentException.class, () -> StrataPath.of(tooManySegments.toString()));
    }
}
