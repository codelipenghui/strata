package io.strata.common;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StrataPathTest {

    @Test
    void acceptsCanonicalAbsolutePaths() {
        StrataPath path = StrataPath.of("/kafka/cluster-a/topic_A.1/00000000000000000000");

        assertEquals("/kafka/cluster-a/topic_A.1/00000000000000000000", path.toString());
        assertEquals(List.of("kafka", "cluster-a", "topic_A.1", "00000000000000000000"), path.segments());
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
    }
}
