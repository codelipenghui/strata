package io.strata.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StrataNamespaceTest {

    @Test
    void acceptsSimpleIdentifiers() {
        StrataNamespace namespace = StrataNamespace.of("kafka_cluster-a.1");

        assertEquals("kafka_cluster-a.1", namespace.toString());
        assertEquals("kafka_cluster-a.1", namespace.value());
    }

    @Test
    void rejectsAmbiguousOrReservedNamespaces() {
        assertThrows(NullPointerException.class, () -> StrataNamespace.of(null));
        assertThrows(IllegalArgumentException.class, () -> StrataNamespace.of(""));
        assertThrows(IllegalArgumentException.class, () -> StrataNamespace.of("."));
        assertThrows(IllegalArgumentException.class, () -> StrataNamespace.of(".."));
        assertThrows(IllegalArgumentException.class, () -> StrataNamespace.of("__file"));
        assertThrows(IllegalArgumentException.class, () -> StrataNamespace.of("__internal"));
        assertThrows(IllegalArgumentException.class, () -> StrataNamespace.of("kafka/cluster"));
        assertThrows(IllegalArgumentException.class, () -> StrataNamespace.of("kafka cluster"));
    }
}
