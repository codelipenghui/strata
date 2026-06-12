package io.strata.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StrataNamespaceTest {

    @Test
    void acceptsSimpleIdentifiers() {
        StrataNamespace namespace = StrataNamespace.of("Kafka_cluster-a.1");

        assertEquals("Kafka_cluster-a.1", namespace.toString());
        assertEquals("Kafka_cluster-a.1", namespace.value());
        assertTrue(StrataNamespace.of("b").compareTo(StrataNamespace.of("a")) > 0);
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
        assertThrows(IllegalArgumentException.class, () -> StrataNamespace.of("kafka~cluster"));
        assertThrows(IllegalArgumentException.class,
                () -> StrataNamespace.of("a".repeat(StrataNamespace.MAX_BYTES + 1)));
    }
}
