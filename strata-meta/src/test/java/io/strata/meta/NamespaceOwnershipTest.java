package io.strata.meta;

import io.strata.common.StrataNamespace;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NamespaceOwnershipTest {

    private static final List<String> THREE = List.of("m1:9301", "m2:9301", "m3:9301");

    @Test
    void singleEndpointOwnsEveryNamespace() {
        NamespaceOwnership own = new NamespaceOwnership("m1:9301", List.of("m1:9301"), 0, 3);
        assertTrue(own.ownsAll());
        for (int i = 0; i < 20; i++) {
            StrataNamespace ns = StrataNamespace.of("ns-" + i);
            assertTrue(own.isOwner(ns));
            assertEquals("m1:9301", own.ownerOf(ns));
        }
    }

    @Test
    void emptyMembershipOwnsEveryNamespace() {
        NamespaceOwnership own = new NamespaceOwnership("m1:9301", List.of(), 0, 3);
        assertTrue(own.ownsAll());
        assertTrue(own.isOwner(StrataNamespace.of("tenant-a")));
        assertEquals("m1:9301", own.ownerOf(StrataNamespace.of("tenant-a")));
    }

    @Test
    void singleEndpointMembershipMustNameThisNode() {
        // a lone endpoint that is NOT this node would silently route everything here (ownsAll) — reject it
        assertThrows(IllegalArgumentException.class,
                () -> new NamespaceOwnership("m1:9301", List.of("m2:9301"), 0, 3));
    }

    @Test
    void multiEndpointStaticOwnershipIsRemoved() {
        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
                () -> new NamespaceOwnership("m1:9301", THREE, 0, 3));
        assertTrue(rejected.getMessage().contains("persisted assignments"));
    }
}
