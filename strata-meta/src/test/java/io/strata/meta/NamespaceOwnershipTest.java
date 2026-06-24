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
    void everyNamespaceIsOwnedByExactlyOneEndpoint() {
        NamespaceOwnership m1 = new NamespaceOwnership("m1:9301", THREE, 0, 3);
        NamespaceOwnership m2 = new NamespaceOwnership("m2:9301", THREE, 0, 3);
        NamespaceOwnership m3 = new NamespaceOwnership("m3:9301", THREE, 0, 3);
        for (int i = 0; i < 100; i++) {
            StrataNamespace ns = StrataNamespace.of("ns-" + i);
            int owners = (m1.isOwner(ns) ? 1 : 0) + (m2.isOwner(ns) ? 1 : 0) + (m3.isOwner(ns) ? 1 : 0);
            assertEquals(1, owners, "exactly one endpoint owns " + ns);
            String owner = m1.ownerOf(ns);
            assertEquals(owner, m2.ownerOf(ns), "all nodes compute the same owner");
            assertEquals(owner, m3.ownerOf(ns));
            assertEquals(owner.equals("m2:9301"), m2.isOwner(ns));
        }
    }

    @Test
    void ownerMatchesPolicyPreferredLeader() {
        NamespaceOwnership own = new NamespaceOwnership("m2:9301", THREE, 0, 3);
        StrataNamespace ns = StrataNamespace.of("tenant-x");
        assertEquals(NamespaceAssignmentPolicy.assign(ns, 0, THREE, 3).preferredLeader(),
                own.ownerOf(ns));
        assertEquals(NamespaceAssignmentPolicy.assign(ns, 0, THREE, 3).replicaSet(),
                own.assignmentOf(ns).replicaSet());
    }
}
