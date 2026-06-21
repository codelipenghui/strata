package io.strata.meta;

import io.strata.common.StrataNamespace;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NamespaceAssignmentPolicyTest {

    private static final List<String> THREE = List.of("m1:9301", "m2:9301", "m3:9301");

    @Test
    void assignmentIsDeterministic() {
        StrataNamespace ns = StrataNamespace.of("tenant-a");
        NamespaceAssignmentPolicy.Assignment a = NamespaceAssignmentPolicy.assign(ns, 0, THREE, 3);
        NamespaceAssignmentPolicy.Assignment b = NamespaceAssignmentPolicy.assign(ns, 0, THREE, 3);
        assertEquals(a.replicaSet(), b.replicaSet());
        assertEquals(a.preferredLeader(), b.preferredLeader());
    }

    @Test
    void replicaSetSizeIsClampedToMembership() {
        StrataNamespace ns = StrataNamespace.of("tenant-a");
        assertEquals(3, NamespaceAssignmentPolicy.assign(ns, 0, THREE, 3).replicaSet().size());
        assertEquals(2, NamespaceAssignmentPolicy.assign(ns, 0, THREE, 2).replicaSet().size());
        assertEquals(1, NamespaceAssignmentPolicy.assign(ns, 0, THREE, 1).replicaSet().size());
        assertEquals(3, NamespaceAssignmentPolicy.assign(ns, 0, THREE, 9).replicaSet().size(),
                "replicaCount above membership is clamped down");
        assertEquals(1, NamespaceAssignmentPolicy.assign(ns, 0, THREE, 0).replicaSet().size(),
                "replicaCount below 1 is clamped up");
    }

    @Test
    void preferredLeaderIsFirstAndInReplicaSet() {
        for (int i = 0; i < 50; i++) {
            StrataNamespace ns = StrataNamespace.of("ns-" + i);
            NamespaceAssignmentPolicy.Assignment a = NamespaceAssignmentPolicy.assign(ns, 0, THREE, 3);
            assertEquals(a.replicaSet().get(0), a.preferredLeader());
            assertTrue(a.replicaSet().contains(a.preferredLeader()));
        }
    }

    @Test
    void emptyMembershipYieldsEmptyReplicaSet() {
        NamespaceAssignmentPolicy.Assignment a =
                NamespaceAssignmentPolicy.assign(StrataNamespace.of("tenant-a"), 0, List.of(), 3);
        assertTrue(a.replicaSet().isEmpty());
        assertThrows(IllegalStateException.class, a::preferredLeader);
    }

    @Test
    void leadershipIsSpreadAcrossEndpoints() {
        Map<String, Integer> leads = new HashMap<>();
        for (int i = 0; i < 300; i++) {
            String leader = NamespaceAssignmentPolicy.assign(StrataNamespace.of("ns-" + i), 0, THREE, 3)
                    .preferredLeader();
            leads.merge(leader, 1, Integer::sum);
        }
        for (String e : THREE) {
            assertTrue(leads.getOrDefault(e, 0) > 0, "endpoint " + e + " should lead some namespaces");
        }
    }

    @Test
    void addingAnEndpointOnlyMovesNamespacesThatHashToIt() {
        List<String> before = List.of("m1:9301", "m2:9301", "m3:9301");
        List<String> after = List.of("m1:9301", "m2:9301", "m3:9301", "m4:9301");
        int moved = 0;
        int total = 400;
        for (int i = 0; i < total; i++) {
            StrataNamespace ns = StrataNamespace.of("ns-" + i);
            String b = NamespaceAssignmentPolicy.assign(ns, 0, before, 3).preferredLeader();
            String a = NamespaceAssignmentPolicy.assign(ns, 0, after, 3).preferredLeader();
            if (!a.equals(b)) {
                assertEquals("m4:9301", a, "a namespace only ever moves onto the newly added endpoint");
                moved++;
            }
        }
        // HRW minimal disruption: only a fraction move, and every move lands on the new endpoint.
        assertTrue(moved > 0 && moved < total / 2, "moved=" + moved);
    }
}
