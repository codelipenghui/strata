package io.strata.meta;

import io.strata.common.ErrorCode;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Capacity-weighted random placement with anti-affinity (tech design §8):
 * weights derive from free capacity; no two replicas share a host; only REGISTERED nodes are eligible,
 * so DEAD and DRAINING nodes are excluded. Suspect REGISTERED nodes inside dead-grace remain candidates
 * so metadata failover or heartbeat stalls do not remove otherwise reachable storage nodes before node
 * RPCs can probe them.
 */
final class Placement {

    /**
     * Picks {@code count} distinct nodes to hold replicas of a chunk in {@code namespace}; throws
     * NO_CAPACITY when impossible. The namespace is the per-tenant placement hook: a future policy can
     * restrict candidates to a namespace's allowed nodes (affinity/isolation) or weight by per-namespace
     * usage. The current policy is namespace-agnostic.
     */
    static List<NodeRegistry.LiveNode> choose(StrataNamespace namespace, NodeRegistry registry, int count,
                                              Set<Integer> excludeNodes, Set<String> excludeHosts) {
        List<NodeRegistry.LiveNode> candidates = new ArrayList<>();
        for (NodeRegistry.LiveNode n : registry.candidatesFor(namespace)) {
            if (excludeNodes.contains(n.record.nodeId())) continue;
            candidates.add(n);
        }
        List<NodeRegistry.LiveNode> picked = new ArrayList<>(count);
        Set<String> usedHosts = new HashSet<>(excludeHosts);
        for (int i = 0; i < count; i++) {
            NodeRegistry.LiveNode pick = weightedPick(candidates, usedHosts);
            if (pick == null) {
                throw new ScpException(ErrorCode.NO_CAPACITY,
                        "need " + count + " nodes, found " + picked.size());
            }
            picked.add(pick);
            usedHosts.add(pick.record.host());
            candidates.remove(pick);
        }
        return picked;
    }

    private static NodeRegistry.LiveNode weightedPick(List<NodeRegistry.LiveNode> candidates,
                                                      Set<String> usedHosts) {
        double totalWeight = 0;
        List<NodeRegistry.LiveNode> eligible = new ArrayList<>();
        for (NodeRegistry.LiveNode n : candidates) {
            if (usedHosts.contains(n.record.host())) continue;
            if (n.freeBytes <= 0) continue;
            double w = Math.max(1.0d, (double) n.freeBytes);
            eligible.add(n);
            totalWeight += w;
        }
        if (eligible.isEmpty()) return null;
        double r = ThreadLocalRandom.current().nextDouble(totalWeight);
        double acc = 0;
        for (NodeRegistry.LiveNode n : eligible) {
            acc += Math.max(1.0d, (double) n.freeBytes);
            if (r < acc) return n;
        }
        return eligible.get(eligible.size() - 1);
    }

    private Placement() {}
}
