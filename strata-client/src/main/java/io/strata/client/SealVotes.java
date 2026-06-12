package io.strata.client;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Seal-vote bookkeeping shared by the appender's seal path and recovery (invariant §14.6):
 * replicas vote with their (finalLength, crc); metadata may only be committed over the largest
 * set of replicas that sealed identical bytes, and only if that set reaches the quorum.
 */
final class SealVotes {
    record Key(long finalLength, int crc) {}

    private final Map<Key, List<Integer>> votes = new LinkedHashMap<>();

    void add(long finalLength, int crc, int nodeId) {
        votes.computeIfAbsent(new Key(finalLength, crc), ignored -> new ArrayList<>()).add(nodeId);
    }

    /** Total successful seals across all vote keys. */
    int total() {
        return votes.values().stream().mapToInt(List::size).sum();
    }

    /** True when replicas sealed the same length with different bytes. */
    boolean divergent() {
        return votes.size() > 1;
    }

    /** The largest agreeing set with at least {@code quorum} votes; null if none qualifies. */
    Map.Entry<Key, List<Integer>> best(int quorum) {
        Map.Entry<Key, List<Integer>> best = null;
        for (Map.Entry<Key, List<Integer>> entry : votes.entrySet()) {
            if (entry.getValue().size() < quorum) continue;
            if (best == null || entry.getValue().size() > best.getValue().size()) {
                best = entry;
            }
        }
        return best;
    }
}
