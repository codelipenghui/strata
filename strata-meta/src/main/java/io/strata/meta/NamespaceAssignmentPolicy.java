package io.strata.meta;

import io.strata.common.StrataNamespace;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Static rendezvous-hash (HRW) assignment of a namespace to a small replica set of metadata
 * endpoints (design §6.1).
 *
 * <p>{@code score = hash(namespace, membershipGeneration, endpoint)}; the replica set is the top
 * {@code replicaCount} endpoints by score and {@code preferredLeader = replicaSet[0]}. The function
 * is deterministic, so every metadata node computes the same owner for a namespace from the same
 * membership view with no consensus read. Adding an endpoint only moves the namespaces whose new
 * highest score is that endpoint (HRW minimal disruption); existing assignments are otherwise stable.
 */
public final class NamespaceAssignmentPolicy {
    private NamespaceAssignmentPolicy() {}

    /** A namespace's ordered replica set (highest score first) for one membership generation. */
    public record Assignment(StrataNamespace namespace, int generation, List<String> replicaSet) {
        public Assignment {
            replicaSet = List.copyOf(replicaSet);
        }

        /** The active controller leader for the namespace = the highest-scoring endpoint. */
        public String preferredLeader() {
            if (replicaSet.isEmpty()) {
                throw new IllegalStateException("no eligible controller endpoints for namespace " + namespace);
            }
            return replicaSet.get(0);
        }
    }

    /**
     * Assigns {@code namespace} to the top-{@code replicaCount} of {@code eligibleEndpoints} by
     * rendezvous score. {@code replicaCount} is clamped to {@code [1, eligibleEndpoints.size()]};
     * an empty membership yields an empty replica set.
     */
    public static Assignment assign(StrataNamespace namespace, int generation,
                                    List<String> eligibleEndpoints, int replicaCount) {
        List<String> distinct = eligibleEndpoints.stream().distinct().toList();
        if (distinct.isEmpty()) {
            return new Assignment(namespace, generation, List.of());
        }
        int count = Math.min(Math.max(replicaCount, 1), distinct.size());
        List<Scored> scored = new ArrayList<>(distinct.size());
        for (String endpoint : distinct) {
            scored.add(new Scored(endpoint, score(namespace, generation, endpoint)));
        }
        // Highest score wins; tie-break on the endpoint string so the order is total and stable.
        scored.sort(Comparator.<Scored>comparingLong(s -> s.score).reversed()
                .thenComparing(s -> s.endpoint));
        List<String> replicaSet = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            replicaSet.add(scored.get(i).endpoint);
        }
        return new Assignment(namespace, generation, replicaSet);
    }

    /** The rendezvous score for one {@code (namespace, generation, endpoint)} tuple. */
    static long score(StrataNamespace namespace, int generation, String endpoint) {
        MessageDigest sha = sha256();
        sha.update(namespace.toString().getBytes(StandardCharsets.UTF_8));
        sha.update((byte) 0);
        sha.update((byte) (generation >>> 24));
        sha.update((byte) (generation >>> 16));
        sha.update((byte) (generation >>> 8));
        sha.update((byte) generation);
        sha.update((byte) 0);
        sha.update(endpoint.getBytes(StandardCharsets.UTF_8));
        byte[] digest = sha.digest();
        long score = 0L;
        for (int i = 0; i < 8; i++) {
            score = (score << 8) | (digest[i] & 0xFFL);
        }
        return score;
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private record Scored(String endpoint, long score) {}
}
