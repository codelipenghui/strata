package io.strata.meta;

import io.strata.common.StrataNamespace;

import java.util.List;
import java.util.Objects;

/**
 * Resolves the controller owner of a namespace from a static membership of controller endpoints using
 * {@link NamespaceAssignmentPolicy} (design §6, §6.1). A single-endpoint (or empty) membership means
 * this node owns every namespace, preserving single-leader behavior for non-sharded deployments.
 *
 * <p>Ownership is computed, not read from consensus: every node derives the same owner from the same
 * membership, and the lazily-persisted assignment record (see {@code MetadataStore.putNamespaceAssignment})
 * agrees. A request that lands on a non-owner is redirected with NOT_LEADER + the owner endpoint hint.
 */
public final class NamespaceOwnership {
    private final String localEndpoint;
    private final List<String> eligibleEndpoints;
    private final int generation;
    private final int replicaCount;

    public NamespaceOwnership(String localEndpoint, List<String> eligibleEndpoints,
                             int generation, int replicaCount) {
        this.localEndpoint = Objects.requireNonNull(localEndpoint, "localEndpoint");
        this.eligibleEndpoints = List.copyOf(eligibleEndpoints);
        this.generation = generation;
        this.replicaCount = replicaCount;
    }

    /** Single-endpoint / empty membership: this node owns every namespace (no sharding). */
    public boolean ownsAll() {
        return eligibleEndpoints.size() <= 1;
    }

    /** The endpoint that leads {@code namespace}. */
    public String ownerOf(StrataNamespace namespace) {
        if (ownsAll()) {
            return localEndpoint;
        }
        return NamespaceAssignmentPolicy.assign(namespace, generation, eligibleEndpoints, replicaCount)
                .preferredLeader();
    }

    /** Whether this node is the controller owner of {@code namespace}. */
    public boolean isOwner(StrataNamespace namespace) {
        return ownsAll() || ownerOf(namespace).equals(localEndpoint);
    }

    /** The full ordered replica set for {@code namespace} (preferred leader first). */
    public NamespaceAssignmentPolicy.Assignment assignmentOf(StrataNamespace namespace) {
        if (ownsAll()) {
            return new NamespaceAssignmentPolicy.Assignment(namespace, generation, List.of(localEndpoint));
        }
        return NamespaceAssignmentPolicy.assign(namespace, generation, eligibleEndpoints, replicaCount);
    }

    public String localEndpoint() {
        return localEndpoint;
    }

    public List<String> eligibleEndpoints() {
        return eligibleEndpoints;
    }
}
