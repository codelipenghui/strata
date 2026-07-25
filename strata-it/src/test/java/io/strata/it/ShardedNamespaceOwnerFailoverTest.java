package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.client.StrataFile;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import io.strata.common.StrataPath;
import io.strata.meta.NamespaceAssignmentPolicy;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Real namespace-log failover coverage for namespace-sharded controllers.
 *
 * <p>Unlike the global-controller failover tests, these scenarios choose a namespace from the rendezvous
 * replica order, kill that namespace's owner, and require the next live replica to recover the namespace log.
 * The same long-lived {@link StrataClient} is retained across the fault so its cached owner is stale.
 */
class ShardedNamespaceOwnerFailoverTest {

    private static final long OWNER_DEADLINE_MS = 30_000;

    @Test
    void cachedClientRollsAcrossOwnerKillAndOldOwnerRestartDoesNotFailBack() throws Exception {
        try (MiniCluster cluster = MiniCluster.sharded(3, 2)) {
            List<String> endpoints = cluster.configuredMetaEndpoints();
            String oldOwner = endpoints.get(0);
            String successor = endpoints.get(1);
            StrataNamespace primary =
                    namespaceWithReplicaOrder(endpoints, List.of(oldOwner, successor), "failover");
            StrataNamespace unaffected =
                    namespaceWithReplicaOrder(endpoints, List.of(successor, oldOwner), "unaffected");
            int oldOwnerSlot = endpoints.indexOf(oldOwner);

            ClientConfig config = new ClientConfig(endpoints, 512, 3_000)
                    .withControllerRetryDeadlineMs(30_000)
                    .withControllerRetryBackoffMs(50)
                    .withDataNodeConnectionsPerEndpoint(2);
            try (StrataClient client = StrataClient.connect(config)) {
                String primaryPath = "/cached-owner";
                String unaffectedPath = "/unaffected";
                FileId primaryId = client.create(
                        StrataClient.FileSpec.log(primary.value(), primaryPath)).id();
                FileId unaffectedId = client.create(
                        StrataClient.FileSpec.log(unaffected.value(), unaffectedPath)).id();
                awaitOwner(cluster, oldOwner, primary, primaryPath, primaryId);
                awaitOwner(cluster, successor, unaffected, unaffectedPath, unaffectedId);

                Workload primaryWorkload = new Workload();
                Workload unaffectedWorkload = new Workload();
                try (StrataFile.Appender primaryAppender =
                             client.openById(primary, primaryId).openForAppend()) {
                    // Keep the first chunk open while warming both the appender and ControllerClient's
                    // namespace->owner cache.
                    primaryWorkload.appendAcked(primaryAppender, 0, 20);

                    cluster.stopMeta(oldOwnerSlot);
                    assertNull(cluster.metaAtSlot(oldOwnerSlot));
                    assertFalse(cluster.metaEndpoints().contains(oldOwner),
                            "a stopped controller must disappear from the active endpoint view");

                    // A namespace already owned by the surviving controller remains available while the
                    // failed namespace is waiting for reassignment/recovery.
                    try (StrataFile.Appender unaffectedAppender =
                                 client.openById(unaffected, unaffectedId).openForAppend()) {
                        unaffectedWorkload.appendAcked(unaffectedAppender, 10_000, 80);
                        unaffectedAppender.seal();
                    }

                    // Enough data for several chunk rolls. The first boundary observes the cached dead owner;
                    // retries must discover the successor and complete through its recovered metadata repo.
                    primaryWorkload.appendAcked(primaryAppender, 20, 300);
                    StrataFile.SealInfo sealed = primaryAppender.seal();
                    assertEquals(primaryWorkload.ackedBytes(), sealed.sealedLength());
                }

                awaitOwner(cluster, successor, primary, primaryPath, primaryId);
                primaryWorkload.verifyAckedPrefix(client, primary, primaryId);
                unaffectedWorkload.verifyAckedPrefix(client, unaffected, unaffectedId);

                // Rejoin at the exact same endpoint/session identity. Persisted ownership must stay on the
                // successor; a returning preferred replica does not implicitly fail back.
                cluster.startMeta(oldOwnerSlot);
                assertEquals(oldOwner, cluster.metaEndpoint(oldOwnerSlot));
                assertNotNull(cluster.metaAtSlot(oldOwnerSlot));
                awaitOwner(cluster, successor, primary, primaryPath, primaryId);
                awaitRedirect(oldOwner, primary, primaryPath, successor);
                assertEquals(primaryId, client.open(primary.value(), primaryPath).id());
            }
        }
    }

    @Test
    void threeControllersFailOverInReplicaOrder() throws Exception {
        try (MiniCluster cluster = MiniCluster.sharded(3, 3)) {
            List<String> endpoints = cluster.configuredMetaEndpoints();
            StrataNamespace namespace = StrataNamespace.of("three-owner-chain");
            List<String> replicas = NamespaceAssignmentPolicy.assign(
                    namespace, 0, endpoints, endpoints.size()).replicaSet();
            String path = "/chain";

            ClientConfig config = new ClientConfig(endpoints, 512, 3_000)
                    .withControllerRetryDeadlineMs(30_000)
                    .withControllerRetryBackoffMs(50);
            try (StrataClient client = StrataClient.connect(config)) {
                FileId fileId = client.create(StrataClient.FileSpec.log(namespace.value(), path)).id();
                awaitOwner(cluster, replicas.get(0), namespace, path, fileId);

                cluster.stopMeta(endpoints.indexOf(replicas.get(0)));
                awaitOwner(cluster, replicas.get(1), namespace, path, fileId);
                assertEquals(2, cluster.metas.size());
                // Route the long-lived client through B so the second failure starts with a genuinely
                // stale B owner cache, rather than merely retaining the already-dead A hint.
                assertEquals(fileId, client.open(namespace.value(), path).id());

                cluster.stopMeta(endpoints.indexOf(replicas.get(1)));
                awaitOwner(cluster, replicas.get(2), namespace, path, fileId);
                assertEquals(1, cluster.metas.size());

                Workload workload = new Workload();
                try (StrataFile.Appender appender = client.openById(namespace, fileId).openForAppend()) {
                    workload.appendAcked(appender, 0, 160);
                    StrataFile.SealInfo sealed = appender.seal();
                    assertEquals(workload.ackedBytes(), sealed.sealedLength());
                }
                workload.verifyAckedPrefix(client, namespace, fileId);
            }
        }
    }

    private static StrataNamespace namespaceWithReplicaOrder(
            List<String> endpoints, List<String> expectedOrder, String prefix) {
        for (int i = 0; i < 10_000; i++) {
            StrataNamespace namespace = StrataNamespace.of(prefix + "-" + i);
            List<String> replicas = NamespaceAssignmentPolicy.assign(
                    namespace, 0, endpoints, endpoints.size()).replicaSet();
            if (replicas.equals(expectedOrder)) {
                return namespace;
            }
        }
        throw new AssertionError("no namespace with replica order " + expectedOrder);
    }

    private static void awaitOwner(MiniCluster cluster, String expectedOwner,
                                   StrataNamespace namespace, String path, FileId expectedFileId)
            throws Exception {
        long deadline = System.currentTimeMillis() + OWNER_DEADLINE_MS;
        Throwable last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                assertEquals(expectedFileId, lookupPath(expectedOwner, namespace, path));
                for (String endpoint : cluster.metaEndpoints()) {
                    if (!endpoint.equals(expectedOwner)) {
                        assertRedirect(endpoint, namespace, path, expectedOwner);
                    }
                }
                return;
            } catch (Exception | AssertionError e) {
                last = e;
                Thread.sleep(50);
            }
        }
        AssertionError timeout = new AssertionError(
                "namespace " + namespace + " did not converge to owner " + expectedOwner);
        if (last != null) {
            timeout.addSuppressed(last);
        }
        throw timeout;
    }

    private static void awaitRedirect(String endpoint, StrataNamespace namespace,
                                      String path, String expectedOwner) throws Exception {
        long deadline = System.currentTimeMillis() + OWNER_DEADLINE_MS;
        Throwable last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                assertRedirect(endpoint, namespace, path, expectedOwner);
                return;
            } catch (Exception | AssertionError e) {
                last = e;
                Thread.sleep(50);
            }
        }
        AssertionError timeout = new AssertionError(
                endpoint + " did not redirect " + namespace + " to " + expectedOwner);
        if (last != null) {
            timeout.addSuppressed(last);
        }
        throw timeout;
    }

    private static void assertRedirect(String endpoint, StrataNamespace namespace,
                                       String path, String expectedOwner) throws Exception {
        try {
            FileId unexpected = lookupPath(endpoint, namespace, path);
            throw new AssertionError(
                    endpoint + " unexpectedly served " + namespace + " as owner; file=" + unexpected);
        } catch (ScpException e) {
            assertEquals(ErrorCode.NOT_LEADER, e.code(),
                    () -> endpoint + " returned " + e.code() + " instead of redirecting " + namespace);
            assertEquals(expectedOwner, e.leaderHint(),
                    () -> endpoint + " redirected " + namespace + " to the wrong owner");
        }
    }

    private static FileId lookupPath(String endpoint, StrataNamespace namespace, String path) throws Exception {
        int colon = endpoint.lastIndexOf(':');
        String host = endpoint.substring(0, colon);
        int port = Integer.parseInt(endpoint.substring(colon + 1));
        try (ScpClient client = new ScpClient(host, port, ScpClient.KIND_TOOL, "owner-failover-probe")) {
            return Messages.LookupPathResp.decode(client.call(
                    Opcode.LOOKUP_PATH,
                    new Messages.LookupPath(namespace, StrataPath.of(path)).encode(),
                    null,
                    2_000)).fileId();
        }
    }
}
