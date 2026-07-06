package io.strata.proto;

/**
 * Per-request namespace carrier from the SCP handler to {@link ScpServer}'s request observer, without a
 * metrics dependency in the transport. A handler sets the namespace synchronously while decoding the
 * request (the synchronous portion of {@code handleAsync} runs in-order on the single-threaded
 * per-connection executor); {@code ScpServer} reads it with {@link #takeNamespace()} right after dispatch
 * — valid for both the sync and the async (group-committed APPEND) paths, since the namespace is known at
 * decode time, not at completion time. {@code take} clears the slot so a value never leaks to the next
 * request served on the same thread.
 */
public final class RequestContext {
    private static final ThreadLocal<String> NAMESPACE = new ThreadLocal<>();
    private static final ThreadLocal<Client> CLIENT = new ThreadLocal<>();

    private record Client(byte kind, String id) {}

    private RequestContext() {
    }

    /** Records the namespace of the request currently being decoded on this thread. */
    public static void setNamespace(String namespace) {
        NAMESPACE.set(namespace);
    }

    /** Records the declared SCP HELLO identity for the request currently being decoded. */
    static void setClient(byte kind, String id) {
        CLIENT.set(new Client(kind, id));
    }

    /** Client kind from the SCP HELLO frame, or {@code 0} before HELLO / outside a request. */
    public static byte clientKind() {
        Client client = CLIENT.get();
        return client == null ? 0 : client.kind();
    }

    /** Client id from the SCP HELLO frame, or {@code "-"} before HELLO / outside a request. */
    public static String clientId() {
        Client client = CLIENT.get();
        return client == null ? "-" : client.id();
    }

    /** Returns the namespace set for the current request and clears it; {@code "-"} when unset. */
    public static String takeNamespace() {
        String ns = NAMESPACE.get();
        NAMESPACE.set(null);
        return ns == null ? "-" : ns;
    }

    static void clearClient() {
        CLIENT.set(null);
    }
}
