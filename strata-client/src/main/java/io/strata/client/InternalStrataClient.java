package io.strata.client;

import io.strata.proto.ScpClient;

import java.util.Objects;

/**
 * Narrow factory for Strata's own control-plane file clients.
 *
 * <p>This class is public only because {@code strata-meta} is a separate Maven module. Application
 * clients must use {@link StrataClient#connect(ClientConfig)}; the metadata role is reserved for the
 * controller's metadata-log/snapshot store. The data-node orphan-confirm lane is its only other holder,
 * and presents it only for LOOKUP_FILE requests in the reserved {@code strata-meta} namespace; ordinary
 * namespace confirms retain the tool role.
 * The HELLO role is a typed protocol distinction, not authentication on today's unauthenticated SCP
 * transport.
 *
 * <p>The role applies only to controller connections. Data-node connections deliberately remain
 * {@link ScpClient#KIND_BROKER}: writer seal and cleanup RPCs use the broker-compatible, unstamped
 * owner-epoch path.
 */
public final class InternalStrataClient {
    private InternalStrataClient() {
    }

    /** Connects a client whose controller HELLO identifies the system metadata role. */
    public static StrataClient connectMetadata(ClientConfig config) {
        return new StrataClientImpl(Objects.requireNonNull(config, "config"),
                ScpClient.KIND_METADATA, "strata-system-metadata");
    }
}
