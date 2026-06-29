package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.common.FileId;
import io.strata.common.StrataNamespace;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class DeleteApiTest {

    @Test
    void deleteOfMissingFileIsIdempotent() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3);
             StrataClient client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()))) {
            // deleting a never-created file is idempotent success, not FILE_NOT_FOUND: a delete
            // retry whose first ack was lost — or that lands after the tombstone is reaped — must
            // observe a single logical deletion rather than surface a spurious error. Repeating it
            // documents that the contract holds across retries.
            assertDoesNotThrow(() -> client.deleteById(StrataNamespace.of("test"), FileId.of(1)));
            assertDoesNotThrow(() -> client.deleteById(StrataNamespace.of("test"), FileId.of(1)));
        }
    }
}
