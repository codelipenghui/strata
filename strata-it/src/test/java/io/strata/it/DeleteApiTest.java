package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.StrataClient;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.common.StrataNamespace;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DeleteApiTest {

    @Test
    void deleteSurfacesPerFileFailures() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3);
             StrataClient client = StrataClient.connect(ClientConfig.of(cluster.metaEndpoint()))) {
            ScpException e = assertThrows(ScpException.class,
                    () -> client.deleteById(StrataNamespace.of("test"), FileId.of(1)));
            assertEquals(ErrorCode.FILE_NOT_FOUND, e.code());
        }
    }
}
