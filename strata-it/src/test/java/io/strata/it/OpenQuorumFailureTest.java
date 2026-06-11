package io.strata.it;

import io.strata.client.ClientConfig;
import io.strata.client.SegmentStore;
import io.strata.client.StrataClient;
import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OpenQuorumFailureTest {

    @Test
    void failedInitialOpenAbortsMetadataTail() throws Exception {
        try (MiniCluster cluster = new MiniCluster(3);
             StrataClient client = new StrataClient(new ClientConfig(
                     List.of(cluster.metaEndpoint()), 4096, 500))) {
            FileId fileId = client.create(SegmentStore.FileSpec.log("open-abort"));

            cluster.killNode(1);
            cluster.killNode(2);

            try (SegmentStore.Appender appender = client.openForAppend(fileId, 1)) {
                ScpException e = assertThrows(ScpException.class,
                        () -> appender.append(ByteBuffer.wrap(new byte[]{1})));
                assertEquals(ErrorCode.INTERNAL, e.code());
            }

            var lookup = lookup(cluster.metaEndpoint(), fileId);
            assertEquals(0, lookup.chunks().size(),
                    "failed OPEN_CHUNK quorum must not strand an OPEN metadata tail");
        }
    }

    private static Messages.LookupFileResp lookup(String endpoint, FileId fileId) throws Exception {
        String[] hp = endpoint.split(":");
        try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]), ScpClient.KIND_TOOL, "lookup")) {
            ByteBuffer h = direct.call(Opcode.LOOKUP_FILE, new Messages.LookupFile(fileId).encode(), null, 5000);
            return Messages.LookupFileResp.decode(h);
        }
    }
}
