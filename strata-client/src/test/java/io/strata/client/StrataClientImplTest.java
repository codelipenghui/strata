package io.strata.client;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class StrataClientImplTest {

    @Test
    void usesSeparateStoragePoolsForAppendAndReadTraffic() throws Exception {
        ClientConfig config = new ClientConfig(List.of("127.0.0.1:1"), 1024, 500);

        try (StrataClientImpl client = new StrataClientImpl(config)) {
            NodePool appendPool = nodePool(client, "appendPool");
            NodePool readPool = nodePool(client, "readPool");

            assertNotSame(appendPool, readPool);
            assertEquals("strata-client-append", clientIdPrefix(appendPool));
            assertEquals("strata-client-read", clientIdPrefix(readPool));
        }
    }

    private static NodePool nodePool(StrataClientImpl client, String fieldName) throws Exception {
        Field field = StrataClientImpl.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (NodePool) field.get(client);
    }

    private static String clientIdPrefix(NodePool pool) throws Exception {
        Field field = NodePool.class.getDeclaredField("clientIdPrefix");
        field.setAccessible(true);
        return (String) field.get(pool);
    }
}
