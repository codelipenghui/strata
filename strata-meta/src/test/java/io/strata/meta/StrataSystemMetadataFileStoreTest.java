package io.strata.meta;

import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertThrows;

class StrataSystemMetadataFileStoreTest {

    @Test
    void readChunkConstructorIsAvailable() throws Exception {
        StrataSystemMetadataFileStore.class.getDeclaredConstructor(
                Supplier.class, int.class, int.class, boolean.class, int.class);
    }

    @Test
    void readChunkBytesValidationRejectsZero() {
        assertThrows(IllegalArgumentException.class,
                () -> new StrataSystemMetadataFileStore(() -> "127.0.0.1:0", 3, 2, false, 0),
                "readChunkBytes must be positive: 0");
    }

    @Test
    void readChunkBytesValidationRejectsNegative() {
        assertThrows(IllegalArgumentException.class,
                () -> new StrataSystemMetadataFileStore(() -> "127.0.0.1:0", 3, 2, false, -1),
                "readChunkBytes must be positive: -1");
    }
}
