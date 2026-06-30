package io.strata.common;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class EnvConfigTest {
    @Test
    void unsetReturnsDefault() {
        assertEquals(7, EnvConfig.intEnv("STRATA_DEFINITELY_UNSET_XYZ_INT", 7));
        assertEquals(9L, EnvConfig.longEnv("STRATA_DEFINITELY_UNSET_XYZ_LONG", 9L));
    }
}
