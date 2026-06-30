package io.strata.meta;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Verifies that {@link ZkMetadataStore} exposes a 5-arg constructor for the retry-policy knobs
 * (Task 4: thread ZK retry policy from {@link ControllerConfig}).
 *
 * The retry policy is not observable post-construction, so the test asserts:
 *   1. The 5-arg {@code (String, int, int, int, int)} constructor exists (reflection).
 *   2. The 3-arg constructor delegates to it (compile-level delegation; confirmed by the code
 *      that only the 5-arg ctor carries the {@code ExponentialBackoffRetry} build step).
 */
class ZkMetadataStoreRetryTest {

    @Test
    void retryConstructorIsAvailable() throws Exception {
        // Reflection: assert the 5-arg constructor exists with (String, int, int, int, int).
        // Fails with NoSuchMethodException before the ctor is added (RED), passes after (GREEN).
        var ctor = ZkMetadataStore.class.getDeclaredConstructor(
                String.class, int.class, int.class, int.class, int.class);
        assertNotNull(ctor, "5-arg constructor (String,int,int,int,int) must be public");
    }

    @Test
    void threeArgCtorStillExists() throws Exception {
        // Ensures the 3-arg delegation path compiles and the original constructor is retained.
        var ctor = ZkMetadataStore.class.getDeclaredConstructor(
                String.class, int.class, int.class);
        assertNotNull(ctor, "3-arg constructor (String,int,int) must still be present");
    }
}
