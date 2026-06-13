package io.strata.it;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Multi-seed stress/fault soak. Defaults are intentionally bounded for local runs; increase with:
 * mvn -Psoak -pl strata-it test -Dstrata.soak.iterations=20 -Dstrata.soak.batches=100
 */
@Tag("soak")
class StressFaultSoakTest {

    private static final long DEFAULT_BASE_SEED = 0x50a6_2026_0612L;
    private static final long SEED_STEP = 0x9e37_79b9_7f4a_7c15L;

    @Test
    void multiSeedMixedReplicaAndMetadataFaultsPreserveAckedPrefix() throws Exception {
        int iterations = Integer.getInteger("strata.soak.iterations", 5);
        int batches = Integer.getInteger("strata.soak.batches", 45);
        long baseSeed = configuredBaseSeed();
        if (iterations <= 0) {
            throw new IllegalArgumentException("strata.soak.iterations must be positive");
        }
        if (batches < 15) {
            throw new IllegalArgumentException("strata.soak.batches must be at least 15");
        }
        String baseSeedHex = Long.toUnsignedString(baseSeed, 16);
        CorrectnessArtifact artifact = CorrectnessArtifact.create("stress-fault-soak",
                "base-" + baseSeedHex + "-i" + iterations + "-b" + batches);
        artifact.add("scenario=stress-fault-soak",
                "iterations=" + iterations,
                "batches=" + batches,
                "baseSeed=" + baseSeedHex,
                "seedStep=" + Long.toUnsignedString(SEED_STEP, 16),
                "replayCommand=./scripts/verify.sh --skip-default --soak"
                        + " -Dstrata.soak.seed=" + baseSeedHex
                        + " -Dstrata.soak.iterations=" + iterations
                        + " -Dstrata.soak.batches=" + batches);
        System.out.printf("stress/fault soak: iterations=%d batches=%d baseSeed=%s seedStep=%s%n",
                iterations, batches, baseSeedHex, Long.toUnsignedString(SEED_STEP, 16));
        for (int i = 0; i < iterations; i++) {
            long seed = baseSeed + i * SEED_STEP;
            String seedHex = Long.toUnsignedString(seed, 16);
            System.out.printf("stress/fault soak iteration %d/%d seed=%s%n",
                    i + 1, iterations, seedHex);
            artifact.add("iteration=" + (i + 1) + "/" + iterations + " seed=" + seedHex,
                    "iterationReplayCommand=" + (i + 1)
                            + " ./scripts/verify.sh --skip-default --fault"
                            + " --stress-only"
                            + " -Dstrata.stress.seed=" + seedHex
                            + " -Dstrata.stress.batches=" + batches);
            try {
                StressFaultTest.runPolicyMatrix(seed, batches);
                artifact.add("iterationPassed=" + (i + 1) + " seed=" + seedHex);
            } catch (Throwable t) {
                artifact.addFailure(t);
                throw new AssertionError("stress/fault soak iteration failed: iteration="
                        + (i + 1) + "/" + iterations
                        + " seed=" + seedHex
                        + " batches=" + batches, t);
            }
        }
        artifact.markPassed();
        artifact.requireReplayableSuccess("iterations", "batches", "baseSeed", "seedStep");
        artifact.requireAnyLineStartingWith("iteration=", "iterationReplayCommand=",
                "iterationPassed=");
    }

    private static long configuredBaseSeed() {
        String value = System.getProperty("strata.soak.seed");
        if (value == null || value.isBlank()) {
            return DEFAULT_BASE_SEED;
        }
        return StressFaultTest.parseSeed(value);
    }
}
