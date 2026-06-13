package io.strata.it;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CorrectnessArtifactTest {
    @TempDir
    Path artifactRoot;

    @Test
    void artifactsIncludeRuntimeContextAtCreation() throws Exception {
        String id = UUID.randomUUID().toString();

        CorrectnessArtifact artifact = CorrectnessArtifact.create(artifactRoot, "artifact-test", id);

        String text = Files.readString(artifact.file());
        assertTrue(text.contains("artifact.scenario=artifact-test"));
        assertTrue(text.contains("artifact.id=" + id));
        assertTrue(text.contains("artifact.path=" + artifact.file()));
        assertTrue(text.matches("(?s).*artifact.createdAtEpochMs=\\d+.*"));
        assertTrue(text.contains("runtime.javaVersion=" + System.getProperty("java.version")));
        assertTrue(text.contains("runtime.osName=" + System.getProperty("os.name")));
        assertTrue(text.matches("(?s).*runtime.availableProcessors=\\d+.*"));
    }

    @Test
    void failureArtifactsIncludeStackTraceLines() throws Exception {
        CorrectnessArtifact artifact = CorrectnessArtifact.create(artifactRoot, "artifact-test",
                UUID.randomUUID().toString());

        artifact.addFailure(new IllegalStateException("synthetic failure"));

        String text = Files.readString(artifact.file());
        assertTrue(text.contains("status=failed"));
        assertTrue(text.contains("failureType=java.lang.IllegalStateException"));
        assertTrue(text.contains("failureMessage=synthetic failure"));
        assertTrue(text.contains("failureStackTrace.lines="));
        assertTrue(text.contains("failureStackTrace.0=java.lang.IllegalStateException: synthetic failure"));
        assertTrue(text.contains("CorrectnessArtifactTest.failureArtifactsIncludeStackTraceLines"));
    }

    @Test
    void failureArtifactsKeepFirstFailureOnly() throws Exception {
        CorrectnessArtifact artifact = CorrectnessArtifact.create(artifactRoot, "artifact-test",
                UUID.randomUUID().toString());

        artifact.addFailure(new IllegalStateException("first failure"));
        artifact.addFailure(new IllegalArgumentException("second failure"));

        String text = Files.readString(artifact.file());
        assertTrue(text.contains("failureType=java.lang.IllegalStateException"));
        assertTrue(text.contains("failureMessage=first failure"));
        assertFalse(text.contains("failureType=java.lang.IllegalArgumentException"));
        assertFalse(text.contains("failureMessage=second failure"));
    }

    @Test
    void replayableSuccessRequiresStatusAndRequiredEvidence() throws Exception {
        CorrectnessArtifact artifact = CorrectnessArtifact.create(artifactRoot, "artifact-test",
                UUID.randomUUID().toString());
        artifact.add("scenario=artifact-test",
                "replayCommand=./scripts/verify.sh --skip-default --fault",
                "seed=abc123",
                "finalAckedBytes=10");

        AssertionError missingStatus = assertThrows(AssertionError.class,
                () -> artifact.requireReplayableSuccess("seed", "finalAckedBytes"));
        assertTrue(missingStatus.getMessage().contains("status=passed"));

        artifact.markPassed();
        artifact.requireReplayableSuccess("seed", "finalAckedBytes");
        artifact.requireAnyLineStartingWith("replayCommand=");
        artifact.requireAnyLineContaining("--fault");
    }

    @Test
    void replayableSuccessRejectsMissingRequiredKeys() throws Exception {
        CorrectnessArtifact artifact = CorrectnessArtifact.create(artifactRoot, "artifact-test",
                UUID.randomUUID().toString());
        artifact.add("scenario=artifact-test",
                "replayCommand=./scripts/verify.sh --skip-default --fault");
        artifact.markPassed();

        AssertionError error = assertThrows(AssertionError.class,
                () -> artifact.requireReplayableSuccess("seed", "finalAckedBytes"));

        assertTrue(error.getMessage().contains("seed"));
        assertTrue(error.getMessage().contains("finalAckedBytes"));
    }

    @Test
    void replayableSuccessRejectsBlankRequiredValues() throws Exception {
        CorrectnessArtifact artifact = CorrectnessArtifact.create(artifactRoot, "artifact-test",
                UUID.randomUUID().toString());
        artifact.add("scenario=artifact-test",
                "replayCommand=./scripts/verify.sh --skip-default --fault",
                "seed=",
                "finalAckedBytes=   ");
        artifact.markPassed();

        AssertionError error = assertThrows(AssertionError.class,
                () -> artifact.requireReplayableSuccess("seed", "finalAckedBytes"));

        assertTrue(error.getMessage().contains("seed"));
        assertTrue(error.getMessage().contains("finalAckedBytes"));
        assertTrue(error.getMessage().contains("non-empty"));
    }

    @Test
    void failedArtifactCannotBeMarkedPassed() throws Exception {
        CorrectnessArtifact artifact = CorrectnessArtifact.create(artifactRoot, "artifact-test",
                UUID.randomUUID().toString());

        artifact.addFailure(new IllegalStateException("failed before success"));

        AssertionError error = assertThrows(AssertionError.class, artifact::markPassed);
        assertTrue(error.getMessage().contains("cannot mark failed correctness artifact as passed"));
    }
}
