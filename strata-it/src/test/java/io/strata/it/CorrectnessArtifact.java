package io.strata.it;

import io.strata.proto.Messages;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Incrementally flushed, replay-oriented artifacts for stress and fault tests. */
final class CorrectnessArtifact {
    private final Path file;
    private final List<String> lines = new ArrayList<>();
    private boolean failureRecorded;

    private CorrectnessArtifact(Path file) {
        this.file = file;
    }

    static CorrectnessArtifact create(String scenario, String id) throws IOException {
        return create(Path.of("target", "correctness-artifacts"), scenario, id);
    }

    static CorrectnessArtifact create(Path root, String scenario, String id) throws IOException {
        Files.createDirectories(root);
        CorrectnessArtifact artifact = new CorrectnessArtifact(
                root.resolve(sanitize(scenario + "-" + id) + ".txt"));
        artifact.add("artifact.scenario=" + scenario,
                "artifact.id=" + id,
                "artifact.path=" + artifact.file,
                "artifact.createdAtEpochMs=" + System.currentTimeMillis(),
                "runtime.javaVersion=" + System.getProperty("java.version"),
                "runtime.javaVendor=" + System.getProperty("java.vendor"),
                "runtime.javaVmName=" + System.getProperty("java.vm.name"),
                "runtime.osName=" + System.getProperty("os.name"),
                "runtime.osVersion=" + System.getProperty("os.version"),
                "runtime.osArch=" + System.getProperty("os.arch"),
                "runtime.availableProcessors=" + Runtime.getRuntime().availableProcessors());
        return artifact;
    }

    Path file() {
        return file;
    }

    void add(String... entries) throws IOException {
        for (String entry : entries) {
            lines.add(entry);
        }
        flush();
    }

    void markPassed() throws IOException {
        if (failureRecorded) {
            throw new AssertionError("cannot mark failed correctness artifact as passed: " + file);
        }
        add("status=passed");
    }

    void addFailure(Throwable failure) throws IOException {
        if (failureRecorded) {
            return;
        }
        failureRecorded = true;
        List<String> entries = new ArrayList<>();
        entries.add("status=failed");
        entries.add("failureType=" + failure.getClass().getName());
        entries.add("failureMessage=" + String.valueOf(failure.getMessage()));
        StringWriter stack = new StringWriter();
        failure.printStackTrace(new PrintWriter(stack));
        String[] stackLines = stack.toString().split("\\R", -1);
        entries.add("failureStackTrace.lines=" + stackLines.length);
        for (int i = 0; i < stackLines.length; i++) {
            entries.add("failureStackTrace." + i + "=" + stackLines[i]);
        }
        add(entries.toArray(String[]::new));
    }

    void addDescriptor(String label, Messages.LookupFileResp file) throws IOException {
        add(label + ".fileState=" + file.fileState(),
                label + ".policy=rf" + file.writePolicy().replicationFactor()
                        + "-aq" + file.writePolicy().ackQuorum()
                        + "-fsync" + file.writePolicy().fsyncOnAck(),
                label + ".chunkCount=" + file.chunks().size());
        for (Messages.ChunkInfo chunk : file.chunks()) {
            add(label + ".chunk=" + chunk.chunkId()
                    + " state=" + chunk.state()
                    + " length=" + chunk.length()
                    + " crc=" + chunk.crc()
                    + " writeEpoch=" + chunk.writeEpoch()
                    + " replicas=" + replicas(chunk.replicas()));
        }
    }

    void requireReplayableSuccess(String... requiredKeys) {
        List<String> allKeys = new ArrayList<>(List.of(
                "artifact.scenario",
                "artifact.id",
                "artifact.path",
                "artifact.createdAtEpochMs",
                "runtime.javaVersion",
                "runtime.osName",
                "runtime.availableProcessors",
                "scenario",
                "replayCommand"));
        for (String key : requiredKeys) {
            allKeys.add(key);
        }
        requireKeys(allKeys.toArray(String[]::new));
        requireStatusPassed();
    }

    void requireKeys(String... keys) {
        List<String> missing = new ArrayList<>();
        for (String key : keys) {
            if (!containsNonBlankValue(key)) {
                missing.add(key);
            }
        }
        if (!missing.isEmpty()) {
            throw new AssertionError("correctness artifact " + file
                    + " is missing required non-empty keys " + missing);
        }
    }

    void requireAnyLineStartingWith(String... prefixes) {
        List<String> missing = new ArrayList<>();
        for (String prefix : prefixes) {
            boolean found = false;
            for (String line : lines) {
                if (line.startsWith(prefix)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                missing.add(prefix);
            }
        }
        if (!missing.isEmpty()) {
            throw new AssertionError("correctness artifact " + file
                    + " is missing lines starting with " + missing);
        }
    }

    void requireAnyLineContaining(String... fragments) {
        List<String> missing = new ArrayList<>();
        for (String fragment : fragments) {
            boolean found = false;
            for (String line : lines) {
                if (line.contains(fragment)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                missing.add(fragment);
            }
        }
        if (!missing.isEmpty()) {
            throw new AssertionError("correctness artifact " + file
                    + " is missing lines containing " + missing);
        }
    }

    private void requireStatusPassed() {
        List<String> statusLines = lines.stream()
                .filter(line -> line.startsWith("status="))
                .toList();
        if (!statusLines.equals(List.of("status=passed")) || failureRecorded
                || containsKey("failureType") || containsKey("failureStackTrace.lines")) {
            throw new AssertionError("correctness artifact " + file
                    + " must contain exactly status=passed and no recorded failure, found "
                    + statusLines);
        }
    }

    private boolean containsKey(String key) {
        String prefix = key + "=";
        for (String line : lines) {
            if (line.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private boolean containsNonBlankValue(String key) {
        String prefix = key + "=";
        for (String line : lines) {
            if (line.startsWith(prefix)
                    && !line.substring(prefix.length()).isBlank()) {
                return true;
            }
        }
        return false;
    }

    private void flush() throws IOException {
        Files.writeString(file, String.join(System.lineSeparator(), lines)
                + System.lineSeparator());
    }

    private static String replicas(List<Messages.Replica> replicas) {
        List<String> parts = new ArrayList<>(replicas.size());
        for (Messages.Replica replica : replicas) {
            parts.add(replica.nodeId() + "@" + replica.endpoint());
        }
        return parts.toString();
    }

    private static String sanitize(String value) {
        StringBuilder out = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.') {
                out.append(c);
            } else {
                out.append('-');
            }
        }
        return out.toString();
    }
}
