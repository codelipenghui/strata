package io.strata.it;

import io.strata.common.ErrorCode;
import io.strata.common.FileId;
import io.strata.common.ScpException;
import io.strata.meta.ZkMetadataStore;
import io.strata.proto.Messages;
import io.strata.proto.Opcode;
import io.strata.proto.ScpClient;
import org.apache.curator.test.TestingServer;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test-only child-JVM cluster harness for external-style fault schedules. */
final class ExternalCluster implements AutoCloseable {
    private final TestingServer zk;
    private final Path root;
    private final String logDirectory;
    private final List<ExternalMetadata> metadata = new ArrayList<>();
    private final List<ExternalStorage> storage = new ArrayList<>();

    ExternalCluster(String name) throws Exception {
        this.zk = new TestingServer(true);
        this.root = Files.createTempDirectory("strata-" + name);
        this.logDirectory = name + "-process-logs";
    }

    String zkConnect() {
        return zk.getConnectString();
    }

    Path root() {
        return root;
    }

    List<ExternalMetadata> metadataProcesses() {
        return List.copyOf(metadata);
    }

    List<ExternalStorage> storageProcesses() {
        return List.copyOf(storage);
    }

    List<String> metadataEndpoints() {
        return metadata.stream().map(ExternalMetadata::endpoint).toList();
    }

    ExternalMetadata startMetadata(String name) throws Exception {
        int port = freePort();
        Path logRoot = processLogRoot(logDirectory);
        String processName = name + "-" + System.nanoTime();
        Path readyFile = logRoot.resolve(processName + ".ready");
        Path logFile = logRoot.resolve(processName + ".log");
        Files.createFile(logFile);

        ProcessBuilder builder = new ProcessBuilder(
                javaCommand(),
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
                "-cp", System.getProperty("java.class.path"),
                MetadataServiceProcessMain.class.getName(),
                zk.getConnectString(),
                Integer.toString(port),
                readyFile.toString());
        builder.redirectErrorStream(true);
        builder.redirectOutput(logFile.toFile());
        Process process = builder.start();
        ExternalMetadata ready;
        try {
            ready = waitMetadataReady(new ExternalMetadata(
                    "127.0.0.1:" + port, readyFile, logFile, process));
        } catch (Exception e) {
            cleanupStartedProcess(process, e);
            throw e;
        } catch (AssertionError e) {
            cleanupStartedProcess(process, e);
            throw e;
        }
        metadata.add(ready);
        return ready;
    }

    ExternalStorage startStorage(String host) throws Exception {
        return startStorage(root.resolve(host), host, 0, null);
    }

    ExternalStorage startStorage(String host, int listenPort, String advertisedEndpoint)
            throws Exception {
        return startStorage(root.resolve(host), host, listenPort, advertisedEndpoint);
    }

    ExternalStorage startStorage(Path dataDir, String host) throws Exception {
        return startStorage(dataDir, host, 0, null);
    }

    ExternalStorage startStorage(Path dataDir, String host, int listenPort,
                                 String advertisedEndpoint) throws Exception {
        return startStorage(dataDir, host, listenPort, advertisedEndpoint, metadataEndpoints());
    }

    ExternalStorage startStorage(Path dataDir, String host, int listenPort,
                                 String advertisedEndpoint, List<String> metadataEndpoints)
            throws Exception {
        String processName = host + "-" + System.nanoTime();
        Path logRoot = processLogRoot(logDirectory);
        Path readyFile = logRoot.resolve(processName + ".ready");
        Path logFile = logRoot.resolve(processName + ".log");
        Files.createFile(logFile);

        List<String> command = new ArrayList<>();
        command.add(javaCommand());
        command.add("-Dorg.slf4j.simpleLogger.defaultLogLevel=warn");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(StorageNodeProcessMain.class.getName());
        command.add(dataDir.toString());
        command.add(String.join(",", metadataEndpoints));
        command.add(host);
        command.add(readyFile.toString());
        if (advertisedEndpoint != null) {
            command.add(Integer.toString(listenPort));
            command.add(advertisedEndpoint);
        }

        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        builder.redirectOutput(logFile.toFile());
        Process process = builder.start();
        ExternalStorage ready;
        try {
            ready = waitStorageReady(new ExternalStorage(dataDir, host, readyFile,
                    logFile, process, -1, "", listenPort, advertisedEndpoint,
                    List.copyOf(metadataEndpoints)));
        } catch (Exception e) {
            cleanupStartedProcess(process, e);
            throw e;
        } catch (AssertionError e) {
            cleanupStartedProcess(process, e);
            throw e;
        }
        storage.add(ready);
        return ready;
    }

    ExternalStorage restartStorage(ExternalStorage old) throws Exception {
        kill(old);
        ExternalStorage restarted = startStorage(old.dataDir(), old.host(), old.listenPort(),
                old.advertisedEndpoint(), old.metadataEndpoints());
        storage.remove(restarted);
        replaceStorage(old, restarted);
        return restarted;
    }

    void kill(ExternalMetadata meta) throws Exception {
        if (meta == null || !meta.process().isAlive()) {
            return;
        }
        meta.process().destroyForcibly();
        if (!meta.process().waitFor(10, TimeUnit.SECONDS)) {
            throw new AssertionError("metadata process did not exit after destroyForcibly");
        }
    }

    void kill(ExternalStorage node) throws Exception {
        if (node == null || !node.process().isAlive()) {
            return;
        }
        node.process().destroyForcibly();
        if (!node.process().waitFor(10, TimeUnit.SECONDS)) {
            throw new AssertionError("storage process did not exit after destroyForcibly");
        }
    }

    void awaitRegistered(int expected) throws Exception {
        try (ZkMetadataStore store = new ZkMetadataStore(zk.getConnectString())) {
            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline) {
                if (store.listNodes().size() >= expected) {
                    return;
                }
                Thread.sleep(50);
            }
        }
        throw new AssertionError("nodes did not register in time");
    }

    String awaitLeader() throws Exception {
        return awaitLeader(metadataEndpoints());
    }

    static String awaitLeader(List<String> metadataEndpoints) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        AssertionError failure = new AssertionError("no metadata process became leader");
        while (System.currentTimeMillis() < deadline) {
            for (String endpoint : metadataEndpoints) {
                String[] hp = endpoint.split(":");
                try (ScpClient direct = new ScpClient(hp[0], Integer.parseInt(hp[1]),
                        ScpClient.KIND_TOOL, "external-cluster-leader-probe")) {
                    direct.call(Opcode.PING, Messages.okHeader(), null, 1_000);
                    return endpoint;
                } catch (ScpException e) {
                    if (e.code() != ErrorCode.NOT_LEADER) {
                        failure.addSuppressed(e);
                    }
                } catch (Exception e) {
                    failure.addSuppressed(e);
                }
            }
            Thread.sleep(50);
        }
        throw failure;
    }

    ExternalMetadata metadataByEndpoint(String endpoint) {
        return metadata.stream()
                .filter(meta -> meta.endpoint().equals(endpoint))
                .findFirst()
                .orElseThrow(() -> new AssertionError("metadata endpoint not found: " + endpoint));
    }

    ExternalStorage storageByNodeId(int nodeId) {
        return storage.stream()
                .filter(node -> node.nodeId() == nodeId)
                .findFirst()
                .orElseThrow(() -> new AssertionError("storage node not found: " + nodeId));
    }

    Map<Integer, ExternalStorage> storageMapByNodeId() {
        Map<Integer, ExternalStorage> byId = new HashMap<>();
        for (ExternalStorage node : storage) {
            byId.put(node.nodeId(), node);
        }
        return byId;
    }

    void waitForAllReplicaEndpoints(FileId fileId) throws Exception {
        waitForAllReplicaEndpoints(metadataEndpoints(), fileId, storageMapByNodeId());
    }

    static void waitForAllReplicaEndpoints(List<String> metadataEndpoints, FileId fileId,
                                           Map<Integer, ExternalStorage> expected)
            throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            Messages.LookupFileResp lookup = ConsistencyVerifier.lookupFile(metadataEndpoints, fileId);
            boolean allRefreshed = true;
            int replicasSeen = 0;
            for (Messages.ChunkInfo chunk : lookup.chunks()) {
                for (Messages.Replica replica : chunk.replicas()) {
                    ExternalStorage node = expected.get(replica.nodeId());
                    if (node == null || !node.endpoint().equals(replica.endpoint())) {
                        allRefreshed = false;
                        break;
                    }
                    replicasSeen++;
                }
                if (!allRefreshed) {
                    break;
                }
            }
            if (allRefreshed && replicasSeen > 0) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("replica endpoints did not refresh");
    }

    String storageSummary() {
        List<String> parts = new ArrayList<>(storage.size());
        for (ExternalStorage node : storage) {
            parts.add(node.host() + "#" + node.nodeId() + "@" + node.endpoint()
                    + " dir=" + node.dataDir());
        }
        return String.join(",", parts);
    }

    static Messages.ChunkInfo lastChunk(Messages.LookupFileResp file) {
        assertTrue(!file.chunks().isEmpty(), "file has no chunks");
        return file.chunks().get(file.chunks().size() - 1);
    }

    static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    static int port(String endpoint) {
        int colon = endpoint.lastIndexOf(':');
        if (colon <= 0 || colon == endpoint.length() - 1) {
            throw new IllegalArgumentException("invalid endpoint: " + endpoint);
        }
        return Integer.parseInt(endpoint.substring(colon + 1));
    }

    @Override
    public void close() throws Exception {
        AssertionError failure = null;
        for (ExternalStorage node : storage) {
            try {
                kill(node);
            } catch (Exception e) {
                failure = addSuppressed(failure, e);
            }
        }
        for (ExternalMetadata meta : metadata) {
            try {
                kill(meta);
            } catch (Exception e) {
                failure = addSuppressed(failure, e);
            }
        }
        try {
            zk.close();
        } catch (Exception e) {
            failure = addSuppressed(failure, e);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void replaceStorage(ExternalStorage old, ExternalStorage restarted) {
        for (int i = 0; i < storage.size(); i++) {
            if (storage.get(i).nodeId() == old.nodeId()) {
                storage.set(i, restarted);
                return;
            }
        }
        storage.add(restarted);
    }

    private static ExternalMetadata waitMetadataReady(ExternalMetadata meta) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(meta.readyFile())) {
                String endpoint = Files.readString(meta.readyFile()).trim();
                if (!endpoint.isBlank()) {
                    return new ExternalMetadata(endpoint, meta.readyFile(), meta.logFile(),
                            meta.process());
                }
            }
            if (!meta.process().isAlive()) {
                throw new AssertionError("metadata process exited early with code "
                        + meta.process().exitValue() + "\n" + childLog(meta));
            }
            Thread.sleep(50);
        }
        throw new AssertionError("metadata process did not become ready\n" + childLog(meta));
    }

    private static ExternalStorage waitStorageReady(ExternalStorage node) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(node.readyFile())) {
                String[] parts = Files.readString(node.readyFile()).trim().split("\\s+");
                if (parts.length == 2) {
                    return new ExternalStorage(node.dataDir(), node.host(), node.readyFile(),
                            node.logFile(), node.process(), Integer.parseInt(parts[0]), parts[1],
                            node.listenPort(), node.advertisedEndpoint(), node.metadataEndpoints());
                }
            }
            if (!node.process().isAlive()) {
                throw new AssertionError("storage process exited early with code "
                        + node.process().exitValue() + "\n" + childLog(node));
            }
            Thread.sleep(50);
        }
        throw new AssertionError("storage process did not become ready\n" + childLog(node));
    }

    private static String childLog(ExternalMetadata meta) throws Exception {
        return Files.exists(meta.logFile()) ? Files.readString(meta.logFile()) : "";
    }

    private static String childLog(ExternalStorage node) throws Exception {
        return Files.exists(node.logFile()) ? Files.readString(node.logFile()) : "";
    }

    private static Path processLogRoot(String directory) throws Exception {
        Path root = Path.of("target", directory);
        Files.createDirectories(root);
        return root;
    }

    private static String javaCommand() {
        return Path.of(System.getProperty("java.home"), "bin", "java").toString();
    }

    private static void cleanupStartedProcess(Process process, Throwable failure) {
        process.destroyForcibly();
        try {
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                failure.addSuppressed(new AssertionError(
                        "child process did not exit after failed startup cleanup"));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure.addSuppressed(e);
        }
    }

    private static AssertionError addSuppressed(AssertionError failure, Exception e) {
        AssertionError out = failure;
        if (out == null) {
            out = new AssertionError("failed to close external cluster");
        }
        out.addSuppressed(e);
        return out;
    }

    record ExternalMetadata(String endpoint, Path readyFile, Path logFile, Process process) {
    }

    record ExternalStorage(Path dataDir, String host, Path readyFile, Path logFile,
                           Process process, int nodeId, String endpoint, int listenPort,
                           String advertisedEndpoint, List<String> metadataEndpoints) {
    }
}
