package io.strata.proto;

import io.strata.common.ErrorCode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;

/**
 * Process-isolated SCP compatibility peer. Tests run this class with either current classes or a
 * released artifact classpath so same-named protocol classes never mix in one JVM.
 */
public final class ScpCompatPeerMain {
    private ScpCompatPeerMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("usage: ScpCompatPeerMain client <host> <port> | server <readyFile>");
        }
        switch (args[0]) {
            case "client" -> runClient(args);
            case "server" -> runServer(args);
            default -> throw new IllegalArgumentException("unknown mode: " + args[0]);
        }
    }

    private static void runClient(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("usage: ScpCompatPeerMain client <host> <port>");
        }
        try (ScpClient client = new ScpClient(args[1], Integer.parseInt(args[2]),
                ScpClient.KIND_BROKER, "scp-compat-client")) {
            for (ScpCompatCorpus.Exchange exchange : ScpCompatCorpus.exchanges()) {
                Frame frame = client.callFrame(Opcode.valueOf(exchange.opcodeName()),
                        exchange.requestHeader(),
                        ScpCompatCorpus.payloadBuffer(exchange.requestPayload()),
                        5_000);
                check(exchange.responseHeaderHex().equals(ScpCompatCorpus.hex(frame.headerSlice())),
                        exchange.name() + " response header mismatch");
                check(exchange.responsePayloadHex().equals(ScpCompatCorpus.hex(frame.payloadSlice())),
                        exchange.name() + " response payload mismatch");
            }
        }
    }

    private static void runServer(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: ScpCompatPeerMain server <readyFile>");
        }
        Path readyFile = Path.of(args[1]);
        ArrayDeque<ScpCompatCorpus.Exchange> expected = new ArrayDeque<>(ScpCompatCorpus.exchanges());
        try (ScpServer server = new ScpServer(0, 42, 0x1111222233334444L, 0x5555666677778888L, req -> {
            try {
                ScpCompatCorpus.Exchange exchange = expected.removeFirst();
                check(Opcode.valueOf(exchange.opcodeName()).code == req.opcode(),
                        exchange.name() + " opcode mismatch");
                check(exchange.requestHeaderHex().equals(ScpCompatCorpus.hex(req.headerSlice())),
                        exchange.name() + " request header mismatch");
                check(exchange.requestPayloadHex().equals(ScpCompatCorpus.hex(req.payloadSlice())),
                        exchange.name() + " request payload mismatch");
                return ScpServer.ok(req, exchange.responseHeader(),
                        ScpCompatCorpus.payloadBuffer(exchange.responsePayload()));
            } catch (Throwable t) {
                return Frame.response(req, Resp.error(ErrorCode.INTERNAL, t.toString(), 0), null);
            }
        })) {
            Files.createDirectories(readyFile.getParent());
            Files.writeString(readyFile, Integer.toString(server.port()) + System.lineSeparator());
            long deadline = System.currentTimeMillis() + 30_000;
            while (!expected.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            check(expected.isEmpty(), "server did not receive all expected exchanges: " + expected.size());
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
