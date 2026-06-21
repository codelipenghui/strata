package io.strata.proto;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;

/** Small representative SCP v0 corpus shared by in-process and released-artifact checks. */
final class ScpCompatCorpus {
    private ScpCompatCorpus() {
    }

    static List<Exchange> exchanges() {
        return List.of(
                exchange("ping", "PING", "00", "", "000000", "70696e672d7630"),
                exchange("append", "APPEND",
                        "1111111122223333444455555555555500000003000000050000000000000400"
                                + "000000000000020000",
                        "617070656e642d7630",
                        "0000000000000000080000", ""),
                exchange("read", "READ",
                        "111111112222333344445555555555550000000300000000000000630001000000",
                        "",
                        "00000000000000001000000000000000080000", "726561642d7630"),
                exchange("fence", "FENCE",
                        "11111111222233334444555555555555000000030000000600",
                        "",
                        "000000000007000000000000006400000000000000500000", ""),
                exchange("registerDataNode", "REGISTER_NODE",
                        "00000000000000010000000000000002020768313a393030300768323a39303030"
                                + "027a3102723105686f73743101000001000000000000000001000000000000000000",
                        "",
                        "00000000002a0000000000000009000003e80000271000", ""),
                exchange("nodeHeartbeat", "NODE_HEARTBEAT",
                        "0000002a000000000000000100000000000000020000000000000009010000000000"
                                + "00006400000000000003840000000301000b0100000000000000070000",
                        "",
                        "0000000000000001e2400000", ""),
                exchange("createFile", "CREATE_FILE",
                        "0474657374242f6b61666b612f746f706963412f302f3030303030303030303030"
                                + "303030303030303000000003000000020011111111222233334444555555555555"
                                + "0123456789abcdeffedcba987654321000",
                        "",
                        "00001111111122223333444455555555555500", ""),
                exchange("lookupFile", "LOOKUP_FILE",
                        "1111111122223333444455555555555500",
                        "",
                        "00000474657374242f6b61666b612f746f706963412f302f303030303030303030"
                                + "303030303030303000000003000000020000011111111122223333444455555555"
                                + "5555000000030000000000000000000000000000000005010000000103613a3100",
                        ""));
    }

    static Exchange exchange(String name, String opcodeName, String requestHeaderHex,
                             String requestPayloadHex, String responseHeaderHex,
                             String responsePayloadHex) {
        return new Exchange(name, opcodeName, requestHeaderHex, requestPayloadHex,
                responseHeaderHex, responsePayloadHex);
    }

    static ByteBuffer payloadBuffer(byte[] payload) {
        return payload.length == 0 ? null : ByteBuffer.wrap(payload).asReadOnlyBuffer();
    }

    static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] out = new byte[copy.remaining()];
        copy.get(out);
        return out;
    }

    static String hex(ByteBuffer buffer) {
        return hex(bytes(buffer));
    }

    static String hex(byte[] bytes) {
        StringBuilder out = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            out.append(String.format("%02x", b & 0xFF));
        }
        return out.toString();
    }

    static byte[] hexBytes(String hex) {
        if ((hex.length() & 1) != 0) {
            throw new IllegalArgumentException("odd hex length");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(hex.length() / 2);
        for (int i = 0; i < hex.length(); i += 2) {
            out.write(Integer.parseInt(hex.substring(i, i + 2), 16));
        }
        return out.toByteArray();
    }

    record Exchange(String name, String opcodeName, String requestHeaderHex, String requestPayloadHex,
                    String responseHeaderHex, String responsePayloadHex) {
        byte[] requestHeader() {
            return hexBytes(requestHeaderHex);
        }

        byte[] requestPayload() {
            return hexBytes(requestPayloadHex);
        }

        byte[] responseHeader() {
            return hexBytes(responseHeaderHex);
        }

        byte[] responsePayload() {
            return hexBytes(responsePayloadHex);
        }
    }
}
