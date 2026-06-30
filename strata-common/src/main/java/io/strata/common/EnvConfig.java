package io.strata.common;

/** Reads STRATA_* environment variables for static-init config in the common/proto transport layer. */
public final class EnvConfig {
    private EnvConfig() {}

    public static int intEnv(String name, int def) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? def : Integer.parseInt(v.trim());
    }

    public static long longEnv(String name, long def) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? def : Long.parseLong(v.trim());
    }
}
