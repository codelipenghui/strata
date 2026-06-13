package io.strata.common;

/** Shared rules for Strata identifier names (namespaces and path segments). */
final class Names {
    private Names() {}

    /** The single owner of the allowed character set for namespaces and path segments. */
    static boolean isNameChar(char c) {
        return c >= 'A' && c <= 'Z'
                || c >= 'a' && c <= 'z'
                || c >= '0' && c <= '9'
                || c == '.'
                || c == '_'
                || c == '-';
    }
}
