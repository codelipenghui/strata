package io.strata.meta;

import io.strata.common.FileId;
import io.strata.common.StrataNamespace;

import java.nio.charset.StandardCharsets;

/**
 * Test-only deterministic {@link FileId} derivation for local metadata-log fixtures. Production system
 * files use the consensus-root monotonic allocator in {@link NamespaceLogBackend}; this helper is not a
 * production identity or collision-freedom contract.
 *
 * <h3>Derivation scheme</h3>
 * <p>A system file is identified by three coordinates: {@code (namespace, generation, kind)}.
 * <ul>
 *   <li>{@code namespace} — the user namespace whose metadata this file belongs to (e.g. "perf-0").</li>
 *   <li>{@code generation} — the manifest generation counter, incremented on every compaction/roll, so
 *       the new system file always gets a fresh id and never reuses the id of a still-referenced file.</li>
 *   <li>{@code kind} — 0 for an open-log segment, 1 for a snapshot.</li>
 * </ul>
 *
 * <p>The id is produced by feeding all three into a SplitMix64-style avalanche finalizer:
 * <ol>
 *   <li>Compute a stable 64-bit seed from the namespace UTF-8 bytes using FNV-1a-64.</li>
 *   <li>XOR-fold with {@code generation} (shifted left 1) and {@code kind} (bit 0), giving a single 64-bit
 *       accumulator where every distinct {@code (ns, generation, kind)} triple maps to a distinct input.</li>
 *   <li>Pass through the standard SplitMix64 finalizer (three multiply–XOR–shift rounds) to scatter
 *       nearby inputs across the full 64-bit range.</li>
 * </ol>
 *
 * <p>The mixer has a low collision probability at the fixture's small cardinality, and its test checks a
 * representative sample. That is sufficient for isolated test files, but it is not a mathematical guarantee.
 */
final class SystemFileIds {

    private SystemFileIds() {
    }

    /**
     * Returns a deterministic test-fixture {@link FileId} for a metadata system file.
     *
     * @param ns         the user namespace whose metadata this system file stores
     * @param generation the manifest generation counter (bumped on every compaction/roll)
     * @param kind       0 for an open-log segment; 1 for a snapshot
     */
    static FileId of(StrataNamespace ns, long generation, int kind) {
        if (kind != 0 && kind != 1) {
            throw new IllegalArgumentException("kind must be 0 (log) or 1 (snapshot): " + kind);
        }
        long h = fnv1a64(ns.value().getBytes(StandardCharsets.UTF_8));
        // Merge generation and kind into the hash seed.
        // Shift generation left by 1 so bit-0 is dedicated to kind → no overlap for adjacent generations.
        long raw = h ^ ((generation << 1) | (kind & 1L));
        return FileId.of(splitMix64(raw));
    }

    /** FNV-1a 64-bit hash of the given bytes — stable, distribution-friendly namespace fingerprint. */
    private static long fnv1a64(byte[] bytes) {
        long h = 0xcbf29ce484222325L; // FNV offset basis
        for (byte b : bytes) {
            h ^= b & 0xFFL;
            h *= 0x00000100000001b3L; // FNV prime
        }
        return h;
    }

    /** SplitMix64 finalizer — a bijective avalanche mixer over its 64-bit input. */
    private static long splitMix64(long x) {
        x = (x ^ (x >>> 30)) * 0xbf58476d1ce4e5b9L;
        x = (x ^ (x >>> 27)) * 0x94d049bb133111ebL;
        return x ^ (x >>> 31);
    }
}
