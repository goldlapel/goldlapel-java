package com.goldlapel.rxjava3;

/**
 * Shared integration-test gating — standardized across all Gold Lapel wrappers.
 * Duplicated in each Maven module's test sources (core, reactor, rxjava3) so
 * modules don't need a test-jar dependency on core.
 *
 * <p>See {@code core/src/test/java/com/goldlapel/IntegrationGate.java} for the
 * canonical documentation. Convention: {@code GOLDLAPEL_INTEGRATION=1} +
 * {@code GOLDLAPEL_TEST_UPSTREAM} must both be set. Classes use
 * {@link org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable} on
 * the opt-in flag, then call {@link #requireUpstream()} in {@code @BeforeAll}
 * to read the upstream URL and fail loudly on half-configured CI.
 */
final class IntegrationGate {
    private IntegrationGate() {}

    static String requireUpstream() {
        String upstream = System.getenv("GOLDLAPEL_TEST_UPSTREAM");
        if (upstream == null || upstream.isEmpty()) {
            throw new IllegalStateException(
                "GOLDLAPEL_INTEGRATION=1 is set but GOLDLAPEL_TEST_UPSTREAM is " +
                "missing. Set GOLDLAPEL_TEST_UPSTREAM to a Postgres URL " +
                "(e.g. postgresql://postgres@localhost/postgres) or unset " +
                "GOLDLAPEL_INTEGRATION to skip integration tests."
            );
        }
        return upstream;
    }
}
