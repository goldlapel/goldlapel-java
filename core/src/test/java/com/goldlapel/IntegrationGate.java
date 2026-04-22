package com.goldlapel;

/**
 * Shared integration-test gating — standardized across all Gold Lapel wrappers.
 *
 * <p>Convention:
 * <ul>
 *   <li>{@code GOLDLAPEL_INTEGRATION=1} — explicit opt-in gate ("yes, really run these")</li>
 *   <li>{@code GOLDLAPEL_TEST_UPSTREAM} — Postgres URL for the test upstream</li>
 * </ul>
 *
 * <p>Both must be set. Classes use {@link org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable}
 * to skip on the opt-in flag alone, then call {@link #requireUpstream()} in
 * {@code @BeforeAll} to read the upstream URL. If {@code GOLDLAPEL_INTEGRATION=1}
 * is set but {@code GOLDLAPEL_TEST_UPSTREAM} is missing, {@code requireUpstream()}
 * throws — preventing a half-configured CI from silently skipping integration
 * tests and producing a false-green unit-only run.
 */
final class IntegrationGate {
    private IntegrationGate() {}

    /**
     * Returns the upstream Postgres URL from {@code GOLDLAPEL_TEST_UPSTREAM}.
     * Throws {@link IllegalStateException} if the env var is missing (the
     * class-level {@code @EnabledIfEnvironmentVariable("GOLDLAPEL_INTEGRATION")}
     * gate ensures this is only called when {@code GOLDLAPEL_INTEGRATION=1},
     * so a missing upstream means the CI is half-configured — fail loudly).
     */
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
