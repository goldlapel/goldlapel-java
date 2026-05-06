package com.goldlapel;

/**
 * Controls when the wrapper schedules a post-DML async GUC-state verify.
 *
 * <p>Background: Wave 1's verify-on-checkout / post-call verify path (filed at
 * {@code goldlapel/docs/todos/guc-rls-cache-safety.md}) covers stored functions
 * and procedures — bodies that issue {@code SET app.user_id = ...} which the
 * wire layer never sees. It does NOT cover trigger-internal SETs: a customer
 * trigger that fires on INSERT/UPDATE/DELETE and runs a {@code SET} from
 * inside its body. That's filed as the opt-in "aggressive verify" feature
 * (see {@code goldlapel/docs/todos/aggressive-verify-flag.md}).
 *
 * <p>This wrapper's twist: <b>smart-auto-enable</b>. On the first connection
 * to a given upstream, the wrapper probes {@code pg_trigger}/{@code pg_proc}
 * for trigger functions whose body looks like it issues a {@code SET}. If we
 * find any, the wrapper opts that connection (and every future connection to
 * the same JDBC URL) into post-DML verify automatically. Customers who don't
 * have such triggers pay nothing; customers who do are protected without
 * having to manually flip a flag they may not realise applies to them.
 *
 * <p>The override flag exists for the cases the smart-auto path can't reach:
 * customers who want the safety regardless ({@link #ON}), customers who know
 * their schema is clean and want zero overhead even if the probe is wrong
 * ({@link #OFF}), and the default ({@link #AUTO}) which lets the wrapper
 * decide.
 *
 * <p>Precedence (highest to lowest):
 * <ol>
 *   <li>License-payload {@code aggressive_verify_active} (if HQ has the
 *       customer's preference recorded — {@link AggressiveVerifyDetector#setLicenseOverride}).</li>
 *   <li>The mode set on this enum (CLI/env/option/Spring binding).</li>
 *   <li>For {@link #AUTO}: the first-connection detection result.</li>
 * </ol>
 */
public enum AggressiveVerifyMode {
    /**
     * Detect on first connection. The wrapper probes {@code pg_trigger}
     * joined with {@code pg_proc} for trigger functions whose source body
     * looks like it issues a session {@code SET}. If found, behaves like
     * {@link #ON} for every connection to that JDBC URL. If not, behaves
     * like {@link #OFF}. Detection runs once per JDBC URL and caches the
     * result for the JVM's lifetime — adding a new SET-issuing trigger
     * after process start requires a process restart to pick up.
     */
    AUTO,
    /**
     * Always schedule a post-DML verify. Use when you know you have
     * SET-issuing triggers, want belt-and-suspenders coverage, or are
     * running compliance-heavy workloads where a missed SET is materially
     * worse than the ~1ms post-write tax.
     */
    ON,
    /**
     * Never schedule a post-DML verify. Use when you've audited your schema
     * and know no triggers issue session-level SETs, and you'd rather not
     * pay the verify-pool tax. Wave 1's post-function-call verify still
     * runs in this mode — only the post-DML expansion is suppressed.
     */
    OFF;

    /**
     * Parse a case-insensitive string into a mode. Returns {@code null} for
     * unrecognised inputs so callers can decide on the fallback (Spring
     * binding falls back to {@link #AUTO}; the options setter throws). Used
     * by the Spring-Boot binding ({@code goldlapel.aggressive-verify}) and
     * by env-var paths in tests.
     */
    public static AggressiveVerifyMode parse(String s) {
        if (s == null) return null;
        String t = s.trim();
        if (t.isEmpty()) return null;
        switch (t.toLowerCase(java.util.Locale.ROOT)) {
            case "auto":
                return AUTO;
            case "on":
            case "true":
            case "1":
            case "yes":
                return ON;
            case "off":
            case "false":
            case "0":
            case "no":
                return OFF;
            default:
                return null;
        }
    }
}
