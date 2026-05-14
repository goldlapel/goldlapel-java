package com.goldlapel;

/**
 * Controls whether the wrapper bumps the per-connection post-DML sequence
 * counter ({@link GucState#bumpDmlSeq()}) after every observed
 * INSERT/UPDATE/DELETE/MERGE/TRUNCATE/CALL/DDL.
 *
 * <p>Background. Wave 1's verify-on-checkout / post-call verify path covers
 * stored functions and procedures — bodies that issue {@code SET app.user_id
 * = ...} which the wire layer never sees. It does NOT cover
 * trigger-internal SETs: a customer trigger that fires on INSERT/UPDATE/
 * DELETE and runs a {@code SET} from inside its body. The
 * "aggressive verify" feature plugs that gap by rolling the cache-key
 * forward on every observed write, so a cached pre-DML response cannot be
 * served against the mutated session state. See
 * {@code goldlapel/docs/todos/aggressive-verify-flag.md} and
 * {@code docs/todos/guc-rls-cache-safety.md}.
 *
 * <p>Wave 2 (this file). Previous iterations gated the bump on a
 * smart-auto-enable probe ({@code pg_trigger} scan on first connection per
 * JDBC URL). The probe adds startup latency, churns through a per-URL
 * detection cache, and can be wrong (lock-downs on the catalog, dialects
 * the regex doesn't match, triggers added after process start). Replaced
 * with always-on bumping: the cost is one tiny counter increment + hash
 * recompute per write — measurably free against the wire-trip a write
 * already takes — and the safety is universal. {@link #OFF} remains an
 * opt-out for customers who have audited their schema and want the
 * peer-shareable cache slot post-DML.
 *
 * <p>{@link #AUTO} and {@link #ON} are now semantic synonyms — both mean
 * "always bump." {@link #AUTO} stays the documented default; {@link #ON}
 * is kept for callers that wrote their config to be explicit. The
 * distinction had teeth under the smart-auto-enable design and is preserved
 * as a no-op API for backwards-compatibility with existing wrapper configs.
 */
public enum AggressiveVerifyMode {
    /**
     * Default. Bump the post-DML sequence counter on every observed write
     * so the cache key rolls forward and pre-DML cached entries cannot be
     * served against post-DML state. Identical behaviour to {@link #ON} —
     * the distinction is documentation only (AUTO = "I haven't thought
     * about this," ON = "I explicitly want this").
     */
    AUTO,
    /**
     * Bump the post-DML sequence counter on every observed write. Currently
     * identical to {@link #AUTO} — preserved as a distinct enum value so
     * existing config that names {@code on} doesn't break.
     */
    ON,
    /**
     * Skip the post-DML bump. Use only when you've audited your schema and
     * confirmed no triggers issue session-level SETs from inside their
     * bodies — otherwise a trigger-mutated session state could replay a
     * stale cached row through the cache layer. Wave 1's
     * post-function-call verify still runs in OFF mode (the body-of-a-call
     * SET case stays covered).
     *
     * <p>The wrapper logs a one-time warning on the first OFF-mode
     * connection so the operator sees the explicit opt-out in their logs —
     * if a future investigation surfaces a trigger-internal SET as the
     * cause of a cache-safety bug, the log line is the audit trail.
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
