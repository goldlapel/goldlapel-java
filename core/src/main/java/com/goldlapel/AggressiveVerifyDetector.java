package com.goldlapel;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Process-wide cache of "does this database have triggers that issue session
 * SETs?" — answers that question once per JDBC URL and reuses the result for
 * every subsequent connection to the same URL.
 *
 * <p>Invoked from {@link ConnectionProxy#wrap(Connection, NativeCache,
 * AggressiveVerifyMode, String)} when the mode is
 * {@link AggressiveVerifyMode#AUTO}. The probe runs a single SQL query that
 * joins {@code pg_trigger} → {@code pg_proc} and scans the trigger function's
 * body for a top-level {@code SET} via a regex on {@code prosrc}. If any row
 * matches, the URL is marked active and the wrapper schedules post-DML
 * verifies for every connection to that URL.
 *
 * <p><b>Caching key.</b> JDBC connection URL string. We don't try to resolve
 * the URL to (host, port, dbname) — the URL is what the application configured
 * the pool with, and any reconfiguration that changes the URL is a deliberate
 * customer action that warrants a fresh probe. Two pools pointing at the same
 * physical database via different URLs (e.g. via different replicas, different
 * userinfo) will probe twice; that's a vanishingly rare configuration and the
 * extra probe is one query per JVM lifetime per pool.
 *
 * <p><b>Thread-safety.</b> {@link ConcurrentHashMap#computeIfAbsent} ensures
 * each URL probes exactly once even under contention. The probe query is
 * issued on a freshly-acquired physical connection, so it doesn't deadlock
 * with whatever the calling thread is doing — the per-connection lock in
 * {@link ConnectionProxy.ConnectionHandler} kicks in only after wrap() has
 * returned.
 *
 * <p><b>License-payload integration.</b> If HQ has flagged the account's
 * preference (see {@code aggressive_verify_active} on the license payload),
 * call {@link #setLicenseOverride(String, boolean)} once at startup with the
 * authoritative value and the detector skips the probe entirely. Local
 * detection is the fallback when the license payload doesn't expose the
 * field (older licenses, offline-only deployments).
 */
public final class AggressiveVerifyDetector {

    private AggressiveVerifyDetector() {}

    /**
     * URL → cached "active?" decision. Populated lazily by {@link #isActive}.
     * Boxed Boolean rather than primitive so we can distinguish "not yet
     * probed" (absent) from "probed and false" (present + false).
     */
    private static final ConcurrentHashMap<String, Boolean> CACHE = new ConcurrentHashMap<>();

    /**
     * Probe SQL — checks for trigger functions whose source body contains
     * a likely session-{@code SET} statement. The {@code WHERE} clause is
     * intentionally permissive: a regex match on {@code prosrc} catches
     * {@code SET app.user_id = ...}, {@code PERFORM set_config(...)}, and
     * the typical {@code EXECUTE format('SET ...')} patterns. False
     * positives are fine (we'd over-verify, costing ~1ms per write); false
     * negatives are the bug to avoid.
     *
     * <p>Filters out {@code SET LOCAL}: that's transactional and doesn't
     * leak into a cacheable response (the wrapper's cache layer is gated
     * on {@code !inTransaction}).
     *
     * <p>The function language matters: only {@code plpgsql} bodies can
     * legally issue session-level SETs that survive the function call;
     * {@code sql} functions can't. We restrict to {@code plpgsql} and the
     * {@code internal} catalog to keep the probe cheap on schemas with
     * many trigger functions in non-PL languages.
     */
    static final String PROBE_SQL =
        "SELECT EXISTS ("
        + " SELECT 1"
        + " FROM pg_trigger t"
        + " JOIN pg_proc p ON p.oid = t.tgfoid"
        + " JOIN pg_language l ON l.oid = p.prolang"
        + " WHERE NOT t.tgisinternal"
        + "   AND l.lanname IN ('plpgsql', 'plpython3u', 'plperl')"
        + "   AND ("
        + "     p.prosrc ~* '(^|[^a-zA-Z_])set[[:space:]]+(?!local[[:space:]])[a-zA-Z_][a-zA-Z0-9_.]*[[:space:]]*(=|to[[:space:]])'"
        + "     OR p.prosrc ~* '(^|[^a-zA-Z_])(perform|select)[[:space:]]+(pg_catalog\\.)?set_config[[:space:]]*\\('"
        + "   )"
        + ")";

    /**
     * Return whether post-DML aggressive verify is active for connections to
     * {@code jdbcUrl}. Runs the probe at most once per URL per JVM. On any
     * SQLException from the probe (permissions, network, etc.) we cache
     * {@code false} and never retry — the customer's hot path is more
     * important than re-probing on transient failures, and customers who
     * actually have SET-issuing triggers can pin {@link AggressiveVerifyMode#ON}
     * to bypass detection.
     *
     * <p>{@code probeConn} is the freshly-wrapped connection the calling
     * {@link ConnectionProxy#wrap} invocation just built — re-using it for
     * the probe avoids a separate physical-connection acquire.
     */
    public static boolean isActive(String jdbcUrl, Connection probeConn) {
        if (jdbcUrl == null) return false;
        Boolean cached = CACHE.get(jdbcUrl);
        if (cached != null) return cached;
        // computeIfAbsent ensures only one probe runs even if 100 threads call
        // through here at the same time. The lambda runs the probe and the
        // result is published atomically.
        return CACHE.computeIfAbsent(jdbcUrl, k -> probe(probeConn));
    }

    /**
     * License-payload override. Call this once at startup with the value HQ
     * provided (typically inside the proxy's startup path, before the first
     * customer connection is wrapped). Subsequent calls to {@link #isActive}
     * for the same URL return the override without running the probe.
     *
     * <p>The "license override" framing is for documentation: any caller can
     * pre-seed the cache, but the canonical caller is the license-loading
     * code. The signature is symmetric with {@link #isActive}'s contract:
     * cache once, reuse forever.
     */
    public static void setLicenseOverride(String jdbcUrl, boolean active) {
        if (jdbcUrl == null) return;
        CACHE.put(jdbcUrl, active);
    }

    /** Test-only — drop the cached decision so the next {@link #isActive}
     *  re-probes. Visible only inside the {@code com.goldlapel} package. */
    static void resetForTesting() {
        CACHE.clear();
    }

    /** Test-only — peek at the cached value without triggering a probe. */
    static Boolean peek(String jdbcUrl) {
        return CACHE.get(jdbcUrl);
    }

    private static boolean probe(Connection conn) {
        if (conn == null) return false;
        try (PreparedStatement ps = conn.prepareStatement(PROBE_SQL);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getBoolean(1);
            }
            return false;
        } catch (SQLException | RuntimeException e) {
            // Probe failed — lock in "off" so we don't keep retrying on every
            // connection. Customer can pin ON manually if they actually need
            // post-DML coverage but the catalog probe can't run (locked-down
            // permissions, restricted superuser-only views, etc.). Catching
            // RuntimeException too: a misbehaving JDBC driver shouldn't tank
            // the wrap path on the user's hot path.
            return false;
        }
    }
}
