package com.goldlapel;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Counters namespace API — accessible as {@code gl.counters}.
 *
 * <p>Phase 5 of schema-to-core: the proxy owns counter DDL. Each call here:
 *
 * <ol>
 *   <li>Calls {@code POST /api/ddl/counter/create} (idempotent) to materialize
 *       the canonical {@code _goldlapel.counter_<name>} table and pull its
 *       query patterns.
 *   <li>Caches {@code (tables, query_patterns)} on the parent {@link GoldLapel}
 *       instance for the session's lifetime — one HTTP round-trip per
 *       (family, name) per session.
 *   <li>Hands the patterns off to the {@code Utils.counter*} helpers, which
 *       execute the proxy-emitted SQL verbatim (after {@code $N → ?} JDBC
 *       translation).
 * </ol>
 *
 * <p>Mirrors {@link DocumentsApi} exactly — the canonical schema-to-core
 * sub-API shape. Each method takes the counter namespace name as the first
 * positional argument; the per-key value-mutation surface follows.
 *
 * <p>Phase 5 contract: every counter row carries an {@code updated_at}
 * column stamped on every write — the wrapper does NOT paper over this.
 */
public final class CountersApi {
    private final GoldLapel gl;

    CountersApi(GoldLapel gl) {
        // Hold a back-reference to the parent client. Never copy lifecycle
        // state (token, port, conn) onto this instance — always read through
        // `gl` so a config change on the parent (e.g. proxy restart with a
        // new dashboard token) is reflected immediately on the next call.
        this.gl = gl;
    }

    /**
     * Fetch (and cache per-instance) canonical counter DDL + query patterns
     * from the proxy. Cache lives on the parent {@link GoldLapel} instance.
     */
    public Map<String, String> patterns(String name) {
        return patterns(name, false);
    }

    public Map<String, String> patterns(String name, boolean unlogged) {
        Utils.validateIdentifier(name);
        String token = gl.dashboardToken() != null ? gl.dashboardToken() : Ddl.tokenFromEnvOrFile();
        Map<String, Object> options = null;
        if (unlogged) {
            options = new java.util.LinkedHashMap<>();
            options.put("unlogged", Boolean.TRUE);
        }
        Map<String, Object> entry = Ddl.fetchPatterns(gl.ddlCache(), "counter", name,
            gl.dashboardPort(), token, options);
        return Ddl.queryPatterns(entry);
    }

    /**
     * Eagerly materialize the counter table. Other methods will also
     * materialize on first use, so calling this is optional — provided for
     * callers that want explicit setup at startup time.
     */
    public void create(String name) {
        patterns(name);
    }

    public void create(String name, boolean unlogged) {
        patterns(name, unlogged);
    }

    // ── Per-key ops ────────────────────────────────────────────

    /** Increment-or-insert a counter; returns the new value. */
    public long incr(String name, String key) throws SQLException {
        return incr(name, key, 1L);
    }

    public long incr(String name, String key, long amount) throws SQLException {
        return Utils.counterIncr(gl.resolveConn(), name, key, amount, patterns(name));
    }

    public long incr(String name, String key, long amount, Connection conn) throws SQLException {
        return Utils.counterIncr(conn, name, key, amount, patterns(name));
    }

    /** Decrement is incr with a negative amount. Provided as a separate
     *  method so callers don't need to remember the sign convention. */
    public long decr(String name, String key) throws SQLException {
        return decr(name, key, 1L);
    }

    public long decr(String name, String key, long amount) throws SQLException {
        return Utils.counterDecr(gl.resolveConn(), name, key, amount, patterns(name));
    }

    public long decr(String name, String key, long amount, Connection conn) throws SQLException {
        return Utils.counterDecr(conn, name, key, amount, patterns(name));
    }

    /** Idempotent set-key; returns the value just stored. */
    public long set(String name, String key, long value) throws SQLException {
        return Utils.counterSet(gl.resolveConn(), name, key, value, patterns(name));
    }

    public long set(String name, String key, long value, Connection conn) throws SQLException {
        return Utils.counterSet(conn, name, key, value, patterns(name));
    }

    /** Get a counter's current value. Returns 0 for unknown keys (matches
     *  the Redis convention — no NULL surprise on cold cache). */
    public long get(String name, String key) throws SQLException {
        return Utils.counterGet(gl.resolveConn(), name, key, patterns(name));
    }

    public long get(String name, String key, Connection conn) throws SQLException {
        return Utils.counterGet(conn, name, key, patterns(name));
    }

    /** Delete a counter row. Returns true if a row was deleted, false if the
     *  key was already absent. */
    public boolean delete(String name, String key) throws SQLException {
        return Utils.counterDelete(gl.resolveConn(), name, key, patterns(name));
    }

    public boolean delete(String name, String key, Connection conn) throws SQLException {
        return Utils.counterDelete(conn, name, key, patterns(name));
    }

    /** Total distinct keys in the counter namespace. */
    public long countKeys(String name) throws SQLException {
        return Utils.counterCountKeys(gl.resolveConn(), name, patterns(name));
    }

    public long countKeys(String name, Connection conn) throws SQLException {
        return Utils.counterCountKeys(conn, name, patterns(name));
    }
}
