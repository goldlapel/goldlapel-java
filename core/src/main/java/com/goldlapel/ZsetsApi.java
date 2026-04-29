package com.goldlapel;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Sorted-set (zset) namespace API — accessible as {@code gl.zsets}.
 *
 * <p>Phase 5 of schema-to-core. The proxy's v1 zset schema introduces a
 * {@code zset_key} column so a single namespace table holds many sorted
 * sets — matching Redis's mental model. Every method below threads
 * {@code zsetKey} as the first positional arg after the namespace
 * {@code name}.
 *
 * <p>Method shape: {@code gl.zsets.<verb>(name, zsetKey, ...)}. {@code name}
 * is the namespace (one Postgres table); {@code zsetKey} partitions multiple
 * sorted sets within that namespace.
 */
public final class ZsetsApi {
    private final GoldLapel gl;

    ZsetsApi(GoldLapel gl) {
        this.gl = gl;
    }

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
        Map<String, Object> entry = Ddl.fetchPatterns(gl.ddlCache(), "zset", name,
            gl.dashboardPort(), token, options);
        return Ddl.queryPatterns(entry);
    }

    public void create(String name) {
        patterns(name);
    }

    public void create(String name, boolean unlogged) {
        patterns(name, unlogged);
    }

    // ── Member ops ────────────────────────────────────────────

    /** Set-or-update a member's score under {@code zsetKey}; returns the new score. */
    public double add(String name, String zsetKey, String member, double score) throws SQLException {
        return Utils.zsetAdd(gl.resolveConn(), name, zsetKey, member, score, patterns(name));
    }

    public double add(String name, String zsetKey, String member, double score, Connection conn) throws SQLException {
        return Utils.zsetAdd(conn, name, zsetKey, member, score, patterns(name));
    }

    /** Atomic increment-or-insert; returns the new score. */
    public double incrBy(String name, String zsetKey, String member, double delta) throws SQLException {
        return Utils.zsetIncrBy(gl.resolveConn(), name, zsetKey, member, delta, patterns(name));
    }

    public double incrBy(String name, String zsetKey, String member, double delta, Connection conn) throws SQLException {
        return Utils.zsetIncrBy(conn, name, zsetKey, member, delta, patterns(name));
    }

    /** Get a member's score, or {@code null} if absent. */
    public Double score(String name, String zsetKey, String member) throws SQLException {
        return Utils.zsetScore(gl.resolveConn(), name, zsetKey, member, patterns(name));
    }

    public Double score(String name, String zsetKey, String member, Connection conn) throws SQLException {
        return Utils.zsetScore(conn, name, zsetKey, member, patterns(name));
    }

    /** Remove a member; true if removed, false if absent. */
    public boolean remove(String name, String zsetKey, String member) throws SQLException {
        return Utils.zsetRemove(gl.resolveConn(), name, zsetKey, member, patterns(name));
    }

    public boolean remove(String name, String zsetKey, String member, Connection conn) throws SQLException {
        return Utils.zsetRemove(conn, name, zsetKey, member, patterns(name));
    }

    /**
     * Get members by rank within {@code zsetKey}.
     *
     * <p>Returns a list of (member, score) entries. {@code desc=true} orders
     * highest score first (leaderboard order). {@code start}/{@code stop} are
     * 0-based inclusive bounds Redis-style; the SQL converts to LIMIT/OFFSET.
     *
     * @param stop  inclusive end rank; pass {@code -1} for "to end" (mapped to
     *              a large limit since the proxy pattern is LIMIT/OFFSET-based).
     */
    public List<Map.Entry<String, Double>> range(String name, String zsetKey,
            int start, int stop, boolean desc) throws SQLException {
        if (stop == -1) stop = 9999;
        return Utils.zsetRange(gl.resolveConn(), name, zsetKey, start, stop, desc, patterns(name));
    }

    public List<Map.Entry<String, Double>> range(String name, String zsetKey,
            int start, int stop, boolean desc, Connection conn) throws SQLException {
        if (stop == -1) stop = 9999;
        return Utils.zsetRange(conn, name, zsetKey, start, stop, desc, patterns(name));
    }

    public List<Map.Entry<String, Double>> rangeByScore(String name, String zsetKey,
            double minScore, double maxScore, int limit, int offset) throws SQLException {
        return Utils.zsetRangeByScore(gl.resolveConn(), name, zsetKey,
            minScore, maxScore, limit, offset, patterns(name));
    }

    public List<Map.Entry<String, Double>> rangeByScore(String name, String zsetKey,
            double minScore, double maxScore, int limit, int offset, Connection conn) throws SQLException {
        return Utils.zsetRangeByScore(conn, name, zsetKey,
            minScore, maxScore, limit, offset, patterns(name));
    }

    /** 0-based rank within {@code zsetKey}, or {@code null} if member absent. */
    public Long rank(String name, String zsetKey, String member, boolean desc) throws SQLException {
        return Utils.zsetRank(gl.resolveConn(), name, zsetKey, member, desc, patterns(name));
    }

    public Long rank(String name, String zsetKey, String member, boolean desc, Connection conn) throws SQLException {
        return Utils.zsetRank(conn, name, zsetKey, member, desc, patterns(name));
    }

    /** Cardinality of one zset_key namespace. */
    public long card(String name, String zsetKey) throws SQLException {
        return Utils.zsetCard(gl.resolveConn(), name, zsetKey, patterns(name));
    }

    public long card(String name, String zsetKey, Connection conn) throws SQLException {
        return Utils.zsetCard(conn, name, zsetKey, patterns(name));
    }
}
