package com.goldlapel;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Hash namespace API — accessible as {@code gl.hashes}.
 *
 * <p>Phase 5 of schema-to-core. The proxy's v1 hash schema is row-per-field
 * ({@code hash_key}, {@code field}, {@code value}) — NOT the legacy
 * JSONB-blob-per-key shape. Every method threads {@code hashKey} as the first
 * positional arg after the namespace {@code name}. {@code value} is a JSON
 * string the caller has already serialized (the wrapper does not bundle a
 * JSON tree library; pick your own).
 *
 * <p>{@link #getAll(String, String)} re-assembles the (field, value) rows on
 * the client into a {@code Map<String, String>} keyed by field name. Values
 * remain raw JSON strings — same shape as {@link #get(String, String, String)}.
 */
public final class HashesApi {
    private final GoldLapel gl;

    HashesApi(GoldLapel gl) {
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
        Map<String, Object> entry = Ddl.fetchPatterns(gl.ddlCache(), "hash", name,
            gl.dashboardPort(), token, options);
        return Ddl.queryPatterns(entry);
    }

    public void create(String name) {
        patterns(name);
    }

    public void create(String name, boolean unlogged) {
        patterns(name, unlogged);
    }

    // ── Field ops ────────────────────────────────────────────

    /** Set a field's value (single-row UPSERT). {@code valueJson} is stored
     *  as JSONB. Returns the value just stored. */
    public String set(String name, String hashKey, String field, String valueJson) throws SQLException {
        return Utils.hashSet(gl.resolveConn(), name, hashKey, field, valueJson, patterns(name));
    }

    public String set(String name, String hashKey, String field, String valueJson, Connection conn) throws SQLException {
        return Utils.hashSet(conn, name, hashKey, field, valueJson, patterns(name));
    }

    /** Get a field's value (raw JSON string), or {@code null} if absent. */
    public String get(String name, String hashKey, String field) throws SQLException {
        return Utils.hashGet(gl.resolveConn(), name, hashKey, field, patterns(name));
    }

    public String get(String name, String hashKey, String field, Connection conn) throws SQLException {
        return Utils.hashGet(conn, name, hashKey, field, patterns(name));
    }

    /** Re-assemble every (field, value) under {@code hashKey} into a single
     *  map. Empty map if the key has no fields. */
    public Map<String, String> getAll(String name, String hashKey) throws SQLException {
        return Utils.hashGetAll(gl.resolveConn(), name, hashKey, patterns(name));
    }

    public Map<String, String> getAll(String name, String hashKey, Connection conn) throws SQLException {
        return Utils.hashGetAll(conn, name, hashKey, patterns(name));
    }

    public List<String> keys(String name, String hashKey) throws SQLException {
        return Utils.hashKeys(gl.resolveConn(), name, hashKey, patterns(name));
    }

    public List<String> keys(String name, String hashKey, Connection conn) throws SQLException {
        return Utils.hashKeys(conn, name, hashKey, patterns(name));
    }

    public List<String> values(String name, String hashKey) throws SQLException {
        return Utils.hashValues(gl.resolveConn(), name, hashKey, patterns(name));
    }

    public List<String> values(String name, String hashKey, Connection conn) throws SQLException {
        return Utils.hashValues(conn, name, hashKey, patterns(name));
    }

    public boolean exists(String name, String hashKey, String field) throws SQLException {
        return Utils.hashExists(gl.resolveConn(), name, hashKey, field, patterns(name));
    }

    public boolean exists(String name, String hashKey, String field, Connection conn) throws SQLException {
        return Utils.hashExists(conn, name, hashKey, field, patterns(name));
    }

    /** Delete a field; true if deleted, false if absent. */
    public boolean delete(String name, String hashKey, String field) throws SQLException {
        return Utils.hashDelete(gl.resolveConn(), name, hashKey, field, patterns(name));
    }

    public boolean delete(String name, String hashKey, String field, Connection conn) throws SQLException {
        return Utils.hashDelete(conn, name, hashKey, field, patterns(name));
    }

    /** Number of fields under {@code hashKey}. */
    public long len(String name, String hashKey) throws SQLException {
        return Utils.hashLen(gl.resolveConn(), name, hashKey, patterns(name));
    }

    public long len(String name, String hashKey, Connection conn) throws SQLException {
        return Utils.hashLen(conn, name, hashKey, patterns(name));
    }
}
