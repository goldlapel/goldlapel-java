package com.goldlapel;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Geo namespace API — accessible as {@code gl.geos}.
 *
 * <p>Phase 5 of schema-to-core. The proxy's v1 geo schema uses GEOGRAPHY
 * (not GEOMETRY), {@code member TEXT PRIMARY KEY} (not BIGSERIAL + name),
 * and a GIST index on the location column. {@link #add(String, String, double, double)}
 * is idempotent on the member name — re-adding a member updates its location.
 *
 * <p>Distance unit: methods accept {@code unit = "m" | "km" | "mi" | "ft"}.
 * The proxy column is meters-native (GEOGRAPHY default); the wrapper
 * converts at the edge.
 *
 * <p><b>Radius bind-order contract:</b>
 * <ul>
 *   <li>{@link #radius}: {@code $1=lon, $2=lat, $3=radius_m, $4=limit} —
 *       proxy CTE-anchors so each {@code $N} appears exactly once in SQL.
 *       Bind {@code (lon, lat, radius_m, limit)} — 4 args, no duplicates.
 *   <li>{@link #radiusByMember}: {@code $1, $2} are both the anchor member
 *       name (one for the join, one for the self-exclusion); {@code $3=radius_m},
 *       {@code $4=limit}. Bind {@code (member, member, radius_m, limit)}.
 * </ul>
 */
public final class GeosApi {
    private final GoldLapel gl;

    GeosApi(GoldLapel gl) {
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
        Map<String, Object> entry = Ddl.fetchPatterns(gl.ddlCache(), "geo", name,
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

    /** Set-or-update a member's lon/lat. Idempotent on the member name (PK).
     *  Returns the just-stored {@link Utils.GeoPos}. */
    public Utils.GeoPos add(String name, String member, double lon, double lat) throws SQLException {
        return Utils.geoAdd(gl.resolveConn(), name, member, lon, lat, patterns(name));
    }

    public Utils.GeoPos add(String name, String member, double lon, double lat, Connection conn) throws SQLException {
        return Utils.geoAdd(conn, name, member, lon, lat, patterns(name));
    }

    /** Fetch a member's (lon, lat), or {@code null} if absent. */
    public Utils.GeoPos pos(String name, String member) throws SQLException {
        return Utils.geoPos(gl.resolveConn(), name, member, patterns(name));
    }

    public Utils.GeoPos pos(String name, String member, Connection conn) throws SQLException {
        return Utils.geoPos(conn, name, member, patterns(name));
    }

    /** Distance between two members. {@code unit} accepts m/km/mi/ft.
     *  Returns the distance in the requested unit, or {@code null} if either
     *  member is absent. */
    public Double dist(String name, String memberA, String memberB) throws SQLException {
        return dist(name, memberA, memberB, "m");
    }

    public Double dist(String name, String memberA, String memberB, String unit) throws SQLException {
        return Utils.geoDist(gl.resolveConn(), name, memberA, memberB, unit, patterns(name));
    }

    public Double dist(String name, String memberA, String memberB, String unit, Connection conn) throws SQLException {
        return Utils.geoDist(conn, name, memberA, memberB, unit, patterns(name));
    }

    /**
     * Members within {@code radius} of {@code (lon, lat)}. Returns each
     * row as a map with {@code member}, {@code lon}, {@code lat},
     * {@code distance_m}.
     */
    public List<Map<String, Object>> radius(String name, double lon, double lat,
            double radius) throws SQLException {
        return radius(name, lon, lat, radius, "m", 50);
    }

    public List<Map<String, Object>> radius(String name, double lon, double lat,
            double radius, String unit, int limit) throws SQLException {
        return Utils.geoRadius(gl.resolveConn(), name, lon, lat, radius, unit, limit, patterns(name));
    }

    public List<Map<String, Object>> radius(String name, double lon, double lat,
            double radius, String unit, int limit, Connection conn) throws SQLException {
        return Utils.geoRadius(conn, name, lon, lat, radius, unit, limit, patterns(name));
    }

    /** Members within {@code radius} of {@code member}'s location. */
    public List<Map<String, Object>> radiusByMember(String name, String member,
            double radius) throws SQLException {
        return radiusByMember(name, member, radius, "m", 50);
    }

    public List<Map<String, Object>> radiusByMember(String name, String member,
            double radius, String unit, int limit) throws SQLException {
        return Utils.geoRadiusByMember(gl.resolveConn(), name, member, radius, unit, limit, patterns(name));
    }

    public List<Map<String, Object>> radiusByMember(String name, String member,
            double radius, String unit, int limit, Connection conn) throws SQLException {
        return Utils.geoRadiusByMember(conn, name, member, radius, unit, limit, patterns(name));
    }

    /** Delete a member; true if removed, false if absent. */
    public boolean remove(String name, String member) throws SQLException {
        return Utils.geoRemove(gl.resolveConn(), name, member, patterns(name));
    }

    public boolean remove(String name, String member, Connection conn) throws SQLException {
        return Utils.geoRemove(conn, name, member, patterns(name));
    }

    /** Total members in the namespace. */
    public long count(String name) throws SQLException {
        return Utils.geoCount(gl.resolveConn(), name, patterns(name));
    }

    public long count(String name, Connection conn) throws SQLException {
        return Utils.geoCount(conn, name, patterns(name));
    }
}
