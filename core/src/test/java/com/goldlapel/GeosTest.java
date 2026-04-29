package com.goldlapel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link GeosApi} and {@code Utils.geo*} helpers.
 *
 * <p>Phase 5 contract verified:
 * <ul>
 *   <li>GEOGRAPHY-native (no {@code ::geography} casts on column refs).
 *   <li>{@code geoadd} is idempotent on {@code member} (PK), via
 *       ON CONFLICT DO UPDATE.
 *   <li>{@link GeosApi#radius} bind order is
 *       {@code (lon, lat, radius_m, limit)} — 4 args matching the proxy's
 *       CTE-anchored {@code $1=lon, $2=lat, $3=radius_m, $4=limit}.
 *   <li>{@link GeosApi#radiusByMember} bind order is
 *       {@code (member, member, radius_m, limit)} — 4 args; both
 *       {@code $1} and {@code $2} hold the anchor member name.
 *   <li>Distance unit conversion (m / km / mi / ft) at the wrapper edge.
 *   <li>The legacy flat {@code geoadd}/{@code georadius}/{@code geodist}
 *       methods are gone.
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class GeosTest {

    @Mock Connection conn;
    @Mock PreparedStatement ps;
    @Mock ResultSet rs;
    @Mock ResultSetMetaData meta;

    GoldLapel gl;

    static Map<String, String> fakePatterns() {
        // Proxy v1 geo schema. CTE-anchored radius patterns: each $N
        // appears exactly once in the rendered SQL so JDBC binds match
        // (lon, lat, radius_m, limit) for georadius_with_dist and
        // (member, member, radius_m, limit) for geosearch_member.
        String main = "_goldlapel.geo_riders";
        Map<String, String> p = new LinkedHashMap<>();
        p.put("geoadd", "INSERT INTO " + main + " (member, location, updated_at) "
            + "VALUES ($1, ST_SetSRID(ST_MakePoint($2, $3), 4326)::geography, NOW()) "
            + "ON CONFLICT (member) DO UPDATE SET location = EXCLUDED.location, "
            + "updated_at = NOW() RETURNING ST_X(location::geometry) AS lon, "
            + "ST_Y(location::geometry) AS lat");
        p.put("geopos", "SELECT ST_X(location::geometry) AS lon, "
            + "ST_Y(location::geometry) AS lat FROM " + main + " WHERE member = $1");
        p.put("geodist", "SELECT ST_Distance(a.location, b.location) AS distance_m "
            + "FROM " + main + " a, " + main + " b "
            + "WHERE a.member = $1 AND b.member = $2");
        // CTE-anchored: each $N appears once. Proxy is responsible for the
        // anchor materialization; the wrapper just binds 4 values.
        p.put("georadius_with_dist",
            "WITH anchor AS (SELECT ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography AS pt) "
            + "SELECT b.member, ST_X(b.location::geometry) AS lon, "
            + "ST_Y(b.location::geometry) AS lat, "
            + "ST_Distance(b.location, anchor.pt) AS distance_m "
            + "FROM " + main + " b, anchor "
            + "WHERE ST_DWithin(b.location, anchor.pt, $3) "
            + "ORDER BY distance_m LIMIT $4");
        // Both $1 and $2 are the anchor member; bind member twice.
        p.put("geosearch_member",
            "SELECT b.member, ST_X(b.location::geometry) AS lon, "
            + "ST_Y(b.location::geometry) AS lat, "
            + "ST_Distance(b.location, a.location) AS distance_m "
            + "FROM " + main + " a, " + main + " b "
            + "WHERE a.member = $1 AND ST_DWithin(b.location, a.location, $3) "
            + "AND b.member <> $2 ORDER BY distance_m LIMIT $4");
        p.put("geo_remove", "DELETE FROM " + main + " WHERE member = $1");
        p.put("geo_count", "SELECT COUNT(*) FROM " + main);
        return p;
    }

    @BeforeEach
    void setUp() throws Exception {
        gl = new GoldLapel("postgresql://u:p@host:5432/db", new GoldLapelOptions());
        java.lang.reflect.Field f = GoldLapel.class.getDeclaredField("internalConn");
        f.setAccessible(true);
        f.set(gl, conn);
        Map<String, Object> tables = new LinkedHashMap<>();
        tables.put("main", "riders");
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("tables", tables);
        entry.put("query_patterns", fakePatterns());
        gl.ddlCache().put("geo:riders", entry);
    }


    @Nested class NamespaceShape {

        @Test
        void geosIsAGeosApi() {
            assertNotNull(gl.geos);
            assertSame(GeosApi.class, gl.geos.getClass());
        }

        @Test
        void noLegacyFlatMethods() {
            for (String legacy : new String[]{"geoadd", "geodist", "georadius"}) {
                assertFalse(java.util.Arrays.stream(GoldLapel.class.getMethods())
                        .anyMatch(m -> m.getName().equals(legacy)),
                    "Phase 5 removed flat " + legacy);
            }
        }
    }


    @Nested class AddTest {

        @Test
        void idempotentViaOnConflictDoUpdate() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getDouble(1)).thenReturn(13.4);
            when(rs.getDouble(2)).thenReturn(52.5);

            Utils.GeoPos pos = gl.geos.add("riders", "alice", 13.4, 52.5);

            assertNotNull(pos);
            assertEquals(13.4, pos.lon());
            assertEquals(52.5, pos.lat());

            ArgumentCaptor<String> sqlC = ArgumentCaptor.forClass(String.class);
            verify(conn).prepareStatement(sqlC.capture());
            String sql = sqlC.getValue();
            // Phase 5 idempotency: re-add updates location on member-PK conflict.
            assertTrue(sql.contains("ON CONFLICT (member)"));
            assertTrue(sql.contains("DO UPDATE"));
            // GEOGRAPHY-native (cast at insert site only — column itself IS geography).
            assertTrue(sql.contains("::geography"));
        }
    }


    @Nested class RadiusTest {

        // Common mock plumbing for radius queries that return a row set with
        // four columns. We don't care about the row contents here — we only
        // need to assert bind order.
        void mockEmptyRadiusResult() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);
            when(rs.getMetaData()).thenReturn(meta);
            when(meta.getColumnCount()).thenReturn(0);
        }

        @Test
        void radiusBindsLonLatRadiusLimit_FourArgs_NoDuplicates() throws SQLException {
            // The hard contract from the dispatcher prompt:
            //   $1=lon, $2=lat, $3=radius_m, $4=limit
            // CTE-anchored proxy emits each $N once, so JDBC binds 4 unique args.
            mockEmptyRadiusResult();

            gl.geos.radius("riders", 13.4, 52.5, 5, "km", 50);

            // 5 km → 5000.0 m at the wrapper edge.
            verify(ps).setDouble(1, 13.4);
            verify(ps).setDouble(2, 52.5);
            verify(ps).setDouble(3, 5000.0);
            verify(ps).setInt(4, 50);
            // Crucially: no extra bindings (the prompt explicitly says
            // "4 args, no duplicates").
            verify(ps, times(3)).setDouble(anyInt(), anyDouble());
            verify(ps, times(1)).setInt(anyInt(), anyInt());
        }

        @Test
        void radiusByMemberBindsMemberMemberRadiusLimit() throws SQLException {
            // The hard contract from the dispatcher prompt:
            //   geosearch_member: $1=member, $2=member, $3=radius_m, $4=limit
            // Both $1 and $2 are the anchor member name (one for the join,
            // one for the self-exclusion).
            mockEmptyRadiusResult();

            gl.geos.radiusByMember("riders", "alice", 1000, "m", 50);

            verify(ps).setString(1, "alice");
            verify(ps).setString(2, "alice");
            verify(ps).setDouble(3, 1000.0);
            verify(ps).setInt(4, 50);
        }

        @Test
        void radiusDefaultsAreMetersAndLimit50() throws SQLException {
            mockEmptyRadiusResult();
            gl.geos.radius("riders", 13.4, 52.5, 1000);
            verify(ps).setDouble(3, 1000.0);
            verify(ps).setInt(4, 50);
        }
    }


    @Nested class DistTest {

        @Test
        void defaultUnitIsMeters() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getDouble(1)).thenReturn(1234.0);

            assertEquals(1234.0, gl.geos.dist("riders", "alice", "bob"));
        }

        @Test
        void convertsToKilometers() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getDouble(1)).thenReturn(1234.0);

            assertEquals(1.234, gl.geos.dist("riders", "alice", "bob", "km"), 1e-9);
        }

        @Test
        void convertsToMiles() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getDouble(1)).thenReturn(1609.344);

            assertEquals(1.0, gl.geos.dist("riders", "alice", "bob", "mi"), 1e-6);
        }

        @Test
        void convertsToFeet() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getDouble(1)).thenReturn(0.3048);

            assertEquals(1.0, gl.geos.dist("riders", "alice", "bob", "ft"), 1e-9);
        }

        @Test
        void unknownUnitRaises() {
            assertThrows(IllegalArgumentException.class, () ->
                gl.geos.dist("riders", "alice", "bob", "parsec"));
        }

        @Test
        void absentMembersReturnsNull() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);
            assertNull(gl.geos.dist("riders", "alice", "bob"));
        }
    }


    @Nested class RemoveAndCountTest {

        @Test
        void removeReturnsTrueOnRowcount() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeUpdate()).thenReturn(1);
            assertTrue(gl.geos.remove("riders", "alice"));

            when(ps.executeUpdate()).thenReturn(0);
            assertFalse(gl.geos.remove("riders", "missing"));
        }

        @Test
        void countReturnsLong() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(3L);
            assertEquals(3L, gl.geos.count("riders"));
        }
    }


    @Nested class UnitConversionTest {

        // Direct exercises of the package-private toMeters / convertDistanceMeters
        // helpers — sanity checks that the conversion table matches Python's
        // (m=1, km=1000, mi=1609.344, ft=0.3048).

        @Test
        void toMeters() {
            assertEquals(1.0, Utils.toMeters(1.0, "m"));
            assertEquals(1000.0, Utils.toMeters(1.0, "km"));
            assertEquals(1609.344, Utils.toMeters(1.0, "mi"));
            assertEquals(0.3048, Utils.toMeters(1.0, "ft"));
        }

        @Test
        void convertDistanceMeters() {
            assertEquals(1.0, Utils.convertDistanceMeters(1.0, "m"));
            assertEquals(0.001, Utils.convertDistanceMeters(1.0, "km"));
            assertEquals(1.0, Utils.convertDistanceMeters(1609.344, "mi"), 1e-9);
            assertEquals(1.0, Utils.convertDistanceMeters(0.3048, "ft"), 1e-9);
        }
    }
}
