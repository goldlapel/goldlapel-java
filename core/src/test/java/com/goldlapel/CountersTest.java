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
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link CountersApi} and the {@code Utils.counter*} helpers.
 *
 * <p>Phase 5 contract verified:
 * <ul>
 *   <li>{@code gl.counters} is a {@link CountersApi} bound to the parent.
 *   <li>The legacy flat {@code gl.incr} / {@code gl.getCounter} methods
 *       are gone — confirmed by reflection.
 *   <li>SQL execution uses the proxy's canonical patterns verbatim (after
 *       {@code $N → ?} JDBC translation), with bind order matching the
 *       proxy's placeholder numbering.
 *   <li>The canonical {@code incr} pattern stamps {@code updated_at} on
 *       every write — Phase 5 parity.
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CountersTest {

    @Mock Connection conn;
    @Mock PreparedStatement ps;
    @Mock ResultSet rs;

    GoldLapel gl;

    static Map<String, String> fakePatterns() {
        // Canonical patterns matching the proxy's v1 counter family. The
        // updated_at column is stamped on EVERY write — wrappers must not
        // paper over this.
        String main = "_goldlapel.counter_pageviews";
        Map<String, String> p = new LinkedHashMap<>();
        p.put("incr",
            "INSERT INTO " + main + " (key, value, updated_at) "
            + "VALUES ($1, $2, NOW()) ON CONFLICT (key) DO UPDATE "
            + "SET value = " + main + ".value + EXCLUDED.value, "
            + "updated_at = NOW() RETURNING value");
        p.put("set",
            "INSERT INTO " + main + " (key, value, updated_at) "
            + "VALUES ($1, $2, NOW()) ON CONFLICT (key) DO UPDATE "
            + "SET value = EXCLUDED.value, updated_at = NOW() RETURNING value");
        p.put("get", "SELECT value FROM " + main + " WHERE key = $1");
        p.put("delete", "DELETE FROM " + main + " WHERE key = $1");
        p.put("count_keys", "SELECT COUNT(*) FROM " + main);
        return p;
    }

    @BeforeEach
    void setUp() throws Exception {
        gl = new GoldLapel("postgresql://u:p@host:5432/db", new GoldLapelOptions());
        java.lang.reflect.Field f = GoldLapel.class.getDeclaredField("internalConn");
        f.setAccessible(true);
        f.set(gl, conn);
        // Seed cache so the family API skips the proxy round-trip.
        Map<String, Object> tables = new LinkedHashMap<>();
        tables.put("main", "pageviews");
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("tables", tables);
        entry.put("query_patterns", fakePatterns());
        gl.ddlCache().put("counter:pageviews", entry);
    }


    @Nested class NamespaceShape {

        @Test
        void countersIsACountersApi() {
            assertNotNull(gl.counters);
            assertSame(CountersApi.class, gl.counters.getClass());
        }

        @Test
        void noLegacyFlatMethods() {
            // Phase 5 hard cut — gl.incr / gl.getCounter are gone.
            for (String legacy : new String[]{"incr", "getCounter"}) {
                assertFalse(java.util.Arrays.stream(GoldLapel.class.getMethods())
                        .anyMatch(m -> m.getName().equals(legacy)),
                    "Phase 5 removed flat " + legacy + " — use gl.counters.<verb>.");
            }
        }

        @Test
        void countersHoldsBackReferenceToParent() throws Exception {
            // Sanity check — the namespace should never copy lifecycle state.
            java.lang.reflect.Field f = CountersApi.class.getDeclaredField("gl");
            f.setAccessible(true);
            assertSame(gl, f.get(gl.counters));
        }
    }


    @Nested class IncrTest {

        @Test
        void executesProxyIncrPattern() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(7L);

            long val = gl.counters.incr("pageviews", "home", 5);

            assertEquals(7L, val);
            ArgumentCaptor<String> sqlC = ArgumentCaptor.forClass(String.class);
            verify(conn).prepareStatement(sqlC.capture());
            String sql = sqlC.getValue();
            // Proxy's $N translated to ? for JDBC.
            assertFalse(sql.contains("$"));
            // Phase 5 canonical: updated_at stamped on every UPDATE.
            assertTrue(sql.contains("updated_at = NOW()"),
                "Phase 5 contract: incr pattern must stamp updated_at");
            // Bind order matches $1=key, $2=amount.
            verify(ps).setString(1, "home");
            verify(ps).setLong(2, 5L);
        }

        @Test
        void defaultIncrAmountIsOne() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(1L);

            gl.counters.incr("pageviews", "home");

            verify(ps).setLong(2, 1L);
        }

        @Test
        void decrPassesNegativeAmount() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(-3L);

            gl.counters.decr("pageviews", "home", 3);

            // decr is implemented as incr(-amount).
            verify(ps).setLong(2, -3L);
        }
    }


    @Nested class GetTest {

        @Test
        void returnsZeroForUnknownKey() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            // Redis convention: cold cache reads return 0, not null.
            assertEquals(0L, gl.counters.get("pageviews", "missing"));
        }

        @Test
        void returnsValueForKnownKey() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(42L);

            assertEquals(42L, gl.counters.get("pageviews", "home"));
        }
    }


    @Nested class DeleteTest {

        @Test
        void returnsTrueWhenRowDeleted() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeUpdate()).thenReturn(1);
            assertTrue(gl.counters.delete("pageviews", "home"));
        }

        @Test
        void returnsFalseWhenAbsent() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeUpdate()).thenReturn(0);
            assertFalse(gl.counters.delete("pageviews", "missing"));
        }
    }


    @Nested class MissingPatternsTest {

        @Test
        void utilsRaisesWhenPatternsNull() {
            assertThrows(IllegalArgumentException.class, () ->
                Utils.counterIncr(conn, "pageviews", "k", 1L, null));
        }
    }
}
