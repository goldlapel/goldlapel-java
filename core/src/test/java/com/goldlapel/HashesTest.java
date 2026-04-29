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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link HashesApi} and {@code Utils.hash*} helpers.
 *
 * <p>Phase 5 contract verified:
 * <ul>
 *   <li>Storage flipped from "JSONB blob per key" to "row-per-field"
 *       ({@code hash_key}, {@code field}, {@code value}).
 *   <li>{@code set} executes a single-row UPSERT (NOT load-merge-save).
 *   <li>{@code getAll} re-assembles rows into a {@code Map} client-side.
 *   <li>{@code delete} returns {@code true}/{@code false} from rowcount,
 *       not from a JSONB key probe.
 *   <li>The legacy flat {@code hset}/{@code hget}/{@code hgetall}/{@code hdel}
 *       methods are gone.
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HashesTest {

    @Mock Connection conn;
    @Mock PreparedStatement ps;
    @Mock ResultSet rs;

    GoldLapel gl;

    static Map<String, String> fakePatterns() {
        String main = "_goldlapel.hash_sessions";
        Map<String, String> p = new LinkedHashMap<>();
        p.put("hset", "INSERT INTO " + main + " (hash_key, field, value) "
            + "VALUES ($1, $2, $3::jsonb) ON CONFLICT (hash_key, field) "
            + "DO UPDATE SET value = EXCLUDED.value RETURNING value");
        p.put("hget", "SELECT value FROM " + main
            + " WHERE hash_key = $1 AND field = $2");
        p.put("hgetall", "SELECT field, value FROM " + main
            + " WHERE hash_key = $1 ORDER BY field");
        p.put("hkeys", "SELECT field FROM " + main
            + " WHERE hash_key = $1 ORDER BY field");
        p.put("hvals", "SELECT value FROM " + main
            + " WHERE hash_key = $1 ORDER BY field");
        p.put("hexists", "SELECT EXISTS (SELECT 1 FROM " + main
            + " WHERE hash_key = $1 AND field = $2)");
        p.put("hdel", "DELETE FROM " + main + " WHERE hash_key = $1 AND field = $2");
        p.put("hlen", "SELECT COUNT(*) FROM " + main + " WHERE hash_key = $1");
        return p;
    }

    @BeforeEach
    void setUp() throws Exception {
        gl = new GoldLapel("postgresql://u:p@host:5432/db", new GoldLapelOptions());
        java.lang.reflect.Field f = GoldLapel.class.getDeclaredField("internalConn");
        f.setAccessible(true);
        f.set(gl, conn);
        Map<String, Object> tables = new LinkedHashMap<>();
        tables.put("main", "sessions");
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("tables", tables);
        entry.put("query_patterns", fakePatterns());
        gl.ddlCache().put("hash:sessions", entry);
    }


    @Nested class NamespaceShape {

        @Test
        void hashesIsAHashesApi() {
            assertNotNull(gl.hashes);
            assertSame(HashesApi.class, gl.hashes.getClass());
        }

        @Test
        void noLegacyFlatMethods() {
            for (String legacy : new String[]{"hset", "hget", "hgetall", "hdel"}) {
                assertFalse(java.util.Arrays.stream(GoldLapel.class.getMethods())
                        .anyMatch(m -> m.getName().equals(legacy)),
                    "Phase 5 removed flat " + legacy);
            }
        }
    }


    @Nested class SetTest {

        @Test
        void executesSingleRowUpsertNotLoadMerge() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getString(1)).thenReturn("\"alice\"");

            String result = gl.hashes.set("sessions", "user:1", "name", "\"alice\"");

            assertEquals("\"alice\"", result);
            // Single execute — not load-then-merge-then-save.
            verify(conn, times(1)).prepareStatement(anyString());
            ArgumentCaptor<String> sqlC = ArgumentCaptor.forClass(String.class);
            verify(conn).prepareStatement(sqlC.capture());
            String sql = sqlC.getValue();
            assertTrue(sql.contains("(hash_key, field, value)"));
            assertTrue(sql.contains("ON CONFLICT (hash_key, field)"));
            // Phase 5: NO blob-merge construction.
            assertFalse(sql.contains("jsonb_build_object"));
            // Bind order $1=hash_key, $2=field, $3=value.
            verify(ps).setString(1, "user:1");
            verify(ps).setString(2, "name");
            verify(ps).setString(3, "\"alice\"");
        }
    }


    @Nested class GetTest {

        @Test
        void returnsNullForAbsentField() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);
            assertNull(gl.hashes.get("sessions", "user:1", "missing"));
        }

        @Test
        void returnsValue() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getString(1)).thenReturn("\"alice\"");
            assertEquals("\"alice\"", gl.hashes.get("sessions", "user:1", "name"));
        }
    }


    @Nested class GetAllTest {

        @Test
        void rebuildsMapFromRows() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true, true, false);
            when(rs.getString(1)).thenReturn("email", "name");
            when(rs.getString(2)).thenReturn("\"a@x\"", "\"alice\"");

            Map<String, String> result = gl.hashes.getAll("sessions", "user:1");

            assertEquals(2, result.size());
            assertEquals("\"a@x\"", result.get("email"));
            assertEquals("\"alice\"", result.get("name"));
        }

        @Test
        void returnsEmptyMapForUnknownKey() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);
            Map<String, String> result = gl.hashes.getAll("sessions", "missing");
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }
    }


    @Nested class DeleteAndExistsTest {

        @Test
        void deleteUsesRowcount() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeUpdate()).thenReturn(1);
            assertTrue(gl.hashes.delete("sessions", "user:1", "name"));

            when(ps.executeUpdate()).thenReturn(0);
            assertFalse(gl.hashes.delete("sessions", "user:1", "missing"));
        }

        @Test
        void existsReadsBoolean() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getBoolean(1)).thenReturn(true);
            assertTrue(gl.hashes.exists("sessions", "user:1", "name"));
        }
    }


    @Nested class KeysAndValuesTest {

        @Test
        void keysReturnsList() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true, true, false);
            when(rs.getString(1)).thenReturn("email", "name");

            List<String> keys = gl.hashes.keys("sessions", "user:1");
            assertEquals(2, keys.size());
            assertEquals("email", keys.get(0));
            assertEquals("name", keys.get(1));
        }
    }
}
