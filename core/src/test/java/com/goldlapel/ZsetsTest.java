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
 * Unit tests for {@link ZsetsApi} and {@code Utils.zset*} helpers.
 *
 * <p>Phase 5 contract verified:
 * <ul>
 *   <li>{@code zset_key} threads through every method as the first
 *       positional arg after the namespace {@code name} (Redis ZADD shape).
 *   <li>Pattern selection picks {@code zrange_asc} vs {@code zrange_desc}
 *       based on the {@code desc} arg.
 *   <li>{@code range} translates Redis-inclusive {@code stop} to a SQL LIMIT.
 *   <li>SQL bind order is {@code (zset_key, member, score)} matching the
 *       proxy's {@code $1, $2, $3} placeholders.
 *   <li>The legacy flat methods {@code zadd}/{@code zincrby}/{@code zrange}/...
 *       are gone.
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ZsetsTest {

    @Mock Connection conn;
    @Mock PreparedStatement ps;
    @Mock ResultSet rs;

    GoldLapel gl;

    static Map<String, String> fakePatterns() {
        String main = "_goldlapel.zset_leaderboard";
        Map<String, String> p = new LinkedHashMap<>();
        p.put("zadd", "INSERT INTO " + main + " (zset_key, member, score) "
            + "VALUES ($1, $2, $3) ON CONFLICT (zset_key, member) "
            + "DO UPDATE SET score = EXCLUDED.score RETURNING score");
        p.put("zincrby", "INSERT INTO " + main + " (zset_key, member, score) "
            + "VALUES ($1, $2, $3) ON CONFLICT (zset_key, member) "
            + "DO UPDATE SET score = " + main + ".score + EXCLUDED.score RETURNING score");
        p.put("zscore", "SELECT score FROM " + main + " WHERE zset_key = $1 AND member = $2");
        p.put("zrem", "DELETE FROM " + main + " WHERE zset_key = $1 AND member = $2");
        p.put("zrange_asc", "SELECT member, score FROM " + main
            + " WHERE zset_key = $1 ORDER BY score ASC, member ASC LIMIT $2 OFFSET $3");
        p.put("zrange_desc", "SELECT member, score FROM " + main
            + " WHERE zset_key = $1 ORDER BY score DESC, member DESC LIMIT $2 OFFSET $3");
        p.put("zrangebyscore", "SELECT member, score FROM " + main
            + " WHERE zset_key = $1 AND score >= $2 AND score <= $3 "
            + "ORDER BY score ASC, member ASC LIMIT $4 OFFSET $5");
        p.put("zrank_asc", "SELECT rank FROM (SELECT member, ROW_NUMBER() OVER "
            + "(ORDER BY score ASC, member ASC) - 1 AS rank FROM " + main
            + " WHERE zset_key = $1) ranked WHERE member = $2");
        p.put("zrank_desc", "SELECT rank FROM (SELECT member, ROW_NUMBER() OVER "
            + "(ORDER BY score DESC, member DESC) - 1 AS rank FROM " + main
            + " WHERE zset_key = $1) ranked WHERE member = $2");
        p.put("zcard", "SELECT COUNT(*) FROM " + main + " WHERE zset_key = $1");
        return p;
    }

    @BeforeEach
    void setUp() throws Exception {
        gl = new GoldLapel("postgresql://u:p@host:5432/db", new GoldLapelOptions());
        java.lang.reflect.Field f = GoldLapel.class.getDeclaredField("internalConn");
        f.setAccessible(true);
        f.set(gl, conn);
        Map<String, Object> tables = new LinkedHashMap<>();
        tables.put("main", "leaderboard");
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("tables", tables);
        entry.put("query_patterns", fakePatterns());
        gl.ddlCache().put("zset:leaderboard", entry);
    }


    @Nested class NamespaceShape {

        @Test
        void zsetsIsAZsetsApi() {
            assertNotNull(gl.zsets);
            assertSame(ZsetsApi.class, gl.zsets.getClass());
        }

        @Test
        void noLegacyFlatMethods() {
            for (String legacy : new String[]{
                "zadd", "zincrby", "zrange", "zrank", "zscore", "zrem"
            }) {
                assertFalse(java.util.Arrays.stream(GoldLapel.class.getMethods())
                        .anyMatch(m -> m.getName().equals(legacy)),
                    "Phase 5 removed flat " + legacy);
            }
        }
    }


    @Nested class AddTest {

        @Test
        void bindOrderIsZsetKeyMemberScore() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getDouble(1)).thenReturn(100.0);

            double val = gl.zsets.add("leaderboard", "global", "alice", 100.0);

            assertEquals(100.0, val);
            // Phase 5 contract: $1=zset_key, $2=member, $3=score.
            verify(ps).setString(1, "global");
            verify(ps).setString(2, "alice");
            verify(ps).setDouble(3, 100.0);
            ArgumentCaptor<String> sqlC = ArgumentCaptor.forClass(String.class);
            verify(conn).prepareStatement(sqlC.capture());
            assertTrue(sqlC.getValue().contains("(zset_key, member, score)"));
        }
    }


    @Nested class RangeTest {

        @Test
        void picksDescPatternByDefault() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            gl.zsets.range("leaderboard", "global", 0, 9, true);

            ArgumentCaptor<String> sqlC = ArgumentCaptor.forClass(String.class);
            verify(conn).prepareStatement(sqlC.capture());
            assertTrue(sqlC.getValue().contains("DESC"));
        }

        @Test
        void picksAscWhenDescFalse() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            gl.zsets.range("leaderboard", "global", 0, 5, false);

            ArgumentCaptor<String> sqlC = ArgumentCaptor.forClass(String.class);
            verify(conn).prepareStatement(sqlC.capture());
            assertTrue(sqlC.getValue().contains("ORDER BY score ASC"));
        }

        @Test
        void inclusiveStopMapsToLimit() throws SQLException {
            // start=0, stop=9 inclusive → limit=10, offset=0.
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            gl.zsets.range("leaderboard", "global", 0, 9, true);

            verify(ps).setString(1, "global");
            verify(ps).setInt(2, 10);
            verify(ps).setInt(3, 0);
        }

        @Test
        void stopMinusOneMapsToLargeLimit() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            // stop=-1 sentinel → "to end" mapped to a large limit (10000).
            gl.zsets.range("leaderboard", "global", 0, -1, true);
            verify(ps).setInt(2, 10000);
        }
    }


    @Nested class RangeByScoreTest {

        @Test
        void inclusiveBoundsBindOrder() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            gl.zsets.rangeByScore("leaderboard", "global", 50, 200, 10, 2);

            // $1=zset_key, $2=min, $3=max, $4=limit, $5=offset.
            verify(ps).setString(1, "global");
            verify(ps).setDouble(2, 50.0);
            verify(ps).setDouble(3, 200.0);
            verify(ps).setInt(4, 10);
            verify(ps).setInt(5, 2);
        }
    }


    @Nested class CardAndRemoveTest {

        @Test
        void cardReturnsZeroForUnknownKey() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);
            assertEquals(0L, gl.zsets.card("leaderboard", "missing"));
        }

        @Test
        void removeReturnsTrueOnRowcountOne() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeUpdate()).thenReturn(1);
            assertTrue(gl.zsets.remove("leaderboard", "global", "alice"));
        }

        @Test
        void removeReturnsFalseOnRowcountZero() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeUpdate()).thenReturn(0);
            assertFalse(gl.zsets.remove("leaderboard", "global", "missing"));
        }
    }
}
