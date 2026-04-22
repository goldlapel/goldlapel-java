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

import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class InstanceMethodsTest {

    @Mock Connection conn;
    @Mock PreparedStatement ps;
    @Mock Statement stmt;
    @Mock ResultSet rs;
    @Mock ResultSetMetaData meta;

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    GoldLapel gl;

    @BeforeEach
    void setUp() {
        gl = new GoldLapel("postgresql://user:pass@host:5432/db", new GoldLapelOptions());
        // Inject mock connection via reflection
        try {
            java.lang.reflect.Field f = GoldLapel.class.getDeclaredField("internalConn");
            f.setAccessible(true);
            f.set(gl, conn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void emptyResultSet(String... columnNames) throws SQLException {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(false);
        when(rs.getMetaData()).thenReturn(meta);
        when(meta.getColumnCount()).thenReturn(columnNames.length);
        for (int i = 0; i < columnNames.length; i++) {
            when(meta.getColumnLabel(i + 1)).thenReturn(columnNames[i]);
        }
    }

    void singleRowResultSet(String... columnNames) throws SQLException {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getMetaData()).thenReturn(meta);
        when(meta.getColumnCount()).thenReturn(columnNames.length);
        for (int i = 0; i < columnNames.length; i++) {
            when(meta.getColumnLabel(i + 1)).thenReturn(columnNames[i]);
        }
    }

    void allowCreateStatement() throws SQLException {
        when(conn.createStatement()).thenReturn(stmt);
    }

    void allowUpdate(int count) throws SQLException {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeUpdate()).thenReturn(count);
    }


    // -------------------------------------------------------------------------
    // connection() getter
    // -------------------------------------------------------------------------

    @Nested class ConnectionGetterTest {

        @Test
        void returnsInjectedConnection() {
            assertSame(conn, gl.connection());
        }

        @Test
        void throwsWhenNoConnection() {
            GoldLapel gl2 = new GoldLapel("postgresql://localhost:5432/db", new GoldLapelOptions());
            IllegalStateException ex = assertThrows(IllegalStateException.class, gl2::connection);
            assertTrue(ex.getMessage().contains("No connection available"));
            assertTrue(ex.getMessage().contains("GoldLapel.start()"));
        }
    }


    // -------------------------------------------------------------------------
    // docInsert — representative doc method
    // -------------------------------------------------------------------------

    @Nested class DocInsertInstanceTest {

        @Test
        void delegatesToUtils() throws SQLException {
            allowCreateStatement();
            singleRowResultSet("_id", "data", "created_at", "updated_at");
            when(rs.getObject(1)).thenReturn("uuid-1");
            when(rs.getObject(2)).thenReturn("{\"name\":\"alice\"}");

            Map<String, Object> result = gl.docInsert("users", "{\"name\":\"alice\"}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("INSERT INTO users"));
            assertTrue(sql.contains("VALUES (?::jsonb)"));
            assertNotNull(result);
            assertEquals("uuid-1", result.get("_id"));
        }
    }


    // -------------------------------------------------------------------------
    // docFind — representative query method
    // -------------------------------------------------------------------------

    @Nested class DocFindInstanceTest {

        @Test
        void delegatesToUtils() throws SQLException {
            emptyResultSet("_id", "data", "created_at", "updated_at");
            List<Map<String, Object>> results = gl.docFind("users", "{\"active\":true}", null, 10, null);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("SELECT _id, data, created_at, updated_at FROM users"));
            assertTrue(sql.contains("WHERE data @> ?::jsonb"));
            assertTrue(sql.contains("LIMIT ?"));
        }
    }


    // -------------------------------------------------------------------------
    // docUpdate — representative mutation method
    // -------------------------------------------------------------------------

    @Nested class DocUpdateInstanceTest {

        @Test
        void delegatesToUtils() throws SQLException {
            allowUpdate(3);
            int count = gl.docUpdate("users", "{\"active\":true}", "{\"score\":10}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("UPDATE users SET data = data || ?::jsonb"));
            assertEquals(3, count);
        }
    }


    // -------------------------------------------------------------------------
    // search — representative search method
    // -------------------------------------------------------------------------

    @Nested class SearchInstanceTest {

        @Test
        void singleColumnDelegates() throws SQLException {
            emptyResultSet("_score");
            gl.search("articles", "title", "postgres", 10, "english", false);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("to_tsvector(?, coalesce(title, ''))"));
            assertTrue(sql.contains("FROM articles"));
        }

        @Test
        void multiColumnDelegates() throws SQLException {
            emptyResultSet("_score", "_highlight");
            gl.search("articles", new String[]{"title", "body"}, "search", 5, "english", true);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("coalesce(title, '') || ' ' || coalesce(body, '')"));
            assertTrue(sql.contains("ts_headline"));
        }
    }


    // -------------------------------------------------------------------------
    // incr — representative counter method
    // -------------------------------------------------------------------------

    @Nested class IncrInstanceTest {

        @Test
        void delegatesToUtils() throws SQLException {
            allowCreateStatement();
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(5L);

            long val = gl.incr("counters", "page_views", 1);

            assertEquals(5L, val);
        }
    }


    // -------------------------------------------------------------------------
    // publish — representative pubsub method
    // -------------------------------------------------------------------------

    @Nested class PublishInstanceTest {

        @Test
        void delegatesToUtils() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            gl.publish("events", "{\"type\":\"click\"}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("pg_notify"));
            verify(ps).setString(1, "events");
            verify(ps).setString(2, "{\"type\":\"click\"}");
        }
    }


    // -------------------------------------------------------------------------
    // hset / hget — representative hash methods
    // -------------------------------------------------------------------------

    @Nested class HashInstanceTest {

        @Test
        void hsetDelegates() throws SQLException {
            allowCreateStatement();
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeUpdate()).thenReturn(1);

            gl.hset("hashes", "user:1", "name", "\"alice\"");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("INSERT INTO hashes"));
            assertTrue(sql.contains("jsonb_build_object"));
        }

        @Test
        void hgetDelegates() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getString(1)).thenReturn("\"alice\"");

            String val = gl.hget("hashes", "user:1", "name");

            assertEquals("\"alice\"", val);
        }
    }


    // -------------------------------------------------------------------------
    // zadd / zrange — representative sorted set methods
    // -------------------------------------------------------------------------

    @Nested class SortedSetInstanceTest {

        @Test
        void zaddDelegates() throws SQLException {
            allowCreateStatement();
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeUpdate()).thenReturn(1);

            gl.zadd("leaderboard", "alice", 100.0);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("INSERT INTO leaderboard"));
            verify(ps).setString(1, "alice");
            verify(ps).setDouble(2, 100.0);
        }

        @Test
        void zrangeDelegates() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            List<Map.Entry<String, Double>> results = gl.zrange("leaderboard", 0, 10, true);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("SELECT member, score FROM leaderboard"));
            assertTrue(sql.contains("ORDER BY score DESC"));
            assertTrue(results.isEmpty());
        }
    }


    // -------------------------------------------------------------------------
    // enqueue / dequeue — representative queue methods
    // -------------------------------------------------------------------------

    @Nested class QueueInstanceTest {

        @Test
        void enqueueDelegates() throws SQLException {
            allowCreateStatement();
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeUpdate()).thenReturn(1);

            gl.enqueue("jobs", "{\"task\":\"send_email\"}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("INSERT INTO jobs"));
            verify(ps).setString(1, "{\"task\":\"send_email\"}");
        }

        @Test
        void dequeueDelegates() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getString(1)).thenReturn("{\"task\":\"send_email\"}");

            String payload = gl.dequeue("jobs");

            assertEquals("{\"task\":\"send_email\"}", payload);
        }
    }


    // -------------------------------------------------------------------------
    // analyze — representative analysis method
    // -------------------------------------------------------------------------

    @Nested class AnalyzeInstanceTest {

        @Test
        void delegatesToUtils() throws SQLException {
            emptyResultSet("alias", "description", "token");
            List<Map<String, Object>> results = gl.analyze("hello world");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("ts_debug"));
            verify(ps).setString(1, "english");
            verify(ps).setString(2, "hello world");
        }

        @Test
        void withLangDelegates() throws SQLException {
            emptyResultSet("alias", "description", "token");
            gl.analyze("hola mundo", "spanish");

            verify(ps).setString(1, "spanish");
            verify(ps).setString(2, "hola mundo");
        }

        @Test
        void withExplicitConnUsesDefaultLang() throws SQLException {
            // New overload: analyze(text, conn) should use the default lang
            // ("english") and run against the caller-supplied connection —
            // not the GoldLapel's internal/resolved connection.
            Connection explicitConn = mock(Connection.class);
            PreparedStatement explicitPs = mock(PreparedStatement.class);
            ResultSet explicitRs = mock(ResultSet.class);
            ResultSetMetaData explicitMeta = mock(ResultSetMetaData.class);

            when(explicitConn.prepareStatement(anyString())).thenReturn(explicitPs);
            when(explicitPs.executeQuery()).thenReturn(explicitRs);
            when(explicitRs.next()).thenReturn(false);
            when(explicitRs.getMetaData()).thenReturn(explicitMeta);
            when(explicitMeta.getColumnCount()).thenReturn(0);

            gl.analyze("hello world", explicitConn);

            // The explicit connection got the prepareStatement call, NOT the
            // internal one.
            verify(explicitConn).prepareStatement(anyString());
            verify(conn, never()).prepareStatement(anyString());
            verify(explicitPs).setString(1, "english");
            verify(explicitPs).setString(2, "hello world");
        }
    }


    // -------------------------------------------------------------------------
    // All instance methods throw when no connection
    // -------------------------------------------------------------------------

    @Nested class NoConnectionTest {

        GoldLapel bare;

        @BeforeEach
        void setUp() {
            bare = new GoldLapel("postgresql://localhost:5432/db", new GoldLapelOptions());
        }

        @Test
        void docInsertThrows() {
            assertThrows(IllegalStateException.class,
                () -> bare.docInsert("col", "{}"));
        }

        @Test
        void searchThrows() {
            assertThrows(IllegalStateException.class,
                () -> bare.search("tbl", "col", "q", 10, "english", false));
        }

        @Test
        void publishThrows() {
            assertThrows(IllegalStateException.class,
                () -> bare.publish("ch", "msg"));
        }

        @Test
        void incrThrows() {
            assertThrows(IllegalStateException.class,
                () -> bare.incr("tbl", "k", 1));
        }

        @Test
        void zaddThrows() {
            assertThrows(IllegalStateException.class,
                () -> bare.zadd("tbl", "m", 1.0));
        }

        @Test
        void analyzeThrows() {
            assertThrows(IllegalStateException.class,
                () -> bare.analyze("text"));
        }

        @Test
        void streamAddThrows() {
            // streamAdd now fetches DDL patterns first — the failure mode for
            // a non-started instance is RuntimeException ("dashboard not
            // reachable" / "no dashboard token"), not IllegalStateException.
            assertThrows(RuntimeException.class,
                () -> bare.streamAdd("stream", "{}"));
        }

        @Test
        void percolateAddThrows() {
            assertThrows(IllegalStateException.class,
                () -> bare.percolateAdd("idx", "q1", "query text"));
        }

        @Test
        void scriptThrows() {
            assertThrows(IllegalStateException.class,
                () -> bare.script("return 1"));
        }

        @Test
        void geodistThrows() {
            assertThrows(IllegalStateException.class,
                () -> bare.geodist("tbl", "geom", "name", "a", "b"));
        }
    }
}
