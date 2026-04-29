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

    /**
     * Pre-populate the per-instance DDL cache so {@code gl.documents.<verb>}
     * / {@code gl.streams.<verb>} skip the proxy round-trip in unit tests.
     * Tests use the user-supplied collection name as the canonical table —
     * matches how DocTest's {@code P(...)} helper builds patterns.
     */
    void seedDdlCache(String family, String name) {
        Map<String, Object> tables = new java.util.LinkedHashMap<>();
        tables.put("main", name);
        Map<String, Object> entry = new java.util.LinkedHashMap<>();
        entry.put("tables", tables);
        entry.put("query_patterns", Collections.emptyMap());
        gl.ddlCache().put(family + ":" + name, entry);
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
            seedDdlCache("doc_store", "users");
            allowCreateStatement();
            singleRowResultSet("_id", "data", "created_at", "updated_at");
            when(rs.getObject(1)).thenReturn("uuid-1");
            when(rs.getObject(2)).thenReturn("{\"name\":\"alice\"}");

            Map<String, Object> result = gl.documents.insert("users", "{\"name\":\"alice\"}");

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
            seedDdlCache("doc_store", "users");
            emptyResultSet("_id", "data", "created_at", "updated_at");
            List<Map<String, Object>> results = gl.documents.find("users", "{\"active\":true}", null, 10, null);

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
            seedDdlCache("doc_store", "users");
            allowUpdate(3);
            int count = gl.documents.update("users", "{\"active\":true}", "{\"score\":10}");

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
    // gl.counters.<verb> — representative counter ops via Phase 5 namespace
    // -------------------------------------------------------------------------

    /**
     * Seed query patterns into the per-instance DDL cache so the family
     * sub-API skips the proxy round-trip in unit tests. Patterns map keys
     * are the canonical {@code $N}-placeholder SQL the proxy would emit;
     * the wrapper rewrites {@code $N → ?} before binding.
     */
    void seedFamilyCache(String family, String name, Map<String, String> queryPatterns) {
        Map<String, Object> tables = new java.util.LinkedHashMap<>();
        tables.put("main", name);
        Map<String, Object> entry = new java.util.LinkedHashMap<>();
        entry.put("tables", tables);
        entry.put("query_patterns", queryPatterns);
        gl.ddlCache().put(family + ":" + name, entry);
    }

    @Nested class CountersNamespaceTest {

        @Test
        void incrDispatchesViaProxyPattern() throws SQLException {
            Map<String, String> patterns = new java.util.LinkedHashMap<>();
            patterns.put("incr",
                "INSERT INTO _goldlapel.counter_pageviews (key, value, updated_at) "
                + "VALUES ($1, $2, NOW()) ON CONFLICT (key) DO UPDATE "
                + "SET value = _goldlapel.counter_pageviews.value + EXCLUDED.value, "
                + "updated_at = NOW() RETURNING value");
            seedFamilyCache("counter", "pageviews", patterns);

            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(7L);

            long val = gl.counters.incr("pageviews", "home", 5);

            assertEquals(7L, val);
            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            // The proxy stamps updated_at — wrappers don't paper over this.
            assertTrue(sql.contains("updated_at = NOW()"));
            verify(ps).setString(1, "home");
            verify(ps).setLong(2, 5L);
        }
    }


    // -------------------------------------------------------------------------
    // publish — representative pubsub method (still flat — Phase 5 left
    // pubsub alone; counter/zset/hash/queue/geo were the migration targets)
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
    // gl.hashes.<verb> — representative hash ops via Phase 5 namespace
    // -------------------------------------------------------------------------

    @Nested class HashesNamespaceTest {

        @Test
        void setDispatchesRowPerFieldUpsert() throws SQLException {
            Map<String, String> patterns = new java.util.LinkedHashMap<>();
            patterns.put("hset",
                "INSERT INTO _goldlapel.hash_sessions (hash_key, field, value) "
                + "VALUES ($1, $2, $3::jsonb) ON CONFLICT (hash_key, field) "
                + "DO UPDATE SET value = EXCLUDED.value RETURNING value");
            seedFamilyCache("hash", "sessions", patterns);

            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getString(1)).thenReturn("\"alice\"");

            String result = gl.hashes.set("sessions", "user:1", "name", "\"alice\"");

            assertEquals("\"alice\"", result);
            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            // Phase 5 storage: row-per-field, NOT a JSONB-blob merge.
            assertTrue(sql.contains("(hash_key, field, value)"));
            assertTrue(sql.contains("ON CONFLICT (hash_key, field)"));
            assertFalse(sql.contains("jsonb_build_object"));
            verify(ps).setString(1, "user:1");
            verify(ps).setString(2, "name");
            verify(ps).setString(3, "\"alice\"");
        }

        @Test
        void getAllRebuildsMapClientSide() throws SQLException {
            Map<String, String> patterns = new java.util.LinkedHashMap<>();
            patterns.put("hgetall",
                "SELECT field, value FROM _goldlapel.hash_sessions "
                + "WHERE hash_key = $1 ORDER BY field");
            seedFamilyCache("hash", "sessions", patterns);

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
    }


    // -------------------------------------------------------------------------
    // gl.zsets.<verb> — representative sorted-set ops via Phase 5 namespace
    // -------------------------------------------------------------------------

    @Nested class ZsetsNamespaceTest {

        @Test
        void addThreadsZsetKeyFirst() throws SQLException {
            Map<String, String> patterns = new java.util.LinkedHashMap<>();
            patterns.put("zadd",
                "INSERT INTO _goldlapel.zset_leaderboard (zset_key, member, score) "
                + "VALUES ($1, $2, $3) ON CONFLICT (zset_key, member) "
                + "DO UPDATE SET score = EXCLUDED.score RETURNING score");
            seedFamilyCache("zset", "leaderboard", patterns);

            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getDouble(1)).thenReturn(100.0);

            double val = gl.zsets.add("leaderboard", "global", "alice", 100.0);

            assertEquals(100.0, val);
            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("(zset_key, member, score)"));
            // Bind order matches proxy's $1, $2, $3 → (zset_key, member, score).
            verify(ps).setString(1, "global");
            verify(ps).setString(2, "alice");
            verify(ps).setDouble(3, 100.0);
        }

        @Test
        void rangeMapsInclusiveStopToLimit() throws SQLException {
            Map<String, String> patterns = new java.util.LinkedHashMap<>();
            patterns.put("zrange_desc",
                "SELECT member, score FROM _goldlapel.zset_leaderboard "
                + "WHERE zset_key = $1 ORDER BY score DESC, member DESC "
                + "LIMIT $2 OFFSET $3");
            seedFamilyCache("zset", "leaderboard", patterns);

            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            List<Map.Entry<String, Double>> results =
                gl.zsets.range("leaderboard", "global", 0, 9, true);

            assertTrue(results.isEmpty());
            // start=0, stop=9 inclusive → limit=10, offset=0.
            verify(ps).setString(1, "global");
            verify(ps).setInt(2, 10);
            verify(ps).setInt(3, 0);
        }
    }


    // -------------------------------------------------------------------------
    // gl.queues.<verb> — Phase 5: claim/ack at-least-once. NO dequeue alias.
    // -------------------------------------------------------------------------

    @Nested class QueuesNamespaceTest {

        @Test
        void enqueueReturnsAssignedId() throws SQLException {
            Map<String, String> patterns = new java.util.LinkedHashMap<>();
            patterns.put("enqueue",
                "INSERT INTO _goldlapel.queue_jobs (payload) VALUES ($1::jsonb) "
                + "RETURNING id, created_at");
            seedFamilyCache("queue", "jobs", patterns);

            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(99L);

            long id = gl.queues.enqueue("jobs", "{\"task\":\"x\"}");

            assertEquals(99L, id);
            verify(ps).setString(1, "{\"task\":\"x\"}");
        }

        @Test
        void claimReturnsClaimedMessageOrNull() throws SQLException {
            Map<String, String> patterns = new java.util.LinkedHashMap<>();
            patterns.put("claim",
                "WITH next_msg AS (SELECT id FROM _goldlapel.queue_jobs "
                + "WHERE status = 'ready' AND visible_at <= NOW() "
                + "ORDER BY visible_at, id FOR UPDATE SKIP LOCKED LIMIT 1) "
                + "UPDATE _goldlapel.queue_jobs SET status = 'claimed', "
                + "visible_at = NOW() + INTERVAL '1 millisecond' * $1 "
                + "FROM next_msg WHERE _goldlapel.queue_jobs.id = next_msg.id "
                + "RETURNING _goldlapel.queue_jobs.id, _goldlapel.queue_jobs.payload, "
                + "_goldlapel.queue_jobs.visible_at, _goldlapel.queue_jobs.created_at");
            seedFamilyCache("queue", "jobs", patterns);

            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(7L);
            when(rs.getString(2)).thenReturn("{\"task\":\"send\"}");

            Utils.ClaimedMessage msg = gl.queues.claim("jobs", 30000L);

            assertNotNull(msg);
            assertEquals(7L, msg.id());
            assertEquals("{\"task\":\"send\"}", msg.payload());
            verify(ps).setLong(1, 30000L);
        }

        @Test
        void noDequeueShim() {
            // Phase 5 contract: there is no dequeue compat alias on the
            // queues namespace. Ack must be explicit.
            assertFalse(java.util.Arrays.stream(QueuesApi.class.getMethods())
                    .anyMatch(m -> m.getName().equals("dequeue")),
                "Phase 5 forbids a dequeue alias — claim+ack is explicit by design.");
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
            // documents.insert now fetches DDL patterns first — the failure
            // mode for a non-started instance is RuntimeException ("dashboard
            // not reachable" / "no dashboard token"), not IllegalStateException.
            assertThrows(RuntimeException.class,
                () -> bare.documents.insert("col", "{}"));
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
        void countersIncrThrows() {
            // Phase 5 namespaces fetch DDL patterns first — failure mode for
            // a non-started instance is RuntimeException ("dashboard not
            // reachable" / "no dashboard token"), not IllegalStateException.
            assertThrows(RuntimeException.class,
                () -> bare.counters.incr("ns", "k", 1));
        }

        @Test
        void zsetsAddThrows() {
            assertThrows(RuntimeException.class,
                () -> bare.zsets.add("ns", "z", "m", 1.0));
        }

        @Test
        void hashesSetThrows() {
            assertThrows(RuntimeException.class,
                () -> bare.hashes.set("ns", "k", "f", "\"v\""));
        }

        @Test
        void queuesEnqueueThrows() {
            assertThrows(RuntimeException.class,
                () -> bare.queues.enqueue("jobs", "{}"));
        }

        @Test
        void geosAddThrows() {
            assertThrows(RuntimeException.class,
                () -> bare.geos.add("ns", "alice", 0.0, 0.0));
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
                () -> bare.streams.add("stream", "{}"));
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
    }
}
