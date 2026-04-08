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
class DocTest {

    @Mock Connection conn;
    @Mock PreparedStatement ps;
    @Mock Statement stmt;
    @Mock ResultSet rs;
    @Mock ResultSetMetaData meta;

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

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
    // parseSortClause (unit tests on the helper)
    // -------------------------------------------------------------------------

    @Nested class ParseSortClauseTest {

        @Test
        void nullReturnsEmpty() {
            assertEquals("", Utils.parseSortClause(null));
        }

        @Test
        void emptyReturnsEmpty() {
            assertEquals("", Utils.parseSortClause(""));
            assertEquals("", Utils.parseSortClause("  "));
        }

        @Test
        void emptyObjectReturnsEmpty() {
            assertEquals("", Utils.parseSortClause("{}"));
        }

        @Test
        void singleAsc() {
            assertEquals("data->>'name' ASC", Utils.parseSortClause("{\"name\": 1}"));
        }

        @Test
        void singleDesc() {
            assertEquals("data->>'age' DESC", Utils.parseSortClause("{\"age\": -1}"));
        }

        @Test
        void multipleKeys() {
            String result = Utils.parseSortClause("{\"name\": 1, \"age\": -1}");
            assertEquals("data->>'name' ASC, data->>'age' DESC", result);
        }

        @Test
        void invalidDirectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.parseSortClause("{\"name\": 2}"));
        }

        @Test
        void invalidKeyThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.parseSortClause("{\"bad key\": 1}"));
        }

        @Test
        void notJsonObjectThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.parseSortClause("[1,2]"));
        }
    }


    // -------------------------------------------------------------------------
    // docInsert
    // -------------------------------------------------------------------------

    @Nested class DocInsertTest {

        @Test
        void sqlAndParams() throws SQLException {
            allowCreateStatement();
            singleRowResultSet("id", "data", "created_at", "updated_at");
            when(rs.getObject(1)).thenReturn(1L);
            when(rs.getObject(2)).thenReturn("{\"name\":\"alice\"}");

            Map<String, Object> result = Utils.docInsert(conn, "users", "{\"name\":\"alice\"}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("INSERT INTO users"));
            assertTrue(sql.contains("VALUES (?::jsonb)"));
            assertTrue(sql.contains("RETURNING id, data, created_at, updated_at"));
            verify(ps).setString(1, "{\"name\":\"alice\"}");
            assertNotNull(result);
            assertEquals(1L, result.get("id"));
        }

        @Test
        void createsTable() throws SQLException {
            allowCreateStatement();
            singleRowResultSet("id", "data", "created_at", "updated_at");
            when(rs.getObject(1)).thenReturn(1L);

            Utils.docInsert(conn, "users", "{\"a\":1}");

            ArgumentCaptor<String> ddlCaptor = ArgumentCaptor.forClass(String.class);
            verify(stmt).execute(ddlCaptor.capture());
            String ddl = ddlCaptor.getValue();
            assertTrue(ddl.contains("CREATE TABLE IF NOT EXISTS users"));
            assertTrue(ddl.contains("data JSONB NOT NULL"));
            assertTrue(ddl.contains("BIGSERIAL PRIMARY KEY"));
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docInsert(conn, "bad table", "{}"));
        }
    }


    // -------------------------------------------------------------------------
    // docInsertMany
    // -------------------------------------------------------------------------

    @Nested class DocInsertManyTest {

        @Test
        void insertsMultiple() throws SQLException {
            allowCreateStatement();
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getMetaData()).thenReturn(meta);
            when(meta.getColumnCount()).thenReturn(4);
            when(meta.getColumnLabel(1)).thenReturn("id");
            when(meta.getColumnLabel(2)).thenReturn("data");
            when(meta.getColumnLabel(3)).thenReturn("created_at");
            when(meta.getColumnLabel(4)).thenReturn("updated_at");
            when(rs.getObject(1)).thenReturn(1L, 2L);

            List<String> docs = Arrays.asList("{\"a\":1}", "{\"b\":2}");
            List<Map<String, Object>> results = Utils.docInsertMany(conn, "items", docs);

            assertEquals(2, results.size());
            verify(ps, times(2)).setString(eq(1), anyString());
        }

        @Test
        void emptyListReturnsEmpty() throws SQLException {
            allowCreateStatement();
            when(conn.prepareStatement(anyString())).thenReturn(ps);

            List<Map<String, Object>> results = Utils.docInsertMany(conn, "items", Collections.emptyList());
            assertTrue(results.isEmpty());
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docInsertMany(conn, "1bad", Collections.emptyList()));
        }
    }


    // -------------------------------------------------------------------------
    // docFind
    // -------------------------------------------------------------------------

    @Nested class DocFindTest {

        @Test
        void sqlWithFilterSortLimitSkip() throws SQLException {
            emptyResultSet("id", "data", "created_at", "updated_at");
            Utils.docFind(conn, "users", "{\"active\":true}", "{\"name\": 1}", 10, 5);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("SELECT id, data, created_at, updated_at FROM users"));
            assertTrue(sql.contains("WHERE data @> ?::jsonb"));
            assertTrue(sql.contains("ORDER BY data->>'name' ASC"));
            assertTrue(sql.contains("LIMIT ?"));
            assertTrue(sql.contains("OFFSET ?"));

            verify(ps).setString(1, "{\"active\":true}");
            verify(ps).setInt(2, 10);
            verify(ps).setInt(3, 5);
        }

        @Test
        void sqlNoFilter() throws SQLException {
            emptyResultSet("id", "data", "created_at", "updated_at");
            Utils.docFind(conn, "users", null, null, null, null);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("SELECT id, data, created_at, updated_at FROM users"));
            assertFalse(sql.contains("WHERE"));
            assertFalse(sql.contains("ORDER BY"));
            assertFalse(sql.contains("LIMIT"));
            assertFalse(sql.contains("OFFSET"));
        }

        @Test
        void sqlFilterOnly() throws SQLException {
            emptyResultSet("id", "data", "created_at", "updated_at");
            Utils.docFind(conn, "users", "{\"x\":1}", null, null, null);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("WHERE data @> ?::jsonb"));
            assertFalse(sql.contains("ORDER BY"));
        }

        @Test
        void returnsRows() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true, true, false);
            when(rs.getMetaData()).thenReturn(meta);
            when(meta.getColumnCount()).thenReturn(2);
            when(meta.getColumnLabel(1)).thenReturn("id");
            when(meta.getColumnLabel(2)).thenReturn("data");
            when(rs.getObject(1)).thenReturn(1L, 2L);
            when(rs.getObject(2)).thenReturn("{\"a\":1}", "{\"b\":2}");

            List<Map<String, Object>> results = Utils.docFind(conn, "users", null, null, null, null);
            assertEquals(2, results.size());
            assertEquals(1L, results.get(0).get("id"));
            assertEquals(2L, results.get(1).get("id"));
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docFind(conn, "bad table", null, null, null, null));
        }
    }


    // -------------------------------------------------------------------------
    // docFindOne
    // -------------------------------------------------------------------------

    @Nested class DocFindOneTest {

        @Test
        void returnsFirstMatch() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true, false);
            when(rs.getMetaData()).thenReturn(meta);
            when(meta.getColumnCount()).thenReturn(2);
            when(meta.getColumnLabel(1)).thenReturn("id");
            when(meta.getColumnLabel(2)).thenReturn("data");
            when(rs.getObject(1)).thenReturn(42L);
            when(rs.getObject(2)).thenReturn("{\"name\":\"bob\"}");

            Map<String, Object> result = Utils.docFindOne(conn, "users", "{\"name\":\"bob\"}");
            assertNotNull(result);
            assertEquals(42L, result.get("id"));
        }

        @Test
        void returnsNullWhenEmpty() throws SQLException {
            emptyResultSet("id", "data", "created_at", "updated_at");
            Map<String, Object> result = Utils.docFindOne(conn, "users", "{\"x\":1}");
            assertNull(result);
        }

        @Test
        void delegatesToDocFind() throws SQLException {
            emptyResultSet("id", "data", "created_at", "updated_at");
            Utils.docFindOne(conn, "users", "{\"a\":1}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("LIMIT ?"));
            verify(ps).setInt(eq(2), eq(1));
        }
    }


    // -------------------------------------------------------------------------
    // docUpdate
    // -------------------------------------------------------------------------

    @Nested class DocUpdateTest {

        @Test
        void sqlAndParams() throws SQLException {
            allowUpdate(3);
            int count = Utils.docUpdate(conn, "users", "{\"active\":true}", "{\"score\":10}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("UPDATE users SET data = data || ?::jsonb"));
            assertTrue(sql.contains("updated_at = NOW()"));
            assertTrue(sql.contains("WHERE data @> ?::jsonb"));
            verify(ps).setString(1, "{\"score\":10}");
            verify(ps).setString(2, "{\"active\":true}");
            assertEquals(3, count);
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docUpdate(conn, "bad table", "{}", "{}"));
        }
    }


    // -------------------------------------------------------------------------
    // docUpdateOne
    // -------------------------------------------------------------------------

    @Nested class DocUpdateOneTest {

        @Test
        void sqlAndParams() throws SQLException {
            allowUpdate(1);
            int count = Utils.docUpdateOne(conn, "users", "{\"name\":\"alice\"}", "{\"age\":30}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("UPDATE users SET data = data || ?::jsonb"));
            assertTrue(sql.contains("updated_at = NOW()"));
            assertTrue(sql.contains("WHERE id = (SELECT id FROM users WHERE data @> ?::jsonb LIMIT 1)"));
            verify(ps).setString(1, "{\"age\":30}");
            verify(ps).setString(2, "{\"name\":\"alice\"}");
            assertEquals(1, count);
        }

        @Test
        void returnsZeroWhenNoMatch() throws SQLException {
            allowUpdate(0);
            int count = Utils.docUpdateOne(conn, "users", "{\"x\":1}", "{\"y\":2}");
            assertEquals(0, count);
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docUpdateOne(conn, "1bad", "{}", "{}"));
        }
    }


    // -------------------------------------------------------------------------
    // docDelete
    // -------------------------------------------------------------------------

    @Nested class DocDeleteTest {

        @Test
        void sqlAndParams() throws SQLException {
            allowUpdate(5);
            int count = Utils.docDelete(conn, "users", "{\"inactive\":true}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("DELETE FROM users WHERE data @> ?::jsonb"));
            verify(ps).setString(1, "{\"inactive\":true}");
            assertEquals(5, count);
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docDelete(conn, "bad table", "{}"));
        }
    }


    // -------------------------------------------------------------------------
    // docDeleteOne
    // -------------------------------------------------------------------------

    @Nested class DocDeleteOneTest {

        @Test
        void sqlAndParams() throws SQLException {
            allowUpdate(1);
            int count = Utils.docDeleteOne(conn, "users", "{\"name\":\"bob\"}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("DELETE FROM users WHERE id = (SELECT id FROM users WHERE data @> ?::jsonb LIMIT 1)"));
            verify(ps).setString(1, "{\"name\":\"bob\"}");
            assertEquals(1, count);
        }

        @Test
        void returnsZeroWhenNoMatch() throws SQLException {
            allowUpdate(0);
            int count = Utils.docDeleteOne(conn, "users", "{\"x\":1}");
            assertEquals(0, count);
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docDeleteOne(conn, "1bad", "{}"));
        }
    }


    // -------------------------------------------------------------------------
    // docCount
    // -------------------------------------------------------------------------

    @Nested class DocCountTest {

        @Test
        void withFilter() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(42L);

            long count = Utils.docCount(conn, "users", "{\"active\":true}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("SELECT COUNT(*) FROM users WHERE data @> ?::jsonb"));
            verify(ps).setString(1, "{\"active\":true}");
            assertEquals(42L, count);
        }

        @Test
        void withoutFilter() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(100L);

            long count = Utils.docCount(conn, "users", null);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertEquals("SELECT COUNT(*) FROM users", sql);
            assertEquals(100L, count);
        }

        @Test
        void emptyFilterCountsAll() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(100L);

            long count = Utils.docCount(conn, "users", "");
            verify(conn).prepareStatement(sqlCaptor.capture());
            assertFalse(sqlCaptor.getValue().contains("WHERE"));
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docCount(conn, "bad table", null));
        }
    }


    // -------------------------------------------------------------------------
    // docAggregate
    // -------------------------------------------------------------------------

    @Nested class DocAggregateTest {

        @Test
        void fullPipeline() throws SQLException {
            emptyResultSet("_id", "total");
            Utils.docAggregate(conn, "orders",
                "[{\"$match\": {\"status\":\"shipped\"}}, " +
                "{\"$group\": {\"_id\": \"$region\", \"total\": {\"$sum\": \"$amount\"}}}, " +
                "{\"$sort\": {\"total\": -1}}, " +
                "{\"$limit\": 10}, " +
                "{\"$skip\": 5}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("SELECT data->>'region' AS _id, SUM((data->>'amount')::numeric) AS total"));
            assertTrue(sql.contains("FROM orders"));
            assertTrue(sql.contains("WHERE data @> ?::jsonb"));
            assertTrue(sql.contains("GROUP BY data->>'region'"));
            assertTrue(sql.contains("ORDER BY total DESC"));
            assertTrue(sql.contains("LIMIT ?"));
            assertTrue(sql.contains("OFFSET ?"));
            verify(ps).setString(1, "{\"status\":\"shipped\"}");
            verify(ps).setInt(2, 10);
            verify(ps).setInt(3, 5);
        }

        @Test
        void accumulators() throws SQLException {
            emptyResultSet("_id", "cnt", "total", "mean", "lo", "hi");
            Utils.docAggregate(conn, "orders",
                "[{\"$group\": {" +
                "\"_id\": \"$category\", " +
                "\"cnt\": {\"$sum\": 1}, " +
                "\"total\": {\"$sum\": \"$price\"}, " +
                "\"mean\": {\"$avg\": \"$price\"}, " +
                "\"lo\": {\"$min\": \"$price\"}, " +
                "\"hi\": {\"$max\": \"$price\"}}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("COUNT(*) AS cnt"));
            assertTrue(sql.contains("SUM((data->>'price')::numeric) AS total"));
            assertTrue(sql.contains("AVG((data->>'price')::numeric) AS mean"));
            assertTrue(sql.contains("MIN((data->>'price')::numeric) AS lo"));
            assertTrue(sql.contains("MAX((data->>'price')::numeric) AS hi"));
            assertTrue(sql.contains("GROUP BY data->>'category'"));
        }

        @Test
        void nullGroupId() throws SQLException {
            emptyResultSet("total");
            Utils.docAggregate(conn, "orders",
                "[{\"$group\": {\"_id\": null, \"total\": {\"$sum\": \"$amount\"}}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("SUM((data->>'amount')::numeric) AS total"));
            assertFalse(sql.contains("GROUP BY"));
            assertFalse(sql.contains("AS _id"));
        }

        @Test
        void matchOnly() throws SQLException {
            emptyResultSet("id", "data", "created_at", "updated_at");
            Utils.docAggregate(conn, "users",
                "[{\"$match\": {\"active\":true}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("SELECT id, data, created_at, updated_at FROM users"));
            assertTrue(sql.contains("WHERE data @> ?::jsonb"));
            assertFalse(sql.contains("GROUP BY"));
            verify(ps).setString(1, "{\"active\":true}");
        }

        @Test
        void sortContextBeforeGroup() throws SQLException {
            emptyResultSet("id", "data", "created_at", "updated_at");
            Utils.docAggregate(conn, "users",
                "[{\"$sort\": {\"name\": 1}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            // Before any $group: sort uses data->>'field'
            assertTrue(sql.contains("ORDER BY data->>'name' ASC"));
        }

        @Test
        void sortContextAfterGroup() throws SQLException {
            emptyResultSet("_id", "cnt");
            Utils.docAggregate(conn, "users",
                "[{\"$group\": {\"_id\": \"$role\", \"cnt\": {\"$sum\": 1}}}, " +
                "{\"$sort\": {\"cnt\": -1}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            // After $group: sort uses alias directly
            assertTrue(sql.contains("ORDER BY cnt DESC"));
            assertFalse(sql.contains("data->>'cnt'"));
        }

        @Test
        void unsupportedStageThrows() {
            assertThrows(IllegalArgumentException.class,
                () -> Utils.docAggregate(conn, "users", "[{\"$lookup\": {}}]"));
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                () -> Utils.docAggregate(conn, "bad table", "[]"));
        }

        @Test
        void nullPipelineThrows() {
            assertThrows(IllegalArgumentException.class,
                () -> Utils.docAggregate(conn, "users", null));
        }

        @Test
        void countAccumulator() throws SQLException {
            emptyResultSet("_id", "n");
            Utils.docAggregate(conn, "events",
                "[{\"$group\": {\"_id\": \"$type\", \"n\": {\"$count\": {}}}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("COUNT(*) AS n"));
        }

        @Test
        void emptyPipeline() throws SQLException {
            emptyResultSet("id", "data", "created_at", "updated_at");
            Utils.docAggregate(conn, "users", "[]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("SELECT id, data, created_at, updated_at FROM users"));
            assertFalse(sql.contains("WHERE"));
            assertFalse(sql.contains("GROUP BY"));
        }
    }


    // -------------------------------------------------------------------------
    // docCreateIndex
    // -------------------------------------------------------------------------

    @Nested class DocCreateIndexTest {

        @Test
        void singleKey() throws SQLException {
            allowCreateStatement();
            Utils.docCreateIndex(conn, "users", Collections.singletonList("name"));

            ArgumentCaptor<String> ddlCaptor = ArgumentCaptor.forClass(String.class);
            verify(stmt).execute(ddlCaptor.capture());
            String ddl = ddlCaptor.getValue();
            assertTrue(ddl.contains("CREATE INDEX IF NOT EXISTS idx_users_name"));
            assertTrue(ddl.contains("ON users ((data->>'name'))"));
        }

        @Test
        void multipleKeys() throws SQLException {
            allowCreateStatement();
            Utils.docCreateIndex(conn, "users", Arrays.asList("name", "age"));

            ArgumentCaptor<String> ddlCaptor = ArgumentCaptor.forClass(String.class);
            verify(stmt).execute(ddlCaptor.capture());
            String ddl = ddlCaptor.getValue();
            assertTrue(ddl.contains("CREATE INDEX IF NOT EXISTS idx_users_name_age"));
            assertTrue(ddl.contains("ON users ((data->>'name'), (data->>'age'))"));
        }

        @Test
        void emptyKeysThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docCreateIndex(conn, "users", Collections.emptyList()));
        }

        @Test
        void nullKeysThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docCreateIndex(conn, "users", null));
        }

        @Test
        void invalidKeyThrows() throws SQLException {
            allowCreateStatement();
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docCreateIndex(conn, "users", Collections.singletonList("bad key")));
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docCreateIndex(conn, "bad table", Collections.singletonList("name")));
        }
    }
}
