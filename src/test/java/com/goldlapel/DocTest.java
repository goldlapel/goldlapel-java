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

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

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
                () -> Utils.docAggregate(conn, "users", "[{\"$bucket\": {}}]"));
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

        @Test
        void compositeGroupId() throws SQLException {
            emptyResultSet("_id", "total");
            Utils.docAggregate(conn, "orders",
                "[{\"$group\": {\"_id\": {\"region\": \"$region\", \"year\": \"$year\"}, " +
                "\"total\": {\"$sum\": \"$amount\"}}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("json_build_object('region', data->>'region', 'year', data->>'year') AS _id"));
            assertTrue(sql.contains("SUM((data->>'amount')::numeric) AS total"));
            assertTrue(sql.contains("GROUP BY data->>'region', data->>'year'"));
        }

        @Test
        void compositeGroupIdWithSort() throws SQLException {
            emptyResultSet("_id", "cnt");
            Utils.docAggregate(conn, "orders",
                "[{\"$group\": {\"_id\": {\"status\": \"$status\", \"region\": \"$region\"}, " +
                "\"cnt\": {\"$sum\": 1}}}, " +
                "{\"$sort\": {\"cnt\": -1}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("json_build_object('status', data->>'status', 'region', data->>'region') AS _id"));
            assertTrue(sql.contains("COUNT(*) AS cnt"));
            assertTrue(sql.contains("GROUP BY data->>'status', data->>'region'"));
            assertTrue(sql.contains("ORDER BY cnt DESC"));
        }

        @Test
        void pushAccumulator() throws SQLException {
            emptyResultSet("_id", "names");
            Utils.docAggregate(conn, "users",
                "[{\"$group\": {\"_id\": \"$role\", \"names\": {\"$push\": \"$name\"}}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("array_agg(data->>'name') AS names"));
            assertTrue(sql.contains("GROUP BY data->>'role'"));
        }

        @Test
        void addToSetAccumulator() throws SQLException {
            emptyResultSet("_id", "cities");
            Utils.docAggregate(conn, "users",
                "[{\"$group\": {\"_id\": \"$country\", \"cities\": {\"$addToSet\": \"$city\"}}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("array_agg(DISTINCT data->>'city') AS cities"));
            assertTrue(sql.contains("GROUP BY data->>'country'"));
        }

        @Test
        void compositeGroupIdWithMatch() throws SQLException {
            emptyResultSet("_id", "total");
            Utils.docAggregate(conn, "orders",
                "[{\"$match\": {\"active\":true}}, " +
                "{\"$group\": {\"_id\": {\"dept\": \"$dept\", \"role\": \"$role\"}, " +
                "\"total\": {\"$sum\": \"$salary\"}}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("json_build_object('dept', data->>'dept', 'role', data->>'role') AS _id"));
            assertTrue(sql.contains("WHERE data @> ?::jsonb"));
            assertTrue(sql.contains("GROUP BY data->>'dept', data->>'role'"));
            verify(ps).setString(1, "{\"active\":true}");
        }

        @Test
        void pushWithCompositeId() throws SQLException {
            emptyResultSet("_id", "items");
            Utils.docAggregate(conn, "orders",
                "[{\"$group\": {\"_id\": {\"store\": \"$store\", \"day\": \"$day\"}, " +
                "\"items\": {\"$push\": \"$product\"}}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("json_build_object('store', data->>'store', 'day', data->>'day') AS _id"));
            assertTrue(sql.contains("array_agg(data->>'product') AS items"));
            assertTrue(sql.contains("GROUP BY data->>'store', data->>'day'"));
        }

        @Test
        void addToSetWithNullGroupId() throws SQLException {
            emptyResultSet("tags");
            Utils.docAggregate(conn, "posts",
                "[{\"$group\": {\"_id\": null, \"tags\": {\"$addToSet\": \"$tag\"}}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("array_agg(DISTINCT data->>'tag') AS tags"));
            assertFalse(sql.contains("GROUP BY"));
            assertFalse(sql.contains("AS _id"));
        }

        // ----- $project tests -----

        @Test
        void projectInclude() throws SQLException {
            emptyResultSet("name", "age");
            Utils.docAggregate(conn, "users",
                "[{\"$project\": {\"name\": 1, \"age\": 1}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("data->>'name' AS name"));
            assertTrue(sql.contains("data->>'age' AS age"));
            assertTrue(sql.contains("FROM users"));
        }

        @Test
        void projectExclude() throws SQLException {
            emptyResultSet("name");
            Utils.docAggregate(conn, "users",
                "[{\"$project\": {\"name\": 1, \"_id\": 0}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("data->>'name' AS name"));
            assertFalse(sql.contains("AS _id"));
        }

        @Test
        void projectRename() throws SQLException {
            emptyResultSet("fullName");
            Utils.docAggregate(conn, "users",
                "[{\"$project\": {\"fullName\": \"$name\"}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("data->>'name' AS fullName"));
        }

        @Test
        void projectDotNotation() throws SQLException {
            emptyResultSet("city");
            Utils.docAggregate(conn, "users",
                "[{\"$project\": {\"city\": \"$address.city\"}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("data->'address'->>'city' AS city"));
        }

        // ----- $unwind tests -----

        @Test
        void unwindString() throws SQLException {
            emptyResultSet("id", "data", "created_at", "updated_at");
            Utils.docAggregate(conn, "orders",
                "[{\"$unwind\": \"$items\"}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("CROSS JOIN LATERAL jsonb_array_elements_text(data->'items') AS _u_items(val)"));
        }

        @Test
        void unwindObject() throws SQLException {
            emptyResultSet("id", "data", "created_at", "updated_at");
            Utils.docAggregate(conn, "orders",
                "[{\"$unwind\": {\"path\": \"$tags\"}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("CROSS JOIN LATERAL jsonb_array_elements_text(data->'tags') AS _u_tags(val)"));
        }

        @Test
        void unwindThenGroup() throws SQLException {
            emptyResultSet("_id", "cnt");
            Utils.docAggregate(conn, "orders",
                "[{\"$unwind\": \"$items\"}, " +
                "{\"$group\": {\"_id\": \"$items\", \"cnt\": {\"$sum\": 1}}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("CROSS JOIN LATERAL jsonb_array_elements_text(data->'items') AS _u_items(val)"));
            assertTrue(sql.contains("_u_items.val AS _id"));
            assertTrue(sql.contains("GROUP BY _u_items.val"));
            assertTrue(sql.contains("COUNT(*) AS cnt"));
        }

        @Test
        void unwindThenGroupSum() throws SQLException {
            emptyResultSet("_id", "total");
            Utils.docAggregate(conn, "orders",
                "[{\"$unwind\": \"$scores\"}, " +
                "{\"$group\": {\"_id\": \"$category\", \"total\": {\"$sum\": \"$scores\"}}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("CROSS JOIN LATERAL jsonb_array_elements_text(data->'scores') AS _u_scores(val)"));
            assertTrue(sql.contains("data->>'category' AS _id"));
            assertTrue(sql.contains("SUM((_u_scores.val)::numeric) AS total"));
            assertTrue(sql.contains("GROUP BY data->>'category'"));
        }

        // ----- $lookup tests -----

        @Test
        void lookupBasic() throws SQLException {
            emptyResultSet("id", "data", "created_at", "updated_at", "items");
            Utils.docAggregate(conn, "orders",
                "[{\"$lookup\": {\"from\": \"products\", \"localField\": \"productId\", " +
                "\"foreignField\": \"pid\", \"as\": \"items\"}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("COALESCE((SELECT json_agg(products.data) FROM products"));
            assertTrue(sql.contains("WHERE products.data->>'pid' = orders.data->>'productId'"));
            assertTrue(sql.contains("), '[]'::json) AS items"));
            assertTrue(sql.contains("FROM orders"));
        }

        @Test
        void lookupWithMatch() throws SQLException {
            emptyResultSet("id", "data", "created_at", "updated_at", "details");
            Utils.docAggregate(conn, "orders",
                "[{\"$match\": {\"status\": \"active\"}}, " +
                "{\"$lookup\": {\"from\": \"inventory\", \"localField\": \"sku\", " +
                "\"foreignField\": \"sku\", \"as\": \"details\"}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("COALESCE((SELECT json_agg(inventory.data) FROM inventory"));
            assertTrue(sql.contains("WHERE inventory.data->>'sku' = orders.data->>'sku'"));
            assertTrue(sql.contains("), '[]'::json) AS details"));
            assertTrue(sql.contains("WHERE data @> ?::jsonb"));
            verify(ps).setString(1, "{\"status\": \"active\"}");
        }

        @Test
        void lookupMissingFieldThrows() {
            assertThrows(IllegalArgumentException.class,
                () -> Utils.docAggregate(conn, "orders",
                    "[{\"$lookup\": {\"from\": \"products\", \"localField\": \"pid\"}}]"));
        }

        @Test
        void lookupWithProjection() throws SQLException {
            emptyResultSet("orderId", "matched");
            Utils.docAggregate(conn, "orders",
                "[{\"$lookup\": {\"from\": \"items\", \"localField\": \"itemId\", " +
                "\"foreignField\": \"iid\", \"as\": \"matched\"}}, " +
                "{\"$project\": {\"orderId\": \"$oid\", \"matched\": 1}}]");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("data->>'oid' AS orderId"));
            assertTrue(sql.contains("COALESCE((SELECT json_agg(items.data) FROM items"));
            assertTrue(sql.contains("WHERE items.data->>'iid' = orders.data->>'itemId'"));
            assertTrue(sql.contains("), '[]'::json) AS matched"));
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


    // -------------------------------------------------------------------------
    // fieldPath (unit tests on the helper)
    // -------------------------------------------------------------------------

    @Nested class FieldPathTest {

        @Test
        void simpleKey() {
            assertEquals("data->>'name'", Utils.fieldPath("name"));
        }

        @Test
        void nestedKey() {
            assertEquals("data->'address'->>'city'", Utils.fieldPath("address.city"));
        }

        @Test
        void deeplyNested() {
            assertEquals("data->'a'->'b'->>'c'", Utils.fieldPath("a.b.c"));
        }

        @Test
        void invalidKeyThrows() {
            assertThrows(IllegalArgumentException.class, () -> Utils.fieldPath("bad key"));
        }

        @Test
        void invalidPartThrows() {
            assertThrows(IllegalArgumentException.class, () -> Utils.fieldPath("ok.bad key"));
        }
    }


    // -------------------------------------------------------------------------
    // buildFilter (unit tests on comparison operators)
    // -------------------------------------------------------------------------

    @Nested class BuildFilterTest {

        @Test
        void plainContainmentPassthrough() {
            Utils.FilterResult r = Utils.buildFilter("{\"active\":true}");
            assertEquals("data @> ?::jsonb", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("{\"active\":true}", r.params.get(0));
        }

        @Test
        void gtNumeric() {
            Utils.FilterResult r = Utils.buildFilter("{\"age\": {\"$gt\": 21}}");
            assertEquals("(data->>'age')::numeric > ?", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals(21.0, r.params.get(0));
        }

        @Test
        void gteNumeric() {
            Utils.FilterResult r = Utils.buildFilter("{\"score\": {\"$gte\": 90}}");
            assertEquals("(data->>'score')::numeric >= ?", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals(90.0, r.params.get(0));
        }

        @Test
        void ltAndLte() {
            Utils.FilterResult r = Utils.buildFilter("{\"price\": {\"$lt\": 100, \"$gte\": 10}}");
            assertEquals("(data->>'price')::numeric < ? AND (data->>'price')::numeric >= ?", r.whereClause);
            assertEquals(2, r.params.size());
            assertEquals(100.0, r.params.get(0));
            assertEquals(10.0, r.params.get(1));
        }

        @Test
        void eqString() {
            Utils.FilterResult r = Utils.buildFilter("{\"status\": {\"$eq\": \"active\"}}");
            assertEquals("data->>'status' = ?", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("active", r.params.get(0));
        }

        @Test
        void neOperator() {
            Utils.FilterResult r = Utils.buildFilter("{\"status\": {\"$ne\": \"deleted\"}}");
            assertEquals("data->>'status' != ?", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("deleted", r.params.get(0));
        }

        @Test
        void inOperator() {
            Utils.FilterResult r = Utils.buildFilter("{\"color\": {\"$in\": [\"red\", \"blue\"]}}");
            assertEquals("data->>'color' IN (?, ?)", r.whereClause);
            assertEquals(2, r.params.size());
            assertEquals("red", r.params.get(0));
            assertEquals("blue", r.params.get(1));
        }

        @Test
        void ninOperator() {
            Utils.FilterResult r = Utils.buildFilter("{\"color\": {\"$nin\": [\"red\"]}}");
            assertEquals("data->>'color' NOT IN (?)", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("red", r.params.get(0));
        }

        @Test
        void existsTrue() {
            Utils.FilterResult r = Utils.buildFilter("{\"email\": {\"$exists\": true}}");
            assertEquals("data ?? ?", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("email", r.params.get(0));
        }

        @Test
        void existsFalse() {
            Utils.FilterResult r = Utils.buildFilter("{\"email\": {\"$exists\": false}}");
            assertEquals("NOT (data ?? ?)", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("email", r.params.get(0));
        }

        @Test
        void regexOperator() {
            Utils.FilterResult r = Utils.buildFilter("{\"name\": {\"$regex\": \"^A.*\"}}");
            assertEquals("data->>'name' ~ ?", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("^A.*", r.params.get(0));
        }

        @Test
        void mixedContainmentAndOperator() {
            Utils.FilterResult r = Utils.buildFilter("{\"active\": true, \"age\": {\"$gte\": 18}}");
            assertEquals("data @> ?::jsonb AND (data->>'age')::numeric >= ?", r.whereClause);
            assertEquals(2, r.params.size());
            assertEquals("{\"active\": true}", r.params.get(0));
            assertEquals(18.0, r.params.get(1));
        }

        @Test
        void nestedFieldPath() {
            Utils.FilterResult r = Utils.buildFilter("{\"address.city\": {\"$eq\": \"NYC\"}}");
            assertEquals("data->'address'->>'city' = ?", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("NYC", r.params.get(0));
        }

        @Test
        void nullFilterReturnsEmpty() {
            Utils.FilterResult r = Utils.buildFilter(null);
            assertEquals("", r.whereClause);
            assertTrue(r.params.isEmpty());
        }

        @Test
        void unsupportedOperatorThrows() {
            assertThrows(IllegalArgumentException.class,
                () -> Utils.buildFilter("{\"x\": {\"$unknown\": 1}}"));
        }

        @Test
        void dotNotationPlainContainment() {
            Utils.FilterResult r = Utils.buildFilter("{\"address.city\": \"NYC\"}");
            assertEquals("data @> ?::jsonb", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("{\"address\": {\"city\": \"NYC\"}}", r.params.get(0));
        }

        @Test
        void dotNotationMultiLevel() {
            Utils.FilterResult r = Utils.buildFilter("{\"a.b.c\": 1}");
            assertEquals("data @> ?::jsonb", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("{\"a\": {\"b\": {\"c\": 1}}}", r.params.get(0));
        }

        @Test
        void dotNotationSharedPrefix() {
            Utils.FilterResult r = Utils.buildFilter("{\"a.b\": 1, \"a.c\": 2}");
            assertEquals("data @> ?::jsonb", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("{\"a\": {\"b\": 1, \"c\": 2}}", r.params.get(0));
        }

        @Test
        void dotNotationMixedWithPlainKey() {
            Utils.FilterResult r = Utils.buildFilter("{\"name\": \"Alice\", \"address.city\": \"NYC\"}");
            assertEquals("data @> ?::jsonb", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("{\"name\": \"Alice\", \"address\": {\"city\": \"NYC\"}}", r.params.get(0));
        }

        @Test
        void dotNotationInMixedContainmentAndOperator() {
            Utils.FilterResult r = Utils.buildFilter("{\"address.city\": \"NYC\", \"age\": {\"$gte\": 18}}");
            assertEquals("data @> ?::jsonb AND (data->>'age')::numeric >= ?", r.whereClause);
            assertEquals(2, r.params.size());
            assertEquals("{\"address\": {\"city\": \"NYC\"}}", r.params.get(0));
            assertEquals(18.0, r.params.get(1));
        }

        @Test
        void dotNotationWithBooleanValue() {
            Utils.FilterResult r = Utils.buildFilter("{\"settings.notifications.email\": true}");
            assertEquals("data @> ?::jsonb", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("{\"settings\": {\"notifications\": {\"email\": true}}}", r.params.get(0));
        }

        @Test
        void dotNotationMultipleSeparatePaths() {
            Utils.FilterResult r = Utils.buildFilter("{\"address.city\": \"NYC\", \"profile.verified\": true}");
            assertEquals("data @> ?::jsonb", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("{\"address\": {\"city\": \"NYC\"}, \"profile\": {\"verified\": true}}", r.params.get(0));
        }

        @Test
        void dotNotationWithNumericValue() {
            Utils.FilterResult r = Utils.buildFilter("{\"metrics.score\": 99}");
            assertEquals("data @> ?::jsonb", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("{\"metrics\": {\"score\": 99}}", r.params.get(0));
        }
    }


    // -------------------------------------------------------------------------
    // Comparison operators in doc* methods (integration-level)
    // -------------------------------------------------------------------------

    @Nested class ComparisonOperatorIntegrationTest {

        @Test
        void docFindWithGt() throws SQLException {
            emptyResultSet("id", "data", "created_at", "updated_at");
            Utils.docFind(conn, "users", "{\"age\": {\"$gt\": 21}}", null, null, null);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("WHERE (data->>'age')::numeric > ?"));
            verify(ps).setDouble(1, 21.0);
        }

        @Test
        void docCountWithGte() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(5L);

            long count = Utils.docCount(conn, "users", "{\"score\": {\"$gte\": 90}}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("WHERE (data->>'score')::numeric >= ?"));
            verify(ps).setDouble(1, 90.0);
            assertEquals(5L, count);
        }

        @Test
        void docDeleteWithIn() throws SQLException {
            allowUpdate(3);
            int count = Utils.docDelete(conn, "users", "{\"status\": {\"$in\": [\"banned\", \"spam\"]}}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("WHERE data->>'status' IN (?, ?)"));
            verify(ps).setString(1, "banned");
            verify(ps).setString(2, "spam");
            assertEquals(3, count);
        }

        @Test
        void docUpdateWithLt() throws SQLException {
            allowUpdate(2);
            int count = Utils.docUpdate(conn, "users",
                "{\"score\": {\"$lt\": 50}}", "{\"flagged\":true}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("SET data = data || ?::jsonb"));
            assertTrue(sql.contains("WHERE (data->>'score')::numeric < ?"));
            verify(ps).setString(1, "{\"flagged\":true}");
            verify(ps).setDouble(2, 50.0);
            assertEquals(2, count);
        }

        @Test
        void docDeleteOneWithRegex() throws SQLException {
            allowUpdate(1);
            int count = Utils.docDeleteOne(conn, "users", "{\"name\": {\"$regex\": \"^test\"}}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("WHERE data->>'name' ~ ?"));
            verify(ps).setString(1, "^test");
            assertEquals(1, count);
        }
    }


    // -------------------------------------------------------------------------
    // docWatch
    // -------------------------------------------------------------------------

    @Nested class DocWatchTest {

        @Mock PGConnection pgConn;

        @Test
        void createsTriggersAndListens() throws SQLException {
            when(conn.createStatement()).thenReturn(stmt);
            when(conn.unwrap(PGConnection.class)).thenReturn(pgConn);
            when(pgConn.getNotifications(5000)).thenReturn(null);

            Thread t = Utils.docWatch(conn, "events", (ch, payload) -> {});

            ArgumentCaptor<String> ddlCaptor = ArgumentCaptor.forClass(String.class);
            verify(stmt, atLeast(4)).execute(ddlCaptor.capture());
            List<String> ddls = ddlCaptor.getAllValues();

            assertTrue(ddls.stream().anyMatch(s -> s.contains("CREATE OR REPLACE FUNCTION events_notify_fn()")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("DROP TRIGGER IF EXISTS events_notify_trg ON events")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("CREATE TRIGGER events_notify_trg")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("LISTEN events_changes")));

            assertNotNull(t);
            assertTrue(t.isDaemon());
            t.interrupt();
        }

        @Test
        void triggerBodyContainsNotify() throws SQLException {
            when(conn.createStatement()).thenReturn(stmt);
            when(conn.unwrap(PGConnection.class)).thenReturn(pgConn);
            when(pgConn.getNotifications(5000)).thenReturn(null);

            Thread t = Utils.docWatch(conn, "orders", (ch, payload) -> {});

            ArgumentCaptor<String> ddlCaptor = ArgumentCaptor.forClass(String.class);
            verify(stmt, atLeast(4)).execute(ddlCaptor.capture());
            List<String> ddls = ddlCaptor.getAllValues();

            String funcDdl = ddls.stream()
                .filter(s -> s.contains("CREATE OR REPLACE FUNCTION"))
                .findFirst().orElse("");
            assertTrue(funcDdl.contains("pg_notify('orders_changes'"));
            assertTrue(funcDdl.contains("OLD.id::text"));
            assertTrue(funcDdl.contains("NEW.id::text"));
            assertTrue(funcDdl.contains("NEW.data"));

            t.interrupt();
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docWatch(conn, "bad table", (ch, p) -> {}));
        }
    }


    // -------------------------------------------------------------------------
    // docUnwatch
    // -------------------------------------------------------------------------

    @Nested class DocUnwatchTest {

        @Test
        void dropsTriggersAndUnlistens() throws SQLException {
            when(conn.createStatement()).thenReturn(stmt);

            Utils.docUnwatch(conn, "events");

            ArgumentCaptor<String> ddlCaptor = ArgumentCaptor.forClass(String.class);
            verify(stmt, times(3)).execute(ddlCaptor.capture());
            List<String> ddls = ddlCaptor.getAllValues();

            assertTrue(ddls.stream().anyMatch(s -> s.contains("DROP TRIGGER IF EXISTS events_notify_trg ON events")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("DROP FUNCTION IF EXISTS events_notify_fn()")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("UNLISTEN events_changes")));
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docUnwatch(conn, "bad table"));
        }
    }


    // -------------------------------------------------------------------------
    // docCreateTtlIndex
    // -------------------------------------------------------------------------

    @Nested class DocCreateTtlIndexTest {

        @Test
        void createsIndexTriggerAndFunction() throws SQLException {
            when(conn.createStatement()).thenReturn(stmt);

            Utils.docCreateTtlIndex(conn, "sessions", 3600);

            ArgumentCaptor<String> ddlCaptor = ArgumentCaptor.forClass(String.class);
            verify(stmt, times(4)).execute(ddlCaptor.capture());
            List<String> ddls = ddlCaptor.getAllValues();

            assertTrue(ddls.stream().anyMatch(s -> s.contains("CREATE INDEX IF NOT EXISTS sessions_ttl_idx ON sessions (created_at)")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("CREATE OR REPLACE FUNCTION sessions_ttl_fn()")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("INTERVAL '3600 seconds'")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("DROP TRIGGER IF EXISTS sessions_ttl_trg ON sessions")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("CREATE TRIGGER sessions_ttl_trg")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("BEFORE INSERT")));
        }

        @Test
        void customField() throws SQLException {
            when(conn.createStatement()).thenReturn(stmt);

            Utils.docCreateTtlIndex(conn, "logs", 7200, "updated_at");

            ArgumentCaptor<String> ddlCaptor = ArgumentCaptor.forClass(String.class);
            verify(stmt, atLeast(1)).execute(ddlCaptor.capture());
            List<String> ddls = ddlCaptor.getAllValues();

            assertTrue(ddls.stream().anyMatch(s -> s.contains("ON logs (updated_at)")));
            assertTrue(ddls.stream().anyMatch(s ->
                s.contains("WHERE updated_at < NOW() - INTERVAL '7200 seconds'")));
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docCreateTtlIndex(conn, "bad table", 3600));
        }

        @Test
        void zeroSecondsThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docCreateTtlIndex(conn, "logs", 0));
        }

        @Test
        void negativeSecondsThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docCreateTtlIndex(conn, "logs", -100));
        }
    }


    // -------------------------------------------------------------------------
    // docRemoveTtlIndex
    // -------------------------------------------------------------------------

    @Nested class DocRemoveTtlIndexTest {

        @Test
        void dropsTriggerFunctionAndIndex() throws SQLException {
            when(conn.createStatement()).thenReturn(stmt);

            Utils.docRemoveTtlIndex(conn, "sessions");

            ArgumentCaptor<String> ddlCaptor = ArgumentCaptor.forClass(String.class);
            verify(stmt, times(3)).execute(ddlCaptor.capture());
            List<String> ddls = ddlCaptor.getAllValues();

            assertTrue(ddls.stream().anyMatch(s -> s.contains("DROP TRIGGER IF EXISTS sessions_ttl_trg ON sessions")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("DROP FUNCTION IF EXISTS sessions_ttl_fn()")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("DROP INDEX IF EXISTS sessions_ttl_idx")));
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docRemoveTtlIndex(conn, "1bad"));
        }
    }


    // -------------------------------------------------------------------------
    // docCreateCapped
    // -------------------------------------------------------------------------

    @Nested class DocCreateCappedTest {

        @Test
        void ensuresCollectionAndCreatesTrigger() throws SQLException {
            when(conn.createStatement()).thenReturn(stmt);

            Utils.docCreateCapped(conn, "logs", 1000);

            ArgumentCaptor<String> ddlCaptor = ArgumentCaptor.forClass(String.class);
            verify(stmt, atLeast(4)).execute(ddlCaptor.capture());
            List<String> ddls = ddlCaptor.getAllValues();

            assertTrue(ddls.stream().anyMatch(s -> s.contains("CREATE TABLE IF NOT EXISTS logs")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("CREATE OR REPLACE FUNCTION logs_cap_fn()")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("- 1000, 0)")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("ORDER BY created_at ASC, id ASC")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("DROP TRIGGER IF EXISTS logs_cap_trg ON logs")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("CREATE TRIGGER logs_cap_trg")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("AFTER INSERT")));
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docCreateCapped(conn, "bad table", 100));
        }

        @Test
        void zeroMaxDocsThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docCreateCapped(conn, "logs", 0));
        }

        @Test
        void negativeMaxDocsThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docCreateCapped(conn, "logs", -5));
        }
    }


    // -------------------------------------------------------------------------
    // docRemoveCap
    // -------------------------------------------------------------------------

    @Nested class DocRemoveCapTest {

        @Test
        void dropsTriggerAndFunction() throws SQLException {
            when(conn.createStatement()).thenReturn(stmt);

            Utils.docRemoveCap(conn, "logs");

            ArgumentCaptor<String> ddlCaptor = ArgumentCaptor.forClass(String.class);
            verify(stmt, times(2)).execute(ddlCaptor.capture());
            List<String> ddls = ddlCaptor.getAllValues();

            assertTrue(ddls.stream().anyMatch(s -> s.contains("DROP TRIGGER IF EXISTS logs_cap_trg ON logs")));
            assertTrue(ddls.stream().anyMatch(s -> s.contains("DROP FUNCTION IF EXISTS logs_cap_fn()")));
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.docRemoveCap(conn, "1bad"));
        }
    }


    // -------------------------------------------------------------------------
    // Logical operators ($or, $and, $not) in buildFilter
    // -------------------------------------------------------------------------

    @Nested class LogicalOperatorTest {

        @Test
        void orOperator() {
            Utils.FilterResult r = Utils.buildFilter(
                "{\"$or\": [{\"status\": \"active\"}, {\"status\": \"pending\"}]}");
            assertEquals("(data @> ?::jsonb OR data @> ?::jsonb)", r.whereClause);
            assertEquals(2, r.params.size());
            assertEquals("{\"status\": \"active\"}", r.params.get(0));
            assertEquals("{\"status\": \"pending\"}", r.params.get(1));
        }

        @Test
        void andOperator() {
            Utils.FilterResult r = Utils.buildFilter(
                "{\"$and\": [{\"age\": {\"$gte\": 18}}, {\"age\": {\"$lt\": 65}}]}");
            assertEquals("((data->>'age')::numeric >= ? AND (data->>'age')::numeric < ?)", r.whereClause);
            assertEquals(2, r.params.size());
            assertEquals(18.0, r.params.get(0));
            assertEquals(65.0, r.params.get(1));
        }

        @Test
        void notOperator() {
            Utils.FilterResult r = Utils.buildFilter(
                "{\"$not\": {\"status\": \"deleted\"}}");
            assertEquals("NOT (data @> ?::jsonb)", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals("{\"status\": \"deleted\"}", r.params.get(0));
        }

        @Test
        void notWithComparisonOperator() {
            Utils.FilterResult r = Utils.buildFilter(
                "{\"$not\": {\"age\": {\"$lt\": 18}}}");
            assertEquals("NOT ((data->>'age')::numeric < ?)", r.whereClause);
            assertEquals(1, r.params.size());
            assertEquals(18.0, r.params.get(0));
        }

        @Test
        void orWithMixedFilters() {
            Utils.FilterResult r = Utils.buildFilter(
                "{\"$or\": [{\"name\": \"alice\"}, {\"age\": {\"$gt\": 30}}]}");
            assertEquals("(data @> ?::jsonb OR (data->>'age')::numeric > ?)", r.whereClause);
            assertEquals(2, r.params.size());
            assertEquals("{\"name\": \"alice\"}", r.params.get(0));
            assertEquals(30.0, r.params.get(1));
        }

        @Test
        void logicalWithOtherClauses() {
            Utils.FilterResult r = Utils.buildFilter(
                "{\"active\": true, \"$or\": [{\"role\": \"admin\"}, {\"role\": \"moderator\"}]}");
            assertTrue(r.whereClause.contains("data @> ?::jsonb"));
            assertTrue(r.whereClause.contains("(data @> ?::jsonb OR data @> ?::jsonb)"));
            assertEquals(3, r.params.size());
        }

        @Test
        void docFindWithOr() throws SQLException {
            emptyResultSet("id", "data", "created_at", "updated_at");
            Utils.docFind(conn, "users",
                "{\"$or\": [{\"role\": \"admin\"}, {\"role\": \"editor\"}]}",
                null, null, null);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("WHERE (data @> ?::jsonb OR data @> ?::jsonb)"));
        }

        @Test
        void docCountWithNot() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(10L);

            long count = Utils.docCount(conn, "users",
                "{\"$not\": {\"banned\": true}}");
            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("WHERE NOT (data @> ?::jsonb)"));
            assertEquals(10L, count);
        }

        @Test
        void notWithNonObjectThrows() {
            assertThrows(IllegalArgumentException.class,
                () -> Utils.buildFilter("{\"$not\": [1, 2]}"));
        }

        @Test
        void orWithEmptyArrayThrows() {
            assertThrows(IllegalArgumentException.class,
                () -> Utils.buildFilter("{\"$or\": []}"));
        }
    }


    // -------------------------------------------------------------------------
    // buildUpdate unit tests
    // -------------------------------------------------------------------------

    @Nested class BuildUpdateTest {

        @Test
        void plainMerge() {
            Utils.UpdateResult r = Utils.buildUpdate("{\"name\": \"alice\"}");
            assertEquals("data || ?::jsonb", r.expr);
            assertEquals(1, r.params.size());
            assertEquals("{\"name\": \"alice\"}", r.params.get(0));
        }

        @Test
        void setOperator() {
            Utils.UpdateResult r = Utils.buildUpdate("{\"$set\": {\"name\": \"bob\", \"age\": 30}}");
            assertEquals("(data || ?::jsonb)", r.expr);
            assertEquals(1, r.params.size());
            assertEquals("{\"name\": \"bob\", \"age\": 30}", r.params.get(0));
        }

        @Test
        void unsetSingleField() {
            Utils.UpdateResult r = Utils.buildUpdate("{\"$unset\": {\"temp\": \"\"}}");
            assertEquals("(data - ?)", r.expr);
            assertEquals(1, r.params.size());
            assertEquals("temp", r.params.get(0));
        }

        @Test
        void unsetNestedField() {
            Utils.UpdateResult r = Utils.buildUpdate("{\"$unset\": {\"meta.tmp\": \"\"}}");
            assertEquals("(data #- ?::text[])", r.expr);
            assertEquals(1, r.params.size());
            assertEquals("{meta,tmp}", r.params.get(0));
        }

        @Test
        void incOperator() {
            Utils.UpdateResult r = Utils.buildUpdate("{\"$inc\": {\"score\": 5}}");
            assertTrue(r.expr.contains("jsonb_set(data, ?::text[], to_jsonb(COALESCE((data->>'score')::numeric, 0) + ?))"));
            assertEquals(2, r.params.size());
            assertEquals("{score}", r.params.get(0));
            assertEquals(5.0, r.params.get(1));
        }

        @Test
        void mulOperator() {
            Utils.UpdateResult r = Utils.buildUpdate("{\"$mul\": {\"price\": 1.1}}");
            assertTrue(r.expr.contains("jsonb_set(data, ?::text[], to_jsonb(COALESCE((data->>'price')::numeric, 0) * ?))"));
            assertEquals(2, r.params.size());
            assertEquals("{price}", r.params.get(0));
            assertEquals(1.1, r.params.get(1));
        }

        @Test
        void renameOperator() {
            Utils.UpdateResult r = Utils.buildUpdate("{\"$rename\": {\"oldName\": \"newName\"}}");
            assertTrue(r.expr.contains("jsonb_set((data - ?), ?::text[], data->'oldName')"));
            assertEquals(2, r.params.size());
            assertEquals("oldName", r.params.get(0));
            assertEquals("{newName}", r.params.get(1));
        }

        @Test
        void renameNestedField() {
            Utils.UpdateResult r = Utils.buildUpdate("{\"$rename\": {\"meta.old\": \"meta.fresh\"}}");
            assertTrue(r.expr.contains("#- ?::text[]"));
            assertTrue(r.expr.contains("jsonb_set("));
            assertTrue(r.expr.contains("data->'meta'->'old'"));
            assertEquals(2, r.params.size());
            assertEquals("{meta,old}", r.params.get(0));
            assertEquals("{meta,fresh}", r.params.get(1));
        }

        @Test
        void pushOperator() {
            Utils.UpdateResult r = Utils.buildUpdate("{\"$push\": {\"tags\": \"new\"}}");
            assertTrue(r.expr.contains("jsonb_set(data, ?::text[], COALESCE(data->'tags', '[]'::jsonb) || to_jsonb(?::text))"));
            assertEquals(2, r.params.size());
            assertEquals("{tags}", r.params.get(0));
            assertEquals("new", r.params.get(1));
        }

        @Test
        void pushNumeric() {
            Utils.UpdateResult r = Utils.buildUpdate("{\"$push\": {\"scores\": 42}}");
            assertTrue(r.expr.contains("COALESCE(data->'scores', '[]'::jsonb) || to_jsonb(?::numeric)"));
            assertEquals(2, r.params.size());
            assertEquals("{scores}", r.params.get(0));
            assertEquals("42", r.params.get(1));
        }

        @Test
        void pullOperator() {
            Utils.UpdateResult r = Utils.buildUpdate("{\"$pull\": {\"tags\": \"old\"}}");
            assertTrue(r.expr.contains("jsonb_set(data, ?::text[]"));
            assertTrue(r.expr.contains("SELECT jsonb_agg(elem) FROM jsonb_array_elements(data->'tags') AS elem"));
            assertTrue(r.expr.contains("WHERE elem != to_jsonb(?::text)"));
            assertEquals(2, r.params.size());
            assertEquals("{tags}", r.params.get(0));
            assertEquals("old", r.params.get(1));
        }

        @Test
        void addToSetOperator() {
            Utils.UpdateResult r = Utils.buildUpdate("{\"$addToSet\": {\"tags\": \"unique\"}}");
            assertTrue(r.expr.contains("jsonb_set(data, ?::text[]"));
            assertTrue(r.expr.contains("CASE WHEN COALESCE(data->'tags', '[]'::jsonb) @> to_jsonb(?::text)"));
            assertTrue(r.expr.contains("ELSE COALESCE(data->'tags', '[]'::jsonb) || to_jsonb(?::text) END"));
            assertEquals(3, r.params.size());
            assertEquals("{tags}", r.params.get(0));
            assertEquals("unique", r.params.get(1));
            assertEquals("unique", r.params.get(2));
        }

        @Test
        void combinedSetAndInc() {
            Utils.UpdateResult r = Utils.buildUpdate(
                "{\"$set\": {\"name\": \"bob\"}, \"$inc\": {\"visits\": 1}}");
            assertTrue(r.expr.contains("|| ?::jsonb"));
            assertTrue(r.expr.contains("jsonb_set("));
            assertTrue(r.expr.contains("COALESCE((data->>'visits')::numeric, 0) + ?"));
            assertEquals(3, r.params.size());
        }

        @Test
        void emptyObjectReturnsPlainMerge() {
            Utils.UpdateResult r = Utils.buildUpdate("{}");
            assertEquals("data || ?::jsonb", r.expr);
        }

        @Test
        void nullUpdateThrows() {
            assertThrows(IllegalArgumentException.class,
                () -> Utils.buildUpdate(null));
        }

        @Test
        void unsupportedOperatorThrows() {
            assertThrows(IllegalArgumentException.class,
                () -> Utils.buildUpdate("{\"$unknown\": {\"x\": 1}}"));
        }
    }


    // -------------------------------------------------------------------------
    // Update operators in doc* methods (integration-level)
    // -------------------------------------------------------------------------

    @Nested class UpdateOperatorIntegrationTest {

        @Test
        void docUpdateWithSet() throws SQLException {
            allowUpdate(2);
            int count = Utils.docUpdate(conn, "users",
                "{\"active\": true}", "{\"$set\": {\"role\": \"admin\"}}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("SET data = (data || ?::jsonb), updated_at = NOW()"));
            assertTrue(sql.contains("WHERE data @> ?::jsonb"));
            verify(ps).setString(1, "{\"role\": \"admin\"}");
            verify(ps).setString(2, "{\"active\": true}");
            assertEquals(2, count);
        }

        @Test
        void docUpdateOneWithInc() throws SQLException {
            allowUpdate(1);
            int count = Utils.docUpdateOne(conn, "users",
                "{\"name\": \"alice\"}", "{\"$inc\": {\"score\": 10}}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("jsonb_set(data, ?::text[], to_jsonb(COALESCE((data->>'score')::numeric, 0) + ?))"));
            assertTrue(sql.contains("WHERE id = (SELECT id FROM users WHERE data @> ?::jsonb LIMIT 1)"));
            verify(ps).setString(1, "{score}");
            verify(ps).setDouble(2, 10.0);
            verify(ps).setString(3, "{\"name\": \"alice\"}");
            assertEquals(1, count);
        }

        @Test
        void docUpdateWithPush() throws SQLException {
            allowUpdate(1);
            Utils.docUpdate(conn, "users", "{\"name\": \"bob\"}",
                "{\"$push\": {\"tags\": \"vip\"}}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("jsonb_set(data, ?::text[], COALESCE(data->'tags', '[]'::jsonb) || to_jsonb(?::text))"));
        }
    }


    // -------------------------------------------------------------------------
    // docFindOneAndUpdate
    // -------------------------------------------------------------------------

    @Nested class DocFindOneAndUpdateTest {

        @Test
        void sqlAndParams() throws SQLException {
            singleRowResultSet("id", "data", "created_at", "updated_at");
            when(rs.getObject(1)).thenReturn(1L);
            when(rs.getObject(2)).thenReturn("{\"name\":\"alice\",\"score\":100}");

            Map<String, Object> result = Utils.docFindOneAndUpdate(conn, "users",
                "{\"name\":\"alice\"}", "{\"$inc\": {\"score\": 10}}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("UPDATE users SET data ="));
            assertTrue(sql.contains("updated_at = NOW()"));
            assertTrue(sql.contains("WHERE id = (SELECT id FROM users WHERE"));
            assertTrue(sql.contains("LIMIT 1)"));
            assertTrue(sql.contains("RETURNING id, data, created_at, updated_at"));
            assertNotNull(result);
            assertEquals(1L, result.get("id"));
        }

        @Test
        void plainMergeUpdate() throws SQLException {
            singleRowResultSet("id", "data", "created_at", "updated_at");
            when(rs.getObject(1)).thenReturn(1L);

            Utils.docFindOneAndUpdate(conn, "users",
                "{\"name\":\"alice\"}", "{\"score\": 99}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("SET data = data || ?::jsonb"));
            assertTrue(sql.contains("RETURNING id, data, created_at, updated_at"));
        }

        @Test
        void returnsNullWhenNoMatch() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            Map<String, Object> result = Utils.docFindOneAndUpdate(conn, "users",
                "{\"name\":\"nobody\"}", "{\"x\": 1}");
            assertNull(result);
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                () -> Utils.docFindOneAndUpdate(conn, "bad table", "{}", "{}"));
        }
    }


    // -------------------------------------------------------------------------
    // docFindOneAndDelete
    // -------------------------------------------------------------------------

    @Nested class DocFindOneAndDeleteTest {

        @Test
        void sqlAndParams() throws SQLException {
            singleRowResultSet("id", "data", "created_at", "updated_at");
            when(rs.getObject(1)).thenReturn(42L);
            when(rs.getObject(2)).thenReturn("{\"name\":\"bob\"}");

            Map<String, Object> result = Utils.docFindOneAndDelete(conn, "users",
                "{\"name\":\"bob\"}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("DELETE FROM users WHERE id ="));
            assertTrue(sql.contains("SELECT id FROM users WHERE data @> ?::jsonb LIMIT 1"));
            assertTrue(sql.contains("RETURNING id, data, created_at, updated_at"));
            verify(ps).setString(1, "{\"name\":\"bob\"}");
            assertNotNull(result);
            assertEquals(42L, result.get("id"));
        }

        @Test
        void returnsNullWhenNoMatch() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            Map<String, Object> result = Utils.docFindOneAndDelete(conn, "users",
                "{\"name\":\"nobody\"}");
            assertNull(result);
        }

        @Test
        void noFilterDeletesFirst() throws SQLException {
            singleRowResultSet("id", "data", "created_at", "updated_at");
            when(rs.getObject(1)).thenReturn(1L);

            Utils.docFindOneAndDelete(conn, "users", null);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("WHERE TRUE LIMIT 1"));
            assertTrue(sql.contains("RETURNING"));
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                () -> Utils.docFindOneAndDelete(conn, "1bad", "{}"));
        }
    }


    // -------------------------------------------------------------------------
    // docDistinct
    // -------------------------------------------------------------------------

    @Nested class DocDistinctTest {

        @Test
        void sqlWithFilter() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true, true, false);
            when(rs.getString(1)).thenReturn("admin", "user");

            List<String> result = Utils.docDistinct(conn, "users", "role",
                "{\"active\": true}");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("SELECT DISTINCT data->>'role' FROM users"));
            assertTrue(sql.contains("WHERE data->>'role' IS NOT NULL"));
            assertTrue(sql.contains("AND data @> ?::jsonb"));
            verify(ps).setString(1, "{\"active\": true}");
            assertEquals(2, result.size());
            assertEquals("admin", result.get(0));
            assertEquals("user", result.get(1));
        }

        @Test
        void sqlWithoutFilter() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true, false);
            when(rs.getString(1)).thenReturn("red");

            List<String> result = Utils.docDistinct(conn, "items", "color", null);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("SELECT DISTINCT data->>'color' FROM items"));
            assertTrue(sql.contains("WHERE data->>'color' IS NOT NULL"));
            assertFalse(sql.contains("AND"));
            assertEquals(1, result.size());
            assertEquals("red", result.get(0));
        }

        @Test
        void nestedFieldPath() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            Utils.docDistinct(conn, "users", "address.city", null);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("SELECT DISTINCT data->'address'->>'city' FROM users"));
            assertTrue(sql.contains("WHERE data->'address'->>'city' IS NOT NULL"));
        }

        @Test
        void emptyResultReturnsEmptyList() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            List<String> result = Utils.docDistinct(conn, "users", "name", null);
            assertTrue(result.isEmpty());
        }

        @Test
        void invalidCollectionThrows() {
            assertThrows(IllegalArgumentException.class,
                () -> Utils.docDistinct(conn, "bad table", "name", null));
        }
    }


    // -------------------------------------------------------------------------
    // Helper method unit tests
    // -------------------------------------------------------------------------

    @Nested class HelperMethodTest {

        @Test
        void fieldPathJsonSimple() {
            assertEquals("data->'name'", Utils.fieldPathJson("name"));
        }

        @Test
        void fieldPathJsonNested() {
            assertEquals("data->'address'->'city'", Utils.fieldPathJson("address.city"));
        }

        @Test
        void fieldPathJsonDeep() {
            assertEquals("data->'a'->'b'->'c'", Utils.fieldPathJson("a.b.c"));
        }

        @Test
        void jsonbPathSimple() {
            assertEquals("{name}", Utils.jsonbPath("name"));
        }

        @Test
        void jsonbPathNested() {
            assertEquals("{address,city}", Utils.jsonbPath("address.city"));
        }

        @Test
        void jsonbPathDeep() {
            assertEquals("{a,b,c}", Utils.jsonbPath("a.b.c"));
        }
    }
}
