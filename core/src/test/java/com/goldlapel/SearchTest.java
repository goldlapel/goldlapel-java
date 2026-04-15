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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SearchTest {

    @Mock Connection conn;
    @Mock PreparedStatement ps;
    @Mock Statement stmt;
    @Mock ResultSet rs;
    @Mock ResultSetMetaData meta;

    // Captures the SQL passed to conn.prepareStatement(sql)
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    // Helper: wire up an empty result set (no rows) with given column names
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

    // Helper: wire up conn.createStatement() for extension creation
    void allowCreateStatement() throws SQLException {
        when(conn.createStatement()).thenReturn(stmt);
    }


    // -------------------------------------------------------------------------
    // 1. search (single column, no highlight)
    // -------------------------------------------------------------------------

    @Nested class SearchSingleColumnTest {

        @Test
        void sqlAndParams() throws SQLException {
            emptyResultSet("_score");
            List<Map<String, Object>> result = Utils.search(conn, "articles", "title",
                    "postgres full text", 10, "english", false);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            // tsvector built from single column
            assertTrue(sql.contains("to_tsvector(?, coalesce(title, ''))"), "tsvector expression");
            assertTrue(sql.contains("plainto_tsquery(?, ?)"), "tsquery expression");
            assertTrue(sql.contains("FROM articles"), "table name");
            assertTrue(sql.contains("ORDER BY _score DESC LIMIT ?"), "ordering + limit");
            assertFalse(sql.contains("ts_headline"), "no highlight");

            // Verify parameter bindings: lang, lang, query, lang, lang, query, limit
            // search without highlight: ts_rank(tsvExpr, plainto_tsquery(?, ?)) => lang, lang, query
            //                          WHERE tsvExpr @@ plainto_tsquery(?, ?) => lang, lang, query
            //                          LIMIT ?
            verify(ps).setString(1, "english"); // ts_rank tsvector lang
            verify(ps).setString(2, "english"); // ts_rank tsquery lang
            verify(ps).setString(3, "postgres full text"); // ts_rank tsquery query
            verify(ps).setString(4, "english"); // WHERE tsvector lang
            verify(ps).setString(5, "english"); // WHERE tsquery lang
            verify(ps).setString(6, "postgres full text"); // WHERE tsquery query
            verify(ps).setInt(7, 10); // limit

            assertTrue(result.isEmpty());
        }

        @Test
        void invalidTableThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.search(conn, "drop table", "title", "q", 10, "english", false));
        }

        @Test
        void invalidColumnThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.search(conn, "articles", "bad col", "q", 10, "english", false));
        }
    }


    // -------------------------------------------------------------------------
    // 2. search (multi-column with highlight)
    // -------------------------------------------------------------------------

    @Nested class SearchMultiColumnHighlightTest {

        @Test
        void sqlAndParams() throws SQLException {
            emptyResultSet("_score", "_highlight");
            List<Map<String, Object>> result = Utils.search(conn, "articles",
                    new String[]{"title", "body"}, "search terms", 5, "english", true);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            // Multi-column coalesce
            assertTrue(sql.contains("coalesce(title, '') || ' ' || coalesce(body, '')"),
                    "multi-column coalesce concat");
            // Highlight present
            assertTrue(sql.contains("ts_headline"), "has highlight");
            assertTrue(sql.contains("StartSel=<mark>"), "mark tags");
            // Highlight uses first column
            assertTrue(sql.contains("coalesce(title, '')"), "highlight on first column");

            // With highlight, parameter order:
            // ts_rank: lang(1), lang(2), query(3)
            // ts_headline: lang(4), lang(5), query(6)
            // WHERE: lang(7), lang(8), query(9)
            // LIMIT: 10
            verify(ps).setString(1, "english");
            verify(ps).setString(3, "search terms");
            verify(ps).setString(6, "search terms");
            verify(ps).setString(9, "search terms");
            verify(ps).setInt(10, 5);

            assertTrue(result.isEmpty());
        }

        @Test
        void singleColumnDelegates() throws SQLException {
            emptyResultSet("_score");
            Utils.search(conn, "articles", "title", "q", 10, "english", false);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("coalesce(title, '')"));
        }

        @Test
        void invalidColumnInArrayThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.search(conn, "articles", new String[]{"ok", "bad col"}, "q", 10, "english", false));
        }
    }


    // -------------------------------------------------------------------------
    // 3. searchFuzzy
    // -------------------------------------------------------------------------

    @Nested class SearchFuzzyTest {

        @Test
        void sqlAndParams() throws SQLException {
            emptyResultSet("_score");
            Utils.searchFuzzy(conn, "products", "name", "postgrse", 10, 0.3);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            assertTrue(sql.contains("similarity(name, ?) AS _score"), "similarity scoring");
            assertTrue(sql.contains("WHERE similarity(name, ?) > ?"), "threshold filter");
            assertTrue(sql.contains("FROM products"), "table name");
            assertTrue(sql.contains("ORDER BY _score DESC LIMIT ?"), "ordering + limit");

            verify(ps).setString(1, "postgrse"); // similarity for SELECT
            verify(ps).setString(2, "postgrse"); // similarity for WHERE
            verify(ps).setDouble(3, 0.3);        // threshold
            verify(ps).setInt(4, 10);             // limit
        }

        @Test
        void invalidTableThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.searchFuzzy(conn, "1bad", "name", "q", 10, 0.3));
        }

        @Test
        void invalidColumnThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.searchFuzzy(conn, "products", "bad col", "q", 10, 0.3));
        }
    }


    // -------------------------------------------------------------------------
    // 4. searchPhonetic
    // -------------------------------------------------------------------------

    @Nested class SearchPhoneticTest {

        @Test
        void sqlAndParams() throws SQLException {
            emptyResultSet("_score");
            Utils.searchPhonetic(conn, "users", "last_name", "Smith", 20);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            assertTrue(sql.contains("similarity(last_name, ?) AS _score"), "similarity scoring");
            assertTrue(sql.contains("WHERE soundex(last_name) = soundex(?)"), "soundex filter");
            assertTrue(sql.contains("FROM users"), "table name");
            assertTrue(sql.contains("ORDER BY _score DESC, last_name LIMIT ?"), "ordering");

            verify(ps).setString(1, "Smith"); // similarity
            verify(ps).setString(2, "Smith"); // soundex comparison
            verify(ps).setInt(3, 20);         // limit
        }

        @Test
        void invalidTableThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.searchPhonetic(conn, "bad table", "name", "q", 10));
        }
    }


    // -------------------------------------------------------------------------
    // 5. similar (vector)
    // -------------------------------------------------------------------------

    @Nested class SimilarTest {

        @Test
        void sqlAndParams() throws SQLException {
            emptyResultSet("_score");
            double[] vector = {0.1, 0.2, 0.3};
            Utils.similar(conn, "embeddings", "embedding", vector, 5);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            assertTrue(sql.contains("(embedding <=> ?::vector) AS _score"), "cosine distance");
            assertTrue(sql.contains("FROM embeddings"), "table name");
            assertTrue(sql.contains("ORDER BY _score LIMIT ?"), "ordering");

            verify(ps).setString(1, "[0.1,0.2,0.3]"); // vector literal
            verify(ps).setInt(2, 5);                    // limit
        }

        @Test
        void singleElementVector() throws SQLException {
            emptyResultSet("_score");
            Utils.similar(conn, "embeddings", "vec", new double[]{42.0}, 1);

            verify(ps).setString(1, "[42.0]");
        }

        @Test
        void invalidTableThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.similar(conn, "bad table", "vec", new double[]{1.0}, 1));
        }

        @Test
        void invalidColumnThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.similar(conn, "embeddings", "bad col", new double[]{1.0}, 1));
        }
    }


    // -------------------------------------------------------------------------
    // 6. suggest
    // -------------------------------------------------------------------------

    @Nested class SuggestTest {

        @Test
        void sqlAndParams() throws SQLException {
            emptyResultSet("_score");
            Utils.suggest(conn, "products", "name", "post", 10);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            assertTrue(sql.contains("similarity(name, ?) AS _score"), "similarity scoring");
            assertTrue(sql.contains("WHERE name ILIKE ?"), "ILIKE prefix filter");
            assertTrue(sql.contains("FROM products"), "table name");
            assertTrue(sql.contains("ORDER BY _score DESC, name LIMIT ?"), "ordering");

            verify(ps).setString(1, "post");   // similarity arg
            verify(ps).setString(2, "post%");  // ILIKE pattern with % suffix
            verify(ps).setInt(3, 10);          // limit
        }

        @Test
        void invalidTableThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.suggest(conn, "1bad", "name", "q", 10));
        }
    }


    // -------------------------------------------------------------------------
    // 7. facets (without query)
    // -------------------------------------------------------------------------

    @Nested class FacetsNoQueryTest {

        @Test
        void sqlAndParams() throws SQLException {
            emptyResultSet("value", "count");
            Utils.facets(conn, "orders", "status", 10);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            assertTrue(sql.contains("SELECT status AS value, COUNT(*) AS count"), "select expression");
            assertTrue(sql.contains("FROM orders"), "table name");
            assertTrue(sql.contains("GROUP BY status"), "group by");
            assertTrue(sql.contains("ORDER BY count DESC, status LIMIT ?"), "ordering");
            assertFalse(sql.contains("WHERE"), "no WHERE clause without query");

            verify(ps).setInt(1, 10); // limit only
        }

        @Test
        void invalidTableThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.facets(conn, "bad table", "col", 10));
        }

        @Test
        void invalidColumnThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.facets(conn, "orders", "bad col", 10));
        }
    }


    // -------------------------------------------------------------------------
    // 8. facets (with single-column query filter)
    // -------------------------------------------------------------------------

    @Nested class FacetsSingleColumnQueryTest {

        @Test
        void sqlAndParams() throws SQLException {
            emptyResultSet("value", "count");
            Utils.facets(conn, "articles", "category", 5, "postgres", "title", "english");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            assertTrue(sql.contains("SELECT category AS value, COUNT(*) AS count"), "select expr");
            assertTrue(sql.contains("WHERE to_tsvector(?, coalesce(title, ''))"), "tsvector filter");
            assertTrue(sql.contains("@@ plainto_tsquery(?, ?)"), "tsquery");
            assertTrue(sql.contains("GROUP BY category ORDER BY count DESC, category LIMIT ?"), "group + order");

            verify(ps).setString(1, "english");  // tsvector lang
            verify(ps).setString(2, "english");  // tsquery lang
            verify(ps).setString(3, "postgres"); // query text
            verify(ps).setInt(4, 5);             // limit
        }
    }


    // -------------------------------------------------------------------------
    // 9. facets (with multi-column query filter)
    // -------------------------------------------------------------------------

    @Nested class FacetsMultiColumnQueryTest {

        @Test
        void sqlCoalesceMultiColumn() throws SQLException {
            emptyResultSet("value", "count");
            Utils.facets(conn, "articles", "category", 5,
                    "search terms", new String[]{"title", "body"}, "english");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            assertTrue(sql.contains("coalesce(title, '') || ' ' || coalesce(body, '')"),
                    "multi-column coalesce");
        }

        @Test
        void nullQuerySkipsFilter() throws SQLException {
            emptyResultSet("value", "count");
            Utils.facets(conn, "orders", "status", 10, null, (String[]) null, null);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertFalse(sql.contains("WHERE"), "no WHERE when query is null");
        }

        @Test
        void nullQueryColumnsSkipsFilter() throws SQLException {
            emptyResultSet("value", "count");
            Utils.facets(conn, "orders", "status", 10, "some query", (String[]) null, "english");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertFalse(sql.contains("WHERE"), "no WHERE when queryColumns is null");
        }

        @Test
        void emptyQueryColumnsSkipsFilter() throws SQLException {
            emptyResultSet("value", "count");
            Utils.facets(conn, "orders", "status", 10, "some query", new String[]{}, "english");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertFalse(sql.contains("WHERE"), "no WHERE when queryColumns is empty");
        }

        @Test
        void invalidQueryColumnThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.facets(conn, "articles", "category", 5,
                            "q", new String[]{"bad col"}, "english"));
        }
    }


    // -------------------------------------------------------------------------
    // 10. aggregate (without groupBy)
    // -------------------------------------------------------------------------

    @Nested class AggregateNoGroupTest {

        @Test
        void sumSql() throws SQLException {
            emptyResultSet("value");
            Utils.aggregate(conn, "orders", "amount", "sum");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            assertEquals("SELECT SUM(amount) AS value FROM orders", sql);
            // No parameters bound when no groupBy
            verify(ps, never()).setInt(anyInt(), anyInt());
        }

        @Test
        void countUsesCountStar() throws SQLException {
            emptyResultSet("value");
            Utils.aggregate(conn, "orders", "id", "count");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            assertTrue(sql.contains("COUNT(*)"), "count uses COUNT(*)");
            assertFalse(sql.contains("COUNT(id)"), "count does NOT use column");
        }

        @Test
        void avgSql() throws SQLException {
            emptyResultSet("value");
            Utils.aggregate(conn, "orders", "price", "avg");

            verify(conn).prepareStatement(sqlCaptor.capture());
            assertTrue(sqlCaptor.getValue().contains("AVG(price)"));
        }

        @Test
        void minSql() throws SQLException {
            emptyResultSet("value");
            Utils.aggregate(conn, "orders", "price", "min");

            verify(conn).prepareStatement(sqlCaptor.capture());
            assertTrue(sqlCaptor.getValue().contains("MIN(price)"));
        }

        @Test
        void maxSql() throws SQLException {
            emptyResultSet("value");
            Utils.aggregate(conn, "orders", "price", "max");

            verify(conn).prepareStatement(sqlCaptor.capture());
            assertTrue(sqlCaptor.getValue().contains("MAX(price)"));
        }

        @Test
        void invalidFuncThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.aggregate(conn, "orders", "amount", "median"));
        }

        @Test
        void invalidFuncMessageListsValid() {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> Utils.aggregate(conn, "orders", "amount", "bogus"));
            assertTrue(ex.getMessage().contains("count"));
            assertTrue(ex.getMessage().contains("sum"));
            assertTrue(ex.getMessage().contains("avg"));
            assertTrue(ex.getMessage().contains("min"));
            assertTrue(ex.getMessage().contains("max"));
        }

        @Test
        void invalidTableThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.aggregate(conn, "bad table", "amount", "sum"));
        }

        @Test
        void invalidColumnThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.aggregate(conn, "orders", "bad col", "sum"));
        }

        @Test
        void funcIsCaseInsensitive() throws SQLException {
            emptyResultSet("value");
            Utils.aggregate(conn, "orders", "amount", "SUM");

            verify(conn).prepareStatement(sqlCaptor.capture());
            assertTrue(sqlCaptor.getValue().contains("SUM(amount)"));
        }
    }


    // -------------------------------------------------------------------------
    // 11. aggregate (with groupBy)
    // -------------------------------------------------------------------------

    @Nested class AggregateGroupByTest {

        @Test
        void sqlAndParams() throws SQLException {
            emptyResultSet("category", "value");
            Utils.aggregate(conn, "orders", "amount", "sum", "category", 10);

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            assertTrue(sql.contains("SELECT category, SUM(amount) AS value"), "select expr");
            assertTrue(sql.contains("FROM orders"), "table name");
            assertTrue(sql.contains("GROUP BY category"), "group by");
            assertTrue(sql.contains("ORDER BY value DESC LIMIT ?"), "ordering + limit");

            verify(ps).setInt(1, 10); // limit
        }

        @Test
        void invalidGroupByThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.aggregate(conn, "orders", "amount", "sum", "bad col", 10));
        }
    }


    // -------------------------------------------------------------------------
    // 12. createSearchConfig
    // -------------------------------------------------------------------------

    @Nested class CreateSearchConfigTest {

        @Test
        void checksExistenceFirst() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true); // config already exists
            when(conn.createStatement()).thenReturn(stmt);

            Utils.createSearchConfig(conn, "my_config", "english");

            // Should check pg_ts_config
            verify(conn).prepareStatement(sqlCaptor.capture());
            assertTrue(sqlCaptor.getValue().contains("pg_ts_config"));
            verify(ps).setString(1, "my_config");

            // Should NOT create since it exists
            verify(stmt, never()).execute(contains("CREATE TEXT SEARCH CONFIGURATION"));
        }

        @Test
        void createsWhenNotExists() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false); // config does not exist
            when(conn.createStatement()).thenReturn(stmt);

            Utils.createSearchConfig(conn, "my_config", "english");

            verify(stmt).execute("CREATE TEXT SEARCH CONFIGURATION my_config (COPY = english)");
        }

        @Test
        void defaultCopyFromEnglish() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);
            when(conn.createStatement()).thenReturn(stmt);

            Utils.createSearchConfig(conn, "my_config");

            verify(stmt).execute("CREATE TEXT SEARCH CONFIGURATION my_config (COPY = english)");
        }

        @Test
        void invalidNameThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.createSearchConfig(conn, "bad name", "english"));
        }

        @Test
        void invalidCopyFromThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.createSearchConfig(conn, "my_config", "bad name"));
        }
    }


    // -------------------------------------------------------------------------
    // 13. percolateAdd
    // -------------------------------------------------------------------------

    @Nested class PercolateAddTest {

        @Test
        void sqlAndParams() throws SQLException {
            allowCreateStatement();
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeUpdate()).thenReturn(1);

            Utils.percolateAdd(conn, "alerts", "q1", "database performance", "english", "{\"priority\":\"high\"}");

            // Verify table + index creation
            verify(stmt).execute(contains("CREATE TABLE IF NOT EXISTS alerts"));
            verify(stmt).execute(contains("CREATE INDEX IF NOT EXISTS alerts_tsq_idx"));

            // Verify upsert SQL
            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("INSERT INTO alerts"), "insert");
            assertTrue(sql.contains("plainto_tsquery(?, ?)"), "tsquery conversion");
            assertTrue(sql.contains("ON CONFLICT (query_id) DO UPDATE"), "upsert");

            verify(ps).setString(1, "q1");                     // query_id
            verify(ps).setString(2, "database performance");   // query_text
            verify(ps).setString(3, "english");                // tsquery lang
            verify(ps).setString(4, "database performance");   // tsquery text
            verify(ps).setString(5, "english");                // lang
            verify(ps).setString(6, "{\"priority\":\"high\"}"); // metadata
        }

        @Test
        void nullMetadataSetsNull() throws SQLException {
            allowCreateStatement();
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeUpdate()).thenReturn(1);

            Utils.percolateAdd(conn, "alerts", "q1", "test query", "english", null);

            verify(ps).setNull(6, Types.OTHER);
        }

        @Test
        void convenienceOverloadUsesDefaults() throws SQLException {
            allowCreateStatement();
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeUpdate()).thenReturn(1);

            Utils.percolateAdd(conn, "alerts", "q1", "test query");

            verify(ps).setString(3, "english"); // default lang
            verify(ps).setNull(6, Types.OTHER); // default null metadata
        }

        @Test
        void invalidNameThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.percolateAdd(conn, "bad table", "q1", "query"));
        }

        @Test
        void tableCreationSql() throws SQLException {
            allowCreateStatement();
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeUpdate()).thenReturn(1);

            Utils.percolateAdd(conn, "alerts", "q1", "query");

            ArgumentCaptor<String> ddlCaptor = ArgumentCaptor.forClass(String.class);
            verify(stmt, atLeast(2)).execute(ddlCaptor.capture());
            List<String> ddls = ddlCaptor.getAllValues();

            // Table creation should have query_id, query_text, tsquery, lang, metadata columns
            String createTable = ddls.stream().filter(s -> s.contains("CREATE TABLE")).findFirst().orElse("");
            assertTrue(createTable.contains("query_id TEXT PRIMARY KEY"));
            assertTrue(createTable.contains("tsquery TSQUERY NOT NULL"));
            assertTrue(createTable.contains("metadata JSONB"));

            // Index creation
            String createIndex = ddls.stream().filter(s -> s.contains("CREATE INDEX")).findFirst().orElse("");
            assertTrue(createIndex.contains("USING GIST (tsquery)"));
        }
    }


    // -------------------------------------------------------------------------
    // 14. percolate (match document against stored queries)
    // -------------------------------------------------------------------------

    @Nested class PercolateTest {

        @Test
        void sqlAndParams() throws SQLException {
            emptyResultSet("query_id", "query_text", "metadata", "_score");
            Utils.percolate(conn, "alerts", "Postgres is fast and reliable", 20, "english");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            assertTrue(sql.contains("SELECT query_id, query_text, metadata"), "select columns");
            assertTrue(sql.contains("ts_rank(to_tsvector(?, ?), tsquery) AS _score"), "ranking");
            assertTrue(sql.contains("WHERE to_tsvector(?, ?) @@ tsquery"), "filter");
            assertTrue(sql.contains("FROM alerts"), "table name");
            assertTrue(sql.contains("ORDER BY _score DESC LIMIT ?"), "ordering + limit");

            verify(ps).setString(1, "english");                        // rank tsvector lang
            verify(ps).setString(2, "Postgres is fast and reliable");  // rank tsvector text
            verify(ps).setString(3, "english");                        // where tsvector lang
            verify(ps).setString(4, "Postgres is fast and reliable");  // where tsvector text
            verify(ps).setInt(5, 20);                                  // limit
        }

        @Test
        void convenienceOverloadDefaults() throws SQLException {
            emptyResultSet("query_id", "query_text", "metadata", "_score");
            Utils.percolate(conn, "alerts", "test text");

            verify(ps).setString(1, "english"); // default lang
            verify(ps).setInt(5, 50);           // default limit
        }

        @Test
        void invalidNameThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.percolate(conn, "bad table", "text"));
        }
    }


    // -------------------------------------------------------------------------
    // 15. percolateDelete
    // -------------------------------------------------------------------------

    @Nested class PercolateDeleteTest {

        @Test
        void sqlAndParams() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);

            boolean deleted = Utils.percolateDelete(conn, "alerts", "q1");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            assertTrue(sql.contains("DELETE FROM alerts WHERE query_id = ?"), "delete sql");
            assertTrue(sql.contains("RETURNING query_id"), "returning clause");
            verify(ps).setString(1, "q1");
            assertTrue(deleted);
        }

        @Test
        void returnsFalseWhenNotFound() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            boolean deleted = Utils.percolateDelete(conn, "alerts", "nonexistent");
            assertFalse(deleted);
        }

        @Test
        void invalidNameThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.percolateDelete(conn, "bad table", "q1"));
        }
    }


    // -------------------------------------------------------------------------
    // 16. analyze
    // -------------------------------------------------------------------------

    @Nested class AnalyzeTest {

        @Test
        void sqlAndParams() throws SQLException {
            emptyResultSet("alias", "description", "token", "dictionaries", "dictionary", "lexemes");
            Utils.analyze(conn, "The quick brown fox", "english");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            assertTrue(sql.contains("ts_debug(?, ?)"), "ts_debug call");
            assertTrue(sql.contains("SELECT alias, description, token, dictionaries, dictionary, lexemes"),
                    "all ts_debug columns");

            verify(ps).setString(1, "english");
            verify(ps).setString(2, "The quick brown fox");
        }

        @Test
        void convenienceOverloadUsesEnglish() throws SQLException {
            emptyResultSet("alias", "description", "token", "dictionaries", "dictionary", "lexemes");
            Utils.analyze(conn, "test");

            verify(ps).setString(1, "english"); // default lang
            verify(ps).setString(2, "test");
        }
    }


    // -------------------------------------------------------------------------
    // 17. explainScore
    // -------------------------------------------------------------------------

    @Nested class ExplainScoreTest {

        @Test
        void sqlAndParams() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false); // row not found

            Map<String, Object> result = Utils.explainScore(conn, "articles", "body",
                    "full text search", "id", 42, "english");

            verify(conn).prepareStatement(sqlCaptor.capture());
            String sql = sqlCaptor.getValue();

            assertTrue(sql.contains("to_tsvector(?, body)::text AS document_tokens"), "document tokens");
            assertTrue(sql.contains("plainto_tsquery(?, ?)::text AS query_tokens"), "query tokens");
            assertTrue(sql.contains("to_tsvector(?, body) @@ plainto_tsquery(?, ?) AS matches"), "matches");
            assertTrue(sql.contains("ts_rank(to_tsvector(?, body), plainto_tsquery(?, ?)) AS score"), "rank score");
            assertTrue(sql.contains("ts_headline(?, body, plainto_tsquery(?, ?)"), "headline");
            assertTrue(sql.contains("StartSel=**, StopSel=**"), "bold markers");
            assertTrue(sql.contains("FROM articles WHERE id = ?"), "table + id filter");

            // Parameter verification: 4 tsvector/tsquery blocks, each using lang pairs + query
            // document_tokens: lang(1)
            // query_tokens: lang(2), query(3)
            // matches: lang(4), lang(5), query(6)
            // score: lang(7), lang(8), query(9)
            // headline: lang(10), lang(11), query(12)
            // WHERE id = ?: idValue(13)
            verify(ps).setString(1, "english");
            verify(ps).setString(2, "english");
            verify(ps).setString(3, "full text search");
            verify(ps).setString(4, "english");
            verify(ps).setString(5, "english");
            verify(ps).setString(6, "full text search");
            verify(ps).setString(7, "english");
            verify(ps).setString(8, "english");
            verify(ps).setString(9, "full text search");
            verify(ps).setString(10, "english");
            verify(ps).setString(11, "english");
            verify(ps).setString(12, "full text search");
            verify(ps).setObject(13, 42);

            assertNull(result, "null when row not found");
        }

        @Test
        void convenienceOverloadUsesEnglish() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            Utils.explainScore(conn, "articles", "body", "query", "id", 1);

            verify(ps).setString(1, "english");
        }

        @Test
        void returnsRowWhenFound() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getMetaData()).thenReturn(meta);
            when(meta.getColumnCount()).thenReturn(2);
            when(meta.getColumnLabel(1)).thenReturn("score");
            when(meta.getColumnLabel(2)).thenReturn("matches");
            when(rs.getObject(1)).thenReturn(0.5);
            when(rs.getObject(2)).thenReturn(true);

            Map<String, Object> result = Utils.explainScore(conn, "articles", "body", "q", "id", 1, "english");

            assertNotNull(result);
            assertEquals(0.5, result.get("score"));
            assertEquals(true, result.get("matches"));
        }

        @Test
        void invalidTableThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.explainScore(conn, "bad table", "body", "q", "id", 1));
        }

        @Test
        void invalidColumnThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.explainScore(conn, "articles", "bad col", "q", "id", 1));
        }

        @Test
        void invalidIdColumnThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> Utils.explainScore(conn, "articles", "body", "q", "bad id", 1));
        }

        @Test
        void stringIdValue() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            Utils.explainScore(conn, "articles", "body", "query", "slug", "my-article", "english");

            verify(ps).setObject(13, "my-article");
        }
    }


    // -------------------------------------------------------------------------
    // Identifier validation (shared across all methods)
    // -------------------------------------------------------------------------

    @Nested class IdentifierValidationTest {

        @Test
        void validIdentifiers() {
            assertDoesNotThrow(() -> Utils.validateIdentifier("users"));
            assertDoesNotThrow(() -> Utils.validateIdentifier("_private"));
            assertDoesNotThrow(() -> Utils.validateIdentifier("Table1"));
            assertDoesNotThrow(() -> Utils.validateIdentifier("a"));
        }

        @Test
        void invalidIdentifiers() {
            assertThrows(IllegalArgumentException.class, () -> Utils.validateIdentifier("bad table"));
            assertThrows(IllegalArgumentException.class, () -> Utils.validateIdentifier("1bad"));
            assertThrows(IllegalArgumentException.class, () -> Utils.validateIdentifier("drop;--"));
            assertThrows(IllegalArgumentException.class, () -> Utils.validateIdentifier(""));
            assertThrows(IllegalArgumentException.class, () -> Utils.validateIdentifier("has-dash"));
        }
    }
}
