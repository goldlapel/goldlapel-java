package com.goldlapel;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

public class Utils {

    private static final Pattern IDENTIFIER_RE = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    static void validateIdentifier(String name) {
        if (!IDENTIFIER_RE.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid identifier: " + name);
        }
    }

    /**
     * Publish a message to a channel. Like redis.publish().
     * Uses PostgreSQL NOTIFY under the hood.
     */
    public static void publish(Connection conn, String channel, String message) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT pg_notify(?, ?)")) {
            ps.setString(1, channel);
            ps.setString(2, message);
            ps.execute();
        }
    }

    /**
     * Add a job to a queue table. Like redis.lpush().
     * Creates the queue table if it doesn't exist. Payload is stored as JSONB.
     */
    public static void enqueue(Connection conn, String queueTable, String payloadJson) throws SQLException {
        try (java.sql.Statement st = conn.createStatement()) {
            st.execute(
                "CREATE TABLE IF NOT EXISTS " + queueTable + " (" +
                "id BIGSERIAL PRIMARY KEY, " +
                "payload JSONB NOT NULL, " +
                "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
            );
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + queueTable + " (payload) VALUES (?::jsonb)")) {
            ps.setString(1, payloadJson);
            ps.executeUpdate();
        }
    }

    /**
     * Pop the next job from a queue table. Like redis.brpop() (non-blocking).
     * Uses FOR UPDATE SKIP LOCKED for safe concurrent access.
     * Returns the payload JSON string, or null if the queue is empty.
     */
    public static String dequeue(Connection conn, String queueTable) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM " + queueTable +
                " WHERE id = (" +
                "SELECT id FROM " + queueTable +
                " ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1" +
                ") RETURNING payload")) {
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        }
    }

    /**
     * Increment a counter. Like redis.incr().
     * Creates the counter table if it doesn't exist. Returns the new value.
     */
    public static long incr(Connection conn, String table, String key, long amount) throws SQLException {
        try (java.sql.Statement st = conn.createStatement()) {
            st.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " (" +
                "key TEXT PRIMARY KEY, " +
                "value BIGINT NOT NULL DEFAULT 0)"
            );
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + table + " (key, value) VALUES (?, ?) " +
                "ON CONFLICT (key) DO UPDATE SET value = " + table + ".value + ? " +
                "RETURNING value")) {
            ps.setString(1, key);
            ps.setLong(2, amount);
            ps.setLong(3, amount);
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getLong(1);
        }
    }

    /**
     * Add a member with a score to a sorted set. Like redis.zadd().
     * Creates the sorted set table if it doesn't exist.
     * If the member already exists, updates the score.
     */
    public static void zadd(Connection conn, String table, String member, double score) throws SQLException {
        try (java.sql.Statement st = conn.createStatement()) {
            st.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " (" +
                "member TEXT PRIMARY KEY, " +
                "score DOUBLE PRECISION NOT NULL)"
            );
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + table + " (member, score) VALUES (?, ?) " +
                "ON CONFLICT (member) DO UPDATE SET score = EXCLUDED.score")) {
            ps.setString(1, member);
            ps.setDouble(2, score);
            ps.executeUpdate();
        }
    }

    /**
     * Get members by score rank. Like redis.zrange().
     * Returns a list of (member, score) entries.
     * desc=true returns highest scores first (leaderboard order).
     */
    public static List<Map.Entry<String, Double>> zrange(Connection conn, String table,
            int start, int stop, boolean desc) throws SQLException {
        String order = desc ? "DESC" : "ASC";
        int limit = stop - start;
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT member, score FROM " + table +
                " ORDER BY score " + order +
                " LIMIT ? OFFSET ?")) {
            ps.setInt(1, limit);
            ps.setInt(2, start);
            ResultSet rs = ps.executeQuery();
            List<Map.Entry<String, Double>> results = new ArrayList<>();
            while (rs.next()) {
                results.add(new AbstractMap.SimpleImmutableEntry<>(
                    rs.getString(1), rs.getDouble(2)));
            }
            return results;
        }
    }

    /**
     * Set a field in a hash. Like redis.hset().
     * Creates the hash table if it doesn't exist. Uses JSONB for storage.
     */
    public static void hset(Connection conn, String table, String key, String field, String valueJson) throws SQLException {
        try (java.sql.Statement st = conn.createStatement()) {
            st.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " (" +
                "key TEXT PRIMARY KEY, " +
                "data JSONB NOT NULL DEFAULT '{}'::jsonb)"
            );
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + table + " (key, data) VALUES (?, jsonb_build_object(?, ?::jsonb)) " +
                "ON CONFLICT (key) DO UPDATE SET data = " + table + ".data || jsonb_build_object(?, ?::jsonb)")) {
            ps.setString(1, key);
            ps.setString(2, field);
            ps.setString(3, valueJson);
            ps.setString(4, field);
            ps.setString(5, valueJson);
            ps.executeUpdate();
        }
    }

    /**
     * Get a field from a hash. Like redis.hget().
     * Returns the value as a JSON string, or null if key or field doesn't exist.
     */
    public static String hget(Connection conn, String table, String key, String field) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT data->>? FROM " + table + " WHERE key = ?")) {
            ps.setString(1, field);
            ps.setString(2, key);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        }
    }

    /**
     * Get all fields from a hash. Like redis.hgetall().
     * Returns the full JSONB object as a string, or null if key doesn't exist.
     */
    public static String hgetall(Connection conn, String table, String key) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT data FROM " + table + " WHERE key = ?")) {
            ps.setString(1, key);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        }
    }

    /**
     * Remove a field from a hash. Like redis.hdel().
     * Returns true if the field existed, false otherwise.
     */
    public static boolean hdel(Connection conn, String table, String key, String field) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT data ? ? AS existed FROM " + table + " WHERE key = ?")) {
            ps.setString(1, field);
            ps.setString(2, key);
            ResultSet rs = ps.executeQuery();
            if (!rs.next() || !rs.getBoolean("existed")) {
                return false;
            }
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE " + table + " SET data = data - ? WHERE key = ?")) {
            ps.setString(1, field);
            ps.setString(2, key);
            ps.executeUpdate();
        }
        return true;
    }

    /**
     * Add a location to a geo table. Like redis.geoadd().
     * Creates the table with PostGIS geometry column if it doesn't exist.
     * Requires PostGIS extension.
     */
    public static void geoadd(Connection conn, String table, String nameColumn,
            String geomColumn, String name, double lon, double lat) throws SQLException {
        try (java.sql.Statement st = conn.createStatement()) {
            st.execute("CREATE EXTENSION IF NOT EXISTS postgis");
        }
        try (java.sql.Statement st = conn.createStatement()) {
            st.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " (" +
                "id BIGSERIAL PRIMARY KEY, " +
                nameColumn + " TEXT NOT NULL, " +
                geomColumn + " GEOMETRY(Point, 4326) NOT NULL)"
            );
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + table + " (" + nameColumn + ", " + geomColumn + ") " +
                "VALUES (?, ST_SetSRID(ST_MakePoint(?, ?), 4326))")) {
            ps.setString(1, name);
            ps.setDouble(2, lon);
            ps.setDouble(3, lat);
            ps.executeUpdate();
        }
    }

    /**
     * Find rows within a radius of a point. Like redis.georadius().
     * Requires PostGIS extension. Uses ST_DWithin with geography type
     * for accurate distance on the Earth's surface.
     * Returns a list of maps with all columns plus a "distance_m" field.
     */
    public static List<Map<String, Object>> georadius(Connection conn, String table,
            String geomColumn, double lon, double lat, double radiusMeters, int limit) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT *, ST_Distance(" +
                geomColumn + "::geography, " +
                "ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography" +
                ") AS distance_m " +
                "FROM " + table + " " +
                "WHERE ST_DWithin(" +
                geomColumn + "::geography, " +
                "ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography, " +
                "?) " +
                "ORDER BY distance_m " +
                "LIMIT ?")) {
            ps.setDouble(1, lon);
            ps.setDouble(2, lat);
            ps.setDouble(3, lon);
            ps.setDouble(4, lat);
            ps.setDouble(5, radiusMeters);
            ps.setInt(6, limit);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                results.add(row);
            }
            return results;
        }
    }

    /**
     * Get distance between two members in meters. Like redis.geodist().
     * Returns the distance in meters, or null if either member doesn't exist.
     */
    public static Double geodist(Connection conn, String table, String geomColumn,
            String nameColumn, String nameA, String nameB) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT ST_Distance(a." + geomColumn + "::geography, b." + geomColumn + "::geography) " +
                "FROM " + table + " a, " + table + " b " +
                "WHERE a." + nameColumn + " = ? AND b." + nameColumn + " = ?")) {
            ps.setString(1, nameA);
            ps.setString(2, nameB);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getDouble(1);
            }
            return null;
        }
    }

    public static Thread subscribe(Connection conn, String channel,
            BiConsumer<String, String> callback) throws SQLException {
        return subscribe(conn, channel, callback, true);
    }

    public static Thread subscribe(Connection conn, String channel,
            BiConsumer<String, String> callback, boolean blocking) throws SQLException {
        PGConnection pgConn = conn.unwrap(PGConnection.class);
        Runnable listen = () -> {
            try {
                try (Statement st = conn.createStatement()) {
                    st.execute("LISTEN " + channel);
                }
                while (true) {
                    PGNotification[] notifications = pgConn.getNotifications(5000);
                    if (notifications != null) {
                        for (PGNotification n : notifications) {
                            callback.accept(n.getName(), n.getParameter());
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
        if (blocking) {
            listen.run();
            return null;
        }
        Thread t = new Thread(listen);
        t.setDaemon(true);
        t.start();
        return t;
    }

    public static long getCounter(Connection conn, String table, String key) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT value FROM " + table + " WHERE key = ?")) {
            ps.setString(1, key);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        }
    }

    public static double zincrby(Connection conn, String table, String member,
            double amount) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + table + " (member, score) VALUES (?, ?) " +
                "ON CONFLICT (member) DO UPDATE SET score = " + table + ".score + ? " +
                "RETURNING score")) {
            ps.setString(1, member);
            ps.setDouble(2, amount);
            ps.setDouble(3, amount);
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getDouble(1);
        }
    }

    public static Long zrank(Connection conn, String table, String member,
            boolean desc) throws SQLException {
        String order = desc ? "DESC" : "ASC";
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT rank FROM (" +
                "SELECT member, ROW_NUMBER() OVER (ORDER BY score " + order + ") - 1 AS rank " +
                "FROM " + table +
                ") sub WHERE member = ?")) {
            ps.setString(1, member);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
            return null;
        }
    }

    public static Double zscore(Connection conn, String table, String member) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT score FROM " + table + " WHERE member = ?")) {
            ps.setString(1, member);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getDouble(1);
            }
            return null;
        }
    }

    public static boolean zrem(Connection conn, String table, String member) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM " + table + " WHERE member = ?")) {
            ps.setString(1, member);
            return ps.executeUpdate() > 0;
        }
    }

    public static long countDistinct(Connection conn, String table, String column) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(DISTINCT " + column + ") FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    public static long streamAdd(Connection conn, String stream, String payload) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(
                "CREATE TABLE IF NOT EXISTS " + stream + " (" +
                "id BIGSERIAL PRIMARY KEY, " +
                "payload JSONB NOT NULL, " +
                "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
            );
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + stream + " (payload) VALUES (?::jsonb) RETURNING id")) {
            ps.setString(1, payload);
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getLong(1);
        }
    }

    public static void streamCreateGroup(Connection conn, String stream, String group) throws SQLException {
        String groupTable = stream + "_groups";
        String pelTable = stream + "_pel";
        try (Statement st = conn.createStatement()) {
            st.execute(
                "CREATE TABLE IF NOT EXISTS " + groupTable + " (" +
                "group_name TEXT PRIMARY KEY, " +
                "last_delivered_id BIGINT NOT NULL DEFAULT 0)"
            );
        }
        try (Statement st = conn.createStatement()) {
            st.execute(
                "CREATE TABLE IF NOT EXISTS " + pelTable + " (" +
                "message_id BIGINT NOT NULL, " +
                "group_name TEXT NOT NULL, " +
                "consumer TEXT NOT NULL, " +
                "delivered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), " +
                "PRIMARY KEY (message_id, group_name))"
            );
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + groupTable + " (group_name) VALUES (?) " +
                "ON CONFLICT (group_name) DO NOTHING")) {
            ps.setString(1, group);
            ps.executeUpdate();
        }
    }

    public static List<Map<String, Object>> streamRead(Connection conn, String stream,
            String group, String consumer, int count) throws SQLException {
        String groupTable = stream + "_groups";
        String pelTable = stream + "_pel";
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT last_delivered_id FROM " + groupTable + " WHERE group_name = ?")) {
            ps.setString(1, group);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                throw new SQLException("Consumer group '" + group + "' does not exist");
            }
        }
        List<Map<String, Object>> results = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "WITH new_messages AS (" +
                "SELECT s.id, s.payload, s.created_at FROM " + stream + " s " +
                "WHERE s.id > (SELECT last_delivered_id FROM " + groupTable + " WHERE group_name = ?) " +
                "AND NOT EXISTS (SELECT 1 FROM " + pelTable + " p WHERE p.message_id = s.id AND p.group_name = ?) " +
                "ORDER BY s.id LIMIT ?" +
                ") SELECT * FROM new_messages")) {
            ps.setString(1, group);
            ps.setString(2, group);
            ps.setInt(3, count);
            ResultSet rs = ps.executeQuery();
            long maxId = 0;
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                long id = rs.getLong("id");
                row.put("id", id);
                row.put("payload", rs.getString("payload"));
                row.put("created_at", rs.getTimestamp("created_at"));
                results.add(row);
                if (id > maxId) maxId = id;
            }
            if (maxId > 0) {
                try (PreparedStatement upd = conn.prepareStatement(
                        "UPDATE " + groupTable + " SET last_delivered_id = ? WHERE group_name = ? AND last_delivered_id < ?")) {
                    upd.setLong(1, maxId);
                    upd.setString(2, group);
                    upd.setLong(3, maxId);
                    upd.executeUpdate();
                }
                for (Map<String, Object> row : results) {
                    try (PreparedStatement ins = conn.prepareStatement(
                            "INSERT INTO " + pelTable + " (message_id, group_name, consumer) VALUES (?, ?, ?) " +
                            "ON CONFLICT (message_id, group_name) DO NOTHING")) {
                        ins.setLong(1, (Long) row.get("id"));
                        ins.setString(2, group);
                        ins.setString(3, consumer);
                        ins.executeUpdate();
                    }
                }
            }
        }
        return results;
    }

    public static boolean streamAck(Connection conn, String stream, String group, long messageId) throws SQLException {
        String pelTable = stream + "_pel";
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM " + pelTable + " WHERE message_id = ? AND group_name = ?")) {
            ps.setLong(1, messageId);
            ps.setString(2, group);
            return ps.executeUpdate() > 0;
        }
    }

    public static List<Map<String, Object>> streamClaim(Connection conn, String stream,
            String group, String consumer, long minIdleMs) throws SQLException {
        String pelTable = stream + "_pel";
        List<Long> claimedIds = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE " + pelTable + " SET consumer = ?, delivered_at = NOW() " +
                "WHERE group_name = ? AND delivered_at < NOW() - (? || ' milliseconds')::interval " +
                "RETURNING message_id")) {
            ps.setString(1, consumer);
            ps.setString(2, group);
            ps.setString(3, String.valueOf(minIdleMs));
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                claimedIds.add(rs.getLong(1));
            }
        }
        if (claimedIds.isEmpty()) {
            return new ArrayList<>();
        }
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < claimedIds.size(); i++) {
            if (i > 0) placeholders.append(", ");
            placeholders.append("?");
        }
        List<Map<String, Object>> results = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, payload, created_at FROM " + stream +
                " WHERE id IN (" + placeholders + ") ORDER BY id")) {
            for (int i = 0; i < claimedIds.size(); i++) {
                ps.setLong(i + 1, claimedIds.get(i));
            }
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id", rs.getLong("id"));
                row.put("payload", rs.getString("payload"));
                row.put("created_at", rs.getTimestamp("created_at"));
                results.add(row);
            }
        }
        return results;
    }

    public static String script(Connection conn, String luaCode, String... args) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE EXTENSION IF NOT EXISTS pllua");
        }
        String funcName = "_gl_lua_" + Long.toHexString(Double.doubleToLongBits(Math.random())).substring(0, 8);
        StringBuilder params = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            if (i > 0) params.append(", ");
            params.append("p").append(i + 1).append(" text");
        }
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE OR REPLACE FUNCTION pg_temp." + funcName + "(" + params + ") RETURNS text LANGUAGE pllua AS $pllua$ " + luaCode + " $pllua$");
        }
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            if (i > 0) placeholders.append(", ");
            placeholders.append("?");
        }
        String query = "SELECT pg_temp." + funcName + "(" + placeholders + ")";
        try (PreparedStatement ps = conn.prepareStatement(query)) {
            for (int i = 0; i < args.length; i++) {
                ps.setString(i + 1, args[i]);
            }
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    /**
     * Full-text search with ranking. Like Elasticsearch match query.
     * Uses PostgreSQL tsvector/tsquery under the hood.
     * Returns a list of maps with all columns plus a "_score" field.
     * If highlight is true, adds a "_highlight" field with matched terms wrapped in &lt;mark&gt; tags.
     * Single-column convenience overload.
     */
    public static List<Map<String, Object>> search(Connection conn, String table,
            String column, String query, int limit, String lang, boolean highlight) throws SQLException {
        return search(conn, table, new String[]{column}, query, limit, lang, highlight);
    }

    /**
     * Full-text search with ranking across one or more columns.
     * Like Elasticsearch match query. Uses PostgreSQL tsvector/tsquery.
     * Multiple columns are concatenated for searching.
     * Returns a list of maps with all columns plus a "_score" field.
     * If highlight is true, adds a "_highlight" field with matched terms wrapped in &lt;mark&gt; tags.
     */
    public static List<Map<String, Object>> search(Connection conn, String table,
            String[] columns, String query, int limit, String lang, boolean highlight) throws SQLException {
        validateIdentifier(table);
        for (String col : columns) {
            validateIdentifier(col);
        }

        // Build tsvector expression: coalesce(col1, '') || ' ' || coalesce(col2, '')
        StringBuilder tsvParts = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) tsvParts.append(" || ' ' || ");
            tsvParts.append("coalesce(").append(columns[i]).append(", '')");
        }
        String tsvExpr = "to_tsvector(?, " + tsvParts + ")";
        String highlightCol = columns[0];

        String sql;
        if (highlight) {
            sql = "SELECT *, " +
                "ts_rank(" + tsvExpr + ", plainto_tsquery(?, ?)) AS _score, " +
                "ts_headline(?, coalesce(" + highlightCol + ", ''), plainto_tsquery(?, ?), " +
                "'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15') AS _highlight " +
                "FROM " + table + " " +
                "WHERE " + tsvExpr + " @@ plainto_tsquery(?, ?) " +
                "ORDER BY _score DESC LIMIT ?";
        } else {
            sql = "SELECT *, " +
                "ts_rank(" + tsvExpr + ", plainto_tsquery(?, ?)) AS _score " +
                "FROM " + table + " " +
                "WHERE " + tsvExpr + " @@ plainto_tsquery(?, ?) " +
                "ORDER BY _score DESC LIMIT ?";
        }
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            ps.setString(idx++, lang);
            ps.setString(idx++, lang);
            ps.setString(idx++, query);
            if (highlight) {
                ps.setString(idx++, lang);
                ps.setString(idx++, lang);
                ps.setString(idx++, query);
            }
            ps.setString(idx++, lang);
            ps.setString(idx++, lang);
            ps.setString(idx++, query);
            ps.setInt(idx++, limit);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                results.add(row);
            }
            return results;
        }
    }

    /**
     * Typo-tolerant fuzzy search. Like Elasticsearch fuzzy query.
     * Uses pg_trgm similarity() under the hood.
     * Returns a list of maps with all columns plus a "_score" field.
     */
    public static List<Map<String, Object>> searchFuzzy(Connection conn, String table,
            String column, String query, int limit, double threshold) throws SQLException {
        validateIdentifier(table);
        validateIdentifier(column);
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm");
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT *, similarity(" + column + ", ?) AS _score " +
                "FROM " + table + " " +
                "WHERE similarity(" + column + ", ?) > ? " +
                "ORDER BY _score DESC LIMIT ?")) {
            ps.setString(1, query);
            ps.setString(2, query);
            ps.setDouble(3, threshold);
            ps.setInt(4, limit);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                results.add(row);
            }
            return results;
        }
    }

    /**
     * Sound-alike phonetic search. Like Elasticsearch phonetic plugin.
     * Uses fuzzystrmatch soundex() with pg_trgm similarity for ranking.
     * Returns a list of maps with all columns plus a "_score" field.
     */
    public static List<Map<String, Object>> searchPhonetic(Connection conn, String table,
            String column, String query, int limit) throws SQLException {
        validateIdentifier(table);
        validateIdentifier(column);
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch");
        }
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm");
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT *, similarity(" + column + ", ?) AS _score " +
                "FROM " + table + " " +
                "WHERE soundex(" + column + ") = soundex(?) " +
                "ORDER BY _score DESC, " + column + " LIMIT ?")) {
            ps.setString(1, query);
            ps.setString(2, query);
            ps.setInt(3, limit);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                results.add(row);
            }
            return results;
        }
    }

    /**
     * Vector similarity search. Like Elasticsearch kNN.
     * Uses pgvector's <=> (cosine distance) operator.
     * Returns a list of maps with all columns plus a "_score" field (lower = more similar).
     */
    public static List<Map<String, Object>> similar(Connection conn, String table,
            String column, double[] vector, int limit) throws SQLException {
        validateIdentifier(table);
        validateIdentifier(column);
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE EXTENSION IF NOT EXISTS vector");
        }
        StringBuilder vecLiteral = new StringBuilder("[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) vecLiteral.append(",");
            vecLiteral.append(vector[i]);
        }
        vecLiteral.append("]");
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT *, (" + column + " <=> ?::vector) AS _score " +
                "FROM " + table + " " +
                "ORDER BY _score LIMIT ?")) {
            ps.setString(1, vecLiteral.toString());
            ps.setInt(2, limit);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                results.add(row);
            }
            return results;
        }
    }

    /**
     * Autocomplete/typeahead suggestions. Like Elasticsearch completion suggester.
     * Uses pg_trgm for similarity ranking with ILIKE prefix matching.
     * Returns a list of maps with all columns plus a "_score" field.
     */
    public static List<Map<String, Object>> suggest(Connection conn, String table,
            String column, String prefix, int limit) throws SQLException {
        validateIdentifier(table);
        validateIdentifier(column);
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm");
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT *, similarity(" + column + ", ?) AS _score " +
                "FROM " + table + " " +
                "WHERE " + column + " ILIKE ? " +
                "ORDER BY _score DESC, " + column + " LIMIT ?")) {
            ps.setString(1, prefix);
            ps.setString(2, prefix + "%");
            ps.setInt(3, limit);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                results.add(row);
            }
            return results;
        }
    }

    /**
     * Terms aggregation (faceted counts). Like Elasticsearch terms aggregation.
     * Returns a list of maps with {value, count} for each distinct value in the column.
     * Convenience overload without full-text filtering.
     */
    public static List<Map<String, Object>> facets(Connection conn, String table,
            String column, int limit) throws SQLException {
        return facets(conn, table, column, limit, null, (String[]) null, null);
    }

    /**
     * Terms aggregation with optional full-text search filter.
     * Single-column convenience overload.
     */
    public static List<Map<String, Object>> facets(Connection conn, String table,
            String column, int limit, String query, String queryColumn, String lang) throws SQLException {
        return facets(conn, table, column, limit, query,
                queryColumn != null ? new String[]{queryColumn} : null, lang);
    }

    /**
     * Terms aggregation with optional full-text search filter across multiple columns.
     * Like Elasticsearch terms aggregation with a query filter.
     * Returns a list of maps with {value, count} for each distinct value in the column.
     * When query and queryColumns are non-null, filters rows by full-text search first.
     */
    public static List<Map<String, Object>> facets(Connection conn, String table,
            String column, int limit, String query, String[] queryColumns, String lang) throws SQLException {
        validateIdentifier(table);
        validateIdentifier(column);

        boolean hasQuery = query != null && queryColumns != null && queryColumns.length > 0;

        if (hasQuery) {
            for (String qc : queryColumns) {
                validateIdentifier(qc);
            }
        }

        String sql;
        if (hasQuery) {
            StringBuilder tsvParts = new StringBuilder();
            for (int i = 0; i < queryColumns.length; i++) {
                if (i > 0) tsvParts.append(" || ' ' || ");
                tsvParts.append("coalesce(").append(queryColumns[i]).append(", '')");
            }
            sql = "SELECT " + column + " AS value, COUNT(*) AS count " +
                "FROM " + table + " " +
                "WHERE to_tsvector(?, " + tsvParts + ") @@ plainto_tsquery(?, ?) " +
                "GROUP BY " + column + " ORDER BY count DESC, " + column + " LIMIT ?";
        } else {
            sql = "SELECT " + column + " AS value, COUNT(*) AS count " +
                "FROM " + table + " " +
                "GROUP BY " + column + " ORDER BY count DESC, " + column + " LIMIT ?";
        }

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            if (hasQuery) {
                ps.setString(idx++, lang);
                ps.setString(idx++, lang);
                ps.setString(idx++, query);
            }
            ps.setInt(idx++, limit);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                results.add(row);
            }
            return results;
        }
    }

    private static final java.util.Set<String> AGGREGATE_FUNCS =
        java.util.Set.of("count", "sum", "avg", "min", "max");

    /**
     * Aggregate function on a column. Like Elasticsearch metric aggregation.
     * Convenience overload without groupBy — returns a single-row result.
     */
    public static List<Map<String, Object>> aggregate(Connection conn, String table,
            String column, String func) throws SQLException {
        return aggregate(conn, table, column, func, null, 0);
    }

    /**
     * Aggregate function on a column with optional groupBy.
     * Like Elasticsearch metric aggregation with bucket grouping.
     * func must be one of: count, sum, avg, min, max.
     * With groupBy: returns grouped results ordered by value DESC.
     * Without groupBy: returns a single row with the aggregate value.
     */
    public static List<Map<String, Object>> aggregate(Connection conn, String table,
            String column, String func, String groupBy, int limit) throws SQLException {
        validateIdentifier(table);
        validateIdentifier(column);
        String funcLower = func.toLowerCase();
        if (!AGGREGATE_FUNCS.contains(funcLower)) {
            throw new IllegalArgumentException(
                "Invalid aggregate function: " + func + ". Must be one of: count, sum, avg, min, max");
        }

        String funcExpr = funcLower.equals("count")
            ? "COUNT(*)"
            : funcLower.toUpperCase() + "(" + column + ")";

        String sql;
        if (groupBy != null) {
            validateIdentifier(groupBy);
            sql = "SELECT " + groupBy + ", " + funcExpr + " AS value " +
                "FROM " + table + " " +
                "GROUP BY " + groupBy + " ORDER BY value DESC LIMIT ?";
        } else {
            sql = "SELECT " + funcExpr + " AS value FROM " + table;
        }

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (groupBy != null) {
                ps.setInt(1, limit);
            }
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                results.add(row);
            }
            return results;
        }
    }

    /**
     * Create a custom text search configuration.
     * Convenience overload that copies from "english" by default.
     */
    public static void createSearchConfig(Connection conn, String name) throws SQLException {
        createSearchConfig(conn, name, "english");
    }

    /**
     * Create a custom text search configuration if it doesn't already exist.
     * Like Elasticsearch custom analyzer creation.
     * Copies from an existing configuration (default: english).
     */
    public static void createSearchConfig(Connection conn, String name, String copyFrom) throws SQLException {
        validateIdentifier(name);
        validateIdentifier(copyFrom);

        boolean exists = false;
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM pg_ts_config WHERE cfgname = ?")) {
            ps.setString(1, name);
            ResultSet rs = ps.executeQuery();
            exists = rs.next();
        }

        if (!exists) {
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TEXT SEARCH CONFIGURATION " + name + " (COPY = " + copyFrom + ")");
            }
        }
    }

    /**
     * Register a named query for reverse matching. Like Elasticsearch percolator.
     * Convenience overload with lang="english" and no metadata.
     */
    public static void percolateAdd(Connection conn, String name, String queryId,
            String query) throws SQLException {
        percolateAdd(conn, name, queryId, query, "english", null);
    }

    /**
     * Register a named query for reverse matching. Like Elasticsearch percolator.
     * Creates the percolator table and GIN index if they don't exist.
     * Upserts the query — if queryId already exists, updates it.
     */
    public static void percolateAdd(Connection conn, String name, String queryId,
            String query, String lang, String metadataJson) throws SQLException {
        validateIdentifier(name);
        try (Statement st = conn.createStatement()) {
            st.execute(
                "CREATE TABLE IF NOT EXISTS " + name + " (" +
                "query_id TEXT PRIMARY KEY, " +
                "query_text TEXT NOT NULL, " +
                "tsquery TSQUERY NOT NULL, " +
                "lang TEXT NOT NULL DEFAULT 'english', " +
                "metadata JSONB, " +
                "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
            );
        }
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE INDEX IF NOT EXISTS " + name + "_tsq_idx ON " + name + " USING GIN (tsquery)");
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + name + " (query_id, query_text, tsquery, lang, metadata) " +
                "VALUES (?, ?, plainto_tsquery(?, ?), ?, ?) " +
                "ON CONFLICT (query_id) DO UPDATE SET " +
                "query_text = EXCLUDED.query_text, " +
                "tsquery = EXCLUDED.tsquery, " +
                "lang = EXCLUDED.lang, " +
                "metadata = EXCLUDED.metadata")) {
            ps.setString(1, queryId);
            ps.setString(2, query);
            ps.setString(3, lang);
            ps.setString(4, query);
            ps.setString(5, lang);
            if (metadataJson != null) {
                ps.setString(6, metadataJson);
            } else {
                ps.setNull(6, Types.OTHER);
            }
            ps.executeUpdate();
        }
    }

    /**
     * Match a document against stored queries. Like Elasticsearch percolate API.
     * Convenience overload with limit=50 and lang="english".
     */
    public static List<Map<String, Object>> percolate(Connection conn, String name,
            String text) throws SQLException {
        return percolate(conn, name, text, 50, "english");
    }

    /**
     * Match a document against stored queries. Like Elasticsearch percolate API.
     * Returns matching queries ranked by relevance score.
     */
    public static List<Map<String, Object>> percolate(Connection conn, String name,
            String text, int limit, String lang) throws SQLException {
        validateIdentifier(name);
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT query_id, query_text, metadata, " +
                "ts_rank(to_tsvector(?, ?), tsquery) AS _score " +
                "FROM " + name + " " +
                "WHERE to_tsvector(?, ?) @@ tsquery " +
                "ORDER BY _score DESC LIMIT ?")) {
            ps.setString(1, lang);
            ps.setString(2, text);
            ps.setString(3, lang);
            ps.setString(4, text);
            ps.setInt(5, limit);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                results.add(row);
            }
            return results;
        }
    }

    /**
     * Remove a stored query from a percolator index. Like Elasticsearch delete percolator query.
     * Returns true if the query existed, false otherwise.
     */
    public static boolean percolateDelete(Connection conn, String name,
            String queryId) throws SQLException {
        validateIdentifier(name);
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM " + name + " WHERE query_id = ? RETURNING query_id")) {
            ps.setString(1, queryId);
            ResultSet rs = ps.executeQuery();
            return rs.next();
        }
    }

    /**
     * Analyze how text is tokenized by the full-text search engine.
     * Like Elasticsearch _analyze API. Uses ts_debug() under the hood.
     * Returns a list of maps with alias, description, token, dictionaries, dictionary, lexemes.
     * Convenience overload with lang="english".
     */
    public static List<Map<String, Object>> analyze(Connection conn, String text) throws SQLException {
        return analyze(conn, text, "english");
    }

    /**
     * Analyze how text is tokenized by the full-text search engine.
     * Like Elasticsearch _analyze API. Uses ts_debug() under the hood.
     * Returns a list of maps with alias, description, token, dictionaries, dictionary, lexemes.
     */
    public static List<Map<String, Object>> analyze(Connection conn, String text, String lang) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT alias, description, token, dictionaries, dictionary, lexemes FROM ts_debug(?, ?)")) {
            ps.setString(1, lang);
            ps.setString(2, text);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                results.add(row);
            }
            return results;
        }
    }

    /**
     * Explain how a specific row scores against a full-text query.
     * Like Elasticsearch _explain API. Shows document tokens, query tokens,
     * match status, rank score, and headline with matched terms highlighted.
     * Convenience overload with lang="english".
     */
    public static Map<String, Object> explainScore(Connection conn, String table, String column,
            String query, String idColumn, Object idValue) throws SQLException {
        return explainScore(conn, table, column, query, idColumn, idValue, "english");
    }

    /**
     * Explain how a specific row scores against a full-text query.
     * Like Elasticsearch _explain API. Shows document tokens, query tokens,
     * match status, rank score, and headline with matched terms highlighted.
     * Returns a single map or null if the row is not found.
     */
    public static Map<String, Object> explainScore(Connection conn, String table, String column,
            String query, String idColumn, Object idValue, String lang) throws SQLException {
        validateIdentifier(table);
        validateIdentifier(column);
        validateIdentifier(idColumn);

        String sql = "SELECT " + column + " AS document_text, to_tsvector(?, " + column + ")::text AS document_tokens, " +
            "plainto_tsquery(?, ?)::text AS query_tokens, " +
            "to_tsvector(?, " + column + ") @@ plainto_tsquery(?, ?) AS matches, " +
            "ts_rank(to_tsvector(?, " + column + "), plainto_tsquery(?, ?)) AS score, " +
            "ts_headline(?, " + column + ", plainto_tsquery(?, ?), " +
            "'StartSel=**, StopSel=**, MaxWords=50, MinWords=20') AS headline " +
            "FROM " + table + " WHERE " + idColumn + " = ?";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            ps.setString(idx++, lang);
            ps.setString(idx++, lang);
            ps.setString(idx++, query);
            ps.setString(idx++, lang);
            ps.setString(idx++, lang);
            ps.setString(idx++, query);
            ps.setString(idx++, lang);
            ps.setString(idx++, lang);
            ps.setString(idx++, query);
            ps.setString(idx++, lang);
            ps.setString(idx++, lang);
            ps.setString(idx++, query);
            ps.setObject(idx++, idValue);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                return null;
            }
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= colCount; i++) {
                row.put(meta.getColumnLabel(i), rs.getObject(i));
            }
            return row;
        }
    }

    // =========================================================================
    // Document store (MongoDB-like CRUD on JSONB)
    // =========================================================================

    private static final Pattern FIELD_PART_RE = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    private static final Map<String, String> COMPARISON_OPS = Map.of(
        "$gt", ">", "$gte", ">=", "$lt", "<", "$lte", "<=",
        "$eq", "=", "$ne", "!="
    );

    private static final Set<String> SUPPORTED_FILTER_OPS =
        Set.of("$gt", "$gte", "$lt", "$lte", "$eq", "$ne", "$in", "$nin", "$exists", "$regex", "$elemMatch", "$text");

    private static final Set<String> LOGICAL_OPS = Set.of("$or", "$and", "$not");

    private static final Set<String> UPDATE_OPS =
        Set.of("$set", "$unset", "$inc", "$mul", "$rename", "$push", "$pull", "$addToSet");

    static class FilterResult {
        final String whereClause;
        final List<Object> params;

        FilterResult(String whereClause, List<Object> params) {
            this.whereClause = whereClause;
            this.params = params;
        }
    }

    static class UpdateResult {
        final String expr;
        final List<Object> params;

        UpdateResult(String expr, List<Object> params) {
            this.expr = expr;
            this.params = params;
        }
    }

    static String fieldPath(String key) {
        String[] parts = key.split("\\.");
        for (String part : parts) {
            if (!FIELD_PART_RE.matcher(part).matches()) {
                throw new IllegalArgumentException("Invalid filter key: " + key);
            }
        }
        if (parts.length == 1) {
            return "data->>'" + parts[0] + "'";
        }
        StringBuilder sb = new StringBuilder("data");
        for (int i = 0; i < parts.length - 1; i++) {
            sb.append("->'").append(parts[i]).append("'");
        }
        sb.append("->>'").append(parts[parts.length - 1]).append("'");
        return sb.toString();
    }

    static String fieldPathJson(String key) {
        String[] parts = key.split("\\.");
        for (String part : parts) {
            if (!FIELD_PART_RE.matcher(part).matches()) {
                throw new IllegalArgumentException("Invalid field key: " + key);
            }
        }
        StringBuilder sb = new StringBuilder("data");
        for (String part : parts) {
            sb.append("->'").append(part).append("'");
        }
        return sb.toString();
    }

    static String jsonbPath(String key) {
        String[] parts = key.split("\\.");
        for (String part : parts) {
            if (!FIELD_PART_RE.matcher(part).matches()) {
                throw new IllegalArgumentException("Invalid field key: " + key);
            }
        }
        return "{" + String.join(",", parts) + "}";
    }

    private static String[] toJsonbExpr(String value) {
        value = value.trim();
        if (value.equals("true") || value.equals("false")) {
            return new String[]{"to_jsonb(?::boolean)", value};
        } else if (isNumericLiteral(value)) {
            return new String[]{"to_jsonb(?::numeric)", value};
        } else if (value.startsWith("\"") && value.endsWith("\"")) {
            return new String[]{"to_jsonb(?::text)", unquote(value)};
        } else {
            // JSON object/array — pass as jsonb
            return new String[]{"?::jsonb", value};
        }
    }

    private static List<String> parseJsonObjectArray(String s) {
        s = s.trim();
        if (!s.startsWith("[") || !s.endsWith("]")) {
            throw new IllegalArgumentException("Expected JSON array: " + s);
        }
        String inner = s.substring(1, s.length() - 1).trim();
        if (inner.isEmpty()) {
            return new ArrayList<>();
        }
        List<String> objects = new ArrayList<>();
        int depth = 0;
        int start = -1;
        boolean inString = false;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '"') {
                int bs = 0;
                for (int j = i - 1; j >= 0 && inner.charAt(j) == '\\'; j--) bs++;
                if (bs % 2 == 0) inString = !inString;
            } else if (!inString) {
                if (c == '{') {
                    if (depth == 0) start = i;
                    depth++;
                } else if (c == '}') {
                    depth--;
                    if (depth == 0 && start >= 0) {
                        objects.add(inner.substring(start, i + 1));
                        start = -1;
                    }
                }
            }
        }
        return objects;
    }

    static FilterResult buildFilter(String filterJson) {
        if (filterJson == null || filterJson.trim().isEmpty()) {
            return new FilterResult("", new ArrayList<>());
        }
        String s = filterJson.trim();
        if (!s.startsWith("{") || !s.endsWith("}")) {
            throw new IllegalArgumentException("Filter must be a JSON object");
        }
        String body = s.substring(1, s.length() - 1).trim();
        if (body.isEmpty()) {
            return new FilterResult("", new ArrayList<>());
        }

        List<String[]> pairs = splitKeyValuePairs(body);

        // Fast path: if no values contain operator keys and no logical operators,
        // treat the entire filter as a JSONB containment query and pass through the original string
        boolean hasOperators = false;
        boolean hasLogical = false;
        for (String[] kv : pairs) {
            String key = kv[0].trim().replaceAll("^\"|\"$", "");
            if (LOGICAL_OPS.contains(key) || key.equals("$text")) {
                hasLogical = true;
                hasOperators = true;
                break;
            }
            String val = kv[1].trim();
            if (val.startsWith("{") && hasOperatorKeys(val)) {
                hasOperators = true;
                break;
            }
        }
        if (!hasOperators) {
            // Check if any keys contain dots — if so, expand to nested objects
            boolean hasDots = false;
            for (String[] kv : pairs) {
                String key = kv[0].trim().replaceAll("^\"|\"$", "");
                if (key.contains(".")) {
                    hasDots = true;
                    break;
                }
            }
            List<Object> params = new ArrayList<>();
            if (hasDots) {
                Map<String, String> containment = new LinkedHashMap<>();
                for (String[] kv : pairs) {
                    String key = kv[0].trim().replaceAll("^\"|\"$", "");
                    containment.put(key, kv[1].trim());
                }
                params.add(rebuildJsonObject(containment));
            } else {
                params.add(s);
            }
            return new FilterResult("data @> ?::jsonb", params);
        }

        Map<String, String> containment = new LinkedHashMap<>();
        List<String> clauses = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        for (String[] kv : pairs) {
            String key = kv[0].trim().replaceAll("^\"|\"$", "");
            String val = kv[1].trim();

            if (key.equals("$text")) {
                // Top-level $text: search across entire document
                if (!val.startsWith("{")) {
                    throw new IllegalArgumentException("$text requires {$search: 'query'}");
                }
                List<String[]> textPairs = splitKeyValuePairs(
                    val.substring(1, val.length() - 1).trim());
                String searchQuery = null;
                String lang = "english";
                for (String[] tp : textPairs) {
                    String tk = tp[0].trim().replaceAll("^\"|\"$", "");
                    String tv = tp[1].trim();
                    if (tk.equals("$search")) {
                        searchQuery = unquote(tv);
                    } else if (tk.equals("$language")) {
                        lang = unquote(tv);
                    }
                }
                if (searchQuery == null) {
                    throw new IllegalArgumentException("$text requires {$search: 'query'}");
                }
                clauses.add("to_tsvector(?, data::text) @@ plainto_tsquery(?, ?)");
                params.add(lang);
                params.add(lang);
                params.add(searchQuery);
            } else if (LOGICAL_OPS.contains(key)) {
                if (key.equals("$not")) {
                    if (!val.startsWith("{")) {
                        throw new IllegalArgumentException("$not value must be a filter object");
                    }
                    FilterResult sub = buildFilter(val);
                    if (!sub.whereClause.isEmpty()) {
                        clauses.add("NOT (" + sub.whereClause + ")");
                        params.addAll(sub.params);
                    }
                } else {
                    // $or / $and
                    List<String> subObjects = parseJsonObjectArray(val);
                    if (subObjects.isEmpty()) {
                        throw new IllegalArgumentException(key + " value must be a non-empty array");
                    }
                    String joiner = key.equals("$or") ? " OR " : " AND ";
                    List<String> subClauses = new ArrayList<>();
                    for (String subObj : subObjects) {
                        FilterResult sub = buildFilter(subObj);
                        if (!sub.whereClause.isEmpty()) {
                            subClauses.add(sub.whereClause);
                            params.addAll(sub.params);
                        }
                    }
                    if (!subClauses.isEmpty()) {
                        clauses.add("(" + String.join(joiner, subClauses) + ")");
                    }
                }
            } else if (val.startsWith("{") && hasOperatorKeys(val)) {
                String fieldExpr = fieldPath(key);
                List<String[]> opPairs = splitKeyValuePairs(
                    val.substring(1, val.length() - 1).trim());
                for (String[] opKv : opPairs) {
                    String op = opKv[0].trim().replaceAll("^\"|\"$", "");
                    String operand = opKv[1].trim();
                    if (!SUPPORTED_FILTER_OPS.contains(op)) {
                        throw new IllegalArgumentException("Unsupported filter operator: " + op);
                    }

                    if (COMPARISON_OPS.containsKey(op)) {
                        String sqlOp = COMPARISON_OPS.get(op);
                        if (isNumericLiteral(operand)) {
                            clauses.add("(" + fieldExpr + ")::numeric " + sqlOp + " ?");
                            params.add(Double.parseDouble(operand));
                        } else {
                            String strVal = unquote(operand);
                            clauses.add(fieldExpr + " " + sqlOp + " ?");
                            params.add(strVal);
                        }
                    } else if (op.equals("$in") || op.equals("$nin")) {
                        List<String> elements = parseJsonArray(operand);
                        if (elements.isEmpty()) {
                            if (op.equals("$in")) {
                                clauses.add("FALSE");
                            }
                            // $nin with empty array matches everything, no clause needed
                        } else {
                            StringBuilder placeholders = new StringBuilder();
                            for (int i = 0; i < elements.size(); i++) {
                                if (i > 0) placeholders.append(", ");
                                placeholders.append("?");
                            }
                            String notPrefix = op.equals("$nin") ? "NOT " : "";
                            clauses.add(fieldExpr + " " + notPrefix + "IN (" + placeholders + ")");
                            for (String elem : elements) {
                                params.add(unquote(elem.trim()));
                            }
                        }
                    } else if (op.equals("$exists")) {
                        String topKey = key.split("\\.")[0];
                        boolean exists = operand.equals("true");
                        if (exists) {
                            clauses.add("data ?? ?");
                        } else {
                            clauses.add("NOT (data ?? ?)");
                        }
                        params.add(topKey);
                    } else if (op.equals("$regex")) {
                        String pattern = unquote(operand);
                        clauses.add(fieldExpr + " ~ ?");
                        params.add(pattern);
                    } else if (op.equals("$elemMatch")) {
                        if (!operand.startsWith("{")) {
                            throw new IllegalArgumentException("$elemMatch value must be an object");
                        }
                        String fieldJson = fieldPathJson(key);
                        List<String[]> subPairs = splitKeyValuePairs(
                            operand.substring(1, operand.length() - 1).trim());
                        List<String> elemClauses = new ArrayList<>();
                        for (String[] sp : subPairs) {
                            String subOp = sp[0].trim().replaceAll("^\"|\"$", "");
                            String subVal = sp[1].trim();
                            if (COMPARISON_OPS.containsKey(subOp)) {
                                String sqlOp = COMPARISON_OPS.get(subOp);
                                if (isNumericLiteral(subVal)) {
                                    elemClauses.add("(elem#>>'{}')::numeric " + sqlOp + " ?");
                                    params.add(Double.parseDouble(subVal));
                                } else {
                                    elemClauses.add("elem#>>'{}' " + sqlOp + " ?");
                                    params.add(unquote(subVal));
                                }
                            } else if (subOp.equals("$regex")) {
                                elemClauses.add("elem#>>'{}' ~ ?");
                                params.add(unquote(subVal));
                            } else {
                                throw new IllegalArgumentException(
                                    "Unsupported $elemMatch operator: " + subOp);
                            }
                        }
                        if (!elemClauses.isEmpty()) {
                            clauses.add("EXISTS (SELECT 1 FROM jsonb_array_elements(" +
                                fieldJson + ") AS elem WHERE " +
                                String.join(" AND ", elemClauses) + ")");
                        }
                    } else if (op.equals("$text")) {
                        if (!operand.startsWith("{")) {
                            throw new IllegalArgumentException("$text requires {$search: 'query'}");
                        }
                        List<String[]> textPairs = splitKeyValuePairs(
                            operand.substring(1, operand.length() - 1).trim());
                        String searchQuery = null;
                        String lang = "english";
                        for (String[] tp : textPairs) {
                            String tk = tp[0].trim().replaceAll("^\"|\"$", "");
                            String tv = tp[1].trim();
                            if (tk.equals("$search")) {
                                searchQuery = unquote(tv);
                            } else if (tk.equals("$language")) {
                                lang = unquote(tv);
                            }
                        }
                        if (searchQuery == null) {
                            throw new IllegalArgumentException("$text requires {$search: 'query'}");
                        }
                        clauses.add("to_tsvector(?, " + fieldExpr + ") @@ plainto_tsquery(?, ?)");
                        params.add(lang);
                        params.add(lang);
                        params.add(searchQuery);
                    }
                }
            } else {
                containment.put(key, val);
            }
        }

        List<String> allClauses = new ArrayList<>();
        List<Object> allParams = new ArrayList<>();
        if (!containment.isEmpty()) {
            allClauses.add("data @> ?::jsonb");
            allParams.add(rebuildJsonObject(containment));
        }
        allClauses.addAll(clauses);
        allParams.addAll(params);

        if (allClauses.isEmpty()) {
            return new FilterResult("", allParams);
        }
        return new FilterResult(String.join(" AND ", allClauses), allParams);
    }

    private static boolean hasOperatorKeys(String objStr) {
        String inner = objStr.substring(1, objStr.length() - 1).trim();
        if (inner.isEmpty()) return false;
        // Check if first key starts with $
        String firstKey;
        if (inner.charAt(0) == '"') {
            int end = inner.indexOf('"', 1);
            if (end < 0) return false;
            firstKey = inner.substring(1, end);
        } else {
            int colon = inner.indexOf(':');
            if (colon < 0) return false;
            firstKey = inner.substring(0, colon).trim();
        }
        return firstKey.startsWith("$");
    }

    private static boolean isNumericLiteral(String s) {
        if (s.isEmpty()) return false;
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static String unquote(String s) {
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private static List<String> parseJsonArray(String s) {
        s = s.trim();
        if (!s.startsWith("[") || !s.endsWith("]")) {
            throw new IllegalArgumentException("Expected JSON array: " + s);
        }
        String inner = s.substring(1, s.length() - 1).trim();
        if (inner.isEmpty()) {
            return new ArrayList<>();
        }
        List<String> elements = new ArrayList<>();
        int depth = 0;
        int start = 0;
        boolean inString = false;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '"') {
                int bs = 0;
                for (int j = i - 1; j >= 0 && inner.charAt(j) == '\\'; j--) bs++;
                if (bs % 2 == 0) inString = !inString;
            } else if (!inString) {
                if (c == '{' || c == '[') depth++;
                else if (c == '}' || c == ']') depth--;
                else if (c == ',' && depth == 0) {
                    elements.add(inner.substring(start, i).trim());
                    start = i + 1;
                }
            }
        }
        elements.add(inner.substring(start).trim());
        return elements;
    }

    private static String rebuildJsonObject(Map<String, String> pairs) {
        // Expand dot-notation keys into nested objects before serializing.
        // e.g. {"address.city": "\"NYC\""} -> {"address": {"city": "NYC"}}
        // Multiple keys sharing a prefix merge:
        // {"a.b": "1", "a.c": "2"} -> {"a": {"b": 1, "c": 2}}
        Map<String, Object> tree = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : pairs.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();
            String[] parts = key.split("\\.");
            if (parts.length == 1) {
                tree.put(key, val);
            } else {
                // Walk/create nested maps for intermediate parts
                Map<String, Object> current = tree;
                for (int i = 0; i < parts.length - 1; i++) {
                    Object existing = current.get(parts[i]);
                    if (existing instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> nested = (Map<String, Object>) existing;
                        current = nested;
                    } else {
                        Map<String, Object> nested = new LinkedHashMap<>();
                        current.put(parts[i], nested);
                        current = nested;
                    }
                }
                current.put(parts[parts.length - 1], val);
            }
        }
        return serializeJsonTree(tree);
    }

    private static String serializeJsonTree(Map<String, Object> tree) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : tree.entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            String key = entry.getKey();
            if (!key.startsWith("\"")) {
                sb.append("\"").append(key).append("\"");
            } else {
                sb.append(key);
            }
            sb.append(": ");
            Object val = entry.getValue();
            if (val instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nested = (Map<String, Object>) val;
                sb.append(serializeJsonTree(nested));
            } else {
                sb.append(val);
            }
        }
        sb.append("}");
        return sb.toString();
    }

    static UpdateResult buildUpdate(String updateJson) {
        if (updateJson == null || updateJson.trim().isEmpty()) {
            throw new IllegalArgumentException("Update must not be null or empty");
        }
        String s = updateJson.trim();
        if (!s.startsWith("{") || !s.endsWith("}")) {
            throw new IllegalArgumentException("Update must be a JSON object");
        }
        String body = s.substring(1, s.length() - 1).trim();
        if (body.isEmpty()) {
            return new UpdateResult("data || ?::jsonb", List.of(s));
        }

        List<String[]> pairs = splitKeyValuePairs(body);

        // Check if any top-level key starts with $
        boolean hasUpdateOps = false;
        for (String[] kv : pairs) {
            String key = kv[0].trim().replaceAll("^\"|\"$", "");
            if (key.startsWith("$")) {
                hasUpdateOps = true;
                break;
            }
        }

        if (!hasUpdateOps) {
            // Plain merge: data || update::jsonb
            return new UpdateResult("data || ?::jsonb", List.of(s));
        }

        String expr = "data";
        List<Object> params = new ArrayList<>();

        for (String[] kv : pairs) {
            String opKey = kv[0].trim().replaceAll("^\"|\"$", "");
            String opVal = kv[1].trim();

            if (!UPDATE_OPS.contains(opKey)) {
                throw new IllegalArgumentException("Unsupported update operator: " + opKey);
            }

            switch (opKey) {
                case "$set": {
                    expr = "(" + expr + " || ?::jsonb)";
                    params.add(opVal);
                    break;
                }
                case "$unset": {
                    // opVal is an object like {"field1": "", "field2": ""}
                    if (!opVal.startsWith("{") || !opVal.endsWith("}")) {
                        throw new IllegalArgumentException("$unset value must be an object");
                    }
                    List<String[]> unsetPairs = splitKeyValuePairs(
                        opVal.substring(1, opVal.length() - 1).trim());
                    for (String[] up : unsetPairs) {
                        String field = up[0].trim().replaceAll("^\"|\"$", "");
                        String[] parts = field.split("\\.");
                        for (String part : parts) {
                            if (!FIELD_PART_RE.matcher(part).matches()) {
                                throw new IllegalArgumentException("Invalid field key: " + field);
                            }
                        }
                        if (parts.length == 1) {
                            expr = "(" + expr + " - ?)";
                            params.add(field);
                        } else {
                            String path = "{" + String.join(",", parts) + "}";
                            expr = "(" + expr + " #- ?::text[])";
                            params.add(path);
                        }
                    }
                    break;
                }
                case "$inc": {
                    if (!opVal.startsWith("{") || !opVal.endsWith("}")) {
                        throw new IllegalArgumentException("$inc value must be an object");
                    }
                    List<String[]> incPairs = splitKeyValuePairs(
                        opVal.substring(1, opVal.length() - 1).trim());
                    for (String[] ip : incPairs) {
                        String field = ip[0].trim().replaceAll("^\"|\"$", "");
                        String amount = ip[1].trim();
                        String jp = jsonbPath(field);
                        String fp = fieldPath(field);
                        expr = "jsonb_set(" + expr + ", ?::text[], to_jsonb(COALESCE((" + fp + ")::numeric, 0) + ?))";
                        params.add(jp);
                        params.add(Double.parseDouble(amount));
                    }
                    break;
                }
                case "$mul": {
                    if (!opVal.startsWith("{") || !opVal.endsWith("}")) {
                        throw new IllegalArgumentException("$mul value must be an object");
                    }
                    List<String[]> mulPairs = splitKeyValuePairs(
                        opVal.substring(1, opVal.length() - 1).trim());
                    for (String[] mp : mulPairs) {
                        String field = mp[0].trim().replaceAll("^\"|\"$", "");
                        String factor = mp[1].trim();
                        String jp = jsonbPath(field);
                        String fp = fieldPath(field);
                        expr = "jsonb_set(" + expr + ", ?::text[], to_jsonb(COALESCE((" + fp + ")::numeric, 0) * ?))";
                        params.add(jp);
                        params.add(Double.parseDouble(factor));
                    }
                    break;
                }
                case "$rename": {
                    if (!opVal.startsWith("{") || !opVal.endsWith("}")) {
                        throw new IllegalArgumentException("$rename value must be an object");
                    }
                    List<String[]> renamePairs = splitKeyValuePairs(
                        opVal.substring(1, opVal.length() - 1).trim());
                    for (String[] rp : renamePairs) {
                        String oldName = rp[0].trim().replaceAll("^\"|\"$", "");
                        String newName = rp[1].trim().replaceAll("^\"|\"$", "");
                        for (String part : oldName.split("\\.")) {
                            if (!FIELD_PART_RE.matcher(part).matches()) {
                                throw new IllegalArgumentException("Invalid field key: " + oldName);
                            }
                        }
                        for (String part : newName.split("\\.")) {
                            if (!FIELD_PART_RE.matcher(part).matches()) {
                                throw new IllegalArgumentException("Invalid field key: " + newName);
                            }
                        }
                        String oldJson = fieldPathJson(oldName);
                        String newJp = jsonbPath(newName);
                        if (oldName.contains(".")) {
                            String oldPath = "{" + String.join(",", oldName.split("\\.")) + "}";
                            expr = "jsonb_set((" + expr + " #- ?::text[]), ?::text[], " + oldJson + ")";
                            params.add(oldPath);
                            params.add(newJp);
                        } else {
                            expr = "jsonb_set((" + expr + " - ?), ?::text[], " + oldJson + ")";
                            params.add(oldName);
                            params.add(newJp);
                        }
                    }
                    break;
                }
                case "$push": {
                    if (!opVal.startsWith("{") || !opVal.endsWith("}")) {
                        throw new IllegalArgumentException("$push value must be an object");
                    }
                    List<String[]> pushPairs = splitKeyValuePairs(
                        opVal.substring(1, opVal.length() - 1).trim());
                    for (String[] pp : pushPairs) {
                        String field = pp[0].trim().replaceAll("^\"|\"$", "");
                        String value = pp[1].trim();
                        String jp = jsonbPath(field);
                        String fj = fieldPathJson(field);
                        String[] valExpr = toJsonbExpr(value);
                        expr = "jsonb_set(" + expr + ", ?::text[], COALESCE(" + fj + ", '[]'::jsonb) || " + valExpr[0] + ")";
                        params.add(jp);
                        params.add(valExpr[1]);
                    }
                    break;
                }
                case "$pull": {
                    if (!opVal.startsWith("{") || !opVal.endsWith("}")) {
                        throw new IllegalArgumentException("$pull value must be an object");
                    }
                    List<String[]> pullPairs = splitKeyValuePairs(
                        opVal.substring(1, opVal.length() - 1).trim());
                    for (String[] pp : pullPairs) {
                        String field = pp[0].trim().replaceAll("^\"|\"$", "");
                        String value = pp[1].trim();
                        String jp = jsonbPath(field);
                        String fj = fieldPathJson(field);
                        String[] valExpr = toJsonbExpr(value);
                        expr = "jsonb_set(" + expr + ", ?::text[], " +
                            "COALESCE((SELECT jsonb_agg(elem) FROM jsonb_array_elements(" + fj + ") AS elem " +
                            "WHERE elem != " + valExpr[0] + "), '[]'::jsonb))";
                        params.add(jp);
                        params.add(valExpr[1]);
                    }
                    break;
                }
                case "$addToSet": {
                    if (!opVal.startsWith("{") || !opVal.endsWith("}")) {
                        throw new IllegalArgumentException("$addToSet value must be an object");
                    }
                    List<String[]> asPairs = splitKeyValuePairs(
                        opVal.substring(1, opVal.length() - 1).trim());
                    for (String[] ap : asPairs) {
                        String field = ap[0].trim().replaceAll("^\"|\"$", "");
                        String value = ap[1].trim();
                        String jp = jsonbPath(field);
                        String fj = fieldPathJson(field);
                        String[] valExpr = toJsonbExpr(value);
                        expr = "jsonb_set(" + expr + ", ?::text[], " +
                            "CASE WHEN COALESCE(" + fj + ", '[]'::jsonb) @> " + valExpr[0] + " " +
                            "THEN " + fj + " " +
                            "ELSE COALESCE(" + fj + ", '[]'::jsonb) || " + valExpr[0] + " END)";
                        params.add(jp);
                        // $addToSet uses the value expression 3 times but only needs params for ? placeholders
                        params.add(valExpr[1]);
                        params.add(valExpr[1]);
                    }
                    break;
                }
            }
        }

        return new UpdateResult(expr, params);
    }

    private static void setFilterParam(PreparedStatement ps, int idx, Object param) throws SQLException {
        if (param instanceof Double) {
            ps.setDouble(idx, (Double) param);
        } else {
            ps.setString(idx, param.toString());
        }
    }

    private static void ensureCollection(Connection conn, String collection) throws SQLException {
        ensureCollection(conn, collection, false);
    }

    private static void ensureCollection(Connection conn, String collection, boolean unlogged) throws SQLException {
        validateIdentifier(collection);
        String prefix = unlogged ? "CREATE UNLOGGED TABLE" : "CREATE TABLE";
        try (Statement st = conn.createStatement()) {
            st.execute(
                prefix + " IF NOT EXISTS " + collection + " (" +
                "_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), " +
                "data JSONB NOT NULL, " +
                "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), " +
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
            );
        }
    }

    /**
     * Explicitly create a collection table. Like MongoDB createCollection().
     * Optionally creates an UNLOGGED table for high-throughput ephemeral data.
     * UNLOGGED tables are not crash-safe but significantly faster for writes.
     */
    public static void docCreateCollection(Connection conn, String collection,
            boolean unlogged) throws SQLException {
        validateIdentifier(collection);
        ensureCollection(conn, collection, unlogged);
    }

    public static void docCreateCollection(Connection conn, String collection) throws SQLException {
        docCreateCollection(conn, collection, false);
    }

    private static Map<String, Object> rowToMap(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= colCount; i++) {
            row.put(meta.getColumnLabel(i), rs.getObject(i));
        }
        return row;
    }

    /**
     * Insert a document into a collection. Like MongoDB insertOne().
     * Creates the collection table if it doesn't exist.
     * Returns the inserted row (_id, data, created_at, updated_at).
     */
    public static Map<String, Object> docInsert(Connection conn, String collection,
            String documentJson) throws SQLException {
        ensureCollection(conn, collection);
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + collection + " (data) VALUES (?::jsonb) " +
                "RETURNING _id, data, created_at, updated_at")) {
            ps.setString(1, documentJson);
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rowToMap(rs);
        }
    }

    /**
     * Insert multiple documents into a collection. Like MongoDB insertMany().
     * Creates the collection table if it doesn't exist.
     * Returns the list of inserted rows.
     */
    public static List<Map<String, Object>> docInsertMany(Connection conn, String collection,
            List<String> documents) throws SQLException {
        ensureCollection(conn, collection);
        List<Map<String, Object>> results = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + collection + " (data) VALUES (?::jsonb) " +
                "RETURNING _id, data, created_at, updated_at")) {
            for (String doc : documents) {
                ps.setString(1, doc);
                ResultSet rs = ps.executeQuery();
                rs.next();
                results.add(rowToMap(rs));
            }
        }
        return results;
    }

    /**
     * Parse a MongoDB-like sort JSON object into an ORDER BY clause.
     * Input: {"name": 1, "age": -1} => "data->>'name' ASC, data->>'age' DESC"
     * Each key must be a valid identifier. Values: 1 for ASC, -1 for DESC.
     * Returns empty string if sortJson is null or empty.
     */
    static String parseSortClause(String sortJson) {
        if (sortJson == null || sortJson.trim().isEmpty()) {
            return "";
        }
        // Minimal JSON object parser for {"key": 1, "key2": -1}
        String body = sortJson.trim();
        if (!body.startsWith("{") || !body.endsWith("}")) {
            throw new IllegalArgumentException("Sort must be a JSON object");
        }
        body = body.substring(1, body.length() - 1).trim();
        if (body.isEmpty()) {
            return "";
        }
        StringBuilder orderBy = new StringBuilder();
        String[] pairs = body.split(",");
        for (String pair : pairs) {
            String[] kv = pair.split(":");
            if (kv.length != 2) {
                throw new IllegalArgumentException("Invalid sort entry: " + pair.trim());
            }
            String key = kv[0].trim().replaceAll("^\"|\"$", "");
            validateIdentifier(key);
            int dir = Integer.parseInt(kv[1].trim());
            if (dir != 1 && dir != -1) {
                throw new IllegalArgumentException("Sort direction must be 1 or -1, got: " + dir);
            }
            if (orderBy.length() > 0) {
                orderBy.append(", ");
            }
            orderBy.append("data->>'").append(key).append("' ").append(dir == 1 ? "ASC" : "DESC");
        }
        return orderBy.toString();
    }

    /**
     * Find documents matching a filter. Like MongoDB find().
     * filterJson is a JSONB containment filter (uses @> operator).
     * sortJson is a MongoDB-like sort object: {"field": 1} for ASC, {"field": -1} for DESC.
     * limit and skip control pagination. Pass null for no limit/skip.
     * Returns a list of matching rows.
     */
    public static List<Map<String, Object>> docFind(Connection conn, String collection,
            String filterJson, String sortJson, Integer limit, Integer skip) throws SQLException {
        validateIdentifier(collection);
        FilterResult filter = buildFilter(filterJson);
        StringBuilder sql = new StringBuilder("SELECT _id, data, created_at, updated_at FROM " + collection);
        if (!filter.whereClause.isEmpty()) {
            sql.append(" WHERE ").append(filter.whereClause);
        }
        String orderClause = parseSortClause(sortJson);
        if (!orderClause.isEmpty()) {
            sql.append(" ORDER BY ").append(orderClause);
        }
        if (limit != null) {
            sql.append(" LIMIT ?");
        }
        if (skip != null) {
            sql.append(" OFFSET ?");
        }
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            for (Object param : filter.params) {
                setFilterParam(ps, idx++, param);
            }
            if (limit != null) {
                ps.setInt(idx++, limit);
            }
            if (skip != null) {
                ps.setInt(idx++, skip);
            }
            ResultSet rs = ps.executeQuery();
            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                results.add(rowToMap(rs));
            }
            return results;
        }
    }

    /**
     * Find one document matching a filter. Like MongoDB findOne().
     * Returns the first matching row, or null if none found.
     */
    public static Map<String, Object> docFindOne(Connection conn, String collection,
            String filterJson) throws SQLException {
        List<Map<String, Object>> results = docFind(conn, collection, filterJson, null, 1, null);
        return results.isEmpty() ? null : results.get(0);
    }

    public static Iterator<Map<String, Object>> docFindCursor(Connection conn, String collection,
            String filterJson, String sortJson, Integer limit, Integer skip,
            int batchSize) throws SQLException {
        validateIdentifier(collection);
        FilterResult filter = buildFilter(filterJson);
        StringBuilder sql = new StringBuilder("SELECT _id, data, created_at, updated_at FROM " + collection);
        if (!filter.whereClause.isEmpty()) {
            sql.append(" WHERE ").append(filter.whereClause);
        }
        String orderClause = parseSortClause(sortJson);
        if (!orderClause.isEmpty()) {
            sql.append(" ORDER BY ").append(orderClause);
        }
        if (limit != null) {
            sql.append(" LIMIT ?");
        }
        if (skip != null) {
            sql.append(" OFFSET ?");
        }

        boolean origAutoCommit = conn.getAutoCommit();
        if (origAutoCommit) {
            conn.setAutoCommit(false);
        }

        // Use JDBC server-side cursor via setFetchSize (requires autocommit=false)
        PreparedStatement ps = conn.prepareStatement(sql.toString());
        ps.setFetchSize(batchSize);
        int idx = 1;
        for (Object param : filter.params) {
            setFilterParam(ps, idx++, param);
        }
        if (limit != null) {
            ps.setInt(idx++, limit);
        }
        if (skip != null) {
            ps.setInt(idx++, skip);
        }
        ResultSet rs = ps.executeQuery();

        return new Iterator<Map<String, Object>>() {
            private boolean hasNext;
            private boolean closed = false;
            private final boolean restoreAutoCommit = origAutoCommit;

            {
                advance();
            }

            private void advance() {
                try {
                    hasNext = rs.next();
                    if (!hasNext) {
                        close();
                    }
                } catch (SQLException e) {
                    close();
                    throw new RuntimeException(e);
                }
            }

            private void close() {
                if (closed) return;
                closed = true;
                try { rs.close(); } catch (SQLException ignored) {}
                try { ps.close(); } catch (SQLException ignored) {}
                if (restoreAutoCommit) {
                    try { conn.setAutoCommit(true); } catch (SQLException ignored) {}
                }
            }

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public Map<String, Object> next() {
                if (!hasNext) {
                    throw new NoSuchElementException();
                }
                try {
                    Map<String, Object> row = rowToMap(rs);
                    advance();
                    return row;
                } catch (SQLException e) {
                    close();
                    throw new RuntimeException(e);
                }
            }
        };
    }

    /**
     * Update documents matching a filter. Like MongoDB updateMany().
     * Supports MongoDB-style update operators ($set, $inc, $unset, $mul, $rename,
     * $push, $pull, $addToSet) as well as plain merge (JSONB concatenation).
     * Returns the number of updated rows.
     */
    public static int docUpdate(Connection conn, String collection, String filterJson,
            String updateJson) throws SQLException {
        validateIdentifier(collection);
        FilterResult filter = buildFilter(filterJson);
        UpdateResult update = buildUpdate(updateJson);
        StringBuilder sql = new StringBuilder(
            "UPDATE " + collection + " SET data = " + update.expr + ", updated_at = NOW()");
        if (!filter.whereClause.isEmpty()) {
            sql.append(" WHERE ").append(filter.whereClause);
        }
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            for (Object param : update.params) {
                setFilterParam(ps, idx++, param);
            }
            for (Object param : filter.params) {
                setFilterParam(ps, idx++, param);
            }
            return ps.executeUpdate();
        }
    }

    /**
     * Update one document matching a filter. Like MongoDB updateOne().
     * Supports MongoDB-style update operators ($set, $inc, $unset, $mul, $rename,
     * $push, $pull, $addToSet) as well as plain merge (JSONB concatenation).
     * Returns the number of updated rows (0 or 1).
     */
    public static int docUpdateOne(Connection conn, String collection, String filterJson,
            String updateJson) throws SQLException {
        validateIdentifier(collection);
        FilterResult filter = buildFilter(filterJson);
        UpdateResult update = buildUpdate(updateJson);
        String whereExpr = filter.whereClause.isEmpty() ? "TRUE" : filter.whereClause;
        String sql = "UPDATE " + collection + " SET data = " + update.expr + ", updated_at = NOW() " +
            "WHERE _id = (SELECT _id FROM " + collection + " WHERE " + whereExpr + " LIMIT 1)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (Object param : update.params) {
                setFilterParam(ps, idx++, param);
            }
            for (Object param : filter.params) {
                setFilterParam(ps, idx++, param);
            }
            return ps.executeUpdate();
        }
    }

    /**
     * Delete documents matching a filter. Like MongoDB deleteMany().
     * Returns the number of deleted rows.
     */
    public static int docDelete(Connection conn, String collection,
            String filterJson) throws SQLException {
        validateIdentifier(collection);
        FilterResult filter = buildFilter(filterJson);
        StringBuilder sql = new StringBuilder("DELETE FROM " + collection);
        if (!filter.whereClause.isEmpty()) {
            sql.append(" WHERE ").append(filter.whereClause);
        }
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            for (Object param : filter.params) {
                setFilterParam(ps, idx++, param);
            }
            return ps.executeUpdate();
        }
    }

    /**
     * Delete one document matching a filter. Like MongoDB deleteOne().
     * Returns the number of deleted rows (0 or 1).
     */
    public static int docDeleteOne(Connection conn, String collection,
            String filterJson) throws SQLException {
        validateIdentifier(collection);
        FilterResult filter = buildFilter(filterJson);
        String whereExpr = filter.whereClause.isEmpty() ? "TRUE" : filter.whereClause;
        String sql = "DELETE FROM " + collection + " WHERE _id = " +
            "(SELECT _id FROM " + collection + " WHERE " + whereExpr + " LIMIT 1)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (Object param : filter.params) {
                setFilterParam(ps, idx++, param);
            }
            return ps.executeUpdate();
        }
    }

    /**
     * Find one document, update it, and return the updated document.
     * Like MongoDB findOneAndUpdate().
     * Returns the updated row, or null if no match found.
     */
    public static Map<String, Object> docFindOneAndUpdate(Connection conn, String collection,
            String filterJson, String updateJson) throws SQLException {
        validateIdentifier(collection);
        FilterResult filter = buildFilter(filterJson);
        UpdateResult update = buildUpdate(updateJson);
        String whereExpr = filter.whereClause.isEmpty() ? "TRUE" : filter.whereClause;
        String sql = "UPDATE " + collection + " SET data = " + update.expr + ", updated_at = NOW() " +
            "WHERE _id = (SELECT _id FROM " + collection + " WHERE " + whereExpr + " LIMIT 1) " +
            "RETURNING _id, data, created_at, updated_at";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (Object param : update.params) {
                setFilterParam(ps, idx++, param);
            }
            for (Object param : filter.params) {
                setFilterParam(ps, idx++, param);
            }
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                return null;
            }
            return rowToMap(rs);
        }
    }

    /**
     * Find one document, delete it, and return the deleted document.
     * Like MongoDB findOneAndDelete().
     * Returns the deleted row, or null if no match found.
     */
    public static Map<String, Object> docFindOneAndDelete(Connection conn, String collection,
            String filterJson) throws SQLException {
        validateIdentifier(collection);
        FilterResult filter = buildFilter(filterJson);
        String whereExpr = filter.whereClause.isEmpty() ? "TRUE" : filter.whereClause;
        String sql = "DELETE FROM " + collection + " WHERE _id = " +
            "(SELECT _id FROM " + collection + " WHERE " + whereExpr + " LIMIT 1) " +
            "RETURNING _id, data, created_at, updated_at";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (Object param : filter.params) {
                setFilterParam(ps, idx++, param);
            }
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                return null;
            }
            return rowToMap(rs);
        }
    }

    /**
     * Get distinct values for a field across documents. Like MongoDB distinct().
     * Returns a list of distinct string values, excluding nulls.
     * Optional filterJson restricts which documents are considered.
     */
    public static List<String> docDistinct(Connection conn, String collection,
            String field, String filterJson) throws SQLException {
        validateIdentifier(collection);
        String fieldExpr = fieldPath(field);
        FilterResult filter = buildFilter(filterJson);
        StringBuilder sql = new StringBuilder("SELECT DISTINCT " + fieldExpr + " FROM " + collection);
        List<String> whereParts = new ArrayList<>();
        whereParts.add(fieldExpr + " IS NOT NULL");
        if (!filter.whereClause.isEmpty()) {
            whereParts.add(filter.whereClause);
        }
        sql.append(" WHERE ").append(String.join(" AND ", whereParts));
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            for (Object param : filter.params) {
                setFilterParam(ps, idx++, param);
            }
            ResultSet rs = ps.executeQuery();
            List<String> results = new ArrayList<>();
            while (rs.next()) {
                results.add(rs.getString(1));
            }
            return results;
        }
    }

    /**
     * Count documents matching a filter. Like MongoDB countDocuments().
     * Pass null filterJson to count all documents.
     * Returns the count.
     */
    public static long docCount(Connection conn, String collection,
            String filterJson) throws SQLException {
        validateIdentifier(collection);
        FilterResult filter = buildFilter(filterJson);
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM " + collection);
        if (!filter.whereClause.isEmpty()) {
            sql.append(" WHERE ").append(filter.whereClause);
        }
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            for (Object param : filter.params) {
                setFilterParam(ps, idx++, param);
            }
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getLong(1);
        }
    }

    /**
     * Aggregate documents in a collection. Like MongoDB aggregate().
     * pipelineJson is a JSON array of stage objects. Supported stages:
     * $match, $group, $sort, $limit, $skip, $project, $unwind, $lookup.
     *
     * $group accumulators: $sum, $avg, $min, $max, $count, $push, $addToSet.
     * $sum with 1 becomes COUNT(*). $sum/$avg/$min/$max with "$field" operates on (data->>'field')::numeric.
     *
     * $project: {field: 1} include, {_id: 0} exclude, {alias: "$field"} rename.
     * $unwind: "$field" or {"path": "$field"} — CROSS JOIN LATERAL jsonb_array_elements.
     * $lookup: {from, localField, foreignField, as} — correlated subquery with json_agg.
     *
     * Returns a list of result rows as maps.
     */
    public static List<Map<String, Object>> docAggregate(Connection conn, String collection,
            String pipelineJson) throws SQLException {
        validateIdentifier(collection);
        List<Map<String, String>> stages = parsePipeline(pipelineJson);

        FilterResult matchResult = null;
        String groupIdField = null;  // null = _id:null, non-null = field name
        boolean hasGroup = false;
        boolean hasProject = false;
        List<String> selectExprs = new ArrayList<>();
        List<String> groupByExprs = new ArrayList<>();
        String sortClause = null;
        boolean sortAfterGroup = false;
        Integer limitVal = null;
        Integer skipVal = null;
        // Unwind state: field -> alias mapping for resolving field refs in group stage
        Map<String, String> unwindMap = new LinkedHashMap<>();
        // Join clauses (CROSS JOIN for unwind, correlated subquery for lookup)
        List<String> joinClauses = new ArrayList<>();
        // Lookup: alias -> full COALESCE subquery expression
        Map<String, String> lookupExprs = new LinkedHashMap<>();

        for (Map<String, String> stage : stages) {
            String type = stage.get("_type");
            switch (type) {
                case "$match":
                    matchResult = buildFilter(stage.get("_body"));
                    break;
                case "$group": {
                    hasGroup = true;
                    String idRaw = stage.get("_id");
                    if (idRaw != null && !idRaw.equals("null")) {
                        if (idRaw.startsWith("{")) {
                            // Composite _id: {"region": "$region", "year": "$year"}
                            parseCompositeGroupId(idRaw, selectExprs, groupByExprs);
                        } else if (idRaw.startsWith("\"$") && idRaw.endsWith("\"")) {
                            // "$field" reference
                            groupIdField = idRaw.substring(2, idRaw.length() - 1);
                            validateIdentifier(groupIdField);
                            String resolved = resolveFieldRef(groupIdField, unwindMap);
                            selectExprs.add(resolved + " AS _id");
                            groupByExprs.add(resolved);
                        } else if (idRaw.startsWith("$")) {
                            groupIdField = idRaw.substring(1);
                            validateIdentifier(groupIdField);
                            String resolved = resolveFieldRef(groupIdField, unwindMap);
                            selectExprs.add(resolved + " AS _id");
                            groupByExprs.add(resolved);
                        } else {
                            // bare field name (unquoted or quoted without $)
                            groupIdField = idRaw.replaceAll("^\"|\"$", "");
                            validateIdentifier(groupIdField);
                            String resolved = resolveFieldRef(groupIdField, unwindMap);
                            selectExprs.add(resolved + " AS _id");
                            groupByExprs.add(resolved);
                        }
                    }
                    // Parse accumulators
                    for (Map.Entry<String, String> entry : stage.entrySet()) {
                        String alias = entry.getKey();
                        if (alias.equals("_type") || alias.equals("_id") || alias.equals("_body")) continue;
                        validateIdentifier(alias);
                        String accValue = entry.getValue();
                        String accExpr = parseAccumulator(accValue, unwindMap);
                        selectExprs.add(accExpr + " AS " + alias);
                    }
                    break;
                }
                case "$sort": {
                    sortAfterGroup = hasGroup;
                    String sortBody = stage.get("_body");
                    if (sortAfterGroup) {
                        sortClause = parseAliasSortClause(sortBody);
                    } else {
                        sortClause = parseSortClause(sortBody);
                    }
                    break;
                }
                case "$limit":
                    limitVal = Integer.parseInt(stage.get("_body"));
                    break;
                case "$skip":
                    skipVal = Integer.parseInt(stage.get("_body"));
                    break;
                case "$project": {
                    hasProject = true;
                    parseProjectStage(stage.get("_body"), selectExprs, lookupExprs);
                    break;
                }
                case "$unwind": {
                    String unwindBody = stage.get("_body");
                    String unwindField = parseUnwindField(unwindBody);
                    validateIdentifier(unwindField);
                    String alias = "_u_" + unwindField;
                    unwindMap.put(unwindField, alias);
                    joinClauses.add("CROSS JOIN LATERAL jsonb_array_elements_text(data->'" +
                            unwindField + "') AS " + alias + "(val)");
                    break;
                }
                case "$lookup": {
                    String fromTable = stage.get("from");
                    String localField = stage.get("localField");
                    String foreignField = stage.get("foreignField");
                    String asField = stage.get("as");
                    if (fromTable == null || localField == null || foreignField == null || asField == null) {
                        throw new IllegalArgumentException(
                                "$lookup requires from, localField, foreignField, and as");
                    }
                    validateIdentifier(fromTable);
                    validateIdentifier(localField);
                    validateIdentifier(foreignField);
                    validateIdentifier(asField);
                    lookupExprs.put(asField,
                        "COALESCE((SELECT json_agg(" + fromTable + ".data) FROM " + fromTable +
                        " WHERE " + fromTable + ".data->>'" + foreignField + "' = " +
                        collection + ".data->>'" + localField + "'), '[]'::json) AS " + asField);
                    break;
                }
                default:
                    throw new IllegalArgumentException("Unsupported pipeline stage: " + type);
            }
        }

        StringBuilder sql = new StringBuilder();
        if (hasGroup && !selectExprs.isEmpty()) {
            sql.append("SELECT ");
            for (int i = 0; i < selectExprs.size(); i++) {
                if (i > 0) sql.append(", ");
                sql.append(selectExprs.get(i));
            }
        } else if (hasProject && !selectExprs.isEmpty()) {
            sql.append("SELECT ");
            for (int i = 0; i < selectExprs.size(); i++) {
                if (i > 0) sql.append(", ");
                sql.append(selectExprs.get(i));
            }
        } else {
            sql.append("SELECT _id, data, created_at, updated_at");
            // Append any lookup expressions not consumed by $project
            for (String lookupExpr : lookupExprs.values()) {
                sql.append(", ").append(lookupExpr);
            }
        }
        sql.append(" FROM ").append(collection);

        // Append JOIN clauses (unwind)
        for (String joinClause : joinClauses) {
            sql.append(" ").append(joinClause);
        }

        if (matchResult != null && !matchResult.whereClause.isEmpty()) {
            sql.append(" WHERE ").append(matchResult.whereClause);
        }
        if (!groupByExprs.isEmpty()) {
            sql.append(" GROUP BY ");
            for (int i = 0; i < groupByExprs.size(); i++) {
                if (i > 0) sql.append(", ");
                sql.append(groupByExprs.get(i));
            }
        }
        if (sortClause != null && !sortClause.isEmpty()) {
            sql.append(" ORDER BY ").append(sortClause);
        }
        if (limitVal != null) {
            sql.append(" LIMIT ?");
        }
        if (skipVal != null) {
            sql.append(" OFFSET ?");
        }

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            if (matchResult != null && !matchResult.whereClause.isEmpty()) {
                for (Object param : matchResult.params) {
                    setFilterParam(ps, idx++, param);
                }
            }
            if (limitVal != null) {
                ps.setInt(idx++, limitVal);
            }
            if (skipVal != null) {
                ps.setInt(idx++, skipVal);
            }
            ResultSet rs = ps.executeQuery();
            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                results.add(rowToMap(rs));
            }
            return results;
        }
    }

    private static void parseProjectStage(String projectBody, List<String> selectExprs,
            Map<String, String> lookupExprs) {
        if (!projectBody.startsWith("{") || !projectBody.endsWith("}")) {
            throw new IllegalArgumentException("$project value must be an object");
        }
        String body = projectBody.substring(1, projectBody.length() - 1).trim();
        if (body.isEmpty()) {
            throw new IllegalArgumentException("$project must have at least one field");
        }
        List<String[]> pairs = splitKeyValuePairs(body);
        for (String[] kv : pairs) {
            String key = kv[0].trim().replaceAll("^\"|\"$", "");
            String val = kv[1].trim();
            validateIdentifier(key);
            if (val.equals("0")) {
                // Exclusion — skip this field (don't add to select)
                continue;
            } else if (val.equals("1")) {
                // Inclusion — if this field is a lookup alias, use the lookup expression
                if (lookupExprs.containsKey(key)) {
                    selectExprs.add(lookupExprs.get(key));
                } else {
                    selectExprs.add(fieldPath(key) + " AS " + key);
                }
            } else {
                // Rename/computed: value is "$field" reference
                String ref = val.replaceAll("^\"|\"$", "");
                if (!ref.startsWith("$")) {
                    throw new IllegalArgumentException(
                            "$project values must be 0, 1, or a $field reference: " + val);
                }
                String sourceField = ref.substring(1);
                selectExprs.add(fieldPath(sourceField) + " AS " + key);
            }
        }
    }

    private static String parseUnwindField(String unwindBody) {
        String s = unwindBody.trim();
        if (s.startsWith("\"") && s.endsWith("\"")) {
            // String form: "$field"
            String inner = s.substring(1, s.length() - 1);
            if (!inner.startsWith("$")) {
                throw new IllegalArgumentException("$unwind string must start with $: " + s);
            }
            return inner.substring(1);
        } else if (s.startsWith("{")) {
            // Object form: {"path": "$field"}
            if (!s.endsWith("}")) {
                throw new IllegalArgumentException("$unwind object is malformed: " + s);
            }
            String body = s.substring(1, s.length() - 1).trim();
            List<String[]> pairs = splitKeyValuePairs(body);
            for (String[] kv : pairs) {
                String key = kv[0].trim().replaceAll("^\"|\"$", "");
                if (key.equals("path")) {
                    String pathVal = kv[1].trim().replaceAll("^\"|\"$", "");
                    if (!pathVal.startsWith("$")) {
                        throw new IllegalArgumentException(
                                "$unwind path must start with $: " + pathVal);
                    }
                    return pathVal.substring(1);
                }
            }
            throw new IllegalArgumentException("$unwind object must have a 'path' field");
        } else {
            throw new IllegalArgumentException("$unwind must be a string or object: " + s);
        }
    }

    private static String resolveFieldRef(String field, Map<String, String> unwindMap) {
        if (unwindMap.containsKey(field)) {
            return unwindMap.get(field) + ".val";
        }
        return "data->>'" + field + "'";
    }

    static List<Map<String, String>> parsePipeline(String pipelineJson) {
        if (pipelineJson == null) {
            throw new IllegalArgumentException("Pipeline must not be null");
        }
        String s = pipelineJson.trim();
        if (!s.startsWith("[") || !s.endsWith("]")) {
            throw new IllegalArgumentException("Pipeline must be a JSON array");
        }
        s = s.substring(1, s.length() - 1).trim();
        if (s.isEmpty()) {
            return new ArrayList<>();
        }
        // Split into top-level objects by finding matching braces
        List<String> objects = splitTopLevelObjects(s);
        List<Map<String, String>> stages = new ArrayList<>();
        for (String obj : objects) {
            stages.add(parseStageObject(obj.trim()));
        }
        return stages;
    }

    private static List<String> splitTopLevelObjects(String s) {
        List<String> result = new ArrayList<>();
        int depth = 0;
        int start = -1;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '{') {
                if (depth == 0) start = i;
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0 && start >= 0) {
                    result.add(s.substring(start, i + 1));
                    start = -1;
                }
            }
        }
        return result;
    }

    private static Map<String, String> parseStageObject(String obj) {
        if (!obj.startsWith("{") || !obj.endsWith("}")) {
            throw new IllegalArgumentException("Invalid stage: " + obj);
        }
        String body = obj.substring(1, obj.length() - 1).trim();
        // Find the stage type key (first key starting with $)
        int colonPos = body.indexOf(':');
        if (colonPos < 0) {
            throw new IllegalArgumentException("Invalid stage: " + obj);
        }
        String stageKey = body.substring(0, colonPos).trim().replaceAll("^\"|\"$", "");
        String stageValue = body.substring(colonPos + 1).trim();

        Map<String, String> stage = new LinkedHashMap<>();
        stage.put("_type", stageKey);

        if (stageKey.equals("$group")) {
            parseGroupStage(stageValue, stage);
        } else if (stageKey.equals("$sort")) {
            // Store the sort object body for parsing
            stage.put("_body", stageValue);
        } else if (stageKey.equals("$match")) {
            stage.put("_body", stageValue);
        } else if (stageKey.equals("$project")) {
            stage.put("_body", stageValue);
        } else if (stageKey.equals("$unwind")) {
            stage.put("_body", stageValue);
        } else if (stageKey.equals("$lookup")) {
            parseLookupStage(stageValue, stage);
        } else {
            // $limit, $skip — scalar values
            stage.put("_body", stageValue.trim());
        }
        return stage;
    }

    private static void parseGroupStage(String value, Map<String, String> stage) {
        // value is a JSON object: { "_id": ..., "alias": { "$op": ... }, ... }
        if (!value.startsWith("{") || !value.endsWith("}")) {
            throw new IllegalArgumentException("$group value must be an object");
        }
        String body = value.substring(1, value.length() - 1).trim();
        // Split into top-level key-value pairs, respecting nested braces
        List<String[]> pairs = splitKeyValuePairs(body);
        for (String[] kv : pairs) {
            String key = kv[0].trim().replaceAll("^\"|\"$", "");
            String val = kv[1].trim();
            if (key.equals("_id")) {
                stage.put("_id", val);
            } else {
                // accumulator: {"$sum": 1} or {"$sum": "$field"}
                stage.put(key, val);
            }
        }
    }

    private static void parseLookupStage(String value, Map<String, String> stage) {
        if (!value.startsWith("{") || !value.endsWith("}")) {
            throw new IllegalArgumentException("$lookup value must be an object");
        }
        String body = value.substring(1, value.length() - 1).trim();
        List<String[]> pairs = splitKeyValuePairs(body);
        for (String[] kv : pairs) {
            String key = kv[0].trim().replaceAll("^\"|\"$", "");
            String val = kv[1].trim().replaceAll("^\"|\"$", "");
            stage.put(key, val);
        }
    }

    private static void parseCompositeGroupId(String idJson, List<String> selectExprs,
            List<String> groupByExprs) {
        // idJson is {"region": "$region", "year": "$year"}
        if (!idJson.startsWith("{") || !idJson.endsWith("}")) {
            throw new IllegalArgumentException("Composite _id must be a JSON object");
        }
        String body = idJson.substring(1, idJson.length() - 1).trim();
        List<String[]> pairs = splitKeyValuePairs(body);
        if (pairs.isEmpty()) {
            throw new IllegalArgumentException("Composite _id must have at least one field");
        }
        StringBuilder jsonBuild = new StringBuilder("json_build_object(");
        for (int i = 0; i < pairs.size(); i++) {
            String alias = pairs.get(i)[0].trim().replaceAll("^\"|\"$", "");
            String ref = pairs.get(i)[1].trim().replaceAll("^\"|\"$", "");
            validateIdentifier(alias);
            if (!ref.startsWith("$")) {
                throw new IllegalArgumentException("Composite _id values must be $field references: " + ref);
            }
            String field = ref.substring(1);
            validateIdentifier(field);
            if (i > 0) jsonBuild.append(", ");
            jsonBuild.append("'").append(alias).append("', data->>'").append(field).append("'");
            groupByExprs.add("data->>'" + field + "'");
        }
        jsonBuild.append(") AS _id");
        selectExprs.add(jsonBuild.toString());
    }

    private static List<String[]> splitKeyValuePairs(String body) {
        List<String[]> pairs = new ArrayList<>();
        int i = 0;
        while (i < body.length()) {
            // Skip whitespace and commas
            while (i < body.length() && (body.charAt(i) == ',' || body.charAt(i) == ' '
                    || body.charAt(i) == '\n' || body.charAt(i) == '\r' || body.charAt(i) == '\t')) {
                i++;
            }
            if (i >= body.length()) break;

            // Find key
            int keyStart, keyEnd;
            if (body.charAt(i) == '"') {
                keyStart = i;
                i++;
                while (i < body.length() && body.charAt(i) != '"') {
                    if (body.charAt(i) == '\\') i++;
                    i++;
                }
                keyEnd = i + 1;
                i++;
            } else {
                keyStart = i;
                while (i < body.length() && body.charAt(i) != ':') i++;
                keyEnd = i;
            }
            String key = body.substring(keyStart, keyEnd).trim();

            // Skip colon
            while (i < body.length() && body.charAt(i) != ':') i++;
            i++; // skip ':'

            // Skip whitespace
            while (i < body.length() && (body.charAt(i) == ' ' || body.charAt(i) == '\t')) i++;

            // Find value (could be object, array, string, number, null)
            int valStart = i;
            if (i < body.length() && body.charAt(i) == '{') {
                int depth = 0;
                while (i < body.length()) {
                    if (body.charAt(i) == '{') depth++;
                    else if (body.charAt(i) == '}') { depth--; if (depth == 0) { i++; break; } }
                    i++;
                }
            } else if (i < body.length() && body.charAt(i) == '[') {
                int depth = 0;
                while (i < body.length()) {
                    if (body.charAt(i) == '[') depth++;
                    else if (body.charAt(i) == ']') { depth--; if (depth == 0) { i++; break; } }
                    else if (body.charAt(i) == '"') { i++; while (i < body.length() && body.charAt(i) != '"') { if (body.charAt(i) == '\\') i++; i++; } }
                    i++;
                }
            } else if (i < body.length() && body.charAt(i) == '"') {
                i++;
                while (i < body.length() && body.charAt(i) != '"') {
                    if (body.charAt(i) == '\\') i++; // skip escaped char
                    i++;
                }
                i++; // closing quote
            } else {
                // number or null or bare token
                while (i < body.length() && body.charAt(i) != ',' && body.charAt(i) != '}'
                        && body.charAt(i) != ' ' && body.charAt(i) != '\n') {
                    i++;
                }
            }
            String val = body.substring(valStart, i).trim();
            pairs.add(new String[]{key, val});
        }
        return pairs;
    }

    private static String parseAccumulator(String accJson, Map<String, String> unwindMap) {
        // accJson is like {"$sum": 1} or {"$sum": "$field"} or {"$count": {}}
        String s = accJson.trim();
        if (!s.startsWith("{") || !s.endsWith("}")) {
            throw new IllegalArgumentException("Accumulator must be an object: " + accJson);
        }
        String inner = s.substring(1, s.length() - 1).trim();
        int colonPos = inner.indexOf(':');
        if (colonPos < 0) {
            throw new IllegalArgumentException("Invalid accumulator: " + accJson);
        }
        String op = inner.substring(0, colonPos).trim().replaceAll("^\"|\"$", "");
        String arg = inner.substring(colonPos + 1).trim();

        switch (op) {
            case "$sum":
                if (arg.equals("1")) {
                    return "COUNT(*)";
                } else {
                    String field = extractFieldRef(arg);
                    String resolved = resolveFieldRef(field, unwindMap);
                    if (unwindMap.containsKey(field)) {
                        return "SUM((" + resolved + ")::numeric)";
                    }
                    return "SUM((data->>'" + field + "')::numeric)";
                }
            case "$avg": {
                String field = extractFieldRef(arg);
                String resolved = resolveFieldRef(field, unwindMap);
                if (unwindMap.containsKey(field)) {
                    return "AVG((" + resolved + ")::numeric)";
                }
                return "AVG((data->>'" + field + "')::numeric)";
            }
            case "$min": {
                String field = extractFieldRef(arg);
                String resolved = resolveFieldRef(field, unwindMap);
                if (unwindMap.containsKey(field)) {
                    return "MIN((" + resolved + ")::numeric)";
                }
                return "MIN((data->>'" + field + "')::numeric)";
            }
            case "$max": {
                String field = extractFieldRef(arg);
                String resolved = resolveFieldRef(field, unwindMap);
                if (unwindMap.containsKey(field)) {
                    return "MAX((" + resolved + ")::numeric)";
                }
                return "MAX((data->>'" + field + "')::numeric)";
            }
            case "$count":
                return "COUNT(*)";
            case "$push": {
                String field = extractFieldRef(arg);
                String resolved = resolveFieldRef(field, unwindMap);
                if (unwindMap.containsKey(field)) {
                    return "array_agg(" + resolved + ")";
                }
                return "array_agg(data->>'" + field + "')";
            }
            case "$addToSet": {
                String field = extractFieldRef(arg);
                String resolved = resolveFieldRef(field, unwindMap);
                if (unwindMap.containsKey(field)) {
                    return "array_agg(DISTINCT " + resolved + ")";
                }
                return "array_agg(DISTINCT data->>'" + field + "')";
            }
            default:
                throw new IllegalArgumentException("Unsupported accumulator: " + op);
        }
    }

    private static String extractFieldRef(String arg) {
        // arg is "$field" (with or without quotes)
        String s = arg.trim().replaceAll("^\"|\"$", "");
        if (!s.startsWith("$")) {
            throw new IllegalArgumentException("Accumulator field must be a $reference: " + arg);
        }
        String field = s.substring(1);
        validateIdentifier(field);
        return field;
    }

    static String parseAliasSortClause(String sortJson) {
        if (sortJson == null || sortJson.trim().isEmpty()) {
            return "";
        }
        String body = sortJson.trim();
        if (!body.startsWith("{") || !body.endsWith("}")) {
            throw new IllegalArgumentException("Sort must be a JSON object");
        }
        body = body.substring(1, body.length() - 1).trim();
        if (body.isEmpty()) {
            return "";
        }
        StringBuilder orderBy = new StringBuilder();
        String[] pairs = body.split(",");
        for (String pair : pairs) {
            String[] kv = pair.split(":");
            if (kv.length != 2) {
                throw new IllegalArgumentException("Invalid sort entry: " + pair.trim());
            }
            String key = kv[0].trim().replaceAll("^\"|\"$", "");
            validateIdentifier(key);
            int dir = Integer.parseInt(kv[1].trim());
            if (dir != 1 && dir != -1) {
                throw new IllegalArgumentException("Sort direction must be 1 or -1, got: " + dir);
            }
            if (orderBy.length() > 0) {
                orderBy.append(", ");
            }
            orderBy.append(key).append(dir == 1 ? " ASC" : " DESC");
        }
        return orderBy.toString();
    }

    // =========================================================================
    // Change Streams, TTL, Capped Collections
    // =========================================================================

    /**
     * Watch a collection for changes. Like MongoDB change streams.
     * Creates a trigger that fires pg_notify on INSERT/UPDATE/DELETE,
     * then starts a background listener thread that invokes the callback
     * with each change event as a JSON string.
     *
     * The callback receives (channel, payload) where payload is a JSON object
     * with "op" (INSERT/UPDATE/DELETE), "_id" (row id), and "data" (for non-DELETE).
     *
     * Returns a daemon Thread that polls for notifications. Interrupt the thread
     * or close the connection to stop watching.
     */
    public static Thread docWatch(Connection conn, String collection,
            BiConsumer<String, String> callback) throws SQLException {
        validateIdentifier(collection);
        String channel = collection + "_changes";
        String funcName = collection + "_notify_fn";
        String triggerName = collection + "_notify_trg";

        try (Statement st = conn.createStatement()) {
            st.execute(
                "CREATE OR REPLACE FUNCTION " + funcName + "() " +
                "RETURNS TRIGGER LANGUAGE plpgsql AS $$ " +
                "BEGIN " +
                "IF TG_OP = 'DELETE' THEN " +
                "PERFORM pg_notify('" + channel + "', " +
                "json_build_object('op', TG_OP, '_id', OLD._id::text)::text); " +
                "RETURN OLD; " +
                "ELSE " +
                "PERFORM pg_notify('" + channel + "', " +
                "json_build_object('op', TG_OP, '_id', NEW._id::text, 'data', NEW.data)::text); " +
                "RETURN NEW; " +
                "END IF; " +
                "END; $$"
            );
        }

        try (Statement st = conn.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS " + triggerName + " ON " + collection);
        }

        try (Statement st = conn.createStatement()) {
            st.execute(
                "CREATE TRIGGER " + triggerName + " " +
                "AFTER INSERT OR UPDATE OR DELETE ON " + collection + " " +
                "FOR EACH ROW EXECUTE FUNCTION " + funcName + "()"
            );
        }

        PGConnection pgConn = conn.unwrap(PGConnection.class);
        try (Statement st = conn.createStatement()) {
            st.execute("LISTEN " + channel);
        }

        Thread t = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    PGNotification[] notifications = pgConn.getNotifications(5000);
                    if (notifications != null) {
                        for (PGNotification n : notifications) {
                            callback.accept(n.getName(), n.getParameter());
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        t.setDaemon(true);
        t.start();
        return t;
    }

    /**
     * Stop watching a collection for changes. Like MongoDB change stream close().
     * Drops the trigger, trigger function, and issues UNLISTEN.
     */
    public static void docUnwatch(Connection conn, String collection) throws SQLException {
        validateIdentifier(collection);
        String channel = collection + "_changes";
        String funcName = collection + "_notify_fn";
        String triggerName = collection + "_notify_trg";

        try (Statement st = conn.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS " + triggerName + " ON " + collection);
        }
        try (Statement st = conn.createStatement()) {
            st.execute("DROP FUNCTION IF EXISTS " + funcName + "()");
        }
        try (Statement st = conn.createStatement()) {
            st.execute("UNLISTEN " + channel);
        }
    }

    /**
     * Create a TTL index on a collection. Like MongoDB TTL indexes.
     * Automatically deletes documents older than expireAfterSeconds based on
     * the specified field (defaults to "created_at").
     * Uses a BEFORE INSERT trigger to purge expired rows on each insert.
     */
    public static void docCreateTtlIndex(Connection conn, String collection,
            int expireAfterSeconds, String field) throws SQLException {
        validateIdentifier(collection);
        validateIdentifier(field);
        if (expireAfterSeconds <= 0) {
            throw new IllegalArgumentException("expireAfterSeconds must be a positive integer");
        }

        String idxName = collection + "_ttl_idx";
        String funcName = collection + "_ttl_fn";
        String triggerName = collection + "_ttl_trg";

        try (Statement st = conn.createStatement()) {
            st.execute("CREATE INDEX IF NOT EXISTS " + idxName + " ON " + collection + " (" + field + ")");
        }

        try (Statement st = conn.createStatement()) {
            st.execute(
                "CREATE OR REPLACE FUNCTION " + funcName + "() " +
                "RETURNS TRIGGER LANGUAGE plpgsql AS $$ " +
                "BEGIN " +
                "DELETE FROM " + collection + " WHERE " + field +
                " < NOW() - INTERVAL '" + expireAfterSeconds + " seconds'; " +
                "RETURN NEW; " +
                "END; $$"
            );
        }

        try (Statement st = conn.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS " + triggerName + " ON " + collection);
        }

        try (Statement st = conn.createStatement()) {
            st.execute(
                "CREATE TRIGGER " + triggerName + " " +
                "BEFORE INSERT ON " + collection + " " +
                "FOR EACH ROW EXECUTE FUNCTION " + funcName + "()"
            );
        }
    }

    /**
     * Create a TTL index with the default field "created_at".
     */
    public static void docCreateTtlIndex(Connection conn, String collection,
            int expireAfterSeconds) throws SQLException {
        docCreateTtlIndex(conn, collection, expireAfterSeconds, "created_at");
    }

    /**
     * Remove a TTL index from a collection.
     * Drops the trigger, trigger function, and index.
     */
    public static void docRemoveTtlIndex(Connection conn, String collection) throws SQLException {
        validateIdentifier(collection);

        String idxName = collection + "_ttl_idx";
        String funcName = collection + "_ttl_fn";
        String triggerName = collection + "_ttl_trg";

        try (Statement st = conn.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS " + triggerName + " ON " + collection);
        }
        try (Statement st = conn.createStatement()) {
            st.execute("DROP FUNCTION IF EXISTS " + funcName + "()");
        }
        try (Statement st = conn.createStatement()) {
            st.execute("DROP INDEX IF EXISTS " + idxName);
        }
    }

    /**
     * Create a capped collection. Like MongoDB capped collections.
     * Automatically deletes the oldest documents when the collection exceeds
     * maxDocuments. Uses an AFTER INSERT trigger to enforce the cap.
     * Creates the collection table if it doesn't exist.
     */
    public static void docCreateCapped(Connection conn, String collection,
            int maxDocuments) throws SQLException {
        validateIdentifier(collection);
        if (maxDocuments <= 0) {
            throw new IllegalArgumentException("maxDocuments must be a positive integer");
        }

        ensureCollection(conn, collection);

        String funcName = collection + "_cap_fn";
        String triggerName = collection + "_cap_trg";

        try (Statement st = conn.createStatement()) {
            st.execute(
                "CREATE OR REPLACE FUNCTION " + funcName + "() " +
                "RETURNS TRIGGER LANGUAGE plpgsql AS $$ " +
                "BEGIN " +
                "DELETE FROM " + collection + " WHERE _id IN (" +
                "SELECT _id FROM " + collection + " " +
                "ORDER BY created_at ASC, _id ASC " +
                "LIMIT GREATEST((SELECT COUNT(*) FROM " + collection + ") - " + maxDocuments + ", 0)" +
                "); " +
                "RETURN NEW; " +
                "END; $$"
            );
        }

        try (Statement st = conn.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS " + triggerName + " ON " + collection);
        }

        try (Statement st = conn.createStatement()) {
            st.execute(
                "CREATE TRIGGER " + triggerName + " " +
                "AFTER INSERT ON " + collection + " " +
                "FOR EACH ROW EXECUTE FUNCTION " + funcName + "()"
            );
        }
    }

    /**
     * Remove the cap from a collection.
     * Drops the trigger and trigger function. Existing documents are kept.
     */
    public static void docRemoveCap(Connection conn, String collection) throws SQLException {
        validateIdentifier(collection);

        String funcName = collection + "_cap_fn";
        String triggerName = collection + "_cap_trg";

        try (Statement st = conn.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS " + triggerName + " ON " + collection);
        }
        try (Statement st = conn.createStatement()) {
            st.execute("DROP FUNCTION IF EXISTS " + funcName + "()");
        }
    }

    /**
     * Create an index on JSONB keys in a collection. Like MongoDB createIndex().
     * keys is a list of JSONB field names to include in the index.
     * Uses btree expression index on (data->>'field') for each key.
     */
    public static void docCreateIndex(Connection conn, String collection,
            List<String> keys) throws SQLException {
        validateIdentifier(collection);
        if (keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException("Index keys must not be empty");
        }
        StringBuilder indexCols = new StringBuilder();
        StringBuilder indexName = new StringBuilder("idx_" + collection);
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            validateIdentifier(key);
            if (i > 0) indexCols.append(", ");
            indexCols.append("(data->>'").append(key).append("')");
            indexName.append("_").append(key);
        }
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE INDEX IF NOT EXISTS " + indexName +
                " ON " + collection + " (" + indexCols + ")");
        }
    }
}
