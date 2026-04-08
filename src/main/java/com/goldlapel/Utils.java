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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    private static void ensureCollection(Connection conn, String collection) throws SQLException {
        validateIdentifier(collection);
        try (Statement st = conn.createStatement()) {
            st.execute(
                "CREATE TABLE IF NOT EXISTS " + collection + " (" +
                "id BIGSERIAL PRIMARY KEY, " +
                "data JSONB NOT NULL, " +
                "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), " +
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
            );
        }
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
     * Returns the inserted row (id, data, created_at, updated_at).
     */
    public static Map<String, Object> docInsert(Connection conn, String collection,
            String documentJson) throws SQLException {
        ensureCollection(conn, collection);
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + collection + " (data) VALUES (?::jsonb) " +
                "RETURNING id, data, created_at, updated_at")) {
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
                "RETURNING id, data, created_at, updated_at")) {
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
        StringBuilder sql = new StringBuilder("SELECT id, data, created_at, updated_at FROM " + collection);
        boolean hasFilter = filterJson != null && !filterJson.trim().isEmpty();
        if (hasFilter) {
            sql.append(" WHERE data @> ?::jsonb");
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
            if (hasFilter) {
                ps.setString(idx++, filterJson);
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

    /**
     * Update documents matching a filter. Like MongoDB updateMany().
     * updateJson is merged into matching documents using || (JSONB concatenation).
     * Returns the number of updated rows.
     */
    public static int docUpdate(Connection conn, String collection, String filterJson,
            String updateJson) throws SQLException {
        validateIdentifier(collection);
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE " + collection + " SET data = data || ?::jsonb, updated_at = NOW() " +
                "WHERE data @> ?::jsonb")) {
            ps.setString(1, updateJson);
            ps.setString(2, filterJson);
            return ps.executeUpdate();
        }
    }

    /**
     * Update one document matching a filter. Like MongoDB updateOne().
     * updateJson is merged into the first matching document using || (JSONB concatenation).
     * Returns the number of updated rows (0 or 1).
     */
    public static int docUpdateOne(Connection conn, String collection, String filterJson,
            String updateJson) throws SQLException {
        validateIdentifier(collection);
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE " + collection + " SET data = data || ?::jsonb, updated_at = NOW() " +
                "WHERE id = (SELECT id FROM " + collection + " WHERE data @> ?::jsonb LIMIT 1)")) {
            ps.setString(1, updateJson);
            ps.setString(2, filterJson);
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
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM " + collection + " WHERE data @> ?::jsonb")) {
            ps.setString(1, filterJson);
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
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM " + collection + " WHERE id = " +
                "(SELECT id FROM " + collection + " WHERE data @> ?::jsonb LIMIT 1)")) {
            ps.setString(1, filterJson);
            return ps.executeUpdate();
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
        boolean hasFilter = filterJson != null && !filterJson.trim().isEmpty();
        String sql = hasFilter
            ? "SELECT COUNT(*) FROM " + collection + " WHERE data @> ?::jsonb"
            : "SELECT COUNT(*) FROM " + collection;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (hasFilter) {
                ps.setString(1, filterJson);
            }
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getLong(1);
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
