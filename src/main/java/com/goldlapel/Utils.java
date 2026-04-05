package com.goldlapel;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
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
     */
    public static List<Map<String, Object>> search(Connection conn, String table,
            String column, String query, int limit, String lang, boolean highlight) throws SQLException {
        validateIdentifier(table);
        validateIdentifier(column);
        String sql;
        if (highlight) {
            sql = "SELECT *, " +
                "ts_rank(to_tsvector(?, " + column + "), plainto_tsquery(?, ?)) AS _score, " +
                "ts_headline(?, " + column + ", plainto_tsquery(?, ?), " +
                "'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15') AS _highlight " +
                "FROM " + table + " " +
                "WHERE to_tsvector(?, " + column + ") @@ plainto_tsquery(?, ?) " +
                "ORDER BY _score DESC LIMIT ?";
        } else {
            sql = "SELECT *, " +
                "ts_rank(to_tsvector(?, " + column + "), plainto_tsquery(?, ?)) AS _score " +
                "FROM " + table + " " +
                "WHERE to_tsvector(?, " + column + ") @@ plainto_tsquery(?, ?) " +
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
}
