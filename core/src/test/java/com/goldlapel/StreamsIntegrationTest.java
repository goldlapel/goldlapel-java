package com.goldlapel;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.*;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end streams integration test — proxy-owned DDL (Phase 3).
 * Mirrors goldlapel-python/tests/test_streams_integration.py.
 *
 * Runs only when GOLDLAPEL_INTEGRATION=1 is set, plus:
 *   - DATABASE_URL (default: postgresql://sgibson@localhost:5432/postgres)
 *   - GOLDLAPEL_BINARY (explicit path to the goldlapel binary)
 */
@EnabledIfEnvironmentVariable(named = "GOLDLAPEL_INTEGRATION", matches = "1")
class StreamsIntegrationTest {

    private static String PG_URL;

    @BeforeAll
    static void setupPgUrl() throws ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        PG_URL = System.getenv().getOrDefault(
            "DATABASE_URL", "postgresql://sgibson@localhost:5432/postgres"
        );
    }

    private GoldLapel gl;
    private String streamName;

    @BeforeEach
    void spawnProxy() {
        int port = 7700 + (int) (System.currentTimeMillis() % 100);
        gl = GoldLapel.start(PG_URL, opts -> {
            opts.setPort(port);
            opts.setSilent(true);
        });
        streamName = "gl_java_int_" + System.currentTimeMillis();
    }

    @AfterEach
    void stopProxy() {
        if (gl != null) gl.stop();
    }

    @Test
    void streamAdd_createsPrefixedTable() throws SQLException {
        gl.streamAdd(streamName, "{\"type\":\"click\"}");

        try (Connection direct = DriverManager.getConnection(toJdbc(PG_URL));
             PreparedStatement ps = direct.prepareStatement(
                 "SELECT COUNT(*) FROM information_schema.tables "
                 + "WHERE table_schema = '_goldlapel' AND table_name = ?")) {
            ps.setString(1, "stream_" + streamName);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(1, rs.getInt(1), "expected _goldlapel.stream_<name>");
            }
            try (PreparedStatement ps2 = direct.prepareStatement(
                "SELECT COUNT(*) FROM information_schema.tables "
                + "WHERE table_schema = 'public' AND table_name = ?")) {
                ps2.setString(1, streamName);
                try (ResultSet rs = ps2.executeQuery()) {
                    rs.next();
                    assertEquals(0, rs.getInt(1), "no public.<name> — proxy owns DDL");
                }
            }
        }
    }

    @Test
    void schemaMeta_rowRecorded() throws SQLException {
        gl.streamAdd(streamName, "{\"type\":\"click\"}");

        try (Connection direct = DriverManager.getConnection(toJdbc(PG_URL));
             PreparedStatement ps = direct.prepareStatement(
                 "SELECT family, name, schema_version FROM _goldlapel.schema_meta "
                 + "WHERE family = 'stream' AND name = ?")) {
            ps.setString(1, streamName);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "schema_meta row missing");
                assertEquals("stream", rs.getString("family"));
                assertEquals(streamName, rs.getString("name"));
                assertEquals("v1", rs.getString("schema_version"));
                assertFalse(rs.next(), "exactly one row expected");
            }
        }
    }

    @Test
    void addAndRead_fullRoundTrip() throws SQLException {
        gl.streamCreateGroup(streamName, "workers");
        long id1 = gl.streamAdd(streamName, "{\"i\":1}");
        long id2 = gl.streamAdd(streamName, "{\"i\":2}");
        assertTrue(id2 > id1);

        List<Map<String, Object>> messages = gl.streamRead(streamName, "workers", "c", 10);
        assertEquals(2, messages.size());
        assertEquals(id1, (long) (Long) messages.get(0).get("id"));
        assertEquals(id2, (long) (Long) messages.get(1).get("id"));
    }

    @Test
    void ackRemovesPending() throws SQLException {
        gl.streamCreateGroup(streamName, "workers");
        long id = gl.streamAdd(streamName, "{\"i\":1}");
        gl.streamRead(streamName, "workers", "c", 10);
        assertTrue(gl.streamAck(streamName, "workers", id));
        assertFalse(gl.streamAck(streamName, "workers", id));
    }

    @Test
    void claimReassignsIdle() throws SQLException {
        gl.streamCreateGroup(streamName, "workers");
        gl.streamAdd(streamName, "{\"i\":1}");
        gl.streamRead(streamName, "workers", "consumer-a", 10);
        List<Map<String, Object>> claimed = gl.streamClaim(streamName, "workers", "consumer-b", 0L);
        assertEquals(1, claimed.size());
    }

    private static String toJdbc(String pgUrl) {
        // JDBC doesn't accept user@host in the URL — split and pass user as a
        // DriverManager property via the URL's query string.
        String rest;
        if (pgUrl.startsWith("postgresql://")) {
            rest = pgUrl.substring("postgresql://".length());
        } else if (pgUrl.startsWith("postgres://")) {
            rest = pgUrl.substring("postgres://".length());
        } else {
            return pgUrl;
        }
        String user = null, authlessRest = rest;
        int at = rest.indexOf('@');
        if (at >= 0) {
            user = rest.substring(0, at);
            authlessRest = rest.substring(at + 1);
        }
        StringBuilder b = new StringBuilder("jdbc:postgresql://").append(authlessRest);
        if (user != null) {
            b.append(authlessRest.contains("?") ? "&" : "?")
             .append("user=").append(user);
        }
        return b.toString();
    }
}
