package com.goldlapel;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end streams integration test — proxy-owned DDL (Phase 3).
 * Mirrors goldlapel-python/tests/test_streams_integration.py.
 *
 * Gated on the standardized Gold Lapel integration-test convention
 * (GOLDLAPEL_INTEGRATION=1 + GOLDLAPEL_TEST_UPSTREAM) — see
 * {@link IntegrationGate}. Also requires GOLDLAPEL_BINARY pointing at the
 * goldlapel binary.
 */
@EnabledIfEnvironmentVariable(named = "GOLDLAPEL_INTEGRATION", matches = "1")
class StreamsIntegrationTest {

    private static String PG_URL;

    @BeforeAll
    static void setupPgUrl() throws ClassNotFoundException {
        // requireUpstream() throws if GOLDLAPEL_TEST_UPSTREAM is missing,
        // surfacing half-configured CI as a loud failure rather than a
        // silent skip. The class-level @EnabledIfEnvironmentVariable
        // ensures this @BeforeAll only runs when GOLDLAPEL_INTEGRATION=1.
        Class.forName("org.postgresql.Driver");
        PG_URL = IntegrationGate.requireUpstream();
    }

    private GoldLapel gl;
    private String streamName;

    @BeforeEach
    void spawnProxy() {
        int port = 7700 + (int) (System.currentTimeMillis() % 100);
        gl = GoldLapel.start(PG_URL, opts -> {
            opts.setProxyPort(port);
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
        gl.streams.add(streamName, "{\"type\":\"click\"}");

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
        gl.streams.add(streamName, "{\"type\":\"click\"}");

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
        gl.streams.createGroup(streamName, "workers");
        long id1 = gl.streams.add(streamName, "{\"i\":1}");
        long id2 = gl.streams.add(streamName, "{\"i\":2}");
        assertTrue(id2 > id1);

        List<Map<String, Object>> messages = gl.streams.read(streamName, "workers", "c", 10);
        assertEquals(2, messages.size());
        assertEquals(id1, (long) (Long) messages.get(0).get("id"));
        assertEquals(id2, (long) (Long) messages.get(1).get("id"));
    }

    @Test
    void ackRemovesPending() throws SQLException {
        gl.streams.createGroup(streamName, "workers");
        long id = gl.streams.add(streamName, "{\"i\":1}");
        gl.streams.read(streamName, "workers", "c", 10);
        assertTrue(gl.streams.ack(streamName, "workers", id));
        assertFalse(gl.streams.ack(streamName, "workers", id));
    }

    @Test
    void claimReassignsIdle() throws SQLException {
        gl.streams.createGroup(streamName, "workers");
        gl.streams.add(streamName, "{\"i\":1}");
        gl.streams.read(streamName, "workers", "consumer-a", 10);
        List<Map<String, Object>> claimed = gl.streams.claim(streamName, "workers", "consumer-b", 0L);
        assertEquals(1, claimed.size());
    }

    /**
     * Concurrency regression: two consumers running streamRead at the same
     * time on the same group must divide the pending messages between them,
     * never claiming the same message twice.
     *
     * Without the BEGIN/COMMIT wrapping in {@link Utils#streamRead} the
     * SELECT ... FOR UPDATE lock is released immediately under autocommit,
     * letting both consumers read the same cursor and claim duplicate
     * messages.
     */
    @Test
    void concurrentConsumersDoNotDoubleClaim() throws Exception {
        gl.streams.createGroup(streamName, "workers");
        final int N = 40;
        for (int i = 0; i < N; i++) {
            gl.streams.add(streamName, "{\"i\":" + i + "}");
        }

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch start = new CountDownLatch(1);
            // Each "consumer" task loops until the stream is drained so the
            // two threads genuinely interleave at the cursor.
            Callable<List<Long>> mkTask = () -> {
                start.await();
                String me = "c-" + Thread.currentThread().getId();
                List<Long> mine = new ArrayList<>();
                while (true) {
                    List<Map<String, Object>> batch =
                        gl.streams.read(streamName, "workers", me, 4);
                    if (batch.isEmpty()) break;
                    for (Map<String, Object> m : batch) {
                        mine.add((Long) m.get("id"));
                    }
                }
                return mine;
            };
            Future<List<Long>> fa = pool.submit(mkTask);
            Future<List<Long>> fb = pool.submit(mkTask);
            start.countDown();
            List<Long> a = fa.get(30, TimeUnit.SECONDS);
            List<Long> b = fb.get(30, TimeUnit.SECONDS);

            Set<Long> union = new TreeSet<>();
            union.addAll(a);
            union.addAll(b);
            assertEquals(N, union.size(), "all messages delivered once (a=" + a + " b=" + b + ")");
            assertEquals(a.size() + b.size(), union.size(),
                "no message delivered to both consumers");
        } finally {
            pool.shutdownNow();
        }
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
