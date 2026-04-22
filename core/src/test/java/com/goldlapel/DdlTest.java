package com.goldlapel;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link Ddl} — the DDL API client + per-session cache.
 * Mirrors goldlapel-python/tests/test_ddl.py and goldlapel-js/test/ddl.test.js.
 *
 * Uses JDK's {@link HttpServer} to stand up a fake dashboard.
 */
class DdlTest {
    private HttpServer server;
    private int port;
    private List<int[]> responses;       // list of [status, response_slot_index]
    private List<String> responseBodies; // parallel list of bodies
    private List<Capture> captured;

    private static class Capture {
        final String path;
        final Map<String, String> headers;
        final String body;
        Capture(String path, Map<String, String> headers, String body) {
            this.path = path;
            this.headers = headers;
            this.body = body;
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        responses = new ArrayList<>();
        responseBodies = new ArrayList<>();
        captured = new ArrayList<>();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        port = server.getAddress().getPort();
        server.createContext("/", exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            Map<String, String> hdrs = new LinkedHashMap<>();
            exchange.getRequestHeaders().forEach((k, v) -> hdrs.put(k.toLowerCase(), v.get(0)));
            captured.add(new Capture(exchange.getRequestURI().getPath(), hdrs,
                new String(body, StandardCharsets.UTF_8)));
            int[] statusSlot;
            String respBody;
            if (responses.isEmpty()) {
                statusSlot = new int[]{500, 0};
                respBody = "{\"error\":\"no_response\"}";
            } else {
                statusSlot = responses.remove(0);
                respBody = responseBodies.remove(0);
            }
            byte[] out = respBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(statusSlot[0], out.length);
            exchange.getResponseBody().write(out);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    private void queue(int status, String body) {
        responses.add(new int[]{status, 0});
        responseBodies.add(body);
    }

    @Test
    void supportedVersion_stream_is_v1() {
        assertEquals("v1", Ddl.supportedVersion("stream"));
    }

    @Test
    void happyPath_postsCorrectBodyAndHeaders() {
        queue(200, "{\"accepted\":true,\"family\":\"stream\",\"schema_version\":\"v1\"," +
            "\"tables\":{\"main\":\"_goldlapel.stream_events\"}," +
            "\"query_patterns\":{\"insert\":\"INSERT ...\"}}");

        ConcurrentHashMap<String, Map<String, Object>> cache = new ConcurrentHashMap<>();
        Map<String, Object> entry = Ddl.fetch(cache, "stream", "events", port, "tok");
        Map<String, String> qp = Ddl.queryPatterns(entry);
        assertEquals("INSERT ...", qp.get("insert"));

        assertEquals(1, captured.size());
        Capture c = captured.get(0);
        assertEquals("/api/ddl/stream/create", c.path);
        assertEquals("tok", c.headers.get("x-gl-dashboard"));
        assertTrue(c.body.contains("\"name\":\"events\""));
        assertTrue(c.body.contains("\"schema_version\":\"v1\""));
    }

    @Test
    void cacheHit_secondCallDoesNotRePost() {
        queue(200, "{\"tables\":{\"main\":\"_goldlapel.stream_events\"}," +
            "\"query_patterns\":{\"insert\":\"X\"}}");

        ConcurrentHashMap<String, Map<String, Object>> cache = new ConcurrentHashMap<>();
        Map<String, Object> r1 = Ddl.fetch(cache, "stream", "events", port, "tok");
        Map<String, Object> r2 = Ddl.fetch(cache, "stream", "events", port, "tok");
        assertSame(r1, r2);
        assertEquals(1, captured.size());
    }

    @Test
    void differentOwnersIsolated() {
        queue(200, "{\"tables\":{\"main\":\"x\"},\"query_patterns\":{\"insert\":\"X\"}}");
        queue(200, "{\"tables\":{\"main\":\"x\"},\"query_patterns\":{\"insert\":\"X\"}}");

        ConcurrentHashMap<String, Map<String, Object>> c1 = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Map<String, Object>> c2 = new ConcurrentHashMap<>();
        Ddl.fetch(c1, "stream", "events", port, "tok");
        Ddl.fetch(c2, "stream", "events", port, "tok");
        assertEquals(2, captured.size());
    }

    @Test
    void differentNamesMissCache() {
        queue(200, "{\"tables\":{\"main\":\"_goldlapel.stream_events\"},\"query_patterns\":{\"insert\":\"INSERT events\"}}");
        queue(200, "{\"tables\":{\"main\":\"_goldlapel.stream_orders\"},\"query_patterns\":{\"insert\":\"INSERT orders\"}}");

        ConcurrentHashMap<String, Map<String, Object>> cache = new ConcurrentHashMap<>();
        Ddl.fetch(cache, "stream", "events", port, "tok");
        Ddl.fetch(cache, "stream", "orders", port, "tok");
        assertEquals(2, captured.size(), "different names must each trigger a fetch");
    }

    @Test
    void versionMismatch_throwsActionable() {
        queue(409, "{\"error\":\"version_mismatch\",\"detail\":\"wrapper requested v1; proxy speaks v2 — upgrade proxy\"}");
        ConcurrentHashMap<String, Map<String, Object>> cache = new ConcurrentHashMap<>();
        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> Ddl.fetch(cache, "stream", "events", port, "tok"));
        assertTrue(ex.getMessage().contains("schema version mismatch"),
            "expected actionable mismatch msg, got: " + ex.getMessage());
    }

    @Test
    void forbidden_throwsTokenError() {
        queue(403, "{\"error\":\"forbidden\"}");
        ConcurrentHashMap<String, Map<String, Object>> cache = new ConcurrentHashMap<>();
        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> Ddl.fetch(cache, "stream", "events", port, "tok"));
        assertTrue(ex.getMessage().contains("dashboard token"),
            "expected token-specific msg, got: " + ex.getMessage());
    }

    @Test
    void missingToken_throwsBeforeHttp() {
        ConcurrentHashMap<String, Map<String, Object>> cache = new ConcurrentHashMap<>();
        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> Ddl.fetch(cache, "stream", "events", 9999, null));
        assertTrue(ex.getMessage().contains("No dashboard token"));
        assertEquals(0, captured.size());
    }

    @Test
    void missingPort_throwsBeforeHttp() {
        ConcurrentHashMap<String, Map<String, Object>> cache = new ConcurrentHashMap<>();
        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> Ddl.fetch(cache, "stream", "events", 0, "tok"));
        assertTrue(ex.getMessage().contains("No dashboard port"));
    }

    @Test
    void unreachable_throwsActionable() {
        ConcurrentHashMap<String, Map<String, Object>> cache = new ConcurrentHashMap<>();
        // Port 1 is guaranteed unreachable in tests.
        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> Ddl.fetch(cache, "stream", "events", 1, "tok"));
        assertTrue(ex.getMessage().contains("dashboard not reachable"),
            "expected unreachable msg, got: " + ex.getMessage());
    }

    @Test
    void invalidate_dropsCache() {
        queue(200, "{\"tables\":{\"main\":\"x\"},\"query_patterns\":{\"insert\":\"X\"}}");
        queue(200, "{\"tables\":{\"main\":\"x\"},\"query_patterns\":{\"insert\":\"X\"}}");

        ConcurrentHashMap<String, Map<String, Object>> cache = new ConcurrentHashMap<>();
        Ddl.fetch(cache, "stream", "events", port, "tok");
        Ddl.invalidate(cache);
        Ddl.fetch(cache, "stream", "events", port, "tok");
        assertEquals(2, captured.size());
    }

    @Test
    void jsonMini_parsesNumbersAndNestedObjects() {
        String s = "{\"a\":1,\"b\":\"x\",\"c\":{\"d\":true},\"e\":null,\"f\":1.5}";
        Map<String, Object> m = JsonMini.parseObject(s);
        assertEquals(1L, m.get("a"));
        assertEquals("x", m.get("b"));
        assertTrue(m.get("c") instanceof Map);
        assertNull(m.get("e"));
        assertEquals(1.5, m.get("f"));
    }

    @Test
    void tokenFromEnvOrFile_fallbackReturnsNullWhenNothing() {
        // Can't mock System.getenv() without reflection; this asserts the
        // method is callable and either returns null or a real token. In CI
        // with no env var and no ~/.goldlapel/dashboard_token, returns null.
        String tok = Ddl.tokenFromEnvOrFile();
        // Just assert we didn't throw — value depends on the test host.
        assertTrue(tok == null || !tok.isEmpty());
    }
}
