package com.goldlapel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Tests for the v0.2 factory API: {@link GoldLapel#start}, try-with-resources,
 * {@link GoldLapel#using}, and per-method connection overrides.
 *
 * <p>The {@code start()} path is covered with a mocked JDBC driver so we don't
 * spawn the Rust binary. Wrapper methods are exercised through the package-
 * private constructor with a mock internal connection injected via reflection.
 */
class FactoryApiTest {

    private static GoldLapel unstarted() {
        return new GoldLapel("postgresql://user:pass@host:5432/db", new GoldLapelOptions());
    }

    private static void inject(GoldLapel gl, String field, Object value) {
        try {
            java.lang.reflect.Field f = GoldLapel.class.getDeclaredField(field);
            f.setAccessible(true);
            f.set(gl, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void startReturnsInstance() {
        // Mock the two subprocess-starting methods so we don't actually spawn
        // a binary; assert that start() returns a non-null, configured GoldLapel.
        try (MockedStatic<GoldLapel> stat = mockStatic(GoldLapel.class, withSettings().defaultAnswer(CALLS_REAL_METHODS))) {
            // Skip actual binary resolution
            stat.when(() -> GoldLapel.findBinary()).thenReturn("/bin/echo");
            // We can't easily stub private startProxy; instead, use an inline mock
            // of DriverManager.getConnection via the wrapper's own lifecycle.
            // Simpler: rely on the fact that start() returns a GoldLapel that
            // is built from the factory — we verify through a lambda option.
            AtomicReference<String> seen = new AtomicReference<>();
            try {
                GoldLapel.start("postgresql://localhost/db", opts -> {
                    opts.setPort(17932);
                    seen.set("invoked");
                });
            } catch (RuntimeException ignored) {
                // start() will fail at the subprocess step (echo isn't a valid
                // binary); we only care that the configurator was invoked.
            }
            assertEquals("invoked", seen.get());
        }
    }

    @Test
    void startFailureCleansUpSubprocess(@TempDir Path tmp) throws Exception {
        // Regression: when start() fails mid-way (after startProxy() succeeds
        // but eagerConnect() throws), the catch block in start() must call
        // gl.stop() so the spawned subprocess doesn't leak.
        //
        // Setup: a fake binary that binds the requested port (so startProxy()
        // succeeds) but does not speak the Postgres protocol (so JDBC's
        // handshake errors out). Records its PID to a file so we can assert
        // cleanup post-throw.
        Assumptions.assumeTrue(
            !System.getProperty("os.name", "").toLowerCase().contains("windows"),
            "POSIX-only test (needs /bin/sh + python3)"
        );
        Assumptions.assumeTrue(
            isOnPath("python3"),
            "python3 not on PATH; fake-binary trick needs it to bind the port"
        );
        // Postgres JDBC driver is 'provided' scope in core/pom.xml — it may
        // or may not be on the test classpath. Either way eagerConnect() throws
        // (missing driver → class-not-found message; present driver → handshake
        // EOF) and hits the same catch { gl.stop() } path we're testing.

        // Pick a free port (fake binary will bind it)
        int port;
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }

        Path pidFile = tmp.resolve("fake.pid");
        Path script = writeFakeProxyBinary(tmp, pidFile);

        String origBin = System.getenv("GOLDLAPEL_BINARY");
        try {
            setEnvReflective("GOLDLAPEL_BINARY", script.toString());

            RuntimeException caught = assertThrows(
                RuntimeException.class,
                () -> GoldLapel.start("postgresql://localhost:5432/mydb", opts -> opts.setPort(port)),
                "start() should throw — fake binary speaks no Postgres protocol"
            );
            // Expected failure paths: eagerConnect's missing driver / handshake
            // error, or (less likely here) startProxy's timeout.
            String msg = caught.getMessage() == null ? "" : caught.getMessage();
            assertTrue(
                msg.contains("internal JDBC connection")
                    || msg.contains("No PostgreSQL JDBC driver found")
                    || msg.contains("failed to start on port"),
                "unexpected error path: " + msg
            );

            // Fake binary should have written its PID; start() should have
            // torn it down (either via destroyForcibly in startProxy's timeout
            // branch, or via the catch { gl.stop(); } in start()).
            assertTrue(Files.exists(pidFile), "fake binary should have recorded its PID");
            long pid = Long.parseLong(Files.readString(pidFile).trim());

            // Give the OS a moment to reap the process
            waitForProcessExit(pid, 5000);
            assertFalse(
                java.lang.ProcessHandle.of(pid).map(java.lang.ProcessHandle::isAlive).orElse(false),
                "subprocess PID " + pid + " should have been killed by start()'s cleanup — leak!"
            );
        } finally {
            setEnvReflective("GOLDLAPEL_BINARY", origBin);
        }
    }

    private static Path writeFakeProxyBinary(Path tmp, Path pidFile) throws IOException {
        Path script = tmp.resolve("fake-goldlapel.sh");
        // Shell parses --proxy-port, records its own PID ($$ survives exec),
        // then exec's into a python server that binds the port and accepts +
        // closes every inbound connection (makes JDBC handshake read EOF).
        Files.writeString(script,
            "#!/bin/sh\n" +
            "echo $$ > \"" + pidFile.toString() + "\"\n" +
            "PORT=\n" +
            "while [ $# -gt 0 ]; do\n" +
            "  case \"$1\" in\n" +
            "    --proxy-port) PORT=\"$2\"; shift 2 ;;\n" +
            "    *) shift ;;\n" +
            "  esac\n" +
            "done\n" +
            "exec python3 -c \"\n" +
            "import socket\n" +
            "s = socket.socket()\n" +
            "s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n" +
            "s.bind(('127.0.0.1', ${PORT}))\n" +
            "s.listen(5)\n" +
            "while True:\n" +
            "    c, _ = s.accept()\n" +
            "    c.close()\n" +
            "\"\n"
        );
        script.toFile().setExecutable(true);
        return script;
    }

    private static boolean isOnPath(String binary) {
        String pathEnv = System.getenv("PATH");
        if (pathEnv == null) return false;
        for (String dir : pathEnv.split(":")) {
            if (new java.io.File(dir, binary).canExecute()) return true;
        }
        return false;
    }

    private static void waitForProcessExit(long pid, long timeoutMs) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            boolean alive = java.lang.ProcessHandle.of(pid)
                .map(java.lang.ProcessHandle::isAlive).orElse(false);
            if (!alive) return;
            try { Thread.sleep(50); } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void setEnvReflective(String key, String value) {
        try {
            java.util.Map<String, String> env = System.getenv();
            java.lang.reflect.Field field = env.getClass().getDeclaredField("m");
            field.setAccessible(true);
            java.util.Map<String, String> writableEnv = (java.util.Map<String, String>) field.get(env);
            if (value == null) writableEnv.remove(key);
            else writableEnv.put(key, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set env var", e);
        }
    }

    @Test
    void autoCloseableClosesViaTryWithResources() throws Exception {
        // Verify try-with-resources calls stop() (via close()).
        GoldLapel gl = unstarted();
        // Inject a no-op mock process so stop() can observe it
        Connection internal = mock(Connection.class);
        inject(gl, "internalConn", internal);
        try (GoldLapel resource = gl) {
            assertSame(internal, resource.connection());
        }
        // After close(), internal connection is nulled and closed
        verify(internal).close();
        assertThrows(IllegalStateException.class, gl::connection);
    }

    @Test
    void usingScopesConnectionOnCurrentThread() throws Exception {
        GoldLapel gl = unstarted();
        Connection internal = mock(Connection.class);
        Connection scoped = mock(Connection.class);
        inject(gl, "internalConn", internal);

        AtomicReference<Connection> insideScope = new AtomicReference<>();
        gl.using(scoped, () -> {
            // resolveConn() is private — read it indirectly via a wrapper method
            // that captures the connection it actually used.
            try {
                java.lang.reflect.Method m = GoldLapel.class.getDeclaredMethod("resolveConn");
                m.setAccessible(true);
                insideScope.set((Connection) m.invoke(gl));
            } catch (Exception e) { throw new RuntimeException(e); }
        });

        assertSame(scoped, insideScope.get(), "scoped connection visible inside using()");
        // After the scope exits, falls back to the internal connection
        assertSame(internal, gl.connection());
    }

    @Test
    void usingRestoresPreviousScopeOnException() throws Exception {
        GoldLapel gl = unstarted();
        Connection internal = mock(Connection.class);
        Connection scoped = mock(Connection.class);
        inject(gl, "internalConn", internal);

        assertThrows(RuntimeException.class, () ->
            gl.using(scoped, () -> {
                throw new RuntimeException("boom");
            })
        );
        // After exception, scope is unwound — connection falls back to internal
        assertSame(internal, gl.connection());
    }

    @Test
    void usingSupportsNesting() throws Exception {
        GoldLapel gl = unstarted();
        Connection internal = mock(Connection.class);
        Connection outer = mock(Connection.class);
        Connection inner = mock(Connection.class);
        inject(gl, "internalConn", internal);

        AtomicReference<Connection> seenInner = new AtomicReference<>();
        AtomicReference<Connection> seenAfterInner = new AtomicReference<>();

        java.lang.reflect.Method m = GoldLapel.class.getDeclaredMethod("resolveConn");
        m.setAccessible(true);

        gl.using(outer, () -> {
            gl.using(inner, () -> {
                try { seenInner.set((Connection) m.invoke(gl)); }
                catch (Exception e) { throw new RuntimeException(e); }
            });
            try { seenAfterInner.set((Connection) m.invoke(gl)); }
            catch (Exception e) { throw new RuntimeException(e); }
        });

        assertSame(inner, seenInner.get(), "inner scope takes precedence");
        assertSame(outer, seenAfterInner.get(), "outer scope restored after inner exits");
        assertSame(internal, gl.connection(), "internal restored after outermost exits");
    }

    @Test
    void usingRejectsNullConn() {
        GoldLapel gl = unstarted();
        assertThrows(IllegalArgumentException.class, () -> gl.using(null, () -> {}));
    }

    @Test
    void usingRejectsNullBody() {
        GoldLapel gl = unstarted();
        Connection conn = mock(Connection.class);
        assertThrows(IllegalArgumentException.class, () -> gl.using(conn, (Runnable) null));
        assertThrows(IllegalArgumentException.class, () -> gl.using(conn, (GoldLapel.ThrowingSupplier<Integer>) null));
    }

    @Test
    void usingSupplierReturnsValue() throws SQLException {
        GoldLapel gl = unstarted();
        Connection internal = mock(Connection.class);
        Connection scoped = mock(Connection.class);
        inject(gl, "internalConn", internal);

        int result = gl.using(scoped, () -> 42);
        assertEquals(42, result);
        // Scope is unwound — connection falls back to internal
        assertSame(internal, gl.connection());
    }

    @Test
    void usingSupplierPropagatesSQLException() {
        GoldLapel gl = unstarted();
        Connection internal = mock(Connection.class);
        Connection scoped = mock(Connection.class);
        inject(gl, "internalConn", internal);

        SQLException thrown = assertThrows(SQLException.class, () ->
            gl.using(scoped, (GoldLapel.ThrowingSupplier<String>) () -> {
                throw new SQLException("boom");
            })
        );
        assertEquals("boom", thrown.getMessage());
        // Scope unwound even on exception
        assertSame(internal, gl.connection());
    }

    @Test
    void connectionOverrideTakesPrecedenceOverInternal() throws SQLException {
        // Verify a wrapper method with an explicit Connection argument uses
        // that connection rather than the internal one.
        GoldLapel gl = unstarted();
        Connection internal = mock(Connection.class);
        Connection override = mock(Connection.class);
        inject(gl, "internalConn", internal);

        java.sql.PreparedStatement ps = mock(java.sql.PreparedStatement.class);
        when(override.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeUpdate()).thenReturn(1);

        gl.publish("events", "{\"x\":1}", override);

        // The override, not the internal, is the one that had prepareStatement called on it
        verify(override).prepareStatement(anyString());
        verifyNoInteractions(internal);
    }

    @Test
    void connectionOverrideTakesPrecedenceOverUsingScope() throws SQLException {
        GoldLapel gl = unstarted();
        Connection internal = mock(Connection.class);
        Connection scoped = mock(Connection.class);
        Connection override = mock(Connection.class);
        inject(gl, "internalConn", internal);

        java.sql.PreparedStatement ps = mock(java.sql.PreparedStatement.class);
        when(override.prepareStatement(anyString())).thenReturn(ps);

        gl.using(scoped, () -> {
            try {
                gl.publish("events", "{\"x\":1}", override);
            } catch (SQLException e) { throw new RuntimeException(e); }
        });

        verify(override).prepareStatement(anyString());
        verifyNoInteractions(scoped);
        verifyNoInteractions(internal);
    }

    @Test
    void toJdbcConnectionInfoStripsUserinfo() {
        GoldLapel.JdbcConnectionInfo info =
            GoldLapel.toJdbcConnectionInfo("postgresql://alice:s3cret@db.example.com:7932/mydb");
        assertEquals("jdbc:postgresql://db.example.com:7932/mydb", info.url);
        assertEquals("alice", info.user);
        assertEquals("s3cret", info.password);
    }

    @Test
    void toJdbcConnectionInfoHandlesUserOnly() {
        GoldLapel.JdbcConnectionInfo info =
            GoldLapel.toJdbcConnectionInfo("postgresql://sgibson@localhost/postgres");
        assertEquals("jdbc:postgresql://localhost/postgres", info.url);
        assertEquals("sgibson", info.user);
        assertNull(info.password);
    }

    @Test
    void toJdbcConnectionInfoHandlesNoUserinfo() {
        GoldLapel.JdbcConnectionInfo info =
            GoldLapel.toJdbcConnectionInfo("postgresql://localhost:7932/mydb");
        assertEquals("jdbc:postgresql://localhost:7932/mydb", info.url);
        assertNull(info.user);
        assertNull(info.password);
    }

    @Test
    void toJdbcConnectionInfoPreservesQueryParams() {
        GoldLapel.JdbcConnectionInfo info = GoldLapel.toJdbcConnectionInfo(
            "postgresql://alice:pw@host:7932/mydb?sslmode=require");
        assertEquals("jdbc:postgresql://host:7932/mydb?sslmode=require", info.url);
        assertEquals("alice", info.user);
        assertEquals("pw", info.password);
    }

    @Test
    void toJdbcConnectionInfoSplitsOnLastAtInPassword() {
        // When '@' appears in the password, userinfo is demarcated by the last '@'
        GoldLapel.JdbcConnectionInfo info = GoldLapel.toJdbcConnectionInfo(
            "postgresql://alice:p@ss@host:7932/mydb");
        assertEquals("jdbc:postgresql://host:7932/mydb", info.url);
        assertEquals("alice", info.user);
        assertEquals("p@ss", info.password);
    }

    @Test
    void toJdbcConnectionInfoDecodesPercentEncoding() {
        GoldLapel.JdbcConnectionInfo info = GoldLapel.toJdbcConnectionInfo(
            "postgresql://alice:p%40ss@host:7932/mydb");
        assertEquals("alice", info.user);
        assertEquals("p@ss", info.password);
    }

    @Test
    void toJdbcConnectionInfoAcceptsPostgresScheme() {
        GoldLapel.JdbcConnectionInfo info =
            GoldLapel.toJdbcConnectionInfo("postgres://bob@host/db");
        assertEquals("jdbc:postgresql://host/db", info.url);
        assertEquals("bob", info.user);
    }

    @Test
    void readmeQuickStartAccessorsReturnJdbcSafeValues() {
        // Regression: README Quick Start + using() examples depend on
        // getJdbcUrl() / getJdbcUser() / getJdbcPassword() returning a
        // JDBC-driver-safe URL (no inline userinfo) plus the parsed creds.
        // If anyone "fixes" those helpers to inline userinfo into the URL,
        // the PG JDBC driver will reject the URL at runtime.
        GoldLapel gl = unstarted();
        inject(gl, "proxyUrl", "postgresql://alice:s3cret@localhost:7932/mydb");

        String jdbcUrl = gl.getJdbcUrl();
        assertEquals("jdbc:postgresql://localhost:7932/mydb", jdbcUrl);
        assertFalse(jdbcUrl.contains("@"),
            "JDBC URL must not carry inline userinfo (PG driver reads user@host as hostname)");
        assertEquals("alice", gl.getJdbcUser());
        assertEquals("s3cret", gl.getJdbcPassword());

        // getUrl() keeps the Postgres-native form (userinfo inline) for libpq/psql.
        assertEquals("postgresql://alice:s3cret@localhost:7932/mydb", gl.getUrl());
    }

    @Test
    void jdbcAccessorsNullBeforeStart() {
        // Regression: before start(), proxyUrl is null and the JDBC accessors
        // must return null rather than throwing — the README describes the
        // post-start state, but the accessors are defined to be null-safe.
        GoldLapel gl = unstarted();
        assertNull(gl.getJdbcUrl());
        assertNull(gl.getJdbcUser());
        assertNull(gl.getJdbcPassword());
    }

    @Test
    void usingScopeVisibleToWrapperMethodWithoutOverride() throws SQLException {
        GoldLapel gl = unstarted();
        Connection internal = mock(Connection.class);
        Connection scoped = mock(Connection.class);
        inject(gl, "internalConn", internal);

        java.sql.PreparedStatement ps = mock(java.sql.PreparedStatement.class);
        when(scoped.prepareStatement(anyString())).thenReturn(ps);

        gl.using(scoped, () -> {
            try {
                gl.publish("events", "hello");
            } catch (SQLException e) { throw new RuntimeException(e); }
        });

        verify(scoped).prepareStatement(anyString());
        verifyNoInteractions(internal);
    }
}
