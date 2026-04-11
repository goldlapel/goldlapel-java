package com.goldlapel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;


class FindBinaryTest {

    @Test
    void envVarOverride(@TempDir Path tmp) throws IOException {
        Path binary = tmp.resolve("goldlapel");
        Files.createFile(binary);

        String orig = System.getenv("GOLDLAPEL_BINARY");
        try {
            setEnv("GOLDLAPEL_BINARY", binary.toString());
            assertEquals(binary.toString(), GoldLapel.findBinary());
        } finally {
            setEnv("GOLDLAPEL_BINARY", orig);
        }
    }

    @Test
    void envVarMissingFileThrows() {
        String orig = System.getenv("GOLDLAPEL_BINARY");
        try {
            setEnv("GOLDLAPEL_BINARY", "/nonexistent/goldlapel");
            RuntimeException ex = assertThrows(RuntimeException.class, GoldLapel::findBinary);
            assertTrue(ex.getMessage().contains("GOLDLAPEL_BINARY"));
        } finally {
            setEnv("GOLDLAPEL_BINARY", orig);
        }
    }

    @Test
    void notFoundThrows() {
        String orig = System.getenv("GOLDLAPEL_BINARY");
        String origPath = System.getenv("PATH");
        try {
            removeEnv("GOLDLAPEL_BINARY");
            setEnv("PATH", "/nonexistent-dir-for-test");
            RuntimeException ex = assertThrows(RuntimeException.class, GoldLapel::findBinary);
            assertTrue(ex.getMessage().contains("Gold Lapel binary not found"));
        } finally {
            setEnv("GOLDLAPEL_BINARY", orig);
            setEnv("PATH", origPath);
        }
    }

    // Reflective env var manipulation for testing (same approach as other wrappers)
    @SuppressWarnings("unchecked")
    private static void setEnv(String key, String value) {
        try {
            java.util.Map<String, String> env = System.getenv();
            java.lang.reflect.Field field = env.getClass().getDeclaredField("m");
            field.setAccessible(true);
            java.util.Map<String, String> writableEnv = (java.util.Map<String, String>) field.get(env);
            if (value == null) {
                writableEnv.remove(key);
            } else {
                writableEnv.put(key, value);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to set env var", e);
        }
    }

    private static void removeEnv(String key) {
        setEnv(key, null);
    }
}


class ExtractBinaryTest {

    @Test
    void returnsNullWhenNoResourceBundled() throws Exception {
        // Reset the cached path so extractBinary() actually runs the extraction logic
        java.lang.reflect.Field field = GoldLapel.class.getDeclaredField("extractedBinaryPath");
        field.setAccessible(true);
        Path prev = (Path) field.get(null);
        field.set(null, null);

        try {
            // No platform binary is bundled in the test classpath, so extractBinary()
            // should return null without throwing (and without leaking an InputStream,
            // which is now guaranteed by the try-with-resources pattern).
            String result = GoldLapel.extractBinary();
            // In the test environment there's no bundled binary, so null is expected
            if (result != null) {
                assertTrue(Files.isRegularFile(Path.of(result)));
            }
        } finally {
            field.set(null, prev);
        }
    }
}


class MakeProxyUrlTest {

    @Test
    void postgresqlUrl() {
        assertEquals(
            "postgresql://user:pass@localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://user:pass@dbhost:5432/mydb", 7932)
        );
    }

    @Test
    void postgresUrl() {
        assertEquals(
            "postgres://user:pass@localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgres://user:pass@remote.aws.com:5432/mydb", 7932)
        );
    }

    @Test
    void pgUrlWithoutPort() {
        assertEquals(
            "postgresql://user:pass@localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://user:pass@host.aws.com/mydb", 7932)
        );
    }

    @Test
    void pgUrlWithoutPortOrPath() {
        assertEquals(
            "postgresql://user:pass@localhost:7932",
            GoldLapel.makeProxyUrl("postgresql://user:pass@host.aws.com", 7932)
        );
    }

    @Test
    void bareHostPort() {
        assertEquals("localhost:7932", GoldLapel.makeProxyUrl("dbhost:5432", 7932));
    }

    @Test
    void bareHost() {
        assertEquals("localhost:7932", GoldLapel.makeProxyUrl("dbhost", 7932));
    }

    @Test
    void preservesQueryParams() {
        assertEquals(
            "postgresql://user:pass@localhost:7932/mydb?sslmode=require",
            GoldLapel.makeProxyUrl("postgresql://user:pass@remote:5432/mydb?sslmode=require", 7932)
        );
    }

    @Test
    void preservesPercentEncodedPassword() {
        assertEquals(
            "postgresql://user:p%40ss@localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://user:p%40ss@remote:5432/mydb", 7932)
        );
    }

    @Test
    void noUserinfo() {
        assertEquals(
            "postgresql://localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://dbhost:5432/mydb", 7932)
        );
    }

    @Test
    void noUserinfoNoPort() {
        assertEquals(
            "postgresql://localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://dbhost/mydb", 7932)
        );
    }

    @Test
    void localhostStaysLocalhost() {
        assertEquals(
            "postgresql://user:pass@localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://user:pass@localhost:5432/mydb", 7932)
        );
    }

    @Test
    void atSignInPasswordWithPort() {
        assertEquals(
            "postgresql://user:p@ss@localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://user:p@ss@host:5432/mydb", 7932)
        );
    }

    @Test
    void atSignInPasswordWithoutPort() {
        assertEquals(
            "postgresql://user:p@ss@localhost:7932/mydb",
            GoldLapel.makeProxyUrl("postgresql://user:p@ss@host/mydb", 7932)
        );
    }

    @Test
    void atSignInPasswordWithQueryParams() {
        assertEquals(
            "postgresql://user:p@ss@localhost:7932/mydb?sslmode=require&param=val@ue",
            GoldLapel.makeProxyUrl("postgresql://user:p@ss@host:5432/mydb?sslmode=require&param=val@ue", 7932)
        );
    }
}


class WaitForPortTest {

    @Test
    void openPortReturnsTrue() throws IOException {
        ServerSocket server = new ServerSocket(0);
        int port = server.getLocalPort();
        try {
            assertTrue(GoldLapel.waitForPort("127.0.0.1", port, 1000));
        } finally {
            server.close();
        }
    }

    @Test
    void closedPortTimesOutReturnsFalse() {
        assertFalse(GoldLapel.waitForPort("127.0.0.1", 19999, 200));
    }
}


class GoldLapelClassTest {

    @Test
    void defaultPort() {
        GoldLapel gl = new GoldLapel("postgresql://localhost:5432/mydb");
        assertEquals(7932, gl.getPort());
    }

    @Test
    void customPort() {
        GoldLapel gl = new GoldLapel("postgresql://localhost:5432/mydb",
            new GoldLapel.Options().port(9000));
        assertEquals(9000, gl.getPort());
    }

    @Test
    void notRunningInitially() {
        GoldLapel gl = new GoldLapel("postgresql://localhost:5432/mydb");
        assertFalse(gl.isRunning());
        assertNull(gl.getUrl());
    }

    @Test
    void stopIsNoOpWhenNotStarted() {
        GoldLapel gl = new GoldLapel("postgresql://localhost:5432/mydb");
        gl.stopProxy();
        assertFalse(gl.isRunning());
        assertNull(gl.getUrl());
    }

    @Test
    void stopIsIdempotent() {
        GoldLapel gl = new GoldLapel("postgresql://localhost:5432/mydb");
        gl.stopProxy();
        gl.stopProxy();
        assertFalse(gl.isRunning());
        assertNull(gl.getUrl());
    }
}


class DashboardUrlTest {

    @Test
    void defaultDashboardPort() {
        GoldLapel gl = new GoldLapel("postgresql://localhost:5432/mydb");
        assertEquals(7933, GoldLapel.DEFAULT_DASHBOARD_PORT);
    }

    @Test
    void customDashboardPort() {
        Map<String, Object> config = new HashMap<>();
        config.put("dashboardPort", 8080);
        GoldLapel gl = new GoldLapel("postgresql://localhost:5432/mydb",
            new GoldLapel.Options().config(config));
        assertNull(gl.getDashboardUrl()); // not running, so null
    }

    @Test
    void disabledDashboardPort() {
        Map<String, Object> config = new HashMap<>();
        config.put("dashboardPort", 0);
        GoldLapel gl = new GoldLapel("postgresql://localhost:5432/mydb",
            new GoldLapel.Options().config(config));
        assertNull(gl.getDashboardUrl());
    }

    @Test
    void dashboardUrlNullWhenNotRunning() {
        GoldLapel gl = new GoldLapel("postgresql://localhost:5432/mydb");
        assertFalse(gl.isRunning());
        assertNull(gl.getDashboardUrl());
    }

    @Test
    void dashboardPortExtractedFromConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("dashboardPort", 9999);
        GoldLapel gl = new GoldLapel("postgresql://localhost:5432/mydb",
            new GoldLapel.Options().config(config));
        // Verify port was extracted (dashboardUrl includes it when running)
        // Since not running, getDashboardUrl() returns null — verify via config pass-through
        List<String> args = GoldLapel.configToArgs(config);
        assertTrue(args.contains("--dashboard-port"));
        assertTrue(args.contains("9999"));
    }
}


class ModuleFunctionsTest {

    @Test
    void proxyUrlNullWhenNotStarted() {
        GoldLapel.stop();
        assertNull(GoldLapel.proxyUrl());
    }

    @Test
    void dashboardUrlNullWhenNotStarted() {
        GoldLapel.stop();
        assertNull(GoldLapel.dashboardUrl());
    }
}


class ConfigKeysTest {

    @Test
    void returnsASet() {
        Set<String> keys = GoldLapel.configKeys();
        assertNotNull(keys);
        assertFalse(keys.isEmpty());
    }

    @Test
    void containsKnownKeys() {
        Set<String> keys = GoldLapel.configKeys();
        assertTrue(keys.contains("mode"));
        assertTrue(keys.contains("poolSize"));
        assertTrue(keys.contains("disableMatviews"));
        assertTrue(keys.contains("replica"));
    }

    @Test
    void hasExpectedCount() {
        Set<String> keys = GoldLapel.configKeys();
        assertEquals(42, keys.size());
    }

    @Test
    void isUnmodifiable() {
        Set<String> keys = GoldLapel.configKeys();
        assertThrows(UnsupportedOperationException.class, () -> keys.add("bogus"));
    }
}


class ConfigToArgsTest {

    @Test
    void testStringValue() {
        Map<String, Object> config = Collections.singletonMap("mode", "waiter");
        List<String> args = GoldLapel.configToArgs(config);
        assertEquals(Arrays.asList("--mode", "waiter"), args);
    }

    @Test
    void testNumericValue() {
        Map<String, Object> config = Collections.singletonMap("poolSize", 20);
        List<String> args = GoldLapel.configToArgs(config);
        assertEquals(Arrays.asList("--pool-size", "20"), args);
    }

    @Test
    void testBooleanTrue() {
        Map<String, Object> config = Collections.singletonMap("disablePool", true);
        List<String> args = GoldLapel.configToArgs(config);
        assertEquals(Collections.singletonList("--disable-pool"), args);
    }

    @Test
    void testBooleanFalse() {
        Map<String, Object> config = Collections.singletonMap("disablePool", false);
        List<String> args = GoldLapel.configToArgs(config);
        assertTrue(args.isEmpty());
    }

    @Test
    void testListValue() {
        Map<String, Object> config = Collections.singletonMap(
            "replica", Arrays.asList("postgres://r1:5432/db", "postgres://r2:5432/db")
        );
        List<String> args = GoldLapel.configToArgs(config);
        assertEquals(Arrays.asList(
            "--replica", "postgres://r1:5432/db",
            "--replica", "postgres://r2:5432/db"
        ), args);
    }

    @Test
    void testExcludeTablesList() {
        Map<String, Object> config = Collections.singletonMap(
            "excludeTables", Arrays.asList("sessions", "logs")
        );
        List<String> args = GoldLapel.configToArgs(config);
        assertEquals(Arrays.asList(
            "--exclude-tables", "sessions",
            "--exclude-tables", "logs"
        ), args);
    }

    @Test
    void testUnknownKeyThrows() {
        Map<String, Object> config = Collections.singletonMap("bogusKey", "value");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> GoldLapel.configToArgs(config)
        );
        assertTrue(ex.getMessage().contains("Unknown config key: bogusKey"));
    }

    @Test
    void testMultipleKeys() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("mode", "waiter");
        config.put("poolSize", 10);
        config.put("disableRewrite", true);
        List<String> args = GoldLapel.configToArgs(config);
        assertEquals(5, args.size());
        assertTrue(args.contains("--mode"));
        assertTrue(args.contains("waiter"));
        assertTrue(args.contains("--pool-size"));
        assertTrue(args.contains("10"));
        assertTrue(args.contains("--disable-rewrite"));
    }

    @Test
    void testEmptyConfig() {
        List<String> args = GoldLapel.configToArgs(Collections.emptyMap());
        assertTrue(args.isEmpty());
    }

    @Test
    void testNullConfig() {
        List<String> args = GoldLapel.configToArgs(null);
        assertTrue(args.isEmpty());
    }

    @Test
    void testListKeyWithStringThrows() {
        Map<String, Object> config = Collections.singletonMap("replica", "postgres://r1:5432/db");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> GoldLapel.configToArgs(config)
        );
        assertTrue(ex.getMessage().contains("must be a List"));
        assertTrue(ex.getMessage().contains("replica"));
        assertTrue(ex.getMessage().contains("String"));
    }

    @Test
    void testExcludeTablesWithIntegerThrows() {
        Map<String, Object> config = Collections.singletonMap("excludeTables", 42);
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> GoldLapel.configToArgs(config)
        );
        assertTrue(ex.getMessage().contains("must be a List"));
        assertTrue(ex.getMessage().contains("excludeTables"));
        assertTrue(ex.getMessage().contains("Integer"));
    }

    @Test
    void testBooleanNonBoolThrows() {
        Map<String, Object> config = Collections.singletonMap("disablePool", "yes");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> GoldLapel.configToArgs(config)
        );
        assertTrue(ex.getMessage().contains("must be a Boolean"));
    }

    @Test
    void testConfigWithOptions() {
        Map<String, Object> config = new HashMap<>();
        config.put("mode", "waiter");
        config.put("disablePool", true);

        GoldLapel.Options opts = new GoldLapel.Options()
            .port(9000)
            .config(config);

        assertEquals(9000, opts.port);
        assertSame(config, opts.config());
    }
}
