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

    // Helper: construct a GoldLapel without starting the proxy. Uses the
    // package-private constructor to avoid spawning the Rust binary in unit tests.
    static GoldLapel newUnstarted(String upstream) {
        return new GoldLapel(upstream, new GoldLapelOptions());
    }

    static GoldLapel newUnstarted(String upstream, GoldLapelOptions opts) {
        return new GoldLapel(upstream, opts);
    }

    @Test
    void defaultPort() {
        GoldLapel gl = newUnstarted("postgresql://localhost:5432/mydb");
        assertEquals(7932, gl.getProxyPort());
    }

    @Test
    void customPort() {
        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setProxyPort(9000);
        GoldLapel gl = newUnstarted("postgresql://localhost:5432/mydb", opts);
        assertEquals(9000, gl.getProxyPort());
    }

    @Test
    void notRunningInitially() {
        GoldLapel gl = newUnstarted("postgresql://localhost:5432/mydb");
        assertFalse(gl.isRunning());
        assertNull(gl.getUrl());
    }

    @Test
    void stopIsNoOpWhenNotStarted() {
        GoldLapel gl = newUnstarted("postgresql://localhost:5432/mydb");
        gl.stop();
        assertFalse(gl.isRunning());
        assertNull(gl.getUrl());
    }

    @Test
    void stopIsIdempotent() {
        GoldLapel gl = newUnstarted("postgresql://localhost:5432/mydb");
        gl.stop();
        gl.stop();
        assertFalse(gl.isRunning());
        assertNull(gl.getUrl());
    }

    @Test
    void closeCallsStop() {
        GoldLapel gl = newUnstarted("postgresql://localhost:5432/mydb");
        gl.close();
        assertFalse(gl.isRunning());
        assertNull(gl.getUrl());
    }

    @Test
    void logLevelDebugTranslatesToDoubleVerbose() {
        assertEquals("-vv", GoldLapel.translateLogLevel("debug"));
    }

    @Test
    void logLevelTraceTranslatesToTripleVerbose() {
        assertEquals("-vvv", GoldLapel.translateLogLevel("trace"));
    }

    @Test
    void logLevelInfoTranslatesToSingleVerbose() {
        assertEquals("-v", GoldLapel.translateLogLevel("info"));
    }

    @Test
    void logLevelWarnOmitted() {
        assertNull(GoldLapel.translateLogLevel("warn"));
    }

    @Test
    void logLevelErrorOmitted() {
        assertNull(GoldLapel.translateLogLevel("error"));
    }

    @Test
    void logLevelCaseInsensitive() {
        assertEquals("-vv", GoldLapel.translateLogLevel("DEBUG"));
    }

    @Test
    void logLevelStoredOnInstance() throws Exception {
        // logLevel is a top-level option — it lives on the instance directly,
        // not in the config map or extraArgs. The translation to -v/-vv/-vvv
        // happens at spawn time in startProxy().
        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setLogLevel("debug");
        GoldLapel gl = newUnstarted("postgresql://localhost:5432/mydb", opts);
        java.lang.reflect.Field f = GoldLapel.class.getDeclaredField("logLevel");
        f.setAccessible(true);
        assertEquals("debug", f.get(gl));
    }

    @Test
    void logLevelInvalidThrowsAtTranslate() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> GoldLapel.translateLogLevel("verbose")
        );
        assertTrue(ex.getMessage().contains("log_level must be one of"),
            "unexpected message: " + ex.getMessage());
    }

    @Test
    void logLevelNullReturnsNull() {
        assertNull(GoldLapel.translateLogLevel(null));
    }
}


class DashboardUrlTest {

    @Test
    void defaultDashboardPort() {
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://localhost:5432/mydb");
        assertEquals(7933, gl.getDashboardPort());
    }

    @Test
    void dashboardPortDerivesFromCustomProxyPort() {
        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setProxyPort(17932);
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://localhost:5432/mydb", opts);
        assertEquals(17933, gl.getDashboardPort());
    }

    @Test
    void explicitDashboardPortOverridesDerivation() {
        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setProxyPort(17932);
        opts.setDashboardPort(9999);
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://localhost:5432/mydb", opts);
        assertEquals(9999, gl.getDashboardPort());
    }

    @Test
    void customDashboardPort() {
        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setDashboardPort(8080);
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://localhost:5432/mydb", opts);
        assertNull(gl.getDashboardUrl()); // not running, so null
        assertEquals(8080, gl.getDashboardPort());
    }

    @Test
    void disabledDashboardPort() {
        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setDashboardPort(0);
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://localhost:5432/mydb", opts);
        assertNull(gl.getDashboardUrl());
    }

    @Test
    void dashboardUrlNullWhenNotRunning() {
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://localhost:5432/mydb");
        assertFalse(gl.isRunning());
        assertNull(gl.getDashboardUrl());
    }

    @Test
    void dashboardPortInConfigMapRejected() {
        // Regression guard: dashboardPort was promoted out of the config map
        // to a top-level option. Passing it via setConfig() must raise.
        Map<String, Object> config = new HashMap<>();
        config.put("dashboardPort", 9999);
        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setConfig(config);
        assertThrows(
            IllegalArgumentException.class,
            () -> GoldLapelClassTest.newUnstarted("postgresql://localhost:5432/mydb", opts)
        );
    }

    @Test
    void invalidationPortDerivesFromCustomProxyPort() {
        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setProxyPort(17932);
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://localhost:5432/mydb", opts);
        assertEquals(17934, gl.invalidationPort());
    }

    @Test
    void explicitInvalidationPortOverridesDerivation() {
        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setProxyPort(17932);
        opts.setInvalidationPort(9999);
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://localhost:5432/mydb", opts);
        assertEquals(9999, gl.invalidationPort());
    }
}


class ClientOptionTest {

    // Helper: read private client field via reflection
    private static String clientOf(GoldLapel gl) {
        try {
            java.lang.reflect.Field f = GoldLapel.class.getDeclaredField("client");
            f.setAccessible(true);
            return (String) f.get(gl);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void defaultClientIsJava() {
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://localhost:5432/mydb");
        assertEquals("java", clientOf(gl));
    }

    @Test
    void clientOptionOverridesDefault() {
        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setClient("my-app");
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://localhost:5432/mydb", opts);
        assertEquals("my-app", clientOf(gl));
    }

    @Test
    void clientOptionGetterSetter() {
        GoldLapelOptions opts = new GoldLapelOptions();
        assertNull(opts.getClient());
        opts.setClient("custom");
        assertEquals("custom", opts.getClient());
    }

    // Spawn a shell script that dumps GOLDLAPEL_CLIENT to a file, then exits.
    // The wrapper's startProxy() will time out (port never opens) and throw —
    // but by then the script has already captured the env var. This lets us
    // assert the actual subprocess-visible value, not just the Java-side field.
    //
    // Skipped on Windows (no /bin/sh).
    private static Path writeDumpScript(Path tmp, Path outFile) throws IOException {
        Path script = tmp.resolve("goldlapel-dump.sh");
        Files.writeString(script,
            "#!/bin/sh\n" +
            "echo \"${GOLDLAPEL_CLIENT:-<unset>}\" > \"" + outFile.toString() + "\"\n" +
            "exit 1\n"
        );
        script.toFile().setExecutable(true);
        return script;
    }

    private static boolean isPosix() {
        return !System.getProperty("os.name", "").toLowerCase().contains("windows");
    }

    @Test
    void clientForwardedToSubprocessEnv(@TempDir Path tmp) throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(isPosix(), "POSIX-only test (needs /bin/sh)");
        Path out = tmp.resolve("client.txt");
        Path script = writeDumpScript(tmp, out);

        String origBin = System.getenv("GOLDLAPEL_BINARY");
        String origClient = System.getenv("GOLDLAPEL_CLIENT");
        try {
            ClientOptionTest.setEnv("GOLDLAPEL_BINARY", script.toString());
            ClientOptionTest.setEnv("GOLDLAPEL_CLIENT", null); // ensure parent env is clean
            try {
                GoldLapel.start("postgresql://localhost:5432/mydb", opts -> opts.setClient("from-opts"));
                fail("start() should have thrown — dump script exits immediately");
            } catch (RuntimeException expected) {
                // Expected: port never opens because the script exited.
            }
            // Wait briefly for the script's write to flush (process has already exited).
            assertTrue(Files.exists(out), "dump script should have written output file");
            assertEquals("from-opts", Files.readString(out).trim());
        } finally {
            ClientOptionTest.setEnv("GOLDLAPEL_BINARY", origBin);
            ClientOptionTest.setEnv("GOLDLAPEL_CLIENT", origClient);
        }
    }

    @Test
    void clientDefaultsToJavaWhenOptionUnset(@TempDir Path tmp) throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(isPosix(), "POSIX-only test (needs /bin/sh)");
        Path out = tmp.resolve("client.txt");
        Path script = writeDumpScript(tmp, out);

        String origBin = System.getenv("GOLDLAPEL_BINARY");
        String origClient = System.getenv("GOLDLAPEL_CLIENT");
        try {
            ClientOptionTest.setEnv("GOLDLAPEL_BINARY", script.toString());
            ClientOptionTest.setEnv("GOLDLAPEL_CLIENT", null);
            try {
                GoldLapel.start("postgresql://localhost:5432/mydb");
                fail("start() should have thrown — dump script exits immediately");
            } catch (RuntimeException expected) {
                // expected
            }
            assertTrue(Files.exists(out));
            assertEquals("java", Files.readString(out).trim());
        } finally {
            ClientOptionTest.setEnv("GOLDLAPEL_BINARY", origBin);
            ClientOptionTest.setEnv("GOLDLAPEL_CLIENT", origClient);
        }
    }

    @Test
    void optionBeatsInheritedEnvClient(@TempDir Path tmp) throws Exception {
        // Documents precedence: the wrapper uses
        // ProcessBuilder.environment().put("GOLDLAPEL_CLIENT", client),
        // so the config-lambda value wins over any inherited GOLDLAPEL_CLIENT
        // from the parent JVM. Matches Spring Boot / Micronaut / Quarkus
        // semantics (explicit config > env > defaults) — code that runs is
        // what the developer wrote. If this test fails someone changed the
        // precedence; update both the code and this test.
        org.junit.jupiter.api.Assumptions.assumeTrue(isPosix(), "POSIX-only test (needs /bin/sh)");
        Path out = tmp.resolve("client.txt");
        Path script = writeDumpScript(tmp, out);

        String origBin = System.getenv("GOLDLAPEL_BINARY");
        String origClient = System.getenv("GOLDLAPEL_CLIENT");
        try {
            ClientOptionTest.setEnv("GOLDLAPEL_BINARY", script.toString());
            ClientOptionTest.setEnv("GOLDLAPEL_CLIENT", "from-parent-env");
            try {
                GoldLapel.start("postgresql://localhost:5432/mydb", opts -> opts.setClient("from-opts"));
                fail("start() should have thrown");
            } catch (RuntimeException expected) {
                // expected
            }
            assertTrue(Files.exists(out));
            // put → config-lambda value wins over inherited parent env
            assertEquals("from-opts", Files.readString(out).trim());
        } finally {
            ClientOptionTest.setEnv("GOLDLAPEL_BINARY", origBin);
            ClientOptionTest.setEnv("GOLDLAPEL_CLIENT", origClient);
        }
    }

    // Reflective env var manipulation (same approach as FindBinaryTest)
    @SuppressWarnings("unchecked")
    static void setEnv(String key, String value) {
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
        // Tuning knobs still live in the structured config map.
        assertTrue(keys.contains("poolSize"));
        assertTrue(keys.contains("disableMatviews"));
        assertTrue(keys.contains("replica"));
    }

    @Test
    void doesNotContainPromotedTopLevelKeys() {
        // Canonical surface: mode, logLevel, dashboardPort, invalidationPort,
        // client, config, license are top-level options on GoldLapelOptions,
        // not structured-config keys.
        Set<String> keys = GoldLapel.configKeys();
        assertFalse(keys.contains("mode"));
        assertFalse(keys.contains("logLevel"));
        assertFalse(keys.contains("dashboardPort"));
        assertFalse(keys.contains("invalidationPort"));
        assertFalse(keys.contains("client"));
        assertFalse(keys.contains("config"));
        assertFalse(keys.contains("license"));
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
        Map<String, Object> config = Collections.singletonMap("poolMode", "transaction");
        List<String> args = GoldLapel.configToArgs(config);
        assertEquals(Arrays.asList("--pool-mode", "transaction"), args);
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
        config.put("poolMode", "transaction");
        config.put("poolSize", 10);
        config.put("disableRewrite", true);
        List<String> args = GoldLapel.configToArgs(config);
        assertEquals(5, args.size());
        assertTrue(args.contains("--pool-mode"));
        assertTrue(args.contains("transaction"));
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
        config.put("poolMode", "transaction");
        config.put("disablePool", true);

        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setProxyPort(9000);
        opts.setMode("waiter");
        opts.setConfig(config);

        assertEquals(9000, opts.getProxyPort());
        assertEquals("waiter", opts.getMode());
        assertSame(config, opts.getConfig());
    }

    @Test
    void testLogLevelInConfigMapRejected() {
        // Regression guard: logLevel was promoted out of the config map.
        Map<String, Object> config = Collections.singletonMap("logLevel", "info");
        assertThrows(
            IllegalArgumentException.class,
            () -> GoldLapel.configToArgs(config)
        );
    }

    @Test
    void testModeInConfigMapRejected() {
        // Regression guard: mode was promoted out of the config map.
        Map<String, Object> config = Collections.singletonMap("mode", "waiter");
        assertThrows(
            IllegalArgumentException.class,
            () -> GoldLapel.configToArgs(config)
        );
    }
}
