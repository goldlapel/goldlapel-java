package com.goldlapel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/**
 * Port allocation, per-upstream sharing and startup failure reporting in
 * {@link GoldLapel#start}, against a fake proxy binary: a script that binds
 * {@code --proxy-port} the way the proxy does, and — like the proxy — prints
 * "already in use" and exits 1 when it can't. The JDBC driver is mocked so
 * the eager connect succeeds without a Postgres protocol.
 */
class ProxySharingTest {

    @TempDir
    Path tmp;

    private Path spawnLog;
    private String origBin;
    private MockedStatic<DriverManager> driver;
    private final List<GoldLapel> started = new ArrayList<>();

    @BeforeEach
    void fakeProxy() throws IOException {
        Assumptions.assumeTrue(
            !System.getProperty("os.name", "").toLowerCase().contains("windows"),
            "POSIX-only test (needs /bin/sh + python3)");
        Assumptions.assumeTrue(isOnPath("python3"), "python3 not on PATH");
        spawnLog = tmp.resolve("spawns.log");
        Path py = tmp.resolve("fake_proxy.py");
        Files.writeString(py,
            "import socket, sys\n" +
            "port = int(sys.argv[1])\n" +
            "s = socket.socket()\n" +
            "s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n" +
            "try:\n" +
            "    s.bind(('0.0.0.0', port))\n" +
            "except OSError:\n" +
            "    sys.stderr.buffer.write(('I\\'m afraid port %d, for the proxy, is already in use \\u2014 " +
                "perhaps another Gold Lapel.\\n' % port).encode('utf-8'))\n" +
            "    sys.exit(1)\n" +
            "s.listen(16)\n" +
            "conns = []\n" +
            "while True:\n" +
            "    c, _ = s.accept()\n" +
            "    conns.append(c)\n");
        Path sh = tmp.resolve("fake-goldlapel.sh");
        Files.writeString(sh,
            "#!/bin/sh\n" +
            "echo \"$$ $*\" >> \"" + spawnLog + "\"\n" +
            "PORT=\n" +
            "while [ $# -gt 0 ]; do\n" +
            "  case \"$1\" in\n" +
            "    --proxy-port) PORT=\"$2\"; shift 2 ;;\n" +
            "    *) shift ;;\n" +
            "  esac\n" +
            "done\n" +
            "exec python3 \"" + py + "\" \"$PORT\"\n");
        assertTrue(sh.toFile().setExecutable(true));
        origBin = System.getenv("GOLDLAPEL_BINARY");
        setEnv("GOLDLAPEL_BINARY", sh.toString());
        driver = mockStatic(DriverManager.class);
        driver.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
            .thenAnswer(inv -> mock(Connection.class));
    }

    @AfterEach
    void cleanUp() {
        for (GoldLapel gl : started) gl.stop();
        if (driver != null) driver.close();
        setEnv("GOLDLAPEL_BINARY", origBin);
    }

    private GoldLapel start(String upstream, java.util.function.Consumer<GoldLapelOptions> cfg) {
        GoldLapel gl = GoldLapel.start(upstream, opts -> {
            opts.setSilent(true);
            if (cfg != null) cfg.accept(opts);
        });
        started.add(gl);
        return gl;
    }

    private List<String> spawns() throws IOException {
        return Files.exists(spawnLog) ? Files.readAllLines(spawnLog) : List.of();
    }

    private static long pidOf(String spawnLine) {
        return Long.parseLong(spawnLine.split(" ")[0]);
    }

    @Test
    void sameUpstreamSharesOneProxyUntilTheLastHolderStops() throws Exception {
        GoldLapel a = start("postgresql://localhost:5432/shared", null);
        GoldLapel b = start("postgresql://localhost:5432/shared", null);

        assertEquals(1, spawns().size(), "one subprocess for one upstream");
        assertNotSame(a, b);
        assertEquals(a.getProxyPort(), b.getProxyPort());
        assertEquals(a.getUrl(), b.getUrl());
        assertNotSame(a.connection(), b.connection(), "each holder has its own internal connection");

        a.stop();
        assertTrue(b.isRunning(), "the other holder still has its proxy");
        long pid = pidOf(spawns().get(0));
        assertTrue(ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false));

        b.stop();
        waitForExit(pid);
        assertFalse(ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false),
            "the last holder's stop() ends the subprocess");

        // And the upstream starts fresh afterwards.
        GoldLapel c = start("postgresql://localhost:5432/shared", null);
        assertTrue(c.isRunning());
        assertEquals(2, spawns().size());
    }

    @Test
    void differentUpstreamsGetDisjointPortPairs() throws Exception {
        GoldLapel a = start("postgresql://localhost:5432/one", null);
        GoldLapel b = start("postgresql://localhost:5432/two", null);
        GoldLapel c = start("postgresql://localhost:5432/three", o -> o.setDashboardPort(0));

        assertTrue(a.getProxyPort() >= GoldLapel.DEFAULT_PROXY_PORT);
        assertEquals(a.getProxyPort() + 1, a.dashboardPort());
        assertEquals(b.getProxyPort() + 1, b.dashboardPort());
        assertEquals(0, c.dashboardPort());
        List<Integer> claimed = List.of(a.getProxyPort(), a.dashboardPort(),
            b.getProxyPort(), b.dashboardPort(), c.getProxyPort());
        assertEquals(claimed.size(), claimed.stream().distinct().count(), "ports overlap: " + claimed);
        // The spawned command carries the allocated port.
        assertTrue(spawns().get(1).contains("--proxy-port " + b.getProxyPort()), spawns().get(1));
    }

    @Test
    void allocationSkipsAnotherProxysExplicitDashboardPort() throws Exception {
        int p = GoldLapel.DEFAULT_PROXY_PORT;
        while (!(GoldLapel.isPortFree(p) && GoldLapel.isPortFree(p + 1) && GoldLapel.isPortFree(p + 2)
                && GoldLapel.isPortFree(p + 3) && GoldLapel.isPortFree(p + 4))) p++;
        int dashboard = p + 2;
        GoldLapel a = start("postgresql://localhost:5432/one", o -> o.setDashboardPort(dashboard));
        GoldLapel b = start("postgresql://localhost:5432/two", null);

        assertEquals(p, a.getProxyPort());
        assertEquals(dashboard, a.dashboardPort());
        // p is taken; p+1 would need dashboard p+2, which a holds; p+2 is a's.
        assertEquals(p + 3, b.getProxyPort());
        assertEquals(p + 4, b.dashboardPort());
    }

    @Test
    void allocationSkipsPortsAnotherProgramHolds() throws Exception {
        int p = GoldLapel.DEFAULT_PROXY_PORT;
        while (!(GoldLapel.isPortFree(p) && GoldLapel.isPortFree(p + 1))) p++;
        // Hold what would be the dashboard port of the first free pair.
        try (ServerSocket squatter = new ServerSocket(p + 1)) {
            GoldLapel gl = start("postgresql://localhost:5432/squatted", null);
            assertNotEquals(p, gl.getProxyPort());
            assertNotEquals(p + 1, gl.getProxyPort());
            assertNotEquals(p + 1, gl.dashboardPort());
        }
    }

    @Test
    void explicitPortAnotherUpstreamHoldsIsAClearError() throws Exception {
        GoldLapel a = start("postgresql://alice:s3cret@localhost:5432/first", null);

        IllegalStateException proxyClash = assertThrows(IllegalStateException.class,
            () -> start("postgresql://localhost:5432/second", o -> o.setProxyPort(a.getProxyPort())));
        assertEquals("I'm afraid port " + a.getProxyPort() + ", for the proxy, is already in use by "
            + "this process's Gold Lapel for postgresql://alice:***@localhost:5432/first. Choose "
            + "another with setProxyPort, or leave it unset and a free one is chosen.",
            proxyClash.getMessage());

        IllegalStateException dashboardClash = assertThrows(IllegalStateException.class,
            () -> start("postgresql://localhost:5432/second", o -> o.setDashboardPort(a.getProxyPort())));
        assertTrue(dashboardClash.getMessage().contains("port " + a.getProxyPort() + ", for the dashboard"),
            dashboardClash.getMessage());
        assertFalse(dashboardClash.getMessage().contains("s3cret"));

        // An explicit proxy port whose derived dashboard port is taken.
        assertThrows(IllegalStateException.class,
            () -> start("postgresql://localhost:5432/second", o -> o.setProxyPort(a.getProxyPort() - 1)));

        assertEquals(1, spawns().size(), "nothing was spawned for the refused starts");
        // The refusals left no claims behind.
        GoldLapel b = start("postgresql://localhost:5432/second", null);
        assertTrue(b.isRunning());
    }

    @Test
    void explicitPortAnotherProgramHoldsFailsWithTheProxysMessage() throws Exception {
        // The squatter accepts connections, so a readiness connect alone
        // would pass and the app would talk to the wrong server.
        try (ServerSocket squatter = new ServerSocket(0)) {
            int port = squatter.getLocalPort();
            RuntimeException ex = assertThrows(RuntimeException.class,
                () -> start("postgresql://localhost:5432/busy", o -> o.setProxyPort(port)));
            assertTrue(ex.getMessage().contains("the proxy exited with status 1"), ex.getMessage());
            assertTrue(ex.getMessage().contains(
                "I'm afraid port " + port + ", for the proxy, is already in use — perhaps another Gold Lapel."),
                ex.getMessage());
        }
    }

    @Test
    void anErrorAfterTheSpawnStillStopsTheSubprocess() throws Exception {
        // Not a RuntimeException: start() must clean up on any Throwable.
        driver.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
            .thenThrow(new AssertionError("boom"));
        AssertionError err = assertThrows(AssertionError.class,
            () -> start("postgresql://localhost:5432/erroring", null));
        assertEquals("boom", err.getMessage());

        long pid = pidOf(spawns().get(0));
        waitForExit(pid);
        assertFalse(ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false),
            "subprocess PID " + pid + " leaked");

        // Its claim was released: the same upstream spawns afresh.
        driver.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
            .thenAnswer(inv -> mock(Connection.class));
        assertTrue(start("postgresql://localhost:5432/erroring", null).isRunning());
        assertEquals(2, spawns().size());
    }

    // ── helpers ──────────────────────────────────────────────

    private static void waitForExit(long pid) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (System.nanoTime() < deadline
                && ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false)) {
            Thread.sleep(25);
        }
    }

    private static boolean isOnPath(String binary) {
        String pathEnv = System.getenv("PATH");
        if (pathEnv == null) return false;
        for (String dir : pathEnv.split(":")) {
            if (new java.io.File(dir, binary).canExecute()) return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static void setEnv(String key, String value) {
        try {
            java.util.Map<String, String> env = System.getenv();
            java.lang.reflect.Field field = env.getClass().getDeclaredField("m");
            field.setAccessible(true);
            java.util.Map<String, String> writable = (java.util.Map<String, String>) field.get(env);
            if (value == null) writable.remove(key);
            else writable.put(key, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set env var", e);
        }
    }
}
