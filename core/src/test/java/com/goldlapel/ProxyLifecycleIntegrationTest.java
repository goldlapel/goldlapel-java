package com.goldlapel;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ports, sharing and connection identity against the real proxy binary and
 * a real Postgres. Gated on GOLDLAPEL_INTEGRATION=1 + GOLDLAPEL_TEST_UPSTREAM
 * (see {@link IntegrationGate}).
 */
@EnabledIfEnvironmentVariable(named = "GOLDLAPEL_INTEGRATION", matches = "1")
class ProxyLifecycleIntegrationTest {

    private static String upstream;

    @BeforeAll
    static void setup() throws ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        upstream = IntegrationGate.requireUpstream();
    }

    // The same database under a different upstream URL, so it gets its own proxy.
    private static String otherUpstream() {
        return upstream.startsWith("postgresql://")
            ? "postgres://" + upstream.substring("postgresql://".length())
            : "postgresql://" + upstream.substring("postgres://".length());
    }

    private static Connection connect(GoldLapel gl) throws SQLException {
        Properties props = new Properties();
        if (gl.getJdbcUser() != null) props.setProperty("user", gl.getJdbcUser());
        if (gl.getJdbcPassword() != null) props.setProperty("password", gl.getJdbcPassword());
        return DriverManager.getConnection(gl.getJdbcUrl(), props);
    }

    private static String applicationName(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()")) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    @Test
    void connectionsAreTaggedInPgStatActivity() throws SQLException {
        try (GoldLapel gl = GoldLapel.start(upstream, o -> o.setSilent(true));
             Connection conn = connect(gl)) {
            assertEquals(GoldLapel.applicationNameMarker(), applicationName(conn));
            assertEquals(GoldLapel.applicationNameMarker(), applicationName(gl.connection()));
        }
    }

    @Test
    void twoUpstreamsRunSideBySide() throws SQLException {
        try (GoldLapel a = GoldLapel.start(upstream, o -> o.setSilent(true));
             GoldLapel b = GoldLapel.start(otherUpstream(), o -> o.setSilent(true))) {
            assertNotEquals(a.getProxyPort(), b.getProxyPort());
            assertNotEquals(a.dashboardPort(), b.getProxyPort());
            assertNotEquals(a.getProxyPort(), b.dashboardPort());
            try (Connection ca = connect(a); Connection cb = connect(b);
                 Statement sa = ca.createStatement(); Statement sb = cb.createStatement()) {
                assertTrue(sa.executeQuery("SELECT 1").next());
                assertTrue(sb.executeQuery("SELECT 1").next());
            }
        }
    }

    @Test
    void sameUpstreamSharesTheProxyUntilTheLastStop() throws SQLException {
        GoldLapel a = GoldLapel.start(upstream, o -> o.setSilent(true));
        GoldLapel b = GoldLapel.start(upstream, o -> o.setSilent(true));
        try {
            assertEquals(a.getProxyPort(), b.getProxyPort());
            a.stop();
            assertTrue(b.isRunning());
            try (Connection conn = connect(b); Statement st = conn.createStatement()) {
                assertTrue(st.executeQuery("SELECT 1").next());
            }
        } finally {
            a.stop();
            b.stop();
        }
        assertFalse(b.isRunning());
    }

    @Test
    void explicitPortAnotherUpstreamHoldsIsRefusedBeforeSpawning() {
        try (GoldLapel a = GoldLapel.start(upstream, o -> o.setSilent(true))) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> GoldLapel.start(otherUpstream(), o -> {
                    o.setSilent(true);
                    o.setProxyPort(a.getProxyPort());
                }));
            assertTrue(ex.getMessage().contains("port " + a.getProxyPort() + ", for the proxy"), ex.getMessage());
        }
    }

    @Test
    void explicitPortAnotherProgramHoldsSurfacesTheProxysRefusal() throws Exception {
        try (ServerSocket squatter = new ServerSocket(0)) {
            int port = squatter.getLocalPort();
            RuntimeException ex = assertThrows(RuntimeException.class,
                () -> GoldLapel.start(upstream, o -> {
                    o.setSilent(true);
                    o.setProxyPort(port);
                }));
            assertTrue(ex.getMessage().contains("exited with status 1"), ex.getMessage());
            assertTrue(ex.getMessage().contains("I'm afraid port " + port + ", for the proxy, is already in use"),
                ex.getMessage());
        }
    }
}
