package com.goldlapel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Detection logic for the post-DML aggressive-verify smart-auto path. The
 * detector probes {@code pg_trigger}/{@code pg_proc} on the first connection
 * for a given JDBC URL and caches the boolean answer for the JVM's lifetime.
 *
 * <p>Tests use a hand-rolled JDBC proxy so we can return whatever the probe
 * "found" without spinning up Postgres. Mirrors the fake-driver scaffolding
 * in {@link ConnectionProxyVerifyTest}.
 */
class AggressiveVerifyDetectorTest {

    @BeforeEach
    void setup() {
        AggressiveVerifyDetector.resetForTesting();
    }

    @AfterEach
    void cleanup() {
        AggressiveVerifyDetector.resetForTesting();
    }

    @Test
    void returnsTrueWhenProbeFindsSetIssuingTrigger() {
        FakeProbeDriver driver = new FakeProbeDriver();
        driver.probeReturns = Boolean.TRUE;
        Connection conn = makeFakeConnection(driver);
        boolean active = AggressiveVerifyDetector.isActive("jdbc:test:url-a", conn);
        assertTrue(active, "trigger detected → aggressive-verify must be on");
        assertEquals(1, driver.probeCount.get(), "probe must run exactly once");
    }

    @Test
    void returnsFalseWhenProbeFindsNoSetIssuingTrigger() {
        FakeProbeDriver driver = new FakeProbeDriver();
        driver.probeReturns = Boolean.FALSE;
        Connection conn = makeFakeConnection(driver);
        boolean active = AggressiveVerifyDetector.isActive("jdbc:test:url-b", conn);
        assertFalse(active, "no SET-issuing trigger → aggressive-verify off");
        assertEquals(1, driver.probeCount.get(), "probe must run exactly once");
    }

    @Test
    void cachesPerUrlAcrossCalls() {
        FakeProbeDriver driver = new FakeProbeDriver();
        driver.probeReturns = Boolean.TRUE;
        Connection conn = makeFakeConnection(driver);
        AggressiveVerifyDetector.isActive("jdbc:test:url-c", conn);
        AggressiveVerifyDetector.isActive("jdbc:test:url-c", conn);
        AggressiveVerifyDetector.isActive("jdbc:test:url-c", conn);
        assertEquals(1, driver.probeCount.get(),
            "second + third lookups must hit the per-URL cache");
    }

    @Test
    void distinctUrlsProbeIndependently() {
        FakeProbeDriver driver = new FakeProbeDriver();
        driver.probeReturns = Boolean.TRUE;
        Connection conn = makeFakeConnection(driver);
        AggressiveVerifyDetector.isActive("jdbc:test:db1", conn);
        AggressiveVerifyDetector.isActive("jdbc:test:db2", conn);
        AggressiveVerifyDetector.isActive("jdbc:test:db1", conn);
        assertEquals(2, driver.probeCount.get(),
            "two URLs → two probes; second db1 lookup hits cache");
    }

    @Test
    void licenseOverrideSkipsProbe() {
        FakeProbeDriver driver = new FakeProbeDriver();
        driver.probeReturns = Boolean.TRUE;
        Connection conn = makeFakeConnection(driver);
        AggressiveVerifyDetector.setLicenseOverride("jdbc:test:license-on", true);
        boolean active = AggressiveVerifyDetector.isActive("jdbc:test:license-on", conn);
        assertTrue(active);
        assertEquals(0, driver.probeCount.get(),
            "license override must short-circuit the probe");
    }

    @Test
    void licenseOverrideCanForceOff() {
        FakeProbeDriver driver = new FakeProbeDriver();
        driver.probeReturns = Boolean.TRUE;
        Connection conn = makeFakeConnection(driver);
        AggressiveVerifyDetector.setLicenseOverride("jdbc:test:license-off", false);
        boolean active = AggressiveVerifyDetector.isActive("jdbc:test:license-off", conn);
        assertFalse(active);
        assertEquals(0, driver.probeCount.get(),
            "license override (false) must skip the probe");
    }

    @Test
    void probeFailureCachesAsOff() {
        FakeProbeDriver driver = new FakeProbeDriver();
        driver.failProbe = true;
        Connection conn = makeFakeConnection(driver);
        boolean active = AggressiveVerifyDetector.isActive("jdbc:test:fail", conn);
        assertFalse(active, "probe failure must cache as off, not propagate the SQLException");
        // Second call must NOT re-probe — we cached the off decision.
        AggressiveVerifyDetector.isActive("jdbc:test:fail", conn);
        assertEquals(1, driver.probeCount.get(),
            "failure must be sticky-cached; never retry on the customer's hot path");
    }

    @Test
    void nullUrlSkipsDetection() {
        FakeProbeDriver driver = new FakeProbeDriver();
        driver.probeReturns = Boolean.TRUE;
        Connection conn = makeFakeConnection(driver);
        boolean active = AggressiveVerifyDetector.isActive(null, conn);
        assertFalse(active);
        assertEquals(0, driver.probeCount.get(),
            "null URL → no probe (caller deliberately opted out)");
    }

    @Test
    void peekDoesNotTriggerProbe() {
        assertNull(AggressiveVerifyDetector.peek("jdbc:test:never-probed"),
            "peek must return null for un-probed URLs");
        AggressiveVerifyDetector.setLicenseOverride("jdbc:test:peeked", true);
        assertEquals(Boolean.TRUE, AggressiveVerifyDetector.peek("jdbc:test:peeked"));
    }

    // --- fake driver ---

    private static class FakeProbeDriver {
        Boolean probeReturns = Boolean.FALSE;
        boolean failProbe = false;
        final AtomicInteger probeCount = new AtomicInteger();
    }

    private static Connection makeFakeConnection(FakeProbeDriver driver) {
        return (Connection) Proxy.newProxyInstance(
            AggressiveVerifyDetectorTest.class.getClassLoader(),
            new Class[]{Connection.class},
            (proxy, method, args) -> {
                if ("prepareStatement".equals(method.getName())) {
                    String sql = (String) args[0];
                    assertNotNull(sql, "probe SQL must be non-null");
                    assertTrue(sql.contains("pg_trigger"),
                        "probe must query pg_trigger; got: " + sql);
                    return makeFakePreparedStatement(driver);
                }
                if ("close".equals(method.getName())) return null;
                return defaultReturn(method.getReturnType());
            });
    }

    private static PreparedStatement makeFakePreparedStatement(FakeProbeDriver driver) {
        return (PreparedStatement) Proxy.newProxyInstance(
            AggressiveVerifyDetectorTest.class.getClassLoader(),
            new Class[]{PreparedStatement.class},
            new InvocationHandler() {
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) throws SQLException {
                    if ("executeQuery".equals(method.getName())) {
                        driver.probeCount.incrementAndGet();
                        if (driver.failProbe) {
                            throw new SQLException("simulated probe failure");
                        }
                        return makeBooleanResultSet(driver.probeReturns);
                    }
                    if ("close".equals(method.getName())) return null;
                    return defaultReturn(method.getReturnType());
                }
            });
    }

    private static ResultSet makeBooleanResultSet(Boolean value) {
        return (ResultSet) Proxy.newProxyInstance(
            AggressiveVerifyDetectorTest.class.getClassLoader(),
            new Class[]{ResultSet.class},
            new InvocationHandler() {
                boolean nextCalled = false;
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) {
                    switch (method.getName()) {
                        case "next":
                            if (!nextCalled) { nextCalled = true; return Boolean.TRUE; }
                            return Boolean.FALSE;
                        case "getBoolean":
                            return value == null ? Boolean.FALSE : value;
                        case "getMetaData":
                            return makeMetaData();
                        case "close":
                            return null;
                        default:
                            return defaultReturn(method.getReturnType());
                    }
                }
            });
    }

    private static ResultSetMetaData makeMetaData() {
        return (ResultSetMetaData) Proxy.newProxyInstance(
            AggressiveVerifyDetectorTest.class.getClassLoader(),
            new Class[]{ResultSetMetaData.class},
            (proxy, method, args) -> {
                if ("getColumnCount".equals(method.getName())) return 1;
                if ("getColumnLabel".equals(method.getName())
                    || "getColumnName".equals(method.getName())) return "exists";
                return defaultReturn(method.getReturnType());
            });
    }

    private static final Map<Class<?>, Object> PRIMITIVE_DEFAULTS = new HashMap<>();
    static {
        PRIMITIVE_DEFAULTS.put(boolean.class, Boolean.FALSE);
        PRIMITIVE_DEFAULTS.put(byte.class, (byte) 0);
        PRIMITIVE_DEFAULTS.put(short.class, (short) 0);
        PRIMITIVE_DEFAULTS.put(int.class, 0);
        PRIMITIVE_DEFAULTS.put(long.class, 0L);
        PRIMITIVE_DEFAULTS.put(float.class, 0.0f);
        PRIMITIVE_DEFAULTS.put(double.class, 0.0);
        PRIMITIVE_DEFAULTS.put(char.class, '\0');
    }

    private static Object defaultReturn(Class<?> ret) {
        if (ret == void.class) return null;
        return PRIMITIVE_DEFAULTS.getOrDefault(ret, null);
    }
}
