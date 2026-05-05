package com.goldlapel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for the GUC state-hash wiring in
 * {@link ConnectionProxy}. Two simulated tenants ({@code app.user_id=42},
 * {@code app.user_id=99}) issue the same query against a faked driver — the
 * native cache must serve each tenant their own rows, never cross-leak.
 *
 * <p>Uses a tiny java.sql {@link Proxy}-backed fake to avoid a real Postgres
 * dependency. The fake returns a constant scalar that varies with a probe
 * value the test can set, so we can verify cache-hit behaviour by inspecting
 * which probe value comes back from each tenant's read.
 */
class ConnectionProxyGucIntegrationTest {

    @BeforeEach
    void setup() { NativeCache.reset(); }

    @AfterEach
    void cleanup() { NativeCache.reset(); }

    @Test
    void differentTenantsDoNotShareCacheEntries() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection raw = makeFakeConnection(driver);
        Connection wrapped = ConnectionProxy.wrap(raw, cache);

        // Tenant A: SET app.user_id=42, then read.
        try (Statement s = wrapped.createStatement()) {
            s.execute("SET app.user_id = '42'");
        }
        // Drive value: tenant A is reading "42-rows".
        driver.scalar = "42-rows";
        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT data FROM accounts");
            assertTrue(rs.next());
            assertEquals("42-rows", rs.getString(1));
        }

        // Now tenant B reuses the same connection (rare in real life but
        // tests the worst-case sharing scenario), SETs a different user_id,
        // and reads the same SQL. The fake driver returns "99-rows" — if the
        // state-hash isn't folded into the cache key, the wrapper would
        // serve tenant A's "42-rows" by mistake.
        try (Statement s = wrapped.createStatement()) {
            s.execute("SET app.user_id = '99'");
        }
        driver.scalar = "99-rows";
        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT data FROM accounts");
            assertTrue(rs.next());
            assertEquals("99-rows", rs.getString(1),
                "state-hash must isolate tenants — got A's cached row for B");
        }

        // Sanity: switching back to A's GUC value MUST hit the original
        // cache slot (the proxy stored "42-rows" under user_id=42's hash).
        try (Statement s = wrapped.createStatement()) {
            s.execute("SET app.user_id = '42'");
        }
        // Change the underlying scalar so we'd notice a re-read.
        driver.scalar = "WOULD-NOT-BE-CACHED";
        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT data FROM accounts");
            assertTrue(rs.next());
            assertEquals("42-rows", rs.getString(1),
                "switching back to A's GUC must hit A's cached slot");
        }
    }

    @Test
    void safeSetDoesNotChangeCacheKey() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection raw = makeFakeConnection(driver);
        Connection wrapped = ConnectionProxy.wrap(raw, cache);

        // Read once, then issue a HARMLESS SET (timezone), then read again.
        // The state hash must NOT move, so the second read must hit the cache.
        driver.scalar = "first";
        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT data FROM accounts");
            assertTrue(rs.next());
            assertEquals("first", rs.getString(1));
        }
        try (Statement s = wrapped.createStatement()) {
            s.execute("SET timezone = 'UTC'");
        }
        driver.scalar = "WOULD-NOT-BE-CACHED-IF-HIT";
        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT data FROM accounts");
            assertTrue(rs.next());
            assertEquals("first", rs.getString(1),
                "safe SET must not invalidate the cache key");
        }
    }

    // --- fake driver scaffolding ---

    private static NativeCache makeConnectedCache() throws Exception {
        NativeCache cache = NativeCache.getInstance();
        java.lang.reflect.Field connected = NativeCache.class.getDeclaredField("invalidationConnected");
        connected.setAccessible(true);
        connected.setBoolean(cache, true);
        return cache;
    }

    /**
     * Minimal mutable state shared between the fake Connection / Statement /
     * ResultSet. The {@link #scalar} value is what {@code executeQuery} will
     * return as a single-column "data" row on its next call.
     */
    private static class FakeDriverState {
        String scalar = "default";
    }

    /** Returns a {@link Connection} proxy backed by an in-memory state object. */
    private static Connection makeFakeConnection(FakeDriverState driver) {
        return (Connection) Proxy.newProxyInstance(
            ConnectionProxyGucIntegrationTest.class.getClassLoader(),
            new Class[]{Connection.class},
            new FakeConnectionHandler(driver));
    }

    private static class FakeConnectionHandler implements InvocationHandler {
        final FakeDriverState driver;
        FakeConnectionHandler(FakeDriverState driver) { this.driver = driver; }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "createStatement":
                    return makeFakeStatement(driver);
                case "getAutoCommit":
                    return Boolean.TRUE;
                case "setAutoCommit":
                case "close":
                case "commit":
                case "rollback":
                    return null;
                default:
                    return defaultReturn(method.getReturnType());
            }
        }
    }

    private static Statement makeFakeStatement(FakeDriverState driver) {
        return (Statement) Proxy.newProxyInstance(
            ConnectionProxyGucIntegrationTest.class.getClassLoader(),
            new Class[]{Statement.class},
            new FakeStatementHandler(driver));
    }

    private static class FakeStatementHandler implements InvocationHandler {
        final FakeDriverState driver;
        FakeStatementHandler(FakeDriverState driver) { this.driver = driver; }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "executeQuery":
                    // Return a single-row, single-column ResultSet whose
                    // value is the driver's current `scalar`. Snapshotting
                    // the value at executeQuery time matches real driver
                    // semantics (the rows materialise at execution).
                    return makeFakeResultSet(driver.scalar);
                case "execute":
                case "executeUpdate":
                    // SET commands path through here. Always succeed.
                    return method.getReturnType() == int.class ? Integer.valueOf(0) : Boolean.FALSE;
                case "close":
                    return null;
                default:
                    return defaultReturn(method.getReturnType());
            }
        }
    }

    private static ResultSet makeFakeResultSet(String scalar) {
        return (ResultSet) Proxy.newProxyInstance(
            ConnectionProxyGucIntegrationTest.class.getClassLoader(),
            new Class[]{ResultSet.class},
            new FakeResultSetHandler(scalar));
    }

    private static class FakeResultSetHandler implements InvocationHandler {
        final String scalar;
        boolean nextCalled = false;
        FakeResultSetHandler(String scalar) { this.scalar = scalar; }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "next":
                    if (!nextCalled) {
                        nextCalled = true;
                        return Boolean.TRUE;
                    }
                    return Boolean.FALSE;
                case "getString":
                case "getObject":
                    return scalar;
                case "getMetaData":
                    return makeFakeMetaData();
                case "close":
                    return null;
                default:
                    return defaultReturn(method.getReturnType());
            }
        }
    }

    private static ResultSetMetaData makeFakeMetaData() {
        return (ResultSetMetaData) Proxy.newProxyInstance(
            ConnectionProxyGucIntegrationTest.class.getClassLoader(),
            new Class[]{ResultSetMetaData.class},
            (proxy, method, args) -> {
                switch (method.getName()) {
                    case "getColumnCount":
                        return Integer.valueOf(1);
                    case "getColumnLabel":
                    case "getColumnName":
                        return "data";
                    default:
                        return defaultReturn(method.getReturnType());
                }
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
