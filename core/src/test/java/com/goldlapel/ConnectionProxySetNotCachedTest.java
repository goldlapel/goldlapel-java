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
 * Integration tests for the cache-skip on session-state commands
 * ({@code wrapper-cache-set-responses.md}, filed 2026-05-04). The PG JDBC
 * driver permits {@code Statement.executeQuery("SET ...")} even though the
 * command returns no rows; pre-fix the empty result-set landed in the native
 * cache, bloating it with entries that never serve real data and applying
 * eviction pressure on real reads.
 *
 * <p>Plumbing mirrors {@link ConnectionProxyGucIntegrationTest} — a
 * {@link Proxy}-backed fake driver that takes no real-Postgres dependency.
 */
class ConnectionProxySetNotCachedTest {

    @BeforeEach
    void setup() { NativeCache.reset(); }

    @AfterEach
    void cleanup() { NativeCache.reset(); }

    @Test
    void setResponseNotCached() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        // Route a SET through executeQuery — JDBC permits this even though the
        // command returns no rows. Pre-fix this would land an empty entry in
        // the cache, bloating it for no benefit.
        try (Statement s = wrapped.createStatement()) {
            s.executeQuery("SET app.user_id = '42'");
        }

        assertEquals(0, cache.size(),
            "SET response must not be stored in the native cache");
    }

    @Test
    void resetResponseNotCached() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        try (Statement s = wrapped.createStatement()) {
            s.executeQuery("RESET ALL");
        }

        assertEquals(0, cache.size(),
            "RESET response must not be stored in the native cache");
    }

    @Test
    void listenAndNotifyResponsesNotCached() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        try (Statement s = wrapped.createStatement()) {
            s.executeQuery("LISTEN ch_x");
            s.executeQuery("NOTIFY ch_x");
            s.executeQuery("UNLISTEN ch_x");
        }

        assertEquals(0, cache.size(),
            "LISTEN / NOTIFY / UNLISTEN responses must not be stored in the native cache");
    }

    @Test
    void selectStillCached() throws Exception {
        // Sanity: the skip-list must NOT swallow real reads. A SELECT continues
        // to populate the cache as before.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        driver.scalar = "row1";
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT data FROM orders");
            assertTrue(rs.next());
            assertEquals("row1", rs.getString(1));
        }

        assertNotNull(cache.get("SELECT data FROM orders", null),
            "SELECT response should still cache normally");
    }

    // --- fake driver scaffolding (mirrors ConnectionProxyGucIntegrationTest) ---

    private static NativeCache makeConnectedCache() throws Exception {
        NativeCache cache = NativeCache.getInstance();
        java.lang.reflect.Field connected = NativeCache.class.getDeclaredField("invalidationConnected");
        connected.setAccessible(true);
        connected.setBoolean(cache, true);
        return cache;
    }

    private static class FakeDriverState {
        String scalar = "default";
    }

    private static Connection makeFakeConnection(FakeDriverState driver) {
        return (Connection) Proxy.newProxyInstance(
            ConnectionProxySetNotCachedTest.class.getClassLoader(),
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
            ConnectionProxySetNotCachedTest.class.getClassLoader(),
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
                    return makeFakeResultSet(driver.scalar);
                case "execute":
                case "executeUpdate":
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
            ConnectionProxySetNotCachedTest.class.getClassLoader(),
            new Class[]{ResultSet.class},
            new FakeResultSetHandler(scalar));
    }

    private static class FakeResultSetHandler implements InvocationHandler {
        final String scalar;
        boolean nextCalled = false;
        FakeResultSetHandler(String scalar) { this.scalar = scalar; }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
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
            ConnectionProxySetNotCachedTest.class.getClassLoader(),
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
