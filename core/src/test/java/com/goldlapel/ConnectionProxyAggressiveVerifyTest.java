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
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behaviour of the post-DML aggressive-verify path
 * ({@code java-smart-aggressive-verify}, 2026-05-05): given a wrapped
 * connection in each of the three modes (AUTO / ON / OFF), confirm that a
 * post-INSERT/UPDATE/DELETE invocation does (or doesn't) schedule a verify
 * that ends up reading {@code pg_settings}.
 *
 * <p>Uses the same fake-driver scaffolding pattern as
 * {@link ConnectionProxyVerifyTest} so we don't need a live Postgres.
 */
class ConnectionProxyAggressiveVerifyTest {

    @BeforeEach
    void setup() {
        NativeCache.reset();
        AggressiveVerifyDetector.resetForTesting();
    }

    @AfterEach
    void cleanup() {
        NativeCache.reset();
        AggressiveVerifyDetector.resetForTesting();
    }

    @Test
    void onModeSchedulesVerifyAfterInsert() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        driver.pgSettings.put("app.user_id", "post-insert");

        Connection wrapped = ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.ON, null);

        try (Statement s = wrapped.createStatement()) {
            s.executeUpdate("INSERT INTO orders (user_id, total) VALUES (1, 100)");
        }

        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);
        // dirty must be set immediately — the executor task can't run while
        // the user thread held the connection lock through executeUpdate.
        assertTrue(handler.gucState.isDirty(),
            "ON mode + INSERT must mark dirty + schedule a verify");

        // Wait for the verify executor to drain.
        long deadline = System.currentTimeMillis() + 2000;
        while (handler.gucState.isDirty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertFalse(handler.gucState.isDirty(),
            "post-INSERT verify must clear dirty within the deadline");
        assertEquals(1, driver.verifyCount.get(),
            "exactly one pg_settings verify must have run");
    }

    @Test
    void onModeSchedulesVerifyAfterUpdateDeleteAndTruncate() throws Exception {
        NativeCache cache = makeConnectedCache();

        for (String sql : new String[] {
            "UPDATE orders SET total = 200 WHERE id = 1",
            "DELETE FROM orders WHERE id = 1",
            "TRUNCATE TABLE orders",
            "MERGE INTO orders USING src ON orders.id = src.id WHEN MATCHED THEN UPDATE SET total = src.total"
        }) {
            FakeDriverState driver = new FakeDriverState();
            driver.pgSettings.put("app.user_id", "any");

            Connection wrapped = ConnectionProxy.wrap(
                makeFakeConnection(driver), cache, AggressiveVerifyMode.ON, null);

            try (Statement s = wrapped.createStatement()) {
                s.executeUpdate(sql);
            }

            // Drain.
            ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);
            long deadline = System.currentTimeMillis() + 2000;
            while (handler.gucState.isDirty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(1, driver.verifyCount.get(),
                "ON mode + DML must trigger one verify (sql: " + sql + ")");
        }
    }

    @Test
    void offModeDoesNotScheduleVerifyAfterInsert() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();

        Connection wrapped = ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.OFF, null);

        try (Statement s = wrapped.createStatement()) {
            s.executeUpdate("INSERT INTO orders (user_id) VALUES (1)");
        }

        // Give any spuriously-scheduled verify a chance to run.
        Thread.sleep(100);

        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);
        assertFalse(handler.gucState.isDirty(),
            "OFF mode must not mark dirty after a write");
        assertEquals(0, driver.verifyCount.get(),
            "OFF mode must not run a verify after a write");
    }

    @Test
    void offModeStillSchedulesVerifyAfterFunctionCall() throws Exception {
        // OFF only suppresses the post-DML expansion. The Wave 1 post-function-
        // call verify must still run — that's the safety net for stored
        // procedures that issue SETs we couldn't see on the wire.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        driver.pgSettings.put("app.user_id", "fn");

        Connection wrapped = ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.OFF, null);

        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT my_func()");
            assertTrue(rs.next());
        }

        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);
        assertTrue(handler.gucState.isDirty(),
            "Wave 1's post-function-call verify must still fire in OFF mode");
        long deadline = System.currentTimeMillis() + 2000;
        while (handler.gucState.isDirty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(1, driver.verifyCount.get(),
            "function-call verify must run once even in OFF mode");
    }

    @Test
    void autoModeWithDetectedTriggerEnablesVerify() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        driver.pgSettings.put("app.user_id", "auto-on");
        // Probe response: trigger detected.
        driver.probeReturns = Boolean.TRUE;

        Connection wrapped = ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.AUTO,
            "jdbc:test:auto-detected-on");

        // First, the probe ran once on wrap.
        assertEquals(1, driver.probeCount.get(),
            "AUTO mode must run the probe on the first wrap for a URL");

        // Now an INSERT — verify must follow.
        try (Statement s = wrapped.createStatement()) {
            s.executeUpdate("INSERT INTO orders (id) VALUES (1)");
        }

        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);
        long deadline = System.currentTimeMillis() + 2000;
        while (handler.gucState.isDirty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(1, driver.verifyCount.get(),
            "AUTO + detected-trigger must behave like ON for post-DML");
    }

    @Test
    void autoModeWithoutDetectedTriggerStaysOff() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        driver.probeReturns = Boolean.FALSE;

        Connection wrapped = ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.AUTO,
            "jdbc:test:auto-detected-off");

        try (Statement s = wrapped.createStatement()) {
            s.executeUpdate("INSERT INTO orders (id) VALUES (1)");
        }

        Thread.sleep(100);

        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);
        assertFalse(handler.gucState.isDirty(),
            "AUTO + no-trigger-detected must behave like OFF for post-DML");
        assertEquals(0, driver.verifyCount.get(),
            "AUTO + no-trigger-detected must not run a verify");
    }

    @Test
    void autoModeProbesOncePerJdbcUrl() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        driver.probeReturns = Boolean.TRUE;

        // Wrap the same fake connection twice with the same URL — second
        // wrap must re-use the cached probe result, never re-probe.
        ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.AUTO,
            "jdbc:test:reused-url");
        ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.AUTO,
            "jdbc:test:reused-url");

        assertEquals(1, driver.probeCount.get(),
            "AUTO must probe only on the first wrap per URL");
    }

    @Test
    void autoModeRespectsLicenseOverride() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        // Even though the schema would probe true, license said false —
        // license wins, no probe runs.
        driver.probeReturns = Boolean.TRUE;
        AggressiveVerifyDetector.setLicenseOverride("jdbc:test:license-wins", false);

        Connection wrapped = ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.AUTO,
            "jdbc:test:license-wins");

        assertEquals(0, driver.probeCount.get(),
            "license override must short-circuit AUTO's probe");

        try (Statement s = wrapped.createStatement()) {
            s.executeUpdate("INSERT INTO orders (id) VALUES (1)");
        }

        Thread.sleep(100);
        assertEquals(0, driver.verifyCount.get(),
            "license-overridden-off must suppress post-DML verify");
    }

    @Test
    void preparedStatementUpdateAlsoSchedulesVerifyInOnMode() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        driver.pgSettings.put("app.user_id", "prep");

        Connection wrapped = ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.ON, null);

        try (PreparedStatement ps = wrapped.prepareStatement("UPDATE orders SET total = ? WHERE id = ?")) {
            ps.setInt(1, 200);
            ps.setInt(2, 1);
            ps.executeUpdate();
        }

        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);
        long deadline = System.currentTimeMillis() + 2000;
        while (handler.gucState.isDirty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(1, driver.verifyCount.get(),
            "PreparedStatement.executeUpdate must also fire post-DML verify in ON mode");
    }

    @Test
    void onModeSelectDoesNotScheduleVerify() throws Exception {
        // SELECT queries are not writes; aggressive-verify should only fire on
        // statements that detectWritesMulti recognises as DML/DDL.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();

        Connection wrapped = ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.ON, null);

        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1 FROM orders WHERE id = 1");
            assertTrue(rs.next());
        }

        Thread.sleep(100);
        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);
        assertFalse(handler.gucState.isDirty(),
            "ON mode + plain SELECT (no function call, no write) must NOT schedule a verify");
        assertEquals(0, driver.verifyCount.get());
    }

    // --- fake driver scaffolding ---

    private static NativeCache makeConnectedCache() throws Exception {
        NativeCache cache = NativeCache.getInstance();
        java.lang.reflect.Field connected = NativeCache.class.getDeclaredField("invalidationConnected");
        connected.setAccessible(true);
        connected.setBoolean(cache, true);
        return cache;
    }

    private static ConnectionProxy.ConnectionHandler handlerFor(Connection wrapped) {
        return (ConnectionProxy.ConnectionHandler) Proxy.getInvocationHandler(wrapped);
    }

    private static class FakeDriverState {
        final Map<String, String> pgSettings = new LinkedHashMap<>();
        final AtomicInteger verifyCount = new AtomicInteger();
        final AtomicInteger probeCount = new AtomicInteger();
        Boolean probeReturns = Boolean.FALSE;
    }

    private static Connection makeFakeConnection(FakeDriverState driver) {
        return (Connection) Proxy.newProxyInstance(
            ConnectionProxyAggressiveVerifyTest.class.getClassLoader(),
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
                case "prepareStatement":
                    return makeFakePreparedStatement(driver, (String) args[0]);
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
            ConnectionProxyAggressiveVerifyTest.class.getClassLoader(),
            new Class[]{Statement.class},
            (proxy, method, args) -> {
                switch (method.getName()) {
                    case "executeQuery": {
                        String sql = (String) args[0];
                        if (sql.contains("pg_settings")) {
                            driver.verifyCount.incrementAndGet();
                            return makePgSettingsResultSet(driver.pgSettings);
                        }
                        return makeSingleScalarResultSet("data");
                    }
                    case "executeUpdate":
                        return Integer.valueOf(1);
                    case "execute":
                        return Boolean.FALSE;
                    case "close":
                        return null;
                    default:
                        return defaultReturn(method.getReturnType());
                }
            });
    }

    private static PreparedStatement makeFakePreparedStatement(FakeDriverState driver, String sql) {
        return (PreparedStatement) Proxy.newProxyInstance(
            ConnectionProxyAggressiveVerifyTest.class.getClassLoader(),
            new Class[]{PreparedStatement.class},
            (proxy, method, args) -> {
                switch (method.getName()) {
                    case "executeQuery":
                        // No-arg form — runs against the bound SQL.
                        if (args == null || args.length == 0) {
                            if (sql.contains("pg_trigger")) {
                                // The detector probe path.
                                driver.probeCount.incrementAndGet();
                                return makeBooleanResultSet(driver.probeReturns);
                            }
                            return makeSingleScalarResultSet("data");
                        }
                        // String overload (rare on PS).
                        return makeSingleScalarResultSet("data");
                    case "executeUpdate":
                        return Integer.valueOf(1);
                    case "execute":
                        return Boolean.FALSE;
                    case "close":
                        return null;
                    default:
                        if (method.getName().startsWith("set")) return null;
                        return defaultReturn(method.getReturnType());
                }
            });
    }

    private static ResultSet makeSingleScalarResultSet(String scalar) {
        return (ResultSet) Proxy.newProxyInstance(
            ConnectionProxyAggressiveVerifyTest.class.getClassLoader(),
            new Class[]{ResultSet.class},
            new InvocationHandler() {
                boolean nextCalled = false;
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) {
                    switch (method.getName()) {
                        case "next":
                            if (!nextCalled) { nextCalled = true; return Boolean.TRUE; }
                            return Boolean.FALSE;
                        case "getString":
                        case "getObject":
                            return scalar;
                        case "getMetaData":
                            return makeFakeMetaData(1, "data");
                        case "close":
                            return null;
                        default:
                            return defaultReturn(method.getReturnType());
                    }
                }
            });
    }

    private static ResultSet makeBooleanResultSet(Boolean value) {
        return (ResultSet) Proxy.newProxyInstance(
            ConnectionProxyAggressiveVerifyTest.class.getClassLoader(),
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
                            return makeFakeMetaData(1, "exists");
                        case "close":
                            return null;
                        default:
                            return defaultReturn(method.getReturnType());
                    }
                }
            });
    }

    private static ResultSet makePgSettingsResultSet(Map<String, String> rows) {
        return (ResultSet) Proxy.newProxyInstance(
            ConnectionProxyAggressiveVerifyTest.class.getClassLoader(),
            new Class[]{ResultSet.class},
            new InvocationHandler() {
                final java.util.Iterator<Map.Entry<String, String>> it = rows.entrySet().iterator();
                Map.Entry<String, String> current;

                @Override
                public Object invoke(Object proxy, Method method, Object[] args) {
                    switch (method.getName()) {
                        case "next":
                            if (it.hasNext()) {
                                current = it.next();
                                return Boolean.TRUE;
                            }
                            return Boolean.FALSE;
                        case "getString":
                            int col = args[0] instanceof Integer ? (Integer) args[0] : 0;
                            if (col == 1) return current.getKey();
                            if (col == 2) return current.getValue();
                            return null;
                        case "getMetaData":
                            return makeFakeMetaData(2, "name", "setting");
                        case "close":
                            return null;
                        default:
                            return defaultReturn(method.getReturnType());
                    }
                }
            });
    }

    private static ResultSetMetaData makeFakeMetaData(int columnCount, String... labels) {
        return (ResultSetMetaData) Proxy.newProxyInstance(
            ConnectionProxyAggressiveVerifyTest.class.getClassLoader(),
            new Class[]{ResultSetMetaData.class},
            (proxy, method, args) -> {
                switch (method.getName()) {
                    case "getColumnCount":
                        return Integer.valueOf(columnCount);
                    case "getColumnLabel":
                    case "getColumnName": {
                        int idx = (Integer) args[0] - 1;
                        return idx >= 0 && idx < labels.length ? labels[idx] : "col";
                    }
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
