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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the GUC-RLS hardening fallback paths
 * ({@code java-rls-hardening}, 2026-05-05): post-call async verify after a
 * top-level {@code SELECT <function>(...)} or {@code CALL <proc>(...)}, plus
 * the lazy verify-on-checkout that runs synchronously before the next cache
 * lookup if the dirty flag is set.
 *
 * <p>Uses a hand-rolled JDBC proxy so we can drive {@code pg_settings} reads
 * without spinning up Postgres. Mirrors the scaffolding pattern in
 * {@link ConnectionProxyGucIntegrationTest}.
 */
class ConnectionProxyVerifyTest {

    @BeforeEach
    void setup() { NativeCache.reset(); }

    @AfterEach
    void cleanup() { NativeCache.reset(); }

    @Test
    void postCallVerifySchedulesAfterTopLevelFunction() throws Exception {
        // Wire the function-call observation: a SELECT my_func() call must
        // mark the connection's gucState dirty AND submit a verify task.
        // We capture both signals via the fake driver to avoid timing flakiness.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        driver.pgSettings.put("app.user_id", "post-call-99");

        Connection raw = makeFakeConnection(driver);
        Connection wrapped = ConnectionProxy.wrap(raw, cache);

        // Run the function call.
        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT my_func()");
            assertTrue(rs.next());
        }

        // dirty flag must be set immediately after the call returns (the
        // executor task can't have run yet because it queues behind the
        // connection lock the call held).
        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);
        assertTrue(handler.gucState.isDirty(),
            "post-call verify must mark dirty so the next call re-reads pg_settings");

        // Wait for the verify executor to drain — the verify reads pg_settings
        // via the fake driver, which now reports app.user_id=post-call-99.
        // After the task runs, dirty clears and the state hash reflects the
        // newly-discovered server-side state.
        long deadline = System.currentTimeMillis() + 2000;
        while (handler.gucState.isDirty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertFalse(handler.gucState.isDirty(),
            "verify task must clear dirty within the deadline");
        assertEquals(1, driver.verifyCount.get(),
            "exactly one pg_settings verify must have run");

        // Reading the same SQL again now uses the post-verify state hash —
        // we don't assert on the exact hash value, just that we did populate
        // app.user_id from the verify.
        long postVerifyHash = handler.gucState.hash();
        assertNotEquals(0L, postVerifyHash,
            "verify must have populated app.user_id from pg_settings");
    }

    @Test
    void plainSelectDoesNotScheduleVerify() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT * FROM accounts");
            assertTrue(rs.next());
        }

        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);
        assertFalse(handler.gucState.isDirty(),
            "plain SELECT must not schedule verify or mark dirty");

        // Give the executor a moment in case anything was queued.
        Thread.sleep(100);
        assertEquals(0, driver.verifyCount.get(),
            "no verify should run for plain SELECTs");
    }

    @Test
    void verifyOnCheckoutReconcilesDirtyState() throws Exception {
        // The lazy fallback path: an external mutation marks dirty, the next
        // user query reconciles before consulting the cache.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        driver.pgSettings.put("app.user_id", "lazy-77");

        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);
        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);

        // Simulate "something happened we couldn't see on the wire" — a
        // trigger fired server-side, etc.
        handler.gucState.markDirty();
        assertTrue(handler.gucState.isDirty());
        long preHash = handler.gucState.hash();

        // The next plain SELECT runs verify-on-checkout BEFORE building the
        // cache key, then proceeds with the freshly-reconciled state hash.
        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT * FROM accounts");
            assertTrue(rs.next());
        }

        assertFalse(handler.gucState.isDirty(),
            "verifyIfDirty must clear dirty after a successful read");
        assertNotEquals(preHash, handler.gucState.hash(),
            "verify must have rebuilt state from pg_settings");
        assertEquals(1, driver.verifyCount.get(),
            "exactly one pg_settings verify must have run");
    }

    @Test
    void verifyFailureLeavesDirtySet() throws Exception {
        // If the verify SQL throws, the dirty flag must remain set so the
        // next checkout retries — and the user's hot path must not propagate
        // the verify error.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        driver.failVerify = true;

        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);
        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);
        handler.gucState.markDirty();

        // Read should succeed even though verify throws inside it.
        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT * FROM accounts");
            assertTrue(rs.next());
        }
        assertTrue(handler.gucState.isDirty(),
            "verify failure must leave dirty set so the next call retries");
    }

    @Test
    void verifyExecutorSerializesWithUserCalls() throws Exception {
        // Concurrency interlock: the post-call verify executor and the
        // user-facing JDBC call MUST serialise on the connection lock,
        // otherwise two threads would race against the same physical
        // Connection (which JDBC forbids per spec). We simulate this by
        // making verify slow and confirming the executor's verify either
        // ran-before or ran-after the user's next call — never overlapped.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        driver.pgSettings.put("app.user_id", "race");
        driver.verifyHoldMs = 200;

        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);
        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);

        // Trigger a verify by running a function call.
        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT my_func()");
            assertTrue(rs.next());
        }

        // Issue a series of subsequent reads; the test passes if no overlap
        // is observed (driver tracks concurrent active calls).
        for (int i = 0; i < 5; i++) {
            try (Statement s = wrapped.createStatement()) {
                ResultSet rs = s.executeQuery("SELECT * FROM accounts WHERE i = " + i);
                assertTrue(rs.next());
            }
        }

        // Wait for any in-flight verify to drain.
        long deadline = System.currentTimeMillis() + 2000;
        while (handler.gucState.isDirty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }

        assertEquals(0, driver.maxConcurrent.get() > 1 ? driver.maxConcurrent.get() : 0,
            "verify and user calls must never overlap on a single connection");
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
        // Public Proxy API — no module-opens dance, works on Java 17+.
        return (ConnectionProxy.ConnectionHandler) Proxy.getInvocationHandler(wrapped);
    }

    /**
     * Driver state. {@code pgSettings} is what the fake's
     * {@code SELECT name, setting FROM pg_settings WHERE source='session'}
     * reports back; {@code verifyCount} bumps each time the fake serves that
     * query so tests can assert "verify ran exactly once". {@code verifyHoldMs}
     * lets a test simulate a slow verify, and {@code activeCalls} /
     * {@code maxConcurrent} pin the no-overlap concurrency contract.
     */
    private static class FakeDriverState {
        final Map<String, String> pgSettings = new LinkedHashMap<>();
        final AtomicInteger verifyCount = new AtomicInteger();
        final AtomicInteger activeCalls = new AtomicInteger();
        final AtomicInteger maxConcurrent = new AtomicInteger();
        volatile long verifyHoldMs = 0;
        volatile boolean failVerify = false;
    }

    private static Connection makeFakeConnection(FakeDriverState driver) {
        return (Connection) Proxy.newProxyInstance(
            ConnectionProxyVerifyTest.class.getClassLoader(),
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
            ConnectionProxyVerifyTest.class.getClassLoader(),
            new Class[]{Statement.class},
            new FakeStatementHandler(driver));
    }

    private static class FakeStatementHandler implements InvocationHandler {
        final FakeDriverState driver;
        FakeStatementHandler(FakeDriverState driver) { this.driver = driver; }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            // Track concurrent active calls — the connection lock should make
            // this strictly 1.
            int active = driver.activeCalls.incrementAndGet();
            driver.maxConcurrent.updateAndGet(prev -> Math.max(prev, active));
            try {
                switch (method.getName()) {
                    case "executeQuery": {
                        String sql = (String) args[0];
                        if (sql.contains("pg_settings")) {
                            // Simulate the verify path.
                            if (driver.failVerify) {
                                throw new java.sql.SQLException("simulated verify failure");
                            }
                            if (driver.verifyHoldMs > 0) {
                                Thread.sleep(driver.verifyHoldMs);
                            }
                            driver.verifyCount.incrementAndGet();
                            return makePgSettingsResultSet(driver.pgSettings);
                        }
                        return makeSingleScalarResultSet("data");
                    }
                    case "execute":
                    case "executeUpdate":
                        return method.getReturnType() == int.class ? Integer.valueOf(0) : Boolean.FALSE;
                    case "close":
                        return null;
                    default:
                        return defaultReturn(method.getReturnType());
                }
            } finally {
                driver.activeCalls.decrementAndGet();
            }
        }
    }

    private static ResultSet makeSingleScalarResultSet(String scalar) {
        return (ResultSet) Proxy.newProxyInstance(
            ConnectionProxyVerifyTest.class.getClassLoader(),
            new Class[]{ResultSet.class},
            new SingleScalarResultSetHandler(scalar));
    }

    private static class SingleScalarResultSetHandler implements InvocationHandler {
        final String scalar;
        boolean nextCalled = false;
        SingleScalarResultSetHandler(String scalar) { this.scalar = scalar; }

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
    }

    private static ResultSet makePgSettingsResultSet(Map<String, String> rows) {
        return (ResultSet) Proxy.newProxyInstance(
            ConnectionProxyVerifyTest.class.getClassLoader(),
            new Class[]{ResultSet.class},
            new PgSettingsResultSetHandler(rows));
    }

    private static class PgSettingsResultSetHandler implements InvocationHandler {
        final java.util.Iterator<Map.Entry<String, String>> it;
        Map.Entry<String, String> current;
        PgSettingsResultSetHandler(Map<String, String> rows) {
            this.it = rows.entrySet().iterator();
        }

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
    }

    private static ResultSetMetaData makeFakeMetaData(int columnCount, String... labels) {
        return (ResultSetMetaData) Proxy.newProxyInstance(
            ConnectionProxyVerifyTest.class.getClassLoader(),
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
