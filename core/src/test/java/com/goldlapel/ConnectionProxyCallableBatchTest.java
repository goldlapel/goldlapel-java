package com.goldlapel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the JDBC paths that previously bypassed
 * {@code GucState.observeSql}: {@link CallableStatement} (issued via
 * {@link Connection#prepareCall(String)}) and {@link Statement#addBatch(String)}.
 * Pre-fix, SET commands routed through these paths never shifted the
 * wrapper-side L1 state hash, allowing stale entries to be served against an
 * updated session state.
 *
 * <p>Filed at {@code java-jdbc-callable-batch-statehash-gap.md} (2026-05-04).
 *
 * <p>Plumbing mirrors {@link ConnectionProxyGucIntegrationTest} — a
 * {@link Proxy}-backed fake driver that takes no real-Postgres dependency.
 */
class ConnectionProxyCallableBatchTest {

    @BeforeEach
    void setup() { NativeCache.reset(); }

    @AfterEach
    void cleanup() { NativeCache.reset(); }

    // --- CallableStatement ---

    @Test
    void prepareCallObservesSetCommand() throws Exception {
        // The headline regression: a SET command issued via prepareCall +
        // CallableStatement.execute() must shift the wrapper-side state
        // hash so subsequent reads use a different cache slot. Pre-fix,
        // the call path didn't invoke observeSql at all.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        // Capture the per-connection GucState — read it back via reflection
        // since the field is package-private but the wrapper is in the same
        // package (clean reflection-free access via the instance, not the
        // class).
        GucState gucState = extractGucState(wrapped);
        long before = gucState.hash();

        try (CallableStatement cs = wrapped.prepareCall("SET app.user_id = '42'")) {
            cs.execute();
        }

        long after = gucState.hash();
        assertNotEquals(before, after,
            "SET via CallableStatement.execute() must mutate the GucState hash");
    }

    @Test
    void prepareCallExecuteOnUnsafeGucChangesCacheKey() throws Exception {
        // End-to-end regression: two tenants each set their app.user_id via
        // prepareCall, then read the same SQL. The cache must isolate them.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        try (CallableStatement cs = wrapped.prepareCall("SET app.user_id = '42'")) {
            cs.execute();
        }
        driver.scalar = "42-rows";
        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT data FROM accounts");
            assertTrue(rs.next());
            assertEquals("42-rows", rs.getString(1));
        }

        try (CallableStatement cs = wrapped.prepareCall("SET app.user_id = '99'")) {
            cs.execute();
        }
        driver.scalar = "99-rows";
        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT data FROM accounts");
            assertTrue(rs.next());
            assertEquals("99-rows", rs.getString(1),
                "CallableStatement-driven SET must shift the cache key — got tenant A's row for B");
        }
    }

    @Test
    void prepareCallOnDdlInvalidatesCache() throws Exception {
        // CALL classifies as DDL_SENTINEL, so any prepareCall +
        // CallableStatement.execute() on the bare-CALL form (PG 11+) must
        // trigger a full-cache invalidation. (The JDBC escape form
        // `{call ...}` first-tokens as `{call` and slips past detectWrite —
        // a cosmetic gap not in scope here; users hitting CALL through the
        // escape form would still go through the proxy-side guard. The
        // wrapper-side invalidation guarantee is for the bare form.)
        NativeCache cache = makeConnectedCache();
        cache.put("SELECT * FROM orders", null,
            java.util.Collections.singletonList(new Object[]{"stale"}), new String[]{"data"});
        assertNotNull(cache.get("SELECT * FROM orders", null));

        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        try (CallableStatement cs = wrapped.prepareCall("CALL my_proc()")) {
            cs.execute();
        }

        // CALL → DDL → invalidateAll. The pre-seeded entry must be gone.
        assertNull(cache.get("SELECT * FROM orders", null));
    }

    // --- Statement.addBatch(String) ---

    @Test
    void addBatchObservesSetCommand() throws Exception {
        // Pre-fix, addBatch buffered the SET on the real Statement without
        // ever calling observeSql, so the wrapper-side hash never moved
        // when a session-state change was queued via batch.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        GucState gucState = extractGucState(wrapped);
        long before = gucState.hash();

        try (Statement s = wrapped.createStatement()) {
            s.addBatch("SET app.user_id = '42'");
        }

        long after = gucState.hash();
        assertNotEquals(before, after,
            "Statement.addBatch(SET ...) must observe the SQL at addBatch time");
    }

    @Test
    void addBatchTwoSetsBothObserved() throws Exception {
        // Multiple batched SETs — the LAST one must determine the final
        // hash (mirrors how the server applies them in source order).
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        GucState gucState = extractGucState(wrapped);

        try (Statement s = wrapped.createStatement()) {
            s.addBatch("SET app.user_id = '42'");
        }
        long afterFirst = gucState.hash();

        try (Statement s = wrapped.createStatement()) {
            s.addBatch("SET app.user_id = '99'");
        }
        long afterSecond = gucState.hash();

        assertNotEquals(afterFirst, afterSecond,
            "second batched SET must shift the hash from the first");
    }

    @Test
    void addBatchSelectDoesNotMoveHash() throws Exception {
        // Sanity: addBatch on a plain SELECT mustn't touch the GucState
        // (observeSql ignores non-SET/RESET SQL).
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        GucState gucState = extractGucState(wrapped);
        long before = gucState.hash();

        try (Statement s = wrapped.createStatement()) {
            s.addBatch("SELECT 1");
        }

        assertEquals(before, gucState.hash(),
            "non-SET SQL queued via addBatch must not move the hash");
    }

    // --- helpers ---

    private static GucState extractGucState(Connection wrapped) throws Exception {
        // The proxy's InvocationHandler holds the per-connection GucState as
        // a package-private final field. Pull it out for assertions.
        java.lang.reflect.InvocationHandler ih = Proxy.getInvocationHandler(wrapped);
        java.lang.reflect.Field f = ih.getClass().getDeclaredField("gucState");
        f.setAccessible(true);
        return (GucState) f.get(ih);
    }

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
            ConnectionProxyCallableBatchTest.class.getClassLoader(),
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
                case "prepareCall":
                    return makeFakeCallableStatement(driver);
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
            ConnectionProxyCallableBatchTest.class.getClassLoader(),
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
                case "addBatch":
                case "close":
                    return null;
                default:
                    return defaultReturn(method.getReturnType());
            }
        }
    }

    private static CallableStatement makeFakeCallableStatement(FakeDriverState driver) {
        return (CallableStatement) Proxy.newProxyInstance(
            ConnectionProxyCallableBatchTest.class.getClassLoader(),
            new Class[]{CallableStatement.class},
            new FakeCallableStatementHandler(driver));
    }

    private static class FakeCallableStatementHandler implements InvocationHandler {
        final FakeDriverState driver;
        FakeCallableStatementHandler(FakeDriverState driver) { this.driver = driver; }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "execute":
                    return Boolean.FALSE;
                case "executeQuery":
                    return makeFakeResultSet(driver.scalar);
                case "executeUpdate":
                    return Integer.valueOf(0);
                case "addBatch":
                case "close":
                    return null;
                default:
                    return defaultReturn(method.getReturnType());
            }
        }
    }

    private static ResultSet makeFakeResultSet(String scalar) {
        return (ResultSet) Proxy.newProxyInstance(
            ConnectionProxyCallableBatchTest.class.getClassLoader(),
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
            ConnectionProxyCallableBatchTest.class.getClassLoader(),
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
