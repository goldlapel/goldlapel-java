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
    void addBatchDefersUntilExecuteBatch() throws Exception {
        // SET-applied semantics (java-set-actually-applied, 2026-05-05): the
        // hash MUST NOT shift on addBatch — the SET hasn't reached the server
        // yet. Observation happens on executeBatch success (the SET could
        // fail server-side, in which case wire-side observation would diverge
        // from server state).
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        GucState gucState = extractGucState(wrapped);
        long before = gucState.hash();

        try (Statement s = wrapped.createStatement()) {
            s.addBatch("SET app.user_id = '42'");
            // Pre-executeBatch: hash unchanged.
            assertEquals(before, gucState.hash(),
                "addBatch must NOT mutate the state hash — SET hasn't been sent yet");
            s.executeBatch();
            // Post-success: hash now reflects the batched SET.
            assertNotEquals(before, gucState.hash(),
                "executeBatch success must apply each pending SET observation");
        }
    }

    @Test
    void addBatchTwoSetsBothObservedOnExecuteBatch() throws Exception {
        // Multiple batched SETs — the LAST one must determine the final
        // hash (mirrors how the server applies them in source order). Both
        // observations land at executeBatch time, in source order.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        GucState gucState = extractGucState(wrapped);

        try (Statement s = wrapped.createStatement()) {
            s.addBatch("SET app.user_id = '42'");
            s.addBatch("SET app.user_id = '99'");
            assertEquals(0L, gucState.hash(),
                "neither addBatch observed yet — hash still baseline");
            s.executeBatch();
        }

        // Both SETs observed on executeBatch; the last one wins.
        GucState reference = new GucState();
        reference.observeSql("SET app.user_id = '42'");
        reference.observeSql("SET app.user_id = '99'");
        assertEquals(reference.hash(), gucState.hash(),
            "post-executeBatch hash must match source-order replay");
    }

    @Test
    void executeBatchFailureLeavesStateAtBaseline() throws Exception {
        // executeBatch throws → drop the pending observations and mark dirty
        // so the next checkout reconciles via pg_settings. The wrapper-side
        // hash MUST NOT show the batched SET as applied — the server may
        // not have applied it.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        driver.failExecuteBatch = true;
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        GucState gucState = extractGucState(wrapped);

        try (Statement s = wrapped.createStatement()) {
            s.addBatch("SET app.user_id = '42'");
            assertThrows(java.sql.SQLException.class, s::executeBatch);
        }

        assertEquals(0L, gucState.hash(),
            "failed executeBatch must leave hash at baseline — SET never applied");
        assertTrue(gucState.isDirty(),
            "failed executeBatch must mark dirty so next checkout reconciles");
    }

    @Test
    void clearBatchDropsPendingObservations() throws Exception {
        // clearBatch removes the buffered statements; subsequent executeBatch
        // must NOT observe the cleared entries.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        GucState gucState = extractGucState(wrapped);

        try (Statement s = wrapped.createStatement()) {
            s.addBatch("SET app.user_id = '42'");
            s.clearBatch();
            s.executeBatch();
        }

        assertEquals(0L, gucState.hash(),
            "clearBatch must drop pending observations — hash stays baseline");
    }

    @Test
    void addBatchSelectDoesNotMoveHash() throws Exception {
        // Sanity: a SELECT queued via addBatch (then executeBatch'd) mustn't
        // touch the GucState — observeSql ignores non-SET/RESET SQL.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        GucState gucState = extractGucState(wrapped);
        long before = gucState.hash();

        try (Statement s = wrapped.createStatement()) {
            s.addBatch("SELECT 1");
            s.executeBatch();
        }

        assertEquals(before, gucState.hash(),
            "non-SET SQL queued via addBatch + executeBatch must not move the hash");
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
        volatile boolean failExecuteBatch = false;
        volatile boolean failNextExecute = false;
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
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "executeQuery":
                    if (driver.failNextExecute) {
                        driver.failNextExecute = false;
                        throw new java.sql.SQLException("simulated executeQuery failure");
                    }
                    return makeFakeResultSet(driver.scalar);
                case "execute":
                case "executeUpdate":
                    if (driver.failNextExecute) {
                        driver.failNextExecute = false;
                        throw new java.sql.SQLException("simulated execute failure");
                    }
                    return method.getReturnType() == int.class ? Integer.valueOf(0) : Boolean.FALSE;
                case "executeBatch":
                    if (driver.failExecuteBatch) {
                        throw new java.sql.SQLException("simulated executeBatch failure");
                    }
                    return new int[0];
                case "addBatch":
                case "clearBatch":
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
