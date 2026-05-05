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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for multi-statement-aware write detection
 * ({@code wrapper-multistatement-write-detection.md}, filed 2026-05-04).
 * A write buried after a SET / BEGIN in a multi-statement Q body must still
 * drive an invalidation — the single-token detectWrite would only see the
 * first token and miss the trailing INSERT.
 *
 * <p>Plumbing mirrors {@link ConnectionProxyGucIntegrationTest} — a
 * {@link Proxy}-backed fake driver that takes no real-Postgres dependency.
 */
class ConnectionProxyMultiStatementTest {

    @BeforeEach
    void setup() { NativeCache.reset(); }

    @AfterEach
    void cleanup() { NativeCache.reset(); }

    @Test
    void multiStatementSetThenInsertInvalidatesTable() throws Exception {
        NativeCache cache = makeConnectedCache();
        // Pre-seed a stale `orders` cache entry as if a previous query cached it.
        cache.put("SELECT * FROM orders", null,
            Collections.singletonList(new Object[]{"stale"}), new String[]{"data"});
        assertNotNull(cache.get("SELECT * FROM orders", null),
            "pre-seeded entry must be visible before the multi-statement write");

        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        // The bug: a Q body of "SET ... ; INSERT INTO orders ..." used to slip
        // past write detection because only the first token (SET) was
        // inspected. With the multi-statement fix, the INSERT segment must
        // drive an invalidation of the `orders` table.
        try (Statement s = wrapped.createStatement()) {
            s.execute("SET app.user_id = '42'; INSERT INTO orders VALUES (1)");
        }

        assertNull(cache.get("SELECT * FROM orders", null),
            "INSERT buried after a SET in a multi-statement body must invalidate `orders`");
    }

    @Test
    void multiStatementDdlInvalidatesAll() throws Exception {
        NativeCache cache = makeConnectedCache();
        cache.put("SELECT * FROM orders", null,
            Collections.singletonList(new Object[]{"o"}), new String[]{"data"});
        cache.put("SELECT * FROM users", null,
            Collections.singletonList(new Object[]{"u"}), new String[]{"data"});

        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        try (Statement s = wrapped.createStatement()) {
            s.execute("INSERT INTO orders VALUES (1); DROP TABLE foo");
        }

        // DDL anywhere in the body must collapse to a full invalidation.
        assertNull(cache.get("SELECT * FROM orders", null));
        assertNull(cache.get("SELECT * FROM users", null));
    }

    @Test
    void multiStatementWriteOnExecuteQueryPath() throws Exception {
        // executeQuery is the path the driver uses when the caller expected
        // rows — but a sloppy caller may still send a multi-statement body
        // through it. The write-detection guard now lives before the cache
        // path so even the rows-expected route invalidates correctly.
        NativeCache cache = makeConnectedCache();
        cache.put("SELECT * FROM orders", null,
            Collections.singletonList(new Object[]{"stale"}), new String[]{"data"});

        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        try (Statement s = wrapped.createStatement()) {
            s.executeQuery("SET app.tenant = 'x'; UPDATE orders SET name = 'y'");
        }

        assertNull(cache.get("SELECT * FROM orders", null));
    }

    @Test
    void beginInsertCommitInvalidates() throws Exception {
        // The "BEGIN; INSERT; COMMIT" multi-statement transactional body —
        // before the fix, isTxStart matched first and the INSERT was lost
        // (return-early skipped write detection). After the fix, the flag
        // updates AND the write-detection pass runs.
        NativeCache cache = makeConnectedCache();
        cache.put("SELECT * FROM orders", null,
            Collections.singletonList(new Object[]{"stale"}), new String[]{"data"});

        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        try (Statement s = wrapped.createStatement()) {
            s.executeQuery("BEGIN; INSERT INTO orders VALUES (1); COMMIT");
        }

        assertNull(cache.get("SELECT * FROM orders", null));
    }

    @Test
    void beginInsertCommitLeavesWrapperOutOfTx() throws Exception {
        // Tx-flag bookkeeping regression (java tx-flag bookkeeping fix,
        // 2026-05-04). Pre-fix: isTxStart matched first on
        // "BEGIN; INSERT; COMMIT" and pinned the wrapper into in-tx mode
        // forever — every subsequent read bypassed the cache. After the
        // fix, updateTxState walks every segment, the trailing COMMIT
        // wins, and the wrapper goes back to letting the cache serve reads.
        NativeCache cache = makeConnectedCache();

        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(makeFakeConnection(driver), cache);

        try (Statement s = wrapped.createStatement()) {
            // Multi-statement BEGIN-COMMIT body — should net out to "not in tx".
            s.executeQuery("BEGIN; SELECT 1; COMMIT");

            // Prime the cache with a read that would only be cached if the
            // wrapper believes it's NOT in a transaction.
            s.executeQuery("SELECT * FROM users");
        }

        // If the wrapper still believed it was in-tx, the SELECT * FROM users
        // path would have skipped the cache.put — assert the cache entry
        // landed.
        assertNotNull(cache.get("SELECT * FROM users", null),
            "wrapper must net out of tx after BEGIN; ...; COMMIT in one body");
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
            ConnectionProxyMultiStatementTest.class.getClassLoader(),
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
            ConnectionProxyMultiStatementTest.class.getClassLoader(),
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
            ConnectionProxyMultiStatementTest.class.getClassLoader(),
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
            ConnectionProxyMultiStatementTest.class.getClassLoader(),
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
