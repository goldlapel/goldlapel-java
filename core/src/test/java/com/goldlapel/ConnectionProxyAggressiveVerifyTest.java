package com.goldlapel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behaviour of the always-on post-DML sequence-bump path (Wave 2,
 * 2026-05-06): given a wrapped connection in each of the three modes
 * (AUTO / ON / OFF), confirm that a post-INSERT/UPDATE/DELETE invocation
 * either does or doesn't bump {@link GucState#dmlSeq()}.
 *
 * <p>Replaces the earlier smart-auto-enable test (which probed
 * {@code pg_trigger}). The new design is bump-on-every-write under AUTO/ON,
 * with OFF as the audited-schema opt-out + a one-time warning.
 *
 * <p>Uses the same fake-driver scaffolding pattern as
 * {@link ConnectionProxyVerifyTest} so we don't need a live Postgres.
 */
class ConnectionProxyAggressiveVerifyTest {

    @BeforeEach
    void setup() {
        NativeCache.reset();
        ConnectionProxy.resetOptOutWarningForTesting();
    }

    @AfterEach
    void cleanup() {
        NativeCache.reset();
        ConnectionProxy.resetOptOutWarningForTesting();
    }

    @Test
    void onModeBumpsDmlSeqAfterInsert() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();

        Connection wrapped = ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.ON, null);

        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);
        long preHash = handler.gucState.hash();
        long preSeq = handler.gucState.dmlSeq();

        try (Statement s = wrapped.createStatement()) {
            s.executeUpdate("INSERT INTO orders (user_id, total) VALUES (1, 100)");
        }

        assertEquals(preSeq + 1, handler.gucState.dmlSeq(),
            "ON mode + INSERT must bump dml_seq exactly once");
        assertNotEquals(preHash, handler.gucState.hash(),
            "dml_seq bump must roll the state hash forward");
        // No verify ran — the new design doesn't schedule a pg_settings read
        // on every DML; it just rolls the cache key forward.
        assertEquals(0, driver.verifyCount.get(),
            "post-DML must not trigger a pg_settings verify (sequence-bump only)");
    }

    @Test
    void onModeBumpsDmlSeqAfterUpdateDeleteTruncateMerge() throws Exception {
        for (String sql : new String[] {
            "UPDATE orders SET total = 200 WHERE id = 1",
            "DELETE FROM orders WHERE id = 1",
            "TRUNCATE TABLE orders",
            "MERGE INTO orders USING src ON orders.id = src.id WHEN MATCHED THEN UPDATE SET total = src.total"
        }) {
            NativeCache cache = makeConnectedCache();
            FakeDriverState driver = new FakeDriverState();
            Connection wrapped = ConnectionProxy.wrap(
                makeFakeConnection(driver), cache, AggressiveVerifyMode.ON, null);
            ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);

            try (Statement s = wrapped.createStatement()) {
                s.executeUpdate(sql);
            }

            assertEquals(1L, handler.gucState.dmlSeq(),
                "ON mode + DML must bump dml_seq (sql: " + sql + ")");
        }
    }

    @Test
    void autoModeBumpsDmlSeqAfterInsert() throws Exception {
        // AUTO is now a synonym for ON — no probe, no per-URL cache, always
        // bump on every observed write.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();

        Connection wrapped = ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.AUTO, null);
        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);

        try (Statement s = wrapped.createStatement()) {
            s.executeUpdate("INSERT INTO orders (id) VALUES (1)");
        }

        assertEquals(1L, handler.gucState.dmlSeq(),
            "AUTO mode behaves like ON — always-on bump on every write");
    }

    @Test
    void autoModeDoesNotProbePgTrigger() throws Exception {
        // The smart-auto-enable design probed pg_trigger on first wrap per
        // JDBC URL — Wave 2 removes that. AUTO must never query pg_trigger.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();

        ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.AUTO,
            "jdbc:test:no-probe-please");

        assertEquals(0, driver.probeCount.get(),
            "AUTO must not probe pg_trigger — Wave 2 is always-on bump");
    }

    @Test
    void offModeDoesNotBumpDmlSeqAfterInsert() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();

        Connection wrapped = ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.OFF, null);
        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);

        try (Statement s = wrapped.createStatement()) {
            s.executeUpdate("INSERT INTO orders (user_id) VALUES (1)");
        }

        assertEquals(0L, handler.gucState.dmlSeq(),
            "OFF mode must not bump dml_seq after a write");
        assertEquals(0L, handler.gucState.hash(),
            "OFF + no SETs + no DML bump → baseline hash preserved");
    }

    @Test
    void offModeLogsOneTimeWarning() {
        // OFF mode must surface the explicit opt-out in stderr so the
        // operator's audit trail captures the trigger-internal-SET risk
        // they took on by disabling the safety bump. Warning fires exactly
        // once per JVM regardless of how many connections are wrapped.
        PrintStream originalErr = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try {
            System.setErr(new PrintStream(captured));

            // Two OFF-mode wraps in a row.
            NativeCache cache = makeConnectedCache();
            ConnectionProxy.wrap(
                makeFakeConnection(new FakeDriverState()), cache,
                AggressiveVerifyMode.OFF, null);
            ConnectionProxy.wrap(
                makeFakeConnection(new FakeDriverState()), cache,
                AggressiveVerifyMode.OFF, null);

            String stderr = captured.toString();
            assertTrue(stderr.contains("AggressiveVerifyMode=OFF"),
                "OFF wrap must log the opt-out warning to stderr; got: " + stderr);
            assertTrue(stderr.contains("trigger-internal SET")
                    || stderr.contains("Trigger-internal SET"),
                "warning must mention the trigger-internal-SET risk; got: " + stderr);

            int firstWarnEnd = stderr.indexOf('\n', stderr.indexOf("AggressiveVerifyMode=OFF"));
            int secondOccurrence = stderr.indexOf("AggressiveVerifyMode=OFF", firstWarnEnd + 1);
            assertEquals(-1, secondOccurrence,
                "warning must emit exactly once per JVM, even for repeat OFF wraps");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            System.setErr(originalErr);
        }
    }

    @Test
    void offModeStillSchedulesVerifyAfterFunctionCall() throws Exception {
        // OFF only suppresses the post-DML bump. The Wave 1 post-function-
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
    void sequentialBumpsYieldDistinctHashes() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.AUTO, null);
        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);

        long[] hashes = new long[4];
        hashes[0] = handler.gucState.hash();
        try (Statement s = wrapped.createStatement()) {
            s.executeUpdate("INSERT INTO orders (id) VALUES (1)");
            hashes[1] = handler.gucState.hash();
            s.executeUpdate("INSERT INTO orders (id) VALUES (2)");
            hashes[2] = handler.gucState.hash();
            s.executeUpdate("INSERT INTO orders (id) VALUES (3)");
            hashes[3] = handler.gucState.hash();
        }

        // Every consecutive pair must differ — three writes, three distinct
        // post-DML cache slots.
        for (int i = 0; i < hashes.length; i++) {
            for (int j = i + 1; j < hashes.length; j++) {
                assertNotEquals(hashes[i], hashes[j],
                    "hashes[" + i + "] and hashes[" + j + "] must differ");
            }
        }
    }

    @Test
    void cacheMissAfterDmlBump() throws Exception {
        // The whole point of the bump: a row cached pre-DML must NOT be
        // served after a DML, even if the same SELECT statement runs at the
        // same param values. Subsequent reads land on a fresh slot.
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        driver.selectScalar = "pre-dml";
        Connection wrapped = ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.AUTO, null);

        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT id FROM accounts WHERE id = 7");
            assertTrue(rs.next());
            assertEquals("pre-dml", rs.getObject(1));
        }

        // DML — bumps dml_seq, rolls the cache key.
        try (Statement s = wrapped.createStatement()) {
            s.executeUpdate("INSERT INTO orders (id) VALUES (1)");
        }

        // The fake driver now reports post-dml for SELECTs. If the cache
        // were keyed on (sql, params) alone, we'd see pre-dml from cache.
        // With the dml_seq bump, the post-DML key misses and the fresh
        // result comes through.
        driver.selectScalar = "post-dml";
        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT id FROM accounts WHERE id = 7");
            assertTrue(rs.next());
            assertEquals("post-dml", rs.getObject(1),
                "post-DML SELECT must miss the cache and fetch fresh");
        }
    }

    @Test
    void dirtyBypassRoutesCurrentAndSubsequentReadsToProxy() throws Exception {
        // When verify-on-checkout fails, the dirty flag stays set — both
        // the current query AND subsequent reads must bypass the L1 cache
        // until something clears the flag. We assert that by failing the
        // verify on the first checkout, observing that the current read
        // routes through the driver, and confirming a second read on the
        // same SQL also routes through the driver (not served from the
        // cache slot we'd otherwise have populated).
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();
        driver.failVerify = true;
        driver.selectScalar = "fresh";

        Connection wrapped = ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.AUTO, null);
        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);
        handler.gucState.markDirty();

        // First read — verify fails (failVerify=true), dirty stays set, the
        // cache lookup is bypassed, the actual query lands at the driver.
        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT id FROM accounts WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("fresh", rs.getObject(1));
        }
        assertTrue(handler.gucState.isDirty(),
            "failed verify must leave dirty set");

        // Driver bumps selectCount on every real SELECT. After the first
        // query, that's 1. A second identical SELECT under a non-dirty
        // wrapper would hit the cache (count stays 1); under the dirty
        // bypass, it must route to the driver again (count → 2).
        int countAfterFirst = driver.selectCount.get();

        try (Statement s = wrapped.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT id FROM accounts WHERE id = 1");
            assertTrue(rs.next());
        }

        assertTrue(driver.selectCount.get() > countAfterFirst,
            "dirty bypass must route subsequent reads to the proxy too "
            + "(first count=" + countAfterFirst + ", after=" + driver.selectCount.get() + ")");
    }

    @Test
    void preparedStatementUpdateAlsoBumpsDmlSeqInOnMode() throws Exception {
        NativeCache cache = makeConnectedCache();
        FakeDriverState driver = new FakeDriverState();

        Connection wrapped = ConnectionProxy.wrap(
            makeFakeConnection(driver), cache, AggressiveVerifyMode.ON, null);
        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);

        try (PreparedStatement ps = wrapped.prepareStatement("UPDATE orders SET total = ? WHERE id = ?")) {
            ps.setInt(1, 200);
            ps.setInt(2, 1);
            ps.executeUpdate();
        }

        assertEquals(1L, handler.gucState.dmlSeq(),
            "PreparedStatement.executeUpdate must also bump dml_seq in ON mode");
    }

    @Test
    void onModeSelectDoesNotBumpDmlSeq() throws Exception {
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

        ConnectionProxy.ConnectionHandler handler = handlerFor(wrapped);
        assertEquals(0L, handler.gucState.dmlSeq(),
            "plain SELECT must not bump dml_seq");
        assertFalse(handler.gucState.isDirty(),
            "ON mode + plain SELECT (no function call, no write) must NOT mark dirty");
    }

    // --- fake driver scaffolding ---

    private static NativeCache makeConnectedCache() {
        try {
            NativeCache cache = NativeCache.getInstance();
            java.lang.reflect.Field connected = NativeCache.class.getDeclaredField("invalidationConnected");
            connected.setAccessible(true);
            connected.setBoolean(cache, true);
            return cache;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ConnectionProxy.ConnectionHandler handlerFor(Connection wrapped) {
        return (ConnectionProxy.ConnectionHandler) Proxy.getInvocationHandler(wrapped);
    }

    private static class FakeDriverState {
        final Map<String, String> pgSettings = new LinkedHashMap<>();
        final AtomicInteger verifyCount = new AtomicInteger();
        final AtomicInteger probeCount = new AtomicInteger();
        final AtomicInteger selectCount = new AtomicInteger();
        volatile boolean failVerify = false;
        volatile String selectScalar = "data";
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
                            if (driver.failVerify) {
                                throw new java.sql.SQLException("simulated verify failure");
                            }
                            driver.verifyCount.incrementAndGet();
                            return makePgSettingsResultSet(driver.pgSettings);
                        }
                        driver.selectCount.incrementAndGet();
                        return makeSingleScalarResultSet(driver.selectScalar);
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
                        if (args == null || args.length == 0) {
                            if (sql.contains("pg_trigger")) {
                                driver.probeCount.incrementAndGet();
                                return makeSingleScalarResultSet(driver.selectScalar);
                            }
                            driver.selectCount.incrementAndGet();
                            return makeSingleScalarResultSet(driver.selectScalar);
                        }
                        return makeSingleScalarResultSet(driver.selectScalar);
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
