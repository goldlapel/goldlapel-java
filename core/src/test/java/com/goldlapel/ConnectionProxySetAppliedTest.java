package com.goldlapel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for "SET actually applied" — the wrapper must not mutate
 * the GUC state hash on optimistic wire-side observation alone. The state
 * hash only commits when the JDBC driver reports success; a SQLException
 * means the SET never reached the server (or the server rejected it), and
 * the wrapper must restore the pre-call snapshot so cache-key generation
 * doesn't drift from server-side state.
 *
 * <p>Wave 2 of the GUC-RLS hardening track. Filed as
 * {@code java-set-actually-applied-2026-05-05}; mirrors the proxy-side
 * commit-on-success contract in {@code src/guc_state.rs}.
 *
 * <p>Plumbing pattern matches {@link ConnectionProxyGucIntegrationTest} —
 * a hand-rolled JDBC fake driver that fails on demand.
 */
class ConnectionProxySetAppliedTest {

    @BeforeEach
    void setup() { NativeCache.reset(); }

    @AfterEach
    void cleanup() { NativeCache.reset(); }

    // --- Statement: success / failure paths ---

    @Test
    void successfulSetCommitsHash() throws Exception {
        // Baseline: a SET that succeeds must shift the hash exactly as
        // before this fix. Pins the "no regression in the happy path".
        Connection wrapped = makeWrapped(new FakeDriverState());
        GucState gucState = extractGucState(wrapped);

        try (Statement s = wrapped.createStatement()) {
            s.execute("SET app.user_id = '42'");
        }

        assertNotEquals(0L, gucState.hash(),
            "successful SET must commit to the state hash");
        assertFalse(gucState.isDirty(), "successful SET must not leave dirty");
    }

    @Test
    void failedSetExecuteRevertsHash() throws Exception {
        // Statement.execute() throws → snapshot must restore. The hash
        // must NOT show the SET as applied.
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = makeWrapped(driver);
        GucState gucState = extractGucState(wrapped);

        driver.failNextExecute = true;
        try (Statement s = wrapped.createStatement()) {
            assertThrows(SQLException.class, () -> s.execute("SET app.user_id = '42'"));
        }

        assertEquals(0L, gucState.hash(),
            "failed SET must NOT shift the hash — server never applied it");
        assertTrue(gucState.isDirty(),
            "failed SET must mark dirty so next checkout reconciles");
    }

    @Test
    void failedSetExecuteUpdateRevertsHash() throws Exception {
        // executeUpdate path — same revert semantics as execute.
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = makeWrapped(driver);
        GucState gucState = extractGucState(wrapped);

        driver.failNextExecute = true;
        try (Statement s = wrapped.createStatement()) {
            assertThrows(SQLException.class, () -> s.executeUpdate("SET app.user_id = '42'"));
        }

        assertEquals(0L, gucState.hash(),
            "failed executeUpdate(SET) must not shift the hash");
        assertTrue(gucState.isDirty());
    }

    @Test
    void failedSetExecuteQueryRevertsHash() throws Exception {
        // Some drivers route SET through executeQuery — pgjdbc tolerates it.
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = makeWrapped(driver);
        GucState gucState = extractGucState(wrapped);

        driver.failNextExecute = true;
        try (Statement s = wrapped.createStatement()) {
            assertThrows(SQLException.class, () -> s.executeQuery("SET app.user_id = '42'"));
        }

        assertEquals(0L, gucState.hash(),
            "failed executeQuery(SET) must not shift the hash");
        assertTrue(gucState.isDirty());
    }

    @Test
    void failedSecondSetPreservesFirstSuccess() throws Exception {
        // Layered semantics — a successful SET commits, a subsequent failed
        // SET must revert ONLY its own attempted mutation. The first SET's
        // value must remain in the state.
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = makeWrapped(driver);
        GucState gucState = extractGucState(wrapped);

        try (Statement s = wrapped.createStatement()) {
            s.execute("SET app.user_id = '42'");
        }
        long afterFirst = gucState.hash();
        assertNotEquals(0L, afterFirst);

        driver.failNextExecute = true;
        try (Statement s = wrapped.createStatement()) {
            assertThrows(SQLException.class,
                () -> s.execute("SET app.user_id = '99'"));
        }

        assertEquals(afterFirst, gucState.hash(),
            "failed second SET must revert to the first SET's hash");
        assertTrue(gucState.isDirty(),
            "failed second SET must still mark dirty (it might have partially applied)");
    }

    // --- Multi-statement bodies ---

    @Test
    void failedMultiStatementBodyRevertsAllSetsInBatch() throws Exception {
        // "SET a; SET b; UPDATE bad" — if the body throws, the wrapper must
        // not show ANY of the buffered SETs as applied. Server-side, pgjdbc
        // wraps multi-statement bodies in an implicit transaction that rolls
        // back on error; the wrapper-side hash must match.
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = makeWrapped(driver);
        GucState gucState = extractGucState(wrapped);

        driver.failNextExecute = true;
        try (Statement s = wrapped.createStatement()) {
            assertThrows(SQLException.class, () -> s.execute(
                "SET app.user_id = '42'; SET app.tenant = 'alpha'; SELECT 1"));
        }

        assertEquals(0L, gucState.hash(),
            "failed multi-statement body must revert ALL buffered SETs");
        assertTrue(gucState.isDirty(),
            "failed multi-statement body must mark dirty for next-call reconcile");
    }

    @Test
    void successfulMultiStatementBodyAppliesAllSets() throws Exception {
        // Sanity check — a successful multi-statement body still commits all
        // SETs at once. Pins that the snapshot pattern doesn't accidentally
        // drop the success-path observations.
        Connection wrapped = makeWrapped(new FakeDriverState());
        GucState gucState = extractGucState(wrapped);

        try (Statement s = wrapped.createStatement()) {
            s.execute("SET app.user_id = '42'; SET app.tenant = 'alpha'");
        }

        GucState reference = new GucState();
        reference.observeSql("SET app.user_id = '42'");
        reference.observeSql("SET app.tenant = 'alpha'");
        assertEquals(reference.hash(), gucState.hash(),
            "successful multi-statement must apply both SETs");
    }

    // --- PreparedStatement paths ---

    @Test
    void failedPreparedExecuteRevertsHash() throws Exception {
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = makeWrapped(driver);
        GucState gucState = extractGucState(wrapped);

        driver.failNextExecute = true;
        try (PreparedStatement ps = wrapped.prepareStatement("SET app.user_id = '42'")) {
            assertThrows(SQLException.class, ps::execute);
        }

        assertEquals(0L, gucState.hash());
        assertTrue(gucState.isDirty());
    }

    @Test
    void failedPreparedExecuteQueryRevertsHash() throws Exception {
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = makeWrapped(driver);
        GucState gucState = extractGucState(wrapped);

        driver.failNextExecute = true;
        try (PreparedStatement ps = wrapped.prepareStatement("SET app.user_id = '42'")) {
            assertThrows(SQLException.class, ps::executeQuery);
        }

        assertEquals(0L, gucState.hash());
        assertTrue(gucState.isDirty());
    }

    @Test
    void failedPreparedExecuteUpdateRevertsHash() throws Exception {
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = makeWrapped(driver);
        GucState gucState = extractGucState(wrapped);

        driver.failNextExecute = true;
        try (PreparedStatement ps = wrapped.prepareStatement("SET app.user_id = '42'")) {
            assertThrows(SQLException.class, ps::executeUpdate);
        }

        assertEquals(0L, gucState.hash());
        assertTrue(gucState.isDirty());
    }

    // --- CallableStatement path ---

    @Test
    void failedCallableExecuteRevertsHash() throws Exception {
        // CallableStatement reflects through method.invoke, so the catch
        // path has to unwrap InvocationTargetException — pin that this works.
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = makeWrapped(driver);
        GucState gucState = extractGucState(wrapped);

        driver.failNextExecute = true;
        try (CallableStatement cs = wrapped.prepareCall("SET app.user_id = '42'")) {
            assertThrows(SQLException.class, cs::execute);
        }

        assertEquals(0L, gucState.hash(),
            "failed CallableStatement.execute must revert the SET");
        assertTrue(gucState.isDirty());
    }

    // --- Connection.rollback() — PG reverts session SETs done in the txn ---

    @Test
    void connectionRollbackMarksDirty() throws Exception {
        // PG: SET inside a transaction that rolls back is reverted. The
        // wrapper can't know which SETs were inside the txn without snapshotting
        // at BEGIN; mark dirty instead so the next checkout reconciles via
        // pg_settings. Cheaper than per-txn snapshotting; correct for the
        // common case (SETs outside txn are rare-or-flushed at pool checkout).
        Connection wrapped = makeWrapped(new FakeDriverState());
        GucState gucState = extractGucState(wrapped);

        wrapped.setAutoCommit(false);
        try (Statement s = wrapped.createStatement()) {
            s.execute("SET app.user_id = '42'");
        }
        assertNotEquals(0L, gucState.hash(),
            "in-txn SET should still optimistically observe");
        assertFalse(gucState.isDirty());

        wrapped.rollback();
        assertTrue(gucState.isDirty(),
            "Connection.rollback() must mark dirty — server reverts session SETs done in txn");
    }

    @Test
    void connectionCommitDoesNotMarkDirty() throws Exception {
        // PG: COMMIT preserves SETs. Wrapper must not gratuitously mark dirty
        // (would force a pg_settings round-trip on the next read for no reason).
        Connection wrapped = makeWrapped(new FakeDriverState());
        GucState gucState = extractGucState(wrapped);

        wrapped.setAutoCommit(false);
        try (Statement s = wrapped.createStatement()) {
            s.execute("SET app.user_id = '42'");
        }
        wrapped.commit();

        assertFalse(gucState.isDirty(),
            "Connection.commit() must not mark dirty — server preserves SETs");
        assertNotEquals(0L, gucState.hash());
    }

    // --- executeBatch failure ---

    @Test
    void executeBatchFailureRevertsAllPendingSets() throws Exception {
        // Several SETs added to the batch; executeBatch throws → wrapper
        // observes none of them, marks dirty.
        FakeDriverState driver = new FakeDriverState();
        driver.failExecuteBatch = true;
        Connection wrapped = makeWrapped(driver);
        GucState gucState = extractGucState(wrapped);

        try (Statement s = wrapped.createStatement()) {
            s.addBatch("SET app.user_id = '42'");
            s.addBatch("SET app.tenant = 'alpha'");
            assertThrows(SQLException.class, s::executeBatch);
        }

        assertEquals(0L, gucState.hash(),
            "failed executeBatch must drop all pending observations");
        assertTrue(gucState.isDirty());
    }

    @Test
    void executeBatchSuccessAppliesPendingSetsInOrder() throws Exception {
        // Mirror of the failure case: success applies all pending observations
        // in source order. Already covered in ConnectionProxyCallableBatchTest;
        // duplicated here for the SET-applied test surface to be self-contained.
        Connection wrapped = makeWrapped(new FakeDriverState());
        GucState gucState = extractGucState(wrapped);

        try (Statement s = wrapped.createStatement()) {
            s.addBatch("SET app.user_id = '42'");
            s.addBatch("SET app.tenant = 'alpha'");
            s.executeBatch();
        }

        GucState reference = new GucState();
        reference.observeSql("SET app.user_id = '42'");
        reference.observeSql("SET app.tenant = 'alpha'");
        assertEquals(reference.hash(), gucState.hash());
    }

    // --- DISCARD ALL revert path ---

    @Test
    void failedDiscardAllRevertsHash() throws Exception {
        // DISCARD ALL drops all unsafe state; if the call throws, the prior
        // state must come back. (HikariCP's connectionInitSql wires DISCARD
        // ALL on every checkout — a failed checkout can't be allowed to wipe
        // the wrapper-side state.)
        FakeDriverState driver = new FakeDriverState();
        Connection wrapped = makeWrapped(driver);
        GucState gucState = extractGucState(wrapped);

        try (Statement s = wrapped.createStatement()) {
            s.execute("SET app.user_id = '42'");
        }
        long preDiscard = gucState.hash();
        assertNotEquals(0L, preDiscard);

        driver.failNextExecute = true;
        try (Statement s = wrapped.createStatement()) {
            assertThrows(SQLException.class, () -> s.execute("DISCARD ALL"));
        }

        assertEquals(preDiscard, gucState.hash(),
            "failed DISCARD ALL must restore the prior state — client never saw the wipe");
        assertTrue(gucState.isDirty());
    }

    // --- Snapshot mechanism direct test ---

    @Test
    void snapshotRestoreRoundTrip() throws Exception {
        // Direct test of the GucState.Snapshot / restoreOrReset contract.
        GucState s = new GucState();
        s.observeSql("SET app.user_id = '42'");
        long preSnapshotHash = s.hash();
        GucState.Snapshot snap = s.snapshot();

        s.observeSql("SET app.user_id = '99'");
        assertNotEquals(preSnapshotHash, s.hash(), "mutation should change hash");

        s.restoreOrReset(snap);
        assertEquals(preSnapshotHash, s.hash(),
            "restore must return state to the snapshot's hash");

        // Idempotent: restoring twice from the same snapshot is a no-op.
        s.restoreOrReset(snap);
        assertEquals(preSnapshotHash, s.hash());
    }

    @Test
    void snapshotOnEmptyStateReturnsNull() throws Exception {
        // Snapshot of empty state allocates nothing; restoreOrReset(null)
        // resets to empty.
        GucState s = new GucState();
        assertNull(s.snapshot(), "empty-state snapshot must be null (no allocation)");

        s.observeSql("SET app.user_id = '42'");
        s.restoreOrReset(null);
        assertEquals(0L, s.hash(),
            "restoreOrReset(null) must wipe to baseline");
    }

    // --- helpers ---

    private static Connection makeWrapped(FakeDriverState driver) throws Exception {
        NativeCache cache = NativeCache.getInstance();
        java.lang.reflect.Field connected =
            NativeCache.class.getDeclaredField("invalidationConnected");
        connected.setAccessible(true);
        connected.setBoolean(cache, true);
        return ConnectionProxy.wrap(makeFakeConnection(driver), cache);
    }

    private static GucState extractGucState(Connection wrapped) throws Exception {
        InvocationHandler ih = Proxy.getInvocationHandler(wrapped);
        java.lang.reflect.Field f = ih.getClass().getDeclaredField("gucState");
        f.setAccessible(true);
        return (GucState) f.get(ih);
    }

    private static class FakeDriverState {
        String scalar = "default";
        volatile boolean failNextExecute = false;
        volatile boolean failExecuteBatch = false;
    }

    private static Connection makeFakeConnection(FakeDriverState driver) {
        return (Connection) Proxy.newProxyInstance(
            ConnectionProxySetAppliedTest.class.getClassLoader(),
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
                    return makeFakePreparedStatement(driver);
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
            ConnectionProxySetAppliedTest.class.getClassLoader(),
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
                        throw new SQLException("simulated executeQuery failure");
                    }
                    return makeFakeResultSet(driver.scalar);
                case "execute":
                case "executeUpdate":
                    if (driver.failNextExecute) {
                        driver.failNextExecute = false;
                        throw new SQLException("simulated execute failure");
                    }
                    return method.getReturnType() == int.class ? Integer.valueOf(0) : Boolean.FALSE;
                case "executeBatch":
                    if (driver.failExecuteBatch) {
                        throw new SQLException("simulated executeBatch failure");
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

    private static PreparedStatement makeFakePreparedStatement(FakeDriverState driver) {
        return (PreparedStatement) Proxy.newProxyInstance(
            ConnectionProxySetAppliedTest.class.getClassLoader(),
            new Class[]{PreparedStatement.class},
            new FakePreparedStatementHandler(driver));
    }

    private static class FakePreparedStatementHandler implements InvocationHandler {
        final FakeDriverState driver;
        FakePreparedStatementHandler(FakeDriverState driver) { this.driver = driver; }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "executeQuery":
                    if (driver.failNextExecute) {
                        driver.failNextExecute = false;
                        throw new SQLException("simulated executeQuery failure");
                    }
                    return makeFakeResultSet(driver.scalar);
                case "execute":
                    if (driver.failNextExecute) {
                        driver.failNextExecute = false;
                        throw new SQLException("simulated execute failure");
                    }
                    return Boolean.FALSE;
                case "executeUpdate":
                    if (driver.failNextExecute) {
                        driver.failNextExecute = false;
                        throw new SQLException("simulated executeUpdate failure");
                    }
                    return Integer.valueOf(0);
                case "executeBatch":
                    if (driver.failExecuteBatch) {
                        throw new SQLException("simulated executeBatch failure");
                    }
                    return new int[0];
                case "addBatch":
                case "clearBatch":
                case "clearParameters":
                case "close":
                    return null;
                default:
                    return defaultReturn(method.getReturnType());
            }
        }
    }

    private static CallableStatement makeFakeCallableStatement(FakeDriverState driver) {
        return (CallableStatement) Proxy.newProxyInstance(
            ConnectionProxySetAppliedTest.class.getClassLoader(),
            new Class[]{CallableStatement.class},
            new FakeCallableStatementHandler(driver));
    }

    private static class FakeCallableStatementHandler implements InvocationHandler {
        final FakeDriverState driver;
        FakeCallableStatementHandler(FakeDriverState driver) { this.driver = driver; }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "execute":
                    if (driver.failNextExecute) {
                        driver.failNextExecute = false;
                        throw new SQLException("simulated execute failure");
                    }
                    return Boolean.FALSE;
                case "executeQuery":
                    if (driver.failNextExecute) {
                        driver.failNextExecute = false;
                        throw new SQLException("simulated executeQuery failure");
                    }
                    return makeFakeResultSet(driver.scalar);
                case "executeUpdate":
                    if (driver.failNextExecute) {
                        driver.failNextExecute = false;
                        throw new SQLException("simulated executeUpdate failure");
                    }
                    return Integer.valueOf(0);
                case "executeBatch":
                    if (driver.failExecuteBatch) {
                        throw new SQLException("simulated executeBatch failure");
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

    private static ResultSet makeFakeResultSet(String scalar) {
        return (ResultSet) Proxy.newProxyInstance(
            ConnectionProxySetAppliedTest.class.getClassLoader(),
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
                    if (!nextCalled) { nextCalled = true; return Boolean.TRUE; }
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
            ConnectionProxySetAppliedTest.class.getClassLoader(),
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
