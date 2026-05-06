package com.goldlapel;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class ConnectionProxy {

    /**
     * Shared executor for post-call GUC-state verifies. One pool across all
     * wrapped connections — verifies are infrequent (fire only when the
     * wrapper observes a top-level {@code SELECT <function>(...)} or
     * {@code CALL <proc>(...)}), and the work serializes per-connection on
     * the connection lock anyway. A small fixed pool keeps thread overhead
     * predictable; queueing on saturation is fine because the user's hot path
     * never blocks on the verify.
     *
     * <p>Daemon threads so the pool never holds JVM shutdown.
     */
    private static final ExecutorService VERIFY_EXECUTOR = Executors.newFixedThreadPool(
        2,
        new ThreadFactory() {
            private final AtomicLong n = new AtomicLong();
            @Override public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "goldlapel-guc-verify-" + n.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        }
    );

    /** Visible for testing — drain pending verifies between tests. */
    static java.util.concurrent.Future<?> submitVerify(Runnable r) {
        return VERIFY_EXECUTOR.submit(r);
    }

    public static Connection wrap(Connection real, NativeCache cache) {
        return (Connection) Proxy.newProxyInstance(
            ConnectionProxy.class.getClassLoader(),
            new Class[]{Connection.class},
            new ConnectionHandler(real, cache)
        );
    }

    static class ConnectionHandler implements InvocationHandler {
        final Connection real;
        private final NativeCache cache;
        // Per-connection unsafe-GUC state. SET / RESET observed on every query
        // mutates this; the hash is folded into the native-cache key so two
        // connections that have set different unsafe GUCs never share a slot
        // (custom-GUC-driven RLS would otherwise leak across users). Mirrors
        // the proxy-side ConnectionGucState in src/guc_state.rs.
        final GucState gucState = new GucState();
        boolean inTransaction = false;

        /**
         * Per-connection lock that serializes user-facing JDBC calls with the
         * post-call verify executor. JDBC connections aren't safe for
         * concurrent thread use (per spec), so the wrapper's threading
         * contract still holds: a single user thread per connection. The lock
         * exists strictly to interlock <i>our own</i> async verify task with
         * the user's serial calls — a verify scheduled after a
         * {@code SELECT my_func()} runs only when the user is between
         * statements.
         *
         * <p>Held by every {@code invoke()} on this handler and by every
         * Statement / PreparedStatement / CallableStatement wrapper's
         * {@code invoke()}, plus by the verify executor's task. Contention is
         * vanishingly rare on real apps (one user thread, one verify thread,
         * verify only fires when the user's call has already returned).
         */
        final Object connectionLock = new Object();

        ConnectionHandler(Connection real, NativeCache cache) {
            this.real = real;
            this.cache = cache;
        }

        /**
         * Run a verify on this handler's underlying connection through the
         * shared executor, serialised on {@link #connectionLock}. Safe to
         * call from any thread (user-facing JDBC call sites do this after
         * observing a function-call statement); the actual verify executes
         * when the user's call has released the lock. If a verify is already
         * queued and {@link GucState#isDirty} flips back to true before the
         * worker runs, the worker will pick up the latest state on its next
         * run — verifies are idempotent.
         */
        void scheduleVerify() {
            VERIFY_EXECUTOR.submit(() -> {
                synchronized (connectionLock) {
                    try {
                        gucState.verify(real);
                    } catch (Throwable t) {
                        gucState.markDirty();
                    }
                }
            });
        }

        /**
         * Lazy verify-on-checkout. Called from the user-facing invoke paths
         * <i>before</i> consulting the cache; runs synchronously under the
         * connection lock so the next cache key uses up-to-date state.
         * Failures leave the dirty flag set — the next call will retry.
         */
        void verifyIfDirty() {
            if (gucState.isDirty()) {
                try {
                    gucState.verify(real);
                } catch (Throwable t) {
                    gucState.markDirty();
                }
            }
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            // Hold the connection lock for the entire call so post-call
            // verify tasks (which take the same lock) never interleave with
            // a user-facing JDBC call — JDBC connections aren't thread-safe.
            synchronized (connectionLock) {
                switch (method.getName()) {
                    case "createStatement":
                        Statement stmt = (Statement) method.invoke(real, args);
                        return wrapStatement(stmt);
                    case "prepareStatement":
                        String sql = (String) args[0];
                        PreparedStatement ps = (PreparedStatement) method.invoke(real, args);
                        return wrapPreparedStatement(ps, sql);
                    case "prepareCall":
                        // CallableStatement (e.g. `{call my_proc(?)}` or
                        // `SET app.user_id = '42'` issued via prepareCall) was
                        // bypassing GucState.observeSql — the wrapper-side L1
                        // state hash never shifted on SET commands routed through
                        // this path, allowing stale entries to be served against
                        // an updated session state. Wrap it so each execute*
                        // observes the SQL before delegating, matching the
                        // PreparedStatement / Statement paths.
                        // (java-jdbc-callable-batch-statehash-gap.md, 2026-05-04)
                        String callSql = (String) args[0];
                        CallableStatement cs = (CallableStatement) method.invoke(real, args);
                        return wrapCallableStatement(cs, callSql);
                    case "setAutoCommit":
                        boolean autoCommit = (boolean) args[0];
                        inTransaction = !autoCommit;
                        return method.invoke(real, args);
                    case "commit":
                        inTransaction = false;
                        return method.invoke(real, args);
                    case "rollback":
                        inTransaction = false;
                        return method.invoke(real, args);
                    default:
                        return method.invoke(real, args);
                }
            }
        }

        private Statement wrapStatement(Statement real) {
            return (Statement) Proxy.newProxyInstance(
                ConnectionProxy.class.getClassLoader(),
                new Class[]{Statement.class},
                new StatementHandler(real, cache, this)
            );
        }

        private PreparedStatement wrapPreparedStatement(PreparedStatement real, String sql) {
            return (PreparedStatement) Proxy.newProxyInstance(
                ConnectionProxy.class.getClassLoader(),
                new Class[]{PreparedStatement.class},
                new PreparedStatementHandler(real, sql, cache, this)
            );
        }

        private CallableStatement wrapCallableStatement(CallableStatement real, String sql) {
            return (CallableStatement) Proxy.newProxyInstance(
                ConnectionProxy.class.getClassLoader(),
                new Class[]{CallableStatement.class},
                new CallableStatementHandler(real, sql, cache, this)
            );
        }
    }

    private static class StatementHandler implements InvocationHandler {
        private final Statement real;
        private final NativeCache cache;
        private final ConnectionHandler connHandler;

        StatementHandler(Statement real, NativeCache cache, ConnectionHandler connHandler) {
            this.real = real;
            this.cache = cache;
            this.connHandler = connHandler;
        }

        /**
         * Multi-statement-aware write invalidation. Calls
         * {@link NativeCache#detectWritesMulti} so a write buried after a SET
         * in a multi-statement Q body still drives the right invalidation.
         * Returns whether a write was detected (callers use this to skip the
         * cache-read path).
         */
        private boolean handleWriteInvalidation(String sql) {
            NativeCache.WriteSummary w = NativeCache.detectWritesMulti(sql);
            if (w == null) return false;
            if (w.ddl) {
                cache.invalidateAll();
            } else {
                for (String table : w.tables) cache.invalidateTable(table);
            }
            return true;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            // Hold the connection lock across the entire call so the post-call
            // verify executor never races with a user-facing JDBC method on
            // the same physical connection (JDBC connections aren't thread-
            // safe per spec).
            synchronized (connHandler.connectionLock) {
                switch (method.getName()) {
                    case "executeQuery":
                        return handleExecuteQuery((String) args[0]);
                    case "executeUpdate":
                        return handleExecuteUpdate((String) args[0]);
                    case "execute":
                        if (args != null && args.length > 0 && args[0] instanceof String) {
                            return handleExecute((String) args[0]);
                        }
                        return method.invoke(real, args);
                    case "addBatch":
                        // Statement.addBatch(String) appends the SQL to a batch
                        // that's flushed on executeBatch(). Pre-fix, neither
                        // addBatch nor executeBatch observed SET commands —
                        // batched session-state changes never shifted the
                        // wrapper-side L1 state hash. We observe at addBatch
                        // time so the side effect is recorded in source order
                        // (executeBatch is atomic on the wire, but each
                        // observed SET must take effect before subsequent
                        // batched statements would see it).
                        // (java-jdbc-callable-batch-statehash-gap.md, 2026-05-04)
                        if (args != null && args.length > 0 && args[0] instanceof String) {
                            connHandler.gucState.observeSql((String) args[0]);
                        }
                        return method.invoke(real, args);
                    default:
                        return method.invoke(real, args);
                }
            }
        }

        private ResultSet handleExecuteQuery(String sql) throws SQLException {
            // Transaction tracking. Updates the flag but doesn't return — a
            // multi-statement body like "BEGIN; INSERT INTO orders ..." needs
            // the write-detection pass below to also fire so the stale
            // `orders` cache is invalidated.
            //
            // Multi-statement-aware: the single-token isTxStart/isTxEnd only
            // sees the FIRST token of the whole body. A
            // "BEGIN; INSERT...; COMMIT" body would otherwise flip the
            // wrapper into "in tx" on the BEGIN and never see the COMMIT,
            // pinning the wrapper into cache-bypass mode forever. updateTxState
            // walks every segment so the resulting flag matches what the
            // server actually settled on after running the whole body.
            connHandler.inTransaction = NativeCache.updateTxState(connHandler.inTransaction, sql);

            // Write detection — multi-statement-aware so writes buried after
            // a SET in a Q body still invalidate the right tables.
            if (handleWriteInvalidation(sql)) {
                ResultSet rs = real.executeQuery(sql);
                maybeScheduleVerify(sql);
                return rs;
            }

            // Pure-TX commands skip the cache path entirely (no rows to cache).
            if (NativeCache.isTxStart(sql) || NativeCache.isTxEnd(sql)) {
                return real.executeQuery(sql);
            }

            // SET / RESET observation. Runs on every query so a SET that's
            // batched into a multi-statement Q ("SET app.user_id='42'; SELECT
            // ...") still updates the per-connection state hash before we
            // build the cache key.
            connHandler.gucState.observeSql(sql);

            // Lazy verify-on-checkout — if a previous stored-function call
            // marked us dirty (or a previous verify failed), reconcile state
            // before we use the hash as a cache key. Synchronous; we already
            // hold the connection lock.
            connHandler.verifyIfDirty();

            // In transaction: bypass cache
            if (connHandler.inTransaction) {
                ResultSet rs = real.executeQuery(sql);
                maybeScheduleVerify(sql);
                return rs;
            }

            // Check native cache
            long stateHash = connHandler.gucState.hash();
            NativeCache.CacheEntry entry = cache.get(sql, null, stateHash);
            if (entry != null) {
                // Function-call hits don't trigger a verify either way — the
                // function never actually ran on this trip (cache served the
                // result), so the server-side state can't have shifted.
                return CachedResultSet.create(entry.rows, entry.columns);
            }

            // Cache miss
            ResultSet rs = real.executeQuery(sql);
            ResultSet out = cacheAndReturn(sql, null, rs, stateHash);
            maybeScheduleVerify(sql);
            return out;
        }

        private int handleExecuteUpdate(String sql) throws SQLException {
            // Same tx-state walk as handleExecuteQuery — executeUpdate is the
            // path some drivers route DDL/DML through, and a multi-statement
            // "BEGIN; UPDATE...; COMMIT" body must leave the wrapper-side
            // tx flag matching the server's post-COMMIT state.
            connHandler.inTransaction = NativeCache.updateTxState(connHandler.inTransaction, sql);
            connHandler.gucState.observeSql(sql);
            handleWriteInvalidation(sql);
            int rows = real.executeUpdate(sql);
            maybeScheduleVerify(sql);
            return rows;
        }

        private boolean handleExecute(String sql) throws SQLException {
            // Multi-statement-aware tx-state walk; see handleExecuteQuery for
            // the BEGIN-then-COMMIT-buried-in-the-same-body bug it fixes.
            connHandler.inTransaction = NativeCache.updateTxState(connHandler.inTransaction, sql);
            connHandler.gucState.observeSql(sql);
            handleWriteInvalidation(sql);
            boolean result = real.execute(sql);
            maybeScheduleVerify(sql);
            return result;
        }

        /**
         * Schedule an async post-call GUC-state verify if {@code sql} is a
         * top-level function or procedure call. Function bodies can issue
         * {@code SET}s the wire layer never observed; the verify reconciles
         * via {@code pg_settings} on the same connection (serialised by the
         * connection lock so we never race the user's next call).
         *
         * <p>Best effort — if the executor is saturated, the next user query
         * will see {@link GucState#isDirty} false and miss the chance to
         * reconcile until something else flips the bit. Rare, and the
         * fragmentation cost of a stale state is bounded by Option Y's
         * per-tenant cache slotting.
         */
        private void maybeScheduleVerify(String sql) {
            if (GucState.isFunctionCall(sql)) {
                connHandler.gucState.markDirty();
                connHandler.scheduleVerify();
            }
        }

        ResultSet cacheAndReturn(String sql, Object[] params, ResultSet rs, long stateHash) throws SQLException {
            try {
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();
                String[] columns = new String[colCount];
                for (int i = 0; i < colCount; i++) {
                    columns[i] = meta.getColumnLabel(i + 1);
                }

                List<Object[]> rows = new ArrayList<>();
                while (rs.next()) {
                    Object[] row = new Object[colCount];
                    for (int i = 0; i < colCount; i++) {
                        row[i] = rs.getObject(i + 1);
                    }
                    rows.add(row);
                }
                rs.close();

                // Skip caching session-state commands (SET / RESET / LISTEN /
                // BEGIN / COMMIT / etc.). Their responses are empty or
                // status-only; storing them bloats the cache with entries that
                // never serve real data and adds eviction pressure if a
                // session does many SETs. The PG JDBC driver permits these
                // through executeQuery(), so the guard has to live here — the
                // executeUpdate / execute paths already skip the cache layer.
                if (!NativeCache.isSessionStateCommand(sql)) {
                    cache.put(sql, params, rows, columns, stateHash);
                }
                return CachedResultSet.create(rows, columns);
            } catch (Exception e) {
                return rs;
            }
        }
    }

    private static class PreparedStatementHandler implements InvocationHandler {
        private final PreparedStatement real;
        private final String sql;
        private final NativeCache cache;
        private final ConnectionHandler connHandler;
        private final Map<Integer, Object> params = new HashMap<>();

        PreparedStatementHandler(PreparedStatement real, String sql, NativeCache cache, ConnectionHandler connHandler) {
            this.real = real;
            this.sql = sql;
            this.cache = cache;
            this.connHandler = connHandler;
        }

        /** See {@link StatementHandler#handleWriteInvalidation(String)}. */
        private boolean handleWriteInvalidation(String sql) {
            NativeCache.WriteSummary w = NativeCache.detectWritesMulti(sql);
            if (w == null) return false;
            if (w.ddl) {
                cache.invalidateAll();
            } else {
                for (String table : w.tables) cache.invalidateTable(table);
            }
            return true;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();

            // Track parameter setting — these don't touch the wire so they
            // don't need the connection lock. Held outside `synchronized` to
            // keep parameter binding contention-free in the common case.
            if (name.startsWith("set") && args != null && args.length >= 2 && args[0] instanceof Integer) {
                params.put((Integer) args[0], args[1]);
                return method.invoke(real, args);
            }

            synchronized (connHandler.connectionLock) {
                switch (name) {
                    case "executeQuery":
                        if (args == null || args.length == 0) {
                            return handlePreparedQuery();
                        }
                        // executeQuery(String) — delegate to Statement behavior
                        return method.invoke(real, args);
                    case "executeUpdate":
                        if (args == null || args.length == 0) {
                            return handlePreparedUpdate();
                        }
                        return method.invoke(real, args);
                    case "execute":
                        if (args == null || args.length == 0) {
                            return handlePreparedExecute();
                        }
                        return method.invoke(real, args);
                    case "clearParameters":
                        params.clear();
                        return method.invoke(real, args);
                    default:
                        return method.invoke(real, args);
                }
            }
        }

        private Object[] paramsArray() {
            if (params.isEmpty()) return null;
            int max = Collections.max(params.keySet());
            Object[] arr = new Object[max];
            for (Map.Entry<Integer, Object> e : params.entrySet()) {
                arr[e.getKey() - 1] = e.getValue();
            }
            return arr;
        }

        private ResultSet handlePreparedQuery() throws SQLException {
            Object[] p = paramsArray();

            if (handleWriteInvalidation(sql)) {
                ResultSet rs = real.executeQuery();
                maybeScheduleVerify(sql);
                return rs;
            }

            // SET / RESET observation. PreparedStatement is unusual for SET
            // (parameters typically aren't permitted in `SET name = $1`), but
            // the bookkeeping is cheap and the symmetry with Statement keeps
            // the cache key shape consistent regardless of which path the
            // SET arrived on.
            connHandler.gucState.observeSql(sql);

            // Lazy verify-on-checkout — see StatementHandler.handleExecuteQuery.
            connHandler.verifyIfDirty();

            if (connHandler.inTransaction) {
                ResultSet rs = real.executeQuery();
                maybeScheduleVerify(sql);
                return rs;
            }

            long stateHash = connHandler.gucState.hash();
            NativeCache.CacheEntry entry = cache.get(sql, p, stateHash);
            if (entry != null) {
                return CachedResultSet.create(entry.rows, entry.columns);
            }

            ResultSet rs = real.executeQuery();
            // Reuse Statement's caching logic
            try {
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();
                String[] columns = new String[colCount];
                for (int i = 0; i < colCount; i++) {
                    columns[i] = meta.getColumnLabel(i + 1);
                }
                List<Object[]> rows = new ArrayList<>();
                while (rs.next()) {
                    Object[] row = new Object[colCount];
                    for (int i = 0; i < colCount; i++) {
                        row[i] = rs.getObject(i + 1);
                    }
                    rows.add(row);
                }
                rs.close();
                // See StatementHandler.cacheAndReturn — same skip-list applies
                // to PreparedStatement.executeQuery() routes too.
                if (!NativeCache.isSessionStateCommand(sql)) {
                    cache.put(sql, p, rows, columns, stateHash);
                }
                ResultSet cached = CachedResultSet.create(rows, columns);
                maybeScheduleVerify(sql);
                return cached;
            } catch (Exception e) {
                maybeScheduleVerify(sql);
                return rs;
            }
        }

        private int handlePreparedUpdate() throws SQLException {
            connHandler.gucState.observeSql(sql);
            handleWriteInvalidation(sql);
            int rows = real.executeUpdate();
            maybeScheduleVerify(sql);
            return rows;
        }

        private boolean handlePreparedExecute() throws SQLException {
            connHandler.gucState.observeSql(sql);
            handleWriteInvalidation(sql);
            boolean result = real.execute();
            maybeScheduleVerify(sql);
            return result;
        }

        /** See {@link StatementHandler#maybeScheduleVerify(String)}. */
        private void maybeScheduleVerify(String sql) {
            if (GucState.isFunctionCall(sql)) {
                connHandler.gucState.markDirty();
                connHandler.scheduleVerify();
            }
        }
    }

    /**
     * CallableStatement proxy — observes the bound SQL on each {@code execute*}
     * and runs write-invalidation, mirroring the PreparedStatement path.
     * Stored-proc invocations ({@code {call my_proc(?)}}) classify as
     * {@code CALL → DDL_SENTINEL} in {@link NativeCache#detectWrite}, so any
     * call collapses to a full-cache invalidation — the proc body could
     * mutate any table.
     *
     * <p>We never read CallableStatement results out of the cache (procs
     * may have side effects and out-params; replaying cached rows would be
     * wrong). The handler only needs to: (1) observe SQL for GUC-state
     * tracking, (2) drive invalidation, then (3) delegate to the real
     * CallableStatement and pass the result through unchanged.
     *
     * <p>Filed as part of
     * {@code java-jdbc-callable-batch-statehash-gap.md} (2026-05-04).
     */
    private static class CallableStatementHandler implements InvocationHandler {
        private final CallableStatement real;
        private final String sql;
        private final NativeCache cache;
        private final ConnectionHandler connHandler;

        CallableStatementHandler(CallableStatement real, String sql, NativeCache cache, ConnectionHandler connHandler) {
            this.real = real;
            this.sql = sql;
            this.cache = cache;
            this.connHandler = connHandler;
        }

        /** See {@link StatementHandler#handleWriteInvalidation(String)}. */
        private void handleWriteInvalidation(String sql) {
            NativeCache.WriteSummary w = NativeCache.detectWritesMulti(sql);
            if (w == null) return;
            if (w.ddl) {
                cache.invalidateAll();
            } else {
                for (String table : w.tables) cache.invalidateTable(table);
            }
        }

        /**
         * Whether the bound SQL looks like a JDBC stored-procedure call —
         * either the JDBC escape form ({@code {call my_proc(...)}} or
         * {@code {?= call my_func(...)}}) or the bare {@code CALL ...} /
         * {@code SELECT ident(...)} form (which {@link GucState#isFunctionCall}
         * also catches; we keep this method symmetric so the
         * CallableStatement path is self-contained).
         */
        private static boolean looksLikeProcCall(String sql) {
            if (sql == null) return false;
            String s = sql.trim();
            // Strip leading `{?= ` and `{` for the JDBC escape forms.
            if (s.startsWith("{")) {
                int idx = 1;
                while (idx < s.length() && Character.isWhitespace(s.charAt(idx))) idx++;
                if (idx < s.length() && s.charAt(idx) == '?') {
                    idx++;
                    while (idx < s.length() && Character.isWhitespace(s.charAt(idx))) idx++;
                    if (idx < s.length() && s.charAt(idx) == '=') {
                        idx++;
                        while (idx < s.length() && Character.isWhitespace(s.charAt(idx))) idx++;
                    }
                }
                if (idx + 4 <= s.length() && s.substring(idx, idx + 4).equalsIgnoreCase("call")
                    && (idx + 4 == s.length() || Character.isWhitespace(s.charAt(idx + 4)))) {
                    return true;
                }
            }
            return GucState.isFunctionCall(s);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            synchronized (connHandler.connectionLock) {
                switch (name) {
                    case "executeQuery":
                    case "executeUpdate":
                    case "execute":
                        // Observe + invalidate ONLY for the no-arg variants —
                        // those use the bound SQL captured at prepareCall time.
                        // The (String) overloads aren't part of the
                        // CallableStatement contract (inherited but undefined),
                        // so we just delegate without observation.
                        if (args == null || args.length == 0) {
                            connHandler.gucState.observeSql(sql);
                            handleWriteInvalidation(sql);
                            Object result = method.invoke(real, args);
                            // Schedule post-call verify when the bound SQL is
                            // recognisably a stored function or proc call —
                            // including the JDBC `{call my_proc(...)}` escape
                            // form, which lexes differently from the bare
                            // `SELECT ident(` / `CALL ident(` checked by
                            // GucState.isFunctionCall. CallableStatement is
                            // also a legitimate Statement substitute (some
                            // apps run `SET app.user_id = '42'` through
                            // prepareCall), so we can't unconditionally fire —
                            // a verify after every prepareCall execute would
                            // wipe wire-observed SETs against any non-PG-aware
                            // backend.
                            if (looksLikeProcCall(sql) || GucState.isFunctionCall(sql)) {
                                connHandler.gucState.markDirty();
                                connHandler.scheduleVerify();
                            }
                            return result;
                        }
                        return method.invoke(real, args);
                    case "addBatch":
                        // Mirror StatementHandler.addBatch: observe the
                        // String-arg form. The no-arg form uses the bound SQL
                        // (already observed at execute time, so re-observing
                        // at addBatch would double-count).
                        if (args != null && args.length > 0 && args[0] instanceof String) {
                            connHandler.gucState.observeSql((String) args[0]);
                        }
                        return method.invoke(real, args);
                    default:
                        return method.invoke(real, args);
                }
            }
        }
    }
}
