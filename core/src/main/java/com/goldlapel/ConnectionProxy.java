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

    /**
     * Wrap {@code real} with the GUC-aware proxy and the wrapper-side native
     * cache. Aggressive-verify is OFF for connections wrapped through this
     * overload — Wave 1's verify-on-checkout / post-call-function verify still
     * runs, but no post-DML expansion is scheduled. Suitable for tests and
     * for callers that have decided aggressive-verify is not warranted.
     */
    public static Connection wrap(Connection real, NativeCache cache) {
        return wrap(real, cache, AggressiveVerifyMode.OFF, null);
    }

    /**
     * Wrap {@code real} with the GUC-aware proxy plus optional smart-auto
     * post-DML aggressive verify. {@code mode} controls whether the wrapper
     * schedules a verify after every INSERT/UPDATE/DELETE/MERGE/TRUNCATE/DDL
     * (in addition to the post-function-call verify Wave 1 already wires):
     *
     * <ul>
     *   <li>{@link AggressiveVerifyMode#AUTO} — probe {@code pg_trigger} on
     *       first connection per JDBC URL via
     *       {@link AggressiveVerifyDetector#isActive}. If the schema has any
     *       trigger that issues a session SET, post-DML verify is enabled
     *       for every connection to that URL (and the result is cached for
     *       the JVM's lifetime).</li>
     *   <li>{@link AggressiveVerifyMode#ON} — always schedule post-DML
     *       verify, regardless of detection.</li>
     *   <li>{@link AggressiveVerifyMode#OFF} — never schedule post-DML
     *       verify. Wave 1 paths still run.</li>
     * </ul>
     *
     * <p>{@code jdbcUrl} is used as the detection cache key in AUTO mode and
     * for license-payload overrides; pass {@code null} to skip detection
     * (treats AUTO as OFF). The probe runs synchronously on {@code real}
     * before the wrap returns the proxy — the calling thread pays the
     * one-query cost on the first wrapped connection per URL, after which
     * the cached decision is free.
     */
    public static Connection wrap(Connection real, NativeCache cache,
                                  AggressiveVerifyMode mode, String jdbcUrl) {
        boolean aggressive = resolveAggressive(real, mode, jdbcUrl);
        return (Connection) Proxy.newProxyInstance(
            ConnectionProxy.class.getClassLoader(),
            new Class[]{Connection.class},
            new ConnectionHandler(real, cache, aggressive)
        );
    }

    /**
     * Resolve the effective post-DML verify decision. AUTO consults the
     * detector (which caches per-URL); ON / OFF are pass-through. A null URL
     * disables detection — the caller deliberately opted out of the
     * per-URL cache (e.g. a unit-test fake connection where probing
     * {@code pg_trigger} would have no meaning).
     */
    static boolean resolveAggressive(Connection probeConn, AggressiveVerifyMode mode, String jdbcUrl) {
        if (mode == null) mode = AggressiveVerifyMode.AUTO;
        switch (mode) {
            case ON:
                return true;
            case OFF:
                return false;
            case AUTO:
            default:
                if (jdbcUrl == null) return false;
                return AggressiveVerifyDetector.isActive(jdbcUrl, probeConn);
        }
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
         * Whether to schedule a post-DML verify after every observed write
         * (INSERT / UPDATE / DELETE / MERGE / TRUNCATE / CALL / DDL). Set at
         * wrap time per {@link AggressiveVerifyMode}; immutable for the
         * lifetime of the connection. {@code false} means Wave 1's
         * post-function-call verify is the only verify trigger — that path
         * still runs in OFF mode and covers the common stored-function case.
         *
         * <p>When {@code true}, the wrapper also fires a verify after writes
         * to catch trigger-internal SETs (a customer trigger that runs
         * {@code SET app.user_id = ...} on INSERT). The cost is ~1ms of
         * post-write verify-pool occupancy per write, doesn't block the
         * write response, and is the only way to cover the trigger-internal
         * SET case without server-side instrumentation. See
         * {@code goldlapel/docs/todos/aggressive-verify-flag.md}.
         */
        final boolean aggressiveVerify;

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
            this(real, cache, false);
        }

        ConnectionHandler(Connection real, NativeCache cache, boolean aggressiveVerify) {
            this.real = real;
            this.cache = cache;
            this.aggressiveVerify = aggressiveVerify;
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
                        // COMMIT preserves session-level SETs issued inside
                        // the txn — no GUC-state revert needed (the wrapper's
                        // wire-side observation already reflects server state).
                        inTransaction = false;
                        return method.invoke(real, args);
                    case "rollback":
                        // PG semantics: SET issued inside a transaction is
                        // reverted on ROLLBACK (per the SQL-SET docs). We don't
                        // know which (if any) SETs the user issued during the
                        // transaction, so mark dirty — the next user query's
                        // verifyIfDirty path will reconcile against pg_settings
                        // before consulting the cache. Cheaper than tracking a
                        // per-txn snapshot for a code path most apps hit
                        // rarely. Applies to both 0-arg rollback and the
                        // Savepoint-arg rollback (partial rollback can also
                        // revert SETs done after the savepoint).
                        inTransaction = false;
                        gucState.markDirty();
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
        /**
         * Pending batched SQL strings. Recorded at {@code addBatch} time but
         * not observed against the GUC state until the JDBC driver reports
         * {@code executeBatch} success (the SETs may have failed mid-batch on
         * the server, in which case we can't trust optimistic observation).
         * On {@code executeBatch} success: each entry is observed in source
         * order. On failure: dropped + state marked dirty so the next path
         * reconciles with pg_settings (JDBC's BatchUpdateException carries
         * partial-success counts, but PG runs the whole batch in an implicit
         * transaction by default — partial state on the wrapper side is worse
         * than a forced reconcile). On {@code clearBatch}: dropped without
         * observation.
         */
        private final java.util.List<String> pendingBatch = new java.util.ArrayList<>();

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
                        // Statement.addBatch(String) buffers the SQL on the
                        // driver side; nothing hits the server until
                        // executeBatch(). We accumulate the same buffer locally
                        // and observe each entry against the GUC state ONLY
                        // after executeBatch reports success — observing at
                        // addBatch time would diverge state-hash from the
                        // server if executeBatch later throws.
                        // (java-set-actually-applied, 2026-05-05)
                        if (args != null && args.length > 0 && args[0] instanceof String) {
                            pendingBatch.add((String) args[0]);
                        }
                        return method.invoke(real, args);
                    case "clearBatch":
                        // Driver drops the buffer; we drop our pending list to
                        // match. No state-hash effect — nothing was observed.
                        pendingBatch.clear();
                        return method.invoke(real, args);
                    case "executeBatch":
                        return handleExecuteBatch();
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
                // SET-in-the-same-body still needs the wire-side observation,
                // but the JDBC call may fail and we'd be left with a hash that
                // doesn't match server state. Snapshot + revert-on-throw.
                GucState.Snapshot snap = connHandler.gucState.snapshot();
                connHandler.gucState.observeSql(sql);
                try {
                    ResultSet rs = real.executeQuery(sql);
                    maybeScheduleVerify(sql);
                    maybeSchedulePostDmlVerify();
                    return rs;
                } catch (SQLException | RuntimeException e) {
                    connHandler.gucState.restoreOrReset(snap);
                    connHandler.gucState.markDirty();
                    throw e;
                }
            }

            // Pure-TX commands skip the cache path entirely (no rows to cache).
            if (NativeCache.isTxStart(sql) || NativeCache.isTxEnd(sql)) {
                return real.executeQuery(sql);
            }

            // Lazy verify-on-checkout — if a previous stored-function call
            // marked us dirty (or a previous verify failed), reconcile state
            // before we use the hash as a cache key. Synchronous; we already
            // hold the connection lock. Runs BEFORE the snapshot so a JDBC-
            // failure revert doesn't unwind a fresh pg_settings reconciliation
            // (the verify may have happened on stale dirty bits unrelated to
            // this SQL — preserving its result on the revert path saves a
            // round-trip on the next call).
            connHandler.verifyIfDirty();

            // SET / RESET observation. Runs on every query so a SET that's
            // batched into a multi-statement Q ("SET app.user_id='42'; SELECT
            // ...") still updates the per-connection state hash before we
            // build the cache key. Optimistic — the wire-side mutation may
            // be wrong if the JDBC call throws; snap + revert in the catch.
            GucState.Snapshot snap = connHandler.gucState.snapshot();
            connHandler.gucState.observeSql(sql);

            // In transaction: bypass cache
            if (connHandler.inTransaction) {
                try {
                    ResultSet rs = real.executeQuery(sql);
                    maybeScheduleVerify(sql);
                    return rs;
                } catch (SQLException | RuntimeException e) {
                    connHandler.gucState.restoreOrReset(snap);
                    connHandler.gucState.markDirty();
                    throw e;
                }
            }

            // Check native cache
            long stateHash = connHandler.gucState.hash();
            NativeCache.CacheEntry entry = cache.get(sql, null, stateHash);
            if (entry != null) {
                // Function-call hits don't trigger a verify either way — the
                // function never actually ran on this trip (cache served the
                // result), so the server-side state can't have shifted. We
                // don't need to revert the snapshot either: a cache HIT means
                // no JDBC call was made, the optimistic SET observation
                // (if any) was applied without a wire round-trip, and the
                // server already had the same SET applied earlier (else the
                // hash wouldn't match the cached entry's slot).
                return CachedResultSet.create(entry.rows, entry.columns);
            }

            // Cache miss — issue the real query, revert state on failure so
            // the wrapper-side hash doesn't diverge from what the server
            // actually applied.
            try {
                ResultSet rs = real.executeQuery(sql);
                ResultSet out = cacheAndReturn(sql, null, rs, stateHash);
                maybeScheduleVerify(sql);
                return out;
            } catch (SQLException | RuntimeException e) {
                connHandler.gucState.restoreOrReset(snap);
                connHandler.gucState.markDirty();
                throw e;
            }
        }

        private int handleExecuteUpdate(String sql) throws SQLException {
            // Same tx-state walk as handleExecuteQuery — executeUpdate is the
            // path some drivers route DDL/DML through, and a multi-statement
            // "BEGIN; UPDATE...; COMMIT" body must leave the wrapper-side
            // tx flag matching the server's post-COMMIT state.
            connHandler.inTransaction = NativeCache.updateTxState(connHandler.inTransaction, sql);
            GucState.Snapshot snap = connHandler.gucState.snapshot();
            connHandler.gucState.observeSql(sql);
            boolean isWrite = handleWriteInvalidation(sql);
            try {
                int rows = real.executeUpdate(sql);
                maybeScheduleVerify(sql);
                if (isWrite) maybeSchedulePostDmlVerify();
                return rows;
            } catch (SQLException | RuntimeException e) {
                connHandler.gucState.restoreOrReset(snap);
                connHandler.gucState.markDirty();
                throw e;
            }
        }

        private boolean handleExecute(String sql) throws SQLException {
            // Multi-statement-aware tx-state walk; see handleExecuteQuery for
            // the BEGIN-then-COMMIT-buried-in-the-same-body bug it fixes.
            connHandler.inTransaction = NativeCache.updateTxState(connHandler.inTransaction, sql);
            GucState.Snapshot snap = connHandler.gucState.snapshot();
            connHandler.gucState.observeSql(sql);
            boolean isWrite = handleWriteInvalidation(sql);
            try {
                boolean result = real.execute(sql);
                maybeScheduleVerify(sql);
                if (isWrite) maybeSchedulePostDmlVerify();
                return result;
            } catch (SQLException | RuntimeException e) {
                connHandler.gucState.restoreOrReset(snap);
                connHandler.gucState.markDirty();
                throw e;
            }
        }

        /**
         * Schedule a post-DML verify if {@link ConnectionHandler#aggressiveVerify}
         * is set on this connection. Called from the executeQuery / executeUpdate /
         * execute paths after any write was observed by
         * {@link #handleWriteInvalidation(String)}, so we cover INSERT / UPDATE /
         * DELETE / MERGE / TRUNCATE / CALL / DDL — anything {@link NativeCache#detectWritesMulti}
         * recognises as state-mutating.
         *
         * <p>Triggers fire server-side on these statements, and a customer
         * trigger could legitimately {@code SET app.user_id = ...} in its
         * body; the wrapper has no way to see that SET on the wire. Marking
         * dirty + queueing a verify means the next user query reconciles
         * via {@code pg_settings} before consulting the cache. Skipping this
         * when aggressive mode is off keeps the no-trigger path zero-tax.
         */
        private void maybeSchedulePostDmlVerify() {
            if (!connHandler.aggressiveVerify) return;
            connHandler.gucState.markDirty();
            connHandler.scheduleVerify();
        }

        /**
         * Execute the buffered batch with deferred GUC-state observation.
         * On JDBC success: replay each pending SQL through {@code observeSql}
         * in source order so the state hash settles to what the server saw.
         * On any failure (SQLException, BatchUpdateException, or runtime
         * error mid-flight): drop the pending list, mark dirty, and rethrow.
         * pgjdbc executes batches inside an implicit transaction with a
         * single error-ends-the-batch failure mode — a partial-success commit
         * could leave us inconsistent either way, so the conservative path is
         * "force a verify on next checkout".
         */
        private int[] handleExecuteBatch() throws SQLException {
            try {
                int[] counts = real.executeBatch();
                // Driver's buffer is flushed on success; ours commits.
                for (String sql : pendingBatch) {
                    connHandler.gucState.observeSql(sql);
                }
                pendingBatch.clear();
                return counts;
            } catch (SQLException | RuntimeException e) {
                pendingBatch.clear();
                connHandler.gucState.markDirty();
                throw e;
            }
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
        /**
         * Whether {@code addBatch()} (no-arg form, buffers the current
         * parameter set against the prepared SQL) has been called since the
         * last {@code executeBatch} / {@code clearBatch}. Used to defer GUC
         * observation: prepared SQL doesn't change between adds, so we observe
         * once on executeBatch success regardless of how many param sets were
         * batched. {@code SET name = $1} isn't actually accepted by PG (SET
         * takes literals, not parameters), so this is largely cosmetic — but
         * cheap, and keeps the cache key shape consistent with the Statement
         * path's deferred-observation contract.
         */
        private boolean pendingBatchAdds = false;

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
                    case "addBatch":
                        // PreparedStatement.addBatch() (no-arg) buffers the
                        // current parameter set. addBatch(String) is also
                        // inherited from Statement but pgjdbc throws — we
                        // delegate either way. The no-arg form is what
                        // matters: observation is deferred until executeBatch
                        // succeeds (see pendingBatchAdds field doc).
                        if (args == null || args.length == 0) {
                            pendingBatchAdds = true;
                        }
                        return method.invoke(real, args);
                    case "clearBatch":
                        pendingBatchAdds = false;
                        return method.invoke(real, args);
                    case "executeBatch":
                        return handlePreparedExecuteBatch();
                    default:
                        return method.invoke(real, args);
                }
            }
        }

        /**
         * Mirror of {@link StatementHandler#handleExecuteBatch()} for the
         * PreparedStatement no-arg execute path. Defers observing the
         * prepared SQL until the JDBC call returns successfully — on
         * SQLException / runtime error, drops the deferred observation and
         * marks dirty. Observation runs at most once even if N param sets
         * were batched: the SQL string is the same for every add.
         */
        private int[] handlePreparedExecuteBatch() throws SQLException {
            try {
                int[] counts = real.executeBatch();
                if (pendingBatchAdds) {
                    connHandler.gucState.observeSql(sql);
                    pendingBatchAdds = false;
                }
                return counts;
            } catch (SQLException | RuntimeException e) {
                pendingBatchAdds = false;
                connHandler.gucState.markDirty();
                throw e;
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
                GucState.Snapshot snap = connHandler.gucState.snapshot();
                connHandler.gucState.observeSql(sql);
                try {
                    ResultSet rs = real.executeQuery();
                    maybeScheduleVerify(sql);
                    maybeSchedulePostDmlVerify();
                    return rs;
                } catch (SQLException | RuntimeException e) {
                    connHandler.gucState.restoreOrReset(snap);
                    connHandler.gucState.markDirty();
                    throw e;
                }
            }

            // SET / RESET observation. PreparedStatement is unusual for SET
            // (parameters typically aren't permitted in `SET name = $1`), but
            // the bookkeeping is cheap and the symmetry with Statement keeps
            // the cache key shape consistent regardless of which path the
            // SET arrived on. Observation is optimistic — if the JDBC call
            // throws, we revert from the pre-call snapshot.
            GucState.Snapshot snap = connHandler.gucState.snapshot();
            connHandler.gucState.observeSql(sql);

            // Lazy verify-on-checkout — see StatementHandler.handleExecuteQuery.
            connHandler.verifyIfDirty();

            if (connHandler.inTransaction) {
                try {
                    ResultSet rs = real.executeQuery();
                    maybeScheduleVerify(sql);
                    return rs;
                } catch (SQLException | RuntimeException e) {
                    connHandler.gucState.restoreOrReset(snap);
                    connHandler.gucState.markDirty();
                    throw e;
                }
            }

            long stateHash = connHandler.gucState.hash();
            NativeCache.CacheEntry entry = cache.get(sql, p, stateHash);
            if (entry != null) {
                // Cache hit — no JDBC call, no opportunity for divergence.
                // See StatementHandler.handleExecuteQuery for the symmetry
                // argument.
                return CachedResultSet.create(entry.rows, entry.columns);
            }

            ResultSet rs;
            try {
                rs = real.executeQuery();
            } catch (SQLException | RuntimeException e) {
                connHandler.gucState.restoreOrReset(snap);
                connHandler.gucState.markDirty();
                throw e;
            }
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
            GucState.Snapshot snap = connHandler.gucState.snapshot();
            connHandler.gucState.observeSql(sql);
            boolean isWrite = handleWriteInvalidation(sql);
            try {
                int rows = real.executeUpdate();
                maybeScheduleVerify(sql);
                if (isWrite) maybeSchedulePostDmlVerify();
                return rows;
            } catch (SQLException | RuntimeException e) {
                connHandler.gucState.restoreOrReset(snap);
                connHandler.gucState.markDirty();
                throw e;
            }
        }

        private boolean handlePreparedExecute() throws SQLException {
            GucState.Snapshot snap = connHandler.gucState.snapshot();
            connHandler.gucState.observeSql(sql);
            boolean isWrite = handleWriteInvalidation(sql);
            try {
                boolean result = real.execute();
                maybeScheduleVerify(sql);
                if (isWrite) maybeSchedulePostDmlVerify();
                return result;
            } catch (SQLException | RuntimeException e) {
                connHandler.gucState.restoreOrReset(snap);
                connHandler.gucState.markDirty();
                throw e;
            }
        }

        /** See {@link StatementHandler#maybeScheduleVerify(String)}. */
        private void maybeScheduleVerify(String sql) {
            if (GucState.isFunctionCall(sql)) {
                connHandler.gucState.markDirty();
                connHandler.scheduleVerify();
            }
        }

        /** See {@link StatementHandler#maybeSchedulePostDmlVerify()}. */
        private void maybeSchedulePostDmlVerify() {
            if (!connHandler.aggressiveVerify) return;
            connHandler.gucState.markDirty();
            connHandler.scheduleVerify();
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
        /**
         * Pending batched SQL strings from {@code addBatch(String)} calls
         * (CallableStatement inherits Statement.addBatch(String)). The
         * no-arg {@code addBatch()} variant uses the bound SQL and is
         * tracked by {@link #pendingBoundBatchAdds}.
         *
         * <p>See {@link StatementHandler#pendingBatch} for the deferred-
         * observation rationale.
         */
        private final java.util.List<String> pendingStringBatch = new java.util.ArrayList<>();
        /**
         * Whether the no-arg {@code addBatch()} (using the bound SQL captured
         * at {@code prepareCall} time) has been called since the last
         * {@code executeBatch} / {@code clearBatch}. Mirrors the
         * PreparedStatement flag with the same name.
         */
        private boolean pendingBoundBatchAdds = false;

        CallableStatementHandler(CallableStatement real, String sql, NativeCache cache, ConnectionHandler connHandler) {
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
                            // Optimistic observation — snapshot first, then on
                            // JDBC failure restore. Matches Statement /
                            // PreparedStatement deferral semantics so a
                            // CallableStatement-routed SET that errors at the
                            // server doesn't leave the wrapper-side hash
                            // diverged.
                            GucState.Snapshot snap = connHandler.gucState.snapshot();
                            connHandler.gucState.observeSql(sql);
                            boolean isWrite = handleWriteInvalidation(sql);
                            try {
                                Object result = method.invoke(real, args);
                                // Schedule post-call verify when the bound SQL
                                // is recognisably a stored function or proc
                                // call — including the JDBC `{call ...}`
                                // escape form, which lexes differently from
                                // the bare `SELECT ident(` / `CALL ident(`
                                // checked by GucState.isFunctionCall.
                                // CallableStatement is also a legitimate
                                // Statement substitute (some apps run
                                // `SET app.user_id = '42'` through
                                // prepareCall), so we can't unconditionally
                                // fire — a verify after every prepareCall
                                // execute would wipe wire-observed SETs
                                // against any non-PG-aware backend.
                                if (looksLikeProcCall(sql) || GucState.isFunctionCall(sql)) {
                                    connHandler.gucState.markDirty();
                                    connHandler.scheduleVerify();
                                } else if (isWrite && connHandler.aggressiveVerify) {
                                    // Post-DML aggressive verify on the
                                    // CallableStatement path. CallableStatement
                                    // is also a legitimate Statement substitute
                                    // (some apps run plain INSERTs through
                                    // prepareCall); cover that case too. The
                                    // function-call branch already covers the
                                    // CALL/SELECT-fn shapes — this guard
                                    // catches the bare DML routes.
                                    connHandler.gucState.markDirty();
                                    connHandler.scheduleVerify();
                                }
                                return result;
                            } catch (java.lang.reflect.InvocationTargetException ite) {
                                // Reflection wraps the JDBC driver's actual
                                // exception — unwrap so the caller sees the
                                // SQLException pgjdbc would have thrown if
                                // we'd called real.execute() directly.
                                Throwable cause = ite.getCause();
                                connHandler.gucState.restoreOrReset(snap);
                                connHandler.gucState.markDirty();
                                throw cause != null ? cause : ite;
                            } catch (RuntimeException e) {
                                connHandler.gucState.restoreOrReset(snap);
                                connHandler.gucState.markDirty();
                                throw e;
                            }
                        }
                        return method.invoke(real, args);
                    case "addBatch":
                        // Defer observation: track addBatch(String) entries
                        // and the no-arg form, then observe each on
                        // executeBatch success. Pre-fix, we observed at
                        // addBatch time — but executeBatch can fail and the
                        // server may never have applied any of the buffered
                        // SETs. The Statement-path equivalent uses the same
                        // pendingBatch list pattern.
                        if (args == null || args.length == 0) {
                            pendingBoundBatchAdds = true;
                        } else if (args[0] instanceof String) {
                            pendingStringBatch.add((String) args[0]);
                        }
                        return method.invoke(real, args);
                    case "clearBatch":
                        pendingStringBatch.clear();
                        pendingBoundBatchAdds = false;
                        return method.invoke(real, args);
                    case "executeBatch":
                        return handleExecuteBatch();
                    default:
                        return method.invoke(real, args);
                }
            }
        }

        /**
         * Execute the buffered batch with deferred GUC-state observation.
         * On JDBC success: replay each pending addBatch(String) SQL through
         * {@code observeSql}, then observe the bound SQL once if the no-arg
         * {@code addBatch()} was called. On failure: drop both buffers + mark
         * dirty so the next path reconciles via pg_settings.
         */
        private int[] handleExecuteBatch() throws SQLException {
            try {
                int[] counts = real.executeBatch();
                for (String s : pendingStringBatch) {
                    connHandler.gucState.observeSql(s);
                }
                if (pendingBoundBatchAdds) {
                    connHandler.gucState.observeSql(sql);
                }
                pendingStringBatch.clear();
                pendingBoundBatchAdds = false;
                return counts;
            } catch (SQLException | RuntimeException e) {
                pendingStringBatch.clear();
                pendingBoundBatchAdds = false;
                connHandler.gucState.markDirty();
                throw e;
            }
        }
    }
}
