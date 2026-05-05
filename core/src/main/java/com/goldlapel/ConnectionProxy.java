package com.goldlapel;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;

public class ConnectionProxy {

    public static Connection wrap(Connection real, NativeCache cache) {
        return (Connection) Proxy.newProxyInstance(
            ConnectionProxy.class.getClassLoader(),
            new Class[]{Connection.class},
            new ConnectionHandler(real, cache)
        );
    }

    private static class ConnectionHandler implements InvocationHandler {
        private final Connection real;
        private final NativeCache cache;
        // Per-connection unsafe-GUC state. SET / RESET observed on every query
        // mutates this; the hash is folded into the native-cache key so two
        // connections that have set different unsafe GUCs never share a slot
        // (custom-GUC-driven RLS would otherwise leak across users). Mirrors
        // the proxy-side ConnectionGucState in src/guc_state.rs.
        final GucState gucState = new GucState();
        boolean inTransaction = false;

        ConnectionHandler(Connection real, NativeCache cache) {
            this.real = real;
            this.cache = cache;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
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
                return real.executeQuery(sql);
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

            // In transaction: bypass cache
            if (connHandler.inTransaction) {
                return real.executeQuery(sql);
            }

            // Check native cache
            long stateHash = connHandler.gucState.hash();
            NativeCache.CacheEntry entry = cache.get(sql, null, stateHash);
            if (entry != null) {
                return CachedResultSet.create(entry.rows, entry.columns);
            }

            // Cache miss
            ResultSet rs = real.executeQuery(sql);
            return cacheAndReturn(sql, null, rs, stateHash);
        }

        private int handleExecuteUpdate(String sql) throws SQLException {
            // Same tx-state walk as handleExecuteQuery — executeUpdate is the
            // path some drivers route DDL/DML through, and a multi-statement
            // "BEGIN; UPDATE...; COMMIT" body must leave the wrapper-side
            // tx flag matching the server's post-COMMIT state.
            connHandler.inTransaction = NativeCache.updateTxState(connHandler.inTransaction, sql);
            connHandler.gucState.observeSql(sql);
            handleWriteInvalidation(sql);
            return real.executeUpdate(sql);
        }

        private boolean handleExecute(String sql) throws SQLException {
            // Multi-statement-aware tx-state walk; see handleExecuteQuery for
            // the BEGIN-then-COMMIT-buried-in-the-same-body bug it fixes.
            connHandler.inTransaction = NativeCache.updateTxState(connHandler.inTransaction, sql);
            connHandler.gucState.observeSql(sql);
            handleWriteInvalidation(sql);
            return real.execute(sql);
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

            // Track parameter setting
            if (name.startsWith("set") && args != null && args.length >= 2 && args[0] instanceof Integer) {
                params.put((Integer) args[0], args[1]);
                return method.invoke(real, args);
            }

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
                return real.executeQuery();
            }

            // SET / RESET observation. PreparedStatement is unusual for SET
            // (parameters typically aren't permitted in `SET name = $1`), but
            // the bookkeeping is cheap and the symmetry with Statement keeps
            // the cache key shape consistent regardless of which path the
            // SET arrived on.
            connHandler.gucState.observeSql(sql);

            if (connHandler.inTransaction) {
                return real.executeQuery();
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
                return CachedResultSet.create(rows, columns);
            } catch (Exception e) {
                return rs;
            }
        }

        private int handlePreparedUpdate() throws SQLException {
            connHandler.gucState.observeSql(sql);
            handleWriteInvalidation(sql);
            return real.executeUpdate();
        }

        private boolean handlePreparedExecute() throws SQLException {
            connHandler.gucState.observeSql(sql);
            handleWriteInvalidation(sql);
            return real.execute();
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

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
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
