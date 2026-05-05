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

        private void handleWriteInvalidation(String sql) {
            String writeTable = NativeCache.detectWrite(sql);
            if (writeTable != null) {
                if (writeTable.equals(NativeCache.DDL_SENTINEL)) {
                    cache.invalidateAll();
                } else {
                    cache.invalidateTable(writeTable);
                }
            }
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
                default:
                    return method.invoke(real, args);
            }
        }

        private ResultSet handleExecuteQuery(String sql) throws SQLException {
            // Transaction tracking
            if (NativeCache.isTxStart(sql)) {
                connHandler.inTransaction = true;
                return real.executeQuery(sql);
            }
            if (NativeCache.isTxEnd(sql)) {
                connHandler.inTransaction = false;
                return real.executeQuery(sql);
            }

            // Write detection
            if (NativeCache.detectWrite(sql) != null) {
                handleWriteInvalidation(sql);
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
            connHandler.gucState.observeSql(sql);
            handleWriteInvalidation(sql);
            return real.executeUpdate(sql);
        }

        private boolean handleExecute(String sql) throws SQLException {
            if (NativeCache.isTxStart(sql)) {
                connHandler.inTransaction = true;
            } else if (NativeCache.isTxEnd(sql)) {
                connHandler.inTransaction = false;
            }
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

                cache.put(sql, params, rows, columns, stateHash);
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

        private void handleWriteInvalidation(String sql) {
            String writeTable = NativeCache.detectWrite(sql);
            if (writeTable != null) {
                if (writeTable.equals(NativeCache.DDL_SENTINEL)) {
                    cache.invalidateAll();
                } else {
                    cache.invalidateTable(writeTable);
                }
            }
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

            if (NativeCache.detectWrite(sql) != null) {
                handleWriteInvalidation(sql);
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
                cache.put(sql, p, rows, columns, stateHash);
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
}
