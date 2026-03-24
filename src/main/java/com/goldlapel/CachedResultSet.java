package com.goldlapel;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

public class CachedResultSet {

    public static ResultSet create(List<Object[]> rows, String[] columns) {
        return (ResultSet) Proxy.newProxyInstance(
            CachedResultSet.class.getClassLoader(),
            new Class[]{ResultSet.class},
            new Handler(rows, columns)
        );
    }

    private static class Handler implements InvocationHandler {
        private final List<Object[]> rows;
        private final String[] columns;
        private final Map<String, Integer> columnIndex;
        private int cursor = -1;
        private boolean closed = false;
        private boolean wasNull = false;

        Handler(List<Object[]> rows, String[] columns) {
            this.rows = rows;
            this.columns = columns;
            this.columnIndex = new HashMap<>();
            for (int i = 0; i < columns.length; i++) {
                columnIndex.put(columns[i].toLowerCase(), i);
            }
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "next": return next();
                case "close": closed = true; return null;
                case "isClosed": return closed;
                case "wasNull": return wasNull;
                case "getRow": return cursor + 1;

                // Column by index
                case "getObject":
                    if (args[0] instanceof Integer) return getByIndex((int) args[0]);
                    return getByName((String) args[0]);
                case "getString":
                    if (args[0] instanceof Integer) return asString(getByIndex((int) args[0]));
                    return asString(getByName((String) args[0]));
                case "getInt":
                    if (args[0] instanceof Integer) return asInt(getByIndex((int) args[0]));
                    return asInt(getByName((String) args[0]));
                case "getLong":
                    if (args[0] instanceof Integer) return asLong(getByIndex((int) args[0]));
                    return asLong(getByName((String) args[0]));
                case "getDouble":
                    if (args[0] instanceof Integer) return asDouble(getByIndex((int) args[0]));
                    return asDouble(getByName((String) args[0]));
                case "getFloat":
                    if (args[0] instanceof Integer) return asFloat(getByIndex((int) args[0]));
                    return asFloat(getByName((String) args[0]));
                case "getBoolean":
                    if (args[0] instanceof Integer) return asBoolean(getByIndex((int) args[0]));
                    return asBoolean(getByName((String) args[0]));
                case "getShort":
                    if (args[0] instanceof Integer) return asShort(getByIndex((int) args[0]));
                    return asShort(getByName((String) args[0]));
                case "getByte":
                    if (args[0] instanceof Integer) return asByte(getByIndex((int) args[0]));
                    return asByte(getByName((String) args[0]));

                case "getMetaData": return createMetaData();
                case "findColumn": return findColumn((String) args[0]);
                case "isBeforeFirst": return cursor < 0 && !rows.isEmpty();
                case "isAfterLast": return cursor >= rows.size();
                case "isFirst": return cursor == 0;
                case "isLast": return cursor == rows.size() - 1;
                case "beforeFirst": cursor = -1; return null;
                case "afterLast": cursor = rows.size(); return null;
                case "first":
                    if (rows.isEmpty()) return false;
                    cursor = 0; return true;
                case "last":
                    if (rows.isEmpty()) return false;
                    cursor = rows.size() - 1; return true;
                case "absolute":
                    int row = (int) args[0];
                    if (row > 0) cursor = row - 1;
                    else if (row < 0) cursor = rows.size() + row;
                    else cursor = -1;
                    return cursor >= 0 && cursor < rows.size();
                case "relative":
                    cursor += (int) args[0];
                    return cursor >= 0 && cursor < rows.size();

                case "toString": return "CachedResultSet[" + rows.size() + " rows]";
                case "hashCode": return System.identityHashCode(proxy);
                case "equals": return proxy == args[0];

                default:
                    // Return sensible defaults for unimplemented methods
                    Class<?> returnType = method.getReturnType();
                    if (returnType == boolean.class) return false;
                    if (returnType == int.class) return 0;
                    if (returnType == long.class) return 0L;
                    if (returnType == float.class) return 0.0f;
                    if (returnType == double.class) return 0.0;
                    if (returnType == short.class) return (short) 0;
                    if (returnType == byte.class) return (byte) 0;
                    return null;
            }
        }

        private boolean next() {
            cursor++;
            return cursor < rows.size();
        }

        private Object getByIndex(int idx) {
            Object val = rows.get(cursor)[idx - 1];
            wasNull = (val == null);
            return val;
        }

        private Object getByName(String name) {
            Integer idx = columnIndex.get(name.toLowerCase());
            if (idx == null) throw new RuntimeException("Column not found: " + name);
            Object val = rows.get(cursor)[idx];
            wasNull = (val == null);
            return val;
        }

        private int findColumn(String name) {
            Integer idx = columnIndex.get(name.toLowerCase());
            if (idx == null) throw new RuntimeException("Column not found: " + name);
            return idx + 1;
        }

        private String asString(Object val) { return val == null ? null : val.toString(); }
        private int asInt(Object val) {
            if (val == null) return 0;
            if (val instanceof Number) return ((Number) val).intValue();
            return Integer.parseInt(val.toString());
        }
        private long asLong(Object val) {
            if (val == null) return 0L;
            if (val instanceof Number) return ((Number) val).longValue();
            return Long.parseLong(val.toString());
        }
        private double asDouble(Object val) {
            if (val == null) return 0.0;
            if (val instanceof Number) return ((Number) val).doubleValue();
            return Double.parseDouble(val.toString());
        }
        private float asFloat(Object val) {
            if (val == null) return 0.0f;
            if (val instanceof Number) return ((Number) val).floatValue();
            return Float.parseFloat(val.toString());
        }
        private boolean asBoolean(Object val) {
            if (val == null) return false;
            if (val instanceof Boolean) return (Boolean) val;
            return Boolean.parseBoolean(val.toString());
        }
        private short asShort(Object val) {
            if (val == null) return 0;
            if (val instanceof Number) return ((Number) val).shortValue();
            return Short.parseShort(val.toString());
        }
        private byte asByte(Object val) {
            if (val == null) return 0;
            if (val instanceof Number) return ((Number) val).byteValue();
            return Byte.parseByte(val.toString());
        }

        private ResultSetMetaData createMetaData() {
            return (ResultSetMetaData) Proxy.newProxyInstance(
                CachedResultSet.class.getClassLoader(),
                new Class[]{ResultSetMetaData.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getColumnCount": return columns.length;
                        case "getColumnName":
                        case "getColumnLabel":
                            return columns[(int) args[0] - 1];
                        case "toString": return "CachedResultSetMetaData[" + columns.length + " cols]";
                        default:
                            Class<?> rt = method.getReturnType();
                            if (rt == int.class) return 0;
                            if (rt == boolean.class) return false;
                            if (rt == String.class) return "";
                            return null;
                    }
                }
            );
        }
    }
}
