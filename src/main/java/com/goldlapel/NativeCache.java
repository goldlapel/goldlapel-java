package com.goldlapel;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NativeCache {
    static final String DDL_SENTINEL = "__ddl__";

    private static final Pattern TX_START = Pattern.compile("^\\s*(BEGIN|START\\s+TRANSACTION)\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern TX_END = Pattern.compile("^\\s*(COMMIT|ROLLBACK|END)\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern TABLE_PATTERN = Pattern.compile("\\b(?:FROM|JOIN)\\s+(?:ONLY\\s+)?(?:(\\w+)\\.)?(\\w+)", Pattern.CASE_INSENSITIVE);

    private static final Set<String> SQL_KEYWORDS = Set.of(
        "select", "from", "where", "and", "or", "not", "in", "exists",
        "between", "like", "is", "null", "true", "false", "as", "on",
        "left", "right", "inner", "outer", "cross", "full", "natural",
        "group", "order", "having", "limit", "offset", "union", "intersect",
        "except", "all", "distinct", "lateral", "values"
    );

    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> tableIndex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> accessOrder = new ConcurrentHashMap<>();
    private final AtomicLong counter = new AtomicLong(0);
    private final int maxEntries;
    private final boolean enabled;

    private volatile boolean invalidationConnected = false;
    private volatile boolean invalidationStop = false;
    private Thread invalidationThread;
    private Socket invalidationSocket;
    private int invalidationPort;
    private int reconnectAttempt = 0;

    final AtomicLong statsHits = new AtomicLong(0);
    final AtomicLong statsMisses = new AtomicLong(0);
    final AtomicLong statsInvalidations = new AtomicLong(0);

    private static NativeCache instance;

    public NativeCache() {
        String sizeStr = System.getenv("GOLDLAPEL_NATIVE_CACHE_SIZE");
        this.maxEntries = sizeStr != null ? Integer.parseInt(sizeStr) : 32768;
        String enabledStr = System.getenv("GOLDLAPEL_NATIVE_CACHE");
        this.enabled = enabledStr == null || !"false".equalsIgnoreCase(enabledStr);
    }

    public static synchronized NativeCache getInstance() {
        if (instance == null) {
            instance = new NativeCache();
        }
        return instance;
    }

    public static synchronized void reset() {
        if (instance != null) {
            instance.stopInvalidation();
            instance = null;
        }
    }

    public boolean isConnected() { return invalidationConnected; }
    public boolean isEnabled() { return enabled; }
    public int size() { return cache.size(); }

    // --- Cache operations ---

    public CacheEntry get(String sql, Object[] params) {
        if (!enabled || !invalidationConnected) return null;
        String key = makeKey(sql, params);
        if (key == null) return null;
        CacheEntry entry = cache.get(key);
        if (entry != null) {
            accessOrder.put(key, counter.incrementAndGet());
            statsHits.incrementAndGet();
            return entry;
        }
        statsMisses.incrementAndGet();
        return null;
    }

    public void put(String sql, Object[] params, List<Object[]> rows, String[] columns) {
        if (!enabled || !invalidationConnected) return;
        String key = makeKey(sql, params);
        if (key == null) return;
        Set<String> tables = extractTables(sql);
        if (!cache.containsKey(key) && cache.size() >= maxEntries) {
            evictOne();
        }
        cache.put(key, new CacheEntry(rows, columns, tables));
        accessOrder.put(key, counter.incrementAndGet());
        for (String table : tables) {
            tableIndex.computeIfAbsent(table, k -> ConcurrentHashMap.newKeySet()).add(key);
        }
    }

    public void invalidateTable(String table) {
        table = table.toLowerCase();
        Set<String> keys = tableIndex.remove(table);
        if (keys == null) return;
        String finalTable = table;
        for (String key : keys) {
            CacheEntry entry = cache.remove(key);
            accessOrder.remove(key);
            if (entry != null) {
                for (String otherTable : entry.tables) {
                    if (!otherTable.equals(finalTable)) {
                        Set<String> otherKeys = tableIndex.get(otherTable);
                        if (otherKeys != null) {
                            otherKeys.remove(key);
                            if (otherKeys.isEmpty()) tableIndex.remove(otherTable);
                        }
                    }
                }
            }
        }
        statsInvalidations.addAndGet(keys.size());
    }

    public void invalidateAll() {
        long count = cache.size();
        cache.clear();
        tableIndex.clear();
        accessOrder.clear();
        statsInvalidations.addAndGet(count);
    }

    // --- Invalidation ---

    public void connectInvalidation(int port) {
        if (invalidationThread != null && invalidationThread.isAlive()) return;
        this.invalidationPort = port;
        this.invalidationStop = false;
        this.reconnectAttempt = 0;
        invalidationThread = new Thread(this::invalidationLoop);
        invalidationThread.setDaemon(true);
        invalidationThread.setName("goldlapel-invalidation");
        invalidationThread.start();
    }

    public void stopInvalidation() {
        invalidationStop = true;
        if (invalidationSocket != null) {
            try { invalidationSocket.close(); } catch (IOException ignored) {}
        }
        if (invalidationThread != null) {
            try { invalidationThread.join(5000); } catch (InterruptedException ignored) {}
            invalidationThread = null;
        }
        invalidationConnected = false;
    }

    private void invalidationLoop() {
        while (!invalidationStop) {
            try {
                invalidationSocket = new Socket("127.0.0.1", invalidationPort);
                invalidationConnected = true;
                reconnectAttempt = 0;

                BufferedReader reader = new BufferedReader(
                    new InputStreamReader(invalidationSocket.getInputStream())
                );
                invalidationSocket.setSoTimeout(30000);

                while (!invalidationStop) {
                    try {
                        String line = reader.readLine();
                        if (line == null) break;
                        processSignal(line);
                    } catch (java.net.SocketTimeoutException e) {
                        break;
                    }
                }
            } catch (IOException ignored) {
            } finally {
                if (invalidationConnected) {
                    invalidationConnected = false;
                    invalidateAll();
                }
                if (invalidationSocket != null) {
                    try { invalidationSocket.close(); } catch (IOException ignored) {}
                    invalidationSocket = null;
                }
            }

            if (invalidationStop) break;
            int delay = Math.min(1 << reconnectAttempt, 15);
            reconnectAttempt++;
            try { Thread.sleep(delay * 1000L); } catch (InterruptedException e) { break; }
        }
    }

    void processSignal(String line) {
        if (line.startsWith("I:")) {
            String table = line.substring(2).trim();
            if ("*".equals(table)) {
                invalidateAll();
            } else {
                invalidateTable(table);
            }
        }
    }

    // --- SQL parsing ---

    static String makeKey(String sql, Object[] params) {
        if (params == null || params.length == 0) {
            return sql + "\0null";
        }
        return sql + "\0" + Arrays.toString(params);
    }

    static String detectWrite(String sql) {
        String trimmed = sql.trim();
        String[] tokens = trimmed.split("\\s+");
        if (tokens.length == 0) return null;
        String first = tokens[0].toUpperCase();

        switch (first) {
            case "INSERT":
                if (tokens.length < 3 || !"INTO".equalsIgnoreCase(tokens[1])) return null;
                return bareTable(tokens[2]);
            case "UPDATE":
                if (tokens.length < 2) return null;
                return bareTable(tokens[1]);
            case "DELETE":
                if (tokens.length < 3 || !"FROM".equalsIgnoreCase(tokens[1])) return null;
                return bareTable(tokens[2]);
            case "TRUNCATE":
                if (tokens.length < 2) return null;
                if ("TABLE".equalsIgnoreCase(tokens[1])) {
                    if (tokens.length < 3) return null;
                    return bareTable(tokens[2]);
                }
                return bareTable(tokens[1]);
            case "CREATE": case "ALTER": case "DROP":
                return DDL_SENTINEL;
            case "COPY":
                if (tokens.length < 2) return null;
                String raw = tokens[1];
                if (raw.startsWith("(")) return null;
                String tablePart = raw.split("\\(")[0];
                for (int i = 2; i < tokens.length; i++) {
                    String upper = tokens[i].toUpperCase();
                    if ("FROM".equals(upper)) return bareTable(tablePart);
                    if ("TO".equals(upper)) return null;
                }
                return null;
            case "WITH":
                String restUpper = trimmed.substring(tokens[0].length()).toUpperCase();
                for (String token : restUpper.split("\\s+")) {
                    String word = token.replaceFirst("^\\(+", "");
                    if ("INSERT".equals(word) || "UPDATE".equals(word) || "DELETE".equals(word)) {
                        return DDL_SENTINEL;
                    }
                }
                return null;
            default:
                return null;
        }
    }

    static String bareTable(String raw) {
        String table = raw.split("\\(")[0];
        String[] parts = table.split("\\.");
        table = parts[parts.length - 1];
        return table.toLowerCase();
    }

    static Set<String> extractTables(String sql) {
        Set<String> tables = new HashSet<>();
        Matcher matcher = TABLE_PATTERN.matcher(sql);
        while (matcher.find()) {
            String table = matcher.group(2).toLowerCase();
            if (!SQL_KEYWORDS.contains(table)) {
                tables.add(table);
            }
        }
        return tables;
    }

    static boolean isTxStart(String sql) { return TX_START.matcher(sql).find(); }
    static boolean isTxEnd(String sql) { return TX_END.matcher(sql).find(); }

    private void evictOne() {
        String lruKey = null;
        long minCounter = Long.MAX_VALUE;
        for (Map.Entry<String, Long> entry : accessOrder.entrySet()) {
            if (entry.getValue() < minCounter) {
                minCounter = entry.getValue();
                lruKey = entry.getKey();
            }
        }
        if (lruKey == null) return;
        CacheEntry entry = cache.remove(lruKey);
        accessOrder.remove(lruKey);
        if (entry != null) {
            for (String table : entry.tables) {
                Set<String> keys = tableIndex.get(table);
                if (keys != null) {
                    keys.remove(lruKey);
                    if (keys.isEmpty()) tableIndex.remove(table);
                }
            }
        }
    }

    // --- Inner class ---

    public static class CacheEntry {
        public final List<Object[]> rows;
        public final String[] columns;
        public final Set<String> tables;

        public CacheEntry(List<Object[]> rows, String[] columns, Set<String> tables) {
            this.rows = rows;
            this.columns = columns;
            this.tables = tables;
        }
    }
}
