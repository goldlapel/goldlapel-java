package com.goldlapel;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.function.BiConsumer;

public class GoldLapel {

    static final int DEFAULT_PORT = 7932;
    static final int DEFAULT_DASHBOARD_PORT = 7933;
    static final long STARTUP_TIMEOUT_MS = 10000;
    static final long STARTUP_POLL_INTERVAL_MS = 50;

    private static final Set<String> VALID_CONFIG_KEYS;
    private static final Set<String> BOOLEAN_KEYS;
    private static final Set<String> LIST_KEYS;

    static {
        Set<String> keys = new HashSet<>();
        Collections.addAll(keys,
            "mode", "minPatternCount", "refreshIntervalSecs", "patternTtlSecs",
            "maxTablesPerView", "maxColumnsPerView", "deepPaginationThreshold",
            "reportIntervalSecs", "resultCacheSize", "batchCacheSize",
            "batchCacheTtlSecs", "poolSize", "poolTimeoutSecs",
            "poolMode", "mgmtIdleTimeout", "fallback", "readAfterWriteSecs",
            "n1Threshold", "n1WindowMs", "n1CrossThreshold",
            "tlsCert", "tlsKey", "tlsClientCa", "config", "dashboardPort",
            "disableMatviews", "disableConsolidation", "disableBtreeIndexes",
            "disableTrigramIndexes", "disableExpressionIndexes",
            "disablePartialIndexes", "disableRewrite", "disablePreparedCache",
            "disableResultCache", "disablePool",
            "disableN1", "disableN1CrossConnection", "disableShadowMode",
            "enableCoalescing", "replica", "excludeTables",
            "invalidationPort"
        );
        VALID_CONFIG_KEYS = Collections.unmodifiableSet(keys);

        Set<String> bools = new HashSet<>();
        Collections.addAll(bools,
            "disableMatviews", "disableConsolidation", "disableBtreeIndexes",
            "disableTrigramIndexes", "disableExpressionIndexes",
            "disablePartialIndexes", "disableRewrite", "disablePreparedCache",
            "disableResultCache", "disablePool",
            "disableN1", "disableN1CrossConnection", "disableShadowMode",
            "enableCoalescing"
        );
        BOOLEAN_KEYS = Collections.unmodifiableSet(bools);

        Set<String> lists = new HashSet<>();
        Collections.addAll(lists, "replica", "excludeTables");
        LIST_KEYS = Collections.unmodifiableSet(lists);
    }

    private final String upstream;
    private final int port;
    private final int dashboardPort;
    private final Map<String, Object> config;
    private final List<String> extraArgs;
    private final String client;
    private Process process;
    private String proxyUrl;
    Connection wrappedConn;
    private Connection instanceConn;

    public GoldLapel(String upstream) {
        this(upstream, new Options());
    }

    public GoldLapel(String upstream, Options options) {
        this.upstream = upstream;
        this.port = options.port != null ? options.port : DEFAULT_PORT;
        this.dashboardPort = options.config != null && options.config.containsKey("dashboardPort")
            ? ((Number) options.config.get("dashboardPort")).intValue()
            : DEFAULT_DASHBOARD_PORT;
        this.config = options.config;
        this.extraArgs = options.extraArgs != null ? options.extraArgs : new ArrayList<>();
        this.client = options.client != null ? options.client : "java";
        this.process = null;
        this.proxyUrl = null;
    }

    public String startProxy() {
        if (process != null && process.isAlive()) {
            return proxyUrl;
        }

        String binary = findBinary();
        List<String> cmd = new ArrayList<>();
        cmd.add(binary);
        cmd.add("--upstream");
        cmd.add(upstream);
        cmd.add("--proxy-port");
        cmd.add(String.valueOf(port));
        cmd.addAll(configToArgs(config));
        cmd.addAll(extraArgs);

        try {
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.environment().putIfAbsent("GOLDLAPEL_CLIENT", client);
            pb.redirectInput(ProcessBuilder.Redirect.PIPE);
            pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
            pb.redirectError(ProcessBuilder.Redirect.PIPE);
            process = pb.start();
            process.getOutputStream().close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start Gold Lapel process", e);
        }

        // Drain stderr on a daemon thread to prevent pipe-buffer deadlock
        StringBuilder stderrBuf = new StringBuilder();
        Thread stderrDrain = new Thread(() -> {
            try {
                InputStream err = process.getErrorStream();
                byte[] buf = new byte[1024];
                int n;
                while ((n = err.read(buf)) != -1) {
                    stderrBuf.append(new String(buf, 0, n));
                }
            } catch (IOException ignored) {}
        });
        stderrDrain.setDaemon(true);
        stderrDrain.start();

        // Poll for port readiness, short-circuiting if the process exits early
        long deadline = System.nanoTime() + STARTUP_TIMEOUT_MS * 1_000_000L;
        boolean ready = false;
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) break;
            if (waitForPort("127.0.0.1", port, 500)) {
                ready = true;
                break;
            }
        }

        if (!ready) {
            process.destroyForcibly();
            try { process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
            try { stderrDrain.join(2000); } catch (InterruptedException ignored) {}
            throw new RuntimeException(
                "Gold Lapel failed to start on port " + port +
                " within " + (STARTUP_TIMEOUT_MS / 1000) + "s.\nstderr: " + stderrBuf
            );
        }

        proxyUrl = makeProxyUrl(upstream, port);

        try {
            String jdbcUrl = proxyUrl;
            if (jdbcUrl.startsWith("postgres://")) {
                jdbcUrl = "jdbc:postgresql://" + jdbcUrl.substring("postgres://".length());
            } else if (jdbcUrl.startsWith("postgresql://")) {
                jdbcUrl = "jdbc:postgresql://" + jdbcUrl.substring("postgresql://".length());
            }
            instanceConn = DriverManager.getConnection(jdbcUrl);
        } catch (SQLException e) {
            // Connection creation is best-effort; callers can still use proxyUrl directly
            instanceConn = null;
        }

        if (dashboardPort > 0) {
            System.out.println("goldlapel → :" + port + " (proxy) | http://127.0.0.1:" + dashboardPort + " (dashboard)");
        } else {
            System.out.println("goldlapel → :" + port + " (proxy)");
        }

        return proxyUrl;
    }

    public void stopProxy() {
        if (instanceConn != null) {
            try { instanceConn.close(); } catch (SQLException ignored) {}
            instanceConn = null;
        }
        Process proc = process;
        process = null;
        proxyUrl = null;
        if (proc != null && proc.isAlive()) {
            proc.destroy();
            try {
                if (!proc.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    proc.destroyForcibly();
                    proc.waitFor();
                }
            } catch (InterruptedException e) {
                proc.destroyForcibly();
                Thread.currentThread().interrupt();
            }
        }
    }

    public String getUrl() {
        return proxyUrl;
    }

    public int getPort() {
        return port;
    }

    public String getDashboardUrl() {
        if (dashboardPort > 0 && process != null && process.isAlive()) {
            return "http://127.0.0.1:" + dashboardPort;
        }
        return null;
    }

    public boolean isRunning() {
        return process != null && process.isAlive();
    }

    public Connection connection() {
        if (instanceConn == null) {
            throw new IllegalStateException(
                "No connection available. Call startProxy() first, and ensure a PostgreSQL JDBC driver is on the classpath.");
        }
        return instanceConn;
    }

    private Connection requireConn() {
        return connection();
    }

    // ── Instance methods (delegate to Utils with stored connection) ─────

    // Document store

    public Map<String, Object> docInsert(String collection, String documentJson) throws SQLException {
        return Utils.docInsert(requireConn(), collection, documentJson);
    }

    public List<Map<String, Object>> docInsertMany(String collection, List<String> documents) throws SQLException {
        return Utils.docInsertMany(requireConn(), collection, documents);
    }

    public List<Map<String, Object>> docFind(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip) throws SQLException {
        return Utils.docFind(requireConn(), collection, filterJson, sortJson, limit, skip);
    }

    public Map<String, Object> docFindOne(String collection, String filterJson) throws SQLException {
        return Utils.docFindOne(requireConn(), collection, filterJson);
    }

    public Iterator<Map<String, Object>> docFindCursor(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, int batchSize) throws SQLException {
        return Utils.docFindCursor(requireConn(), collection, filterJson, sortJson, limit, skip, batchSize);
    }

    public int docUpdate(String collection, String filterJson, String updateJson) throws SQLException {
        return Utils.docUpdate(requireConn(), collection, filterJson, updateJson);
    }

    public int docUpdateOne(String collection, String filterJson, String updateJson) throws SQLException {
        return Utils.docUpdateOne(requireConn(), collection, filterJson, updateJson);
    }

    public int docDelete(String collection, String filterJson) throws SQLException {
        return Utils.docDelete(requireConn(), collection, filterJson);
    }

    public int docDeleteOne(String collection, String filterJson) throws SQLException {
        return Utils.docDeleteOne(requireConn(), collection, filterJson);
    }

    public Map<String, Object> docFindOneAndUpdate(String collection, String filterJson,
            String updateJson) throws SQLException {
        return Utils.docFindOneAndUpdate(requireConn(), collection, filterJson, updateJson);
    }

    public Map<String, Object> docFindOneAndDelete(String collection, String filterJson) throws SQLException {
        return Utils.docFindOneAndDelete(requireConn(), collection, filterJson);
    }

    public List<String> docDistinct(String collection, String field, String filterJson) throws SQLException {
        return Utils.docDistinct(requireConn(), collection, field, filterJson);
    }

    public long docCount(String collection, String filterJson) throws SQLException {
        return Utils.docCount(requireConn(), collection, filterJson);
    }

    public void docCreateIndex(String collection, List<String> keys) throws SQLException {
        Utils.docCreateIndex(requireConn(), collection, keys);
    }

    public List<Map<String, Object>> docAggregate(String collection, String pipelineJson) throws SQLException {
        return Utils.docAggregate(requireConn(), collection, pipelineJson);
    }

    public Thread docWatch(String collection, BiConsumer<String, String> callback) throws SQLException {
        return Utils.docWatch(requireConn(), collection, callback);
    }

    public void docUnwatch(String collection) throws SQLException {
        Utils.docUnwatch(requireConn(), collection);
    }

    public void docCreateTtlIndex(String collection, int expireAfterSeconds) throws SQLException {
        Utils.docCreateTtlIndex(requireConn(), collection, expireAfterSeconds);
    }

    public void docCreateTtlIndex(String collection, int expireAfterSeconds, String field) throws SQLException {
        Utils.docCreateTtlIndex(requireConn(), collection, expireAfterSeconds, field);
    }

    public void docRemoveTtlIndex(String collection) throws SQLException {
        Utils.docRemoveTtlIndex(requireConn(), collection);
    }

    public void docCreateCapped(String collection, int maxDocuments) throws SQLException {
        Utils.docCreateCapped(requireConn(), collection, maxDocuments);
    }

    public void docRemoveCap(String collection) throws SQLException {
        Utils.docRemoveCap(requireConn(), collection);
    }

    // Search

    public List<Map<String, Object>> search(String table, String column, String query,
            int limit, String lang, boolean highlight) throws SQLException {
        return Utils.search(requireConn(), table, column, query, limit, lang, highlight);
    }

    public List<Map<String, Object>> search(String table, String[] columns, String query,
            int limit, String lang, boolean highlight) throws SQLException {
        return Utils.search(requireConn(), table, columns, query, limit, lang, highlight);
    }

    public List<Map<String, Object>> searchFuzzy(String table, String column, String query,
            int limit, double threshold) throws SQLException {
        return Utils.searchFuzzy(requireConn(), table, column, query, limit, threshold);
    }

    public List<Map<String, Object>> searchPhonetic(String table, String column, String query,
            int limit) throws SQLException {
        return Utils.searchPhonetic(requireConn(), table, column, query, limit);
    }

    public List<Map<String, Object>> similar(String table, String column, double[] vector,
            int limit) throws SQLException {
        return Utils.similar(requireConn(), table, column, vector, limit);
    }

    public List<Map<String, Object>> suggest(String table, String column, String prefix,
            int limit) throws SQLException {
        return Utils.suggest(requireConn(), table, column, prefix, limit);
    }

    public List<Map<String, Object>> facets(String table, String column, int limit) throws SQLException {
        return Utils.facets(requireConn(), table, column, limit);
    }

    public List<Map<String, Object>> facets(String table, String column, int limit,
            String query, String queryColumn, String lang) throws SQLException {
        return Utils.facets(requireConn(), table, column, limit, query, queryColumn, lang);
    }

    public List<Map<String, Object>> facets(String table, String column, int limit,
            String query, String[] queryColumns, String lang) throws SQLException {
        return Utils.facets(requireConn(), table, column, limit, query, queryColumns, lang);
    }

    public List<Map<String, Object>> aggregate(String table, String column, String func) throws SQLException {
        return Utils.aggregate(requireConn(), table, column, func);
    }

    public List<Map<String, Object>> aggregate(String table, String column, String func,
            String groupBy, int limit) throws SQLException {
        return Utils.aggregate(requireConn(), table, column, func, groupBy, limit);
    }

    public void createSearchConfig(String name) throws SQLException {
        Utils.createSearchConfig(requireConn(), name);
    }

    public void createSearchConfig(String name, String copyFrom) throws SQLException {
        Utils.createSearchConfig(requireConn(), name, copyFrom);
    }

    // PubSub and queues

    public void publish(String channel, String message) throws SQLException {
        Utils.publish(requireConn(), channel, message);
    }

    public Thread subscribe(String channel, BiConsumer<String, String> callback) throws SQLException {
        return Utils.subscribe(requireConn(), channel, callback);
    }

    public Thread subscribe(String channel, BiConsumer<String, String> callback,
            boolean blocking) throws SQLException {
        return Utils.subscribe(requireConn(), channel, callback, blocking);
    }

    public void enqueue(String queueTable, String payloadJson) throws SQLException {
        Utils.enqueue(requireConn(), queueTable, payloadJson);
    }

    public String dequeue(String queueTable) throws SQLException {
        return Utils.dequeue(requireConn(), queueTable);
    }

    // Counters

    public long incr(String table, String key, long amount) throws SQLException {
        return Utils.incr(requireConn(), table, key, amount);
    }

    public long getCounter(String table, String key) throws SQLException {
        return Utils.getCounter(requireConn(), table, key);
    }

    // Hashes

    public void hset(String table, String key, String field, String valueJson) throws SQLException {
        Utils.hset(requireConn(), table, key, field, valueJson);
    }

    public String hget(String table, String key, String field) throws SQLException {
        return Utils.hget(requireConn(), table, key, field);
    }

    public String hgetall(String table, String key) throws SQLException {
        return Utils.hgetall(requireConn(), table, key);
    }

    public boolean hdel(String table, String key, String field) throws SQLException {
        return Utils.hdel(requireConn(), table, key, field);
    }

    // Sorted sets

    public void zadd(String table, String member, double score) throws SQLException {
        Utils.zadd(requireConn(), table, member, score);
    }

    public double zincrby(String table, String member, double amount) throws SQLException {
        return Utils.zincrby(requireConn(), table, member, amount);
    }

    public List<Map.Entry<String, Double>> zrange(String table, int start, int stop,
            boolean desc) throws SQLException {
        return Utils.zrange(requireConn(), table, start, stop, desc);
    }

    public Long zrank(String table, String member, boolean desc) throws SQLException {
        return Utils.zrank(requireConn(), table, member, desc);
    }

    public Double zscore(String table, String member) throws SQLException {
        return Utils.zscore(requireConn(), table, member);
    }

    public boolean zrem(String table, String member) throws SQLException {
        return Utils.zrem(requireConn(), table, member);
    }

    // Geo

    public List<Map<String, Object>> georadius(String table, String geomColumn, double lon,
            double lat, double radiusMeters, int limit) throws SQLException {
        return Utils.georadius(requireConn(), table, geomColumn, lon, lat, radiusMeters, limit);
    }

    public void geoadd(String table, String nameColumn, String geomColumn, String name,
            double lon, double lat) throws SQLException {
        Utils.geoadd(requireConn(), table, nameColumn, geomColumn, name, lon, lat);
    }

    public Double geodist(String table, String geomColumn, String nameColumn,
            String nameA, String nameB) throws SQLException {
        return Utils.geodist(requireConn(), table, geomColumn, nameColumn, nameA, nameB);
    }

    // Misc

    public long countDistinct(String table, String column) throws SQLException {
        return Utils.countDistinct(requireConn(), table, column);
    }

    public String script(String luaCode, String... args) throws SQLException {
        return Utils.script(requireConn(), luaCode, args);
    }

    // Streams

    public long streamAdd(String stream, String payload) throws SQLException {
        return Utils.streamAdd(requireConn(), stream, payload);
    }

    public void streamCreateGroup(String stream, String group) throws SQLException {
        Utils.streamCreateGroup(requireConn(), stream, group);
    }

    public List<Map<String, Object>> streamRead(String stream, String group,
            String consumer, int count) throws SQLException {
        return Utils.streamRead(requireConn(), stream, group, consumer, count);
    }

    public boolean streamAck(String stream, String group, long messageId) throws SQLException {
        return Utils.streamAck(requireConn(), stream, group, messageId);
    }

    public List<Map<String, Object>> streamClaim(String stream, String group,
            String consumer, long minIdleMs) throws SQLException {
        return Utils.streamClaim(requireConn(), stream, group, consumer, minIdleMs);
    }

    // Percolator

    public void percolateAdd(String name, String queryId, String query) throws SQLException {
        Utils.percolateAdd(requireConn(), name, queryId, query);
    }

    public void percolateAdd(String name, String queryId, String query,
            String lang, String metadataJson) throws SQLException {
        Utils.percolateAdd(requireConn(), name, queryId, query, lang, metadataJson);
    }

    public List<Map<String, Object>> percolate(String name, String text) throws SQLException {
        return Utils.percolate(requireConn(), name, text);
    }

    public List<Map<String, Object>> percolate(String name, String text,
            int limit, String lang) throws SQLException {
        return Utils.percolate(requireConn(), name, text, limit, lang);
    }

    public boolean percolateDelete(String name, String queryId) throws SQLException {
        return Utils.percolateDelete(requireConn(), name, queryId);
    }

    // Analysis

    public List<Map<String, Object>> analyze(String text) throws SQLException {
        return Utils.analyze(requireConn(), text);
    }

    public List<Map<String, Object>> analyze(String text, String lang) throws SQLException {
        return Utils.analyze(requireConn(), text, lang);
    }

    public Map<String, Object> explainScore(String table, String column, String query,
            String idColumn, Object idValue) throws SQLException {
        return Utils.explainScore(requireConn(), table, column, query, idColumn, idValue);
    }

    public Map<String, Object> explainScore(String table, String column, String query,
            String idColumn, Object idValue, String lang) throws SQLException {
        return Utils.explainScore(requireConn(), table, column, query, idColumn, idValue, lang);
    }

    public static Set<String> configKeys() {
        return Collections.unmodifiableSet(VALID_CONFIG_KEYS);
    }

    // ── Config ─────────────────────────────────────────────

    static String camelToKebab(String key) {
        StringBuilder sb = new StringBuilder();
        for (char c : key.toCharArray()) {
            if (Character.isUpperCase(c)) {
                sb.append('-').append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    static List<String> configToArgs(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> args = new ArrayList<>();

        for (Map.Entry<String, Object> entry : config.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (!VALID_CONFIG_KEYS.contains(key)) {
                throw new IllegalArgumentException("Unknown config key: " + key);
            }

            String flag = "--" + camelToKebab(key);

            if (BOOLEAN_KEYS.contains(key)) {
                if (!(value instanceof Boolean)) {
                    throw new IllegalArgumentException(
                        "Config key '" + key + "' must be a Boolean, got " +
                        value.getClass().getSimpleName()
                    );
                }
                if ((Boolean) value) {
                    args.add(flag);
                }
            } else if (LIST_KEYS.contains(key)) {
                if (!(value instanceof List)) {
                    throw new IllegalArgumentException(
                        "Config key '" + key + "' must be a List, got " +
                        value.getClass().getSimpleName()
                    );
                }
                List<?> items = (List<?>) value;
                for (Object item : items) {
                    args.add(flag);
                    args.add(item.toString());
                }
            } else {
                args.add(flag);
                args.add(value.toString());
            }
        }

        return args;
    }

    // ── Options ────────────────────────────────────────────

    public static class Options {
        Integer port;
        Map<String, Object> config;
        List<String> extraArgs;
        String client;

        public Options port(int port) {
            this.port = port;
            return this;
        }

        public Options config(Map<String, Object> config) {
            this.config = config;
            return this;
        }

        public Map<String, Object> config() {
            return config;
        }

        public Options extraArgs(String... args) {
            this.extraArgs = Arrays.asList(args);
            return this;
        }

        public Options client(String client) {
            this.client = client;
            return this;
        }
    }

    // ── Module-level singleton ─────────────────────────────

    private static GoldLapel instance;
    private static boolean cleanupRegistered = false;

    public static synchronized Connection start(String upstream) {
        return start(upstream, new Options());
    }

    public static synchronized Connection start(String upstream, Options options) {
        if (instance != null && instance.isRunning()) {
            if (!instance.upstream.equals(upstream)) {
                throw new RuntimeException(
                    "Gold Lapel is already running for a different upstream. " +
                    "Call GoldLapel.stop() before starting with a new upstream."
                );
            }
            Connection cached = instance.wrappedConn;
            if (cached != null) return cached;
            Connection wrapped = tryWrapConnection(instance);
            if (wrapped == null) {
                throw new RuntimeException(
                    "No PostgreSQL JDBC driver found. Add org.postgresql:postgresql to your dependencies, " +
                    "or use GoldLapel.startUrl() to get the proxy URL and connect manually."
                );
            }
            return wrapped;
        }
        instance = new GoldLapel(upstream, options);
        if (!cleanupRegistered) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (instance != null) {
                    instance.stopProxy();
                    instance = null;
                }
            }));
            cleanupRegistered = true;
        }
        instance.startProxy();
        Connection wrapped = tryWrapConnection(instance);
        if (wrapped == null) {
            throw new RuntimeException(
                "No PostgreSQL JDBC driver found. Add org.postgresql:postgresql to your dependencies, " +
                "or use GoldLapel.startUrl() to get the proxy URL and connect manually."
            );
        }
        return wrapped;
    }

    public static synchronized String startUrl(String upstream) {
        return startUrl(upstream, new Options());
    }

    public static synchronized String startUrl(String upstream, Options options) {
        if (instance != null && instance.isRunning()) {
            if (!instance.upstream.equals(upstream)) {
                throw new RuntimeException(
                    "Gold Lapel is already running for a different upstream. " +
                    "Call GoldLapel.stop() before starting with a new upstream."
                );
            }
            return instance.getUrl();
        }
        instance = new GoldLapel(upstream, options);
        if (!cleanupRegistered) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (instance != null) {
                    instance.stopProxy();
                    instance = null;
                }
            }));
            cleanupRegistered = true;
        }
        return instance.startProxy();
    }

    private static Connection tryWrapConnection(GoldLapel inst) {
        try {
            Class.forName("org.postgresql.Driver");
            String url = inst.getUrl();
            // JDBC requires jdbc:postgresql:// prefix
            if (url.startsWith("postgres://")) {
                url = "jdbc:postgresql://" + url.substring("postgres://".length());
            } else if (url.startsWith("postgresql://")) {
                url = "jdbc:postgresql://" + url.substring("postgresql://".length());
            }
            Connection conn = DriverManager.getConnection(url);
            NativeCache cache = NativeCache.getInstance();
            int invPort = inst.port + 2;
            if (inst.config != null) {
                Object p = inst.config.get("invalidationPort");
                if (p != null) invPort = Integer.parseInt(p.toString());
            }
            cache.connectInvalidation(invPort);
            Connection wrapped = ConnectionProxy.wrap(conn, cache);
            inst.wrappedConn = wrapped;
            return wrapped;
        } catch (Exception e) {
            return null;
        }
    }

    public static synchronized void stop() {
        if (instance != null) {
            instance.stopProxy();
            instance = null;
        }
        NativeCache.reset();
    }

    public static String proxyUrl() {
        return instance != null ? instance.getUrl() : null;
    }

    public static String dashboardUrl() {
        return instance != null ? instance.getDashboardUrl() : null;
    }

    // ── Internal methods ───────────────────────────────────

    private static final Pattern WITH_PORT =
        Pattern.compile("^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+):(\\d+)(.*)$");

    private static final Pattern NO_PORT =
        Pattern.compile("^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+)(.*)$");

    static String findBinary() {
        // 1. Explicit override via env var
        String envPath = System.getenv("GOLDLAPEL_BINARY");
        if (envPath != null && !envPath.isEmpty()) {
            File f = new File(envPath);
            if (f.isFile()) return envPath;
            throw new RuntimeException(
                "GOLDLAPEL_BINARY points to " + envPath + " but file not found"
            );
        }

        // 2. Bundled binary (extracted from JAR resources)
        String extracted = extractBinary();
        if (extracted != null) return extracted;

        // 3. On PATH
        String onPath = findOnPath("goldlapel");
        if (onPath != null) return onPath;

        throw new RuntimeException(
            "Gold Lapel binary not found. Set GOLDLAPEL_BINARY env var, " +
            "bundle the binary in the JAR, or ensure 'goldlapel' is on PATH."
        );
    }

    static String makeProxyUrl(String upstream, int port) {
        // Build a proxy URL: replace host with localhost and set the proxy port.
        // Uses regex instead of java.net.URI to avoid decoding percent-encoded
        // characters in passwords (e.g. %40 for @), which would corrupt the URL.

        // pg URL with explicit port: scheme://[userinfo@]host:PORT[/path][?query]
        Matcher m = WITH_PORT.matcher(upstream);
        if (m.matches()) {
            return m.group(1) + "localhost:" + port + m.group(4);
        }

        // pg URL without port: scheme://[userinfo@]host[/path][?query]
        m = NO_PORT.matcher(upstream);
        if (m.matches()) {
            return m.group(1) + "localhost:" + port + m.group(3);
        }

        // bare host:port (only if not a URL — guard against splitting on scheme colons)
        if (!upstream.contains("://") && upstream.contains(":")) {
            return "localhost:" + port;
        }

        // bare host
        return "localhost:" + port;
    }

    static boolean waitForPort(String host, int port, long timeoutMs) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            try (Socket sock = new Socket()) {
                sock.connect(new java.net.InetSocketAddress(host, port), 500);
                return true;
            } catch (IOException e) {
                try {
                    Thread.sleep(STARTUP_POLL_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return false;
    }

    private static Path extractedBinaryPath;

    static String extractBinary() {
        // Return cached path if already extracted and still present on disk
        if (extractedBinaryPath != null && Files.isRegularFile(extractedBinaryPath)) {
            return extractedBinaryPath.toString();
        }

        String os = System.getProperty("os.name", "").toLowerCase();
        String arch = System.getProperty("os.arch", "").toLowerCase();

        String archName;
        if (arch.equals("amd64") || arch.equals("x86_64")) {
            archName = "x86_64";
        } else if (arch.equals("aarch64") || arch.equals("arm64")) {
            archName = "aarch64";
        } else {
            archName = arch;
        }

        String osName;
        boolean isWindows = false;
        if (os.contains("linux")) {
            osName = "linux";
        } else if (os.contains("mac") || os.contains("darwin")) {
            osName = "darwin";
        } else if (os.contains("windows")) {
            osName = "windows";
            isWindows = true;
        } else {
            osName = os.replaceAll("\\s+", "-");
        }

        String resourceName = "bin/goldlapel-" + osName + "-" + archName;
        if (osName.equals("linux") && isMusl(archName)) resourceName += "-musl";
        if (isWindows) resourceName += ".exe";
        InputStream in = GoldLapel.class.getClassLoader().getResourceAsStream(resourceName);
        if (in == null) return null;

        try (in) {
            // Read the full resource into memory so we can hash it
            byte[] bytes = in.readAllBytes();

            // Compute SHA-256 content hash (first 16 hex chars)
            String hash;
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                byte[] digest = md.digest(bytes);
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 8; i++) {
                    sb.append(String.format("%02x", digest[i]));
                }
                hash = sb.toString();
            } catch (java.security.NoSuchAlgorithmException e) {
                // SHA-256 is guaranteed by the JVM spec, but handle gracefully
                hash = String.valueOf(bytes.length);
            }

            // Build a deterministic path: /tmp/goldlapel-{hash}-{os}-{arch}[.exe]
            String suffix = isWindows ? ".exe" : "";
            String fileName = "goldlapel-" + hash + "-" + osName + "-" + archName + suffix;
            Path target = Paths.get(System.getProperty("java.io.tmpdir"), fileName);

            // If the hashed file already exists and is executable, reuse it
            if (Files.isRegularFile(target) && target.toFile().canExecute()) {
                extractedBinaryPath = target;
                return target.toString();
            }

            // Extract to a temp file in the same directory, then atomic-rename
            Path tmp = Files.createTempFile(target.getParent(), "goldlapel-", suffix);
            try {
                Files.write(tmp, bytes);
            } catch (IOException e) {
                Files.deleteIfExists(tmp);
                return null;
            }

            try {
                Files.setPosixFilePermissions(tmp, PosixFilePermissions.fromString("rwxr-xr-x"));
            } catch (UnsupportedOperationException e) {
                tmp.toFile().setExecutable(true);
            }

            // Atomic move to the deterministic path (race-safe across processes)
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                // Atomic move may fail across filesystems; fall back to copy
                try {
                    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e2) {
                    // Another process may have beaten us — if target exists, use it
                    Files.deleteIfExists(tmp);
                    if (Files.isRegularFile(target) && target.toFile().canExecute()) {
                        extractedBinaryPath = target;
                        return target.toString();
                    }
                    return null;
                }
            }

            extractedBinaryPath = target;
            return target.toString();
        } catch (IOException e) {
            return null;
        }
    }

    static boolean isMusl(String arch) {
        return new File("/lib/ld-musl-" + arch + ".so.1").exists();
    }

    static String findOnPath(String name) {
        String pathEnv = System.getenv("PATH");
        if (pathEnv == null) return null;
        boolean isWindows = System.getProperty("os.name", "").toLowerCase().contains("windows");
        String[] names = isWindows ? new String[]{name + ".exe", name} : new String[]{name};
        for (String dir : pathEnv.split(File.pathSeparator)) {
            for (String n : names) {
                File f = new File(dir, n);
                if (f.isFile() && f.canExecute()) return f.getAbsolutePath();
            }
        }
        return null;
    }
}
