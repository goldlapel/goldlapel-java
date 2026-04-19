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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.function.BiConsumer;

/**
 * Gold Lapel — self-optimizing Postgres proxy, Java wrapper (v0.2 factory API).
 *
 * <p>Primary entry point is {@link #start(String, Consumer)}:
 *
 * <pre>{@code
 * try (GoldLapel gl = GoldLapel.start("postgresql://user:pass@db/mydb", opts -> {
 *     opts.setPort(7932);
 *     opts.setLogLevel("info");
 * })) {
 *     // JDBC: use getJdbcUrl() + getJdbcUser() + getJdbcPassword() — the PG
 *     // JDBC driver rejects inline userinfo in the URL.
 *     Properties props = new Properties();
 *     if (gl.getJdbcUser() != null) props.setProperty("user", gl.getJdbcUser());
 *     if (gl.getJdbcPassword() != null) props.setProperty("password", gl.getJdbcPassword());
 *     try (Connection conn = DriverManager.getConnection(gl.getJdbcUrl(), props)) {
 *         // ... raw JDBC ...
 *     }
 *     gl.docInsert("events", "{\"type\":\"signup\"}");
 * } // gl.close() auto-stops the proxy
 * }</pre>
 */
public class GoldLapel implements AutoCloseable {

    static final int DEFAULT_PORT = 7932;
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
    private Connection internalConn;

    // Scoped connection override — set by using(conn, runnable) for the duration
    // of the lambda. Uses ThreadLocal so only the calling thread sees the override.
    private final ThreadLocal<Connection> scopedConn = new ThreadLocal<>();

    // Package-private: exposed so unit tests can construct an instance without
    // actually spawning the proxy subprocess. Production callers use start().
    GoldLapel(String upstream, GoldLapelOptions options) {
        this.upstream = upstream;
        this.port = options.getPort() != null ? options.getPort() : DEFAULT_PORT;
        Map<String, Object> cfg = options.getConfig();
        this.dashboardPort = cfg != null && cfg.containsKey("dashboardPort")
            ? ((Number) cfg.get("dashboardPort")).intValue()
            : this.port + 1;
        this.config = cfg;
        List<String> extras = options.getExtraArgs() != null
            ? new ArrayList<>(options.getExtraArgs())
            : new ArrayList<>();
        String verboseFlag = translateLogLevel(options.getLogLevel());
        if (verboseFlag != null) {
            extras.add(verboseFlag);
        }
        this.extraArgs = extras;
        this.client = options.getClient() != null ? options.getClient() : "java";
        this.process = null;
        this.proxyUrl = null;
    }

    // ── Factory ───────────────────────────────────────────────

    /**
     * Start a Gold Lapel proxy for the given upstream Postgres URL. Returns a
     * {@code GoldLapel} instance backed by an eagerly-opened internal JDBC
     * connection. Implements {@link AutoCloseable} so try-with-resources
     * cleans up the proxy (and the internal connection) automatically.
     */
    public static GoldLapel start(String upstream) {
        return start(upstream, null);
    }

    /**
     * Start a Gold Lapel proxy, configuring it via the supplied lambda.
     *
     * <pre>{@code
     * GoldLapel gl = GoldLapel.start(url, opts -> {
     *     opts.setPort(7932);
     *     opts.setLogLevel("info");
     * });
     * }</pre>
     */
    public static GoldLapel start(String upstream, Consumer<GoldLapelOptions> configurator) {
        GoldLapelOptions options = new GoldLapelOptions();
        if (configurator != null) {
            configurator.accept(options);
        }
        GoldLapel gl = new GoldLapel(upstream, options);
        try {
            gl.startProxy();
            gl.eagerConnect();
        } catch (RuntimeException e) {
            gl.stop();
            throw e;
        }
        return gl;
    }

    private void startProxy() {
        if (process != null && process.isAlive()) {
            return;
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
            // Explicit config wins over inherited env (matches Spring Boot /
            // Micronaut / Quarkus precedence: explicit config > env > defaults).
            pb.environment().put("GOLDLAPEL_CLIENT", client);
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

        if (dashboardPort > 0) {
            System.out.println("goldlapel → :" + port + " (proxy) | http://127.0.0.1:" + dashboardPort + " (dashboard)");
        } else {
            System.out.println("goldlapel → :" + port + " (proxy)");
        }
    }

    // Eagerly open the internal JDBC connection used by wrapper methods that
    // don't receive an explicit connection. Mandatory: start() reports a clear
    // error if the PostgreSQL JDBC driver isn't on the classpath.
    private void eagerConnect() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                "No PostgreSQL JDBC driver found. Add org.postgresql:postgresql to your dependencies."
            );
        }
        try {
            JdbcConnectionInfo info = toJdbcConnectionInfo(proxyUrl);
            java.util.Properties props = new java.util.Properties();
            if (info.user != null) props.setProperty("user", info.user);
            if (info.password != null) props.setProperty("password", info.password);
            internalConn = DriverManager.getConnection(info.url, props);
        } catch (SQLException e) {
            throw new RuntimeException(
                "Gold Lapel failed to open internal JDBC connection: " + e.getMessage(), e
            );
        }
    }

    // Split a postgres:// URL into a JDBC URL + user/password properties.
    // JDBC's PostgreSQL driver rejects userinfo inline in the URL (it reads
    // everything before '@' as the hostname), so we split it out explicitly.
    static JdbcConnectionInfo toJdbcConnectionInfo(String url) {
        String stripped;
        if (url.startsWith("postgres://")) {
            stripped = url.substring("postgres://".length());
        } else if (url.startsWith("postgresql://")) {
            stripped = url.substring("postgresql://".length());
        } else {
            stripped = url;
        }

        String user = null;
        String password = null;
        // If there's userinfo (something before the last '@' in the authority),
        // split it out. We use the LAST '@' because '@' can appear in passwords
        // (common with generated credentials).
        int pathStart = indexOfAny(stripped, "/?#");
        String authority = pathStart < 0 ? stripped : stripped.substring(0, pathStart);
        String rest = pathStart < 0 ? "" : stripped.substring(pathStart);
        int at = authority.lastIndexOf('@');
        if (at >= 0) {
            String userinfo = authority.substring(0, at);
            int colon = userinfo.indexOf(':');
            if (colon >= 0) {
                user = urlDecode(userinfo.substring(0, colon));
                password = urlDecode(userinfo.substring(colon + 1));
            } else {
                user = urlDecode(userinfo);
            }
            authority = authority.substring(at + 1);
        }
        return new JdbcConnectionInfo("jdbc:postgresql://" + authority + rest, user, password);
    }

    /**
     * Translate a log level string to the proxy's count-based verbosity flag.
     * The Rust binary exposes verbosity as -v / -vv / -vvv (count flag) rather
     * than --log-level <level>, so wrappers translate on the spawn side.
     *
     * @return "-v" / "-vv" / "-vvv" for info/debug/trace, or null for warn/error/null
     * @throws IllegalArgumentException if the string is not a recognized level
     */
    static String translateLogLevel(String level) {
        if (level == null) {
            return null;
        }
        switch (level.toLowerCase(java.util.Locale.ROOT)) {
            case "trace":
                return "-vvv";
            case "debug":
                return "-vv";
            case "info":
                return "-v";
            case "warn":
            case "warning":
            case "error":
                return null;
            default:
                throw new IllegalArgumentException(
                    "log_level must be one of: trace, debug, info, warn, error"
                );
        }
    }

    private static int indexOfAny(String s, String chars) {
        int min = -1;
        for (int i = 0; i < chars.length(); i++) {
            int idx = s.indexOf(chars.charAt(i));
            if (idx >= 0 && (min < 0 || idx < min)) min = idx;
        }
        return min;
    }

    private static String urlDecode(String s) {
        try {
            return java.net.URLDecoder.decode(s, java.nio.charset.StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            // If the string contains invalid %xx sequences, fall back to raw
            return s;
        }
    }

    static class JdbcConnectionInfo {
        final String url;
        final String user;
        final String password;
        JdbcConnectionInfo(String url, String user, String password) {
            this.url = url;
            this.user = user;
            this.password = password;
        }
    }

    // ── Lifecycle ─────────────────────────────────────────────

    /**
     * Stop the proxy and close the internal connection. Idempotent.
     * Called automatically by {@link #close()} (try-with-resources).
     */
    public void stop() {
        if (internalConn != null) {
            try { internalConn.close(); } catch (SQLException ignored) {}
            internalConn = null;
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

    @Override
    public void close() {
        stop();
    }

    // ── Accessors ─────────────────────────────────────────────

    /**
     * Proxy URL in the standard Postgres form
     * ({@code postgresql://user:pass@localhost:7932/mydb}). Matches the shape
     * of the upstream URL. For JDBC, use {@link #getJdbcUrl()} — the JDBC
     * driver rejects inline userinfo.
     */
    public String getUrl() {
        return proxyUrl;
    }

    /**
     * JDBC connection URL for the proxy, suitable for
     * {@link DriverManager#getConnection(String, java.util.Properties)}. The
     * returned URL has the {@code jdbc:postgresql://} scheme and no userinfo;
     * retrieve the user and password separately via
     * {@link #getJdbcUser()} and {@link #getJdbcPassword()}.
     */
    public String getJdbcUrl() {
        if (proxyUrl == null) return null;
        return toJdbcConnectionInfo(proxyUrl).url;
    }

    /** User name parsed from the upstream URL, or {@code null} if absent. */
    public String getJdbcUser() {
        if (proxyUrl == null) return null;
        return toJdbcConnectionInfo(proxyUrl).user;
    }

    /** Password parsed from the upstream URL, or {@code null} if absent. */
    public String getJdbcPassword() {
        if (proxyUrl == null) return null;
        return toJdbcConnectionInfo(proxyUrl).password;
    }

    public int getPort() {
        return port;
    }

    // Package-private accessor for tests — not part of the public API.
    int getDashboardPort() {
        return dashboardPort;
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

    /** Internal JDBC connection opened by {@link #start}. Used by wrapper methods by default. */
    public Connection connection() {
        if (internalConn == null) {
            throw new IllegalStateException(
                "No connection available. GoldLapel.start() must be called successfully first.");
        }
        return internalConn;
    }

    // Resolve the connection a wrapper method should use when no explicit
    // override is provided: scoped (using) > internal.
    private Connection resolveConn() {
        Connection scoped = scopedConn.get();
        if (scoped != null) return scoped;
        return connection();
    }

    /**
     * Run {@code body} with {@code conn} bound as the connection seen by all
     * wrapper methods called on this thread inside the lambda. The scope is
     * fiber/thread-local — concurrent callers on other threads are unaffected.
     * Nested {@code using(...)} calls restore the outer connection on exit.
     *
     * <pre>{@code
     * gl.using(conn, () -> {
     *     gl.docInsert("events", "{\"type\":\"order\"}");
     * });
     * }</pre>
     */
    public void using(Connection conn, Runnable body) {
        if (conn == null) throw new IllegalArgumentException("using(conn, ...): conn must not be null");
        if (body == null) throw new IllegalArgumentException("using(conn, ...): body must not be null");
        Connection prev = scopedConn.get();
        scopedConn.set(conn);
        try {
            body.run();
        } finally {
            if (prev == null) {
                scopedConn.remove();
            } else {
                scopedConn.set(prev);
            }
        }
    }

    /**
     * Value-returning variant of {@link #using(Connection, Runnable)}: runs
     * {@code body} with {@code conn} bound as the scoped connection and returns
     * whatever the body produces. Matches the cross-wrapper consensus where
     * {@code using} propagates the callback's return value (JS/PHP/Ruby/.NET/Reactor).
     *
     * <pre>{@code
     * long count = gl.using(conn, () -> gl.docCount("events", "{}"));
     * }</pre>
     */
    public <T> T using(Connection conn, ThrowingSupplier<T> body) throws SQLException {
        if (conn == null) throw new IllegalArgumentException("using(conn, ...): conn must not be null");
        if (body == null) throw new IllegalArgumentException("using(conn, ...): body must not be null");
        Connection prev = scopedConn.get();
        scopedConn.set(conn);
        try {
            return body.get();
        } finally {
            if (prev == null) {
                scopedConn.remove();
            } else {
                scopedConn.set(prev);
            }
        }
    }

    /**
     * Supplier variant that may throw {@link SQLException}, for use with
     * {@link #using(Connection, ThrowingSupplier)}.
     */
    @FunctionalInterface
    public interface ThrowingSupplier<T> {
        T get() throws SQLException;
    }

    // ── Wrapper methods (each has a no-conn and an explicit-conn overload) ─

    // Document store

    public Map<String, Object> docInsert(String collection, String documentJson) throws SQLException {
        return Utils.docInsert(resolveConn(), collection, documentJson);
    }

    public Map<String, Object> docInsert(String collection, String documentJson, Connection conn) throws SQLException {
        return Utils.docInsert(conn, collection, documentJson);
    }

    public List<Map<String, Object>> docInsertMany(String collection, List<String> documents) throws SQLException {
        return Utils.docInsertMany(resolveConn(), collection, documents);
    }

    public List<Map<String, Object>> docInsertMany(String collection, List<String> documents, Connection conn) throws SQLException {
        return Utils.docInsertMany(conn, collection, documents);
    }

    public List<Map<String, Object>> docFind(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip) throws SQLException {
        return Utils.docFind(resolveConn(), collection, filterJson, sortJson, limit, skip);
    }

    public List<Map<String, Object>> docFind(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, Connection conn) throws SQLException {
        return Utils.docFind(conn, collection, filterJson, sortJson, limit, skip);
    }

    public Map<String, Object> docFindOne(String collection, String filterJson) throws SQLException {
        return Utils.docFindOne(resolveConn(), collection, filterJson);
    }

    public Map<String, Object> docFindOne(String collection, String filterJson, Connection conn) throws SQLException {
        return Utils.docFindOne(conn, collection, filterJson);
    }

    public Iterator<Map<String, Object>> docFindCursor(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, int batchSize) throws SQLException {
        return Utils.docFindCursor(resolveConn(), collection, filterJson, sortJson, limit, skip, batchSize);
    }

    public Iterator<Map<String, Object>> docFindCursor(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, int batchSize, Connection conn) throws SQLException {
        return Utils.docFindCursor(conn, collection, filterJson, sortJson, limit, skip, batchSize);
    }

    public int docUpdate(String collection, String filterJson, String updateJson) throws SQLException {
        return Utils.docUpdate(resolveConn(), collection, filterJson, updateJson);
    }

    public int docUpdate(String collection, String filterJson, String updateJson, Connection conn) throws SQLException {
        return Utils.docUpdate(conn, collection, filterJson, updateJson);
    }

    public int docUpdateOne(String collection, String filterJson, String updateJson) throws SQLException {
        return Utils.docUpdateOne(resolveConn(), collection, filterJson, updateJson);
    }

    public int docUpdateOne(String collection, String filterJson, String updateJson, Connection conn) throws SQLException {
        return Utils.docUpdateOne(conn, collection, filterJson, updateJson);
    }

    public int docDelete(String collection, String filterJson) throws SQLException {
        return Utils.docDelete(resolveConn(), collection, filterJson);
    }

    public int docDelete(String collection, String filterJson, Connection conn) throws SQLException {
        return Utils.docDelete(conn, collection, filterJson);
    }

    public int docDeleteOne(String collection, String filterJson) throws SQLException {
        return Utils.docDeleteOne(resolveConn(), collection, filterJson);
    }

    public int docDeleteOne(String collection, String filterJson, Connection conn) throws SQLException {
        return Utils.docDeleteOne(conn, collection, filterJson);
    }

    public Map<String, Object> docFindOneAndUpdate(String collection, String filterJson,
            String updateJson) throws SQLException {
        return Utils.docFindOneAndUpdate(resolveConn(), collection, filterJson, updateJson);
    }

    public Map<String, Object> docFindOneAndUpdate(String collection, String filterJson,
            String updateJson, Connection conn) throws SQLException {
        return Utils.docFindOneAndUpdate(conn, collection, filterJson, updateJson);
    }

    public Map<String, Object> docFindOneAndDelete(String collection, String filterJson) throws SQLException {
        return Utils.docFindOneAndDelete(resolveConn(), collection, filterJson);
    }

    public Map<String, Object> docFindOneAndDelete(String collection, String filterJson, Connection conn) throws SQLException {
        return Utils.docFindOneAndDelete(conn, collection, filterJson);
    }

    public List<String> docDistinct(String collection, String field, String filterJson) throws SQLException {
        return Utils.docDistinct(resolveConn(), collection, field, filterJson);
    }

    public List<String> docDistinct(String collection, String field, String filterJson, Connection conn) throws SQLException {
        return Utils.docDistinct(conn, collection, field, filterJson);
    }

    public long docCount(String collection, String filterJson) throws SQLException {
        return Utils.docCount(resolveConn(), collection, filterJson);
    }

    public long docCount(String collection, String filterJson, Connection conn) throws SQLException {
        return Utils.docCount(conn, collection, filterJson);
    }

    public void docCreateIndex(String collection, List<String> keys) throws SQLException {
        Utils.docCreateIndex(resolveConn(), collection, keys);
    }

    public void docCreateIndex(String collection, List<String> keys, Connection conn) throws SQLException {
        Utils.docCreateIndex(conn, collection, keys);
    }

    public List<Map<String, Object>> docAggregate(String collection, String pipelineJson) throws SQLException {
        return Utils.docAggregate(resolveConn(), collection, pipelineJson);
    }

    public List<Map<String, Object>> docAggregate(String collection, String pipelineJson, Connection conn) throws SQLException {
        return Utils.docAggregate(conn, collection, pipelineJson);
    }

    public Thread docWatch(String collection, BiConsumer<String, String> callback) throws SQLException {
        return Utils.docWatch(resolveConn(), collection, callback);
    }

    public Thread docWatch(String collection, BiConsumer<String, String> callback, Connection conn) throws SQLException {
        return Utils.docWatch(conn, collection, callback);
    }

    public void docUnwatch(String collection) throws SQLException {
        Utils.docUnwatch(resolveConn(), collection);
    }

    public void docUnwatch(String collection, Connection conn) throws SQLException {
        Utils.docUnwatch(conn, collection);
    }

    public void docCreateTtlIndex(String collection, int expireAfterSeconds) throws SQLException {
        Utils.docCreateTtlIndex(resolveConn(), collection, expireAfterSeconds);
    }

    public void docCreateTtlIndex(String collection, int expireAfterSeconds, Connection conn) throws SQLException {
        Utils.docCreateTtlIndex(conn, collection, expireAfterSeconds);
    }

    public void docCreateTtlIndex(String collection, int expireAfterSeconds, String field) throws SQLException {
        Utils.docCreateTtlIndex(resolveConn(), collection, expireAfterSeconds, field);
    }

    public void docCreateTtlIndex(String collection, int expireAfterSeconds, String field, Connection conn) throws SQLException {
        Utils.docCreateTtlIndex(conn, collection, expireAfterSeconds, field);
    }

    public void docRemoveTtlIndex(String collection) throws SQLException {
        Utils.docRemoveTtlIndex(resolveConn(), collection);
    }

    public void docRemoveTtlIndex(String collection, Connection conn) throws SQLException {
        Utils.docRemoveTtlIndex(conn, collection);
    }

    public void docCreateCollection(String collection, boolean unlogged) throws SQLException {
        Utils.docCreateCollection(resolveConn(), collection, unlogged);
    }

    public void docCreateCollection(String collection, boolean unlogged, Connection conn) throws SQLException {
        Utils.docCreateCollection(conn, collection, unlogged);
    }

    public void docCreateCollection(String collection) throws SQLException {
        Utils.docCreateCollection(resolveConn(), collection);
    }

    public void docCreateCollection(String collection, Connection conn) throws SQLException {
        Utils.docCreateCollection(conn, collection);
    }

    public void docCreateCapped(String collection, int maxDocuments) throws SQLException {
        Utils.docCreateCapped(resolveConn(), collection, maxDocuments);
    }

    public void docCreateCapped(String collection, int maxDocuments, Connection conn) throws SQLException {
        Utils.docCreateCapped(conn, collection, maxDocuments);
    }

    public void docRemoveCap(String collection) throws SQLException {
        Utils.docRemoveCap(resolveConn(), collection);
    }

    public void docRemoveCap(String collection, Connection conn) throws SQLException {
        Utils.docRemoveCap(conn, collection);
    }

    // Search

    public List<Map<String, Object>> search(String table, String column, String query,
            int limit, String lang, boolean highlight) throws SQLException {
        return Utils.search(resolveConn(), table, column, query, limit, lang, highlight);
    }

    public List<Map<String, Object>> search(String table, String column, String query,
            int limit, String lang, boolean highlight, Connection conn) throws SQLException {
        return Utils.search(conn, table, column, query, limit, lang, highlight);
    }

    public List<Map<String, Object>> search(String table, String[] columns, String query,
            int limit, String lang, boolean highlight) throws SQLException {
        return Utils.search(resolveConn(), table, columns, query, limit, lang, highlight);
    }

    public List<Map<String, Object>> search(String table, String[] columns, String query,
            int limit, String lang, boolean highlight, Connection conn) throws SQLException {
        return Utils.search(conn, table, columns, query, limit, lang, highlight);
    }

    public List<Map<String, Object>> searchFuzzy(String table, String column, String query,
            int limit, double threshold) throws SQLException {
        return Utils.searchFuzzy(resolveConn(), table, column, query, limit, threshold);
    }

    public List<Map<String, Object>> searchFuzzy(String table, String column, String query,
            int limit, double threshold, Connection conn) throws SQLException {
        return Utils.searchFuzzy(conn, table, column, query, limit, threshold);
    }

    public List<Map<String, Object>> searchPhonetic(String table, String column, String query,
            int limit) throws SQLException {
        return Utils.searchPhonetic(resolveConn(), table, column, query, limit);
    }

    public List<Map<String, Object>> searchPhonetic(String table, String column, String query,
            int limit, Connection conn) throws SQLException {
        return Utils.searchPhonetic(conn, table, column, query, limit);
    }

    public List<Map<String, Object>> similar(String table, String column, double[] vector,
            int limit) throws SQLException {
        return Utils.similar(resolveConn(), table, column, vector, limit);
    }

    public List<Map<String, Object>> similar(String table, String column, double[] vector,
            int limit, Connection conn) throws SQLException {
        return Utils.similar(conn, table, column, vector, limit);
    }

    public List<Map<String, Object>> suggest(String table, String column, String prefix,
            int limit) throws SQLException {
        return Utils.suggest(resolveConn(), table, column, prefix, limit);
    }

    public List<Map<String, Object>> suggest(String table, String column, String prefix,
            int limit, Connection conn) throws SQLException {
        return Utils.suggest(conn, table, column, prefix, limit);
    }

    public List<Map<String, Object>> facets(String table, String column, int limit) throws SQLException {
        return Utils.facets(resolveConn(), table, column, limit);
    }

    public List<Map<String, Object>> facets(String table, String column, int limit,
            String query, String queryColumn, String lang) throws SQLException {
        return Utils.facets(resolveConn(), table, column, limit, query, queryColumn, lang);
    }

    public List<Map<String, Object>> facets(String table, String column, int limit,
            String query, String queryColumn, String lang, Connection conn) throws SQLException {
        return Utils.facets(conn, table, column, limit, query, queryColumn, lang);
    }

    public List<Map<String, Object>> facets(String table, String column, int limit,
            String query, String[] queryColumns, String lang) throws SQLException {
        return Utils.facets(resolveConn(), table, column, limit, query, queryColumns, lang);
    }

    public List<Map<String, Object>> facets(String table, String column, int limit,
            String query, String[] queryColumns, String lang, Connection conn) throws SQLException {
        return Utils.facets(conn, table, column, limit, query, queryColumns, lang);
    }

    public List<Map<String, Object>> facets(String table, String column, int limit,
            Connection conn) throws SQLException {
        return Utils.facets(conn, table, column, limit);
    }

    public List<Map<String, Object>> aggregate(String table, String column, String func) throws SQLException {
        return Utils.aggregate(resolveConn(), table, column, func);
    }

    public List<Map<String, Object>> aggregate(String table, String column, String func,
            Connection conn) throws SQLException {
        return Utils.aggregate(conn, table, column, func);
    }

    public List<Map<String, Object>> aggregate(String table, String column, String func,
            String groupBy, int limit) throws SQLException {
        return Utils.aggregate(resolveConn(), table, column, func, groupBy, limit);
    }

    public List<Map<String, Object>> aggregate(String table, String column, String func,
            String groupBy, int limit, Connection conn) throws SQLException {
        return Utils.aggregate(conn, table, column, func, groupBy, limit);
    }

    public void createSearchConfig(String name) throws SQLException {
        Utils.createSearchConfig(resolveConn(), name);
    }

    public void createSearchConfig(String name, Connection conn) throws SQLException {
        Utils.createSearchConfig(conn, name);
    }

    public void createSearchConfig(String name, String copyFrom) throws SQLException {
        Utils.createSearchConfig(resolveConn(), name, copyFrom);
    }

    public void createSearchConfig(String name, String copyFrom, Connection conn) throws SQLException {
        Utils.createSearchConfig(conn, name, copyFrom);
    }

    // PubSub and queues

    public void publish(String channel, String message) throws SQLException {
        Utils.publish(resolveConn(), channel, message);
    }

    public void publish(String channel, String message, Connection conn) throws SQLException {
        Utils.publish(conn, channel, message);
    }

    public Thread subscribe(String channel, BiConsumer<String, String> callback) throws SQLException {
        return Utils.subscribe(resolveConn(), channel, callback);
    }

    public Thread subscribe(String channel, BiConsumer<String, String> callback,
            Connection conn) throws SQLException {
        return Utils.subscribe(conn, channel, callback);
    }

    public Thread subscribe(String channel, BiConsumer<String, String> callback,
            boolean blocking) throws SQLException {
        return Utils.subscribe(resolveConn(), channel, callback, blocking);
    }

    public Thread subscribe(String channel, BiConsumer<String, String> callback,
            boolean blocking, Connection conn) throws SQLException {
        return Utils.subscribe(conn, channel, callback, blocking);
    }

    public void enqueue(String queueTable, String payloadJson) throws SQLException {
        Utils.enqueue(resolveConn(), queueTable, payloadJson);
    }

    public void enqueue(String queueTable, String payloadJson, Connection conn) throws SQLException {
        Utils.enqueue(conn, queueTable, payloadJson);
    }

    public String dequeue(String queueTable) throws SQLException {
        return Utils.dequeue(resolveConn(), queueTable);
    }

    public String dequeue(String queueTable, Connection conn) throws SQLException {
        return Utils.dequeue(conn, queueTable);
    }

    // Counters

    public long incr(String table, String key, long amount) throws SQLException {
        return Utils.incr(resolveConn(), table, key, amount);
    }

    public long incr(String table, String key, long amount, Connection conn) throws SQLException {
        return Utils.incr(conn, table, key, amount);
    }

    public long getCounter(String table, String key) throws SQLException {
        return Utils.getCounter(resolveConn(), table, key);
    }

    public long getCounter(String table, String key, Connection conn) throws SQLException {
        return Utils.getCounter(conn, table, key);
    }

    // Hashes

    public void hset(String table, String key, String field, String valueJson) throws SQLException {
        Utils.hset(resolveConn(), table, key, field, valueJson);
    }

    public void hset(String table, String key, String field, String valueJson, Connection conn) throws SQLException {
        Utils.hset(conn, table, key, field, valueJson);
    }

    public String hget(String table, String key, String field) throws SQLException {
        return Utils.hget(resolveConn(), table, key, field);
    }

    public String hget(String table, String key, String field, Connection conn) throws SQLException {
        return Utils.hget(conn, table, key, field);
    }

    public String hgetall(String table, String key) throws SQLException {
        return Utils.hgetall(resolveConn(), table, key);
    }

    public String hgetall(String table, String key, Connection conn) throws SQLException {
        return Utils.hgetall(conn, table, key);
    }

    public boolean hdel(String table, String key, String field) throws SQLException {
        return Utils.hdel(resolveConn(), table, key, field);
    }

    public boolean hdel(String table, String key, String field, Connection conn) throws SQLException {
        return Utils.hdel(conn, table, key, field);
    }

    // Sorted sets

    public void zadd(String table, String member, double score) throws SQLException {
        Utils.zadd(resolveConn(), table, member, score);
    }

    public void zadd(String table, String member, double score, Connection conn) throws SQLException {
        Utils.zadd(conn, table, member, score);
    }

    public double zincrby(String table, String member, double amount) throws SQLException {
        return Utils.zincrby(resolveConn(), table, member, amount);
    }

    public double zincrby(String table, String member, double amount, Connection conn) throws SQLException {
        return Utils.zincrby(conn, table, member, amount);
    }

    public List<Map.Entry<String, Double>> zrange(String table, int start, int stop,
            boolean desc) throws SQLException {
        return Utils.zrange(resolveConn(), table, start, stop, desc);
    }

    public List<Map.Entry<String, Double>> zrange(String table, int start, int stop,
            boolean desc, Connection conn) throws SQLException {
        return Utils.zrange(conn, table, start, stop, desc);
    }

    public Long zrank(String table, String member, boolean desc) throws SQLException {
        return Utils.zrank(resolveConn(), table, member, desc);
    }

    public Long zrank(String table, String member, boolean desc, Connection conn) throws SQLException {
        return Utils.zrank(conn, table, member, desc);
    }

    public Double zscore(String table, String member) throws SQLException {
        return Utils.zscore(resolveConn(), table, member);
    }

    public Double zscore(String table, String member, Connection conn) throws SQLException {
        return Utils.zscore(conn, table, member);
    }

    public boolean zrem(String table, String member) throws SQLException {
        return Utils.zrem(resolveConn(), table, member);
    }

    public boolean zrem(String table, String member, Connection conn) throws SQLException {
        return Utils.zrem(conn, table, member);
    }

    // Geo

    public List<Map<String, Object>> georadius(String table, String geomColumn, double lon,
            double lat, double radiusMeters, int limit) throws SQLException {
        return Utils.georadius(resolveConn(), table, geomColumn, lon, lat, radiusMeters, limit);
    }

    public List<Map<String, Object>> georadius(String table, String geomColumn, double lon,
            double lat, double radiusMeters, int limit, Connection conn) throws SQLException {
        return Utils.georadius(conn, table, geomColumn, lon, lat, radiusMeters, limit);
    }

    public void geoadd(String table, String nameColumn, String geomColumn, String name,
            double lon, double lat) throws SQLException {
        Utils.geoadd(resolveConn(), table, nameColumn, geomColumn, name, lon, lat);
    }

    public void geoadd(String table, String nameColumn, String geomColumn, String name,
            double lon, double lat, Connection conn) throws SQLException {
        Utils.geoadd(conn, table, nameColumn, geomColumn, name, lon, lat);
    }

    public Double geodist(String table, String geomColumn, String nameColumn,
            String nameA, String nameB) throws SQLException {
        return Utils.geodist(resolveConn(), table, geomColumn, nameColumn, nameA, nameB);
    }

    public Double geodist(String table, String geomColumn, String nameColumn,
            String nameA, String nameB, Connection conn) throws SQLException {
        return Utils.geodist(conn, table, geomColumn, nameColumn, nameA, nameB);
    }

    // Misc

    public long countDistinct(String table, String column) throws SQLException {
        return Utils.countDistinct(resolveConn(), table, column);
    }

    public long countDistinct(String table, String column, Connection conn) throws SQLException {
        return Utils.countDistinct(conn, table, column);
    }

    /**
     * Run a Lua script server-side with the given string arguments.
     *
     * <p><b>Caveat — no {@code Connection} overload.</b> The trailing
     * {@code String...} varargs collides with a would-be
     * {@code script(String luaCode, String... args, Connection conn)}
     * overload (Java resolves the last {@code Object} as part of the varargs
     * array, not as a separate parameter). To run {@code script} against a
     * specific connection, wrap the call in {@link #using(Connection, Runnable)}:
     *
     * <pre>{@code
     * gl.using(conn, () -> {
     *     try {
     *         gl.script("return redis.call('incr', KEYS[1])", "mykey");
     *     } catch (SQLException e) { throw new RuntimeException(e); }
     * });
     * }</pre>
     *
     * <p>Without an active {@code using(...)} scope, {@code script} runs
     * against Gold Lapel's internal connection.
     */
    public String script(String luaCode, String... args) throws SQLException {
        return Utils.script(resolveConn(), luaCode, args);
    }

    // Streams

    public long streamAdd(String stream, String payload) throws SQLException {
        return Utils.streamAdd(resolveConn(), stream, payload);
    }

    public long streamAdd(String stream, String payload, Connection conn) throws SQLException {
        return Utils.streamAdd(conn, stream, payload);
    }

    public void streamCreateGroup(String stream, String group) throws SQLException {
        Utils.streamCreateGroup(resolveConn(), stream, group);
    }

    public void streamCreateGroup(String stream, String group, Connection conn) throws SQLException {
        Utils.streamCreateGroup(conn, stream, group);
    }

    public List<Map<String, Object>> streamRead(String stream, String group,
            String consumer, int count) throws SQLException {
        return Utils.streamRead(resolveConn(), stream, group, consumer, count);
    }

    public List<Map<String, Object>> streamRead(String stream, String group,
            String consumer, int count, Connection conn) throws SQLException {
        return Utils.streamRead(conn, stream, group, consumer, count);
    }

    public boolean streamAck(String stream, String group, long messageId) throws SQLException {
        return Utils.streamAck(resolveConn(), stream, group, messageId);
    }

    public boolean streamAck(String stream, String group, long messageId, Connection conn) throws SQLException {
        return Utils.streamAck(conn, stream, group, messageId);
    }

    public List<Map<String, Object>> streamClaim(String stream, String group,
            String consumer, long minIdleMs) throws SQLException {
        return Utils.streamClaim(resolveConn(), stream, group, consumer, minIdleMs);
    }

    public List<Map<String, Object>> streamClaim(String stream, String group,
            String consumer, long minIdleMs, Connection conn) throws SQLException {
        return Utils.streamClaim(conn, stream, group, consumer, minIdleMs);
    }

    // Percolator

    public void percolateAdd(String name, String queryId, String query) throws SQLException {
        Utils.percolateAdd(resolveConn(), name, queryId, query);
    }

    public void percolateAdd(String name, String queryId, String query, Connection conn) throws SQLException {
        Utils.percolateAdd(conn, name, queryId, query);
    }

    public void percolateAdd(String name, String queryId, String query,
            String lang, String metadataJson) throws SQLException {
        Utils.percolateAdd(resolveConn(), name, queryId, query, lang, metadataJson);
    }

    public void percolateAdd(String name, String queryId, String query,
            String lang, String metadataJson, Connection conn) throws SQLException {
        Utils.percolateAdd(conn, name, queryId, query, lang, metadataJson);
    }

    public List<Map<String, Object>> percolate(String name, String text) throws SQLException {
        return Utils.percolate(resolveConn(), name, text);
    }

    public List<Map<String, Object>> percolate(String name, String text, Connection conn) throws SQLException {
        return Utils.percolate(conn, name, text);
    }

    public List<Map<String, Object>> percolate(String name, String text,
            int limit, String lang) throws SQLException {
        return Utils.percolate(resolveConn(), name, text, limit, lang);
    }

    public List<Map<String, Object>> percolate(String name, String text,
            int limit, String lang, Connection conn) throws SQLException {
        return Utils.percolate(conn, name, text, limit, lang);
    }

    public boolean percolateDelete(String name, String queryId) throws SQLException {
        return Utils.percolateDelete(resolveConn(), name, queryId);
    }

    public boolean percolateDelete(String name, String queryId, Connection conn) throws SQLException {
        return Utils.percolateDelete(conn, name, queryId);
    }

    // Analysis

    public List<Map<String, Object>> analyze(String text) throws SQLException {
        return Utils.analyze(resolveConn(), text);
    }

    public List<Map<String, Object>> analyze(String text, String lang) throws SQLException {
        return Utils.analyze(resolveConn(), text, lang);
    }

    public List<Map<String, Object>> analyze(String text, Connection conn) throws SQLException {
        return Utils.analyze(conn, text);
    }

    public List<Map<String, Object>> analyze(String text, String lang, Connection conn) throws SQLException {
        return Utils.analyze(conn, text, lang);
    }

    public Map<String, Object> explainScore(String table, String column, String query,
            String idColumn, Object idValue) throws SQLException {
        return Utils.explainScore(resolveConn(), table, column, query, idColumn, idValue);
    }

    public Map<String, Object> explainScore(String table, String column, String query,
            String idColumn, Object idValue, Connection conn) throws SQLException {
        return Utils.explainScore(conn, table, column, query, idColumn, idValue);
    }

    public Map<String, Object> explainScore(String table, String column, String query,
            String idColumn, Object idValue, String lang) throws SQLException {
        return Utils.explainScore(resolveConn(), table, column, query, idColumn, idValue, lang);
    }

    public Map<String, Object> explainScore(String table, String column, String query,
            String idColumn, Object idValue, String lang, Connection conn) throws SQLException {
        return Utils.explainScore(conn, table, column, query, idColumn, idValue, lang);
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
