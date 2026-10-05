package com.goldlapel;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 *     opts.setProxyPort(7932);
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
 *     gl.documents.insert("events", "{\"type\":\"signup\"}");
 * } // gl.close() auto-stops the proxy
 * }</pre>
 */
public class GoldLapel implements AutoCloseable {

    static final int DEFAULT_PROXY_PORT = 7932;
    static final long STARTUP_TIMEOUT_MS = 10000;
    static final long STARTUP_POLL_INTERVAL_MS = 50;
    // How much of the proxy's stderr a failed start reports.
    static final int STDERR_TAIL_CHARS = 4096;
    // Upper bound on the eager JDBC connect (pgjdbc loginTimeout). Without it a
    // proxy that accepts TCP but never answers the startup handshake hangs
    // start() indefinitely. A loginTimeout in the URL query still wins.
    static final int EAGER_CONNECT_TIMEOUT_S = 30;

    // Keys that are valid inside the structured `config` map. Top-level
    // concepts (proxyPort, dashboardPort, logLevel, mode, license, client,
    // configFile) live on GoldLapelOptions directly and are NOT accepted
    // here — passing them via Config raises.
    private static final Set<String> VALID_CONFIG_KEYS;
    private static final Set<String> BOOLEAN_KEYS;
    private static final Set<String> LIST_KEYS;

    static {
        Set<String> keys = new HashSet<>();
        Collections.addAll(keys,
            "minPatternCount", "deepPaginationThreshold",
            "reportIntervalSecs", "proxyCacheSize", "batchCacheSize",
            "batchCacheTtlSecs", "poolSize", "poolTimeoutSecs",
            "poolMode", "mgmtIdleTimeout", "fallback", "readAfterWriteSecs",
            "n1Threshold", "n1WindowMs", "n1CrossThreshold",
            "tlsCert", "tlsKey", "tlsClientCa",
            "disableBtreeIndexes",
            "disableTrigramIndexes", "disableExpressionIndexes",
            "disablePartialIndexes", "disableRewritePreparedCache",
            "disablePool",
            "disableN1", "disableN1CrossConnection",
            "disableCoalescing", "replica", "excludeTables"
        );
        VALID_CONFIG_KEYS = Collections.unmodifiableSet(keys);

        Set<String> bools = new HashSet<>();
        Collections.addAll(bools,
            "disableBtreeIndexes",
            "disableTrigramIndexes", "disableExpressionIndexes",
            "disablePartialIndexes", "disableRewritePreparedCache",
            "disablePool",
            "disableN1", "disableN1CrossConnection",
            "disableCoalescing"
        );
        BOOLEAN_KEYS = Collections.unmodifiableSet(bools);

        Set<String> lists = new HashSet<>();
        Collections.addAll(lists, "replica", "excludeTables");
        LIST_KEYS = Collections.unmodifiableSet(lists);
    }

    // Options that no longer exist, and why — so passing one in `config`
    // says what happened instead of just "unknown".
    private static final Map<String, String> REMOVED_CONFIG_KEYS = Map.ofEntries(
        Map.entry("invalidationPort", "the in-process cache"),
        Map.entry("disableNativeCache", "the in-process cache"),
        Map.entry("nativeCacheSize", "the in-process cache"),
        Map.entry("aggressiveVerify", "the in-process cache"),
        Map.entry("disableMatviews", "materialized views"),
        Map.entry("refreshIntervalSecs", "materialized views"),
        Map.entry("patternTtlSecs", "materialized views"),
        Map.entry("maxTablesPerView", "materialized views"),
        Map.entry("maxColumnsPerView", "materialized views"),
        Map.entry("disableConsolidation", "materialized views"),
        Map.entry("disableRewrite", "materialized views"),
        Map.entry("disableShadowMode", "materialized views")
    );

    // TLS/GSS parameters of the upstream URL. They configure the proxy's hop
    // to Postgres, not the app's hop to the proxy — which is plain TCP unless
    // the proxy has its own --tls-cert/--tls-key — so the app's URL drops
    // them (`?sslmode=require` would make every connection fail). Lower-case;
    // matched case-insensitively. The libpq names, then pgjdbc's own.
    private static final Set<String> UPSTREAM_TLS_PARAMS = Set.of(
        "sslmode", "sslcert", "sslkey", "sslrootcert", "sslcrl", "sslcrldir",
        "sslpassword", "sslsni", "sslnegotiation", "ssl_min_protocol_version",
        "ssl_max_protocol_version", "requiressl", "channel_binding", "gssencmode",
        "krbsrvname", "gsslib",
        "ssl", "sslfactory", "sslfactoryarg", "sslhostnameverifier",
        "sslpasswordcallback", "sslresponsetimeout", "kerberosservername"
    );

    // Proxies this process has spawned, by upstream URL. A start() for an
    // upstream already here shares its proxy; one for a new upstream gets
    // ports no live entry claims. Guarded by its own monitor, as is each
    // instance's `proxy` field and each entry's `holders`.
    private static final Map<String, Proxy> PROXIES = new HashMap<>();

    // A spawned proxy subprocess, shared by every GoldLapel holding it and
    // stopped when the last of them stops.
    private static final class Proxy {
        final String upstream;
        final int proxyPort;
        final int dashboardPort; // 0: no dashboard
        // Completes when the spawn succeeds or fails; holders that didn't
        // spawn it wait here.
        final CompletableFuture<Void> ready = new CompletableFuture<>();
        volatile Process process;
        volatile String url;
        volatile String dashboardToken;
        int holders;

        Proxy(String upstream, int proxyPort, int dashboardPort) {
            this.upstream = upstream;
            this.proxyPort = proxyPort;
            this.dashboardPort = dashboardPort;
        }

        boolean claims(int port) {
            return port == proxyPort || (dashboardPort > 0 && port == dashboardPort);
        }

        // Started, then exited (or failed to start): its ports are free
        // again, and a start() for its upstream spawns a fresh one.
        boolean isDead() {
            Process proc = process;
            return ready.isDone() && (ready.isCompletedExceptionally() || proc == null || !proc.isAlive());
        }

        // Every holder stopped while it was still starting.
        boolean abandoned() {
            synchronized (PROXIES) {
                return holders == 0;
            }
        }
    }

    private final String upstream;
    // Requested ports until start() settles them: allocated when not
    // explicit, or the shared proxy's when it reuses one.
    private volatile int proxyPort;
    private volatile int dashboardPort;
    private final boolean proxyPortExplicit;
    private final boolean dashboardPortExplicit;
    private final String logLevel;
    private final String mode;
    private final String license;
    private final String configFile;
    private final Map<String, Object> config;
    private final List<String> extraArgs;
    // The proxy itself serves TLS to the app (--tls-cert): the app's URL
    // then keeps the upstream URL's TLS parameters.
    private final boolean clientTls;
    private final String client;
    private final boolean silent;
    private final boolean mesh;
    private final String meshTag;
    private final boolean disableProxyCache;
    private final boolean disableSqloptimize;
    private final boolean disableAutoIndexes;
    // Volatile with `stopped`: stop() may run on another thread while start()
    // is spawning/connecting (reactive cancellation). stop() sets `stopped`
    // then reads process/internalConn; start writes them then reads
    // `stopped` — so one side always sees the other and cleans up.
    private volatile Process process;
    private volatile String proxyUrl;
    private volatile Connection internalConn;
    private volatile boolean stopped;
    // The proxy this instance holds, from start() until stop(). Guarded by PROXIES.
    private Proxy proxy;

    // Nested namespaces — canonical schema-to-core sub-API instances. Each
    // holds a back-reference to this client for shared state (license,
    // dashboard token, http session, conn, DDL pattern cache).
    //
    // As of Phase 5, the Redis-compat helper families (counter / zset /
    // hash / queue / geo) are nested too, alongside streams (Phase 1+2)
    // and documents (Phase 4). Search / pubsub / percolator stay flat —
    // they'll migrate when their own schema-to-core phase fires.
    //
    // Final fields (Option A from cross-wrapper consensus): direct
    // access, no method-call indirection.
    /** Document store sub-API — accessible as {@code gl.documents.<verb>(...)}. */
    public final DocumentsApi documents;
    /** Streams sub-API — accessible as {@code gl.streams.<verb>(...)}. */
    public final StreamsApi streams;
    /** Counters sub-API — accessible as {@code gl.counters.<verb>(...)}. */
    public final CountersApi counters;
    /** Sorted-sets sub-API — accessible as {@code gl.zsets.<verb>(...)}. */
    public final ZsetsApi zsets;
    /** Hashes sub-API — accessible as {@code gl.hashes.<verb>(...)}. */
    public final HashesApi hashes;
    /** Queues sub-API (at-least-once with visibility timeout) — accessible
     *  as {@code gl.queues.<verb>(...)}. */
    public final QueuesApi queues;
    /** Geo sub-API (PostGIS GEOGRAPHY-native) — accessible as
     *  {@code gl.geos.<verb>(...)}. */
    public final GeosApi geos;
    // Dashboard token — provisioned per proxy when it is spawned. Exposed to the
    // DDL client via dashboardToken(). Non-final because we clear it on stop().
    private volatile String dashboardToken;
    // DDL pattern cache — one entry per (family, name) fetched from the proxy.
    // Populated on first stream_*/doc_*/etc call that touches a given helper.
    private final java.util.concurrent.ConcurrentHashMap<String, Map<String, Object>> ddlCache =
        new java.util.concurrent.ConcurrentHashMap<>();

    // Scoped connection override — set by using(conn, runnable) for the duration
    // of the lambda. Uses ThreadLocal so only the calling thread sees the override.
    private final ThreadLocal<Connection> scopedConn = new ThreadLocal<>();

    // Package-private: exposed so unit tests can construct an instance without
    // actually spawning the proxy subprocess. Production callers use start().
    GoldLapel(String upstream, GoldLapelOptions options) {
        this.upstream = upstream;
        this.proxyPortExplicit = options.getProxyPort() != null;
        this.proxyPort = proxyPortExplicit ? options.getProxyPort() : DEFAULT_PROXY_PORT;

        // Dashboard: null on options → derive from proxyPort. Non-null →
        // record the explicit override so buildSpawnCmd() emits --dashboard-port.
        Integer dp = options.getDashboardPort();
        this.dashboardPortExplicit = (dp != null);
        this.dashboardPort = dp != null ? dp : this.proxyPort + 1;

        this.logLevel = options.getLogLevel();
        this.mode = options.getMode();
        this.license = options.getLicense();
        this.configFile = options.getConfigFile();

        Map<String, Object> cfg = options.getConfig();
        // Validate config keys eagerly so unit tests that construct without
        // spawning still catch bad keys (same contract as configToArgs()).
        if (cfg != null) {
            for (String key : cfg.keySet()) {
                checkConfigKey(key);
            }
        }
        this.config = cfg;

        this.extraArgs = options.getExtraArgs() != null
            ? new ArrayList<>(options.getExtraArgs())
            : new ArrayList<>();
        this.clientTls = (cfg != null && cfg.containsKey("tlsCert"))
            || extraArgs.stream().anyMatch(a -> a.equals("--tls-cert") || a.startsWith("--tls-cert="));
        this.client = options.getClient() != null ? options.getClient() : "java";
        this.silent = options.isSilent();
        this.mesh = options.isMesh();
        String tag = options.getMeshTag();
        this.meshTag = (tag == null || tag.isEmpty()) ? null : tag;
        this.disableProxyCache = options.isDisableProxyCache();
        this.disableSqloptimize = options.isDisableSqloptimize();
        this.disableAutoIndexes = options.isDisableAutoIndexes();
        this.process = null;
        this.proxyUrl = null;

        // Nested namespaces. Constructed last so they capture the fully
        // initialized parent — sub-APIs only ever read from the parent at
        // call time, so even if other fields shifted later they would
        // observe the latest values via the back-reference.
        this.documents = new DocumentsApi(this);
        this.streams = new StreamsApi(this);
        this.counters = new CountersApi(this);
        this.zsets = new ZsetsApi(this);
        this.hashes = new HashesApi(this);
        this.queues = new QueuesApi(this);
        this.geos = new GeosApi(this);
    }

    // ── Factory ───────────────────────────────────────────────

    /**
     * Start a Gold Lapel proxy for the given upstream Postgres URL. Returns a
     * {@code GoldLapel} instance backed by an eagerly-opened internal JDBC
     * connection. Implements {@link AutoCloseable} so try-with-resources
     * cleans up the proxy (and the internal connection) automatically.
     *
     * <p>Ports: without {@link GoldLapelOptions#setProxyPort}, the proxy
     * takes the first port from 7932 up whose proxy port and dashboard port
     * (proxy port + 1, unless set) are both free — of this process's other
     * proxies and of anything else on the machine. An explicit port another
     * proxy of this process already uses throws {@link IllegalStateException};
     * one some other program uses fails with the proxy's own message.
     *
     * <p>One proxy per upstream: starting an upstream this process already
     * runs a proxy for returns a new instance sharing that proxy (its ports
     * and options; the new call's options are not applied) with its own
     * internal connection. The proxy stops when the last instance sharing
     * it stops.
     */
    public static GoldLapel start(String upstream) {
        return start(upstream, null);
    }

    /**
     * Start a Gold Lapel proxy, configuring it via the supplied lambda. See
     * {@link #start(String)} for how ports are chosen and proxies shared.
     *
     * <pre>{@code
     * GoldLapel gl = GoldLapel.start(url, opts -> {
     *     opts.setProxyPort(7932);
     *     opts.setLogLevel("info");
     * });
     * }</pre>
     */
    public static GoldLapel start(String upstream, Consumer<GoldLapelOptions> configurator) {
        return start(upstream, configurator, null);
    }

    /**
     * Internal hook for the {@code goldlapel-reactor} and
     * {@code goldlapel-rxjava3} wrappers; application code should call
     * {@link #start(String, Consumer)}. Not covered by compatibility
     * guarantees.
     *
     * <p>Starts like {@link #start(String, Consumer)}, but first hands the
     * new, not-yet-started instance to {@code onCreated}, on the calling
     * thread. The only method valid on it inside the callback, or from
     * another thread before this method returns, is {@link #stop()}: it
     * aborts the in-flight start (the subprocess is killed unless another
     * instance shares it) and this method throws. Everything else —
     * connections, URLs, ports, the sub-APIs — is undefined until this
     * method returns. Whatever the callback throws aborts the start.
     */
    public static GoldLapel start(String upstream, Consumer<GoldLapelOptions> configurator,
                                  Consumer<GoldLapel> onCreated) {
        GoldLapelOptions options = new GoldLapelOptions();
        if (configurator != null) {
            configurator.accept(options);
        }
        GoldLapel gl = new GoldLapel(upstream, options);
        boolean started = false;
        try {
            if (onCreated != null) {
                onCreated.accept(gl);
            }
            gl.acquireProxy();
            gl.eagerConnect();
            // A concurrent stop() that ran before internalConn was assigned
            // couldn't close it; the stop() below does.
            if (gl.stopped) {
                throw abortedStart();
            }
            started = true;
        } finally {
            // Any failure, Errors included, gives up this instance's hold on
            // the proxy — stopping the subprocess unless someone shares it.
            if (!started) {
                gl.stop();
            }
        }
        return gl;
    }

    private static RuntimeException abortedStart() {
        return new RuntimeException("Gold Lapel start aborted: stop() was called while starting");
    }

    // Hold this upstream's proxy: share the one this process already runs,
    // or allocate ports and spawn one.
    private void acquireProxy() {
        Proxy p;
        boolean spawn;
        synchronized (PROXIES) {
            if (stopped) {
                throw abortedStart();
            }
            p = PROXIES.get(upstream);
            if (p != null && p.isDead()) {
                PROXIES.remove(upstream);
                p = null;
            }
            spawn = p == null;
            if (spawn) {
                allocatePorts();
                p = new Proxy(upstream, proxyPort, dashboardPort);
                PROXIES.put(upstream, p);
            }
            p.holders++;
            proxy = p;
        }
        if (spawn) {
            try {
                spawnProxy(p);
            } catch (RuntimeException | Error e) {
                synchronized (PROXIES) {
                    PROXIES.remove(upstream, p);
                }
                p.ready.completeExceptionally(e);
                throw e;
            }
            p.ready.complete(null);
        } else {
            awaitReady(p);
        }
        proxyPort = p.proxyPort;
        dashboardPort = p.dashboardPort;
        process = p.process;
        dashboardToken = p.dashboardToken;
        proxyUrl = p.url;
        if (spawn) {
            printBanner(System.err);
        }
    }

    // Wait for another start() of the same upstream to finish spawning.
    private void awaitReady(Proxy p) {
        while (true) {
            if (stopped) {
                throw abortedStart();
            }
            try {
                p.ready.get(STARTUP_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
                return;
            } catch (TimeoutException e) {
                // Still spawning; check for stop() and wait again.
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                throw new RuntimeException(cause.getMessage(), cause);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for Gold Lapel to start", e);
            }
        }
    }

    // Choose this proxy's ports. Called holding the PROXIES lock, so two
    // concurrent starts can't pick the same pair. An explicit port that
    // another live proxy here listens on is an error. Otherwise the proxy
    // takes the smallest port from 7932 up such that neither it nor its
    // dashboard port (explicit, or proxy port + 1) is claimed here, and the
    // OS lets us bind both right now. An explicit port in use by another
    // program is left for the proxy to refuse, with its own message.
    private void allocatePorts() {
        if (proxyPortExplicit) {
            requireUnclaimed(proxyPort, "proxy");
        }
        if (dashboardPort > 0 && (dashboardPortExplicit || proxyPortExplicit)) {
            requireUnclaimed(dashboardPort, "dashboard");
        }
        if (proxyPortExplicit) {
            return;
        }
        for (int port = DEFAULT_PROXY_PORT; port < 65535; port++) {
            int dashboard = dashboardPortExplicit ? dashboardPort : port + 1;
            if (port == dashboard || isClaimed(port) || !isPortFree(port)) {
                continue;
            }
            if (!dashboardPortExplicit && (isClaimed(dashboard) || !isPortFree(dashboard))) {
                continue;
            }
            proxyPort = port;
            dashboardPort = dashboard;
            return;
        }
        throw new IllegalStateException(
            "I'm afraid Gold Lapel found no free pair of ports at or above " + DEFAULT_PROXY_PORT +
            ". Choose one with setProxyPort.");
    }

    private static boolean isClaimed(int port) {
        return claimant(port) != null;
    }

    private static Proxy claimant(int port) {
        for (Proxy p : PROXIES.values()) {
            if (!p.isDead() && p.claims(port)) {
                return p;
            }
        }
        return null;
    }

    private static void requireUnclaimed(int port, String what) {
        Proxy other = claimant(port);
        if (other != null) {
            String setter = what.equals("proxy")
                ? "setProxyPort, or leave it unset and a free one is chosen"
                : "setDashboardPort (0 turns the dashboard off)";
            throw new IllegalStateException(
                "I'm afraid port " + port + ", for the " + what + ", is already in use by this " +
                "process's Gold Lapel for " + redactUpstream(other.upstream) +
                ". Choose another with " + setter + ".");
        }
    }

    /**
     * Whether the OS lets a listener bind {@code port} on every interface
     * right now — the same check the proxy makes before it starts. The
     * socket is closed straight away. Java's default SO_REUSEADDR matches
     * Rust's (on for POSIX, where it only skips TIME_WAIT); SO_REUSEPORT is
     * never set, so a live listener always makes the bind fail.
     */
    static boolean isPortFree(int port) {
        try (ServerSocket s = new ServerSocket()) {
            s.bind(new InetSocketAddress(port));
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /** {@code url} with the password in its userinfo, if any, replaced by {@code ***}. */
    static String redactUpstream(String url) {
        int scheme = url.indexOf("://");
        int start = scheme < 0 ? 0 : scheme + 3;
        int end = indexOfAny(url.substring(start), "/?#");
        end = end < 0 ? url.length() : start + end;
        int at = url.lastIndexOf('@', end - 1);
        if (at < start) {
            return url;
        }
        int colon = url.indexOf(':', start);
        if (colon < 0 || colon > at) {
            return url;
        }
        return url.substring(0, colon + 1) + "***" + url.substring(at);
    }

    private void spawnProxy(Proxy p) {
        String binary = findBinary();
        List<String> cmd = buildSpawnCmd(binary);
        // Something else already listening on our port answers the readiness
        // connect, so that would prove nothing: the proxy refuses a busy port
        // and exits, and we wait for that instead.
        boolean portsBusy = !isPortFree(proxyPort) || (dashboardPort > 0 && !isPortFree(dashboardPort));

        Process proc;
        try {
            ProcessBuilder pb = new ProcessBuilder(cmd);
            // Explicit config wins over inherited env (matches Spring Boot /
            // Micronaut / Quarkus precedence: explicit config > env > defaults).
            pb.environment().put("GOLDLAPEL_CLIENT", client);
            // Provision a session-scoped dashboard token for /api/ddl/* calls.
            // Pre-set env wins; otherwise generate a fresh one per session.
            String existingToken = pb.environment().get("GOLDLAPEL_DASHBOARD_TOKEN");
            if (existingToken != null && !existingToken.isEmpty()) {
                p.dashboardToken = existingToken;
            } else {
                byte[] randomBytes = new byte[32];
                new java.security.SecureRandom().nextBytes(randomBytes);
                StringBuilder sb = new StringBuilder();
                for (byte b : randomBytes) sb.append(String.format("%02x", b));
                p.dashboardToken = sb.toString();
                pb.environment().put("GOLDLAPEL_DASHBOARD_TOKEN", p.dashboardToken);
            }
            pb.redirectInput(ProcessBuilder.Redirect.PIPE);
            pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
            pb.redirectError(ProcessBuilder.Redirect.PIPE);
            proc = pb.start();
            p.process = proc;
            proc.getOutputStream().close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start Gold Lapel process", e);
        }
        if (p.abandoned()) {
            proc.destroyForcibly();
            throw abortedStart();
        }

        // Drain stderr on a daemon thread to prevent pipe-buffer deadlock,
        // keeping only the tail: it runs for the proxy's whole life.
        StringBuilder stderrTail = new StringBuilder();
        Thread stderrDrain = new Thread(() -> {
            try (java.io.Reader err = new java.io.InputStreamReader(
                    proc.getErrorStream(), java.nio.charset.StandardCharsets.UTF_8)) {
                char[] buf = new char[1024];
                int n;
                while ((n = err.read(buf)) != -1) {
                    synchronized (stderrTail) {
                        stderrTail.append(buf, 0, n);
                        if (stderrTail.length() > 2 * STDERR_TAIL_CHARS) {
                            stderrTail.delete(0, stderrTail.length() - STDERR_TAIL_CHARS);
                        }
                    }
                }
            } catch (IOException ignored) {}
        });
        stderrDrain.setDaemon(true);
        stderrDrain.start();

        // Ready once the port answers while our child is still alive — a
        // child that exited (say, refusing a busy port) isn't what answered.
        long deadline = System.nanoTime() + STARTUP_TIMEOUT_MS * 1_000_000L;
        boolean ready = false;
        while (System.nanoTime() < deadline) {
            if (!proc.isAlive() || p.abandoned()) break;
            if (portsBusy) {
                try {
                    proc.waitFor(STARTUP_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } else if (waitForPort("127.0.0.1", proxyPort, 500)) {
                ready = proc.isAlive();
                break;
            }
        }

        if (!ready) {
            boolean exited = !proc.isAlive();
            proc.destroyForcibly();
            if (p.abandoned()) {
                throw abortedStart();
            }
            try { proc.waitFor(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
            try { stderrDrain.join(2000); } catch (InterruptedException ignored) {}
            String stderr;
            synchronized (stderrTail) {
                stderr = stderrTail.toString().strip();
            }
            String why = exited
                ? ": the proxy exited with status " + proc.exitValue() + "."
                : " within " + (STARTUP_TIMEOUT_MS / 1000) + "s.";
            throw new RuntimeException(
                "Gold Lapel failed to start on port " + proxyPort + why + "\nstderr: " + stderr
            );
        }

        p.url = makeProxyUrl(upstream, proxyPort, clientTls);
    }

    /**
     * Build the argv that {@link #spawnProxy} hands to {@link ProcessBuilder}.
     * Package-private so tests can verify CLI-flag emission for top-level
     * options (mesh, disableProxyCache, etc.) without spawning the Rust
     * binary. Order: required flags first ({@code --upstream}, {@code --proxy-port}),
     * then top-level options that emit only when the user set them, then the
     * tuning-knob config map, then any caller-supplied {@code extraArgs}.
     */
    List<String> buildSpawnCmd(String binary) {
        List<String> cmd = new ArrayList<>();
        cmd.add(binary);
        cmd.add("--upstream");
        cmd.add(upstream);
        cmd.add("--proxy-port");
        cmd.add(String.valueOf(proxyPort));
        // Top-level options (promoted out of the config map) emit their own
        // CLI flags before the tuning-knob config map. Each is suppressed
        // when the user hasn't set it, so the Rust binary applies its own
        // defaults (and tuning stays aligned with the CLI/TOML/env surfaces).
        if (dashboardPortExplicit) {
            cmd.add("--dashboard-port");
            cmd.add(String.valueOf(dashboardPort));
        }
        String verboseFlag = translateLogLevel(logLevel);
        if (verboseFlag != null) {
            cmd.add(verboseFlag);
        }
        if (mode != null) {
            cmd.add("--mode");
            cmd.add(mode);
        }
        if (license != null) {
            cmd.add("--license");
            cmd.add(license);
        }
        if (configFile != null) {
            cmd.add("--config");
            cmd.add(configFile);
        }
        if (mesh) {
            cmd.add("--mesh");
        }
        if (meshTag != null) {
            cmd.add("--mesh-tag");
            cmd.add(meshTag);
        }
        // Promoted disable flags. Each maps 1:1 to a proxy CLI flag and emits
        // only when the user set it (false leaves the proxy's own default in
        // place — matches the canonical surface contract).
        if (disableProxyCache) {
            cmd.add("--disable-proxy-cache");
        }
        if (disableSqloptimize) {
            cmd.add("--disable-sqloptimize");
        }
        if (disableAutoIndexes) {
            cmd.add("--disable-auto-indexes");
        }
        cmd.addAll(configToArgs(config));
        cmd.addAll(extraArgs);
        return cmd;
    }

    /**
     * Write the one-line startup banner to {@code stream}. No-op when the
     * {@code silent} option is set. Package-private so tests can exercise the
     * routing and silent-suppression paths directly without spawning the Rust
     * binary. {@link #acquireProxy()} calls this, for the start that spawned the proxy, with {@code System.err}
     * — library code must never write to stdout unconditionally (app stdout is
     * piped, captured by test runners, consumed by CLI tools).
     */
    void printBanner(java.io.PrintStream stream) {
        if (silent) {
            return;
        }
        if (dashboardPort > 0) {
            stream.println("goldlapel → :" + proxyPort + " (proxy) | http://127.0.0.1:" + dashboardPort + " (dashboard)");
        } else {
            stream.println("goldlapel → :" + proxyPort + " (proxy)");
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
            props.setProperty("loginTimeout", String.valueOf(EAGER_CONNECT_TIMEOUT_S));
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
        // pgjdbc ignores libpq's application_name; its name is ApplicationName.
        int q = rest.indexOf('?');
        if (q >= 0) {
            rest = rest.substring(0, q) + rest.substring(q).replaceAll("([?&])application_name=", "$1ApplicationName=");
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
     * Called automatically by {@link #close()} (try-with-resources). A proxy
     * shared with other instances (same upstream) keeps running until the
     * last of them stops.
     */
    public void stop() {
        // Set first: an in-flight start() checks this after each step.
        stopped = true;
        // Drop any cached DDL patterns — they're tied to the proxy we're
        // about to release.
        ddlCache.clear();
        dashboardToken = null;
        if (internalConn != null) {
            try { internalConn.close(); } catch (SQLException ignored) {}
            internalConn = null;
        }
        process = null;
        proxyUrl = null;
        Proxy p;
        boolean last;
        synchronized (PROXIES) {
            p = proxy;
            proxy = null;
            last = p != null && --p.holders == 0;
            if (last) {
                PROXIES.remove(p.upstream, p);
            }
        }
        // Still spawning: the spawning start() sees it abandoned and kills it.
        Process proc = last ? p.process : null;
        if (proc != null && proc.isAlive()) {
            proc.destroy();
            try {
                if (!proc.waitFor(5, TimeUnit.SECONDS)) {
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

    public int getProxyPort() {
        return proxyPort;
    }

    // Package-private accessor for tests — not part of the public API.
    int getDashboardPort() {
        return dashboardPort;
    }

    /**
     * Return the dashboard port this instance's proxy is listening on
     * (typically proxy port + 1). Returned value is valid between start()
     * and stop(). Used by DDL API clients on this process.
     */
    public int dashboardPort() {
        return dashboardPort;
    }

    /**
     * Return the dashboard token the wrapper uses to authenticate against
     * /api/ddl/* on this instance's proxy. Provisioned when the proxy is spawned
     * for internally-spawned proxies; {@code null} for externally-launched
     * proxies (in that case the DDL client falls back to env/file).
     */
    public String dashboardToken() {
        return dashboardToken;
    }

    // Package-private accessor used by Utils.streamX to share the per-instance
    // DDL pattern cache. Keyed on "family:name".
    java.util.concurrent.ConcurrentHashMap<String, Map<String, Object>> ddlCache() {
        return ddlCache;
    }

    // Package-private accessors for tests to verify mesh wiring without spawning.
    boolean isMesh() {
        return mesh;
    }

    String meshTag() {
        return meshTag;
    }

    // Package-private accessors for tests to verify the three promoted disable
    // flags flow from the options bag onto the GoldLapel instance.
    boolean disableProxyCache() {
        return disableProxyCache;
    }

    boolean disableSqloptimize() {
        return disableSqloptimize;
    }

    boolean disableAutoIndexes() {
        return disableAutoIndexes;
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
    // override is provided: scoped (using) > internal. Package-private so
    // sub-API classes (DocumentsApi, StreamsApi) read through the same
    // resolution path as flat methods on this class.
    Connection resolveConn() {
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
     *     gl.documents.insert("events", "{\"type\":\"order\"}");
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
     * long count = gl.using(conn, () -> gl.documents.count("events", "{}"));
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

    // Phase 4: gl.documents.<verb>(...). See DocumentsApi.
    // Phase 5: gl.counters / gl.zsets / gl.hashes / gl.queues / gl.geos.
    //          See per-family Api classes.

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

    // Phase 5 Redis-compat families: gl.counters / gl.zsets / gl.hashes /
    // gl.queues / gl.geos. The legacy flat methods (incr, hset, zadd,
    // enqueue/dequeue, geoadd, …) are gone — see the per-family API classes
    // (CountersApi, ZsetsApi, HashesApi, QueuesApi, GeosApi).

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

    // Streams: gl.streams.<verb>(...). See StreamsApi. Proxy-owned DDL —
    // each call fetches (and caches) canonical query patterns from the
    // dashboard's /api/ddl/stream/create endpoint on first use; subsequent
    // calls use the cached patterns.

    /**
     * Fetch (and cache per-instance) canonical stream DDL + query patterns.
     * Public so {@link StreamsApi} and reactor/rxjava3 wrappers in sibling
     * artifacts can reuse the same cache.
     */
    public Map<String, String> streamPatterns(String stream) {
        Utils.validateIdentifier(stream);
        String token = dashboardToken != null ? dashboardToken : Ddl.tokenFromEnvOrFile();
        Map<String, Object> entry = Ddl.fetchPatterns(ddlCache, "stream", stream, dashboardPort, token);
        return Ddl.queryPatterns(entry);
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

            checkConfigKey(key);

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

    private static void checkConfigKey(String key) {
        String removedWith = REMOVED_CONFIG_KEYS.get(key);
        if (removedWith != null) {
            throw new IllegalArgumentException(
                "Config key '" + key + "' was removed with " + removedWith + "; drop it from your config.");
        }
        if (!VALID_CONFIG_KEYS.contains(key)) {
            throw new IllegalArgumentException("Unknown config key: " + key);
        }
    }

    // ── Internal methods ───────────────────────────────────

    private static final Pattern WITH_PORT =
        Pattern.compile("^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+):(\\d+)(.*)$");

    private static final Pattern NO_PORT =
        Pattern.compile("^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+)(.*)$");

    // libpq spells it application_name; pgjdbc only reads ApplicationName.
    private static final Pattern APP_NAME_PRESENT =
        Pattern.compile("[?&](application_name|ApplicationName)=");

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

    /**
     * The wrapper's installed version. Read from the JAR's
     * {@code Implementation-Version} manifest entry; CI sets this from the git
     * tag at publish time. Local dev / test builds return {@code "0.0.0"}.
     * Used to build the {@code application_name} marker on PG connections so
     * Gold Lapel's connections are recognisable in {@code pg_stat_activity}.
     */
    static String wrapperVersion() {
        Package pkg = GoldLapel.class.getPackage();
        if (pkg != null) {
            String v = pkg.getImplementationVersion();
            if (v != null && !v.isEmpty()) return v;
        }
        return "0.0.0";
    }

    static String applicationNameMarker() {
        return "goldlapel:java:" + wrapperVersion();
    }

    /**
     * Append {@code application_name=goldlapel:java:<version>} to {@code url}
     * unless one is already present (or {@code PGAPPNAME} is set in the env).
     * The marker identifies wrapper connections in {@code pg_stat_activity};
     * the proxy caches them like any other client. Idempotent and
     * override-respecting.
     */
    static String injectApplicationName(String url) {
        if (APP_NAME_PRESENT.matcher(url).find()) return url;
        String pgAppName = System.getenv("PGAPPNAME");
        if (pgAppName != null && !pgAppName.isEmpty()) return url;
        char sep = url.indexOf('?') >= 0 ? '&' : '?';
        return url + sep + "application_name=" + applicationNameMarker();
    }

    static String makeProxyUrl(String upstream, int port) {
        return makeProxyUrl(upstream, port, false);
    }

    /**
     * The URL the app connects to: {@code upstream} with the host replaced
     * by {@code localhost:port}, the application-name marker added, and —
     * unless the proxy serves TLS to the app ({@code clientTls}) — the
     * upstream's TLS/GSS parameters removed.
     */
    static String makeProxyUrl(String upstream, int port, boolean clientTls) {
        // Build a proxy URL: replace host with localhost and set the proxy port.
        // Uses regex instead of java.net.URI to avoid decoding percent-encoded
        // characters in passwords (e.g. %40 for @), which would corrupt the URL.

        // pg URL with explicit port: scheme://[userinfo@]host:PORT[/path][?query]
        Matcher m = WITH_PORT.matcher(upstream);
        if (m.matches()) {
            String rest = clientTls ? m.group(4) : withoutTlsParams(m.group(4));
            return injectApplicationName(m.group(1) + "localhost:" + port + rest);
        }

        // pg URL without port: scheme://[userinfo@]host[/path][?query]
        m = NO_PORT.matcher(upstream);
        if (m.matches()) {
            String rest = clientTls ? m.group(3) : withoutTlsParams(m.group(3));
            return injectApplicationName(m.group(1) + "localhost:" + port + rest);
        }

        // bare host:port or bare host — no query, and no marker (atypical
        // caller path).
        return "localhost:" + port;
    }

    // `rest` ([/path][?query][#fragment], everything after host:port) without
    // the upstream TLS/GSS parameters.
    static String withoutTlsParams(String rest) {
        int q = rest.indexOf('?');
        if (q < 0) {
            return rest;
        }
        int hash = rest.indexOf('#', q);
        String query = hash < 0 ? rest.substring(q + 1) : rest.substring(q + 1, hash);
        StringBuilder kept = new StringBuilder();
        for (String param : query.split("&")) {
            int eq = param.indexOf('=');
            String key = (eq < 0 ? param : param.substring(0, eq)).toLowerCase(Locale.ROOT);
            if (param.isEmpty() || UPSTREAM_TLS_PARAMS.contains(key)) {
                continue;
            }
            kept.append(kept.length() == 0 ? "" : "&").append(param);
        }
        return rest.substring(0, q)
            + (kept.length() == 0 ? "" : "?" + kept)
            + (hash < 0 ? "" : rest.substring(hash));
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
