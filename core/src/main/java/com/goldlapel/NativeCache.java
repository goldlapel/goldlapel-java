package com.goldlapel;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
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

    // --- native-cache telemetry tuning ---
    //
    // Demand-driven model (matches goldlapel-python cache.py): the wrapper has
    // NO background timer. Cache counters increment on cache ops (free);
    // state-change events are emitted synchronously when a relevant counter
    // crosses a threshold; snapshot replies are sent only when the proxy asks
    // via ?:<request>.
    //
    // Eviction-rate sliding window. cache_full fires when >= EVICT_RATE_HIGH
    // of the last EVICT_RATE_WINDOW puts caused an eviction; cache_recovered
    // fires when the rate falls back below EVICT_RATE_LOW.
    static final int EVICT_RATE_WINDOW = 200;
    static final double EVICT_RATE_HIGH = 0.5;  // 50% of recent puts evicted -> cache_full
    static final double EVICT_RATE_LOW = 0.1;   // <= 10% -> cache_recovered

    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> tableIndex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> accessOrder = new ConcurrentHashMap<>();
    private final AtomicLong counter = new AtomicLong(0);
    final int maxEntries;
    final boolean enabled;
    // Wrapper-side native-cache opt-out. When true, the cache acts as a no-op
    // pass-through: get() always returns null (and ticks misses for proxy
    // visibility), put() never stores. Distinct from `enabled` (which is the
    // env-var GOLDLAPEL_NATIVE_CACHE kill-switch) and from capacity=0 (which
    // forces users to lose their tuned size to toggle the layer). Lets users
    // keep their cacheSize and toggle the layer with a separate flag.
    //
    // volatile (not final) so GoldLapel.start() can flip it on the existing
    // singleton at startup time (env-var or constructor sets the initial
    // default; the option setter from start() overrides). Mirrors the
    // .NET wrapper's `volatile bool _disableNativeCache` pattern. Volatile
    // because it's read on every get()/put() from any caller thread and
    // written from the start() thread; without it a stale read could let a
    // `put` sneak through after the user opted out.
    volatile boolean disabled;

    private volatile boolean invalidationConnected = false;
    private volatile boolean invalidationStop = false;
    private Thread invalidationThread;
    // Volatile because it's written by the recv-loop thread and read by
    // cache-op threads in `sendLine` (state-change emissions) and by the
    // shutdown path in `stopInvalidation`. Without volatile a reader could
    // see a stale null after the recv thread connected, so the very first
    // `S:wrapper_connected` could be silently dropped on slower CPUs.
    private volatile Socket invalidationSocket;
    private int invalidationPort;
    private int reconnectAttempt = 0;

    final AtomicLong statsHits = new AtomicLong(0);
    final AtomicLong statsMisses = new AtomicLong(0);
    final AtomicLong statsInvalidations = new AtomicLong(0);
    // native-cache telemetry: eviction counter — bumped in evictOne(). Atomic so the
    // existing concurrent-access tests stay lock-free on the hot path.
    final AtomicLong statsEvictions = new AtomicLong(0);

    // native-cache telemetry: stable wrapper identity for the lifetime of the process.
    // Lets the proxy aggregate per-wrapper across reconnects.
    private final String wrapperId = UUID.randomUUID().toString();
    private static final String WRAPPER_LANG = "java";
    private final String wrapperVersion;

    // Set GOLDLAPEL_REPORT_STATS=false to disable all snapshot replies and
    // state-change emissions (cache continues to function — only telemetry
    // output is suppressed). Volatile (not final) so test code can flip it
    // without reflection-on-final, which is brittle on Java 17+.
    private volatile boolean reportStats;

    // Sliding window for eviction-rate state-change detection. Bounded ring;
    // updates are O(1) amortised. Guarded by `evictWindowLock` so the latched
    // state flag flip is atomic with the rate computation.
    private final byte[] recentEvictions = new byte[EVICT_RATE_WINDOW];
    private int recentEvictionsLen = 0;       // number of valid entries (grows to WINDOW)
    private int recentEvictionsIdx = 0;       // next write index once at capacity
    private boolean stateCacheFull = false;   // latched — only flip on transition
    private final Object evictWindowLock = new Object();

    // Synchronizes writes from the recv thread (replies to ?:) and any
    // cache-op thread (state-change emissions). The socket is a single
    // full-duplex stream; concurrent writes would interleave bytes.
    private final Object sendLock = new Object();

    // Send strategy is pluggable for tests: production sends to the live
    // socket, unit tests inject a Consumer<String> that captures emissions.
    // Volatile so the recv thread sees writes from the test thread.
    private volatile Consumer<String> sendOverride = null;

    private static NativeCache instance;
    private static Thread shutdownHook;

    public NativeCache() {
        this(envCapacity(), envEnabled(), envReportStats(), envDisabled());
    }

    /**
     * Test-only constructor with explicit overrides. Package-private.
     * Defaults {@code disabled} to false so existing call sites keep their
     * current semantics.
     */
    NativeCache(int capacity, boolean enabled, boolean reportStats) {
        this(capacity, enabled, reportStats, false);
    }

    /** Test-only constructor including the disabled toggle. Package-private. */
    NativeCache(int capacity, boolean enabled, boolean reportStats, boolean disabled) {
        this.maxEntries = capacity;
        this.enabled = enabled;
        this.reportStats = reportStats;
        this.disabled = disabled;
        // Read package version from JAR manifest. Falls back to "unknown" in
        // dev / IDE runs where the class wasn't loaded from a packaged JAR.
        String v;
        try {
            v = NativeCache.class.getPackage().getImplementationVersion();
        } catch (Exception ignored) {
            v = null;
        }
        this.wrapperVersion = v != null ? v : "unknown";
    }

    private static int envCapacity() {
        String sizeStr = System.getenv("GOLDLAPEL_NATIVE_CACHE_SIZE");
        return sizeStr != null ? Integer.parseInt(sizeStr) : 32768;
    }

    private static boolean envEnabled() {
        String s = System.getenv("GOLDLAPEL_NATIVE_CACHE");
        return s == null || !"false".equalsIgnoreCase(s);
    }

    private static boolean envReportStats() {
        String s = System.getenv("GOLDLAPEL_REPORT_STATS");
        return s == null || !"false".equalsIgnoreCase(s);
    }

    private static boolean envDisabled() {
        String s = System.getenv("GOLDLAPEL_DISABLE_NATIVE_CACHE");
        return s != null && "true".equalsIgnoreCase(s);
    }

    public static synchronized NativeCache getInstance() {
        if (instance == null) {
            instance = new NativeCache();
            instance.registerShutdownHook();
        }
        return instance;
    }

    public static synchronized void reset() {
        if (instance != null) {
            instance.stopInvalidation();
            instance = null;
        }
        // Drop the shutdown hook so a new one (with a fresh instance ref) can
        // register on the next getInstance(). Tests that reset between cases
        // would otherwise pile up dead hooks.
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // JVM already shutting down — no-op.
            }
            shutdownHook = null;
        }
    }

    public boolean isConnected() { return invalidationConnected; }
    public boolean isEnabled() { return enabled; }
    public int size() { return cache.size(); }

    /**
     * Whether the wrapper-side native cache is currently opted out. Mirrors the
     * {@code GoldLapelOptions.disableNativeCache} flag — when {@code true},
     * {@link #get} returns null (incrementing the miss counter) and
     * {@link #put} silently drops. The invalidation thread keeps running so
     * snapshot replies still reach the proxy.
     */
    public boolean isDisabled() { return disabled; }

    /**
     * Flip the native-cache opt-out flag at runtime. Called by
     * {@link GoldLapel#start(String, java.util.function.Consumer)} to push
     * {@code GoldLapelOptions.disableNativeCache} onto the singleton before the
     * invalidation thread connects, so the very first {@code wrapper_connected}
     * snapshot carries the correct {@code disabled} field.
     *
     * <p>Precedence: the {@code GOLDLAPEL_DISABLE_NATIVE_CACHE} env var (read at
     * singleton construction time) wins. {@link GoldLapel#start} only invokes
     * this setter when the env var didn't already force the flag on, so an
     * env-var-true session can never be silently re-enabled by an option.
     */
    public void setDisabled(boolean value) {
        this.disabled = value;
    }

    // --- Cache operations ---

    public CacheEntry get(String sql, Object[] params) {
        return get(sql, params, 0L);
    }

    /**
     * Native-cache lookup folding a per-connection unsafe-GUC state hash
     * into the cache key. Two connections that have set different unsafe
     * GUCs ({@code app.user_id}, {@code role}, {@code search_path}, etc.)
     * never share a cache slot — see {@link GucState} for the security
     * rationale. Pass {@code 0} for {@code gucStateHash} to use the
     * baseline (no unsafe GUCs set) slot, which matches the existing
     * 2-arg overload's behaviour.
     */
    public CacheEntry get(String sql, Object[] params, long gucStateHash) {
        if (!enabled || !invalidationConnected) return null;
        // disableNativeCache — wrapper-side native-cache opt-out. Tick misses so
        // the proxy still sees per-query traffic in the snapshot; hits stay
        // zero by definition.
        if (disabled) {
            statsMisses.incrementAndGet();
            return null;
        }
        String key = makeKey(sql, params, gucStateHash);
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
        put(sql, params, rows, columns, 0L);
    }

    /**
     * Native-cache store folding a per-connection unsafe-GUC state hash
     * into the cache key — see {@link #get(String, Object[], long)}.
     */
    public void put(String sql, Object[] params, List<Object[]> rows, String[] columns, long gucStateHash) {
        if (!enabled || !invalidationConnected) return;
        // disableNativeCache — silently drop. No store, no eviction, no state-change.
        if (disabled) return;
        String key = makeKey(sql, params, gucStateHash);
        if (key == null) return;
        Set<String> tables = extractTables(sql);
        boolean evicted = false;
        if (!cache.containsKey(key) && cache.size() >= maxEntries) {
            evictOne();
            evicted = true;
        }
        cache.put(key, new CacheEntry(rows, columns, tables));
        accessOrder.put(key, counter.incrementAndGet());
        for (String table : tables) {
            tableIndex.computeIfAbsent(table, k -> ConcurrentHashMap.newKeySet()).add(key);
        }
        recordEviction(evicted);
        // Eviction-rate threshold check happens outside the window lock — emit
        // may take `sendLock` and we don't want to nest locks.
        maybeEmitEvictionRateStateChange();
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
        synchronized (this) {
            long count = cache.size();
            cache.clear();
            tableIndex.clear();
            accessOrder.clear();
            statsInvalidations.addAndGet(count);
        }
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

                // native-cache telemetry: emit `wrapper_connected` on the freshly-wired
                // socket. Done before entering the recv loop so it's the very
                // first line on the connection.
                emitStateChange("wrapper_connected");

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
                // Drop the socket reference under sendLock so any concurrent
                // emitter doesn't race a write against socket close.
                synchronized (sendLock) {
                    if (invalidationSocket != null) {
                        try { invalidationSocket.close(); } catch (IOException ignored) {}
                        invalidationSocket = null;
                    }
                }
                if (invalidationConnected) {
                    invalidationConnected = false;
                    invalidateAll();
                }
            }

            if (invalidationStop) break;
            int delay = Math.min(1 << reconnectAttempt, 15);
            reconnectAttempt++;
            try { Thread.sleep(delay * 1000L); } catch (InterruptedException e) { break; }
        }
    }

    void processSignal(String line) {
        // Backwards-compat: unknown prefixes are silently ignored. Older
        // proxies sent only `I:`, `C:`, and `P:` (keepalive); newer proxies
        // may add request types here.
        if (line.startsWith("I:")) {
            String table = line.substring(2).trim();
            if ("*".equals(table)) {
                invalidateAll();
            } else {
                invalidateTable(table);
            }
        } else if (line.startsWith("?:")) {
            // Snapshot request from the proxy. Reply with R:<json>.
            processRequest(line.substring(2));
        }
        // C: (config), P: (ping), and anything else — ignored.
    }

    // --- native-cache telemetry: sliding window + state-change emission ---

    private void recordEviction(boolean evicted) {
        synchronized (evictWindowLock) {
            byte v = (byte) (evicted ? 1 : 0);
            if (recentEvictionsLen < EVICT_RATE_WINDOW) {
                recentEvictions[recentEvictionsLen++] = v;
            } else {
                recentEvictions[recentEvictionsIdx] = v;
                recentEvictionsIdx = (recentEvictionsIdx + 1) % EVICT_RATE_WINDOW;
            }
        }
    }

    /**
     * Build the native-cache snapshot the proxy aggregates per-tick. Counter
     * reads use the existing AtomicLong getters — no critical section needed;
     * the proxy computes deltas across ticks and tolerates per-field skew.
     */
    Map<String, Object> buildSnapshot() {
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("wrapper_id", wrapperId);
        snap.put("lang", WRAPPER_LANG);
        snap.put("version", wrapperVersion);
        snap.put("hits", statsHits.get());
        snap.put("misses", statsMisses.get());
        snap.put("evictions", statsEvictions.get());
        snap.put("invalidations", statsInvalidations.get());
        snap.put("current_size_entries", (long) cache.size());
        snap.put("capacity_entries", (long) maxEntries);
        // Native-cache opt-out marker — only emitted when the wrapper is running
        // with disableNativeCache=true, so the proxy can distinguish "native
        // cache disabled by config" from "native cache underperforming". Absent
        // in the default case to keep the common-path snapshot stable.
        if (disabled) {
            snap.put("disabled", true);
        }
        return snap;
    }

    /**
     * Serialize a line write under sendLock. Best-effort — socket errors are
     * swallowed (the recv loop will detect the broken connection on its next
     * iteration and reconnect). Test override path bypasses the socket
     * entirely so tests can capture emissions without spinning a TCP server.
     */
    void sendLine(String line) {
        if (!reportStats) return;
        Consumer<String> override = sendOverride;
        if (override != null) {
            override.accept(line);
            return;
        }
        String payload = line.endsWith("\n") ? line : line + "\n";
        synchronized (sendLock) {
            Socket sock = invalidationSocket;
            if (sock == null) return;
            try {
                OutputStream out = sock.getOutputStream();
                out.write(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                out.flush();
            } catch (IOException ignored) {
                // Connection dead — recv loop will rebuild on next iteration.
                // Don't try to repair here; we'd race the reconnect logic.
            }
        }
    }

    /** Emit S:<json> with snapshot + state name. */
    void emitStateChange(String state) {
        if (!reportStats) return;
        Map<String, Object> snap = buildSnapshot();
        snap.put("state", state);
        snap.put("ts_ms", System.currentTimeMillis());
        sendLine("S:" + jsonObject(snap));
    }

    /** Emit R:<json> snapshot reply to a ?:<request>. */
    void emitResponse() {
        if (!reportStats) return;
        Map<String, Object> snap = buildSnapshot();
        snap.put("ts_ms", System.currentTimeMillis());
        sendLine("R:" + jsonObject(snap));
    }

    /**
     * Check the eviction-rate sliding window and emit a state change if the
     * latched state should flip. Hysteresis-guarded: crossing HIGH emits
     * cache_full; falling back below LOW emits cache_recovered; rates between
     * LOW and HIGH leave the latched state unchanged (no flapping).
     */
    private void maybeEmitEvictionRateStateChange() {
        String emit = null;
        synchronized (evictWindowLock) {
            // Need at least a full window before reporting state — a single
            // eviction in 3 puts is noise.
            if (recentEvictionsLen < EVICT_RATE_WINDOW) return;
            int sum = 0;
            for (int i = 0; i < recentEvictionsLen; i++) sum += recentEvictions[i];
            double rate = (double) sum / recentEvictionsLen;
            if (!stateCacheFull && rate >= EVICT_RATE_HIGH) {
                stateCacheFull = true;
                emit = "cache_full";
            } else if (stateCacheFull && rate <= EVICT_RATE_LOW) {
                stateCacheFull = false;
                emit = "cache_recovered";
            }
        }
        // Emit outside the window lock — emitStateChange takes sendLock and
        // may block on a socket write; never nest locks across I/O.
        if (emit != null) emitStateChange(emit);
    }

    /**
     * Handle ?:<request> from the proxy. Today the only request is `snapshot`
     * — the proxy asks for a current counter snapshot and we reply with
     * R:<json>. Future request types can extend this without breaking older
     * proxies (they'd ignore unknown R: lines, but only the proxy that sent
     * ?:<x> will be expecting a reply, so the contract is local to the
     * request type). Empty body is treated as snapshot for forward-compat.
     */
    void processRequest(String raw) {
        String body = raw == null ? "" : raw.trim();
        if (body.isEmpty() || "snapshot".equals(body)) {
            emitResponse();
        }
    }

    /**
     * Emit a final `wrapper_disconnected` snapshot before shutdown. Called
     * from the JVM shutdown hook — best effort; the socket may already be
     * torn down.
     */
    public void emitWrapperDisconnected() {
        emitStateChange("wrapper_disconnected");
    }

    /** Visible for testing — install a synchronous send capture. */
    void setSendOverride(Consumer<String> override) {
        this.sendOverride = override;
    }

    /** Visible for testing — read the stable wrapper identity. */
    String getWrapperId() {
        return wrapperId;
    }

    /** Visible for testing — read the configured opt-out flag. */
    boolean isReportStats() {
        return reportStats;
    }

    /** Visible for testing — flip the opt-out flag without env var or restart. */
    void setReportStats(boolean value) {
        this.reportStats = value;
    }

    private void registerShutdownHook() {
        if (shutdownHook != null) return;
        shutdownHook = new Thread(() -> {
            try {
                emitWrapperDisconnected();
            } catch (Throwable ignored) {
                // Best effort on shutdown — never block JVM exit on telemetry.
            }
        }, "goldlapel-shutdown");
        try {
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        } catch (IllegalStateException ignored) {
            // JVM already shutting down — drop the registration.
            shutdownHook = null;
        }
    }

    // --- SQL parsing ---

    static String makeKey(String sql, Object[] params) {
        return makeKey(sql, params, 0L);
    }

    /**
     * Build the native-cache key including a per-connection unsafe-GUC state
     * hash. The hash is appended in lowercase hex so {@code 0} (the baseline
     * empty-state hash) renders as {@code "0"}, keeping the no-GUC keyspace
     * distinct from any non-zero state. Mirrors the proxy's cache-key shape
     * (proxy uses {@code {:x}}).
     */
    static String makeKey(String sql, Object[] params, long gucStateHash) {
        String paramsPart = (params == null || params.length == 0)
            ? "null"
            : Arrays.toString(params);
        return sql + "\0" + paramsPart + "\0" + Long.toHexString(gucStateHash);
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
            case "CREATE": case "ALTER": case "DROP": case "REFRESH": case "DO": case "CALL":
                return DDL_SENTINEL;
            case "MERGE":
                if (tokens.length < 3 || !"INTO".equalsIgnoreCase(tokens[1])) return null;
                return bareTable(tokens[2]);
            case "SELECT":
                boolean sawInto = false;
                String intoTarget = null;
                for (int i = 1; i < tokens.length; i++) {
                    String upper = tokens[i].toUpperCase();
                    if ("INTO".equals(upper) && !sawInto) {
                        sawInto = true;
                        continue;
                    }
                    if (sawInto && intoTarget == null) {
                        if ("TEMPORARY".equals(upper) || "TEMP".equals(upper) || "UNLOGGED".equals(upper)) {
                            continue;
                        }
                        intoTarget = tokens[i];
                        continue;
                    }
                    if (sawInto && intoTarget != null && "FROM".equals(upper)) {
                        return DDL_SENTINEL;
                    }
                    if ("FROM".equals(upper)) {
                        return null;
                    }
                }
                return null;
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

    /**
     * Multi-statement-aware write detection. A single Q wire-message body can
     * carry multiple semicolon-separated statements (e.g.
     * {@code "SET app.tenant='x'; INSERT INTO orders VALUES (1)"}); the
     * single-token {@link #detectWrite} only sees the first token (here
     * {@code SET}) and returns null, leaking the trailing INSERT's
     * invalidation. This helper splits on top-level {@code ;} (reusing
     * {@link GucState#splitStatements} so we get the same string-literal-aware
     * splitter used for SET/RESET observation), runs {@link #detectWrite} on
     * each segment, and unions the resulting invalidations. Any segment
     * returning {@link #DDL_SENTINEL} short-circuits to a full invalidation.
     *
     * <p>The return value is null when no write was detected in any segment
     * (the caller should follow the read path); otherwise a
     * {@link WriteSummary} describing what to invalidate.
     */
    static WriteSummary detectWritesMulti(String sql) {
        if (sql == null) return null;
        // Fast path — single-statement SQL avoids the splitter allocation.
        if (sql.indexOf(';') < 0) {
            String t = detectWrite(sql);
            if (t == null) return null;
            if (DDL_SENTINEL.equals(t)) return WriteSummary.ddl();
            return WriteSummary.table(t);
        }
        String[] segments = GucState.splitStatements(sql);
        // Splitter strips trailing-semicolon-only inputs to a single segment;
        // even so, fall back through the loop below so the contract stays
        // uniform.
        Set<String> tables = null;
        for (String seg : segments) {
            String t = detectWrite(seg);
            if (t == null) continue;
            if (DDL_SENTINEL.equals(t)) return WriteSummary.ddl();
            if (tables == null) tables = new HashSet<>();
            tables.add(t);
        }
        if (tables == null) return null;
        return WriteSummary.tables(tables);
    }

    /**
     * Result of {@link #detectWritesMulti} — either a DDL-class write
     * (invalidate everything) or a set of specific table names. Never empty
     * when non-null; callers treat null as "not a write."
     */
    static final class WriteSummary {
        final boolean ddl;
        final Set<String> tables;
        private WriteSummary(boolean ddl, Set<String> tables) {
            this.ddl = ddl;
            this.tables = tables;
        }
        static WriteSummary ddl() { return new WriteSummary(true, null); }
        static WriteSummary table(String t) { return new WriteSummary(false, Collections.singleton(t)); }
        static WriteSummary tables(Set<String> ts) { return new WriteSummary(false, ts); }
    }

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
        statsEvictions.incrementAndGet();
    }

    // --- native-cache telemetry: minimal JSON serializer ---
    //
    // Hand-rolled to avoid pulling in Jackson/Gson; the snapshot map is flat
    // and shape-stable so a 30-line serializer is cheaper than a dependency.
    // Mirrors goldlapel-python's `json.dumps(payload, separators=(",", ":"))`
    // — compact form, snake_case keys, no whitespace.

    static String jsonObject(Map<String, Object> map) {
        StringBuilder b = new StringBuilder(128);
        b.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> e : map.entrySet()) {
            if (!first) b.append(',');
            first = false;
            jsonString(b, e.getKey());
            b.append(':');
            jsonValue(b, e.getValue());
        }
        b.append('}');
        return b.toString();
    }

    private static void jsonValue(StringBuilder b, Object v) {
        if (v == null) {
            b.append("null");
        } else if (v instanceof String) {
            jsonString(b, (String) v);
        } else if (v instanceof Boolean) {
            b.append(((Boolean) v) ? "true" : "false");
        } else if (v instanceof Number) {
            // Long, Integer, Double — toString round-trips correctly for the
            // counter / timestamp shapes we emit. NaN / Infinity not expected.
            b.append(v.toString());
        } else {
            // Fallback — never exercised today but keeps the serializer total.
            jsonString(b, v.toString());
        }
    }

    private static void jsonString(StringBuilder b, String s) {
        b.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':  b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\b': b.append("\\b"); break;
                case '\f': b.append("\\f"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        b.append(String.format("\\u%04x", (int) c));
                    } else {
                        b.append(c);
                    }
            }
        }
        b.append('"');
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
