package com.goldlapel.reactor;

import com.goldlapel.GoldLapel;
import com.goldlapel.GoldLapelOptions;
import com.goldlapel.Utils;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.ContextView;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Reactive flavour of {@link GoldLapel} — Project Reactor + R2DBC.
 *
 * <p>Strictly additive: the sync {@link GoldLapel} class is untouched. Users
 * who want blocking JDBC continue to use {@code GoldLapel}; users who want
 * reactive get a {@code ReactiveGoldLapel} via {@link #start(String)}.
 *
 * <pre>{@code
 * ReactiveGoldLapel.start("postgresql://user:pass@host/db")
 *     .flatMap(gl -> gl.documents.insert("events", "{\"type\":\"signup\"}")
 *         .thenMany(gl.search("events", "type", "signup"))
 *         .collectList()
 *         .flatMap(results -> gl.stop().thenReturn(results)))
 *     .subscribe(System.out::println);
 * }</pre>
 *
 * <h2>Design</h2>
 *
 * <p>The wrapper methods ({@link #documents}, {@link #search}, etc.) delegate
 * to the sync {@link Utils} helpers, run on {@link Schedulers#boundedElastic()}.
 * This gives full API parity with zero behavioural drift from the sync path.
 * Reactor docs explicitly recommend {@code boundedElastic} for bridging
 * blocking JDBC into reactive pipelines.
 *
 * <p>The genuinely reactive value is {@link #connectionFactory()} — an R2DBC
 * {@link ConnectionFactory} pointing at the proxy. Build your own reactive
 * queries (via Spring Data R2DBC, {@code DatabaseClient}, etc.) against that
 * for non-blocking I/O all the way through.
 *
 * <h2>Scoping connections with {@link #using}</h2>
 *
 * <p>To pin a JDBC {@link Connection} to all reactive wrapper calls in a Mono
 * chain (analogous to the sync {@code using(conn, Runnable)}):
 *
 * <pre>{@code
 * gl.using(conn, g -> g.documents.insert("events", json)
 *         .then(g.documents.count("events", "{}")))
 *   .subscribe();
 * }</pre>
 *
 * <p>Under the hood this propagates the connection through Reactor's
 * {@link ContextView}, and each wrapper method reads it via
 * {@link Mono#deferContextual}. No {@link ThreadLocal} — safe across threads
 * and inner {@code flatMap} boundaries.
 */
public final class ReactiveGoldLapel implements AutoCloseable {

    /** Context key used by {@link #using} to scope a JDBC connection over a Mono chain. */
    static final String CONN_KEY = "com.goldlapel.reactor.conn";

    final GoldLapel sync;
    private final ConnectionFactory r2dbcFactory;

    /**
     * Reactive document store sub-API — accessible as {@code gl.documents}.
     * Final field (Option A from cross-wrapper consensus): direct field
     * access matching the sync flavour's {@link GoldLapel#documents}.
     */
    public final ReactiveDocumentsApi documents;
    /** Reactive streams sub-API — accessible as {@code gl.streams}. */
    public final ReactiveStreamsApi streams;
    /** Reactive counters sub-API — Phase 5 schema-to-core. */
    public final ReactiveCountersApi counters;
    /** Reactive sorted-sets sub-API — Phase 5 schema-to-core. */
    public final ReactiveZsetsApi zsets;
    /** Reactive hashes sub-API — Phase 5 schema-to-core. */
    public final ReactiveHashesApi hashes;
    /** Reactive queues sub-API (at-least-once with visibility timeout). */
    public final ReactiveQueuesApi queues;
    /** Reactive geo sub-API (PostGIS GEOGRAPHY-native). */
    public final ReactiveGeosApi geos;

    private ReactiveGoldLapel(GoldLapel sync, ConnectionFactory r2dbcFactory) {
        this.sync = sync;
        this.r2dbcFactory = r2dbcFactory;
        this.documents = new ReactiveDocumentsApi(this);
        this.streams = new ReactiveStreamsApi(this);
        this.counters = new ReactiveCountersApi(this);
        this.zsets = new ReactiveZsetsApi(this);
        this.hashes = new ReactiveHashesApi(this);
        this.queues = new ReactiveQueuesApi(this);
        this.geos = new ReactiveGeosApi(this);
    }

    // ── Factory ───────────────────────────────────────────────

    /**
     * Start a Gold Lapel proxy for the given upstream URL and return a
     * {@code Mono<ReactiveGoldLapel>} that emits once the proxy is up.
     *
     * <p>If the subscription is cancelled mid-spawn, the subprocess is
     * torn down — no leaks.
     */
    public static Mono<ReactiveGoldLapel> start(String upstream) {
        return start(upstream, null);
    }

    /**
     * Start a Gold Lapel proxy with the given configurator lambda.
     * See {@link GoldLapel#start(String, Consumer)} for available options.
     */
    public static Mono<ReactiveGoldLapel> start(String upstream, Consumer<GoldLapelOptions> configurator) {
        return Mono.<ReactiveGoldLapel>create(sink -> {
            // Run the actual blocking spawn on boundedElastic so we don't
            // block a non-blocking thread. Track the sync instance so we can
            // clean up on cancellation.
            final GoldLapel[] spawned = new GoldLapel[1];
            final boolean[] cancelled = new boolean[1];

            sink.onCancel(() -> {
                cancelled[0] = true;
                GoldLapel gl = spawned[0];
                if (gl != null) {
                    try { gl.stop(); } catch (RuntimeException ignored) {}
                }
            });

            Schedulers.boundedElastic().schedule(() -> {
                try {
                    GoldLapel gl = GoldLapel.start(upstream, configurator);
                    spawned[0] = gl;
                    // If the subscription got cancelled while we were spawning,
                    // stop the proxy we just started and don't emit.
                    if (cancelled[0]) {
                        try { gl.stop(); } catch (RuntimeException ignored) {}
                        return;
                    }
                    ConnectionFactory cf = buildR2dbcFactory(gl);
                    sink.success(new ReactiveGoldLapel(gl, cf));
                } catch (Throwable t) {
                    sink.error(t);
                }
            });
        });
    }

    private static ConnectionFactory buildR2dbcFactory(GoldLapel gl) {
        // Build an R2DBC ConnectionFactory pointing at the proxy. Parse the
        // JDBC URL / user / password to populate the R2DBC options — the
        // proxy URL is already a plain postgres:// URL, but R2DBC uses its
        // own ConnectionFactoryOptions DSL rather than URL strings.
        String host = "127.0.0.1";
        int port = gl.getProxyPort();
        String jdbcUrl = gl.getJdbcUrl(); // jdbc:postgresql://host:port/db?query
        String database = parseDatabase(jdbcUrl);

        ConnectionFactoryOptions.Builder builder = ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "postgresql")
            .option(ConnectionFactoryOptions.HOST, host)
            .option(ConnectionFactoryOptions.PORT, port);
        if (database != null) {
            builder.option(ConnectionFactoryOptions.DATABASE, database);
        }
        String user = gl.getJdbcUser();
        String password = gl.getJdbcPassword();
        if (user != null) {
            builder.option(ConnectionFactoryOptions.USER, user);
        }
        if (password != null) {
            builder.option(ConnectionFactoryOptions.PASSWORD, password);
        }
        return ConnectionFactories.get(builder.build());
    }

    // Extract the database name from a JDBC URL. Returns null if absent.
    // jdbc:postgresql://host:port/dbname?query → "dbname"
    static String parseDatabase(String jdbcUrl) {
        if (jdbcUrl == null) return null;
        String prefix = "jdbc:postgresql://";
        if (!jdbcUrl.startsWith(prefix)) return null;
        String rest = jdbcUrl.substring(prefix.length());
        int slash = rest.indexOf('/');
        if (slash < 0) return null;
        String afterSlash = rest.substring(slash + 1);
        int q = afterSlash.indexOf('?');
        if (q >= 0) afterSlash = afterSlash.substring(0, q);
        int h = afterSlash.indexOf('#');
        if (h >= 0) afterSlash = afterSlash.substring(0, h);
        return afterSlash.isEmpty() ? null : afterSlash;
    }

    // ── Lifecycle ─────────────────────────────────────────────

    /**
     * Stop the proxy and close the internal JDBC connection. Idempotent.
     * R2DBC connections opened via {@link #connectionFactory()} are the
     * caller's responsibility to close.
     */
    public Mono<Void> stop() {
        return Mono.<Void>fromRunnable(sync::stop)
            .subscribeOn(Schedulers.boundedElastic())
            .then();
    }

    /**
     * Blocking close for try-with-resources-ish semantics. Prefer
     * {@link #stop()} in reactive pipelines.
     */
    @Override
    public void close() {
        sync.stop();
    }

    // ── Accessors ─────────────────────────────────────────────

    /** R2DBC ConnectionFactory pointing at the proxy. Use for your own reactive queries. */
    public ConnectionFactory connectionFactory() {
        return r2dbcFactory;
    }

    /** Underlying sync {@link GoldLapel} instance — provided for advanced interop. */
    public GoldLapel sync() {
        return sync;
    }

    /** Proxy URL in {@code postgresql://…} form. Matches {@link GoldLapel#getUrl()}. */
    public String getUrl() { return sync.getUrl(); }

    public String getJdbcUrl() { return sync.getJdbcUrl(); }

    public String getJdbcUser() { return sync.getJdbcUser(); }

    public String getJdbcPassword() { return sync.getJdbcPassword(); }

    public int getProxyPort() { return sync.getProxyPort(); }

    public String getDashboardUrl() { return sync.getDashboardUrl(); }

    public boolean isRunning() { return sync.isRunning(); }

    /** Internal JDBC connection opened by {@link #start}. Used by wrapper methods by default. */
    public Connection connection() { return sync.connection(); }

    // ── using(conn) — Reactor-Context scoping ─────────────────

    /**
     * Run {@code body} with {@code conn} bound as the connection seen by all
     * reactive wrapper methods inside the resulting Mono. The binding travels
     * through Reactor's {@link ContextView} and is visible to nested
     * {@code flatMap}/{@code zip}/etc. operators — no {@link ThreadLocal}.
     *
     * <pre>{@code
     * gl.using(conn, g ->
     *     g.documents.insert("events", json)
     *      .then(g.documents.count("events", "{}"))
     * ).subscribe();
     * }</pre>
     *
     * <p>Nested {@code using} calls override on the way in and restore the
     * outer connection on the way out — standard Context semantics.
     */
    public <T> Mono<T> using(Connection conn, Function<ReactiveGoldLapel, Mono<T>> body) {
        if (conn == null) throw new IllegalArgumentException("using(conn, ...): conn must not be null");
        if (body == null) throw new IllegalArgumentException("using(conn, ...): body must not be null");
        return body.apply(this).contextWrite(ctx -> ctx.put(CONN_KEY, conn));
    }

    /** {@link Flux} variant of {@link #using(Connection, Function)}. */
    public <T> Flux<T> usingMany(Connection conn, Function<ReactiveGoldLapel, Flux<T>> body) {
        if (conn == null) throw new IllegalArgumentException("usingMany(conn, ...): conn must not be null");
        if (body == null) throw new IllegalArgumentException("usingMany(conn, ...): body must not be null");
        return body.apply(this).contextWrite(ctx -> ctx.put(CONN_KEY, conn));
    }

    // Resolve the connection for a wrapper method call: explicit conn arg >
    // Reactor Context (from using) > internal connection on the sync GoldLapel.
    private Connection resolveConn(ContextView ctx, Connection explicit) {
        if (explicit != null) return explicit;
        return ctx.getOrDefault(CONN_KEY, sync.connection());
    }

    // Core adapter: wrap a blocking ThrowingSupplier in a Mono that reads
    // the scoped conn from Reactor Context, runs on boundedElastic, and
    // propagates SQLException as Mono.error.
    <T> Mono<T> call(Connection explicit, ConnFunction<T> body) {
        return Mono.deferContextual(ctx -> {
            Connection conn = resolveConn(ctx, explicit);
            return Mono.<T>fromCallable(() -> body.apply(conn))
                .subscribeOn(Schedulers.boundedElastic());
        });
    }

    // Void variant — Mono<Void> with no value on completion.
    Mono<Void> run(Connection explicit, ConnRunnable body) {
        return Mono.deferContextual(ctx -> {
            Connection conn = resolveConn(ctx, explicit);
            return Mono.<Void>fromCallable(() -> { body.apply(conn); return null; })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
        });
    }

    // Stream variant — expose a List<T>-returning helper as Flux<T>.
    <T> Flux<T> flux(Connection explicit, ConnFunction<List<T>> body) {
        return this.<List<T>>call(explicit, body).flatMapMany(Flux::fromIterable);
    }

    @FunctionalInterface
    interface ConnFunction<T> {
        T apply(Connection conn) throws SQLException;
    }

    @FunctionalInterface
    interface ConnRunnable {
        void apply(Connection conn) throws SQLException;
    }

    // ═══════════════════════════════════════════════════════════
    // Wrapper methods — Mono<T> / Flux<T> parity with sync API
    // ═══════════════════════════════════════════════════════════

    // ── Document store: gl.documents.<verb>(...). See ReactiveDocumentsApi.
    // ── Streams:        gl.streams.<verb>(...).    See ReactiveStreamsApi.

    // Iterator→Flux bridge: pull batches on boundedElastic via generate().
    <T> Flux<T> cursorFlux(Connection explicit, ConnFunction<Iterator<T>> openCursor) {
        return Mono.<Iterator<T>>deferContextual(ctx -> {
            Connection conn = resolveConn(ctx, explicit);
            return Mono.<Iterator<T>>fromCallable(() -> openCursor.apply(conn))
                .subscribeOn(Schedulers.boundedElastic());
        }).flatMapMany(iter -> Flux.<T, Iterator<T>>generate(() -> iter, (it, sink) -> {
            try {
                if (it.hasNext()) sink.next(it.next());
                else sink.complete();
            } catch (RuntimeException e) {
                sink.error(e);
            }
            return it;
        }).subscribeOn(Schedulers.boundedElastic()));
    }

    // (doc methods moved to ReactiveDocumentsApi; access via gl.documents.<verb>)

    // ── Search ────────────────────────────────────────────────

    public Flux<Map<String, Object>> search(String table, String column, String query,
            int limit, String lang, boolean highlight) {
        return flux(null, c -> Utils.search(c, table, column, query, limit, lang, highlight));
    }
    public Flux<Map<String, Object>> search(String table, String column, String query,
            int limit, String lang, boolean highlight, Connection conn) {
        return flux(conn, c -> Utils.search(c, table, column, query, limit, lang, highlight));
    }
    public Flux<Map<String, Object>> search(String table, String[] columns, String query,
            int limit, String lang, boolean highlight) {
        return flux(null, c -> Utils.search(c, table, columns, query, limit, lang, highlight));
    }
    public Flux<Map<String, Object>> search(String table, String[] columns, String query,
            int limit, String lang, boolean highlight, Connection conn) {
        return flux(conn, c -> Utils.search(c, table, columns, query, limit, lang, highlight));
    }

    public Flux<Map<String, Object>> searchFuzzy(String table, String column, String query,
            int limit, double threshold) {
        return flux(null, c -> Utils.searchFuzzy(c, table, column, query, limit, threshold));
    }
    public Flux<Map<String, Object>> searchFuzzy(String table, String column, String query,
            int limit, double threshold, Connection conn) {
        return flux(conn, c -> Utils.searchFuzzy(c, table, column, query, limit, threshold));
    }

    public Flux<Map<String, Object>> searchPhonetic(String table, String column, String query, int limit) {
        return flux(null, c -> Utils.searchPhonetic(c, table, column, query, limit));
    }
    public Flux<Map<String, Object>> searchPhonetic(String table, String column, String query, int limit, Connection conn) {
        return flux(conn, c -> Utils.searchPhonetic(c, table, column, query, limit));
    }

    public Flux<Map<String, Object>> similar(String table, String column, double[] vector, int limit) {
        return flux(null, c -> Utils.similar(c, table, column, vector, limit));
    }
    public Flux<Map<String, Object>> similar(String table, String column, double[] vector, int limit, Connection conn) {
        return flux(conn, c -> Utils.similar(c, table, column, vector, limit));
    }

    public Flux<Map<String, Object>> suggest(String table, String column, String prefix, int limit) {
        return flux(null, c -> Utils.suggest(c, table, column, prefix, limit));
    }
    public Flux<Map<String, Object>> suggest(String table, String column, String prefix, int limit, Connection conn) {
        return flux(conn, c -> Utils.suggest(c, table, column, prefix, limit));
    }

    public Flux<Map<String, Object>> facets(String table, String column, int limit) {
        return flux(null, c -> Utils.facets(c, table, column, limit));
    }
    public Flux<Map<String, Object>> facets(String table, String column, int limit, Connection conn) {
        return flux(conn, c -> Utils.facets(c, table, column, limit));
    }
    public Flux<Map<String, Object>> facets(String table, String column, int limit,
            String query, String queryColumn, String lang) {
        return flux(null, c -> Utils.facets(c, table, column, limit, query, queryColumn, lang));
    }
    public Flux<Map<String, Object>> facets(String table, String column, int limit,
            String query, String queryColumn, String lang, Connection conn) {
        return flux(conn, c -> Utils.facets(c, table, column, limit, query, queryColumn, lang));
    }
    public Flux<Map<String, Object>> facets(String table, String column, int limit,
            String query, String[] queryColumns, String lang) {
        return flux(null, c -> Utils.facets(c, table, column, limit, query, queryColumns, lang));
    }
    public Flux<Map<String, Object>> facets(String table, String column, int limit,
            String query, String[] queryColumns, String lang, Connection conn) {
        return flux(conn, c -> Utils.facets(c, table, column, limit, query, queryColumns, lang));
    }

    public Flux<Map<String, Object>> aggregate(String table, String column, String func) {
        return flux(null, c -> Utils.aggregate(c, table, column, func));
    }
    public Flux<Map<String, Object>> aggregate(String table, String column, String func, Connection conn) {
        return flux(conn, c -> Utils.aggregate(c, table, column, func));
    }
    public Flux<Map<String, Object>> aggregate(String table, String column, String func,
            String groupBy, int limit) {
        return flux(null, c -> Utils.aggregate(c, table, column, func, groupBy, limit));
    }
    public Flux<Map<String, Object>> aggregate(String table, String column, String func,
            String groupBy, int limit, Connection conn) {
        return flux(conn, c -> Utils.aggregate(c, table, column, func, groupBy, limit));
    }

    public Mono<Void> createSearchConfig(String name) {
        return run(null, c -> Utils.createSearchConfig(c, name));
    }
    public Mono<Void> createSearchConfig(String name, Connection conn) {
        return run(conn, c -> Utils.createSearchConfig(c, name));
    }
    public Mono<Void> createSearchConfig(String name, String copyFrom) {
        return run(null, c -> Utils.createSearchConfig(c, name, copyFrom));
    }
    public Mono<Void> createSearchConfig(String name, String copyFrom, Connection conn) {
        return run(conn, c -> Utils.createSearchConfig(c, name, copyFrom));
    }

    // ── PubSub and queues ─────────────────────────────────────

    public Mono<Void> publish(String channel, String message) {
        return run(null, c -> Utils.publish(c, channel, message));
    }
    public Mono<Void> publish(String channel, String message, Connection conn) {
        return run(conn, c -> Utils.publish(c, channel, message));
    }

    public Mono<Thread> subscribe(String channel, BiConsumer<String, String> callback) {
        return call(null, c -> Utils.subscribe(c, channel, callback));
    }
    public Mono<Thread> subscribe(String channel, BiConsumer<String, String> callback, Connection conn) {
        return call(conn, c -> Utils.subscribe(c, channel, callback));
    }
    public Mono<Thread> subscribe(String channel, BiConsumer<String, String> callback, boolean blocking) {
        return call(null, c -> Utils.subscribe(c, channel, callback, blocking));
    }
    public Mono<Thread> subscribe(String channel, BiConsumer<String, String> callback, boolean blocking, Connection conn) {
        return call(conn, c -> Utils.subscribe(c, channel, callback, blocking));
    }

    // Phase 5 Redis-compat families: gl.counters / gl.zsets / gl.hashes /
    // gl.queues / gl.geos. The legacy flat methods (incr, hset, zadd,
    // enqueue/dequeue, geoadd, …) are gone — see the per-family Reactive*Api
    // classes.

    // ── Misc ──────────────────────────────────────────────────

    public Mono<Long> countDistinct(String table, String column) {
        return call(null, c -> Utils.countDistinct(c, table, column));
    }
    public Mono<Long> countDistinct(String table, String column, Connection conn) {
        return call(conn, c -> Utils.countDistinct(c, table, column));
    }

    /**
     * Run a Lua script server-side. Varargs collision with an explicit
     * Connection overload is the same footgun as sync — use {@link #using}
     * to pin a specific connection:
     *
     * <pre>{@code
     * gl.using(conn, g -> g.script("return redis.call('incr', KEYS[1])", "mykey"))
     *   .subscribe();
     * }</pre>
     */
    public Mono<String> script(String luaCode, String... args) {
        return call(null, c -> Utils.script(c, luaCode, args));
    }

    // (stream methods moved to ReactiveStreamsApi; access via gl.streams.<verb>)

    // ── Percolator ────────────────────────────────────────────

    public Mono<Void> percolateAdd(String name, String queryId, String query) {
        return run(null, c -> Utils.percolateAdd(c, name, queryId, query));
    }
    public Mono<Void> percolateAdd(String name, String queryId, String query, Connection conn) {
        return run(conn, c -> Utils.percolateAdd(c, name, queryId, query));
    }
    public Mono<Void> percolateAdd(String name, String queryId, String query,
            String lang, String metadataJson) {
        return run(null, c -> Utils.percolateAdd(c, name, queryId, query, lang, metadataJson));
    }
    public Mono<Void> percolateAdd(String name, String queryId, String query,
            String lang, String metadataJson, Connection conn) {
        return run(conn, c -> Utils.percolateAdd(c, name, queryId, query, lang, metadataJson));
    }

    public Flux<Map<String, Object>> percolate(String name, String text) {
        return flux(null, c -> Utils.percolate(c, name, text));
    }
    public Flux<Map<String, Object>> percolate(String name, String text, Connection conn) {
        return flux(conn, c -> Utils.percolate(c, name, text));
    }
    public Flux<Map<String, Object>> percolate(String name, String text, int limit, String lang) {
        return flux(null, c -> Utils.percolate(c, name, text, limit, lang));
    }
    public Flux<Map<String, Object>> percolate(String name, String text, int limit, String lang, Connection conn) {
        return flux(conn, c -> Utils.percolate(c, name, text, limit, lang));
    }

    public Mono<Boolean> percolateDelete(String name, String queryId) {
        return call(null, c -> Utils.percolateDelete(c, name, queryId));
    }
    public Mono<Boolean> percolateDelete(String name, String queryId, Connection conn) {
        return call(conn, c -> Utils.percolateDelete(c, name, queryId));
    }

    // ── Analysis ──────────────────────────────────────────────

    public Flux<Map<String, Object>> analyze(String text) {
        return flux(null, c -> Utils.analyze(c, text));
    }
    public Flux<Map<String, Object>> analyze(String text, String lang) {
        return flux(null, c -> Utils.analyze(c, text, lang));
    }
    public Flux<Map<String, Object>> analyze(String text, Connection conn) {
        return flux(conn, c -> Utils.analyze(c, text));
    }
    public Flux<Map<String, Object>> analyze(String text, String lang, Connection conn) {
        return flux(conn, c -> Utils.analyze(c, text, lang));
    }

    public Mono<Map<String, Object>> explainScore(String table, String column, String query,
            String idColumn, Object idValue) {
        return call(null, c -> Utils.explainScore(c, table, column, query, idColumn, idValue));
    }
    public Mono<Map<String, Object>> explainScore(String table, String column, String query,
            String idColumn, Object idValue, Connection conn) {
        return call(conn, c -> Utils.explainScore(c, table, column, query, idColumn, idValue));
    }
    public Mono<Map<String, Object>> explainScore(String table, String column, String query,
            String idColumn, Object idValue, String lang) {
        return call(null, c -> Utils.explainScore(c, table, column, query, idColumn, idValue, lang));
    }
    public Mono<Map<String, Object>> explainScore(String table, String column, String query,
            String idColumn, Object idValue, String lang, Connection conn) {
        return call(conn, c -> Utils.explainScore(c, table, column, query, idColumn, idValue, lang));
    }
}
