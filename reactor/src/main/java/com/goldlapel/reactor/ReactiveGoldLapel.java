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
 *     .flatMap(gl -> gl.docInsert("events", "{\"type\":\"signup\"}")
 *         .thenMany(gl.search("events", "type", "signup"))
 *         .collectList()
 *         .flatMap(results -> gl.stop().thenReturn(results)))
 *     .subscribe(System.out::println);
 * }</pre>
 *
 * <h2>Design</h2>
 *
 * <p>The wrapper methods ({@link #docInsert}, {@link #search}, etc.) delegate
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
 * gl.using(conn, g -> g.docInsert("events", json)
 *         .then(g.docCount("events", "{}")))
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

    private final GoldLapel sync;
    private final ConnectionFactory r2dbcFactory;

    private ReactiveGoldLapel(GoldLapel sync, ConnectionFactory r2dbcFactory) {
        this.sync = sync;
        this.r2dbcFactory = r2dbcFactory;
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
        int port = gl.getPort();
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

    public int getPort() { return sync.getPort(); }

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
     *     g.docInsert("events", json)
     *      .then(g.docCount("events", "{}"))
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
    private <T> Mono<T> call(Connection explicit, ConnFunction<T> body) {
        return Mono.deferContextual(ctx -> {
            Connection conn = resolveConn(ctx, explicit);
            return Mono.<T>fromCallable(() -> body.apply(conn))
                .subscribeOn(Schedulers.boundedElastic());
        });
    }

    // Void variant — Mono<Void> with no value on completion.
    private Mono<Void> run(Connection explicit, ConnRunnable body) {
        return Mono.deferContextual(ctx -> {
            Connection conn = resolveConn(ctx, explicit);
            return Mono.<Void>fromCallable(() -> { body.apply(conn); return null; })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
        });
    }

    // Stream variant — expose a List<T>-returning helper as Flux<T>.
    private <T> Flux<T> flux(Connection explicit, ConnFunction<List<T>> body) {
        return this.<List<T>>call(explicit, body).flatMapMany(Flux::fromIterable);
    }

    @FunctionalInterface
    private interface ConnFunction<T> {
        T apply(Connection conn) throws SQLException;
    }

    @FunctionalInterface
    private interface ConnRunnable {
        void apply(Connection conn) throws SQLException;
    }

    // ═══════════════════════════════════════════════════════════
    // Wrapper methods — Mono<T> / Flux<T> parity with sync API
    // ═══════════════════════════════════════════════════════════

    // ── Document store ────────────────────────────────────────

    public Mono<Map<String, Object>> docInsert(String collection, String documentJson) {
        return call(null, c -> Utils.docInsert(c, collection, documentJson));
    }
    public Mono<Map<String, Object>> docInsert(String collection, String documentJson, Connection conn) {
        return call(conn, c -> Utils.docInsert(c, collection, documentJson));
    }

    public Flux<Map<String, Object>> docInsertMany(String collection, List<String> documents) {
        return flux(null, c -> Utils.docInsertMany(c, collection, documents));
    }
    public Flux<Map<String, Object>> docInsertMany(String collection, List<String> documents, Connection conn) {
        return flux(conn, c -> Utils.docInsertMany(c, collection, documents));
    }

    public Flux<Map<String, Object>> docFind(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip) {
        return flux(null, c -> Utils.docFind(c, collection, filterJson, sortJson, limit, skip));
    }
    public Flux<Map<String, Object>> docFind(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, Connection conn) {
        return flux(conn, c -> Utils.docFind(c, collection, filterJson, sortJson, limit, skip));
    }

    public Mono<Map<String, Object>> docFindOne(String collection, String filterJson) {
        return call(null, c -> Utils.docFindOne(c, collection, filterJson));
    }
    public Mono<Map<String, Object>> docFindOne(String collection, String filterJson, Connection conn) {
        return call(conn, c -> Utils.docFindOne(c, collection, filterJson));
    }

    public Flux<Map<String, Object>> docFindCursor(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, int batchSize) {
        return cursorFlux(null, c -> Utils.docFindCursor(c, collection, filterJson, sortJson, limit, skip, batchSize));
    }
    public Flux<Map<String, Object>> docFindCursor(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, int batchSize, Connection conn) {
        return cursorFlux(conn, c -> Utils.docFindCursor(c, collection, filterJson, sortJson, limit, skip, batchSize));
    }

    // Iterator→Flux bridge: pull batches on boundedElastic via generate().
    private <T> Flux<T> cursorFlux(Connection explicit, ConnFunction<Iterator<T>> openCursor) {
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

    public Mono<Integer> docUpdate(String collection, String filterJson, String updateJson) {
        return call(null, c -> Utils.docUpdate(c, collection, filterJson, updateJson));
    }
    public Mono<Integer> docUpdate(String collection, String filterJson, String updateJson, Connection conn) {
        return call(conn, c -> Utils.docUpdate(c, collection, filterJson, updateJson));
    }

    public Mono<Integer> docUpdateOne(String collection, String filterJson, String updateJson) {
        return call(null, c -> Utils.docUpdateOne(c, collection, filterJson, updateJson));
    }
    public Mono<Integer> docUpdateOne(String collection, String filterJson, String updateJson, Connection conn) {
        return call(conn, c -> Utils.docUpdateOne(c, collection, filterJson, updateJson));
    }

    public Mono<Integer> docDelete(String collection, String filterJson) {
        return call(null, c -> Utils.docDelete(c, collection, filterJson));
    }
    public Mono<Integer> docDelete(String collection, String filterJson, Connection conn) {
        return call(conn, c -> Utils.docDelete(c, collection, filterJson));
    }

    public Mono<Integer> docDeleteOne(String collection, String filterJson) {
        return call(null, c -> Utils.docDeleteOne(c, collection, filterJson));
    }
    public Mono<Integer> docDeleteOne(String collection, String filterJson, Connection conn) {
        return call(conn, c -> Utils.docDeleteOne(c, collection, filterJson));
    }

    public Mono<Map<String, Object>> docFindOneAndUpdate(String collection, String filterJson,
            String updateJson) {
        return call(null, c -> Utils.docFindOneAndUpdate(c, collection, filterJson, updateJson));
    }
    public Mono<Map<String, Object>> docFindOneAndUpdate(String collection, String filterJson,
            String updateJson, Connection conn) {
        return call(conn, c -> Utils.docFindOneAndUpdate(c, collection, filterJson, updateJson));
    }

    public Mono<Map<String, Object>> docFindOneAndDelete(String collection, String filterJson) {
        return call(null, c -> Utils.docFindOneAndDelete(c, collection, filterJson));
    }
    public Mono<Map<String, Object>> docFindOneAndDelete(String collection, String filterJson, Connection conn) {
        return call(conn, c -> Utils.docFindOneAndDelete(c, collection, filterJson));
    }

    public Flux<String> docDistinct(String collection, String field, String filterJson) {
        return flux(null, c -> Utils.docDistinct(c, collection, field, filterJson));
    }
    public Flux<String> docDistinct(String collection, String field, String filterJson, Connection conn) {
        return flux(conn, c -> Utils.docDistinct(c, collection, field, filterJson));
    }

    public Mono<Long> docCount(String collection, String filterJson) {
        return call(null, c -> Utils.docCount(c, collection, filterJson));
    }
    public Mono<Long> docCount(String collection, String filterJson, Connection conn) {
        return call(conn, c -> Utils.docCount(c, collection, filterJson));
    }

    public Mono<Void> docCreateIndex(String collection, List<String> keys) {
        return run(null, c -> Utils.docCreateIndex(c, collection, keys));
    }
    public Mono<Void> docCreateIndex(String collection, List<String> keys, Connection conn) {
        return run(conn, c -> Utils.docCreateIndex(c, collection, keys));
    }

    public Flux<Map<String, Object>> docAggregate(String collection, String pipelineJson) {
        return flux(null, c -> Utils.docAggregate(c, collection, pipelineJson));
    }
    public Flux<Map<String, Object>> docAggregate(String collection, String pipelineJson, Connection conn) {
        return flux(conn, c -> Utils.docAggregate(c, collection, pipelineJson));
    }

    // docWatch uses a background Thread in sync; the reactive equivalent
    // keeps the same contract — returns a Mono<Thread> that callers can
    // still interrupt. A dedicated Flux<(channel, payload)> shape is deferred
    // to a follow-up to avoid designing a new API under pressure here.
    public Mono<Thread> docWatch(String collection, BiConsumer<String, String> callback) {
        return call(null, c -> Utils.docWatch(c, collection, callback));
    }
    public Mono<Thread> docWatch(String collection, BiConsumer<String, String> callback, Connection conn) {
        return call(conn, c -> Utils.docWatch(c, collection, callback));
    }

    public Mono<Void> docUnwatch(String collection) {
        return run(null, c -> Utils.docUnwatch(c, collection));
    }
    public Mono<Void> docUnwatch(String collection, Connection conn) {
        return run(conn, c -> Utils.docUnwatch(c, collection));
    }

    public Mono<Void> docCreateTtlIndex(String collection, int expireAfterSeconds) {
        return run(null, c -> Utils.docCreateTtlIndex(c, collection, expireAfterSeconds));
    }
    public Mono<Void> docCreateTtlIndex(String collection, int expireAfterSeconds, Connection conn) {
        return run(conn, c -> Utils.docCreateTtlIndex(c, collection, expireAfterSeconds));
    }
    public Mono<Void> docCreateTtlIndex(String collection, int expireAfterSeconds, String field) {
        return run(null, c -> Utils.docCreateTtlIndex(c, collection, expireAfterSeconds, field));
    }
    public Mono<Void> docCreateTtlIndex(String collection, int expireAfterSeconds, String field, Connection conn) {
        return run(conn, c -> Utils.docCreateTtlIndex(c, collection, expireAfterSeconds, field));
    }

    public Mono<Void> docRemoveTtlIndex(String collection) {
        return run(null, c -> Utils.docRemoveTtlIndex(c, collection));
    }
    public Mono<Void> docRemoveTtlIndex(String collection, Connection conn) {
        return run(conn, c -> Utils.docRemoveTtlIndex(c, collection));
    }

    public Mono<Void> docCreateCollection(String collection, boolean unlogged) {
        return run(null, c -> Utils.docCreateCollection(c, collection, unlogged));
    }
    public Mono<Void> docCreateCollection(String collection, boolean unlogged, Connection conn) {
        return run(conn, c -> Utils.docCreateCollection(c, collection, unlogged));
    }
    public Mono<Void> docCreateCollection(String collection) {
        return run(null, c -> Utils.docCreateCollection(c, collection));
    }
    public Mono<Void> docCreateCollection(String collection, Connection conn) {
        return run(conn, c -> Utils.docCreateCollection(c, collection));
    }

    public Mono<Void> docCreateCapped(String collection, int maxDocuments) {
        return run(null, c -> Utils.docCreateCapped(c, collection, maxDocuments));
    }
    public Mono<Void> docCreateCapped(String collection, int maxDocuments, Connection conn) {
        return run(conn, c -> Utils.docCreateCapped(c, collection, maxDocuments));
    }

    public Mono<Void> docRemoveCap(String collection) {
        return run(null, c -> Utils.docRemoveCap(c, collection));
    }
    public Mono<Void> docRemoveCap(String collection, Connection conn) {
        return run(conn, c -> Utils.docRemoveCap(c, collection));
    }

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

    public Mono<Void> enqueue(String queueTable, String payloadJson) {
        return run(null, c -> Utils.enqueue(c, queueTable, payloadJson));
    }
    public Mono<Void> enqueue(String queueTable, String payloadJson, Connection conn) {
        return run(conn, c -> Utils.enqueue(c, queueTable, payloadJson));
    }

    public Mono<String> dequeue(String queueTable) {
        return call(null, c -> Utils.dequeue(c, queueTable));
    }
    public Mono<String> dequeue(String queueTable, Connection conn) {
        return call(conn, c -> Utils.dequeue(c, queueTable));
    }

    // ── Counters ──────────────────────────────────────────────

    public Mono<Long> incr(String table, String key, long amount) {
        return call(null, c -> Utils.incr(c, table, key, amount));
    }
    public Mono<Long> incr(String table, String key, long amount, Connection conn) {
        return call(conn, c -> Utils.incr(c, table, key, amount));
    }

    public Mono<Long> getCounter(String table, String key) {
        return call(null, c -> Utils.getCounter(c, table, key));
    }
    public Mono<Long> getCounter(String table, String key, Connection conn) {
        return call(conn, c -> Utils.getCounter(c, table, key));
    }

    // ── Hashes ────────────────────────────────────────────────

    public Mono<Void> hset(String table, String key, String field, String valueJson) {
        return run(null, c -> Utils.hset(c, table, key, field, valueJson));
    }
    public Mono<Void> hset(String table, String key, String field, String valueJson, Connection conn) {
        return run(conn, c -> Utils.hset(c, table, key, field, valueJson));
    }

    public Mono<String> hget(String table, String key, String field) {
        return call(null, c -> Utils.hget(c, table, key, field));
    }
    public Mono<String> hget(String table, String key, String field, Connection conn) {
        return call(conn, c -> Utils.hget(c, table, key, field));
    }

    public Mono<String> hgetall(String table, String key) {
        return call(null, c -> Utils.hgetall(c, table, key));
    }
    public Mono<String> hgetall(String table, String key, Connection conn) {
        return call(conn, c -> Utils.hgetall(c, table, key));
    }

    public Mono<Boolean> hdel(String table, String key, String field) {
        return call(null, c -> Utils.hdel(c, table, key, field));
    }
    public Mono<Boolean> hdel(String table, String key, String field, Connection conn) {
        return call(conn, c -> Utils.hdel(c, table, key, field));
    }

    // ── Sorted sets ───────────────────────────────────────────

    public Mono<Void> zadd(String table, String member, double score) {
        return run(null, c -> Utils.zadd(c, table, member, score));
    }
    public Mono<Void> zadd(String table, String member, double score, Connection conn) {
        return run(conn, c -> Utils.zadd(c, table, member, score));
    }

    public Mono<Double> zincrby(String table, String member, double amount) {
        return call(null, c -> Utils.zincrby(c, table, member, amount));
    }
    public Mono<Double> zincrby(String table, String member, double amount, Connection conn) {
        return call(conn, c -> Utils.zincrby(c, table, member, amount));
    }

    public Flux<Map.Entry<String, Double>> zrange(String table, int start, int stop, boolean desc) {
        return flux(null, c -> Utils.zrange(c, table, start, stop, desc));
    }
    public Flux<Map.Entry<String, Double>> zrange(String table, int start, int stop, boolean desc, Connection conn) {
        return flux(conn, c -> Utils.zrange(c, table, start, stop, desc));
    }

    public Mono<Long> zrank(String table, String member, boolean desc) {
        return call(null, c -> Utils.zrank(c, table, member, desc));
    }
    public Mono<Long> zrank(String table, String member, boolean desc, Connection conn) {
        return call(conn, c -> Utils.zrank(c, table, member, desc));
    }

    public Mono<Double> zscore(String table, String member) {
        return call(null, c -> Utils.zscore(c, table, member));
    }
    public Mono<Double> zscore(String table, String member, Connection conn) {
        return call(conn, c -> Utils.zscore(c, table, member));
    }

    public Mono<Boolean> zrem(String table, String member) {
        return call(null, c -> Utils.zrem(c, table, member));
    }
    public Mono<Boolean> zrem(String table, String member, Connection conn) {
        return call(conn, c -> Utils.zrem(c, table, member));
    }

    // ── Geo ───────────────────────────────────────────────────

    public Flux<Map<String, Object>> georadius(String table, String geomColumn, double lon,
            double lat, double radiusMeters, int limit) {
        return flux(null, c -> Utils.georadius(c, table, geomColumn, lon, lat, radiusMeters, limit));
    }
    public Flux<Map<String, Object>> georadius(String table, String geomColumn, double lon,
            double lat, double radiusMeters, int limit, Connection conn) {
        return flux(conn, c -> Utils.georadius(c, table, geomColumn, lon, lat, radiusMeters, limit));
    }

    public Mono<Void> geoadd(String table, String nameColumn, String geomColumn, String name,
            double lon, double lat) {
        return run(null, c -> Utils.geoadd(c, table, nameColumn, geomColumn, name, lon, lat));
    }
    public Mono<Void> geoadd(String table, String nameColumn, String geomColumn, String name,
            double lon, double lat, Connection conn) {
        return run(conn, c -> Utils.geoadd(c, table, nameColumn, geomColumn, name, lon, lat));
    }

    public Mono<Double> geodist(String table, String geomColumn, String nameColumn,
            String nameA, String nameB) {
        return call(null, c -> Utils.geodist(c, table, geomColumn, nameColumn, nameA, nameB));
    }
    public Mono<Double> geodist(String table, String geomColumn, String nameColumn,
            String nameA, String nameB, Connection conn) {
        return call(conn, c -> Utils.geodist(c, table, geomColumn, nameColumn, nameA, nameB));
    }

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

    // ── Streams ───────────────────────────────────────────────
    //
    // Each stream op fetches canonical DDL + query patterns from the proxy
    // on first use (cached thereafter). The DDL fetch runs in the blocking
    // scheduler because it's a one-shot HTTP round-trip — not a hot path.

    public Mono<Long> streamAdd(String stream, String payload) {
        return call(null, c -> Utils.streamAdd(c, stream, payload, sync.streamPatterns(stream)));
    }
    public Mono<Long> streamAdd(String stream, String payload, Connection conn) {
        return call(conn, c -> Utils.streamAdd(c, stream, payload, sync.streamPatterns(stream)));
    }

    public Mono<Void> streamCreateGroup(String stream, String group) {
        return run(null, c -> Utils.streamCreateGroup(c, stream, group, sync.streamPatterns(stream)));
    }
    public Mono<Void> streamCreateGroup(String stream, String group, Connection conn) {
        return run(conn, c -> Utils.streamCreateGroup(c, stream, group, sync.streamPatterns(stream)));
    }

    public Flux<Map<String, Object>> streamRead(String stream, String group,
            String consumer, int count) {
        return flux(null, c -> Utils.streamRead(c, stream, group, consumer, count, sync.streamPatterns(stream)));
    }
    public Flux<Map<String, Object>> streamRead(String stream, String group,
            String consumer, int count, Connection conn) {
        return flux(conn, c -> Utils.streamRead(c, stream, group, consumer, count, sync.streamPatterns(stream)));
    }

    public Mono<Boolean> streamAck(String stream, String group, long messageId) {
        return call(null, c -> Utils.streamAck(c, stream, group, messageId, sync.streamPatterns(stream)));
    }
    public Mono<Boolean> streamAck(String stream, String group, long messageId, Connection conn) {
        return call(conn, c -> Utils.streamAck(c, stream, group, messageId, sync.streamPatterns(stream)));
    }

    public Flux<Map<String, Object>> streamClaim(String stream, String group,
            String consumer, long minIdleMs) {
        return flux(null, c -> Utils.streamClaim(c, stream, group, consumer, minIdleMs, sync.streamPatterns(stream)));
    }
    public Flux<Map<String, Object>> streamClaim(String stream, String group,
            String consumer, long minIdleMs, Connection conn) {
        return flux(conn, c -> Utils.streamClaim(c, stream, group, consumer, minIdleMs, sync.streamPatterns(stream)));
    }

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
