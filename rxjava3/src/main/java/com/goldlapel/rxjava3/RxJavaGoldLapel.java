package com.goldlapel.rxjava3;

import com.goldlapel.GoldLapel;
import com.goldlapel.GoldLapelOptions;
import com.goldlapel.reactor.ReactiveGoldLapel;

import io.r2dbc.spi.ConnectionFactory;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import reactor.adapter.rxjava.RxJava3Adapter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * RxJava 3 façade over {@link ReactiveGoldLapel}.
 *
 * <p>Every method here is a thin wrap around a Reactor {@code Mono} or
 * {@code Flux} returning the equivalent RxJava 3 type:
 *
 * <table>
 *   <caption>Type mapping</caption>
 *   <tr><th>Reactor</th><th>RxJava 3</th></tr>
 *   <tr><td>{@code Mono<T>} (always-emits)</td><td>{@code Single<T>}</td></tr>
 *   <tr><td>{@code Mono<T>} (may be empty)</td><td>{@code Maybe<T>}</td></tr>
 *   <tr><td>{@code Mono<Void>}</td><td>{@code Completable}</td></tr>
 *   <tr><td>{@code Flux<T>}</td><td>{@code Flowable<T>}</td></tr>
 * </table>
 *
 * <p>The underlying JDBC helpers ({@code hget}, {@code zscore},
 * {@code documents.findOne}, etc.) return {@code null} when a row is not found. In Reactor those surface
 * as an empty {@code Mono}; here they map to {@code Maybe<T>} so "not found"
 * is a clean {@code onComplete} rather than a {@link java.util.NoSuchElementException}
 * that {@code monoToSingle} would raise.
 *
 * <p>This exists for ergonomics — RxJava-first codebases can write
 * {@code gl.search(...).subscribe(...)} instead of sprinkling
 * {@code RxJava3Adapter.monoToSingle(...)} over every call. There is no
 * extra logic: each method simply calls through to the reactor instance and
 * applies the appropriate adapter.
 *
 * <h2>Scoping connections</h2>
 *
 * <p>RxJava has no equivalent of Reactor's {@code ContextView}, so there is
 * no {@code using(conn, body)} method here. Every method takes an optional
 * explicit {@link Connection} parameter — pass it per call if you want to
 * pin a connection. Users who want context-scoped connections should use
 * {@link ReactiveGoldLapel} directly via {@link #reactor()}.
 *
 * <pre>{@code
 * RxJavaGoldLapel.start("postgresql://user:pass@host/db")
 *     .flatMap(gl -> gl.documents.insert("events", "{\"type\":\"signup\"}")
 *         .flatMap(inserted -> gl.documents.count("events", "{}")
 *             .flatMap(count -> gl.stop().toSingleDefault(count))))
 *     .subscribe(System.out::println);
 * }</pre>
 */
public final class RxJavaGoldLapel implements AutoCloseable {

    private final ReactiveGoldLapel inner;

    /**
     * RxJava 3 document store sub-API — accessible as
     * {@code gl.documents.<verb>(...)}. Final field (Option A from
     * cross-wrapper consensus): direct field access, mirrors
     * {@link com.goldlapel.GoldLapel#documents}.
     */
    public final RxJavaDocumentsApi documents;
    /** RxJava 3 streams sub-API — accessible as {@code gl.streams.<verb>(...)}. */
    public final RxJavaStreamsApi streams;

    RxJavaGoldLapel(ReactiveGoldLapel inner) {
        this.inner = inner;
        this.documents = new RxJavaDocumentsApi(inner.documents);
        this.streams = new RxJavaStreamsApi(inner.streams);
    }

    // ── Factory ───────────────────────────────────────────────

    /**
     * Start a Gold Lapel proxy for the given upstream URL and emit the
     * {@code RxJavaGoldLapel} once the proxy is up. Cancellation before
     * completion tears the subprocess down — no leaks.
     */
    public static Single<RxJavaGoldLapel> start(String upstream) {
        return RxJava3Adapter.monoToSingle(
            ReactiveGoldLapel.start(upstream).map(RxJavaGoldLapel::new)
        );
    }

    /**
     * Start with a configurator lambda. See
     * {@link GoldLapel#start(String, Consumer)} for available options.
     */
    public static Single<RxJavaGoldLapel> start(String upstream, Consumer<GoldLapelOptions> configurator) {
        return RxJava3Adapter.monoToSingle(
            ReactiveGoldLapel.start(upstream, configurator).map(RxJavaGoldLapel::new)
        );
    }

    // ── Lifecycle ─────────────────────────────────────────────

    /** Stop the proxy and close the internal JDBC connection. Idempotent. */
    public Completable stop() {
        return RxJava3Adapter.monoToCompletable(inner.stop());
    }

    /** Blocking close — for try-with-resources. Prefer {@link #stop()}. */
    @Override
    public void close() {
        stop().blockingAwait();
    }

    // ── Accessors ─────────────────────────────────────────────

    /** The underlying {@link ReactiveGoldLapel} — for users who want
     *  Reactor-Context-scoped {@code using(conn, ...)} or raw Mono/Flux returns. */
    public ReactiveGoldLapel reactor() { return inner; }

    /** The underlying sync {@link GoldLapel} — for advanced interop. */
    public GoldLapel sync() { return inner.sync(); }

    /** R2DBC ConnectionFactory pointing at the proxy. Use for your own reactive queries. */
    public ConnectionFactory connectionFactory() { return inner.connectionFactory(); }

    public String getUrl() { return inner.getUrl(); }
    public String getJdbcUrl() { return inner.getJdbcUrl(); }
    public String getJdbcUser() { return inner.getJdbcUser(); }
    public String getJdbcPassword() { return inner.getJdbcPassword(); }
    public int getProxyPort() { return inner.getProxyPort(); }
    public String getDashboardUrl() { return inner.getDashboardUrl(); }
    public boolean isRunning() { return inner.isRunning(); }
    public Connection connection() { return inner.connection(); }

    // ═══════════════════════════════════════════════════════════
    // Wrapper methods — Single / Completable / Flowable parity
    // ═══════════════════════════════════════════════════════════

    // ── Document store ────────────────────────────────────────
    // Moved to gl.documents.<verb>(...) — see RxJavaDocumentsApi.

    // ── Search ────────────────────────────────────────────────

    public Flowable<Map<String, Object>> search(String table, String column, String query,
            int limit, String lang, boolean highlight) {
        return RxJava3Adapter.fluxToFlowable(inner.search(table, column, query, limit, lang, highlight));
    }
    public Flowable<Map<String, Object>> search(String table, String column, String query,
            int limit, String lang, boolean highlight, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.search(table, column, query, limit, lang, highlight, conn));
    }
    public Flowable<Map<String, Object>> search(String table, String[] columns, String query,
            int limit, String lang, boolean highlight) {
        return RxJava3Adapter.fluxToFlowable(inner.search(table, columns, query, limit, lang, highlight));
    }
    public Flowable<Map<String, Object>> search(String table, String[] columns, String query,
            int limit, String lang, boolean highlight, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.search(table, columns, query, limit, lang, highlight, conn));
    }

    public Flowable<Map<String, Object>> searchFuzzy(String table, String column, String query,
            int limit, double threshold) {
        return RxJava3Adapter.fluxToFlowable(inner.searchFuzzy(table, column, query, limit, threshold));
    }
    public Flowable<Map<String, Object>> searchFuzzy(String table, String column, String query,
            int limit, double threshold, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.searchFuzzy(table, column, query, limit, threshold, conn));
    }

    public Flowable<Map<String, Object>> searchPhonetic(String table, String column, String query, int limit) {
        return RxJava3Adapter.fluxToFlowable(inner.searchPhonetic(table, column, query, limit));
    }
    public Flowable<Map<String, Object>> searchPhonetic(String table, String column, String query, int limit, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.searchPhonetic(table, column, query, limit, conn));
    }

    public Flowable<Map<String, Object>> similar(String table, String column, double[] vector, int limit) {
        return RxJava3Adapter.fluxToFlowable(inner.similar(table, column, vector, limit));
    }
    public Flowable<Map<String, Object>> similar(String table, String column, double[] vector, int limit, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.similar(table, column, vector, limit, conn));
    }

    public Flowable<Map<String, Object>> suggest(String table, String column, String prefix, int limit) {
        return RxJava3Adapter.fluxToFlowable(inner.suggest(table, column, prefix, limit));
    }
    public Flowable<Map<String, Object>> suggest(String table, String column, String prefix, int limit, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.suggest(table, column, prefix, limit, conn));
    }

    public Flowable<Map<String, Object>> facets(String table, String column, int limit) {
        return RxJava3Adapter.fluxToFlowable(inner.facets(table, column, limit));
    }
    public Flowable<Map<String, Object>> facets(String table, String column, int limit, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.facets(table, column, limit, conn));
    }
    public Flowable<Map<String, Object>> facets(String table, String column, int limit,
            String query, String queryColumn, String lang) {
        return RxJava3Adapter.fluxToFlowable(inner.facets(table, column, limit, query, queryColumn, lang));
    }
    public Flowable<Map<String, Object>> facets(String table, String column, int limit,
            String query, String queryColumn, String lang, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.facets(table, column, limit, query, queryColumn, lang, conn));
    }
    public Flowable<Map<String, Object>> facets(String table, String column, int limit,
            String query, String[] queryColumns, String lang) {
        return RxJava3Adapter.fluxToFlowable(inner.facets(table, column, limit, query, queryColumns, lang));
    }
    public Flowable<Map<String, Object>> facets(String table, String column, int limit,
            String query, String[] queryColumns, String lang, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.facets(table, column, limit, query, queryColumns, lang, conn));
    }

    public Flowable<Map<String, Object>> aggregate(String table, String column, String func) {
        return RxJava3Adapter.fluxToFlowable(inner.aggregate(table, column, func));
    }
    public Flowable<Map<String, Object>> aggregate(String table, String column, String func, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.aggregate(table, column, func, conn));
    }
    public Flowable<Map<String, Object>> aggregate(String table, String column, String func,
            String groupBy, int limit) {
        return RxJava3Adapter.fluxToFlowable(inner.aggregate(table, column, func, groupBy, limit));
    }
    public Flowable<Map<String, Object>> aggregate(String table, String column, String func,
            String groupBy, int limit, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.aggregate(table, column, func, groupBy, limit, conn));
    }

    public Completable createSearchConfig(String name) {
        return RxJava3Adapter.monoToCompletable(inner.createSearchConfig(name));
    }
    public Completable createSearchConfig(String name, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.createSearchConfig(name, conn));
    }
    public Completable createSearchConfig(String name, String copyFrom) {
        return RxJava3Adapter.monoToCompletable(inner.createSearchConfig(name, copyFrom));
    }
    public Completable createSearchConfig(String name, String copyFrom, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.createSearchConfig(name, copyFrom, conn));
    }

    // ── PubSub and queues ─────────────────────────────────────

    public Completable publish(String channel, String message) {
        return RxJava3Adapter.monoToCompletable(inner.publish(channel, message));
    }
    public Completable publish(String channel, String message, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.publish(channel, message, conn));
    }

    public Single<Thread> subscribe(String channel, BiConsumer<String, String> callback) {
        return RxJava3Adapter.monoToSingle(inner.subscribe(channel, callback));
    }
    public Single<Thread> subscribe(String channel, BiConsumer<String, String> callback, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.subscribe(channel, callback, conn));
    }
    public Single<Thread> subscribe(String channel, BiConsumer<String, String> callback, boolean blocking) {
        return RxJava3Adapter.monoToSingle(inner.subscribe(channel, callback, blocking));
    }
    public Single<Thread> subscribe(String channel, BiConsumer<String, String> callback, boolean blocking, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.subscribe(channel, callback, blocking, conn));
    }

    public Completable enqueue(String queueTable, String payloadJson) {
        return RxJava3Adapter.monoToCompletable(inner.enqueue(queueTable, payloadJson));
    }
    public Completable enqueue(String queueTable, String payloadJson, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.enqueue(queueTable, payloadJson, conn));
    }

    /** {@code Maybe} — empty when the queue is empty. */
    public Maybe<String> dequeue(String queueTable) {
        return RxJava3Adapter.monoToMaybe(inner.dequeue(queueTable));
    }
    public Maybe<String> dequeue(String queueTable, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.dequeue(queueTable, conn));
    }

    // ── Counters ──────────────────────────────────────────────

    public Single<Long> incr(String table, String key, long amount) {
        return RxJava3Adapter.monoToSingle(inner.incr(table, key, amount));
    }
    public Single<Long> incr(String table, String key, long amount, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.incr(table, key, amount, conn));
    }

    public Single<Long> getCounter(String table, String key) {
        return RxJava3Adapter.monoToSingle(inner.getCounter(table, key));
    }
    public Single<Long> getCounter(String table, String key, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.getCounter(table, key, conn));
    }

    // ── Hashes ────────────────────────────────────────────────

    public Completable hset(String table, String key, String field, String valueJson) {
        return RxJava3Adapter.monoToCompletable(inner.hset(table, key, field, valueJson));
    }
    public Completable hset(String table, String key, String field, String valueJson, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.hset(table, key, field, valueJson, conn));
    }

    /** {@code Maybe} — empty when the field is absent. */
    public Maybe<String> hget(String table, String key, String field) {
        return RxJava3Adapter.monoToMaybe(inner.hget(table, key, field));
    }
    public Maybe<String> hget(String table, String key, String field, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.hget(table, key, field, conn));
    }

    /** {@code Maybe} — empty when the key is absent. */
    public Maybe<String> hgetall(String table, String key) {
        return RxJava3Adapter.monoToMaybe(inner.hgetall(table, key));
    }
    public Maybe<String> hgetall(String table, String key, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.hgetall(table, key, conn));
    }

    public Single<Boolean> hdel(String table, String key, String field) {
        return RxJava3Adapter.monoToSingle(inner.hdel(table, key, field));
    }
    public Single<Boolean> hdel(String table, String key, String field, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.hdel(table, key, field, conn));
    }

    // ── Sorted sets ───────────────────────────────────────────

    public Completable zadd(String table, String member, double score) {
        return RxJava3Adapter.monoToCompletable(inner.zadd(table, member, score));
    }
    public Completable zadd(String table, String member, double score, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.zadd(table, member, score, conn));
    }

    public Single<Double> zincrby(String table, String member, double amount) {
        return RxJava3Adapter.monoToSingle(inner.zincrby(table, member, amount));
    }
    public Single<Double> zincrby(String table, String member, double amount, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.zincrby(table, member, amount, conn));
    }

    public Flowable<Map.Entry<String, Double>> zrange(String table, int start, int stop, boolean desc) {
        return RxJava3Adapter.fluxToFlowable(inner.zrange(table, start, stop, desc));
    }
    public Flowable<Map.Entry<String, Double>> zrange(String table, int start, int stop, boolean desc, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.zrange(table, start, stop, desc, conn));
    }

    /** {@code Maybe} — empty when the member is not in the set. */
    public Maybe<Long> zrank(String table, String member, boolean desc) {
        return RxJava3Adapter.monoToMaybe(inner.zrank(table, member, desc));
    }
    public Maybe<Long> zrank(String table, String member, boolean desc, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.zrank(table, member, desc, conn));
    }

    /** {@code Maybe} — empty when the member is not in the set. */
    public Maybe<Double> zscore(String table, String member) {
        return RxJava3Adapter.monoToMaybe(inner.zscore(table, member));
    }
    public Maybe<Double> zscore(String table, String member, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.zscore(table, member, conn));
    }

    public Single<Boolean> zrem(String table, String member) {
        return RxJava3Adapter.monoToSingle(inner.zrem(table, member));
    }
    public Single<Boolean> zrem(String table, String member, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.zrem(table, member, conn));
    }

    // ── Geo ───────────────────────────────────────────────────

    public Flowable<Map<String, Object>> georadius(String table, String geomColumn, double lon,
            double lat, double radiusMeters, int limit) {
        return RxJava3Adapter.fluxToFlowable(inner.georadius(table, geomColumn, lon, lat, radiusMeters, limit));
    }
    public Flowable<Map<String, Object>> georadius(String table, String geomColumn, double lon,
            double lat, double radiusMeters, int limit, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.georadius(table, geomColumn, lon, lat, radiusMeters, limit, conn));
    }

    public Completable geoadd(String table, String nameColumn, String geomColumn, String name,
            double lon, double lat) {
        return RxJava3Adapter.monoToCompletable(inner.geoadd(table, nameColumn, geomColumn, name, lon, lat));
    }
    public Completable geoadd(String table, String nameColumn, String geomColumn, String name,
            double lon, double lat, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.geoadd(table, nameColumn, geomColumn, name, lon, lat, conn));
    }

    /** {@code Maybe} — empty when either endpoint is missing from the table. */
    public Maybe<Double> geodist(String table, String geomColumn, String nameColumn,
            String nameA, String nameB) {
        return RxJava3Adapter.monoToMaybe(inner.geodist(table, geomColumn, nameColumn, nameA, nameB));
    }
    public Maybe<Double> geodist(String table, String geomColumn, String nameColumn,
            String nameA, String nameB, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.geodist(table, geomColumn, nameColumn, nameA, nameB, conn));
    }

    // ── Misc ──────────────────────────────────────────────────

    public Single<Long> countDistinct(String table, String column) {
        return RxJava3Adapter.monoToSingle(inner.countDistinct(table, column));
    }
    public Single<Long> countDistinct(String table, String column, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.countDistinct(table, column, conn));
    }

    /**
     * Run a Lua script server-side. Same varargs/Connection collision as
     * sync — no {@code (luaCode, String... args, Connection conn)} overload.
     * Users who need a pinned connection should call
     * {@link ReactiveGoldLapel#using(Connection, Function)} directly on
     * {@link #reactor()}.
     */
    public Maybe<String> script(String luaCode, String... args) {
        return RxJava3Adapter.monoToMaybe(inner.script(luaCode, args));
    }

    // ── Streams ────────────────────────────────────────
    // Moved to gl.streams.<verb>(...) — see RxJavaStreamsApi.

    // ── Percolator ────────────────────────────────────────────

    public Completable percolateAdd(String name, String queryId, String query) {
        return RxJava3Adapter.monoToCompletable(inner.percolateAdd(name, queryId, query));
    }
    public Completable percolateAdd(String name, String queryId, String query, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.percolateAdd(name, queryId, query, conn));
    }
    public Completable percolateAdd(String name, String queryId, String query,
            String lang, String metadataJson) {
        return RxJava3Adapter.monoToCompletable(inner.percolateAdd(name, queryId, query, lang, metadataJson));
    }
    public Completable percolateAdd(String name, String queryId, String query,
            String lang, String metadataJson, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.percolateAdd(name, queryId, query, lang, metadataJson, conn));
    }

    public Flowable<Map<String, Object>> percolate(String name, String text) {
        return RxJava3Adapter.fluxToFlowable(inner.percolate(name, text));
    }
    public Flowable<Map<String, Object>> percolate(String name, String text, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.percolate(name, text, conn));
    }
    public Flowable<Map<String, Object>> percolate(String name, String text, int limit, String lang) {
        return RxJava3Adapter.fluxToFlowable(inner.percolate(name, text, limit, lang));
    }
    public Flowable<Map<String, Object>> percolate(String name, String text, int limit, String lang, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.percolate(name, text, limit, lang, conn));
    }

    public Single<Boolean> percolateDelete(String name, String queryId) {
        return RxJava3Adapter.monoToSingle(inner.percolateDelete(name, queryId));
    }
    public Single<Boolean> percolateDelete(String name, String queryId, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.percolateDelete(name, queryId, conn));
    }

    // ── Analysis ──────────────────────────────────────────────

    public Flowable<Map<String, Object>> analyze(String text) {
        return RxJava3Adapter.fluxToFlowable(inner.analyze(text));
    }
    public Flowable<Map<String, Object>> analyze(String text, String lang) {
        return RxJava3Adapter.fluxToFlowable(inner.analyze(text, lang));
    }
    public Flowable<Map<String, Object>> analyze(String text, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.analyze(text, conn));
    }
    public Flowable<Map<String, Object>> analyze(String text, String lang, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.analyze(text, lang, conn));
    }

    /** {@code Maybe} — empty when the row is not found. */
    public Maybe<Map<String, Object>> explainScore(String table, String column, String query,
            String idColumn, Object idValue) {
        return RxJava3Adapter.monoToMaybe(inner.explainScore(table, column, query, idColumn, idValue));
    }
    public Maybe<Map<String, Object>> explainScore(String table, String column, String query,
            String idColumn, Object idValue, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.explainScore(table, column, query, idColumn, idValue, conn));
    }
    public Maybe<Map<String, Object>> explainScore(String table, String column, String query,
            String idColumn, Object idValue, String lang) {
        return RxJava3Adapter.monoToMaybe(inner.explainScore(table, column, query, idColumn, idValue, lang));
    }
    public Maybe<Map<String, Object>> explainScore(String table, String column, String query,
            String idColumn, Object idValue, String lang, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.explainScore(table, column, query, idColumn, idValue, lang, conn));
    }
}
