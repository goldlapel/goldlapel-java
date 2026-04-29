package com.goldlapel.reactor;

import com.goldlapel.Utils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Reactive document store sub-API — accessible as
 * {@code gl.documents} on a {@link ReactiveGoldLapel}.
 *
 * <p>Each call delegates to the sync {@link com.goldlapel.GoldLapel}'s
 * {@link com.goldlapel.DocumentsApi} for canonical patterns + DDL bookkeeping
 * (one HTTP round-trip per (family, name) per session, cached on the parent),
 * then runs the SQL on
 * {@link reactor.core.scheduler.Schedulers#boundedElastic()} — same model as
 * the sync flavour, no behavioural drift.
 */
public final class ReactiveDocumentsApi {
    private final ReactiveGoldLapel gl;

    ReactiveDocumentsApi(ReactiveGoldLapel gl) {
        // Hold a back-reference to the parent client. State (token, port,
        // conn, DDL pattern cache) is shared via this reference.
        this.gl = gl;
    }

    /**
     * Eagerly materialize the doc-store table on the proxy. Other methods
     * also materialize on first use, so calling this is optional. Returns
     * {@code Mono<Void>} so eager setup composes into a Reactor pipeline.
     */
    public Mono<Void> createCollection(String collection) {
        return Mono.<Void>fromRunnable(() -> gl.sync.documents.patterns(collection))
            .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
            .then();
    }

    public Mono<Void> createCollection(String collection, boolean unlogged) {
        return Mono.<Void>fromRunnable(() -> gl.sync.documents.patterns(collection, unlogged))
            .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
            .then();
    }

    // -- CRUD ----------------------------------------------------------------

    public Mono<Map<String, Object>> insert(String collection, String documentJson) {
        return gl.call(null, c -> Utils.docInsert(c, collection, documentJson, gl.sync.documents.patterns(collection)));
    }
    public Mono<Map<String, Object>> insert(String collection, String documentJson, Connection conn) {
        return gl.call(conn, c -> Utils.docInsert(c, collection, documentJson, gl.sync.documents.patterns(collection)));
    }

    public Flux<Map<String, Object>> insertMany(String collection, List<String> documents) {
        return gl.flux(null, c -> Utils.docInsertMany(c, collection, documents, gl.sync.documents.patterns(collection)));
    }
    public Flux<Map<String, Object>> insertMany(String collection, List<String> documents, Connection conn) {
        return gl.flux(conn, c -> Utils.docInsertMany(c, collection, documents, gl.sync.documents.patterns(collection)));
    }

    public Flux<Map<String, Object>> find(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip) {
        return gl.flux(null, c -> Utils.docFind(c, collection, filterJson, sortJson, limit, skip, gl.sync.documents.patterns(collection)));
    }
    public Flux<Map<String, Object>> find(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, Connection conn) {
        return gl.flux(conn, c -> Utils.docFind(c, collection, filterJson, sortJson, limit, skip, gl.sync.documents.patterns(collection)));
    }

    public Mono<Map<String, Object>> findOne(String collection, String filterJson) {
        return gl.call(null, c -> Utils.docFindOne(c, collection, filterJson, gl.sync.documents.patterns(collection)));
    }
    public Mono<Map<String, Object>> findOne(String collection, String filterJson, Connection conn) {
        return gl.call(conn, c -> Utils.docFindOne(c, collection, filterJson, gl.sync.documents.patterns(collection)));
    }

    public Flux<Map<String, Object>> findCursor(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, int batchSize) {
        return gl.cursorFlux(null, c -> Utils.docFindCursor(c, collection, filterJson, sortJson, limit, skip, batchSize, gl.sync.documents.patterns(collection)));
    }
    public Flux<Map<String, Object>> findCursor(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, int batchSize, Connection conn) {
        return gl.cursorFlux(conn, c -> Utils.docFindCursor(c, collection, filterJson, sortJson, limit, skip, batchSize, gl.sync.documents.patterns(collection)));
    }

    public Mono<Integer> update(String collection, String filterJson, String updateJson) {
        return gl.call(null, c -> Utils.docUpdate(c, collection, filterJson, updateJson, gl.sync.documents.patterns(collection)));
    }
    public Mono<Integer> update(String collection, String filterJson, String updateJson, Connection conn) {
        return gl.call(conn, c -> Utils.docUpdate(c, collection, filterJson, updateJson, gl.sync.documents.patterns(collection)));
    }

    public Mono<Integer> updateOne(String collection, String filterJson, String updateJson) {
        return gl.call(null, c -> Utils.docUpdateOne(c, collection, filterJson, updateJson, gl.sync.documents.patterns(collection)));
    }
    public Mono<Integer> updateOne(String collection, String filterJson, String updateJson, Connection conn) {
        return gl.call(conn, c -> Utils.docUpdateOne(c, collection, filterJson, updateJson, gl.sync.documents.patterns(collection)));
    }

    public Mono<Integer> delete(String collection, String filterJson) {
        return gl.call(null, c -> Utils.docDelete(c, collection, filterJson, gl.sync.documents.patterns(collection)));
    }
    public Mono<Integer> delete(String collection, String filterJson, Connection conn) {
        return gl.call(conn, c -> Utils.docDelete(c, collection, filterJson, gl.sync.documents.patterns(collection)));
    }

    public Mono<Integer> deleteOne(String collection, String filterJson) {
        return gl.call(null, c -> Utils.docDeleteOne(c, collection, filterJson, gl.sync.documents.patterns(collection)));
    }
    public Mono<Integer> deleteOne(String collection, String filterJson, Connection conn) {
        return gl.call(conn, c -> Utils.docDeleteOne(c, collection, filterJson, gl.sync.documents.patterns(collection)));
    }

    public Mono<Map<String, Object>> findOneAndUpdate(String collection, String filterJson,
            String updateJson) {
        return gl.call(null, c -> Utils.docFindOneAndUpdate(c, collection, filterJson, updateJson, gl.sync.documents.patterns(collection)));
    }
    public Mono<Map<String, Object>> findOneAndUpdate(String collection, String filterJson,
            String updateJson, Connection conn) {
        return gl.call(conn, c -> Utils.docFindOneAndUpdate(c, collection, filterJson, updateJson, gl.sync.documents.patterns(collection)));
    }

    public Mono<Map<String, Object>> findOneAndDelete(String collection, String filterJson) {
        return gl.call(null, c -> Utils.docFindOneAndDelete(c, collection, filterJson, gl.sync.documents.patterns(collection)));
    }
    public Mono<Map<String, Object>> findOneAndDelete(String collection, String filterJson, Connection conn) {
        return gl.call(conn, c -> Utils.docFindOneAndDelete(c, collection, filterJson, gl.sync.documents.patterns(collection)));
    }

    public Flux<String> distinct(String collection, String field, String filterJson) {
        return gl.flux(null, c -> Utils.docDistinct(c, collection, field, filterJson, gl.sync.documents.patterns(collection)));
    }
    public Flux<String> distinct(String collection, String field, String filterJson, Connection conn) {
        return gl.flux(conn, c -> Utils.docDistinct(c, collection, field, filterJson, gl.sync.documents.patterns(collection)));
    }

    public Mono<Long> count(String collection, String filterJson) {
        return gl.call(null, c -> Utils.docCount(c, collection, filterJson, gl.sync.documents.patterns(collection)));
    }
    public Mono<Long> count(String collection, String filterJson, Connection conn) {
        return gl.call(conn, c -> Utils.docCount(c, collection, filterJson, gl.sync.documents.patterns(collection)));
    }

    public Mono<Void> createIndex(String collection, List<String> keys) {
        return gl.run(null, c -> Utils.docCreateIndex(c, collection, keys, gl.sync.documents.patterns(collection)));
    }
    public Mono<Void> createIndex(String collection, List<String> keys, Connection conn) {
        return gl.run(conn, c -> Utils.docCreateIndex(c, collection, keys, gl.sync.documents.patterns(collection)));
    }

    public Flux<Map<String, Object>> aggregate(String collection, String pipelineJson) {
        return gl.flux(null, c -> Utils.docAggregate(c, collection, pipelineJson,
            gl.sync.documents.patterns(collection),
            Utils.resolveLookupTables(gl.sync.documents, pipelineJson)));
    }
    public Flux<Map<String, Object>> aggregate(String collection, String pipelineJson, Connection conn) {
        return gl.flux(conn, c -> Utils.docAggregate(c, collection, pipelineJson,
            gl.sync.documents.patterns(collection),
            Utils.resolveLookupTables(gl.sync.documents, pipelineJson)));
    }

    // docWatch keeps the sync contract — Mono<Thread> the caller can interrupt.
    public Mono<Thread> watch(String collection, BiConsumer<String, String> callback) {
        return gl.call(null, c -> Utils.docWatch(c, collection, callback, gl.sync.documents.patterns(collection)));
    }
    public Mono<Thread> watch(String collection, BiConsumer<String, String> callback, Connection conn) {
        return gl.call(conn, c -> Utils.docWatch(c, collection, callback, gl.sync.documents.patterns(collection)));
    }

    public Mono<Void> unwatch(String collection) {
        return gl.run(null, c -> Utils.docUnwatch(c, collection, gl.sync.documents.patterns(collection)));
    }
    public Mono<Void> unwatch(String collection, Connection conn) {
        return gl.run(conn, c -> Utils.docUnwatch(c, collection, gl.sync.documents.patterns(collection)));
    }

    public Mono<Void> createTtlIndex(String collection, int expireAfterSeconds) {
        return gl.run(null, c -> Utils.docCreateTtlIndex(c, collection, expireAfterSeconds, gl.sync.documents.patterns(collection)));
    }
    public Mono<Void> createTtlIndex(String collection, int expireAfterSeconds, Connection conn) {
        return gl.run(conn, c -> Utils.docCreateTtlIndex(c, collection, expireAfterSeconds, gl.sync.documents.patterns(collection)));
    }
    public Mono<Void> createTtlIndex(String collection, int expireAfterSeconds, String field) {
        return gl.run(null, c -> Utils.docCreateTtlIndex(c, collection, expireAfterSeconds, field, gl.sync.documents.patterns(collection)));
    }
    public Mono<Void> createTtlIndex(String collection, int expireAfterSeconds, String field, Connection conn) {
        return gl.run(conn, c -> Utils.docCreateTtlIndex(c, collection, expireAfterSeconds, field, gl.sync.documents.patterns(collection)));
    }

    public Mono<Void> removeTtlIndex(String collection) {
        return gl.run(null, c -> Utils.docRemoveTtlIndex(c, collection, gl.sync.documents.patterns(collection)));
    }
    public Mono<Void> removeTtlIndex(String collection, Connection conn) {
        return gl.run(conn, c -> Utils.docRemoveTtlIndex(c, collection, gl.sync.documents.patterns(collection)));
    }

    public Mono<Void> createCapped(String collection, int maxDocuments) {
        return gl.run(null, c -> Utils.docCreateCapped(c, collection, maxDocuments, gl.sync.documents.patterns(collection)));
    }
    public Mono<Void> createCapped(String collection, int maxDocuments, Connection conn) {
        return gl.run(conn, c -> Utils.docCreateCapped(c, collection, maxDocuments, gl.sync.documents.patterns(collection)));
    }

    public Mono<Void> removeCap(String collection) {
        return gl.run(null, c -> Utils.docRemoveCap(c, collection, gl.sync.documents.patterns(collection)));
    }
    public Mono<Void> removeCap(String collection, Connection conn) {
        return gl.run(conn, c -> Utils.docRemoveCap(c, collection, gl.sync.documents.patterns(collection)));
    }
}
