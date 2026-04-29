package com.goldlapel.rxjava3;

import com.goldlapel.reactor.ReactiveDocumentsApi;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import reactor.adapter.rxjava.RxJava3Adapter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * RxJava 3 document store sub-API — accessible as {@code gl.documents}.
 * Wraps {@link ReactiveDocumentsApi} and converts {@code Mono}/{@code Flux}
 * to RxJava {@code Single}/{@code Maybe}/{@code Flowable}/{@code Completable}
 * types via {@link RxJava3Adapter}.
 */
public final class RxJavaDocumentsApi {
    private final ReactiveDocumentsApi inner;

    RxJavaDocumentsApi(ReactiveDocumentsApi inner) {
        this.inner = inner;
    }

    public Completable createCollection(String collection) {
        return RxJava3Adapter.monoToCompletable(inner.createCollection(collection));
    }
    public Completable createCollection(String collection, boolean unlogged) {
        return RxJava3Adapter.monoToCompletable(inner.createCollection(collection, unlogged));
    }

    public Single<Map<String, Object>> insert(String collection, String documentJson) {
        return RxJava3Adapter.monoToSingle(inner.insert(collection, documentJson));
    }
    public Single<Map<String, Object>> insert(String collection, String documentJson, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.insert(collection, documentJson, conn));
    }

    public Flowable<Map<String, Object>> insertMany(String collection, List<String> documents) {
        return RxJava3Adapter.fluxToFlowable(inner.insertMany(collection, documents));
    }
    public Flowable<Map<String, Object>> insertMany(String collection, List<String> documents, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.insertMany(collection, documents, conn));
    }

    public Flowable<Map<String, Object>> find(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip) {
        return RxJava3Adapter.fluxToFlowable(inner.find(collection, filterJson, sortJson, limit, skip));
    }
    public Flowable<Map<String, Object>> find(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.find(collection, filterJson, sortJson, limit, skip, conn));
    }

    /** {@code Maybe} — empty when no document matches. */
    public Maybe<Map<String, Object>> findOne(String collection, String filterJson) {
        return RxJava3Adapter.monoToMaybe(inner.findOne(collection, filterJson));
    }
    public Maybe<Map<String, Object>> findOne(String collection, String filterJson, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.findOne(collection, filterJson, conn));
    }

    public Flowable<Map<String, Object>> findCursor(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, int batchSize) {
        return RxJava3Adapter.fluxToFlowable(inner.findCursor(collection, filterJson, sortJson, limit, skip, batchSize));
    }
    public Flowable<Map<String, Object>> findCursor(String collection, String filterJson,
            String sortJson, Integer limit, Integer skip, int batchSize, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.findCursor(collection, filterJson, sortJson, limit, skip, batchSize, conn));
    }

    public Single<Integer> update(String collection, String filterJson, String updateJson) {
        return RxJava3Adapter.monoToSingle(inner.update(collection, filterJson, updateJson));
    }
    public Single<Integer> update(String collection, String filterJson, String updateJson, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.update(collection, filterJson, updateJson, conn));
    }

    public Single<Integer> updateOne(String collection, String filterJson, String updateJson) {
        return RxJava3Adapter.monoToSingle(inner.updateOne(collection, filterJson, updateJson));
    }
    public Single<Integer> updateOne(String collection, String filterJson, String updateJson, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.updateOne(collection, filterJson, updateJson, conn));
    }

    public Single<Integer> delete(String collection, String filterJson) {
        return RxJava3Adapter.monoToSingle(inner.delete(collection, filterJson));
    }
    public Single<Integer> delete(String collection, String filterJson, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.delete(collection, filterJson, conn));
    }

    public Single<Integer> deleteOne(String collection, String filterJson) {
        return RxJava3Adapter.monoToSingle(inner.deleteOne(collection, filterJson));
    }
    public Single<Integer> deleteOne(String collection, String filterJson, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.deleteOne(collection, filterJson, conn));
    }

    /** {@code Maybe} — empty when no document matches. */
    public Maybe<Map<String, Object>> findOneAndUpdate(String collection, String filterJson,
            String updateJson) {
        return RxJava3Adapter.monoToMaybe(inner.findOneAndUpdate(collection, filterJson, updateJson));
    }
    public Maybe<Map<String, Object>> findOneAndUpdate(String collection, String filterJson,
            String updateJson, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.findOneAndUpdate(collection, filterJson, updateJson, conn));
    }

    /** {@code Maybe} — empty when no document matches. */
    public Maybe<Map<String, Object>> findOneAndDelete(String collection, String filterJson) {
        return RxJava3Adapter.monoToMaybe(inner.findOneAndDelete(collection, filterJson));
    }
    public Maybe<Map<String, Object>> findOneAndDelete(String collection, String filterJson, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.findOneAndDelete(collection, filterJson, conn));
    }

    public Flowable<String> distinct(String collection, String field, String filterJson) {
        return RxJava3Adapter.fluxToFlowable(inner.distinct(collection, field, filterJson));
    }
    public Flowable<String> distinct(String collection, String field, String filterJson, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.distinct(collection, field, filterJson, conn));
    }

    public Single<Long> count(String collection, String filterJson) {
        return RxJava3Adapter.monoToSingle(inner.count(collection, filterJson));
    }
    public Single<Long> count(String collection, String filterJson, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.count(collection, filterJson, conn));
    }

    public Completable createIndex(String collection, List<String> keys) {
        return RxJava3Adapter.monoToCompletable(inner.createIndex(collection, keys));
    }
    public Completable createIndex(String collection, List<String> keys, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.createIndex(collection, keys, conn));
    }

    public Flowable<Map<String, Object>> aggregate(String collection, String pipelineJson) {
        return RxJava3Adapter.fluxToFlowable(inner.aggregate(collection, pipelineJson));
    }
    public Flowable<Map<String, Object>> aggregate(String collection, String pipelineJson, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.aggregate(collection, pipelineJson, conn));
    }

    public Single<Thread> watch(String collection, BiConsumer<String, String> callback) {
        return RxJava3Adapter.monoToSingle(inner.watch(collection, callback));
    }
    public Single<Thread> watch(String collection, BiConsumer<String, String> callback, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.watch(collection, callback, conn));
    }

    public Completable unwatch(String collection) {
        return RxJava3Adapter.monoToCompletable(inner.unwatch(collection));
    }
    public Completable unwatch(String collection, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.unwatch(collection, conn));
    }

    public Completable createTtlIndex(String collection, int expireAfterSeconds) {
        return RxJava3Adapter.monoToCompletable(inner.createTtlIndex(collection, expireAfterSeconds));
    }
    public Completable createTtlIndex(String collection, int expireAfterSeconds, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.createTtlIndex(collection, expireAfterSeconds, conn));
    }
    public Completable createTtlIndex(String collection, int expireAfterSeconds, String field) {
        return RxJava3Adapter.monoToCompletable(inner.createTtlIndex(collection, expireAfterSeconds, field));
    }
    public Completable createTtlIndex(String collection, int expireAfterSeconds, String field, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.createTtlIndex(collection, expireAfterSeconds, field, conn));
    }

    public Completable removeTtlIndex(String collection) {
        return RxJava3Adapter.monoToCompletable(inner.removeTtlIndex(collection));
    }
    public Completable removeTtlIndex(String collection, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.removeTtlIndex(collection, conn));
    }

    public Completable createCapped(String collection, int maxDocuments) {
        return RxJava3Adapter.monoToCompletable(inner.createCapped(collection, maxDocuments));
    }
    public Completable createCapped(String collection, int maxDocuments, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.createCapped(collection, maxDocuments, conn));
    }

    public Completable removeCap(String collection) {
        return RxJava3Adapter.monoToCompletable(inner.removeCap(collection));
    }
    public Completable removeCap(String collection, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.removeCap(collection, conn));
    }
}
