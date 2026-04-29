package com.goldlapel.rxjava3;

import com.goldlapel.reactor.ReactiveHashesApi;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import reactor.adapter.rxjava.RxJava3Adapter;

import java.sql.Connection;
import java.util.Map;

/**
 * RxJava 3 hash sub-API — accessible as {@code gl.hashes}. Phase 5
 * storage: row-per-field.
 */
public final class RxJavaHashesApi {
    private final ReactiveHashesApi inner;

    RxJavaHashesApi(ReactiveHashesApi inner) {
        this.inner = inner;
    }

    public Completable create(String name) {
        return RxJava3Adapter.monoToCompletable(inner.create(name));
    }

    public Single<String> set(String name, String hashKey, String field, String valueJson) {
        return RxJava3Adapter.monoToSingle(inner.set(name, hashKey, field, valueJson));
    }
    public Single<String> set(String name, String hashKey, String field, String valueJson, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.set(name, hashKey, field, valueJson, conn));
    }

    /** {@code Maybe} — empty when the field is absent. */
    public Maybe<String> get(String name, String hashKey, String field) {
        return RxJava3Adapter.monoToMaybe(inner.get(name, hashKey, field));
    }
    public Maybe<String> get(String name, String hashKey, String field, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.get(name, hashKey, field, conn));
    }

    public Single<Map<String, String>> getAll(String name, String hashKey) {
        return RxJava3Adapter.monoToSingle(inner.getAll(name, hashKey));
    }
    public Single<Map<String, String>> getAll(String name, String hashKey, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.getAll(name, hashKey, conn));
    }

    public Flowable<String> keys(String name, String hashKey) {
        return RxJava3Adapter.fluxToFlowable(inner.keys(name, hashKey));
    }
    public Flowable<String> keys(String name, String hashKey, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.keys(name, hashKey, conn));
    }

    public Flowable<String> values(String name, String hashKey) {
        return RxJava3Adapter.fluxToFlowable(inner.values(name, hashKey));
    }
    public Flowable<String> values(String name, String hashKey, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.values(name, hashKey, conn));
    }

    public Single<Boolean> exists(String name, String hashKey, String field) {
        return RxJava3Adapter.monoToSingle(inner.exists(name, hashKey, field));
    }
    public Single<Boolean> exists(String name, String hashKey, String field, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.exists(name, hashKey, field, conn));
    }

    public Single<Boolean> delete(String name, String hashKey, String field) {
        return RxJava3Adapter.monoToSingle(inner.delete(name, hashKey, field));
    }
    public Single<Boolean> delete(String name, String hashKey, String field, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.delete(name, hashKey, field, conn));
    }

    public Single<Long> len(String name, String hashKey) {
        return RxJava3Adapter.monoToSingle(inner.len(name, hashKey));
    }
    public Single<Long> len(String name, String hashKey, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.len(name, hashKey, conn));
    }
}
