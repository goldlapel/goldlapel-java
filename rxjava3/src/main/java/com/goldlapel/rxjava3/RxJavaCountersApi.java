package com.goldlapel.rxjava3;

import com.goldlapel.reactor.ReactiveCountersApi;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import reactor.adapter.rxjava.RxJava3Adapter;

import java.sql.Connection;

/**
 * RxJava 3 counters sub-API — accessible as {@code gl.counters}.
 * Wraps {@link ReactiveCountersApi}.
 */
public final class RxJavaCountersApi {
    private final ReactiveCountersApi inner;

    RxJavaCountersApi(ReactiveCountersApi inner) {
        this.inner = inner;
    }

    public Completable create(String name) {
        return RxJava3Adapter.monoToCompletable(inner.create(name));
    }

    public Single<Long> incr(String name, String key) {
        return RxJava3Adapter.monoToSingle(inner.incr(name, key));
    }
    public Single<Long> incr(String name, String key, long amount) {
        return RxJava3Adapter.monoToSingle(inner.incr(name, key, amount));
    }
    public Single<Long> incr(String name, String key, long amount, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.incr(name, key, amount, conn));
    }

    public Single<Long> decr(String name, String key) {
        return RxJava3Adapter.monoToSingle(inner.decr(name, key));
    }
    public Single<Long> decr(String name, String key, long amount) {
        return RxJava3Adapter.monoToSingle(inner.decr(name, key, amount));
    }
    public Single<Long> decr(String name, String key, long amount, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.decr(name, key, amount, conn));
    }

    public Single<Long> set(String name, String key, long value) {
        return RxJava3Adapter.monoToSingle(inner.set(name, key, value));
    }
    public Single<Long> set(String name, String key, long value, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.set(name, key, value, conn));
    }

    public Single<Long> get(String name, String key) {
        return RxJava3Adapter.monoToSingle(inner.get(name, key));
    }
    public Single<Long> get(String name, String key, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.get(name, key, conn));
    }

    public Single<Boolean> delete(String name, String key) {
        return RxJava3Adapter.monoToSingle(inner.delete(name, key));
    }
    public Single<Boolean> delete(String name, String key, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.delete(name, key, conn));
    }

    public Single<Long> countKeys(String name) {
        return RxJava3Adapter.monoToSingle(inner.countKeys(name));
    }
    public Single<Long> countKeys(String name, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.countKeys(name, conn));
    }
}
