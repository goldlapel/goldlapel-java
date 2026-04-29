package com.goldlapel.rxjava3;

import com.goldlapel.reactor.ReactiveStreamsApi;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import reactor.adapter.rxjava.RxJava3Adapter;

import java.sql.Connection;
import java.util.Map;

/**
 * RxJava 3 streams sub-API — accessible as {@code gl.streams}. Wraps
 * {@link ReactiveStreamsApi} and converts {@code Mono}/{@code Flux} to RxJava
 * {@code Single}/{@code Flowable}/{@code Completable} types via
 * {@link RxJava3Adapter}.
 */
public final class RxJavaStreamsApi {
    private final ReactiveStreamsApi inner;

    RxJavaStreamsApi(ReactiveStreamsApi inner) {
        this.inner = inner;
    }

    public Single<Long> add(String stream, String payload) {
        return RxJava3Adapter.monoToSingle(inner.add(stream, payload));
    }
    public Single<Long> add(String stream, String payload, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.add(stream, payload, conn));
    }

    public Completable createGroup(String stream, String group) {
        return RxJava3Adapter.monoToCompletable(inner.createGroup(stream, group));
    }
    public Completable createGroup(String stream, String group, Connection conn) {
        return RxJava3Adapter.monoToCompletable(inner.createGroup(stream, group, conn));
    }

    public Flowable<Map<String, Object>> read(String stream, String group,
            String consumer, int count) {
        return RxJava3Adapter.fluxToFlowable(inner.read(stream, group, consumer, count));
    }
    public Flowable<Map<String, Object>> read(String stream, String group,
            String consumer, int count, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.read(stream, group, consumer, count, conn));
    }

    public Single<Boolean> ack(String stream, String group, long messageId) {
        return RxJava3Adapter.monoToSingle(inner.ack(stream, group, messageId));
    }
    public Single<Boolean> ack(String stream, String group, long messageId, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.ack(stream, group, messageId, conn));
    }

    public Flowable<Map<String, Object>> claim(String stream, String group,
            String consumer, long minIdleMs) {
        return RxJava3Adapter.fluxToFlowable(inner.claim(stream, group, consumer, minIdleMs));
    }
    public Flowable<Map<String, Object>> claim(String stream, String group,
            String consumer, long minIdleMs, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.claim(stream, group, consumer, minIdleMs, conn));
    }
}
