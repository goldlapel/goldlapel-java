package com.goldlapel.rxjava3;

import com.goldlapel.reactor.ReactiveZsetsApi;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import reactor.adapter.rxjava.RxJava3Adapter;

import java.sql.Connection;
import java.util.Map;

/**
 * RxJava 3 sorted-set sub-API — accessible as {@code gl.zsets}.
 * Phase 5: every method threads {@code zsetKey} as the first arg after
 * the namespace {@code name}.
 */
public final class RxJavaZsetsApi {
    private final ReactiveZsetsApi inner;

    RxJavaZsetsApi(ReactiveZsetsApi inner) {
        this.inner = inner;
    }

    public Completable create(String name) {
        return RxJava3Adapter.monoToCompletable(inner.create(name));
    }

    public Single<Double> add(String name, String zsetKey, String member, double score) {
        return RxJava3Adapter.monoToSingle(inner.add(name, zsetKey, member, score));
    }
    public Single<Double> add(String name, String zsetKey, String member, double score, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.add(name, zsetKey, member, score, conn));
    }

    public Single<Double> incrBy(String name, String zsetKey, String member, double delta) {
        return RxJava3Adapter.monoToSingle(inner.incrBy(name, zsetKey, member, delta));
    }
    public Single<Double> incrBy(String name, String zsetKey, String member, double delta, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.incrBy(name, zsetKey, member, delta, conn));
    }

    /** {@code Maybe} — empty when the member isn't in the set. */
    public Maybe<Double> score(String name, String zsetKey, String member) {
        return RxJava3Adapter.monoToMaybe(inner.score(name, zsetKey, member));
    }
    public Maybe<Double> score(String name, String zsetKey, String member, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.score(name, zsetKey, member, conn));
    }

    public Single<Boolean> remove(String name, String zsetKey, String member) {
        return RxJava3Adapter.monoToSingle(inner.remove(name, zsetKey, member));
    }
    public Single<Boolean> remove(String name, String zsetKey, String member, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.remove(name, zsetKey, member, conn));
    }

    public Flowable<Map.Entry<String, Double>> range(String name, String zsetKey,
            int start, int stop, boolean desc) {
        return RxJava3Adapter.fluxToFlowable(inner.range(name, zsetKey, start, stop, desc));
    }
    public Flowable<Map.Entry<String, Double>> range(String name, String zsetKey,
            int start, int stop, boolean desc, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.range(name, zsetKey, start, stop, desc, conn));
    }

    public Flowable<Map.Entry<String, Double>> rangeByScore(String name, String zsetKey,
            double minScore, double maxScore, int limit, int offset) {
        return RxJava3Adapter.fluxToFlowable(inner.rangeByScore(name, zsetKey, minScore, maxScore, limit, offset));
    }
    public Flowable<Map.Entry<String, Double>> rangeByScore(String name, String zsetKey,
            double minScore, double maxScore, int limit, int offset, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.rangeByScore(name, zsetKey, minScore, maxScore, limit, offset, conn));
    }

    /** {@code Maybe} — empty when the member isn't in the set. */
    public Maybe<Long> rank(String name, String zsetKey, String member, boolean desc) {
        return RxJava3Adapter.monoToMaybe(inner.rank(name, zsetKey, member, desc));
    }
    public Maybe<Long> rank(String name, String zsetKey, String member, boolean desc, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.rank(name, zsetKey, member, desc, conn));
    }

    public Single<Long> card(String name, String zsetKey) {
        return RxJava3Adapter.monoToSingle(inner.card(name, zsetKey));
    }
    public Single<Long> card(String name, String zsetKey, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.card(name, zsetKey, conn));
    }
}
