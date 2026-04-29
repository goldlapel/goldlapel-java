package com.goldlapel.rxjava3;

import com.goldlapel.Utils;
import com.goldlapel.reactor.ReactiveGeosApi;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import reactor.adapter.rxjava.RxJava3Adapter;

import java.sql.Connection;
import java.util.Map;

/**
 * RxJava 3 geo sub-API — accessible as {@code gl.geos}. Phase 5:
 * GEOGRAPHY-native, idempotent {@code add} on the member name.
 */
public final class RxJavaGeosApi {
    private final ReactiveGeosApi inner;

    RxJavaGeosApi(ReactiveGeosApi inner) {
        this.inner = inner;
    }

    public Completable create(String name) {
        return RxJava3Adapter.monoToCompletable(inner.create(name));
    }

    public Single<Utils.GeoPos> add(String name, String member, double lon, double lat) {
        return RxJava3Adapter.monoToSingle(inner.add(name, member, lon, lat));
    }
    public Single<Utils.GeoPos> add(String name, String member, double lon, double lat, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.add(name, member, lon, lat, conn));
    }

    /** {@code Maybe} — empty when the member isn't in the namespace. */
    public Maybe<Utils.GeoPos> pos(String name, String member) {
        return RxJava3Adapter.monoToMaybe(inner.pos(name, member));
    }
    public Maybe<Utils.GeoPos> pos(String name, String member, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.pos(name, member, conn));
    }

    /** {@code Maybe} — empty when either endpoint is missing. */
    public Maybe<Double> dist(String name, String memberA, String memberB) {
        return RxJava3Adapter.monoToMaybe(inner.dist(name, memberA, memberB));
    }
    public Maybe<Double> dist(String name, String memberA, String memberB, String unit) {
        return RxJava3Adapter.monoToMaybe(inner.dist(name, memberA, memberB, unit));
    }
    public Maybe<Double> dist(String name, String memberA, String memberB, String unit, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.dist(name, memberA, memberB, unit, conn));
    }

    public Flowable<Map<String, Object>> radius(String name, double lon, double lat, double radius) {
        return RxJava3Adapter.fluxToFlowable(inner.radius(name, lon, lat, radius));
    }
    public Flowable<Map<String, Object>> radius(String name, double lon, double lat,
            double radius, String unit, int limit) {
        return RxJava3Adapter.fluxToFlowable(inner.radius(name, lon, lat, radius, unit, limit));
    }
    public Flowable<Map<String, Object>> radius(String name, double lon, double lat,
            double radius, String unit, int limit, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.radius(name, lon, lat, radius, unit, limit, conn));
    }

    public Flowable<Map<String, Object>> radiusByMember(String name, String member, double radius) {
        return RxJava3Adapter.fluxToFlowable(inner.radiusByMember(name, member, radius));
    }
    public Flowable<Map<String, Object>> radiusByMember(String name, String member,
            double radius, String unit, int limit) {
        return RxJava3Adapter.fluxToFlowable(inner.radiusByMember(name, member, radius, unit, limit));
    }
    public Flowable<Map<String, Object>> radiusByMember(String name, String member,
            double radius, String unit, int limit, Connection conn) {
        return RxJava3Adapter.fluxToFlowable(inner.radiusByMember(name, member, radius, unit, limit, conn));
    }

    public Single<Boolean> remove(String name, String member) {
        return RxJava3Adapter.monoToSingle(inner.remove(name, member));
    }
    public Single<Boolean> remove(String name, String member, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.remove(name, member, conn));
    }

    public Single<Long> count(String name) {
        return RxJava3Adapter.monoToSingle(inner.count(name));
    }
    public Single<Long> count(String name, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.count(name, conn));
    }
}
