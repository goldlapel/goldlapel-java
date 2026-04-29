package com.goldlapel.reactor;

import com.goldlapel.Utils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.util.Map;

/**
 * Reactive geo sub-API — accessible as {@code gl.geos} on a
 * {@link ReactiveGoldLapel}. Phase 5 schema is GEOGRAPHY-native;
 * {@link #add} is idempotent on the member name (re-adding updates the
 * location).
 */
public final class ReactiveGeosApi {
    private final ReactiveGoldLapel gl;

    ReactiveGeosApi(ReactiveGoldLapel gl) {
        this.gl = gl;
    }

    public Mono<Void> create(String name) {
        return Mono.<Void>fromRunnable(() -> gl.sync.geos.patterns(name))
            .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
            .then();
    }

    public Mono<Utils.GeoPos> add(String name, String member, double lon, double lat) {
        return gl.call(null, c -> Utils.geoAdd(c, name, member, lon, lat, gl.sync.geos.patterns(name)));
    }
    public Mono<Utils.GeoPos> add(String name, String member, double lon, double lat, Connection conn) {
        return gl.call(conn, c -> Utils.geoAdd(c, name, member, lon, lat, gl.sync.geos.patterns(name)));
    }

    public Mono<Utils.GeoPos> pos(String name, String member) {
        return gl.call(null, c -> Utils.geoPos(c, name, member, gl.sync.geos.patterns(name)));
    }
    public Mono<Utils.GeoPos> pos(String name, String member, Connection conn) {
        return gl.call(conn, c -> Utils.geoPos(c, name, member, gl.sync.geos.patterns(name)));
    }

    public Mono<Double> dist(String name, String memberA, String memberB) {
        return dist(name, memberA, memberB, "m");
    }
    public Mono<Double> dist(String name, String memberA, String memberB, String unit) {
        return gl.call(null, c -> Utils.geoDist(c, name, memberA, memberB, unit, gl.sync.geos.patterns(name)));
    }
    public Mono<Double> dist(String name, String memberA, String memberB, String unit, Connection conn) {
        return gl.call(conn, c -> Utils.geoDist(c, name, memberA, memberB, unit, gl.sync.geos.patterns(name)));
    }

    public Flux<Map<String, Object>> radius(String name, double lon, double lat, double radius) {
        return radius(name, lon, lat, radius, "m", 50);
    }
    public Flux<Map<String, Object>> radius(String name, double lon, double lat,
            double radius, String unit, int limit) {
        return gl.flux(null, c -> Utils.geoRadius(c, name, lon, lat, radius, unit, limit, gl.sync.geos.patterns(name)));
    }
    public Flux<Map<String, Object>> radius(String name, double lon, double lat,
            double radius, String unit, int limit, Connection conn) {
        return gl.flux(conn, c -> Utils.geoRadius(c, name, lon, lat, radius, unit, limit, gl.sync.geos.patterns(name)));
    }

    public Flux<Map<String, Object>> radiusByMember(String name, String member, double radius) {
        return radiusByMember(name, member, radius, "m", 50);
    }
    public Flux<Map<String, Object>> radiusByMember(String name, String member,
            double radius, String unit, int limit) {
        return gl.flux(null, c -> Utils.geoRadiusByMember(c, name, member, radius, unit, limit, gl.sync.geos.patterns(name)));
    }
    public Flux<Map<String, Object>> radiusByMember(String name, String member,
            double radius, String unit, int limit, Connection conn) {
        return gl.flux(conn, c -> Utils.geoRadiusByMember(c, name, member, radius, unit, limit, gl.sync.geos.patterns(name)));
    }

    public Mono<Boolean> remove(String name, String member) {
        return gl.call(null, c -> Utils.geoRemove(c, name, member, gl.sync.geos.patterns(name)));
    }
    public Mono<Boolean> remove(String name, String member, Connection conn) {
        return gl.call(conn, c -> Utils.geoRemove(c, name, member, gl.sync.geos.patterns(name)));
    }

    public Mono<Long> count(String name) {
        return gl.call(null, c -> Utils.geoCount(c, name, gl.sync.geos.patterns(name)));
    }
    public Mono<Long> count(String name, Connection conn) {
        return gl.call(conn, c -> Utils.geoCount(c, name, gl.sync.geos.patterns(name)));
    }
}
