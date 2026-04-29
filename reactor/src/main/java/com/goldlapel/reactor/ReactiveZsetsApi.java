package com.goldlapel.reactor;

import com.goldlapel.Utils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.util.Map;

/**
 * Reactive sorted-set sub-API — accessible as {@code gl.zsets} on a
 * {@link ReactiveGoldLapel}. Phase 5 schema adds {@code zset_key} as the
 * partition column so a single namespace table holds many sorted sets.
 */
public final class ReactiveZsetsApi {
    private final ReactiveGoldLapel gl;

    ReactiveZsetsApi(ReactiveGoldLapel gl) {
        this.gl = gl;
    }

    public Mono<Void> create(String name) {
        return Mono.<Void>fromRunnable(() -> gl.sync.zsets.patterns(name))
            .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
            .then();
    }

    public Mono<Double> add(String name, String zsetKey, String member, double score) {
        return gl.call(null, c -> Utils.zsetAdd(c, name, zsetKey, member, score, gl.sync.zsets.patterns(name)));
    }
    public Mono<Double> add(String name, String zsetKey, String member, double score, Connection conn) {
        return gl.call(conn, c -> Utils.zsetAdd(c, name, zsetKey, member, score, gl.sync.zsets.patterns(name)));
    }

    public Mono<Double> incrBy(String name, String zsetKey, String member, double delta) {
        return gl.call(null, c -> Utils.zsetIncrBy(c, name, zsetKey, member, delta, gl.sync.zsets.patterns(name)));
    }
    public Mono<Double> incrBy(String name, String zsetKey, String member, double delta, Connection conn) {
        return gl.call(conn, c -> Utils.zsetIncrBy(c, name, zsetKey, member, delta, gl.sync.zsets.patterns(name)));
    }

    public Mono<Double> score(String name, String zsetKey, String member) {
        return gl.call(null, c -> Utils.zsetScore(c, name, zsetKey, member, gl.sync.zsets.patterns(name)));
    }
    public Mono<Double> score(String name, String zsetKey, String member, Connection conn) {
        return gl.call(conn, c -> Utils.zsetScore(c, name, zsetKey, member, gl.sync.zsets.patterns(name)));
    }

    public Mono<Boolean> remove(String name, String zsetKey, String member) {
        return gl.call(null, c -> Utils.zsetRemove(c, name, zsetKey, member, gl.sync.zsets.patterns(name)));
    }
    public Mono<Boolean> remove(String name, String zsetKey, String member, Connection conn) {
        return gl.call(conn, c -> Utils.zsetRemove(c, name, zsetKey, member, gl.sync.zsets.patterns(name)));
    }

    public Flux<Map.Entry<String, Double>> range(String name, String zsetKey,
            int start, int stop, boolean desc) {
        final int s = stop == -1 ? 9999 : stop;
        return gl.flux(null, c -> Utils.zsetRange(c, name, zsetKey, start, s, desc, gl.sync.zsets.patterns(name)));
    }
    public Flux<Map.Entry<String, Double>> range(String name, String zsetKey,
            int start, int stop, boolean desc, Connection conn) {
        final int s = stop == -1 ? 9999 : stop;
        return gl.flux(conn, c -> Utils.zsetRange(c, name, zsetKey, start, s, desc, gl.sync.zsets.patterns(name)));
    }

    public Flux<Map.Entry<String, Double>> rangeByScore(String name, String zsetKey,
            double minScore, double maxScore, int limit, int offset) {
        return gl.flux(null, c -> Utils.zsetRangeByScore(c, name, zsetKey,
            minScore, maxScore, limit, offset, gl.sync.zsets.patterns(name)));
    }
    public Flux<Map.Entry<String, Double>> rangeByScore(String name, String zsetKey,
            double minScore, double maxScore, int limit, int offset, Connection conn) {
        return gl.flux(conn, c -> Utils.zsetRangeByScore(c, name, zsetKey,
            minScore, maxScore, limit, offset, gl.sync.zsets.patterns(name)));
    }

    public Mono<Long> rank(String name, String zsetKey, String member, boolean desc) {
        return gl.call(null, c -> Utils.zsetRank(c, name, zsetKey, member, desc, gl.sync.zsets.patterns(name)));
    }
    public Mono<Long> rank(String name, String zsetKey, String member, boolean desc, Connection conn) {
        return gl.call(conn, c -> Utils.zsetRank(c, name, zsetKey, member, desc, gl.sync.zsets.patterns(name)));
    }

    public Mono<Long> card(String name, String zsetKey) {
        return gl.call(null, c -> Utils.zsetCard(c, name, zsetKey, gl.sync.zsets.patterns(name)));
    }
    public Mono<Long> card(String name, String zsetKey, Connection conn) {
        return gl.call(conn, c -> Utils.zsetCard(c, name, zsetKey, gl.sync.zsets.patterns(name)));
    }
}
