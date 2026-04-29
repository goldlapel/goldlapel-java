package com.goldlapel.reactor;

import com.goldlapel.Utils;

import reactor.core.publisher.Mono;

import java.sql.Connection;

/**
 * Reactive counters sub-API — accessible as {@code gl.counters} on a
 * {@link ReactiveGoldLapel}.
 *
 * <p>Each call delegates to the sync {@link com.goldlapel.GoldLapel}'s
 * {@link com.goldlapel.CountersApi} for canonical patterns + DDL bookkeeping
 * (one HTTP round-trip per (family, name) per session, cached on the
 * parent), then runs the SQL on
 * {@link reactor.core.scheduler.Schedulers#boundedElastic()}.
 */
public final class ReactiveCountersApi {
    private final ReactiveGoldLapel gl;

    ReactiveCountersApi(ReactiveGoldLapel gl) {
        this.gl = gl;
    }

    public Mono<Void> create(String name) {
        return Mono.<Void>fromRunnable(() -> gl.sync.counters.patterns(name))
            .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
            .then();
    }

    public Mono<Long> incr(String name, String key) {
        return incr(name, key, 1L);
    }
    public Mono<Long> incr(String name, String key, long amount) {
        return gl.call(null, c -> Utils.counterIncr(c, name, key, amount, gl.sync.counters.patterns(name)));
    }
    public Mono<Long> incr(String name, String key, long amount, Connection conn) {
        return gl.call(conn, c -> Utils.counterIncr(c, name, key, amount, gl.sync.counters.patterns(name)));
    }

    public Mono<Long> decr(String name, String key) {
        return decr(name, key, 1L);
    }
    public Mono<Long> decr(String name, String key, long amount) {
        return gl.call(null, c -> Utils.counterDecr(c, name, key, amount, gl.sync.counters.patterns(name)));
    }
    public Mono<Long> decr(String name, String key, long amount, Connection conn) {
        return gl.call(conn, c -> Utils.counterDecr(c, name, key, amount, gl.sync.counters.patterns(name)));
    }

    public Mono<Long> set(String name, String key, long value) {
        return gl.call(null, c -> Utils.counterSet(c, name, key, value, gl.sync.counters.patterns(name)));
    }
    public Mono<Long> set(String name, String key, long value, Connection conn) {
        return gl.call(conn, c -> Utils.counterSet(c, name, key, value, gl.sync.counters.patterns(name)));
    }

    public Mono<Long> get(String name, String key) {
        return gl.call(null, c -> Utils.counterGet(c, name, key, gl.sync.counters.patterns(name)));
    }
    public Mono<Long> get(String name, String key, Connection conn) {
        return gl.call(conn, c -> Utils.counterGet(c, name, key, gl.sync.counters.patterns(name)));
    }

    public Mono<Boolean> delete(String name, String key) {
        return gl.call(null, c -> Utils.counterDelete(c, name, key, gl.sync.counters.patterns(name)));
    }
    public Mono<Boolean> delete(String name, String key, Connection conn) {
        return gl.call(conn, c -> Utils.counterDelete(c, name, key, gl.sync.counters.patterns(name)));
    }

    public Mono<Long> countKeys(String name) {
        return gl.call(null, c -> Utils.counterCountKeys(c, name, gl.sync.counters.patterns(name)));
    }
    public Mono<Long> countKeys(String name, Connection conn) {
        return gl.call(conn, c -> Utils.counterCountKeys(c, name, gl.sync.counters.patterns(name)));
    }
}
