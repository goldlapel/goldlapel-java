package com.goldlapel.reactor;

import com.goldlapel.Utils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.util.Map;

/**
 * Reactive hash sub-API — accessible as {@code gl.hashes} on a
 * {@link ReactiveGoldLapel}. Phase 5 storage: row-per-field
 * ({@code hash_key}, {@code field}, {@code value JSONB}).
 */
public final class ReactiveHashesApi {
    private final ReactiveGoldLapel gl;

    ReactiveHashesApi(ReactiveGoldLapel gl) {
        this.gl = gl;
    }

    public Mono<Void> create(String name) {
        return Mono.<Void>fromRunnable(() -> gl.sync.hashes.patterns(name))
            .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
            .then();
    }

    public Mono<String> set(String name, String hashKey, String field, String valueJson) {
        return gl.call(null, c -> Utils.hashSet(c, name, hashKey, field, valueJson, gl.sync.hashes.patterns(name)));
    }
    public Mono<String> set(String name, String hashKey, String field, String valueJson, Connection conn) {
        return gl.call(conn, c -> Utils.hashSet(c, name, hashKey, field, valueJson, gl.sync.hashes.patterns(name)));
    }

    public Mono<String> get(String name, String hashKey, String field) {
        return gl.call(null, c -> Utils.hashGet(c, name, hashKey, field, gl.sync.hashes.patterns(name)));
    }
    public Mono<String> get(String name, String hashKey, String field, Connection conn) {
        return gl.call(conn, c -> Utils.hashGet(c, name, hashKey, field, gl.sync.hashes.patterns(name)));
    }

    public Mono<Map<String, String>> getAll(String name, String hashKey) {
        return gl.call(null, c -> Utils.hashGetAll(c, name, hashKey, gl.sync.hashes.patterns(name)));
    }
    public Mono<Map<String, String>> getAll(String name, String hashKey, Connection conn) {
        return gl.call(conn, c -> Utils.hashGetAll(c, name, hashKey, gl.sync.hashes.patterns(name)));
    }

    public Flux<String> keys(String name, String hashKey) {
        return gl.flux(null, c -> Utils.hashKeys(c, name, hashKey, gl.sync.hashes.patterns(name)));
    }
    public Flux<String> keys(String name, String hashKey, Connection conn) {
        return gl.flux(conn, c -> Utils.hashKeys(c, name, hashKey, gl.sync.hashes.patterns(name)));
    }

    public Flux<String> values(String name, String hashKey) {
        return gl.flux(null, c -> Utils.hashValues(c, name, hashKey, gl.sync.hashes.patterns(name)));
    }
    public Flux<String> values(String name, String hashKey, Connection conn) {
        return gl.flux(conn, c -> Utils.hashValues(c, name, hashKey, gl.sync.hashes.patterns(name)));
    }

    public Mono<Boolean> exists(String name, String hashKey, String field) {
        return gl.call(null, c -> Utils.hashExists(c, name, hashKey, field, gl.sync.hashes.patterns(name)));
    }
    public Mono<Boolean> exists(String name, String hashKey, String field, Connection conn) {
        return gl.call(conn, c -> Utils.hashExists(c, name, hashKey, field, gl.sync.hashes.patterns(name)));
    }

    public Mono<Boolean> delete(String name, String hashKey, String field) {
        return gl.call(null, c -> Utils.hashDelete(c, name, hashKey, field, gl.sync.hashes.patterns(name)));
    }
    public Mono<Boolean> delete(String name, String hashKey, String field, Connection conn) {
        return gl.call(conn, c -> Utils.hashDelete(c, name, hashKey, field, gl.sync.hashes.patterns(name)));
    }

    public Mono<Long> len(String name, String hashKey) {
        return gl.call(null, c -> Utils.hashLen(c, name, hashKey, gl.sync.hashes.patterns(name)));
    }
    public Mono<Long> len(String name, String hashKey, Connection conn) {
        return gl.call(conn, c -> Utils.hashLen(c, name, hashKey, gl.sync.hashes.patterns(name)));
    }
}
