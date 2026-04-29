package com.goldlapel.reactor;

import com.goldlapel.Utils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Reactive streams sub-API — accessible as
 * {@code gl.streams} on a {@link ReactiveGoldLapel}.
 *
 * <p>Each call delegates to the sync {@link com.goldlapel.GoldLapel}'s DDL
 * pattern cache (via {@code sync.streamPatterns(name)}) for canonical query
 * patterns and runs the SQL on {@link reactor.core.scheduler.Schedulers#boundedElastic()}
 * — same model as the sync flavour, no behavioural drift.
 */
public final class ReactiveStreamsApi {
    private final ReactiveGoldLapel gl;

    ReactiveStreamsApi(ReactiveGoldLapel gl) {
        // Hold a back-reference to the parent client. State (token, port,
        // conn, DDL pattern cache) is shared via this reference.
        this.gl = gl;
    }

    public Mono<Long> add(String stream, String payload) {
        return gl.call(null, c -> Utils.streamAdd(c, stream, payload, gl.sync.streamPatterns(stream)));
    }
    public Mono<Long> add(String stream, String payload, Connection conn) {
        return gl.call(conn, c -> Utils.streamAdd(c, stream, payload, gl.sync.streamPatterns(stream)));
    }

    public Mono<Void> createGroup(String stream, String group) {
        return gl.run(null, c -> Utils.streamCreateGroup(c, stream, group, gl.sync.streamPatterns(stream)));
    }
    public Mono<Void> createGroup(String stream, String group, Connection conn) {
        return gl.run(conn, c -> Utils.streamCreateGroup(c, stream, group, gl.sync.streamPatterns(stream)));
    }

    public Flux<Map<String, Object>> read(String stream, String group,
            String consumer, int count) {
        return gl.flux(null, c -> Utils.streamRead(c, stream, group, consumer, count, gl.sync.streamPatterns(stream)));
    }
    public Flux<Map<String, Object>> read(String stream, String group,
            String consumer, int count, Connection conn) {
        return gl.flux(conn, c -> Utils.streamRead(c, stream, group, consumer, count, gl.sync.streamPatterns(stream)));
    }

    public Mono<Boolean> ack(String stream, String group, long messageId) {
        return gl.call(null, c -> Utils.streamAck(c, stream, group, messageId, gl.sync.streamPatterns(stream)));
    }
    public Mono<Boolean> ack(String stream, String group, long messageId, Connection conn) {
        return gl.call(conn, c -> Utils.streamAck(c, stream, group, messageId, gl.sync.streamPatterns(stream)));
    }

    public Flux<Map<String, Object>> claim(String stream, String group,
            String consumer, long minIdleMs) {
        return gl.flux(null, c -> Utils.streamClaim(c, stream, group, consumer, minIdleMs, gl.sync.streamPatterns(stream)));
    }
    public Flux<Map<String, Object>> claim(String stream, String group,
            String consumer, long minIdleMs, Connection conn) {
        return gl.flux(conn, c -> Utils.streamClaim(c, stream, group, consumer, minIdleMs, gl.sync.streamPatterns(stream)));
    }
}
