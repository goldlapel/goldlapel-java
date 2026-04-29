package com.goldlapel.reactor;

import com.goldlapel.Utils;

import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Reactive queue sub-API — accessible as {@code gl.queues} on a
 * {@link ReactiveGoldLapel}. Phase 5 contract: at-least-once with
 * visibility-timeout — explicit {@code claim}/{@code ack}, no
 * {@code dequeue} compat shim.
 */
public final class ReactiveQueuesApi {
    private final ReactiveGoldLapel gl;

    ReactiveQueuesApi(ReactiveGoldLapel gl) {
        this.gl = gl;
    }

    public Mono<Void> create(String name) {
        return Mono.<Void>fromRunnable(() -> gl.sync.queues.patterns(name))
            .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
            .then();
    }

    public Mono<Long> enqueue(String name, String payloadJson) {
        return gl.call(null, c -> Utils.queueEnqueue(c, name, payloadJson, gl.sync.queues.patterns(name)));
    }
    public Mono<Long> enqueue(String name, String payloadJson, Connection conn) {
        return gl.call(conn, c -> Utils.queueEnqueue(c, name, payloadJson, gl.sync.queues.patterns(name)));
    }

    /** Lease the next ready message; emits the {@link Utils.ClaimedMessage}
     *  or completes empty when the queue has nothing ready. */
    public Mono<Utils.ClaimedMessage> claim(String name) {
        return claim(name, 30000L);
    }
    public Mono<Utils.ClaimedMessage> claim(String name, long visibilityTimeoutMs) {
        return gl.<Utils.ClaimedMessage>call(null,
                c -> Utils.queueClaim(c, name, visibilityTimeoutMs, gl.sync.queues.patterns(name)))
            .flatMap(msg -> msg == null ? Mono.empty() : Mono.just(msg));
    }
    public Mono<Utils.ClaimedMessage> claim(String name, long visibilityTimeoutMs, Connection conn) {
        return gl.<Utils.ClaimedMessage>call(conn,
                c -> Utils.queueClaim(c, name, visibilityTimeoutMs, gl.sync.queues.patterns(name)))
            .flatMap(msg -> msg == null ? Mono.empty() : Mono.just(msg));
    }

    public Mono<Boolean> ack(String name, long messageId) {
        return gl.call(null, c -> Utils.queueAck(c, name, messageId, gl.sync.queues.patterns(name)));
    }
    public Mono<Boolean> ack(String name, long messageId, Connection conn) {
        return gl.call(conn, c -> Utils.queueAck(c, name, messageId, gl.sync.queues.patterns(name)));
    }

    public Mono<Boolean> abandon(String name, long messageId) {
        return gl.call(null, c -> Utils.queueAbandon(c, name, messageId, gl.sync.queues.patterns(name)));
    }
    public Mono<Boolean> abandon(String name, long messageId, Connection conn) {
        return gl.call(conn, c -> Utils.queueAbandon(c, name, messageId, gl.sync.queues.patterns(name)));
    }

    public Mono<Timestamp> extend(String name, long messageId, long additionalMs) {
        return gl.call(null, c -> Utils.queueExtend(c, name, messageId, additionalMs, gl.sync.queues.patterns(name)));
    }
    public Mono<Timestamp> extend(String name, long messageId, long additionalMs, Connection conn) {
        return gl.call(conn, c -> Utils.queueExtend(c, name, messageId, additionalMs, gl.sync.queues.patterns(name)));
    }

    public Mono<Map<String, Object>> peek(String name) {
        return gl.call(null, c -> Utils.queuePeek(c, name, gl.sync.queues.patterns(name)));
    }
    public Mono<Map<String, Object>> peek(String name, Connection conn) {
        return gl.call(conn, c -> Utils.queuePeek(c, name, gl.sync.queues.patterns(name)));
    }

    public Mono<Long> countReady(String name) {
        return gl.call(null, c -> Utils.queueCountReady(c, name, gl.sync.queues.patterns(name)));
    }
    public Mono<Long> countReady(String name, Connection conn) {
        return gl.call(conn, c -> Utils.queueCountReady(c, name, gl.sync.queues.patterns(name)));
    }

    public Mono<Long> countClaimed(String name) {
        return gl.call(null, c -> Utils.queueCountClaimed(c, name, gl.sync.queues.patterns(name)));
    }
    public Mono<Long> countClaimed(String name, Connection conn) {
        return gl.call(conn, c -> Utils.queueCountClaimed(c, name, gl.sync.queues.patterns(name)));
    }
}
