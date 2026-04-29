package com.goldlapel.rxjava3;

import com.goldlapel.Utils;
import com.goldlapel.reactor.ReactiveQueuesApi;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import reactor.adapter.rxjava.RxJava3Adapter;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.Map;

/**
 * RxJava 3 queue sub-API — accessible as {@code gl.queues}. Phase 5
 * contract: at-least-once with visibility-timeout. {@link #claim} maps
 * to {@code Maybe} so an empty queue is a clean {@code onComplete}.
 * No {@code dequeue} compat shim.
 */
public final class RxJavaQueuesApi {
    private final ReactiveQueuesApi inner;

    RxJavaQueuesApi(ReactiveQueuesApi inner) {
        this.inner = inner;
    }

    public Completable create(String name) {
        return RxJava3Adapter.monoToCompletable(inner.create(name));
    }

    public Single<Long> enqueue(String name, String payloadJson) {
        return RxJava3Adapter.monoToSingle(inner.enqueue(name, payloadJson));
    }
    public Single<Long> enqueue(String name, String payloadJson, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.enqueue(name, payloadJson, conn));
    }

    /** {@code Maybe} — empty when the queue has nothing ready. Caller MUST
     *  {@code ack} the returned id (or {@code abandon}); a missing ack
     *  triggers redelivery after the visibility window. */
    public Maybe<Utils.ClaimedMessage> claim(String name) {
        return RxJava3Adapter.monoToMaybe(inner.claim(name));
    }
    public Maybe<Utils.ClaimedMessage> claim(String name, long visibilityTimeoutMs) {
        return RxJava3Adapter.monoToMaybe(inner.claim(name, visibilityTimeoutMs));
    }
    public Maybe<Utils.ClaimedMessage> claim(String name, long visibilityTimeoutMs, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.claim(name, visibilityTimeoutMs, conn));
    }

    public Single<Boolean> ack(String name, long messageId) {
        return RxJava3Adapter.monoToSingle(inner.ack(name, messageId));
    }
    public Single<Boolean> ack(String name, long messageId, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.ack(name, messageId, conn));
    }

    public Single<Boolean> abandon(String name, long messageId) {
        return RxJava3Adapter.monoToSingle(inner.abandon(name, messageId));
    }
    public Single<Boolean> abandon(String name, long messageId, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.abandon(name, messageId, conn));
    }

    /** {@code Maybe} — empty when {@code messageId} isn't a claimed message. */
    public Maybe<Timestamp> extend(String name, long messageId, long additionalMs) {
        return RxJava3Adapter.monoToMaybe(inner.extend(name, messageId, additionalMs));
    }
    public Maybe<Timestamp> extend(String name, long messageId, long additionalMs, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.extend(name, messageId, additionalMs, conn));
    }

    /** {@code Maybe} — empty when nothing is ready. */
    public Maybe<Map<String, Object>> peek(String name) {
        return RxJava3Adapter.monoToMaybe(inner.peek(name));
    }
    public Maybe<Map<String, Object>> peek(String name, Connection conn) {
        return RxJava3Adapter.monoToMaybe(inner.peek(name, conn));
    }

    public Single<Long> countReady(String name) {
        return RxJava3Adapter.monoToSingle(inner.countReady(name));
    }
    public Single<Long> countReady(String name, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.countReady(name, conn));
    }

    public Single<Long> countClaimed(String name) {
        return RxJava3Adapter.monoToSingle(inner.countClaimed(name));
    }
    public Single<Long> countClaimed(String name, Connection conn) {
        return RxJava3Adapter.monoToSingle(inner.countClaimed(name, conn));
    }
}
