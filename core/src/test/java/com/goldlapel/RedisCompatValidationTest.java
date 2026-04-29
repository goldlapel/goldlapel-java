package com.goldlapel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.sql.Connection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression: Redis-compat helpers reject injection-shaped identifier args.
 * Phase 5 surface — counter / zset / hash / queue / geo helpers all
 * validateIdentifier(name) before doing any work. See v0.2 security review
 * finding C1.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RedisCompatValidationTest {

    @Mock Connection conn;

    static final String BAD = "foo; DROP TABLE users--";
    // Stand-in patterns map — identifier validation runs before patterns
    // are even consulted, so an empty map is fine here.
    static final Map<String, String> EMPTY_PATTERNS = java.util.Collections.emptyMap();

    @Test void publishRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.publish(conn, BAD, "m"));
    }

    @Test void subscribeRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.subscribe(conn, BAD, (a, b) -> {}, false));
    }

    @Test void countDistinctRejectsBadTable() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.countDistinct(conn, BAD, "col"));
    }

    @Test void countDistinctRejectsBadColumn() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.countDistinct(conn, "tbl", BAD));
    }

    // ── Counter family ─────────────────────────────────────────

    @Test void counterIncrRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.counterIncr(conn, BAD, "k", 1L, EMPTY_PATTERNS));
    }

    @Test void counterDecrRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.counterDecr(conn, BAD, "k", 1L, EMPTY_PATTERNS));
    }

    @Test void counterSetRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.counterSet(conn, BAD, "k", 1L, EMPTY_PATTERNS));
    }

    @Test void counterGetRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.counterGet(conn, BAD, "k", EMPTY_PATTERNS));
    }

    @Test void counterDeleteRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.counterDelete(conn, BAD, "k", EMPTY_PATTERNS));
    }

    @Test void counterCountKeysRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.counterCountKeys(conn, BAD, EMPTY_PATTERNS));
    }

    // ── Zset family ─────────────────────────────────────────────

    @Test void zsetAddRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.zsetAdd(conn, BAD, "k", "m", 1.0, EMPTY_PATTERNS));
    }

    @Test void zsetIncrByRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.zsetIncrBy(conn, BAD, "k", "m", 1.0, EMPTY_PATTERNS));
    }

    @Test void zsetScoreRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.zsetScore(conn, BAD, "k", "m", EMPTY_PATTERNS));
    }

    @Test void zsetRemoveRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.zsetRemove(conn, BAD, "k", "m", EMPTY_PATTERNS));
    }

    @Test void zsetRangeRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.zsetRange(conn, BAD, "k", 0, 10, true, EMPTY_PATTERNS));
    }

    @Test void zsetRangeByScoreRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.zsetRangeByScore(conn, BAD, "k", 0, 100, 10, 0, EMPTY_PATTERNS));
    }

    @Test void zsetRankRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.zsetRank(conn, BAD, "k", "m", true, EMPTY_PATTERNS));
    }

    @Test void zsetCardRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.zsetCard(conn, BAD, "k", EMPTY_PATTERNS));
    }

    // ── Hash family ─────────────────────────────────────────────

    @Test void hashSetRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.hashSet(conn, BAD, "k", "f", "\"v\"", EMPTY_PATTERNS));
    }

    @Test void hashGetRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.hashGet(conn, BAD, "k", "f", EMPTY_PATTERNS));
    }

    @Test void hashGetAllRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.hashGetAll(conn, BAD, "k", EMPTY_PATTERNS));
    }

    @Test void hashKeysRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.hashKeys(conn, BAD, "k", EMPTY_PATTERNS));
    }

    @Test void hashValuesRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.hashValues(conn, BAD, "k", EMPTY_PATTERNS));
    }

    @Test void hashExistsRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.hashExists(conn, BAD, "k", "f", EMPTY_PATTERNS));
    }

    @Test void hashDeleteRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.hashDelete(conn, BAD, "k", "f", EMPTY_PATTERNS));
    }

    @Test void hashLenRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.hashLen(conn, BAD, "k", EMPTY_PATTERNS));
    }

    // ── Queue family ────────────────────────────────────────────

    @Test void queueEnqueueRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.queueEnqueue(conn, BAD, "{}", EMPTY_PATTERNS));
    }

    @Test void queueClaimRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.queueClaim(conn, BAD, 30000L, EMPTY_PATTERNS));
    }

    @Test void queueAckRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.queueAck(conn, BAD, 1L, EMPTY_PATTERNS));
    }

    @Test void queueAbandonRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.queueAbandon(conn, BAD, 1L, EMPTY_PATTERNS));
    }

    @Test void queueExtendRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.queueExtend(conn, BAD, 1L, 5000L, EMPTY_PATTERNS));
    }

    @Test void queuePeekRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.queuePeek(conn, BAD, EMPTY_PATTERNS));
    }

    @Test void queueCountReadyRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.queueCountReady(conn, BAD, EMPTY_PATTERNS));
    }

    @Test void queueCountClaimedRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.queueCountClaimed(conn, BAD, EMPTY_PATTERNS));
    }

    // ── Geo family ──────────────────────────────────────────────

    @Test void geoAddRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.geoAdd(conn, BAD, "alice", 0.0, 0.0, EMPTY_PATTERNS));
    }

    @Test void geoPosRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.geoPos(conn, BAD, "alice", EMPTY_PATTERNS));
    }

    @Test void geoDistRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.geoDist(conn, BAD, "a", "b", "m", EMPTY_PATTERNS));
    }

    @Test void geoRadiusRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.geoRadius(conn, BAD, 0.0, 0.0, 100, "m", 10, EMPTY_PATTERNS));
    }

    @Test void geoRadiusByMemberRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.geoRadiusByMember(conn, BAD, "alice", 100, "m", 10, EMPTY_PATTERNS));
    }

    @Test void geoRemoveRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.geoRemove(conn, BAD, "alice", EMPTY_PATTERNS));
    }

    @Test void geoCountRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.geoCount(conn, BAD, EMPTY_PATTERNS));
    }

    // ── Streams ─────────────────────────────────────────────────
    // Streams accept a `patterns` kwarg from the proxy — null is OK here
    // because identifier validation runs first.
    @Test void streamAddRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.streamAdd(conn, BAD, "{}", null));
    }

    @Test void streamCreateGroupRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.streamCreateGroup(conn, BAD, "g", null));
    }

    @Test void streamReadRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.streamRead(conn, BAD, "g", "c", 1, null));
    }

    @Test void streamAckRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.streamAck(conn, BAD, "g", 1L, null));
    }

    @Test void streamClaimRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.streamClaim(conn, BAD, "g", "c", 60000L, null));
    }
}
