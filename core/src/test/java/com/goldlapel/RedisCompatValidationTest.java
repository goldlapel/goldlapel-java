package com.goldlapel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression: Redis-compat helpers reject injection-shaped identifier args.
 * See v0.2 security review finding C1.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RedisCompatValidationTest {

    @Mock Connection conn;

    static final String BAD = "foo; DROP TABLE users--";

    @Test void publishRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.publish(conn, BAD, "m"));
    }

    @Test void subscribeRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.subscribe(conn, BAD, (a, b) -> {}, false));
    }

    @Test void enqueueRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.enqueue(conn, BAD, "{}"));
    }

    @Test void dequeueRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.dequeue(conn, BAD));
    }

    @Test void incrRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.incr(conn, BAD, "k", 1));
    }

    @Test void getCounterRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.getCounter(conn, BAD, "k"));
    }

    @Test void zaddRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.zadd(conn, BAD, "m", 1.0));
    }

    @Test void zincrbyRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.zincrby(conn, BAD, "m", 1.0));
    }

    @Test void zrangeRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.zrange(conn, BAD, 0, 10, true));
    }

    @Test void zrankRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.zrank(conn, BAD, "m", true));
    }

    @Test void zscoreRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.zscore(conn, BAD, "m"));
    }

    @Test void zremRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.zrem(conn, BAD, "m"));
    }

    @Test void hsetRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.hset(conn, BAD, "k", "f", "\"v\""));
    }

    @Test void hgetRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.hget(conn, BAD, "k", "f"));
    }

    @Test void hgetallRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.hgetall(conn, BAD, "k"));
    }

    @Test void hdelRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.hdel(conn, BAD, "k", "f"));
    }

    @Test void countDistinctRejectsBadTable() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.countDistinct(conn, BAD, "col"));
    }

    @Test void countDistinctRejectsBadColumn() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.countDistinct(conn, "tbl", BAD));
    }

    @Test void geoaddRejectsBadTable() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.geoadd(conn, BAD, "name", "geom", "x", 0.0, 0.0));
    }

    @Test void geoaddRejectsBadNameColumn() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.geoadd(conn, "tbl", BAD, "geom", "x", 0.0, 0.0));
    }

    @Test void geoaddRejectsBadGeomColumn() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.geoadd(conn, "tbl", "name", BAD, "x", 0.0, 0.0));
    }

    @Test void georadiusRejectsBadTable() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.georadius(conn, BAD, "geom", 0.0, 0.0, 100.0, 10));
    }

    @Test void georadiusRejectsBadGeomColumn() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.georadius(conn, "tbl", BAD, 0.0, 0.0, 100.0, 10));
    }

    @Test void geodistRejectsBadTable() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.geodist(conn, BAD, "geom", "name", "a", "b"));
    }

    @Test void streamAddRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.streamAdd(conn, BAD, "{}"));
    }

    @Test void streamCreateGroupRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.streamCreateGroup(conn, BAD, "g"));
    }

    @Test void streamReadRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.streamRead(conn, BAD, "g", "c", 1));
    }

    @Test void streamAckRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.streamAck(conn, BAD, "g", 1L));
    }

    @Test void streamClaimRejectsBad() {
        assertThrows(IllegalArgumentException.class,
                () -> Utils.streamClaim(conn, BAD, "g", "c", 60000L));
    }
}
