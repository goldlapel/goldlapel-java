package com.goldlapel;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Streams namespace API — accessible as {@code gl.streams}.
 *
 * <p>Wraps the wire-level stream methods in a sub-API instance held on the
 * parent {@link GoldLapel} client. The instance shares all state (license,
 * dashboard token, http session, conn, DDL pattern cache) by reference back
 * to the parent — no duplication.
 *
 * <p>This is the canonical sub-API shape for the schema-to-core wrapper
 * rollout. Other namespaces ({@code search}, {@code cache}, {@code publish}/
 * {@code subscribe}, {@code incr}, {@code zadd}, {@code hset}, {@code geoadd},
 * …) stay flat for now; they migrate to nested form one-at-a-time as their
 * own schema-to-core phase fires.
 */
public final class StreamsApi {
    private final GoldLapel gl;

    StreamsApi(GoldLapel gl) {
        // Hold a back-reference to the parent client. Never copy lifecycle
        // state (token, port, conn) onto this instance — always read through
        // `gl` so a config change on the parent (e.g. proxy restart with a
        // new dashboard token) is reflected immediately on the next call.
        this.gl = gl;
    }

    /**
     * Fetch (and cache per-instance) canonical stream DDL + query patterns.
     * Public so reactor/rxjava3 wrappers in sibling artifacts can reuse the
     * same cache.
     */
    public Map<String, String> patterns(String stream) {
        return gl.streamPatterns(stream);
    }

    public long add(String stream, String payload) throws SQLException {
        return Utils.streamAdd(gl.resolveConn(), stream, payload, patterns(stream));
    }

    public long add(String stream, String payload, Connection conn) throws SQLException {
        return Utils.streamAdd(conn, stream, payload, patterns(stream));
    }

    public void createGroup(String stream, String group) throws SQLException {
        Utils.streamCreateGroup(gl.resolveConn(), stream, group, patterns(stream));
    }

    public void createGroup(String stream, String group, Connection conn) throws SQLException {
        Utils.streamCreateGroup(conn, stream, group, patterns(stream));
    }

    public List<Map<String, Object>> read(String stream, String group,
            String consumer, int count) throws SQLException {
        return Utils.streamRead(gl.resolveConn(), stream, group, consumer, count, patterns(stream));
    }

    public List<Map<String, Object>> read(String stream, String group,
            String consumer, int count, Connection conn) throws SQLException {
        return Utils.streamRead(conn, stream, group, consumer, count, patterns(stream));
    }

    public boolean ack(String stream, String group, long messageId) throws SQLException {
        return Utils.streamAck(gl.resolveConn(), stream, group, messageId, patterns(stream));
    }

    public boolean ack(String stream, String group, long messageId, Connection conn) throws SQLException {
        return Utils.streamAck(conn, stream, group, messageId, patterns(stream));
    }

    public List<Map<String, Object>> claim(String stream, String group,
            String consumer, long minIdleMs) throws SQLException {
        return Utils.streamClaim(gl.resolveConn(), stream, group, consumer, minIdleMs, patterns(stream));
    }

    public List<Map<String, Object>> claim(String stream, String group,
            String consumer, long minIdleMs, Connection conn) throws SQLException {
        return Utils.streamClaim(conn, stream, group, consumer, minIdleMs, patterns(stream));
    }
}
