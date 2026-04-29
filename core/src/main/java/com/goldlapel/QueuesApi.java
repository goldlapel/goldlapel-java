package com.goldlapel;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Queue namespace API — accessible as {@code gl.queues}.
 *
 * <p>Phase 5 of schema-to-core. The proxy's v1 queue schema is at-least-once
 * with visibility-timeout — NOT the legacy fire-and-forget shape. The
 * breaking change:
 *
 * <pre>{@code
 * // Before:
 * String payload = gl.dequeue("jobs");                     // delete-on-fetch, may lose work
 *
 * // After:
 * Utils.ClaimedMessage msg = gl.queues.claim("jobs", 30000);
 * if (msg != null) {
 *     // ... handle the work ...
 *     gl.queues.ack("jobs", msg.id());                     // commit
 *     // missing ack → message redelivered after visibility window
 * }
 * }</pre>
 *
 * <p>{@link #claim(String, long)} returns the {@link Utils.ClaimedMessage} or
 * {@code null}. The caller MUST {@link #ack(String, long)} to commit, or
 * {@link #abandon(String, long)} to release the lease immediately. A consumer
 * that crashes leaves the lease standing; the message becomes ready again
 * after {@code visibilityTimeoutMs} and is redelivered to the next claim.
 *
 * <p>No {@code dequeue} compat shim — explicit claim+ack is the contract.
 */
public final class QueuesApi {
    private final GoldLapel gl;

    QueuesApi(GoldLapel gl) {
        this.gl = gl;
    }

    public Map<String, String> patterns(String name) {
        return patterns(name, false);
    }

    public Map<String, String> patterns(String name, boolean unlogged) {
        Utils.validateIdentifier(name);
        String token = gl.dashboardToken() != null ? gl.dashboardToken() : Ddl.tokenFromEnvOrFile();
        Map<String, Object> options = null;
        if (unlogged) {
            options = new java.util.LinkedHashMap<>();
            options.put("unlogged", Boolean.TRUE);
        }
        Map<String, Object> entry = Ddl.fetchPatterns(gl.ddlCache(), "queue", name,
            gl.dashboardPort(), token, options);
        return Ddl.queryPatterns(entry);
    }

    public void create(String name) {
        patterns(name);
    }

    public void create(String name, boolean unlogged) {
        patterns(name, unlogged);
    }

    // ── Queue ops ────────────────────────────────────────────

    /** Add a message; returns its assigned id. */
    public long enqueue(String name, String payloadJson) throws SQLException {
        return Utils.queueEnqueue(gl.resolveConn(), name, payloadJson, patterns(name));
    }

    public long enqueue(String name, String payloadJson, Connection conn) throws SQLException {
        return Utils.queueEnqueue(conn, name, payloadJson, patterns(name));
    }

    /**
     * Claim the next ready message; returns the {@link Utils.ClaimedMessage}
     * or {@code null} when the queue is empty. Default visibility window is
     * 30s; tune via the overload.
     */
    public Utils.ClaimedMessage claim(String name) throws SQLException {
        return claim(name, 30000L);
    }

    public Utils.ClaimedMessage claim(String name, long visibilityTimeoutMs) throws SQLException {
        return Utils.queueClaim(gl.resolveConn(), name, visibilityTimeoutMs, patterns(name));
    }

    public Utils.ClaimedMessage claim(String name, long visibilityTimeoutMs, Connection conn) throws SQLException {
        return Utils.queueClaim(conn, name, visibilityTimeoutMs, patterns(name));
    }

    /** Mark a claimed message done (DELETEs the row). Returns true if the
     *  message existed and was removed. */
    public boolean ack(String name, long messageId) throws SQLException {
        return Utils.queueAck(gl.resolveConn(), name, messageId, patterns(name));
    }

    public boolean ack(String name, long messageId, Connection conn) throws SQLException {
        return Utils.queueAck(conn, name, messageId, patterns(name));
    }

    /** Release a claimed message back to ready immediately so it's
     *  redelivered without waiting for the visibility timeout. Equivalent
     *  to a NACK. */
    public boolean abandon(String name, long messageId) throws SQLException {
        return Utils.queueAbandon(gl.resolveConn(), name, messageId, patterns(name));
    }

    public boolean abandon(String name, long messageId, Connection conn) throws SQLException {
        return Utils.queueAbandon(conn, name, messageId, patterns(name));
    }

    /** Push the visibility deadline forward by {@code additionalMs}.
     *  Returns the new {@code visible_at}, or {@code null} if {@code id}
     *  wasn't a claimed message. */
    public java.sql.Timestamp extend(String name, long messageId, long additionalMs) throws SQLException {
        return Utils.queueExtend(gl.resolveConn(), name, messageId, additionalMs, patterns(name));
    }

    public java.sql.Timestamp extend(String name, long messageId, long additionalMs, Connection conn) throws SQLException {
        return Utils.queueExtend(conn, name, messageId, additionalMs, patterns(name));
    }

    /** Look at the next-ready message without claiming. Returns a map with
     *  id / payload / visible_at / status / created_at, or {@code null}
     *  when nothing is ready. */
    public Map<String, Object> peek(String name) throws SQLException {
        return Utils.queuePeek(gl.resolveConn(), name, patterns(name));
    }

    public Map<String, Object> peek(String name, Connection conn) throws SQLException {
        return Utils.queuePeek(conn, name, patterns(name));
    }

    public long countReady(String name) throws SQLException {
        return Utils.queueCountReady(gl.resolveConn(), name, patterns(name));
    }

    public long countReady(String name, Connection conn) throws SQLException {
        return Utils.queueCountReady(conn, name, patterns(name));
    }

    public long countClaimed(String name) throws SQLException {
        return Utils.queueCountClaimed(gl.resolveConn(), name, patterns(name));
    }

    public long countClaimed(String name, Connection conn) throws SQLException {
        return Utils.queueCountClaimed(conn, name, patterns(name));
    }
}
