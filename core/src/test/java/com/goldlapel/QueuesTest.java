package com.goldlapel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link QueuesApi} and {@code Utils.queue*} helpers.
 *
 * <p>Phase 5 contract verified:
 * <ul>
 *   <li>{@code enqueue} returns the assigned id from the proxy's RETURNING.
 *   <li>{@code claim} returns a {@link Utils.ClaimedMessage} or {@code null}.
 *   <li>{@code ack} is a separate explicit call — NOT bundled into claim.
 *   <li>{@code abandon} (NACK) releases the lease immediately.
 *   <li>No {@code dequeue} compat shim on {@link QueuesApi}.
 *   <li>The legacy flat {@code enqueue}/{@code dequeue} methods on
 *       {@link GoldLapel} are gone.
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class QueuesTest {

    @Mock Connection conn;
    @Mock PreparedStatement ps;
    @Mock ResultSet rs;

    GoldLapel gl;

    static Map<String, String> fakePatterns() {
        String main = "_goldlapel.queue_jobs";
        Map<String, String> p = new LinkedHashMap<>();
        p.put("enqueue", "INSERT INTO " + main + " (payload) VALUES ($1::jsonb) "
            + "RETURNING id, created_at");
        p.put("claim", "WITH next_msg AS (SELECT id FROM " + main
            + " WHERE status = 'ready' AND visible_at <= NOW() "
            + "ORDER BY visible_at, id FOR UPDATE SKIP LOCKED LIMIT 1) "
            + "UPDATE " + main + " SET status = 'claimed', "
            + "visible_at = NOW() + INTERVAL '1 millisecond' * $1 "
            + "FROM next_msg WHERE " + main + ".id = next_msg.id "
            + "RETURNING " + main + ".id, " + main + ".payload, "
            + main + ".visible_at, " + main + ".created_at");
        p.put("ack", "DELETE FROM " + main + " WHERE id = $1");
        p.put("nack", "UPDATE " + main + " SET status = 'ready', visible_at = NOW() "
            + "WHERE id = $1 AND status = 'claimed' RETURNING id");
        p.put("extend", "WITH target AS (SELECT $1::bigint AS id, "
            + "$2::bigint AS additional_ms) UPDATE " + main + " m "
            + "SET visible_at = m.visible_at + INTERVAL '1 millisecond' * target.additional_ms "
            + "FROM target WHERE m.id = target.id AND m.status = 'claimed' "
            + "RETURNING m.visible_at");
        p.put("peek", "SELECT id, payload, visible_at, status, created_at FROM "
            + main + " WHERE status = 'ready' AND visible_at <= NOW() "
            + "ORDER BY visible_at, id LIMIT 1");
        p.put("count_ready", "SELECT COUNT(*) FROM " + main
            + " WHERE status = 'ready' AND visible_at <= NOW()");
        p.put("count_claimed", "SELECT COUNT(*) FROM " + main
            + " WHERE status = 'claimed'");
        return p;
    }

    @BeforeEach
    void setUp() throws Exception {
        gl = new GoldLapel("postgresql://u:p@host:5432/db", new GoldLapelOptions());
        java.lang.reflect.Field f = GoldLapel.class.getDeclaredField("internalConn");
        f.setAccessible(true);
        f.set(gl, conn);
        Map<String, Object> tables = new LinkedHashMap<>();
        tables.put("main", "jobs");
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("tables", tables);
        entry.put("query_patterns", fakePatterns());
        gl.ddlCache().put("queue:jobs", entry);
    }


    @Nested class NamespaceShape {

        @Test
        void queuesIsAQueuesApi() {
            assertNotNull(gl.queues);
            assertSame(QueuesApi.class, gl.queues.getClass());
        }

        @Test
        void noLegacyFlatMethods() {
            // Phase 5 hard cut — gl.enqueue / gl.dequeue are gone.
            for (String legacy : new String[]{"enqueue", "dequeue"}) {
                assertFalse(java.util.Arrays.stream(GoldLapel.class.getMethods())
                        .anyMatch(m -> m.getName().equals(legacy)),
                    "Phase 5 removed flat " + legacy + " — use gl.queues.<verb>.");
            }
        }

        @Test
        void noDequeueShimOnQueuesApi() {
            // Phase 5 forbids a dequeue alias — claim+ack is explicit by design.
            assertFalse(java.util.Arrays.stream(QueuesApi.class.getMethods())
                    .anyMatch(m -> m.getName().equals("dequeue")),
                "Phase 5 forbids a dequeue alias on QueuesApi.");
        }
    }


    @Nested class EnqueueTest {

        @Test
        void returnsAssignedId() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(99L);

            long id = gl.queues.enqueue("jobs", "{\"task\":\"x\"}");

            assertEquals(99L, id);
            verify(ps).setString(1, "{\"task\":\"x\"}");
        }
    }


    @Nested class ClaimTest {

        @Test
        void returnsClaimedMessageWhenReady() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(7L);
            when(rs.getString(2)).thenReturn("{\"task\":\"send\"}");

            Utils.ClaimedMessage msg = gl.queues.claim("jobs", 30000L);

            assertNotNull(msg);
            assertEquals(7L, msg.id());
            assertEquals("{\"task\":\"send\"}", msg.payload());
            verify(ps).setLong(1, 30000L);
        }

        @Test
        void returnsNullWhenEmpty() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            assertNull(gl.queues.claim("jobs", 30000L));
        }

        @Test
        void claimAndAckAreDistinctSqlCalls() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(7L);
            when(rs.getString(2)).thenReturn("{}");

            gl.queues.claim("jobs", 30000L);
            // claim runs UPDATE…RETURNING — does not delete.
            ArgumentCaptor<String> sqlC = ArgumentCaptor.forClass(String.class);
            verify(conn, atLeastOnce()).prepareStatement(sqlC.capture());
            assertFalse(sqlC.getValue().toUpperCase().contains("DELETE"),
                "claim must not delete — caller acks explicitly");
        }

        @Test
        void defaultClaimVisibilityIs30s() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);

            gl.queues.claim("jobs");
            verify(ps).setLong(1, 30000L);
        }
    }


    @Nested class AckAndAbandonTest {

        @Test
        void ackReturnsTrueWhenDeleted() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeUpdate()).thenReturn(1);
            assertTrue(gl.queues.ack("jobs", 42L));
        }

        @Test
        void ackReturnsFalseWhenIdUnknown() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeUpdate()).thenReturn(0);
            assertFalse(gl.queues.ack("jobs", 999L));
        }

        @Test
        void abandonUsesNackPattern() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            assertTrue(gl.queues.abandon("jobs", 42L));

            ArgumentCaptor<String> sqlC = ArgumentCaptor.forClass(String.class);
            verify(conn).prepareStatement(sqlC.capture());
            // abandon (NACK) sets status back to 'ready'.
            assertTrue(sqlC.getValue().contains("status = 'ready'"));
        }
    }


    @Nested class ExtendTest {

        @Test
        void bindOrderIsIdThenAdditionalMs() throws SQLException {
            // Proxy contract: $1=id, $2=additional_ms.
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            Timestamp ts = new Timestamp(123456L);
            when(rs.getTimestamp(1)).thenReturn(ts);

            Timestamp result = gl.queues.extend("jobs", 42L, 5000L);

            assertEquals(ts, result);
            verify(ps).setLong(1, 42L);
            verify(ps).setLong(2, 5000L);
        }
    }


    @Nested class PeekAndCountTest {

        @Test
        void peekReturnsMapOrNull() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(42L);
            when(rs.getString(2)).thenReturn("{\"work\":\"foo\"}");
            when(rs.getString(4)).thenReturn("ready");

            Map<String, Object> result = gl.queues.peek("jobs");
            assertNotNull(result);
            assertEquals(42L, result.get("id"));
            assertEquals("ready", result.get("status"));
        }

        @Test
        void peekReturnsNullWhenEmpty() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(false);
            assertNull(gl.queues.peek("jobs"));
        }

        @Test
        void countReadyAndCountClaimed() throws SQLException {
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(5L, 2L);
            assertEquals(5L, gl.queues.countReady("jobs"));
            assertEquals(2L, gl.queues.countClaimed("jobs"));
        }
    }
}
