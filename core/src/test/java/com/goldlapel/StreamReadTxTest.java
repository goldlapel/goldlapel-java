package com.goldlapel;

import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link Utils#streamRead} transaction wrapping.
 *
 * Under autocommit, {@code SELECT ... FOR UPDATE} releases its row lock as
 * soon as the statement returns, so concurrent consumers can both advance
 * the group cursor and claim the same pending messages. These tests prove
 * that streamRead wraps the cursor-read → advance → pending-insert sequence
 * in an explicit transaction.
 *
 * The end-to-end concurrency test (two consumers racing on a real Postgres
 * instance) lives in {@link StreamsIntegrationTest} and only runs when
 * {@code GOLDLAPEL_INTEGRATION=1} is set.
 */
class StreamReadTxTest {

    private static Map<String, String> patterns() {
        return Map.of(
            "group_get_cursor",     "SELECT last_delivered_id FROM g WHERE group_name = ? FOR UPDATE",
            "read_since",           "SELECT id, payload, created_at FROM m WHERE id > ? ORDER BY id LIMIT ?",
            "group_advance_cursor", "UPDATE g SET last_delivered_id = ? WHERE group_name = ?",
            "pending_insert",       "INSERT INTO p (message_id, group_name, consumer) VALUES (?, ?, ?)"
        );
    }

    @Test
    void setsAutoCommitFalseAndCommits() throws SQLException {
        Connection conn = mock(Connection.class);
        when(conn.getAutoCommit()).thenReturn(true);

        PreparedStatement cursorPs = mock(PreparedStatement.class);
        ResultSet cursorRs = mock(ResultSet.class);
        when(cursorRs.next()).thenReturn(true, false);
        when(cursorRs.getLong(1)).thenReturn(0L);
        when(cursorPs.executeQuery()).thenReturn(cursorRs);

        PreparedStatement readPs = mock(PreparedStatement.class);
        ResultSet readRs = mock(ResultSet.class);
        when(readRs.next()).thenReturn(false);
        when(readPs.executeQuery()).thenReturn(readRs);

        when(conn.prepareStatement(anyString())).thenAnswer(inv -> {
            String sql = inv.getArgument(0);
            if (sql.contains("FOR UPDATE")) return cursorPs;
            if (sql.contains("ORDER BY id")) return readPs;
            return mock(PreparedStatement.class);
        });

        Utils.streamRead(conn, "s", "g", "c", 10, patterns());

        verify(conn).setAutoCommit(false);
        verify(conn).commit();
        verify(conn, never()).rollback();
        verify(conn).setAutoCommit(true); // restored
    }

    @Test
    void commitsEvenWhenCursorRowMissing() throws SQLException {
        Connection conn = mock(Connection.class);
        when(conn.getAutoCommit()).thenReturn(true);

        PreparedStatement cursorPs = mock(PreparedStatement.class);
        ResultSet cursorRs = mock(ResultSet.class);
        when(cursorRs.next()).thenReturn(false);
        when(cursorPs.executeQuery()).thenReturn(cursorRs);
        when(conn.prepareStatement(anyString())).thenReturn(cursorPs);

        List<Map<String, Object>> result = Utils.streamRead(conn, "s", "g", "c", 10, patterns());

        assertTrue(result.isEmpty());
        verify(conn).setAutoCommit(false);
        verify(conn).commit();
        verify(conn).setAutoCommit(true);
    }

    @Test
    void rollsBackOnException() throws SQLException {
        Connection conn = mock(Connection.class);
        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.prepareStatement(anyString())).thenThrow(new SQLException("boom"));

        SQLException err = assertThrows(SQLException.class,
            () -> Utils.streamRead(conn, "s", "g", "c", 10, patterns()));
        assertEquals("boom", err.getMessage());

        verify(conn).setAutoCommit(false);
        verify(conn).rollback();
        verify(conn, never()).commit();
        verify(conn).setAutoCommit(true);
    }

    @Test
    void preservesExistingTransactionWhenAutoCommitAlreadyFalse() throws SQLException {
        Connection conn = mock(Connection.class);
        when(conn.getAutoCommit()).thenReturn(false); // caller is already in a tx

        PreparedStatement cursorPs = mock(PreparedStatement.class);
        ResultSet cursorRs = mock(ResultSet.class);
        when(cursorRs.next()).thenReturn(false);
        when(cursorPs.executeQuery()).thenReturn(cursorRs);
        when(conn.prepareStatement(anyString())).thenReturn(cursorPs);

        Utils.streamRead(conn, "s", "g", "c", 10, patterns());

        // Caller owns the tx boundary — don't flip autoCommit, don't commit,
        // don't rollback.
        verify(conn, never()).setAutoCommit(anyBoolean());
        verify(conn, never()).commit();
        verify(conn, never()).rollback();
    }
}
