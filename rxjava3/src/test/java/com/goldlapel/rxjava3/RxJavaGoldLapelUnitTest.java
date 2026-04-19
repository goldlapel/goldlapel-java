package com.goldlapel.rxjava3;

import com.goldlapel.GoldLapel;
import com.goldlapel.GoldLapelOptions;
import com.goldlapel.reactor.ReactiveGoldLapel;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link RxJavaGoldLapel}.
 *
 * <p>Scope: verify the adapter translation from Reactor types to RxJava 3
 * types produces the expected shapes and terminal events. The underlying
 * JDBC behaviour is already exhaustively tested in core + reactor — here
 * we only need to prove the adapter is correctly applied.
 *
 * <p>Spot-checks across the four shape types:
 * <ul>
 *   <li>{@code Single<T>}   — docCount (JDBC-backed count)
 *   <li>{@code Flowable<T>} — docFind (JDBC-backed list)
 *   <li>{@code Completable} — publish (JDBC-backed void)
 *   <li>{@link TestObserver}/{@link TestSubscriber} — explicit RxJava terminal-event asserts
 * </ul>
 *
 * <p>Also verifies: lifecycle (close delegates to stop), accessors
 * delegate through, and the {@code reactor()} escape hatch returns the
 * same instance.
 */
class RxJavaGoldLapelUnitTest {

    private static RxJavaGoldLapel newInstance(GoldLapel sync) throws Exception {
        // Build a ReactiveGoldLapel via its package-private ctor with a null
        // ConnectionFactory (we never exercise the R2DBC path in unit tests),
        // then wrap it in RxJavaGoldLapel.
        Constructor<ReactiveGoldLapel> reactiveCtor = ReactiveGoldLapel.class
            .getDeclaredConstructor(GoldLapel.class, io.r2dbc.spi.ConnectionFactory.class);
        reactiveCtor.setAccessible(true);
        ReactiveGoldLapel reactive = reactiveCtor.newInstance(sync, null);
        return new RxJavaGoldLapel(reactive);
    }

    private static GoldLapel syncWithConnection(Connection internal) throws Exception {
        Constructor<GoldLapel> ctor = GoldLapel.class
            .getDeclaredConstructor(String.class, GoldLapelOptions.class);
        ctor.setAccessible(true);
        GoldLapel gl = ctor.newInstance("postgresql://u:p@host:5432/db", new GoldLapelOptions());
        java.lang.reflect.Field f = GoldLapel.class.getDeclaredField("internalConn");
        f.setAccessible(true);
        f.set(gl, internal);
        return gl;
    }

    private static Connection mockConnForDocCount(long count) throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getLong(1)).thenReturn(count);
        return conn;
    }

    // Mock a Connection such that `executeQuery` returns a two-row
    // single-column string ResultSet — matches what Utils.docDistinct uses.
    private static Connection mockConnForDocDistinct(String... values) throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        Boolean[] hasNext = new Boolean[values.length + 1];
        for (int i = 0; i < values.length; i++) hasNext[i] = true;
        hasNext[values.length] = false;
        when(rs.next()).thenReturn(hasNext[0], java.util.Arrays.copyOfRange(hasNext, 1, hasNext.length));
        if (values.length > 0) {
            when(rs.getString(1)).thenReturn(values[0], java.util.Arrays.copyOfRange(values, 1, values.length));
        }
        return conn;
    }

    private static Connection mockConnForPublish() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeUpdate()).thenReturn(0);
        return conn;
    }

    // ── Single<T> shape ───────────────────────────────────────

    @Test
    void docCountReturnsSingleAndEmitsValue() throws Exception {
        Connection internal = mockConnForDocCount(42);
        GoldLapel sync = syncWithConnection(internal);
        RxJavaGoldLapel gl = newInstance(sync);

        Single<Long> single = gl.docCount("events", "{}");
        Long result = single.blockingGet();
        assertEquals(42L, result);
    }

    @Test
    void docCountEmitsViaTestObserver() throws Exception {
        Connection internal = mockConnForDocCount(7);
        GoldLapel sync = syncWithConnection(internal);
        RxJavaGoldLapel gl = newInstance(sync);

        TestObserver<Long> obs = gl.docCount("events", "{}").test();
        obs.await();
        obs.assertComplete();
        obs.assertNoErrors();
        obs.assertValue(7L);
    }

    // ── Flowable<T> shape ─────────────────────────────────────

    @Test
    void docDistinctReturnsFlowableAndEmitsItems() throws Exception {
        Connection internal = mockConnForDocDistinct("alpha", "beta", "gamma");
        GoldLapel sync = syncWithConnection(internal);
        RxJavaGoldLapel gl = newInstance(sync);

        Flowable<String> flowable = gl.docDistinct("events", "type", null);
        TestSubscriber<String> ts = flowable.test();
        ts.await();
        ts.assertComplete();
        ts.assertNoErrors();
        ts.assertValues("alpha", "beta", "gamma");
    }

    // ── Maybe<T> shape (nullable returns) ─────────────────────

    // Mock a Connection for Utils.hget: executeQuery returns a ResultSet
    // whose next() is `hasRow`; if hasRow, getString(1) returns `value`
    // (or null, since hget returns null when the column is SQL-NULL).
    private static Connection mockConnForHget(boolean hasRow, String value) throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(hasRow);
        if (hasRow) {
            when(rs.getString(1)).thenReturn(value);
        }
        return conn;
    }

    @Test
    void hgetReturnsMaybeAndEmitsValueWhenPresent() throws Exception {
        Connection internal = mockConnForHget(true, "hello");
        GoldLapel sync = syncWithConnection(internal);
        RxJavaGoldLapel gl = newInstance(sync);

        Maybe<String> maybe = gl.hget("h", "key1", "f1");
        TestObserver<String> obs = maybe.test();
        obs.await();
        obs.assertComplete();
        obs.assertNoErrors();
        obs.assertValue("hello");
    }

    @Test
    void hgetReturnsEmptyMaybeWhenAbsent() throws Exception {
        // No row → hget returns null → empty Maybe (not an error).
        Connection internal = mockConnForHget(false, null);
        GoldLapel sync = syncWithConnection(internal);
        RxJavaGoldLapel gl = newInstance(sync);

        Maybe<String> maybe = gl.hget("h", "missing", "f1");
        TestObserver<String> obs = maybe.test();
        obs.await();
        obs.assertComplete();
        obs.assertNoErrors();
        obs.assertNoValues();
    }

    // ── Completable shape ─────────────────────────────────────

    @Test
    void publishReturnsCompletableAndCompletes() throws Exception {
        Connection internal = mockConnForPublish();
        GoldLapel sync = syncWithConnection(internal);
        RxJavaGoldLapel gl = newInstance(sync);

        Completable c = gl.publish("chan", "hello");
        TestObserver<Void> obs = c.test();
        obs.await();
        obs.assertComplete();
        obs.assertNoErrors();
    }

    // ── Error propagation ─────────────────────────────────────

    @Test
    void sqlExceptionPropagatesAsRxError() throws Exception {
        Connection internal = mock(Connection.class);
        when(internal.prepareStatement(anyString())).thenThrow(new java.sql.SQLException("boom"));
        GoldLapel sync = syncWithConnection(internal);
        RxJavaGoldLapel gl = newInstance(sync);

        TestObserver<Long> obs = gl.docCount("events", "{}").test();
        obs.await();
        obs.assertError(err -> err instanceof java.sql.SQLException
                            || err.getCause() instanceof java.sql.SQLException);
    }

    // ── Explicit Connection overload ──────────────────────────

    @Test
    void explicitConnectionArgumentUsesThatConnection() throws Exception {
        Connection internal = mockConnForDocCount(99);
        Connection explicit = mockConnForDocCount(3);
        GoldLapel sync = syncWithConnection(internal);
        RxJavaGoldLapel gl = newInstance(sync);

        assertEquals(3L, gl.docCount("events", "{}", explicit).blockingGet());
        assertEquals(99L, gl.docCount("events", "{}").blockingGet());
    }

    // ── Lifecycle ─────────────────────────────────────────────

    @Test
    void stopReturnsCompletableAndIsIdempotent() throws Exception {
        Connection internal = mock(Connection.class);
        GoldLapel sync = syncWithConnection(internal);
        RxJavaGoldLapel gl = newInstance(sync);

        gl.stop().blockingAwait();
        gl.stop().blockingAwait(); // idempotent — does not throw
    }

    @Test
    void closeDelegatesToStop() throws Exception {
        Connection internal = mock(Connection.class);
        GoldLapel sync = syncWithConnection(internal);
        RxJavaGoldLapel gl = newInstance(sync);

        gl.close();
        assertThrows(IllegalStateException.class, sync::connection);
    }

    // ── Accessors ─────────────────────────────────────────────

    @Test
    void accessorsDelegateThrough() throws Exception {
        Connection internal = mock(Connection.class);
        GoldLapel sync = syncWithConnection(internal);
        RxJavaGoldLapel gl = newInstance(sync);

        assertEquals(7932, gl.getPort());
        assertNull(gl.getUrl());
        assertNull(gl.getJdbcUrl());
        assertNull(gl.getDashboardUrl());
        assertFalse(gl.isRunning());
        assertSame(internal, gl.connection());
        assertSame(sync, gl.sync());
    }

    @Test
    void reactorAccessorReturnsUnderlyingReactiveInstance() throws Exception {
        Connection internal = mock(Connection.class);
        GoldLapel sync = syncWithConnection(internal);
        RxJavaGoldLapel gl = newInstance(sync);

        ReactiveGoldLapel reactor = gl.reactor();
        assertNotNull(reactor);
        assertSame(sync, reactor.sync());
    }

    // ── Start-factory input validation ────────────────────────
    // We don't call start() with a real URL here — that would spawn the
    // Rust binary. The happy path is covered by the integration test.
    // We only assert that the static factory returns a Single (not null,
    // non-terminal until subscribed).

    @Test
    void startFactoriesReturnSingleType() {
        Single<RxJavaGoldLapel> s1 = RxJavaGoldLapel.start("postgresql://bogus/x");
        Single<RxJavaGoldLapel> s2 = RxJavaGoldLapel.start("postgresql://bogus/x", opts -> opts.setPort(1));
        assertNotNull(s1);
        assertNotNull(s2);
    }
}
