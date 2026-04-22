package com.goldlapel.reactor;

import com.goldlapel.GoldLapel;
import com.goldlapel.GoldLapelOptions;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests that exercise {@link ReactiveGoldLapel} without spawning the
 * proxy subprocess or opening real R2DBC connections. Constructs instances
 * via reflection against the package-private constructor, injects a mock
 * sync {@link GoldLapel} with a mock internal {@link Connection}, and
 * verifies:
 *
 * <ul>
 *   <li>{@link ReactiveGoldLapel#using} propagates the scoped connection
 *       through Reactor Context.
 *   <li>Explicit {@code Connection} parameters win over the context.
 *   <li>Context does NOT leak out of the {@code using} block.
 *   <li>URL helpers ({@link ReactiveGoldLapel#parseDatabase}) work.
 * </ul>
 */
class ReactiveGoldLapelUnitTest {

    private static ReactiveGoldLapel newInstance(GoldLapel sync) throws Exception {
        Constructor<ReactiveGoldLapel> ctor = ReactiveGoldLapel.class
            .getDeclaredConstructor(GoldLapel.class, io.r2dbc.spi.ConnectionFactory.class);
        ctor.setAccessible(true);
        return ctor.newInstance(sync, null);
    }

    private static GoldLapel syncWithConnection(Connection internal) throws Exception {
        // Build an unstarted sync GoldLapel via the package-private ctor
        // (reflection — we're in a different package) and inject the mock
        // internal connection.
        Constructor<GoldLapel> ctor = GoldLapel.class
            .getDeclaredConstructor(String.class, GoldLapelOptions.class);
        ctor.setAccessible(true);
        GoldLapel gl = ctor.newInstance("postgresql://u:p@host:5432/db", new GoldLapelOptions());
        java.lang.reflect.Field f = GoldLapel.class.getDeclaredField("internalConn");
        f.setAccessible(true);
        f.set(gl, internal);
        return gl;
    }

    // Mock a Connection such that PreparedStatement.executeQuery() returns
    // a ResultSet whose first row has a single long value matching `count`.
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

    @Test
    void usingPutsConnectionInContext() throws Exception {
        Connection internal = mock(Connection.class);
        Connection scoped = mock(Connection.class);
        GoldLapel sync = syncWithConnection(internal);
        ReactiveGoldLapel gl = newInstance(sync);

        // Probe: read the context inside the using block and assert it's
        // the scoped connection, not the internal one.
        Mono<Connection> probe = gl.using(scoped, g ->
            Mono.deferContextual(ctx -> Mono.just(
                ctx.getOrDefault(ReactiveGoldLapel.CONN_KEY, internal)))
        );
        StepVerifier.create(probe)
            .expectNext(scoped)
            .verifyComplete();
    }

    @Test
    void usingDoesNotLeakOutOfScope() throws Exception {
        Connection internal = mock(Connection.class);
        Connection scoped = mock(Connection.class);
        GoldLapel sync = syncWithConnection(internal);
        ReactiveGoldLapel gl = newInstance(sync);

        // Compose: using(scoped, ...) returns a Mono whose Context has
        // CONN_KEY=scoped ONLY while evaluating the body. After using
        // returns, concat a downstream Mono that reads CONN_KEY — it
        // should fall back to the default.
        Mono<String> inside = gl.using(scoped, g ->
            Mono.deferContextual(ctx -> Mono.just("inside:" + ctx.getOrDefault(ReactiveGoldLapel.CONN_KEY, internal)))
        );
        Mono<String> outside = Mono.deferContextual(ctx ->
            Mono.just("outside:" + ctx.getOrDefault(ReactiveGoldLapel.CONN_KEY, internal))
        );
        StepVerifier.create(inside.zipWith(outside))
            .assertNext(tuple -> {
                assertTrue(tuple.getT1().contains(scoped.toString()), "inside should see scoped");
                assertTrue(tuple.getT2().contains(internal.toString()), "outside should see default");
            })
            .verifyComplete();
    }

    @Test
    void nestedUsingRestoresOuterConnection() throws Exception {
        Connection internal = mock(Connection.class);
        Connection outer = mock(Connection.class);
        Connection inner = mock(Connection.class);
        GoldLapel sync = syncWithConnection(internal);
        ReactiveGoldLapel gl = newInstance(sync);

        // using(outer, g -> using(inner, ...).then(readContext()))
        // After the inner using completes, the downstream read should
        // still see OUTER — not default, not inner.
        Mono<Connection> probe = gl.using(outer, g -> {
            Mono<Connection> innerRead = g.using(inner, h ->
                Mono.deferContextual(ctx -> Mono.just(
                    (Connection) ctx.getOrDefault(ReactiveGoldLapel.CONN_KEY, internal)))
            );
            Mono<Connection> outerRead = Mono.deferContextual(ctx ->
                Mono.just((Connection) ctx.getOrDefault(ReactiveGoldLapel.CONN_KEY, internal)));
            return innerRead.then(outerRead);
        });

        StepVerifier.create(probe)
            .expectNext(outer)
            .verifyComplete();
    }

    @Test
    void usingRejectsNullArgs() throws Exception {
        GoldLapel sync = syncWithConnection(mock(Connection.class));
        ReactiveGoldLapel gl = newInstance(sync);

        assertThrows(IllegalArgumentException.class, () -> gl.using(null, g -> Mono.empty()));
        assertThrows(IllegalArgumentException.class, () -> gl.using(mock(Connection.class), null));
    }

    @Test
    void wrapperMethodReadsConnectionFromContext() throws Exception {
        // End-to-end: docCount via wrapper method, with using() pinning the
        // scoped connection. The scoped mock is configured to return 42.
        Connection internal = mockConnForDocCount(99); // default/internal
        Connection scoped = mockConnForDocCount(42);   // visible inside using
        GoldLapel sync = syncWithConnection(internal);
        ReactiveGoldLapel gl = newInstance(sync);

        StepVerifier.create(
            gl.using(scoped, g -> g.docCount("events", "{}"))
        ).expectNext(42L).verifyComplete();

        // Without using, the internal connection is used
        StepVerifier.create(gl.docCount("events", "{}"))
            .expectNext(99L)
            .verifyComplete();
    }

    @Test
    void explicitConnArgumentWinsOverContext() throws Exception {
        Connection internal = mockConnForDocCount(99);
        Connection scoped = mockConnForDocCount(42);
        Connection explicit = mockConnForDocCount(7);
        GoldLapel sync = syncWithConnection(internal);
        ReactiveGoldLapel gl = newInstance(sync);

        StepVerifier.create(
            gl.using(scoped, g -> g.docCount("events", "{}", explicit))
        ).expectNext(7L).verifyComplete();
    }

    @Test
    void parseDatabaseExtractsName() {
        assertEquals("mydb", ReactiveGoldLapel.parseDatabase("jdbc:postgresql://localhost:5432/mydb"));
        assertEquals("mydb", ReactiveGoldLapel.parseDatabase("jdbc:postgresql://localhost/mydb"));
        assertEquals("mydb", ReactiveGoldLapel.parseDatabase("jdbc:postgresql://localhost:5432/mydb?sslmode=require"));
        assertNull(ReactiveGoldLapel.parseDatabase("jdbc:postgresql://localhost:5432/"));
        assertNull(ReactiveGoldLapel.parseDatabase("jdbc:postgresql://localhost:5432"));
        assertNull(ReactiveGoldLapel.parseDatabase(null));
        assertNull(ReactiveGoldLapel.parseDatabase("postgresql://localhost/foo")); // no jdbc: prefix
    }

    @Test
    void stopIsIdempotent() throws Exception {
        Connection internal = mock(Connection.class);
        GoldLapel sync = syncWithConnection(internal);
        ReactiveGoldLapel gl = newInstance(sync);

        StepVerifier.create(gl.stop()).verifyComplete();
        StepVerifier.create(gl.stop()).verifyComplete(); // second call does not throw
    }

    @Test
    void closeDelegatesToStop() throws Exception {
        Connection internal = mock(Connection.class);
        GoldLapel sync = syncWithConnection(internal);
        ReactiveGoldLapel gl = newInstance(sync);

        gl.close();
        // After close() the sync instance nulls its internalConn and throws
        // IllegalStateException on connection() — verify that path.
        assertThrows(IllegalStateException.class, sync::connection);
    }

    @Test
    void accessorsDelegateToSync() throws Exception {
        Connection internal = mock(Connection.class);
        GoldLapel sync = syncWithConnection(internal);
        ReactiveGoldLapel gl = newInstance(sync);

        assertEquals(7932, gl.getProxyPort());
        // proxyUrl is null until start() runs, but the accessor should not NPE
        assertNull(gl.getUrl());
        assertNull(gl.getJdbcUrl());
        assertNull(gl.getDashboardUrl()); // process is null → null URL
        assertFalse(gl.isRunning());
        assertSame(internal, gl.connection());
        assertSame(sync, gl.sync());
    }
}
