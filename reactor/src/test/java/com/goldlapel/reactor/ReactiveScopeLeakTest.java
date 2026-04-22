package com.goldlapel.reactor;

import com.goldlapel.GoldLapel;
import com.goldlapel.GoldLapelOptions;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Regression test for scope leaks across parallel Mono chains. Mirrors the
 * Ruby {@code test_async_native.rb::test_using_scope_under_async_reactor}
 * test which guards the same bug class in the Ruby wrapper.
 *
 * <p>If {@link ReactiveGoldLapel#using} stored the scoped connection on the
 * instance (field / ThreadLocal) instead of propagating via Reactor
 * {@link reactor.util.context.ContextView}, a parallel chain running on a
 * different {@link Schedulers#parallel()} worker would observe chain A's
 * scoped connection — a cross-subscription leak. This test forces actual
 * parallelism with {@link CountDownLatch} synchronisation and asserts that
 * the parallel chain sees the default connection, not the scoped one.
 */
class ReactiveScopeLeakTest {

    private static ReactiveGoldLapel newInstance(GoldLapel sync) throws Exception {
        Constructor<ReactiveGoldLapel> ctor = ReactiveGoldLapel.class
            .getDeclaredConstructor(GoldLapel.class, io.r2dbc.spi.ConnectionFactory.class);
        ctor.setAccessible(true);
        return ctor.newInstance(sync, null);
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

    /**
     * Chain A enters {@code using(connA, …)} and parks inside the body on
     * a latch. Chain B runs concurrently on a different {@code parallel()}
     * worker, reads the scoped CONN_KEY via {@code deferContextual}, and
     * records what it saw. Chain B MUST observe the default (internal)
     * connection — not connA. If it sees connA, scope has leaked across
     * subscriptions (i.e. the wrapper stores scope on the instance instead
     * of propagating via Reactor Context).
     */
    @Test
    void usingScopeDoesNotLeakToParallelChain() throws Exception {
        Connection internal = mock(Connection.class);
        Connection connA = mock(Connection.class);
        GoldLapel sync = syncWithConnection(internal);
        ReactiveGoldLapel gl = newInstance(sync);

        CountDownLatch aEnteredUsing = new CountDownLatch(1);
        CountDownLatch bFinishedObservation = new CountDownLatch(1);
        AtomicReference<Connection> observedByB = new AtomicReference<>();
        AtomicReference<Connection> observedByA = new AtomicReference<>();

        // Chain A: enter using(connA, …), signal entry, block until B has
        // made its observation. While A is suspended inside using(), B is
        // free to run — but B's subscription has its OWN Reactor Context,
        // which does NOT include connA.
        Mono<Void> chainA = gl.using(connA, g ->
            Mono.deferContextual(ctx -> {
                // Record what A sees — should be connA.
                observedByA.set(ctx.getOrDefault(ReactiveGoldLapel.CONN_KEY, internal));
                aEnteredUsing.countDown();
                return Mono.fromRunnable(() -> {
                    try {
                        if (!bFinishedObservation.await(10, TimeUnit.SECONDS)) {
                            throw new AssertionError(
                                "Chain B did not finish its observation within 10s");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }).subscribeOn(Schedulers.boundedElastic()).then();
            })
        ).subscribeOn(Schedulers.parallel()).then();

        // Chain B: wait for A to be inside using(), then read the CONN_KEY
        // from B's own Reactor Context via deferContextual. This is exactly
        // the same mechanism every wrapper method uses (resolveConn →
        // ctx.getOrDefault(CONN_KEY, sync.connection())). B MUST see the
        // default (internal) — seeing connA would mean scope leaked from
        // A's subscription to B's.
        Mono<Void> chainB = Mono.<Void>defer(() -> {
            try {
                if (!aEnteredUsing.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("Chain A did not enter using() within 10s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Mono.error(e);
            }
            return Mono.deferContextual(ctx -> {
                observedByB.set(ctx.getOrDefault(ReactiveGoldLapel.CONN_KEY, internal));
                bFinishedObservation.countDown();
                return Mono.empty();
            });
        }).subscribeOn(Schedulers.parallel());

        // Run both in parallel. Mono.when subscribes to both eagerly, and
        // subscribeOn(Schedulers.parallel()) on each forces them onto
        // different workers.
        Mono.when(chainA, chainB).block(java.time.Duration.ofSeconds(30));

        // Sanity: A should have seen its own scoped conn.
        assertSame(connA, observedByA.get(),
            "chain A inside using(connA) should see connA in its Context");

        // The actual regression assertion: B must NOT see connA.
        assertNotSame(connA, observedByB.get(),
            "scope leak: parallel chain B observed chain A's scoped connection — "
            + "using() is not propagating via Reactor Context correctly");
        assertSame(internal, observedByB.get(),
            "parallel chain B should fall back to the default (internal) connection");
    }

    /**
     * Variant that exercises the full {@code resolveConn} path (the one
     * every wrapper method actually uses) rather than probing CONN_KEY
     * directly. If the wrapper stored scope on the instance, a parallel
     * chain calling {@code resolveConn} concurrently would see the leaked
     * conn. Probes via reflection to avoid needing a mocked prepared
     * statement.
     */
    @Test
    void resolveConnDoesNotLeakAcrossParallelSubscriptions() throws Exception {
        Connection internal = mock(Connection.class);
        Connection connA = mock(Connection.class);
        GoldLapel sync = syncWithConnection(internal);
        ReactiveGoldLapel gl = newInstance(sync);

        java.lang.reflect.Method resolveConn = ReactiveGoldLapel.class
            .getDeclaredMethod("resolveConn",
                reactor.util.context.ContextView.class, Connection.class);
        resolveConn.setAccessible(true);

        CountDownLatch aEntered = new CountDownLatch(1);
        CountDownLatch bDone = new CountDownLatch(1);
        AtomicReference<Connection> bSaw = new AtomicReference<>();

        Mono<Void> chainA = gl.using(connA, g ->
            Mono.<Void>deferContextual(ctx -> {
                aEntered.countDown();
                return Mono.fromRunnable(() -> {
                    try {
                        bDone.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }).subscribeOn(Schedulers.boundedElastic()).then();
            })
        ).subscribeOn(Schedulers.parallel()).then();

        Mono<Void> chainB = Mono.<Void>defer(() -> {
            try {
                aEntered.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Mono.error(e);
            }
            return Mono.deferContextual(ctx -> {
                try {
                    bSaw.set((Connection) resolveConn.invoke(gl, ctx, null));
                } catch (ReflectiveOperationException e) {
                    return Mono.error(e);
                }
                bDone.countDown();
                return Mono.empty();
            });
        }).subscribeOn(Schedulers.parallel());

        Mono.when(chainA, chainB).block(java.time.Duration.ofSeconds(30));

        assertNotSame(connA, bSaw.get(),
            "resolveConn on a parallel subscription must not pick up connA "
            + "from chain A's concurrent using() scope");
        assertSame(internal, bSaw.get(),
            "resolveConn on a parallel subscription with no scope should "
            + "return the default (internal) connection");
    }
}
