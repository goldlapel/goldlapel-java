package com.goldlapel.reactor;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.ServerSocket;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration test: real Postgres upstream, real proxy binary
 * (spawned by the sync wrapper), real R2DBC driver on top.
 *
 * <p>Gated on the standardized Gold Lapel integration-test convention
 * (GOLDLAPEL_INTEGRATION=1 + GOLDLAPEL_TEST_UPSTREAM) — see
 * {@link IntegrationGate}.
 *
 * <p>Run locally:
 * <pre>
 *   export GOLDLAPEL_BINARY=/home/you/dev/gl/goldlapel/target/release/goldlapel
 *   export GOLDLAPEL_INTEGRATION=1
 *   export GOLDLAPEL_TEST_UPSTREAM=postgresql://postgres@127.0.0.1/postgres
 *   ./mvnw -pl reactor test
 * </pre>
 *
 * <p>Covers:
 * <ul>
 *   <li>{@code start} → wrapper method (docInsert + docCount + docFind)
 *       → {@code stop}, all chained via Mono/Flux operators
 *   <li>{@link ReactiveGoldLapel#using} with a scoped JDBC connection
 *   <li>{@link ReactiveGoldLapel#connectionFactory()} — raw R2DBC query
 *       against the proxy, verifying the extended-protocol / CloseComplete
 *       path works with the fixed proxy binary on main.
 * </ul>
 */
@EnabledIfEnvironmentVariable(named = "GOLDLAPEL_INTEGRATION", matches = "1")
class ReactiveIntegrationTest {

    static String upstream;
    static int proxyPort;

    @BeforeAll
    static void setup() throws Exception {
        // requireUpstream() throws if GOLDLAPEL_TEST_UPSTREAM is missing,
        // surfacing half-configured CI as a loud failure rather than a
        // silent skip.
        upstream = IntegrationGate.requireUpstream();
        // Pick a free port to avoid collisions with a running dev proxy.
        try (ServerSocket s = new ServerSocket(0)) {
            proxyPort = s.getLocalPort();
        }
    }

    @Test
    void endToEndChainedMono() {
        // Unique collection name per run so tests don't interfere.
        String coll = "rx_it_chain_" + System.nanoTime();

        Mono<Long> pipeline = ReactiveGoldLapel.start(upstream, opts -> opts.setProxyPort(proxyPort))
            .flatMap(gl ->
                gl.documents.createCollection(coll)
                  .then(gl.documents.insert(coll, "{\"type\":\"signup\",\"n\":1}"))
                  .then(gl.documents.insert(coll, "{\"type\":\"signup\",\"n\":2}"))
                  .then(gl.documents.insert(coll, "{\"type\":\"login\",\"n\":3}"))
                  .then(gl.documents.count(coll, "{}"))
                  .flatMap(count ->
                      gl.documents.find(coll, "{\"type\":\"signup\"}", null, null, null)
                        .count()
                        .flatMap(signups -> gl.stop().thenReturn(count + signups * 100))
                  )
            );

        StepVerifier.create(pipeline)
            .assertNext(result -> {
                // 3 docs total + 2 signups * 100 = 203
                assertEquals(203L, result);
            })
            .verifyComplete();
    }

    @Test
    void usingScopesJdbcConnectionAcrossReactiveChain() throws Exception {
        String coll = "rx_it_using_" + System.nanoTime();

        // Open our own JDBC connection to the proxy and use it for all
        // wrapper calls inside a using() block.
        StepVerifier.create(
            ReactiveGoldLapel.start(upstream, opts -> opts.setProxyPort(proxyPort + 1))
                .flatMap(gl -> {
                    String jdbcUrl = gl.getJdbcUrl();
                    java.util.Properties props = new java.util.Properties();
                    if (gl.getJdbcUser() != null) props.setProperty("user", gl.getJdbcUser());
                    if (gl.getJdbcPassword() != null) props.setProperty("password", gl.getJdbcPassword());
                    try {
                        java.sql.Connection myConn = DriverManager.getConnection(jdbcUrl, props);
                        return gl.using(myConn, g ->
                                g.documents.createCollection(coll)
                                 .then(g.documents.insert(coll, "{\"x\":1}"))
                                 .then(g.documents.insert(coll, "{\"x\":2}"))
                                 .then(g.documents.count(coll, "{}"))
                            )
                            .flatMap(count -> {
                                try { myConn.close(); } catch (SQLException ignored) {}
                                return gl.stop().thenReturn(count);
                            });
                    } catch (SQLException e) {
                        return gl.stop().then(Mono.error(e));
                    }
                })
        )
        .expectNext(2L)
        .verifyComplete();
    }

    @Test
    void rawR2dbcQueryAgainstProxy() {
        // The real "reactive" surface — a raw R2DBC query using the
        // ConnectionFactory from ReactiveGoldLapel, going through the proxy.
        // This exercises R2DBC's prepared-statement / extended-protocol path,
        // which relies on the CloseComplete fix in the proxy on main.
        Mono<Long> pipeline = ReactiveGoldLapel.start(upstream, opts -> opts.setProxyPort(proxyPort + 2))
            .flatMap(gl -> {
                Flux<Long> count = Mono.from(gl.connectionFactory().create())
                    .flatMapMany(conn ->
                        Flux.from(conn.createStatement("SELECT 42::bigint AS answer").execute())
                            .flatMap(result -> result.map((row, meta) -> row.get(0, Long.class)))
                            .concatWith(Mono.from(conn.close()).cast(Long.class))
                    );
                return count.next().flatMap(v -> gl.stop().thenReturn(v));
            });

        StepVerifier.create(pipeline)
            .expectNext(42L)
            .verifyComplete();
    }
}
