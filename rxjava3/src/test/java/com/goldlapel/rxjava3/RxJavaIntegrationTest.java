package com.goldlapel.rxjava3;

import io.reactivex.rxjava3.core.Single;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration test for {@link RxJavaGoldLapel}: spawns the
 * real proxy binary, talks to a real Postgres upstream, composes a full
 * chain in RxJava 3 operators.
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
 *   ./mvnw -pl rxjava3 test
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "GOLDLAPEL_INTEGRATION", matches = "1")
class RxJavaIntegrationTest {

    static String upstream;
    static int proxyPort;

    @BeforeAll
    static void setup() throws Exception {
        // requireUpstream() throws if GOLDLAPEL_TEST_UPSTREAM is missing,
        // surfacing half-configured CI as a loud failure rather than a
        // silent skip.
        upstream = IntegrationGate.requireUpstream();
        try (ServerSocket s = new ServerSocket(0)) {
            proxyPort = s.getLocalPort();
        }
    }

    @Test
    void endToEndChainedSingle() {
        String coll = "rxj_it_chain_" + System.nanoTime();

        Single<Long> pipeline = RxJavaGoldLapel.start(upstream, opts -> opts.setProxyPort(proxyPort))
            .flatMap(gl ->
                gl.documents.createCollection(coll)
                  .andThen(gl.documents.insert(coll, "{\"type\":\"signup\",\"n\":1}"))
                  .flatMap(ignored -> gl.documents.insert(coll, "{\"type\":\"signup\",\"n\":2}"))
                  .flatMap(ignored -> gl.documents.insert(coll, "{\"type\":\"login\",\"n\":3}"))
                  .flatMap(ignored -> gl.documents.count(coll, "{}"))
                  .flatMap(count ->
                      gl.documents.find(coll, "{\"type\":\"signup\"}", null, null, null)
                        .count()
                        .flatMap(signups ->
                            gl.stop().toSingleDefault(count + signups * 100L)
                        )
                  )
            );

        Long result = pipeline.blockingGet();
        assertEquals(203L, result, "3 docs + 2 signups*100 = 203");
    }
}
