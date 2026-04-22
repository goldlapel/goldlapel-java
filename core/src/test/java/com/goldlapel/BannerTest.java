package com.goldlapel;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for the startup banner stream + silent opt-out.
 *
 * <p>Library code must not unconditionally print to stdout — it pollutes app
 * output, CI logs, and anything that captures stdout (server logs, pipelines,
 * test harnesses). The banner goes to {@code System.err}; {@code setSilent(true)}
 * on {@link GoldLapelOptions} suppresses it entirely.
 *
 * <p>Cross-wrapper parity (v0.2 coverage audit): every other wrapper (Python,
 * JS, Ruby, Go, PHP, .NET) has the banner-to-stderr + silent feature. Java was
 * the only wrapper missing it prior to this change.
 */
class BannerTest {

    // Capture a PrintStream into a String. Avoids mutating System.err globally
    // (which would leak across tests and make failures noisy) — we invoke
    // printBanner directly with our own stream instead.
    private static String capture(GoldLapel gl) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(bytes, true, StandardCharsets.UTF_8);
        gl.printBanner(ps);
        return bytes.toString(StandardCharsets.UTF_8);
    }

    @Test
    void bannerPrintsByDefault() {
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://host:5432/mydb");

        String out = capture(gl);

        assertTrue(out.contains("goldlapel"),
            "banner should contain 'goldlapel', got: " + out);
        assertTrue(out.contains("(proxy)"),
            "banner should contain '(proxy)', got: " + out);
        assertTrue(out.contains("7932"),
            "banner should contain the default proxy port 7932, got: " + out);
    }

    @Test
    void bannerIncludesDashboardByDefault() {
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://host:5432/mydb");

        String out = capture(gl);

        assertTrue(out.contains("(dashboard)"),
            "banner should contain '(dashboard)' when dashboard port is live, got: " + out);
        assertTrue(out.contains("http://127.0.0.1:7933"),
            "banner should contain the default dashboard URL http://127.0.0.1:7933, got: " + out);
    }

    @Test
    void silentSuppressesBanner() {
        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setSilent(true);
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://host:5432/mydb", opts);

        String out = capture(gl);

        assertEquals("", out,
            "setSilent(true) must suppress the banner entirely, got: " + out);
    }

    @Test
    void silentFalsePrintsBanner() {
        // Explicit silent=false should behave the same as the default.
        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setSilent(false);
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://host:5432/mydb", opts);

        String out = capture(gl);

        assertTrue(out.contains("goldlapel"),
            "setSilent(false) should print the banner, got: " + out);
    }

    @Test
    void bannerUsesCustomProxyPort() {
        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setPort(17932);
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://host:5432/mydb", opts);

        String out = capture(gl);

        assertTrue(out.contains("17932"),
            "banner should reflect custom proxy port, got: " + out);
        assertTrue(out.contains("17933"),
            "banner should reflect derived dashboard port (proxy+1), got: " + out);
    }

    @Test
    void bannerOmitsDashboardWhenDisabled() {
        // dashboardPort = 0 means the dashboard is disabled — banner should
        // only mention the proxy, not a bogus http://127.0.0.1:0 URL.
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("dashboardPort", 0);
        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setConfig(cfg);
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://host:5432/mydb", opts);

        String out = capture(gl);

        assertTrue(out.contains("(proxy)"),
            "banner should still mention the proxy, got: " + out);
        assertFalse(out.contains("(dashboard)"),
            "banner should omit '(dashboard)' when dashboard port is 0, got: " + out);
        assertFalse(out.contains("127.0.0.1:0"),
            "banner should never print a bogus :0 dashboard URL, got: " + out);
    }

    @Test
    void silentOptionDefaultsToFalse() {
        GoldLapelOptions opts = new GoldLapelOptions();
        assertFalse(opts.isSilent(),
            "default value of silent should be false so banners print by default");
    }

    @Test
    void silentOptionGetterSetter() {
        GoldLapelOptions opts = new GoldLapelOptions();
        opts.setSilent(true);
        assertTrue(opts.isSilent());
        opts.setSilent(false);
        assertFalse(opts.isSilent());
    }

    @Test
    void bannerEndsWithNewline() {
        // println() appends the platform line separator. We only assert that
        // the banner ends in a newline character (some sort) — not the specific
        // separator — to keep the test robust on Windows CI.
        GoldLapel gl = GoldLapelClassTest.newUnstarted("postgresql://host:5432/mydb");

        String out = capture(gl);

        assertTrue(out.endsWith("\n") || out.endsWith("\r\n"),
            "banner should end with a newline, got: " + out);
    }
}
