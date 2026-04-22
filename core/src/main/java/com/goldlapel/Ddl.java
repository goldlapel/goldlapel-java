package com.goldlapel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DDL API client — fetches canonical helper-table DDL + query patterns from
 * the Rust proxy's dashboard port so the wrapper never hand-writes CREATE
 * TABLE for helper families (streams, docs, counters, ...).
 *
 * <p>Architecture: see {@code docs/wrapper-v0.2/SCHEMA-TO-CORE-PLAN.md} in
 * the {@code goldlapel} repo.
 *
 * <ul>
 *   <li>One HTTP call per (family, name) per session (cached).
 *   <li>Cache lives on the {@link GoldLapel} instance ({@link GoldLapel#ddlCache()}).
 *   <li>Errors: HTTP failures throw {@link RuntimeException} with actionable text.
 * </ul>
 *
 * <p>Token + port resolution:
 * <ul>
 *   <li>{@link GoldLapel#dashboardToken()} is populated when the wrapper
 *       spawned the proxy (happy path).
 *   <li>For externally-launched proxies, {@link #tokenFromEnvOrFile()} reads
 *       {@code GOLDLAPEL_DASHBOARD_TOKEN} env or
 *       {@code ~/.goldlapel/dashboard-token} file.
 * </ul>
 *
 * <p>The {@code tables} and {@code query_patterns} return values are both
 * {@code Map<String, String>}; callers (Utils.streamX) key into the patterns
 * map. The proxy emits SQL with numbered {@code $1, $2, …} placeholders which
 * {@code Utils.pgToJdbc} rewrites to JDBC's positional {@code ?} form before
 * preparing each statement.
 */
public final class Ddl {
    private Ddl() {}

    private static final Map<String, String> SUPPORTED_VERSIONS = Map.of(
        "stream", "v1"
    );

    /** Resolve the dashboard token from env or the on-disk file. */
    public static String tokenFromEnvOrFile() {
        String env = System.getenv("GOLDLAPEL_DASHBOARD_TOKEN");
        if (env != null && !env.trim().isEmpty()) {
            return env.trim();
        }
        String home = System.getProperty("user.home");
        if (home == null) return null;
        Path path = Path.of(home, ".goldlapel", "dashboard-token");
        if (Files.exists(path)) {
            try {
                String text = Files.readString(path, StandardCharsets.UTF_8).trim();
                return text.isEmpty() ? null : text;
            } catch (IOException ignored) {
                return null;
            }
        }
        return null;
    }

    public static String supportedVersion(String family) {
        String v = SUPPORTED_VERSIONS.get(family);
        if (v == null) {
            throw new IllegalArgumentException("Unknown helper family: " + family);
        }
        return v;
    }

    /**
     * Fetch (and cache) the canonical {tables, query_patterns} for a helper.
     *
     * @param cache  per-instance cache — typically {@link GoldLapel#ddlCache()}
     * @param family "stream" / "doc" / etc.
     * @param name   user-supplied helper name
     * @param dashboardPort  proxy dashboard port
     * @param dashboardToken proxy dashboard token ({@link GoldLapel#dashboardToken()} / {@link #tokenFromEnvOrFile()})
     * @return a map with keys "tables" and "query_patterns", each a
     *         {@code Map<String, String>}.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> fetchPatterns(
        ConcurrentHashMap<String, Map<String, Object>> cache,
        String family,
        String name,
        int dashboardPort,
        String dashboardToken
    ) {
        String key = family + ":" + name;
        Map<String, Object> cached = cache.get(key);
        if (cached != null) return cached;

        if (dashboardToken == null || dashboardToken.isEmpty()) {
            throw new RuntimeException(
                "No dashboard token available. Set GOLDLAPEL_DASHBOARD_TOKEN or let "
                + "GoldLapel.start spawn the proxy (which provisions a token automatically)."
            );
        }
        if (dashboardPort <= 0) {
            throw new RuntimeException(
                "No dashboard port available. Gold Lapel's helper families ("
                + family + ", ...) require the proxy's dashboard to be reachable."
            );
        }

        String url = "http://127.0.0.1:" + dashboardPort + "/api/ddl/" + family + "/create";
        String body = "{\"name\":" + jsonString(name)
            + ",\"schema_version\":" + jsonString(supportedVersion(family)) + "}";

        int status;
        String respBody;
        try {
            HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
            c.setRequestMethod("POST");
            c.setRequestProperty("Content-Type", "application/json");
            c.setRequestProperty("X-GL-Dashboard", dashboardToken);
            c.setDoOutput(true);
            c.setConnectTimeout(5000);
            c.setReadTimeout(10000);
            try (OutputStream os = c.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }
            status = c.getResponseCode();
            InputStream in = status >= 400 ? c.getErrorStream() : c.getInputStream();
            respBody = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(
                "Gold Lapel dashboard not reachable at " + url + ": " + e.getMessage()
                + ". Is `goldlapel` running? The dashboard port must be open "
                + "for helper families (streams, docs, ...) to work.",
                e
            );
        }

        Map<String, Object> parsed = JsonMini.parseObject(respBody);
        if (status != 200) {
            String error = parsed.get("error") instanceof String ? (String) parsed.get("error") : "unknown";
            String detail = parsed.get("detail") instanceof String ? (String) parsed.get("detail") : respBody;
            if (status == 409 && "version_mismatch".equals(error)) {
                throw new RuntimeException(
                    "Gold Lapel schema version mismatch for " + family + " '" + name + "': " + detail
                    + ". Upgrade the proxy or the wrapper so versions agree."
                );
            }
            if (status == 403) {
                throw new RuntimeException(
                    "Gold Lapel dashboard rejected the DDL request (403). "
                    + "The dashboard token is missing or incorrect — check "
                    + "GOLDLAPEL_DASHBOARD_TOKEN or ~/.goldlapel/dashboard-token."
                );
            }
            throw new RuntimeException(
                "Gold Lapel DDL API " + family + "/" + name
                + " failed with " + status + " " + error + ": " + detail
            );
        }

        Object tables = parsed.get("tables");
        Object patterns = parsed.get("query_patterns");
        if (!(tables instanceof Map) || !(patterns instanceof Map)) {
            throw new RuntimeException(
                "Gold Lapel DDL API returned an unexpected response shape: " + respBody
            );
        }
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("tables", tables);
        entry.put("query_patterns", patterns);
        cache.put(key, entry);
        return entry;
    }

    /** Typed helper: return the query_patterns map from a fetched entry. */
    @SuppressWarnings("unchecked")
    public static Map<String, String> queryPatterns(Map<String, Object> entry) {
        return (Map<String, String>) entry.get("query_patterns");
    }

    /** Remove all cached entries. */
    public static void invalidate(ConcurrentHashMap<String, Map<String, Object>> cache) {
        cache.clear();
    }

    // Package-private for tests — swap the URL builder to hit a fake dashboard.
    // (Kept minimal; full swap would require an HttpClient indirection. For
    // unit tests we cover this path with a real HttpServer.)

    private static String jsonString(String s) {
        StringBuilder b = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '"': b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                default:
                    if (ch < 0x20) {
                        b.append(String.format("\\u%04x", (int) ch));
                    } else {
                        b.append(ch);
                    }
            }
        }
        b.append("\"");
        return b.toString();
    }
}
