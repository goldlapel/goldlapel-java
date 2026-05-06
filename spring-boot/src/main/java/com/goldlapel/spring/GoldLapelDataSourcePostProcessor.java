package com.goldlapel.spring;

import com.goldlapel.AggressiveVerifyMode;
import com.goldlapel.GoldLapel;
import com.goldlapel.NativeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.config.BeanPostProcessor;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GoldLapelDataSourcePostProcessor implements BeanPostProcessor, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(GoldLapelDataSourcePostProcessor.class);
    private static final String JDBC_PREFIX = "jdbc:";
    private static final String JDBC_PG_PREFIX = "jdbc:postgresql://";

    private final GoldLapelProperties properties;
    private final List<GoldLapel> proxies = new ArrayList<>();
    // Track which upstream URLs have been assigned which port, so each unique
    // upstream gets its own proxy instance while duplicate DataSources sharing
    // the same upstream reuse the same proxy.
    private final Map<String, Integer> upstreamPorts = new LinkedHashMap<>();
    private int nextPort;

    public GoldLapelDataSourcePostProcessor(GoldLapelProperties properties) {
        this.properties = properties;
        this.nextPort = properties.getProxyPort();
    }

    /**
     * Spring lifecycle: stop all proxies when the application context is
     * closed. This fires on devtools restart, integration-test context
     * teardown, and multi-context app shutdown — scenarios where the JVM keeps
     * running but the context goes away. Using {@link DisposableBean} instead
     * of {@code Runtime.addShutdownHook} avoids orphaned proxy subprocesses
     * and port-collision failures on the next context start.
     */
    @Override
    public void destroy() {
        for (GoldLapel proxy : proxies) {
            try {
                proxy.stop();
            } catch (RuntimeException e) {
                // One proxy failing to stop shouldn't block the others.
                log.warn("Gold Lapel: proxy stop() failed during context shutdown", e);
            }
        }
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (!(bean instanceof DataSource ds)) {
            return bean;
        }

        String jdbcUrl = extractJdbcUrl(ds);
        if (jdbcUrl == null) {
            return bean;
        }

        if (!jdbcUrl.startsWith(JDBC_PG_PREFIX)) {
            return bean;
        }

        // GUC-RLS hardening (java-rls-hardening, 2026-05-05). HikariCP doesn't
        // issue any "wipe session state" command on connection acquire by
        // default — pooled connections retain GUCs (and session-level state
        // generally) from whichever request released them last. For RLS-style
        // patterns where one request sets `app.user_id` per-tenant, that
        // means the next request can inherit a stale identity. Wire HikariCP's
        // `connectionInitSql` to `DISCARD ALL` so each freshly-created
        // physical connection starts with clean state. We don't override a
        // user-configured value (their setup wins); we don't apply this to
        // non-Hikari pools (out of scope for v1; they get the wrapper-side
        // verify-on-checkout fallback instead).
        applyHikariConnectionInitSql(ds, beanName);

        String upstream = jdbcUrl.substring(JDBC_PREFIX.length());

        // If the DataSource carries credentials as separate properties
        // (idiomatic Spring: spring.datasource.username / .password), inject
        // them into the upstream URL so the Rust binary has creds for its
        // bookkeeping connection. If the URL already has inline userinfo, we
        // leave it alone — inline creds take precedence.
        String dsUser = invokeStringGetter(ds, "getUsername");
        String dsPassword = invokeStringGetter(ds, "getPassword");
        if (!upstreamHasUserinfo(upstream) && dsUser != null && !dsUser.isEmpty()) {
            upstream = injectUserinfo(upstream, dsUser, dsPassword);
        }

        // Assign a unique port per unique upstream URL. If two DataSource beans
        // point to the same upstream, they share a proxy. Otherwise each gets
        // its own port so they don't collide.
        int port = upstreamPorts.computeIfAbsent(upstream, k -> nextPort++);

        String extraArgsStr = properties.getExtraArgs();
        Map<String, String> configMap = properties.getConfig();
        final int assignedPort = port;

        GoldLapel proxy;
        try {
            proxy = GoldLapel.start(upstream, opts -> {
                opts.setProxyPort(assignedPort);
                if (configMap != null && !configMap.isEmpty()) {
                    opts.setConfig(normalizeCamelCase(configMap));
                }
                String[] parsedExtraArgs = parseExtraArgs(extraArgsStr);
                if (parsedExtraArgs.length > 0) {
                    opts.setExtraArgs(parsedExtraArgs);
                }
                opts.setSilent(properties.isSilent());
                opts.setMesh(properties.isMesh());
                if (properties.getMeshTag() != null) {
                    opts.setMeshTag(properties.getMeshTag());
                }
                opts.setDisableProxyCache(properties.isDisableProxyCache());
                opts.setDisableMatviews(properties.isDisableMatviews());
                opts.setDisableSqloptimize(properties.isDisableSqloptimize());
                opts.setDisableAutoIndexes(properties.isDisableAutoIndexes());
                if (properties.getDashboardPort() != null) {
                    opts.setDashboardPort(properties.getDashboardPort());
                }
                // Forward an explicitly-configured invalidation port to the
                // spawned proxy so the wrapper-side connectInvalidation() port
                // (read further down) and the proxy-side listener agree. Pre-
                // fix, GoldLapelProperties.invalidationPort fed only the
                // wrapper-side connect, leaving the proxy on its default
                // proxy_port + 2 — mismatch → invalidation socket failure.
                // 0 is the "unset" sentinel here (default), so only forward
                // when the user actually set a value.
                if (properties.getInvalidationPort() != 0) {
                    opts.setInvalidationPort(properties.getInvalidationPort());
                }
                if (properties.getLogLevel() != null) {
                    opts.setLogLevel(properties.getLogLevel());
                }
                if (properties.getMode() != null) {
                    opts.setMode(properties.getMode());
                }
                if (properties.getLicense() != null) {
                    opts.setLicense(properties.getLicense());
                }
                if (properties.getConfigFile() != null) {
                    opts.setConfigFile(properties.getConfigFile());
                }
                opts.setDisableNativeCache(properties.isDisableNativeCache());
                opts.setAggressiveVerify(resolveAggressiveVerify(properties.getAggressiveVerify()));
                opts.setClient("spring-boot");
            });
        } catch (RuntimeException e) {
            String safeUpstream = upstream.replaceAll("://.*@", "://***@");
            throw new RuntimeException(
                    "Gold Lapel failed to start proxy for datasource '" + beanName +
                    "' (upstream: " + safeUpstream + ", port: " + port + ")", e);
        }

        proxies.add(proxy);
        // Use the wrapper's JDBC-safe helpers: the PG JDBC driver rejects
        // inline userinfo (it reads user@host as the hostname), so we set the
        // URL without userinfo and push the user/password onto the DataSource
        // via its separate setters (same reflection pattern as setJdbcUrl).
        setJdbcUrl(ds, proxy.getJdbcUrl());
        String jdbcUser = proxy.getJdbcUser();
        String jdbcPassword = proxy.getJdbcPassword();
        if (jdbcUser != null) {
            setStringProperty(ds, "setUsername", jdbcUser);
        }
        if (jdbcPassword != null) {
            setStringProperty(ds, "setPassword", jdbcPassword);
        }

        log.info("Gold Lapel proxy started — {} now routes through localhost:{}", beanName, port);

        if (properties.isDisableNativeCache()) {
            return ds;
        }

        int invPort = properties.getInvalidationPort();
        if (invPort == 0) {
            invPort = port + 2;
        }
        NativeCache cache = NativeCache.getInstance();
        cache.connectInvalidation(invPort);

        log.info("Gold Lapel native cache enabled for {} (invalidation port {})", beanName, invPort);

        // Thread the aggressive-verify mode + the proxy's JDBC URL into the
        // wrapped DataSource so the per-connection wrap path picks the right
        // detection cache key. The URL is the proxy's URL (the one the
        // JDBC driver actually connects to) — that's what every connection
        // out of this DataSource will use, and it's the right key for the
        // first-connection probe. Routing the original upstream URL would
        // probe pg_trigger via the proxy on a different port and key the
        // cache off a string the user pool never actually opens.
        AggressiveVerifyMode mode = resolveAggressiveVerify(properties.getAggressiveVerify());
        return new CachedDataSource(ds, cache, mode, proxy.getJdbcUrl());
    }

    /**
     * Map the {@code goldlapel.aggressive-verify} string property onto an
     * {@link AggressiveVerifyMode}. Falls back to {@link AggressiveVerifyMode#AUTO}
     * for null / unrecognised inputs (matches the property's documented
     * default — a typo shouldn't disable a safety feature).
     */
    static AggressiveVerifyMode resolveAggressiveVerify(String raw) {
        AggressiveVerifyMode parsed = AggressiveVerifyMode.parse(raw);
        return parsed == null ? AggressiveVerifyMode.AUTO : parsed;
    }

    // Visible for testing
    List<GoldLapel> getProxies() {
        return proxies;
    }

    // Visible for testing
    Map<String, Integer> getUpstreamPorts() {
        return upstreamPorts;
    }

    /**
     * Wire HikariCP's {@code connectionInitSql} to {@code DISCARD ALL} so each
     * physical connection starts with clean session state — wipes any GUC
     * left behind by a prior pooled checkout. No-op for non-Hikari
     * DataSources (unrecognised pools fall through to the wrapper-side
     * verify-on-checkout fallback). No-op when the user has already set a
     * non-empty {@code connectionInitSql} — their value wins, on the
     * assumption that anyone who customised it has a deliberate reason
     * (e.g. they're already issuing {@code DISCARD ALL; SET SESSION ...}
     * from their own init SQL).
     *
     * <p>Reflection-based so the spring-boot module doesn't have to
     * compile-depend on HikariCP's internal API surface (the public
     * {@code HikariDataSource} class is stable, but using reflection here
     * keeps us robust against minor-version method-signature drift and
     * lets us share this code path with non-Hikari Hikari-look-alikes that
     * happen to expose the same setter shape).
     */
    static void applyHikariConnectionInitSql(DataSource ds, String beanName) {
        // HikariDataSource is the canonical class; check by class name rather
        // than instanceof to avoid pulling HikariCP into the auto-config
        // hot-load path when the user is on a different pool.
        String className = ds.getClass().getName();
        if (!className.equals("com.zaxxer.hikari.HikariDataSource")) {
            return;
        }
        try {
            Method getter = ds.getClass().getMethod("getConnectionInitSql");
            Object existing = getter.invoke(ds);
            if (existing instanceof String s && !s.isBlank()) {
                // User-configured init SQL — leave alone.
                log.debug(
                    "Gold Lapel: HikariDataSource '{}' already has connectionInitSql='{}', " +
                    "leaving unchanged (Gold Lapel relies on the wrapper-side verify-on-checkout " +
                    "fallback for GUC-RLS safety in this case)", beanName, s);
                return;
            }
            Method setter = ds.getClass().getMethod("setConnectionInitSql", String.class);
            setter.invoke(ds, "DISCARD ALL");
            log.info(
                "Gold Lapel: wired HikariDataSource '{}' connectionInitSql=\"DISCARD ALL\" " +
                "for GUC-RLS cache safety (clears session GUCs on each new physical connection)",
                beanName);
        } catch (NoSuchMethodException e) {
            // Older HikariCP without these getters/setters — extremely
            // unlikely in practice (the API has been stable for years).
            // Silently fall through to the verify-on-checkout fallback.
            log.debug("Gold Lapel: HikariDataSource '{}' missing connectionInitSql accessors", beanName);
        } catch (Exception e) {
            log.warn(
                "Gold Lapel: failed to wire connectionInitSql on HikariDataSource '{}'; " +
                "GUC-RLS safety falls back to the wrapper-side verify-on-checkout path",
                beanName, e);
        }
    }

    // Extract the JDBC URL from any DataSource implementation. Tries common
    // getter methods used by HikariCP, Tomcat DBCP, C3P0, etc.
    static String extractJdbcUrl(DataSource ds) {
        // Try the most common getter names across popular pools
        for (String methodName : new String[]{"getJdbcUrl", "getUrl", "getURL"}) {
            try {
                Method m = ds.getClass().getMethod(methodName);
                Object result = m.invoke(ds);
                if (result instanceof String url && !url.isEmpty()) {
                    return url;
                }
            } catch (Exception ignored) {
                // Method not found or not accessible — try the next one
            }
        }
        log.warn("Gold Lapel: could not extract JDBC URL from DataSource bean of type {}. " +
                "Gold Lapel proxy will not be applied. " +
                "Supported pools: HikariCP, Tomcat DBCP, Commons DBCP2, C3P0.",
                ds.getClass().getName());
        return null;
    }

    // Set the JDBC URL on any DataSource implementation. Tries common setter
    // methods used by HikariCP, Tomcat DBCP, C3P0, etc.
    private static void setJdbcUrl(DataSource ds, String jdbcUrl) {
        for (String methodName : new String[]{"setJdbcUrl", "setUrl", "setURL"}) {
            try {
                Method m = ds.getClass().getMethod(methodName, String.class);
                m.invoke(ds, jdbcUrl);
                return;
            } catch (Exception ignored) {
                // Method not found or not accessible — try the next one
            }
        }
        log.warn("Gold Lapel: could not set JDBC URL on DataSource bean of type {}. " +
                "The proxy URL may not be applied.",
                ds.getClass().getName());
    }

    // Invoke a zero-arg String getter via reflection. Returns null if the
    // method doesn't exist, isn't accessible, or returns a non-String value.
    static String invokeStringGetter(Object target, String methodName) {
        try {
            Method m = target.getClass().getMethod(methodName);
            Object result = m.invoke(target);
            if (result instanceof String s) {
                return s;
            }
        } catch (Exception ignored) {
            // Method not found or not accessible — fall through
        }
        return null;
    }

    // Invoke a single-String-arg setter via reflection. Silently ignores
    // missing or inaccessible methods (the bean may simply not support it).
    private static void setStringProperty(Object target, String methodName, String value) {
        try {
            Method m = target.getClass().getMethod(methodName, String.class);
            m.invoke(target, value);
        } catch (Exception ignored) {
            // Method not found or not accessible — silently skip
        }
    }

    // True iff the authority portion of the postgres URL contains userinfo
    // (i.e. a '@' before any /?# path/query delimiter).
    static boolean upstreamHasUserinfo(String upstream) {
        int schemeIdx = upstream.indexOf("://");
        int start = schemeIdx < 0 ? 0 : schemeIdx + 3;
        int end = upstream.length();
        for (int i = start; i < end; i++) {
            char c = upstream.charAt(i);
            if (c == '/' || c == '?' || c == '#') {
                end = i;
                break;
            }
        }
        return upstream.lastIndexOf('@', end - 1) >= start;
    }

    // Inject userinfo into a postgres URL that lacks it. user and password are
    // percent-encoded so special characters (e.g. '@', ':', '/', ' ') don't
    // corrupt the resulting URL. password may be null.
    static String injectUserinfo(String upstream, String user, String password) {
        int schemeIdx = upstream.indexOf("://");
        if (schemeIdx < 0) {
            // Not a URL we recognize — leave alone rather than mangle it
            return upstream;
        }
        String prefix = upstream.substring(0, schemeIdx + 3);
        String rest = upstream.substring(schemeIdx + 3);
        StringBuilder userinfo = new StringBuilder();
        userinfo.append(percentEncodeUserinfo(user));
        if (password != null) {
            userinfo.append(':').append(percentEncodeUserinfo(password));
        }
        userinfo.append('@');
        return prefix + userinfo + rest;
    }

    // Percent-encode a userinfo component. Uses URLEncoder (form encoding) and
    // then rewrites '+' as '%20' so spaces decode correctly against RFC 3986
    // URL parsers (which treat '+' literally in userinfo).
    private static String percentEncodeUserinfo(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }

    // Convert kebab-case keys to camelCase and coerce String values to their
    // native types so the Java wrapper's configToArgs() gets what it expects.
    //
    // Spring Boot YAML properties always arrive as Strings (e.g. "true" not true),
    // but the wrapper does instanceof Boolean / instanceof List checks.
    //
    //   goldlapel.config.pool-size=30        -> poolSize: "30"       (stays String)
    //   goldlapel.config.disable-n1=true      -> disableN1: true     (Boolean)
    //   goldlapel.config.exclude-tables=a,b   -> excludeTables: ["a","b"] (List)
    static Map<String, Object> normalizeCamelCase(Map<String, String> input) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : input.entrySet()) {
            result.put(kebabToCamel(entry.getKey()), coerceValue(entry.getValue()));
        }
        return result;
    }

    // Coerce Spring Boot String property values to the native types the Java
    // wrapper expects:
    //   "true" / "false"  -> Boolean  (for boolean flag keys like disableN1)
    //   "a,b,c"           -> List     (for list keys like excludeTables, replica)
    //   everything else   -> String   (numeric values stay as strings; the wrapper
    //                                  calls .toString() on them anyway)
    static Object coerceValue(String value) {
        if (value == null) {
            return null;
        }
        if (value.equalsIgnoreCase("true")) {
            return Boolean.TRUE;
        }
        if (value.equalsIgnoreCase("false")) {
            return Boolean.FALSE;
        }
        if (value.contains(",")) {
            return Arrays.stream(value.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();
        }
        return value;
    }

    /**
     * Split a {@code goldlapel.extra-args} string into individual CLI args.
     *
     * <p>Args are separated by commas. A comma can be included <em>inside</em>
     * an arg by escaping it with a backslash ({@code \,}) — handy for values
     * like regexes with counted repetition (e.g. {@code \d{1\,3}}). A literal
     * backslash is written as {@code \\}. Empty tokens and pure-whitespace
     * tokens are dropped. A null or empty input yields an empty array.
     *
     * <p>Examples (Java-source: double the backslashes):
     * <pre>
     *   "--foo,--bar"        -&gt; ["--foo", "--bar"]
     *   "--re=\\d{1\\,3}"    -&gt; ["--re=\\d{1,3}"]
     *   "a\\\\,b"            -&gt; ["a\\", "b"]
     *   ""                   -&gt; []
     * </pre>
     *
     * <p>In {@code application.yml}, only one level of escaping is needed:
     * <pre>
     *   goldlapel:
     *     extra-args: "--re=\\d{1\,3}"
     * </pre>
     */
    static String[] parseExtraArgs(String input) {
        if (input == null || input.isEmpty()) {
            return new String[0];
        }
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '\\' && i + 1 < input.length()) {
                char next = input.charAt(i + 1);
                if (next == ',' || next == '\\') {
                    // Recognized escape — consume the backslash and emit the literal.
                    cur.append(next);
                    i++;
                    continue;
                }
                // Unrecognized escape: keep the backslash as-is. (Preserves
                // legacy callers that might pass flags containing a literal
                // backslash before some other character.)
                cur.append(c);
                continue;
            }
            if (c == ',') {
                addIfNotBlank(out, cur.toString());
                cur.setLength(0);
                continue;
            }
            cur.append(c);
        }
        addIfNotBlank(out, cur.toString());
        return out.toArray(new String[0]);
    }

    private static void addIfNotBlank(List<String> list, String s) {
        if (!s.isEmpty() && !s.trim().isEmpty()) {
            list.add(s);
        }
    }

    static String kebabToCamel(String key) {
        if (!key.contains("-")) {
            return key;
        }
        StringBuilder sb = new StringBuilder();
        boolean upper = false;
        for (char c : key.toCharArray()) {
            if (c == '-') {
                upper = true;
            } else if (upper) {
                sb.append(Character.toUpperCase(c));
                upper = false;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
