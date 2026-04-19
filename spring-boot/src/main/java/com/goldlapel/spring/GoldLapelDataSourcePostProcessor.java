package com.goldlapel.spring;

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
        this.nextPort = properties.getPort();
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
                opts.setPort(assignedPort);
                if (configMap != null && !configMap.isEmpty()) {
                    opts.setConfig(normalizeCamelCase(configMap));
                }
                if (extraArgsStr != null && !extraArgsStr.isEmpty()) {
                    opts.setExtraArgs(extraArgsStr.split(","));
                }
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

        if (!properties.isNativeCache()) {
            return ds;
        }

        int invPort = properties.getInvalidationPort();
        if (invPort == 0) {
            invPort = port + 2;
        }
        NativeCache cache = NativeCache.getInstance();
        cache.connectInvalidation(invPort);

        log.info("Gold Lapel L1 native cache enabled for {} (invalidation port {})", beanName, invPort);

        return new CachedDataSource(ds, cache);
    }

    // Visible for testing
    List<GoldLapel> getProxies() {
        return proxies;
    }

    // Visible for testing
    Map<String, Integer> getUpstreamPorts() {
        return upstreamPorts;
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
