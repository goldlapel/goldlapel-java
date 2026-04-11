package com.goldlapel.spring;

import com.goldlapel.GoldLapel;
import com.goldlapel.NativeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GoldLapelDataSourcePostProcessor implements BeanPostProcessor {

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
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (GoldLapel proxy : proxies) {
                proxy.stopProxy();
            }
        }));
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

        // Assign a unique port per unique upstream URL. If two DataSource beans
        // point to the same upstream, they share a proxy. Otherwise each gets
        // its own port so they don't collide.
        int port = upstreamPorts.computeIfAbsent(upstream, k -> nextPort++);

        String extraArgsStr = properties.getExtraArgs();
        Map<String, String> configMap = properties.getConfig();

        GoldLapel.Options options = new GoldLapel.Options().port(port);

        if (configMap != null && !configMap.isEmpty()) {
            options.config(normalizeCamelCase(configMap));
        }

        if (extraArgsStr != null && !extraArgsStr.isEmpty()) {
            options.extraArgs(extraArgsStr.split(","));
        }

        options.client("spring-boot");
        GoldLapel proxy = new GoldLapel(upstream, options);
        String proxyUrl;
        try {
            proxyUrl = proxy.startProxy();
        } catch (RuntimeException e) {
            String safeUpstream = upstream.replaceAll("://.*@", "://***@");
            throw new RuntimeException(
                    "Gold Lapel failed to start proxy for datasource '" + beanName +
                    "' (upstream: " + safeUpstream + ", port: " + port + ")", e);
        }

        proxies.add(proxy);
        String proxyJdbcUrl = JDBC_PREFIX + proxyUrl;
        setJdbcUrl(ds, proxyJdbcUrl);

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
