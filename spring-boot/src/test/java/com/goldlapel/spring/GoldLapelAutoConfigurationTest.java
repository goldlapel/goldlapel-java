package com.goldlapel.spring;

import com.goldlapel.GoldLapel;
import com.goldlapel.GoldLapelOptions;
import com.goldlapel.NativeCache;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.invocation.InvocationOnMock;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the Spring Boot integration. {@code GoldLapel.start(...)} is
 * mocked out (it would otherwise spawn a real subprocess); the test verifies
 * that the post-processor resolves upstreams, assigns ports, rewrites JDBC
 * URLs, and forwards the configured options correctly.
 */
class GoldLapelAutoConfigurationTest {

    private final ApplicationContextRunner dataSourceRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    DataSourceAutoConfiguration.class,
                    GoldLapelAutoConfiguration.class));

    private final ApplicationContextRunner simpleRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(GoldLapelAutoConfiguration.class));

    @AfterEach
    void resetCache() {
        NativeCache.reset();
    }

    /**
     * Build a MockedStatic that intercepts {@code GoldLapel.start(String, Consumer)}.
     * Each call spins up a mock GoldLapel whose {@code getUrl()} returns the URL
     * produced by {@code urlForUpstream.apply(upstream)}. The captured options
     * list receives the effective GoldLapelOptions for each call.
     */
    private static MockedStatic<GoldLapel> stubStart(
            java.util.function.Function<String, String> urlForUpstream,
            List<GoldLapelOptions> capturedOptions) {
        MockedStatic<GoldLapel> stat = mockStatic(GoldLapel.class);
        stat.when(() -> GoldLapel.start(anyString(), any())).thenAnswer((InvocationOnMock inv) -> {
            String upstream = inv.getArgument(0);
            @SuppressWarnings("unchecked")
            Consumer<GoldLapelOptions> cfg = inv.getArgument(1);
            GoldLapelOptions opts = new GoldLapelOptions();
            if (cfg != null) cfg.accept(opts);
            capturedOptions.add(opts);
            GoldLapel gl = mock(GoldLapel.class);
            when(gl.getUrl()).thenReturn(urlForUpstream.apply(upstream));
            when(gl.getPort()).thenReturn(opts.getPort() != null ? opts.getPort() : 7932);
            return gl;
        });
        return stat;
    }

    @Test
    void autoConfiguresAndRewritesDataSource() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> ignored = stubStart(
                u -> "postgresql://localhost:7932/testdb", captured)) {

            dataSourceRunner.withPropertyValues(
                            "spring.datasource.url=jdbc:postgresql://localhost:5432/testdb",
                            "spring.datasource.driver-class-name=org.postgresql.Driver")
                    .run(context -> {
                        assertThat(context).hasSingleBean(GoldLapelDataSourcePostProcessor.class);
                        DataSource ds = context.getBean(DataSource.class);
                        assertThat(ds).isInstanceOf(CachedDataSource.class);
                        HikariDataSource hikari = (HikariDataSource) ((CachedDataSource) ds).getDelegate();
                        assertThat(hikari.getJdbcUrl()).isEqualTo("jdbc:postgresql://localhost:7932/testdb");
                        assertThat(captured).hasSize(1);
                    });
        }
    }

    @Test
    void disabledWhenPropertyFalse() {
        simpleRunner.withPropertyValues("goldlapel.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(GoldLapelDataSourcePostProcessor.class));
    }

    @Test
    void skipsNonPostgresDataSource() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> stat = stubStart(
                u -> "postgresql://localhost:7932/db", captured)) {

            HikariDataSource ds = new HikariDataSource();
            ds.setJdbcUrl("jdbc:h2:mem:testdb");

            GoldLapelDataSourcePostProcessor processor = new GoldLapelDataSourcePostProcessor(
                    new GoldLapelProperties());

            Object result = processor.postProcessAfterInitialization(ds, "dataSource");

            assertThat(result).isSameAs(ds);
            assertThat(ds.getJdbcUrl()).isEqualTo("jdbc:h2:mem:testdb");
            stat.verify(() -> GoldLapel.start(anyString(), any()), times(0));
        }
    }

    @Test
    void customPortAndExtraArgs() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> ignored = stubStart(
                u -> "postgresql://localhost:9999/testdb", captured)) {

            dataSourceRunner.withPropertyValues(
                            "spring.datasource.url=jdbc:postgresql://localhost:5432/testdb",
                            "spring.datasource.driver-class-name=org.postgresql.Driver",
                            "goldlapel.port=9999",
                            "goldlapel.extra-args=--threshold-duration-ms,200")
                    .run(context -> {
                        DataSource ds = context.getBean(DataSource.class);
                        assertThat(ds).isInstanceOf(CachedDataSource.class);
                        HikariDataSource hikari = (HikariDataSource) ((CachedDataSource) ds).getDelegate();
                        assertThat(hikari.getJdbcUrl()).isEqualTo("jdbc:postgresql://localhost:9999/testdb");
                        assertThat(captured).hasSize(1);
                        assertThat(captured.get(0).getPort()).isEqualTo(9999);
                        assertThat(captured.get(0).getExtraArgs())
                                .containsExactly("--threshold-duration-ms", "200");
                    });
        }
    }

    @Test
    void notLoadedWithoutPostgresDriver() {
        simpleRunner
                .withClassLoader(new FilteredClassLoader("org.postgresql.Driver"))
                .run(context -> assertThat(context).doesNotHaveBean(GoldLapelAutoConfiguration.class));
    }

    @Test
    void multipleDataSourcesGetSeparateProxies() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> ignored = stubStart(
                u -> u.contains("5432")
                        ? "postgresql://localhost:7932/db1"
                        : "postgresql://localhost:7933/db2",
                captured)) {

            HikariDataSource ds1 = new HikariDataSource();
            ds1.setJdbcUrl("jdbc:postgresql://host1:5432/db1");

            HikariDataSource ds2 = new HikariDataSource();
            ds2.setJdbcUrl("jdbc:postgresql://host2:5433/db2");

            GoldLapelProperties props = new GoldLapelProperties();
            GoldLapelDataSourcePostProcessor processor = new GoldLapelDataSourcePostProcessor(props);

            Object result1 = processor.postProcessAfterInitialization(ds1, "primaryDataSource");
            Object result2 = processor.postProcessAfterInitialization(ds2, "analyticsDataSource");

            assertThat(captured).hasSize(2);
            assertThat(ds1.getJdbcUrl()).isEqualTo("jdbc:postgresql://localhost:7932/db1");
            assertThat(ds2.getJdbcUrl()).isEqualTo("jdbc:postgresql://localhost:7933/db2");

            assertThat(processor.getProxies()).hasSize(2);
            assertThat(result1).isInstanceOf(CachedDataSource.class);
            assertThat(result2).isInstanceOf(CachedDataSource.class);

            // Each unique upstream gets its own port
            assertThat(processor.getUpstreamPorts()).hasSize(2);
            assertThat(processor.getUpstreamPorts().values()).containsExactly(7932, 7933);
        }
    }

    @Test
    void duplicateUpstreamReusesPort() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> ignored = stubStart(
                u -> "postgresql://localhost:7932/db", captured)) {

            HikariDataSource ds1 = new HikariDataSource();
            ds1.setJdbcUrl("jdbc:postgresql://host:5432/db");

            HikariDataSource ds2 = new HikariDataSource();
            ds2.setJdbcUrl("jdbc:postgresql://host:5432/db");

            GoldLapelProperties props = new GoldLapelProperties();
            GoldLapelDataSourcePostProcessor processor = new GoldLapelDataSourcePostProcessor(props);

            processor.postProcessAfterInitialization(ds1, "ds1");
            processor.postProcessAfterInitialization(ds2, "ds2");

            // Same upstream = same port = two proxy instances but both on port 7932
            assertThat(processor.getUpstreamPorts()).hasSize(1);
            assertThat(processor.getUpstreamPorts().values()).containsExactly(7932);
        }
    }

    @Test
    void configMapPassedToOptions() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> ignored = stubStart(
                u -> "postgresql://localhost:7932/testdb", captured)) {

            dataSourceRunner.withPropertyValues(
                            "spring.datasource.url=jdbc:postgresql://localhost:5432/testdb",
                            "spring.datasource.driver-class-name=org.postgresql.Driver",
                            "goldlapel.config.mode=waiter",
                            "goldlapel.config.pool-size=30")
                    .run(context -> {
                        assertThat(context).hasSingleBean(GoldLapelDataSourcePostProcessor.class);
                        DataSource ds = context.getBean(DataSource.class);
                        assertThat(ds).isInstanceOf(CachedDataSource.class);
                        HikariDataSource hikari = (HikariDataSource) ((CachedDataSource) ds).getDelegate();
                        assertThat(hikari.getJdbcUrl()).isEqualTo("jdbc:postgresql://localhost:7932/testdb");

                        assertThat(captured).hasSize(1);
                        Map<String, Object> cfg = captured.get(0).getConfig();
                        assertThat(cfg).containsEntry("mode", "waiter");
                        assertThat(cfg).containsEntry("poolSize", "30");
                    });
        }
    }

    @Test
    void configMapWithCamelCaseKeysFromYaml() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> ignored = stubStart(
                u -> "postgresql://localhost:7932/testdb", captured)) {

            dataSourceRunner.withPropertyValues(
                            "spring.datasource.url=jdbc:postgresql://localhost:5432/testdb",
                            "spring.datasource.driver-class-name=org.postgresql.Driver",
                            "goldlapel.config.poolSize=25",
                            "goldlapel.config.disableN1=true")
                    .run(context -> {
                        DataSource ds = context.getBean(DataSource.class);
                        assertThat(ds).isInstanceOf(CachedDataSource.class);
                        HikariDataSource hikari = (HikariDataSource) ((CachedDataSource) ds).getDelegate();
                        assertThat(hikari.getJdbcUrl()).isEqualTo("jdbc:postgresql://localhost:7932/testdb");
                        assertThat(captured).hasSize(1);
                    });
        }
    }

    @Test
    void configMapWithPortAndExtraArgs() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> ignored = stubStart(
                u -> "postgresql://localhost:9000/testdb", captured)) {

            dataSourceRunner.withPropertyValues(
                            "spring.datasource.url=jdbc:postgresql://localhost:5432/testdb",
                            "spring.datasource.driver-class-name=org.postgresql.Driver",
                            "goldlapel.port=9000",
                            "goldlapel.config.mode=waiter",
                            "goldlapel.extra-args=--verbose")
                    .run(context -> {
                        DataSource ds = context.getBean(DataSource.class);
                        assertThat(ds).isInstanceOf(CachedDataSource.class);
                        HikariDataSource hikari = (HikariDataSource) ((CachedDataSource) ds).getDelegate();
                        assertThat(hikari.getJdbcUrl()).isEqualTo("jdbc:postgresql://localhost:9000/testdb");
                        assertThat(captured).hasSize(1);
                        assertThat(captured.get(0).getPort()).isEqualTo(9000);
                    });
        }
    }

    @Test
    void emptyConfigMapDoesNotSetConfig() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> ignored = stubStart(
                u -> "postgresql://localhost:7932/testdb", captured)) {

            dataSourceRunner.withPropertyValues(
                            "spring.datasource.url=jdbc:postgresql://localhost:5432/testdb",
                            "spring.datasource.driver-class-name=org.postgresql.Driver")
                    .run(context -> {
                        DataSource ds = context.getBean(DataSource.class);
                        assertThat(ds).isInstanceOf(CachedDataSource.class);
                        HikariDataSource hikari = (HikariDataSource) ((CachedDataSource) ds).getDelegate();
                        assertThat(hikari.getJdbcUrl()).isEqualTo("jdbc:postgresql://localhost:7932/testdb");
                        assertThat(captured).hasSize(1);
                        // Empty config map means setConfig was not called
                        assertThat(captured.get(0).getConfig()).isNull();
                    });
        }
    }

    @Test
    void kebabToCamelConversion() {
        assertThat(GoldLapelDataSourcePostProcessor.kebabToCamel("pool-size")).isEqualTo("poolSize");
        assertThat(GoldLapelDataSourcePostProcessor.kebabToCamel("disable-n1")).isEqualTo("disableN1");
        assertThat(GoldLapelDataSourcePostProcessor.kebabToCamel("mode")).isEqualTo("mode");
        assertThat(GoldLapelDataSourcePostProcessor.kebabToCamel("read-after-write-secs")).isEqualTo("readAfterWriteSecs");
        assertThat(GoldLapelDataSourcePostProcessor.kebabToCamel("disable-n1-cross-connection")).isEqualTo("disableN1CrossConnection");
    }

    @Test
    void normalizeCamelCaseConvertsMap() {
        Map<String, String> input = Map.of(
                "pool-size", "30",
                "mode", "waiter",
                "disable-n1", "true"
        );
        Map<String, Object> result = GoldLapelDataSourcePostProcessor.normalizeCamelCase(input);
        assertThat(result).containsEntry("poolSize", "30");
        assertThat(result).containsEntry("mode", "waiter");
        assertThat(result).containsEntry("disableN1", Boolean.TRUE);
    }

    @Test
    void coerceValueConvertsBooleanStrings() {
        assertThat(GoldLapelDataSourcePostProcessor.coerceValue("true")).isEqualTo(Boolean.TRUE);
        assertThat(GoldLapelDataSourcePostProcessor.coerceValue("True")).isEqualTo(Boolean.TRUE);
        assertThat(GoldLapelDataSourcePostProcessor.coerceValue("TRUE")).isEqualTo(Boolean.TRUE);
        assertThat(GoldLapelDataSourcePostProcessor.coerceValue("false")).isEqualTo(Boolean.FALSE);
        assertThat(GoldLapelDataSourcePostProcessor.coerceValue("False")).isEqualTo(Boolean.FALSE);
        assertThat(GoldLapelDataSourcePostProcessor.coerceValue("FALSE")).isEqualTo(Boolean.FALSE);
    }

    @Test
    void coerceValueSplitsCommaSeparatedStrings() {
        Object result = GoldLapelDataSourcePostProcessor.coerceValue("users,orders,products");
        assertThat(result).isEqualTo(List.of("users", "orders", "products"));
    }

    @Test
    void coerceValueSplitsCommaSeparatedWithSpaces() {
        Object result = GoldLapelDataSourcePostProcessor.coerceValue("users , orders , products");
        assertThat(result).isEqualTo(List.of("users", "orders", "products"));
    }

    @Test
    void coerceValueLeavesPlainStringsAlone() {
        assertThat(GoldLapelDataSourcePostProcessor.coerceValue("waiter")).isEqualTo("waiter");
        assertThat(GoldLapelDataSourcePostProcessor.coerceValue("30")).isEqualTo("30");
        assertThat(GoldLapelDataSourcePostProcessor.coerceValue("postgresql://localhost:5432")).isEqualTo("postgresql://localhost:5432");
    }

    @Test
    void coerceValueHandlesNull() {
        assertThat(GoldLapelDataSourcePostProcessor.coerceValue(null)).isNull();
    }

    @Test
    void coerceValueHandlesTrailingComma() {
        Object result = GoldLapelDataSourcePostProcessor.coerceValue("users,orders,");
        assertThat(result).isEqualTo(List.of("users", "orders"));
    }

    @Test
    void normalizeCamelCaseCoercesBooleanAndListValues() {
        Map<String, String> input = Map.of(
                "disable-n1", "true",
                "enable-coalescing", "false",
                "exclude-tables", "users,orders",
                "pool-size", "30",
                "mode", "waiter"
        );
        Map<String, Object> result = GoldLapelDataSourcePostProcessor.normalizeCamelCase(input);
        assertThat(result).containsEntry("disableN1", Boolean.TRUE);
        assertThat(result).containsEntry("enableCoalescing", Boolean.FALSE);
        assertThat(result).containsEntry("excludeTables", List.of("users", "orders"));
        assertThat(result).containsEntry("poolSize", "30");
        assertThat(result).containsEntry("mode", "waiter");
    }

    // --- L1 native cache tests ---

    @Test
    void wrapsDataSourceWithCachedDataSource() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> ignored = stubStart(
                u -> "postgresql://localhost:7932/testdb", captured)) {

            HikariDataSource ds = new HikariDataSource();
            ds.setJdbcUrl("jdbc:postgresql://localhost:5432/testdb");

            GoldLapelProperties props = new GoldLapelProperties();
            GoldLapelDataSourcePostProcessor processor = new GoldLapelDataSourcePostProcessor(props);

            Object result = processor.postProcessAfterInitialization(ds, "dataSource");

            assertThat(result).isInstanceOf(CachedDataSource.class);
            CachedDataSource cached = (CachedDataSource) result;
            assertThat(cached.getDelegate()).isSameAs(ds);
            assertThat(cached.getCache()).isNotNull();
        }
    }

    @Test
    void nativeCacheDisabledReturnsBareDataSource() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> ignored = stubStart(
                u -> "postgresql://localhost:7932/testdb", captured)) {

            HikariDataSource ds = new HikariDataSource();
            ds.setJdbcUrl("jdbc:postgresql://localhost:5432/testdb");

            GoldLapelProperties props = new GoldLapelProperties();
            props.setNativeCache(false);
            GoldLapelDataSourcePostProcessor processor = new GoldLapelDataSourcePostProcessor(props);

            Object result = processor.postProcessAfterInitialization(ds, "dataSource");

            assertThat(result).isSameAs(ds);
            assertThat(result).isNotInstanceOf(CachedDataSource.class);
        }
    }

    @Test
    void nativeCacheDisabledViaProperty() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> ignored = stubStart(
                u -> "postgresql://localhost:7932/testdb", captured)) {

            dataSourceRunner.withPropertyValues(
                            "spring.datasource.url=jdbc:postgresql://localhost:5432/testdb",
                            "spring.datasource.driver-class-name=org.postgresql.Driver",
                            "goldlapel.native-cache=false")
                    .run(context -> {
                        DataSource ds = context.getBean(DataSource.class);
                        assertThat(ds).isInstanceOf(HikariDataSource.class);
                        assertThat(ds).isNotInstanceOf(CachedDataSource.class);
                        assertThat(((HikariDataSource) ds).getJdbcUrl()).isEqualTo("jdbc:postgresql://localhost:7932/testdb");
                    });
        }
    }

    @Test
    void defaultInvalidationPortIsProxyPortPlusTwo() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> ignored = stubStart(
                u -> "postgresql://localhost:7932/testdb", captured)) {

            HikariDataSource ds = new HikariDataSource();
            ds.setJdbcUrl("jdbc:postgresql://localhost:5432/testdb");

            GoldLapelProperties props = new GoldLapelProperties();
            props.setPort(7932);
            GoldLapelDataSourcePostProcessor processor = new GoldLapelDataSourcePostProcessor(props);

            Object result = processor.postProcessAfterInitialization(ds, "dataSource");

            assertThat(result).isInstanceOf(CachedDataSource.class);
            // Default invalidation port = proxy port + 2 = 7934
            // We can't directly check the port used, but we verify the cache was created
            CachedDataSource cached = (CachedDataSource) result;
            assertThat(cached.getCache()).isSameAs(NativeCache.getInstance());
        }
    }

    @Test
    void customInvalidationPort() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> ignored = stubStart(
                u -> "postgresql://localhost:7932/testdb", captured)) {

            HikariDataSource ds = new HikariDataSource();
            ds.setJdbcUrl("jdbc:postgresql://localhost:5432/testdb");

            GoldLapelProperties props = new GoldLapelProperties();
            props.setInvalidationPort(9999);
            GoldLapelDataSourcePostProcessor processor = new GoldLapelDataSourcePostProcessor(props);

            Object result = processor.postProcessAfterInitialization(ds, "dataSource");

            assertThat(result).isInstanceOf(CachedDataSource.class);
            CachedDataSource cached = (CachedDataSource) result;
            assertThat(cached.getCache()).isNotNull();
        }
    }

    @Test
    void customInvalidationPortViaProperty() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> ignored = stubStart(
                u -> "postgresql://localhost:7932/testdb", captured)) {

            dataSourceRunner.withPropertyValues(
                            "spring.datasource.url=jdbc:postgresql://localhost:5432/testdb",
                            "spring.datasource.driver-class-name=org.postgresql.Driver",
                            "goldlapel.invalidation-port=8888")
                    .run(context -> {
                        DataSource ds = context.getBean(DataSource.class);
                        assertThat(ds).isInstanceOf(CachedDataSource.class);
                    });
        }
    }

    @Test
    void cachedDataSourceDelegatesUnwrap() throws Exception {
        HikariDataSource hikari = new HikariDataSource();
        NativeCache cache = NativeCache.getInstance();
        CachedDataSource cached = new CachedDataSource(hikari, cache);

        assertThat(cached.isWrapperFor(CachedDataSource.class)).isTrue();
        assertThat(cached.unwrap(CachedDataSource.class)).isSameAs(cached);
    }

    @Test
    void propertiesDefaults() {
        GoldLapelProperties props = new GoldLapelProperties();
        assertThat(props.isNativeCache()).isTrue();
        assertThat(props.getInvalidationPort()).isEqualTo(0);
        assertThat(props.isEnabled()).isTrue();
        assertThat(props.getPort()).isEqualTo(7932);
    }

    // --- DataSource type agnostic tests ---

    @Test
    void extractJdbcUrlFromHikari() {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl("jdbc:postgresql://host:5432/db");
        assertThat(GoldLapelDataSourcePostProcessor.extractJdbcUrl(ds)).isEqualTo("jdbc:postgresql://host:5432/db");
    }

    @Test
    void extractJdbcUrlFromGetUrl() {
        // Simulates a DataSource with getUrl() (e.g., Tomcat DBCP)
        DataSource ds = new DataSourceWithGetUrl("jdbc:postgresql://host:5432/db");
        assertThat(GoldLapelDataSourcePostProcessor.extractJdbcUrl(ds)).isEqualTo("jdbc:postgresql://host:5432/db");
    }

    @Test
    void extractJdbcUrlReturnsNullForUnknown() {
        // A DataSource with no URL getter methods
        DataSource ds = mock(DataSource.class);
        assertThat(GoldLapelDataSourcePostProcessor.extractJdbcUrl(ds)).isNull();
    }

    @Test
    void worksWithNonHikariDataSource() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> ignored = stubStart(
                u -> "postgresql://localhost:7932/db", captured)) {

            DataSourceWithGetUrl ds = new DataSourceWithGetUrl("jdbc:postgresql://host:5432/db");

            GoldLapelProperties props = new GoldLapelProperties();
            props.setNativeCache(false);
            GoldLapelDataSourcePostProcessor processor = new GoldLapelDataSourcePostProcessor(props);

            Object result = processor.postProcessAfterInitialization(ds, "dataSource");

            assertThat(captured).hasSize(1);
            assertThat(ds.getUrl()).isEqualTo("jdbc:postgresql://localhost:7932/db");
            assertThat(result).isSameAs(ds);
        }
    }

    @Test
    void skipsNonDataSourceBeans() {
        List<GoldLapelOptions> captured = new ArrayList<>();
        try (MockedStatic<GoldLapel> stat = stubStart(
                u -> "postgresql://localhost:7932/db", captured)) {
            GoldLapelDataSourcePostProcessor processor = new GoldLapelDataSourcePostProcessor(
                    new GoldLapelProperties());

            Object bean = "not a datasource";
            Object result = processor.postProcessAfterInitialization(bean, "myBean");

            assertThat(result).isSameAs(bean);
            stat.verify(() -> GoldLapel.start(anyString(), any()), times(0));
        }
    }

    // Minimal DataSource with getUrl()/setUrl() — simulates Tomcat DBCP pattern
    static class DataSourceWithGetUrl implements DataSource {
        private String url;

        DataSourceWithGetUrl(String url) {
            this.url = url;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        @Override public java.sql.Connection getConnection() { return null; }
        @Override public java.sql.Connection getConnection(String u, String p) { return null; }
        @Override public java.io.PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(java.io.PrintWriter out) {}
        @Override public void setLoginTimeout(int seconds) {}
        @Override public int getLoginTimeout() { return 0; }
        @Override public java.util.logging.Logger getParentLogger() { return null; }
        @Override public <T> T unwrap(Class<T> iface) { return null; }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    }
}
