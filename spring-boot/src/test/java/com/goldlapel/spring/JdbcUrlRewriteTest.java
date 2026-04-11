package com.goldlapel.spring;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcUrlRewriteTest {

    private static final String JDBC_PREFIX = "jdbc:";

    private static String stripJdbc(String jdbcUrl) {
        return jdbcUrl.substring(JDBC_PREFIX.length());
    }

    private static String prependJdbc(String url) {
        return JDBC_PREFIX + url;
    }

    @ParameterizedTest
    @CsvSource({
            "jdbc:postgresql://localhost:5432/db, postgresql://localhost:5432/db",
            "jdbc:postgresql://user:pass@host:5432/db, postgresql://user:pass@host:5432/db",
            "jdbc:postgresql://host/db?sslmode=require, postgresql://host/db?sslmode=require",
    })
    void stripJdbcPrefix(String jdbcUrl, String expected) {
        assertThat(stripJdbc(jdbcUrl)).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({
            "postgresql://localhost:7932/db, jdbc:postgresql://localhost:7932/db",
            "postgresql://user:pass@localhost:7932/db, jdbc:postgresql://user:pass@localhost:7932/db",
    })
    void prependJdbcPrefix(String proxyUrl, String expected) {
        assertThat(prependJdbc(proxyUrl)).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({
            "jdbc:postgresql://localhost:5432/db, postgresql://localhost:7932/db, jdbc:postgresql://localhost:7932/db",
    })
    void roundTrip(String originalJdbc, String proxyUrl, String expectedJdbc) {
        String upstream = stripJdbc(originalJdbc);
        assertThat(upstream).doesNotStartWith("jdbc:");
        String result = prependJdbc(proxyUrl);
        assertThat(result).isEqualTo(expectedJdbc);
    }
}
