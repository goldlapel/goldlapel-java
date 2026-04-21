# goldlapel

[![Tests](https://github.com/goldlapel/goldlapel-java/actions/workflows/test.yml/badge.svg)](https://github.com/goldlapel/goldlapel-java/actions/workflows/test.yml)

The Java wrapper for [Gold Lapel](https://goldlapel.com) — a self-optimizing Postgres proxy that watches query patterns and creates materialized views + indexes automatically. Zero code changes beyond the connection string.

## Install

```xml
<dependency>
    <groupId>com.goldlapel</groupId>
    <artifactId>goldlapel</artifactId>
    <version>0.2.0</version>
</dependency>
```

The PostgreSQL JDBC driver is bundled transitively.

## Quickstart

```java
import com.goldlapel.GoldLapel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

try (GoldLapel gl = GoldLapel.start("postgresql://user:pass@localhost:5432/mydb")) {
    // JDBC rejects inline userinfo — use the JDBC helpers:
    Properties props = new Properties();
    props.setProperty("user", gl.getJdbcUser());
    props.setProperty("password", gl.getJdbcPassword());

    try (Connection conn = DriverManager.getConnection(gl.getJdbcUrl(), props)) {
        var stmt = conn.prepareStatement("SELECT * FROM users WHERE id = ?");
        stmt.setLong(1, 42);
        var rs = stmt.executeQuery();
    }
}
// try-with-resources auto-stops the proxy
```

Point any JDBC driver at `gl.getJdbcUrl()` (with `gl.getJdbcUser()` / `gl.getJdbcPassword()` in a `Properties`, since JDBC rejects inline userinfo). Gold Lapel sits between your app and your DB, watching query patterns and creating materialized views + indexes automatically. Zero code changes beyond the connection string.

Scoped transactional coordination via `gl.using(conn, Runnable)`, reactive (`goldlapel-reactor`, `goldlapel-rxjava3`) and Spring Boot (`goldlapel-spring-boot`) flavours are in the docs.

## Dashboard

Gold Lapel exposes a live dashboard at `gl.getDashboardUrl()`:

```java
System.out.println(gl.getDashboardUrl());
// -> http://127.0.0.1:7933
```

## Documentation

Full API reference, configuration, reactive (Reactor / RxJava 3), Spring Boot integration, upgrading from v0.1, and production deployment: https://goldlapel.com/docs/java

## License

MIT. See `LICENSE`.
