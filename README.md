# goldlapel

[![Tests](https://github.com/goldlapel/goldlapel-java/actions/workflows/test.yml/badge.svg)](https://github.com/goldlapel/goldlapel-java/actions/workflows/test.yml)

The Java wrapper for [Gold Lapel](https://goldlapel.com) — a self-optimizing Postgres proxy that caches query results, creates indexes from your query patterns, and keeps the cache correct as your data changes. Zero code changes beyond the connection string.

The wrapper runs the proxy as a managed subprocess: it finds the bundled binary, starts it with your app and stops it on `gl.close()`, translates options into proxy flags, generates the dashboard token, and hands back a driver-ready JDBC URL. It also provides Postgres-backed helpers — search and percolator, a document store, streams, counters, sorted sets, hashes, queues, geo, and pub/sub. Caching happens in the proxy, which serves every client the same way; the connections you open are plain PostgreSQL JDBC connections, and the Spring Boot integration repoints your existing `DataSource` at the proxy without wrapping it.

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

Point any JDBC driver at `gl.getJdbcUrl()` (with `gl.getJdbcUser()` / `gl.getJdbcPassword()` in a `Properties`, since JDBC rejects inline userinfo). Gold Lapel sits between your app and your DB, caching results and creating indexes from your query patterns. Connections are tagged `goldlapel:java:<version>` — `ApplicationName` in `getJdbcUrl()`, `application_name` in `getUrl()` — so they're recognisable in `pg_stat_activity`. The upstream URL's TLS parameters (`sslmode` and the like) stay on the proxy's hop to Postgres; the app's URL leaves them out.

The proxy listens on two ports: the proxy itself (`setProxyPort`) and the dashboard (`setDashboardPort`, default proxy port + 1; `0` disables it). Leave the proxy port unset and it's the first port from 7932 up where both are free, so several databases (or test contexts) in one JVM never collide. Starting the same upstream twice shares one proxy, which stops when the last instance using it stops.

Scoped transactional coordination via `gl.using(conn, Runnable)`, reactive (`goldlapel-reactor`, `goldlapel-rxjava3`) and Spring Boot (`goldlapel-spring-boot`) flavours are in the docs.

## Dashboard

Gold Lapel exposes a live dashboard at `gl.getDashboardUrl()`:

```java
System.out.println(gl.getDashboardUrl());
// -> http://127.0.0.1:7933
```

## Documentation

Full API reference, configuration, reactive (Reactor / RxJava 3), Spring Boot integration, upgrading from v0.1, and production deployment: https://goldlapel.com/docs/java

## Uninstalling

Before removing the package, drop Gold Lapel's helper schema and indexes from your Postgres:

```bash
goldlapel clean
```

Then remove the `<dependency>` block from your `pom.xml` (or the equivalent line from `build.gradle`) and clear any local state:

```bash
mvn dependency:purge-local-repository -Dinclude=com.goldlapel:goldlapel
rm -rf ~/.goldlapel
rm -f goldlapel.toml     # only if you wrote one
```

Cancelling your subscription does not delete your data — only Gold Lapel's helper schema and indexes go away.

## License

MIT. See `LICENSE`.
