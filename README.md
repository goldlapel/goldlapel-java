# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```xml
<dependency>
    <groupId>com.goldlapel</groupId>
    <artifactId>goldlapel</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Quick Start

```java
import com.goldlapel.GoldLapel;

// Start the proxy — returns a connection string pointing at Gold Lapel
String url = GoldLapel.start("postgresql://user:pass@localhost:5432/mydb");

// Use the URL with any Postgres driver
Connection conn = DriverManager.getConnection(url);

// Or HikariCP, JOOQ, Hibernate, Spring JDBC — anything that speaks Postgres
```

Gold Lapel is driver-agnostic. `start()` returns a connection string (`postgresql://...@localhost:7932/...`) that works with any Postgres driver or ORM.

## API

### `GoldLapel.start(upstream)`
### `GoldLapel.start(upstream, options)`

Starts the Gold Lapel proxy and returns the proxy connection string.

- `upstream` — your Postgres connection string (e.g. `postgresql://user:pass@localhost:5432/mydb`)
- `options.port(int)` — proxy port (default: 7932)
- `options.extraArgs(String...)` — additional CLI flags passed to the binary (e.g. `"--threshold-impact", "5000"`)

### `GoldLapel.stop()`

Stops the proxy. Also called automatically on JVM shutdown.

### `GoldLapel.proxyUrl()`

Returns the current proxy URL, or `null` if not running.

### `new GoldLapel(upstream)` / `new GoldLapel(upstream, options)`

Instance API for managing multiple proxies:

```java
import com.goldlapel.GoldLapel;

GoldLapel proxy = new GoldLapel("postgresql://user:pass@localhost:5432/mydb",
    new GoldLapel.Options().port(7932));
String url = proxy.startProxy();
// ...
proxy.stopProxy();
```

## Configuration

The proxy binary accepts all standard Gold Lapel flags. Pass them via `extraArgs`:

```java
String url = GoldLapel.start(
    "postgresql://user:pass@localhost:5432/mydb",
    new GoldLapel.Options()
        .extraArgs("--threshold-duration-ms", "200", "--refresh-interval-secs", "30")
);
```

Or set environment variables (`GOLDLAPEL_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## How It Works

This package bundles the Gold Lapel Rust binary for your platform. When you call `start()`, it:

1. Locates the binary (bundled in JAR, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Returns a connection string pointing at the proxy
5. Cleans up automatically on JVM shutdown

The binary does all the work — this wrapper just manages its lifecycle.

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
