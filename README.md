# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes, with an L1 native cache that serves repeated reads in microseconds. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```xml
<dependency>
    <groupId>com.goldlapel</groupId>
    <artifactId>goldlapel</artifactId>
    <version>0.1.0-rc16</version>
</dependency>
```

## Quick Start

```java
import com.goldlapel.GoldLapel;

// Create and start the proxy
GoldLapel gl = new GoldLapel("postgresql://user:pass@localhost:5432/mydb");
Connection conn = gl.start();

// Use the connection directly — no DriverManager needed
ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM users WHERE id = 42");
```

## API

### `new GoldLapel(upstream)`
### `new GoldLapel(upstream, options)`

Creates a new Gold Lapel proxy instance.

- `upstream` — your Postgres connection string (e.g. `postgresql://user:pass@localhost:5432/mydb`)
- `options.port(int)` — proxy port (default: 7932)
- `options.extraArgs(String...)` — additional CLI flags passed to the binary (e.g. `"--threshold-impact", "5000"`)

### `gl.start()`

Starts the proxy and returns a database connection with L1 cache. Also registers a JVM shutdown hook for cleanup.

### `gl.stop()`

Stops the proxy.

### `gl.proxyUrl()`

Returns the current proxy URL, or `null` if not running.

### `gl.dashboardUrl()`

Returns the dashboard URL (e.g. `http://127.0.0.1:7933`), or `null` if not running. The dashboard port defaults to 7933 and can be changed via config:

```java
GoldLapel gl = new GoldLapel("postgresql://user:pass@localhost/mydb",
    new GoldLapel.Options().config(Map.of("dashboardPort", 8080)));
gl.start();
String dashboard = gl.dashboardUrl(); // http://127.0.0.1:8080
```

Set `dashboardPort` to `0` to disable.

## Configuration

Pass a config map via the Options builder:

```java
import com.goldlapel.GoldLapel;
import java.util.List;
import java.util.Map;

GoldLapel gl = new GoldLapel("postgresql://user:pass@localhost/mydb",
    new GoldLapel.Options().config(Map.of(
        "mode", "waiter",
        "poolSize", 50,
        "disableMatviews", true,
        "replica", List.of("postgresql://user:pass@replica1/mydb")
    )));
Connection conn = gl.start();
```

Keys use `camelCase` and map to CLI flags (`poolSize` → `--pool-size`). Boolean keys are flags — `true` enables them. List keys produce repeated flags.

Unknown keys throw `IllegalArgumentException`. To see all valid keys:

```java
GoldLapel.configKeys()
```

For the full configuration reference, see the [main documentation](https://github.com/goldlapel/goldlapel#setting-reference).

You can also pass raw CLI flags via `extraArgs`:

```java
GoldLapel gl = new GoldLapel(
    "postgresql://user:pass@localhost:5432/mydb",
    new GoldLapel.Options()
        .extraArgs("--threshold-duration-ms", "200", "--refresh-interval-secs", "30")
);
Connection conn = gl.start();
```

Or set environment variables (`GOLDLAPEL_PROXY_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## How It Works

This package bundles the Gold Lapel Rust binary for your platform. When you call `gl.start()`, it:

1. Locates the binary (bundled in JAR, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Returns a database connection with L1 native cache built in
5. Cleans up automatically on JVM shutdown

The binary does all the work — this wrapper just manages its lifecycle.

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
