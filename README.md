# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes, with an L1 native cache that serves repeated reads in microseconds. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```xml
<dependency>
    <groupId>com.goldlapel</groupId>
    <artifactId>goldlapel</artifactId>
    <version>0.2.0</version>
</dependency>
```

## Quick Start

```java
import com.goldlapel.GoldLapel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

try (GoldLapel gl = GoldLapel.start("postgresql://user:pass@localhost:5432/mydb", opts -> {
        opts.setPort(7932);
        opts.setLogLevel("info");
})) {
    // JDBC: use getJdbcUrl() + user/password properties (the JDBC driver
    // rejects userinfo inline in the URL).
    Properties props = new Properties();
    if (gl.getJdbcUser() != null) props.setProperty("user", gl.getJdbcUser());
    if (gl.getJdbcPassword() != null) props.setProperty("password", gl.getJdbcPassword());
    try (Connection conn = DriverManager.getConnection(gl.getJdbcUrl(), props)) {
        var stmt = conn.prepareStatement("SELECT * FROM users WHERE id = ?");
        stmt.setLong(1, 42);
        var rs = stmt.executeQuery();
        // ...
    }

    // Or use the built-in wrapper methods directly — they reuse the
    // connection GoldLapel opened at start():
    var hits = gl.search("articles", "body", "postgres tuning", 10, "english", false);
    gl.docInsert("events", "{\"type\":\"signup\"}");
}
// try-with-resources auto-stops the proxy
```

`gl.getUrl()` returns the URL in the usual Postgres form
(`postgresql://user:pass@localhost:7932/mydb`) — handy for passing to libpq
or psql. `gl.getJdbcUrl()` returns the JDBC-ready form without userinfo.

## API

### `GoldLapel.start(upstream)`
### `GoldLapel.start(upstream, configurator)`

Starts a Gold Lapel proxy and returns a `GoldLapel` instance. Configuration is supplied via a `Consumer<GoldLapelOptions>` lambda:

```java
GoldLapel gl = GoldLapel.start("postgresql://user:pass@localhost/mydb", opts -> {
    opts.setPort(9000);
    opts.setLogLevel("debug");
    opts.setConfig(Map.of("mode", "waiter", "poolSize", 50));
    opts.setExtraArgs("--threshold-duration-ms", "200");
});
```

Options:

- `setPort(int)` — proxy port (default: `7932`)
- `setLogLevel(String)` — log level passed to the binary (`trace`/`debug`/`info`/`warn`/`error`)
- `setConfig(Map<String, Object>)` — structured config; see below
- `setExtraArgs(String...)` — raw CLI flags appended to the binary invocation
- `setClient(String)` — identifies the wrapper in telemetry (default: `java`)

### `gl.getUrl()`

Returns the proxy URL in the standard Postgres form (e.g. `postgresql://user:pass@localhost:7932/mydb`) — mirrors the shape of the upstream URL, suitable for libpq/psql and any driver that accepts inline userinfo. The PostgreSQL JDBC driver does **not** accept inline userinfo (it reads everything before `@` as the hostname); for JDBC use `getJdbcUrl()` / `getJdbcUser()` / `getJdbcPassword()` below.

### `gl.getJdbcUrl()` / `gl.getJdbcUser()` / `gl.getJdbcPassword()`

Returns the JDBC-ready form of the proxy URL (no userinfo) plus the parsed user and password. Hand them to `DriverManager.getConnection(...)` with a `Properties` object, or pass them separately:

```java
Properties props = new Properties();
if (gl.getJdbcUser() != null) props.setProperty("user", gl.getJdbcUser());
if (gl.getJdbcPassword() != null) props.setProperty("password", gl.getJdbcPassword());
try (Connection conn = DriverManager.getConnection(gl.getJdbcUrl(), props)) {
    // ...
}
```

### `gl.close()` / `gl.stop()`

Stops the proxy and closes the internal JDBC connection. `GoldLapel` implements `AutoCloseable`, so try-with-resources handles cleanup automatically.

### `gl.using(conn, runnable)`

Scope a block to a caller-supplied connection. Wrapper methods called inside the lambda (on the same thread) use that connection instead of Gold Lapel's internal one. Ideal for transactional work:

```java
Properties props = new Properties();
if (gl.getJdbcUser() != null) props.setProperty("user", gl.getJdbcUser());
if (gl.getJdbcPassword() != null) props.setProperty("password", gl.getJdbcPassword());
try (Connection tx = DriverManager.getConnection(gl.getJdbcUrl(), props)) {
    tx.setAutoCommit(false);
    gl.using(tx, () -> {
        gl.docInsert("orders", "{\"id\":42}");
        gl.docInsert("events", "{\"type\":\"order.created\",\"order_id\":42}");
    });
    tx.commit();
}
```

The scope is thread-local: concurrent callers on other threads are unaffected. Nested `using(...)` calls restore the outer connection on exit, and exceptions unwind cleanly.

> **Footgun — `using()` is `ThreadLocal`-scoped.** The scope attaches to the thread calling `using(...)` and **does not propagate** across thread boundaries. In particular, wrapper calls that run on a different thread will NOT see the scoped connection:
>
> - `ExecutorService.submit(...)` / `.execute(...)`
> - `CompletableFuture.supplyAsync(...)` / `.runAsync(...)`
> - `parallelStream()` / `stream().parallel()`
> - any framework task scheduler that hops threads
>
> If your work fans out to a worker pool, pass `conn` explicitly as the last argument to each wrapper call instead of relying on `using(...)`:
>
> ```java
> // DON'T — the supplyAsync body runs on a ForkJoin worker that
> // never entered the using() scope.
> gl.using(tx, () -> {
>     CompletableFuture.supplyAsync(() -> gl.docInsert("events", "{}"));
> });
>
> // DO — pass tx explicitly.
> CompletableFuture.supplyAsync(() -> gl.docInsert("events", "{}", tx));
> ```

### Per-method connection override

Every wrapper method has an overload that accepts an explicit `Connection` as its last argument:

```java
gl.docInsert("events", "{\"type\":\"x\"}", conn);
gl.search("articles", "body", "postgres", 10, "english", false, conn);
```

Precedence: **explicit `conn` argument > `using()` scope > internal connection**.

> **Note:** `gl.script(String luaCode, String... args)` has no `Connection` overload — the trailing `String...` varargs would swallow a `Connection` passed as the last argument. To run `script` against a specific connection, wrap it in `using`:
>
> ```java
> gl.using(conn, () -> gl.script("return 1", "x"));
> ```

### `gl.getDashboardUrl()`

Returns the dashboard URL (e.g. `http://127.0.0.1:7933`), or `null` if the proxy isn't running. The dashboard port is the proxy port + 1 by default and can be changed via config:

```java
GoldLapel gl = GoldLapel.start("postgresql://user:pass@localhost/mydb", opts ->
    opts.setConfig(Map.of("dashboardPort", 8080)));
String dashboard = gl.getDashboardUrl(); // http://127.0.0.1:8080
```

Set `dashboardPort` to `0` to disable.

## Configuration

Pass a config map via `setConfig`:

```java
import com.goldlapel.GoldLapel;
import java.util.List;
import java.util.Map;

GoldLapel gl = GoldLapel.start("postgresql://user:pass@localhost/mydb", opts ->
    opts.setConfig(Map.of(
        "mode", "waiter",
        "poolSize", 50,
        "disableMatviews", true,
        "replica", List.of("postgresql://user:pass@replica1/mydb")
    )));
```

Keys use `camelCase` and map to CLI flags (`poolSize` → `--pool-size`). Boolean keys are flags — `true` enables them. List keys produce repeated flags.

Unknown keys throw `IllegalArgumentException`. To see all valid keys:

```java
GoldLapel.configKeys()
```

For the full configuration reference, see the [main documentation](https://github.com/goldlapel/goldlapel#setting-reference).

Or set environment variables (`GOLDLAPEL_PROXY_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## Reactive API

A separate `goldlapel-reactor` artifact exposes a Project Reactor + R2DBC flavour — full parity with the sync API, returning `Mono<T>` / `Flux<T>` instead of blocking values. Use it in Spring WebFlux, reactor-based apps, or anywhere you need non-blocking I/O.

```xml
<dependency>
    <groupId>com.goldlapel</groupId>
    <artifactId>goldlapel-reactor</artifactId>
    <version>0.2.0</version>
</dependency>
```

The sync `goldlapel` artifact is untouched — reactive users pull R2DBC + Reactor through this additional dependency.

### Quick Start

```java
import com.goldlapel.reactor.ReactiveGoldLapel;
import reactor.core.publisher.Mono;

Mono<Long> pipeline = ReactiveGoldLapel.start("postgresql://user:pass@db/mydb")
    .flatMap(gl ->
        gl.docInsert("events", "{\"type\":\"signup\"}")
          .then(gl.docCount("events", "{}"))
          .flatMap(count -> gl.stop().thenReturn(count))
    );

pipeline.subscribe(count -> System.out.println("Total: " + count));
```

Cancelling the `Mono<ReactiveGoldLapel>` mid-spawn tears the subprocess down — no leaks.

### `using(conn)` — scope a connection over a Mono chain

The reactive `using()` propagates a JDBC `Connection` through Reactor Context (not ThreadLocal), so it survives `flatMap` boundaries and scheduler hops:

```java
try (java.sql.Connection myConn = dataSource.getConnection()) {
    gl.using(myConn, g ->
        g.docInsert("events", json)
         .then(g.docCount("events", "{}"))
    ).block();
}
```

Nested `using` blocks restore the outer connection on the way out — standard Context semantics.

### Raw R2DBC queries against the proxy

For your own non-blocking queries, `gl.connectionFactory()` returns an `io.r2dbc.spi.ConnectionFactory` wired to the proxy — plug it into Spring Data R2DBC's `DatabaseClient`, jOOQ-R2DBC, or raw R2DBC:

```java
Flux<Long> ids = Mono.from(gl.connectionFactory().create())
    .flatMapMany(conn ->
        Flux.from(conn.createStatement("SELECT id FROM users WHERE active").execute())
            .flatMap(r -> r.map((row, meta) -> row.get(0, Long.class)))
            .concatWith(Mono.from(conn.close()).cast(Long.class))
    );
```

The ~61 wrapper helpers (`search`, `docFind`, `incr`, etc.) bridge through JDBC on `Schedulers.boundedElastic()` — Reactor's canonical pattern for running blocking code inside a reactive pipeline. That keeps the wire semantics identical to the sync API. The reactive value is in the R2DBC `ConnectionFactory` above: your own queries go end-to-end non-blocking.

## RxJava 3 API

A separate `goldlapel-rxjava3` artifact exposes the same reactive surface with RxJava 3 return types — `Single` / `Flowable` / `Completable` / `Maybe` — for codebases already standardised on RxJava.

```xml
<dependency>
    <groupId>com.goldlapel</groupId>
    <artifactId>goldlapel-rxjava3</artifactId>
    <version>0.2.0</version>
</dependency>
```

Pulls in `goldlapel-reactor` + `io.reactivex.rxjava3:rxjava` transitively; no conflict with a plain sync or plain reactive install, but typically you'll depend on only one of the three flavours.

### Quick Start

```java
import com.goldlapel.rxjava3.RxJavaGoldLapel;
import io.reactivex.rxjava3.core.Single;

Single<Long> pipeline = RxJavaGoldLapel.start("postgresql://user:pass@db/mydb")
    .flatMap(gl ->
        gl.docInsert("events", "{\"type\":\"signup\"}")
          .flatMap(ignored -> gl.docCount("events", "{}"))
          .flatMap(count -> gl.stop().toSingleDefault(count))
    );

pipeline.subscribe(count -> System.out.println("Total: " + count));
```

### Type mapping

| Reactor (`goldlapel-reactor`) | RxJava 3 (`goldlapel-rxjava3`) |
|---|---|
| `Mono<T>` (always emits) | `Single<T>` |
| `Mono<T>` (may be empty — `hget`, `docFindOne`, `zscore`, etc.) | `Maybe<T>` |
| `Mono<Void>` | `Completable` |
| `Flux<T>` | `Flowable<T>` |

Nullable lookups return `Maybe<T>` so "not found" is a clean `onComplete` with no value, rather than a `NoSuchElementException`.

### Scoping connections

RxJava has no `ContextView` equivalent of Reactor, so there's no `using(conn, body)` method on `RxJavaGoldLapel`. Two options:

1. **Per-call explicit `Connection`** — every wrapper method has an overload taking a final `Connection conn` argument:
   ```java
   gl.docInsert("events", json, myConn)
     .flatMap(ignored -> gl.docCount("events", "{}", myConn))
     .subscribe();
   ```
2. **Drop into the Reactor API for a chain** — `gl.reactor()` returns the underlying `ReactiveGoldLapel`, which supports Reactor-Context scoping:
   ```java
   gl.reactor().using(myConn, g ->
       g.docInsert("events", json).then(g.docCount("events", "{}"))
   ).subscribe();
   ```

### Raw R2DBC queries

Same as the Reactor module — `gl.connectionFactory()` returns an R2DBC `ConnectionFactory` pointing at the proxy. Mix with `io.reactivex.rxjava3.core.Flowable.fromPublisher` to pull rows back into RxJava types.

## Spring Boot

A separate `goldlapel-spring-boot` artifact auto-configures the proxy in front of every Postgres `DataSource` in your context:

```xml
<dependency>
    <groupId>com.goldlapel</groupId>
    <artifactId>goldlapel-spring-boot</artifactId>
    <version>0.2.0</version>
</dependency>
```

With the starter on the classpath, every `DataSource` with a `jdbc:postgresql://...` URL is transparently routed through a Gold Lapel proxy. Configure via `application.yml`:

```yaml
goldlapel:
  enabled: true
  port: 7932
  native-cache: true
  config:
    mode: waiter
    pool-size: 30
    disable-n1: true
```

## How It Works

This package bundles the Gold Lapel Rust binary for your platform. When you call `GoldLapel.start(...)`, it:

1. Locates the binary (bundled in JAR, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Opens an internal JDBC connection (eager — fails fast if the driver is missing)
5. Returns a `GoldLapel` instance you close with try-with-resources

The binary does all the work — this wrapper just manages its lifecycle.

## Upgrading from v0.1

v0.2.0 replaces the instance-constructor API with a static factory and `AutoCloseable` lifecycle. Breaking changes:

| v0.1 | v0.2 |
| ---- | ---- |
| `new GoldLapel(url)` + `gl.start()` | `GoldLapel.start(url)` |
| `new GoldLapel.Options().port(9000)` | `opts -> opts.setPort(9000)` |
| `gl.stopProxy()` | `gl.close()` or `gl.stop()` |
| `GoldLapel.stop()` (static) | removed — instance-scoped only |
| `GoldLapel.proxyUrl()` (static) | use `gl.getUrl()` |

New in v0.2:

- `GoldLapel implements AutoCloseable` — works with try-with-resources
- `gl.using(conn, Runnable)` — scope wrapper calls to a caller-supplied connection
- Per-method `Connection`-override overload on every wrapper method
- Eager connect — `start()` opens the internal JDBC connection up-front and fails fast

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
