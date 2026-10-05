# Changelog

## Unreleased

### Breaking changes

**The in-process cache (L1) is gone.** The proxy's result cache now serves
every client the same way, so the wrapper no longer carries its own. Deleted
with it: `NativeCache`, `ConnectionProxy` and `CachedResultSet`, the
session-settings tracker (`GucState`), aggressive post-DML verify
(`AggressiveVerifyMode`), the invalidation-socket client and its stats
reporting, and Spring Boot's `CachedDataSource`. `gl.connection()` and the
connections you open from `gl.getJdbcUrl()` were never wrapped; the Spring
Boot integration now returns your `DataSource` itself, repointed at the
proxy, and no longer sets HikariCP's `connectionInitSql` to `DISCARD ALL`.

**Removed options, no aliases:** `GoldLapelOptions.setInvalidationPort`,
`setDisableNativeCache`, `setAggressiveVerify` and `setDisableMatviews`
(with their getters), the `gl.invalidationPort()` and `gl.aggressiveVerify()`
accessors, and the Spring properties `goldlapel.invalidation-port`,
`goldlapel.disable-native-cache`, `goldlapel.aggressive-verify` and
`goldlapel.disable-matviews`. The `GOLDLAPEL_DISABLE_NATIVE_CACHE`,
`GOLDLAPEL_NATIVE_CACHE`, `GOLDLAPEL_NATIVE_CACHE_SIZE` and
`GOLDLAPEL_REPORT_STATS` env vars are no longer read. The proxy now uses two
ports: proxy and dashboard (proxy + 1).

**Removed `config` keys** for materialized views, which the proxy no longer
has: `refreshIntervalSecs`, `patternTtlSecs`, `maxTablesPerView`,
`maxColumnsPerView`, `disableConsolidation`, `disableRewrite`,
`disableShadowMode`. `enableCoalescing` is replaced by `disableCoalescing`,
matching the proxy (coalescing is on by default). Passing a removed key
throws `IllegalArgumentException` at `start`.

---

**Phase 5 of schema-to-core: 5 Redis-compat families moved under nested
namespaces.** The flat `gl.incr` / `gl.getCounter`, `gl.zadd` / `gl.zrange`
/ `gl.zincrby` / `gl.zrank` / `gl.zscore` / `gl.zrem`, `gl.hset` / `gl.hget`
/ `gl.hgetall` / `gl.hdel`, `gl.enqueue` / `gl.dequeue`, and `gl.geoadd` /
`gl.georadius` / `gl.geodist` methods are gone. Counter, sorted-set, hash,
queue, and geo operations now live under their nested namespaces:

| Old (flat)                                              | New (nested)                                                              |
| ------------------------------------------------------- | ------------------------------------------------------------------------- |
| `gl.incr(table, key, amount)`                           | `gl.counters.incr(name, key, amount)`                                     |
| `gl.getCounter(table, key)`                             | `gl.counters.get(name, key)`                                              |
| `gl.zadd(table, member, score)`                         | `gl.zsets.add(name, zsetKey, member, score)` *(zset_key now first arg)*   |
| `gl.zincrby(table, member, amount)`                     | `gl.zsets.incrBy(name, zsetKey, member, delta)`                           |
| `gl.zrange(table, start, stop, desc)`                   | `gl.zsets.range(name, zsetKey, start, stop, desc)`                        |
| `gl.zrank(table, member, desc)`                         | `gl.zsets.rank(name, zsetKey, member, desc)`                              |
| `gl.zscore(table, member)`                              | `gl.zsets.score(name, zsetKey, member)`                                   |
| `gl.zrem(table, member)`                                | `gl.zsets.remove(name, zsetKey, member)`                                  |
| `gl.hset(table, key, field, valueJson)`                 | `gl.hashes.set(name, hashKey, field, valueJson)` *(row-per-field storage)*|
| `gl.hget(table, key, field)`                            | `gl.hashes.get(name, hashKey, field)`                                     |
| `gl.hgetall(table, key)` *→ JSON String*                | `gl.hashes.getAll(name, hashKey)` *→ `Map<String,String>`*                |
| `gl.hdel(table, key, field)`                            | `gl.hashes.delete(name, hashKey, field)`                                  |
| `gl.enqueue(table, payload)`                            | `gl.queues.enqueue(name, payload)`                                        |
| `gl.dequeue(table)` *(delete-on-fetch)*                 | `gl.queues.claim(name, visibilityMs)` + `gl.queues.ack(name, id)` *(at-least-once)* |
| `gl.geoadd(table, ncol, gcol, name, lon, lat)`          | `gl.geos.add(name, member, lon, lat)` *(GEOGRAPHY-native, idempotent)*    |
| `gl.georadius(table, gcol, lon, lat, radius, limit)`    | `gl.geos.radius(name, lon, lat, radius, unit, limit)`                     |
| `gl.geodist(table, gcol, ncol, a, b)`                   | `gl.geos.dist(name, a, b, unit)`                                          |

**Phase 5 schema changes:**

- **Counter:** `(key, value, updated_at)` per row — `updated_at` stamped on every write.
- **Zset:** `zset_key` column added so a single namespace table holds many sorted sets — Redis ZADD shape.
- **Hash:** Storage flipped from JSONB-blob-per-key to row-per-field (`hash_key`, `field`, `value JSONB`).
- **Queue:** At-least-once delivery with visibility timeout. `dequeue` (delete-on-fetch) replaced by explicit `claim`/`ack` pair. **No `dequeue` compat shim.**
- **Geo:** GEOGRAPHY-native (drops `::geography` casts on column refs), `member TEXT PRIMARY KEY` (idempotent `add`).

**Geo radius bind-order contract** (load-bearing — the proxy CTE-anchors so each `$N` appears once in rendered SQL):

- `gl.geos.radius(...)`: 4 args `(lon, lat, radius_m, limit)` — no duplicates.
- `gl.geos.radiusByMember(...)`: 4 args `(member, member, radius_m, limit)` — `$1` and `$2` both hold the anchor member.

The five family namespaces (`counters`, `zsets`, `hashes`, `queues`, `geos`)
are `public final` fields on `GoldLapel`, `ReactiveGoldLapel`, and
`RxJavaGoldLapel` — same shape as the existing `documents` and `streams`
namespaces (and Python/JS/Ruby/PHP/Go/.NET wrappers).

**Phase 5 DDL is owned by the proxy.** Each `gl.<family>.<verb>` call fetches
the canonical query patterns from `POST /api/ddl/<family>/create` (idempotent),
caches them per session, and runs the proxy-emitted SQL verbatim (after
`$N → ?` JDBC translation). The wrapper no longer hand-writes `CREATE TABLE`
for any of these families.

---

**Doc-store and stream methods moved under nested namespaces.** The flat
`gl.docX` and `gl.streamX` methods are gone; document and stream operations
now live under `gl.documents.<verb>` and `gl.streams.<verb>`. No
backwards-compat aliases — search and replace once.

Migration map (sync `GoldLapel`, `ReactiveGoldLapel`, and `RxJavaGoldLapel`):

| Old (flat)                                | New (nested)                                  |
| ----------------------------------------- | --------------------------------------------- |
| `gl.docInsert(name, doc)`                 | `gl.documents.insert(name, doc)`              |
| `gl.docInsertMany(name, docs)`            | `gl.documents.insertMany(name, docs)`         |
| `gl.docFind(name, filter, ...)`           | `gl.documents.find(name, filter, ...)`        |
| `gl.docFindOne(name, filter)`             | `gl.documents.findOne(name, filter)`          |
| `gl.docFindCursor(name, ...)`             | `gl.documents.findCursor(name, ...)`          |
| `gl.docUpdate(name, f, u)`                | `gl.documents.update(name, f, u)`             |
| `gl.docUpdateOne(name, f, u)`             | `gl.documents.updateOne(name, f, u)`          |
| `gl.docDelete(name, f)`                   | `gl.documents.delete(name, f)`                |
| `gl.docDeleteOne(name, f)`                | `gl.documents.deleteOne(name, f)`             |
| `gl.docFindOneAndUpdate(...)`             | `gl.documents.findOneAndUpdate(...)`          |
| `gl.docFindOneAndDelete(...)`             | `gl.documents.findOneAndDelete(...)`          |
| `gl.docDistinct(name, field, f)`          | `gl.documents.distinct(name, field, f)`       |
| `gl.docCount(name, filter)`               | `gl.documents.count(name, filter)`            |
| `gl.docCreateIndex(name, keys)`           | `gl.documents.createIndex(name, keys)`        |
| `gl.docAggregate(name, pipeline)`         | `gl.documents.aggregate(name, pipeline)`      |
| `gl.docWatch(name, cb)`                   | `gl.documents.watch(name, cb)`                |
| `gl.docUnwatch(name)`                     | `gl.documents.unwatch(name)`                  |
| `gl.docCreateTtlIndex(name, n[, field])`  | `gl.documents.createTtlIndex(name, n[, f])`   |
| `gl.docRemoveTtlIndex(name)`              | `gl.documents.removeTtlIndex(name)`           |
| `gl.docCreateCapped(name, max)`           | `gl.documents.createCapped(name, max)`        |
| `gl.docRemoveCap(name)`                   | `gl.documents.removeCap(name)`                |
| `gl.docCreateCollection(name[, unlogged])`| `gl.documents.createCollection(name[, unlogged])` |
| `gl.streamAdd(name, payload)`             | `gl.streams.add(name, payload)`               |
| `gl.streamCreateGroup(name, group)`       | `gl.streams.createGroup(name, group)`         |
| `gl.streamRead(name, g, c, count)`        | `gl.streams.read(name, g, c, count)`          |
| `gl.streamAck(name, group, id)`           | `gl.streams.ack(name, group, id)`             |
| `gl.streamClaim(name, g, c, idle)`        | `gl.streams.claim(name, g, c, idle)`          |

`documents` and `streams` are `public final` fields on `GoldLapel`,
`ReactiveGoldLapel`, and `RxJavaGoldLapel` — direct field access matches the
shape of the cross-wrapper consensus (Python, JS, Ruby, PHP, Go, .NET).

Other namespaces (`gl.search`, `gl.publish` / `gl.subscribe`, percolator) remain
flat and will migrate to nested form in subsequent releases (one namespace per
schema-to-core phase).

**Doc-store DDL is now owned by the proxy.** The wrapper no longer emits
`CREATE TABLE _goldlapel.doc_<name>` SQL when a collection is first used.
Instead, `gl.documents.<verb>` calls `POST /api/ddl/doc_store/create`
against the proxy's dashboard port; the proxy runs the canonical DDL on its
management connection and returns the table reference + query patterns. The
wrapper caches `(tables, query_patterns)` per session — one HTTP round-trip
per `(family, name)` per session.

Canonical doc-store schema (v1) standardizes the column shape across every
Gold Lapel wrapper:

```
_id        UUID PRIMARY KEY DEFAULT gen_random_uuid()
data       JSONB NOT NULL
created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
```

Both timestamps are `NOT NULL`. Any wrapper (Python, JS, Ruby, Java, PHP,
Go, .NET) writing to a doc-store collection now produces the same table.

**Upgrade path for dev databases:** wipe and recreate. There is no in-place
migration. Pre-1.0, dev databases get rebuilt freely.

```bash
goldlapel clean   # drops _goldlapel.* tables
# ...drop/recreate your DB if needed...
```

### Spring Boot

`goldlapel-spring-boot` auto-configuration is unchanged — the `GoldLapel`
bean exposed in the application context still drops in via DataSource
post-processing. Code that called the renamed methods through the bean
(e.g. `goldLapel.docInsert(...)`) needs the same search-and-replace as any
other caller.

**Multiple DataSources no longer collide on ports.** Each proxy listens on
two ports (proxy and dashboard, proxy + 1), but a second upstream was given
the next port up — the first proxy's dashboard. Each new upstream now gets
the smallest port at or above `goldlapel.proxy-port` whose proxy and
dashboard ports are both unclaimed, so two DataSources land on 7932 and 7934.
An explicit `goldlapel.dashboard-port` now applies to the first proxy only
(it used to be handed to every proxy, which could never all bind it); later
proxies derive proxy port + 1, and allocation skips the explicit dashboard
port. `goldlapel.dashboard-port: 0` disables the dashboard on every proxy,
so each claims only its proxy port and they sit on consecutive ports (7932,
7933). DataSources sharing an upstream now share one proxy — previously
each started its own process on the same port, which could not bind.

### Reactive start

**Cancelling a reactive start now stops the proxy right away.** Disposing the
`Mono` from `ReactiveGoldLapel.start` (or the `Single` from
`RxJavaGoldLapel.start`) while the proxy was still starting used to leave the
subprocess running until the blocking start finished on its own. The
instance is now handed over before the spawn, so cancellation kills it
immediately. The new `GoldLapel.start(upstream, configurator, onCreated)`
overload exposes the same hook: calling `stop()` on the instance passed to
`onCreated` aborts an in-flight start, which then throws.

**`start()` no longer waits indefinitely on a silent proxy.** The internal
JDBC connection now uses a 30-second `loginTimeout`; a `loginTimeout` in the
upstream URL's query still takes precedence. The PostgreSQL JDBC driver is
now 42.7.11.
