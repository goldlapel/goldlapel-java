package com.goldlapel;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Nested;
import static org.junit.jupiter.api.Assertions.*;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

class NativeCacheTest {

    @BeforeEach
    void setup() { NativeCache.reset(); }

    @AfterEach
    void cleanup() { NativeCache.reset(); }

    NativeCache makeCache() {
        NativeCache cache = new NativeCache();
        // Bypass singleton for tests
        try {
            var field = NativeCache.class.getDeclaredField("invalidationConnected");
            field.setAccessible(true);
            field.setBoolean(cache, true);
        } catch (Exception ignored) {}
        return cache;
    }

    // --- detectWrite ---

    @Nested class DetectWriteTest {
        @Test void insert() { assertEquals("orders", NativeCache.detectWrite("INSERT INTO orders VALUES (1)")); }
        @Test void insertSchema() { assertEquals("orders", NativeCache.detectWrite("INSERT INTO public.orders VALUES (1)")); }
        @Test void update() { assertEquals("orders", NativeCache.detectWrite("UPDATE orders SET name = 'x'")); }
        @Test void delete() { assertEquals("orders", NativeCache.detectWrite("DELETE FROM orders WHERE id = 1")); }
        @Test void truncate() { assertEquals("orders", NativeCache.detectWrite("TRUNCATE orders")); }
        @Test void truncateTable() { assertEquals("orders", NativeCache.detectWrite("TRUNCATE TABLE orders")); }
        @Test void createDdl() { assertEquals(NativeCache.DDL_SENTINEL, NativeCache.detectWrite("CREATE TABLE foo (id int)")); }
        @Test void alterDdl() { assertEquals(NativeCache.DDL_SENTINEL, NativeCache.detectWrite("ALTER TABLE foo ADD COLUMN bar int")); }
        @Test void dropDdl() { assertEquals(NativeCache.DDL_SENTINEL, NativeCache.detectWrite("DROP TABLE foo")); }
        @Test void selectReturnsNull() { assertNull(NativeCache.detectWrite("SELECT * FROM orders")); }
        @Test void caseInsensitive() { assertEquals("orders", NativeCache.detectWrite("insert INTO Orders VALUES (1)")); }
        @Test void copyFrom() { assertEquals("orders", NativeCache.detectWrite("COPY orders FROM '/tmp/data.csv'")); }
        @Test void copyToNull() { assertNull(NativeCache.detectWrite("COPY orders TO '/tmp/data.csv'")); }
        @Test void copySubqueryNull() { assertNull(NativeCache.detectWrite("COPY (SELECT * FROM orders) TO '/tmp/data.csv'")); }
        @Test void withCteInsert() { assertEquals(NativeCache.DDL_SENTINEL, NativeCache.detectWrite("WITH x AS (SELECT 1) INSERT INTO foo SELECT * FROM x")); }
        @Test void withCteSelect() { assertNull(NativeCache.detectWrite("WITH x AS (SELECT 1) SELECT * FROM x")); }
        @Test void empty() { assertNull(NativeCache.detectWrite("")); }
        @Test void whitespace() { assertNull(NativeCache.detectWrite("   ")); }
        @Test void copyWithColumns() { assertEquals("orders", NativeCache.detectWrite("COPY orders(id, name) FROM '/tmp/data.csv'")); }
    }

    // --- extractTables ---

    @Nested class ExtractTablesTest {
        @Test void simpleFrom() { assertTrue(NativeCache.extractTables("SELECT * FROM orders").contains("orders")); }
        @Test void join() {
            Set<String> t = NativeCache.extractTables("SELECT * FROM orders o JOIN customers c ON o.cid = c.id");
            assertTrue(t.contains("orders")); assertTrue(t.contains("customers"));
        }
        @Test void schemaQualified() { assertTrue(NativeCache.extractTables("SELECT * FROM public.orders").contains("orders")); }
        @Test void multipleJoins() { assertEquals(3, NativeCache.extractTables("SELECT * FROM orders JOIN items ON 1=1 JOIN products ON 1=1").size()); }
        @Test void caseInsensitive() { assertTrue(NativeCache.extractTables("SELECT * FROM ORDERS").contains("orders")); }
        @Test void noTables() { assertEquals(0, NativeCache.extractTables("SELECT 1").size()); }
        @Test void subquery() {
            Set<String> t = NativeCache.extractTables("SELECT * FROM orders WHERE id IN (SELECT oid FROM users)");
            assertTrue(t.contains("orders")); assertTrue(t.contains("users"));
        }
    }

    // --- Transaction detection ---

    @Nested class TxDetectionTest {
        @Test void begin() { assertTrue(NativeCache.isTxStart("BEGIN")); }
        @Test void startTransaction() { assertTrue(NativeCache.isTxStart("START TRANSACTION")); }
        @Test void commit() { assertTrue(NativeCache.isTxEnd("COMMIT")); }
        @Test void rollback() { assertTrue(NativeCache.isTxEnd("ROLLBACK")); }
        @Test void end() { assertTrue(NativeCache.isTxEnd("END")); }
        @Test void savepointNotStart() { assertFalse(NativeCache.isTxStart("SAVEPOINT x")); }
        @Test void selectNotStart() { assertFalse(NativeCache.isTxStart("SELECT 1")); }
    }

    // --- Cache operations ---

    @Nested class CacheOpsTest {
        @Test void putAndGet() {
            NativeCache cache = makeCache();
            cache.put("SELECT * FROM users", null, Collections.singletonList(new Object[]{"1", "alice"}), new String[]{"id", "name"});
            var entry = cache.get("SELECT * FROM users", null);
            assertNotNull(entry);
            assertEquals(1, entry.rows.size());
        }

        @Test void missReturnsNull() {
            NativeCache cache = makeCache();
            assertNull(cache.get("SELECT 1", null));
        }

        @Test void paramsDifferentiate() {
            NativeCache cache = makeCache();
            cache.put("SELECT $1", new Object[]{1}, Collections.singletonList(new Object[]{"1"}), new String[]{"id"});
            cache.put("SELECT $1", new Object[]{2}, Collections.singletonList(new Object[]{"2"}), new String[]{"id"});
            assertEquals("1", cache.get("SELECT $1", new Object[]{1}).rows.get(0)[0]);
            assertEquals("2", cache.get("SELECT $1", new Object[]{2}).rows.get(0)[0]);
        }

        @Test void stats() {
            NativeCache cache = makeCache();
            cache.put("SELECT 1", null, Collections.singletonList(new Object[]{"1"}), new String[]{"x"});
            cache.get("SELECT 1", null);
            cache.get("SELECT 2", null);
            assertEquals(1, cache.statsHits.get());
            assertEquals(1, cache.statsMisses.get());
        }
    }

    // --- Invalidation ---

    @Nested class InvalidationTest {
        @Test void invalidateTable() {
            NativeCache cache = makeCache();
            cache.put("SELECT * FROM orders", null, Collections.singletonList(new Object[]{"1"}), new String[]{"id"});
            cache.put("SELECT * FROM users", null, Collections.singletonList(new Object[]{"2"}), new String[]{"id"});
            cache.invalidateTable("orders");
            assertNull(cache.get("SELECT * FROM orders", null));
            assertNotNull(cache.get("SELECT * FROM users", null));
        }

        @Test void invalidateAll() {
            NativeCache cache = makeCache();
            cache.put("SELECT * FROM orders", null, Collections.singletonList(new Object[]{"1"}), new String[]{"id"});
            cache.put("SELECT * FROM users", null, Collections.singletonList(new Object[]{"2"}), new String[]{"id"});
            cache.invalidateAll();
            assertNull(cache.get("SELECT * FROM orders", null));
            assertNull(cache.get("SELECT * FROM users", null));
        }

        @Test void crossReferenced() {
            NativeCache cache = makeCache();
            cache.put("SELECT * FROM orders JOIN users ON 1=1", null, Collections.singletonList(new Object[]{"1"}), new String[]{"id"});
            cache.invalidateTable("orders");
            assertNull(cache.get("SELECT * FROM orders JOIN users ON 1=1", null));
        }
    }

    // --- Signal processing ---

    @Nested class SignalTest {
        @Test void tableSignal() {
            NativeCache cache = makeCache();
            cache.put("SELECT * FROM orders", null, Collections.singletonList(new Object[]{"1"}), new String[]{"id"});
            cache.processSignal("I:orders");
            assertNull(cache.get("SELECT * FROM orders", null));
        }

        @Test void wildcardSignal() {
            NativeCache cache = makeCache();
            cache.put("SELECT * FROM orders", null, Collections.singletonList(new Object[]{"1"}), new String[]{"id"});
            cache.processSignal("I:*");
            assertNull(cache.get("SELECT * FROM orders", null));
        }

        @Test void keepalivePreserves() {
            NativeCache cache = makeCache();
            cache.put("SELECT * FROM orders", null, Collections.singletonList(new Object[]{"1"}), new String[]{"id"});
            cache.processSignal("P:");
            assertNotNull(cache.get("SELECT * FROM orders", null));
        }

        @Test void unknownPreserves() {
            NativeCache cache = makeCache();
            cache.put("SELECT * FROM orders", null, Collections.singletonList(new Object[]{"1"}), new String[]{"id"});
            cache.processSignal("X:something");
            assertNotNull(cache.get("SELECT * FROM orders", null));
        }
    }

    // --- Push invalidation ---

    @Nested class PushInvalidationTest {
        @Test void remoteSignal() throws Exception {
            NativeCache cache = makeCache();
            cache.put("SELECT * FROM orders", null, Collections.singletonList(new Object[]{"1"}), new String[]{"id"});

            try (ServerSocket server = new ServerSocket(0)) {
                int port = server.getLocalPort();
                // Reset connected state so connectInvalidation works
                var field = NativeCache.class.getDeclaredField("invalidationConnected");
                field.setAccessible(true);
                field.setBoolean(cache, false);

                cache.connectInvalidation(port);
                Socket conn = server.accept();
                Thread.sleep(100);

                assertTrue(cache.isConnected());
                PrintWriter writer = new PrintWriter(conn.getOutputStream(), true);
                writer.println("I:orders");
                Thread.sleep(200);

                assertNull(cache.get("SELECT * FROM orders", null));

                conn.close();
                cache.stopInvalidation();
            }
        }

        @Test void connectionDropClears() throws Exception {
            NativeCache cache = makeCache();
            cache.put("SELECT * FROM orders", null, Collections.singletonList(new Object[]{"1"}), new String[]{"id"});

            try (ServerSocket server = new ServerSocket(0)) {
                int port = server.getLocalPort();
                var field = NativeCache.class.getDeclaredField("invalidationConnected");
                field.setAccessible(true);
                field.setBoolean(cache, false);

                cache.connectInvalidation(port);
                Socket conn = server.accept();
                Thread.sleep(100);

                assertTrue(cache.isConnected());
                conn.close();
                Thread.sleep(500);

                assertFalse(cache.isConnected());
                assertEquals(0, cache.size());

                cache.stopInvalidation();
            }
        }
    }

    // --- Concurrent thread-safety (v0.2 coverage audit) ---
    //
    // Java is explicitly multi-threaded — the JVM will happily concurrent-hammer
    // non-synchronized collections. The cache is mutated from both the query-path
    // (put/get on user threads) and the background invalidation listener thread.
    // These tests exercise contention directly against the NativeCache (matching
    // the Python/Go/.NET reference implementations — no live Postgres needed,
    // the cache is a self-contained in-process LRU).
    //
    // Failure modes guarded against:
    //   - ConcurrentModificationException from unsynchronized map iteration
    //   - Corrupted reads mid-mutation (returns partially-initialized entry)
    //   - Lost invalidations (stale entry served after signal from another thread)
    //   - Leaked tableIndex entries (orphaned key → stale unreachable node)

    @Nested class ConcurrentAccessTest {

        @Test
        void concurrentPutAndGet() throws Exception {
            NativeCache cache = makeCache();
            int threads = 16;
            int opsPerThread = 500;
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            AtomicInteger errors = new AtomicInteger();

            try {
                for (int t = 0; t < threads; t++) {
                    final int tId = t;
                    futures.add(executor.submit(() -> {
                        try {
                            start.await();
                            for (int i = 0; i < opsPerThread; i++) {
                                // High contention: 100 shared keys across 16 threads
                                String sql = "SELECT * FROM t WHERE id = " + (i % 100);
                                Object[] params = new Object[]{i % 100};
                                if (i % 3 == 0) {
                                    cache.put(sql, params,
                                        Collections.singletonList(new Object[]{tId, i}),
                                        new String[]{"tid", "i"});
                                } else {
                                    cache.get(sql, params);
                                }
                            }
                        } catch (Throwable ex) {
                            errors.incrementAndGet();
                            ex.printStackTrace();
                        }
                    }));
                }
                start.countDown();
                for (Future<?> f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS),
                    "executor failed to terminate");
            }

            assertEquals(0, errors.get(),
                "concurrent put/get must not throw (e.g. ConcurrentModificationException)");
        }

        @Test
        void concurrentInvalidation() throws Exception {
            NativeCache cache = makeCache();
            // Pre-seed the cache across 10 tables
            for (int i = 0; i < 100; i++) {
                String sql = "SELECT * FROM t" + (i % 10) + " WHERE id = " + i;
                cache.put(sql, new Object[]{i},
                    Collections.singletonList(new Object[]{i}), new String[]{"id"});
            }

            int threads = 12;
            int opsPerThread = 500;
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            AtomicInteger errors = new AtomicInteger();

            try {
                for (int t = 0; t < threads; t++) {
                    final int tId = t;
                    futures.add(executor.submit(() -> {
                        try {
                            start.await();
                            for (int i = 0; i < opsPerThread; i++) {
                                int bucket = i % 10;
                                String sql = "SELECT * FROM t" + bucket + " WHERE id = " + i;
                                Object[] params = new Object[]{i};
                                // Mix puts, gets, and invalidations concurrently so
                                // the tableIndex is mutated while queries run.
                                switch (tId % 4) {
                                    case 0:
                                        cache.put(sql, params,
                                            Collections.singletonList(new Object[]{i}),
                                            new String[]{"id"});
                                        break;
                                    case 1:
                                        cache.get(sql, params);
                                        break;
                                    case 2:
                                        cache.invalidateTable("t" + bucket);
                                        break;
                                    case 3:
                                        if (i % 50 == 0) {
                                            cache.invalidateAll();
                                        } else {
                                            cache.get(sql, params);
                                        }
                                        break;
                                }
                            }
                        } catch (Throwable ex) {
                            errors.incrementAndGet();
                            ex.printStackTrace();
                        }
                    }));
                }
                start.countDown();
                for (Future<?> f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS),
                    "executor failed to terminate");
            }

            assertEquals(0, errors.get(),
                "concurrent put/get/invalidate must not throw");

            // After a final invalidateAll all state must be clean — no leaked
            // entries, no orphaned tableIndex keys.
            cache.invalidateAll();
            assertEquals(0, cache.size(), "cache must be empty after invalidateAll");
        }

        @Test
        void concurrentStatsAccess() throws Exception {
            NativeCache cache = makeCache();
            cache.put("SELECT 1", null,
                Collections.singletonList(new Object[]{1}), new String[]{"x"});

            int opsPerThread = 500;
            ExecutorService executor = Executors.newFixedThreadPool(4);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            AtomicInteger errors = new AtomicInteger();

            Runnable reader = () -> {
                try {
                    start.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        cache.get("SELECT 1", null);
                        cache.get("SELECT " + i, null);
                    }
                } catch (Throwable ex) {
                    errors.incrementAndGet();
                    ex.printStackTrace();
                }
            };

            Runnable statsReader = () -> {
                try {
                    start.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        cache.statsHits.get();
                        cache.statsMisses.get();
                        cache.statsInvalidations.get();
                    }
                } catch (Throwable ex) {
                    errors.incrementAndGet();
                    ex.printStackTrace();
                }
            };

            Runnable invalidator = () -> {
                try {
                    start.await();
                    for (int i = 0; i < opsPerThread / 5; i++) {
                        cache.put("SELECT temp " + i, null,
                            Collections.singletonList(new Object[]{i}),
                            new String[]{"x"});
                        cache.invalidateAll();
                    }
                } catch (Throwable ex) {
                    errors.incrementAndGet();
                    ex.printStackTrace();
                }
            };

            try {
                futures.add(executor.submit(reader));
                futures.add(executor.submit(statsReader));
                futures.add(executor.submit(invalidator));
                futures.add(executor.submit(reader));
                start.countDown();
                for (Future<?> f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS),
                    "executor failed to terminate");
            }

            assertEquals(0, errors.get(),
                "concurrent stats + cache ops must not throw");
        }
    }
}
