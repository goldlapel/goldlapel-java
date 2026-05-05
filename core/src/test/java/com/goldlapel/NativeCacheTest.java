package com.goldlapel;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Nested;
import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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

        // --- SELECT-INTO false positives on string literals
        // (wrapper-detect-write-string-literal-false-positive.md, 2026-05-04)
        // detectWrite's SELECT branch tokenizes by whitespace; a bare `INTO`
        // sitting inside a `'...'` literal or `"..."` identifier was
        // misclassified as SELECT-INTO DDL. The literal-stripping pass on
        // re-tokenization must keep these as plain reads.

        @Test void selectIntoInsideSingleQuoteLiteralIsRead() {
            // Real-world shape: `SELECT 'INSERT INTO orders' FROM audit_log`
            // — `INTO` is inside the literal, not a DDL clause.
            assertNull(NativeCache.detectWrite("SELECT 'INSERT INTO orders' FROM audit_log"));
        }

        @Test void selectIntoInsideDoubleQuotedIdentifierIsRead() {
            // `"into_table"` is a quoted identifier, not a SELECT-INTO target.
            assertNull(NativeCache.detectWrite("SELECT * FROM \"into_table\""));
        }

        @Test void selectLikePatternContainingIntoIsRead() {
            // `WHERE message LIKE '%INTO%'` — `INTO` lives inside the
            // pattern literal.
            assertNull(NativeCache.detectWrite("SELECT message FROM logs WHERE message LIKE '%INTO%'"));
        }

        @Test void selectDoubledQuoteEscapeContainingIntoIsRead() {
            // PG's doubled-single-quote escape: `'it''s INTO time'`. The
            // doubled `''` stays inside the literal — `INTO` must not leak.
            assertNull(NativeCache.detectWrite("SELECT 'it''s INTO time' FROM events"));
        }

        @Test void selectIntoOutsideLiteralStillDetected() {
            // Regression guard: real `SELECT ... INTO new_table FROM ...`
            // (the actual DDL form) must still classify as DDL.
            assertEquals(NativeCache.DDL_SENTINEL,
                NativeCache.detectWrite("SELECT a, b INTO new_table FROM source"));
        }
    }

    // --- stripStringLiterals — literal-aware tokenizer primitive ---

    @Nested class StripStringLiteralsTest {
        @Test void singleQuoteLiteralReplaced() {
            // "hello world" is 11 chars — body is blanked, delimiters kept.
            assertEquals("SELECT '           ' FROM t",
                NativeCache.stripStringLiterals("SELECT 'hello world' FROM t"));
        }

        @Test void doubleQuotedIdentifierReplaced() {
            // "into_table" is 10 chars — body is blanked, delimiters kept.
            assertEquals("SELECT * FROM \"          \"",
                NativeCache.stripStringLiterals("SELECT * FROM \"into_table\""));
        }

        @Test void doubledSingleQuoteEscapeStaysInside() {
            // `'it''s'` is one literal; result preserves length and only the
            // outer quotes stay as quote chars.
            String in = "'it''s INTO'";
            String out = NativeCache.stripStringLiterals(in);
            assertEquals(in.length(), out.length());
            assertEquals('\'', out.charAt(0));
            assertEquals('\'', out.charAt(out.length() - 1));
            // Body fully blanked (including the doubled-quote pair).
            for (int i = 1; i < out.length() - 1; i++) {
                assertEquals(' ', out.charAt(i), "char " + i + " should be blanked");
            }
        }

        @Test void preservesLength() {
            String in = "SELECT 'a INTO b' FROM \"qq\" WHERE x='c'";
            String out = NativeCache.stripStringLiterals(in);
            assertEquals(in.length(), out.length());
        }

        @Test void nullAndEmpty() {
            assertNull(NativeCache.stripStringLiterals(null));
            assertEquals("", NativeCache.stripStringLiterals(""));
        }
    }

    // --- detectWritesMulti — multi-statement Q-message bodies ---

    @Nested class DetectWritesMultiTest {
        @Test void singleSelectReturnsNull() {
            assertNull(NativeCache.detectWritesMulti("SELECT * FROM orders"));
        }

        @Test void singleInsertWraps() {
            NativeCache.WriteSummary w = NativeCache.detectWritesMulti("INSERT INTO orders VALUES (1)");
            assertNotNull(w);
            assertFalse(w.ddl);
            assertEquals(Set.of("orders"), w.tables);
        }

        @Test void singleDdlWraps() {
            NativeCache.WriteSummary w = NativeCache.detectWritesMulti("DROP TABLE foo");
            assertNotNull(w);
            assertTrue(w.ddl);
        }

        @Test void setThenInsertDetectsInsert() {
            // The headline bug: SET first, INSERT second — single-token detectWrite
            // sees only SET and returns null, leaking the INSERT's invalidation.
            NativeCache.WriteSummary w = NativeCache.detectWritesMulti(
                "SET app.user_id = '42'; INSERT INTO orders VALUES (1)");
            assertNotNull(w, "multi-statement INSERT after SET should be detected");
            assertFalse(w.ddl);
            assertEquals(Set.of("orders"), w.tables);
        }

        @Test void multipleWritesUnion() {
            NativeCache.WriteSummary w = NativeCache.detectWritesMulti(
                "INSERT INTO orders VALUES (1); UPDATE users SET name = 'x'");
            assertNotNull(w);
            assertFalse(w.ddl);
            assertEquals(Set.of("orders", "users"), w.tables);
        }

        @Test void ddlShortCircuits() {
            // DDL anywhere in the body collapses to a full-cache invalidation.
            NativeCache.WriteSummary w = NativeCache.detectWritesMulti(
                "INSERT INTO orders VALUES (1); DROP TABLE users");
            assertNotNull(w);
            assertTrue(w.ddl);
        }

        @Test void beginInsertCommitDetectsWrite() {
            NativeCache.WriteSummary w = NativeCache.detectWritesMulti(
                "BEGIN; INSERT INTO orders VALUES (1); COMMIT");
            assertNotNull(w);
            assertFalse(w.ddl);
            assertEquals(Set.of("orders"), w.tables);
        }

        @Test void allReadsReturnsNull() {
            assertNull(NativeCache.detectWritesMulti("SET app.x='1'; SELECT * FROM orders; RESET ALL"));
        }

        @Test void semicolonInsideStringLiteral() {
            // The splitter is string-literal-aware — ';' inside '...' must not
            // split. Single statement, single write.
            NativeCache.WriteSummary w = NativeCache.detectWritesMulti(
                "INSERT INTO orders VALUES ('a;b')");
            assertNotNull(w);
            assertFalse(w.ddl);
            assertEquals(Set.of("orders"), w.tables);
        }

        @Test void emptyAndNullSafe() {
            assertNull(NativeCache.detectWritesMulti(""));
            assertNull(NativeCache.detectWritesMulti(null));
            assertNull(NativeCache.detectWritesMulti(";;;"));
        }
    }

    // --- isSessionStateCommand — cache.put skip-list ---

    @Nested class IsSessionStateCommandTest {
        @Test void setCommand() { assertTrue(NativeCache.isSessionStateCommand("SET foo = 'bar'")); }
        @Test void setLocal() { assertTrue(NativeCache.isSessionStateCommand("SET LOCAL foo = 'bar'")); }
        @Test void setSession() { assertTrue(NativeCache.isSessionStateCommand("SET SESSION foo = 'bar'")); }
        @Test void resetCommand() { assertTrue(NativeCache.isSessionStateCommand("RESET foo")); }
        @Test void resetAll() { assertTrue(NativeCache.isSessionStateCommand("RESET ALL")); }
        @Test void listenCommand() { assertTrue(NativeCache.isSessionStateCommand("LISTEN channel_x")); }
        @Test void unlistenCommand() { assertTrue(NativeCache.isSessionStateCommand("UNLISTEN channel_x")); }
        @Test void notifyCommand() { assertTrue(NativeCache.isSessionStateCommand("NOTIFY channel_x")); }
        @Test void beginCommand() { assertTrue(NativeCache.isSessionStateCommand("BEGIN")); }
        @Test void commitCommand() { assertTrue(NativeCache.isSessionStateCommand("COMMIT")); }
        @Test void rollbackCommand() { assertTrue(NativeCache.isSessionStateCommand("ROLLBACK")); }
        @Test void savepointCommand() { assertTrue(NativeCache.isSessionStateCommand("SAVEPOINT sp1")); }

        @Test void selectIsNotSessionState() {
            assertFalse(NativeCache.isSessionStateCommand("SELECT * FROM orders"));
        }
        @Test void insertIsNotSessionState() {
            assertFalse(NativeCache.isSessionStateCommand("INSERT INTO orders VALUES (1)"));
        }

        @Test void caseInsensitive() {
            assertTrue(NativeCache.isSessionStateCommand("set foo = 'bar'"));
            assertTrue(NativeCache.isSessionStateCommand("Begin"));
        }

        @Test void leadingWhitespace() {
            assertTrue(NativeCache.isSessionStateCommand("   SET foo = 'bar'"));
        }

        @Test void emptyAndNullSafe() {
            assertFalse(NativeCache.isSessionStateCommand(""));
            assertFalse(NativeCache.isSessionStateCommand("   "));
            assertFalse(NativeCache.isSessionStateCommand(null));
        }
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

    // --- updateTxState — multi-statement-aware tx-flag bookkeeping
    // (java tx-flag bookkeeping fix, 2026-05-04)
    // The single-token isTxStart/isTxEnd only see the first segment of a
    // multi-statement Q body — "BEGIN; INSERT...; COMMIT" used to flip the
    // wrapper into "in tx" and never come back, pinning every subsequent
    // read into cache-bypass mode. updateTxState walks every segment so the
    // resulting flag matches the server's post-execution state.

    @Nested class UpdateTxStateTest {
        @Test void singleBeginEntersTx() {
            assertTrue(NativeCache.updateTxState(false, "BEGIN"));
        }

        @Test void singleCommitExitsTx() {
            assertFalse(NativeCache.updateTxState(true, "COMMIT"));
        }

        @Test void beginInsertCommitEndsOutOfTx() {
            // The headline regression: BEGIN-then-COMMIT in the same Q body
            // must leave the wrapper out-of-tx so subsequent reads can hit
            // the cache.
            assertFalse(NativeCache.updateTxState(false,
                "BEGIN; INSERT INTO orders VALUES (1); COMMIT"));
        }

        @Test void beginInsertWithoutCommitStaysInTx() {
            // BEGIN without a trailing COMMIT/ROLLBACK leaves the wrapper
            // in-tx so writes inside the open tx still bypass the cache.
            assertTrue(NativeCache.updateTxState(false,
                "BEGIN; INSERT INTO orders VALUES (1)"));
        }

        @Test void rollbackEndsOutOfTx() {
            assertFalse(NativeCache.updateTxState(true,
                "BEGIN; UPDATE orders SET x = 1; ROLLBACK"));
        }

        @Test void savepointEntersTx() {
            // SAVEPOINT in autocommit implicitly opens a tx (PG semantics).
            assertTrue(NativeCache.updateTxState(false, "SAVEPOINT sp1"));
        }

        @Test void releaseExitsTx() {
            assertFalse(NativeCache.updateTxState(true, "RELEASE sp1"));
        }

        @Test void endTreatedAsCommit() {
            // END is a synonym for COMMIT in PG.
            assertFalse(NativeCache.updateTxState(true, "END"));
        }

        @Test void startTransactionEntersTx() {
            assertTrue(NativeCache.updateTxState(false, "START TRANSACTION"));
        }

        @Test void plainSelectKeepsState() {
            // No tx verb anywhere — flag carries through unchanged.
            assertFalse(NativeCache.updateTxState(false, "SELECT 1"));
            assertTrue(NativeCache.updateTxState(true, "SELECT 1"));
        }

        @Test void lastTxVerbWins() {
            // Two flips in one body — the trailing COMMIT must win even
            // though a SAVEPOINT lives between them.
            assertFalse(NativeCache.updateTxState(false,
                "BEGIN; SAVEPOINT sp1; SELECT 1; RELEASE sp1; COMMIT"));
        }

        @Test void caseInsensitive() {
            assertFalse(NativeCache.updateTxState(true, "begin; commit"));
            assertTrue(NativeCache.updateTxState(false, "Begin"));
        }

        @Test void emptyAndNullSafe() {
            assertFalse(NativeCache.updateTxState(false, ""));
            assertTrue(NativeCache.updateTxState(true, ""));
            assertFalse(NativeCache.updateTxState(false, null));
            assertTrue(NativeCache.updateTxState(true, null));
        }
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

    // --- native-cache telemetry: counters + snapshot shape ---

    @Nested class EvictionsCounterTest {
        @Test void startsZero() {
            NativeCache cache = makeCache();
            assertEquals(0, cache.statsEvictions.get());
        }

        @Test void bumpsOnOverflow() throws Exception {
            // Force capacity = 4 by setting env before construction. The cache
            // reads GOLDLAPEL_NATIVE_CACHE_SIZE in the constructor, so we
            // can't change it after — use reflection to override the field.
            NativeCache cache = makeCacheWithCapacity(4);
            for (int i = 0; i < 8; i++) {
                cache.put("SELECT " + i, null,
                    Collections.singletonList(new Object[]{i}), new String[]{"x"});
            }
            // 8 puts, capacity 4 -> 4 evictions.
            assertEquals(4, cache.statsEvictions.get());
        }

        @Test void noBumpWithinCapacity() throws Exception {
            NativeCache cache = makeCacheWithCapacity(8);
            for (int i = 0; i < 4; i++) {
                cache.put("SELECT " + i, null,
                    Collections.singletonList(new Object[]{i}), new String[]{"x"});
            }
            assertEquals(0, cache.statsEvictions.get());
        }
    }

    @Nested class SnapshotShapeTest {
        @Test void carriesRequiredFields() {
            NativeCache cache = makeCache();
            cache.put("SELECT 1", null,
                Collections.singletonList(new Object[]{1}), new String[]{"x"});
            cache.get("SELECT 1", null);
            cache.get("SELECT MISS", null);
            Map<String, Object> snap = cache.buildSnapshot();
            assertEquals(cache.getWrapperId(), snap.get("wrapper_id"));
            assertEquals("java", snap.get("lang"));
            assertNotNull(snap.get("version"));
            assertEquals(1L, snap.get("hits"));
            assertEquals(1L, snap.get("misses"));
            assertEquals(0L, snap.get("evictions"));
            assertEquals(0L, snap.get("invalidations"));
            assertEquals(1L, snap.get("current_size_entries"));
            assertNotNull(snap.get("capacity_entries"));
        }

        @Test void wrapperIdIsUuid() {
            NativeCache cache = makeCache();
            // Throws IllegalArgumentException if not a valid UUID.
            UUID parsed = UUID.fromString(cache.getWrapperId());
            // UUID4 has version bits = 0b0100 (high nibble of byte 6).
            assertEquals(4, parsed.version());
        }

        @Test void wrapperIdStableAcrossCalls() {
            NativeCache cache = makeCache();
            String a = (String) cache.buildSnapshot().get("wrapper_id");
            String b = (String) cache.buildSnapshot().get("wrapper_id");
            assertEquals(a, b);
        }
    }

    @Nested class JsonSerializerTest {
        @Test void emitsCompactObject() {
            // Uses LinkedHashMap so iteration order matches insertion — the
            // serializer never reorders.
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("a", 1L);
            m.put("b", "x");
            m.put("c", true);
            assertEquals("{\"a\":1,\"b\":\"x\",\"c\":true}", NativeCache.jsonObject(m));
        }

        @Test void escapesQuotesAndBackslashes() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("k", "a\"b\\c");
            assertEquals("{\"k\":\"a\\\"b\\\\c\"}", NativeCache.jsonObject(m));
        }

        @Test void escapesControlChars() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("k", "x\ny\tz");
            assertEquals("{\"k\":\"x\\ny\\tz\"}", NativeCache.jsonObject(m));
        }

        @Test void serializesNullsAndNumbers() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("n", null);
            m.put("i", 42L);
            m.put("d", 1.5);
            assertEquals("{\"n\":null,\"i\":42,\"d\":1.5}", NativeCache.jsonObject(m));
        }
    }

    // --- native-cache telemetry: state-change emission via send override (unit) ---

    @Nested class StateChangeUnitTest {
        @Test void evictionRateFiresCacheFull() throws Exception {
            // Capacity 4 — every put past the 4th evicts. Window = 200 puts.
            NativeCache cache = makeCacheWithCapacity(4);
            List<String> emissions = Collections.synchronizedList(new ArrayList<>());
            cache.setSendOverride(emissions::add);
            // Need to fill the window before any state-change can fire.
            for (int i = 0; i < NativeCache.EVICT_RATE_WINDOW + 10; i++) {
                cache.put("SELECT " + i, null,
                    Collections.singletonList(new Object[]{i}), new String[]{"x"});
            }
            boolean any = emissions.stream().anyMatch(l -> l.contains("cache_full"));
            assertTrue(any, "expected at least one cache_full emission, got " + emissions);
        }

        @Test void noFireBelowWindow() throws Exception {
            // Fewer puts than the window -> no state-change fires (warmup gate).
            NativeCache cache = makeCacheWithCapacity(2);
            List<String> emissions = Collections.synchronizedList(new ArrayList<>());
            cache.setSendOverride(emissions::add);
            for (int i = 0; i < NativeCache.EVICT_RATE_WINDOW - 1; i++) {
                cache.put("SELECT " + i, null,
                    Collections.singletonList(new Object[]{i}), new String[]{"x"});
            }
            boolean any = emissions.stream().anyMatch(l -> l.contains("cache_full"));
            assertFalse(any, "no cache_full expected before window fills, got " + emissions);
        }

        @Test void requestSnapshotEmitsResponse() {
            NativeCache cache = makeCache();
            List<String> emissions = Collections.synchronizedList(new ArrayList<>());
            cache.setSendOverride(emissions::add);
            cache.processRequest("snapshot");
            List<String> rLines = filter(emissions, "R:");
            assertEquals(1, rLines.size(), emissions.toString());
            assertTrue(rLines.get(0).contains("\"wrapper_id\":\"" + cache.getWrapperId() + "\""));
        }

        @Test void requestEmptyBodyTreatedAsSnapshot() {
            NativeCache cache = makeCache();
            List<String> emissions = Collections.synchronizedList(new ArrayList<>());
            cache.setSendOverride(emissions::add);
            cache.processRequest("");
            assertEquals(1, filter(emissions, "R:").size());
        }

        @Test void requestUnknownBodyDropped() {
            NativeCache cache = makeCache();
            List<String> emissions = Collections.synchronizedList(new ArrayList<>());
            cache.setSendOverride(emissions::add);
            cache.processRequest("future_request_type");
            assertEquals(0, filter(emissions, "R:").size());
        }

        @Test void unknownProxyPrefixSilentlyIgnored() {
            // Backwards-compat: future proxy could send unknown prefixes; the
            // wrapper must not crash.
            NativeCache cache = makeCache();
            cache.processSignal("Z:future-prefix");
            cache.processSignal("$:bogus");
            // No assertion needed — the test passes if no exception is raised.
        }
    }

    // --- native-cache telemetry: protocol shape via real socket (integration) ---

    @Nested class StateChangeIntegrationTest {
        @Test void wrapperConnectedEmittedOnSocketConnect() throws Exception {
            NativeCache cache = makeCache();
            try (ServerSocket server = new ServerSocket(0)) {
                int port = server.getLocalPort();
                resetConnectedFlag(cache);
                cache.connectInvalidation(port);
                Socket conn = server.accept();
                List<String> lines = Collections.synchronizedList(new ArrayList<>());
                Thread reader = startReader(conn, lines);
                try {
                    waitFor(() -> lines.stream().anyMatch(l -> l.startsWith("S:")), 2000);
                    List<String> sLines = filter(lines, "S:");
                    assertFalse(sLines.isEmpty(), "expected S: line, got " + lines);
                    String body = sLines.get(0).substring(2);
                    assertTrue(body.contains("\"state\":\"wrapper_connected\""), body);
                    assertTrue(body.contains("\"lang\":\"java\""), body);
                    assertTrue(body.contains("\"wrapper_id\":\"" + cache.getWrapperId() + "\""), body);
                } finally {
                    conn.close();
                    reader.interrupt();
                    cache.stopInvalidation();
                }
            }
        }

        @Test void snapshotRequestReturnsResponse() throws Exception {
            NativeCache cache = makeCache();
            cache.put("SELECT 1", null,
                Collections.singletonList(new Object[]{1}), new String[]{"x"});
            cache.get("SELECT 1", null);
            try (ServerSocket server = new ServerSocket(0)) {
                int port = server.getLocalPort();
                resetConnectedFlag(cache);
                cache.connectInvalidation(port);
                Socket conn = server.accept();
                List<String> lines = Collections.synchronizedList(new ArrayList<>());
                Thread reader = startReader(conn, lines);
                try {
                    // Wait for wrapper_connected so we know the socket is wired.
                    waitFor(() -> lines.stream().anyMatch(l -> l.startsWith("S:")), 2000);
                    PrintWriter w = new PrintWriter(conn.getOutputStream(), true);
                    w.println("?:snapshot");
                    waitFor(() -> lines.stream().anyMatch(l -> l.startsWith("R:")), 2000);
                    List<String> rLines = filter(lines, "R:");
                    assertFalse(rLines.isEmpty(), "expected R: line, got " + lines);
                    String body = rLines.get(0).substring(2);
                    assertTrue(body.contains("\"wrapper_id\":\"" + cache.getWrapperId() + "\""), body);
                    assertTrue(body.contains("\"hits\":1"), body);
                    assertTrue(body.contains("\"current_size_entries\":1"), body);
                } finally {
                    conn.close();
                    reader.interrupt();
                    cache.stopInvalidation();
                }
            }
        }

        @Test void reportStatsDisabledSuppressesEmissions() throws Exception {
            // Construct an instance under env-var override. The cache reads
            // GOLDLAPEL_REPORT_STATS in the constructor, so we set the env var
            // by reflection on ProcessEnvironment — JVMs don't expose a setenv
            // on the public API, so we override the field directly instead.
            NativeCache cache = makeCache();
            cache.setReportStats(false);
            assertFalse(cache.isReportStats());
            try (ServerSocket server = new ServerSocket(0)) {
                int port = server.getLocalPort();
                resetConnectedFlag(cache);
                cache.connectInvalidation(port);
                Socket conn = server.accept();
                List<String> lines = Collections.synchronizedList(new ArrayList<>());
                Thread reader = startReader(conn, lines);
                try {
                    Thread.sleep(200);
                    PrintWriter w = new PrintWriter(conn.getOutputStream(), true);
                    w.println("?:snapshot");
                    Thread.sleep(200);
                    long noisy = lines.stream()
                        .filter(l -> l.startsWith("S:") || l.startsWith("R:"))
                        .count();
                    assertEquals(0, noisy, "expected no S/R lines, got " + lines);
                } finally {
                    conn.close();
                    reader.interrupt();
                    cache.stopInvalidation();
                }
            }
        }
    }

    // --- disableNativeCache (wrapper-side native-cache opt-out) ---

    @Nested class DisableNativeCacheTest {
        @Test void defaultCacheBehavesAsToday() {
            // Sanity: makeCache() builds with the default 3-arg constructor,
            // which routes through the 4-arg form with disabled=false.
            NativeCache cache = makeCache();
            cache.put("SELECT * FROM users", null,
                Collections.singletonList(new Object[]{"1", "alice"}),
                new String[]{"id", "name"});
            var entry = cache.get("SELECT * FROM users", null);
            assertNotNull(entry);
            assertEquals(1L, cache.statsHits.get());
            assertEquals(0L, cache.statsMisses.get());
            assertEquals(1, cache.size());
        }

        @Test void disabledGetReturnsNull() throws Exception {
            NativeCache cache = makeDisabledCache();
            cache.put("SELECT 1", null,
                Collections.singletonList(new Object[]{1}), new String[]{"x"});
            // Even a key we just "put" must miss — put is a no-op.
            assertNull(cache.get("SELECT 1", null));
        }

        @Test void disabledPutDoesNotStore() throws Exception {
            NativeCache cache = makeDisabledCache();
            cache.put("SELECT 1", null,
                Collections.singletonList(new Object[]{1}), new String[]{"x"});
            cache.put("SELECT 2", null,
                Collections.singletonList(new Object[]{2}), new String[]{"x"});
            assertEquals(0, cache.size());
        }

        @Test void disabledMissesTickHitsStayZero() throws Exception {
            NativeCache cache = makeDisabledCache();
            // Three gets; all misses since put is a no-op.
            cache.get("SELECT 1", null);
            cache.get("SELECT 2", null);
            cache.get("SELECT 3", null);
            assertEquals(0L, cache.statsHits.get());
            assertEquals(3L, cache.statsMisses.get());
            assertEquals(0L, cache.statsEvictions.get());
        }

        @Test void disabledNoEvictionsEvenAtCapacity() throws Exception {
            // Capacity 2 — without disable, the 3rd put would evict. With
            // disable, neither put stores, so eviction count stays 0.
            NativeCache cache = makeDisabledCacheWithCapacity(2);
            for (int i = 0; i < 5; i++) {
                cache.put("SELECT " + i, null,
                    Collections.singletonList(new Object[]{i}), new String[]{"x"});
            }
            assertEquals(0, cache.size());
            assertEquals(0L, cache.statsEvictions.get());
        }

        @Test void snapshotIncludesDisabledWhenDisabled() throws Exception {
            NativeCache cache = makeDisabledCache();
            Map<String, Object> snap = cache.buildSnapshot();
            assertEquals(Boolean.TRUE, snap.get("disabled"));
        }

        @Test void snapshotOmitsDisabledWhenEnabled() {
            // Default (disabled=false): the field is absent, not present-and-false.
            // Keeps the common-path snapshot stable.
            NativeCache cache = makeCache();
            Map<String, Object> snap = cache.buildSnapshot();
            assertFalse(snap.containsKey("disabled"));
        }

        @Test void wrapperConnectedEmissionCarriesDisabled() throws Exception {
            NativeCache cache = makeDisabledCache();
            List<String> emissions = Collections.synchronizedList(new ArrayList<>());
            cache.setSendOverride(emissions::add);
            cache.emitStateChange("wrapper_connected");
            List<String> sLines = filter(emissions, "S:");
            assertEquals(1, sLines.size(), emissions.toString());
            String body = sLines.get(0);
            assertTrue(body.contains("\"state\":\"wrapper_connected\""), body);
            assertTrue(body.contains("\"disabled\":true"), body);
        }

        // --- runtime mutability via setDisabled (flip after construction) ---

        @Test void setDisabledTrueFlipsLiveCacheToNoOp() throws Exception {
            // Start enabled, populate, then flip to disabled. After the flip,
            // get() must miss even on a previously-cached key, and put() must
            // become a silent no-op.
            NativeCache cache = makeCache();
            cache.put("SELECT 1", null,
                Collections.singletonList(new Object[]{1}), new String[]{"x"});
            assertNotNull(cache.get("SELECT 1", null));
            assertEquals(1L, cache.statsHits.get());

            cache.setDisabled(true);
            assertTrue(cache.isDisabled());

            // Previously-cached key now misses. statsMisses started at 0
            // (the pre-disable get() was a hit, not a miss); the disabled
            // branch ticks it to 1.
            assertNull(cache.get("SELECT 1", null));
            assertEquals(1L, cache.statsMisses.get());
            // hits did not advance — disabled branch returns before hit++.
            assertEquals(1L, cache.statsHits.get());

            // put is a no-op under the new flag.
            cache.put("SELECT 2", null,
                Collections.singletonList(new Object[]{2}), new String[]{"x"});
            assertNull(cache.get("SELECT 2", null));
        }

        @Test void setDisabledFalseRestoresNormalOperation() throws Exception {
            // Start disabled, then flip to enabled. After the flip, get/put
            // must round-trip normally (matches the env-var-not-set / option-
            // false start path).
            NativeCache cache = makeDisabledCache();
            cache.setDisabled(false);
            assertFalse(cache.isDisabled());

            cache.put("SELECT 1", null,
                Collections.singletonList(new Object[]{1}), new String[]{"x"});
            var entry = cache.get("SELECT 1", null);
            assertNotNull(entry);
            assertEquals(1L, cache.statsHits.get());
        }

        @Test void snapshotReflectsLiveDisabledFlag() throws Exception {
            // Snapshot reads `disabled` directly — flipping at runtime must
            // change the next snapshot's `disabled` field. Guards the
            // volatile-not-final field shape (a copy-on-construct value
            // would give stale snapshots).
            NativeCache cache = makeCache();
            assertFalse(cache.buildSnapshot().containsKey("disabled"));
            cache.setDisabled(true);
            assertEquals(Boolean.TRUE, cache.buildSnapshot().get("disabled"));
            cache.setDisabled(false);
            assertFalse(cache.buildSnapshot().containsKey("disabled"));
        }

        private NativeCache makeDisabledCache() throws Exception {
            return makeDisabledCacheWithCapacity(32768);
        }

        private NativeCache makeDisabledCacheWithCapacity(int capacity) throws Exception {
            NativeCache cache = new NativeCache(capacity, true, true, true);
            // Bypass the connect-required gate so direct get/put under test
            // exercise the disabled branch rather than the !connected branch.
            var connected = NativeCache.class.getDeclaredField("invalidationConnected");
            connected.setAccessible(true);
            connected.setBoolean(cache, true);
            return cache;
        }
    }

    // --- Test helpers ---

    private static List<String> filter(List<String> lines, String prefix) {
        List<String> out = new ArrayList<>();
        synchronized (lines) {
            for (String l : lines) if (l.startsWith(prefix)) out.add(l);
        }
        return out;
    }

    private static void waitFor(java.util.function.BooleanSupplier pred, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (pred.getAsBoolean()) return;
            Thread.sleep(20);
        }
    }

    private static Thread startReader(Socket conn, List<String> sink) {
        Thread t = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                while ((line = r.readLine()) != null) {
                    sink.add(line);
                }
            } catch (Exception ignored) {}
        });
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static void resetConnectedFlag(NativeCache cache) throws Exception {
        var f = NativeCache.class.getDeclaredField("invalidationConnected");
        f.setAccessible(true);
        f.setBoolean(cache, false);
    }

    private static NativeCache makeCacheWithCapacity(int capacity) throws Exception {
        NativeCache cache = new NativeCache(capacity, true, true);
        var connected = NativeCache.class.getDeclaredField("invalidationConnected");
        connected.setAccessible(true);
        connected.setBoolean(cache, true);
        return cache;
    }
}
