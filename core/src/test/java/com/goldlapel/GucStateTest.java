package com.goldlapel;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Mirrors the proxy's {@code src/guc_state.rs} test suite — keeps the
 * wrapper-side classifier / parser / state-hash invariants in lockstep with
 * the Rust authority. Any drift here is a real cache-safety bug.
 */
class GucStateTest {

    // ---- isUnsafeGuc ----

    @Nested class IsUnsafeGucTest {
        @Test void unsafeShortListMembers() {
            assertTrue(GucState.isUnsafeGuc("search_path"));
            assertTrue(GucState.isUnsafeGuc("role"));
            assertTrue(GucState.isUnsafeGuc("session_authorization"));
            assertTrue(GucState.isUnsafeGuc("default_transaction_isolation"));
            assertTrue(GucState.isUnsafeGuc("default_transaction_read_only"));
            assertTrue(GucState.isUnsafeGuc("transaction_isolation"));
            assertTrue(GucState.isUnsafeGuc("row_security"));
        }

        @Test void caseInsensitive() {
            assertTrue(GucState.isUnsafeGuc("ROLE"));
            assertTrue(GucState.isUnsafeGuc("Search_Path"));
            assertTrue(GucState.isUnsafeGuc("SEARCH_PATH"));
        }

        @Test void namespacedGucsAreUnsafe() {
            assertTrue(GucState.isUnsafeGuc("app.user_id"));
            assertTrue(GucState.isUnsafeGuc("myapp.tenant"));
            assertTrue(GucState.isUnsafeGuc("rls.account"));
            // Even unknown / arbitrarily nested namespaces.
            assertTrue(GucState.isUnsafeGuc("a.b.c"));
            assertTrue(GucState.isUnsafeGuc("APP.USER"));
        }

        @Test void safeGucsAreSafe() {
            assertFalse(GucState.isUnsafeGuc("application_name"));
            assertFalse(GucState.isUnsafeGuc("statement_timeout"));
            assertFalse(GucState.isUnsafeGuc("work_mem"));
            assertFalse(GucState.isUnsafeGuc("client_encoding"));
            assertFalse(GucState.isUnsafeGuc("seq_page_cost"));
        }

        @Test void localeAndFormattingGucsAreUnsafe() {
            // Locale / formatting GUCs change the textual representation of
            // cached rows — same data, different bytes on the wire — so two
            // connections that disagree on these must not share a cache slot.
            // (java-rls-hardening, 2026-05-05)
            assertTrue(GucState.isUnsafeGuc("DateStyle"));
            assertTrue(GucState.isUnsafeGuc("datestyle"));
            assertTrue(GucState.isUnsafeGuc("IntervalStyle"));
            assertTrue(GucState.isUnsafeGuc("TimeZone"));
            assertTrue(GucState.isUnsafeGuc("timezone"));
            assertTrue(GucState.isUnsafeGuc("bytea_output"));
            assertTrue(GucState.isUnsafeGuc("lc_messages"));
            assertTrue(GucState.isUnsafeGuc("lc_monetary"));
            assertTrue(GucState.isUnsafeGuc("lc_numeric"));
            assertTrue(GucState.isUnsafeGuc("lc_time"));
            assertTrue(GucState.isUnsafeGuc("LC_TIME"));
        }

        @Test void nullIsSafe() {
            // Defensive: a null name is "no GUC" — treating it as safe means
            // observeSql() over malformed input never accidentally moves the
            // hash.
            assertFalse(GucState.isUnsafeGuc(null));
        }
    }

    // ---- parseSetCommand: shapes ----

    @Nested class ParseSetCommandShapesTest {
        @Test void setEqQuoted() {
            GucState.SetCommand c = GucState.parseSetCommand("SET foo = 'bar'");
            assertEquals(GucState.SetCommand.Kind.SET, c.kind);
            assertEquals("foo", c.name);
            assertEquals("bar", c.value);
        }

        @Test void setToQuoted() {
            GucState.SetCommand c = GucState.parseSetCommand("SET foo TO 'bar'");
            assertEquals(GucState.SetCommand.Kind.SET, c.kind);
            assertEquals("foo", c.name);
            assertEquals("bar", c.value);
        }

        @Test void setUnquoted() {
            GucState.SetCommand c = GucState.parseSetCommand("SET foo = 42");
            assertEquals(GucState.SetCommand.Kind.SET, c.kind);
            assertEquals("foo", c.name);
            assertEquals("42", c.value);
        }

        @Test void setSessionModifier() {
            GucState.SetCommand c = GucState.parseSetCommand("SET SESSION foo = 'bar'");
            assertEquals(GucState.SetCommand.Kind.SET, c.kind);
            assertEquals("foo", c.name);
            assertEquals("bar", c.value);
        }

        @Test void setLocalModifier() {
            GucState.SetCommand c = GucState.parseSetCommand("SET LOCAL foo = 'bar'");
            assertEquals(GucState.SetCommand.Kind.SET_LOCAL, c.kind);
            assertEquals("foo", c.name);
            assertEquals("bar", c.value);
        }

        @Test void resetNamed() {
            GucState.SetCommand c = GucState.parseSetCommand("RESET foo");
            assertEquals(GucState.SetCommand.Kind.RESET, c.kind);
            assertEquals("foo", c.name);
            assertNull(c.value);
        }

        @Test void resetAll() {
            GucState.SetCommand c = GucState.parseSetCommand("RESET ALL");
            assertEquals(GucState.SetCommand.Kind.RESET_ALL, c.kind);
            assertNull(c.name);
            assertNull(c.value);
        }
    }

    // ---- parseSetCommand: case + whitespace + semicolon ----

    @Nested class ParseSetCommandCaseTest {
        @Test void caseInsensitiveKeywords() {
            GucState.SetCommand a = GucState.parseSetCommand("set foo = 'bar'");
            assertEquals(GucState.SetCommand.Kind.SET, a.kind);
            assertEquals("foo", a.name);
            assertEquals("bar", a.value);

            GucState.SetCommand b = GucState.parseSetCommand("Set Local foo To 'bar'");
            assertEquals(GucState.SetCommand.Kind.SET_LOCAL, b.kind);
            assertEquals("foo", b.name);
            assertEquals("bar", b.value);

            GucState.SetCommand c = GucState.parseSetCommand("reset all");
            assertEquals(GucState.SetCommand.Kind.RESET_ALL, c.kind);
        }

        @Test void lowercasesGucName() {
            GucState.SetCommand c = GucState.parseSetCommand("SET App.User_ID = '42'");
            assertEquals("app.user_id", c.name);
            assertEquals("42", c.value);
        }

        @Test void toleratesTrailingSemicolon() {
            GucState.SetCommand c1 = GucState.parseSetCommand("SET foo = 'bar';");
            assertEquals("foo", c1.name);
            assertEquals("bar", c1.value);

            GucState.SetCommand c2 = GucState.parseSetCommand("RESET foo ;");
            assertEquals(GucState.SetCommand.Kind.RESET, c2.kind);
            assertEquals("foo", c2.name);
        }

        @Test void toleratesExtraWhitespace() {
            GucState.SetCommand c = GucState.parseSetCommand("   SET    foo   =   'bar'   ");
            assertEquals("foo", c.name);
            assertEquals("bar", c.value);
        }

        @Test void gluedEquals() {
            GucState.SetCommand c = GucState.parseSetCommand("SET app.user_id='42'");
            assertEquals("app.user_id", c.name);
            assertEquals("42", c.value);
        }

        @Test void doubleQuotedValue() {
            GucState.SetCommand c = GucState.parseSetCommand("SET foo = \"bar\"");
            assertEquals("foo", c.name);
            assertEquals("bar", c.value);
        }

        @Test void doubleQuotedName() {
            // "app.user_id" is a quoted identifier — same value as bare.
            GucState.SetCommand c = GucState.parseSetCommand("SET \"app.user_id\" = '42'");
            assertEquals("app.user_id", c.name);
            assertEquals("42", c.value);
        }
    }

    // ---- parseSetCommand: rejects ----

    @Nested class ParseSetCommandRejectsTest {
        @Test void rejectsNonSetStatements() {
            assertNull(GucState.parseSetCommand("SELECT 1"));
            assertNull(GucState.parseSetCommand("BEGIN"));
            assertNull(GucState.parseSetCommand("UPDATE t SET x = 1"));
        }

        @Test void rejectsEmpty() {
            assertNull(GucState.parseSetCommand(""));
            assertNull(GucState.parseSetCommand("   "));
            assertNull(GucState.parseSetCommand(";"));
            assertNull(GucState.parseSetCommand(null));
        }

        @Test void rejectsSetWithoutValue() {
            assertNull(GucState.parseSetCommand("SET foo ="));
            assertNull(GucState.parseSetCommand("SET foo TO"));
            assertNull(GucState.parseSetCommand("SET foo"));
        }

        @Test void rejectsResetWithGarbage() {
            assertNull(GucState.parseSetCommand("RESET foo bar"));
        }

        @Test void rejectsSetTimeZoneTwoWordForm() {
            // SET TIME ZONE 'UTC' is the legacy two-word form; we don't model
            // it (the parser tokens are SET / TIME / ZONE / 'UTC', and `TIME`
            // isn't a recognised GUC name). Returning null is acceptable
            // because timezone changes are also caught by SET TimeZone =
            // 'UTC' (the canonical form), which DOES move the hash now that
            // TimeZone is in the unsafe-GUC list. A caller using the legacy
            // syntax temporarily slips past wire-side observation; the
            // post-call verify path or the next checkout's pg_settings read
            // will reconcile it.
            assertNull(GucState.parseSetCommand("SET TIME ZONE 'UTC'"));
        }
    }

    // ---- DISCARD parsing (java-rls-hardening, 2026-05-05) ----

    @Nested class DiscardTest {
        @Test void discardAllParsesToDiscardAllKind() {
            GucState.SetCommand c = GucState.parseSetCommand("DISCARD ALL");
            assertNotNull(c);
            assertEquals(GucState.SetCommand.Kind.DISCARD_ALL, c.kind);
            assertNull(c.name);
            assertNull(c.value);
        }

        @Test void discardAllCaseInsensitive() {
            GucState.SetCommand c = GucState.parseSetCommand("discard all");
            assertEquals(GucState.SetCommand.Kind.DISCARD_ALL, c.kind);
            GucState.SetCommand d = GucState.parseSetCommand("DiScArD aLl");
            assertEquals(GucState.SetCommand.Kind.DISCARD_ALL, d.kind);
        }

        @Test void discardAllToleratesTrailingSemicolon() {
            GucState.SetCommand c = GucState.parseSetCommand("DISCARD ALL;");
            assertEquals(GucState.SetCommand.Kind.DISCARD_ALL, c.kind);
        }

        @Test void discardPlansIsNoop() {
            // Wrapper has no prepared-statement cache of its own — JDBC
            // PreparedStatement objects belong to the driver. Returning null
            // is the correct "not-a-trackable-mutation" outcome.
            assertNull(GucState.parseSetCommand("DISCARD PLANS"));
        }

        @Test void discardSequencesIsNoop() {
            assertNull(GucState.parseSetCommand("DISCARD SEQUENCES"));
        }

        @Test void discardTempIsNoop() {
            assertNull(GucState.parseSetCommand("DISCARD TEMP"));
        }

        @Test void discardTemporaryIsNoop() {
            assertNull(GucState.parseSetCommand("DISCARD TEMPORARY"));
        }

        @Test void discardUnknownTargetIsNoop() {
            // Conservative — PG would raise a syntax error; nothing for us
            // to mutate. Returning null avoids a verify-stuck loop on bad
            // input the user typed.
            assertNull(GucState.parseSetCommand("DISCARD GARBAGE"));
        }

        @Test void discardAllClearsState() {
            GucState s = new GucState();
            s.observeSql("SET app.user_id = '42'");
            s.observeSql("SET search_path TO 'tenant_a'");
            assertNotEquals(0L, s.hash());
            s.observeSql("DISCARD ALL");
            assertEquals(0L, s.hash(), "DISCARD ALL must drop all unsafe state");
        }

        @Test void discardAllInMultiStatement() {
            // The HikariCP connectionInitSql wiring fires DISCARD ALL on each
            // freshly-pooled physical connection — verify the wire-side
            // observation handles it identically when batched.
            GucState s = new GucState();
            s.observeSql("SET app.user_id = '42'");
            s.observeSql("DISCARD ALL; SET app.tenant = 'acme'");
            // Should equal: just SET app.tenant after a clean slate.
            GucState reference = new GucState();
            reference.observeSql("SET app.tenant = 'acme'");
            assertEquals(reference.hash(), s.hash());
        }

        @Test void discardPlansLeavesStateUntouched() {
            GucState s = new GucState();
            s.observeSql("SET app.user_id = '42'");
            long h = s.hash();
            s.observeSql("DISCARD PLANS");
            assertEquals(h, s.hash(),
                "DISCARD PLANS targets prepared-statement cache only, not GUC state");
        }
    }

    // ---- set_config() function form (java-rls-hardening, 2026-05-05) ----

    @Nested class SetConfigTest {
        @Test void parsesBareSetConfig() {
            GucState.SetCommand c = GucState.parseSetConfigCall(
                "SELECT set_config('app.user_id', '42', false)");
            assertNotNull(c);
            assertEquals(GucState.SetCommand.Kind.SET, c.kind);
            assertEquals("app.user_id", c.name);
            assertEquals("42", c.value);
        }

        @Test void parsesPgCatalogQualified() {
            // PostgREST emits the schema-qualified form — this is the
            // canonical Supabase JWT-claim shape we have to recognise.
            GucState.SetCommand c = GucState.parseSetConfigCall(
                "SELECT pg_catalog.set_config('app.user_id', '42', false)");
            assertNotNull(c);
            assertEquals(GucState.SetCommand.Kind.SET, c.kind);
            assertEquals("app.user_id", c.name);
            assertEquals("42", c.value);
        }

        @Test void isLocalTrueProducesSetLocalKind() {
            GucState.SetCommand c = GucState.parseSetConfigCall(
                "SELECT set_config('app.user_id', '42', true)");
            assertNotNull(c);
            assertEquals(GucState.SetCommand.Kind.SET_LOCAL, c.kind);
        }

        @Test void isLocalAcceptsIntForm() {
            assertEquals(GucState.SetCommand.Kind.SET_LOCAL,
                GucState.parseSetConfigCall("SELECT set_config('a.b', 'c', 1)").kind);
            assertEquals(GucState.SetCommand.Kind.SET,
                GucState.parseSetConfigCall("SELECT set_config('a.b', 'c', 0)").kind);
        }

        @Test void caseInsensitiveSelectAndFunctionName() {
            GucState.SetCommand c = GucState.parseSetConfigCall(
                "select Set_Config('app.user_id', '42', FALSE)");
            assertNotNull(c);
            assertEquals("app.user_id", c.name);
            assertEquals("42", c.value);
            assertEquals(GucState.SetCommand.Kind.SET, c.kind);
        }

        @Test void doubleQuotedNameAccepted() {
            GucState.SetCommand c = GucState.parseSetConfigCall(
                "SELECT set_config(\"app.user_id\", \"42\", false)");
            assertNotNull(c);
            assertEquals("app.user_id", c.name);
            assertEquals("42", c.value);
        }

        @Test void doubledQuoteEscapeInValueSurvives() {
            // PG's '' escape inside a literal value. The argument splitter
            // is doubled-quote aware (it treats `''` as a single escaped quote
            // and does NOT split on the embedded ',' so the call still parses
            // as one argument), but stripValueQuotes only peels the outer
            // delimiters — the inner `''` survives in the value. Symmetric
            // with parseSetCommand's handling. Test pins the contract so a
            // future "decode escapes too" change is a deliberate choice.
            GucState.SetCommand c = GucState.parseSetConfigCall(
                "SELECT set_config('app.note', 'it''s ok', false)");
            assertNotNull(c);
            assertEquals("it''s ok", c.value);
        }

        @Test void rejectsNonLiteralName() {
            // We can't evaluate `current_user_id_fn()` from this side; bail.
            assertNull(GucState.parseSetConfigCall(
                "SELECT set_config(name_var, '42', false)"));
        }

        @Test void rejectsNonLiteralValue() {
            assertNull(GucState.parseSetConfigCall(
                "SELECT set_config('app.user_id', some_fn(), false)"));
        }

        @Test void rejectsWrongArity() {
            assertNull(GucState.parseSetConfigCall("SELECT set_config('a', 'b')"));
            assertNull(GucState.parseSetConfigCall("SELECT set_config('a', 'b', false, 'extra')"));
        }

        @Test void rejectsNonSetConfig() {
            assertNull(GucState.parseSetConfigCall("SELECT current_setting('app.x')"));
            assertNull(GucState.parseSetConfigCall("SELECT 1"));
            assertNull(GucState.parseSetConfigCall("UPDATE t SET x = 1"));
        }

        @Test void rejectsExtraSqlAfterCall() {
            assertNull(GucState.parseSetConfigCall(
                "SELECT set_config('a.b', 'c', false) FROM dual"));
        }

        @Test void observeSqlAppliesSetConfig() {
            GucState s = new GucState();
            assertTrue(s.observeSql("SELECT set_config('app.user_id', '42', false)"),
                "set_config must mutate the hash like SET would");
            assertNotEquals(0L, s.hash());

            // Same value via SET produces the same hash — observability parity.
            GucState ref = new GucState();
            ref.observeSql("SET app.user_id = '42'");
            assertEquals(ref.hash(), s.hash());
        }

        @Test void observeSqlSetConfigLocalIsNoop() {
            GucState s = new GucState();
            s.observeSql("SELECT set_config('app.user_id', '42', true)");
            assertEquals(0L, s.hash(), "is_local=true ⇒ SET LOCAL ⇒ no hash change");
        }

        @Test void observeSqlSetConfigInMultiStatement() {
            GucState s = new GucState();
            s.observeSql("SELECT set_config('app.user_id', '42', false); SELECT * FROM t");
            assertNotEquals(0L, s.hash());
        }
    }


    // ---- ConnectionGucState core invariants ----

    @Nested class StateHashTest {
        @Test void emptyStateHashIsZero() {
            GucState s = new GucState();
            assertEquals(0L, s.hash());
            assertEquals(0, s.size());
        }

        @Test void safeSetDoesNotChangeHash() {
            GucState s = new GucState();
            s.observeSql("SET application_name = 'foo'");
            assertEquals(0L, s.hash(), "harmless GUC must leave hash untouched");
            s.observeSql("SET statement_timeout = 5000");
            assertEquals(0L, s.hash());
            s.observeSql("SET work_mem = '64MB'");
            assertEquals(0L, s.hash());
        }

        @Test void unsafeSetChangesHash() {
            GucState s = new GucState();
            long h0 = s.hash();
            s.observeSql("SET app.user_id = '42'");
            long h1 = s.hash();
            assertNotEquals(h0, h1, "unsafe SET must change the hash");
        }

        @Test void sameUnsafeSetYieldsSameHashOnTwoConnections() {
            GucState a = new GucState();
            GucState b = new GucState();
            a.observeSql("SET app.user_id = '42'");
            b.observeSql("SET app.user_id = '42'");
            assertEquals(a.hash(), b.hash(), "identical state ⇒ identical hash");
        }

        @Test void differentUnsafeValuesYieldDifferentHashes() {
            GucState a = new GucState();
            GucState b = new GucState();
            a.observeSql("SET app.user_id = '42'");
            b.observeSql("SET app.user_id = '43'");
            assertNotEquals(a.hash(), b.hash(), "different values ⇒ different hash");
        }

        @Test void insertionOrderDoesNotMatter() {
            GucState a = new GucState();
            a.observeSql("SET app.user_id = '42'");
            a.observeSql("SET app.tenant = 'alpha'");

            GucState b = new GucState();
            b.observeSql("SET app.tenant = 'alpha'");
            b.observeSql("SET app.user_id = '42'");

            assertEquals(a.hash(), b.hash(), "TreeMap ordering ⇒ stable hash");
        }

        @Test void resetReturnsHashToBaseline() {
            GucState s = new GucState();
            long baseline = s.hash();
            s.observeSql("SET app.user_id = '42'");
            assertNotEquals(baseline, s.hash());
            s.observeSql("RESET app.user_id");
            assertEquals(baseline, s.hash(), "RESET must restore the baseline hash");
        }

        @Test void resetAllClearsAllUnsafeState() {
            GucState s = new GucState();
            s.observeSql("SET app.user_id = '42'");
            s.observeSql("SET search_path TO 'tenant_a'");
            s.observeSql("SET role = 'app_user'");
            assertNotEquals(0L, s.hash());
            s.observeSql("RESET ALL");
            assertEquals(0L, s.hash(), "RESET ALL must drop all unsafe state");
        }

        @Test void setLocalDoesNotChangeHash() {
            GucState s = new GucState();
            // Even an unsafe-named SET LOCAL must not move the hash —
            // SET LOCAL only takes effect inside a txn, and the cache layer
            // is gated on !inTransaction.
            s.observeSql("SET LOCAL app.user_id = '42'");
            assertEquals(0L, s.hash());
        }

        @Test void observeSqlReturnsChangeFlag() {
            GucState s = new GucState();
            assertTrue(s.observeSql("SET app.user_id = '42'"), "first set ⇒ changed");
            assertFalse(s.observeSql("SELECT 1"), "non-SET ⇒ unchanged");
            assertFalse(s.observeSql("SET application_name = 'foo'"), "safe SET ⇒ unchanged");
            assertTrue(s.observeSql("RESET app.user_id"), "RESET unsafe ⇒ changed");
        }

        @Test void resetSafeGucIsNoop() {
            GucState s = new GucState();
            s.observeSql("SET app.user_id = '42'");
            long h = s.hash();
            s.observeSql("RESET application_name");
            assertEquals(h, s.hash());
        }

        @Test void overwriteUnsafeValueChangesHash() {
            GucState s = new GucState();
            s.observeSql("SET app.user_id = '42'");
            long h1 = s.hash();
            s.observeSql("SET app.user_id = '43'");
            long h2 = s.hash();
            assertNotEquals(h1, h2);
        }

        @Test void resettingUnsetUnsafeIsNoop() {
            // Resetting a GUC we've never set must not move the hash from
            // baseline. Guards a recompute-on-noop bug.
            GucState s = new GucState();
            assertFalse(s.observeSql("RESET app.user_id"));
            assertEquals(0L, s.hash());
        }
    }

    // ---- splitStatements ----

    @Nested class SplitStatementsTest {
        @Test void simpleTwoStatements() {
            assertArrayEquals(new String[]{"SET foo = '42'", "SELECT 1"},
                GucState.splitStatements("SET foo = '42'; SELECT 1"));
        }

        @Test void dropsEmptySegments() {
            // Trailing ;, leading ;, doubled ;; all produce empty segments
            // which we drop.
            assertArrayEquals(new String[]{"SET foo = '42'", "SELECT 1"},
                GucState.splitStatements("; SET foo = '42';;SELECT 1;"));
        }

        @Test void respectsSingleQuotes() {
            // The ; inside the literal must NOT split the statement.
            assertArrayEquals(new String[]{"SET foo = 'a;b'", "SELECT 1"},
                GucState.splitStatements("SET foo = 'a;b'; SELECT 1"));
        }

        @Test void respectsDoubleQuotes() {
            assertArrayEquals(new String[]{"SET \"app;guc\" = 'x'", "SELECT 1"},
                GucState.splitStatements("SET \"app;guc\" = 'x'; SELECT 1"));
        }

        @Test void handlesDoubledQuoteEscape() {
            // PG escapes a literal ' inside a string by doubling: ''.
            assertArrayEquals(new String[]{"SET foo = 'it''s; ok'", "SELECT 1"},
                GucState.splitStatements("SET foo = 'it''s; ok'; SELECT 1"));
        }

        @Test void singleStatementPassThrough() {
            assertArrayEquals(new String[]{"SET foo = '42'"},
                GucState.splitStatements("SET foo = '42'"));
        }

        @Test void emptyInputs() {
            assertEquals(0, GucState.splitStatements("").length);
            assertEquals(0, GucState.splitStatements("   ").length);
            assertEquals(0, GucState.splitStatements(";;;").length);
            assertEquals(0, GucState.splitStatements(null).length);
        }
    }

    // ---- Multi-statement observe ----

    @Nested class MultiStatementObserveTest {
        @Test void multiStatementAppliesAllSets() {
            GucState s = new GucState();
            s.observeSql("SET app.user_id = '42'; SELECT * FROM accounts");
            assertNotEquals(0L, s.hash());
        }

        @Test void multiStatementAppliesTwoUnsafeSets() {
            GucState a = new GucState();
            a.observeSql("SET app.user_id = '42'");
            a.observeSql("SET app.tenant = 'alpha'");

            GucState b = new GucState();
            b.observeSql("SET app.user_id = '42'; SET app.tenant = 'alpha'");

            assertEquals(a.hash(), b.hash(),
                "batched SET must produce same hash as separate SETs");
        }

        @Test void multiStatementWithQuotedSemicolon() {
            GucState s = new GucState();
            s.observeSql("SET app.tenant = 'has;semicolon'; SELECT 1");
            assertNotEquals(0L, s.hash());
        }

        @Test void distinctValuesContainingQuotedSemicolonHashDifferently() {
            // Pinning the splitter+parser interaction: two sessions setting
            // the same unsafe GUC to different quoted-semicolon values
            // must produce different hashes (the semicolon inside the
            // literal must NOT split the value).
            GucState a = new GucState();
            a.observeSql("SET app.tenant = 'a;b'; SELECT 1");
            GucState b = new GucState();
            b.observeSql("SET app.tenant = 'c;d'; SELECT 1");
            assertNotEquals(a.hash(), b.hash());
        }
    }

    // ---- Cache-key integration: hash flows through ----

    @Nested class CacheKeyIntegrationTest {
        @Test void differentStateHashesYieldDifferentKeys() {
            String k0 = NativeCache.makeKey("SELECT * FROM t", null, 0L);
            String k1 = NativeCache.makeKey("SELECT * FROM t", null, 0xdeadbeefL);
            assertNotEquals(k0, k1, "state hash must be folded into cache key");
        }

        @Test void zeroStateHashKeyMatchesTwoArgVariant() {
            // Backwards compatibility for callers that use the 2-arg key
            // helper without GUC awareness — must match what the 3-arg form
            // produces with hash=0 (the baseline empty-state slot).
            String legacy = NativeCache.makeKey("SELECT * FROM t", null);
            String explicit = NativeCache.makeKey("SELECT * FROM t", null, 0L);
            assertEquals(legacy, explicit);
        }

        @Test void cachePutAndGetWithStateHashRoundTrip() throws Exception {
            // End-to-end: put under one state hash, get with the same hash
            // must hit; get with a different hash must miss. Mirrors the
            // proxy-side cache_key + ProxyCache test in proxy.rs.
            NativeCache.reset();
            try {
                NativeCache cache = new NativeCache(32, true, true);
                java.lang.reflect.Field connected =
                    NativeCache.class.getDeclaredField("invalidationConnected");
                connected.setAccessible(true);
                connected.setBoolean(cache, true);

                cache.put("SELECT * FROM accounts", null,
                    Collections.singletonList(new Object[]{"1", "alice"}),
                    new String[]{"id", "name"}, 0xaaaaL);

                // Same hash — hit.
                NativeCache.CacheEntry hit = cache.get("SELECT * FROM accounts", null, 0xaaaaL);
                assertNotNull(hit);
                assertEquals("alice", hit.rows.get(0)[1]);

                // Different hash — miss.
                NativeCache.CacheEntry miss = cache.get("SELECT * FROM accounts", null, 0xbbbbL);
                assertNull(miss);

                // Baseline (0) — also miss, since we put under 0xaaaa.
                NativeCache.CacheEntry baseline = cache.get("SELECT * FROM accounts", null, 0L);
                assertNull(baseline);
            } finally {
                NativeCache.reset();
            }
        }
    }
}
