package com.goldlapel;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Per-connection GUC state tracking for native-cache key safety.
 *
 * <p>Mirrors the proxy's {@code src/guc_state.rs} (Option Y, locked at
 * {@code docs/todos/guc-rls-cache-safety.md}). Custom-GUC-driven RLS — e.g.
 * {@code SET app.user_id = '42'; SELECT * FROM accounts} where the policy
 * reads {@code current_setting('app.user_id')} — would otherwise leak user A's
 * native-cache rows to user B, because the cache key is just SQL+params. This
 * class fingerprints the subset of GUCs that can change query results so the
 * cache key can include the fingerprint and never cross security boundaries.
 *
 * <p>A GUC is <b>unsafe</b> if it is in a short hardcoded list (search path,
 * role, isolation, etc.) OR contains a {@code .} (namespaced — {@code app.*},
 * {@code myapp.*}, etc.). Unsafe GUC values are stored in a
 * {@link TreeMap} keyed by lowercased GUC name, so iteration order — and the
 * resulting hash — is independent of how the user issued the {@code SET}s.
 *
 * <p>{@code SET LOCAL} is intentionally ignored: the wrapper's
 * native-cache get/put already short-circuits when the connection is in a
 * transaction (see {@link ConnectionProxy}'s {@code inTransaction} gate), so
 * {@code SET LOCAL} effects can never influence a cacheable response.
 *
 * <p><b>Thread-safety.</b> An instance of this class is owned by a single
 * JDBC connection's {@code ConnectionHandler}. JDBC connections are not safe
 * for concurrent use across threads (per the JDBC spec) — applications
 * routing concurrent queries through one {@link java.sql.Connection} are
 * already broken — so this class does not synchronize. The {@code volatile}
 * on {@link #hash} is purely defensive in the rare case where a snapshot of
 * the hash is read by a peer thread (e.g. a logging/telemetry sidecar).
 */
public final class GucState {

    /**
     * GUC names whose value can change query results without changing the
     * SQL text. Matched case-insensitively. Any GUC with a {@code .} in the
     * name is also treated as unsafe (namespaced GUCs are the canonical
     * custom-RLS pattern).
     *
     * <p>Locale / formatting GUCs ({@code DateStyle}, {@code IntervalStyle},
     * {@code TimeZone}, {@code bytea_output}, {@code lc_messages},
     * {@code lc_monetary}, {@code lc_numeric}, {@code lc_time}) are unsafe
     * because they alter the textual representation of cached rows: the same
     * SQL on the same data with a different {@code DateStyle} or
     * {@code lc_numeric} returns different bytes to the client. Two
     * connections that disagree on these GUCs must not share a cache slot.
     */
    private static final Set<String> UNSAFE_GUC_SHORT_LIST = Set.of(
        "search_path",
        "role",
        "session_authorization",
        "default_transaction_isolation",
        "default_transaction_read_only",
        "transaction_isolation",
        "row_security",
        "datestyle",
        "intervalstyle",
        "timezone",
        "bytea_output",
        "lc_messages",
        "lc_monetary",
        "lc_numeric",
        "lc_time"
    );

    /**
     * Lowercased GUC name → raw value string. Only unsafe GUCs are present;
     * harmless GUCs (timezone, application_name, planner cost knobs, etc.)
     * never enter this map and never affect the hash. {@link TreeMap} for
     * stable iteration order so the hash is invariant to insertion order.
     */
    private final TreeMap<String, String> values = new TreeMap<>();

    /**
     * Cached hash of {@link #values} mixed with {@link #dmlSeq}, recomputed
     * on every mutation. {@code 0} for the empty (default) state — a fresh
     * connection's hash must match "no GUCs set" cache slots populated by
     * peers, which is exactly what we want. {@code volatile} for cross-thread
     * visibility (see class doc).
     */
    private volatile long hash = 0L;

    /**
     * Monotonic counter bumped by {@link #bumpDmlSeq()} after every observed
     * INSERT/UPDATE/DELETE/MERGE/TRUNCATE/CALL/DDL. Mixed into {@link #hash}
     * so each post-DML lookup gets a unique cache key — closes the
     * trigger-internal-SET correctness gap (a server-side trigger that did
     * {@code SET app.user_id = ...} would otherwise be invisible to the
     * wire-side state observer and a cached pre-DML response could be served
     * under the mutated session state).
     *
     * <p>Reset to {@code 0} whenever the rest of the state is wiped (RESET
     * ALL / DISCARD ALL) so a recycled connection re-converges to a
     * peer-shareable baseline (hash {@code 0}).
     *
     * <p>{@code volatile} mirrors {@link #hash} — the verify executor reads
     * the state from a worker thread while the user-facing JDBC thread
     * mutates it; the connection lock in {@link ConnectionProxy} provides
     * the serialization, the volatile guarantees visibility.
     */
    private volatile long dmlSeq = 0L;

    /**
     * "State may have shifted server-side without us seeing the wire SET" flag.
     * Set when something we can't reliably parse off the wire mutates session
     * GUCs — e.g. a stored function or procedure body that issues
     * {@code SET app.user_id = ...} internally, or a failed verify attempt.
     * The {@link ConnectionProxy} consults this on each invocation: if dirty,
     * it re-reads {@code pg_settings} to reconstruct {@link #values} before
     * trusting the next cache lookup.
     *
     * <p>{@code volatile} because the post-call verify executor sets it from
     * a worker thread while the user-facing JDBC thread reads it on every
     * query path. The connection lock in {@link ConnectionProxy} provides the
     * serialization for the verify itself; the volatile guarantees the flag's
     * visibility (so a worker that bails early because the connection is busy
     * still flips the bit visibly to the next query).
     */
    private volatile boolean dirty = false;

    /** Current state hash. {@code 0} for the empty (baseline) state. */
    public long hash() {
        return hash;
    }

    /** Number of unsafe GUCs currently tracked. Visible for testing. */
    int size() {
        return values.size();
    }

    /**
     * Read the dirty flag. {@code true} means the wire-side observation may
     * have missed a server-side state mutation (function body, trigger, etc.)
     * — the next query path should reconcile state via {@link #verify} before
     * trusting the cache key.
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * Mark the connection's state as possibly stale. Called by the post-call
     * verify path when a verify attempt fails, and by callers that observe a
     * statement with side-effects we can't parse off the wire.
     */
    public void markDirty() {
        dirty = true;
    }

    /** Test-only — clear the dirty flag without running a real verify. */
    void clearDirty() {
        dirty = false;
    }

    /** Current post-DML sequence counter. Visible for testing. */
    long dmlSeq() {
        return dmlSeq;
    }

    /**
     * Bump the post-DML sequence counter so the next cache-key computation
     * on this connection produces a fresh slot. Called from the
     * {@link ConnectionProxy} statement handlers after every observed
     * INSERT/UPDATE/DELETE/MERGE/TRUNCATE/CALL/DDL.
     *
     * <p>The bump means: any subsequent cacheable read on this connection
     * cannot share a cache slot with a pre-DML read from this same connection
     * — closing the trigger-internal-SET correctness gap (a server-side
     * trigger that mutated {@code app.user_id} via {@code SET} from inside
     * its body would otherwise be invisible to the wire-side state observer,
     * and a stale cached response could be served under the mutated state).
     *
     * <p>This is cache-key isolation, not actual observation of the new GUC
     * values. A trigger that mutated state produces correct results from PG
     * itself (PG always knows its own session state); the wrapper just
     * guarantees the cache can't hand back a stale response keyed on the
     * previous state. Mirrors the proxy's
     * {@code ConnectionGucState::mark_post_dml} in {@code src/guc_state.rs}.
     *
     * <p>{@code wrapping} arithmetic — {@code Long#sum} -style overflow on a
     * {@code long} is fine; even at 1 GHz of bumps (an absurd upper bound)
     * the counter takes ~292 years to wrap, and the worst case at wrap is a
     * single coincidental cache-key collision with a state hundreds of years
     * old on a connection that hasn't been recycled since.
     */
    public void bumpDmlSeq() {
        dmlSeq++;
        recomputeHash();
    }

    /**
     * Reconcile the in-memory state map with the live server-side session
     * GUCs. Issues a single
     * {@code SELECT name, setting FROM pg_settings WHERE source = 'session'}
     * over {@code conn} and rebuilds {@link #values} from the rows whose
     * {@code name} the wrapper considers unsafe (per {@link #isUnsafeGuc}).
     *
     * <p>Called from two places:
     * <ul>
     *   <li>The post-call verify executor, scheduled by
     *       {@link ConnectionProxy} when it observes a top-level
     *       {@code SELECT <function>(...)} or {@code CALL <proc>(...)} —
     *       function bodies can issue {@code SET}s the wire layer never sees,
     *       so we re-read after the call.</li>
     *   <li>The lazy verify-on-checkout fallback in {@link ConnectionProxy} —
     *       when {@link #isDirty} is set on the next user query, this runs
     *       inline before the cache lookup.</li>
     * </ul>
     *
     * <p>Any SQLException is swallowed and {@link #dirty} is left set; the
     * caller's hot path must never observe a verify failure (we'd rather miss
     * a cache hit than throw on the user). Successful runs clear
     * {@link #dirty}.
     *
     * <p>The connection is assumed to be exclusively held by the caller (the
     * {@code connectionLock} in {@link ConnectionProxy.ConnectionHandler}
     * provides this guarantee). JDBC connections aren't safe for concurrent
     * use, so this method does not attempt to coordinate with peer threads;
     * the lock is the contract.
     */
    public void verify(java.sql.Connection conn) {
        if (conn == null) return;
        TreeMap<String, String> rebuilt = new TreeMap<>();
        try (java.sql.Statement st = conn.createStatement();
             java.sql.ResultSet rs = st.executeQuery(
                 "SELECT name, setting FROM pg_settings WHERE source = 'session'")) {
            while (rs.next()) {
                String name = rs.getString(1);
                String value = rs.getString(2);
                if (name == null) continue;
                String lower = name.toLowerCase(java.util.Locale.ROOT);
                if (isUnsafeGuc(lower)) {
                    rebuilt.put(lower, value == null ? "" : value);
                }
            }
        } catch (java.sql.SQLException e) {
            // Verify failed — leave dirty flag set so the next path will retry.
            // Don't clear `values` either: a partially-filled rebuilt map would
            // be worse than a possibly-stale full one.
            return;
        }
        values.clear();
        values.putAll(rebuilt);
        // A successful verify means we've reconciled with the live server
        // state — the post-DML sequence's reason for existing (cache-key
        // isolation across an unknown server-side mutation window) is now
        // closed. Resetting the counter lets this connection re-converge
        // to the peer-shareable baseline (hash {@code 0} if values is
        // empty) for its next cacheable read.
        dmlSeq = 0L;
        recomputeHash();
        dirty = false;
    }

    /**
     * Classify a GUC name as state-affecting ({@code true}) or harmless
     * ({@code false}). A GUC is unsafe if it's in the short hardcoded list
     * OR contains a {@code .} (namespaced). Comparison is case-insensitive.
     */
    public static boolean isUnsafeGuc(String name) {
        if (name == null) return false;
        String lower = name.toLowerCase(java.util.Locale.ROOT);
        if (lower.indexOf('.') >= 0) {
            return true;
        }
        return UNSAFE_GUC_SHORT_LIST.contains(lower);
    }

    /**
     * Snapshot of {@link #values} for transactional revert. Returned by
     * {@link #snapshot()}; restored by {@link #restoreOrReset}. Used by
     * {@link ConnectionProxy} to defer state-hash mutation until the JDBC
     * call returns successfully — wire-side observation is optimistic, but
     * a {@link java.sql.SQLException} from the server means the SET never
     * actually applied, and the wrapper must roll back to avoid diverging
     * from server-side state.
     *
     * <p>Snapshot cost is bounded: typical apps hold a handful of unsafe
     * GUCs (one or two namespaced RLS keys, maybe a search_path) — copying
     * a TreeMap with ~5 entries is &lt;1µs and dominated by allocation.
     * Taken on every Statement / PreparedStatement / CallableStatement
     * invoke that touches the wire — frequent, but cheap.
     *
     * <p>Records the {@link #hash} alongside the values so
     * {@link #restoreOrReset} doesn't have to re-fold the FNV mixer; restore
     * is hot too (the exception path), so paying the bytes here is the right
     * trade.
     */
    public static final class Snapshot {
        final TreeMap<String, String> values;
        final long hash;
        final long dmlSeq;
        Snapshot(TreeMap<String, String> values, long hash, long dmlSeq) {
            this.values = values;
            this.hash = hash;
            this.dmlSeq = dmlSeq;
        }
    }

    /**
     * Capture the current state for a possible {@link #restoreOrReset} on
     * JDBC failure. Returns {@code null} when there's nothing to roll
     * back — an empty-state snapshot would just allocate for no reason;
     * the caller treats null as "no revert needed". Callers who restore
     * unconditionally (e.g. catch-all error path) should use
     * {@link #restoreOrReset} which handles the null-snapshot case.
     *
     * <p>Allocates a fresh TreeMap on every call — JDBC connections are
     * single-threaded per spec, so there's no concurrent mutator to worry
     * about, but the snapshot must be a deep copy so a subsequent
     * {@link #apply} doesn't ride through into the snapshot.
     */
    public Snapshot snapshot() {
        if (values.isEmpty() && hash == 0L && dmlSeq == 0L) return null;
        return new Snapshot(new TreeMap<>(values), hash, dmlSeq);
    }

    /**
     * Restore the state captured by {@link #snapshot()}. {@code null}
     * means "snapshot was taken on an empty state" — restore the empty
     * state directly. Idempotent — restoring twice from the same snapshot
     * leaves the state in the same place.
     */
    public void restoreOrReset(Snapshot snap) {
        values.clear();
        if (snap != null) {
            values.putAll(snap.values);
            hash = snap.hash;
            dmlSeq = snap.dmlSeq;
        } else {
            hash = 0L;
            dmlSeq = 0L;
        }
    }

    /**
     * Apply a parsed {@link SetCommand} to the state. No-op for
     * {@link SetCommand.Kind#SET_LOCAL} (transient — see class doc) and for
     * safe GUC names.
     */
    public void apply(SetCommand cmd) {
        switch (cmd.kind) {
            case SET:
                if (isUnsafeGuc(cmd.name)) {
                    values.put(cmd.name, cmd.value);
                    recomputeHash();
                }
                break;
            case SET_LOCAL:
                // Intentionally ignored. SET LOCAL only takes effect inside a
                // transaction, and the cache layer is already gated on
                // !inTransaction — so SET LOCAL never influences a cacheable
                // response.
                break;
            case RESET:
                if (isUnsafeGuc(cmd.name) && values.remove(cmd.name) != null) {
                    recomputeHash();
                }
                break;
            case RESET_ALL:
            case DISCARD_ALL:
                // DISCARD ALL is RESET ALL plus prepared-statement / temp-table
                // teardown. The wrapper doesn't maintain a prepared-statement
                // cache (the JDBC PreparedStatement objects belong to the
                // driver, not us), so the state-map effect is identical to
                // RESET ALL: drop every tracked unsafe GUC AND reset
                // {@link #dmlSeq} so the connection re-converges to the
                // peer-shareable baseline (a recycled connection should land
                // on hash {@code 0} so its cache entries can be hit by any
                // other connection).
                boolean hadState = !values.isEmpty() || dmlSeq != 0L;
                if (hadState) {
                    values.clear();
                    dmlSeq = 0L;
                    recomputeHash();
                }
                break;
        }
    }

    /**
     * Parse {@code sql} for any {@code SET} / {@code RESET} / {@code DISCARD}
     * command, plus the {@code SELECT set_config(...)} function form, and
     * apply each. Multi-statement bodies (string-literal-aware) are split on
     * top-level {@code ;} so e.g. {@code "SET app.user_id='42'; SELECT 1"}
     * still updates the state. Returns {@code true} iff the hash mutated.
     */
    public boolean observeSql(String sql) {
        if (sql == null) return false;
        long before = hash;
        // Fast path for the common single-statement case — avoid allocating
        // the split list for every wire message that isn't a multi-statement
        // body. We trim trailing whitespace and a trailing ';' before checking
        // for inner ';' so "SET foo='1';" stays single-statement.
        String trimmed = sql.stripTrailing();
        if (trimmed.endsWith(";")) trimmed = trimmed.substring(0, trimmed.length() - 1);
        if (trimmed.indexOf(';') < 0) {
            observeSingleStatement(sql);
        } else {
            for (String stmt : splitStatements(sql)) {
                observeSingleStatement(stmt);
            }
        }
        return hash != before;
    }

    /**
     * Apply any state-affecting command found in a single SQL statement.
     * Dispatches across the recognised forms in fall-through order:
     * {@code SET} / {@code RESET} / {@code DISCARD} → {@link #parseSetCommand},
     * then {@code SELECT [pg_catalog.]set_config(...)} →
     * {@link #parseSetConfigCall}. Anything else is a no-op.
     */
    private void observeSingleStatement(String stmt) {
        SetCommand cmd = parseSetCommand(stmt);
        if (cmd != null) {
            apply(cmd);
            return;
        }
        SetCommand fnCmd = parseSetConfigCall(stmt);
        if (fnCmd != null) {
            apply(fnCmd);
        }
    }

    private void recomputeHash() {
        // Empty values + zero dmlSeq is the canonical "fresh connection"
        // state — keep the hash exactly 0 so the cache-slot-sharing
        // semantics are preserved for the common case (no unsafe SETs, no
        // DMLs yet). Mirrors the proxy's ConnectionGucState::recompute_hash
        // in src/guc_state.rs.
        if (values.isEmpty() && dmlSeq == 0L) {
            hash = 0L;
            return;
        }
        // Fold each (key, value) pair into a 64-bit FNV-1a-style accumulator.
        // Using a hand-rolled combiner instead of Objects.hash(...) so we
        // (a) avoid Object[] allocation per mutation and (b) get full 64 bits
        // (Objects.hash collapses to int). TreeMap iteration is sorted so the
        // accumulator is invariant to insertion order — same property the
        // Rust side gets from BTreeMap.
        long acc = 0xcbf29ce484222325L; // FNV offset basis
        for (Map.Entry<String, String> e : values.entrySet()) {
            acc = mixString(acc, e.getKey());
            // Field separator so {"ab", "c"} hashes differently from {"a", "bc"}.
            acc ^= 0x1FL;
            acc *= 0x100000001b3L;
            acc = mixString(acc, e.getValue());
            // Record separator between (k,v) pairs.
            acc ^= 0x1EL;
            acc *= 0x100000001b3L;
        }
        // Mix in the post-DML sequence so each DML rolls the cache key
        // forward. Mixed last (after the BTreeMap) so a connection with no
        // SETs but a non-zero dmlSeq still gets a unique hash distinct
        // from any peer's "no DMLs yet" baseline.
        long seq = dmlSeq;
        for (int i = 0; i < 8; i++) {
            acc ^= (seq & 0xFFL);
            acc *= 0x100000001b3L;
            seq >>>= 8;
        }
        // Avoid the sentinel 0 (which means "empty / baseline state"). Vanishingly
        // rare collision with a real non-empty state, but we'd rather not have to
        // reason about it elsewhere. Map 0 → 1 as the canonical "non-empty
        // collision sentinel" hash.
        if (acc == 0L) acc = 1L;
        hash = acc;
    }

    private static long mixString(long acc, String s) {
        for (int i = 0; i < s.length(); i++) {
            acc ^= (long) s.charAt(i);
            acc *= 0x100000001b3L; // FNV prime
        }
        return acc;
    }

    /**
     * Split {@code sql} on top-level {@code ;} characters, respecting single-
     * and double-quoted string literals (with PG's doubled-quote escape:
     * {@code ''}, {@code ""}). Returns each non-empty trimmed segment.
     *
     * <p>Lightest possible "statement splitter" — does not understand dollar-
     * quoted strings, comments, or any other lexical nuance. Good enough for
     * splitting {@code SET ...; SELECT 1}-style multi-statement bodies, which
     * is the entire reason it exists.
     */
    public static String[] splitStatements(String sql) {
        if (sql == null || sql.isEmpty()) return new String[0];
        java.util.ArrayList<String> out = new java.util.ArrayList<>();
        int len = sql.length();
        int start = 0;
        char quote = 0; // 0 = not in a quoted region
        for (int i = 0; i < len; i++) {
            char c = sql.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    // Doubled quote escape: '' or "" — skip both characters.
                    if (i + 1 < len && sql.charAt(i + 1) == quote) {
                        i++;
                        continue;
                    }
                    quote = 0;
                }
            } else {
                if (c == '\'' || c == '"') {
                    quote = c;
                } else if (c == ';') {
                    String segment = sql.substring(start, i).trim();
                    if (!segment.isEmpty()) out.add(segment);
                    start = i + 1;
                }
            }
        }
        String tail = sql.substring(start).trim();
        if (!tail.isEmpty()) out.add(tail);
        return out.toArray(new String[0]);
    }

    /**
     * Parse a single {@code SET} / {@code RESET} statement, returning
     * {@code null} for anything that isn't one we track.
     *
     * <p>Recognises:
     * <ul>
     *   <li>{@code SET name = value}, {@code SET name TO value}</li>
     *   <li>{@code SET SESSION name = value}, {@code SET SESSION name TO value}</li>
     *   <li>{@code SET LOCAL name = value}, {@code SET LOCAL name TO value}</li>
     *   <li>{@code RESET name}</li>
     *   <li>{@code RESET ALL}</li>
     * </ul>
     *
     * <p>Returns {@code null} for anything else, including {@code SET TIME ZONE 'UTC'}
     * (the legacy two-word form) — timezone is harmless and the unusual two-word
     * GUC name doesn't fit the pattern; "not-a-trackable-SET" is the correct
     * outcome because it doesn't affect cache safety.
     *
     * <p>For multi-statement SQL bodies, call {@link #splitStatements} first.
     */
    public static SetCommand parseSetCommand(String sql) {
        if (sql == null) return null;
        String s = sql.trim();
        if (s.endsWith(";")) s = s.substring(0, s.length() - 1).trim();
        if (s.isEmpty()) return null;

        String[] tokens = s.split("\\s+");
        if (tokens.length == 0) return null;

        String head = tokens[0];

        // DISCARD branch. PG accepts:
        //   DISCARD ALL          → drop session state (params, plans, temp, sequences)
        //   DISCARD PLANS        → prepared-statement cache only
        //   DISCARD SEQUENCES    → cached sequence info only
        //   DISCARD TEMP         → temp-table teardown
        //   DISCARD TEMPORARY    → alias of TEMP
        // Only ALL touches GUC values; the rest leave session params alone, so
        // we treat them as no-ops by returning null. The wrapper doesn't keep
        // its own prepared-statement cache (those are JDBC driver objects), so
        // DISCARD PLANS in particular needs no extra bookkeeping.
        if (head.equalsIgnoreCase("DISCARD")) {
            if (tokens.length != 2) return null;
            String target = tokens[1];
            if (target.equalsIgnoreCase("ALL")) {
                return new SetCommand(SetCommand.Kind.DISCARD_ALL, null, null);
            }
            // Recognised targets that aren't ALL — explicit no-ops.
            if (target.equalsIgnoreCase("PLANS")
                || target.equalsIgnoreCase("SEQUENCES")
                || target.equalsIgnoreCase("TEMP")
                || target.equalsIgnoreCase("TEMPORARY")) {
                return null;
            }
            // Unknown DISCARD target — be conservative and ignore. PG would
            // raise a syntax error before we ever saw the result; nothing for
            // us to mutate.
            return null;
        }

        // RESET branch.
        if (head.equalsIgnoreCase("RESET")) {
            if (tokens.length < 2 || tokens.length > 2) return null;
            String target = tokens[1];
            if (target.equalsIgnoreCase("ALL")) {
                return new SetCommand(SetCommand.Kind.RESET_ALL, null, null);
            }
            String name = normalizeGucName(target);
            if (name == null) return null;
            return new SetCommand(SetCommand.Kind.RESET, name, null);
        }

        if (!head.equalsIgnoreCase("SET")) return null;

        // Optional LOCAL / SESSION modifier. PG's grammar permits both;
        // SESSION is the default and behaves the same as bare SET.
        int idx = 1;
        if (idx >= tokens.length) return null;
        boolean isLocal = false;
        if (tokens[idx].equalsIgnoreCase("LOCAL")) {
            isLocal = true;
            idx++;
        } else if (tokens[idx].equalsIgnoreCase("SESSION")) {
            idx++;
        }
        if (idx >= tokens.length) return null;

        // tokens[idx] is the GUC name — but it may have an `=` glued onto it
        // (e.g. SET app.user='42'). Split on the first `=` if present.
        String nameToken = tokens[idx];
        String gluedValue = null;
        int eqIdx = nameToken.indexOf('=');
        if (eqIdx >= 0) {
            gluedValue = nameToken.substring(eqIdx + 1);
            nameToken = nameToken.substring(0, eqIdx);
            if (gluedValue.isEmpty()) gluedValue = null;
        }
        idx++;

        String name = normalizeGucName(nameToken);
        if (name == null) return null;

        String valueStr;
        if (gluedValue != null) {
            // SET name=value [extra ...]
            String rest = idx < tokens.length
                ? String.join(" ", Arrays.copyOfRange(tokens, idx, tokens.length))
                : "";
            valueStr = rest.isEmpty() ? gluedValue : gluedValue + " " + rest;
        } else {
            if (idx >= tokens.length) return null;
            String sep = tokens[idx];
            if (!sep.equals("=") && !sep.equalsIgnoreCase("TO")) return null;
            idx++;
            if (idx >= tokens.length) return null;
            valueStr = String.join(" ", Arrays.copyOfRange(tokens, idx, tokens.length));
        }

        String value = stripValueQuotes(valueStr.trim());
        if (value.isEmpty() && valueStr.trim().isEmpty()) return null;

        return new SetCommand(
            isLocal ? SetCommand.Kind.SET_LOCAL : SetCommand.Kind.SET,
            name,
            value
        );
    }

    /**
     * Whether {@code sql} is a top-level call into a stored function or
     * procedure — i.e. the wrapper should schedule a post-call GUC-state
     * verify because the body could have issued SETs we never saw on the wire.
     * Recognises both {@code SELECT <ident>(...)} (with optional
     * {@code schema.}) and {@code CALL <ident>(...)} forms; rejects
     * {@code SELECT * FROM t}, {@code SELECT 1 + 2}, {@code SELECT col FROM
     * t} (no parens after the first identifier), and the
     * {@code SELECT set_config(...)} pattern (which the parser handles
     * directly, no verify needed).
     *
     * <p>This is a static syntactic check — it doesn't try to introspect
     * what the function actually does. False positives ("looks like a
     * function call but is read-only") just incur an extra
     * {@code pg_settings} scan; harmless. False negatives ("is a function
     * call but doesn't match the syntax") fall back to the
     * {@link #isDirty}-by-other-means path or are out-of-scope per the
     * GUC-RLS doc.
     */
    public static boolean isFunctionCall(String sql) {
        if (sql == null) return false;
        String s = sql.trim();
        if (s.endsWith(";")) s = s.substring(0, s.length() - 1).trim();
        if (s.isEmpty()) return false;

        boolean isCall;
        int idx;
        // SELECT <fn>(...) or CALL <proc>(...). PERFORM is plpgsql-only.
        if (s.length() >= 6 && s.substring(0, 6).equalsIgnoreCase("SELECT")
            && (s.length() == 6 || Character.isWhitespace(s.charAt(6)))) {
            isCall = false;
            idx = 6;
        } else if (s.length() >= 4 && s.substring(0, 4).equalsIgnoreCase("CALL")
            && (s.length() == 4 || Character.isWhitespace(s.charAt(4)))) {
            isCall = true;
            idx = 4;
        } else {
            return false;
        }

        // Skip whitespace.
        while (idx < s.length() && Character.isWhitespace(s.charAt(idx))) idx++;
        if (idx >= s.length()) return false;

        // Read identifier (possibly schema-qualified). PG identifiers are
        // [A-Za-z_][A-Za-z0-9_$]* — we accept '.' as a schema separator and
        // bail on anything else. Quoted identifiers with embedded spaces are
        // rare in this position; if they appear we conservatively return
        // false (a verify miss is safer than a syntactic-edge-case false
        // positive that turns into noisy verify load).
        int identStart = idx;
        while (idx < s.length()) {
            char c = s.charAt(idx);
            if (Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '.') {
                idx++;
            } else {
                break;
            }
        }
        if (idx == identStart) return false;
        String ident = s.substring(identStart, idx).toLowerCase(java.util.Locale.ROOT);

        // Skip whitespace, then expect '('.
        while (idx < s.length() && Character.isWhitespace(s.charAt(idx))) idx++;
        if (idx >= s.length() || s.charAt(idx) != '(') return false;

        // SELECT path: exclude obvious read-only catalog functions that don't
        // fit our verify-after-call model — these are hot in real apps and
        // verifying after every one would burn pg_settings reads with no
        // safety benefit. We deliberately do NOT exclude set_config here:
        // the inline parser handles literal-arg shapes, but a non-literal
        // form like {@code SELECT set_config(name_var, '42', false)} bypasses
        // the inline parser; firing a verify on those catches the SET we'd
        // otherwise miss. Cost on the literal-arg path is one extra
        // pg_settings round-trip per call — acceptable given how rare
        // set_config calls are in practice.
        if (!isCall) {
            if (ident.equals("current_setting") || ident.equals("pg_catalog.current_setting")) return false;
            if (ident.equals("version") || ident.equals("pg_catalog.version")) return false;
            if (ident.equals("current_user") || ident.equals("session_user")) return false;
            if (ident.equals("now") || ident.equals("pg_catalog.now")) return false;
        }
        return true;
    }

    /**
     * Parse a {@code SELECT set_config(name, value, is_local)} or
     * {@code SELECT pg_catalog.set_config(...)} call into a {@link SetCommand}.
     * Supabase / PostgREST emit this form to apply per-request JWT claims to
     * the session, so the wrapper's wire-side observation has to recognise it
     * as a state mutation just like {@code SET}.
     *
     * <p>Matches case-insensitively, schema-qualified or bare. The function
     * has exactly three arguments — {@code (name text, value text, is_local
     * bool)}; the boolean third arg controls whether the SET is transient
     * ({@code true} → equivalent to {@code SET LOCAL}) or session-wide
     * ({@code false}). Anything else (different arity, non-string-literal
     * arguments, more SQL after the closing paren) returns {@code null} and
     * the caller falls through to its non-SET path.
     *
     * <p>Returns {@code null} for non-{@code set_config} statements,
     * {@code null} for non-literal arguments (we can't evaluate
     * {@code current_user_id()} from outside the server), and never throws.
     */
    public static SetCommand parseSetConfigCall(String sql) {
        if (sql == null) return null;
        String s = sql.trim();
        // Strip a single trailing ';' to match parseSetCommand's tolerance.
        if (s.endsWith(";")) s = s.substring(0, s.length() - 1).trim();
        if (s.isEmpty()) return null;

        // Must start with SELECT (case-insensitive). PERFORM is plpgsql-only,
        // never on the wire. CALL doesn't apply — set_config is a function.
        if (s.length() < 6 || !s.substring(0, 6).equalsIgnoreCase("SELECT")) return null;
        // Skip whitespace after SELECT.
        int idx = 6;
        while (idx < s.length() && Character.isWhitespace(s.charAt(idx))) idx++;
        if (idx >= s.length()) return null;

        // Optional schema qualifier "pg_catalog." (case-insensitive).
        if (idx + 11 <= s.length() && s.substring(idx, idx + 11).equalsIgnoreCase("pg_catalog.")) {
            idx += 11;
        }

        // Match the function name literal "set_config".
        final String FN = "set_config";
        if (idx + FN.length() > s.length()) return null;
        if (!s.substring(idx, idx + FN.length()).equalsIgnoreCase(FN)) return null;
        idx += FN.length();

        // Optional whitespace then '('.
        while (idx < s.length() && Character.isWhitespace(s.charAt(idx))) idx++;
        if (idx >= s.length() || s.charAt(idx) != '(') return null;
        idx++;

        // Find the matching ')' — string-literal-aware so a ',' inside a
        // quoted value doesn't split the argument list.
        int depth = 1;
        int closeIdx = -1;
        char quote = 0;
        for (int i = idx; i < s.length(); i++) {
            char c = s.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    if (i + 1 < s.length() && s.charAt(i + 1) == quote) {
                        i++;
                        continue;
                    }
                    quote = 0;
                }
            } else {
                if (c == '\'' || c == '"') quote = c;
                else if (c == '(') depth++;
                else if (c == ')') {
                    depth--;
                    if (depth == 0) {
                        closeIdx = i;
                        break;
                    }
                }
            }
        }
        if (closeIdx < 0) return null;

        // Anything after the close paren that isn't whitespace disqualifies —
        // we recognise the bare call only, not e.g. "SELECT set_config(...)
        // FROM dual" or arithmetic on the result.
        for (int i = closeIdx + 1; i < s.length(); i++) {
            if (!Character.isWhitespace(s.charAt(i))) return null;
        }

        // Split the inside-parens region on top-level ','s (string-literal-aware
        // splitter, same rules as splitStatements but on a different separator).
        String[] args = splitArgs(s.substring(idx, closeIdx));
        if (args.length != 3) return null;

        String nameArg = args[0].trim();
        String valueArg = args[1].trim();
        String localArg = args[2].trim();

        // name and value must be string literals — we don't evaluate
        // expressions from this side of the wire. (A non-literal value means
        // the caller is computing it server-side; we conservatively bail.)
        if (!isQuotedLiteral(nameArg) || !isQuotedLiteral(valueArg)) return null;
        // is_local must be a literal boolean — true / false (case-insensitive)
        // or the SQL int forms 0/1 some drivers serialize.
        Boolean isLocal = parseBooleanLiteral(localArg);
        if (isLocal == null) return null;

        String name = normalizeGucName(stripValueQuotes(nameArg));
        if (name == null) return null;
        String value = stripValueQuotes(valueArg);

        return new SetCommand(
            isLocal ? SetCommand.Kind.SET_LOCAL : SetCommand.Kind.SET,
            name,
            value
        );
    }

    /**
     * Split a function-argument list on top-level commas, respecting both
     * single- and double-quoted literals (with PG's doubled-quote escape) and
     * nested parens. Used by {@link #parseSetConfigCall}; not a general
     * SQL-expression parser.
     */
    private static String[] splitArgs(String inside) {
        java.util.ArrayList<String> out = new java.util.ArrayList<>();
        int depth = 0;
        int start = 0;
        char quote = 0;
        for (int i = 0; i < inside.length(); i++) {
            char c = inside.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    if (i + 1 < inside.length() && inside.charAt(i + 1) == quote) {
                        i++;
                        continue;
                    }
                    quote = 0;
                }
            } else {
                if (c == '\'' || c == '"') quote = c;
                else if (c == '(') depth++;
                else if (c == ')') depth--;
                else if (c == ',' && depth == 0) {
                    out.add(inside.substring(start, i));
                    start = i + 1;
                }
            }
        }
        out.add(inside.substring(start));
        return out.toArray(new String[0]);
    }

    /** Whether {@code s} is a single- or double-quoted string literal (after trim). */
    private static boolean isQuotedLiteral(String s) {
        if (s == null || s.length() < 2) return false;
        char first = s.charAt(0);
        char last = s.charAt(s.length() - 1);
        return (first == '\'' && last == '\'') || (first == '"' && last == '"');
    }

    /**
     * Parse a SQL boolean literal (case-insensitive {@code true}/{@code false}
     * or the int forms {@code 1}/{@code 0}). Returns {@code null} for anything
     * else.
     */
    private static Boolean parseBooleanLiteral(String s) {
        if (s == null) return null;
        if (s.equalsIgnoreCase("true") || s.equals("1")) return Boolean.TRUE;
        if (s.equalsIgnoreCase("false") || s.equals("0")) return Boolean.FALSE;
        // Quoted forms PG accepts when cast: 't'::bool, 'f'::bool, 'yes', 'no'.
        // We see them as quoted literals — strip and recurse on the inner text
        // for the common "$bool"::bool case some drivers emit.
        if (isQuotedLiteral(s)) {
            String inner = stripValueQuotes(s).trim().toLowerCase(java.util.Locale.ROOT);
            switch (inner) {
                case "true": case "t": case "yes": case "on": case "1":
                    return Boolean.TRUE;
                case "false": case "f": case "no": case "off": case "0":
                    return Boolean.FALSE;
                default:
                    return null;
            }
        }
        return null;
    }

    /**
     * Lowercase the GUC name and strip surrounding double quotes (PG treats
     * {@code "app.user_id"} and {@code app.user_id} as the same identifier
     * when it's a configuration parameter; double-quoted form just preserves
     * case, which we discard anyway).
     */
    private static String normalizeGucName(String token) {
        if (token == null) return null;
        String t = token;
        if (t.length() >= 2 && t.charAt(0) == '"' && t.charAt(t.length() - 1) == '"') {
            t = t.substring(1, t.length() - 1);
        }
        if (t.isEmpty()) return null;
        return t.toLowerCase(java.util.Locale.ROOT);
    }

    /**
     * Strip a single layer of matching surrounding quotes ({@code '...'} or
     * {@code "..."}) from a value. Multi-token quoted values like
     * {@code 'foo bar'} arrive as the joined string already; this just peels
     * the outer quotes. Unquoted values are returned trimmed.
     */
    private static String stripValueQuotes(String value) {
        if (value == null) return "";
        String v = value.trim();
        if (v.length() >= 2) {
            char first = v.charAt(0);
            char last = v.charAt(v.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                return v.substring(1, v.length() - 1);
            }
        }
        return v;
    }

    /**
     * Parsed {@code SET} / {@code RESET} command. Immutable value object —
     * use the {@code public final} fields directly (no accessors; this is a
     * private-API DTO).
     */
    public static final class SetCommand {
        public enum Kind { SET, SET_LOCAL, RESET, RESET_ALL, DISCARD_ALL }

        public final Kind kind;
        /** Lowercased GUC name. {@code null} for {@link Kind#RESET_ALL}. */
        public final String name;
        /** Unquoted value. {@code null} for {@code RESET} / {@code RESET_ALL}. */
        public final String value;

        public SetCommand(Kind kind, String name, String value) {
            this.kind = kind;
            this.name = name;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SetCommand)) return false;
            SetCommand other = (SetCommand) o;
            return kind == other.kind
                && java.util.Objects.equals(name, other.name)
                && java.util.Objects.equals(value, other.value);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(kind, name, value);
        }

        @Override
        public String toString() {
            return "SetCommand{" + kind + ", " + name + "=" + value + "}";
        }
    }
}
