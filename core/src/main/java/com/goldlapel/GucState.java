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
     */
    private static final Set<String> UNSAFE_GUC_SHORT_LIST = Set.of(
        "search_path",
        "role",
        "session_authorization",
        "default_transaction_isolation",
        "default_transaction_read_only",
        "transaction_isolation",
        "row_security"
    );

    /**
     * Lowercased GUC name → raw value string. Only unsafe GUCs are present;
     * harmless GUCs (timezone, application_name, planner cost knobs, etc.)
     * never enter this map and never affect the hash. {@link TreeMap} for
     * stable iteration order so the hash is invariant to insertion order.
     */
    private final TreeMap<String, String> values = new TreeMap<>();

    /**
     * Cached hash of {@link #values}, recomputed on every mutation. {@code 0}
     * for the empty (default) state — a fresh connection's hash must match
     * "no GUCs set" cache slots populated by peers, which is exactly what we
     * want. {@code volatile} for cross-thread visibility (see class doc).
     */
    private volatile long hash = 0L;

    /** Current state hash. {@code 0} for the empty (baseline) state. */
    public long hash() {
        return hash;
    }

    /** Number of unsafe GUCs currently tracked. Visible for testing. */
    int size() {
        return values.size();
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
                if (!values.isEmpty()) {
                    values.clear();
                    recomputeHash();
                }
                break;
        }
    }

    /**
     * Parse {@code sql} for any {@code SET} / {@code RESET} commands and
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
            SetCommand cmd = parseSetCommand(sql);
            if (cmd != null) apply(cmd);
        } else {
            for (String stmt : splitStatements(sql)) {
                SetCommand cmd = parseSetCommand(stmt);
                if (cmd != null) apply(cmd);
            }
        }
        return hash != before;
    }

    private void recomputeHash() {
        if (values.isEmpty()) {
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
        public enum Kind { SET, SET_LOCAL, RESET, RESET_ALL }

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
