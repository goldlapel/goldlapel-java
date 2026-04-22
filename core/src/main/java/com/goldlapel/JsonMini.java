package com.goldlapel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal, dependency-free JSON parser sufficient for consuming the proxy's
 * DDL API responses. Handles objects, arrays, strings, numbers (Long/Double),
 * booleans, and null. Not a full JSON spec implementation — no scientific
 * notation for integers, no deep-nested-array optimizations.
 *
 * <p>Kept package-private; used by {@link Ddl} only. We deliberately avoid a
 * runtime dependency on Jackson/Gson — Maven Central downloads for this
 * wrapper should stay minimal.
 */
final class JsonMini {
    private final String s;
    private int i;

    private JsonMini(String s) { this.s = s; }

    /** Parse a JSON string expected to be an object. Returns a {@code Map<String, Object>}. */
    @SuppressWarnings("unchecked")
    static Map<String, Object> parseObject(String input) {
        if (input == null || input.isEmpty()) return new LinkedHashMap<>();
        JsonMini p = new JsonMini(input);
        p.skipWs();
        Object v = p.parseValue();
        if (!(v instanceof Map)) {
            // If the body isn't an object (e.g. empty, error text), return an
            // empty map — callers check keys individually with null defaults.
            return new LinkedHashMap<>();
        }
        return (Map<String, Object>) v;
    }

    private Object parseValue() {
        skipWs();
        if (i >= s.length()) throw new IllegalArgumentException("Unexpected end of JSON");
        char c = s.charAt(i);
        if (c == '{') return parseMap();
        if (c == '[') return parseList();
        if (c == '"') return parseString();
        if (c == 't' || c == 'f') return parseBool();
        if (c == 'n') return parseNull();
        return parseNumber();
    }

    private Map<String, Object> parseMap() {
        expect('{');
        Map<String, Object> m = new LinkedHashMap<>();
        skipWs();
        if (peek() == '}') { i++; return m; }
        while (true) {
            skipWs();
            String key = parseString();
            skipWs();
            expect(':');
            Object val = parseValue();
            m.put(key, val);
            skipWs();
            char c = s.charAt(i);
            if (c == ',') { i++; continue; }
            if (c == '}') { i++; return m; }
            throw new IllegalArgumentException("Expected , or } at pos " + i);
        }
    }

    private List<Object> parseList() {
        expect('[');
        List<Object> l = new ArrayList<>();
        skipWs();
        if (peek() == ']') { i++; return l; }
        while (true) {
            Object val = parseValue();
            l.add(val);
            skipWs();
            char c = s.charAt(i);
            if (c == ',') { i++; continue; }
            if (c == ']') { i++; return l; }
            throw new IllegalArgumentException("Expected , or ] at pos " + i);
        }
    }

    private String parseString() {
        expect('"');
        StringBuilder b = new StringBuilder();
        while (i < s.length()) {
            char c = s.charAt(i++);
            if (c == '"') return b.toString();
            if (c == '\\') {
                if (i >= s.length()) throw new IllegalArgumentException("Bad escape at EOF");
                char esc = s.charAt(i++);
                switch (esc) {
                    case '"': b.append('"'); break;
                    case '\\': b.append('\\'); break;
                    case '/': b.append('/'); break;
                    case 'b': b.append('\b'); break;
                    case 'f': b.append('\f'); break;
                    case 'n': b.append('\n'); break;
                    case 'r': b.append('\r'); break;
                    case 't': b.append('\t'); break;
                    case 'u':
                        if (i + 4 > s.length()) throw new IllegalArgumentException("Bad \\u escape");
                        int ch = Integer.parseInt(s.substring(i, i + 4), 16);
                        b.append((char) ch);
                        i += 4;
                        break;
                    default:
                        throw new IllegalArgumentException("Bad escape: \\" + esc);
                }
            } else {
                b.append(c);
            }
        }
        throw new IllegalArgumentException("Unterminated string");
    }

    private Boolean parseBool() {
        if (s.startsWith("true", i)) { i += 4; return Boolean.TRUE; }
        if (s.startsWith("false", i)) { i += 5; return Boolean.FALSE; }
        throw new IllegalArgumentException("Bad bool at pos " + i);
    }

    private Object parseNull() {
        if (s.startsWith("null", i)) { i += 4; return null; }
        throw new IllegalArgumentException("Bad null at pos " + i);
    }

    private Number parseNumber() {
        int start = i;
        if (i < s.length() && s.charAt(i) == '-') i++;
        while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
        boolean isDouble = false;
        if (i < s.length() && s.charAt(i) == '.') {
            isDouble = true;
            i++;
            while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
        }
        if (i < s.length() && (s.charAt(i) == 'e' || s.charAt(i) == 'E')) {
            isDouble = true;
            i++;
            if (i < s.length() && (s.charAt(i) == '+' || s.charAt(i) == '-')) i++;
            while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
        }
        String num = s.substring(start, i);
        return isDouble ? (Number) Double.parseDouble(num) : (Number) Long.parseLong(num);
    }

    private void skipWs() {
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') i++;
            else break;
        }
    }

    private char peek() { return s.charAt(i); }

    private void expect(char c) {
        if (i >= s.length() || s.charAt(i) != c) {
            throw new IllegalArgumentException("Expected '" + c + "' at pos " + i);
        }
        i++;
    }
}
