package com.goldlapel;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.Connection;
import java.sql.DriverManager;

public class GoldLapel {

    static final int DEFAULT_PORT = 7932;
    static final int DEFAULT_DASHBOARD_PORT = 7933;
    static final long STARTUP_TIMEOUT_MS = 10000;
    static final long STARTUP_POLL_INTERVAL_MS = 50;

    private static final Set<String> VALID_CONFIG_KEYS;
    private static final Set<String> BOOLEAN_KEYS;
    private static final Set<String> LIST_KEYS;

    static {
        Set<String> keys = new HashSet<>();
        Collections.addAll(keys,
            "mode", "minPatternCount", "refreshIntervalSecs", "patternTtlSecs",
            "maxTablesPerView", "maxColumnsPerView", "deepPaginationThreshold",
            "reportIntervalSecs", "resultCacheSize", "batchCacheSize",
            "batchCacheTtlSecs", "poolSize", "poolTimeoutSecs",
            "poolMode", "mgmtIdleTimeout", "fallback", "readAfterWriteSecs",
            "n1Threshold", "n1WindowMs", "n1CrossThreshold",
            "tlsCert", "tlsKey", "tlsClientCa", "config", "dashboardPort",
            "disableMatviews", "disableConsolidation", "disableBtreeIndexes",
            "disableTrigramIndexes", "disableExpressionIndexes",
            "disablePartialIndexes", "disableRewrite", "disablePreparedCache",
            "disableResultCache", "disablePool",
            "disableN1", "disableN1CrossConnection", "disableShadowMode",
            "enableCoalescing", "replica", "excludeTables",
            "invalidationPort"
        );
        VALID_CONFIG_KEYS = Collections.unmodifiableSet(keys);

        Set<String> bools = new HashSet<>();
        Collections.addAll(bools,
            "disableMatviews", "disableConsolidation", "disableBtreeIndexes",
            "disableTrigramIndexes", "disableExpressionIndexes",
            "disablePartialIndexes", "disableRewrite", "disablePreparedCache",
            "disableResultCache", "disablePool",
            "disableN1", "disableN1CrossConnection", "disableShadowMode",
            "enableCoalescing"
        );
        BOOLEAN_KEYS = Collections.unmodifiableSet(bools);

        Set<String> lists = new HashSet<>();
        Collections.addAll(lists, "replica", "excludeTables");
        LIST_KEYS = Collections.unmodifiableSet(lists);
    }

    private final String upstream;
    private final int port;
    private final int dashboardPort;
    private final Map<String, Object> config;
    private final List<String> extraArgs;
    private final String client;
    private Process process;
    private String proxyUrl;
    Connection wrappedConn;

    public GoldLapel(String upstream) {
        this(upstream, new Options());
    }

    public GoldLapel(String upstream, Options options) {
        this.upstream = upstream;
        this.port = options.port != null ? options.port : DEFAULT_PORT;
        this.dashboardPort = options.config != null && options.config.containsKey("dashboardPort")
            ? ((Number) options.config.get("dashboardPort")).intValue()
            : DEFAULT_DASHBOARD_PORT;
        this.config = options.config;
        this.extraArgs = options.extraArgs != null ? options.extraArgs : new ArrayList<>();
        this.client = options.client != null ? options.client : "java";
        this.process = null;
        this.proxyUrl = null;
    }

    public String startProxy() {
        if (process != null && process.isAlive()) {
            return proxyUrl;
        }

        String binary = findBinary();
        List<String> cmd = new ArrayList<>();
        cmd.add(binary);
        cmd.add("--upstream");
        cmd.add(upstream);
        cmd.add("--port");
        cmd.add(String.valueOf(port));
        cmd.addAll(configToArgs(config));
        cmd.addAll(extraArgs);

        try {
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.environment().putIfAbsent("GOLDLAPEL_CLIENT", client);
            pb.redirectInput(ProcessBuilder.Redirect.PIPE);
            pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
            pb.redirectError(ProcessBuilder.Redirect.PIPE);
            process = pb.start();
            process.getOutputStream().close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start Gold Lapel process", e);
        }

        // Drain stderr on a daemon thread to prevent pipe-buffer deadlock
        StringBuilder stderrBuf = new StringBuilder();
        Thread stderrDrain = new Thread(() -> {
            try {
                InputStream err = process.getErrorStream();
                byte[] buf = new byte[1024];
                int n;
                while ((n = err.read(buf)) != -1) {
                    stderrBuf.append(new String(buf, 0, n));
                }
            } catch (IOException ignored) {}
        });
        stderrDrain.setDaemon(true);
        stderrDrain.start();

        // Poll for port readiness, short-circuiting if the process exits early
        long deadline = System.nanoTime() + STARTUP_TIMEOUT_MS * 1_000_000L;
        boolean ready = false;
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) break;
            if (waitForPort("127.0.0.1", port, 500)) {
                ready = true;
                break;
            }
        }

        if (!ready) {
            process.destroyForcibly();
            try { process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
            try { stderrDrain.join(2000); } catch (InterruptedException ignored) {}
            throw new RuntimeException(
                "Gold Lapel failed to start on port " + port +
                " within " + (STARTUP_TIMEOUT_MS / 1000) + "s.\nstderr: " + stderrBuf
            );
        }

        proxyUrl = makeProxyUrl(upstream, port);

        if (dashboardPort > 0) {
            System.out.println("goldlapel → :" + port + " (proxy) | http://127.0.0.1:" + dashboardPort + " (dashboard)");
        } else {
            System.out.println("goldlapel → :" + port + " (proxy)");
        }

        return proxyUrl;
    }

    public void stopProxy() {
        Process proc = process;
        process = null;
        proxyUrl = null;
        if (proc != null && proc.isAlive()) {
            proc.destroy();
            try {
                if (!proc.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    proc.destroyForcibly();
                    proc.waitFor();
                }
            } catch (InterruptedException e) {
                proc.destroyForcibly();
                Thread.currentThread().interrupt();
            }
        }
    }

    public String getUrl() {
        return proxyUrl;
    }

    public int getPort() {
        return port;
    }

    public String getDashboardUrl() {
        if (dashboardPort > 0 && process != null && process.isAlive()) {
            return "http://127.0.0.1:" + dashboardPort;
        }
        return null;
    }

    public boolean isRunning() {
        return process != null && process.isAlive();
    }

    public static Set<String> configKeys() {
        return Collections.unmodifiableSet(VALID_CONFIG_KEYS);
    }

    // ── Config ─────────────────────────────────────────────

    static String camelToKebab(String key) {
        StringBuilder sb = new StringBuilder();
        for (char c : key.toCharArray()) {
            if (Character.isUpperCase(c)) {
                sb.append('-').append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    static List<String> configToArgs(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> args = new ArrayList<>();

        for (Map.Entry<String, Object> entry : config.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (!VALID_CONFIG_KEYS.contains(key)) {
                throw new IllegalArgumentException("Unknown config key: " + key);
            }

            String flag = "--" + camelToKebab(key);

            if (BOOLEAN_KEYS.contains(key)) {
                if (!(value instanceof Boolean)) {
                    throw new IllegalArgumentException(
                        "Config key '" + key + "' must be a Boolean, got " +
                        value.getClass().getSimpleName()
                    );
                }
                if ((Boolean) value) {
                    args.add(flag);
                }
            } else if (LIST_KEYS.contains(key)) {
                if (!(value instanceof List)) {
                    throw new IllegalArgumentException(
                        "Config key '" + key + "' must be a List, got " +
                        value.getClass().getSimpleName()
                    );
                }
                List<?> items = (List<?>) value;
                for (Object item : items) {
                    args.add(flag);
                    args.add(item.toString());
                }
            } else {
                args.add(flag);
                args.add(value.toString());
            }
        }

        return args;
    }

    // ── Options ────────────────────────────────────────────

    public static class Options {
        Integer port;
        Map<String, Object> config;
        List<String> extraArgs;
        String client;

        public Options port(int port) {
            this.port = port;
            return this;
        }

        public Options config(Map<String, Object> config) {
            this.config = config;
            return this;
        }

        public Map<String, Object> config() {
            return config;
        }

        public Options extraArgs(String... args) {
            this.extraArgs = Arrays.asList(args);
            return this;
        }

        public Options client(String client) {
            this.client = client;
            return this;
        }
    }

    // ── Module-level singleton ─────────────────────────────

    private static GoldLapel instance;
    private static boolean cleanupRegistered = false;

    public static synchronized Connection start(String upstream) {
        return start(upstream, new Options());
    }

    public static synchronized Connection start(String upstream, Options options) {
        if (instance != null && instance.isRunning()) {
            if (!instance.upstream.equals(upstream)) {
                throw new RuntimeException(
                    "Gold Lapel is already running for a different upstream. " +
                    "Call GoldLapel.stop() before starting with a new upstream."
                );
            }
            Connection cached = instance.wrappedConn;
            if (cached != null) return cached;
            Connection wrapped = tryWrapConnection(instance);
            if (wrapped == null) {
                throw new RuntimeException(
                    "No PostgreSQL JDBC driver found. Add org.postgresql:postgresql to your dependencies, " +
                    "or use GoldLapel.startUrl() to get the proxy URL and connect manually."
                );
            }
            return wrapped;
        }
        instance = new GoldLapel(upstream, options);
        if (!cleanupRegistered) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (instance != null) {
                    instance.stopProxy();
                    instance = null;
                }
            }));
            cleanupRegistered = true;
        }
        instance.startProxy();
        Connection wrapped = tryWrapConnection(instance);
        if (wrapped == null) {
            throw new RuntimeException(
                "No PostgreSQL JDBC driver found. Add org.postgresql:postgresql to your dependencies, " +
                "or use GoldLapel.startUrl() to get the proxy URL and connect manually."
            );
        }
        return wrapped;
    }

    public static synchronized String startUrl(String upstream) {
        return startUrl(upstream, new Options());
    }

    public static synchronized String startUrl(String upstream, Options options) {
        if (instance != null && instance.isRunning()) {
            if (!instance.upstream.equals(upstream)) {
                throw new RuntimeException(
                    "Gold Lapel is already running for a different upstream. " +
                    "Call GoldLapel.stop() before starting with a new upstream."
                );
            }
            return instance.getUrl();
        }
        instance = new GoldLapel(upstream, options);
        if (!cleanupRegistered) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (instance != null) {
                    instance.stopProxy();
                    instance = null;
                }
            }));
            cleanupRegistered = true;
        }
        return instance.startProxy();
    }

    private static Connection tryWrapConnection(GoldLapel inst) {
        try {
            Class.forName("org.postgresql.Driver");
            String url = inst.getUrl();
            // JDBC requires jdbc:postgresql:// prefix
            if (url.startsWith("postgres://")) {
                url = "jdbc:postgresql://" + url.substring("postgres://".length());
            } else if (url.startsWith("postgresql://")) {
                url = "jdbc:postgresql://" + url.substring("postgresql://".length());
            }
            Connection conn = DriverManager.getConnection(url);
            NativeCache cache = NativeCache.getInstance();
            int invPort = inst.port + 2;
            if (inst.config != null) {
                Object p = inst.config.get("invalidationPort");
                if (p != null) invPort = Integer.parseInt(p.toString());
            }
            cache.connectInvalidation(invPort);
            Connection wrapped = ConnectionProxy.wrap(conn, cache);
            inst.wrappedConn = wrapped;
            return wrapped;
        } catch (Exception e) {
            return null;
        }
    }

    public static synchronized void stop() {
        if (instance != null) {
            instance.stopProxy();
            instance = null;
        }
        NativeCache.reset();
    }

    public static String proxyUrl() {
        return instance != null ? instance.getUrl() : null;
    }

    public static String dashboardUrl() {
        return instance != null ? instance.getDashboardUrl() : null;
    }

    // ── Internal methods ───────────────────────────────────

    private static final Pattern WITH_PORT =
        Pattern.compile("^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+):(\\d+)(.*)$");

    private static final Pattern NO_PORT =
        Pattern.compile("^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+)(.*)$");

    static String findBinary() {
        // 1. Explicit override via env var
        String envPath = System.getenv("GOLDLAPEL_BINARY");
        if (envPath != null && !envPath.isEmpty()) {
            File f = new File(envPath);
            if (f.isFile()) return envPath;
            throw new RuntimeException(
                "GOLDLAPEL_BINARY points to " + envPath + " but file not found"
            );
        }

        // 2. Bundled binary (extracted from JAR resources)
        String extracted = extractBinary();
        if (extracted != null) return extracted;

        // 3. On PATH
        String onPath = findOnPath("goldlapel");
        if (onPath != null) return onPath;

        throw new RuntimeException(
            "Gold Lapel binary not found. Set GOLDLAPEL_BINARY env var, " +
            "bundle the binary in the JAR, or ensure 'goldlapel' is on PATH."
        );
    }

    static String makeProxyUrl(String upstream, int port) {
        // Build a proxy URL: replace host with localhost and set the proxy port.
        // Uses regex instead of java.net.URI to avoid decoding percent-encoded
        // characters in passwords (e.g. %40 for @), which would corrupt the URL.

        // pg URL with explicit port: scheme://[userinfo@]host:PORT[/path][?query]
        Matcher m = WITH_PORT.matcher(upstream);
        if (m.matches()) {
            return m.group(1) + "localhost:" + port + m.group(4);
        }

        // pg URL without port: scheme://[userinfo@]host[/path][?query]
        m = NO_PORT.matcher(upstream);
        if (m.matches()) {
            return m.group(1) + "localhost:" + port + m.group(3);
        }

        // bare host:port (only if not a URL — guard against splitting on scheme colons)
        if (!upstream.contains("://") && upstream.contains(":")) {
            return "localhost:" + port;
        }

        // bare host
        return "localhost:" + port;
    }

    static boolean waitForPort(String host, int port, long timeoutMs) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            try (Socket sock = new Socket()) {
                sock.connect(new java.net.InetSocketAddress(host, port), 500);
                return true;
            } catch (IOException e) {
                try {
                    Thread.sleep(STARTUP_POLL_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return false;
    }

    private static Path extractedBinaryPath;

    static String extractBinary() {
        // Return cached path if already extracted
        if (extractedBinaryPath != null && Files.isRegularFile(extractedBinaryPath)) {
            return extractedBinaryPath.toString();
        }

        String os = System.getProperty("os.name", "").toLowerCase();
        String arch = System.getProperty("os.arch", "").toLowerCase();

        String archName;
        if (arch.equals("amd64") || arch.equals("x86_64")) {
            archName = "x86_64";
        } else if (arch.equals("aarch64") || arch.equals("arm64")) {
            archName = "aarch64";
        } else {
            archName = arch;
        }

        String osName;
        boolean isWindows = false;
        if (os.contains("linux")) {
            osName = "linux";
        } else if (os.contains("mac") || os.contains("darwin")) {
            osName = "darwin";
        } else if (os.contains("windows")) {
            osName = "windows";
            isWindows = true;
        } else {
            osName = os.replaceAll("\\s+", "-");
        }

        String resourceName = "bin/goldlapel-" + osName + "-" + archName;
        if (osName.equals("linux") && isMusl(archName)) resourceName += "-musl";
        if (isWindows) resourceName += ".exe";
        InputStream in = GoldLapel.class.getClassLoader().getResourceAsStream(resourceName);
        if (in == null) return null;

        try (in) {
            Path tmp = Files.createTempFile("goldlapel-", isWindows ? ".exe" : "");
            try {
                Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                Files.deleteIfExists(tmp);
                return null;
            }

            try {
                Files.setPosixFilePermissions(tmp, PosixFilePermissions.fromString("rwxr-xr-x"));
            } catch (UnsupportedOperationException e) {
                tmp.toFile().setExecutable(true);
            }

            tmp.toFile().deleteOnExit();
            extractedBinaryPath = tmp;
            return tmp.toString();
        } catch (IOException e) {
            return null;
        }
    }

    static boolean isMusl(String arch) {
        return new File("/lib/ld-musl-" + arch + ".so.1").exists();
    }

    static String findOnPath(String name) {
        String pathEnv = System.getenv("PATH");
        if (pathEnv == null) return null;
        boolean isWindows = System.getProperty("os.name", "").toLowerCase().contains("windows");
        String[] names = isWindows ? new String[]{name + ".exe", name} : new String[]{name};
        for (String dir : pathEnv.split(File.pathSeparator)) {
            for (String n : names) {
                File f = new File(dir, n);
                if (f.isFile() && f.canExecute()) return f.getAbsolutePath();
            }
        }
        return null;
    }
}
