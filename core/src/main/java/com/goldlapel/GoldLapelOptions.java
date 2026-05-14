package com.goldlapel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Configuration options for {@link GoldLapel#start(String, java.util.function.Consumer)}.
 *
 * <p>Use JavaBean-style setters from within a {@code Consumer<GoldLapelOptions>}:
 *
 * <pre>{@code
 * GoldLapel gl = GoldLapel.start("postgresql://localhost/db", opts -> {
 *     opts.setProxyPort(7932);
 *     opts.setLogLevel("info");
 * });
 * }</pre>
 *
 * <p>Top-level option names match the canonical config surface shared across
 * every Gold Lapel wrapper — see
 * {@code docs/reviews/schema-to-core/config-surface-canonical.md} in the
 * main goldlapel repo. The structured {@link #getConfig() config} map only
 * holds the ~45 low-level tuning knobs (pool size, cache sizes, disable_X
 * booleans, replicas, etc.); concepts that users reach for regularly
 * (ports, log level, mode) have their own top-level setters.
 */
public class GoldLapelOptions {
    private Integer proxyPort;
    private Integer dashboardPort;
    private Integer invalidationPort;
    private String logLevel;
    private String mode;
    private String license;
    private String client;
    private String configFile;
    private Map<String, Object> config;
    private List<String> extraArgs;
    private boolean silent;
    private boolean mesh;
    private String meshTag;
    private boolean disableNativeCache;
    private boolean disableProxyCache;
    private boolean disableMatviews;
    private boolean disableSqloptimize;
    private boolean disableAutoIndexes;
    private AggressiveVerifyMode aggressiveVerify = AggressiveVerifyMode.AUTO;

    public Integer getProxyPort() {
        return proxyPort;
    }

    public void setProxyPort(Integer proxyPort) {
        this.proxyPort = proxyPort;
    }

    public Integer getDashboardPort() {
        return dashboardPort;
    }

    /**
     * Dashboard listen port. When {@code null} (default), the port is derived
     * as {@code proxyPort + 1}. Set to {@code 0} to disable the dashboard
     * entirely.
     */
    public void setDashboardPort(Integer dashboardPort) {
        this.dashboardPort = dashboardPort;
    }

    public Integer getInvalidationPort() {
        return invalidationPort;
    }

    /**
     * Cache-invalidation listen port. When {@code null} (default), the port
     * is derived as {@code proxyPort + 2}.
     */
    public void setInvalidationPort(Integer invalidationPort) {
        this.invalidationPort = invalidationPort;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getMode() {
        return mode;
    }

    /** Operating mode, passed as {@code --mode} (e.g. {@code "waiter"}, {@code "consideration"}). */
    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getLicense() {
        return license;
    }

    /** Path to the signed license file, passed as {@code --license}. */
    public void setLicense(String license) {
        this.license = license;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getConfigFile() {
        return configFile;
    }

    /**
     * Path to a TOML config file the Rust binary will parse. Passed as
     * {@code --config}. Distinct from {@link #getConfig()} which is the
     * structured map of tuning keys.
     */
    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    /**
     * Structured config map of the ~45 tuning keys (pool size, cache sizes,
     * disable_X booleans, replicas, exclude_tables, tls_*, n1_*, pattern_*).
     * Keys are {@code camelCase} strings matching the Java idiom; the
     * wrapper translates to {@code --kebab-case} CLI flags.
     *
     * <p>Top-level concepts (proxy port, dashboard port, log level, mode,
     * etc.) use their own setters and are rejected here.
     */
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    public List<String> getExtraArgs() {
        return extraArgs;
    }

    public void setExtraArgs(List<String> extraArgs) {
        this.extraArgs = extraArgs;
    }

    public void setExtraArgs(String... extraArgs) {
        this.extraArgs = new ArrayList<>(Arrays.asList(extraArgs));
    }

    /**
     * Whether to suppress the startup banner. When {@code false} (the default),
     * {@link GoldLapel#start(String, java.util.function.Consumer)} writes a
     * one-line banner to {@code System.err} describing the proxy and dashboard
     * URLs. When {@code true}, nothing is printed.
     *
     * <p>Banner goes to {@code System.err} rather than {@code System.out} so it
     * doesn't pollute app output (CLI tools piping stdout, servers logging to
     * stdout, test harnesses capturing stdout, etc.). Opt out entirely for
     * embedded/daemon scenarios where stderr is inspected too.
     */
    public boolean isSilent() {
        return silent;
    }

    public void setSilent(boolean silent) {
        this.silent = silent;
    }

    /**
     * Whether to opt into the mesh at startup. HQ enforces the license; if
     * mesh isn't covered by the current plan the proxy continues running
     * normally without clustering (concierge, not bouncer).
     *
     * <p>Equivalent CLI flag: {@code --mesh}. Env var: {@code GOLDLAPEL_MESH}.
     * TOML key: {@code [mesh] enabled}. Precedence (highest to lowest):
     * CLI &gt; env &gt; TOML &gt; default.
     */
    public boolean isMesh() {
        return mesh;
    }

    public void setMesh(boolean mesh) {
        this.mesh = mesh;
    }

    public String getMeshTag() {
        return meshTag;
    }

    /**
     * Optional mesh tag — instances sharing a tag cluster together. When
     * unset, mesh-enabled instances join the account's default mesh.
     *
     * <p>Equivalent CLI flag: {@code --mesh-tag <tag>}. Env var:
     * {@code GOLDLAPEL_MESH_TAG}. TOML key: {@code [mesh] tag}.
     */
    public void setMeshTag(String meshTag) {
        this.meshTag = meshTag;
    }

    /**
     * Whether to disable the wrapper's native cache entirely. Default {@code false}.
     * When {@code true}, the in-process {@link NativeCache} acts as a no-op
     * pass-through: {@code get()} always returns null, {@code put()} never
     * stores. Misses still tick (so the proxy sees per-query traffic), hits
     * stay zero, and no eviction happens.
     *
     * <p>This is distinct from setting the cache capacity to zero via the
     * {@code cacheSize} tuning key. {@code cacheSize=0} forces customers to
     * lose their tuned capacity to toggle the layer; {@code disableNativeCache=true}
     * lets them keep the size and toggle independently.
     *
     * <p>The invalidation channel still runs while disabled — the wrapper
     * stays connected to the proxy for telemetry and snapshot replies.
     */
    public boolean isDisableNativeCache() {
        return disableNativeCache;
    }

    public void setDisableNativeCache(boolean disableNativeCache) {
        this.disableNativeCache = disableNativeCache;
    }

    /**
     * Whether to disable the proxy-side cache layer entirely. Default
     * {@code false}. Maps 1:1 to the proxy CLI flag
     * {@code --disable-proxy-cache}. Use this when the proxy-side cache is
     * causing operational pain (debugging stale data, isolating an
     * invalidation bug) and you want to keep every other Gold Lapel
     * feature on. Distinct from {@link #isDisableNativeCache()}, which
     * toggles the wrapper-side cache.
     */
    public boolean isDisableProxyCache() {
        return disableProxyCache;
    }

    public void setDisableProxyCache(boolean disableProxyCache) {
        this.disableProxyCache = disableProxyCache;
    }

    /**
     * Whether to disable automatic materialized-view creation. Default
     * {@code false}. Maps 1:1 to the proxy CLI flag {@code --disable-matviews}.
     * Reach for this in environments where matviews would conflict with an
     * existing migration / ownership model, or while debugging which Gold
     * Lapel optimization is responsible for an observed behavior change.
     */
    public boolean isDisableMatviews() {
        return disableMatviews;
    }

    public void setDisableMatviews(boolean disableMatviews) {
        this.disableMatviews = disableMatviews;
    }

    /**
     * Whether to disable the SQL-rewrite optimization pipeline ("sqloptimize").
     * Default {@code false}. Maps 1:1 to the proxy CLI flag
     * {@code --disable-sqloptimize}. Disable this to bypass per-kind SQL
     * rewriting while keeping caching, matviews, and auto-indexes active.
     */
    public boolean isDisableSqloptimize() {
        return disableSqloptimize;
    }

    public void setDisableSqloptimize(boolean disableSqloptimize) {
        this.disableSqloptimize = disableSqloptimize;
    }

    /**
     * Whether to disable automatic index creation. Default {@code false}.
     * Maps 1:1 to the proxy CLI flag {@code --disable-auto-indexes}. Reach
     * for this when DDL is owned by an external migration tool that takes
     * exception to the proxy adding indexes underneath it, or when isolating
     * which optimization moved a query plan.
     */
    public boolean isDisableAutoIndexes() {
        return disableAutoIndexes;
    }

    public void setDisableAutoIndexes(boolean disableAutoIndexes) {
        this.disableAutoIndexes = disableAutoIndexes;
    }

    /**
     * Post-DML aggressive-verify mode. Default {@link AggressiveVerifyMode#AUTO},
     * which bumps the per-connection post-DML sequence counter after every
     * observed INSERT/UPDATE/DELETE/MERGE/TRUNCATE/CALL/DDL, rolling the
     * wrapper-side cache key forward so a cached pre-DML response cannot be
     * served against potentially-trigger-mutated session state.
     * {@link AggressiveVerifyMode#ON} is a synonym for AUTO;
     * {@link AggressiveVerifyMode#OFF} skips the bump (audited-schema
     * opt-out, logs a one-time warning).
     *
     * <p>Background: Wave 1 covers stored function/procedure SETs. This setting
     * controls the Wave 2 "post-DML expansion" — closing the trigger-internal
     * SET correctness gap. See
     * {@code goldlapel/docs/todos/aggressive-verify-flag.md}.
     */
    public AggressiveVerifyMode getAggressiveVerify() {
        return aggressiveVerify;
    }

    public void setAggressiveVerify(AggressiveVerifyMode aggressiveVerify) {
        this.aggressiveVerify = aggressiveVerify == null ? AggressiveVerifyMode.AUTO : aggressiveVerify;
    }
}
