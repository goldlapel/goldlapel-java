package com.goldlapel.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "goldlapel")
public class GoldLapelProperties {

    private boolean enabled = true;
    private int proxyPort = 7932;
    private Integer dashboardPort = null;
    private String extraArgs = "";
    private String logLevel = null;
    private String mode = null;
    private String license = null;
    private String configFile = null;
    private boolean silent = false;
    private boolean mesh = false;
    private String meshTag = null;
    private boolean disableProxyCache = false;
    private boolean disableSqloptimize = false;
    private boolean disableAutoIndexes = false;
    private Map<String, String> config = new LinkedHashMap<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getProxyPort() {
        return proxyPort;
    }

    /**
     * Proxy listen port (default 7932). The first DataSource's proxy gets it.
     * DataSources sharing an upstream share its proxy. Each further upstream
     * gets the next port at or above it whose proxy and dashboard ports are
     * both free of the proxies already started, so two DataSources land on
     * 7932 and 7934 (7933 is the first one's dashboard), or on 7932 and 7933
     * with {@code dashboard-port: 0}.
     */
    public void setProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
    }

    public String getExtraArgs() {
        return extraArgs;
    }

    /**
     * Raw CLI args appended to the Gold Lapel binary invocation, as a
     * comma-separated list. To include a literal comma inside an arg (e.g. a
     * regex with counted repetition), escape it with a backslash: {@code \,}.
     * A literal backslash is written as {@code \\}.
     *
     * <p>Example {@code application.yml}:
     * <pre>
     *   goldlapel:
     *     extra-args: "--threshold-duration-ms,200"
     *     # with an embedded comma:
     *     extra-args: "--match=\\d{1\,3}"
     * </pre>
     *
     * <p>See {@link GoldLapelDataSourcePostProcessor#parseExtraArgs(String)}
     * for the full parsing rules.
     */
    public void setExtraArgs(String extraArgs) {
        this.extraArgs = extraArgs;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public Integer getDashboardPort() {
        return dashboardPort;
    }

    /**
     * Dashboard listen port. When {@code null} (the default), the dashboard
     * port is auto-derived as {@code proxyPort + 1}. Set to {@code 0} to
     * disable the dashboard on every proxy.
     *
     * <p>With several DataSources pointing at different upstreams, each gets
     * its own proxy, and a non-zero port applies to the <em>first</em> proxy
     * only. Later proxies derive their dashboard as their own proxy port + 1,
     * and port allocation skips this port so nothing collides. For example,
     * {@code proxy-port: 7932} with {@code dashboard-port: 8000} gives the
     * first proxy 7932 + 8000 and the second 7933 + 7934.
     *
     * <p>YAML: {@code goldlapel.dashboard-port: 7933}.
     */
    public void setDashboardPort(Integer dashboardPort) {
        this.dashboardPort = dashboardPort;
    }

    public String getLogLevel() {
        return logLevel;
    }

    /**
     * Log level for the Gold Lapel binary. When {@code null} (the default),
     * no override is applied and the binary uses its built-in default. Spring
     * users reach for this when integrating Gold Lapel logs with their
     * existing log aggregation (e.g. {@code "debug"} during incident
     * response, {@code "warn"} for quieter prod logs).
     *
     * <p>YAML: {@code goldlapel.log-level: debug}.
     */
    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getMode() {
        return mode;
    }

    /**
     * Operating mode for the Gold Lapel binary, passed as {@code --mode}.
     * When {@code null} (the default), the binary runs in its default
     * {@code waiter} mode. Spring users reach for this to switch into
     * {@code consideration} mode (recommendations-only, no caching) for
     * staging environments where the team wants to evaluate Gold Lapel's
     * suggestions without the cache layer affecting query behavior.
     *
     * <p>YAML: {@code goldlapel.mode: consideration}.
     */
    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getLicense() {
        return license;
    }

    /**
     * Path to a Gold Lapel license file, passed as {@code --license}. When
     * {@code null} (the default), the binary searches its standard license
     * locations. Spring users reach for this when their license file lives
     * outside the default search path (e.g. mounted from a Kubernetes
     * Secret at a custom path, or stored in a build-time-injected location).
     *
     * <p>YAML: {@code goldlapel.license: /etc/goldlapel/license.json}.
     */
    public void setLicense(String license) {
        this.license = license;
    }

    public String getConfigFile() {
        return configFile;
    }

    /**
     * Path to a TOML config file the Gold Lapel binary will parse, passed
     * as {@code --config}. When {@code null} (the default), no TOML file
     * is loaded and configuration comes entirely from {@link #getConfig()}
     * and the other top-level properties. Spring users reach for this when
     * sharing a tuned config across multiple apps (one TOML file in a
     * shared volume, referenced from each app's {@code application.yml}).
     *
     * <p>Distinct from {@link #getConfig()} which is the structured map of
     * tuning keys defined inline in {@code application.yml}.
     *
     * <p>YAML: {@code goldlapel.config-file: /etc/goldlapel/goldlapel.toml}.
     */
    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public boolean isSilent() {
        return silent;
    }

    /**
     * Whether to suppress the Gold Lapel startup banner. When {@code false}
     * (the default), the wrapper writes a one-line banner to {@code System.err}
     * describing the proxy and dashboard URLs. Set {@code true} for
     * embedded/daemon scenarios where stderr is inspected (CI logs, structured
     * logging pipelines, container shipping stderr to a log aggregator).
     *
     * <p>YAML: {@code goldlapel.silent: true}.
     */
    public void setSilent(boolean silent) {
        this.silent = silent;
    }

    public boolean isMesh() {
        return mesh;
    }

    /**
     * Whether to opt into the Gold Lapel mesh at startup. HQ enforces the
     * license; if mesh isn't covered by the current plan the proxy continues
     * running normally without clustering (concierge, not bouncer).
     *
     * <p>Set {@code true} in fleet deployments where you want this app's
     * Gold Lapel proxy to participate in cross-instance cache coordination.
     *
     * <p>YAML: {@code goldlapel.mesh: true}.
     */
    public void setMesh(boolean mesh) {
        this.mesh = mesh;
    }

    public String getMeshTag() {
        return meshTag;
    }

    /**
     * Optional mesh tag — instances sharing a tag cluster together. When
     * unset (the default), mesh-enabled instances join the account's default
     * mesh. Use this to segment a single account's instances into multiple
     * meshes (e.g. {@code "us-west-prod"} vs {@code "us-east-prod"}).
     *
     * <p>YAML: {@code goldlapel.mesh-tag: "us-west-prod-1"}.
     */
    public void setMeshTag(String meshTag) {
        this.meshTag = meshTag;
    }

    public boolean isDisableProxyCache() {
        return disableProxyCache;
    }

    /**
     * Whether to disable the proxy-side cache layer entirely. Default
     * {@code false}. Maps 1:1 to the proxy CLI flag
     * {@code --disable-proxy-cache}. Spring users reach for this when
     * the proxy-side cache is causing operational pain (debugging stale
     * data, isolating an invalidation bug) and they want to keep every
     * other Gold Lapel feature on.
     *
     * <p>YAML: {@code goldlapel.disable-proxy-cache: true}.
     */
    public void setDisableProxyCache(boolean disableProxyCache) {
        this.disableProxyCache = disableProxyCache;
    }

    public boolean isDisableSqloptimize() {
        return disableSqloptimize;
    }

    /**
     * Whether to disable the SQL-rewrite optimization pipeline ("sqloptimize").
     * Default {@code false}. Maps 1:1 to the proxy CLI flag
     * {@code --disable-sqloptimize}. Disable to bypass per-kind SQL
     * rewriting while keeping caching and auto-indexes active.
     *
     * <p>YAML: {@code goldlapel.disable-sqloptimize: true}.
     */
    public void setDisableSqloptimize(boolean disableSqloptimize) {
        this.disableSqloptimize = disableSqloptimize;
    }

    public boolean isDisableAutoIndexes() {
        return disableAutoIndexes;
    }

    /**
     * Whether to disable automatic index creation. Default {@code false}.
     * Maps 1:1 to the proxy CLI flag {@code --disable-auto-indexes}. Reach
     * for this when DDL is owned by an external migration tool that takes
     * exception to the proxy adding indexes underneath it, or when isolating
     * which optimization moved a query plan.
     *
     * <p>YAML: {@code goldlapel.disable-auto-indexes: true}.
     */
    public void setDisableAutoIndexes(boolean disableAutoIndexes) {
        this.disableAutoIndexes = disableAutoIndexes;
    }
}
