package com.goldlapel.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "goldlapel")
public class GoldLapelProperties {

    private boolean enabled = true;
    private int proxyPort = 7932;
    private Integer dashboardPort = null;
    private int invalidationPort = 0;
    private String extraArgs = "";
    private String logLevel = null;
    private String mode = null;
    private String license = null;
    private String configFile = null;
    private boolean disableNativeCache = false;
    private boolean silent = false;
    private boolean mesh = false;
    private String meshTag = null;
    private boolean enableProxyCacheForWrappers = false;
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

    public int getInvalidationPort() {
        return invalidationPort;
    }

    public void setInvalidationPort(int invalidationPort) {
        this.invalidationPort = invalidationPort;
    }

    public Integer getDashboardPort() {
        return dashboardPort;
    }

    /**
     * Dashboard listen port. When {@code null} (the default), the dashboard
     * port is auto-derived as {@code proxyPort + 1}. Set to {@code 0} to
     * disable the dashboard entirely. Spring users typically reach for this
     * when running multiple Gold Lapel proxies on the same host and want
     * explicit control over each proxy's dashboard port (e.g. one per pod
     * with hard-coded ports for kubectl port-forward routing).
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

    public boolean isDisableNativeCache() {
        return disableNativeCache;
    }

    /**
     * Whether to disable the in-process native cache layer. The native
     * cache wraps the {@link javax.sql.DataSource} bean in a
     * {@link CachedDataSource} that consults an in-process {@link
     * com.goldlapel.NativeCache} before falling through to the underlying
     * pool — fast lookups on repeat queries, with the proxy keeping the
     * cache fresh via the invalidation port. Spring users reach for this
     * to opt out (e.g. they have their own application-level cache and
     * want the proxy to handle caching exclusively, or they're debugging
     * a query behavior issue and want to eliminate the cache layer).
     *
     * <p>Default {@code false} (the cache is active). YAML to disable:
     * {@code goldlapel.disable-native-cache: true}.
     */
    public void setDisableNativeCache(boolean disableNativeCache) {
        this.disableNativeCache = disableNativeCache;
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

    public boolean isEnableProxyCacheForWrappers() {
        return enableProxyCacheForWrappers;
    }

    /**
     * Whether wrapper-spawned proxies participate in the proxy cache.
     * Default {@code false} — Spring Boot apps usually have their own
     * in-process cache via {@link CachedDataSource}, and a single wrapper
     * rarely benefits from sharing the proxy cache with itself.
     *
     * <p>Set {@code true} for fleet deployments (multi-pod, frequent
     * restarts, mesh) where the proxy cache still earns its keep as a
     * shared cache across many short-lived wrapper processes.
     *
     * <p>YAML: {@code goldlapel.enable-proxy-cache-for-wrappers: true}.
     */
    public void setEnableProxyCacheForWrappers(boolean enableProxyCacheForWrappers) {
        this.enableProxyCacheForWrappers = enableProxyCacheForWrappers;
    }
}
