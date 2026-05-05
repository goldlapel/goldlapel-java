package com.goldlapel.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "goldlapel")
public class GoldLapelProperties {

    private boolean enabled = true;
    private int proxyPort = 7932;
    private String extraArgs = "";
    private int invalidationPort = 0;
    private boolean nativeCache = true;
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

    public boolean isNativeCache() {
        return nativeCache;
    }

    public void setNativeCache(boolean nativeCache) {
        this.nativeCache = nativeCache;
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
