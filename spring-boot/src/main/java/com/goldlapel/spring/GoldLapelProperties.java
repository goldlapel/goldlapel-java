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
}
