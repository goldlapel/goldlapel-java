package com.goldlapel.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "goldlapel")
public class GoldLapelProperties {

    private boolean enabled = true;
    private int port = 7932;
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

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getExtraArgs() {
        return extraArgs;
    }

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
