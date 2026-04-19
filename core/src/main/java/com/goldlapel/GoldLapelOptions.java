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
 *     opts.setPort(7932);
 *     opts.setLogLevel("info");
 * });
 * }</pre>
 */
public class GoldLapelOptions {
    private Integer port;
    private String logLevel;
    private Map<String, Object> config;
    private List<String> extraArgs;
    private String client;

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

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

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }
}
