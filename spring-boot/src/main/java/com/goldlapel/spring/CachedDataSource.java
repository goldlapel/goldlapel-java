package com.goldlapel.spring;

import com.goldlapel.AggressiveVerifyMode;
import com.goldlapel.ConnectionProxy;
import com.goldlapel.NativeCache;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

public class CachedDataSource implements DataSource {

    private final DataSource delegate;
    private final NativeCache cache;
    private final AggressiveVerifyMode aggressiveVerify;
    private final String jdbcUrl;

    public CachedDataSource(DataSource delegate, NativeCache cache) {
        this(delegate, cache, AggressiveVerifyMode.OFF, null);
    }

    /**
     * Construct a wrapped DataSource that applies the given post-DML
     * aggressive-verify mode to every connection it hands out.
     * {@code jdbcUrl} keys the per-URL detection cache used by
     * {@link AggressiveVerifyMode#AUTO}; pass {@code null} to disable
     * AUTO detection (the wrapper falls through to OFF for AUTO).
     */
    public CachedDataSource(DataSource delegate, NativeCache cache,
                            AggressiveVerifyMode aggressiveVerify, String jdbcUrl) {
        this.delegate = delegate;
        this.cache = cache;
        this.aggressiveVerify = aggressiveVerify == null ? AggressiveVerifyMode.OFF : aggressiveVerify;
        this.jdbcUrl = jdbcUrl;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return ConnectionProxy.wrap(delegate.getConnection(), cache, aggressiveVerify, jdbcUrl);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return ConnectionProxy.wrap(
            delegate.getConnection(username, password), cache, aggressiveVerify, jdbcUrl);
    }

    /** Visible for testing. */
    AggressiveVerifyMode aggressiveVerify() {
        return aggressiveVerify;
    }

    /** Visible for testing. */
    String jdbcUrl() {
        return jdbcUrl;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        delegate.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        delegate.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return delegate.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this) || delegate.isWrapperFor(iface);
    }

    DataSource getDelegate() {
        return delegate;
    }

    NativeCache getCache() {
        return cache;
    }
}
