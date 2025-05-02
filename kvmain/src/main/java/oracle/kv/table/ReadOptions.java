/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.table;

import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.KVStoreConfig;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.util.contextlogger.LogContext;

/**
 * ReadOptions is passed to read-only store operations to specify arguments that
 * control non-default behavior related to consistency and operation timeouts.
 * <p>
 * The default behavior is configured when a store is opened using
 * {@link KVStoreConfig}.
 *
 * @since 3.0
 */
public class ReadOptions {

    private final Consistency consistency;
    private final long timeout;
    private final TimeUnit timeoutUnit;
    private LogContext logContext;
    private AuthContext authContext;
    private boolean noCharge = false;

    /**
     * Creates a {@code ReadOptions} with the specified parameters.
     * <p>
     * If {@code consistency} is {@code null}, the
     * {@link KVStoreConfig#getConsistency default consistency}
     * is used.
     * <p>
     * If {@code timeout} is zero the
     * {@link KVStoreConfig#getRequestTimeout default request timeout} is used.
     * <p>
     * The {@code timeout} parameter is an upper bound on the time interval for
     * processing the operation.  A best effort is made not to exceed the
     * specified limit. If zero, the {@link KVStoreConfig#getRequestTimeout
     * default request timeout} is used.
     * <p>
     * If {@code timeout} is not 0, the {@code timeoutUnit} parameter must not
     * be {@code null}.
     *
     * @param consistency the read consistency to use or null
     * @param timeout the timeout value to use
     * @param timeoutUnit the {@link TimeUnit} used by the
     * <code>timeout</code> parameter or null
     *
     * @throws IllegalArgumentException if timeout is negative
     * @throws IllegalArgumentException if timeout is &gt; 0 and timeoutUnit
     * is null
     */
    public ReadOptions(Consistency consistency,
                       long timeout,
                       TimeUnit timeoutUnit) {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must be >= 0");
        }
        if ((timeout != 0) && (timeoutUnit == null)) {
            throw new IllegalArgumentException("A non-zero timeout requires " +
                                               "a non-null timeout unit");
        }
        this.consistency = consistency;
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
    }

    /**
     * Gets the consistency used for a read operation.
     * If null, the {@link KVStoreConfig#getConsistency default consistency}
     * is used.
     *
     * @return the consistency used for a read operation
     */
    public Consistency getConsistency() {
        return consistency;
    }

    /**
     * Gets the timeout, which is an upper bound on the time interval for
     * processing the operation.  A best effort is made not to exceed the
     * specified limit. If zero, the {@link KVStoreConfig#getRequestTimeout
     * default request timeout} is used.
     *
     * @return the timeout
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Gets the unit of the timeout parameter, and may
     * be {@code null} only if {@link #getTimeout} returns zero.
     *
     * @return the timeout unit or null
     */
    public TimeUnit getTimeoutUnit() {
        return timeoutUnit;
    }

    /**
     * @hidden
     */
    public LogContext getLogContext() {
        return logContext;
    }

    /**
     * @hidden
     */
    public ReadOptions setLogContext(LogContext logContext) {
        this.logContext = logContext;
        return this;
    }

    /**
     * @hidden
     */
    public AuthContext getAuthContext() {
        return authContext;
    }

    /**
     * @hidden
     */
    public ReadOptions setAuthContext(AuthContext authContext) {
        this.authContext = authContext;
        return this;
    }

    /**
     * Returns the no-charge flag.
     * 
     * @hidden
     */
    public boolean getNoCharge() {
        return noCharge;
    }

    /**
     * Sets the no-charge flag. If true, read throughput consumed by the
     * operation is not charged to the table. Defaults to false.
     * 
     * @hidden
     */
    public void setNoCharge(boolean flag) {
        noCharge = flag;
    }
}
