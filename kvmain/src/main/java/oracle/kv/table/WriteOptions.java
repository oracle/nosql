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

import oracle.kv.Durability;
import oracle.kv.KVStoreConfig;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.util.contextlogger.LogContext;

/**
 * WriteOptions is passed to store operations that can update the store to
 * specify non-default behavior relating to durability, timeouts and expiry
 * operations.
 * <p>
 * The default behavior is configured when a store is opened using
 * {@link KVStoreConfig}.
 *
 * @since 3.0
 */
public class WriteOptions implements Cloneable {

    private Durability durability;
    private long timeout;
    private TimeUnit timeoutUnit;
    private boolean updateTTL;
    private int maxWriteKB;
    private int identityCacheSize = 0;

    private LogContext logContext;
    private AuthContext authContext;
    private boolean noCharge = false;

    /*
     * For cloud, the local region id.
     */
    private int regionId = Region.NULL_REGION_ID;

    /*
	 * For cloud.
     * Indicates whether put tombstone in delete ops, set true if target table
	 * is external multi-region table.
	 */
    private boolean doTombstone;

    /**
     * Creates a {@code WriteOptions} with default values.
     * Same as WriteOptions(null, 0, null)
     * @see #WriteOptions(Durability, long, TimeUnit)
     *
     * @since 4.0
     */
    public WriteOptions() {
        this(null, 0, null);
    }

    /**
     * Creates a {@code WriteOptions} with the specified parameters.
     * <p>
     * If {@code durability} is {@code null}, the
     * {@link KVStoreConfig#getDurability default durability} is used.
     * <p>
     * The {@code timeout} parameter is an upper bound on the time interval for
     * processing the operation.  A best effort is made not to exceed the
     * specified limit. If zero, the {@link KVStoreConfig#getRequestTimeout
     * default request timeout} is used.
     * <p>
     * If {@code timeout} is not 0, the {@code timeoutUnit} parameter must not
     * be {@code null}.
     *
     * @param durability the write durability to use
     * @param timeout the timeout value to use
     * @param timeoutUnit the {@link TimeUnit} used by the
     * <code>timeout</code> parameter
     *
     * @throws IllegalArgumentException if timeout is negative
     * @throws IllegalArgumentException if timeout is &gt; 0 and timeoutUnit
     * is null
     */
    public WriteOptions(Durability durability,
                        long timeout,
                        TimeUnit timeoutUnit) {
        setTimeout(timeout, timeoutUnit).
        setDurability(durability);
    }

    /*
     * Internal copy constructor
     */
    public WriteOptions(WriteOptions options) {
        this(options.durability, options.timeout, options.timeoutUnit);
        updateTTL = options.updateTTL;
        maxWriteKB = options.maxWriteKB;
        identityCacheSize = options.identityCacheSize;
    }

    /**
     * Sets durability of write operation.
     * @param durability can be null. If {@code null}, the
     * {@link KVStoreConfig#getDurability default durability} will be used.
     * @return this
     *
     * @since 4.0
     */
    public WriteOptions setDurability(Durability durability) {
        this.durability = durability;
        return this;
    }

    /**
     * Sets timeout for write operation.
     *
     * @param timeout the timeout value to use
     * @param timeoutUnit the {@link TimeUnit} used by the
     * <code>timeout</code> parameter
     * @return this
     * @throws IllegalArgumentException if timeout is negative
     * @throws IllegalArgumentException if timeout is &gt; 0 and timeoutUnit
     * is null
     *
     * @since 4.0
     */
    public WriteOptions setTimeout(long timeout, TimeUnit timeoutUnit) {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must be >= 0");
        }
        if ((timeout != 0) && (timeoutUnit == null)) {
            throw new IllegalArgumentException("A non-zero timeout requires " +
                                               "a non-null timeout unit");
        }
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
        return this;
    }

    /**
     * Returns the durability associated with the operation.
     *
     * @return the durability or null
     */
    public Durability getDurability() {
        return durability;
    }

    /**
     * Returns the timeout, which is an upper bound on the time interval for
     * processing the operation.  A best effort is made not to exceed the
     * specified limit. If zero, the {@link KVStoreConfig#getRequestTimeout
     * default request timeout} is used.
     *
     * @return the timeout or zero
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Returns the unit of the timeout parameter.
     *
     * @return the {@code TimeUnit} or null if the timeout has not been set.
     *
     * @since 4.0
     */
    public TimeUnit getTimeoutUnit() {
        return timeoutUnit;
    }

    /**
     * Sets whether absolute expiration time will be modified during update.
     * If false and the operation updates a record, the record's expiration
     * time will not change.
     * <br>
     * If the operation inserts a record, this parameter is ignored and the
     * specified TTL is always applied.
     * <br>
     * By default, this property is false. To update expiration time of an
     * existing record, this flag must be set to true.
     *
     * @param flag set to true if the operation should update an existing
     * record's expiration time.
     *
     * @return this
     *
     * @since 4.0
     */
    public WriteOptions setUpdateTTL(boolean flag) {
        updateTTL = flag;
        return this;
    }

    /**
     * Returns true if the absolute expiration time is to be modified during
     * update operations.
     *
     * @since 4.0
     */
    public boolean getUpdateTTL() {
        return updateTTL;
    }

    /**
     * Sets the number of generated identity values that are requested from
     * the server during a put. This takes precedence to the DDL identity CACHE
     * option.
     *
     * Any value equal of less than 0 means that the DDL identity CACHE value
     * is used.
     *
     * @since 18.3
     */
    public WriteOptions setIdentityCacheSize(int identityCacheSize) {
        this.identityCacheSize = identityCacheSize;
        return this;
    }

    /**
     * Gets the number of generated identity values that are requested from
     * the server during a put.
     *
     * @since 18.3
     */
    public int getIdentityCacheSize() {
        return identityCacheSize;
    }

    /**
     * @hidden
     *
     * Sets the limit on the total KB write during a operation.
     *
     * @param maxWriteKB the max number of KB write
     */
    public WriteOptions setMaxWriteKB(int maxWriteKB) {
        if (maxWriteKB < 0) {
            throw new IllegalArgumentException("maxWriteKB must be >= 0");
        }
        this.maxWriteKB = maxWriteKB;
        return this;
    }
    /**
     * @hidden
     *
     * Returns the limit on the total KB write during a operation.
     *
     * @return the max number of KB write.
     */
    public int getMaxWriteKB() {
        return maxWriteKB;
    }

    /**
     * @since 4.0
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        WriteOptions clone = new WriteOptions();
        clone.setDurability(durability);
        clone.setTimeout(timeout, timeoutUnit);
        clone.setUpdateTTL(updateTTL);
        clone.setIdentityCacheSize(identityCacheSize);
        clone.setNoCharge(noCharge);
        return clone;
    }

    /**
     * @since 4.0
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("Durability ").append(durability)
            .append(" timeout=").append(timeout)
            .append(timeoutUnit == null ? "" : timeoutUnit)
            .append(" updateTTL=").append(updateTTL)
            .append(" identityCacheSize=").append(identityCacheSize)
            .append(" noCharge=").append(noCharge);
        return buf.toString();
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
    public WriteOptions setLogContext(LogContext logContext) {
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
    public WriteOptions setAuthContext(AuthContext authContext) {
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
     * Sets the no-charge flag. If true, write throughput consumed by the
     * operation is not charged to the table. Defaults to false.
     *
     * @hidden
     */
    public void setNoCharge(boolean flag) {
        noCharge = flag;
    }

    /**
     * Sets the external region id, used for table whose region is persisted in
     * external metadata only.
     *
     * @param regionId the region ID of the region performing the operation,
     * or NULL_REGION_ID if not performing a cloud operation
     *
     * @throws IllegalArgumentException if regionId is invalid
     *
     * @hidden
     */
    public WriteOptions setRegionId(int regionId) {
        if (regionId != Region.NULL_REGION_ID) {
            /* check it is a valid external region id */
            Region.checkId(regionId, true);
        }
        this.regionId = regionId;
        return this;
    }

    /**
     * Sets whether put tombstone instead of deleting, used for delete operation
     * on external multi-region table.
     *
     * @param doTombstone true to put tombstone instead of deleting
     *
     * @hidden
     */
    public WriteOptions setDoTombstone(boolean doTombstone) {
        this.doTombstone = doTombstone;
        return this;
    }

    /**
     * For table which region is persist in external metadata only.
     * Returns the external region id or NULL_REGION_ID.
     *
     * @hidden
     */
     public int getRegionId() {
         return regionId;
     }

     /**
      * Returns whether put tombstone instead of deleting, used for delete
      * operation on external multi-region table.
      *
      * @hidden
      */
     public boolean doTombstone() {
         return doTombstone;
     }
}
