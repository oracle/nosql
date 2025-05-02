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

package oracle.kv.impl.sna;

import java.io.Serializable;

import oracle.kv.KVVersion;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

/**
 * StorageNodeStatus represents the current status of a running StorageNode.
 * It includes ServiceStatus and Version.
 */
public class StorageNodeStatus implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Used during testing: A non-null value overrides the current version. */
    private static volatile KVVersion testCurrentKvVersion;

    private final ServiceStatus status;
    /*
     * The use of a Boolean object rather than a boolean primitive is to
     * allow for three states, with the additional state being null, when
     * the balancing information is not available.
     *
     * @see #isMasterBalanced
     * @since 21.1
     */
    private final Boolean isMasterBalanced;
    private final KVVersion kvVersion;

    /**
     * Time when the StorageNodeAgent instance was created, or 0 if not
     * available.
     * @since 21.2
     */
    private final long serviceStartTime;

    public StorageNodeStatus(ServiceStatus status, Boolean isMasterBalanced,
                             long serviceStartTime) {
        this.status = status;
        this.isMasterBalanced = isMasterBalanced;
        this.kvVersion = getCurrentKVVersion();

        /* Initialize release info */
        this.kvVersion.getReleaseDate();

        this.serviceStartTime = serviceStartTime;
    }

    /** Gets the current version. */
    private static KVVersion getCurrentKVVersion() {
        return (testCurrentKvVersion != null) ?
            testCurrentKvVersion :
            KVVersion.CURRENT_VERSION;
    }

    /**
     * Set the current version to a different value, for testing.  Specifying
     * {@code null} reverts to the standard value.
     */
    public static void setTestKVVersion(final KVVersion testKvVersion) {
        testCurrentKvVersion = testKvVersion;
    }

    public ServiceStatus getServiceStatus() {
        return status;
    }

    public KVVersion getKVVersion() {
        return kvVersion;
    }

    /**
     * Returns the time when the StorageNodeAgent instance was created,
     * or 0 if not available.
     *
     * @return the service start time or 0
     */
    public long getServiceStartTime() {
        return serviceStartTime;
    }
    /**
     * Returns true if balanced, false if a master imbalance exists, null if no
     * information is available, e.g. SNA is an older version that does not
     * support this information, or SNA service is not fully initialized and is
     * not in a state to provide the information.
     */
    public Boolean isMasterBalanced() {
        return isMasterBalanced;
    }

    @Override
    public String toString() {
        return status + "," + status;
    }
}
