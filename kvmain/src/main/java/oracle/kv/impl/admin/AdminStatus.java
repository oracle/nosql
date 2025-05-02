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

package oracle.kv.impl.admin;

import java.io.Serializable;

import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.util.PingDisplay;
import com.sleepycat.je.rep.ReplicatedEnvironmentStats;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;

/**
 * Represents the current status of a running AdminService.  It includes
 * ServiceStatus as well as additional state specific to an Admin.
 */
public class AdminStatus implements Serializable, PingDisplay.ServiceInfo {

    private static final long serialVersionUID = 1L;
    private final ServiceStatus status;
    private final State state;

    /**
     * JE HA information about whether this is an authoritative master.  Only
     * meaningful if state is non-null and is MASTER.
     */
    private final boolean isAuthoritativeMaster;
    /**
     * JE HA replication statistics for a master rep node, or null if not the
     * master or otherwise not available.  Added in R18.1.
     */
    private final MasterAdminStats masterAdminStats;

    /**
     * Time when the Admin instance was created, or 0 if not available.
     * @since 21.2
     */
    private final long serviceStartTime;

    /**
     * Time when the admin had it's last JE replication node state
     * change event, or 0 if not available.
     * @since 21.2
     */
    private final long stateChangeTime;

    /**
     * Available storage size for the Admin node obtained from the 
     * Cleaner or 0 if not available.
     * @since 21.3
     */
    private final long availableLogSize;

    public AdminStatus(ServiceStatus status,
                       State state,
                       boolean isAuthoritativeMaster,
                       ReplicatedEnvironmentStats replicatedEnvStats,
                       long availableLogSize,
                       long serviceStartTime,
                       long stateChangeTime) {
        this.status = status;
        this.state = state;
        this.isAuthoritativeMaster =
            isAuthoritativeMaster && (state == State.MASTER);
        this.masterAdminStats =
            MasterAdminStats.create(replicatedEnvStats);
        this.serviceStartTime = serviceStartTime;
        this.stateChangeTime = stateChangeTime;
        this.availableLogSize = availableLogSize;
    }

    @Override
    public ServiceStatus getServiceStatus() {
        return status;
    }

    @Override
    public State getReplicationState() {
        return state;
    }

    /**
     * Returns whether this node is the authoritative master.  Always returns
     * false if the state shows that the node is not the master.
     */
    @Override
    public boolean getIsAuthoritativeMaster() {
        return isAuthoritativeMaster;
    }

    @Override
    public String toString() {
        return status + "," + state + 
             (((state == State.MASTER) && !isAuthoritativeMaster) ?
             " (non-authoritative)" : "");
    }

    /**
     * Returns information about JE HA replication statistics associated with a
     * master admin, or null if this node is not a master or the statistics are
     * otherwise not available.
     *
     * @return the stats or {@code null}
     */
    public MasterAdminStats getMasterAdminStats() {
        return masterAdminStats;
    }

    /**
     * Returns the time when the Admin instance was created, or 0 if not
     * available.
     *
     * @return the service start time or 0
     */
    public long getServiceStartTime() {
        return serviceStartTime;
    }

    /**
     * Returns the time when the Admin had it's last state change event,
     * or 0 if not available.
     *
     * @return the state change time or 0
     */
    public long getStateChangeTime() {
        return stateChangeTime;
    }

    /**
     * Returns the available storage size of the Admin in bytes.
     *
     * @return the available storage size for the Admin
     */
    public long getAvailableLogSize() {
        return availableLogSize;
    }

}
