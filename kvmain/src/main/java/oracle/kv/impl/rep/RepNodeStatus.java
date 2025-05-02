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

package oracle.kv.impl.rep;

import java.io.IOException;
import java.io.Serializable;

import oracle.kv.impl.rep.migration.PartitionMigrationStatus;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.util.PingDisplay;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicatedEnvironmentStats;
import com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStats;
import com.sleepycat.je.rep.utilint.HostPortPair;

/**
 * RepNodeStatus represents the current status of a running RepNodeService.  It
 * includes ServiceStatus as well as additional state specific to a RepNode.
 */
public class RepNodeStatus implements Serializable, PingDisplay.ServiceInfo {

    private static final long serialVersionUID = 1L;
    private final ServiceStatus status;
    private final State state;
    private final long vlsn;

    /* Since R2 */
    private final String haHostPort;

    /* Since R2 */
    private final PartitionMigrationStatus[] migrationStatus;

    /*
     * The haPort field is present for backward compatibility. If deserialized
     * at an R1 node we still want it to function. The added field, haHostPort,
     * was added for elasticity and is not needed for general operation.
     *
     */
    private final int haPort;

    /**
     * JE HA replication statistics for a master rep node, or null if not the
     * master or otherwise not available.  Added in R3.3.
     */
    private final MasterRepNodeStats masterRepNodeStats;

    /**
     * JE HA environment availableLogSize statistics for a node, or 0
     * if not available. Added in R18.3.
     */
    private final long availableLogSize;

    /**
     * JE HA environment totalLogSize statistics for a node, or 0
     * if not available. This represents used storage size.
     * Added in R18.3.
     */
    private final long usedLogSize;

    /**
     * EnvironmentStats availability check. true if available else false
     * Added in R18.3.
     */
    private final boolean isEnvStatsAvailable;

    /**
     * JE HA network backup statistics for a replica that is performing a
     * network restore, or null if not a replica, not performing a network
     * restore, or the information is otherwise not available.  Added in R3.3.
     */
    private final NetworkBackupStats networkRestoreStats;

    /**
     * JE HA information about whether this is an authoritative master.  Only
     * meaningful if state is non-null and is MASTER.  Added in R3.4.
     */
    private final boolean isAuthoritativeMaster;

    /**
     * Request type enabled on this node. It could be ALL, READONLY and NONE,
     * would be ALL if the RepNode is running with version before R18.1.
     * @since 18.1
     */
    private String enabledRequestType;

    /**
     * Storage type on this node. It is the String value of the StorageType enum.
     * @since 21.1
     */
    private final String storageType;

    /**
     * Time when the RepNode instance was created, or 0 if not available.
     * @since 21.2
     */
    private final long serviceStartTime;

    /**
     * Time when the RepNode had it's last JE replication node state change
     * event, or 0 if not available.
     * @since 21.2
     */
    private volatile long stateChangeTime;

    public RepNodeStatus(ServiceStatus status, State state, long vlsn,
                         String haHostPort, String enabledRequestType,
                         PartitionMigrationStatus[] migrationStatus,
                         ReplicatedEnvironmentStats replicatedEnvStats,
                         long availableLogSize, long usedLogSize,
                         boolean isEnvStatsAvaialble,
                         NetworkBackupStats networkRestoreStats,
                         boolean isAuthoritativeMaster,
                         String storageType,
                         long serviceStartTime,
                         long stateChangeTime) {
        this.status = status;
        this.state = state;
        this.vlsn = vlsn;
        this.haHostPort = haHostPort;
        this.enabledRequestType = enabledRequestType;
        this.migrationStatus = migrationStatus;
        haPort = HostPortPair.getPort(haHostPort);
        masterRepNodeStats = MasterRepNodeStats.create(replicatedEnvStats);
        this.availableLogSize = availableLogSize;
        this.usedLogSize = usedLogSize;
        this.isEnvStatsAvailable = isEnvStatsAvaialble;
        this.networkRestoreStats = networkRestoreStats;
        this.isAuthoritativeMaster =
            isAuthoritativeMaster && (state == State.MASTER);
        this.storageType = storageType;
        this.serviceStartTime = serviceStartTime;
        this.stateChangeTime = stateChangeTime;
    }

    @Override
    public ServiceStatus getServiceStatus() {
        return status;
    }

    @Override
    public State getReplicationState() {
        return state;
    }

    public long getVlsn() {
        return vlsn;
    }

    public int getHAPort() {
        return haPort;
    }

    /**
     * Return dynamic available storage size for this
     * repNode. If storagedirsize is not specified, it
     * will be complete available disk space.
     *
     * If environmentStats is null then 0 will be returned.
     */
    public long getAvailableLogSize() {
        return availableLogSize;
    }

    /**
     * Return dynamic total used storage size for this
     * repNode. This will represent used storage size.
     *
     * If environmentStats is null then 0 will be returned.
     */
    public long getUsedLogSize() {
        return usedLogSize;
    }

    /**
     * Return true if environmentStats are available and
     * valid. Else return false.
     *
     * We will display "NOT AVAILABLE" in ping and verify
     * output for statistics not available.
     */
    public boolean isEnvironmentStatsAvailable() {
        return isEnvStatsAvailable;
    }

    /**
     * Returns the HA host and port string. The returned value may be null
     * if this instance represents a pre-R2 RepNodeService.
     *
     * @return the HA host and port string or null
     */
    public String getHAHostPort() {
        return haHostPort;
    }

    public PartitionMigrationStatus[] getPartitionMigrationStatus() {
        /* For compatibility with R1, return an empty array */
        return (migrationStatus == null) ? new PartitionMigrationStatus[0] :
                                           migrationStatus;
    }

    /**
     * Returns information about JE HA replication statistics associated with a
     * master rep node, or null if this node is not a master or the statistics
     * are otherwise not available.
     *
     * @return the stats or {@code null}
     */
    public MasterRepNodeStats getMasterRepNodeStats() {
        return masterRepNodeStats;
    }

    /**
     * Returns network backup statistics for a replica that is performing a
     * network restore, or null if this node is not a replica, is not
     * performing a network restore, or the statistics are otherwise not
     * available.
     *
     * @return the stats or {@code null}
     */
    public NetworkBackupStats getNetworkRestoreStats() {
        return networkRestoreStats;
    }

    /**
     * Returns the storage type which is String value of the StorageType enum
     * null if the node was running with version before R21.1.
     *
     * @return the storage type or {@code null}
     */
    public String getStorageType() {
        return storageType;
    }

    /**
     * Returns the time when the RepNode instance was created, or 0 if not
     * available.
     *
     * @return the service start time or 0
     */
    public long getServiceStartTime() {
        return serviceStartTime;
    }

    /**
     * Returns the time when the RepNode had it's last JE replication
     * node state change event, or 0 if not available.
     *
     * @return the state change time or 0
     */
    public long getStateChangeTime() {
        return stateChangeTime;
    }

    /**
     * Returns whether this node is the authoritative master.  Always returns
     * false if the state shows that the node is not the master.
     */
    @Override
    public boolean getIsAuthoritativeMaster() {
        return isAuthoritativeMaster;
    }

    /**
     * Computes and returns the estimated time in seconds until the node's
     * current network restore operation will complete, or 0 if no network
     * restore operation is known to be underway.
     */
    public long getNetworkRestoreTimeSecs() {
        if (networkRestoreStats != null) {
            final long remainingBytes =
                networkRestoreStats.getExpectedBytes() -
                networkRestoreStats.getTransferredBytes();
            final double transferRate = networkRestoreStats.getTransferRate();
            return (long) Math.ceil(remainingBytes / transferRate);
        }
        return 0;
    }

    /**
     * Returns request type enabled on this node.
     *
     * @return enabled request type, could be NONE, ALL and READONLY
     */
    public String getEnabledRequestType() {
        return enabledRequestType;
    }

    @Override
    public String toString() {
        return status + "," + state +
            (((state == State.MASTER) && !isAuthoritativeMaster) ?
             " (non-authoritative)" : "");
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {

        in.defaultReadObject();

        /* If enabledRequestType is null, initialized as default value ALL */
        if (enabledRequestType == null) {
            enabledRequestType = "ALL";
        }
    }
}
