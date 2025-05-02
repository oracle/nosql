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

package oracle.kv.impl.admin.param;

import static oracle.kv.impl.param.ParameterState.ADMIN_EVENT_RECORDER_POLLING_INTERVAL;
import static oracle.kv.impl.param.ParameterState.AP_TYPE;
import static oracle.kv.impl.param.ParameterState.AP_TYPE_DEFAULT;
import static oracle.kv.impl.param.ParameterState.COMMON_ELECTABLE_GROUP_SIZE_OVERRIDE;
import static oracle.kv.impl.param.ParameterState.COMMON_ELECTABLE_GROUP_SIZE_OVERRIDE_DEFAULT;
import static oracle.kv.impl.param.ParameterState.COMMON_MISC_ASYNC_MAX_CONCURRENT_OPS;
import static oracle.kv.impl.param.ParameterState.COMMON_RESET_REP_GROUP;
import static oracle.kv.impl.param.ParameterState.COMMON_RESET_REP_GROUP_DEFAULT;
import static oracle.kv.impl.param.ParameterState.SP_COLLECT_ENV_STATS;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.StorageNodeId;

import com.sleepycat.je.rep.utilint.HostPortPair;

/**
 * Provides the configuration parameters for an instance of the Admin.  This
 * will include configuration parameters of the replicated environment upon
 * which it is built.
 */
public class AdminParams implements ParamsWithMap, Serializable {

    private static final long serialVersionUID = 1;
    private final ParameterMap map;

    public AdminParams(ParameterMap map) {
        this.map = map;
    }

    /**
     * For bootstapping, before any policy Admin params are available. All
     * local fields are set to default values.
     */
    public AdminParams(AdminId adminId,
                       StorageNodeId storageNodeId,
                       AdminType type,
                       String adminStorageDir) {
        this(new ParameterMap(), adminId, storageNodeId, type,
             adminStorageDir);
    }

    /**
     * This is used by the planner.
     */
    public AdminParams(ParameterMap newmap,
                       AdminId adminId,
                       StorageNodeId snid,
                       AdminType type,
                       String adminStorageDir) {
        this.map = newmap.filter(EnumSet.of(ParameterState.Info.ADMIN));
        setAdminId(adminId);
        setStorageNodeId(snid);
        setType(type);
        setAdminStorageDir(adminStorageDir);
        addDefaults();
        map.setName(adminId.getFullName());
        map.setType(ParameterState.ADMIN_TYPE);
    }

    private void addDefaults() {
        map.addServicePolicyDefaults(ParameterState.Info.ADMIN);
        setDisabled(false);
    }

    @Override
    public ParameterMap getMap() {
        return map;
    }

    public void setAdminId(AdminId aid) {
        map.setParameter(ParameterState.AP_ID,
                         Integer.toString(aid.getAdminInstanceId()));
    }

    public AdminId getAdminId() {
        return new AdminId(map.getOrZeroInt(ParameterState.AP_ID));
    }

    public void setAdminStorageDir(String adminStorageDir) {
        map.setParameter(ParameterState.ADMIN_MOUNT_POINT,
                         adminStorageDir);
    }

    public String getAdminStorageDir() {
        return map.getOrDefault(ParameterState.ADMIN_MOUNT_POINT).asString();
    }

    public StorageNodeId getStorageNodeId() {
        return new StorageNodeId
            (map.getOrZeroInt(ParameterState.COMMON_SN_ID));
    }

    public void setStorageNodeId(StorageNodeId snId) {
        map.setParameter(ParameterState.COMMON_SN_ID,
                         Integer.toString(snId.getStorageNodeId()));
    }

    public String getNodeHostPort() {
        return map.get(ParameterState.JE_HOST_PORT).asString();
    }

    public void setNodeHostPort(String nodeHostPort) {
        map.setParameter(ParameterState.JE_HOST_PORT, nodeHostPort);
    }
    public int getWaitTimeout() {
        DurationParameter dp =
            (DurationParameter) map.get(ParameterState.AP_WAIT_TIMEOUT);
        return (int) dp.getAmount();
    }

    public TimeUnit getWaitTimeoutUnit() {
        DurationParameter dp =
            (DurationParameter) map.get(ParameterState.AP_WAIT_TIMEOUT);
        return dp.getUnit();
    }

    public void setDisabled(boolean disabled) {
        map.setParameter(ParameterState.COMMON_DISABLED,
                         Boolean.toString(disabled));
    }

    public boolean isDisabled() {
        return map.get(ParameterState.COMMON_DISABLED).asBoolean();
    }

    /* Get and set miscAsyncMaxConcurrentOps */

    public int getMiscAsyncMaxConcurrentOps() {
        return getMap().getOrDefault(COMMON_MISC_ASYNC_MAX_CONCURRENT_OPS)
            .asInt();
    }

    public void setMiscAsyncMaxConcurrentOps(int max) {
        getMap().setParameter(COMMON_MISC_ASYNC_MAX_CONCURRENT_OPS,
                              Integer.toString(max));
    }

    public boolean createCSV() {
        return  map.get(ParameterState.MP_CREATE_CSV).asBoolean();
    }

    public int getLogFileCount() {
        return map.getOrZeroInt(ParameterState.AP_LOG_FILE_COUNT);
    }

    public int getLogFileLimit() {
        return map.getOrZeroInt(ParameterState.AP_LOG_FILE_LIMIT);
    }

    public long getPollPeriodMillis() {
        DurationParameter dp =
            (DurationParameter) map.get(ParameterState.MP_POLL_PERIOD);
        return dp.toMillis();
    }

    public String getHelperHosts() {
        return map.get(ParameterState.JE_HELPER_HOSTS).asString();
    }

    public long getEventExpiryAge() {
        DurationParameter dp =
            (DurationParameter) map.get(ParameterState.AP_EVENT_EXPIRY_AGE);
        return dp.toMillis();
    }

    public void setEventExpiryAge(String age) {
        map.setParameter(ParameterState.AP_EVENT_EXPIRY_AGE, age);
    }

    public long getMaxEvents() {
        return map.getOrDefault(ParameterState.AP_EVENT_MAX).asLong();
    }

    /**
     * Gets the broadcast metadata retry delay. This delay is the time between
     * attempts to update RNs when trying to meet the metadata threshold.
     *
     * @return the broadcast metadata retry delay
     */
    public long getBroadcastMetadataRetryDelayMillis() {
        return ParameterUtils.getDurationMillis(map,
                              ParameterState.AP_BROADCAST_METADATA_RETRY_DELAY);
    }

    /**
     * Gets the broadcast metadata threshold. The threshold is the percent
     * of shards that must be successfully updated during a broadcast.
     *
     * @return the broadcast metadata threshold
     */
    public int getBroadcastMetadataThreshold() {
        return map.getOrDefault(
                        ParameterState.AP_BROADCAST_METADATA_THRESHOLD).asInt();
    }

    /**
     * Gets the broadcast topology retry delay. This delay is the time between
     * attempts to update RNs when trying to meet the threshold.
     *
     * @return the broadcast topology retry delay
     */
    public long getBroadcastTopoRetryDelayMillis() {
        return ParameterUtils.getDurationMillis(map,
                                ParameterState.AP_BROADCAST_TOPO_RETRY_DELAY);
    }

    /**
     * Gets the broadcast topology threshold. The threshold is the percent
     * of RNs that must be successfully updated during a broadcast.
     *
     * @return the broadcast topology threshold
     */
    public int getBroadcastTopoThreshold() {
        return
           map.getOrDefault(ParameterState.AP_BROADCAST_TOPO_THRESHOLD).asInt();
    }

    /**
     * Gets the maximum number of topology changes that are retained in the
     * stored topology. Only the latest maxTopoChanges are retained in the
     * stored topology.
     */
    public int getMaxTopoChanges() {
        return
           map.getOrDefault(ParameterState.AP_MAX_TOPO_CHANGES).asInt();
    }

    /**
     * Set the JE HA nodeHostPort to nodeHostname:haPort and the
     * helper host to helperHostname:helperPort.
     */
    public void setJEInfo(String nodeHostname, int haPort,
                          String helperHostname, int helperPort) {

        setJEInfo(HostPortPair.getString(nodeHostname, haPort),
                  HostPortPair.getString(helperHostname, helperPort));
    }

    /**
     * Set the JE HA nodeHostPort and helperHost fields.
     */
    public void setJEInfo(String nodeHostPort, String helperHost) {
        map.setParameter(ParameterState.JE_HOST_PORT, nodeHostPort);
        map.setParameter(ParameterState.JE_HELPER_HOSTS, helperHost);
    }

    /**
     * The total time, in milliseconds, that we should wait for an RN to
     * become consistent with a given lag target.
     */
    public int getAwaitRNConsistencyPeriod() {
        return (int) ParameterUtils.getDurationMillis
            (map, ParameterState.AP_WAIT_RN_CONSISTENCY);
    }

    /**
     * The total time, in milliseconds, that we should wait to contact a
     * service that is currently unreachable.
     */
    public DurationParameter getServiceUnreachablePeriod() {
        return (DurationParameter)
            map.getOrDefault(ParameterState.AP_WAIT_UNREACHABLE_SERVICE);
    }

    /**
     * The time that we should wait to contact an admin that failed over.
     */
    public DurationParameter getAdminFailoverPeriod() {
        return (DurationParameter)
            map.getOrDefault(ParameterState.AP_WAIT_ADMIN_FAILOVER);
    }

    /**
     * The time that we should wait to contact an RN that failed over.
     */
    public DurationParameter getRNFailoverPeriod() {
        return (DurationParameter)
            map.getOrDefault(ParameterState.AP_WAIT_RN_FAILOVER);
    }

    /**
     * The time that we should wait to check on the status of a partition
     * migration.
     */
    public DurationParameter getCheckPartitionMigrationPeriod() {
        return (DurationParameter)
            map.getOrDefault(ParameterState.AP_CHECK_PARTITION_MIGRATION);
    }

    /**
     * The time that we should wait to check on the status of an add index
     * operation.
     */
    public DurationParameter getCheckAddIndexPeriod() {
        return (DurationParameter)
            map.getOrDefault(ParameterState.AP_CHECK_ADD_INDEX);
    }

    /**
     * The time that we should wait to check on the status of a network restore.
     */
    public DurationParameter getCheckNetworkRestorePeriod() {
        return (DurationParameter)
            map.getOrDefault(ParameterState.AP_CHECK_NETWORK_RESTORE);
    }
    public void setHelperHost(String helpers) {
        map.setParameter(ParameterState.JE_HELPER_HOSTS, helpers);
    }

    /*
     * The following accessors are for session and token cache configuration
     */

    public void setSessionLimit(String value) {
        map.setParameter(ParameterState.COMMON_SESSION_LIMIT, value);
    }

    public int getSessionLimit() {
        return map.getOrDefault(ParameterState.COMMON_SESSION_LIMIT).asInt();
    }

    public void setLoginCacheSize(String value) {
        map.setParameter(ParameterState.COMMON_LOGIN_CACHE_SIZE, value);
    }

    public int getLoginCacheSize() {
        return map.getOrDefault(ParameterState.COMMON_LOGIN_CACHE_SIZE).asInt();
    }

    /**
     * Set the Admin's type. If type is null the type is set to the default.
     *
     * @param type the Admin type or null
     */
    public void setType(final AdminType type) {
        final String typeName = (type == null) ? AP_TYPE_DEFAULT : type.name();
        map.setParameter(AP_TYPE, typeName);
    }

    /**
     * Get the Admin's type.
     *
     * @return the Admin type
     */
    public AdminType getType() {
        return AdminType.valueOf(map.getOrDefault(AP_TYPE).asString());
    }

    /**
     * Sets the value of the JE HA electable group size override.
     *
     * @param value the electable group size override
     */
    public void setElectableGroupSizeOverride(int value) {
        final String stringValue = Integer.toString(value);
        map.setParameter(
            COMMON_ELECTABLE_GROUP_SIZE_OVERRIDE,
            (COMMON_ELECTABLE_GROUP_SIZE_OVERRIDE_DEFAULT.equals(stringValue) ?
             null : stringValue));
    }

    /**
     * Returns the value of the JE HA electable group size override.
     *
     * @return the electable group size override
     */
    public int getElectableGroupSizeOverride() {
        return map.getOrDefault(COMMON_ELECTABLE_GROUP_SIZE_OVERRIDE).asInt();
    }

    /**
     * Specifies whether to reset the JE replication group on start up.
     *
     * @param value whether to reset the JE replication group
     */
    public void setResetRepGroup(boolean value) {
        final String stringValue = Boolean.toString(value);
        map.setParameter(COMMON_RESET_REP_GROUP,
                         (COMMON_RESET_REP_GROUP_DEFAULT.equals(stringValue) ?
                          null : stringValue));
    }

    /**
     * Returns whether to reset the JE replication group on start up.
     *
     * @return whether to reset the JE replication group
     */
    public boolean getResetRepGroup() {
        return map.getOrDefault(COMMON_RESET_REP_GROUP).asBoolean();
    }

    /**
     * Returns the replication acknowledgment timeout
     *
     * @return the replication acknowledgment timeout
     */
    public int getAckTimeoutMillis() {
        return (int)ParameterUtils.getDurationMillis(
                map, ParameterState.COMMON_REPLICA_ACK_TIMEOUT);
    }

    public String getMallocArenaMax() {
        return
            map.getOrDefault(ParameterState.ADMIN_MALLOC_ARENA_MAX).asString();
    }

    /**
     * The time interval between processing of the
     * KVMetadataAdmin thread.
     */
    public DurationParameter getKVMetadataAdminCheckInterval() {
        return (DurationParameter)
            map.getOrDefault(ParameterState.AP_PARAM_CHECK_INTERVAL);
    }

    /**
     * The maximum time to wait for a plan executed by the
     * KVMetadataAdmin thread.
     */
    public DurationParameter getKVMetadataAdminMaxPlanWait() {
        return (DurationParameter)
            map.getOrDefault(ParameterState.AP_PARAM_MAX_PLAN_WAIT);
    }

    /**
     * The time interval between processing of the
     * KVVersionUpdater thread.
     */
    public DurationParameter getVersionCheckInterval() {
        return (DurationParameter)
            map.getOrDefault(ParameterState.AP_VERSION_CHECK_INTERVAL);
    }

    /**
     * Enable/Disable the MetadataAdminThread.
     */
    public void setMetadataAdminThreadEnabled(boolean flag) {
        map.setParameter(ParameterState.AP_PARAM_CHECK_ENABLED,
                         Boolean.toString(flag));
    }

    public boolean isMetadataAdminThreadEnabled() {
        return map.getOrDefault(
                   ParameterState.AP_PARAM_CHECK_ENABLED).asBoolean();
    }

    /**
     * Enable/Disable the KVVersionThread.
     */
    public void setVersionThreadEnabled(boolean flag) {
        map.setParameter(ParameterState.AP_VERSION_CHECK_ENABLED,
                         Boolean.toString(flag));
    }

    public boolean isVersionThreadEnabled() {
        return map.getOrDefault(
            ParameterState.AP_VERSION_CHECK_ENABLED).asBoolean();
    }

    public boolean getCollectEnvStats() {
        return map.getOrDefault(SP_COLLECT_ENV_STATS).asBoolean();
    }

    /**
     * Duplicates the dns cache ttl values and returns {@code true} if
     * modified.
     */
    public void duplicateCacheTTLValues(ParameterMap snParamsMap) {
        map.duplicateCacheTTLValues(snParamsMap);
    }

    /**
     * Returns the polling interval in milliseconds for the event recorder.
     */
    public long getEventRecorderPollingIntervalMs() {
        return ParameterUtils.getDurationMillis(
            map, ADMIN_EVENT_RECORDER_POLLING_INTERVAL);
    }
}
