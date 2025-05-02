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

import static oracle.kv.impl.param.ParameterState.COMMON_ELECTABLE_GROUP_SIZE_OVERRIDE;
import static oracle.kv.impl.param.ParameterState.COMMON_ELECTABLE_GROUP_SIZE_OVERRIDE_DEFAULT;
import static oracle.kv.impl.param.ParameterState.COMMON_MASTER_BALANCE;
import static oracle.kv.impl.param.ParameterState.COMMON_RESET_REP_GROUP;
import static oracle.kv.impl.param.ParameterState.JE_CACHE_SIZE;
import static oracle.kv.impl.param.ParameterState.JE_ENABLE_ERASURE;
import static oracle.kv.impl.param.ParameterState.JE_ERASURE_PERIOD;
import static oracle.kv.impl.param.ParameterState.JE_NODE_PRIORITY;
import static oracle.kv.impl.param.ParameterState.KV_CACHE_MODE;
import static oracle.kv.impl.param.ParameterState.REPNODE_TYPE;
import static oracle.kv.impl.param.ParameterState.REQUEST_QUIESCE_TIME;
import static oracle.kv.impl.param.ParameterState.RNLOG_MOUNT_POINT;
import static oracle.kv.impl.param.ParameterState.RNLOG_MOUNT_POINT_SIZE;
import static oracle.kv.impl.param.ParameterState.RN_ACCEPT_MAX_ACTIVE_CONNS;
import static oracle.kv.impl.param.ParameterState.RN_CACHE_MB_MIN;
import static oracle.kv.impl.param.ParameterState.RN_CACHE_PERCENT;
import static oracle.kv.impl.param.ParameterState.RN_DISABLE_AUTH_MASTER;
import static oracle.kv.impl.param.ParameterState.RN_ENABLED_REQUEST_TYPE;
import static oracle.kv.impl.param.ParameterState.RN_ENABLED_REQUEST_TYPE_DEFAULT;
import static oracle.kv.impl.param.ParameterState.RN_HEAP_MB_MIN;
import static oracle.kv.impl.param.ParameterState.RN_MOUNT_POINT;
import static oracle.kv.impl.param.ParameterState.RN_MOUNT_POINT_SIZE;
import static oracle.kv.impl.param.ParameterState.RN_NODE_TYPE;
import static oracle.kv.impl.param.ParameterState.RN_RH_ASYNC_EXEC_MAX_THREADS;
import static oracle.kv.impl.param.ParameterState.RN_RH_ASYNC_EXEC_THREAD_KEEP_ALIVE;
import static oracle.kv.impl.param.ParameterState.RN_RH_ASYNC_MAX_CONCURRENT_REQUESTS;
import static oracle.kv.impl.param.ParameterState.RN_RH_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.RN_RH_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RN_RH_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RP_RN_ID;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.MAIN;

import java.io.File;
import java.util.EnumSet;

import oracle.kv.impl.admin.param.StorageNodeParams.RNHeapAndCacheSize;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.RequestTypeUpdater.RequestType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryArgs;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.table.TimeToLive;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.utilint.JVMSystemUtils;
import com.sleepycat.je.utilint.TaskCoordinator;

/**
 * A class implementing RepNodeParams contains all the per-RepNode operational
 * parameters.
 */
public class RepNodeParams extends GroupNodeParams {

    private static final long serialVersionUID = 1L;

    private final ParameterMap map;

    public RepNodeParams(ParameterMap map) {
        this.map = map;
    }

    public RepNodeParams(RepNodeParams rnp) {
        this(rnp.getMap().copy());
    }

    /**
     * Create a default RepNodeParams for creating a new rep node.
     * TODO - kvLite and unit tests
     */
    public RepNodeParams(StorageNodeId snid,
                         RepNodeId repNodeId,
                         boolean disabled,
                         String haHostname,
                         int haPort,
                         String helperHostname,
                         int helperPort,
                         String directoryPath,
                         NodeType nodeType) {
        this(snid, repNodeId, disabled, haHostname, haPort,
             HostPortPair.getString(helperHostname, helperPort),
             directoryPath, nodeType);
    }

    // TODO - above and unit tests
    public RepNodeParams(StorageNodeId snid,
                         RepNodeId repNodeId,
                         boolean disabled,
                         String haHostname,
                         int haPort,
                         String helperHosts,
                         String directoryPath,
                         NodeType nodeType) {
        this(new ParameterMap(), snid, repNodeId, disabled,
             haHostname, haPort, helperHosts, directoryPath, 0L,
             null, 0L, nodeType);
    }

    /**
     * Create RepNodeParams from existing map, which is probably a copy of the
     * policy map
     */
    // TODO deploy RN and unit tests
    public RepNodeParams(ParameterMap oldmap,
                         StorageNodeId snid,
                         RepNodeId repNodeId,
                         boolean disabled,
                         String haHostname,
                         int haPort,
                         String helperHosts,
                         String directoryPath,
                         long directorySize,
                         String logDirectoryPath,
                         long logDirectorySize,
                         NodeType nodeType) {
        map = oldmap.filter(EnumSet.of(ParameterState.Info.REPNODE));
        setStorageNodeId(snid);
        setRepNodeId(repNodeId);
        setDisabled(disabled);
        setJENodeHostPort(HostPortPair.getString(haHostname, haPort));
        setJEHelperHosts(helperHosts);
        addDefaults();
        map.setName(repNodeId.getFullName());
        map.setType(REPNODE_TYPE);
        setStorageDirectory(directoryPath, directorySize);
        setLogDirectory(logDirectoryPath, logDirectorySize);
        setNodeType(nodeType);
    }

    @Override
    public ParameterMap getMap() {
        return map;
    }

    private void addDefaults() {
        map.addServicePolicyDefaults(ParameterState.Info.REPNODE);
    }

    public RepNodeId getRepNodeId() {
        return RepNodeId.parse(map.get(RP_RN_ID).asString());
    }

    public void setRepNodeId(RepNodeId rnid) {
        map.setParameter(RP_RN_ID, rnid.getFullName());
    }

    /**
     * Returns a new File instance for the storage directory if present.
     *
     * @return a new File instance for the storage directory or null
     */
    public File getStorageDirectoryFile() {
        if (map.exists(RN_MOUNT_POINT)) {
            return new File(map.get(RN_MOUNT_POINT).asString());
        }
        return null;
    }

    /**
     * Returns the storage directory size if present. If the size is not present
     * 0 is returned.
     *
     * @return the storage directory size
     */
    public long getStorageDirectorySize() {
        if (map.exists(RN_MOUNT_POINT_SIZE)) {
            return map.get(RN_MOUNT_POINT_SIZE).asLong();
        }
        return 0L;
    }

    /**
     * Returns the storage directory path if present, otherwise null is
     * returned.
     *
     * @return the storage directory path or null
     */
    public String getStorageDirectoryPath() {
        if (map.exists(RN_MOUNT_POINT)) {
            return map.get(RN_MOUNT_POINT).asString();
        }
        return null;
    }

    /**
     * Sets the storage directory string and size parameters.
     *
     * @param path storage directory path or null
     * @param size directory size (may be 0)
     */
    public void setStorageDirectory(String path, long size) {
        assert size >= 0;

        /*
         * Set the storage directory, even if the path value is null.
         * Setting it to a null value is how we clear it out if we are
         * moving it to the root dir of a new SN.
         */
        map.setParameter(RN_MOUNT_POINT, path);
        map.setParameter(RN_MOUNT_POINT_SIZE, Long.toString(size));
    }

    /**
     * Returns a new File instance for the RN log directory if present.
     *
     * @return a new File instance for the RN log directory or null
     */
    public File getLogDirectoryFile() {
        if (map.exists(RNLOG_MOUNT_POINT)) {
            return new File(map.get(RNLOG_MOUNT_POINT).asString());
        }
        return null;
    }

    /**
     * Returns the RN log directory size if present. If the size is not present
     * 0 is returned.
     *
     * @return the RN log directory size
     */
    public long getLogDirectorySize() {
        if (map.exists(RNLOG_MOUNT_POINT_SIZE)) {
            return map.get(RNLOG_MOUNT_POINT_SIZE).asLong();
        }
        return 0L;
    }

    /**
     * Returns the RN log directory path if present, otherwise null is
     * returned.
     *
     * @return the RN log directory path or null
     */
    public String getLogDirectoryPath() {
        if (map.exists(RNLOG_MOUNT_POINT)) {
            return map.get(RNLOG_MOUNT_POINT).asString();
        }
        return null;
    }

    /**
     * Sets the RN log directory string and size parameters.
     *
     * @param path RN log directory path or null
     * @param size RN log directory size (may be 0)
     */
    public void setLogDirectory(String path, long size) {
        assert size >= 0;

        /*
         * Set the RN log directory, even if the path value is null.
         * Setting it to a null value is how we clear it out if we are
         * moving it to the root dir of a new SN.
         */
        map.setParameter(RNLOG_MOUNT_POINT, path);
        map.setParameter(RNLOG_MOUNT_POINT_SIZE, Long.toString(size));
    }

    public long getJECacheSize() {
        return map.get(JE_CACHE_SIZE).asLong();
    }

    public void setJECacheSize(long size) {
        map.setParameter(JE_CACHE_SIZE, Long.toString(size));
    }

    public boolean getErasureEnabled() {
        return map.getOrDefault(JE_ENABLE_ERASURE).asBoolean();
    }

    public void setErasureEnabled(boolean enable) {
        map.setParameter(JE_ENABLE_ERASURE, Boolean.toString(enable));
    }

    public DurationParameter getErasurePeriod() {
        return (DurationParameter) map.getOrDefault(JE_ERASURE_PERIOD);
    }

    public void setErasurePeriod(DurationParameter erasurePeriod) {
        map.setParameter(JE_ERASURE_PERIOD, erasurePeriod.asString());
    }

    public int getJENodePriority() {
        return map.getOrDefault(JE_NODE_PRIORITY).asInt();
    }

    public void setJENodePriority(int nodePriority) {
        map.setParameter(JE_NODE_PRIORITY, Integer.toString(nodePriority));
    }

    /**
     * Set the RN heap and cache as a function of memory available on the
     * SN, and SN capacity. If either heap or cache is not specified, use
     * the JVM and JE defaults.
     */
    public void setRNHeapAndJECache(RNHeapAndCacheSize heapAndCache) {
        /*
         * If the heap val is null, remove any current -Xms and Xmx flags,
         * else replace them with the new value.
         */
        setJavaMiscParams(replaceOrRemoveJVMArg
                          (getJavaMiscParams(), XMS_FLAG,
                           heapAndCache.getHeapValAndUnit()));
        setJavaMiscParams(replaceOrRemoveJVMArg
                          (getJavaMiscParams(), XMX_FLAG,
                           heapAndCache.getHeapValAndUnit()));
        setJECacheSize(heapAndCache.getCacheBytes());
        setRNCachePercent(heapAndCache.getCachePercent());
    }

    public CacheMode getJECacheMode() {
        return (CacheMode) map.get(KV_CACHE_MODE).asEnum();
    }

    public void setJECacheMode(CacheMode mode) {
        map.setParameter(KV_CACHE_MODE, mode.toString());
    }

    /**
     * Validate the JE cache and JVM heap parameters.
     *
     * @param inTargetJVM is true if the current JVM is the target for
     * validation.  This allows early detection of potential memory issues for
     * RepNode startup.

     * TODO: look at actual physical memory on the machine to validate.
     */
    public void validateCacheAndHeap(boolean inTargetJVM) {
        final double maxPercent = 0.90;

        long maxHeapMB = getMaxHeapMB();
        if (inTargetJVM) {
            long actualMemory = JVMSystemUtils.getRuntimeMaxMemory();
            long specHeap = maxHeapMB >> 20;
            if (specHeap > actualMemory) {
                throw new IllegalArgumentException
                ("Specified maximum heap of " + maxHeapMB  +
                 "MB exceeds available JVM memory of " + actualMemory +
                 ", see JVM parameters:" + getJavaMiscParams());
            }
        }

        long maxHeapBytes = getMaxHeapBytes();
        if (maxHeapBytes > 0 && getJECacheSize() > 0) {
            checkMinSizes(maxHeapBytes, (int)(getJECacheSize()/(1024*1024)));
            /*
             * This check for cacheSize <= heapSize*0.9 is inadequate to
             * predict whether the cache size is really large enough, given
             * that JVMs have overhead in the heap, especially Zing. But it is
             * better than nothing. We could add a Zing-specific check later.
             * It is always better to set cache size as a % rather than bytes.
             */
            if ((getJECacheSize()/((double)maxHeapBytes)) > maxPercent) {
                String msg = "Parameter " + ParameterState.JE_CACHE_SIZE +
                    " (" + getJECacheSize() + ") may not exceed 90% of " +
                    "available Java heap space (" + maxHeapBytes + ")";
                throw new IllegalArgumentException(msg);
            }
        }

        long minHeapMB = getMinHeapMB();
        if ((maxHeapMB > 0) && (minHeapMB > maxHeapMB)) {
            throw new IllegalArgumentException
            ("Mininum heap of " + minHeapMB + " exceeds maximum heap of " +
             maxHeapMB + ", see JVM parameters:" + getJavaMiscParams());
        }
    }

    private void checkMinSizes(long heapBytes, int cacheMB) {
        if ((heapBytes >> 20) < RN_HEAP_MB_MIN) {
            String msg = "JVM heap must be at least " + RN_HEAP_MB_MIN +
                " MB, specified value is " + (heapBytes>>20) + " MB";
            throw new IllegalArgumentException(msg);
        }
        if (cacheMB < RN_CACHE_MB_MIN) {
            String msg = "Cache size must be at least " + RN_CACHE_MB_MIN +
                " MB, specified value is " + cacheMB + " MB";
            throw new IllegalArgumentException(msg);
        }
    }

    public int getRequestQuiesceMs() {
        final long quiesceMs = ParameterUtils.
            getDurationMillis(map, REQUEST_QUIESCE_TIME);
        return (quiesceMs > Integer.MAX_VALUE) ?
                Integer.MAX_VALUE : (int)quiesceMs;
    }

    public SocketFactoryPair getRHSFP(RMISocketPolicy rmiPolicy,
                                      String servicePortRange,
                                      String csfName,
                                      String kvStoreName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(MAIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(RN_RH_SO_BACKLOG).asInt()).
            setSsfAcceptMaxActiveConns(
                map.getOrDefault(RN_ACCEPT_MAX_ACTIVE_CONNS).asInt()).
            setSsfPortRange(servicePortRange).
            setCsfName(csfName).
            setCsfConnectTimeout((int)ParameterUtils.getDurationMillis(
                                     map, RN_RH_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout((int)ParameterUtils.getDurationMillis(
                                  map, RN_RH_SO_READ_TIMEOUT)).
            setKvStoreName(kvStoreName);

        return rmiPolicy.getBindPair(args);
    }

    /* -- Partition migration parameters -- */

    /**
     * Gets the maximum number of concurrent partition migration sources.
     *
     * @return the maximum number of concurrent sources
     */
    public int getConcurrentSourceLimit() {
        return map.getOrDefault(
                ParameterState.RN_PM_CONCURRENT_SOURCE_LIMIT).asInt();
    }

    /**
     * Gets the maximum number of concurrent partition migration targets.
     *
     * @return the maximum number of concurrent migration targets
     */
    public int getConcurrentTargetLimit() {
        return map.getOrDefault(
                ParameterState.RN_PM_CONCURRENT_TARGET_LIMIT).asInt();
    }

    /**
     * Gets the wait time (in milliseconds) before trying a partition migration
     * service request after a busy response.
     *
     * @return the wait time after a busy service response
     */
    public long getWaitAfterBusy() {
        return ParameterUtils.getDurationMillis(map,
                                         ParameterState.RN_PM_WAIT_AFTER_BUSY);
    }

    /**
     * Gets the wait time (in milliseconds) before trying a partition migration
     * service request after an error response.
     *
     * @return the wait time after an error service response
     */
    public long getWaitAfterError() {
        return ParameterUtils.getDurationMillis(map,
                                         ParameterState.RN_PM_WAIT_AFTER_ERROR);
    }

    /**
     * Gets the time interval (in milliseconds) between updates of table
     * level statistics.
     *
     * @return the time interval (in milliseconds) between updates of table
     * level statistics.
     */
    public long getStatsGatherInterval() {
        return ParameterUtils.getDurationMillis(map,
                                         ParameterState.RN_SG_INTERVAL);
    }

    /**
     * Gets the time interval (in milliseconds) between updates of table
     * size statistics.
     *
     * @return the time interval (in milliseconds) between updates of table
     * size statistics.
     */
    public long getStatsTableSizeUpdateInterval() {
        return ParameterUtils.getDurationMillis(map,
                                      ParameterState.RN_SG_SIZE_UPDATE_INTERVAL);
    }

    /**
     * Gets the statistics scanning lease duration time (in milliseconds).
     *
     * @return the lease duration time
     */
    public long getStatsLeaseDuration() {
        return ParameterUtils.getDurationMillis(map,
                                        ParameterState.RN_SG_LEASE_DURATION);
    }

    /**
     * Gets the timeout used to obtain a permit for the task
     *
     * @param task the task requesting the permit
     *
     * @return the timeout in ms
     */
    public long getPermitTimeoutMs(TaskCoordinator.Task task) {
        return ParameterUtils.
            getDurationMillis(map, getPermitTimeoutParamName(task));

    }

    /**
     * Utility to convert a task name into a config parameter name.
     *
     * Note: Do not change, since it will invalidate existing configurations.
     */
    public static String getPermitTimeoutParamName(TaskCoordinator.Task task) {
        return "rn" + task.getName() + "PermitTimeout";
    }

    /**
     * Gets the lease associated with permits obtained by the task
     *
     * @param task the task requesting the permit
     *
     * @return the lease in ms
     */
    public long getPermitLeaseMs(TaskCoordinator.Task task) {
        return ParameterUtils.
            getDurationMillis(map, getPermitLeaseParamName(task));
    }

    /**
     * Utility to convert a task name into a config parameter name.
     *
     * Note: Do not change, since it will invalidate existing configurations.
     */
    public static String getPermitLeaseParamName(TaskCoordinator.Task task) {
        return "rn" + task.getName() + "PermitLease";
    }

    /**
     * Returns whether to enable statistics gathering
     *
     * @return true when enabling gathering; otherwise return false.
     */
    public boolean getStatsEnabled() {
        return map.getOrDefault(ParameterState.RN_SG_ENABLED).asBoolean();
    }

    /**
     * Returns the ttl for table and index statistics data
     *
     * @return the ttl for table and index statistics data
     */
    public TimeToLive getStatsTTL() {
        return ParameterUtils.getTimeToLive(map, ParameterState.RN_SG_TTL);
    }

    /**
     * Returns whether statistics gathering should include information about
     * storage size.
     *
     * @return whether stats should include storage size
     */
    public boolean getStatsIncludeStorageSize() {
        return map.getOrDefault(ParameterState.RN_SG_INCLUDE_STORAGE_SIZE)
            .asBoolean();
    }

    /**
     * Gets the sleep or wait time for statistics scanning (in milliseconds).
     * @return the sleep or wait time
     */
    public long getStatsSleepWaitDuration() {
        return ParameterUtils.getDurationMillis(map,
                                        ParameterState.RN_SG_SLEEP_WAIT);
    }

    /**
     * Gets the RN wait time in store contraction (in milliseconds).
     * @return the wait time in ms
     */
    public long getContractWaitDurationMs() {
        return
            ParameterUtils.getDurationMillis(map,
                                             ParameterState.RN_CONTRACT_WAIT);
    }

    /**
     * Gets the socket read or write timeout (in milliseconds) for the
     * partition migration stream.
     *
     * @return the socket read or write timeout
     */
    public int getReadWriteTimeout() {
        return (int)ParameterUtils.getDurationMillis(map,
                                 ParameterState.RN_PM_SO_READ_WRITE_TIMEOUT);
    }

    /**
     * Gets the socket connect timeout (in milliseconds) for the partition
     * migration stream.
     *
     * @return the socket connect timeout
     */
    public int getConnectTImeout() {
        return (int)ParameterUtils.getDurationMillis(map,
                                 ParameterState.RN_PM_SO_CONNECT_TIMEOUT);
    }

    public int getActiveThreshold() {
        return map.get(ParameterState.SP_ACTIVE_THRESHOLD).asInt();
    }

    public String getMallocArenaMax() {
        return map.getOrDefault(ParameterState.RN_MALLOC_ARENA_MAX).asString();
    }

    public boolean getMasterBalance() {
        return map.getOrDefault(COMMON_MASTER_BALANCE).asBoolean();
    }

    public void setMasterBalance(boolean masterBalance) {
        map.setParameter(COMMON_MASTER_BALANCE,
                         (masterBalance ? "true" : "false"));
    }

    public void setLatencyCeiling(int ceiling) {
        map.setParameter(ParameterState.SP_LATENCY_CEILING,
                         Integer.toString(ceiling));
    }

    public int getLatencyCeiling() {
        return map.getOrZeroInt(ParameterState.SP_LATENCY_CEILING);
    }

    public void setThroughputFloor(int floor) {
        map.setParameter(ParameterState.SP_THROUGHPUT_FLOOR,
                         Integer.toString(floor));
    }

    public int getThroughputFloor() {
        return map.getOrZeroInt(ParameterState.SP_THROUGHPUT_FLOOR);
    }

    /**
     * This percent of the RN heap will be set at the JE cache.
     */
    public int getRNCachePercent() {
        return map.getOrZeroInt(ParameterState.RN_CACHE_PERCENT);
    }

    public void setRNCachePercent(long percent) {
        map.setParameter(RN_CACHE_PERCENT, Long.toString(percent));
    }

    public int getMaxIndexKeysPerRow() {
        return map.getOrDefault(ParameterState.RN_MAX_INDEX_KEYS_PER_ROW).
               asInt();
    }

    public void setMaxIndexKeysPerRow(int max) {
        map.setParameter(ParameterState.RN_MAX_INDEX_KEYS_PER_ROW,
                         Integer.toString(max));
    }

    /**
     * Get the node's JE HA node type.
     *
     * @return the node type
     */
    public NodeType getNodeType() {
        return NodeType.valueOf(map.getOrDefault(RN_NODE_TYPE).asString());
    }

    /**
     * Set the node's JE HA node type.
     *
     * @param nodeType the node type
     */
    public void setNodeType(final NodeType nodeType) {
        map.setParameter(RN_NODE_TYPE, nodeType.name());
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
        map.setParameter(COMMON_RESET_REP_GROUP, stringValue);
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
     * Specifies enabled request type on this node. The type could be all, none
     * and readonly.
     * @param requestType request type being enabled on this node
     */
    public void setEnabledRequestType(RequestType requestType) {
        final String stringValue = requestType.name();
        map.setParameter(
            RN_ENABLED_REQUEST_TYPE,
            (RN_ENABLED_REQUEST_TYPE_DEFAULT.equals(stringValue) ?
            null : stringValue));
    }

    /**
     * Returns enabled request type on this node. The type could be all, none
     * and readonly.
     *
     * @return enabled request type on this node
     */
    public RequestType getEnabledRequestType() {
        return RequestType.valueOf(
            map.getOrDefault(RN_ENABLED_REQUEST_TYPE).asString());
    }

    /**
     * Returns the maximum number of threads in the thread pool the async
     * request handler uses to execute incoming requests.
     *
     * @return the maximum number of threads
     */
    public int getAsyncExecMaxThreads() {
        return map.getOrDefault(RN_RH_ASYNC_EXEC_MAX_THREADS).asInt();
    }

    /**
     * Sets the maximum number of threads in the thread pool the async request
     * handler uses to execute incoming requests.
     *
     * @param maxThreads the maximum number of threads
     */
    public void setAsyncExecMaxThreads(int maxThreads) {
        final String stringValue = Integer.toString(maxThreads);
        map.setParameter(RN_RH_ASYNC_EXEC_MAX_THREADS, stringValue);
    }

    /**
     * Returns the amount of time in milliseconds that a thread in the thread
     * pool the async request handler uses to execute incoming requests will
     * remain idle before terminating.
     *
     * @return the keep alive time in milliseconds
     */
    public int getAsyncExecThreadKeepAliveMs() {
        final long keepAliveMs = ParameterUtils.getDurationMillis(
            map, RN_RH_ASYNC_EXEC_THREAD_KEEP_ALIVE);
        return (keepAliveMs > Integer.MAX_VALUE) ?
            Integer.MAX_VALUE :
            (int) keepAliveMs;
    }

    public TimeToLive getTombstoneTTL() {
        return ParameterUtils.getTimeToLive(map,
                                            ParameterState.RN_TOMBSTONE_TTL);
    }

    /**
     * Sets the amount of time in milliseconds that a thread in the thread
     * pool the async request handler uses to execute incoming requests will
     * remain idle before terminating.
     *
     * @param keepAlive the keep alive time in milliseconds
     */
    public void setAsyncExecThreadKeepAliveMs(int keepAlive) {
        final String stringValue = keepAlive + " milliseconds";
        map.setParameter(RN_RH_ASYNC_EXEC_THREAD_KEEP_ALIVE, stringValue);
    }

    /**
     * Returns the maximum number of active socket connections that the async
     * request handler will permit, or 0 if there is no limit.
     *
     * @return the maximum number of active connections or 0
     */
    public int getAcceptMaxActiveConns() {
        return map.getOrDefault(RN_ACCEPT_MAX_ACTIVE_CONNS).asInt();
    }

    /**
     * Sets the maximum number of active socket connections that the async
     * request handler will permit, or 0 if there is no limit.
     *
     * @param max the maximum number of active connections or 0
     */
    public void setAcceptMaxActiveConns(int max) {
        map.setParameter(RN_ACCEPT_MAX_ACTIVE_CONNS, Integer.toString(max));
    }

    /**
     * Returns true if the RN must use the older(permissive) authoritative
     * master semantics, false if using new(strict) semantics.
     *
     * @see ParameterState#RN_DISABLE_AUTH_MASTER
     */
    public boolean getDisableAuthMaster() {
        return map.getOrDefault(RN_DISABLE_AUTH_MASTER).asBoolean();
    }

    /**
     * Set the JE auth semantics to be used by the RN
     * @param disableAuthMaster true for old (permissive) authoritative master
     * semantics, false for new (strict) semantics.
     */
    public void setDisableAuthMaster(boolean disableAuthMaster) {
        map.setParameter(RN_DISABLE_AUTH_MASTER,
                         Boolean.toString(disableAuthMaster));
    }

    /**
     * Returns the maximum number of concurrent async requests that an RN can
     * handle before it should do throttling.
     */
    public int getAsyncMaxConcurrentRequests() {
        return map.getOrDefault(RN_RH_ASYNC_MAX_CONCURRENT_REQUESTS).asInt();
    }

    /**
     * Sets the maximum number of concurrent async requests that an RN can
     * handle before it should do throttling.
     */
    public void setAsyncMaxConcurrentRequests(int maxConcurrentRequests) {
        map.setParameter(RN_RH_ASYNC_MAX_CONCURRENT_REQUESTS,
                         Integer.toString(maxConcurrentRequests));
    }

    @Override
    public String getJvmOverride() {
        return getMap().getOrDefault(
            ParameterState.JVM_RN_OVERRIDE).asString() +
            " " +
            getMap().getOrDefault(
                ParameterState.JVM_RN_OVERRIDE_NO_RESTART).asString();
    }

    @Override
    public String toString() {
        return map.toString();
    }
}
