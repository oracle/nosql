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

package oracle.kv.impl.param;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.RequestLimitConfig;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.table.TimeToLive;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.config.ConfigParam;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.arbiter.ArbiterConfig;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * This is a utility class with some useful methods for handling Parameters,
 * and in particular for initializing JE parameters. See header comments in
 * Parameter.java for an overview of Parameters.
 *
 * == JE RN Parameter Documentation ==
 *
 * This class defines JE param default values used for RNs, when these are
 * different than the standard JE default values. In the comments below we
 * document the default values and list JE params that must not be changed in
 * KVS, in order to supply this raw material to the doc group. These comments
 * should be updated whenever JE param defaults are changed in the source code,
 * and when the set of modifiable JE params is changed. The doc group can diff
 * this file from release to release to discover changes that may need to be
 * documented.
 *
 * Note that modifying JE params should be done by KVS users with extreme
 * caution. KVS is not tested by Oracle with non-default JE params. This should
 * emphasized in the documentation.
 *
 * Under "JE Parameter" below, the JE class names are abbreviated:
 *
 *   EC: EnvironmentConfig
 *   RC: ReplicationConfig
 *   RMC: ReplicationMutableConfig
 *
 * Note: the lists below are currently incomplete because not all JE params
 * have been fully documented at this stage. This is a work in progress.
 *
 * The following JE params may be modified via special KVS params:
 *
 * JE Parameter      KVS Default Value      KVS Parameter
 * ------------      -----------------      -------------
 * EC.setCacheMode   EVICT_LN               cacheMode store param
 * EC.MAX_DISK       zero                   storageDirSize or rootDirSize
 * EC.MAX_MEMORY     None                   cacheSize RN param
 *
 * Note that MAX_MEMORY is zero in JE and KVS by default, which means that
 * MAX_MEMORY_PERCENT will be used. See the MAX_MEMORY_PERCENT default below.
 *
 * All JE params have JE string constants defined in EC, RC and RMC, and may
 * be modified via the RN configProperties param. However, the following JE
 * params should not be modified because this could result in unpredictable KVS
 * behavior. In a future release, attempting to change them may result in an
 * error message. They're listed alphabetically by JE constant name.
 *
 *   RC.CONSISTENCY_POLICY -- only used for KVS internal operations.
 *   EC.ENV_RUN_EVICTOR -- eviction is run in background.
 *   EC.MAX_MEMORY -- must only be set via KVS cacheSize param.
 *   EC.SHARED_CACHE -- an RN only has one JE environment.
 *   EC.TXN_DURABILITY -- only used for KVS internal operations.
 *
 * The following JE params are assigned default values in KVS, overriding
 * the JE default values. They're listed alphabetically by JE constant name.
 *
 * JE Parameter                     KVS Default Value
 * ------------                     -----------------
 * EC.CHECKPOINTER_BYTES_INTERVAL   Set per storage type, see below
 * EC.CLEANER_MIN_FILE_UTILIZATION  5
 * EC.CLEANER_MIN_UTILIZATION       40
 * EC.CLEANER_READ_SIZE             1,048,576
 * EC.CLEANER_THREADS               2
 * RC.ENV_UNKNOWN_STATE_TIMEOUT     10 s
 * EC.EVICTOR_CRITICAL_PERCENTAGE   20
 * RC.FEEDER_TIMEOUT                10 s
 * EC.LOCK_N_LOCK_TABLES            97
 * EC.LOCK_TIMEOUT                  10 s
 * EC.LOG_FAULT_READ_SIZE           4,096
 * EC.LOG_FILE_CACHE_SIZE           2000
 * EC.LOG_FILE_MAX                  1,073,741,824
 * EC.LOG_ITERATOR_READ_SIZE        1,048,576
 * EC.LOG_NUM_BUFFERS               16
 * EC.LOG_WRITE_QUEUE_SIZE          2,097,152
 * EC.MAX_MEMORY_PERCENT            70%
 * RMC.REPLAY_MAX_OPEN_DB_HANDLES   100
 * RC.REPLICA_ACK_TIMEOUT           5 s
 * RC.REPLICA_TIMEOUT               10 s
 * RC.TXN_ROLLBACK_LIMIT            10
 *
 * The EC.CHECKPOINTER_BYTES_INTERVAL is set differently for each storage type:
 *    NVME: 4,000,000,000
 *    SSD: 2,000,000,000
 *    Default (hard disk): 500,000,000
 * (See RepEnvHandleManager.setEnvCheckpointInterval().)
 *
 * The following JE params are assigned default values in KVS but they are not
 * listed above because the KVS default is the same as the JE default. Perhaps
 * the KVS code that sets them should be removed.
 *
 *   EC.ENV_RUN_EVICTOR
 *   EC.NODE_MAX_ENTRIES
 *
 * The following JE undocumented params are assigned default values in KVS
 * but they are not listed above because they are meant to remain undocumented.
 * They're listed alphabetically.
 *
 *   je.rep.ignoreSecondaryNodeId
 *   je.rep.preHeartbeatTimeoutMs
 *   je.rep.preserveRecordVersion
 *   je.rep.vlsn.distance
 *   je.rep.vlsn.logCacheSize
 */
public class ParameterUtils {

    public final static String HELPER_HOST_SEPARATOR = ",";

    private final ParameterMap map;

    public ParameterUtils(ParameterMap map) {
        this.map = map;
    }

    /**
     * Default values for JE EnvironmentConfig, ReplicationConfig
     */
    private static final String DEFAULT_CONFIG_PROPERTIES=
        EnvironmentConfig.TXN_DURABILITY + "=" +
        "write_no_sync,write_no_sync,simple_majority;" +
        EnvironmentConfig.NODE_MAX_ENTRIES + "=128;" +
        EnvironmentConfig.CLEANER_THREADS + "=2;" +
        EnvironmentConfig.LOG_FILE_CACHE_SIZE + "=2000;" +
        EnvironmentConfig.CLEANER_READ_SIZE + "=1048576;" +
        EnvironmentConfig.ENV_RUN_EVICTOR + "=true;" +
        /* Disable JE data verifier by default */
        EnvironmentConfig.ENV_RUN_VERIFIER + "=false;" +
        EnvironmentConfig.LOG_WRITE_QUEUE_SIZE + "=2097152;" +
        EnvironmentConfig.LOG_NUM_BUFFERS + "=16;" +
        EnvironmentConfig.LOG_FILE_MAX + "=1073741824;" +
        EnvironmentConfig.CLEANER_MIN_UTILIZATION + "=40;" +
        EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION + "=5;" +
        EnvironmentConfig.LOG_FAULT_READ_SIZE + "=4096;" +
        EnvironmentConfig.LOG_ITERATOR_READ_SIZE + "=1048576;" +
        EnvironmentConfig.LOCK_N_LOCK_TABLES + "=97;" +
        EnvironmentConfig.LOCK_TIMEOUT + "=10 s;" +

        /* Replication.  Not all of these are documented publicly */
        ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT + "=10 s;" +
        ReplicationConfig.REPLICA_ACK_TIMEOUT + "=5 s;" +
        ReplicationConfig.CONSISTENCY_POLICY + "=NoConsistencyRequiredPolicy;" +
        ReplicationMutableConfig.REPLAY_MAX_OPEN_DB_HANDLES + "=100;" +

        /*
         * Use timeouts shorter than the default 30sec to speed up failover
         * in network hardware level failure situations.
         */
        ReplicationConfig.FEEDER_TIMEOUT + "=10 s;" +
        ReplicationConfig.REPLICA_TIMEOUT + "=10 s;" +

        /* Ignore old primary node ID when restarting as secondary */
        RepParams.IGNORE_SECONDARY_NODE_ID.getName() + "=true;" +

        EnvironmentParams.REP_PARAM_PREFIX +
        "preHeartbeatTimeoutMs=5000 ms;" +
        EnvironmentParams.REP_PARAM_PREFIX +
        "vlsn.distance=1000000;" +
        EnvironmentParams.REP_PARAM_PREFIX +
        "vlsn.logCacheSize=128;" +
        EnvironmentConfig.EVICTOR_CRITICAL_PERCENTAGE + "=20;";

    /**
     * Extra default RepEnv properties for Admin.
     */
    private static final String EXTRA_ADMIN_CONFIG_PROPERTIES=
        /* Persuade the Admin to listen on all interfaces, all the time. */
        ReplicationConfig.BIND_INADDR_ANY + "=true;" +

        /* Ignore old primary node ID when restarting as secondary */
        RepParams.IGNORE_SECONDARY_NODE_ID.getName() + "=true;" +

        /*
         * Override and reduce the default logCacheSize given in
         * DEFAULT_CONFIG_PROPERTIES.  See [#25435].
         */
        EnvironmentParams.REP_PARAM_PREFIX + "vlsn.logCacheSize=32;" +

        /*
         * Use a small file size to allow cleaning. Note that MAX_DISK is
         * normally only 1GB.
         */
        EnvironmentConfig.LOG_FILE_MAX + "=" + (1024 * 1024 * 50) + ";";

    private static final String DEFAULT_ARB_CONFIG_PROPERTIES =
        ReplicationConfig.ENV_UNKNOWN_STATE_TIMEOUT + "=10 s;" +
        ReplicationConfig.FEEDER_TIMEOUT + "=10 s;" +
        ReplicationConfig.REPLICA_TIMEOUT + "=10 s;" +
        RepParams.MAX_CLOCK_DELTA + "=1 min";

    /**
     * Return an EnvironmentConfig set with the relevant parameters in this
     * object.
     */
    public EnvironmentConfig getEnvConfig() {
        EnvironmentConfig ec;
        ec = new EnvironmentConfig(createProperties(true, false, 0L));
        ec.setAllowCreate(true);
        ec.setTransactional(true);
        if (map.exists(ParameterState.RN_CACHE_PERCENT)) {
            ec.setCachePercent(
                map.get(ParameterState.RN_CACHE_PERCENT).asInt());
        }
        if (map.exists(ParameterState.JE_CACHE_SIZE)) {
            ec.setCacheSize(map.get(ParameterState.JE_CACHE_SIZE).asLong());
        }

        if (map.exists(ParameterState.JE_ENABLE_ERASURE)) {
            boolean enableErasure =
                map.get(ParameterState.JE_ENABLE_ERASURE).asBoolean();
            ec.setConfigParam(EnvironmentParams.ENV_RUN_ERASER.getName(),
                Boolean.toString(enableErasure));
            if (enableErasure) {
                ec.setConfigParam(EnvironmentParams.ERASE_PERIOD.getName(),
                    map.getOrDefault(ParameterState.JE_ERASURE_PERIOD)
                        .asString());
            }
        }

        /*
         * Always set maxDisk so that je.maxDisk (from configProperties) is
         * not used by the RN. je.maxDisk is reserved for admin.
         */
        long maxDisk = 0;
        if (map.exists(ParameterState.RN_MOUNT_POINT_SIZE)) {
            maxDisk = map.get(ParameterState.RN_MOUNT_POINT_SIZE).asLong();
        }
        ec.setMaxDisk(maxDisk);

        return ec;
    }

    /**
     * Return ArbiterConfig set with the relevant parameters in this
     * object.
     */
    public ArbiterConfig getArbConfig(String arbHome) {
        return getArbConfig(arbHome, false);
    }

    private ArbiterConfig getArbConfig(String arbHome, boolean validateOnly) {
        ArbiterConfig arbcfg;
        arbcfg = new ArbiterConfig(createArbProperties());
        if (arbHome != null) {
            arbcfg.setArbiterHome(arbHome);
        }
        if (map.exists(ParameterState.JE_HELPER_HOSTS)) {
            arbcfg.setHelperHosts(
                map.get(ParameterState.JE_HELPER_HOSTS).asString());
        }
        if (map.exists(ParameterState.JE_HOST_PORT)) {

            /*
             * If in validating mode, setting the nodeHostPort param will not
             * work correctly because in that case we are running on the admin
             * host and not the target host.  If validating, treat the
             * host:port as a single-value helper hosts string and validate
             * directly.
             */
            String nhp = map.get(ParameterState.JE_HOST_PORT).asString();
            if (validateOnly) {
                RepParams.HELPER_HOSTS.validateValue(nhp);
            } else {
                arbcfg.setNodeHostPort(nhp);
            }
            arbcfg.setNodeName(map.get(ParameterState.AP_AN_ID).asString());
        }
        return arbcfg;

    }

    /**
     * Return a ReplicationConfig set with RN-specific parameters.
     */
    public ReplicationConfig getRNRepEnvConfig() {
        return getRepEnvConfig(null, false, false);
    }

    /**
     * Return a ReplicationConfig set with Admin-specific parameters.
     */
    public ReplicationConfig getAdminRepEnvConfig() {
        return getRepEnvConfig(null, false, true);
    }

    /**
     * Check for incorrect JE parameters.
     */
    public static void validateParams(ParameterMap map) {

        /* Check for incorrect JE params. */
        try {
            ParameterUtils pu = new ParameterUtils(map);
            pu.getEnvConfig();
            pu.getRepEnvConfig(null, true, false);
        } catch (Exception e) {
            throw new IllegalCommandException("Incorrect parameters: " +
                                              e.getMessage(), e);
        }
    }

    public static void validateArbParams(ParameterMap map) {
        /* Check for incorrect JE params. */
        try {
            ParameterUtils pu = new ParameterUtils(map);
            pu.getArbConfig(null, true);
        } catch (Exception e) {
            throw new IllegalCommandException("Incorrect parameters: " +
                                              e.getMessage(), e);
        }
    }

    private ReplicationConfig getRepEnvConfig(Properties securityProps,
                                              boolean validating,
                                              boolean forAdmin) {
        ReplicationConfig rc;
        Properties allProps =
            removeStandalone(createProperties(false, forAdmin, 0L));
        mergeProps(allProps, securityProps);
        rc = new ReplicationConfig(allProps);
        rc.setConfigParam("je.rep.preserveRecordVersion", "true");

        if (map.exists(ParameterState.JE_HELPER_HOSTS)) {
            rc.setHelperHosts(map.get(ParameterState.JE_HELPER_HOSTS).asString());
        }
        if (map.exists(ParameterState.JE_HOST_PORT)) {

            /*
             * If in validating mode, setting the nodeHostPort param will not
             * work correctly because in that case we are running on the admin
             * host and not the target host.  If validating, treat the
             * host:port as a single-value helper hosts string and validate
             * directly.
             */
            String nhp = map.get(ParameterState.JE_HOST_PORT).asString();
            if (validating) {
                RepParams.HELPER_HOSTS.validateValue(nhp);
            } else {
                rc.setNodeHostPort(nhp);
            }
        }
        if (map.exists(ParameterState.JE_NODE_PRIORITY)) {
            rc.setNodePriority(map.get(ParameterState.JE_NODE_PRIORITY).asInt());
        }
        return rc;
    }

    /**
     * Retain just the standalone env properties by removing any je.rep
     * properties, that is, ones with a je.rep prefix.
     */
    private static Properties removeRep(Properties props) {
        Iterator<?> it = props.keySet().iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            if (key.indexOf(EnvironmentParams.REP_PARAM_PREFIX) != -1) {
                it.remove();
            }
        }
        return props;
    }

    /**
     * Retain just the rep env properties by removing any standalone properties,
     * that is, ones without a je.rep prefix.
     */
    private static Properties removeStandalone(Properties props) {
        Iterator<?> it = props.keySet().iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            if (key.indexOf(EnvironmentParams.REP_PARAM_PREFIX) == -1) {
                it.remove();
            }
        }
        return props;
    }

    private void mergeProps(Properties baseProps,
                            Properties mergeProps) {
        if (mergeProps == null) {
            return;
        }
        for (Object propKey : mergeProps.keySet()) {
            String propSKey = (String) propKey;
            String propVal = mergeProps.getProperty(propSKey);
            baseProps.setProperty(propSKey, propVal);
        }
    }

    /**
     * Create a Properties object from the DEFAULT_CONFIG_PROPERTIES String
     * and if present, the configProperties String.  Priority is given to the
     * configProperties (last property set wins).
     */
    public Properties createProperties(boolean removeReplication,
                                       boolean forAdmin,
                                       long adminDirSize) {
        Properties props = new Properties();
        String propertyString = DEFAULT_CONFIG_PROPERTIES;
        if (forAdmin) {
            propertyString += EXTRA_ADMIN_CONFIG_PROPERTIES;
        }
        String configProperties = map.get(ParameterState.JE_MISC).asString();
        if (configProperties != null) {
            propertyString = propertyString + ";" + configProperties;
        }
        if (forAdmin && !propertyString.contains(EnvironmentConfig.MAX_DISK)) {
            /*
             * adminDirSize calculations already done in Admin hence just
             * assign
             */
             propertyString = propertyString + ";" +
                 EnvironmentConfig.MAX_DISK + "=" +
                 String.valueOf(adminDirSize);
        }
        try {
            props.load(ConfigUtils.getPropertiesStream(propertyString));
            if (removeReplication) {
                removeRep(props);
            }
        } catch (Exception e) {
            /* TODO: do something about this? */
        }
        return props;
    }


    public Properties createArbProperties() {
        Properties props = new Properties();
        String propertyString = DEFAULT_ARB_CONFIG_PROPERTIES;
        String configProperties = map.get(ParameterState.JE_MISC).asString();
        if (configProperties != null) {
            propertyString = propertyString + ";" + configProperties;
        }
        try {
            props.load(ConfigUtils.getPropertiesStream(propertyString));
        } catch (Exception e) {
            /* TODO: do something about this? */
        }
        return props;
    }

    public static CacheMode getCacheMode(ParameterMap map) {
        CacheModeParameter cmp =
            (CacheModeParameter) map.get(ParameterState.KV_CACHE_MODE);
        if (cmp != null) {
            return cmp.asCacheMode();
        }
        return null;
    }

    public static long getRequestQuiesceTime(ParameterMap map)  {
        return getDurationMillis(map, ParameterState.REQUEST_QUIESCE_TIME);
    }

    public static RequestLimitConfig getRequestLimitConfig(ParameterMap map) {
        final int maxActiveRequests =
            map.get(ParameterState.RN_MAX_ACTIVE_REQUESTS).asInt();
        final int requestThresholdPercent =
            map.get(ParameterState.RN_REQUEST_THRESHOLD_PERCENT).asInt();
        final int nodeLimitPercent =
            map.get(ParameterState.RN_NODE_LIMIT_PERCENT).asInt();

        return new RequestLimitConfig(maxActiveRequests,
                                      requestThresholdPercent,
                                      nodeLimitPercent);
    }

    public static long getThreadDumpIntervalMillis(ParameterMap map)  {
        return getDurationMillis(map, ParameterState.SP_DUMP_INTERVAL);
    }

    public static long getDurationMillis(ParameterMap map, String parameter) {
        DurationParameter dp = (DurationParameter) map.getOrDefault(parameter);
        return dp.toMillis();
    }

    public static TimeToLive getTimeToLive(ParameterMap map, String parameter) {
        final TimeToLiveParameter ttl =
                            (TimeToLiveParameter)map.getOrDefault(parameter);
        return ttl.toTimeToLive();
    }

    /* Parse the helper host string and return a list */
    public static List<String> helpersAsList(String helperHosts) {
        List<String> helpers = new ArrayList<>();
        if ((helperHosts == null) || (helperHosts.length() == 0)) {
            return helpers;
        }

        String[] split = helperHosts.split(HELPER_HOST_SEPARATOR);
        for (String element : split) {
            helpers.add(element.trim());
        }

        return helpers;
    }

    /**
     * Given a desired heap size in MB, return the size that will actually be
     * allocated by the JVM when this is passed via -Xmx. Zing will bump the
     * heap up to 1 GB if -Xmx is smaller.
     *
     * It is best to specify the returned value as -Xmx so the Zing warning
     * message is avoided. Also the actual value may be needed by the caller to
     * calculate memory actually used, etc.
     */
    public static int applyMinHeapMB(int heapMB) {
        return Math.max(heapMB, JVMSystemUtils.MIN_HEAP_MB);
    }

    /*
     * Checks the new JE parameters and determines if the parameter
     * changes are mutable. If JE parameter was set previously, and
     * has been removed from the new set, this method will return true.
     */
    public static boolean paramsRequireJEReboot(String oldParams,
                                                String newParams) {
        Properties oldP = new Properties();
        Properties newP = new Properties();
        try {
            if (oldParams != null) {
                oldP.load(ConfigUtils.getPropertiesStream(oldParams));
            }
            if (newParams != null) {
                newP.load(ConfigUtils.getPropertiesStream(newParams));
            }

        } catch (IOException e) {
            // InputStream is memory based so should not occur.
        }

        for (Entry<Object,Object> n : newP.entrySet()) {
            String nk = (String)n.getKey();
            String oldValue = (String)oldP.get(n.getKey());

            if (oldValue != null &&
                oldValue.equals(n.getValue())) {
                continue;
            }

            ConfigParam cp = EnvironmentParams.SUPPORTED_PARAMS.get(nk);
            if (cp == null) {
                /* This should not happen because the parameters are
                 * checked by validateParams, called by
                 * ChangeParamsPlan.
                 */
                return true;
            }
            if (!cp.isMutable()) {
                return true;
            }
        }

        /* If old map specified JE param and new map does not reboot
         * is required.
         */
        for (Entry<Object,Object> o : oldP.entrySet()) {
            if (newP.get(o.getKey()) == null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Sets the mutable JE parameter according to the parameter map.
     */
    public static void setJEMutableParams(
        ParameterMap oldParams,
        ParameterMap newParams,
        ReplicatedEnvironment renv,
        Logger logger)
    {
        Objects.requireNonNull(renv, "replicated environement");
        setJEMutableParams(
            oldParams,
            newParams,
            (k, v) -> setEnvironment(renv, k, v),
            logger);
    }

    private static void setEnvironment(ReplicatedEnvironment renv,
                                       String key,
                                       String val) {
        ConfigParam cp = EnvironmentParams.SUPPORTED_PARAMS.get(key);
        if (cp == null) {
            throw new IllegalStateException(String.format(
                "The calling method should already checked " +
                "that changing JE parameter %s is supported", key));
        }
        if (cp.isForReplication()) {
            renv.setRepMutableConfig(
                renv.getRepMutableConfig().setConfigParam(key, val));
        } else {
            renv.setMutableConfig(
                renv.getMutableConfig().setConfigParam(key, val));
        }
    }

    /**
     * Sets the mutable JE parameter according to the parameter map.
     */
    public static void setJEMutableParams(
        ParameterMap oldParams,
        ParameterMap newParams,
        BiConsumer<String, String> paramsSetter,
        Logger logger)
    {
        Objects.requireNonNull(newParams, "new parameter map");
        Objects.requireNonNull(oldParams, "old parameter map");
        Objects.requireNonNull(paramsSetter, "parameters setter");
        setJEMutableParams(
            oldParams.get(ParameterState.JE_MISC),
            newParams.get(ParameterState.JE_MISC),
            paramsSetter, logger);
    }

    /**
     * Sets the mutable JE parameter according to the parameters.
     */
    private static void setJEMutableParams(
        Parameter oldJEParam,
        Parameter newJEParam,
        BiConsumer<String, String> paramsSetter,
        Logger logger)
    {
        /*
         * TODO: need to check if the node is restarted if the JE_MISC
         * parameter is not present, or is empty.
         */
        if (newJEParam == null) {
            return;
        }
        final String newJEParamString = newJEParam.asString();
        if (newJEParamString == null) {
            return;
        }
        final String oldJEParamString =
            (oldJEParam == null) ? null : oldJEParam.asString();
        if (newJEParamString.equals(oldJEParamString)) {
            return;
        }
        final Properties newProperties = new Properties();
        final Properties oldProperties = new Properties();
        try {
            if (oldJEParamString != null) {
                oldProperties.load(
                    ConfigUtils.getPropertiesStream(oldJEParamString));
            }
            newProperties
                .load(ConfigUtils.getPropertiesStream(newJEParamString));
        } catch (IOException e) {
            // InputStream is memory based so will not occur.
        }
        setJEMutableParams(
            oldProperties, newProperties, paramsSetter, logger);
    }

    /**
     * Sets the mutable JE parameter according to the properties.
     */
    private static void setJEMutableParams(
        Properties oldProperties,
        Properties newProperties,
        BiConsumer<String, String> paramsSetter,
        Logger logger)
    {
        /*
         * Only need to check properties in the new parameter. If a property
         * present in the old parameter is missing in the new one, the node
         * will be restarted which the property will adopt the default value.
         * See ParameterMap#hasRestartRequiredDiff.
         */
        for (Entry<Object, Object> entry : newProperties.entrySet()) {
            final String key = (String) entry.getKey();
            final String val = (String) entry.getValue();
            ConfigParam cp = EnvironmentParams.SUPPORTED_PARAMS.get(key);
            if (cp == null) {
                if (logger != null) {
                    logger.log(Level.INFO, () -> String.format(
                        "Attempted to change an unsupported JE parameter: %s",
                        key));
                }
                continue;
            }
            if (!cp.isMutable()) {
                continue;
            }
            final Object oldVal = oldProperties.get(key);
            if ((oldVal != null) && (val.equals(oldVal))) {
                continue;
            }

            if (logger != null) {
                logger.log(Level.INFO, () -> String.format(
                    "Changing JE configuration: %s to %s", key, val));
            }

            paramsSetter.accept(key, val);
        }
    }
}
