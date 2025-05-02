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

import static oracle.kv.impl.param.ParameterState.COMMON_DISABLED;
import static oracle.kv.impl.param.ParameterState.COMMON_MISC_ASYNC_MAX_CONCURRENT_OPS;
import static oracle.kv.impl.param.ParameterState.COMMON_SN_ID;
import static oracle.kv.impl.param.ParameterState.JE_HELPER_HOSTS;
import static oracle.kv.impl.param.ParameterState.JE_HOST_PORT;
import static oracle.kv.impl.param.ParameterState.RN_ACCEPT_MAX_ACTIVE_CONNS;
import static oracle.kv.impl.param.ParameterState.RN_ADMIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.RN_ADMIN_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RN_ADMIN_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RN_LOGIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.RN_LOGIN_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RN_LOGIN_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RN_MAX_TOPO_CHANGES;
import static oracle.kv.impl.param.ParameterState.RN_MONITOR_AGENT_REPO_SIZE;
import static oracle.kv.impl.param.ParameterState.RN_MONITOR_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.RN_MONITOR_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RN_MONITOR_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.RN_NRCONFIG_RETAIN_LOG_FILES;
import static oracle.kv.impl.param.ParameterState.SP_COLLECT_ENDPOINT_GROUP_STATS;
import static oracle.kv.impl.param.ParameterState.SP_COLLECT_ENV_STATS;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.ADMIN;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.LOGIN;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.MONITOR;

import java.io.Serializable;
import java.util.StringTokenizer;

import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryArgs;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;

import com.sleepycat.je.rep.utilint.HostPortPair;


/**
 * Common subclass for ArbNodeParams and RepNodeParams.
 */
public abstract class GroupNodeParams implements ParamsWithMap, Serializable {

    private static final long serialVersionUID = 1L;

    /*
     * Note that NoSQL DB is assuming that we are running on Hotspot Sun/Oracle
     * JVM, and that we are using JVM proprietary flags.
     */
    public static final String PARALLEL_GC_FLAG = "-XX:ParallelGCThreads=";
    public static final String CONCURRENT_GC_FLAG = "-XX:ConcGCThreads=";
    public static final String XMS_FLAG = "-Xms";
    public static final String XMX_FLAG = "-Xmx";

    public StorageNodeId getStorageNodeId() {
        return new StorageNodeId(getMap().get(COMMON_SN_ID).asInt());
    }

    public void setStorageNodeId(StorageNodeId snId) {
        getMap().setParameter(COMMON_SN_ID,
                              Integer.toString(snId.getStorageNodeId()));
    }

    public void setDisabled(boolean disabled) {
        getMap().setParameter(COMMON_DISABLED, Boolean.toString(disabled));
    }

    public boolean isDisabled() {
        return getMap().get(COMMON_DISABLED).asBoolean();
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

    /*
     * Return the current value of the max heap size as it is set in the jvm
     * misc params.
     */
    public long getMaxHeapMB() {
        long hb = getMaxHeapBytes();
        if (hb == 0) {
            return 0;
        }

        /* Shouldn't be less than 0, JVM spec says -Xmx must be >= 2MB */
        long heapMB = hb >> 20;
        return (heapMB < 0) ? 0 : heapMB;
    }

    /**
     * Return the value specified by the -Xmx value. Note that the
     * value returned by Runtime.maxMemory() can be unreliable and deviate
     * significantly from the -Xmx value -- up to 10% for large heap sizes and
     * its use is therefore avoided in this method.
     *
     * A value of zero is used to indicate that the max size is not available.
     */
    public long getMaxHeapBytes() {
        String jvmArgs =
                getMap().getOrDefault(ParameterState.JVM_MISC)
                        .asString() + " " +
                        getJvmOverride();
        return parseJVMArgsForHeap(XMX_FLAG, jvmArgs);
    }


    /*
     * Return the current value of the min heap size as it is set in the jvm
     * misc params, for validity checking.
     */
    public long getMinHeapMB() {
        String jvmArgs =
                getMap().getOrDefault(ParameterState.JVM_MISC)
                        .asString() + " " +
                        getJvmOverride();
        long minHeap = parseJVMArgsForHeap(XMS_FLAG, jvmArgs);
        if (minHeap == 0) {
            return 0;
        }
        return minHeap >> 20;
    }
    
    abstract String getJvmOverride();

    /**
     * Make this a separate method for unit testing. Return the value,
     * in bytes of of any -Xmx or -Xms string
     */
    public static long parseJVMArgsForHeap(String prefix, String jvmArgs) {
        String heapVal = parseJVMArgsForPrefix(prefix, jvmArgs);
        if (heapVal == null) {
            return 0;
        }

        long size = findHeapNum(heapVal, "g");
        if (size != 0) {
            return size << 30;
        }

        size = findHeapNum(heapVal, "m");
        if (size != 0) {
            return size << 20;
        }

        size = findHeapNum(heapVal, "k");
        if (size != 0) {
            return size << 10;
        }

        return Long.parseLong(heapVal);
    }

    public static String parseJVMArgsForPrefix(String prefix, String jvmArgs) {
        if (jvmArgs == null) {
            return null;
        }

        String[] args = jvmArgs.split(prefix);
        if (args.length < 2) {
            return null;
        }

        /*
         * Get the last occurrence of the prefix flag, since it's the last one
         * that has precedence.
         */
        String lastArg = args[args.length-1];
        String[] lastVal = lastArg.split(" ");
        if (lastVal[0].isEmpty()) {
            return null;
        }
        return lastVal[0].toLowerCase();
    }

    /**
     * Parse targetJavaMisc and remove any argument that starts with prefix.
     * If newArg is not null, add in prefix+newArg.
     * For example, if the string is -Xmx10M -XX:ParallelGCThreads=10 and the
     * prefix is -Xmx and the newArg is 5G, return
     *          -XX:ParallelGCThreads=10 -Xmx5G
     * If newArg is null, return
     *          -XX:ParallelGCThreads=10
     */
    public String replaceOrRemoveJVMArg(String targetJavaMiscParams,
                                        String prefix,
                                        String newArg) {
        StringTokenizer tokenizer = new StringTokenizer(targetJavaMiscParams);
        StringBuilder result = new StringBuilder();
        while (tokenizer.hasMoreTokens()) {
            String arg = tokenizer.nextToken();
            if (!arg.startsWith(prefix)) {
                result.append(arg).append(" ");
            }
        }

        if (newArg == null) {
            return result.toString();
        }
        return result.toString() + " "  + prefix + newArg;
    }

    private static long findHeapNum(String lastArg, String unit) {

        int unitIndex = lastArg.indexOf(unit);
        if (unitIndex == -1) {
            return 0;
        }

        return Long.parseLong(lastArg.substring(0, unitIndex));
    }

    public boolean getNRConfigRetainLogFiles() {
        return getMap().getOrDefault(RN_NRCONFIG_RETAIN_LOG_FILES).asBoolean();
    }

    public int getMaxTopoChanges() {
        return getMap().getOrDefault(RN_MAX_TOPO_CHANGES).asInt();
    }

    public boolean getCollectEnvStats() {
        return getMap().get(SP_COLLECT_ENV_STATS).asBoolean();
    }

    public boolean getCollectEndpointGroupStats() {
        return getMap().getOrDefault(SP_COLLECT_ENDPOINT_GROUP_STATS).
            asBoolean();
    }

    public int getHAPort() {
        return HostPortPair.getPort(getJENodeHostPort());
    }

    public String getJENodeHostPort() {
        return getMap().get(JE_HOST_PORT).asString();
    }

    public String getJEHelperHosts() {
        return getMap().get(JE_HELPER_HOSTS).asString();
    }

    public SocketFactoryPair getAdminSFP(RMISocketPolicy rmiPolicy,
                                         String servicePortRange,
                                         String csfName,
                                         String storeName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        final ParameterMap map = getMap();
        args.setSsfName(ADMIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(RN_ADMIN_SO_BACKLOG).asInt()).
            setSsfAcceptMaxActiveConns(
                map.getOrDefault(RN_ACCEPT_MAX_ACTIVE_CONNS).asInt()).
            setSsfPortRange(servicePortRange).
            setCsfName(csfName).
            setKvStoreName(storeName).
            setCsfConnectTimeout((int)ParameterUtils.getDurationMillis(
                                     map, RN_ADMIN_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout((int)ParameterUtils.getDurationMillis(
                                  map, RN_ADMIN_SO_READ_TIMEOUT));

        return rmiPolicy.getBindPair(args);
    }

    /**
     * Returns the SFP used by the group node for its ULS
     */
    public SocketFactoryPair getLoginSFP(RMISocketPolicy policy,
                                         String servicePortRange,
                                         String csfName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        final ParameterMap map = getMap();
        args.setSsfName(LOGIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(RN_LOGIN_SO_BACKLOG).asInt()).
            setSsfAcceptMaxActiveConns(
                map.getOrDefault(RN_ACCEPT_MAX_ACTIVE_CONNS).asInt()).
            setSsfPortRange(servicePortRange).
            setCsfName(csfName).
            setCsfConnectTimeout((int)ParameterUtils.getDurationMillis(
                                     map, RN_LOGIN_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout((int)ParameterUtils.getDurationMillis(
                                  map, RN_LOGIN_SO_READ_TIMEOUT));

        return policy.getBindPair(args);
    }


    public SocketFactoryPair getMonitorSFP(RMISocketPolicy rmiPolicy,
                                           String servicePortRange,
                                           String csfName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        final ParameterMap map = getMap();
        args.setSsfName(MONITOR.interfaceName()).
            setSsfBacklog(map.getOrDefault(RN_MONITOR_SO_BACKLOG).asInt()).
            setSsfAcceptMaxActiveConns(
                map.getOrDefault(RN_ACCEPT_MAX_ACTIVE_CONNS).asInt()).
            setSsfPortRange(servicePortRange).
            setCsfName(csfName).
            setCsfConnectTimeout((int)ParameterUtils.getDurationMillis(
                                     map, RN_MONITOR_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout((int)ParameterUtils.getDurationMillis(
                                  map, RN_MONITOR_SO_READ_TIMEOUT));

        return rmiPolicy.getBindPair(args);
    }

    public int getMonitorAgentRepoSize() {
        return getMap().getOrDefault(RN_MONITOR_AGENT_REPO_SIZE).asInt();
    }

    /**
     * Set the JE HA nodeHostPort and helperHost fields.
     */
    public void setJENodeHostPort(String nodeHostPort) {
        getMap().setParameter(ParameterState.JE_HOST_PORT, nodeHostPort);
    }

    public void setJEHelperHosts(String helperHost) {
        getMap().setParameter(ParameterState.JE_HELPER_HOSTS, helperHost);
    }

    public String getLoggingConfigProps() {
        return getMap().get(ParameterState.JVM_LOGGING).asString();
    }

    public String getJavaMiscParams() {
        return getMap().getOrDefault(ParameterState.JVM_MISC).asString();
    }

    public void setJavaMiscParams(String misc) {
        getMap().setParameter(ParameterState.JVM_MISC, misc);
    }

    public String getConfigProperties() {
        return getMap().get(ParameterState.JE_MISC).asString();
    }

    public int getParallelGCThreads() {
        String jvmArgs = getJavaMiscParams();
        String val = parseJVMArgsForPrefix(PARALLEL_GC_FLAG, jvmArgs);
        if (val == null) {
            return 0;
        }

        return Integer.parseInt(val);
    }

    /**
     * Set the -XX:ParallelGCThreads flag. If gcThreads is 0, clear the
     * setting from the jvm params.
     */
    public void setParallelGCThreads(int gcThreads) {
        String newVal = (gcThreads == 0) ? null : Integer.toString(gcThreads);

        setJavaMiscParams(replaceOrRemoveJVMArg(getJavaMiscParams(),
                                                PARALLEL_GC_FLAG, newVal));
    }

    public int getConcurrentGCThreads() {
        String jvmArgs = getJavaMiscParams();
        String val = parseJVMArgsForPrefix(CONCURRENT_GC_FLAG, jvmArgs);
        if (val == null) {
            return 0;
        }

        return Integer.parseInt(val);
    }

    /**
     * Set the -XX:ConcGCThreads flag. If gcThreads is 0, clear the
     * setting from the jvm params.
     */
    public void setConcurrentGCThreads(int gcThreads) {
        String newVal = (gcThreads == 0) ? null : Integer.toString(gcThreads);

        setJavaMiscParams(replaceOrRemoveJVMArg(getJavaMiscParams(),
                                                CONCURRENT_GC_FLAG, newVal));
    }

    /*
     * The following accessors are for session and token cache configuration
     */

    public void setSessionLimit(String value) {
        getMap().setParameter(ParameterState.COMMON_SESSION_LIMIT, value);
    }

    public int getSessionLimit() {
        return getMap().getOrDefault(ParameterState.COMMON_SESSION_LIMIT)
            .asInt();
    }

    public void setLoginCacheSize(String value) {
        getMap().setParameter(ParameterState.COMMON_LOGIN_CACHE_SIZE, value);
    }

    public int getLoginCacheSize() {
        return getMap().getOrDefault(ParameterState.COMMON_LOGIN_CACHE_SIZE)
            .asInt();
    }

    /**
     * Duplicates the dns cache ttl values.
     */
    public void duplicateCacheTTLValues(ParameterMap snParamsMap) {
        getMap().duplicateCacheTTLValues(snParamsMap);
    }

    @Override
    public String toString() {
        return getMap().toString();
    }
}
