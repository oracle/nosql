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

import static oracle.kv.impl.param.ParameterState.ADMIN_COMMAND_SERVICE_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.ADMIN_LISTENER_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.ADMIN_LOGIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.ADMIN_WEB_SERVICE_PORT;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_ADMIN_MOUNT_POINTS;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_MOUNT_POINTS;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_RNLOG_MOUNT_POINTS;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_TYPE;
import static oracle.kv.impl.param.ParameterState.COMMON_CAPACITY;
import static oracle.kv.impl.param.ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL;
import static oracle.kv.impl.param.ParameterState.COMMON_DNS_CACHE_TTL;
import static oracle.kv.impl.param.ParameterState.COMMON_HA_HOSTNAME;
import static oracle.kv.impl.param.ParameterState.COMMON_HOSTNAME;
import static oracle.kv.impl.param.ParameterState.COMMON_MASTER_BALANCE;
import static oracle.kv.impl.param.ParameterState.COMMON_MEMORY_MB;
import static oracle.kv.impl.param.ParameterState.COMMON_MGMT_CLASS;
import static oracle.kv.impl.param.ParameterState.COMMON_MGMT_POLL_PORT;
import static oracle.kv.impl.param.ParameterState.COMMON_MGMT_TRAP_HOST;
import static oracle.kv.impl.param.ParameterState.COMMON_MGMT_TRAP_PORT;
import static oracle.kv.impl.param.ParameterState.COMMON_MISC_ASYNC_MAX_CONCURRENT_OPS;
import static oracle.kv.impl.param.ParameterState.COMMON_NUMCPUS;
import static oracle.kv.impl.param.ParameterState.COMMON_PORTRANGE;
import static oracle.kv.impl.param.ParameterState.COMMON_REGISTRY_PORT;
import static oracle.kv.impl.param.ParameterState.COMMON_SERVICE_PORTRANGE;
import static oracle.kv.impl.param.ParameterState.COMMON_SN_ID;
import static oracle.kv.impl.param.ParameterState.INETADDR_CACHE_NEGATIVE_POLICY_PROP;
import static oracle.kv.impl.param.ParameterState.INETADDR_CACHE_POLICY_PROP;
import static oracle.kv.impl.param.ParameterState.JVM_OVERHEAD_PERCENT;
import static oracle.kv.impl.param.ParameterState.SNA_TYPE;
import static oracle.kv.impl.param.ParameterState.SN_ADMIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.SN_ADMIN_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_ADMIN_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_ALLOW_ARBITERS;
import static oracle.kv.impl.param.ParameterState.SN_COMMENT;
import static oracle.kv.impl.param.ParameterState.SN_ENDPOINT_GROUP_THREADS_FLOOR;
import static oracle.kv.impl.param.ParameterState.SN_ENDPOINT_GROUP_MAX_QUIESCENT_SECONDS;
import static oracle.kv.impl.param.ParameterState.SN_ENDPOINT_GROUP_THREADS_PERCENT;
import static oracle.kv.impl.param.ParameterState.SN_LINK_EXEC_WAIT;
import static oracle.kv.impl.param.ParameterState.SN_LOGIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.SN_LOGIN_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_LOGIN_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_LOG_FILE_COMPRESSION;
import static oracle.kv.impl.param.ParameterState.SN_LOG_FILE_COUNT;
import static oracle.kv.impl.param.ParameterState.SN_LOG_FILE_LIMIT;
import static oracle.kv.impl.param.ParameterState.SN_MAX_LINK_COUNT;
import static oracle.kv.impl.param.ParameterState.SN_MONITOR_AGENT_REPO_SIZE;
import static oracle.kv.impl.param.ParameterState.SN_MONITOR_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.SN_MONITOR_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_MONITOR_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_REGISTRY_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.SN_REGISTRY_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_REGISTRY_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_REPNODE_START_WAIT;
import static oracle.kv.impl.param.ParameterState.SN_RN_HEAP_PERCENT;
import static oracle.kv.impl.param.ParameterState.SN_ROOT_DIR_PATH;
import static oracle.kv.impl.param.ParameterState.SN_ROOT_DIR_SIZE;
import static oracle.kv.impl.param.ParameterState.SN_SERVICE_STOP_WAIT;
import static oracle.kv.impl.param.ParameterState.SN_SOFTWARE_VERSION;
import static oracle.kv.impl.param.ParameterState.SN_STORAGE_TYPE;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.LOGIN;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.MAIN;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.TRUSTED_LOGIN;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.security.Security;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminService;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.param.DefaultParameter;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterState.Info;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.PortRange;
import oracle.kv.impl.util.registry.ClearServerSocketFactory;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryArgs;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.ServerSocketFactory;

import com.sleepycat.je.utilint.JVMSystemUtils;



/**
 * A class implementing StorageNodeParams contains the per-StorageNode
 * operational parameters, and information about the machine on which it runs.
 */
public class StorageNodeParams implements ParamsWithMap, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The name of the SSF associated with the registry.
     */
    private static final String REGISTRY_SSF_NAME = "registry";

    /**
     * A name to supply as the store name portion of a CSF name.  This name
     * will not conflict with any store name that the configure command has
     * accepted as valid.
     */
    private static final String CSF_EMPTY_STORE_NAME = "$";

    private final ParameterMap map;

    /**
     * This contains the explicit storage directory entries.
     * This map must have the name BOOTSTRAP_MOUNT_POINTS.
     */
    private ParameterMap mountMap;

    /**
     * This contains the explicit admin directory entries.
     * This map must have the name BOOTSTRAP_ADMIN_MOUNT_POINTS.
     */
    private ParameterMap adminMountMap;

    /**
     * This contains the explicit rnlog directory entries.
     * This map must have the name BOOTSTRAP_RNLOG_MOUNT_POINTS.
     */
    private ParameterMap rnLogMountMap;

    public static final String[] REGISTRATION_PARAMS = {
        COMMON_HA_HOSTNAME,
        COMMON_PORTRANGE,
        COMMON_SERVICE_PORTRANGE,
        COMMON_CAPACITY,
        COMMON_NUMCPUS,
        COMMON_MEMORY_MB,
        COMMON_MGMT_CLASS,
        COMMON_MGMT_POLL_PORT,
        COMMON_MGMT_TRAP_HOST,
        COMMON_MGMT_TRAP_PORT,
        SN_ROOT_DIR_PATH
    };

    /**
     * Whether the current JVM is the IBM J9 JVM.  We assume that the current
     * JVM is the same one that will be used for other processes.
     */
    private static final boolean VENDOR_IBM;
    static {
        final String vendor = System.getProperty("java.vendor");
        VENDOR_IBM = (vendor != null) && vendor.startsWith("IBM");
    }

    /**
     * The maximum JVM heap size in bytes for which compressed OOPs can be
     * used.  We use heap settings in MB, so subtract one MB from the value to
     * make sure we don't go over.
     */
    public static final long MAX_COMPRESSED_OOPS_HEAP_SIZE =
        VENDOR_IBM ?

        /*
         * The IBM J9 doc doesn't specify a limit, but the limit appears to be
         * somewhat less than 32 GB.  Use 31 GB to be on the safe side.
         */
        31L * 1024L * 1024L * 1024L :

        /*
         * Oracle JDK says 32GB max, although the actual limit in JDK 7u65 is
         * 32684 MB.  Reduce 32 GB by 100 MB to be on the safe side.
         */
        (32L * 1024L * 1024L * 1024L) - (100L * 1024L * 1024L);

    /**
     * Zing supports Java heap sizes up to 2 TB.
     */
    public static final long MAX_ZING_HEAP_SIZE =
        (2L * 1024L * 1024L * 1024L * 1024L);

    /**
     * The minimum heap size in bytes that the JVM can have for which we will
     * use non-compressed OOPs unless requested explicitly.  The idea is that
     * there is probably a 50% space overhead when using non-compressed OOPs,
     * so by default it is only worth using them after 1.5 times the largest
     * heap size supported for compressed OOPs.
     */
    static final long MIN_NONCOMPRESSED_OOPS_HEAP_SIZE =
        (long) (1.5 * MAX_COMPRESSED_OOPS_HEAP_SIZE);

    public StorageNodeParams(ParameterMap map) {
        this(map, null, null, null);
    }

    public StorageNodeParams(ParameterMap map,
                             ParameterMap storageDirMap,
                             ParameterMap adminDirMap,
                             ParameterMap rnLogDirMap) {
        this.map = map;
        map.setType(SNA_TYPE);
        map.setName(SNA_TYPE);
        setStorageDirMap(storageDirMap);
        setAdminDirMap(adminDirMap);
        setRNLogDirMap(rnLogDirMap);
    }

    public StorageNodeParams(LoadParameters lp) {
        map = lp.getMap(SNA_TYPE);
        setStorageDirMap(lp.getMap(BOOTSTRAP_MOUNT_POINTS));
        setAdminDirMap(lp.getMap(BOOTSTRAP_ADMIN_MOUNT_POINTS));
        setRNLogDirMap(lp.getMap(BOOTSTRAP_RNLOG_MOUNT_POINTS));
    }

    /**
     * Used by the Admin to hold values obtained from the GUI and passed to the
     * Planner.
     */
    public StorageNodeParams(String hostname,
                             int registryPort,
                             String comment) {
        this(null, hostname, registryPort, comment);
    }

    public StorageNodeParams(StorageNodeId storageNodeId,
                             String hostname,
                             int registryPort,
                             String comment) {
        this(new ParameterMap(SNA_TYPE,
                              SNA_TYPE),
             storageNodeId, hostname, registryPort, comment);
    }

    /**
     * Used by the planner to create a StorageNodeParams to register a new
     * StorageNode. The root dir, haHostName, and haPortRange fields are
     * uninitialized, because it is unknown before the node is registered.
     * These values are dictated by the SNA.
     */
    public StorageNodeParams(ParameterMap map,
                             StorageNodeId storageNodeId,
                             String hostname,
                             int registryPort,
                             String comment) {

        this.map = map;

        map.setType(SNA_TYPE);
        map.setName(SNA_TYPE);
        if (storageNodeId != null) {
            setStorageNodeId(storageNodeId);
        }
        setHostname(hostname);
        setRegistryPort(registryPort);
        if (comment != null) {
            setComment(comment);
        }
        addDefaults();
        map = getFilteredMap();
    }

    private void addDefaults() {
        map.addServicePolicyDefaults(Info.SNA);
    }

    public ParameterMap getFilteredMap() {
        return map.filter(EnumSet.of(Info.SNA));
    }

    @Override
    public ParameterMap getMap() {
        return map;
    }

    public ParameterMap getStorageDirMap() {
        return mountMap;
    }

    public ParameterMap getAdminDirMap() {
        return adminMountMap;
    }

    public ParameterMap getRNLogDirMap() {
        return rnLogMountMap;
    }

    public void setStorageDirMap(ParameterMap storageDirMap) {
        if (storageDirMap != null) {
            storageDirMap.setName(BOOTSTRAP_MOUNT_POINTS);
            storageDirMap.setType(BOOTSTRAP_TYPE);
            storageDirMap.setValidate(false);
        }
        mountMap = storageDirMap;
    }

    public void setAdminDirMap(ParameterMap storageDirMap) {
        if (storageDirMap != null) {
            storageDirMap.setName(BOOTSTRAP_ADMIN_MOUNT_POINTS);
            storageDirMap.setType(BOOTSTRAP_TYPE);
            storageDirMap.setValidate(false);
        }
        adminMountMap = storageDirMap;
    }

    public void setRNLogDirMap(ParameterMap storageDirMap) {
        if (storageDirMap != null) {
            storageDirMap.setName(BOOTSTRAP_RNLOG_MOUNT_POINTS);
            storageDirMap.setType(BOOTSTRAP_TYPE);
            storageDirMap.setValidate(false);
        }
        rnLogMountMap = storageDirMap;
    }

    /**
     * Gets the storage directory parameter for the specified path if present.
     * If the path is not present null is returned.
     *
     * @param storageDirPath
     * @return the storage directory parameter or null
     */
    public Parameter getStorageDirParam(String storageDirPath) {
        return (mountMap == null) ? null : mountMap.get(storageDirPath);
    }

    public List<String> getStorageDirPaths() {
        if (mountMap == null) {
            return Collections.emptyList();
        }
        return BootstrapParams.getStorageDirPaths(mountMap);
    }

    /**
     * Gets the RN log directory parameter for the specified path if present.
     * If the path is not present null is returned.
     *
     * @param rnlogDirPath
     * @return the RN log directory parameter or null
     */
    public Parameter getRNLogDirParam(String rnlogDirPath) {
        return (rnLogMountMap == null) ?
                    null : rnLogMountMap.get(rnlogDirPath);
    }

    public List<String> getRNLogDirPaths() {
        if (rnLogMountMap == null) {
            return Collections.emptyList();
        }
        return BootstrapParams.getStorageDirPaths(rnLogMountMap);
    }

    /**
     * Gets the Admin directory parameter for the specified path if present.
     * If the path is not present null is returned.
     *
     * @param adminDirPath
     * @return the admin directory parameter or null
     */
    public Parameter getAdminDirParam(String adminDirPath) {
        return (adminMountMap == null) ?
                    null : adminMountMap.get(adminDirPath);
    }

    public List<String> getAdminDirPaths() {
        if (adminMountMap == null) {
            return Collections.emptyList();
        }
        return BootstrapParams.getStorageDirPaths(adminMountMap);
    }

    /**
     * Returns a ParameterMap that can be used to add or remove storage
     * directories for a given SN.
     * @param params the parameters used to initialize the storage directory map
     * @param snp the storage node parameters
     * @param add if true, add the specified storage directory. If false,
     * remove that directory.
     * @param path the directory path to add or remove from the list
     * @param size the directory size
     */
    public static ParameterMap changeStorageDirMap(Parameters params,
                                                   StorageNodeParams snp,
                                                   boolean add,
                                                   String path,
                                                   String size)
        throws IllegalCommandException {

        final ParameterMap storageDirMap = snp.getStorageDirMap();

        if (!add) {
            if (storageDirMap == null || !storageDirMap.exists(path)) {
                throw new IllegalCommandException
                    ("Can't remove non-existent storage directory: " + path);
            }
        }

        final ParameterMap newMap = (storageDirMap != null) ?
                storageDirMap.copy(): BootstrapParams.createStorageDirMap();

        if (add) {
            BootstrapParams.addStorageDir(newMap, path, size);
        } else {
            newMap.remove(path);
        }

        /* Check if this storage directory is in use by an RN. */
        validateStorageDirMap(storageDirMap, params, snp.getStorageNodeId());

        return newMap;
    }

    public static ParameterMap changeStorageDirSize(Parameters params,
                                                    StorageNodeParams snp,
                                                    String path,
                                                    String size)
        throws IllegalCommandException {

        final ParameterMap storageDirMap = snp.getStorageDirMap();

        if (storageDirMap == null || !storageDirMap.exists(path)) {
            throw new IllegalCommandException("Storage directory " +
                                              path + " does not exist");
        }
        final ParameterMap newMap = storageDirMap.copy();
        BootstrapParams.addStorageDir(newMap, path, size);

        /* Check if this storage directory is in use by an RN. */
        validateStorageDirMap(storageDirMap, params, snp.getStorageNodeId());

        return newMap;
    }

    /*
     * TODO : Add support for change-admindir[size] and change-rnlogdir[size]
     * in further code drop. Also check for getAdminDirPaths usage.
     *
     * Currently this support is only exposed to AdminUtils test,
     * not to the PlanCommand.
     */
    /**
     * Returns a ParameterMap that can be used to add or remove RN log
     * directories for a given SN.
     * @param params the parameters used to initialize the storage directory map
     * @param snp the storage node parameters
     * @param add if true, add the specified RN log directory. If false,
     * remove that directory.
     * @param path the RN log directory path to add or remove from the list
     * @param size the RN log directory size
     */
    public static ParameterMap changeRNLogDirMap(Parameters params,
                                                 StorageNodeParams snp,
                                                 boolean add,
                                                 String path,
                                                 String size)
        throws IllegalCommandException {

        final ParameterMap rnLogDirMap = snp.getRNLogDirMap();

        if (!add) {
            if (rnLogDirMap == null || !rnLogDirMap.exists(path)) {
                throw new IllegalCommandException
                    ("Can't remove non-existent rn log directory: " + path);
            }
        }

        final ParameterMap newMap = (rnLogDirMap != null) ?
                rnLogDirMap.copy(): BootstrapParams.createRNLogDirMap();

        if (add) {
            BootstrapParams.addStorageDir(newMap, path, size);
        } else {
            newMap.remove(path);
        }

        /* Check if this RN log directory is in use by an RN. */
        validateRNLogDirMap(rnLogDirMap, params, snp.getStorageNodeId());

        return newMap;
    }

    /**
     * Throws an IllegalCommandException if a storage directory is in use.
     */
    public static void validateStorageDirMap(ParameterMap storageDirMap,
                                             Parameters p,
                                             StorageNodeId snid) {
        for (RepNodeParams rnp : p.getRepNodeParams()) {
            if (rnp.getStorageNodeId().equals(snid)) {
                final String path = rnp.getStorageDirectoryPath();
                if (path != null && !storageDirMap.exists(path)) {
                    throw new IllegalCommandException(
                              "Cannot remove an in-use storage directory. " +
                              "Storage directory " +  path +
                              " is in use by RepNode " + rnp.getRepNodeId());
                }
            }
        }
    }

    /**
     * Throws an IllegalCommandException if a RN log directory is in use.
     * TODO : Yet to implement support in case RN logging dir/size is changed
     */
    public static void validateRNLogDirMap(ParameterMap rnlogDirMap,
                                           Parameters p,
                                           StorageNodeId snid) {
        for (RepNodeParams rnp : p.getRepNodeParams()) {
            if (rnp.getStorageNodeId().equals(snid)) {
                final String path = rnp.getLogDirectoryPath();
                if (path != null && !rnlogDirMap.exists(path)) {
                    throw new IllegalCommandException(
                              "Cannot remove an in-use rn log directory. " +
                              "RN log directory " +  path +
                              " is in use by RepNode " + rnp.getRepNodeId());
                }
            }
        }
    }

    /*
     * TODO : Need to check for implementation usage of validateAdminDirMap
     */
    /**
     * The StorageNodeAgent is the owner and arbiter of these fields. Set
     * these fields in this instance using the returned values from the SNA.
     */
    public void setInstallationInfo(ParameterMap bootMap,
                                    ParameterMap storageDirMap,
                                    ParameterMap adminDirMap,
                                    ParameterMap rnLogDirMap,
                                    boolean hostingAdmin,
                                    Logger logger) {
        if (bootMap != null) {
            BootstrapParams.setHostingAdmin(bootMap, hostingAdmin);
            BootstrapParams bp = new BootstrapParams(bootMap,
                                                     storageDirMap,
                                                     adminDirMap,
                                                     rnLogDirMap);
            setRootDirPath(bp.getRootdir());
            setHAHostname(bp.getHAHostname());
            setHAPortRange(bp.getHAPortRange());
            setAdminWebPort(bp.getAdminWebServicePort());
            setSoftwareVersion(
                bp.getSoftwareVersion().getNumericVersionString());


            String servicePortRange = bp.getServicePortRange();
            if (servicePortRange != null) {
                setServicePortRange(servicePortRange);
            }
            setCapacity(bp.getCapacity());

            setNumCPUs(bp.getNumCPUs());
            setMemoryMB(bp.getMemoryMB());
            setMgmtClass(bp.getMgmtClass());
            setMgmtPollingPort(bp.getMgmtPollingPort());
            setMgmtTrapHost(bp.getMgmtTrapHost());
            setMgmtTrapPort(bp.getMgmtTrapPort());
        }
        setStorageDirMap(storageDirMap.copy());
        setAdminDirMap(adminDirMap.copy());
        setRNLogDirMap(rnLogDirMap.copy());

        if (!EmbeddedMode.isEmbedded()) {
            setInstallationDnsCacheTTLValues(bootMap, logger);
        }
    }

    /**
     * Sets the dns cache ttl values during registration.
     *
     * During registration BootstrapParams takes precedence before
     * StorageNodeParams since the StorageNodeParams is a policy value and
     * administors might override it with makebootconfig.
     */
    private void setInstallationDnsCacheTTLValues(ParameterMap bootMap,
                                                 Logger logger) {
        final boolean setCacheTTL =
            bootMap.exists(COMMON_DNS_CACHE_TTL);
        final boolean setCacheNegativeTTL =
            bootMap.exists(COMMON_DNS_CACHE_NEGATIVE_TTL);
        if (setCacheTTL) {
            map.setParameter(
                COMMON_DNS_CACHE_TTL,
                bootMap.get(COMMON_DNS_CACHE_TTL).asString());
        }
        if (setCacheNegativeTTL) {
            map.setParameter(
                COMMON_DNS_CACHE_NEGATIVE_TTL,
                bootMap.get(COMMON_DNS_CACHE_NEGATIVE_TTL).asString());
        }
        if (!(setCacheTTL || setCacheNegativeTTL) &&
            !(verifyDnsCacheTTLValues())) {
            logger.info(
                () ->
                String.format(
                    "SN restart needed. " +
                    "The DNS cache TTL values for this SN were not set by " +
                    "makebootconfig, so this SN is using the default " +
                    "values. When the SN joined the store, its " +
                    "configuration was modified to use the non-default " +
                    "values specified by the policy parameters. " +
                    "To make those policy values take effect, " +
                    "please restart this SN. " +
                    "The current values are %s=%s, %s=%s. " +
                    "The policy values are %s=%s, %s=%s. ",
                    INETADDR_CACHE_POLICY_PROP,
                    Security.getProperty(INETADDR_CACHE_POLICY_PROP),
                    INETADDR_CACHE_NEGATIVE_POLICY_PROP,
                    Security.getProperty(INETADDR_CACHE_NEGATIVE_POLICY_PROP),
                    INETADDR_CACHE_POLICY_PROP,
                    getDnsCacheTTL(),
                    INETADDR_CACHE_NEGATIVE_POLICY_PROP,
                    getDnsCacheNegativeTTL()));
        }
    }

    /**
     * Returns {@code true} if the dns cache ttl property values are consistent
     * with the parameter values.
     */
    public boolean verifyDnsCacheTTLValues() {
        return ((Integer.parseInt(
                     Security.getProperty(
                         INETADDR_CACHE_POLICY_PROP)) ==
                 getDnsCacheTTL()) &&
                (Integer.parseInt(
                     Security.getProperty(
                         INETADDR_CACHE_NEGATIVE_POLICY_PROP)) ==
                 getDnsCacheNegativeTTL()));
    }

    /**
     * Return false for a StorageNodeParams that has a blank root dir,
     * HA hostname and HA portrange. This is true when the SNA has not yet
     * been registered.
     */
    public boolean isRegistered() {
        return ((getHAHostname() != null) &&
                (getRootDirPath() != null) &&
                (getHAPortRange() != null));
    }

    public String getHostname() {
        return map.get(COMMON_HOSTNAME).asString();
    }

    public void setHostname(String hostname) {
        map.setParameter(COMMON_HOSTNAME, hostname);
    }

    public String getHAHostname() {
        return map.get(COMMON_HA_HOSTNAME).asString();
    }

    public void setHAHostname(String hostname) {
        map.setParameter(COMMON_HA_HOSTNAME, hostname);
    }

    public String getHAPortRange() {
        return map.get(COMMON_PORTRANGE).asString();
    }

    public void setHAPortRange(String range) {
        map.setParameter(COMMON_PORTRANGE, range);
    }

    public String getServicePortRange() {
        return map.getOrDefault(COMMON_SERVICE_PORTRANGE).asString();
    }

    public void setServicePortRange(String range) {
        map.setParameter(COMMON_SERVICE_PORTRANGE, range);
    }

    /**
     * Gets the root directory path.
     */
    public String getRootDirPath() {
        return map.get(SN_ROOT_DIR_PATH).asString();
    }

    /**
     * Gets the root directory path.
     */
    public void setRootDirPath(String path) {
        map.setParameter(SN_ROOT_DIR_PATH, path);
    }

    /**
     * Gets the root directory size.
     */
    public long getRootDirSize() {
        return map.getOrZeroLong(SN_ROOT_DIR_SIZE);
    }

    /**
     * Sets the root directory size.
     */
    public void setRootDirSize(long size) {
        map.setParameter(SN_ROOT_DIR_SIZE, Long.toString(size));
    }

    /**
     * Gets the storage directory storage type.
     */
    public String getStorageType() {
        return map.getOrDefault(SN_STORAGE_TYPE).asString();
    }

    /**
     * Sets the software version for kvroot.
     */
    public void setSoftwareVersion(String softwareVersion) {
        map.setParameter(SN_SOFTWARE_VERSION, softwareVersion);
    }

    /**
     * Gets the software version for kvroot.
     */
    public String getSoftwareVersion() {
        return map.getOrDefault(SN_SOFTWARE_VERSION).asString();
    }

    /**
     * Physical topo parameters from the bootstrap configuration.
     */
    public void setNumCPUs(int ncpus) {
        map.setParameter(COMMON_NUMCPUS,
                         Integer.toString(ncpus));
    }

    public int getNumCPUs() {
        return map.getOrZeroInt(COMMON_NUMCPUS);
    }

    public void setCapacity(int capacity) {
        map.setParameter(COMMON_CAPACITY,
                         Integer.toString(capacity));
    }

    public void setMemoryMB(int memoryMB) {
        map.setParameter(COMMON_MEMORY_MB,
                         Integer.toString(memoryMB));
    }

    public int getMemoryMB() {
        return map.getOrZeroInt(COMMON_MEMORY_MB);
    }

    /**
     * Returns the registry port number.
     */
    public int getRegistryPort() {
        return map.getOrZeroInt(COMMON_REGISTRY_PORT);
    }

    public void setRegistryPort(int port) {
        map.setParameter(COMMON_REGISTRY_PORT,
                         Integer.toString(port));
    }

    public int getAdminWebPort() {
        return map.getOrZeroInt(ADMIN_WEB_SERVICE_PORT);
    }

    public void setAdminWebPort(int port) {
        map.setParameter(ADMIN_WEB_SERVICE_PORT,
                         Integer.toString(port));
    }

    public StorageNodeId getStorageNodeId() {
        return new StorageNodeId
            (map.getOrZeroInt(COMMON_SN_ID));
    }

    public void setStorageNodeId(StorageNodeId snid) {
        map.setParameter(COMMON_SN_ID,
                         Integer.toString(snid.getStorageNodeId()));
    }

    /**
     * Return a free-form comment string associated with the Node.
     */
    public String getComment() {
        return map.get(SN_COMMENT).asString();
    }

    /**
     * Set a free-form comment string associated with the Node.
     */
    public void setComment(String c) {
        map.setParameter(SN_COMMENT, c);
    }

    /**
     * @return the loggingFileCount
     */
    public int getLogFileCount() {
        return map.getOrZeroInt(SN_LOG_FILE_COUNT);
    }

    /**
     * @return the loggingFileLimit
     */
    public int getLogFileLimit() {
        return map.getOrZeroInt(SN_LOG_FILE_LIMIT);
    }

    /**
     * Returns whether log compression is enabled.
     */
    public boolean getLogFileCompression() {
        return map.getOrDefault(SN_LOG_FILE_COMPRESSION).asBoolean();
    }

    public int getServiceWaitMillis() {
        return (int) ParameterUtils.getDurationMillis
            (map, SN_SERVICE_STOP_WAIT);
    }

    public int getRepNodeStartSecs() {
        return ((int) ParameterUtils.getDurationMillis
                (map, SN_REPNODE_START_WAIT))/1000;
    }

    public int getLinkExecWaitSecs() {
        return ((int) ParameterUtils.getDurationMillis
                (map, SN_LINK_EXEC_WAIT))/1000;
    }

    public int getMaxLinkCount() {
        return map.getOrZeroInt(SN_MAX_LINK_COUNT);
    }

    private int getDurationParamMillis(String param) {
        return (int)ParameterUtils.getDurationMillis(map, param);
    }

    private static int getDefaultDurationParamMillis(String param) {
        DurationParameter dp =
            (DurationParameter) DefaultParameter.getDefaultParameter(param);

        return (int) dp.toMillis();
    }

    /**
     * Create a string of the form snId(hostname:registryPort) to clearly
     * identify this SN, usually for debugging purposes.
     */
    public String displaySNIdAndHost() {
        return getStorageNodeId() + "(" + getHostname() + ":" +
            getRegistryPort() + ")";
    }

    /**
     * Returns the SFP used by the Admin for its CS
     */
    public SocketFactoryPair getAdminCommandServiceSFP(RMISocketPolicy policy,
                                                       String storeName,
                                                       ClientId clientId) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        final String csfName = ClientSocketFactory.factoryName(
            (storeName == null ? CSF_EMPTY_STORE_NAME : storeName),
            AdminId.getPrefix(), MAIN.interfaceName());

        args.setSsfName(MAIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(
                              ADMIN_COMMAND_SERVICE_SO_BACKLOG).asInt()).
            setSsfPortRange(getServicePortRange()).
            setCsfName(csfName).
            setClientId(clientId);

        return policy.getBindPair(args);
    }

    /**
     * Returns the SFP used by the Admin for its ULS
     */
    public SocketFactoryPair getAdminLoginSFP(RMISocketPolicy policy,
                                              String storeName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        final String csfName = ClientSocketFactory.factoryName(
            (storeName == null ? CSF_EMPTY_STORE_NAME : storeName),
            AdminId.getPrefix(), LOGIN.interfaceName());

        args.setSsfName(LOGIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(
                              ADMIN_LOGIN_SO_BACKLOG).asInt()).
            setSsfPortRange(getServicePortRange()).
            setCsfName(csfName);

        return policy.getBindPair(args);
    }

    /**
     * Returns the SSF used by Admin Listeners.  These listeners are not
     * servers, and so do not have access to the keystores needed to function
     * as an SSL server, so we always create these as clear.  This is
     * unfortunate in a secure environment, but the information transmitted is
     * very low in sensitivity. The information transmitted is simply a wake-up
     * call with a timestamp to notify listeners that have registered that
     * a new event is available to be retrieved.  The actual event is retrieved
     * through the secure interface.
     * See oracle.kv.impl.admin.criticalevent.EventRecorder and
     * oracle.kv.impl.monitor.TrackerListener for usage.
     */
    public ServerSocketFactory getAdminListenerSSF() {
        return ClearServerSocketFactory.create(
             map.getOrDefault(ADMIN_LISTENER_SO_BACKLOG).asInt(),
             getServicePortRange(),
             0 /* acceptMaxActiveConns */);
    }

    /**
     * Returns the SFP used by the SNA's Admin service
     */
    public SocketFactoryPair getStorageNodeAdminSFP(RMISocketPolicy policy,
                                                    String csfName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(MAIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(SN_ADMIN_SO_BACKLOG).asInt()).
            setSsfPortRange(getServicePortRange()).
            setCsfName(csfName).
            setCsfConnectTimeout(getDurationParamMillis(
                                     SN_ADMIN_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout(getDurationParamMillis(
                                  SN_ADMIN_SO_READ_TIMEOUT));

        return policy.getBindPair(args);
    }

    /**
     * Returns the SFP used by the SNA's TrustedLogin service
     */
    public SocketFactoryPair getTrustedLoginSFP(RMISocketPolicy policy,
                                                String csfName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(TRUSTED_LOGIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(SN_LOGIN_SO_BACKLOG).asInt()).
            setSsfPortRange(getServicePortRange()).
            setCsfName(csfName).
            setCsfConnectTimeout(getDurationParamMillis(
                                     SN_LOGIN_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout(getDurationParamMillis(
                                  SN_LOGIN_SO_READ_TIMEOUT));

        return policy.getBindPair(args);
    }

    public SocketFactoryPair getMonitorSFP(RMISocketPolicy policy,
                                           String csfName) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(MAIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(SN_MONITOR_SO_BACKLOG).asInt()).
            setSsfPortRange(getServicePortRange()).
            setCsfName(csfName).
            setCsfConnectTimeout(getDurationParamMillis(
                                     SN_MONITOR_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout(getDurationParamMillis(
                                  SN_MONITOR_SO_READ_TIMEOUT));

        return policy.getBindPair(args);
    }

    public int getMonitorAgentRepoSize() {
        return getMap().getOrDefault(SN_MONITOR_AGENT_REPO_SIZE).asInt();
    }

    public ServerSocketFactory getRegistrySSF(RMISocketPolicy policy) {
        final SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(REGISTRY_SSF_NAME).
            setSsfBacklog(map.getOrDefault(SN_REGISTRY_SO_BACKLOG).asInt()).
            setSsfPortRange(PortRange.UNCONSTRAINED).
            setCsfName(ClientSocketFactory.registryFactoryName()).
            setCsfConnectTimeout(0).
            setCsfReadTimeout(0);

        return policy.getRegistrySSF(args);
    }

    public static ServerSocketFactory getDefaultRegistrySSF(
        RMISocketPolicy policy) {

        SocketFactoryArgs args = new SocketFactoryArgs();

        args.setSsfName(MAIN.interfaceName()).
            setSsfBacklog(DefaultParameter.getDefaultParameter(
                              ADMIN_LISTENER_SO_BACKLOG).asInt()).
            setSsfPortRange(DefaultParameter.getDefaultParameter(
                                COMMON_SERVICE_PORTRANGE).asString()).
            setCsfName(ClientSocketFactory.registryFactoryName()).
            setCsfConnectTimeout(getDefaultDurationParamMillis(
                                     SN_MONITOR_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout(getDefaultDurationParamMillis(
                                  SN_MONITOR_SO_READ_TIMEOUT));

        return policy.getRegistrySSF(args);
    }

    /**
     * Note that this factory is only for use on the requesting client side. We
     * do not make provisions for supplying a CSF when creating a registry,
     * it's always null when the registry is created.
     */
    public void setRegistryCSF(SecurityParams sp) {

        final RMISocketPolicy rmiPolicy = sp.getRMISocketPolicy();

        final SocketFactoryArgs args = new SocketFactoryArgs().
            setCsfName(ClientSocketFactory.registryFactoryName()).
            setCsfConnectTimeout(
                getDurationParamMillis(SN_REGISTRY_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout(
                getDurationParamMillis(SN_REGISTRY_SO_READ_TIMEOUT));

        RegistryUtils.setServerRegistryCSF(rmiPolicy.getRegistryCSF(args));
    }

    public int getCapacity() {
        return map.getOrDefault(COMMON_CAPACITY).asInt();
    }

    public boolean getAllowArbiters() {
        return map.getOrDefault(SN_ALLOW_ARBITERS).asBoolean();
    }

    public void setAllowArbiters(boolean hostArbiters) {
        map.setParameter(SN_ALLOW_ARBITERS,
                         (hostArbiters ? "true" : "false"));
    }

    public boolean getMasterBalance() {
        return map.getOrDefault(COMMON_MASTER_BALANCE).asBoolean();
    }

    public void setMasterBalance(boolean masterBalance) {
        map.setParameter(COMMON_MASTER_BALANCE,
                         (masterBalance ? "true" : "false"));
    }

    /**
     * If mgmt class is not set, return the default no-op class name.
     */
    public String getMgmtClass() {
        return map.getOrDefault(COMMON_MGMT_CLASS).asString();
    }

    public void setMgmtClass(String mgmtClass) {
        map.setParameter(COMMON_MGMT_CLASS, mgmtClass);
    }

    /**
     * Return the snmp polling port, or zero if it is not set.
     */
    public int getMgmtPollingPort() {
        return map.getOrZeroInt(COMMON_MGMT_POLL_PORT);
    }

    public void setMgmtPollingPort(int port) {
        map.setParameter(COMMON_MGMT_POLL_PORT, Integer.toString(port));
    }

    /**
     * If the trap host string is not set, just return null.
     */
    public String getMgmtTrapHost() {
        return map.get(COMMON_MGMT_TRAP_HOST).asString();
    }

    public void setMgmtTrapHost(String hostname) {
        map.setParameter(COMMON_MGMT_TRAP_HOST, hostname);
    }

    /**
     * Return the snmp trap port, or zero if it is not set.
     */
    public int getMgmtTrapPort() {
        return map.getOrZeroInt(COMMON_MGMT_TRAP_PORT);
    }

    public void setMgmtTrapPort(int port) {
        map.setParameter(COMMON_MGMT_TRAP_PORT, Integer.toString(port));
    }

    /*
     * Validate parameters.  TODO: implement more validation.
     */
    public void validate() {
        String servicePortRange = map.get(COMMON_SERVICE_PORTRANGE).asString();
        if (servicePortRange != null) {
            PortRange.validateService(servicePortRange);
            String haRange = getHAPortRange();
            if (haRange != null) {
                PortRange.validateDisjoint(servicePortRange, haRange);
            }
        }
    }

    /**
     * The min value used in the parallel gc thread calculation, which is
     * min(4, <num cores in the node>);
     */
    public int getGCThreadFloor() {
        return map.getOrDefault(ParameterState.SN_GC_THREAD_FLOOR).asInt();
    }

    /**
     * The max value used in the parallel gc thread calculation, which is
     * 8 as defined by Java Performance.
     */
    public int getGCThreadThreshold() {
        return map.getOrDefault(ParameterState.SN_GC_THREAD_THRESHOLD).asInt();
    }

    /**
     * The percentage to use for cpus over the max value used in the parallel
     * gc thread calculation, which is 5/8 as defined by Java Performance.
     */
    public int getGCThreadPercent() {
        return map.getOrDefault(ParameterState.SN_GC_THREAD_PERCENT).asInt();
    }

    /**
     * The -XX:ParallelGCThreads value is set based on the guidelines in the
     * Java Performance book for Java 6 Update 23.
     *
     * "As of Java 6 Update 23, the number of parallel garbage collection
     * threads defaults to the number returned by the Java API
     * Runtime.availableProcessors() if the number returned is less than or
     * equal to 8; otherwise, it defaults to 5/8 the number returned by
     * Runtime.availableProcessors(). A general guideline for the number of
     * parallel garbage collection threads to set in the presence of multiple
     * applications on the same system is taking the total number of virtual
     * processors (the value returned by Runtime.availableProcessors()) and
     * dividing it by the number of applications running on the system,
     * assuming that load and Java heap sizes are similar among the
     * applications."
     *
     * The definition above creates a graph that ascends in a linear fashion
     * and then decreases in slope when the x axis reaches 8. We've found that
     * a gc thread value of less than 4 is insufficient, so we are adding a
     * floor to the graph. The minimum gc threads for any RN will be
     * {@literal MIN(<gcThreadFloor which defaults to 4>,<num cpus in the
     * node>)}.
     */
    public int calcGCThreads() {
        return calcGCThreads(getNumCPUs(),
                             getCapacity(),
                             getGCThreadFloor(),
                             getGCThreadThreshold(),
                             getGCThreadPercent());
    }

    public static int calcGCThreads(int numCPUs,
                                    int capacity,
                                    int gcThreadFloor,
                                    int gcThreadThreshold,
                                    int gcThreadPercent) {

        if (capacity == 0) {
            /* Return gcThreadFloor for zero capacity SNs */
            return gcThreadFloor;
        }

        /* Enforce the condition that gcThreadFloor is <= gcThreadThreshold */
        int floor = (gcThreadFloor < gcThreadThreshold) ? gcThreadFloor :
            gcThreadThreshold;
        /*
         * The absolute minimum number of gc threads we will use is the smaller
         * of the number of cpus, or the floor
         */
        int minGCThreads = Math.min(numCPUs, floor);
        int numCPUsPerRN = numCPUs/capacity;

        if (numCPUs == 0) {
            /* we are not specifying gc threads */
            return 0;
        }

        /* Return the calculated minimum */
        if (numCPUsPerRN <= minGCThreads) {
            return minGCThreads;
        }

        /*
         * Number of threads returned increases as a function of the number
         * of cpus.
         */
        if (numCPUsPerRN <= gcThreadThreshold){
            return numCPUsPerRN;
        }

        /* curve flattens out. */
        return Math.max(gcThreadThreshold,
                        ((numCPUsPerRN * gcThreadPercent)/100));
    }

    /**
     * Returns the minimum number of threads to use for the async endpoint
     * group in an SNA or RN.
     */
    public int getEndpointGroupThreadsFloor() {
        return map.getOrDefault(SN_ENDPOINT_GROUP_THREADS_FLOOR).asInt();
    }

    /**
     * Returns the percentage of the number of available hardware threads that
     * should be used as the number of threads for the async endpoint group on
     * an RN.
     */
    public int getEndpointGroupThreadsPercent() {
        return map.getOrDefault(SN_ENDPOINT_GROUP_THREADS_PERCENT).asInt();
    }

    /**
     * Calculates the number of threads that should be used for the async
     * endpoint group for an RN.
     */
    public int calcEndpointGroupNumThreads() {
        return calcEndpointGroupNumThreads(getNumCPUs(), getCapacity(),
                                           getEndpointGroupThreadsFloor(),
                                           getEndpointGroupThreadsPercent());
    }

    public static int calcEndpointGroupNumThreads(int numCPUs,
                                                  int capacity,
                                                  int threadsFloor,
                                                  int threadsPercent) {
        if (numCPUs < 1) {
            throw new IllegalArgumentException(
                "The number of CPUs must be at least 1");
        }
        if (capacity < 0) {
            throw new IllegalArgumentException(
                "The capacity must not be negative");
        }
        if (threadsFloor < 1) {
            throw new IllegalArgumentException(
                "The minimum number of threads must be at least 1");
        }
        if (threadsPercent < 0) {
            throw new IllegalArgumentException(
                "The threads percent must not be negative");
        }

        /* Return the minimum number of threads for a zero capacity SN */
        if (capacity == 0) {
            return threadsFloor;
        }

        /*
         * Otherwise, compute the percentage, and apply the capacity and the
         * minimum
         */
        final int totalThreads = (numCPUs * threadsPercent) / 100;
        return Math.max(threadsFloor, totalThreads / capacity);
    }

    /**
     * Returns the maximum amount of quiescent time in seconds before an
     * endpoint group thread is shut down.
     */
    public int getEndpointGroupMaxQuiescentSeconds() {
        return map.getOrDefault(SN_ENDPOINT_GROUP_MAX_QUIESCENT_SECONDS).
            asInt();
    }

    /**
     * Return the percentage of SN memory that will be allocated for the
     * RN processes that are hosted by this SN.
     */
    public int getRNHeapPercent() {
        return map.getOrDefault(SN_RN_HEAP_PERCENT).asInt();
    }

    /**
     * Return the percentage of requested Java heap size, for additional memory
     * used by the JVM.
     */
    public int getJVMOverheadPercent() {
        return map.getOrDefault(JVM_OVERHEAD_PERCENT).asInt();
    }

    /**
     * @param numRNsOnSN is the number of RNS hosted by a SN. If the number of
     * RNs is &gt; capacity of the hosting SN, we will use that existing RN
     * number to divide the memory.
     *
     * This can happen if we are decreasing the capacity of a SN that already
     * hosts RNs, or if are migrating the contents of one SN to another.  We do
     * not want to use capacity only, because this could result in a SN hosting
     * multiple RNs that collectively exceed the memory of the node.
     *
     * When this happens, we have overridden the rule that capacity determines
     * heap sizes. We will have to check memory/heap sizings when RNs are
     * deployed or relocated, to see if the sizings need to be changed to
     * conform more to the capacity rules.
     */
    public RNHeapAndCacheSize calculateRNHeapAndCache(ParameterMap policyMap,
                                                      int numRNsOnSN,
                                                      int rnCachePercent,
                                                      int numANsOnSN) {
        return calculateRNHeapAndCache(policyMap,
                                       getCapacity(),
                                       numRNsOnSN,
                                       getMemoryMB(),
                                       getRNHeapPercent(),
                                       rnCachePercent,
                                       numANsOnSN);
    }

    public static RNHeapAndCacheSize
        calculateRNHeapAndCache(ParameterMap policyMap,
                                int capacity,
                                int numRNsOnSN,
                                int memoryMB,
                                int rnHeapPercent,
                                int rnCachePercent,
                                int numARBsOnSN) {

        String jvmArgs =
            policyMap.getOrDefault(ParameterState.JVM_MISC).asString() +
            " " +
            policyMap.getOrDefault(ParameterState.JVM_RN_OVERRIDE).asString() +
            " " +
            policyMap.getOrDefault(
                ParameterState.JVM_RN_OVERRIDE_NO_RESTART).asString();
        long policyHeapBytes = RepNodeParams.parseJVMArgsForHeap
            (RepNodeParams.XMX_FLAG, jvmArgs);

        long policyCacheBytes =
            policyMap.getOrDefault(ParameterState.JE_CACHE_SIZE).asLong();

        if (memoryMB == 0) {
            /*
             * Memory not specified for this SN. If the policy params for heap
             * or cache are set, use those, else default to 0.
             */
            if ((policyCacheBytes != 0) ||
                (policyHeapBytes != 0)) {
                return new RNHeapAndCacheSize(policyHeapBytes,
                                              policyCacheBytes,
                                              rnCachePercent);
            }
            return new RNHeapAndCacheSize(0, 0, rnCachePercent);
        }

        /*
         * Subtract heap sizes of SN, Admin and ANs.
         * Will be converted to bytes -- must be a long.
         * TODO: Check if Admin is assigned to this SN.
         */
        boolean isHostingAdmin = true;
        long rnMem = memoryMB - getNonRNHeapMB(numARBsOnSN, isHostingAdmin);

        final long memoryForHeapBytes =
            ((rnMem << 20) * rnHeapPercent) / 100L;

        /*
         * If there are more RNs on the SN than capacity, use that to divide
         * up resources. If there are fewer RNs than capacity, use capacity
         * as the divisor.
         */
        int divisor = ((numRNsOnSN != 0) && (numRNsOnSN > capacity)) ?
            numRNsOnSN : capacity;
        /* Account for zero capacity SN */
        if (divisor == 0) {
            divisor = 1;
        }

        /*
         * The heap has to be >= 2MB, per the JVM spec and >= our mandated
         * minimum of 32MB per RN.
         */
        long perRNHeap;

        if (policyHeapBytes != 0) {
            /* If the heap is set in the policy use it unmodified. */
            perRNHeap = policyHeapBytes;
        } else {
            perRNHeap = memoryForHeapBytes / divisor;
            /*
             * Check if the heap size is too large for compressed OOPs, and
             * only use the specified size if it is large enough for
             * non-compressed ones to be useful, or if non-compressed OOPs or a
             * specific heap size is requested explicitly. [#22695]
             */
            if (!CompressedOopsSetting.NOT_COMPRESSED.
                equals(getCompressedOopsSetting(jvmArgs)) &&
                (perRNHeap > MAX_COMPRESSED_OOPS_HEAP_SIZE) &&
                (perRNHeap < MIN_NONCOMPRESSED_OOPS_HEAP_SIZE)) {
                perRNHeap = MAX_COMPRESSED_OOPS_HEAP_SIZE;
            }

            final long maxHeapMB = policyMap.
                getOrDefault(ParameterState.SN_RN_HEAP_MAX_MB).asLong();

            final long maxHeapBytes =
                (maxHeapMB != 0) ? (maxHeapMB << 20) :
                 (JVMSystemUtils.ZING_JVM ? MAX_ZING_HEAP_SIZE :
                  MAX_COMPRESSED_OOPS_HEAP_SIZE);

            /* Finally apply the RN heap limit. */
            if (perRNHeap > maxHeapBytes) {
                perRNHeap = maxHeapBytes;
            }
        }

        if (JVMSystemUtils.ZING_JVM) {
            /*
             * An odd number of MB will be rounded down to an even number by
             * Zing and a warning will print. Round down to avoid the warning.
             */
            perRNHeap = ((perRNHeap >> 20) & ~1) << 20;
        }

        if (perRNHeap < ParameterState.RN_HEAP_MB_MIN << 20) {
            return new RNHeapAndCacheSize(0, 0, rnCachePercent);
        }

        if (policyCacheBytes != 0 &&
            policyCacheBytes < ParameterState.RN_CACHE_MB_MIN << 20) {
            return new RNHeapAndCacheSize(0, 0, rnCachePercent);
        }

        return new RNHeapAndCacheSize(
            perRNHeap, policyCacheBytes, rnCachePercent);
    }

    /**
     * Returns the amount of heap memory needed for the SN, ANs and Admin.
     * When running Zing this is a significant amount because each heap is at
     * least 1GB. For a non-Zing JVM it will be between 0.25 to 0.5 GB, and for
     * simplicity we return zero (for now at least) to avoid unit test impact.
     */
    public static int getNonRNHeapMB(int nANs, boolean isHostingAdmin) {
        if (!JVMSystemUtils.ZING_JVM) {
            return 0;
        }
        return StorageNodeAgent.SN_ASSUMED_HEAP_MB +
                (nANs * ParameterState.AN_HEAP_MB_MIN) +
                (isHostingAdmin ? AdminService.ADMIN_MAX_HEAP_MB : 0);
    }

    public static long calculateANHeapSizeMB() {
        return ParameterState.AN_HEAP_MB_MIN;
    }

    /** Choices for compressed OOPs settings. */
    static enum CompressedOopsSetting {
        COMPRESSED, NOT_COMPRESSED, UNSPECIFIED
    }

    /**
     * Returns the setting for compressed OOPs specified by the JVM arguments,
     * which is controlled by the last relevant flag in the arguments, or
     * UNSPECIFIED if no flag is specified.  Note that the IBM J9
     * implementation recognizes both the standard flags and its own special
     * ones.
     */
    private static CompressedOopsSetting getCompressedOopsSetting(
        final String jvmArgs) {

        /*
         * Zing does not support compressed OOPs, so the setting is irrelevant.
         */
        if (JVMSystemUtils.ZING_JVM) {
            return CompressedOopsSetting.NOT_COMPRESSED;
        }

        CompressedOopsSetting setting = CompressedOopsSetting.UNSPECIFIED;
        for (final String flag : jvmArgs.split(" ")) {
            if ("-XX:+UseCompressedOops".equals(flag) ||
                (VENDOR_IBM && "-Xcompressedrefs".equals(flag))) {
                setting = CompressedOopsSetting.COMPRESSED;
            } else if ("-XX:-UseCompressedOops".equals(flag) ||
                       (VENDOR_IBM && "-Xnocompressedrefs".equals(flag))) {
                setting = CompressedOopsSetting.NOT_COMPRESSED;
            }
        }
        return setting;
    }

    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        if (mountMap == null) {
            return;
        }

        /* Earlier releases allowed mountMaps without the proper name */
        if (!BOOTSTRAP_MOUNT_POINTS.equals(mountMap.getName())) {
            mountMap.setName(BOOTSTRAP_MOUNT_POINTS);
            mountMap.setType(BOOTSTRAP_TYPE);
            mountMap.setValidate(false);
        }
    }

    /**
     * A little struct to emphasize that RN cache and heap are calculated
     * together.
     */
    public static class RNHeapAndCacheSize {
        final long heapMB;
        final long cacheBytes;
        final int cachePercent;

        private RNHeapAndCacheSize(
            long heapBytes,
            long cacheBytes,
            int cachePercent) {

            /*
             * If the heap size is known, make sure the JE cache size is no
             * larger than 90% of the heap size, as required by JE
             */
            if (heapBytes != 0) {
                cacheBytes = Math.min(cacheBytes, (long) (0.9 * heapBytes));
            }

            heapMB = heapBytes >> 20;
            this.cacheBytes = cacheBytes;
            this.cachePercent = cachePercent;
        }

        public long getCacheBytes() {
            return cacheBytes;
        }

        public int getCachePercent() {
            return cachePercent;
        }

        public long getHeapMB() {
            return heapMB;
        }

        /**
         * Return the heapMB value in in a form that can be appended to the
         * -Xmx or Xms prefix. If the heapMB is 0, return null;
         */
        public String getHeapValAndUnit() {
            if (heapMB == 0) {
                return null;
            }

            return heapMB + "M";
        }

        @Override
        public String toString() {
            return "heapMB=" + heapMB +
                " cacheBytes=" + cacheBytes +
                " cachePercent=" + cachePercent;
        }
    }

    /**
     * Obtain a user specified prefix for starting up managed processes.
     * This can be useful when applying configuration scripts such as numactl
     * or using profiling.
     */
    public String getProcessStartupPrefix() {
        return map.get(ParameterState.SN_PROCESS_STARTUP_PREFIX).asString();
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


    public boolean isSearchClusterSecure() {
        return map.getOrDefault(ParameterState.SN_SEARCH_CLUSTER_SECURE)
                  .asBoolean();
    }

    public void setSearchClusterSecure(boolean secure) {
        map.setParameter(ParameterState.SN_SEARCH_CLUSTER_SECURE,
                         (secure ? "true" : "false"));
    }

    public String getSearchClusterMembers() {
        return map.getOrDefault
            (ParameterState.SN_SEARCH_CLUSTER_MEMBERS).asString();
    }

    public void setSearchClusterMembers(String members) {
        map.setParameter(ParameterState.SN_SEARCH_CLUSTER_MEMBERS, members);
    }

    public int getSearchMonitorDelayMillis() {
        return ((int) ParameterUtils.getDurationMillis
                (map, ParameterState.SN_SEARCH_MONITOR_DELAY));
    }

    public void setSearchMonitorDelay(String delay) {
        map.setParameter(ParameterState.SN_SEARCH_MONITOR_DELAY, delay);
    }

    public String getSearchClusterName() {
        return map.getOrDefault
            (ParameterState.SN_SEARCH_CLUSTER_NAME).asString();
    }

    public void setSearchClusterName(String name) {
        map.setParameter(ParameterState.SN_SEARCH_CLUSTER_NAME, name);
    }

    /*
     * Getters for dns cache ttl parameters.
     */
    public int getDnsCacheTTL() {
        return map.getOrDefault(ParameterState.COMMON_DNS_CACHE_TTL).asInt();
    }

    public int getDnsCacheNegativeTTL() {
        return map.getOrDefault(
                   ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL).asInt();
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

    @Override
    public String toString() {
        return "StorageNodeParams[" + map + " " + mountMap + "]";
    }
}
