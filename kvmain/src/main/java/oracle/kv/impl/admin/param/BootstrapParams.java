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
import static oracle.kv.impl.param.ParameterState.RN_ADMIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.RN_LOGIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.RN_MONITOR_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.RN_RH_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.SN_ADMIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.SN_LOGIN_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.SN_LOGIN_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_LOGIN_SO_READ_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_MONITOR_SO_BACKLOG;
import static oracle.kv.impl.param.ParameterState.SN_REGISTRY_SO_CONNECT_TIMEOUT;
import static oracle.kv.impl.param.ParameterState.SN_REGISTRY_SO_READ_TIMEOUT;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.MAIN;
import static oracle.kv.impl.util.registry.RegistryUtils.InterfaceType.TRUSTED_LOGIN;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.KVVersion;
import oracle.kv.impl.param.DefaultParameter;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.param.SizeParameter;
import oracle.kv.impl.util.PortRange;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryArgs;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * The Storage Node Agent bootstrap properties
 *
 * The storeName and storageNodeId members are only set when the Agent has been
 * registered.  The presence of the storeName is the indicator that the
 * agent has been registered.
 */
public class BootstrapParams {

    /**
     * A name to supply as the store name portion of a CSF name.  This name
     * will not conflict with any store name that the configure command has
     * accepted as valid.
     */
    private static final String CSF_EMPTY_STORE_NAME = "$";

    private final ParameterMap map;
    private ParameterMap mountMap;
    private ParameterMap adminMountMap;
    private ParameterMap rnLogMountMap;

    /** Creates an instance using the specified maps. */
    public BootstrapParams(ParameterMap map,
                           ParameterMap storageDirMap,
                           ParameterMap adminmountMap,
                           ParameterMap rnlogmountMap) {
        this.map = map;
        map.setName(ParameterState.BOOTSTRAP_PARAMS);
        map.setType(ParameterState.BOOTSTRAP_TYPE);
        setStorageDirMap(storageDirMap);
        setAdminDirMap(adminmountMap);
        setRNLogDirMap(rnlogmountMap);
        validatePortRanges();
    }

    /**
     * Creates an instance using the specified values, including whether to
     * host an admin, and the management class.
     */
    public BootstrapParams(String rootDir,
                           String hostname,
                           String haHostname,
                           String haPortRange,
                           String servicePortRange,
                           String storeName,
                           int rmiRegistryPort,
                           int adminWebServicePort,
                           int capacity,
                           String storageType,
                           String securityDir,
                           boolean hostingAdmin,
                           String mgmtClass) {

        this(rootDir, hostname, haHostname, haPortRange, servicePortRange,
             storeName, rmiRegistryPort, adminWebServicePort, capacity,
             storageType, securityDir, hostingAdmin, mgmtClass, null);
    }

    /**
     * Creates an instance using the specified values, including whether to
     * host an admin, the management class, and the software version. If the
     * softwareVersion value is null, uses the current version. Specifying the
     * software version is used for generating config.xml with backup kv store
     * version rather than kv store version with which recovery tool are being
     * run.
     */
    public BootstrapParams(String rootDir,
                           String hostname,
                           String haHostname,
                           String haPortRange,
                           String servicePortRange,
                           String storeName,
                           int rmiRegistryPort,
                           int adminWebServicePort,
                           int capacity,
                           String storageType,
                           String securityDir,
                           boolean hostingAdmin,
                           String mgmtClass,
                           String softwareVersion) {

        map = new ParameterMap(ParameterState.BOOTSTRAP_PARAMS,
                               ParameterState.BOOTSTRAP_TYPE);
        map.setParameter(ParameterState.BP_ROOTDIR, rootDir);
        map.setParameter(ParameterState.COMMON_HOSTNAME, hostname);
        map.setParameter(ParameterState.COMMON_HA_HOSTNAME, haHostname);
        map.setParameter(ParameterState.COMMON_PORTRANGE, haPortRange);
        map.setParameter(ParameterState.COMMON_SERVICE_PORTRANGE,
                         servicePortRange);
        map.setParameter(ParameterState.COMMON_STORENAME, storeName);
        map.setParameter(ParameterState.COMMON_REGISTRY_PORT,
                         Integer.toString(rmiRegistryPort));
        map.setParameter(ParameterState.ADMIN_WEB_SERVICE_PORT,
                         Integer.toString(adminWebServicePort));
        if (capacity >= 0) {
            map.setParameter(ParameterState.COMMON_CAPACITY,
                             Integer.toString(capacity));
        }
        map.setParameter(ParameterState.SN_STORAGE_TYPE, storageType);
        setStorageDirMap(null);
        map.setParameter(ParameterState.COMMON_SECURITY_DIR, securityDir);
        map.setParameter(ParameterState.BP_HOSTING_ADMIN,
                         Boolean.toString(hostingAdmin));
        map.setParameter(ParameterState.COMMON_MGMT_CLASS, mgmtClass);

        map.setParameter(ParameterState.COMMON_SN_ID, "0");
        map.setParameter(ParameterState.SN_SOFTWARE_VERSION,
                         (softwareVersion != null) ?
                          softwareVersion :
                          KVVersion.CURRENT_VERSION.getNumericVersionString());

        validatePortRanges();
    }

    private void validatePortRanges() {
        assert mountMap != null;
        final String haPortRange = getHAPortRange();
        PortRange.validateHA(haPortRange);
        if (isServicePortRangeConstrained()) {
            final String servicePortRange = getServicePortRange();
            PortRange.validateService(servicePortRange);
            PortRange.validateDisjoint(haPortRange, servicePortRange);
            validateSufficientPorts(getCapacity(),
                                    getSecurityDir(),
                                    isHostingAdmin(),
                                    getMgmtClass());
        }
    }

    private boolean isServicePortRangeConstrained() {
        final String portRange = getServicePortRange();
        return (portRange != null) && !PortRange.isUnconstrained(portRange);
    }

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

    /**
     * Sets the storage directory map to the specified map. If newMap is null,
     * the storage directory map is set to an empty map.
     *
     * @param newMap the new map or null
     */
    public void setStorageDirMap(ParameterMap newMap) {
        if (newMap == null) {
            mountMap = createStorageDirMap();
            return;
        }
        mountMap = newMap;
        mountMap.setName(ParameterState.BOOTSTRAP_MOUNT_POINTS);
        mountMap.setType(ParameterState.BOOTSTRAP_TYPE);
        mountMap.setValidate(false);
    }

    /**
     * Sets the admin directory map to the specified map. If newMap is null,
     * the admin directory map is set to an empty map.
     *
     * @param newMap the new map or null
     */
    public void setAdminDirMap(ParameterMap newMap) {
        if (newMap == null) {
            adminMountMap = createAdminDirMap();
            return;
        }
        adminMountMap = newMap;
        adminMountMap.setName(ParameterState.BOOTSTRAP_ADMIN_MOUNT_POINTS);
        adminMountMap.setType(ParameterState.BOOTSTRAP_TYPE);
        adminMountMap.setValidate(false);
    }

    /**
     * Sets the RN log directory map to the specified map. If newMap is null,
     * the RN log directory map is set to an empty map.
     *
     * @param newMap the new map or null
     */
    public void setRNLogDirMap(ParameterMap newMap) {
        if (newMap == null) {
            rnLogMountMap = createRNLogDirMap();
            return;
        }
        rnLogMountMap = newMap;
        rnLogMountMap.setName(ParameterState.BOOTSTRAP_RNLOG_MOUNT_POINTS);
        rnLogMountMap.setType(ParameterState.BOOTSTRAP_TYPE);
        rnLogMountMap.setValidate(false);
    }

    /**
     * Creates an storage directory map.
     * @return an storage directory map
     */
    public static ParameterMap createStorageDirMap() {
        return new ParameterMap(ParameterState.BOOTSTRAP_MOUNT_POINTS,
                                ParameterState.BOOTSTRAP_TYPE,
                                false,
                                ParameterState.PARAMETER_VERSION);
    }

    /**
     * Creates an admin directory map.
     * @return an admin directory map
     */
    public static ParameterMap createAdminDirMap() {
        return new ParameterMap(ParameterState.BOOTSTRAP_ADMIN_MOUNT_POINTS,
                                ParameterState.BOOTSTRAP_TYPE,
                                false,
                                ParameterState.PARAMETER_VERSION);
    }

    /**
     * Creates an rn log directory map.
     * @return an rn log directory map
     */
    public static ParameterMap createRNLogDirMap() {
        return new ParameterMap(ParameterState.BOOTSTRAP_RNLOG_MOUNT_POINTS,
                                ParameterState.BOOTSTRAP_TYPE,
                                false,
                                ParameterState.PARAMETER_VERSION);
    }

    public List<String> getStorageDirPaths() {
        assert mountMap != null;
        return getStorageDirPaths(mountMap);
    }

    public List<String> getAdminDirPath() {
        assert adminMountMap != null;
        return getStorageDirPaths(adminMountMap);
    }

    public List<String> getRNLogDirPaths() {
        assert rnLogMountMap != null;
        return getStorageDirPaths(rnLogMountMap);
    }

    /**
     * Sets the storage directory map to one initialized with the specified
     * directory paths and sizes. If sizes is != null, then paths
     * must also be non-null. If paths and sizes are both specified then
     * sizes must either be empty (no sizes defined) or have the same number of
     * elements as paths.
     *
     * @param paths list of storage directory strings or null
     * @param sizes list of directory sizes or null
     */
    public void setStorgeDirs(List<String> paths, List<String> sizes) {

        final ParameterMap newMap = createStorageDirMap();
        if (paths == null) {
            if (sizes != null) {
                throw new IllegalArgumentException("Storage directory sizes " +
                                                   "specified without " +
                                                   "directories");
            }
            setStorageDirMap(newMap);
            return;
        }

        if (sizes == null) {
            sizes = Collections.emptyList();
        } else if (!sizes.isEmpty() && (sizes.size() != paths.size())) {
            throw new IllegalArgumentException("The number of storage " +
                                               "directories and sizes " +
                                               "must match");
        }
        int i = 0;
        for (String path : paths) {
            String size = sizes.isEmpty() ? null : sizes.get(i);
            addStorageDir(newMap, path, size);
            i++;
        }
        setStorageDirMap(newMap);
    }

    /**
     * Sets the admin directory to one initialized with the specified
     * directory path and size. If sizes is != null, then path
     * must also be non-null. If path and size are both specified then
     * size must either be empty (no size defined) or have the same number of
     * elements as path.
     *
     * @param path admin directory string or null
     * @param size admin directory size or null
     */
    public void setAdminDir(String path, String size) {

        final ParameterMap newMap = createStorageDirMap();
        if (path == null) {
            if (size != null) {
                throw new IllegalArgumentException("Admin directory size " +
                                                   "specified without " +
                                                   "directory");
            }
            setAdminDirMap(newMap);
            return;
        }

        addStorageDir(newMap, path, size);
        setAdminDirMap(newMap);
    }

    /**
     * Sets the RN log directory map to one initialized with the specified
     * directory paths and sizes. If sizes is != null, then paths
     * must also be non-null. If paths and sizes are both specified then
     * sizes must either be empty (no sizes defined) or have the same number of
     * elements as paths.
     *
     * @param paths list of RN log directory strings or null
     * @param sizes list of RN log directory sizes or null
     */
    public void setRNLogDirs(List<String> paths, List<String> sizes) {

        final ParameterMap newMap = createStorageDirMap();
        if (paths == null) {
            if (sizes != null) {
                throw new IllegalArgumentException("RN log directory sizes " +
                                                   "specified without " +
                                                   "directories");
            }
            setRNLogDirMap(newMap);
            return;
        }

        if (sizes == null) {
            sizes = Collections.emptyList();
        } else if (!sizes.isEmpty() && (sizes.size() != paths.size())) {
            throw new IllegalArgumentException("The number of RN log " +
                                               "directories and sizes " +
                                               "must match");
        }
        int i = 0;
        for (String path : paths) {
            String size = sizes.isEmpty() ? null : sizes.get(i);
            addStorageDir(newMap, path, size);
            i++;
        }
        setRNLogDirMap(newMap);
    }

    /**
     * Adds a storage directory (path and size) to the specified map. If size
     * is null the directory size will be set to 0.
     */
    public static void addStorageDir(ParameterMap pm,
                                     String path,
                                     String size) {
        validateStorageDir(path);
        pm.put(new SizeParameter(path, size));
    }

    /**
     * Returns the storage/admin/rnlog directory paths in the
     * specified map. Important: storage/admin/rnlog directory
     * paths are the names of the name/value pairs in the map.
     * Values are the size, if specified.
     */
    public static List<String> getStorageDirPaths(ParameterMap pm) {
        final List<String> paths = new ArrayList<>(pm.size());
        for (Parameter p : pm) {
            paths.add(p.getName());
        }
        return paths;
    }

    public int getRegistryPort() {
        return map.getOrZeroInt(ParameterState.COMMON_REGISTRY_PORT);
    }

    public int getAdminWebServicePort() {
        return map.getOrZeroInt(ParameterState.ADMIN_WEB_SERVICE_PORT);
    }

    public String getRootdir() {
        return map.get(ParameterState.BP_ROOTDIR).asString();
    }

    public void setRootdir(String rootDir) {
        map.setParameter(ParameterState.BP_ROOTDIR, rootDir);
    }

    public String getStoreName() {
        return map.get(ParameterState.COMMON_STORENAME).asString();
    }

    public void setStoreName(String storeName) {
        map.setParameter(ParameterState.COMMON_STORENAME, storeName);
    }

    public String getStoreVersion() {
        return map.get(ParameterState.GP_STORE_VERSION).asString();
    }

    public void setStoreVersion(String storeVersion) {
        if (storeVersion != null) {
            map.setParameter(ParameterState.GP_STORE_VERSION, storeVersion);
        }
    }

    public int getId() {
        return map.getOrZeroInt(ParameterState.COMMON_SN_ID);
    }

    public void setId(int snId) {
        map.setParameter(ParameterState.COMMON_SN_ID, Integer.toString(snId));
    }

    public String getHostname() {
        return map.get(ParameterState.COMMON_HOSTNAME).asString();
    }

    public void setHostname(String newName) {
        map.setParameter(ParameterState.COMMON_HOSTNAME, newName);
    }

    /**
     * If HA HOSTNAME is not set, return the generic hostname.
     */
    public String getHAHostname() {
        String ret = map.get(ParameterState.COMMON_HA_HOSTNAME).asString();
        if (ret == null) {
            ret = getHostname();
        }
        return ret;
    }

    public void setHAHostname(String newName) {
        map.setParameter(ParameterState.COMMON_HA_HOSTNAME, newName);
    }

    public String getHAPortRange() {
        return map.get(ParameterState.COMMON_PORTRANGE).asString();
    }

    /** Set the HA port range and check port ranges. */
    public void setHAPortRange(String newRange) {
        PortRange.validateHA(newRange);
        if (isServicePortRangeConstrained()) {
            PortRange.validateDisjoint(newRange, getServicePortRange());
        }
        map.setParameter(ParameterState.COMMON_PORTRANGE, newRange);
    }

    /* This will return null if the parameter does not exist */
    public String getServicePortRange() {
        return map.get(ParameterState.COMMON_SERVICE_PORTRANGE).asString();
    }

    public boolean isHostingAdmin() {
        return map.getOrDefault(ParameterState.BP_HOSTING_ADMIN).asBoolean();
    }

    /**
     * Set whether the node is hosting an admin and check for sufficient ports.
     */
    public void setHostingAdmin(boolean value) {
        if (isServicePortRangeConstrained()) {
            validateSufficientPorts(getCapacity(),
                                    getSecurityDir(),
                                    value,
                                    getMgmtClass());
        }
        setHostingAdmin(map, value);
    }

    /**
     * Modify the specified parameter map to note whether the SN is hosting an
     * admin.  Does not check for sufficient ports.
     */
    public static void setHostingAdmin(ParameterMap map, boolean value) {
        map.setParameter(ParameterState.BP_HOSTING_ADMIN,
                         Boolean.toString(value));
    }

    public int getStorageNodeId() {
        return map.getOrZeroInt(ParameterState.COMMON_SN_ID);
    }

    public int getNumCPUs() {
        return map.getOrDefault(ParameterState.COMMON_NUMCPUS).asInt();
    }

    public void setNumCPUs(int ncpus) {
        map.setParameter(ParameterState.COMMON_NUMCPUS,
                         Integer.toString(ncpus));
    }

    public int getCapacity() {
        return map.getOrDefault(ParameterState.COMMON_CAPACITY).asInt();
    }

    /** Set the capacity and check for sufficient ports. */
    public void setCapacity(int capacity) {
        if (isServicePortRangeConstrained()) {
            validateSufficientPorts(capacity,
                                    getSecurityDir(),
                                    isHostingAdmin(),
                                    getMgmtClass());
        }
        map.setParameter(ParameterState.COMMON_CAPACITY,
                         Integer.toString(capacity));
    }

    public int getMemoryMB() {
        return map.getOrDefault(ParameterState.COMMON_MEMORY_MB).asInt();
    }

    public void setMemoryMB(int memoryMB) {
        map.setParameter(ParameterState.COMMON_MEMORY_MB,
                         Integer.toString(memoryMB));
    }

    public void setStorageType(String typeName) {
        map.setParameter(ParameterState.SN_STORAGE_TYPE, typeName);
    }

    public String getStorageType() {
        return map.getOrDefault(ParameterState.SN_STORAGE_TYPE).asString();
    }

    public SocketFactoryPair getStorageNodeAgentSFP(RMISocketPolicy policy) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        final String csfName =
            ClientSocketFactory.factoryName(CSF_EMPTY_STORE_NAME,
                                            "sna",
                                            MAIN.interfaceName());

        args.setSsfName(MAIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(SN_ADMIN_SO_BACKLOG).asInt()).
            setSsfPortRange(getServicePortRange()).
            setCsfName(csfName);

        return policy.getBindPair(args);
    }

    public SocketFactoryPair getSNATrustedLoginSFP(RMISocketPolicy policy) {
        SocketFactoryArgs args = new SocketFactoryArgs();

        final String csfName =
            ClientSocketFactory.factoryName(CSF_EMPTY_STORE_NAME,
                                            "sna",
                                            TRUSTED_LOGIN.interfaceName());

        args.setSsfName(TRUSTED_LOGIN.interfaceName()).
            setSsfBacklog(map.getOrDefault(SN_LOGIN_SO_BACKLOG).asInt()).
            setCsfConnectTimeout(
                getDurationParamMillis(SN_LOGIN_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout(
                getDurationParamMillis(SN_LOGIN_SO_READ_TIMEOUT)).
            setSsfPortRange(getServicePortRange()).
            setCsfName(csfName);

        return policy.getBindPair(args);
    }

    /**
     * Initialize the registry CSF at bootstrap time.
     */
    public static void initRegistryCSF(SecurityParams sp) {
        String registryCsfName = ClientSocketFactory.registryFactoryName();
        SocketFactoryArgs args =
            new SocketFactoryArgs().setCsfName(registryCsfName).
            setCsfConnectTimeout(
                getDefaultDurationParamMillis(SN_REGISTRY_SO_CONNECT_TIMEOUT)).
            setCsfReadTimeout(
                getDefaultDurationParamMillis(SN_REGISTRY_SO_READ_TIMEOUT));

        RegistryUtils.setServerRegistryCSF(
            sp.getRMISocketPolicy().getRegistryCSF(args));
    }

    public int getMgmtPorts() {
        return getMgmtPorts(
            map.get(ParameterState.COMMON_MGMT_CLASS).asString());
    }

    private static int getMgmtPorts(String mgmtClass) {
        if ((mgmtClass != null) &&
            !mgmtClass.equals(ParameterState.COMMON_MGMT_CLASS_DEFAULT)) {
            return 1;
        }
        return 0;
    }

    /**
     * If mgmt class is not set, return the default no-op class name.
     */
    public String getMgmtClass() {
        return map.getOrDefault(ParameterState.COMMON_MGMT_CLASS).asString();
    }

    /** Set the management class and check for sufficient ports. */
    public void setMgmtClass(String mgmtClass) {
        if (isServicePortRangeConstrained()) {
            validateSufficientPorts(getCapacity(),
                                    getSecurityDir(),
                                    isHostingAdmin(),
                                    mgmtClass);
        }
        map.setParameter(ParameterState.COMMON_MGMT_CLASS, mgmtClass);
    }

    /**
     * Return the snmp polling port, or zero if it is not set.
     */
    public int getMgmtPollingPort() {
        return map.getOrZeroInt(ParameterState.COMMON_MGMT_POLL_PORT);
    }

    public void setMgmtPollingPort(int port) {
        map.setParameter
            (ParameterState.COMMON_MGMT_POLL_PORT, Integer.toString(port));
    }

    /**
     * If the trap host string is not set, just return null.
     */
    public String getMgmtTrapHost() {
        return map.get(ParameterState.COMMON_MGMT_TRAP_HOST).asString();
    }

    public void setMgmtTrapHost(String hostname) {
        map.setParameter(ParameterState.COMMON_MGMT_TRAP_HOST, hostname);
    }

    /**
     * Return the snmp trap port, or zero if it is not set.
     */
    public int getMgmtTrapPort() {
        return map.getOrZeroInt(ParameterState.COMMON_MGMT_TRAP_PORT);
    }

    public void setMgmtTrapPort(int port) {
        map.setParameter
            (ParameterState.COMMON_MGMT_TRAP_PORT, Integer.toString(port));
    }

    /**
     * Return the security directory, or null if not set.
     */
    public String getSecurityDir() {
        String result = map.get(ParameterState.COMMON_SECURITY_DIR).asString();
        return (result != null && !result.isEmpty()) ? result : null;
    }

    /** Set the security directory and check for sufficient ports. */
    public void setSecurityDir(String dir) {
        if (isServicePortRangeConstrained()) {
            validateSufficientPorts(getCapacity(),
                                    dir,
                                    isHostingAdmin(),
                                    getMgmtClass());
        }
        map.setParameter(ParameterState.COMMON_SECURITY_DIR, dir);
    }

    /**
     * Return user external authentication methods, or NONE if not set.
     */
    public String getUserExternalAuth() {
        return map.getOrDefault(
            ParameterState.GP_USER_EXTERNAL_AUTH).asString();
    }

    public void setUserExternalAuth(String extAuths) {
        map.setParameter(ParameterState.GP_USER_EXTERNAL_AUTH, extAuths);
    }

    /**
     * Only the structure of the path is validated here, not whether
     * it exists as a directory.  That is done only when used.  It would be
     * nice if there were more things to do.  Possibilities:
     *  - check path length
     *  - look for invalid characters
     */
    private static void validateStorageDir(String path) {
        if (new File(path).isAbsolute()) {
            return;
        }
        throw new IllegalArgumentException
            ("Illegal storage directory: " + path +
             ", storage directories must be valid absolute pathnames");
    }

    /**
     * Gets the version parameter. If the parameter is not present null is
     * returned.
     *
     * @return a version object or null
     */
    public KVVersion getSoftwareVersion() {
        final String versionString =
                map.get(ParameterState.SN_SOFTWARE_VERSION).asString();

        return (versionString == null) ? null :
                                         KVVersion.parseVersion(versionString);
    }

    /**
     * Sets the version parameter. The previous version, if present, is
     * replaced.
     *
     * @param version
     */
    public void setSoftwareVersion(KVVersion version) {
        map.setParameter(ParameterState.SN_SOFTWARE_VERSION,
                         version.getNumericVersionString());
    }

    private static int getDefaultDurationParamMillis(String param) {
        DurationParameter dp =
            (DurationParameter) DefaultParameter.getDefaultParameter(param);

        return (int) dp.toMillis();
    }

    private int getDurationParamMillis(String param) {
        return (int)ParameterUtils.getDurationMillis(map, param);
    }

    /**
     * Make sure there are enough ports for the storage node when using the
     * specified values given that service ports are constrained.
     *
     * <p>This check assumes that services are being run in different
     * processes, and so ports cannot be shared between services, only between
     * different interfaces for the same service.
     *
     * @param capacity the capacity of the storage node
     * @param securityDir the security directory or null
     * @param hostingAdmin whether the storage node hosts an admin
     * @param mgmtClass the management class
     * @throws IllegalArgumentException if the service port range is smaller
     * than required
     */
    private void validateSufficientPorts(int capacity,
                                         String securityDir,
                                         boolean hostingAdmin,
                                         String mgmtClass) {
        assert isServicePortRangeConstrained();
        final Formatter msg = new Formatter();
        final String servicePortRange = getServicePortRange();
        final int size = PortRange.rangeSize(servicePortRange);

        final boolean isSecure = (securityDir != null);
        final Set<Integer> backlogs = new HashSet<>();
        int required = 0;

        /*
         * SNA ports.  Ports for each of the SN's admin and monitor services
         * that have unique backlog values.  For a secure store, the trusted
         * login service has its own port, because it has different security,
         * and the bootstrap (unregistered) SNA has its own port because the
         * security parameters are changed for the registered one.
         */
        msg.format("\nSecurity: %s", isSecure);
        msg.format("\nSN:");
        tallyPortBacklog("SN Admin", SN_ADMIN_SO_BACKLOG, backlogs, msg);
        tallyPortBacklog("SN Monitor", SN_MONITOR_SO_BACKLOG, backlogs, msg);
        required += backlogs.size();
        if (isSecure) {
            /* Trusted login service always uses a separate port */
            msg.format("\n  SN Trusted Login Service");
            required++;
            /*
             * Bootstrap SNA uses a separate part because it has a different
             * set of security parameters
             */
            msg.format("\n  Bootstrap SNA");
            required++;
        }

        /*
         * RN ports. Ports for each RN's request handler, admin, monitor, and,
         * if security is enabled, login service that have unique backlog
         * values.
         */
        msg.format("\nRNs (capacity=%d):", capacity);
        backlogs.clear();
        tallyPortBacklog("RN Request Handler", RN_RH_SO_BACKLOG, backlogs,
                         msg);
        tallyPortBacklog("RN Admin", RN_ADMIN_SO_BACKLOG, backlogs, msg);
        tallyPortBacklog("RN Monitor", RN_MONITOR_SO_BACKLOG, backlogs, msg);
        if (isSecure) {
            tallyPortBacklog("RN Login", RN_LOGIN_SO_BACKLOG, backlogs, msg);
        }
        required += (capacity * backlogs.size());

        /* Management ports */
        int mgtPorts = getMgmtPorts(mgmtClass);
        if (mgtPorts > 0) {
            msg.format("\nManagement: %d", mgtPorts);
        }
        required += mgtPorts;

        /*
         * Admin ports.  If hosting an admin, ports for the admin's command
         * service, client admin service, tracker listeners, and, if security
         * is enabled, login service, that have unique backlog values.  When
         * security is enabled, the listeners always get their own port because
         * the listeners are not secure.
         */
        if (hostingAdmin) {
            msg.format("\nAdmin:");
            backlogs.clear();
            /*
             * The command service and the client admin service always use the
             * same port because they share the same backlog
             */
            tallyPortBacklog("Admin Command Service",
                             ADMIN_COMMAND_SERVICE_SO_BACKLOG, backlogs, msg);
            if (isSecure) {
                tallyPortBacklog("Admin Login", ADMIN_LOGIN_SO_BACKLOG,
                                 backlogs, msg);
            }
            if (isSecure) {
                /* Listeners are separate in secure case */
                msg.format("\n  Admin Listeners");
                required++;
            } else {
                tallyPortBacklog("Admin Listeners", ADMIN_LISTENER_SO_BACKLOG,
                                 backlogs, msg);
            }
            required += backlogs.size();
        }

        if (size < required) {
            throw new IllegalArgumentException(
                "Service port range is too small. " +
                size + " ports were specified and " +
                required + " are required:" +
                msg);
        }
    }

    /** Tally an additional port. */
    private void tallyPortBacklog(String portName,
                                  String backlogPropertyName,
                                  Set<Integer> backlogs,
                                  Formatter msg) {
        final int backlog = map.getOrDefault(backlogPropertyName).asInt();
        final boolean added = backlogs.add(backlog);
        msg.format("\n  %s (SO_BACKLOG=%d%s)",
                   portName, backlog, added ? "" : " shared");
    }

    /**
     * Accessors of the cache ttl parameter for dns cache ttl.
     */
    public void setDnsCacheTTL(int value) {
        map.setParameter(ParameterState.COMMON_DNS_CACHE_TTL,
                         Integer.toString(value));
    }

    public int getDnsCacheTTL() {
        return map.getOrDefault(ParameterState.COMMON_DNS_CACHE_TTL).asInt();
    }

    public void setDnsCacheNegativeTTL(int value) {
        map.setParameter(ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL,
                         Integer.toString(value));
    }

    public int getDnsCacheNegativeTTL() {
        return map.getOrDefault(
                   ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL).asInt();
    }

}
