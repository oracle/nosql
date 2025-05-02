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

package oracle.kv.impl.util;

import static oracle.kv.impl.param.ParameterState.ADMIN_TYPE;
import static oracle.kv.impl.param.ParameterState.ARBNODE_TYPE;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_ADMIN_MOUNT_POINTS;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_MOUNT_POINTS;
import static
    oracle.kv.impl.param.ParameterState.BOOTSTRAP_PARAMETER_R1_VERSION;
import static
    oracle.kv.impl.param.ParameterState.BOOTSTRAP_PARAMETER_R2_VERSION;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_PARAMETER_VERSION;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_PARAMS;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_RNLOG_MOUNT_POINTS;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_TYPE;
import static oracle.kv.impl.param.ParameterState.COMMON_DNS_CACHE_TTL_VALUE;
import static oracle.kv.impl.param.ParameterState.COMMON_DNS_CACHE_NEGATIVE_TTL_VALUE;
import static oracle.kv.impl.param.ParameterState.GLOBAL_TYPE;
import static oracle.kv.impl.param.ParameterState.INETADDR_CACHE_NEGATIVE_POLICY_PROP;
import static oracle.kv.impl.param.ParameterState.INETADDR_CACHE_POLICY_PROP;
import static oracle.kv.impl.param.ParameterState.PARAMETER_VERSION;
import static oracle.kv.impl.param.ParameterState.REPNODE_TYPE;
import static oracle.kv.impl.param.ParameterState.SNA_TYPE;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.Security;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * Utilities that access parameter and configuration files
 */
public class ConfigUtils {

    /**
     * Separator, newline characters for properties that end up as Java
     * Propertie objects.
     */
    public static final Character PROPERTY_SEPARATOR = ';';
    public static final Character PROPERTY_NEWLINE = '\n';

    private final static String SEC_POLICY_STRING =
        "grant {\n permission java.security.AllPermission;\n};\n";

    public static void createSecurityPolicyFile(File dest) {
        FileOutputStream output = null;
        try {
            dest.createNewFile();
            output = new FileOutputStream(dest);
            output.write(SEC_POLICY_STRING.getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    /**
     * Save the object as a bootstrap configuration file using
     * the path (full pathname) specified.
     */
    public static void createBootstrapConfig(BootstrapParams bp,
                                             String fileName) {

        createBootstrapConfig(bp, new File(fileName), null /* logger */);
    }

    public static void createBootstrapConfig(BootstrapParams bp,
                                             String fileName,
                                             Logger logger) {

        createBootstrapConfig(bp, new File(fileName), logger);
    }

    public static void createBootstrapConfig(BootstrapParams bp,
                                             File file) {
        createBootstrapConfig(bp, file, null /* logger */);
    }

    public static void createBootstrapConfig(BootstrapParams bp,
                                             File file,
                                             Logger logger) {

        LoadParameters lp = new LoadParameters();
        lp.addMap(bp.getMap());
        lp.addMap(bp.getStorageDirMap());
        lp.addMap(bp.getAdminDirMap());
        lp.addMap(bp.getRNLogDirMap());
        lp.setVersion(BOOTSTRAP_PARAMETER_VERSION);
        lp.saveParameters(file, logger);
    }

    /**
     * Get bootstrap parameters and also check if file is read only, if not,
     * try to set file read only.
     */
    public static BootstrapParams getBootstrapParams(File configFile) {
        return getBootstrapParams(configFile, true /* check read only*/, null);
    }

    /**
     * Get bootstrap parameters and also check if file is read only, if not,
     * try to set file read only.
     */
    public static BootstrapParams getBootstrapParams(File configFile,
                                                     Logger logger) {
        return getBootstrapParams(configFile, true /* check read only*/, logger);
    }

    public static BootstrapParams getBootstrapParams(File configFile,
                                                     boolean checkReadOnly) {
        return getBootstrapParams(configFile, checkReadOnly, null);
    }

    public static BootstrapParams getBootstrapParams(File configFile,
                                                     boolean checkReadOnly,
                                                     Logger logger) {

        final LoadParameters lp =
            LoadParameters.getParameters(configFile, checkReadOnly, logger);
        ParameterMap pm = null;
        ParameterMap mm = null;
        ParameterMap amm = null;
        ParameterMap rmm = null;

        final int bootstrapVersion = lp.getVersion();
        if (bootstrapVersion == BOOTSTRAP_PARAMETER_R1_VERSION) {
            pm = lp.getMap(BOOTSTRAP_TYPE, BOOTSTRAP_TYPE);
        } else if (bootstrapVersion == BOOTSTRAP_PARAMETER_R2_VERSION) {
            pm = lp.getMap(BOOTSTRAP_PARAMS, BOOTSTRAP_TYPE);
            mm = lp.getMap(BOOTSTRAP_MOUNT_POINTS, BOOTSTRAP_TYPE);
        } else {
            pm = lp.getMap(BOOTSTRAP_PARAMS, BOOTSTRAP_TYPE);
            mm = lp.getMap(BOOTSTRAP_MOUNT_POINTS, BOOTSTRAP_TYPE);
            amm = lp.getMap(BOOTSTRAP_ADMIN_MOUNT_POINTS, BOOTSTRAP_TYPE);
            rmm = lp.getMap(BOOTSTRAP_RNLOG_MOUNT_POINTS, BOOTSTRAP_TYPE);
        }
        if (pm == null) {
            throw new IllegalStateException
                ("Could not get bootstrap params from file: " + configFile);
        }

        final BootstrapParams bp = new BootstrapParams(pm, mm, amm, rmm);

        /*
         * If there is no version in the file, then something is amiss.
         */
        if (bp.getSoftwareVersion() == null) {
            throw new IllegalStateException("Software version missing from" +
                                            " configuration file.");
        }
        return bp;
    }

    public static void createSecurityConfig(SecurityParams sp,
                                            File file) {
        createSecurityConfig(sp, file, null /* logger */);
    }

    public static void createSecurityConfig(SecurityParams sp,
                                            File file,
                                            Logger logger) {

        LoadParameters lp = new LoadParameters();
        lp.addMap(sp.getMap());
        for (ParameterMap transportMap : sp.getTransportMaps()) {
            lp.addMap(transportMap);
        }
        lp.setVersion(PARAMETER_VERSION);
        lp.saveParameters(file, logger);
    }

    public static SecurityParams getSecurityParams(File configFile) {
        return getSecurityParams(configFile, null);
    }

    public static SecurityParams getSecurityParams
        (File configFile, Logger logger) {

        final LoadParameters lp =
            LoadParameters.getParametersByType(configFile, logger);
        final SecurityParams sp = new SecurityParams(lp, configFile);

        return sp;
    }

    public static GlobalParams getGlobalParams(File configFile) {
        return getGlobalParams(configFile, null);
    }

    public static GlobalParams getGlobalParams
        (File configFile, Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        ParameterMap pm = lp.getMap(GLOBAL_TYPE);
        if (pm != null) {
            return new GlobalParams(pm);
        }
        throw new IllegalStateException
            ("Could not get GlobalParams from file: " + configFile);
    }

    public static StorageNodeParams getStorageNodeParams(File configFile) {
        return getStorageNodeParams(configFile, null);
    }

    public static StorageNodeParams getStorageNodeParams
        (File configFile, Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        ParameterMap pm = lp.getMap(SNA_TYPE);
        ParameterMap mm = lp.getMap(BOOTSTRAP_MOUNT_POINTS, BOOTSTRAP_TYPE);
        ParameterMap amm =
            lp.getMap(BOOTSTRAP_ADMIN_MOUNT_POINTS, BOOTSTRAP_TYPE);
        ParameterMap rnm =
            lp.getMap(BOOTSTRAP_RNLOG_MOUNT_POINTS, BOOTSTRAP_TYPE);
        if (pm != null) {
            return new StorageNodeParams(pm, mm, amm, rnm);
        }
        throw new IllegalStateException
            ("Could not get StorageNodeParams from file: " + configFile);
    }

    /**
     * Extract the AdminParams from the configFile.  In this case, null is
     * a reasonable return, indicating that the params do not exist.  This
     * method should only be used for compatibility.
     */
    public static AdminParams getAdminParams(File configFile) {
        return getAdminParams(configFile, null);
    }

    public static AdminParams getAdminParams(File configFile, Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        ParameterMap pm = lp.getMapByType(ADMIN_TYPE);
        if (pm != null) {
            return new AdminParams(pm);
        }
        return null;
    }

    /**
     * Extract the specified AdminParams from the configFile.  If the map is
     * not found by name and tryByType is true, get the map by type.
     */
    public static AdminParams getAdminParams(File configFile,
                                             AdminId adminId,
                                             boolean tryByType) {
        return getAdminParams(configFile, adminId, tryByType, null);
    }

    public static AdminParams getAdminParams(File configFile,
                                             AdminId adminId,
                                             boolean tryByType,
                                             Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        ParameterMap pm = lp.getMap(adminId.getFullName(), ADMIN_TYPE);
        overwriteDnsCacheTTLValues(pm);
        if (pm == null && tryByType) {
            pm = lp.getMapByType(ADMIN_TYPE);
        }
        if (pm != null) {
            return new AdminParams(pm);
        }
        return null;
    }

    private static void overwriteDnsCacheTTLValues(ParameterMap map) {
        if (map == null) {
            return;
        }
        map.setParameter(COMMON_DNS_CACHE_TTL_VALUE,
                         Security.getProperty(INETADDR_CACHE_POLICY_PROP));
        map.setParameter(COMMON_DNS_CACHE_NEGATIVE_TTL_VALUE,
                         Security.getProperty(
                             INETADDR_CACHE_NEGATIVE_POLICY_PROP));
    }


    /**
     * Get RepNodeParams, return null if they do not exist.
     */
    public static RepNodeParams getRepNodeParams(File configFile,
                                                 RepNodeId rnid,
                                                 Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        ParameterMap pm =
            lp.getMap(rnid.getFullName(), REPNODE_TYPE);
        overwriteDnsCacheTTLValues(pm);
        if (pm != null) {
            return new RepNodeParams(pm);
        }
        return null;
    }

    /**
     * Get ArbNodeParams, return null if they do not exist.
     */
    public static ArbNodeParams getArbNodeParams(File configFile,
                                                 ArbNodeId arbid,
                                                 Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        ParameterMap pm =
            lp.getMap(arbid.getFullName(), ARBNODE_TYPE);
        overwriteDnsCacheTTLValues(pm);
        if (pm != null) {
            return new ArbNodeParams(pm);
        }
        return null;
    }

    public static List<ParameterMap> getRepNodes(File configFile,
                                                 Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        return lp.getAllMaps(REPNODE_TYPE);
    }


    public static List<ParameterMap> getArbNodes(File configFile,
                                                 Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        return lp.getAllMaps(ARBNODE_TYPE);
    }


    /**
     * Remove the component from the file.  If the component can't be found by
     * name and type is non-null, remove by type.
     */
    public static ParameterMap removeComponent(File configFile,
                                               ResourceId rid,
                                               String type,
                                               Logger logger) {

        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        ParameterMap map = lp.removeMap(rid.getFullName());
        if (map == null && type != null) {
            map = lp.removeMapByType(type);
        }
        if (map != null) {
            lp.saveParameters(configFile, logger);
        }
        return map;
    }

    public static ParameterMap getAdminMap(AdminId adminId,
                                           StorageNodeParams snp,
                                           GlobalParams gp,
                                           Logger logger) {
        ParameterMap map =
            getParameterMap(snp, gp, adminId.getFullName(), logger);

        /**
         * For now, accept getting the map by type vs name.
         */
        if (map == null) {
            map = getParameterMapByType(snp, gp, ADMIN_TYPE, logger);
        }
        return map;
    }

    public static ParameterMap getRepNodeMap(StorageNodeParams snp,
                                             GlobalParams gp,
                                             RepNodeId rnid,
                                             Logger logger) {
        return getParameterMap(snp, gp, rnid.getFullName(), logger);
    }

    public static ParameterMap getArbNodeMap(StorageNodeParams snp,
                                             GlobalParams gp,
                                             ArbNodeId arbid,
                                             Logger logger) {
        return getParameterMap(snp, gp, arbid.getFullName(), logger);
    }


    public static ParameterMap getGlobalMap(StorageNodeParams snp,
                                            GlobalParams gp,
                                            Logger logger) {
        return getParameterMapByType(snp, gp, GLOBAL_TYPE, logger);
    }

    private static ParameterMap getParameterMap(StorageNodeParams snp,
                                                GlobalParams gp,
                                                String service,
                                                Logger logger) {
        File configFile = FileNames.getSNAConfigFile(snp.getRootDirPath(),
                                                     gp.getKVStoreName(),
                                                     snp.getStorageNodeId());
        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        return lp.getMap(service);
    }

    private static ParameterMap getParameterMapByType(StorageNodeParams snp,
                                                      GlobalParams gp,
                                                      String type,
                                                      Logger logger) {
        File configFile = FileNames.getSNAConfigFile(snp.getRootDirPath(),
                                                     gp.getKVStoreName(),
                                                     snp.getStorageNodeId());
        LoadParameters lp = LoadParameters.getParameters(configFile, logger);
        return lp.getMapByType(type);
    }

    /**
     * Turn parameter format of "name=value;name1=value1;..." to
     * an InputStream compatible with java.util.Properties.
     */
    public static InputStream getPropertiesStream(String properties) {
        String newProps =
            properties.replace(PROPERTY_SEPARATOR, PROPERTY_NEWLINE);
        return new ByteArrayInputStream(newProps.getBytes());
    }

    /**
     * Store properties in a file.
     * @param props a set of properties to store
     * @param comment an optional comment to add to the file
     * @param dest an abstract file naming the location where the properties
     * will be stored.  The directory containing the location must exist and
     * be writable.
     * @throws IOException if an error occurs while trying to write the file
     */
    public static void storeProperties(Properties props,
                                       String comment,
                                       File dest)
        throws IOException {

        FileOutputStream output = null;
        try {
            dest.createNewFile();
            output = new FileOutputStream(dest);
            props.store(output, comment);
        } finally {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException ignored) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }
        }
    }

    /**
     * Returns the root directory from bootstrap parameter.
     */
    public static File getSNRootDir(BootstrapParams bp) {
        final File kvRoot = new File(bp.getRootdir());
        final File kvDir =
            FileNames.getKvDir(kvRoot.toString(), bp.getStoreName());
        final StorageNodeId id = new StorageNodeId(bp.getId());
        final File snRoot = FileNames.getStorageNodeDir(kvDir, id);
        return snRoot;
    }

    /**
     * Returns the KV config file path.
     */
    public static File getSNConfigPath(BootstrapParams bp) {
        if ((bp.getRootdir() == null) || (bp.getStoreName() == null)) {
            return null;
        }
        final File kvRoot = new File(bp.getRootdir());
        final StorageNodeId id = new StorageNodeId(bp.getId());
        final File configPath =
            FileNames.getSNAConfigFile(kvRoot.toString(),
                                       bp.getStoreName(), id);
        return configPath;
    }

    /**
     * Applies the dns cache ttl parameter to the security property to SN.
     *
     * Gets the ttl values from the StorageNodeParam in the config file if we
     * can, otherwise from the BootstrapParams.
     */
    public static void applyDnsCacheTTL(BootstrapParams bp,
                                        Logger logger) {
        applyDnsCacheTTL(getDnsCacheTTLValues(bp), logger);
    }

    private static int[] getDnsCacheTTLValues(BootstrapParams bp) {
        final int[] values = new int[2];
        final File configPath = ConfigUtils.getSNConfigPath(bp);
        if ((configPath != null) && configPath.isFile()) {
            final StorageNodeParams snp =
                ConfigUtils.getStorageNodeParams(configPath);
            values[0] = snp.getDnsCacheTTL();
            values[1] = snp.getDnsCacheNegativeTTL();
        } else {
            values[0] = bp.getDnsCacheTTL();
            values[1] = bp.getDnsCacheNegativeTTL();
        }
        return values;
    }

    public static void applyDnsCacheTTL(int[] values,
                                        Logger logger) {
        Security.setProperty(INETADDR_CACHE_POLICY_PROP,
                             Integer.toString(values[0]));
        Security.setProperty(INETADDR_CACHE_NEGATIVE_POLICY_PROP,
                             Integer.toString(values[1]));
        logDnsCacheTTLValues(logger);
    }

    public static void logDnsCacheTTLValues(Logger logger) {
        if (logger != null) {
            logger.info(
                () ->
                String.format(
                    "Setting java.net.InetAddress cache ttl to: " +
                    "%s=%s, %s=%s",
                    INETADDR_CACHE_POLICY_PROP,
                    Security.getProperty(INETADDR_CACHE_POLICY_PROP),
                    INETADDR_CACHE_NEGATIVE_POLICY_PROP,
                    Security.getProperty(
                        INETADDR_CACHE_NEGATIVE_POLICY_PROP)));
        }
    }

    /**
     * Returns the dns cache ttl properties.
     */
    public static Properties getSecurityProperties(BootstrapParams bp) {
        final Properties properties = new Properties();
        final int[] values = getDnsCacheTTLValues(bp);
        properties.setProperty(INETADDR_CACHE_POLICY_PROP,
                               Integer.toString(values[0]));
        properties.setProperty(INETADDR_CACHE_NEGATIVE_POLICY_PROP,
                               Integer.toString(values[1]));
        return properties;
    }
}
