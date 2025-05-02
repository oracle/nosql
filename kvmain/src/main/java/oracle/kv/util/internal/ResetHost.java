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

package oracle.kv.util.internal;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.GeneralStore;
import oracle.kv.impl.admin.TopologyStore;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.topo.RealizedTopology;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.VersionManager;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.VersionUtil;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.util.DbResetRepGroup;

/**
 * Utility to reset the host names of a store's environment with
 * the purpose of running the store from a new location.
 *
 * The general steps to using this utility:
 *  1. Copy the config files for each source SN to the host for the
 *     associated target SN.
 *  2. Copy the JE environment (JE .jdb log files) for one source node in each
 *     RN/AN or admin shard to the associated target directory. It is best to
 *     copy the environment from the shard member that was last running master.
 *  3. Run the utility for each target SN, specifying the config file, and host
 *     names for all the target SNs.
 *  4. Start the SNAs on the nodes with modified JE environments.
 *  5. Start any remaining SNAs. These nodes will be replicas and should
 *     preform a network restore from the nodes started in step 5.
 *
 * Note that when there are multiple shards there could be SNs that have
 * both masters and replicas. This will mean that all SNs are started in
 * step 4 and may result in replicas starting before the master is up.
 *
 * Running the utility (step 3) is idempotent. For example if a run produces
 * warnings because a SN hostname is missing, it can be re-run with the same
 * arguments with the addition of the missing hostname.
 */
public class ResetHost {

    private static final String HOST_PORT_SEPARATOR = ":";

    /* Command line flags */
    private static final String CONFIG_FILE_FLAG = "-config";
    private static final String BOOTCONFIG_FILE_FLAG = "-bootconfig";
    private static final String SN_FLAG = "-sn";
    private static final String DRY_RUN_FLAG = "-dryrun";
    private static final String SILENT_FLAG = "-silent";
    private static final String DEBUG_FLAG = "-debug";  /* not documented */

    private final File configFile;
    private final File bootconfigFile;
    private final Map<Integer, String> snHosts;
    private final Map<String, String> hostNameMap;
    private final boolean dryRun;
    private final boolean silent;
    private final Logger logger;
    private final LoadParameters lp;

    private int warnings = 0;

    /**
     * ResetHost
     *
     * @param configFile config file (required)
     * @param bootconfigFile bootstrap config file
     * @param snHosts map of SN to host name
     * @param hostNameMap map old host name to new host name
     * @param dryRun only check what would to set if true
     * @param silent suppress output if true
     */
    ResetHost(File configFile,
              File bootconfigFile,
              Map<Integer, String> snHosts,
              Map<String, String> hostNameMap,
              boolean dryRun,
              boolean silent,
              Logger logger) {
        this.configFile = configFile;
        this.bootconfigFile = bootconfigFile;
        this.snHosts = snHosts;
        this.hostNameMap = hostNameMap;
        this.dryRun = dryRun;
        this.silent = silent;
        this.logger = logger;

        if (configFile == null) {
            throw new IllegalArgumentException("Must specify config file");
        }
        if (snHosts.isEmpty()) {
            throw new IllegalArgumentException("At least one sn host must" +
                                               " be specified");
        }

        /**
         * Load the parameters and check the version. The config schema version
         * is odd in that a normal (non-bootstrap) version is 1 and a bootstrap
         * config verion is > 1.
         */
        lp = LoadParameters.getParameters(configFile, logger);
        if (lp.getVersion() > ParameterState.PARAMETER_VERSION) {
            throw new IllegalArgumentException("Unsupported parameter schema" +
                                               " version: " + lp.getVersion() +
                                               ", expected " +
                                             ParameterState.PARAMETER_VERSION);
        }
    }

    /**
     * Do the reset. Returns the number of warnings issued.
     */
    private int reset() {
        warnings = 0;

        /*
         * Storage node parameters. The storage node parameters determine
         * what SN the config file is for. If there is a HostPort
         * entry for the SN, the various host and port parameters are
         * set.
         */
        final StorageNodeParams snp = new StorageNodeParams(lp);

        /*
         * Check the SN software version. It must be the same minor version
         * in order to avoid accidental upgrade (or downgrade).
         */
        final String swVersion = snp.getSoftwareVersion();
        if ((swVersion == null) || swVersion.isEmpty()) {
            report("SN software version not in " + configFile);
        } else {
            final KVVersion snVersion = KVVersion.parseVersion(swVersion);
            if (VersionUtil.compareMinorVersion(KVVersion.CURRENT_VERSION,
                                                snVersion) > 0) {
                throw new IllegalArgumentException
                    ("Cannot reset " + snp.getStorageNodeId().getFullName() +
                     " because it is not at the required minor" +
                     " software version: " +
                     KVVersion.CURRENT_VERSION.getNumericVersionString() +
                     ", found: " + snVersion.getNumericVersionString());
            }
        }

        final StorageNodeId snId = snp.getStorageNodeId();
        if (snId.getStorageNodeId() == 0) {
            throw new IllegalArgumentException("Storage node ID missing" +
                                               " from config file");
        }

        final String hostname = snHosts.get(snId.getStorageNodeId());
        resetConfig(snp, hostname);

        if (bootconfigFile != null) {
            resetBootconfig(hostname);
        }

        final GlobalParams gp =
                new GlobalParams(lp.getMapByType(ParameterState.GLOBAL_TYPE));

        for (ParameterMap adminMap : lp.getAllMaps(ParameterState.ADMIN_TYPE)) {
            resetAdminDb(new AdminParams(adminMap), snp, gp);
        }

        for (ParameterMap rnMap : lp.getAllMaps(ParameterState.REPNODE_TYPE)) {
            resetRNDb(new RepNodeParams(rnMap), snp, gp);
        }
        return warnings;
    }


    /**
     * Reset the config file params.
     */
    private void resetConfig(StorageNodeParams snp, String hostname) {

        report("================");
        report("Resetting config file for " + snp.getStorageNodeId());

        boolean modified = false;

        if (resetStorageNodeParams(snp, hostname)) {
            modified = true;
        }

        /* Admins */
        for (ParameterMap adminMap : lp.getAllMaps(ParameterState.ADMIN_TYPE)) {
            if (resetAdminParams(new AdminParams(adminMap), hostname)) {
                modified = true;
            }
        }

        /* RNs */
        for (ParameterMap rnMap : lp.getAllMaps(ParameterState.REPNODE_TYPE)) {
            if (resetRNParams(new RepNodeParams(rnMap), hostname)) {
                modified = true;
            }
        }

        /* ANs */
        for (ParameterMap anMap : lp.getAllMaps(ParameterState.ARBNODE_TYPE)) {
            if (resetANParams(new ArbNodeParams(anMap), hostname)) {
                modified = true;
            }
        }

        if (!modified) {
            report("No changes to config file");
            return;
        }
        if (!dryRun) {
            report("Writing " + configFile);
            lp.saveParameters(configFile);
        }
    }

    /**
     * Resets the storage node parameters:
     *  COMMON_HA_HOSTNAME
     *  COMMON_HOSTNAME
     */
    private boolean resetStorageNodeParams(StorageNodeParams snp,
                                           String hostname) {
        final String node = snp.getStorageNodeId().getFullName();

        boolean modified = false;

        final String oldHAHostname = snp.getHAHostname();
        if (oldHAHostname != null) {
            if (hostname == null) {
                warning("Parameter " + ParameterState.COMMON_HA_HOSTNAME +
                        " for " + node +
                        " was not changed from " + oldHAHostname);
            } else {
                if (!oldHAHostname.equals(hostname)) {
                    reportAction(ParameterState.COMMON_HA_HOSTNAME +
                                 " for " + node +
                                 " from " + oldHAHostname +
                                 " to " + hostname);
                    snp.setHAHostname(hostname);
                    modified = true;
                }
            }
        }

        final String oldHostname = snp.getHostname();
        if (oldHostname != null) {
            if (hostname == null) {
                warning("Parameter " + ParameterState.COMMON_HOSTNAME +
                        " for " + node +
                        " was not changed from " + oldHostname);
            } else {
                if (!oldHostname.equals(hostname)) {
                    reportAction(ParameterState.COMMON_HOSTNAME +
                                 " for " + node +
                                 " from " + oldHostname +
                                 " to " + hostname);
                    snp.setHostname(hostname);
                    modified = true;
                }
            }
        }

        if (!modified) {
            report("No parameter changes for " + node);
            return false;
        }
        return true;
    }

    /**
     * Resets Admin parameters:
     *  JE_HOST_PORT
     *  JE_HELPER_HOSTS
     */
    private boolean resetAdminParams(AdminParams ap, String hostname) {
        final String node = ap.getAdminId().getFullName();

        boolean modified = false;

        final String oldHostPort = ap.getNodeHostPort();
        if (oldHostPort != null) {
            /*
             * If we don't have a hostname to update the parameter,
             * complain since it likely needed to be changed
             */
            if (hostname == null) {
                warning("Parameter " + ParameterState.JE_HOST_PORT +
                        " for " + node +
                        " was not changed from " + oldHostPort);
            } else {
                final String newHostPort = getNewHostPort(oldHostPort,
                                                          hostname);
                if (!oldHostPort.equals(newHostPort)) {
                    reportAction(ParameterState.JE_HOST_PORT +
                                 " for " + node +
                                 " from " + oldHostPort +
                                 " to " + newHostPort);
                    ap.setNodeHostPort(newHostPort);
                    modified = true;
                }
            }
        }

        final String oldHelperHost = ap.getHelperHosts();
        if (oldHelperHost != null) {
            final String newHelperHost = resetHelperHost(oldHelperHost);
            if (newHelperHost != null) {
                reportAction(ParameterState.JE_HELPER_HOSTS +
                             " for " + node +
                             " from " + oldHelperHost +
                             " to " + newHelperHost);
                ap.setHelperHost(newHelperHost);
                modified = true;
            }
        }

        if (!modified) {
            report("No parameter changes for " + node);
            return false;
        }
        return true;
    }

    /*
     * Replaces the host portion of the hostPort string with hostname. The
     * modified string is returned.
     */
    private String getNewHostPort(String hostPort, String hostname) {
        final String [] parts = hostPort.split(HOST_PORT_SEPARATOR);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Unexpected host port" +
                                               " string: " + hostPort);
        }
        return hostname + ":" + parts[1];
    }

    /**
     * Resets RN parameters:
     *  JE_HOST_PORT
     *  JE_HELPER_HOSTS
     */
    private boolean resetRNParams(RepNodeParams rnp, String hostname) {
        final String node = rnp.getRepNodeId().getFullName();

        boolean modified = false;

        final String oldHostPort = rnp.getJENodeHostPort();
        if (oldHostPort != null) {
            /*
             * If we don't have a hostname to update the parameter,
             * complain since it likely needed to be changed
             */
            if (hostname == null) {
                warning("Parameter " + ParameterState.JE_HOST_PORT +
                        " for " + node + " was not changed");
            } else {
                final String newHostPort = getNewHostPort(oldHostPort,
                                                          hostname);
                if (!oldHostPort.equals(newHostPort)) {
                    reportAction(ParameterState.JE_HOST_PORT +
                                 " for " + node +
                                 " from " + oldHostPort +
                                 " to " + newHostPort);
                    rnp.setJENodeHostPort(newHostPort);
                    modified = true;
                }
            }
        }

        final String oldHelperHost = rnp.getJEHelperHosts();
        if (oldHelperHost != null) {
            final String newHelperHost = resetHelperHost(oldHelperHost);
            if (newHelperHost != null) {
                reportAction(ParameterState.JE_HELPER_HOSTS +
                             " for " + node +
                             " from " + oldHelperHost +
                             " to " + newHelperHost);
                rnp.setJEHelperHosts(newHelperHost);
                modified = true;
            }
        }

        if (!modified) {
            report("No parameter changes for " + node);
            return false;
        }
        return true;
    }

    /**
     * Resets Arbiter parameters:
     *  JE_HOST_PORT
     *  JE_HELPER_HOSTS
     */
    private boolean resetANParams(ArbNodeParams anp, String hostname) {
        final String node = anp.getArbNodeId().getFullName();

        boolean modified = false;

        final String oldHostPort = anp.getJENodeHostPort();
        if (oldHostPort != null) {
            /*
             * If we don't have a hostname to update the parameter,
             * complain since it likely needed to be changed
             */
            if (hostname == null) {
                warning("Parameter " + ParameterState.JE_HOST_PORT +
                        " for " + node + " was not changed");
            } else {
                final String newHostPort = getNewHostPort(oldHostPort,
                                                          hostname);
                if (!oldHostPort.equals(newHostPort)) {
                    reportAction(ParameterState.JE_HOST_PORT +
                                 " for " + node +
                                 " from " + oldHostPort +
                                 " to " + newHostPort);
                    anp.setJENodeHostPort(newHostPort);
                    modified = true;
                }
            }
        }

        final String oldHelperHost = anp.getJEHelperHosts();
        if (oldHelperHost != null) {
            final String newHelperHost = resetHelperHost(oldHelperHost);
            if (newHelperHost != null) {
                reportAction(ParameterState.JE_HELPER_HOSTS +
                             " for " + node +
                             " from " + oldHelperHost +
                             " to " + newHelperHost);
                anp.setJEHelperHosts(newHelperHost);
                modified = true;
            }
        }

        if (!modified) {
            report("No parameter changes to " + node);
            return false;
        }
        return true;
    }

    private String resetHelperHost(String oldHelperHost) {
        final List<String> helpers =
                                ParameterUtils.helpersAsList(oldHelperHost);
        if (helpers.isEmpty()) {
            return null;
        }
        boolean modified = false;
        boolean first = true;
        final StringBuilder sb = new StringBuilder();
        for (String hostPort : helpers) {
            if (first) {
                first = false;
            } else {
                sb.append(ParameterUtils.HELPER_HOST_SEPARATOR);
            }

            final String[] split = hostPort.split(HOST_PORT_SEPARATOR);
            if (split.length != 2) {
                throw new IllegalArgumentException("Malformed helper host" +
                                                   " string " + hostPort);
            }
            final String newHost = hostNameMap.get(split[0]);
            if (newHost != null) {
                sb.append(newHost);
                modified = true;
            } else {
                /*
                 * Complain if not already reset (check if the hostname is one
                 * of the new hosts).
                 */
                if (!hostNameMap.values().contains(split[0])) {
                    warning("Could not reset helper host" + oldHelperHost +
                            " replacement hostname not found for " + split[0]);
                }
                sb.append(split[0]);
            }
            sb.append(HOST_PORT_SEPARATOR).append(split[1]);
        }
        return modified ? sb.toString() : null;
    }

    private void resetBootconfig(String hostname) {

        final BootstrapParams bp =
                ConfigUtils.getBootstrapParams(bootconfigFile, logger);

        final int snId = bp.getStorageNodeId();
        report("================");
        report("Resetting bootconfig file for sn" + snId);

        boolean modified = false;

        final String oldHAHost = bp.getHAHostname();
        if (oldHAHost != null) {
            /*
             * If we don't have a hostname to update the parameter,
             * complain since it likely needed to be changed
             */
            if (hostname == null) {
                warning("Bootconfig parameter " +
                        ParameterState.COMMON_HA_HOSTNAME +
                        " for sn" + snId +
                        " was not changed from " + oldHAHost);
            } else {
                if (!oldHAHost.equals(hostname)) {
                    reportAction(ParameterState.COMMON_HA_HOSTNAME +
                                 " for sn" + snId +
                                 " from " + oldHAHost +
                                 " to " + hostname);
                    bp.setHAHostname(hostname);
                    modified = true;
                }
            }
        }

        final String oldHost = bp.getHostname();
        if (oldHost != null) {

            if (hostname == null) {
                warning("Bootconfig parameter " +
                        ParameterState.COMMON_HOSTNAME +
                        " for sn" + snId +
                        " was not changed from " + oldHost);
            } else {
                if (!oldHost.equals(hostname)) {
                    reportAction(ParameterState.COMMON_HOSTNAME +
                                 " for sn" + snId +
                                 " from " + oldHost +
                                 " to " + hostname);
                    bp.setHostname(hostname);
                    modified = true;
                }
            }
        }

        if (!modified) {
            report("No parameter changes to bootconfig file for sn" + snId);
            return;
        }

        if (!dryRun) {
            report("Writing " + bootconfigFile);
            ConfigUtils.createBootstrapConfig(bp, bootconfigFile, logger);
        }
    }

    /**
     * Reset the Admin database.
     */
    private void resetAdminDb(AdminParams ap,
                              StorageNodeParams snp,
                              GlobalParams gp) {
        /*
         * Find the admin JE environment directory. Code copied from the Admin
         * constructor.
         */
        final ParameterMap adminMountMap = snp.getAdminDirMap();
        String adminDirName = null;
        if (adminMountMap != null) {
            for (Parameter adminDir : adminMountMap) {
                adminDirName = adminDir.getName();
            }
        }

        final AdminId adminId = ap.getAdminId();
        final File envDir = (adminDirName != null) ?
                                FileNames.getAdminEnvDir(adminDirName,
                                                         adminId) :
                                FileNames.getEnvDir(snp.getRootDirPath(),
                                                    gp.getKVStoreName(),
                                                    null,
                                                    snp.getStorageNodeId(),
                                                    adminId);
        if (!envDir.exists()) {
            throw new IllegalArgumentException("JE environment directory " +
                                               envDir + " does not exist");
        }

        report("================");
        if (envDir.list().length == 0) {
            report("No JE environment for " + adminId +
                   " in: " + envDir);
            return;
        }

        report("Resetting database for " + adminId +
               ", environment directory: " + envDir);

        try (Environment env = openEnvironment(envDir,
                               Admin.getAdminRepGroupName(gp.getKVStoreName()),
                               Admin.getAdminRepNodeName(adminId),
                                               ap.getNodeHostPort())) {
            resetAdminDb(env);
        }
    }

    private void resetAdminDb(Environment env) {

        try (TopologyStore topoStore = new TopologyStore(logger,
                                                         env,
                                                         Integer.MAX_VALUE,
                                                         dryRun)) {
            final RealizedTopology rt =
                topoStore.getCurrentRealizedTopology(null);
            if (rt == null) {
                /*
                 * The topo could be missing if the store was not yet
                 * initialized.
                 */
                warning("No topology found in Admin DB");
            } else {
                if (resetTopology(rt.getTopology())) {
                    if (!dryRun) {
                        report("Writing topology: " +
                               TopologyPrinter.printTopology(rt.getTopology()));
                        topoStore.putTopology(null, rt);
                    }
                } else {
                    report("No changes to topology");
                }
            }
        }

        try (GeneralStore generalStore = new GeneralStore(logger,
                                                          env,
                                                          dryRun)) {
            Parameters params = generalStore.getParameters(null);
            if (params == null) {
                /* Unlikely/impossible? */
                warning("No parameters found in Admin DB");
            } else {
                boolean modified = false;

                /* SN parameters */
                for (StorageNodeParams snp : params.getStorageNodeParams()) {
                    final int snId = snp.getStorageNodeId().getStorageNodeId();
                    if (resetStorageNodeParams(snp, snHosts.get(snId))) {
                        modified = true;
                    }
                }

                /* Admin params */
                for (AdminParams ap : params.getAdminParams()) {
                    final int snId = ap.getStorageNodeId().getStorageNodeId();
                    if (resetAdminParams(ap, snHosts.get(snId))) {
                        modified = true;
                    }
                }

                /* RN params */
                for (RepNodeParams rnp : params.getRepNodeParams()) {
                    final int snId = rnp.getStorageNodeId().getStorageNodeId();
                    if (resetRNParams(rnp, snHosts.get(snId))) {
                        modified = true;
                    }
                }

                /* Arbiter Params */
                for (ArbNodeParams anp : params.getArbNodeParams()) {
                    final int snId = anp.getStorageNodeId().getStorageNodeId();
                    if (resetANParams(anp, snHosts.get(snId))) {
                        modified = true;
                    }
                }

                if (modified) {
                    if (!dryRun) {
                        report("Writing parameters");
                        generalStore.putParameters(null, params);
                    }
                } else {
                    report("No changes to parameters");
                }
            }
        }
    }

    /**
     * Resets the RN database. The only item reset is the topology.
     */
    private void resetRNDb(RepNodeParams rnp,
                           StorageNodeParams snp,
                           GlobalParams gp) {

        /*
         * Find the RN JE environment directory. Code copied from
         * RepEnvHandleManager.java
         */
        final RepNodeId rnId = rnp.getRepNodeId();
        final File envDir = FileNames.getEnvDir(snp.getRootDirPath(),
                                                gp.getKVStoreName(),
                                                rnp.getStorageDirectoryFile(),
                                                snp.getStorageNodeId(),
                                                rnId);
        if (!envDir.exists()) {
            throw new IllegalArgumentException("JE environment directory " +
                                               envDir + " does not exist");
        }
        report("================");
        if (envDir.list().length == 0) {
            report("No JE environment for " + rnId +
                   " in: " + envDir);
            return;
        }
        report("Resetting database for " + rnId +
               ", environment directory: " + envDir);

        try (Environment env = openEnvironment(envDir,
                                               rnId.getGroupName(),
                                               rnId.getFullName(),
                                               rnp.getJENodeHostPort())) {
            resetRNDb(env);
        }
    }

    private void resetRNDb(Environment env) {

        /* Both the version and topology databases are non-replicated. */
        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setReadOnly(dryRun);
        dbConfig.setReplicated(false);
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(false);

        final KVVersion rnVersion =
                VersionManager.getLocalVersion(logger, env, dbConfig);
        if (rnVersion == null) {
            throw new IllegalArgumentException("Unable to get RN version, it is" +
                                               " not present or the DB read failed");
        }

        if (VersionUtil.compareMinorVersion(KVVersion.CURRENT_VERSION,
                                            rnVersion) != 0) {
            throw new IllegalArgumentException
                   ("Cannot modify RN environment" +
                    " because it is not at the required minor software version: " +
                    KVVersion.CURRENT_VERSION.getNumericVersionString() +
                    ", found: " + rnVersion.getNumericVersionString());
        }
        report("RN version: " + rnVersion);
        final Topology topo = RepNode.readTopology(env, dbConfig);
        if (topo == null) {
            warning("No topology found");
            return;
        }

        if (!resetTopology(topo)) {
            report("No changes to topology");
            return;
        }
        if (!dryRun) {
            report("Writing topology: " +
                   TopologyPrinter.printTopology(topo));
            RepNode.writeTopology(topo, env, dbConfig);
        }
    }

    /**
     * Attempts to modify the specified topology. Returns true if changes
     * were made.
     */
    private boolean resetTopology(Topology topo) {
        boolean modified = false;

        report("Initial topology: " + TopologyPrinter.printTopology(topo));

        for (StorageNode sn : topo.getSortedStorageNodes()) {

            final StorageNodeId snId = sn.getStorageNodeId();
            final String hostname = snHosts.get(snId.getStorageNodeId());
            if (hostname == null) {
                /* Alert to a missing hostname */
                warning("No hostname for " + snId.getFullName() +
                        " found in topology");
                continue;
            }
            if (!sn.getHostname().equals(hostname)) {
                reportAction(snId.getFullName() +
                             " host name from " + sn.getHostname() +
                             " to " + hostname);
                final StorageNode newSn =
                      new StorageNode(sn.getDatacenterId(),
                                      hostname,
                                      sn.getRegistryPort());
                topo.update(snId, newSn);
                modified = true;
            }
        }
        return modified;
    }

    /**
     * Opens the JE environment. If open for write (dryRun == false) the
     * JE rep group is reset.
     */
    private Environment openEnvironment(File envDir,
                                        String groupName,
                                        String nodeName,
                                        String nodeHostPort) {
        final EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setReadOnly(dryRun);
        envConfig.setTransactional(true);

        if (dryRun) {
            return new Environment(envDir, envConfig);
        }



        /*
         * Setting recover to true will preserve the rep group UUID.
         */
        new DbResetRepGroup(envDir,
                            groupName,
                            nodeName,
                            nodeHostPort,
                            true /*recover*/).reset();
        final ReplicationConfig repConfig =
                new ReplicationConfig(groupName,
                                      nodeName,
                                      nodeHostPort);
        return new ReplicatedEnvironment(envDir, repConfig, envConfig);
    }

    private void reportAction(String action) {
        report((dryRun ? "Would change " : "Changing ") + action);
    }

    private void warning(String message) {
        warnings++;
        report("WARNING: " + message);
    }

    private void report(String message) {
        if (!silent) {
            System.out.println(message);
        }
    }

    public static void main(String[] args) {

        int nArgs = args.length;
        if (nArgs == 0) {
            usage(null);
        }

        File configFile = null;
        File bootconfigFile = null;
        final Map<Integer, String> snHosts = new HashMap<>();
        final Map<String, String> hostNameMap = new HashMap<>();
        boolean dryRun = false;
        boolean silent = false;
        boolean debug = false;

        try {
            for (int i = 0; i < nArgs; i++) {
                final String thisArg = args[i];

                if (thisArg.equals(CONFIG_FILE_FLAG)) {
                    if (i >= nArgs) {
                        usage(thisArg + " requires a file name");
                    }
                    configFile = new File(args[++i]);
                    if (!configFile.isFile()) {
                        usage(configFile + " is not a file");
                    }
                } else if (thisArg.equals(BOOTCONFIG_FILE_FLAG)) {
                    if (i >= nArgs) {
                        usage(thisArg + " requires a file name");
                    }
                    bootconfigFile = new File(args[++i]);
                    if (!bootconfigFile.isFile()) {
                        usage(bootconfigFile + " is not a file");
                    }
                } else if (thisArg.startsWith(SN_FLAG)) {
                    if (i >= nArgs) {
                        usage(thisArg + " requires old and new host names");
                    }
                    final int id = getId(thisArg);
                    String[] names = args[++i].split(",");
                    if (names.length != 2) {
                        usage(thisArg + " requires old and new host names");
                    }
                    if (hostNameMap.containsKey(names[1])) {
                        usage("The host " + names[1] + " is also an old host");
                    }
                    hostNameMap.put(names[0], names[1]);
                    snHosts.put(id, names[1]);
                } else if (thisArg.equals(DRY_RUN_FLAG)) {
                    dryRun = true;
                } else if (thisArg.equals(SILENT_FLAG)) {
                    silent = true;
                } else if (thisArg.equals(DEBUG_FLAG)) {
                    debug = true;
                } else {
                    usage("Unknown argument: " + thisArg);
                }
            }

            final Logger logger = Logger.getLogger(ResetHost.class.getName());
            if (!debug) {
                /* Suppress logging from KV code used by the utility */
                logger.setLevel(Level.OFF);
            }

            final ResetHost rh = new ResetHost(configFile,
                                               bootconfigFile,
                                               snHosts, hostNameMap,
                                               dryRun,  silent,
                                               logger);
            System.exit((rh.reset() > 0) ? -1 : 0);
        } catch (IllegalArgumentException iae) {
            if (debug) {
                throw iae;
            }
            usage(iae.getMessage());
        }
    }

    static private int getId(String arg) {
        String id = arg.substring(SN_FLAG.length());
        try {
            return Integer.valueOf(id);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Invalid " + SN_FLAG +
                                               " flag " + arg);
        }
    }

    /*
     * Output the usage information and exit with 0 status (normal). If msg
     * is non-null it is output before the usage info and the process will
     * exit with -1 status.
     */
    static void usage(String msg) {
        if (msg != null) {
            System.out.println(msg);
        }
        System.out.println("Usage: " + ResetHost.class.getName() + "\n" +
                           CONFIG_FILE_FLAG + " <config file>\n" +
                           "[" + BOOTCONFIG_FILE_FLAG + " <bootconfig file>\n"+
                           SN_FLAG + "1 <oldhost>,<newhost> " +
                           "[" + SN_FLAG + "2 <oldhost>,<newhost> ...]\n" +
                           "[" + DRY_RUN_FLAG + "]\n" +
                           "[" + SILENT_FLAG + "]\n");
        System.exit(msg == null ? 0 : -1);
    }
}
