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

import static oracle.kv.impl.param.ParameterState.ADMIN_TYPE;
import static oracle.kv.impl.param.ParameterState.ARBNODE_TYPE;
import static oracle.kv.impl.param.ParameterState.REPNODE_TYPE;
import static oracle.nosql.common.json.JsonUtils.createObjectNode;
import static oracle.nosql.common.json.JsonUtils.getArray;
import static oracle.nosql.common.json.JsonUtils.getAsText;
import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.CommandResult.CommandFails;
import oracle.kv.impl.admin.CommandResult.CommandSucceeds;
import oracle.kv.impl.admin.TopologyCheck.CONFIG_STATUS;
import oracle.kv.impl.admin.TopologyCheck.CreateRNRemedy;
import oracle.kv.impl.admin.TopologyCheck.RNLocationInput;
import oracle.kv.impl.admin.TopologyCheck.Remedy;
import oracle.kv.impl.admin.TopologyCheck.RemoveRNRemedy;
import oracle.kv.impl.admin.TopologyCheck.TOPO_STATUS;
import oracle.kv.impl.admin.TopologyCheck.UpdateANParamsRemedy;
import oracle.kv.impl.admin.TopologyCheck.UpdateAdminParamsRemedy;
import oracle.kv.impl.admin.TopologyCheck.UpdateRNParamsRemedy;
import oracle.kv.impl.admin.TopologyCheckUtils.SNServices;
import oracle.kv.impl.admin.param.GroupNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.topo.Rules;
import oracle.kv.impl.admin.topo.Rules.Results;
import oracle.kv.impl.admin.topo.Validations.RulesProblem;
import oracle.kv.impl.arb.ArbNodeStatus;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.arb.admin.ArbNodeAdminFaultException;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.SizeParameter;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.RequestTypeUpdater.RequestType;
import oracle.kv.impl.rep.admin.IllegalRepNodeServiceStateException;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.admin.RepNodeAdminFaultException;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeInfo;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ObjectUtil;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.ErrorMessage;
import oracle.kv.util.Ping;
import oracle.kv.util.Ping.AdminStatusFunction;
import oracle.kv.util.Ping.ArbNodeStatusFunction;
import oracle.kv.util.Ping.RepNodeStatusFunction;
import oracle.kv.util.PingDisplay;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.ObjectNode;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;

/**
 * Object encapsulating various verification methods.
 *
 * <p>Here is an annotated example of the output of the 'verify configuration'
 * command in JSON format.  See the {@link Ping} class for information about
 * fields in common with the output of the ping command, and the
 * {@link CommandJsonUtils} class for information about fields that are pertain
 * to results and are derived from the CommandJsonUtils class.
 *
 * <pre>
 * {
 *   // These command result fields are common to all CLI commands
 *   "operation" : "configure",
 *   "return_code" : 5300,
 *   "description" : "Deploy failed: ConnectionIOException ......" ,
 *   "cmd_cleanup_job" : [ "plan repair-topology" ]
 *   // These fields are all the same as for Ping
 *   "topology" : ...,
 *   "shardStatus" : ...,
 *   "adminStatus" : ...,
 *   "zoneStatus" : ...,
 *   "snStatus" : ...,
 *
 *   "storewideLogName" : "localhost:/kvroot/mystore/log/mystore_{0..N}.log",
 *   "violations" : [ {
 *     "resourceId" : "admin2",
 *     "description" : "ping() failed for admin2 : [...]"
 *   }, {
 *     "resourceId" : "rg1-rn2",
 *     "description" : "ping() failed for rg1-rn2 : [...]"
 *   } ],
 *   // Warnings in same format as violations, if any
 *   "warnings" : [ ]
 * }
 * </pre>
 */
public class VerifyConfiguration {

    private static final String eol = System.getProperty("line.separator");
    private static final Comparator<Problem> resourceComparator =
        new Comparator<Problem>() {
            @Override
            public int compare(Problem p1, Problem p2) {
                return p1.getResourceId().toString().compareTo(
                    p2.getResourceId().toString());
            }
        };
    private final Admin admin;
    private final boolean listAll;
    private final boolean showProgress;
    private final boolean json;
    private final Logger logger;

    private final TopologyCheck topoChecker;

    /* Collect output in JSON format. */
    private final ObjectNode jsonTop;

    /*
     * Found violations are stored here. The collection of violations is cleared
     * each time verify is run.
     */
    private final List<Problem> violations;

    /*
     * Issues that are only advisory, and are not true violations.
     */
    private final List<Problem> warnings;

    /*
     * Violations that have suggested remedies that can be carried out by
     * the topology checker.
     */
    private final Remedies remedies;

    /*
     * Available disk storage threshold below which we will display
     * verification notes.
     */
    private static final long AVAILABLE_DISK_STORAGE_THRESHOLD =
        5L * 1024L * 1024L * 1024L;

    private volatile VerifyType verifyType;
    enum VerifyType {
        TOPOLOGY, UPGRADE, PREREQUISITE;
        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    /**
     * Construct a verification object.
     *
     * @param admin the admin node
     * @param showProgress if true, log a line before checking each storage
     * node and its resident services.
     * @param listAll if true, list information for all services checked.
     * @param json if true, produce output in JSON format, otherwise in a
     * human-readable format
     * @param logger output is issued via this logger.
     */
    public VerifyConfiguration(Admin admin,
                               boolean showProgress,
                               boolean listAll,
                               boolean json,
                               Logger logger) {

        this.admin = admin;
        this.showProgress = showProgress;
        this.listAll = listAll;
        this.json = json;
        this.logger = logger;
        violations = new ArrayList<>();
        warnings = new ArrayList<>();
        jsonTop = createObjectNode();
        remedies = new Remedies();

        topoChecker = new TopologyCheck("VerifyConfiguration", logger,
                                        admin.getCurrentTopology(),
                                        admin.getCurrentParameters());
    }

    /**
     * Check whether the current topology obeys layout rules, and that the
     * store matches the layout, software version, and parameters described by
     * the most current topology and parameters. The verifyTopology() method
     * may be called multiple times in the life of a VerifyConfiguration
     * instance, but should be called serially.
     *
     * @return true if no violations were found.
     */
    public boolean verifyTopology() {
        return verifyTopology(admin.getCurrentTopology(),
                              admin.getLoginManager(),
                              admin.getCurrentParameters(),
                              true);
    }

    /**
     * Checks that the nodes in the store have been upgraded to the target
     * version.
     *
     * @param targetVersion
     * @return true if all nodes are up to date
     */
    public boolean verifyUpgrade(KVVersion targetVersion,
                                 List<StorageNodeId> snIds) {

        admin.getLogger().log(Level.INFO,
                              "Verifying upgrade to target version: {0}",
                              targetVersion.getNumericVersionString());

        clear();
        verifyType = VerifyType.UPGRADE;

        final Topology topology = admin.getCurrentTopology();
        PingDisplay.topologyOverviewToJson(topology, null, jsonTop);
        storewideLogNameToJson(admin, jsonTop);
        final RegistryUtils registryUtils =
            new RegistryUtils(topology, admin.getLoginManager(), logger);

        if (snIds == null) {
            snIds = topology.getSortedStorageNodeIds();
        }
        final ArrayNode jsonSNs =
            listAll ? jsonTop.putArray("snStatus") : null;
        for (StorageNodeId snId : snIds) {
            verifySNUpgrade(snId, targetVersion, registryUtils, topology,
                            jsonSNs);
        }
        problemsToJson();
        VerifyResults vResults = getResults();
        logger.log(Level.INFO, "{0}", vResults.display());
        return vResults.okay();
    }

    private void clear() {
        violations.clear();
        warnings.clear();
        remedies.clear();
        jsonTop.removeAll();
        verifyType = null;
    }

    private static void storewideLogNameToJson(Admin admin,
                                               ObjectNode jsonTop) {
        jsonTop.put("storewideLogName", admin.getStorewideLogName());
    }

    private static String displayStorewideLogName(JsonNode jsonTop) {
        final String logName = getAsText(jsonTop, "storewideLogName");
        if (logName == null) {
            return "";
        }
        return "See " + logName + " for progress messages";
    }

    private void problemsToJson() {
        problemsToJson(jsonTop, violations, "violations");
        problemsToJson(jsonTop, warnings, "warnings");
    }

    public static void problemsToJson(ObjectNode jsonTopNode,
                                      List<Problem> problems,
                                      String field) {
        Collections.sort(problems, resourceComparator);
        final ArrayNode jsonProblems = jsonTopNode.putArray(field);
        for (Problem problem : problems) {
            jsonProblems.add(problemToJson(problem));
        }
    }

    private static ObjectNode problemToJson(Problem problem) {
        final ObjectNode on = createObjectNode();
        on.put("resourceId", problem.getResourceId().toString());
        on.put("description", problem.toString());
        return on;
    }

    private static String displayProblem(JsonNode node) {
        return "[" + getAsText(node, "resourceId", "?") +
            "]\t" + getAsText(node, "description", "");
    }

    private void verifySNUpgrade(StorageNodeId snId,
                                 KVVersion targetVersion,
                                 RegistryUtils registryUtils,
                                 Topology topology,
                                 ArrayNode jsonSNs) {
        StorageNodeStatus snStatus = null;

        try {
            snStatus = registryUtils.getStorageNodeAgent(snId).ping();
        } catch (RemoteException re) {
            violations.add(new RMIFailed(snId, re, "ping()", showProgress,
                                         logger));
        } catch (NotBoundException nbe) {
            violations.add(new RMIFailed(snId, nbe, showProgress, logger));
        }

        if (snStatus != null) {
            final KVVersion snVersion = snStatus.getKVVersion();

            if (snVersion.compareTo(targetVersion) < 0) {
                warnings.add(new UpgradeNeeded(snId,
                                               snVersion,
                                               targetVersion,
                                               showProgress, logger));
            } else {
                /* SN is at or above target. Make sure RNs/ANs are up-to-date */
                verifyRNUpgrade(snVersion,
                                topology.getHostedRepNodeIds(snId),
                                registryUtils);
                verifyANUpgrade(snVersion,
                                topology.getHostedArbNodeIds(snId),
                                registryUtils);

            }
        }

        if (listAll) {
            final ObjectNode jsonSN = PingDisplay.storageNodeToJson(
                topology, topology.get(snId), snStatus);
            logger.info("Verify upgrade: " +
                        PingDisplay.displayStorageNode(jsonSN));
            jsonSNs.add(jsonSN);
        }
    }

    private void verifyRNUpgrade(KVVersion snVersion,
                                 Set<RepNodeId> hostedRepNodeIds,
                                 RegistryUtils registryUtils) {

        ServiceStatus rnStatus = null;

        for (RepNodeId rnId : hostedRepNodeIds) {
            try {
                final RepNodeAdminAPI rn = registryUtils.getRepNodeAdmin(rnId);
                rnStatus = rn.ping().getServiceStatus();
                final KVVersion rnVersion = rn.getInfo().getSoftwareVersion();

                if (rnVersion.compareTo(snVersion) != 0) {
                    warnings.add(new UpgradeNeeded(rnId,
                                                   rnVersion,
                                                   snVersion,
                                                   showProgress, logger));
                }
            } catch (RemoteException re) {
                violations.add(new RMIFailed(rnId, re, "ping()", showProgress,
                                             logger));
            } catch (NotBoundException nbe) {
                violations.add(new RMIFailed(rnId, nbe, showProgress, logger));
            } catch (RepNodeAdminFaultException rnafe) {
                if (rnafe.getFaultClassName().equals(
                    IllegalRepNodeServiceStateException.class.getName())) {

                    violations.add(
                        new StatusNotRight(rnId, ServiceStatus.RUNNING,
                                           rnStatus, showProgress, logger));
                } else {
                    throw rnafe;
                }
            }
        }
    }

    private void verifyANUpgrade(KVVersion snVersion,
                                 Set<ArbNodeId> hostedArbNodeIds,
                                 RegistryUtils registryUtils) {

        ServiceStatus anStatus = null;

        for (ArbNodeId anId : hostedArbNodeIds) {
            try {
                final ArbNodeAdminAPI ana = registryUtils.getArbNodeAdmin(anId);
                anStatus = ana.ping().getServiceStatus();
                final KVVersion anVersion = ana.getInfo().getSoftwareVersion();

                if (anVersion.compareTo(snVersion) != 0) {
                    warnings.add(new UpgradeNeeded(anId,
                                                   anVersion,
                                                   snVersion,
                                                   showProgress, logger));
                }
            } catch (RemoteException re) {
                violations.add(new RMIFailed(anId, re, "ping()", showProgress,
                                             logger));
            } catch (NotBoundException nbe) {
                violations.add(new RMIFailed(anId, nbe, showProgress, logger));
            } catch (ArbNodeAdminFaultException anafe) {
                if (anafe.getFaultClassName().equals(
                    IllegalRepNodeServiceStateException.class.getName())) {

                    violations.add(
                        new StatusNotRight(anId, ServiceStatus.RUNNING,
                                           anStatus, showProgress, logger));
                } else {
                    throw anafe;
                }
            }
        }
    }

    /**
     * Checks that the nodes in the store meet the specified prerequisite
     * version in order to be upgraded to the target version.
     *
     * @param targetVersion
     * @param prerequisiteVersion
     * @return true if no violations were found
     */
    public boolean verifyPrerequisite(KVVersion targetVersion,
                                      KVVersion prerequisiteVersion,
                                      List<StorageNodeId> snIds) {

        admin.getLogger().log(Level.INFO,
                              "Checking upgrade to target version: {0}, " +
                              "prerequisite: {1}",
                   new Object[]{targetVersion.getNumericVersionString(),
                                prerequisiteVersion.getNumericVersionString()});

        clear();
        verifyType = VerifyType.PREREQUISITE;
        final Topology topology = admin.getCurrentTopology();
        PingDisplay.topologyOverviewToJson(topology, null, jsonTop);
        storewideLogNameToJson(admin, jsonTop);
        final RegistryUtils registryUtils =
            new RegistryUtils(topology, admin.getLoginManager(), logger);

        if (snIds == null) {
            snIds = topology.getSortedStorageNodeIds();
        }
        final ArrayNode jsonSNs =
            listAll ? jsonTop.putArray("snStatus") : null;
        for (StorageNodeId snId : snIds) {
            verifySNPrerequisite(snId, targetVersion, prerequisiteVersion,
                                 registryUtils, topology, jsonSNs);
        }
        problemsToJson();
        VerifyResults vResults = getResults();
        logger.log(Level.INFO, "{0}", vResults.display());
        return vResults.okay();
    }

    private void verifySNPrerequisite(StorageNodeId snId,
                                      KVVersion targetVersion,
                                      KVVersion prerequisiteVersion,
                                      RegistryUtils registryUtils,
                                      Topology topology,
                                      ArrayNode jsonSNs) {
        StorageNodeStatus snStatus = null;

        try {
            final StorageNodeAgentAPI sna =
                        registryUtils.getStorageNodeAgent(snId);
            snStatus = sna.ping();
        } catch (RemoteException re) {
            violations.add(new RMIFailed(snId, re, "ping()", showProgress,
                                         logger));
        } catch (NotBoundException nbe) {
            violations.add(new RMIFailed(snId, nbe, showProgress, logger));
        }

        if (snStatus != null) {
            final KVVersion snVersion = snStatus.getKVVersion();

            /* Check if the SN is too old (doesn't meet prereq) */
            if (snVersion.compareTo(prerequisiteVersion) < 0) {

                /* SN is too old */
                violations.add(new UpgradeNeeded(snId,
                                                 snVersion,
                                                 prerequisiteVersion,
                                                 showProgress, logger));
                /*
                 * Meets prereq, so check if the SN is too new (downgrade across
                 * minor version)
                 */
            } else if (VersionUtil.compareMinorVersion(snVersion,
                                                       targetVersion) > 0) {

                /*
                 * SN is running later version, can't go back to the target
                 * version
                 */
                violations.add(new BadDowngrade(snId,
                                                snVersion,
                                                targetVersion,
                                                showProgress, logger));
            }
        }

        if (listAll) {
            final ObjectNode jsonSN =
                PingDisplay.storageNodeToJson(topology,
                                              topology.get(snId),
                                              snStatus);
            logger.info("Verify prerequisite: " +
                        PingDisplay.displayStorageNode(jsonSN));
            jsonSNs.add(jsonSN);
        }
    }

    /**
     * Non-private entry point for unit tests. Provides a way to supply an
     * intentionally corrupt topology or parameters, to test for error cases.
     */
    synchronized boolean verifyTopology(Topology topology,
                                        LoginManager loginMgr,
                                        Parameters currentParams,
                                        boolean topoIsDeployed) {

        clear();
        verifyType = VerifyType.TOPOLOGY;
        PingDisplay.topologyOverviewToJson(topology, null, jsonTop);
        logger.info(PingDisplay.displayTopologyOverview(jsonTop));
        storewideLogNameToJson(admin, jsonTop);
        RegistryUtils registryUtils =
            new RegistryUtils(topology, loginMgr, logger);
        Results results = Rules.validate(topology, currentParams,
                                         topoIsDeployed);
        violations.addAll(results.getViolations());
        warnings.addAll(results.getWarnings());

        /* Add any remedies that were generated during the validate phase */
        for (RulesProblem rp : results.getViolations()) {
            final Remedy remedy = rp.getRemedy(topoChecker);

            if (remedy != null) {
                remedies.add(remedy);
            }
        }
        checkServices(topology, currentParams, registryUtils);
        problemsToJson();

        VerifyResults vResults = getResults();
        logger.log(Level.INFO, "{0}", vResults.display());
        return vResults.okay();
    }

    /** Common information for both RNs and ANs. */
    private static abstract class BasicNodeInfo {
        abstract State getState();
        abstract ServiceStatus getStatus();
    }

    /** Store ping and getParams results for an RN. */
    private static class RepNodeInfo extends BasicNodeInfo {
        RepNodeStatus pingStatus;
        RemoteException pingRemoteException;
        NotBoundException pingNotBoundException;
        LoadParameters getParamsResult;
        RemoteException getParamsRemoteException;
        @Override
        State getState() {
            return (pingStatus != null) ?
                pingStatus.getReplicationState() :
                null;
        }
        @Override
        ServiceStatus getStatus() {
            return (pingStatus != null) ? pingStatus.getServiceStatus() : null;
        }
    }

    /** Store ping and getParams results for an AN. */
    private static class ArbNodeInfo extends BasicNodeInfo {
        ArbNodeStatus pingStatus;
        RemoteException pingRemoteException;
        NotBoundException pingNotBoundException;
        LoadParameters getParamsResult;
        RemoteException getParamsRemoteException;
        @Override
        State getState() {
            return (pingStatus != null) ? pingStatus.getArbiterState() : null;
        }
        @Override
        ServiceStatus getStatus() {
            return (pingStatus != null) ? pingStatus.getServiceStatus() : null;
        }
    }

    /**
     * For each SN in the store, contact each service, conduct check.
     */
    /* Suppress Eclipse warning for jsonSNs.add call */
    @SuppressWarnings("null")
    private void checkServices(Topology topology,
                               Parameters currentParams,
                               RegistryUtils registryUtils) {

        /* Collect RN ping and getParams results, and master status info */
        final Map<RepNodeId, RepNodeInfo> rnInfoMap = new HashMap<>();
        final Map<ArbNodeId, ArbNodeInfo> anInfoMap = new HashMap<>();
        final Map<RepGroupId, RepNodeStatus> masterStatusMap = new HashMap<>();
        rnPingAndGetParams(
            topology, registryUtils, rnInfoMap, masterStatusMap);

        anPingAndGetParams(topology, registryUtils, anInfoMap);

        /* Use rnInfoMap to provide RepNodeStatus values */
        final RepNodeStatusFunction rnfunc = new RepNodeStatusFunction() {
            @Override
            public RepNodeStatus get(RepNode node) {
                final RepNodeInfo info = rnInfoMap.get(node.getResourceId());
                if (info.pingStatus != null) {
                    return info.pingStatus;
                }
                return null;
            }
        };

         /* Use anInfoMap to provide ArbNodeStatus values */
        final ArbNodeStatusFunction anfunc = new ArbNodeStatusFunction() {
            @Override
            public ArbNodeStatus get(ArbNode node) {
                final ArbNodeInfo info = anInfoMap.get(node.getResourceId());
                if (info.pingStatus != null) {
                    return info.pingStatus;
                }
                return null;
            }
        };

        PingDisplay.shardOverviewToJson(topology, rnfunc, anfunc, null,
                                        jsonTop);

        final Map<AdminId, AdminInfo> allAdminInfo =
            collectAdminInfo(currentParams, registryUtils);
        final AdminStatusFunction adminStatusFunc = new AdminStatusFunction() {
            @Override
            public AdminStatus get(AdminId adminId) {
                final AdminInfo adminInfo = allAdminInfo.get(adminId);
                return (adminInfo != null) ? adminInfo.adminStatus : null;
            }
        };
        PingDisplay.adminOverviewToJson(currentParams, adminStatusFunc,
                                        jsonTop);

        final ArrayNode jsonZones = jsonTop.putArray("zoneStatus");
        for (final Datacenter dc : topology.getSortedDatacenters()) {
            jsonZones.add(
                    PingDisplay.zoneOverviewToJson(topology, dc, rnfunc,
                                                   anfunc, null));
        }

        Map<StorageNodeId, SNServices> sortedResources =
            TopologyCheckUtils.groupServicesBySN(topology, currentParams);
        final ArrayNode jsonSNs =
            listAll ? jsonTop.putArray("snStatus") : null;

        /* The check is done in SN order */
        for (SNServices nodeInfo : sortedResources.values()) {

            StorageNodeId snId = nodeInfo.getStorageNodeId();

            /* If show progress is set, log per Storage Node. */
            if (showProgress) {
                String msg = "Verify: == checking storage node " + snId +
                    " ==";
                logger.info(msg);
            }

            /* Check the StorageNodeAgent on this node. */
            final ObjectNode jsonSN = checkStorageNode(
                registryUtils, topology, currentParams, snId, nodeInfo,
                rnInfoMap, anInfoMap);
            if (listAll) {
                jsonSNs.add(jsonSN);
            }

            /* If the Admin is there, check it. */
            AdminId adminId = nodeInfo.getAdminId();
            if (adminId != null) {
                checkAdmin(currentParams, adminId, allAdminInfo.get(adminId),
                           jsonSN);
            }

            /* Check all RepNodes on this storage node. */
            final ArrayNode jsonRNs =
                listAll ? jsonSN.putArray("rnStatus") : null;
            for (RepNodeId rnId : nodeInfo.getAllRepNodeIds()) {
                checkRepNode(topology, snId, rnId, currentParams,
                             rnInfoMap.get(rnId),
                             masterStatusMap.get(
                                 new RepGroupId(rnId.getGroupId())),
                             jsonRNs);
            }

            /* Check all ArbNodes on this storage node. */
            final ArrayNode jsonANs =
                listAll ? jsonSN.putArray("anStatus") : null;
            for (ArbNodeId anId : nodeInfo.getAllARBs()) {
                checkArbNode(topology, snId, anId, currentParams,
                             anInfoMap.get(anId),
                             jsonANs);
            }
        }
    }

    /* Suppress Eclipse warning for rna.getParams call */
    @SuppressWarnings("null")
    private static void rnPingAndGetParams(
        Topology topology,
        RegistryUtils registryUtils,
        Map<RepNodeId, RepNodeInfo> rnInfoMap,
        Map<RepGroupId, RepNodeStatus> masterStatusMap) {

        for (final RepGroup rg : topology.getRepGroupMap().getAll()) {
            for (final RepNode rn : rg.getRepNodes()) {
                final RepNodeId rnId = rn.getResourceId();
                RepNodeAdminAPI rna = null;
                final RepNodeInfo rnInfo = new RepNodeInfo();
                try {
                    rna = registryUtils.getRepNodeAdmin(rnId);
                    rnInfo.pingStatus = rna.ping();
                } catch (RemoteException re) {
                    rnInfo.pingRemoteException = re;
                } catch (NotBoundException e) {
                    rnInfo.pingNotBoundException = e;
                }
                if (rnInfo.pingStatus != null) {
                    if (rnInfo.pingStatus.getReplicationState().isMaster()) {
                        masterStatusMap.put(new RepGroupId(rnId.getGroupId()),
                                            rnInfo.pingStatus);
                    }
                    if (ServiceStatus.RUNNING.equals(
                            rnInfo.pingStatus.getServiceStatus())) {
                        try {
                            rnInfo.getParamsResult = rna.getParams();
                        } catch (RemoteException re) {
                            rnInfo.getParamsRemoteException = re;
                        }
                    }
                }
                rnInfoMap.put(rn.getResourceId(), rnInfo);
            }
        }
    }

    /* Suppress Eclipse warning for ana.getParams call */
    @SuppressWarnings("null")
    private static void anPingAndGetParams(
        Topology topology,
        RegistryUtils registryUtils,
        Map<ArbNodeId, ArbNodeInfo> anInfoMap) {

        for (final RepGroup rg : topology.getRepGroupMap().getAll()) {
            for (final ArbNode an : rg.getArbNodes()) {
                final ArbNodeId anId = an.getResourceId();
                ArbNodeAdminAPI ana = null;
                final ArbNodeInfo anInfo = new ArbNodeInfo();
                try {
                    ana = registryUtils.getArbNodeAdmin(anId);
                    anInfo.pingStatus = ana.ping();
                } catch (RemoteException re) {
                    anInfo.pingRemoteException = re;
                } catch (NotBoundException e) {
                    anInfo.pingNotBoundException = e;
                }

                if (anInfo.pingStatus != null) {
                    if (ServiceStatus.RUNNING.equals(
                            anInfo.pingStatus.getServiceStatus())) {
                        try {
                            anInfo.getParamsResult = ana.getParams();
                        } catch (RemoteException re) {
                            anInfo.getParamsRemoteException = re;
                        }
                    }
                }
                anInfoMap.put(an.getResourceId(), anInfo);
            }
        }
    }

    /** Store getAdminStatus results for an Admin. */
    private static class AdminInfo {
        CommandServiceAPI cs;
        AdminStatus adminStatus;
        RemoteException remoteException;
        NotBoundException notBoundException;
    }

    private Map<AdminId, AdminInfo> collectAdminInfo(
        Parameters params, RegistryUtils regUtils) {

        final Map<AdminId, AdminInfo> allAdminInfo = new HashMap<>();
        for (final AdminId adminId : params.getAdminIds()) {
            final StorageNodeId snId = params.get(adminId).getStorageNodeId();
            final AdminInfo adminInfo = new AdminInfo();
            try {
                final CommandServiceAPI cs = regUtils.getAdmin(snId);
                adminInfo.cs = cs;
                adminInfo.adminStatus = cs.getAdminStatus();
            } catch (RemoteException e) {
                adminInfo.remoteException = e;
            } catch (NotBoundException e) {
                adminInfo.notBoundException = e;
            }
            allAdminInfo.put(adminId, adminInfo);
        }
        return allAdminInfo;
    }

    /**
     * Ping this admin and check its params.
     */
    private void checkAdmin(Parameters params, AdminId aId,
                            AdminInfo adminInfo, ObjectNode jsonSN) {
        StorageNodeId hostSN = params.get(aId).getStorageNodeId();
        boolean pingProblem = false;
        CommandServiceAPI cs = null;
        AdminStatus adminStatus = null;
        ServiceStatus status = ServiceStatus.UNREACHABLE;

        boolean isDisabled = params.get(aId).isDisabled();
        ServiceStatus expected = isDisabled ? ServiceStatus.UNREACHABLE :
                ServiceStatus.RUNNING;

        if (adminInfo.adminStatus != null) {
            cs = adminInfo.cs;
            adminStatus = adminInfo.adminStatus;
            status = adminStatus.getServiceStatus();
        } else if (adminInfo.remoteException != null) {
            final RemoteException re = adminInfo.remoteException;
            violations.add(new RMIFailed(aId, re, "ping()",showProgress,
                                         logger));
            pingProblem = true;
        } else {
            final NotBoundException e = adminInfo.notBoundException;
            violations.add(new RMIFailed(aId, e, showProgress, logger));
            pingProblem = true;
        }

        /*
         * Check the JE HA metadata and SN remote config against the AdminDB
         * for this admin.
         */
        Remedy remedy = topoChecker.checkAdminLocation(admin, aId);
        if (remedy.canFix()) {
            remedies.add(remedy);
        }

        if (!pingProblem) {
            if (status.equals(ServiceStatus.RUNNING)) {
                checkAdminParams(cs, params, aId, hostSN);
            } else {
                violations.add(new StatusNotRight(aId, ServiceStatus.RUNNING,
                                                  status, showProgress,
                                                  logger));
            }
        }

        if (listAll) {
            final ObjectNode jsonAdmin =
                PingDisplay.adminToJson(aId, adminStatus, expected);
            logger.info("Verify: " + PingDisplay.displayAdmin(jsonAdmin));
            jsonSN.set("adminStatus", jsonAdmin);
        }
    }

    /**
     * Ping the storage node. If it does not respond, add it to the problem
     * list. If the version doesn't match that of the admin, also add that to
     * the list.
     */
    @SuppressWarnings("null")
    private ObjectNode checkStorageNode(RegistryUtils regUtils,
                                        Topology topology,
                                        Parameters currentParams,
                                        StorageNodeId snId,
                                        SNServices nodeInfo,
                                        Map<RepNodeId, RepNodeInfo> rnInfoMap,
                                        Map<ArbNodeId, ArbNodeInfo> anInfoMap) {

        boolean pingProblem = false;
        StorageNodeStatus snStatus = null;
        StorageNodeAgentAPI sna = null;
        ServiceStatus status = ServiceStatus.UNREACHABLE;
        try {
            sna = regUtils.getStorageNodeAgent(snId);
            snStatus = sna.ping();
            status = snStatus.getServiceStatus();
        } catch (RemoteException re) {
            violations.add(new RMIFailed(snId, re, "ping()", showProgress,
                                         logger));
            pingProblem = true;
        } catch (NotBoundException e) {
            violations.add(new RMIFailed(snId, e, showProgress, logger));
            pingProblem = true;
        }

        ObjectNode jsonSN = null;
        if (listAll) {
            jsonSN = PingDisplay.storageNodeToJson(
                topology, topology.get(snId), snStatus);
            logger.info("Verify: " + PingDisplay.displayStorageNode(jsonSN));
        }

        if (!pingProblem) {
            if (status.equals(ServiceStatus.RUNNING)) {
                checkSNParams(sna, snId, currentParams, nodeInfo, rnInfoMap,
                              anInfoMap);
            } else {
                violations.add(new StatusNotRight(snId, ServiceStatus.RUNNING,
                                                  status, showProgress,
                                                  logger));
            }

            if (!KVVersion.CURRENT_VERSION.equals(snStatus.getKVVersion())) {
                /*
                 * Since version differences are expected during upgrades, we
                 * shouldn't consider this a violation, only a warning
                 */
                warnings.add(new VersionDifference(snId,
                                                   snStatus.getKVVersion(),
                                                   showProgress, logger));
            }

            final KVVersion storeVersion =
               getVersion(
                   currentParams.getGlobalParams().getMap().get(
                       ParameterState.GP_STORE_VERSION).asString());
            if (storeVersion != null &&
                storeVersion.compareTo(snStatus.getKVVersion()) > 0) {
                violations.add(new StoreVersionHigher(snId,
                                                      storeVersion,
                                                      snStatus.getKVVersion(),
                                                      showProgress, logger));
            }

        }

        return jsonSN;
    }

    /**
     * Check ping and getParams results for this repNode.
     */
    private void checkRepNode(Topology topology,
                              StorageNodeId snId,
                              RepNodeId rnId,
                              Parameters currentParams,
                              RepNodeInfo rnInfo,
                              RepNodeStatus masterStatus,
                              ArrayNode jsonRNs) {

        boolean pingProblem = false;
        ServiceStatus status = ServiceStatus.UNREACHABLE;
        RepNodeStatus rnStatus = null;

        boolean isDisabled = currentParams.get(rnId).isDisabled();
        ServiceStatus expected = isDisabled ? ServiceStatus.UNREACHABLE :
            ServiceStatus.RUNNING;

        if (rnInfo.pingStatus != null) {
            rnStatus = rnInfo.pingStatus;
            status = rnStatus.getServiceStatus();
        } else if (rnInfo.pingRemoteException != null) {
            final RemoteException re = rnInfo.pingRemoteException;
            if (!expected.equals(ServiceStatus.UNREACHABLE)) {
                violations.add(new RMIFailed(rnId, re, "ping()", showProgress,
                                             logger));
                pingProblem = true;
            } else {
                /* The RN is configured as being disabled, issue a warning */
                reportStopped(rnId, snId);
            }
        } else {
            final NotBoundException e = rnInfo.pingNotBoundException;
            if (!expected.equals(ServiceStatus.UNREACHABLE)) {
                violations.add(new RMIFailed(rnId, e, showProgress, logger));
                pingProblem = true;
            } else {
                /* The RN is configured as being disabled, issue a warning */
                reportStopped(rnId, snId);
            }
        }

        if (!pingProblem) {
            if (status.equals(expected)) {
                if (status.equals(ServiceStatus.RUNNING)) {
                    checkRNParams(rnId, topology, currentParams, rnInfo);
                }
            } else {
                violations.add(new StatusNotRight(rnId, expected, status,
                                                  showProgress, logger));
            }
        }

        if (rnStatus != null) {
            final String enabledRequestType = rnStatus.getEnabledRequestType();
            if (!enabledRequestType.equalsIgnoreCase(RequestType.ALL.name())) {
                violations.add(new RequestsDisabled(rnId, enabledRequestType,
                                                    showProgress, logger));
            }

            final RepNodeParams repNodeParams = currentParams.get(rnId);

            /*
             * If storagedir is not specified then staticDirSize will be
             * 0L. RNs are part of rootdir in that case. We will just add
             * available and used storage size for getting approx static
             * Storage Size.
             */
            long staticStorageSize =
                repNodeParams.getStorageDirectorySize();
            final long availableStorageSize = rnStatus.getAvailableLogSize();
            final long usedStorageSize = rnStatus.getUsedLogSize();

            if (staticStorageSize == 0L) {
                staticStorageSize = usedStorageSize + availableStorageSize;
            }

            final String staticSizeInUnits =
                TopologyPrinter.getSizeInUnits(staticStorageSize);
            final String availableSizeInUnits =
                    TopologyPrinter.getSizeInUnits(availableStorageSize);

            final String storageDirPath =
                repNodeParams.getStorageDirectoryPath();
            final boolean isEnvironmentStatsAvailable =
                rnStatus.isEnvironmentStatsAvailable();

            /*
             * Raise disk exceed and low violations/warnings only if
             * statistics are available
             */
            if (isEnvironmentStatsAvailable) {
                if (availableStorageSize <= 0) {
                    violations.add(new AvailableStorageExceeded(
                                   rnId, storageDirPath,
                                   staticSizeInUnits, availableSizeInUnits,
                                   showProgress, logger));
                } else if (availableStorageSize <=
                               AVAILABLE_DISK_STORAGE_THRESHOLD) {
            	    warnings.add(new AvailableStorageLow(
            	                 rnId, storageDirPath,
            	                 staticSizeInUnits, availableSizeInUnits,
            	                 showProgress, logger));
               }
            }
        }
        if (listAll) {
            final ObjectNode jsonRN = PingDisplay.repNodeToJson(
                topology.get(rnId), rnStatus, masterStatus, expected);
            logger.info("Verify: " + PingDisplay.displayRepNode(jsonRN));
            jsonRNs.add(jsonRN);
        }
    }

    /**
     * Check ping and getParams results for this arbNode.
     */
    private void checkArbNode(Topology topology,
                              StorageNodeId snId,
                              ArbNodeId anId,
                              Parameters currentParams,
                              ArbNodeInfo anInfo,
                              ArrayNode jsonANs) {

        boolean pingProblem = false;
        ServiceStatus status = ServiceStatus.UNREACHABLE;
        ArbNodeStatus anStatus = null;

        boolean isDisabled = currentParams.get(anId).isDisabled();
        ServiceStatus expected = isDisabled ? ServiceStatus.UNREACHABLE :
            ServiceStatus.RUNNING;

        if (anInfo.pingStatus != null) {
            anStatus = anInfo.pingStatus;
            status = anStatus.getServiceStatus();
        } else if (anInfo.pingRemoteException != null) {
            final RemoteException re = anInfo.pingRemoteException;
            if (!expected.equals(ServiceStatus.UNREACHABLE)) {
                violations.add(new RMIFailed(anId, re, "ping()", showProgress,
                                             logger));
                pingProblem = true;
            } else {
                /* The AN is configured as being disabled, issue a warning */
                reportStopped(anId, snId);
            }
        } else {
            final NotBoundException e = anInfo.pingNotBoundException;
            if (!expected.equals(ServiceStatus.UNREACHABLE)) {
                violations.add(new RMIFailed(anId, e, showProgress, logger));
                pingProblem = true;
            } else {
                /* The AN is configured as being disabled, issue a warning */
                reportStopped(anId, snId);
            }
        }

        if (!pingProblem) {
            if (status.equals(expected)) {
                if (status.equals(ServiceStatus.RUNNING)) {
                    checkANParams(anId, topology, currentParams, anInfo);
                }
            } else {
                violations.add(new StatusNotRight(anId, expected, status,
                                                  showProgress, logger));
            }
        }

        if (listAll) {
            final ObjectNode jsonAN = PingDisplay.arbNodeToJson(
                topology.get(anId), anStatus, expected);
            logger.info("Verify: " + PingDisplay.displayArbNode(jsonAN));
            jsonANs.add(jsonAN);
        }
    }

    /**
     * Report that a RN or AN is not up.
     */
    private void reportStopped(ResourceId resId, StorageNodeId snId) {
        warnings.add(new ServiceStopped(resId, snId, showProgress,
                                        logger, true));

        /*
         * If there are no previously detected errors with this RN, and its
         * location is correct, suggest that it should be restarted.
         */
        final boolean previousRemedy = remedies.remedyExists(resId);

        if (!previousRemedy) {
            remedies.add(
                new CreateRNRemedy(topoChecker,
                                   new RNLocationInput(TOPO_STATUS.HERE,
                                                       CONFIG_STATUS.HERE),
                                   snId, resId, null /* jeHAInfo */));
        }
    }

    /** Check SN configuration parameters against the admin database. */
    private void checkSNParams(StorageNodeAgentAPI sna,
                               StorageNodeId snId,
                               Parameters currentParams,
                               SNServices nodeInfo,
                               Map<RepNodeId, RepNodeInfo> rnInfoMap,
                               Map<ArbNodeId, ArbNodeInfo> anInfoMap) {

        LoadParameters remoteParams;
        try {
            remoteParams = sna.getParams();
        } catch (RemoteException re) {
            violations.add(new RMIFailed(snId, re, "getParams", showProgress,
                                         logger));
            return;
        }

        if (!checkParams(snId, remoteParams, currentParams.get(snId).getMap(),
                         ParamMismatchLocation.CONFIG)) {
            return;
        }

        /* Check the SNAs storage directory map if present */
        final ParameterMap storageDirMap =
                                currentParams.get(snId).getStorageDirMap();
        if (storageDirMap != null) {
            if (!checkParams(snId, remoteParams, storageDirMap,
                             ParamMismatchLocation.CONFIG)) {
                return;
            }
        } else {
            if (remoteParams.getMap(ParameterState.BOOTSTRAP_MOUNT_POINTS) !=
                null) {
                violations.add(
                    new ParamMismatch(
                        snId,
                        "Parameter collection " +
                        ParameterState.BOOTSTRAP_MOUNT_POINTS + " missing " +
                        ParamMismatchLocation.CONFIG.describe(),
                        showProgress, logger));
                return;
            }

        }

        /* Check the SNAs RN log directory map if present */
        final ParameterMap rnLogDirMap =
                                currentParams.get(snId).getRNLogDirMap();
        if (rnLogDirMap != null) {
            if (!checkParams(snId, remoteParams, rnLogDirMap,
                             ParamMismatchLocation.CONFIG)) {
                return;
            }
        } else {
            if (remoteParams.getMap
                    (ParameterState.BOOTSTRAP_RNLOG_MOUNT_POINTS) != null) {
                violations.add(
                    new ParamMismatch(
                        snId,
                        "Parameter collection " +
                        ParameterState.BOOTSTRAP_MOUNT_POINTS + " missing " +
                        ParamMismatchLocation.CONFIG.describe(),
                        showProgress, logger));
                return;
            }

        }

        /* Check the SNAs Admin directory map if present */
        final ParameterMap adminDirMap =
                                currentParams.get(snId).getAdminDirMap();
        if (adminDirMap != null) {
            if (!checkParams(snId, remoteParams, adminDirMap,
                             ParamMismatchLocation.CONFIG)) {
                return;
            }
        } else {
            if (remoteParams.getMap
                    (ParameterState.BOOTSTRAP_ADMIN_MOUNT_POINTS) != null) {
                violations.add(
                    new ParamMismatch(
                        snId,
                        "Parameter collection " +
                        ParameterState.BOOTSTRAP_ADMIN_MOUNT_POINTS + " missing " +
                        ParamMismatchLocation.CONFIG.describe(),
                        showProgress, logger));
                return;
            }
        }

        if (!checkParams(snId, remoteParams,
                         currentParams.getGlobalParams().getMap(),
                         ParamMismatchLocation.CONFIG)) {
            return;
        }

        StorageNodeInfo snInfo = null;
        try {
            snInfo = sna.getStorageNodeInfo();
        } catch (UnsupportedOperationException uoe) /* CHECKSTYLE:OFF */ {
            /*
             * UOE indicates that the SN has not yet been upgraded
             * to a version that supports getStorageNodeInfo(). Ignore
             * this exception as there will be a separate violation
             * created to warn about upgrade.
             */
        } /* CHECKSTYLE:ON */ catch (RemoteException re) {
            violations.add(new RMIFailed(snId, re, "getStorageNodeInfo",
                                         showProgress, logger));
        }

        /*
         * Parameters are OK. If a storage map exist, check the sizes against
         * the physical system.
         */
        if (storageDirMap != null) {
            checkSizes(storageDirMap, snId, snInfo);
        }

        /*
         * Parameters are OK. If a RN log dir map exist, check the sizes
         * against the physical system.
         */
        if (rnLogDirMap != null) {
            /*
             * TODO : Commenting this since rnlogdirsize are not
             * exposed yet.
             */
            //checkSizes(rnLogDirMap, snId, snInfo);
        }

        /*
         * Parameters are OK. If a Admin dir map exist, check the sizes against
         * the physical system.
         */
        if (adminDirMap != null) {
            checkSizes(adminDirMap, snId, snInfo);
        }

        /*
         * Make sure all services that are supposed to be on this SN, according
         * to the topo and params, the SN's config.xml, and the JE HA group
         * have location info that are all correct.
         * The RNs to check are the union of those that are in the SN's
         * config.xml and those in the topology.
         */
        topoChecker.saveSNRemoteParams(snId, remoteParams);
        Set<RepNodeId> rnsToCheck = topoChecker.getPossibleRNs(snId);
        for (RepNodeId rnId: rnsToCheck) {
            check(snId, rnId, currentParams, remoteParams,
                  rnInfoMap.get(rnId));
        }

        /*
         * The Admins to check are the union of those that are in the SN's
         * config.xml and those in AdminDB params.
         */
        ParameterMap adminMap =
            remoteParams.getMapByType(ParameterState.ADMIN_TYPE);
        if (adminMap != null) {
            AdminId aid =
                new AdminId(adminMap.getOrZeroInt(ParameterState.AP_ID));
            if (nodeInfo.getAdminId() == null ||
                !aid.equals(nodeInfo.getAdminId())) {
                violations.add(new ParamMismatch
                             (snId, "Storage Node is managing admin " + aid +
                              " but the admin does not know this",
                              showProgress, logger));
            } else {
                checkParams(aid, remoteParams, currentParams.get(aid).getMap(),
                            ParamMismatchLocation.CONFIG);
            }
        } else if (nodeInfo.getAdminId() != null) {
            violations.add(new ParamMismatch
                         (snId, "Storage Node is not managing an Admin but " +
                          "the admin believes it is",
                          showProgress, logger));
        }
        Set<ArbNodeId> ansToCheck = topoChecker.getPossibleANs(snId);
        for (ArbNodeId anId: ansToCheck) {
            check(snId, anId, currentParams, remoteParams,
                  anInfoMap.get(anId));
        }
    }

    public void checkSizes(ParameterMap dirMap,
                           StorageNodeId snId,
                           StorageNodeInfo snInfo) {
        final Map<String, Long> sizes =
                                    snInfo.getStorageDirectorySizes();
        for (Parameter p : dirMap) {
            final String directory = p.getName();
            final long requiredSize = SizeParameter.getSize(p);
            final Long reportedSize = sizes.get(directory);

            if (reportedSize == null) {
                /* Mount point not defined on SN? */
                violations.add(new StorageDirectoryProblem(snId,
                                   directory,
                                   "directory information not reported",
                                   showProgress, logger));
            } else if (reportedSize < 0) {
                /* SN encountered error getting size */
                violations.add(new StorageDirectoryProblem(snId,
                                    directory,
                                    "node encountered error getting " +
                                    "directory information, see logs " +
                                    "for " + snId,
                                    showProgress, logger));
            } else if (reportedSize < requiredSize) {
                /* Directory too small */
                violations.add(new StorageDirectoryProblem(snId,
                                    directory,
                                    "required size: " + requiredSize +
                                    ", reported size: " + reportedSize,
                                    showProgress, logger));
            }
        }
    }

    /**
     * See if this repNode's params match those held in the admin db.
     */
    private void checkRNParams(RepNodeId rnId,
                               Topology topology,
                               Parameters currentParams,
                               RepNodeInfo rnInfo) {

        /* Check results of asking the RN for its params */
        LoadParameters remoteParams;
        if (rnInfo.getParamsResult != null) {
            remoteParams = rnInfo.getParamsResult;
        } else {
            final RemoteException re = rnInfo.getParamsRemoteException;
            violations.add(new RMIFailed(rnId, re, "getParams", showProgress,
                                         logger));
            return;
        }

        checkServiceParams(topology.get(rnId).getStorageNodeId(), rnId,
                           remoteParams, currentParams);
    }

    private void check(StorageNodeId snId,
                       ResourceId resId,
                       Parameters currentParams,
                       LoadParameters remoteParams,
                       BasicNodeInfo nodeInfo) {
        try {
            final Remedy remedy = topoChecker.checkLocation(
                admin, snId, resId, false /* calledByDeployNewRN */,
                false /* makeRNEnabled */, null /* oldSNId */,
                null /* storageDirectory */);

            if (remedy.canFix()) {
                logger.log(Level.INFO, "{0}", new Object[] {remedy});
                Problem p;
                if (remedy instanceof CreateRNRemedy) {
                    if ((nodeInfo != null) &&
                        nodeInfo.getStatus() != null &&
                        nodeInfo.getStatus().equals(ServiceStatus.RUNNING)) {

                        /*
                         * The CreateRNRemedy indicates that the node is either
                         * not in the SN config or not in the JEHA group. Since
                         * the node is running, this indicates that it is not
                         * in the JEHA group. Could be transient. The
                         * node is in the process of being added to the JEHA
                         * group.
                         */
                        p = new NotInGroup(resId,
                                           snId,
                                           showProgress,
                                           logger);
                    } else {
                        p = new ServiceStopped(resId,
                                               snId,
                                               showProgress,
                                               logger,
                                               false); /* isDisabled */
                    }
                } else {
                    p = new ParamMismatch(resId,
                                          remedy.problemDescription(),
                                          showProgress, logger);
                }
                violations.add(p);
                remedies.add(remedy);
            } else {

                /*
                 * If checking the RN location did not provide a fix, but
                 * the RN appears in the current RN parameters, then check
                 * parameters, and let that be what reports problems and
                 * provides fixes.  Otherwise, just report the parameter
                 * mismatch.
                 */
                final GroupNodeParams currentNodeParams =
                        (resId.getType() == ResourceType.REP_NODE) ?
                        currentParams.get((RepNodeId)resId) :
                        currentParams.get((ArbNodeId)resId);
                if (currentNodeParams != null) {

                    checkParams(resId, remoteParams,
                                currentNodeParams.getMap(),
                                ParamMismatchLocation.CONFIG);
                } else {
                    violations.add(
                        new ParamMismatch(resId,
                                          remedy.problemDescription(),
                                          showProgress, logger));
                }
            }
        } catch (RemoteException e) {
            violations.add
            (new RMIFailed(snId, e, "checkLocation", showProgress,
                           logger));
        } catch (NotBoundException e) {
            violations.add
            (new RMIFailed(snId, e, showProgress, logger));
        }
    }

    /**
     * See if this arbNode's params match those held in the admin db.
     */
    private void checkANParams(ArbNodeId anId,
                               Topology topology,
                               Parameters currentParams,
                               ArbNodeInfo anInfo) {

        /* Check results of asking the RN for its params */
        LoadParameters remoteParams;
        if (anInfo.getParamsResult != null) {
            remoteParams = anInfo.getParamsResult;
        } else {
            final RemoteException re = anInfo.getParamsRemoteException;
            violations.add(new RMIFailed(anId, re, "getParams", showProgress,
                                         logger));
            return;
        }

        checkServiceParams(topology.get(anId).getStorageNodeId(), anId,
                           remoteParams, currentParams);
    }

    /**
     * See if this Admin replica's params match those held in the admin db
     * of the master Admin.
     */
    private void checkAdminParams(CommandServiceAPI cs,
                                  Parameters currentParams,
                                  AdminId targetAdminId,
                                  StorageNodeId hostSN) {

        final LoadParameters remoteParams;
        if (admin != null && targetAdminId.equals
            (admin.getParams().getAdminParams().getAdminId())) {

            /*
             * This is the local admin instance -- get the in-memory parameters
             * directly from the the local admin rather than RMI to allow unit
             * tests that use the Admin at a lower level than the RMI layer to
             * run correctly.
             */
            remoteParams = admin.getAllParams();
        } else {
            try {
                remoteParams = cs.getParams();
            } catch (RemoteException re) {
                violations.add(new RMIFailed(targetAdminId, re, "getParams",
                                             showProgress, logger));
                return;
            }
        }

        checkServiceParams(hostSN, targetAdminId, remoteParams, currentParams);
    }

    /**
     * Check that the service, SN, and global parameter values for the
     * in-memory parameters obtained from the specified running service match
     * the reference parameters from the admin DB.
     *
     * @param snId the ID of the SN hosting the service
     * @param rId the resource ID of the service
     * @param paramsToCheck service parameters to check
     * @param referenceParams admin DB parameters to check against
     * @return whether the parameters match
     */
    private boolean checkServiceParams(StorageNodeId snId,
                                       ResourceId rId,
                                       LoadParameters paramsToCheck,
                                       Parameters referenceParams) {
        return
            /* Service parameters */
            checkParams(rId, paramsToCheck, referenceParams.getMap(rId),
                        ParamMismatchLocation.MEMORY) &&
            /* Only consider SN parameters related to services */
            checkParams(snId, paramsToCheck,
                        referenceParams.get(snId).getMap(),
                        ParamMismatchLocation.MEMORY,
                        SNPCompareParamsFilter.getInstance()) &&
            /* Global parameters */
            checkParams(rId, paramsToCheck,
                        referenceParams.getGlobalParams().getMap(),
                        ParamMismatchLocation.MEMORY);
    }

    /**
     * Check if the parameters associated with the specified resource match the
     * ones in the reference parameters from the admin DB, ignoring
     * parameters that should typically be skipped in comparisons.
     *
     * @param rId the ID of the service
     * @param paramsToCheck service parameters to check
     * @param referenceParamMap admin DB parameters to check against
     * @param location where the parameters to check were found
     * @return whether the parameters match
     */
    private boolean checkParams(ResourceId rId,
                                LoadParameters paramsToCheck,
                                ParameterMap referenceParamMap,
                                ParamMismatchLocation location) {
        return checkParams(rId, paramsToCheck, referenceParamMap,
                           location, CompareParamsFilter.getInstance());
    }

    /**
     * Check if the parameters associated with the specified resource match the
     * ones in the reference parameters from the admin DB, using the specified
     * filter to determine which parameters to compare.
     *
     * @param rId the ID of the service
     * @param paramsToCheck service parameters to check
     * @param referenceParamMap admin DB parameters to check against
     * @param location where the parameters to check were found
     * @param filter returns the parameters to compare
     * @return whether the parameters match
     */
    private boolean checkParams(ResourceId rId,
                                LoadParameters paramsToCheck,
                                ParameterMap referenceParamMap,
                                ParamMismatchLocation location,
                                CompareParamsFilter filter) {
        switch (compareParams(paramsToCheck, referenceParamMap, filter)) {
        case NO_DIFFS:
            return true;
        case MISSING:
            violations.add(
                new ParamMismatch(rId,
                                  "Parameter collection " +
                                  referenceParamMap.getType() +
                                  " missing " + location.describe(),
                                  showProgress, logger));
            return false;
        default:
            violations.add(new ParamMismatch(rId,
                                             paramsToCheck.getMapByType(
                                                 referenceParamMap.getType()),
                                             referenceParamMap, location,
                                             showProgress, logger));
            if (referenceParamMap.getType().equals(ADMIN_TYPE)) {
                remedies.add(new UpdateAdminParamsRemedy(topoChecker,
                                                         (AdminId) rId));
            } else if (referenceParamMap.getType().equals(REPNODE_TYPE)) {
                remedies.add(new UpdateRNParamsRemedy(topoChecker,
                                                      (RepNodeId) rId));
            } else if (referenceParamMap.getType().equals(ARBNODE_TYPE)) {
                remedies.add(new UpdateANParamsRemedy(topoChecker,
                                                      (ArbNodeId) rId));
            }
            return false;
        }
    }

    /** Results of comparing parameters. */
    public enum CompareParamsResult {
        /** No differences */
        NO_DIFFS,
        /** The parameters were missing */
        MISSING,
        /** There were differences, but no restart is required */
        DIFFS,
        /** There were differences that require a restart */
        DIFFS_RESTART;
    }

    /**
     * Compare service, SN, and global parameters from in-memory values
     * obtained from the specified running service with reference parameters
     * from the admin DB.
     *
     * @param snId the ID of the SN hosting the service
     * @param rId the resource ID of the service
     * @param paramsToCheck service parameters to check
     * @param referenceParams admin DB parameters to compare with
     * @return the result of the comparison
     */
    public static CompareParamsResult compareServiceParams(
        StorageNodeId snId,
        ResourceId rId,
        LoadParameters paramsToCheck,
        Parameters referenceParams) {
        return
            compareServiceParams(snId,
                                 rId,
                                 paramsToCheck,
                                 referenceParams,
                                 CompareParamsFilter.getInstance(),
                                 SNPCompareParamsFilter.getInstance());
    }


    /**
     * Method used by the admin thread that corrects parameter inconsistencies.
     * Compare service, SN, and global parameters from in-memory values
     * obtained from the specified running service with reference parameters
     * from the admin DB. This method will return results if there are
     * differences for the GP_STORE_VERSION parameter.
     *
     * @param snId the ID of the SN hosting the service
     * @param rId the resource ID of the service
     * @param paramsToCheck service parameters to check
     * @param referenceParams admin DB parameters to compare with
     * @return the result of the comparison
     */
    public static CompareParamsResult compareServiceParamsPCon(
        StorageNodeId snId,
        ResourceId rId,
        LoadParameters paramsToCheck,
        Parameters referenceParams) {
        return
            compareServiceParams(snId,
                                 rId,
                                 paramsToCheck,
                                 referenceParams,
                                 PConCompareParamsFilter.getInstance(),
                                 SNPCompareParamsFilter.getInstance());
    }

    private static CompareParamsResult compareServiceParams(
        StorageNodeId snId,
        ResourceId rId,
        LoadParameters paramsToCheck,
        Parameters referenceParams,
        CompareParamsFilter serviceFilter,
        CompareParamsFilter snFilter) {
        return combineCompareParamsResults(
            /* Service parameters */
            compareParams(paramsToCheck, referenceParams.getMap(rId),
                          serviceFilter),
            /* Only consider SN parameters related to services */
            compareParams(paramsToCheck, referenceParams.get(snId).getMap(),
                          snFilter),
            /* Global parameters */
            compareParams(paramsToCheck,
                          referenceParams.getGlobalParams().getMap(),
                          serviceFilter));
    }

    /**
     * Compares parameters obtained from a remote service or configuration
     * parameters to reference parameters, ignoring parameters that should
     * typically be skipped in comparisons.  Checks the parameter map that
     * matches the name and type of the reference map.
     *
     * @param paramsToCheck the remote parameters to check
     * @param referenceParamMap the reference map to compare with
     * @return the result of the comparison
     */
    public static CompareParamsResult compareParams(
        LoadParameters paramsToCheck, ParameterMap referenceParamMap) {
        return compareParams(paramsToCheck, referenceParamMap,
                             CompareParamsFilter.getInstance());
    }


    /**
     * Compares parameters obtained from a remote service or configuration
     * parameters to reference parameters, ignoring parameters that should
     * typically be skipped in comparisons.  Checks the parameter map that
     * matches the name and type of the reference map.
     *
     * @param paramsToCheck the remote parameters to check
     * @param referenceParamMap the reference map to compare with
     * @return the result of the comparison
     */
    public static CompareParamsResult compareParamsPCon(
        LoadParameters paramsToCheck, ParameterMap referenceParamMap) {
        return compareParams(paramsToCheck, referenceParamMap,
                             PConCompareParamsFilter.getInstance());
    }

    /**
     * Compares parameters obtained from a remote service or configuration
     * parameters to reference parameters, using the specified filter to
     * determine which parameters to compare.  Checks the parameter map that
     * matches the name and type of the reference map.
     *
     * @param paramsToCheck the remote parameters to check
     * @param referenceParamMap the reference map to compare with
     * @param filter returns the parameters to compare
     * @return the result of the comparison
     */
    private static CompareParamsResult compareParams(
        LoadParameters paramsToCheck,
        ParameterMap referenceParamMap,
        CompareParamsFilter filter) {

        ParameterMap mapToCheck = paramsToCheck.getMap(
            referenceParamMap.getName(), referenceParamMap.getType());
        if (mapToCheck == null) {
            return CompareParamsResult.MISSING;
        }
        referenceParamMap = filter.filter(referenceParamMap);
        mapToCheck = filter.filter(mapToCheck);
        if (referenceParamMap.equals(mapToCheck)) {
            return CompareParamsResult.NO_DIFFS;
        }
        if (referenceParamMap.hasRestartRequiredDiff(mapToCheck)) {
            return CompareParamsResult.DIFFS_RESTART;
        }
        return CompareParamsResult.DIFFS;
    }

    /**
     * Combines the results of several parameter comparisons into a single
     * result.  If any parameters were missing, returns MISSING.  Otherwise,
     * returns DIFFS_RESTART if any different parameters require a restart,
     * DIFFS if there were only differences that do not require a restart, and
     * NO_DIFFS otherwise.
     */
    public static CompareParamsResult combineCompareParamsResults(
        CompareParamsResult... results) {
        CompareParamsResult combinedResult = CompareParamsResult.NO_DIFFS;
        for (CompareParamsResult result : results) {
            switch (result) {
            case NO_DIFFS:
                continue;
            case MISSING:
                return CompareParamsResult.MISSING;
            case DIFFS:
                if (combinedResult == CompareParamsResult.NO_DIFFS) {
                    combinedResult = CompareParamsResult.DIFFS;
                }
                continue;
            case DIFFS_RESTART:
                combinedResult = CompareParamsResult.DIFFS_RESTART;
                continue;
            }
        }
        return combinedResult;
    }

    /**
     * Filter to return the parts of a parameter map that should be considered
     * by default when performing a comparison.  This class skips parameters
     * specified by {@link ParameterState#skipParams}, which includes the
     * disabled parameter.
     */
    static class CompareParamsFilter {
         static final private
             CompareParamsFilter INSTANCE = new CompareParamsFilter();
         static CompareParamsFilter getInstance() {
             return INSTANCE;
         }
         ParameterMap filter(ParameterMap map) {
            return map.filter(ParameterState.skipParams, false);
        }
    }

    /**
     * Filter to return the parts of a parameter map that should be considered
     * by default when performing a comparison by the Parameter Consistency
     * admin thread.  This class skips parameters
     * specified by {@link ParameterState#skipParamsPCon}, which includes the
     * disabled parameter.
     */
     static class PConCompareParamsFilter extends CompareParamsFilter {
         static final private PConCompareParamsFilter INSTANCE =
             new PConCompareParamsFilter();
         static PConCompareParamsFilter getInstance() {
             return INSTANCE;
         }
         @Override
        ParameterMap filter(ParameterMap map) {
            return map.filter(ParameterState.skipParamsPCon, false);
        }
    }

    /**
     * Include only StorageNodeParams parameters that are used by services for
     * their own configuration.
     */
    static class SNPCompareParamsFilter extends CompareParamsFilter {
        static final private  SNPCompareParamsFilter INSTANCE =
            new SNPCompareParamsFilter();
        static SNPCompareParamsFilter getInstance() {
            return INSTANCE;
        }
        @Override
        ParameterMap filter(ParameterMap map) {
            return map.filter(ParameterState.serviceParams, true);
        }
    }

    /**
     * Classes to record violations.
     */
    public interface Problem {
        public ResourceId getResourceId();
    }

    /**
     * Report a service that is stopped either because it is disabled or for
     * some other reason.
     */
    public static class ServiceStopped implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final ResourceId rId;
        private final StorageNodeId snId;
        private final boolean isDisabled;

        ServiceStopped(ResourceId rId,
                       StorageNodeId snId,
                       boolean showProgress,
                       Logger logger,
                       boolean isDisabled) {
            this.rId = rId;
            this.snId = snId;
            this.isDisabled = isDisabled;
            recordProgress(showProgress, logger, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rId;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(rId).append(" on ").append(snId);
            if (isDisabled) {
                sb.append(" was previously stopped and");
            }
            sb.append(" is not running. Consider restarting it with ");
            sb.append("'plan start-service'.");
            return sb.toString();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((rId == null) ? 0 : rId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ServiceStopped)) {
                return false;
            }
            ServiceStopped other = (ServiceStopped) obj;
            if (rId == null) {
                if (other.rId != null) {
                    return false;
                }
            } else if (!rId.equals(other.rId)) {
                return false;
            }
            return true;
        }
    }

    /**
     * Report a RN or AN that is not yet a member of the replication group.
     */
    public static class NotInGroup implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final ResourceId rId;
        private final StorageNodeId snId;

        NotInGroup(ResourceId rId,
                   StorageNodeId snId,
                   boolean showProgress,
                   Logger logger) {
            this.rId = rId;
            this.snId = snId;
            recordProgress(showProgress, logger, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rId;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(rId).append(" on ").append(snId);
            sb.append(" is running but is not a member of ");
            sb.append("the replication group.");
            return sb.toString();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((rId == null) ? 0 : rId.hashCode());
            result = prime * result + ((snId == null) ? 0 : snId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof NotInGroup)) {
                return false;
            }
            NotInGroup other = (NotInGroup) obj;
            if (rId == null) {
                if (other.rId != null) {
                    return false;
                }
            } else if (!rId.equals(other.rId)) {
                return false;
            }
            if (snId == null) {
                if (other.snId != null) {
                    return false;
                }
            } else if (!snId.equals(other.snId)) {
                return false;
            }
            return true;
        }
    }

    public static class StatusNotRight implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final ResourceId rId;
        private final ServiceStatus expected;
        private final ServiceStatus current;

        StatusNotRight(ResourceId rId,
                       ServiceStatus expected,
                       ServiceStatus current,
                       boolean showProgress,
                       Logger logger) {
            this.rId = rId;
            this.expected = expected;
            this.current = current;
            recordProgress(showProgress, logger, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rId;
        }

        public ServiceStatus getExpectedStatus() {
            return expected;
        }

        public ServiceStatus getCurrentStatus() {
            return current;
        }

        @Override
        public String toString() {
            return "For resource " + getResourceId() +
                " expected status " + expected + " but was " + current;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((current == null) ? 0 : current.hashCode());
            result = prime * result
                    + ((expected == null) ? 0 : expected.hashCode());
            result = prime * result + ((rId == null) ? 0 : rId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof StatusNotRight)) {
                return false;
            }
            StatusNotRight other = (StatusNotRight) obj;
            if (current != other.current) {
                return false;
            }
            if (expected != other.expected) {
                return false;
            }
            if (rId == null) {
                if (other.rId != null) {
                    return false;
                }
            } else if (!rId.equals(other.rId)) {
                return false;
            }
            return true;
        }
    }

    public static class RMIFailed implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final ResourceId rId;
        private final String desc;

        RMIFailed(ResourceId rId, RemoteException e, String methodName,
                  boolean showProgress, Logger logger) {
            this.rId = rId;
            desc = methodName + " failed for " + rId + " : " + e.getMessage();
            recordProgress(showProgress, logger, this);
        }

        RMIFailed(ResourceId rId, NotBoundException e, boolean showProgress,
                  Logger logger) {
            this.rId = rId;
            desc = "No RMI service for " + rId + ": service name=" +
                e.getMessage();
            recordProgress(showProgress, logger, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rId;
        }

        @Override
        public String toString() {
            return desc;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((desc == null) ? 0 : desc.hashCode());
            result = prime * result + ((rId == null) ? 0 : rId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof RMIFailed)) {
                return false;
            }
            RMIFailed other = (RMIFailed) obj;
            if (desc == null) {
                if (other.desc != null) {
                    return false;
                }
            } else if (!desc.equals(other.desc)) {
                return false;
            }
            if (rId == null) {
                if (other.rId != null) {
                    return false;
                }
            } else if (!rId.equals(other.rId)) {
                return false;
            }
            return true;
        }
    }

    /** Describes where a parameter mismatch was detected. */
    enum ParamMismatchLocation {
        CONFIG("from configuration for service"),
        MEMORY("on service");
        private final String description;
        private ParamMismatchLocation(String description) {
            this.description = description;
        }
        public String describe() { return description; }
    }

    public static class ParamMismatch implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final ResourceId rId;
        private final String mismatch;

        private static String getMismatchMessage(
            ResourceId resourceId,
            ParameterMap remoteCopy,
            ParameterMap adminCopy,
            ParamMismatchLocation location) {

            final ParameterMap onAdminButNotRemote =
                remoteCopy.diff(adminCopy, false);
            final ParameterMap onRemoteButNotAdmin =
                adminCopy.diff(remoteCopy, false);
            final StringBuilder sb = new StringBuilder();
            if (!onAdminButNotRemote.isEmpty()) {
                sb.append("  Parameters in Admin database but not ")
                    .append(location.describe()).append(" ")
                    .append(resourceId)
                    .append(": ")
                    .append(onAdminButNotRemote.showContents());
            }
            if (!onRemoteButNotAdmin.isEmpty()) {
                if (!onAdminButNotRemote.isEmpty()) {
                    sb.append("\n");
                }
                sb.append("  Parameters ")
                    .append(location.describe()).append(" ")
                    .append(resourceId)
                    .append(" but not in Admin database: ")
                    .append(onRemoteButNotAdmin.showContents());
            }
            return sb.toString();
        }

        ParamMismatch(ResourceId rId, ParameterMap remoteCopy,
                      ParameterMap adminCopy, ParamMismatchLocation location,
                      boolean showProgress, Logger logger) {
            this(rId, getMismatchMessage(rId, remoteCopy, adminCopy, location),
                 showProgress, logger);
        }

        ParamMismatch(ResourceId rId, String msg, boolean showProgress,
                      Logger logger) {

            this.rId = rId;
            mismatch = msg;
            recordProgress(showProgress, logger, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rId;
        }

        @Override
        public String toString() {
            return "Mismatch between metadata in admin service and " + rId +
                ": " + mismatch;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((mismatch == null) ? 0 : mismatch.hashCode());
            result = prime * result + ((rId == null) ? 0 : rId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ParamMismatch)) {
                return false;
            }
            ParamMismatch other = (ParamMismatch) obj;
            if (mismatch == null) {
                if (other.mismatch != null) {
                    return false;
                }
            } else if (!mismatch.equals(other.mismatch)) {
                return false;
            }
            if (rId == null) {
                if (other.rId != null) {
                    return false;
                }
            } else if (!rId.equals(other.rId)) {
                return false;
            }
            return true;
        }
    }

    public static class VersionDifference implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final StorageNodeId snId;
        private final String desc;

        VersionDifference(StorageNodeId snId,
                          KVVersion snVersion,
                          boolean showProgress,
                          Logger logger) {
            this.snId = snId;
            desc = "Admin service version is " + KVVersion.CURRENT_VERSION +
                " but storage node version is " + snVersion;
            recordProgress(showProgress, logger, this);
        }

        @Override
        public ResourceId getResourceId() {
            return snId;
        }

        @Override
        public String toString() {
            return desc;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((desc == null) ? 0 : desc.hashCode());
            result = prime * result + ((snId == null) ? 0 : snId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof VersionDifference)) {
                return false;
            }
            VersionDifference other = (VersionDifference) obj;
            if (desc == null) {
                if (other.desc != null) {
                    return false;
                }
            } else if (!desc.equals(other.desc)) {
                return false;
            }
            if (snId == null) {
                if (other.snId != null) {
                    return false;
                }
            } else if (!snId.equals(other.snId)) {
                return false;
            }
            return true;
        }
    }

    public static class StoreVersionHigher implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final StorageNodeId snId;
        private final String desc;

        StoreVersionHigher(StorageNodeId snId,
                          KVVersion storeVersion,
                          KVVersion snVersion,
                          boolean showProgress,
                          Logger logger) {
            this.snId = snId;
            desc = "KVStore version is " + storeVersion +
                " is greater than the storage node " + snId +
                " version " + snVersion;
            recordProgress(showProgress, logger, this);
        }

        @Override
        public ResourceId getResourceId() {
            return snId;
        }

        @Override
        public String toString() {
            return desc;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((desc == null) ? 0 : desc.hashCode());
            result = prime * result + ((snId == null) ? 0 : snId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof StoreVersionHigher)) {
                return false;
            }
            StoreVersionHigher other = (StoreVersionHigher) obj;
            if (desc == null) {
                if (other.desc != null) {
                    return false;
                }
            } else if (!desc.equals(other.desc)) {
                return false;
            }
            if (snId == null) {
                if (other.snId != null) {
                    return false;
                }
            } else if (!snId.equals(other.snId)) {
                return false;
            }
            return true;
        }
    }

    public static class UpgradeNeeded implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final ResourceId rId;
        private final String desc;

        UpgradeNeeded(ResourceId rId,
                      KVVersion rVersion,
                      KVVersion targetVersion,
                      boolean showProgress,
                      Logger logger) {
            this.rId = rId;
            desc = "Node needs to be upgraded from " +
                   rVersion.getNumericVersionString() +
                   " to version " +
                   targetVersion.getNumericVersionString() +
                   " or newer";

            recordProgress(showProgress, logger, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rId;
        }

        @Override
        public String toString() {
            return desc;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((desc == null) ? 0 : desc.hashCode());
            result = prime * result + ((rId == null) ? 0 : rId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof UpgradeNeeded)) {
                return false;
            }
            UpgradeNeeded other = (UpgradeNeeded) obj;
            if (desc == null) {
                if (other.desc != null) {
                    return false;
                }
            } else if (!desc.equals(other.desc)) {
                return false;
            }
            if (rId == null) {
                if (other.rId != null) {
                    return false;
                }
            } else if (!rId.equals(other.rId)) {
                return false;
            }
            return true;
        }
    }

    public static class BadDowngrade implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final ResourceId rId;
        private final String desc;

        BadDowngrade(ResourceId rId,
                     KVVersion rVersion,
                     KVVersion targetVersion,
                     boolean showProgress,
                     Logger logger) {
            this.rId = rId;
            desc = "Node cannot be changed to use version " +
                targetVersion.getNumericVersionString() +
                " because that would require a downgrade from newer version " +
                rVersion.getNumericVersionString() +
                " it is currently running, and that downgrade is not" +
                " supported.";

            recordProgress(showProgress, logger, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rId;
        }

        @Override
        public String toString() {
            return desc;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((desc == null) ? 0 : desc.hashCode());
            result = prime * result + ((rId == null) ? 0 : rId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof BadDowngrade)) {
                return false;
            }
            BadDowngrade other = (BadDowngrade) obj;
            if (desc == null) {
                if (other.desc != null) {
                    return false;
                }
            } else if (!desc.equals(other.desc)) {
                return false;
            }
            if (rId == null) {
                if (other.rId != null) {
                    return false;
                }
            } else if (!rId.equals(other.rId)) {
                return false;
            }
            return true;
        }
    }

    /**
     * The problem that configuration violate enforced security requirement.
     * This problem is only built on client-side, Admin CLI currently.
     */
    public static class SecurityViolation implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final ResourceId rId;
        private final String desc;

        public SecurityViolation(ResourceId rId, String desc) {
            this(rId, desc, false, null);
        }

        /* Not used currently */
        SecurityViolation(ResourceId rId,
                          String desc,
                          boolean showProgress,
                          Logger logger) {
            this.rId = rId;
            this.desc = desc;
            recordProgress(showProgress, logger, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rId;
        }

        @Override
        public String toString() {
            return desc;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((desc == null) ? 0 : desc.hashCode());
            result = prime * result + ((rId == null) ? 0 : rId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof SecurityViolation)) {
                return false;
            }
            SecurityViolation other = (SecurityViolation) obj;
            if (desc == null) {
                if (other.desc != null) {
                    return false;
                }
            } else if (!desc.equals(other.desc)) {
                return false;
            }
            if (rId == null) {
                if (other.rId != null) {
                    return false;
                }
            } else if (!rId.equals(other.rId)) {
                return false;
            }
            return true;
        }
    }

    public static class StorageDirectoryProblem implements Problem,
                                                           Serializable {
        private static final long serialVersionUID = 1L;
        private final StorageNodeId snId;
        private final String desc;

        private StorageDirectoryProblem(StorageNodeId snId,
                                        String directory,
                                        String msg,
                                        boolean showProgress,
                                        Logger logger) {
            assert snId != null;
            this.snId = snId;
            desc = "Problem with directory " + directory + " on " + snId +
                   ", " + msg + ".";
            recordProgress(showProgress, logger, this);
        }

        @Override
        public ResourceId getResourceId() {
            return snId;
        }

        @Override
        public String toString() {
            return desc;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof StorageDirectoryProblem)) {
                return false;
            }
            final StorageDirectoryProblem other = (StorageDirectoryProblem)obj;
            if (snId != other.snId) {
                return false;
            }
            return desc.equals(other.desc);
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 97 * hash + Objects.hashCode(this.snId);
            hash = 97 * hash + Objects.hashCode(this.desc);
            return hash;
        }
    }

    public static class AvailableStorageExceeded implements Problem,
                                                            Serializable {
    	private static final long serialVersionUID = 1L;
    	private final RepNodeId rnId;
    	private final String desc;
    	private final String path;

        private AvailableStorageExceeded(RepNodeId rnId,
                                         String storageDirPath,
                                         String staticStorageSize,
                                         String availableStorageSize,
                                         boolean showProgress,
                                         Logger logger) {
            assert rnId != null;
            this.rnId = rnId;
            this.path = storageDirPath;

            desc = "Storage directory on " + rnId +
                   " has exceeded storage size" +
                   " [" + path + " size: " + staticStorageSize +
                   " available: " + availableStorageSize + "]";
            recordProgress(showProgress, logger, this);
    	}

        @Override
        public ResourceId getResourceId() {
            return rnId;
        }

        @Override
        public String toString() {
            return desc;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof AvailableStorageExceeded)) {
                return false;
            }
            final AvailableStorageExceeded other =
                (AvailableStorageExceeded)obj;
            if (rnId != other.rnId) {
                return false;
            }
            return desc.equals(other.desc);
        }

    	@Override
    	public int hashCode() {
    	    int hash = 3;
    	    hash = 97 * hash + Objects.hashCode(this.rnId);
    	    hash = 97 * hash + Objects.hashCode(this.desc);
    	    return hash;
    	}
    }

    public static class AvailableStorageLow implements Problem,
                                                       Serializable {
        private static final long serialVersionUID = 1L;
        private final RepNodeId rnId;
        private final String desc;
        private final String path;

        private AvailableStorageLow(RepNodeId rnId,
                                    String storageDirPath,
                                    String staticStorageSize,
                                    String availableStorageSize,
                                    boolean showProgress,
                                    Logger logger) {
            assert rnId != null;
            this.rnId = rnId;
            this.path = storageDirPath;
            desc = "Storage directory on " + rnId +
                    " is running low" +
                    " [" + path + " size: " + staticStorageSize +
                    " available: " + availableStorageSize + "]";
            recordProgress(showProgress, logger, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rnId;
        }

        @Override
        public String toString() {
            return desc;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof AvailableStorageLow)) {
                return false;
            }
            final AvailableStorageLow other =
               (AvailableStorageLow)obj;
            if (rnId != other.rnId) {
                return false;
            }
            return desc.equals(other.desc);
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 97 * hash + Objects.hashCode(this.rnId);
            hash = 97 * hash + Objects.hashCode(this.desc);
            return hash;
        }
    }

    /**
     * Not all types of request are enabled on this node.
     */
    public static class RequestsDisabled implements Problem, Serializable {
        private static final long serialVersionUID = 1L;
        private final ResourceId rId;
        private final String enabledRequestType;

        public RequestsDisabled(ResourceId rId, String desc) {
            this(rId, desc, false, null);
        }

        RequestsDisabled(ResourceId rId,
                         String enabledRequestType,
                         boolean showProgress,
                         Logger logger) {
            ObjectUtil.checkNull("resourceId", rId);
            ObjectUtil.checkNull("enabledRequestType", enabledRequestType);
            this.rId = rId;
            this.enabledRequestType = enabledRequestType;
            recordProgress(showProgress, logger, this);
        }

        @Override
        public ResourceId getResourceId() {
            return rId;
        }

        @Override
        public String toString() {
            return "Not all request are enabled on " + rId +
                ", enabled request type is " + enabledRequestType;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + enabledRequestType.hashCode();
            result = prime * result + rId.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof RequestsDisabled)) {
                return false;
            }
            RequestsDisabled other = (RequestsDisabled) obj;
            if (!enabledRequestType.equals(other.enabledRequestType)) {
                return false;
            }
            if (!rId.equals(other.rId)) {
                return false;
            }
            return true;
        }
    }

    private String getOperation() {
        StringBuilder operation = new StringBuilder("verify");
        switch(verifyType) {
        case TOPOLOGY:
            operation.append(" configuration");
            break;
        case UPGRADE:
            operation.append(" upgrade");
            break;
        case PREREQUISITE:
            operation.append(" prerequisite");
            break;
        default:
            break;
        }
        if(json) {
            operation.append(" -json");
        }
        if(!showProgress) {
            operation.append(" -silent");
        }
        return operation.toString();
    }

    /*
     * Generate the verify command results based on following rules:
     * 1. If there isn't any violation, return CommandSucceeds
     * 2. If there are both violation and remedy, return CommandFails which
     * error code is 5400 and cleanup job is TOPO_REPAIR.
     * 3. If there is RMIFailed or StatusNotRight violation, we may need retry
     * the verify command again later. Then we will return CommandFails which
     * error code is 5300 and no cleanup job.
     * 4. Otherwise we will return CommandFails which error code is 5200.
     */
    private CommandResult getCommandResult() {
        final boolean hasRemedy = !remedies.isEmpty();
        if (violations == null || violations.isEmpty()) {
            if (hasRemedy) {
                return new CommandFails("variable hadRemedy is true although"
                    + " there are no violations",
                    ErrorMessage.NOSQL_5500, CommandResult.NO_CLEANUP_JOBS);
            }
            return new CommandSucceeds(null /* return value */);
        }
        if (hasRemedy) {
            return new CommandFails("There are violations. Please use "
                    + Arrays.toString(CommandResult.TOPO_REPAIR) +
                    " to recover and try again later",
                ErrorMessage.NOSQL_5400, CommandResult.TOPO_REPAIR);
        }
        for (Problem p : violations) {
            if (p instanceof RMIFailed) {
                return new CommandFails("Connect failed, please try again.",
                                        ErrorMessage.NOSQL_5300,
                                        CommandResult.NO_CLEANUP_JOBS);
            }
            if (p instanceof StatusNotRight) {
                final StatusNotRight stNotRight = (StatusNotRight) p;
                final ServiceStatus current = stNotRight.getCurrentStatus();
                final ServiceStatus expected = stNotRight.getExpectedStatus();

                if (expected == ServiceStatus.RUNNING &&
                    (current == ServiceStatus.STARTING ||
                     current == ServiceStatus.ERROR_RESTARTING)) {
                    return new CommandFails(
                        "Waiting for service status, try later",
                        ErrorMessage.NOSQL_5300,
                        CommandResult.NO_CLEANUP_JOBS);
                }
            }
        }
        return new CommandFails("There are violations.",
                                ErrorMessage.NOSQL_5200,
                                CommandResult.NO_CLEANUP_JOBS);
    }

    private String getOutput() {
        if (json) {
            return jsonResults();
        }
        final StringBuilder sb = new StringBuilder();
        sb.append("Verify: starting verification of ")
            .append(PingDisplay.displayTopologyOverview(jsonTop)).append(eol);
        sb.append(displayStorewideLogName(jsonTop)).append(eol);
        if (showProgress) {
            if (verifyType == VerifyType.TOPOLOGY) {
                sb.append("Verify: ")
                    .append(PingDisplay.displayShardOverview(jsonTop))
                    .append(eol);
                sb.append("Verify: ")
                    .append(PingDisplay.displayAdminOverview(jsonTop))
                    .append(eol);
                for (JsonNode jsonZone : getArray(jsonTop, "zoneStatus")) {
                    sb.append("Verify: ")
                        .append(PingDisplay.displayZoneOverview(jsonZone))
                        .append(eol);
                }
            }
            for (JsonNode jsonSN : getArray(jsonTop, "snStatus")) {
                final String snId = getAsText(jsonSN, "resourceId");
                if (verifyType == VerifyType.TOPOLOGY) {
                    sb.append("Verify: == checking storage node ")
                        .append(snId).append(" ==").append(eol);
                }
                showProgressProblems(snId, sb);
                sb.append("Verify")
                    .append((verifyType == VerifyType.TOPOLOGY) ? "" :
                            (" " + verifyType))
                    .append(": ")
                    .append(PingDisplay.displayStorageNode(jsonSN)).append(eol);
                final JsonNode jsonAdmin = jsonSN.get("adminStatus");
                if (jsonAdmin != null) {
                    showProgressProblems(
                        getAsText(jsonAdmin, "resourceId"), sb);
                    sb.append("Verify: ")
                        .append(PingDisplay.displayAdmin(jsonAdmin)).
                        append(eol);
                }
                for (JsonNode jsonRN : getArray(jsonSN, "rnStatus")) {
                    showProgressProblems(
                        getAsText(jsonRN, "resourceId"), sb);
                    sb.append("Verify: ")
                        .append(PingDisplay.displayRepNode(jsonRN)).append(eol);
                }
                for (JsonNode jsonAN : getArray(jsonSN, "anStatus")) {
                    showProgressProblems(
                        getAsText(jsonAN, "resourceId"), sb);
                    sb.append("Verify: ")
                        .append(PingDisplay.displayArbNode(jsonAN)).append(eol);
                }
            }
            sb.append(eol);
        }
        return appendViolations(sb).toString();
    }

    private String jsonResults() {
        String operation = getOperation();
        CommandResult result = getCommandResult();
        CommandJsonUtils.updateNodeWithResult(jsonTop, operation,
		                                      result);
		return CommandJsonUtils.toJsonString(jsonTop);
    }

    private StringBuilder appendViolations(StringBuilder sb) {
        final int numViolations = violations.size();
        final int numWarnings = warnings.size();
        if ((numViolations + numWarnings) == 0) {
            sb.append("Verification complete, no violations.");
            return sb;
        }

        sb.append("Verification complete, ").append(numViolations);
        sb.append((numViolations == 1) ? " violation, " : " violations, ");
        sb.append(numWarnings);
        sb.append((numWarnings == 1) ? " note" : " notes");
        sb.append(" found.").append(eol);

        for (JsonNode jsonProblem : getArray(jsonTop, "violations")) {
            sb.append("Verification violation: ")
                .append(displayProblem(jsonProblem)).append(eol);
        }
        for (JsonNode jsonProblem : getArray(jsonTop, "warnings")) {
            sb.append("Verification note: ")
                .append(displayProblem(jsonProblem)).append(eol);
        }
        return sb;
    }

    private void showProgressProblems(String resourceId, StringBuilder sb) {
        if (showProgress) {
            for (Problem problem : violations) {
                if (resourceId.equals(problem.getResourceId().toString())) {
                    sb.append("Verify:         ")
                        .append(problem.getResourceId()).append(": ")
                        .append(problem).append(eol);
                }
            }
            for (Problem problem : warnings) {
                if (resourceId.equals(problem.getResourceId().toString())) {
                    sb.append("Verify:         ")
                        .append(problem.getResourceId()).append(": ")
                        .append(problem).append(eol);
                }
            }
        }
    }

    private static void recordProgress(boolean showProgress,
                                       Logger logger,
                                       Problem problem) {
        if (showProgress) {
            String msg = "Verify:         " + problem.getResourceId() + ": " +
                          problem;
            logger.info(msg);
        }
    }

    public VerifyResults getResults() {
        return new VerifyResults(getOutput(), violations, warnings);
    }

    public TopologyCheck getTopoChecker() {
        return topoChecker;
    }

    /**
     * Returns the list of remedies that should be applied, in order, to repair
     * the configuration problems that were found.  The master Admin ID is used
     * to perform remedies related to the master admin last so that the most
     * progress can be made before potentially needing to restart the process
     * after transferring the master.
     *
     * @param masterAdminId the master Admin ID
     * @return the list of remedies
     */
    public List<Remedy> getRemedies(AdminId masterAdminId) {
        return remedies.getRemedies(masterAdminId);
    }

    /*
     * TODO: Consider adding a more general facility for ordering remedies
     * if we have to add another ordering criterion.
     */
    /**
     * Organize remedies by type, because CreateRNRemedy instances have to be
     * done first and RemoveRNRemedy ones have to be done last.
     */
    private static class Remedies {
        private final List<Remedy> creates = new ArrayList<>();
        private final List<Remedy> removes = new ArrayList<>();
        private final List<Remedy> other = new ArrayList<>();

        void clear() {
            creates.clear();
            removes.clear();
            other.clear();
        }

        void add(Remedy remedy) {
            if (remedy instanceof CreateRNRemedy) {
                creates.add(remedy);
            } else if (remedy instanceof RemoveRNRemedy) {
                removes.add(remedy);
            } else {
                other.add(remedy);
            }
        }

        List<Remedy> getRemedies(AdminId masterAdminId) {
            checkNull("masterAdminId", masterAdminId);
            final List<Remedy> r = new ArrayList<>();
            collectRemedies(r, creates, masterAdminId);
            collectRemedies(r, other, masterAdminId);
            collectRemedies(r, removes, masterAdminId);
            return r;
        }

        private void collectRemedies(List<Remedy> result,
                                     List<Remedy> add,
                                     AdminId masterAdminId) {
            boolean foundMasterAdmin = false;
            for (final Remedy remedy : add) {
                if (masterAdminId.equals(remedy.getResourceId())) {
                    foundMasterAdmin = true;
                } else {
                    result.add(remedy);
                }
            }
            if (foundMasterAdmin) {
                for (final Remedy remedy : add) {
                    if (masterAdminId.equals(remedy.getResourceId())) {
                        result.add(remedy);
                    }
                }
            }
        }

        boolean remedyExists(ResourceId resourceId) {
            for (Remedy r : creates) {
                if (r.getResourceId().equals(resourceId)) {
                    return true;
                }
            }

            for (Remedy r : other) {
                if (r.getResourceId().equals(resourceId)) {
                    return true;
                }
            }

            for (Remedy r : removes) {
                if (r.getResourceId().equals(resourceId)) {
                    return true;
                }
            }
            return false;
        }

        boolean isEmpty() {
            return creates.isEmpty() && removes.isEmpty() && other.isEmpty();
        }
    }

    private KVVersion getVersion(String versionString) {
        KVVersion retVal = null;
        try {
            retVal = KVVersion.parseVersion(versionString);
        } catch (Exception e) {

        }
        return retVal;
    }
}
