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

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.net.InetSocketAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.TopologyCheckUtils.SNServices;
import oracle.kv.impl.admin.VerifyConfiguration.CompareParamsResult;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.GroupNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.PortTracker;
import oracle.kv.impl.admin.plan.task.ChangeServiceAddresses;
import oracle.kv.impl.admin.plan.task.RelocateAN;
import oracle.kv.impl.admin.plan.task.RelocateRN;
import oracle.kv.impl.admin.plan.task.Task.State;
import oracle.kv.impl.admin.plan.task.UpdateAdminParams;
import oracle.kv.impl.admin.plan.task.UpdateRepNodeParams;
import oracle.kv.impl.admin.plan.task.Utils;
import oracle.kv.impl.admin.topo.StorageDirectory;
import oracle.kv.impl.admin.topo.Validations.InsufficientRNs;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.tif.TextIndexFeederManager;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.util.ErrorMessage;

import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.utilint.HostPortPair;

/**
 * TopologyCheck is used by VerifyConfiguration, by RepairPlan and by other
 * plan tasks that modify topology. It checks that the three representations of
 * layout metadata (the topology, the remote SN config files, and the JEHA
 * group db) are consistent with each other.
 *
 * Both RNs, ANs and Admins are checked.
 *
 * The class provides repair methods that can fix some inconsistencies.
 */
public class TopologyCheck {

    /* TODO: use kvstore param */
    private static final int JE_HA_TIMEOUT_MS = 5000;

    /*
     * The remedies for RN topology mismatches are embodied in this static map.
     * Setup the map of remedies for detected problems.
     */
    private static final Map<RNLocationInput, RNRemedyFactory> RN_REMEDIES =
        new HashMap<>();
    static {
        initRNRemedies();
    }

    private static final Map<RNLocationInput, RNRemedyFactory> AN_REMEDIES =
        new HashMap<>();
    static {
        initANRemedies();
    }

    /*
     * For efficiency, TopoChecker saves the config.xml and information
     * derived from the topology for use across checking various services.
     */

    /* Information about hosted services, as derived from the SN's config.xml*/
    private final Map<StorageNodeId, SNServices> snRemoteParams;

    /*
     * A collection of topology resource ids, grouped by hostingSN, generated
     * from the AdminDb's topology and params
     */
    private final Map<StorageNodeId, SNServices> topoGroupedBySN;

    /**
     * Component doing the check.
     */
    private final String who;

    private final Logger logger;

    /**
     * The topology and params are used at construction time to reorganize
     * topology information for mismatch detection.
     */
    public TopologyCheck(String who,
                         Logger logger,
                         Topology topo,
                         Parameters params) {
        this.who = who + " TopologyCheck";
        this.logger = logger;
        snRemoteParams = new HashMap<>();
        topoGroupedBySN = TopologyCheckUtils.groupServicesBySN(topo, params);
    }

    /**
     * A remote config.xml was obtained from a SN. Save for future use.
     */
    void saveSNRemoteParams(StorageNodeId snId, LoadParameters lp) {
        snRemoteParams.put(snId, processRemoteInfo(snId, lp));
    }

    /**
     * Return the union of RNs that are referenced by the SN's config.xml and
     * those referenced by the AdminDB/topology.
     */
    Set<RepNodeId> getPossibleRNs(StorageNodeId snId) {
        Set<RepNodeId> rnsToCheck = new HashSet<>();
        rnsToCheck.addAll(snRemoteParams.get(snId).getAllRNs());
        rnsToCheck.addAll(topoGroupedBySN.get(snId).getAllRepNodeIds());
        return rnsToCheck;
    }

    /**
     * Return the union of ANs that are referenced by the SN's config.xml and
     * those referenced by the AdminDB/topology.
     */
    public Set<ArbNodeId> getPossibleANs(StorageNodeId snId) {
        Set<ArbNodeId> ansToCheck = new HashSet<>();
        ansToCheck.addAll(snRemoteParams.get(snId).getAllARBs());
        ansToCheck.addAll(topoGroupedBySN.get(snId).getAllARBs());
        return ansToCheck;
    }

    /**
     * Checks whether the specified arbiter node should be on this SN, and
     * optionally should be enabled. For further details see comment for
     * the general checkLocation.
     */
    public Remedy checkLocation(Admin admin,
                                StorageNodeId snId,
                                ArbNodeId resId,
                                boolean calledByDeployNewRN,
                                boolean makeRNEnabled,
                                StorageNodeId oldSNId)
        throws RemoteException, NotBoundException {
        return checkLocation(admin,
                                snId,
                                resId,
                                calledByDeployNewRN,
                                makeRNEnabled,
                                oldSNId,
                                null   /* storageDirectory */ );
    }

    /**
     * Checks whether the specified replication or arbiter node should be on
     * this SN, and optionally should be enabled. Recommend a fix if
     * - the RN's location information is inconsistent, or
     * - the RN's location information is consistent, but the RN needs to be
     * re-enabled, and this optional behavior is requested.
     *
     * Assumes that the checker has been constructed with the most up to date
     * topology and params. If SN remote config params are loaded with
     * saveSNRemoteParams, assume that's also up to date.
     *
     * @param admin the admin
     * @param snId the ID of the storage node to check
     * @param resId the ID of the RN or AN to check
     * @param calledByDeployNewRN if true, the caller was deploying a new RN,
     * so if we don't see the RN in the JE HA info we know it does not appear
     * anywhere
     * @param makeRNEnabled if true, return a remedy if the RN is not enabled,
     * otherwise ignore this issue
     * @param oldSNId if not null, provides the ID of the SN that used to host
     * this RN; used to find the original SN when reverting an RN relocation
     * @param storageDirectory if not null, provides the storage directory
     * for the RN if fixing things up for an RN that has been partially
     * relocated
     * @throws NotBoundException
     * @throws RemoteException
     */
    public Remedy checkLocation(Admin admin,
                                StorageNodeId snId,
                                ResourceId resId,
                                boolean calledByDeployNewRN,
                                boolean makeRNEnabled,
                                StorageNodeId oldSNId,
                                StorageDirectory storageDirectory)
       throws RemoteException, NotBoundException {

        final boolean isRN;
        if (resId.getType().isRepNode()) {
            isRN = true;
        } else if (resId.getType().isArbNode()) {
            isRN = false;
        } else {
            throw new IllegalArgumentException("Unexpected resource ID: " +
                                               resId);
        }

        /*
         * Check that the topo, JE HA, and SN are consistent about the RN's
         * location. Assemble our three inputs:
         *  a. topo
         *  b. remote SN config
         *  c. JEHA groupDB
         */

        /* a. Get the topo's viewpoint */
        TOPO_STATUS topoStatus = TOPO_STATUS.GONE;
        SNServices servicesOnThisSN = topoGroupedBySN.get(snId);
        if (servicesOnThisSN != null) {
            if (isRN)  {
                if (servicesOnThisSN.getAllRepNodeIds().contains(resId)) {
                    topoStatus = TOPO_STATUS.HERE;
                }
            } else {
                if (servicesOnThisSN.getAllARBs().contains(resId)) {
                    topoStatus = TOPO_STATUS.HERE;
                }
            }
        }

        /*
         * b. Get the remote SN config. If we can't reach the SN, we can't do
         * any kind of check. If there are problems reaching the SN, this will
         * throw an exception.
         */
        if (snRemoteParams.get(snId) == null) {
            Topology current = admin.getCurrentTopology();
            RegistryUtils regUtils =
                new RegistryUtils(current, admin.getLoginManager(), logger);
            StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
            LoadParameters remoteParams = sna.getParams();
            snRemoteParams.put(snId, processRemoteInfo(snId, remoteParams));
        }

        /* Does the SN say that the RN is present? */
        CONFIG_STATUS configStatus = CONFIG_STATUS.GONE;
        if (snRemoteParams.get(snId).contains(resId)) {
            configStatus = CONFIG_STATUS.HERE;
        }

        /* c. Get JEHA group metadata */
        JEHAInfo jeHAInfo = getJEHAInfo(admin, resId);

        /*
         * Now that all inputs are assembled, generate a RNLocationInput that
         * serves to lookup the required fix.
         */
        final RNLocationInput remedyKey;
        if (jeHAInfo == null) {
            /* We don't have JE HA info */
            if (calledByDeployNewRN) {
                /* We know that this RN can't exist anywhere else. */
                remedyKey = new RNLocationInput(topoStatus, configStatus,
                                                OTHERSN_STATUS.GONE);
            } else {
                /*
                 * No JEHA info, so check the information gathered from the
                 * config.xmls for other SN. We may be able to conjecture
                 * whether this RN exists anywhere else. If it does show
                 * up on other SNs, we have some info.
                 */
                if (readAllSNRemoteParams(admin.getCurrentTopology(),
                                          admin.getLoginManager())) {
                    boolean found = searchOtherSNs(snId, resId);
                    OTHERSN_STATUS otherSNStatus;
                    if (found) {
                        otherSNStatus = OTHERSN_STATUS.HERE;
                    } else {
                        otherSNStatus = OTHERSN_STATUS.GONE;
                    }

                    remedyKey = new RNLocationInput(topoStatus, configStatus,
                                                  otherSNStatus);
                } else {
                    /*
                     * Couldn't reach all SNs, so there's no proxy for JEHA
                     * info. Any repairs would have to be made solely on the
                     * topology and config status.
                     */
                    remedyKey = new RNLocationInput(topoStatus, configStatus);
                }
            }
        } else {
            /*
             * Great, we have definitive JE HA status. Use that to drive the
             * repairs to make topo and config match JE HA.
             */
            JEHA_STATUS jeHAStatus = JEHA_STATUS.GONE;
            if (jeHAInfo.getSNId().equals(snId)) {
                jeHAStatus = JEHA_STATUS.HERE;
            }

            remedyKey = new RNLocationInput(topoStatus, configStatus,
                                            jeHAStatus);
        }

        /*
         * Look up a remedy. If all seems okay, we'll get an
         * OkayRemedy.FACTORY.
         */
        RNRemedyFactory remedyFactory;

        if (isRN) {
            remedyFactory = RN_REMEDIES.get(remedyKey);
        } else {
            /* Must be an AN */
            remedyFactory = AN_REMEDIES.get(remedyKey);
        }

        /* We have a problem but no remedy -- there's nothing we can do */
        if (remedyFactory == null) {
            remedyFactory = NoFixRemedy.FACTORY;
        }

        /*
         * Even if the RN's information is consistent, we may still need to
         * generate a recommendation for repair. If the RN is disabled,
         * re-enable and restart the RN.
         */
        if (remedyFactory.equals(OkayRemedy.FACTORY) &&
            topoStatus.equals(TOPO_STATUS.HERE) &&
            makeRNEnabled) {
            Parameters params = admin.getCurrentParameters();
            boolean isDisabled =
                isRN ?
                params.get((RepNodeId)resId).isDisabled() :
                params.get((ArbNodeId)resId).isDisabled();

            if (isDisabled) {
                remedyFactory = CreateRNRemedy.FACTORY;
            }
        }

        return remedyFactory.createRemedy(
            this, remedyKey, snId, resId, jeHAInfo, oldSNId, storageDirectory);
    }

    /**
     * Check whether it is possible to move this Admin from oldSN to newSN.
     * Base this decision on the Admin's JE HA group information.
     */
    public Remedy checkAdminMove(Admin admin,
                                 AdminId adminId,
                                 StorageNodeId oldSN,
                                 StorageNodeId newSN) {

        /*
         * Find all the Admins and ask them for the JEHA Group info.
         */
        JEHAInfo jeHAInfo = getAdminJEHAInfo(admin, adminId);
        if (jeHAInfo == null) {
            /*
             * We were not able to get JE HA info. This doesn't make sense,
             * because this code runs on the Admin as part of deploying a
             * topology change, so the Admin it is running on must be an Admin
             * master, and the JEHAGroup info should be available.
             */
            throw new NonfatalAssertionException
                ("Attempting to check location for admin " + adminId +
                 " but could not obtain JE HA group db info");

        }

        Parameters params = admin.getCurrentParameters();
        Topology topo = admin.getCurrentTopology();

        /*
         * Compare the
         *  1. jeHA snId for this admin
         *  2. the Admin params snid
         *  3. the remote config.xml for the new SN
         *
         * These conditions should be true:
         *  a. jeHASNId is either oldSN or newSN
         *  b. adminParamsSNID is either oldSN or newSN
         *  c. the newSN's config.xml has either no admin or this admin.
         */
        StorageNodeId jeHASNId = jeHAInfo.getSNId();
        StorageNodeId adminParamsSNId = params.get(adminId).getStorageNodeId();
        SNServices remoteInfo = readOneSNRemoteParams(topo, newSN,
                                                      admin.getLoginManager());
        boolean remoteNewSNCorrect = false;
        if (remoteInfo != null) {
            AdminId remoteAdminId = remoteInfo.getAdminId();
            remoteNewSNCorrect = ((remoteAdminId == null) ||
                                  (adminId.equals(remoteAdminId)));
        }

        if (!((jeHASNId.equals(oldSN) || jeHASNId.equals(newSN)) &&
              (adminParamsSNId.equals(oldSN) || adminParamsSNId.equals(newSN)) &&
              remoteNewSNCorrect)) {

            /* Unexpectedly, conditions a, b and c are not fulfilled. */
            return new RunRepairRemedy(adminId);
        }

        return new OkayRemedy(adminId, jeHAInfo);
    }

    /**
     * Check that the Admin's JE HA group matches the AdminDB.
     *
     * Since Admins are
     *  1. only moved as a result of migrate-sn
     *  2. by definition, quorum exists
     *  3. the source SN is shut down
     * the AdminDB is always seen as source of truth. Unlike RNs, we never
     * need to revert an Admin location back to the old SN. If the Admin's
     * JE HA group does not match the AdminDB, update its HA address.
     */
    public Remedy checkAdminLocation(Admin admin,
                                     AdminId adminId) {

        /*
         * Find all the Admins and ask them for the JEHA Group info.
         */
        JEHAInfo jeHAInfo = getAdminJEHAInfo(admin, adminId);
        if (jeHAInfo == null) {
            /* There isn't an Admin master */
            return new NoFixRemedy(adminId);
        }

        Parameters params = admin.getCurrentParameters();
        Topology topo = admin.getCurrentTopology();

        /*
         * Compare the
         *  1. jeHA snId for this admin
         *  2. the Admin params snid
         *  3. the remote config.xml for the new SN
         *
         * These conditions should be true:
         *  a. jeHASNId == adminParamsSNID
         *  b. the remote SN has this Admin in its config file.
         */
        StorageNodeId jeHASNId = jeHAInfo.getSNId();
        StorageNodeId adminParamsSNId = params.get(adminId).getStorageNodeId();
        SNServices remoteInfo = readOneSNRemoteParams(topo, adminParamsSNId,
                                                      admin.getLoginManager());
        boolean remoteSNCorrect = false;
        if ((remoteInfo != null) &&
            (adminId.equals(remoteInfo.getAdminId()))) {
            remoteSNCorrect = true;
        }

        AdminLocationInput adminLoc =
            new AdminLocationInput(jeHASNId, adminParamsSNId, remoteSNCorrect);

        if (!(jeHASNId.equals(adminParamsSNId) && remoteSNCorrect)) {

            /* Conditions a and b are not fulfilled. */
            return new FixAdminRemedy(this, adminLoc, adminId, jeHAInfo);
        }

        return new OkayRemedy(adminId, jeHAInfo);
    }

    /**
     * @return true if this RN/AN is in the config file of an SN other than this
     * one.
     */
    private boolean searchOtherSNs(StorageNodeId snId, ResourceId resId) {
        for (Map.Entry<StorageNodeId, SNServices> e :
                 snRemoteParams.entrySet()) {

            /* This is this SN, skip it */
            if (e.getKey().equals(snId)) {
                continue;
            }

            if (e.getValue().contains(resId)) {
                /* Found a different SN that has this RN/AN */
                return true;
            }
        }

        /* Didn't find this RN/AN on any other SN */
        return false;
    }

    /**
     * Try to get the JE HA repgroup db information for this shard.
     */
    private JEHAInfo getJEHAInfo(Admin admin, ResourceId resId) {

        RepGroupId rgId =
            new RepGroupId(Utils.getRepGroupId(resId).getGroupId());
        /*
         * Assemble a set of sockets to query by adding all the helper
         * hosts and nodehostports  from the rep node params for each
         * member of the shard.
         */
        Topology topo = admin.getCurrentTopology();
        Parameters params = admin.getCurrentParameters();
        RepGroup rg = topo.get(rgId);
        if (rg == null) {

            /*
             * Something is quite inconsistent; there's a RN in the SN that is
             * not in the topology. Give up on trying to get JE HA info.
             */
            return null;
        }

        final Set<InetSocketAddress> helperSockets = new HashSet<>();

        /*
         * Find the set of SNs that the topo thinks owns the RN, in order
         * to optimize the translation. The translation will look at those
         * first.
         */
        Set<StorageNodeId> snCheckSet = new HashSet<>();
        for (RepNode member : rg.getRepNodes()) {
            RepNodeParams rnp = params.get(member.getResourceId());
            snCheckSet.add(rnp.getStorageNodeId());
            helperSockets.addAll
                (HostPortPair.getSockets(rnp.getJEHelperHosts()));
            helperSockets.add
                (HostPortPair.getSocket(rnp.getJENodeHostPort()));
        }

        ReplicationNetworkConfig repNetConfig = admin.getRepNetConfig();
        /* Armed with the helper sockets, see if we can find a JE Master */
        Set<ReplicationNode> group =
            TopologyCheckUtils.getJEHAGroup(rgId.getGroupName(),
                                            JE_HA_TIMEOUT_MS,
                                            logger,
                                            helperSockets,
                                            repNetConfig);

        StringBuilder helperHosts = new StringBuilder();
        for (ReplicationNode rNode : group) {
            if (helperHosts.length() > 0) {
                helperHosts.append(",");
            }
            helperHosts.append(HostPortPair.getString(rNode.getHostName(),
                                                      rNode.getPort()));
        }

        for (ReplicationNode rNode : group) {
            StorageNodeId foundSNId = TopologyCheckUtils.translateToSNId
                (topo, params, snCheckSet, rNode.getHostName(),
                 rNode.getPort());

            if (foundSNId == null) {
                /*
                 * Not expected -- why is the SN referred to by the RN not
                 * in the topology?
                 */
                logger.log(Level.SEVERE, "{0} couldn''t find SN for {1}:{2}",
                       new Object[]{who, rNode.getHostName(), rNode.getPort()});
            } else if ((VersionUtil.getJavaMajorVersion() >= 11) && 
                       TextIndexFeederManager.isTIFNode(rNode.getName())) {
                /* skip parse the node name if a TIF node*/
                continue;
            } else {
                ResourceId rid = null;
                try {
                    rid = RepNodeId.parse(rNode.getName());
                } catch (IllegalArgumentException ignore) {
                    rid = ArbNodeId.parse(rNode.getName());
                }

                if (rid.equals(resId)) {
                    return new JEHAInfo(foundSNId,
                                        rNode,
                                        helperHosts.toString());
                }
            }
        }
        return null;
    }

    /**
     * Try to get the JE HA repgroup db information for the Admin group.
     */
    private JEHAInfo getAdminJEHAInfo(Admin admin, AdminId adminId) {

        /*
         * Assemble a set of sockets to query by adding all the helper
         * hosts and nodehostports from the admin params.
         */
        Parameters params = admin.getCurrentParameters();

        final Set<InetSocketAddress> helperSockets = new HashSet<>();

        /*
         * Find the set of SNs that the topo thinks owns admins, in order to
         * optimize the translation of nodeHostPort to SN. The translation will
         * look at those first.
         */
        Set<StorageNodeId> snCheckSet = new HashSet<>();
        for (AdminParams ap: params.getAdminParams()) {
            snCheckSet.add(ap.getStorageNodeId());
            helperSockets.addAll
                (HostPortPair.getSockets(ap.getHelperHosts()));
            helperSockets.add
                (HostPortPair.getSocket(ap.getNodeHostPort()));
        }

        String kvstoreName =
                admin.getCurrentParameters().getGlobalParams().getKVStoreName();
        String groupName = Admin.getAdminRepGroupName(kvstoreName);
        ReplicationNetworkConfig repNetConfig = admin.getRepNetConfig();
        /* Armed with the helper sockets, see if we can find a JE Master */
        Set<ReplicationNode> group =
            TopologyCheckUtils.getJEHAGroup(groupName,
                                            JE_HA_TIMEOUT_MS,
                                            logger,
                                            helperSockets,
                                            repNetConfig);

        StringBuilder helperHosts = new StringBuilder();
        for (ReplicationNode rNode : group) {
            if (helperHosts.length() > 0) {
                helperHosts.append(",");
            }
            helperHosts.append(HostPortPair.getString(rNode.getHostName(),
                                                      rNode.getPort()));
        }

        Topology topo = admin.getCurrentTopology();
        for (ReplicationNode rNode : group) {
            StorageNodeId foundSNId = TopologyCheckUtils.translateToSNId
                (topo, params, snCheckSet, rNode.getHostName(),
                 rNode.getPort());

            if (foundSNId == null) {
                /*
                 * Not expected -- why is the SN referred to by the
                 * ReplicationNode not in the topology?
                 */
                logger.log(Level.SEVERE, "{0} couldn''t find SN for {1}:{2}",
                       new Object[]{who, rNode.getHostName(), rNode.getPort()});
            } else {
                AdminId aId = AdminId.parse(rNode.getName());
                if (aId.equals(adminId)) {
                    return new JEHAInfo(foundSNId,
                                        rNode,
                                        helperHosts.toString());
                }
            }
        }
        return null;
    }

    /**
     * Try to get remote params for all SNs in the topology. Used when we are
     * are trying to deduce what has happened without JE HA rep group db info.
     * @param loginManager
     * @return true if all SNs are found.
     */
    private boolean readAllSNRemoteParams(Topology topo,
                                          LoginManager loginManager) {

        /* Make sure each SN has a copy of its remote params fetched. */
        List<StorageNodeId> allSNs = topo.getStorageNodeIds();
        for (StorageNodeId snId : allSNs) {
            if (readOneSNRemoteParams(topo, snId, loginManager) == null) {
                /* Give up, we can't guarantee that we will find all SN info */
                return false;
            }
        }
        return true;
    }

    /**
     * Read one SN's remote config file and save it in the snRemoteParams map.
     * @return the newly generated information.
     */
    private SNServices readOneSNRemoteParams(Topology topo,
                                             StorageNodeId snId,
                                             LoginManager loginManager) {
        RegistryUtils regUtils = new RegistryUtils(topo, loginManager, logger);

        SNServices remoteInfo = snRemoteParams.get(snId);
        if (remoteInfo != null) {
            return remoteInfo;
        }

        /* Try to get params from the remote SN */
        LoadParameters remoteParams;
        try {
            StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
            remoteParams = sna.getParams();
            remoteInfo = processRemoteInfo(snId, remoteParams);
            snRemoteParams.put(snId, remoteInfo);
            logger.log(Level.INFO, "{0} loaded remote params for {1}",
                       new Object[]{who, snId});
            return remoteInfo;
        } catch (NotBoundException | RemoteException re) {
            logger.log(Level.INFO,
                      "{0} failed to reach {1} to load SN params: {2}",
                      new Object[]{who, snId, re});
        }
        return null;
    }

    /**
     * Make a table of recommendations for how to repair RN inconsistencies.
     * For context, here are the steps taken when a RN is first created by
     * DeployNewRN:
     * 1. update AdminDB
     * 2. create on new SN w/sna.createRepNode
     * 3. that ends up updating the JE HA rep group
     *
     * RNRelocate steps
     * 1. disable RN on old SN
     *    a. update AdminDB w/disable bit
     *    b. remote call to update SN config w/disable bit
     * 2. update AdminDB to move RN to new SN
     * 3. update JE HA rep group
     * 4. create RN on new SN
     * 5. delete RN on old SN
     */
    private static void initRNRemedies() {

        /*
         * When JEHA groupdb is available, give that status highest priority.
         * If it is available, change other locations to match.
         *
         *   RN is on SN      should be
         * topo|config|JEHA|  on thisSN failed task    |  remedy
         * --------------------------------------------------------------
         *  T     T     T      no problems    none
         *  T     T     F      DeployNewRN    clear from AdminDb and config
         *  T     F     T      RelocateRN     call sna.createRepNode
         *  T     F     F      DeployNewRN/   clear from AdminDB or
         *                     RelocateRN     (change AdminDB back to SN
         *                                  indicated by JE HA
         *  F     T     T      RelocateRN     re-add to AdminDB, reenable on SN
         *  F     T     F      RelocateRN     remove from this SN, call delete
         *  F     F     T     --- can't happen ---- check migrateSN
         *  F     F     F     no problems    none
         *
         * When JE HA info is not available, we can still reason what might
         * have happened by looking in the config files of other SNs.
         *
         * topo|cnfg|in other
         *     |    | SN    |
         *     |    |configs|  failed task    |  remedy
         * ----------------------------------------------------------------
         *  T     T     T        RelocateRN     eventually need to disable on
         *                                        other SN
         *  T     T     F        none           the user should re-start the
         *                                        RN
         *  T     F     T        RelocateRN     unknown, we know that
         *                                        task failed, not sure if
         *                                        after 2 or 3
         *  T     F     F    DeployNewRN or     remove from topo
         *  F     T     T        RelocateRN     eventually need to disable,
         *                                        but not safe to decide now
         *  F     T     F        RelocateRN     can't happen
         *  F     F     n/a
         */

        /* All three inputs (AdminDB, SN config, JE HA) are available */
        addRNRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.HERE,
                                        JEHA_STATUS.HERE),
                    OkayRemedy.FACTORY);
        addRNRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.HERE,
                                        JEHA_STATUS.GONE),
                    ClearAdminConfigRemedy.FACTORY);
        addRNRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.GONE,
                                        JEHA_STATUS.HERE),
                    CreateRNRemedy.FACTORY);
        addRNRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.GONE,
                                        JEHA_STATUS.GONE),
                    RevertRNRemedy.FACTORY);
        addRNRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.HERE,
                                        JEHA_STATUS.HERE),
                    RevertRNRemedy.FACTORY);
        addRNRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.HERE,
                                        JEHA_STATUS.GONE),
                    RemoveRNRemedy.FACTORY);
        addRNRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.GONE,
                                        JEHA_STATUS.HERE),
                    NoFixRemedy.FACTORY);
        addRNRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.GONE,
                                        JEHA_STATUS.GONE),
                    OkayRemedy.FACTORY);

        /*
         * JE HA GroupDB not available, but there is information about what
         * is in other remote SN config files. For these situations, try to
         * move forward, don't revert, because we don't know what the JE HA
         * situation is.
         */
        /* Relocate, needs to be disabled on the other SN */
        addRNRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.HERE,
                                        OTHERSN_STATUS.HERE),
                    DisableRNRemedy.FACTORY);

        addRNRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.HERE,
                                        OTHERSN_STATUS.GONE),
                    CreateRNRemedy.FACTORY);

        /* Don't know what to do */
        addRNRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.GONE,
                                        OTHERSN_STATUS.HERE),
                    NoFixRemedy.FACTORY);

        /* Deploy didn't finish */
        addRNRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.GONE,
                                        OTHERSN_STATUS.GONE),
                  ClearAdminConfigRemedy.FACTORY);

        /* Relocate failed, need remove this SN if disabled. */
        addRNRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.HERE,
                                        OTHERSN_STATUS.HERE),
                    NoFixRemedy.FACTORY);
        addRNRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.HERE,
                                        OTHERSN_STATUS.GONE),
                    NoFixRemedy.FACTORY); // TODO, figure out what to do
        addRNRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.GONE,
                                        OTHERSN_STATUS.GONE),
                    OkayRemedy.FACTORY);
        /* TODO: The chart above doesn't say if this case is OK -- is it? */
        addRNRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.GONE,
                                        OTHERSN_STATUS.HERE),
                    OkayRemedy.FACTORY);
    }

    /**
     * Make a table of recommendations for how to repair AN inconsistencies.
     */
    private static void initANRemedies() {

        /*
         * The Admin database is given the highest priority.
         *
         *   AN is on SN
         * topo|config|JEHA|  remedy
         * --------------------------------------------------------------
         *  T     T     T     no problems
         *  T     T     F                     Arbiter will add itself to the JE
         *                                    HA group automatically on start
         *  T     F     T                     call sna.createArbNode
         *  T     F     F                     call sna.createArbNode
         *  F     T     T                     remove from SN
         *  F     T     F          remove from this SN, call delete
         *  F     F     T
         *  F     F     T                    Arbiter needs to be removed from
         *                                   JE HA group by topology repair
         *  F     F     F     no problems    none
         */

        /* All three inputs (AdminDB, SN config, JE HA) are available */
        addANRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.HERE,
                                        JEHA_STATUS.HERE),
                    OkayRemedy.FACTORY);
        /* AN has not joined the group yet. */
        addANRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.HERE,
                                        JEHA_STATUS.GONE),
                    OkayRemedy.FACTORY);
        addANRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.GONE,
                                        JEHA_STATUS.HERE),
                    CreateRNRemedy.FACTORY);
        addANRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.GONE,
                                        JEHA_STATUS.GONE),
                    CreateRNRemedy.FACTORY);
        addANRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.HERE,
                                        JEHA_STATUS.HERE),
                    RemoveRNRemedy.FACTORY);
        addANRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.HERE,
                                        JEHA_STATUS.GONE),
                    RemoveRNRemedy.FACTORY);
        /* This should not happen in normal circumstances because JEHA
         * entry is removed before it is removed from topology.
         */
        addANRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.GONE,
                                        JEHA_STATUS.HERE),
                    NoFixRemedy.FACTORY);
        addANRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.GONE,
                                        JEHA_STATUS.GONE),
                    OkayRemedy.FACTORY);

        /* Needs to be disabled on the other SN */
        addANRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.HERE,
                                        OTHERSN_STATUS.HERE),
                    DisableRNRemedy.FACTORY);

        addANRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.HERE,
                                        OTHERSN_STATUS.GONE),
                    OkayRemedy.FACTORY);

        addANRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.GONE,
                                        OTHERSN_STATUS.HERE),
                    CreateRNRemedy.FACTORY);

        addANRemedy(new RNLocationInput(TOPO_STATUS.HERE,
                                        CONFIG_STATUS.GONE,
                                        OTHERSN_STATUS.GONE),
                  CreateRNRemedy.FACTORY);

        addANRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.HERE,
                                        OTHERSN_STATUS.HERE),
                    RemoveRNRemedy.FACTORY);
        addANRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.HERE,
                                        OTHERSN_STATUS.GONE),
                    RemoveRNRemedy.FACTORY);
        addANRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.GONE,
                                        OTHERSN_STATUS.GONE),
                    OkayRemedy.FACTORY);
        addANRemedy(new RNLocationInput(TOPO_STATUS.GONE,
                                        CONFIG_STATUS.GONE,
                                        OTHERSN_STATUS.HERE),
                    OkayRemedy.FACTORY);
    }


    /**
     * Use this method to add remedies to the RN remedies map, guarding against
     * two remedies for the same set of inputs.
     */
    private static void addRNRemedy(RNLocationInput key,
                                    RNRemedyFactory factory) {
        final RNRemedyFactory oldFactory = RN_REMEDIES.put(key, factory);
        if (oldFactory != null) {
            throw new IllegalStateException("Tried to overwrite remedy " +
                                            key + "/" + oldFactory +
                                            " with " + factory);
        }
    }

    private static void addANRemedy(RNLocationInput key,
                                    RNRemedyFactory factory) {
        final RNRemedyFactory oldFactory = AN_REMEDIES.put(key, factory);
        if (oldFactory != null) {
            throw new IllegalStateException("Tried to overwrite remedy " +
                                            key + "/" + oldFactory +
                                            " with " + factory);
        }
    }

   /**
     * Return all violations which are of a certain type, and are for a
     * specified kind of topology component.
     */
    private <T extends Problem> Set<T> filterViolations
                       (VerifyResults results, Class<T> problemClass) {

        Set<T> found = new HashSet<>();
        for (Problem p : results.getViolations()) {
            if (p.getClass().equals(problemClass)) {
                found.add(problemClass.cast(p));
            }
        }
        return found;
    }

    /** Apply all remedies in the list */
    public void applyRemedies(List<Remedy> repairs, AbstractPlan plan) {
        for (Remedy r: repairs) {
            applyRemedy(r, plan);
        }
    }

    /**
     * Given a remedy type, apply a fix.
     * @return true if fix was completed.
     */
    public boolean applyRemedy(Remedy remedy, AbstractPlan plan) {

        logger.log(Level.INFO, "{0} applying {1}", new Object[]{who, remedy});
        if (!remedy.isOkay() && !remedy.canFix()) {
            /* We should have had a way to fix this! */
            logger.log(Level.INFO, "{0} did not act upon {1}",
                       new Object[]{who, this});
            throw new UnsupportedOperationException();
        }
        return remedy.apply(plan);
    }

    /**
     * Fix Admin issues.
     */
    private boolean repairAdmin(FixAdminRemedy remedy, AbstractPlan plan) {
        Admin admin = plan.getAdmin();
        Parameters params = admin.getCurrentParameters();
        Topology topo = admin.getCurrentTopology();
        final AdminId targetId = remedy.getAdminId();
        final AdminParams ap = params.get(targetId);
        try {
            /*
             * Make sure that the AdminDB's params for a given admin are also
             * correctly reflected by the admin group's jeHAGroupDB, and also
             * that the Admin is started up.
             */
            ChangeServiceAddresses.changeAdminHAAddress
                (plan,
                 "repair Admin location for " + targetId,
                 params,
                 targetId);

            LoginManager loginMgr = admin.getLoginManager();
            RegistryUtils regUtils = new RegistryUtils(topo, loginMgr, logger);
            StorageNodeAgentAPI sna =
                regUtils.getStorageNodeAgent(ap.getStorageNodeId());
            sna.createAdmin(ap.getMap());
            return true;
        } catch (OperationFaultException | RemoteException |
                 NotBoundException e) {
            logger.log(Level.INFO, "{0} repair of Admin saw {1}",
                       new Object[]{who, e});
            return false;
        }
    }

    /**
     * Update the admin parameters on the admin associated with the remedy if
     * they differ from the ones in the admin database or if the admin type
     * needs to be changed to match the datacenter.  Does not correct SN or
     * global parameters.
     */
    private boolean repairAdminParams(UpdateAdminParamsRemedy remedy,
                                      AbstractPlan plan) {
        final AdminId targetId = remedy.getAdminId();
        try {
            final State result = UpdateAdminParams.update(plan, null /* task */,
                                                          targetId);
            return result == State.SUCCEEDED;
        } catch (Exception e) {
            logger.log(Level.INFO, "{0} could not update admin params: {1}",
                       new Object[]{who, e});
            return false;
        }
    }

    /**
     * Remove the RN from the admin db and config.xml of this SN.
     */
    private boolean repairWithClearRN(ClearAdminConfigRemedy remedy,
                                      AbstractPlan plan) {
        Admin admin = plan.getAdmin();
        StorageNodeId snId = remedy.getSNId();
        RepNodeId rnId = remedy.getRNId();
        Topology topo = admin.getCurrentTopology();
        if (remedy.getRNLocationInput().presentInSNConfig) {
            logger.log(Level.INFO, "{0} trying to remove {1} from {2} config",
                       new Object[]{who, rnId, snId});
            RegistryUtils regUtils =
                new RegistryUtils(topo, admin.getLoginManager(), logger);
            try {
                StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
                sna.destroyRepNode(rnId, true /*deleteData*/, plan.toString());
            } catch (NotBoundException | RemoteException re) {
                logger.log(Level.INFO,
                           "{0} couldn''t reach {1} to remove {2} {3}",
                           new Object[]{who, snId, rnId, re});
                return false;
            }
        }

        if (remedy.getRNLocationInput().presentInTopo) {
            topo.remove(rnId);
            admin.saveTopoAndRemoveRN(topo, plan.getDeployedInfo(),
                                      rnId, plan);
            logger.log(Level.INFO,
                      "{0} trying to remove {1} from topo and params",
                      new Object[]{who, rnId});
        }

        return true;
    }

    /**
     * Remove the AN from the admin db and config.xml of this SN.
     */
    private boolean repairWithClearAN(ClearAdminConfigRemedy remedy,
                                      AbstractPlan plan) {
        Admin admin = plan.getAdmin();
        StorageNodeId snId = remedy.getSNId();
        ArbNodeId anId = remedy.getANId();
        Topology topo = admin.getCurrentTopology();
        RepGroupId rgId = new RepGroupId(anId.getGroupId());
        if (remedy.getRNLocationInput().presentInSNConfig) {
            logger.log(Level.INFO, "{0} trying to remove {1} from {2} config",
                       new Object[]{who, anId, snId});
            RegistryUtils regUtils =
                new RegistryUtils(topo, admin.getLoginManager(), logger);
            try {
                StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
                sna.destroyArbNode(anId, true /*deleteData*/, plan.toString());
            } catch (NotBoundException | RemoteException re) {
                logger.log(Level.INFO,
                           "{0} couldn''t reach {1} to remove {2} {3}",
                           new Object[]{who, snId, anId, re});
                return false;
            }
        }

        if (remedy.getRNLocationInput().presentInTopo) {

            /* Update helper hosts on peers. */
            Parameters params = admin.getCurrentParameters();
            String helpers =
                Utils.findHelpers(anId, params, topo);
            /* See if any peer RNs need their helper hosts updated */
            Set<RepNodeParams> needsUpdate = new HashSet<RepNodeParams>();
            RepGroup rg = topo.get(rgId);
            for (RepNode rn : rg.getRepNodes()) {
                if (!rn.getStorageNodeId().equals(snId)) {
                     RepNodeParams rnp = params.get(rn.getResourceId());
                     if (helperMismatch(rnp.getJEHelperHosts(), helpers)) {
                         RepNodeParams newrnp = new RepNodeParams(rnp);
                         newrnp.setJEHelperHosts(helpers);
                         needsUpdate.add(newrnp);
                     }
                }
            }

            topo.remove(anId);
            logger.log(Level.INFO,
                       "{0} trying to remove {1} from topo and params",
                       new Object[]{who, anId});
            admin.saveTopoAndRemoveAN(topo, plan.getDeployedInfo(),
                                      anId, plan);
            topo = admin.getCurrentTopology();
            if (needsUpdate.size() > 0) {
                admin.saveParams(needsUpdate,
                                 Collections.<AdminParams>emptySet(),
                                 Collections.<ArbNodeParams>emptySet());
            }

            /* Send topology changes to all nodes.*/
            try {
                if (!Utils.broadcastTopoChangesToRNs(logger,
                                                     topo,
                                                     "remove AN repair " + anId,
                                                     admin.getParams().
                                                     getAdminParams(),
                                                     plan)) {
                    return false;
                }
            } catch (InterruptedException e) {
                return false;
            }
            logger.log(Level.INFO, "{0} removed AN {1}",
                       new Object[]{who, anId});
        }

        return true;
    }

    /**
     * Change the admin db to make this RN refer to the SN which JE HA thinks
     * is correct.  Note that the remedy's oldSNId may be null if we don't know
     * what to revert it to.
     */
    private boolean repairRevert(RevertRNRemedy remedy, AbstractPlan plan) {

        Admin admin = plan.getAdmin();

        /* Change the topology back to the "old" SN */
        final RepNodeId rnId = remedy.getRNId();
        final StorageNodeId oldSNId = remedy.getOldSNId();
        final ArbNodeId anId = remedy.getANId();
        final ResourceId resId = remedy.getResourceId();

        StorageNodeId correctSNId = null;
        String correctJEHAHostPort = null;
        String correctHelpers = null;
        JEHAInfo jeHAInfo = remedy.getJEHAInfo();
        Topology topo = admin.getCurrentTopology();
        Parameters params = admin.getCurrentParameters();

        if (jeHAInfo != null) {
            /*
             * We got a definitive statement from the JEHA repgroup db to
             * determine where the RN should live.
             */
            correctSNId = jeHAInfo.getSNId();
            correctHelpers = jeHAInfo.getHelpers();
            correctJEHAHostPort = jeHAInfo.getHostPort();
        } else if (oldSNId != null) {

            /* We had some other means of determining the proper SN */
            correctSNId = oldSNId;
            PortTracker portTracker = new PortTracker(topo, params, oldSNId);
            int haPort = portTracker.getNextPort(oldSNId);
            String haHostname = params.get(oldSNId).getHAHostname();
            correctJEHAHostPort = HostPortPair.getString(haHostname, haPort);
            correctHelpers = Utils.findHelpers(resId, params, topo);
            correctHelpers += "," + correctJEHAHostPort;
        }

        /* No known correct location - bail */
        if (correctSNId == null) {
            logger.log(Level.INFO, "{0} could not find correct owning SN {1}",
                       new Object[]{who, remedy});
            return false;
        }

        ChangedParams updated = null;
        boolean topoUpdated = false;
        if (rnId != null) {
            RepNode rn = topo.get(rnId);
            if (!rn.getStorageNodeId().equals(correctSNId)) {
                logger.log(Level.INFO, "{0} updating topology so {1} owns {2}",
                           new Object[]{who, correctSNId, rnId});
                RepNode updatedRN = new RepNode(correctSNId);
                RepGroup rg = topo.get(rn.getRepGroupId());
                rg.update(rn.getResourceId(),updatedRN);
                topoUpdated = true;
            }

             /* Revert the RN's params, and any peer params */
            updated =
                correctRNParams(topo,
                                params,
                                rnId,
                                correctSNId,
                                correctJEHAHostPort,
                                correctHelpers,
                                remedy.getNewStorageDir(),
                                correctSNId.equals(remedy.getSNId()),
                                admin.getLoginManager());
        } else {
            /* Work on AN */
            ArbNode an = topo.get(anId);
            if (!an.getStorageNodeId().equals(correctSNId)) {
                logger.log(Level.INFO, "{0} updating topology so {1} owns {2}",
                           new Object[]{who, correctSNId, anId});
                ArbNode updatedAN = new ArbNode(correctSNId);
                RepGroup rg = topo.get(an.getRepGroupId());
                rg.update(an.getResourceId(), updatedAN);
                topoUpdated = true;
            }

            updated =
                correctANParams(topo,
                                params,
                                anId,
                                correctSNId,
                                correctJEHAHostPort,
                                correctHelpers);

        }
        /* See which RNs might need to be prodded to refresh their params */
        boolean peersNeedUpdate = false;
        boolean rnNeedsUpdate = false;
        Set<RepNodeParams> needsUpdate = updated.getRNP();
        for (RepNodeParams updatedRNP : needsUpdate) {
            if (updatedRNP.getRepNodeId().equals(rnId)) {
                rnNeedsUpdate = true;
            } else {
                peersNeedUpdate = true;
            }
        }

        Set<ArbNodeParams> anUpdate = updated.getANP();
        boolean anNeedsUpdate = false;
        for (ArbNodeParams updatedANP : anUpdate) {
            if (updatedANP.getArbNodeId().equals(anId)) {
                anNeedsUpdate = true;
            } else {
                peersNeedUpdate = true;
            }
        }

        /* Write the changes to the AdminDB */
        if (topoUpdated) {
            admin.saveTopoAndParams(topo, plan.getDeployedInfo(),
                                    needsUpdate,
                                    Collections.<AdminParams>emptySet(),
                                    anUpdate, plan);
            try {
                Utils.broadcastTopoChangesToRNs
                    (logger, admin.getCurrentTopology(),
                     who + " updating topo",
                     admin.getParams().getAdminParams(), plan);
            } catch (InterruptedException e) {
                logger.log(Level.INFO, "{0} couldn''t update topo: {1}",
                           new Object[]{who, e});
                return false;
            }
        } else {
            if (!needsUpdate.isEmpty()) {
                plan.getAdmin().saveParams(
                    needsUpdate, Collections.<AdminParams>emptySet(),
                    anUpdate);
            }
        }

        /*
         * Restart the RN on the correct SN, make sure it houses the RN with
         * the correct params.
         */
        if (rnNeedsUpdate) {
            try {
                RelocateRN.startRN(plan, correctSNId, rnId);
            } catch (Exception e) {
                logger.log(Level.INFO, "{0} couldn''t start {1}",
                           new Object[]{who, rnId});
            }
        }

        /*
         * Restart the AN on the correct SN, make sure it houses the AN with
         * the correct params.
         */
        if (anNeedsUpdate) {
            try {
                RelocateAN.createStartAN(plan, correctSNId, anId);
            } catch (Exception e) {
                logger.log(Level.INFO, "{0} couldn''t start {1}",
                           new Object[]{who, anId});
            }
        }

        /* Update params at peers, if needed */
        if (peersNeedUpdate) {
            try {
                Utils.refreshParamsOnPeers(plan, resId);
            } catch (Exception e) {
                logger.log(Level.INFO,
                           "{0} couldn''t update helper hosts at peers", who);
                return false;
            }
        }

        return true;
    }

    /**
     * Start the RN on this SN.
     */
    private boolean repairStartRN(CreateRNRemedy remedy, AbstractPlan plan) {

        try {
            RelocateRN.startRN(plan, remedy.getSNId(), remedy.getRNId());
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * Start the AN on this SN.
     */
    private boolean repairStartAN(CreateRNRemedy remedy, AbstractPlan plan) {

        try {
            RelocateAN.createStartAN(plan, remedy.getSNId(), remedy.getANId());
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * Remove the RN or AN from this SN.
     */
    private boolean repairRemove(RemoveRNRemedy remedy, AbstractPlan plan) {

        boolean retstatus = false;
        try {
            if (remedy.getRNId() != null) {
                retstatus =
                    Utils.destroyRepNode(plan,
                                         System.currentTimeMillis(),
                                         remedy.getSNId(),
                                         remedy.getRNId());
            } else {
                Admin admin = plan.getAdmin();
                retstatus =
                    Utils.destroyArbNode(admin,
                                         plan,
                                         remedy.getSNId(),
                                         remedy.getANId());
                /* Remove AN from JEHA group */
                if (remedy.getRNLocationInput().presentInJEHA) {
                    JEHAInfo jehainfo = getJEHAInfo(admin, remedy.getANId());
                    if (jehainfo != null &&
                        jehainfo.getSNId().equals(remedy.getSNId())) {
                        Utils.removeHAAddress(
                            admin.getCurrentTopology(),
                            admin.getParams().getAdminParams(),
                            remedy.getANId(), remedy.getSNId(), plan,
                            new RepGroupId(remedy.repGroupId),
                            remedy.getJEHAInfo().groupWideHelperHosts,
                            logger);
                    }
                }
            }
        } catch (InterruptedException e) {
            ResourceId rid =
                remedy.getRNId() != null ? remedy.getRNId() : remedy.getANId();
            logger.log(Level.INFO,
                       "{0} couldn''t remove {1} from {2} because of {3}",
                       new Object[]{who, rid, remedy.getSNId(), e});
        }
        return retstatus;
    }

    /**
     * Update the RN's parameters.
     */
    private boolean repairRNParams(UpdateRNParamsRemedy remedy,
                                   AbstractPlan plan) {
        try {
            final State result =
                UpdateRepNodeParams.update(plan, null /* task */,
                                           remedy.getRNId(), 
                                           true /* do Node type processing */, 
                                           true /* do health check */);
            return result == State.SUCCEEDED;
        } catch (Exception e) {
            logger.log(Level.INFO, "{0} could not update RN params: {1}",
                       new Object[]{who, e});
            return false;
        }
    }

    /**
     * Repair the AN parameters to correct inconsistencies.
     */
    private boolean repairANParams(AbstractPlan plan, ArbNodeId anId) {
        try {
            return repairANParamsInternal(plan, anId);

        } catch (NotBoundException | RemoteException e) {
            logger.log(Level.INFO,
                       "{0} couldn''t correct parameters for{1} because of {2}",
                       new Object[]{who, anId, e});
            return false;
        }
    }

    private boolean repairANParamsInternal(AbstractPlan plan,
                                          ArbNodeId anId)
        throws NotBoundException, RemoteException {

        /* Get admin DB parameters */
        final Admin admin = plan.getAdmin();
        final Parameters dbParams = admin.getCurrentParameters();
        final ArbNodeParams anDbParams = dbParams.get(anId);
        final Topology topo = admin.getCurrentTopology();
        final ArbNode thisRn = topo.get(anId);
        final StorageNodeId snId = thisRn.getStorageNodeId();

        /* Get SN configuration parameters */
        final RegistryUtils regUtils =
            new RegistryUtils(topo, plan.getLoginManager(), logger);
        final StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
        final LoadParameters configParams = sna.getParams();
        final CompareParamsResult snCompare =
            VerifyConfiguration.compareParams(configParams,
                                              anDbParams.getMap());

        /* Get in-memory parameters from the AN */
        LoadParameters serviceParams = null;
        try {
            final ArbNodeAdminAPI ana = regUtils.getArbNodeAdmin(anId);
            serviceParams = ana.getParams();
        } catch (RemoteException | NotBoundException e) {
            logger.log(Level.INFO, "{0} problem calling {1}: {2}",
                       new Object[]{who, anId, e});
        }

        /*
         * Check if parameters file needs to be updated, if the AN needs to
         * read them, and if the AN needs to be restarted.
         */
        final CompareParamsResult serviceCompare;
        final CompareParamsResult combinedCompare;
        if (serviceParams == null) {
            serviceCompare = CompareParamsResult.NO_DIFFS;
            combinedCompare = snCompare;
        } else {
            serviceCompare = VerifyConfiguration.compareServiceParams(
                snId, anId, serviceParams, dbParams);
            combinedCompare = VerifyConfiguration.combineCompareParamsResults(
                snCompare, serviceCompare);
        }
        if (combinedCompare == CompareParamsResult.MISSING) {
            logger.log(Level.INFO,
                       "{0} couldn''t update parameters for {1} " +
                       "because some parameters were missing",
                       new Object[] {who, anId});
            return false;
        }

        if (combinedCompare == CompareParamsResult.NO_DIFFS) {
            return true;
        }

        if (snCompare != CompareParamsResult.NO_DIFFS) {
            logger.log(Level.INFO, "{0} updating AN config parameters", who);
            sna.newArbNodeParameters(anDbParams.getMap());
        }
        if (serviceCompare == CompareParamsResult.DIFFS) {
            logger.log(Level.INFO, "{0} notify AN of new parameters", who);
            regUtils.getArbNodeAdmin(anId).newParameters();
        } else {

            /* Stop running node in preparation for restarting it */
            if (serviceCompare == CompareParamsResult.DIFFS_RESTART) {

                try {
                    Utils.disableAndStopAN(plan, snId, anId);
                } catch (OperationFaultException e) {
                    throw new CommandFaultException(
                        e.getMessage(), e, ErrorMessage.NOSQL_5400,
                        CommandResult.PLAN_CANCEL);
                }
            }

            /*
             * Restart the node, or start it if it was not running and is
             * not disabled
             */
            if ((serviceCompare == CompareParamsResult.DIFFS_RESTART) ||
                ((serviceParams == null) && !anDbParams.isDisabled())) {
                try {
                    Utils.startAN(plan, snId, anId);
                    Utils.waitForNodeState(plan, anId, ServiceStatus.RUNNING);
                } catch (Exception e) {
                    throw new CommandFaultException(
                        e.getMessage(), e, ErrorMessage.NOSQL_5400,
                        CommandResult.PLAN_CANCEL);
                }
            }
        }
        return true;
    }

    /**
     *
     * @param plan
     * @param resId AN or RN identifier
     * @return true if the fix was applied or not needed, and false if the
     * fix failed or could not be applied
     */
    private boolean repairHelpers(AbstractPlan plan,
                                  ResourceId resId) {
        /* Get admin DB parameters */
        final Admin admin = plan.getAdmin();
        final Parameters dbParams = admin.getCurrentParameters();
        final Topology topo = admin.getCurrentTopology();
        final String topoHelpersAsString =
            Utils.findHelpers(resId, dbParams, topo);
        boolean retStatus = true;
        String oldHelpers = null;

        if (resId instanceof ArbNodeId) {
            ArbNodeId anId = (ArbNodeId)resId;
            ArbNodeParams anp = dbParams.get(anId);
            if (anp == null) {
                return retStatus;
            }
            retStatus =
                Utils.updateHelperHost(admin, topo,
                                       anId, logger);

            try {
                /* Have the AN notice its new params */
                RegistryUtils registry =
                    new RegistryUtils(topo, admin.getLoginManager(), logger);
                ArbNodeAdminAPI anAdmin = registry.getArbNodeAdmin(anId);
                anAdmin.newParameters();
            } catch (Exception e) {

            }
        } else {
            RepNodeId rnId = (RepNodeId)resId;
            RepNodeParams rnp = dbParams.get(rnId);
            if (rnp == null) {
                return retStatus;
            }
            retStatus =
                Utils.updateHelperHost(admin, topo,
                                       rnId, logger);
            try {
                /* Have the RN notice its new params */
                RegistryUtils registry =
                    new RegistryUtils(topo, admin.getLoginManager(), logger);
                RepNodeAdminAPI rnAdmin = registry.getRepNodeAdmin(rnId);
                rnAdmin.newParameters();
            } catch (Exception e) {
            }
        }
        logger.log(Level.INFO,
                   "{0} repair of helper hosts for {1} old helpers {2} " +
                   "new helpers {3}",
                   new Object[]{who, resId, oldHelpers, topoHelpersAsString});
        return retStatus;
    }

    /**
     * Generate a set of correct RN params for all nodes of this shard. Set the
     * heap/cache, storage directory, and helper hosts correctly.
     * @param loginManager
     */
    private ChangedParams correctRNParams(Topology topo,
                                          Parameters params,
                                          RepNodeId rnId,
                                          StorageNodeId correctSNId,
                                          String correctJEHAHostPort,
                                          String correctHelpers,
                                          StorageDirectory newStorageDir,
                                          boolean correctSNIsNewSN,
                                          LoginManager loginManager) {
        /*
         * Do the params point at the right SN? If not, make a copy of the
         * RepNodeParams and fix its snId, and other attributes.
         */
        RepNodeParams rnp = params.get(rnId);
        RepNodeParams fixedRNP = new RepNodeParams(rnp);
        boolean addFixedParams = false;

        if (!rnp.getStorageNodeId().equals(correctSNId)) {
            fixedRNP.setStorageNodeId(correctSNId);
            Utils.setRNPHeapCacheGC(params.copyPolicies(),
                                    params.get(correctSNId),
                                    fixedRNP,
                                    topo);

            if (correctSNIsNewSN) {
                if (newStorageDir == null) {
                    fixedRNP.setStorageDirectory(null, 0L);
                } else {
                    fixedRNP.setStorageDirectory(newStorageDir.getPath(),
                                                 newStorageDir.getSize());
                }
            } else {
                /* Look in the remote SN config file for the storage dir info*/
                SNServices remoteInfo =
                        readOneSNRemoteParams(topo, correctSNId, loginManager);
                LoadParameters lp = remoteInfo.remoteParams;
                ParameterMap rMap = lp.getMap(rnId.getFullName(),
                                              ParameterState.REPNODE_TYPE);
                RepNodeParams remoteRNP = new RepNodeParams(rMap);
                fixedRNP.setStorageDirectory(remoteRNP.getStorageDirectoryPath(),
                                       remoteRNP.getStorageDirectorySize());
            }
            addFixedParams = true;

            logger.log(Level.INFO,
                       "{0} repair of repNodeParams for {1}/{2} set " +
                       "storagedir {3}",
                       new Object[]{who, correctSNId, rnId,
                                    fixedRNP.getStorageDirectoryPath()});
        }

        return correctCommonParams(params, rnp, addFixedParams, fixedRNP, rnId,
                                   topo, correctJEHAHostPort, correctHelpers);
    }

    private ChangedParams correctCommonParams(Parameters params,
                                              GroupNodeParams commonParams,
                                              boolean addFixedParams,
                                              GroupNodeParams fixedParams,
                                              ResourceId resId,
                                              Topology topo,
                                              String correctJEHAHostPort,
                                              String correctHelpers) {

        Set<RepNodeParams> needUpdate = new HashSet<>();
        Set<ArbNodeParams> arbNeedUpdate = new HashSet<>();

        /* Is its HA address correct? */
        if (!commonParams.getJENodeHostPort().equals(correctJEHAHostPort)) {
            fixedParams.setJENodeHostPort(correctJEHAHostPort);
            addFixedParams = true;
        }

        /* Are the helpers correct? */
        if (helperMismatch(commonParams.getJEHelperHosts(), correctHelpers)) {
            fixedParams.setJEHelperHosts(correctHelpers);
            addFixedParams = true;
        }

        /* Note that we always assume that this RN should be enabled */
        if (commonParams.isDisabled()) {
            fixedParams.setDisabled(false);
            addFixedParams = true;
        }

        /* Get the rep group id */
        RepGroupId rgId;
        if (resId.getType() == ResourceType.REP_NODE) {
            rgId = topo.get((RepNodeId)resId).getRepGroupId();
            if (addFixedParams) {
                needUpdate.add((RepNodeParams)fixedParams);
            }
        } else {
            rgId = topo.get((ArbNodeId)resId).getRepGroupId();
            if (addFixedParams) {
                arbNeedUpdate.add((ArbNodeParams)fixedParams);
            }
        }

        /* See if any peer RNs need their helper hosts updated */
        for (RepNode peer : topo.get(rgId).getRepNodes()) {
            if (peer.getResourceId().equals(resId)) {
                continue;
            }
            RepNodeParams peerRNP = params.get(peer.getResourceId());
            if (helperMismatch(peerRNP.getJEHelperHosts(),
                               correctHelpers)) {
                RepNodeParams newRNP = new RepNodeParams(peerRNP);
                newRNP.setJEHelperHosts(correctHelpers);
                needUpdate.add(newRNP);
            }
        }

        /* See if any peer ANs need their helper hosts updated */
        for (ArbNode peer : topo.get(rgId).getArbNodes()) {
            if (peer.getResourceId().equals(resId)) {
                continue;
            }
            ArbNodeParams peerANP = params.get(peer.getResourceId());
            if (helperMismatch(peerANP.getJEHelperHosts(),
                               correctHelpers)) {
                ArbNodeParams newANP = new ArbNodeParams(peerANP);
                newANP.setJEHelperHosts(correctHelpers);
                arbNeedUpdate.add(newANP);
            }
        }

        return new ChangedParams(arbNeedUpdate, needUpdate);
    }

    /**
     * Generate a set of correct AN params for all nodes of this shard.
     */
    private ChangedParams correctANParams(Topology topo,
                                          Parameters params,
                                          ArbNodeId anId,
                                          StorageNodeId correctSNId,
                                          String correctJEHAHostPort,
                                          String correctHelpers) {
        /*
         * Do the params point at the right SN? If not, make a copy of the
         * params and fix its snId, and other attributes.
         */
        ArbNodeParams anp = params.get(anId);
        ArbNodeParams fixedANP = new ArbNodeParams(anp);
        boolean addFixedParams = false;
        if (!anp.getStorageNodeId().equals(correctSNId)) {
            fixedANP.setStorageNodeId(correctSNId);
            addFixedParams = true;

            logger.log(Level.INFO, "{0} repair of arbNodeParams for {1}/{2}",
                       new Object[]{who, correctSNId, anId});
        }

        return correctCommonParams(params, anp, addFixedParams, fixedANP, anId,
                                   topo, correctJEHAHostPort, correctHelpers);
    }

    /**
     * return true if the two helper host lists don't match
     */
    private boolean helperMismatch(String helperListA, String helperListB) {

        List<String> helpersA = ParameterUtils.helpersAsList(helperListA);
        List<String> helpersB = ParameterUtils.helpersAsList(helperListB);

        if (!helpersA.containsAll(helpersB)) {
            /* mismatch */
            return true;
        }

        if (!helpersB.containsAll(helpersA)) {
            /* mismatch */
            return true;
        }

        return false;
    }

    /**
     * Check whether the Admin DB's copy of RepNodeParams and the RN's version
     * match for the given shard, and update the RN if needed. Ignore any
     * connectivity issues; this method should succeed if possible, but should
     * not cause an error if not possible.
     *
     * Since this considers the Admin DB's copy to be authoritative, this
     * should only be used when we are sure that the Admin DB has been
     * previously validated and repaired if required.
     */
    private void ensureAdminDBAndRNParams(AbstractPlan plan,
                                          RepGroupId rgId) {
        Admin admin = plan.getAdmin();
        Topology topo = admin.getCurrentTopology();
        RegistryUtils regUtils =
            new RegistryUtils(topo, admin.getLoginManager(), logger);
        Parameters currentParams = admin.getCurrentParameters();

        /* Check all the RNs of the shard */
        for (RepNode rn : topo.get(rgId).getRepNodes()) {
            RepNodeId rnId = rn.getResourceId();
            StorageNodeId snId = rn.getStorageNodeId();
            RepNodeParams rnp = currentParams.get(rnId);
            try {
                RepNodeAdminAPI rna = regUtils.getRepNodeAdmin(rnId);
                LoadParameters remoteParams = rna.getParams();
                if (remoteParams == null) {
                    logger.log(Level.INFO,
                               "{0} admin/rn param check for {1} did not " +
                               "find remote params for {2}",
                               new Object[]{who, plan, rnId});
                    continue;
                }
                ParameterMap remoteCopy =
                    remoteParams.getMapByType(ParameterState.REPNODE_TYPE);
                if (remoteCopy.equals(rnp.getMap())) {
                    /* Nothing to do, they match */
                    continue;
                }
                /* Write new params to the SN */
                StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
                sna.newRepNodeParameters(rnp.getMap());

                /* Notify the RN that there are new params. */
                RepNodeAdminAPI rnAdmin = regUtils.getRepNodeAdmin(rnId);
                rnAdmin.newParameters();
            } catch (RemoteException | NotBoundException ignore) {
                logger.log(Level.INFO,
                        "{0} failed to reach {1}/{2} to ensure admin/rn params",
                        new Object[]{who, snId, rnId});
            }
        }
    }

   /**
     * Remove empty shards if this topology has no RNs whatsoever. Used
     * when the initial deploy topology has failed before any RN or
     * partitions have been made.
     * TODO: what if the initial deploy fails because not all RNS could be
     * made? Then verify needs the number of partitions/the target topo
     * to do some fixing. Or just use topo rebalance?
     */
    public void repairInitialEmptyShards(VerifyResults results,
                                         AbstractPlan plan) {
        Set<InsufficientRNs> insufficientRNs =
            filterViolations(results, InsufficientRNs.class);
        logger.log(Level.FINE,
                   "{0} : RemoveInitialEmptyShards: insufficientRNs = {1}",
                   new Object[] {who, insufficientRNs});
        if (insufficientRNs.isEmpty()) {
            return;
        }

        /*
         * This is not an initial deployment; some RNs exist. Use topo
         * rebalance to fix the problem.
         */
        Topology currentTopo = plan.getAdmin().getCurrentTopology();
        if (!currentTopo.getRepNodeIds().isEmpty()) {
            logger.log(Level.FINE,
                       "{0} : RemoveInitialEmptyShards: {1} RNs exist, " +
                       "try another repair approach",
                       new Object[] {who, insufficientRNs});
            return;
        }

        /*
         * In general, an insufficient number of RNs means that the rebalance
         * command should be rerun. If there are no RNS at all, then the
         * initial deployment failed, and we can safely assume that there
         * are no underlying JE HA groups anywhere.
         */
        Set<RepGroupId> shardIds = currentTopo.getRepGroupIds();
        boolean shardsRemoved = false;
        for (RepGroupId rgId : shardIds) {
            currentTopo.remove(rgId);
            shardsRemoved = true;
        }

        if (shardsRemoved) {
            logger.log(Level.INFO, "{0} for {1} removed empty shards {2}",
                       new Object[]{who, plan, shardIds});
            plan.getAdmin().saveTopo(currentTopo,  plan.getDeployedInfo(),
                                     plan);
        }
    }

    /**
     * Encapsulate the information used to choose an Admin fix.
     */
    public static class AdminLocationInput {

        /* The SN which houses this Admin, based on JEHA */
        private final StorageNodeId jeHASNId;

        /* The SN which houses this Admin, based on its AdminParams */
        private final StorageNodeId adminParamsSNId;

        /* true if the adminParamsSNId also has the Admin in its config.xm */
        private final Boolean remoteNewSNCorrect;

        public AdminLocationInput(StorageNodeId jEHASNId,
                                  StorageNodeId adminParamsSNId,
                                  Boolean remoteNewSNCorrect) {
            this.jeHASNId = jEHASNId;
            this.adminParamsSNId = adminParamsSNId;
            this.remoteNewSNCorrect = remoteNewSNCorrect;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime
                    * result
                    + ((adminParamsSNId == null) ? 0
                            : adminParamsSNId.hashCode());
            result = prime * result
                    + ((jeHASNId == null) ? 0 : jeHASNId.hashCode());
            result = prime
                    * result
                    + ((remoteNewSNCorrect == null) ? 0
                            : remoteNewSNCorrect.hashCode());
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
            if (!(obj instanceof AdminLocationInput)) {
                return false;
            }
            AdminLocationInput other = (AdminLocationInput) obj;
            if (adminParamsSNId == null) {
                if (other.adminParamsSNId != null) {
                    return false;
                }
            } else if (!adminParamsSNId.equals(other.adminParamsSNId)) {
                return false;
            }
            if (jeHASNId == null) {
                if (other.jeHASNId != null) {
                    return false;
                }
            } else if (!jeHASNId.equals(other.jeHASNId)) {
                return false;
            }
            if (remoteNewSNCorrect == null) {
                if (other.remoteNewSNCorrect != null) {
                    return false;
                }
            } else if (!remoteNewSNCorrect.equals(other.remoteNewSNCorrect)) {
                return false;
            }

            return true;
        }

        @Override
        public String toString() {
            return "AdminLocationInput [jeHASNId=" + jeHASNId
                    + ", adminParamsSNId=" + adminParamsSNId
                    + ", remoteJEHASNCorrect=" + remoteNewSNCorrect + "]";
        }
    }

    /*
     * Use these enums for the input to the RNLocationInput, to avoid confusion
     * from mixing up booleans.
     */
    enum TOPO_STATUS {HERE, GONE}
    enum CONFIG_STATUS {HERE, GONE}
    enum JEHA_STATUS {HERE, GONE}
    enum OTHERSN_STATUS{HERE, GONE}

    /**
     * Encapsulate the information used to choose a RN fix.
     */
    public static class RNLocationInput {

        /* if true, this service is on this SN, according to the topo. */
        private final boolean presentInTopo;
        /* if true, this service is on this SN, according to the config.xml. */
        private final boolean presentInSNConfig;

        /*
         * if we are able to get groupDB info, then jeHAKnown is true. If it's
         * false, then presentInJE HA has no meaning.
         */
        private final boolean jeHAKnown;
        /* if true, this service is on this SN, according to the JEHAGroupDB */
        private final boolean presentInJEHA;

        /* if true, this service is present in another SN config file */
        private final boolean otherSNKnown;
        private final boolean presentInOtherSNConfig;

        /*
         * Use when you know neither the JE HA group info nor what
         * other SNs hold.
         */
        RNLocationInput(TOPO_STATUS topoStatus,
                        CONFIG_STATUS configStatus) {
            this(topoStatus, configStatus, false, JEHA_STATUS.GONE, false,
                 OTHERSN_STATUS.GONE);
        }

        /* Use when you know the JE HA group info. */
        RNLocationInput(TOPO_STATUS topoStatus,
                        CONFIG_STATUS configStatus,
                        JEHA_STATUS jeHAStatus) {
            this(topoStatus, configStatus, true, jeHAStatus, false,
                 OTHERSN_STATUS.GONE);
        }

        /*
         * Use when you don't know the JE HA group info, but know what the
         * other SNs hold.
         */
        RNLocationInput(TOPO_STATUS topoStatus,
                        CONFIG_STATUS configStatus,
                        OTHERSN_STATUS otherSNStatus) {
            this(topoStatus, configStatus, false, JEHA_STATUS.GONE, true,
                 otherSNStatus);
        }

        RNLocationInput(TOPO_STATUS topoStatus,
                        CONFIG_STATUS configStatus,
                        boolean jeHAKnown,
                        JEHA_STATUS jeHAStatus,
                        boolean otherSNKnown,
                        OTHERSN_STATUS otherConfigStatus) {

            this.presentInTopo = topoStatus.equals(TOPO_STATUS.HERE);
            this.presentInSNConfig = configStatus.equals(CONFIG_STATUS.HERE);
            this.jeHAKnown = jeHAKnown;
            this.presentInJEHA = jeHAStatus.equals(JEHA_STATUS.HERE);
            this.otherSNKnown = otherSNKnown;
            this.presentInOtherSNConfig =
                otherConfigStatus.equals(OTHERSN_STATUS.HERE);

        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (jeHAKnown ? 1231 : 1237);
            result = prime * result + (otherSNKnown ? 1231 : 1237);
            result = prime * result + (presentInJEHA ? 1231 : 1237);
            result = prime * result + (presentInOtherSNConfig ? 1231 : 1237);
            result = prime * result + (presentInSNConfig ? 1231 : 1237);
            result = prime * result + (presentInTopo ? 1231 : 1237);
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
            if (!(obj instanceof RNLocationInput)) {
                return false;
            }
            RNLocationInput other = (RNLocationInput) obj;
            if (jeHAKnown != other.jeHAKnown) {
                return false;
            }
            if (otherSNKnown != other.otherSNKnown) {
                return false;
            }
            if (presentInJEHA != other.presentInJEHA) {
                return false;
            }
            if (presentInOtherSNConfig != other.presentInOtherSNConfig) {
                return false;
            }
            if (presentInSNConfig != other.presentInSNConfig) {
                return false;
            }
            if (presentInTopo != other.presentInTopo) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "LocationInput [presentInTopo=" + presentInTopo
                + ", presentInSNConfig=" + presentInSNConfig
                + ", jeHAKnown=" + jeHAKnown + ", presentInJEHA="
                + presentInJEHA + ", otherSNKnown=" + otherSNKnown
                + ", presentInOtherSNConfig=" + presentInOtherSNConfig
                + "]";
        }
    }

    /** Provides a remedy for a problem. */
    public abstract static class Remedy {

        Remedy() { }

        /** Returns the resource associated with this remedy. */
        abstract ResourceId getResourceId();

        /** Describes this remedy. */
        abstract String problemDescription();

        /**
         * Returns whether the situation associated with this remedy is OK and
         * there was actually no problem.
         */
        public boolean isOkay() { return false; }

        /**
         * Returns whether there was a problem that can be fixed automatically.
         * This method returns false if the was no problem.
         */
        boolean canFix() { return false; }

        /**
         * Fixes the problem.
         *
         * @param plan the plan performing the fix
         * @return true if the fix was applied or not needed, and false if the
         * fix failed or could not be applied
         * @throws UnsupportedOperationException if this is a problem that
         * cannot be fixed automatically
         */
        abstract boolean apply(AbstractPlan plan);

        /**
         * Adds information to the toString result about additional subclass
         * fields.
         */
        abstract void toStringInternal(StringBuilder builder);

        /** Returns a simple name for the type of the remedy. */
        String remedyType() { return getClass().getSimpleName(); }

        @Override
        public final String toString() {
            final StringBuilder builder = new StringBuilder();
            builder.append("Remedy [");
            builder.append("remedyType=").append(remedyType());
            toStringInternal(builder);
            builder.append("]");
            return builder.toString();
        }
    }

    /** A factory for creating remedies for RN or AN problems. */
    abstract static class RNRemedyFactory {
        abstract Remedy createRemedy(
            TopologyCheck topoCheck, RNLocationInput rnLocationInput,
            StorageNodeId snId, ResourceId resId, JEHAInfo jeHAInfo,
            StorageNodeId oldSNId, StorageDirectory storageDirectory);
    }

    /** Situation is OK, no fix is needed. */
    public static class OkayRemedy extends Remedy {
        static final RNRemedyFactory FACTORY = new RNRemedyFactory() {
            @Override
            OkayRemedy createRemedy(
                TopologyCheck topoCheck, RNLocationInput rnLocationInput,
                StorageNodeId snId, ResourceId resId, JEHAInfo jeHAInfo,
                StorageNodeId oldSNId, StorageDirectory storageDirectory) {
                return new OkayRemedy(resId, jeHAInfo);
            }
        };
        private final ResourceId resourceId;
        private final JEHAInfo jeHAInfo;
        OkayRemedy(ResourceId resourceId, JEHAInfo jeHAInfo) {
            checkNull("resourceId", resourceId);
            this.resourceId = resourceId;
            this.jeHAInfo = jeHAInfo;
        }
        @Override
        ResourceId getResourceId() { return resourceId; }
        @Override
        String problemDescription() { return "No problem with " + resourceId; }
        @Override
        public boolean isOkay() { return true; }
        @Override
        boolean apply(AbstractPlan plan) { return true; }
        @Override
        void toStringInternal(StringBuilder builder) {
            builder.append(", resourceId=").append(resourceId)
                .append(", jeHAInfo=").append(jeHAInfo);
        }
        public JEHAInfo getJEHAInfo() { return jeHAInfo; }
    }

    /** Manual work needed. */
    static class NoFixRemedy extends Remedy {
        static final RNRemedyFactory FACTORY = new RNRemedyFactory() {
            @Override
            NoFixRemedy createRemedy(
                TopologyCheck topoCheck, RNLocationInput rnLocationInput,
                StorageNodeId snId, ResourceId resId, JEHAInfo jeHAInfo,
                StorageNodeId oldSNId, StorageDirectory storageDirectory) {
                return new NoFixRemedy(rnLocationInput, snId, resId, jeHAInfo,
                                       oldSNId);
            }
        };
        private final RNLocationInput rnLocationInput;
        private final StorageNodeId snId;
        private final ResourceId resourceId;
        private final JEHAInfo jeHAInfo;
        private final StorageNodeId oldSNId;
        NoFixRemedy(ResourceId resourceId) {
            this(null, null, resourceId, null, null);
        }
        NoFixRemedy(RNLocationInput rnLocationInput, StorageNodeId snId,
                    ResourceId resourceId, JEHAInfo jeHAInfo,
                    StorageNodeId oldSNId) {
            checkNull("resourceId", resourceId);
            this.rnLocationInput = rnLocationInput;
            this.snId = snId;
            this.resourceId = resourceId;
            this.jeHAInfo = jeHAInfo;
            this.oldSNId = oldSNId;
        }
        @Override
        ResourceId getResourceId() { return resourceId; }
        @Override
        String problemDescription() {
            return "No automatic fix available for problem with " + resourceId;
        }
        @Override
        boolean apply(AbstractPlan plan) {
            throw new UnsupportedOperationException();
        }
        @Override
        void toStringInternal(StringBuilder builder) {
            if (rnLocationInput != null) {
                builder.append(", rnLocationInput=").append(rnLocationInput);
            }
            if (snId != null) {
                builder.append(", snId=").append(snId);
            }
            builder.append(", resourceId=").append(resourceId);
            if (jeHAInfo != null) {
                builder.append(", jeHAInfo=").append(jeHAInfo);
            }
            if (oldSNId != null) {
                builder.append(", oldSNId=").append(oldSNId);
            }
        }
    }

    /** User must run plan repair-topology. */
    static class RunRepairRemedy extends Remedy {
        private final ResourceId resourceId;
        RunRepairRemedy(ResourceId resourceId) {
            checkNull("resourceId", resourceId);
            this.resourceId = resourceId;
        }
        @Override
        ResourceId getResourceId() { return resourceId; }
        @Override
        String problemDescription() {
            return "Please run plan repair-topology to fix inconsistent" +
                " location metadata";
        }
        @Override
        boolean apply(AbstractPlan plan) {
            throw new UnsupportedOperationException();
        }
        @Override
        void toStringInternal(StringBuilder builder) {
            builder.append(", resourceId=").append(resourceId);
        }
    }

    /** Fix an RN problem. */
    abstract static class RNRemedy extends Remedy {
        final TopologyCheck topoCheck;
        final RNLocationInput rNLocationInput;
        final StorageNodeId snId;
        final ResourceId resId;
        final JEHAInfo jeHAInfo;
        final int repGroupId;

        RNRemedy(TopologyCheck topoCheck, RNLocationInput rNLocationInput,
                 StorageNodeId snId, ResourceId resId, JEHAInfo jeHAInfo) {
            this.topoCheck = topoCheck;
            this.rNLocationInput = rNLocationInput;
            this.snId = snId;
            checkNull("resId", resId);
            this.resId = resId;
            this.jeHAInfo = jeHAInfo;
            repGroupId = Utils.getRepGroupId(resId).getGroupId();
        }
        @Override
        ResourceId getResourceId() { return resId; }
        @Override
        final boolean apply(AbstractPlan plan) {
            final boolean result = applyInternal(plan);

            /*
             * Update admin DB and params if the fix was successful and things
             * weren't already OK
             */
            if (result && !isOkay()) {
                topoCheck.ensureAdminDBAndRNParams(
                    plan, new RepGroupId(repGroupId));
            }
            return result;
        }
        abstract boolean applyInternal(AbstractPlan plan);
        RNLocationInput getRNLocationInput() { return rNLocationInput; }
        StorageNodeId getSNId() { return snId; }

        RepNodeId getRNId() {
            return resId.getType().isRepNode() ? (RepNodeId)resId : null;
        }

        ArbNodeId getANId() {
            return resId.getType().isArbNode() ? (ArbNodeId)resId : null;
        }

        JEHAInfo getJEHAInfo() { return jeHAInfo; }
        @Override
        void toStringInternal(StringBuilder builder) {
            if (jeHAInfo != null) {
                builder.append(", jeHAInfo=").append(jeHAInfo);
            }
            if (snId != null) {
                builder.append(", snId=").append(snId);
            }
            builder.append(", resId=").append(resId);
            if (rNLocationInput != null) {
                builder.append(", rNLocationInput=").append(rNLocationInput);
            }
        }
    }

    /** Remove RN from topo/params for this SN. */
    static class ClearAdminConfigRemedy extends RNRemedy {
        final static RNRemedyFactory FACTORY = new RNRemedyFactory() {
            @Override
            ClearAdminConfigRemedy createRemedy(
                TopologyCheck topoCheck, RNLocationInput rnLocationInput,
                StorageNodeId snId, ResourceId resId, JEHAInfo jeHAInfo,
                StorageNodeId oldSNId, StorageDirectory storageDirectory) {
                return new ClearAdminConfigRemedy(
                    topoCheck, rnLocationInput, snId, resId, jeHAInfo);
            }
        };
        ClearAdminConfigRemedy(TopologyCheck topoCheck,
                               RNLocationInput rnLocationInput,
                               StorageNodeId snId, ResourceId resId,
                               JEHAInfo jeHAInfo) {
            super(topoCheck, rnLocationInput, snId, resId, jeHAInfo);
        }
        @Override
        String problemDescription() {
            return resId + " is present in Admin metadata and on " + snId +
                " configuration but has not been created. Must be removed" +
                " from metadata";
        }
        @Override
        boolean canFix() { return true; }
        @Override
        boolean applyInternal(AbstractPlan plan) {
            if (resId.getType().isRepNode()) {
                return topoCheck.repairWithClearRN(this, plan);
            }
            return topoCheck.repairWithClearAN(this, plan);
        }
    }

    /** Tell an SN to create or start an RN. */
    static class CreateRNRemedy extends RNRemedy {
        final static RNRemedyFactory FACTORY = new RNRemedyFactory() {
            @Override
            CreateRNRemedy createRemedy(
                TopologyCheck topoCheck, RNLocationInput rnLocationInput,
                StorageNodeId snId, ResourceId resId, JEHAInfo jeHAInfo,
                StorageNodeId oldSNId, StorageDirectory storageDirectory) {
                return new CreateRNRemedy(
                    topoCheck, rnLocationInput, snId, resId, jeHAInfo);
            }
        };
        CreateRNRemedy(TopologyCheck topoCheck,
                       RNLocationInput rnLocationInput, StorageNodeId snId,
                       ResourceId resId, JEHAInfo jeHAInfo) {
            super(topoCheck, rnLocationInput, snId, resId, jeHAInfo);
        }
        @Override
        String problemDescription() {
            return "Must create or start " + resId + " on this SN.";
        }
        @Override
        boolean canFix() { return true; }
        @Override
        boolean applyInternal(AbstractPlan plan) {
            if (resId.getType().isRepNode()) {
                return topoCheck.repairStartRN(this, plan);
            }
            return topoCheck.repairStartAN(this, plan);
        }
    }

    /** User should run plan stop-service. */
    static class DisableRNRemedy extends RNRemedy {
        static final RNRemedyFactory FACTORY = new RNRemedyFactory() {
            @Override
            DisableRNRemedy createRemedy(
                TopologyCheck topoCheck, RNLocationInput rnLocationInput,
                StorageNodeId snId, ResourceId resId, JEHAInfo jeHAInfo,
                StorageNodeId oldSNId, StorageDirectory storageDirectory) {
                return new DisableRNRemedy(
                    topoCheck, rnLocationInput, snId, resId, jeHAInfo);
            }
        };
        DisableRNRemedy(TopologyCheck topoCheck,
                        RNLocationInput rnLocationInput, StorageNodeId snId,
                        ResourceId resId, JEHAInfo jeHAInfo) {
            super(topoCheck, rnLocationInput, snId, resId, jeHAInfo);
        }
        @Override
        String problemDescription() {
            return resId + " should be stopped and disabled on " + snId;
        }
        @Override
        boolean applyInternal(AbstractPlan plan) {
            throw new UnsupportedOperationException();
        }
    }

    /** Remove RN from this SN. */
    static class RemoveRNRemedy extends RNRemedy {
        static final RNRemedyFactory FACTORY = new RNRemedyFactory() {
            @Override
            RemoveRNRemedy createRemedy(
                TopologyCheck topoCheck, RNLocationInput rnLocationInput,
                StorageNodeId snId, ResourceId resId, JEHAInfo jeHAInfo,
                StorageNodeId oldSNId, StorageDirectory storageDirectory) {
                return new RemoveRNRemedy(
                    topoCheck, rnLocationInput, snId, resId, jeHAInfo);
            }
        };
        RemoveRNRemedy(TopologyCheck topoCheck,
                       RNLocationInput rnLocationInput, StorageNodeId snId,
                       ResourceId resId, JEHAInfo jeHAInfo) {
            super(topoCheck, rnLocationInput, snId, resId, jeHAInfo);
        }
        @Override
        String problemDescription() {
            return resId + " must be removed from " + snId;
        }
        @Override
        boolean canFix() { return true; }
        @Override
        boolean applyInternal(AbstractPlan plan) {
            return topoCheck.repairRemove(this, plan);
        }
    }

    /**
     * Revert RN from new SN back to old SN, or clean up for it to stay at the
     * new SN.
     */
    /* TODO: Maybe have a separate remedy for leaving the RN in the new SN? */
    static class RevertRNRemedy extends RNRemedy {
        static final RNRemedyFactory FACTORY = new RNRemedyFactory() {
            @Override
            RevertRNRemedy createRemedy(
                TopologyCheck topoCheck, RNLocationInput rnLocationInput,
                StorageNodeId snId, ResourceId resId, JEHAInfo jeHAInfo,
                StorageNodeId oldSNId, StorageDirectory newStorageDirectory) {
                return new RevertRNRemedy(
                    topoCheck, rnLocationInput, snId, resId, jeHAInfo,
                    oldSNId, newStorageDirectory);
            }
        };
        final StorageNodeId oldSNId;
        final StorageDirectory newStorageDirectory;
        RevertRNRemedy(TopologyCheck topoCheck,
                       RNLocationInput rnLocationInput, StorageNodeId snId,
                       ResourceId resId, JEHAInfo jeHAInfo,
                       StorageNodeId oldSNId,
                       StorageDirectory newStorageDirectory) {
            super(topoCheck, rnLocationInput, snId, resId, jeHAInfo);
            this.oldSNId = oldSNId;
            this.newStorageDirectory = newStorageDirectory;
        }
        @Override
        String problemDescription() {
            return resId + " must be moved back to its original hosting SN";
        }
        @Override
        boolean canFix() { return true; }
        @Override
        boolean applyInternal(AbstractPlan plan) {
            return topoCheck.repairRevert(this, plan);
        }
        @Override
        void toStringInternal(StringBuilder builder) {
            super.toStringInternal(builder);
            if (oldSNId != null) {
                builder.append(", oldSNId=").append(oldSNId);
            }
            if (newStorageDirectory != null) {
                builder.append(", newStorageDirPath=").
                        append(newStorageDirectory.getPath());
                builder.append(", newStorageDirSize=").
                        append(newStorageDirectory.getSize());
            }
        }
        StorageNodeId getOldSNId() {
            return oldSNId;
        }

        StorageDirectory getNewStorageDir() {
            return newStorageDirectory;
        }
    }

    /**
     * Update the RN parameters to fix differences or if the node type needs to
     * be changed to match the zone.
     */
    public static class UpdateRNParamsRemedy extends RNRemedy {
        public UpdateRNParamsRemedy(TopologyCheck topoCheck, RepNodeId rnId) {
            super(topoCheck, null, null, rnId, null);
        }
        @Override
        String problemDescription() {
            return "Change " + resId + " parameters to match saved values or" +
                " its zone type";
        }
        @Override
        boolean canFix() { return true; }
        @Override
        boolean applyInternal(AbstractPlan plan) {
            return topoCheck.repairRNParams(this, plan);
        }
    }

    /**
     * Update the AN parameters to fix differences.
     */
    public static class UpdateANParamsRemedy extends RNRemedy {
        public UpdateANParamsRemedy(TopologyCheck topoCheck, ArbNodeId anId) {
            super(topoCheck, null, null, anId, null);
        }
        @Override
        String problemDescription() {
            return "Change " + resId + " parameters to match saved values.";
        }
        @Override
        boolean canFix() { return true; }
        @Override
        boolean applyInternal(AbstractPlan plan) {
            return topoCheck.repairANParams(plan, (ArbNodeId)resId);
        }
    }

    /** Fix an admin problem. */
    abstract static class AdminRemedy extends Remedy {
        final TopologyCheck topoCheck;
        final AdminLocationInput adminLocationInput;
        final AdminId adminId;
        final JEHAInfo jeHAInfo;
        AdminRemedy(TopologyCheck topoCheck,
                    AdminLocationInput adminLocationInput, AdminId adminId,
                    JEHAInfo jeHAInfo) {
            this.topoCheck = topoCheck;
            this.adminLocationInput = adminLocationInput;
            checkNull("adminId", adminId);
            this.adminId = adminId;
            this.jeHAInfo = jeHAInfo;
        }
        @Override
        ResourceId getResourceId() { return adminId; }
        @Override
        void toStringInternal(StringBuilder builder) {
            if (jeHAInfo != null) {
                builder.append(", jeHAInfo=").append(jeHAInfo);
            }
            if (adminLocationInput != null) {
                builder.append(", adminLocationInput=")
                       .append(adminLocationInput);
            }
            builder.append(", adminId=").append(adminId);
        }
        AdminId getAdminId() { return adminId; }
    }

    /** Make the Admin's JE HA location consistent. */
    static class FixAdminRemedy extends AdminRemedy {
        FixAdminRemedy(TopologyCheck topoCheck,
                       AdminLocationInput adminLocationInput,
                       AdminId adminId, JEHAInfo jeHAInfo) {
            super(topoCheck, adminLocationInput, adminId, jeHAInfo);
        }
        @Override
        String problemDescription() {
            return "Ensure that the Admin's location metadata is consistent";
        }
        @Override
        boolean canFix() { return true; }
        @Override
        boolean apply(AbstractPlan plan) {
            return topoCheck.repairAdmin(this, plan);
        }
    }

    /**
     * Update the admin parameters to fix differences or if the admin type
     * needs to be changed to match the datacenter.
     */
    public static class UpdateAdminParamsRemedy extends AdminRemedy {
        public UpdateAdminParamsRemedy(TopologyCheck topoCheck,
                                       AdminId adminId) {
            super(topoCheck, null, adminId, null);
        }
        @Override
        String problemDescription() {
            return "Change " + adminId + " parameters to match saved values" +
                " or its zone type";
        }
        @Override
        boolean canFix() { return true; }
        @Override
        boolean apply(AbstractPlan plan) {
            return topoCheck.repairAdminParams(this, plan);
        }
    }

   /**
    * Repairs the helper hosts to match the RNs and ANs specified in the
    * topology.
    */
    public static class TopoHelperRemedy extends Remedy {
        final TopologyCheck topoCheck;
        private final ResourceId resourceId;
        public TopoHelperRemedy(TopologyCheck topoCheck,
                                ResourceId resourceId ) {
            checkNull("resourceId", resourceId);
            this.resourceId = resourceId;
            this.topoCheck = topoCheck;
        }

        @Override
        ResourceId getResourceId() { return resourceId; }
        @Override
        String problemDescription() {
            return "Helper parameters do not "+
                   "match Topology for " + resourceId; }
        @Override
        boolean apply(AbstractPlan plan) {
            return topoCheck.repairHelpers(plan, resourceId);
        }
        @Override
        boolean canFix() { return true; }
        @Override
        void toStringInternal(StringBuilder builder) {
            builder.append(", resourceId=").append(resourceId);
        }
    }

    /**
     * Process an SN's config.xml - to generate a list of the services it
     * thinks it hosts.
     */
    private SNServices processRemoteInfo(StorageNodeId snId,
                                         LoadParameters remoteParams) {

        /* Find all the RNs that are present in the SN's config file */
        List<ParameterMap> rnMaps =
            remoteParams.getAllMaps(ParameterState.REPNODE_TYPE);

        Set<RepNodeId> allRNs = new HashSet<>();
        for (ParameterMap map : rnMaps) {
            RepNodeId rnId = RepNodeId.parse(map.getName());
            allRNs.add(rnId);
        }

        /* Find all the Admins that are present in the SN's config file.*/
        ParameterMap adminMap =
            remoteParams.getMapByType(ParameterState.ADMIN_TYPE);
        AdminId aid = null;
        if (adminMap != null) {
            aid = new AdminId(adminMap.getOrZeroInt(ParameterState.AP_ID));
        }

        List<ParameterMap> arbMaps =
            remoteParams.getAllMaps(ParameterState.ARBNODE_TYPE);

        Set<ArbNodeId> allARBs = new HashSet<>();
        for (ParameterMap map : arbMaps) {
            ArbNodeId arbId = ArbNodeId.parse(map.getName());
            allARBs.add(arbId);
        }

        return new SNServices(snId, allRNs, allARBs, aid, remoteParams);
    }

    /**
     * Info derived from the JEHA group db, about a node's hostname/port
     * and its peers.
     */
    public static class JEHAInfo {
        private final StorageNodeId translatedSNId;
        private final ReplicationNode jeReplicationNode;
        private final String groupWideHelperHosts;

        JEHAInfo(StorageNodeId translatedSNId,
                 ReplicationNode jeReplicationNode,
                 String groupWideHelperHosts) {
            this.translatedSNId = translatedSNId;
            this.jeReplicationNode = jeReplicationNode;
            this.groupWideHelperHosts = groupWideHelperHosts;
        }

        public StorageNodeId getSNId() {
            return translatedSNId;
        }

        String getHostPort() {
            return HostPortPair.getString(jeReplicationNode.getHostName(),
                                          jeReplicationNode.getPort());
        }

        String getHelpers() {
            return groupWideHelperHosts;
        }

        @Override
        public String toString() {
            return "JE derivedSN = " + translatedSNId +
                " RepNode=" + jeReplicationNode +
                " helpers=" + groupWideHelperHosts;
        }
    }

    class ChangedParams {
        private final Set<ArbNodeParams> anParams;
        private final Set<RepNodeParams> rnParams;
        ChangedParams(Set<ArbNodeParams> anp, Set<RepNodeParams> rnp) {
            anParams = anp;
            rnParams = rnp;
        }
        Set<ArbNodeParams> getANP() {
            return anParams;
        }
        Set<RepNodeParams> getRNP() {
            return rnParams;
        }

    }
}
