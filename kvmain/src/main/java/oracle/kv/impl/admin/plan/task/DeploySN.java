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

package oracle.kv.impl.admin.plan.task;

import java.rmi.NotBoundException;
import java.util.List;
import java.util.logging.Level;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.DeploySNPlan;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.sna.SNAFaultException;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.ErrorMessage;

/**
 * Deploy a new storage node.
 *
 * Note that we are saving the topology and params after the task successfully
 * creates and registers the SN. Most other plans save topology and params
 * before task execution, to make sure that the topology is consistent and
 * saved in the admin db before any kvstore component can access it. DeploySN
 * is a special case where it is safe to store the topology after execution
 * because there can be no earlier reference to the SN before the plan
 * finishes. This does mean that this task should be only be the only task used
 * in the plan, to ensure atomicity.
 */
public class DeploySN extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final DeploySNPlan plan;

    /* True if this is the first SN to be deployed in the store. */
    private final boolean isFirst;

    /**
     * Note that this task can only be used in the DeploySN plan, and only
     * one task can be executed within the plan, because of the post-task
     * execution save.
     */
    public DeploySN(DeploySNPlan plan, boolean isFirst) {
        super();
        this.plan = plan;
        this.isFirst = isFirst;
    }

    @Override
    protected DeploySNPlan getPlan() {
        return plan;
    }

    /**
     * Failure and interruption statuses are set by the PlanExecutor, to
     * generalize handling or exception cases.
     */
    @Override
    public State doWork()
        throws Exception {

        /* Check if this storage node is already running. */
        final StorageNodeId snId = plan.getStorageNodeId();
        final RegistryUtils trialRegistry =
            new RegistryUtils(plan.getTopology(),
                              plan.getAdmin().getLoginManager(),
                              plan.getLogger());
        StorageNodeAgentAPI sna;
        try {
            sna = trialRegistry.getStorageNodeAgent(snId);
            ServiceStatus serviceStatus = sna.ping().getServiceStatus();
            if (serviceStatus.isAlive()) {
                if (serviceStatus != ServiceStatus.WAITING_FOR_DEPLOY) {
                    /* This SNA is already up, nothing to do. */
                    plan.getLogger().log(Level.INFO,
                                         "{0} SNA already deployed, " +
                                         "had status of {1}",
                                         new Object[]{this, serviceStatus});
                    return Task.State.SUCCEEDED;
                }
            }
        } catch (NotBoundException notbound) {
            /*
             * It's fine for if the SNA is not bound already. It means that
             * this is truly the first time the DeploySN task has run.
             */
        }

        final StorageNodeParams inputSNP = plan.getInputStorageNodeParams();
        try {
            sna = RegistryUtils.getStorageNodeAgent(
                inputSNP.getHostname(),
                inputSNP.getRegistryPort(),
                GlobalParams.SNA_SERVICE_NAME,
                plan.getLoginManager(),
                plan.getLogger());
        } catch (NotBoundException e) {
            NotBoundException wrapped = new NotBoundException(
                "The bootstrap storage node service was not found, which may" +
                " mean the storage node is part of another store;" +
                " nested exception is:\n\t" + e);
            wrapped.initCause(e);
            throw new CommandFaultException(wrapped.getMessage(), wrapped,
                                            ErrorMessage.NOSQL_5400,
                                            CommandResult.PLAN_CANCEL);
        }

        /*
         * Contact the StorageNodeAgent, register it, and get information about
         * the SNA params.
         */
        final Admin admin = plan.getAdmin();
        final StorageNodeParams registrationParams =
                plan.getRegistrationParams();
        final GlobalParams gp = admin.getParams().getGlobalParams();

        if ((gp.getStoreVersion() == null) && isFirst) {
            gp.setStoreVersion(
                KVVersion.CURRENT_VERSION.getNumericVersionString());
        }

        if (gp.getStoreVersion() != null) {
            StorageNodeStatus sns = sna.ping();
            KVVersion snVersion = sns.getKVVersion();
            KVVersion storeVersion =
                KVVersion.parseVersion(gp.getStoreVersion());
            if (VersionUtil.compareMinorVersion(snVersion, storeVersion) < 0) {
                throw new CommandFaultException(
                    "Unable to deloy Storage node due to incompatible " +
                    "Storage node software version. " +
                    "Store version is " +
                    storeVersion.getNumericVersionString() +
                    " Storeage Node version is " +
                    snVersion.getNumericVersionString() + ".",
                    ErrorMessage.NOSQL_5400,
                    CommandResult.NO_CLEANUP_JOBS);
            }
        }

        final List<ParameterMap> snMaps;
        try {
            snMaps = sna.register(gp.getMap(), registrationParams.getMap(),
                                  isFirst);
        } catch (SNAFaultException e) {
            if (e.getCause() instanceof IllegalStateException) {
                throw new CommandFaultException(e.getMessage(), e,
                                                ErrorMessage.NOSQL_5100,
                                                CommandResult.NO_CLEANUP_JOBS);
            }
            throw e;
        }


        /*
         * If this is the first time the plan has been executed, save the
         * topology and params. If this is the first SN of the store, save
         * whether its address is loopback or not to make sure that this is
         * consistent across the store.  Any inconsistencies will result in an
         * exception from the register() call above.
         */
        final StorageNodeAgent.RegisterReturnInfo rri = new
            StorageNodeAgent.RegisterReturnInfo(snMaps);
        registrationParams.setInstallationInfo(rri.getBootMap(),
                                               rri.getStorageDirMap(),
                                               rri.getAdminDirMap(),
                                               rri.getRNLogDirMap(),
                                               isFirst,
                                               plan.getLogger());

        if (plan.isFirstExecutionAttempt()) {
            if (isFirst) {
                gp.setIsLoopback(rri.getIsLoopback());
                admin.saveTopoAndParams(plan.getTopology(),
                                        plan.getDeployedInfo(),
                                        registrationParams,
                                        gp, plan);
            } else {
                admin.saveTopoAndParams(plan.getTopology(),
                                        plan.getDeployedInfo(),
                                        registrationParams,
                                        null, plan);
            }
        }

        /* Add this SNA to monitoring. */
        admin.getMonitor().registerAgent(registrationParams.getHostname(),
                                         registrationParams.getRegistryPort(),
                                         plan.getStorageNodeId());
        return State.SUCCEEDED;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
        final StorageNodeParams snp = plan.getInputStorageNodeParams();
        /*
         * Note that storage node id  may not be set in snp yet, so we should
         * not use StorageNodeParams.displaySNIdAndHost
         */
        final DatacenterId zoneId =
            plan.getTopology().getDatacenterId(plan.getStorageNodeId());
        return super.getName(sb).append(" ").append(plan.getStorageNodeId())
                                .append("(").append(snp.getHostname())
                                .append(":").append(snp.getRegistryPort())
                                .append(")")
                                .append(" zone=" + zoneId);
    }

    @Override
    public boolean continuePastError() {
        return false;
    }
}
