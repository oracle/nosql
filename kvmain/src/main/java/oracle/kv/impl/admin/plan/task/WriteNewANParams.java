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
import java.rmi.RemoteException;
import java.util.logging.Level;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.VerifyConfiguration;
import oracle.kv.impl.admin.CommandResult.CommandFails;
import oracle.kv.impl.admin.VerifyConfiguration.CompareParamsResult;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.ErrorMessage;

/**
 * A task for asking a storage node to write a new configuration file.
 */
public class WriteNewANParams extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final ParameterMap newParams;
    private final StorageNodeId targetSNId;
    private final ArbNodeId anid;
    private final boolean continuePastError;
    private transient boolean currentContinuePastError;

    public WriteNewANParams(AbstractPlan plan,
                            ParameterMap newParams,
                            ArbNodeId anid,
                            StorageNodeId targetSNId,
                            boolean continuePastError) {
        super();
        this.plan = plan;
        this.newParams = newParams;
        this.anid = anid;
        this.targetSNId = targetSNId;
        this.continuePastError = continuePastError;
        currentContinuePastError = continuePastError;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    /**
     */
    @Override
    public State doWork()
        throws Exception {
        currentContinuePastError = false;
        final Admin admin = plan.getAdmin();
        final ArbNodeParams anp = admin.getArbNodeParams(anid);
        final ParameterMap anMap = anp.getMap();
        final ArbNodeParams newAnp = new ArbNodeParams(newParams);
        newAnp.setArbNodeId(anid);
        final ParameterMap diff = anMap.diff(newParams, true);
        if (!diff.isEmpty()) {
            plan.getLogger().log(Level.INFO,
                                 "{0} changing params for {1}: {2}",
                                 new Object[]{plan, anid, diff});

            /*
             * Merge and store the changed rep node params in the admin db
             * before sending them to the SNA.
             */
            anMap.merge(newParams, true);
            admin.updateParams(anp);
        }
        currentContinuePastError = continuePastError;
        try {
            return updateInternal(anid);
        } catch (RemoteException | NotBoundException e) {
            plan.getLogger().log(Level.WARNING,
                            "Unable change parameters for {0}. " +
                            "Could not access {1} due to exception {2}.",
                            new Object[]{anid, targetSNId, e});
            if (!currentContinuePastError) {
               throw e;
           }
           return State.SUCCEEDED;
       }
    }

    /*
     * Suppress null warnings, since, although the code makes the correct
     * checks for nulls, there seems to be no other way to make Eclipse happy
     */
    private State updateInternal(ArbNodeId anId)
        throws CommandFaultException, RemoteException, NotBoundException {

        /* Get admin DB parameters */
        final Admin admin = plan.getAdmin();
        final Parameters dbParams = admin.getCurrentParameters();
        final ArbNodeParams anDbParams = dbParams.get(anId);
        final Topology topo = admin.getCurrentTopology();
        final ArbNode thisAn = topo.get(anId);
        final StorageNodeId snId = thisAn.getStorageNodeId();

        /* Get SN configuration parameters */
        final RegistryUtils regUtils =
            new RegistryUtils(topo, plan.getLoginManager(), plan.getLogger());
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
            plan.getLogger().fine("Problem calling " + anId + ": " + e);
        }

        /*
         * Check if parameters file needs to be updated, if the RN needs to
         * read them, and if the RN needs to be restarted.
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
            String msg = "Unable to update parameters for " +
                                  anId + "due to missing parameters.";
            final CommandResult taskResult =new CommandFails(
                msg, ErrorMessage.NOSQL_5400,
                CommandResult.TOPO_PLAN_REPAIR);
            setTaskResult(taskResult);
            return State.ERROR;
        }

        /* No diffs */
        if (combinedCompare == CompareParamsResult.NO_DIFFS) {
            return State.SUCCEEDED;
        }

        if (snCompare != CompareParamsResult.NO_DIFFS) {
            plan.getLogger().fine("Updating AN config parameters");
            sna.newArbNodeParameters(anDbParams.getMap());
        }

        if (serviceCompare == CompareParamsResult.DIFFS) {
            plan.getLogger().fine("Notify AN of new parameters");
            regUtils.getArbNodeAdmin(anId).newParameters();

        } else {

            /*
             * Stop running node in preparation for restarting it. Node may
             * not be running so ignore stop errors. Will wait until we
             * try to start the node to report errors.
             */
            if (serviceCompare == CompareParamsResult.DIFFS_RESTART) {
                try {
                     Utils.stopAN(plan, snId, anId);
                 } catch (NotBoundException | RemoteException e) {
                     /* Ignore */
                 }
             }

            /*
             * Restart the node, or start it if it was not running and is
             * not disabled.
             */
            if ((serviceCompare == CompareParamsResult.DIFFS_RESTART) ||
                ((serviceParams == null) && !anDbParams.isDisabled())) {
                try {
                    Utils.startAN(plan, snId, anId);
                    return Utils.waitForNodeState(plan, anId,
                                                  ServiceStatus.RUNNING);
                } catch (Exception e) {
                    throw new CommandFaultException(
                        e.getMessage(), e, ErrorMessage.NOSQL_5400,
                        CommandResult.PLAN_CANCEL);
                }
            }
        }
            return State.SUCCEEDED;
    }

    /* Lock the target SN and AN */
    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockSN(planner, plan, targetSNId);
        LockUtils.lockAN(planner, plan, anid);
    }

    @Override
    public boolean continuePastError() {
        return currentContinuePastError;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(anid);
    }
}
