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

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * A task for asking a storage node to write the new global parameters to
 * configuration file.
 */
public class WriteNewGlobalParams extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final boolean continuePastError;
    private final AbstractPlan plan;
    private ParameterMap newParamMap;
    private final StorageNodeId targetSNId;
    private transient boolean currentContinuePastError;

    public WriteNewGlobalParams(AbstractPlan plan,
                                ParameterMap newParams,
                                StorageNodeId targetSNId,
                                boolean continuePastError) {
        this.plan = plan;
        this.newParamMap = newParams;
        this.continuePastError = continuePastError;
        this.targetSNId = targetSNId;
        currentContinuePastError = continuePastError;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public boolean continuePastError() {
        return currentContinuePastError;
    }

    @Override
    public State doWork() throws Exception {
        currentContinuePastError = false;
        boolean noUpdateAdminDb = false;
        final Admin admin = plan.getAdmin();
        final Parameters params = admin.getCurrentParameters();
        final GlobalParams gp = params.getGlobalParams();
        final ParameterMap gpMap = gp.getMap();

        final RegistryUtils registryUtils =
            new RegistryUtils(admin.getCurrentTopology(),
                              admin.getLoginManager(),
                              plan.getLogger());
        StorageNodeAgentAPI sna = null;

        if (newParamMap == null) {
            newParamMap = gp.getMap();
            noUpdateAdminDb = true;
        }

        final ParameterMap diff =
            gpMap.diff(newParamMap, true /* notReadOnly */);
        plan.getLogger().log(Level.INFO,
                             "{0} changing Global params for {1}: {2}",
                             new Object[]{this, targetSNId, diff});

        /*
         * Merge and store the changed global params in the admin db, and then
         * send them to the SNA.
         * Note that mergeCount is only an indicator of whether the adminDB
         * contains these parameter values. MergeCount doesn't tell us whether
         * the target SN already has these parameter settings. For example, the
         * adminDB may be updated from an earlier call to write new global
         * params for a different SN, and mergeCount would be 0, but this
         * target SN might still need an update.
         */
        final int mergedCount = gpMap.merge(newParamMap, true /*notReadOnly*/);

        final ParameterMap snParams = params.get(targetSNId).getMap();

        String dbVersion =
            snParams.get(ParameterState.SN_SOFTWARE_VERSION).asString();

        boolean doStaticCheck = false;
        if (dbVersion != null) {
           KVVersion snVersion = KVVersion.parseVersion(dbVersion);
           if (snVersion.compareTo(KVVersion.CURRENT_VERSION) == 0) {
               doStaticCheck = true;
           }
        }
        if (doStaticCheck) {
            StorageNodeAgent.checkGlobalParams(gpMap, snParams);
        } else {
            try {
                sna = registryUtils.getStorageNodeAgent(targetSNId);
                /* Check the parameters prior to writing them to the DB. */
                sna.checkParameters(gpMap, null);
            } catch (UnsupportedOperationException ignore) {
                /*
                 * If UOE, the SN is not yet upgraded to a version that
                 * supports this check, so just ignore
                 */
            }
        }

        /* The AdminDB needs updating */
        if (!noUpdateAdminDb && mergedCount > 0) {
            admin.updateParams(gp);
        }

        currentContinuePastError = continuePastError;

        /*
         * Send new parameters to the SNA, ask it to write a new configuration
         * file.
         */
        try {
            if (sna == null) {
                sna = registryUtils.getStorageNodeAgent(targetSNId);
            }
            sna.newGlobalParameters(gpMap);
         } catch (RemoteException | NotBoundException e) {
            if (currentContinuePastError) {
                plan.getLogger().log(
                    Level.WARNING,
                    "{0} could not access SNA in order to change " +
                    "Global parameters for {1} due to exception {2} ",
                                new Object[]{this, targetSNId, e});
                return State.ERROR;
            }
            throw e;
        }

        return State.SUCCEEDED;
    }

    /* Lock the target SN */
    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockSN(planner, plan, targetSNId);
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(targetSNId);
    }
}
