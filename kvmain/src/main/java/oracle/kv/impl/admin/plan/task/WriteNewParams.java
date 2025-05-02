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
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * A task for asking a storage node to write a new configuration file.
 */
public class WriteNewParams extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    /**
     * If true, modify the behavior of writeNewParams to check for removed
     * parameters. For testing.
     */
    public static volatile boolean testEnableCheckRemoved;

    private final AbstractPlan plan;
    private final ParameterMap newParams;
    private final StorageNodeId targetSNId;
    private final RepNodeId rnid;
    private final boolean continuePastError;
    private transient boolean currentContinuePastError;

    public WriteNewParams(AbstractPlan plan,
                          ParameterMap newParams,
                          RepNodeId rnid,
                          StorageNodeId targetSNId,
                          boolean continuePastError) {
        super();
        this.plan = plan;
        this.newParams = newParams;
        this.rnid = rnid;
        this.targetSNId = targetSNId;
        this.continuePastError = continuePastError;
        this.currentContinuePastError = continuePastError;
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
        State retval = State.SUCCEEDED;
        currentContinuePastError = false;
        final Admin admin = plan.getAdmin();
        final Topology topo = admin.getCurrentTopology();
        if (topo.get(rnid) == null) {
            return retval;
        }
        writeToDB(plan, newParams, rnid, targetSNId, false /* checkRemoved */);
        currentContinuePastError = continuePastError;
        try {
            retval =
                UpdateRepNodeParams.update(plan, null, rnid, false, false);
        } catch (RemoteException | NotBoundException e) {
            if (currentContinuePastError) {
                plan.getLogger().log(Level.WARNING, "Unable to access {0} " +
                                     "to change parameters for " +
                                     "{1} due to exception {2}",
                                     new Object[]{targetSNId, rnid, e});
            } else {
                throw e;
            }
        }
        return retval;
    }

    public static boolean writeNewParams(AbstractPlan plan,
                                         ParameterMap newParams,
                                         RepNodeId rnid,
                                         StorageNodeId targetSNId)
        throws Exception {

        return writeNewParams(plan, newParams, rnid, targetSNId,
                              false /* checkRemoved */);
    }

    public static boolean writeNewParams(AbstractPlan plan,
                                         ParameterMap newParams,
                                         RepNodeId rnid,
                                         StorageNodeId targetSNId,
                                         boolean checkRemoved)
        throws Exception {

        ParameterMap rnMap =
            writeToDB(plan, newParams, rnid, targetSNId, checkRemoved);
        if (rnMap == null) {
            return false;
        }
        writeConfig(plan.getAdmin(), plan.getLogger(), targetSNId, rnMap);
        return true;
    }

    /**
     * Write the new RN parameters to the adminDB.
     * @param rnid Needs to be the Id of a RepNode that still exists.
     */
    private static ParameterMap writeToDB(AbstractPlan pln,
                                          ParameterMap newParams,
                                          RepNodeId rnid,
                                          StorageNodeId targetSNId,
                                          boolean checkRemoved)
        throws RemoteException, NotBoundException {

        if (testEnableCheckRemoved) {
            checkRemoved = true;
        }

        final Admin admin = pln.getAdmin();
        final RepNodeParams rnp = admin.getRepNodeParams(rnid);
        final ParameterMap rnMap = rnp.getMap();
        final RepNodeParams newRnp = new RepNodeParams(newParams);
        newRnp.setRepNodeId(rnid);
        final ParameterMap diff = rnMap.diff(newParams, true);
        final ParameterMap removed = checkRemoved ?
            rnMap.missingIn(newParams, true /* notReadOnly */) :
            null;
        pln.getLogger().info(
            () -> String.format("%s changing params for %s: %s%s",
                                pln, rnid, diff,
                                ((removed != null) && !removed.isEmpty() ?
                                 ", removed: " + removed :
                                 "")));

        /*
         * Merge and store the changed rep node params in the admin db before
         * sending them to the SNA.
         */
        if (rnMap.merge(
                newParams, true /* notReadOnly */, checkRemoved) <= 0) {
            return null;
        }
        /* Check the parameters prior to writing them to the DB. */
        ParameterMap snMap =
            admin.getStorageNodeParams(targetSNId).getMap();
        String dbVersion =
           snMap.get(ParameterState.SN_SOFTWARE_VERSION).asString();
        KVVersion snVersion =
            dbVersion == null ? null : KVVersion.parseVersion(dbVersion);
        if (snVersion != null &&
            snVersion.compareTo(KVVersion.CURRENT_VERSION) == 0 &&
            !StorageNodeAgent.isFileSystemCheckRequired(rnMap)) {
            StorageNodeAgent.checkSNParams(rnMap,
                                           admin.getGlobalParams().getMap());
        } else {
            RegistryUtils registryUtils =
                new RegistryUtils(admin.getCurrentTopology(),
                                  admin.getLoginManager(),
                                  pln.getLogger());
            StorageNodeAgentAPI sna =
                registryUtils.getStorageNodeAgent(targetSNId);
            try {
                sna.checkParameters(rnMap, rnid);
            } catch (UnsupportedOperationException ignore) {
                /*
                 * If UOE, the SN is not yet upgraded to a version that
                 * supports this check, so just ignore
                */
            }
        }
        admin.updateParams(rnp);
        return rnMap;
    }

    private static void writeConfig(Admin admin,
                                    Logger logger,
                                    StorageNodeId targetSNId,
                                    ParameterMap rnMap)
        throws RemoteException, NotBoundException {

        RegistryUtils registryUtils =
            new RegistryUtils(admin.getCurrentTopology(),
                              admin.getLoginManager(),
                              logger);
        StorageNodeAgentAPI sna =
            registryUtils.getStorageNodeAgent(targetSNId);
        /* Ask the SNA to write a new configuration file. */

        sna.newRepNodeParameters(rnMap);
    }

    /* Lock the target SN and RN */
    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockSN(planner, plan, targetSNId);
        LockUtils.lockRN(planner, plan, rnid);
    }

    @Override
    public boolean continuePastError() {
        return currentContinuePastError;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(rnid);
    }
}
