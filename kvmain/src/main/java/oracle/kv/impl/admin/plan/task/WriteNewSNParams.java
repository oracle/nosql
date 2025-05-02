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

import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_MOUNT_POINTS;
import static oracle.kv.impl.param.ParameterState.BOOTSTRAP_RNLOG_MOUNT_POINTS;
import static oracle.kv.impl.param.ParameterState.COMMON_SN_ID;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.PlanLocksHeldException;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * A task for asking a storage node to write a new configuration file.
 */
public class WriteNewSNParams extends SingleJobTask {

    private static final long serialVersionUID = 1L;

    private final AbstractPlan plan;
    private final ParameterMap newParams;
    private final StorageNodeId targetSNId;
    private final boolean continuePastError;
    private transient boolean currentContinuePastError;

    public WriteNewSNParams(AbstractPlan plan,
                            StorageNodeId targetSNId,
                            ParameterMap newParams,
                            boolean continuePastError) {
        super();
        this.plan = plan;
        this.newParams = newParams;
        this.targetSNId = targetSNId;
        this.continuePastError = continuePastError;
        this.currentContinuePastError = continuePastError;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    @Override
    public void acquireLocks(Planner planner) throws PlanLocksHeldException {
        LockUtils.lockSN(planner, plan, targetSNId);
    }

    /**
     */
    @Override
    public State doWork()
        throws Exception {
        currentContinuePastError = continuePastError;

        /*
         * Store the changed params in the admin db before sending them to the
         * SNA.  Merge rather than replace in this path.  If nothing is changed
         * return.
         */
        final Admin admin = plan.getAdmin();
        final StorageNodeParams snp = admin.getStorageNodeParams(targetSNId);
        final Logger logger = plan.getLogger();
        ParameterMap snMap = snp.getMap();

        boolean updateAdminDB = true;
        boolean changeMounts = false;
        boolean changeRNMounts = false;
        final ParameterMap storageDirMap =
            newParams.getName().equals(BOOTSTRAP_MOUNT_POINTS) ? newParams :
            null;
        final ParameterMap rnLogDirMap =
            newParams.getName().equals(BOOTSTRAP_RNLOG_MOUNT_POINTS) ? newParams :
            null;
        if (storageDirMap != null) {
            logger.log(Level.INFO,
                       "{0} changing storage directories for {1}: {2}",
                       new Object[]{this, targetSNId, storageDirMap});
            /*
             * Snid may have been stored, remove it
             */
            storageDirMap.remove(COMMON_SN_ID);
            snp.setStorageDirMap(storageDirMap);
            changeMounts = true;
        } else if (rnLogDirMap != null) {
            logger.log(Level.INFO,
                    "{0} changing rn log directories for {1}: {2}",
                    new Object[]{this, targetSNId, rnLogDirMap});
            /*
             * Snid may have been stored, remove it
             */
            rnLogDirMap.remove(COMMON_SN_ID);
            snp.setRNLogDirMap(rnLogDirMap);
            changeRNMounts = true;
        } else {
            final ParameterMap diff = snMap.diff(newParams, true);

            updateAdminDB = snMap.merge(newParams, true) > 0;
            logger.log(Level.INFO,
                       "{0} changing these params for {1}: {2}",
                        new Object[]{this, targetSNId, diff});
        }

        /*
         * TODO : Yet to add support for change-admindir[size] and
         * change-rnlogdir[size]
         */

        StorageNodeAgentAPI sna = null;

        /* Only one or the other map will be non-null */
        final ParameterMap newMap = (changeMounts ? storageDirMap :
                                        changeRNMounts ? rnLogDirMap : snMap);

        if (updateAdminDB) {
            currentContinuePastError = false;
            /* Check the parameters prior to writing them to the DB. */
            String dbVersion =
                snMap.get(ParameterState.SN_SOFTWARE_VERSION).asString();
            KVVersion snVersion =
                dbVersion == null ? null : KVVersion.parseVersion(dbVersion);

            if (snVersion != null &&
                snVersion.compareTo(KVVersion.CURRENT_VERSION) == 0 &&
                !StorageNodeAgent.isFileSystemCheckRequired(newParams)) {
                StorageNodeAgent.checkSNParams(newMap,
                                               admin.getGlobalParams().getMap());
             } else {
                 RegistryUtils registryUtils =
                     new RegistryUtils(admin.getCurrentTopology(),
                                       admin.getLoginManager(),
                                       plan.getLogger());
                 sna =
                     registryUtils.getStorageNodeAgent(targetSNId);
                 try {
                     sna.checkParameters(newMap, targetSNId);
                 } catch (UnsupportedOperationException ignore) {
                     /*
                      * If UOE, the SN is not yet upgraded to a version that
                      * supports this check, so just ignore
                      */
                  }
             }

            /* Update the admin DB */
            admin.updateParams(snp, null);
        }

        currentContinuePastError = continuePastError;

        if (sna == null) {
            try {
            RegistryUtils registryUtils =
                new RegistryUtils(admin.getCurrentTopology(),
                                  admin.getLoginManager(),
                                  logger);
             sna = registryUtils.getStorageNodeAgent(targetSNId);
            } catch (RemoteException | NotBoundException e) {
                if (continuePastError) {
                    logger.log(Level.WARNING,
                               "{0} failed changing params for {1} " +
                               "due to exception {2}.",
                               new Object[]{this, targetSNId, e});
                    return State.SUCCEEDED;
                }
                throw e;
            }
        }
        sna.newStorageNodeParameters(newMap);

        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return currentContinuePastError;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(targetSNId);
    }
}
