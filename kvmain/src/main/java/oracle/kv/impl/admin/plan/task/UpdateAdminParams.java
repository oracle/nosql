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
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.AdminNotReadyException;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandResult.CommandFails;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.VerifyConfiguration;
import oracle.kv.impl.admin.VerifyConfiguration.CompareParamsResult;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.sna.SNAFaultException;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.ErrorMessage;

import com.sleepycat.je.rep.NodeType;

/**
 * A task, with associated utility methods, to update parameters for an admin
 * so that the values in the admin database, the SN configuration file, and the
 * in-memory values in the admin service all agree.  Only modifies the
 * configuration file if it is specifically the admin parameters that are
 * different; does not make modifications for global or SN parameters that also
 * apply to admins.
 */
public class UpdateAdminParams extends BasicUpdateParams {
    private static final long serialVersionUID = 1L;

    /* Number of retry attemts to get a master Admin */
    private static final int ADMIN_RETRY = 10;

    /* Delay between retry attempts */
    private static final long RETRY_DELAY_MS = 1000;

    private final AdminId adminId;
    private final boolean zoneIsOffline;

    public UpdateAdminParams(AbstractPlan plan,
                             AdminId adminId,
                             boolean zoneIsOffline) {
        super(plan);
        this.adminId = adminId;
        this.zoneIsOffline = zoneIsOffline;
    }

    @Override
    public State doWork() throws Exception {
        return update(plan, this, adminId, zoneIsOffline);
    }

    /**
     * Updates the admin database as needed so that the type of the specified
     * admin matches the type of the admin's zone, and performs the necessary
     * updates so that the SN configuration parameters and the in-memory
     * parameters for the admin match the values stored in the admin database.
     * Does nothing if no changes are needed.
     *
     * @param plan the containing plan
     * @param task the working task to set command result. Use null if no task
     * to set command result.
     * @param adminId the ID of the admin to update
     */
    public static State update(AbstractPlan plan, Task task, AdminId adminId)
        throws Exception {

        return update(plan, task, adminId, false);
    }

    private static State update(AbstractPlan plan,
                                Task task,
                                AdminId adminId,
                                boolean zoneIsOffline)
        throws Exception {

        /*
         * See the comment in UpdateRepNodeParams.update for information on the
         * order of modifications to parameters stored by JE and KV.
         *
         * One difference between the admin and RN versions of this method is
         * the way that the electable group size override is handled.  Because
         * the admin both uses and stores parameters, setting a parameter on
         * the admin master also modifies that parameter in the admin DB.  This
         * method maintains that existing behavior.  When setting the electable
         * group size override, the method sets the value on the master admin,
         * which changes it in both the admin DB and for the running service.
         * Because the admin DB is the authoritative source of parameter
         * information (except for the JE rep group DB, which only applies to
         * node types), this method arranges to clear the electable group size
         * from the admin DB whenever it runs, to make sure that any temporary
         * settings are cleared.
         */

        /* Get admin DB parameters */
        final Admin admin = plan.getAdmin();
        final Parameters dbParams = admin.getCurrentParameters();
        final AdminParams adminDbParams = dbParams.get(adminId);

        /*
         * Clear the electable group size override, if present, to undo any
         * temporary setting from a call that did not complete
         */
        if (adminDbParams.getElectableGroupSizeOverride() != 0) {
            plan.getLogger().fine("Clearing group size override");
            adminDbParams.setElectableGroupSizeOverride(0);
            admin.updateParams(adminDbParams);
        }

        final Topology topo = admin.getCurrentTopology();
        final StorageNodeId snId = adminDbParams.getStorageNodeId();
        final StorageNode sn = topo.getStorageNodeMap().get(snId);
        if (sn == null) {
            final String msg = "could not find " + snId;
            logError(plan, adminId, msg);
            if (task != null) {
                final CommandResult taskResult = new CommandFails(
                    msg, ErrorMessage.NOSQL_5400,
                    CommandResult.TOPO_PLAN_REPAIR);
                task.setTaskResult(taskResult);
            }
            return State.ERROR;
        }

        final Datacenter dc = topo.get(sn.getDatacenterId());
        final AdminType expectedAdminType =
            Utils.getAdminType(dc.getDatacenterType());
        final AdminType dbAdminType = adminDbParams.getType();
        final boolean updateDbAdminType =
            !expectedAdminType.equals(dbAdminType);

        /* If zone is offline, just update the admin DB if needed */
        if (zoneIsOffline) {
            if (updateDbAdminType) {
                plan.getLogger().fine("Updating admin type in admin DB");
                adminDbParams.setType(expectedAdminType);
                admin.updateParams(adminDbParams);
            }
            return State.SUCCEEDED;
        }

        /*
         * Update the admin type locally before performing comparisons, but
         * don't make the change persistent, so we can coordinate that change
         * with other changes.
         */
        if (updateDbAdminType) {
            adminDbParams.setType(expectedAdminType);
        }

        /* Get SN configuration parameters */
        final RegistryUtils regUtils =
            new RegistryUtils(topo, plan.getLoginManager(), plan.getLogger());
        final StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
        final LoadParameters configParams = sna.getParams();
        final CompareParamsResult snCompare =
            VerifyConfiguration.compareParams(configParams,
                                              adminDbParams.getMap());

        /*
         * Get in-memory parameters from admin, but make a copy so we don't
         * change the local version directly
         */
        final AdminParams masterAdminParams = new AdminParams(
            admin.getParams().getAdminParams().getMap().copy());
        final AdminId masterAdminId = masterAdminParams.getAdminId();
        LoadParameters serviceParams = null;
        if (adminId.equals(masterAdminId)) {
            /* Make local call when on the master */
            serviceParams = admin.getAllParams();
        } else {
            Exception problem = null;
            CommandServiceAPI cs = null;

            /*
             * If there is a problem with getting the Admin or the Admin
             * is not ready, wait and retry
             */
            for (int i = 0; i < ADMIN_RETRY; i++) {
                try {
                    if (cs == null) {
                        cs = regUtils.getAdmin(snId);
                    }
                    serviceParams = cs.getParams();
                    break;
                } catch (RemoteException | NotBoundException e) {
                    problem = e;
                } catch (AdminFaultException afe) {
                    problem = afe;

                    if (!afe.getFaultClassName().equals(
                                      AdminNotReadyException.class.getName())) {
                        break;
                    }
                }
                Thread.sleep(RETRY_DELAY_MS);
            }
            if (serviceParams == null) {
                plan.getLogger().log(Level.INFO, "Problem calling {0}: {1}",
                                     new Object[]{adminId, problem});
            }
        }

        /*
         * Check if parameters file needs to be updated, if the admin needs to
         * read them, and if the admin needs to be restarted.
         */
        final CompareParamsResult serviceCompare;
        final CompareParamsResult combinedCompare;
        if (serviceParams == null) {
            serviceCompare = CompareParamsResult.NO_DIFFS;
            combinedCompare = snCompare;
        } else {
            serviceCompare = VerifyConfiguration.compareServiceParams(
                snId, adminId, serviceParams, dbParams);
            combinedCompare = VerifyConfiguration.combineCompareParamsResults(
                snCompare, serviceCompare);
        }
        if (combinedCompare == CompareParamsResult.MISSING) {
            final String msg = "some parameters were missing";
            logError(plan, adminId, msg);
            if (task != null) {
                final CommandResult taskResult = new CommandFails(
                    msg, ErrorMessage.NOSQL_5400,
                    CommandResult.TOPO_PLAN_REPAIR);
                task.setTaskResult(taskResult);
            }
            return State.ERROR;
        }

        /* No diffs, just update admin DB if needed */
        if (combinedCompare == CompareParamsResult.NO_DIFFS) {
            if (updateDbAdminType) {
                plan.getLogger().fine("Updating admin type in admin DB");
                admin.updateParams(adminDbParams);
            }
            return State.SUCCEEDED;
        }

        /* Check if deleting a primary admin */
        final boolean deletePrimary;
        if (expectedAdminType.isPrimary()) {
            deletePrimary = false;
        } else if (serviceParams == null) {

            /*
             * Since the node isn't up, better do the check to be on the safe
             * side
             */
            deletePrimary = true;
        } else {
            final AdminParams adminServiceParams = new AdminParams(
                serviceParams.getMap(adminId.getFullName(),
                                     ParameterState.ADMIN_TYPE));
            deletePrimary = adminServiceParams.getType().isPrimary();
        }

        /* Not deleting primary admin, so no need to reduce quorum */
        if (!deletePrimary) {
            if (updateDbAdminType) {
                plan.getLogger().fine("Updating admin type in admin DB");
                admin.updateParams(adminDbParams);
            }
            if (snCompare != CompareParamsResult.NO_DIFFS) {
                plan.getLogger().fine("Updating admin config parameters");
                sna.newAdminParameters(adminDbParams.getMap());
            }
            if (serviceCompare == CompareParamsResult.DIFFS) {
                plan.getLogger().fine("Notifying admin of new parameters");
                regUtils.getAdmin(snId).newParameters();
            } else {

                /* Stop running admin in preparation for restarting it */
                if (serviceCompare == CompareParamsResult.DIFFS_RESTART) {

                    /*
                     * Return on error, or if interrupted because we stopped
                     * the current node
                     */
                    final State state = StopAdmin.stop(plan, adminId, snId);
                    if (state != State.SUCCEEDED) {
                        return state;
                    }
                }

                /*
                 * Restart the admin, or start it if it was not running and is
                 * not disabled
                 */
                if ((serviceCompare == CompareParamsResult.DIFFS_RESTART) ||
                    (serviceParams == null) && !adminDbParams.isDisabled()) {
                    StartAdmin.start(plan, adminId, snId);
                    try {
                        ServiceUtils.waitForAdmin(
                            sn.getHostname(), sn.getRegistryPort(),
                            plan.getLoginManager(),
                            masterAdminParams.getWaitTimeoutUnit().toSeconds(
                                masterAdminParams.getWaitTimeout()),
                            ServiceStatus.RUNNING,
                            plan.getLogger());
                    } catch (Exception e) {
                        throw new CommandFaultException(
                            e.getMessage(), e, ErrorMessage.NOSQL_5400,
                            CommandResult.PLAN_CANCEL);
                    }

                }
            }
            return State.SUCCEEDED;
        }

        /*
         * Compute the primary RF and check if stopping the node will cause a
         * loss of quorum for RF > 2
         */
        final int primaryRepFactor;
        try {
            HealthCheck healthCheck = HealthCheck.create(
                    plan.getAdmin(),
                    (task == null) ? plan.toString() : task.toString(),
                    adminId,
                    (expectedAdminType == AdminType.SECONDARY) ?
                    NodeType.SECONDARY : NodeType.ELECTABLE);
            primaryRepFactor =
                healthCheck.ping().get(0).getNumDataElectables();
            /*
             * Use a non-default requirement, since views could be
             * inconsistent.
             */
            healthCheck.await((new HealthCheck.Requirements()).
                    and(HealthCheck.SIMPMAJ_WRITE_IF_STOP_ELECTABLE).
                    and(HealthCheck.PROMOTING_CAUGHT_UP));
        } catch (OperationFaultException e) {
            throw new CommandFaultException(
                e.getMessage(), e, ErrorMessage.NOSQL_5400,
                CommandResult.PLAN_CANCEL);
        }
        if ((primaryRepFactor == 1) && (serviceParams != null)) {
            final String msg =
                "changing the type of an electable admin is not" +
                " supported for RF=1";
            logError(plan, adminId, msg);
            if (task != null) {
                final CommandResult taskResult = new CommandFails(
                    msg, ErrorMessage.NOSQL_5100,
                    CommandResult.NO_CLEANUP_JOBS);
                task.setTaskResult(taskResult);
            }
            return State.ERROR;
        }

        /*
         * If the admin being restarted is the current admin, then transfer the
         * master and let the new master continue with the operation.
         */
        State state;
        try {
            state = EnsureAdminNotMaster.ensure(plan, adminId);
        } catch (IllegalStateException e) {
            throw new CommandFaultException(
                e.getMessage(), e,
                ErrorMessage.NOSQL_5100,
                CommandResult.NO_CLEANUP_JOBS);
        }
        if (state == State.INTERRUPTED) {
            plan.getLogger().log(Level.INFO,
                                 "Started master transfer for {0}", adminId);
            return state;
        }
        if (state != State.SUCCEEDED) {
            final String msg = "failed to transfer master";
            logError(plan, adminId, msg);
            if (task != null) {
                final CommandResult taskResult = new CommandFails(
                    msg, ErrorMessage.NOSQL_5100,
                    CommandResult.NO_CLEANUP_JOBS);
                task.setTaskResult(taskResult);
            }
            return State.ERROR;
        }

        /* Set electable group size to maintain quorum with RF=2 */
        if (primaryRepFactor == 2) {
            plan.getLogger().fine("Setting group size override to 1");
            masterAdminParams.setElectableGroupSizeOverride(1);
            admin.updateParams(masterAdminParams);
        }

        try {

            /* Stop node if it is running */
            if (serviceParams != null) {
                state = StopAdmin.stop(plan, adminId, snId);
                if (!state.equals(State.SUCCEEDED)) {
                    final String msg = "failed to stop admin: " + state;
                    logError(plan, adminId, msg);
                    if (task != null) {
                        final CommandResult taskResult = new CommandFails(
                            msg, ErrorMessage.NOSQL_5100,
                            CommandResult.NO_CLEANUP_JOBS);
                        task.setTaskResult(taskResult);
                    }
                    return State.ERROR;
                }
            }

            /*
             * Remove the admin from the JE HA rep group because we're changing
             * its type to be not primary
             */
            if (!deleteMember(plan,
                              admin.getReplicationGroupAdmin(adminId),
                              Admin.getAdminRepNodeName(adminId),
                              adminId)) {

                /* Failed -- restart if we stopped it */
                if (serviceParams != null) {
                    StartAdmin.start(plan, adminId, snId);
                }
                if (task != null) {
                    final CommandResult taskResult = new CommandFails(
                        "Failed to delete a node from the JE replication group",
                        ErrorMessage.NOSQL_5400, CommandResult.PLAN_CANCEL);
                    task.setTaskResult(taskResult);
                }
                return State.ERROR;
            }
        } finally {
            if (primaryRepFactor == 2) {
                plan.getLogger().fine("Clearing group size override");
                masterAdminParams.setElectableGroupSizeOverride(0);
                admin.updateParams(masterAdminParams);
            }
        }

        if (updateDbAdminType) {
            plan.getLogger().fine("Updating admin type in admin DB");
            admin.updateParams(adminDbParams);
        }
        if (snCompare != CompareParamsResult.NO_DIFFS) {
            plan.getLogger().fine("Updating admin config parameters");
            sna.newAdminParameters(adminDbParams.getMap());
        }

        try {
            StartAdmin.start(plan, adminId, snId);
        } catch (SNAFaultException e) {
            if (e.getCause() instanceof IllegalStateException) {
                throw new CommandFaultException(
                    e.getMessage(), e,
                    ErrorMessage.NOSQL_5200,
                    CommandResult.NO_CLEANUP_JOBS);
            }
            throw e;
        }
        try {
            ServiceUtils.waitForAdmin(
                sn.getHostname(), sn.getRegistryPort(), plan.getLoginManager(),
                masterAdminParams.getWaitTimeoutUnit().toSeconds(
                    masterAdminParams.getWaitTimeout()),
                ServiceStatus.RUNNING, plan.getLogger());
        } catch (Exception e) {
            throw new CommandFaultException(
                e.getMessage(), e, ErrorMessage.NOSQL_5400,
                CommandResult.PLAN_CANCEL);
        }
        return State.SUCCEEDED;
    }

    @Override
    public boolean continuePastError() {
        return false;
    }

    @Override
    public boolean restartOnInterrupted() {
        return true;
    }

    @Override
    public StringBuilder getName(StringBuilder sb) {
       return super.getName(sb).append(" ").append(adminId);
    }
}
