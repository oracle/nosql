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

import java.net.URI;
import java.util.logging.Logger;

import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.client.admin.ClientAdminService;
import oracle.kv.impl.client.admin.ExecutionInfo;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.fault.ServiceFaultHandler;
import oracle.kv.impl.security.AccessCheckUtils;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.KVStorePrivilegeLabel;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.security.annotations.SecureInternalMethod;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.contextlogger.LogContext;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;

import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * The server side implementation of the RMI interface which supports the
 * asynchronous execution of DDL statements by the master Admin service.
 */
@SecureAPI
public class ClientAdminServiceImpl
    extends VersionedRemoteImpl implements ClientAdminService {

    /* The service hosting this interface */
    private final AdminService aservice;

    private final ProcessFaultHandler faultHandler;

    public ClientAdminServiceImpl(AdminService aservice,
                                  Logger logger) {
        this.aservice = aservice;
        faultHandler = new ServiceFaultHandler(aservice, logger,
                                               ProcessExitCode.RESTART);
    }

    /**
     * Added validateNamespace
     *
     * @since  18.3
     */
    @Override
    @SecureInternalMethod
    public ExecutionInfo execute(final char[] statement,
        final String namespace,
        final boolean validateNamespace,
        final TableLimits limits,
        final LogContext lc,
        AuthContext authCtx,
        final short serialVersion) {
        final Admin admin = aservice.getAdmin();
        return faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<ExecutionInfo>() {
                @Override
                public ExecutionInfo execute() {
                    return admin.executeStatement(new String(statement),
                        namespace,
                        validateNamespace,
                        limits,
                        lc,
                        serialVersion);
                }
            });
    }

    /**
     * Set table limits
     * @since  18.1
     */
    @Override
    @SecureInternalMethod
    public ExecutionInfo setTableLimits(final String namespace,
                                        final String tableName,
                                        final TableLimits limits,
                                        AuthContext authCtx,
                                        final short serialVersion) {
        final Admin admin = aservice.getAdmin();
        return faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<ExecutionInfo>() {
                @Override
                public ExecutionInfo execute() {
                    return admin.setTableLimits(namespace,
                                                tableName,
                                                limits,
                                                serialVersion);
                }
            });
    }

    /**
     * Get current status for the specified plan.
     */
    @Override
    @SecureInternalMethod
    public ExecutionInfo getExecutionStatus(final int planId,
                                            AuthContext authCtx,
                                            final short serialVersion) {
        final Admin admin = aservice.getAdmin();
        ExecutionInfo info =  faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<ExecutionInfo>() {
                @Override
                 public ExecutionInfo execute() {
                    final Plan plan = admin.getAndCheckPlan(planId);

                    /* Check that we have the right to examine the plan */
                    AccessCheckUtils.checkPermission
                    (aservice,
                     new AccessCheckUtils.PlanAccessContext
                     (plan, "getExecutionStatus"));

                    return admin.getExecutionStatus(planId, serialVersion);
                }
            });
        return info;
    }

    /**
     * Ask the admin service to stop the specified plan. Interruption may or
     * may not be needed.
     */
    @Override
    @SecureInternalMethod
    public ExecutionInfo interruptAndCancel(final int planId,
                                            AuthContext nullCtx,
                                            final short serialVersion) {

        final Admin admin = aservice.getAdmin();
        ExecutionInfo info =  faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<ExecutionInfo>() {
                @Override
                public ExecutionInfo execute() {

                    /* Check that we have the right to interrupt this plan */
                    final Plan plan = admin.getAndCheckPlan(planId);
                    AccessCheckUtils.checkPermission
                    (aservice,
                     new AccessCheckUtils.PlanOperationContext
                     (plan, "interruptAndCancel"));

                    Plan.State currentState = admin.getCurrentPlanState(planId);
                    if (currentState.equals(Plan.State.RUNNING)){
                        admin.interruptPlan(planId);
                        currentState = admin.awaitPlan(planId, 0, null);
                    }

                    /*
                     * Possibilities:
                     * -plan is in INTERRUPT or ERROR state, and can be
                     *   cancelled
                     * -plan is already in a terminal state, nothing more to do
                     * -something else unexpected happened.
                     */
                    final String errMsg =
                        currentState.checkTransition(Plan.State.CANCELED);
                    if (errMsg == null) {
                        admin.cancelPlan(planId);
                    } else if (!currentState.isTerminal()) {
                        /*
                         * Unexpected -- the plan should have transitioned
                         * into something that was finished, or can be
                         * cancelled.
                         */
                        throw new AdminFaultException
                        (new NonfatalAssertionException
                                ("Cancellation of operation " + planId +
                                "unsuccessful, current state = " +
                                 currentState +
                                 (!errMsg.isEmpty() ? ": " + errMsg : "")));
                    }
                    return admin.getExecutionStatus(planId, serialVersion);
                }
            });
        return info;
    }

    /*
     * Only USRVIEW is required here because we want to allow all authenticated
     * users to be able to perform this operation, since it is required by
     * every operation that uses the admin to figure out if the admin is
     * running.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW})
    public boolean canHandleDDL(AuthContext authCtx,
                                final short serialVersion) {

        return faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<Boolean>() {

            @Override
            public Boolean execute() {
                final Admin admin = aservice.getAdmin();
                if (admin == null) {
                    return false; /* indicates unconfigured */
                }

                try {
                    final ReplicatedEnvironment.State repState =
                        admin.getReplicationMode();
                    return repState.isMaster();
                } catch (IllegalStateException iae) {
                    /* State cannot be queried if detached. */
                    return false;
                }
            }
        });
    }

    /*
     * Only USRVIEW is required here because we want to allow all authenticated
     * users to be able to perform this operation, since it is required by
     * every operation that needs to locate the master admin node.
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
    public URI getMasterRmiAddress(AuthContext authCtx, short serialVersion) {

        return faultHandler.execute
            (new ProcessFaultHandler.SimpleOperation<URI>() {

            @Override
            public URI execute() {
                final Admin admin = aservice.getAdmin();
                if (admin == null) {
                    return null;
                }
                return admin.getMasterRmiAddress();
            }
        });
    }

    /**
     * Only USRVIEW is required here because we want to allow all authenticated
     * users to be able to perform this operation, since it is required to
     * connect to the store using client API calls.
     *
     * @since 24.2
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
    public Topology getTopology(AuthContext authCtx, short serialVersion) {
        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<Topology>() {

            @Override
            public Topology execute() {
                final Admin admin = aservice.getAdmin();
                if (admin == null) {
                    return null;
                }

                return admin.getCurrentTopology();
            }
        });
    }

    /**
     * Only USRVIEW is required here because we want to allow all authenticated
     * users to be able to perform this operation, since it is required to
     * compare topologies obtained by different Admins and RNs while connecting
     * to the store using client API calls.
     *
     * @since 24.2
     */
    @Override
    @SecureAutoMethod(privileges = { KVStorePrivilegeLabel.USRVIEW })
    public int getTopoSeqNum(AuthContext authCtx, short serialVersion) {
        return faultHandler.
            execute(new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                final Admin admin = aservice.getAdmin();
                if (admin == null) {
                    return Topology.EMPTY_SEQUENCE_NUMBER;
                }
                final Topology topology =  admin.getCurrentTopology();
                return (topology != null) ?
                        topology.getSequenceNumber() :
                        Topology.EMPTY_SEQUENCE_NUMBER;
            }
        });
    }
}
