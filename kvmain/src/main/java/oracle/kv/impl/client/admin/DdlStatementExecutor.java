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
package oracle.kv.impl.client.admin;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.ExecutionFuture;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStoreConfig;
import oracle.kv.StatementResult;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.fault.WrappedClientException;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.registry.Protocols;
import oracle.kv.query.ExecuteOptions;

/**
 * The DdlStatementExecutor manages resources needed for execution of ddl
 * statements. It provides:
 * - a discovery service to locate the admin master
 * - maintains a handle to the ClientAdminService for RMI communication between
 *   the client and Admin node
 * - manages notifications for ddl operation completion by
 *     - providing an Executor to handle tasks to poll for statement completion
 *     - manages notifications of threads that are awaiting statement completion
 *
 * For each KVStoreImpl, there is a single DdlStatementExecutor per TableAPI.
 *
 * Notification mechanism
 * ----------------------
 * By intent, this class does not automatically maintain a cache of
 * ExecutionFuture instances.  Although DdlFuture, which is the implementation
 * of the ExecutionFuture interface is thread safe, multiple threads may
 * instantiate futures that target the same ddl operation. See the diagram in
 * {@link DdlFuture}.
 *
 * This class does cache the DdlFuture if the future has instigated polling for
 * operation completeness.  Polling is started lazily; it's initiated by a call
 * to DdlFuture.get() or get(time, unit). At that point the DdlFuture calls
 * DdlStatementExecutor to ask for polling to start. DdlStatementExecutor
 * manages the polling requests from multiple futures and ensures that only a
 * single periodic polling task is created per plan. Each DdlFuture that has
 * requested polling registers itself as an interested party for that
 * plan. When plan completion is detected, or an error occurs, the polling task
 * will release all registered DdlFutures.
 *
 * For example:
 *   threadA calls DdlFutureA.get()
 *        DdlStatementExecutor caches DdlFutureA
 *        DdlStatementExecutor creates and submits polling task X
 *        DdlStatementExecutor notes that taskX should notify DdlFutureA
 *   threadB calls DdlFutureB.get()
 *        DdlStatementExecutor caches DdlFutureB
 *        DdlStatementExecutor notes that taskX should notify DdlFutureA and
 *                      DdlFutureB
 * Futures are only cached if the application has called get() and is polling.
 * For synchronization simplicity, cached futures rely on the polling task to
 * free them from the target pool.  If DdlFuture obtains a new status directly,
 * via DdlFuture.updateStatus or isDone(), the new status is applied only to
 * that future instance, and does not affect its own membership in the
 * notification pool, nor the status of any other futures that are focused on
 * the same plan.
 *
 * Synchronization hierarchy:
 * --------------------------
 *  The order of locking is DdlStatementExecutor, followed by DdlFuture.
 */
public class DdlStatementExecutor {

    /* Interface to server, manages refreshing it. */
    private ClientAdminServiceAPI clientAdminService;

    /* Executor for polling tasks */
    private final ScheduledExecutorService completionChecker;

    /* Submitted polling tasks */
    private final Map<Integer, ScheduledFuture<?>> scheduledFutures;

    /*
     * Notification targets are futures that are blocking in get() and
     * are awaiting notification from a polling task.
     */
    private final Map<Integer, Set<DdlFuture>> notificationTargets;

    private final Topology topo;

    /* All access needs to be inside the synchronized block of this object. */
    private LoginManager loginManager;
    private final Logger logger;
    private final ClientId clientId;
    private final Protocols protocols;

    /*
     * The number of times a status check polling task should retry in the face
     * of remote exceptions.
     */
    private final int maxCheckRetries;

    /* The polling interval for status check tasks. */
    private final long checkIntervalMillis;

    /* Test hook can be invoked before execute DDL */
    public static volatile TestHook<Integer> beforeExecuteHook = null;

    public DdlStatementExecutor(KVStoreImpl store) {
        this(store.getDispatcher().getTopologyManager().getTopology(),
             KVStoreImpl.getLoginManager(store),
             store.getMaxCheckRetries(),
             store.getCheckIntervalMillis(),
             store.getLogger(),
             store.getDispatcher().getClientId(),
             store.getDispatcher().getProtocols());
    }

    public DdlStatementExecutor(Topology topo,
                                LoginManager loginManager,
                                int maxCheckRetries,
                                long checkIntervalMillis,
                                Logger logger,
                                ClientId clientId,
                                Protocols protocols) {
        this.topo = topo;
        this.loginManager = loginManager;
        this.maxCheckRetries = maxCheckRetries;
        this.checkIntervalMillis = checkIntervalMillis;
        this.logger = logger;
        this.clientId = clientId;
        this.protocols = protocols;
        this.scheduledFutures = new HashMap<Integer, ScheduledFuture<?>>();
        this.notificationTargets = new HashMap<Integer, Set<DdlFuture>>();

        completionChecker = new ScheduledThreadPoolExecutor
            (1, new KVThreadFactory("DDLChecks", logger));

        /*
         * Try to connect to an Admin master, in preparation for later DDL, but
         * it's okay if it's not possible. We don't necessarily need the
         * connection. The connection isn't too costly though; RMI will
         * background the connection after a number of seconds and cache the
         * binding, so it's okay to try to set it up now.
         */
        try {
            ensureClientAdminService();
        } catch (Exception ignore) {
            /*
             * The attempt to establish a client/admin connection at this point
             * is just an optimization, so a failure to do so should not
             * matter. It's not really needed until a DDL statement is
             * executed. The connection is checked again at that point, so
             * ignore any exceptions now.
             */
        }
    }

    /**
     * Return the RMI interface for client/admin communication.
     */
    public synchronized ClientAdminServiceAPI getClientAdminService() {
        /* If it's not connected, try to connect now */
        ensureClientAdminService();
        return clientAdminService;
    }

    /**
     * Expected to be called in reauthentication case, renew the loginManger
     * and refresh the cached ClientAdminServiceAPI.
     */
    public synchronized void renewLoginManager(LoginManager loginManger) {
        this.loginManager = loginManger;

        final FindClientAdminService finder =
            new FindClientAdminService(topo, logger, loginManager, clientId,
                                       protocols);
        clientAdminService = finder.getDDLService();
    }

    /*
     * For unit test.
     */
    public synchronized LoginManager getLoginManager() {
        return loginManager;
    }

    /**
     * Establish an RMI connection to the admin master. Ensure that the proper
     * credentials are set up. Except the constructor, the other invocation of
     * this method must be inside the synchronized block of this object.
     *
     * A note about login managers:
     * ----------------------------
     * The RepNodeLoginManager instance stored in the KVStoreImpl is used to
     * validate the secure version of this client->admin connection. This is a
     * convenient implementation, as the KVStoreImpl's login manager already
     * has the right infrastructure to be created and refreshed when new
     * security credentials arrive. All communications between the client and
     * Admin service for a secured RMI interface require a login token. This
     * login token needs to be validated by the Admin service, using a login
     * manager, and in this case, an RN acts as this validator.
     *
     * However, since successfully resolved login tokens are cached by the
     * Admin (as they are by RNs), that Admin only needs to communicate with
     * the RN for validation infrequently. Cached tokens have timeout values,
     * and at timeout, validation will happen again.
     */
    private void ensureClientAdminService()
        throws FaultException {

        if (canHandleDDL()) {
            return;
        }

        /* Find the SN which holds the admin master */
        logger.info("Establishing RMI connection for admin DDL");

        FindClientAdminService finder = new FindClientAdminService
            (topo,
             logger,
             loginManager,
             clientId,
             protocols);
        clientAdminService = finder.getDDLService();
        if (clientAdminService == null) {
            throw new FaultException(
                    "Couldn't connect to a store Admin service capable of " +
                    "executing an administrative table statement. Contacted " +
                    "nodes " + finder.getTargets(), false);
        }
    }

    /**
     * Attempt to find ClientAdminService by given admin host and port.
     * This method can be used if there isn't any storage node available
     * in the topology, typically the bootstrap admin case.
     */
    public synchronized void findClientAdminService(String adminHost,
                                                    int adminPort)
        throws FaultException {

        if (canHandleDDL()) {
            return;
        }

        logger.info("Establishing RMI connection for admin DDL");

        FindClientAdminService finder = new FindClientAdminService
            (topo,
             logger,
             loginManager,
             clientId,
             protocols);
        clientAdminService = finder.connectDDL(topo.getKVStoreName(),
                                               adminHost,
                                               adminPort,
                                               clientId);
        if (clientAdminService == null) {
            throw new FaultException(
                    "Couldn't connect to a store Admin service capable of " +
                    "executing an administrative table statement. Contacted " +
                    "nodes " + adminHost + ":" + adminPort, false);
        }
    }

    private boolean canHandleDDL() {
        if (clientAdminService == null) {
            return false;
        }

        try {
            /* See if the service is functional and is the master */
            if (clientAdminService.canHandleDDL()) {
                return true;
            }
        } catch (RemoteException | AuthenticationRequiredException e) {
            logger.fine("Ensuring connection, got " + e);
        }
        /*
         * The RMI service is down, the admin is no longer a master or the
         * cached login token is no longer valid, require re-authentication.
         * Null out the cached connection, so we can find another for next
         * retries.
         */
        clientAdminService = null;
        return false;
    }

    /**
     * Schedule a task to start checking status for the specified plan.
     */
    synchronized void startPolling(int planId, DdlFuture ddlFuture) {

        /*
         * If there's already a polling task for this plan, no need to add
         * another one.
         */
        if (!scheduledFutures.containsKey(planId)) {
            /* schedule the first check */
            scheduleCheckTask(planId, maxCheckRetries,
                              KVStoreConfig.DEFAULT_CHECK_INIT_DELAY_MILLIS,
                              ddlFuture.getAuthContext());
        }

        /*
         * Do register this future as an interested party, whether or not
         * we added a new polling task.
         */
        Set<DdlFuture> targets = notificationTargets.get(planId);
        if (targets == null) {
            targets = new HashSet<>();
            notificationTargets.put(planId, targets);
        }
        targets.add(ddlFuture);
    }

    /**
     * Problem seen, but no new status available. Manufacture a status that
     * says that the operation hasn't ended, but that the call to get() needs
     * to fail. There's no valid result.
     */
    void shutdownWaitersDueToError(int planId, Throwable t) {
        ExecutionInfo errorInfo =
            new ExecutionInfoImpl(planId,
                                  false, // isTerminated
                                  null,  // getInfo
                                  null,  // getInfoAsJson
                                  false, // isSuccess,
                                  false, // isCancelled
                                  t.getMessage(),
                                  false, // needsCancel;
                                  null); // result
        updateWaiters(errorInfo, t, true);
    }

    /**
     * New status has come in. Let any futures who have expressed interest
     * know.
     */
    void updateWaiters(ExecutionInfo newInfo) {
        updateWaiters(newInfo, null, false);
    }

    /**
     * New status has come in. Let any futures who have expressed interest
     * know.
     */
    synchronized void updateWaiters(ExecutionInfo newInfo,
                                    Throwable t,
                                    boolean stopPolling) {
        int planId = newInfo.getPlanId();

        /*
         * Update all waiting futures with the new info. Synchronize
         * to make sure that they are all uniformly updated with the
         * same info, and that we don't add a target to the list and
         * miss an update.
         */
        Set<DdlFuture> waiters = notificationTargets.get(planId);
        if (waiters != null) {
            for (DdlFuture waiter : waiters) {
                waiter.applyNewInfo(newInfo, t);
            }
        }

        /* If the operation is completed, terminate the polling task */
        if (stopPolling || newInfo.isTerminated()) {
            ScheduledFuture<?> taskFuture = scheduledFutures.get(planId);
            if (taskFuture != null) {
                logger.fine("Polling task for plan " + planId +
                            " finished, info = " + newInfo);
                taskFuture.cancel(true);
                scheduledFutures.remove(planId);
            }

            notificationTargets.remove(planId);
        }
    }

    /**
     * Called by a scheduled task to report that it had problems, has retried
     * more than the permissible amount of time, and will no longer execute.
     * @param planId
     * @param e
     */
    void taskFailed(int planId, RemoteException e) {
        throw new UnsupportedOperationException(e);
    }

    /**
     * Executes a DDL statement in an async fashion, returning an
     * ExecutionFuture.
     *
     * Because this method communicates directly with the admin it needs to
     * handle exceptions carefully.
     */
    public ExecutionFuture executeDdl(char[] statement,
                                      String namespace,
                                      ExecuteOptions options,
                                      TableLimits limits)
        throws IllegalArgumentException, FaultException {
        try {
            assert TestHookExecute.doHookIfSet(beforeExecuteHook, null);
            final AuthContext authCtx =
                options == null ? null : options.getAuthContext();
            ExecutionInfo info = getClientAdminService().execute(statement,
                namespace,
                (options == null || options.isValidateNamespace()),
                limits,
                options == null ? null : options.getLogContext(),
                authCtx);
            return new DdlFuture(statement, info, this, authCtx);
        } catch (FaultException fe) {
            throw fe;
        } catch (IllegalArgumentException iae) {
            /* pass through */
            throw iae;
        } catch (WrappedClientException wce) {
            /*
             * These are query exceptions in the form of IAE and illegal
             * state exceptions in the form of QueryStateException.
             */
            if (wce.getCause() instanceof IllegalStateException) {
                /*
                 * Log the exception if a logger is available
                 */
                 if (logger != null) {
                     logger.warning(wce.getCause().toString());
                 }
            }
            throw (RuntimeException) wce.getCause();
        } catch (AdminFaultException e) {
            if (e.getFaultClassName().equals
                (IllegalCommandException.class.getName())) {
                /* unwrap, throw as an IllegalArgException */
                throw new IllegalArgumentException(e.getMessage());
            }

            if (e.getFaultClassName().equals(FaultException.class.getName())) {
                /* unwrap, throw as a FaultException */
                throw new FaultException(e.getMessage(), false);
            }

            /* Wrap, throw as a FaultException */
            throw new FaultException(e.getMessage(), e, false);
        } catch (KVSecurityException e) {
            /* This should be passed along directly */
            throw e;
        } catch (Exception e) {

            /*
             * Other types of exceptions should get wrapped as a fault exception
             */
            throw new FaultException(e.getMessage(), e, false);
        }
    }

    /**
     * Executes a setTableLimits call in an async fashion, returning an
     * ExecutionFuture.
     *
     * Because this method communicates directly with the admin it needs to
     * handle exceptions carefully.
     */
    public ExecutionFuture setTableLimits(String namespace,
                                          String tableName,
                                          TableLimits limits)
        throws IllegalArgumentException, FaultException {
        try {
            assert TestHookExecute.doHookIfSet(beforeExecuteHook, null);
            ExecutionInfo info = getClientAdminService().
                setTableLimits(namespace, tableName, limits);
            return new DdlFuture(info.getPlanId(), this);
        } catch (IllegalArgumentException iae) {
            /* pass through */
            throw iae;
        } catch (AdminFaultException e) {
            if (e.getFaultClassName().equals
                (IllegalCommandException.class.getName())) {
                /* unwrap, throw as an IllegalArgException */
                throw new IllegalArgumentException(e.getMessage());
            }

            if (e.getFaultClassName().equals(FaultException.class.getName())) {
                /* unwrap, throw as a FaultException */
                throw new FaultException(e.getMessage(), false);
            }

            /* Wrap, throw as a FaultException */
            throw new FaultException(e.getMessage(), e, false);
        } catch (KVSecurityException e) {
            /* This should be passed along directly */
            throw e;
        } catch (Exception e) {

            /*
             * Other types of exceptions should get wrapped as a fault exception
             */
            throw new FaultException(e.getMessage(), e, false);
        }
    }

    /**
     * Wait for the execution task to complete, handle the exception if any.
     * @throws FaultException
     * @throws IllegalArgumentException
     */
    public static StatementResult waitExecutionResult(ExecutionFuture future)
        throws FaultException, IllegalArgumentException {
        try {
            StatementResult r = future.get();
            return r;
        } catch (ExecutionException e) {
            /* Unwrap the ExecutionException */
            if (e.getCause() != null) {
                if (e.getCause() instanceof FaultException) {
                    throw (FaultException) e.getCause();
                }
                throw new FaultException(e.getMessage(), e.getCause(), false);
            }

            throw new FaultException(e.getMessage(), false);
        } catch (KVSecurityException e) {
            /* This should be passed along directly */
            throw e;
        } catch (Exception e) {
            /*
             * Other types of exceptions should get wrapped as a fault exception
             */
            throw new FaultException(e.getMessage(), e, false);
        }
    }

    /* Schedules a task with exponential back-off */
    void scheduleCheckTask(int planId, int retries, long delayMs,
                           AuthContext authCtx) {
        /* exponential back-off capped by max delay */
        final long nextDelayMs = Math.min(2 * delayMs, checkIntervalMillis);
        final ScheduledFuture<?> f =
            completionChecker.schedule(
                new DdlCheckTask(planId, this, retries, nextDelayMs, logger,
                                 authCtx),
                delayMs, TimeUnit.MILLISECONDS);
        scheduledFutures.put(planId, f);
    }
}
