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

import static oracle.kv.impl.async.FutureUtils.failedFuture;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.async.AsyncVersionedRemoteAPIWithTimeout;
import oracle.kv.impl.async.VersionedRemoteAsyncImpl;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ContextProxy;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.contextlogger.LogContext;

/**
 * Defines an async version of the ClientAdminService interface.
 *
 * @since 21.2
 */
public class AsyncClientAdminServiceAPI
        extends AsyncVersionedRemoteAPIWithTimeout {

    /** Null value that will be filled in by proxyRemote */
    private static final AuthContext NULL_CTX = null;

    /** The remote proxy */
    private final AsyncClientAdminService remoteProxy;

    /** A proxy wrapped to support security if needed */
    private final AsyncClientAdminService wrappedProxy;

    /** Creates a new instance. */
    private AsyncClientAdminServiceAPI(short serialVersion,
                                       long defaultTimeoutMs,
                                       AsyncClientAdminService remote,
                                       LoginHandle loginHandle) {
        super(serialVersion, defaultTimeoutMs);
        remoteProxy = remote;

        /* Only need to wrap the proxy when security is enabled */
        wrappedProxy = (loginHandle != null) ?
            ContextProxy.createAsync(remote, loginHandle, getSerialVersion()) :
            remote;
    }

    /**
     * Returns the underlying remote service proxy.  If this is a secure
     * store, the return value is not wrapped to support security: it is the
     * original remote proxy.
     */
    public AsyncClientAdminService getRemoteProxy() {
        return remoteProxy;
    }

    /**
     * Makes an asynchronous request to create an instance of this class,
     * returning the result as a future.
     *
     * @param initiator the client side initiator
     * @param loginHandle the login handle for secure stores
     * @param timeoutMs the timeout in milliseconds for obtaining the API
     * @param defaultTimeoutMs the default timeout in milliseconds for
     * operations on the API
     * @return the future
     */
    public static CompletableFuture<AsyncClientAdminServiceAPI>
        wrap(AsyncClientAdminService initiator,
             LoginHandle loginHandle,
             long timeoutMs,
             long defaultTimeoutMs) {
        try {
            return computeSerialVersion(initiator, timeoutMs)
                .thenApply(sv ->
                           new AsyncClientAdminServiceAPI(sv, defaultTimeoutMs,
                                                          initiator,
                                                          loginHandle));
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    /**
     * Creates a proxy that implements the synchronous API using the remote
     * server and timeout from this async API.
     */
    public ClientAdminService createSyncProxy() {
        return VersionedRemoteAsyncImpl.createProxy(
            ClientAdminService.class, remoteProxy, getDefaultTimeoutMs());
    }

    /**
     * Submit a DDL statement for asynchronous execution and return status
     * about the corresponding plan.
     */
    public CompletableFuture<ExecutionInfo> execute(char[] statement,
                                                    String namespace,
                                                    boolean validateNamespace,
                                                    TableLimits limits,
                                                    LogContext lc,
                                                    AuthContext authCtx,
                                                    long timeoutMillis) {
        return wrappedProxy.execute(getSerialVersion(), statement,
                                    namespace, validateNamespace, limits,
                                    lc, authCtx, timeoutMillis);
    }

    public CompletableFuture<ExecutionInfo> setTableLimits(String namespace,
                                                           String tableName,
                                                           TableLimits limits,
                                                           long timeoutMillis)
    {
        return wrappedProxy.setTableLimits(getSerialVersion(), namespace,
                                           tableName, limits, NULL_CTX,
                                           timeoutMillis);
    }

    /**
     * Get current status for the specified plan.
     */
    public CompletableFuture<ExecutionInfo>
        getExecutionStatus(int planId, long timeoutMillis)
    {
        return getExecutionStatus(planId, NULL_CTX, timeoutMillis);
    }

    /**
     * Get current status for the specified plan and authentication context --
     * for use by the proxy.
     */
    public CompletableFuture<ExecutionInfo>
        getExecutionStatus(int planId,
                           AuthContext authCtx,
                           long timeoutMillis) {
        return wrappedProxy.getExecutionStatus(getSerialVersion(), planId,
                                               authCtx, timeoutMillis);
    }

    /**
     * Return true if this Admin can handle DDL operations. That currently
     * equates to whether the Admin is a master or not.
     */
    public CompletableFuture<Boolean> canHandleDDL(long timeoutMillis) {
        return wrappedProxy.canHandleDDL(getSerialVersion(), NULL_CTX,
                                         timeoutMillis);
    }

    /**
     * Return the address of the master Admin. If this Admin doesn't know that,
     * return null.
     */
    public CompletableFuture<URI> getMasterRmiAddress(long timeoutMillis) {
        return wrappedProxy.getMasterRmiAddress(getSerialVersion(), NULL_CTX,
                                                timeoutMillis);
    }

    /**
     * Initiate a plan cancellation.
     */
    public CompletableFuture<ExecutionInfo>
        interruptAndCancel(int planId,
                           AuthContext authCtx,
                           long timeoutMillis) {
        return wrappedProxy.interruptAndCancel(getSerialVersion(), planId,
                                               authCtx, timeoutMillis);
    }

    /**
     * Return the current topology used for connecting client to store using
     * Admin service. If topology is not available at this service, return null.
     *
     * @since 24.2
     */
    public CompletableFuture<Topology> getTopology(long timeoutMillis) {

        return wrappedProxy.getTopology(getSerialVersion(), NULL_CTX,
                                        timeoutMillis);
    }

    /**
     * Returns the sequence number associated with the Topology at the Admin. If
     * topology is not available, return Metadata.EMPTY_SEQUENCE_NUMBER.
     *
     * @since 24.2
     */
    public CompletableFuture<Integer> getTopoSeqNum(long timeoutMillis) {

        return wrappedProxy.getTopoSeqNum(getSerialVersion(), NULL_CTX,
                                          timeoutMillis);
    }
}
