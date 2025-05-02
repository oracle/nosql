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

package oracle.kv.impl.rep.admin;

import static oracle.kv.impl.async.FutureUtils.failedFuture;

import java.util.concurrent.CompletableFuture;

import oracle.kv.impl.async.AsyncVersionedRemoteAPIWithTimeout;
import oracle.kv.impl.async.VersionedRemoteAsyncImpl;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.metadata.MetadataKey;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ContextProxy;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.util.KerberosPrincipals;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * An async version of the administrative interface to a RepNode needed by
 * clients.
 *
 * <p>Several layers of classes and dynamic proxies are used to implement this
 * API, and the associated synchronous one, on the client and server sides.
 * Here is an overview for this class; several other classes use this same
 * approach.
 *
 * Synchronous client over RMI:
 * <ol>
 * <li> ClientRepNodeAdminAPI (class) - The top level API for sync calls. This
 *      layer is provided to support any special handling needed for versioned
 *      upgrades, for example checking that the server supports a new method or
 *      parameter.
 *
 * <li> ContextProxy (dynamic proxy for ClientRepNodeAdmin interface) - A
 *      wrapper added for secure stores that injects login tokens and performs
 *      retries. This synchronous version blocks during retries.
 *
 * <li> Remote (dynamic proxy for ClientRepNodeAdmin interface) - Created by
 *      calling the RMI UnicastRemoteObject.export method: a dynamic proxy that
 *      represents the client side of an RMI interface.
 * </ol>
 * Asynchronous Client:
 * <ol>
 * <li> AsyncClientRepNodeAdminAPI (class) - The top level API for async calls,
 *      to support versioning.
 *
 * <li> ContextProxy (dynamic proxy for AsyncClientRepNodeAdmin interface) - A
 *      wrapper added for secure stores that injects login tokens and performs
 *      retries. This is the async version, created by the createAsync method,
 *      which expects methods to return CompletableFutures and performs retries
 *      asynchronously.
 *
 * <li> AsyncClientRepNodeInitiator (dynamic proxy for AsyncClientRepNodeAdmin
 *      interface created by AsyncInitiatorProxy) - The client side of the
 *      async interface. The dynamic proxy implements the interface by using
 *      MethodCallClass annotations on AsyncClientRepNode to find the
 *      MethodCall objects to use to serialize and deserialize the calls.
 * </ul>
 * Synchronous client over async protocol:
 * <ol>
 * <li> ClientRepNodeAdminAPI (class) - Same as RMI
 *
 * <li> ContextProxy (dynamic proxy for ClientRepNodeAdmin interface) - Same as
 *      RMI. Note that since the context proxy is included here in the stack,
 *      we do no include an async one on top of the AsyncClientRepNodeInitiator
 *      below.
 *
 * <li> Remote (dynamic proxy for ClientRepNodeAdmin interface) - Same as RMI
 *
 * <li> VersionedRemoteAsyncImpl (dynamic proxy for ClientRepNodeAdmin
 *      interface) - A dynamic proxy that implements the RMI Remote interface
 *      by converting the argument list as needed and calling the associated
 *      method on an associated AsyncVersionedRemote object.
 *
 * <li> AsyncClientRepNodeInitiator (dynamic proxy for AsyncClientRepNodeAdmin
 *      interface created by AsyncInitiatorProxy) - The client side dynamic
 *      proxy for the async interface.
 * </ol>
 * RMI Server:
 * <ol>
 * <li> SecureProxy (dynamic proxy for ClientRepNodeAdmin) - A dynamic proxy
 *      that implements the RMI Remote interface and makes security checks to
 *      enforce access control for secure stores.
 *
 * <li> ClientRepNodeAdminImpl (class that implements ClientRepNodeAdmin) - The
 *      server side implementation.
 * </ol>
 * Async Server:
 * <ol>
 * <li> AsyncClientRepNodeAdminResponder (dynamic proxy for
 *      AsyncClientRepNodeAdmin created by VersionedRemoteAsyncServerImpl) - A
 *      dynamic proxy created by the VersionedRemoteAsyncServerImpl class that
 *      deserializes async requests and arranges to execute them by calling the
 *      server implementation.
 *
 * <li> SecureProxy (dynamic proxy for ClientRepNodeAdmin) - Same as RMI
 *
 * <li> ClientRepNodeAdminImpl (class that implements ClientRepNodeAdmin) -
 *      Same as RMI
 * </ol>
 *
 * @since 21.2
 */
public class AsyncClientRepNodeAdminAPI
        extends AsyncVersionedRemoteAPIWithTimeout {

    /** Null value that will be filled in by proxyRemote */
    private static final AuthContext NULL_CTX = null;

    /** The remote proxy */
    private final AsyncClientRepNodeAdmin remoteProxy;

    /** A proxy wrapped to support security if needed */
    private final AsyncClientRepNodeAdmin wrappedProxy;

    private AsyncClientRepNodeAdminAPI(short serialVersion,
                                       long defaultTimeoutMs,
                                       AsyncClientRepNodeAdmin remote,
                                       LoginHandle loginHandle) {
        super(serialVersion, defaultTimeoutMs);
        remoteProxy = remote;
        wrappedProxy = (loginHandle != null) ?
            ContextProxy.createAsync(remote, loginHandle, getSerialVersion()) :
            remote;
    }

    /**
     * Returns the underlying remote service proxy.  If this is a secure
     * store, the return value is not wrapped to support security: it is the
     * original remote proxy.
     */
    public AsyncClientRepNodeAdmin getRemoteProxy() {
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
    public static CompletableFuture<AsyncClientRepNodeAdminAPI>
        wrap(AsyncClientRepNodeAdmin initiator,
             LoginHandle loginHandle,
             long timeoutMs,
             long defaultTimeoutMs) {
        try {
            return computeSerialVersion(initiator, timeoutMs)
                .thenApply(sv ->
                           new AsyncClientRepNodeAdminAPI(sv, defaultTimeoutMs,
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
    public RepNodeAdmin createSyncProxy() {
        return VersionedRemoteAsyncImpl.createProxy(
            RepNodeAdmin.class, remoteProxy, getDefaultTimeoutMs());
    }

    /**
     * Returns this RN's view of the Topology. In a distributed system like
     * KVS, it may be temporarily different from the Topology at other nodes,
     * but will eventually become eventually consistent.
     */
    public CompletableFuture<Topology> getTopology(long timeoutMillis) {
        return wrappedProxy.getTopology(getSerialVersion(), NULL_CTX,
                                        timeoutMillis);
    }

    /**
     * Returns the sequence number associated with the Topology at the RN.
     */
    public CompletableFuture<Integer> getTopoSeqNum(long timeoutMillis) {
        return wrappedProxy.getTopoSeqNum(getSerialVersion(), NULL_CTX,
                                          timeoutMillis);
    }

    /**
     * Returns a string containing the HA hostname and port for the master in
     * this RN's shard.
     */
    public CompletableFuture<String> getHAHostPort(long timeoutMillis) {
        return wrappedProxy.getHAHostPort(getSerialVersion(), NULL_CTX,
                                          timeoutMillis);
    }

    /**
     * Returns this node's replication state.
     */
    public CompletableFuture<ReplicatedEnvironment.State>
        getReplicationState(long timeoutMillis)
    {
        return wrappedProxy.getReplicationState(getSerialVersion(), NULL_CTX,
                                                timeoutMillis);
    }

    /**
     * Returns this node's vlsn.
     */
    public CompletableFuture<Long> getVlsn(long timeoutMillis)
    {
        return wrappedProxy.getVlsn(getSerialVersion(), NULL_CTX,
                                                timeoutMillis);
    }

    public CompletableFuture<Integer> getMetadataSeqNum(MetadataType type,
                                                        long timeoutMillis) {
        return wrappedProxy.getMetadataSeqNum(getSerialVersion(), type,
                                              NULL_CTX, timeoutMillis);
    }

    public CompletableFuture<Metadata<?>> getMetadata(MetadataType type,
                                                      long timeoutMillis) {
        return wrappedProxy.getMetadata(getSerialVersion(), type, NULL_CTX,
                                        timeoutMillis);
    }

    public CompletableFuture<MetadataInfo> getMetadata(MetadataType type,
                                                       int seqNum,
                                                       long timeoutMillis) {
        return wrappedProxy.getMetadata(getSerialVersion(), type, seqNum,
                                        NULL_CTX, timeoutMillis);
    }

    public CompletableFuture<MetadataInfo> getMetadata(MetadataType type,
                                                       MetadataKey key,
                                                       int seqNum,
                                                       long timeoutMillis) {
        return wrappedProxy.getMetadata(getSerialVersion(), type, key, seqNum,
                                        NULL_CTX, timeoutMillis);
    }

    public CompletableFuture<Integer> updateMetadata(MetadataInfo metadataInfo,
                                                     long timeoutMillis) {
        return wrappedProxy.updateMetadata(getSerialVersion(), metadataInfo,
                                           NULL_CTX, timeoutMillis);
    }

    public CompletableFuture<Void> updateMetadata(Metadata<?> newMetadata,
                                                  long timeoutMillis) {
        return wrappedProxy.updateMetadata(getSerialVersion(),
                                           newMetadata, NULL_CTX,
                                           timeoutMillis);
    }

    public CompletableFuture<KerberosPrincipals>
        getKerberosPrincipals(long timeoutMs)
    {
        return wrappedProxy.getKerberosPrincipals(getSerialVersion(), NULL_CTX,
                                                  timeoutMs);
    }

    /**
     * Retrieve table metadata information by specific table id.
     *
     * @param tableId number of table id
     * @return metadata information which is a table instance
     */
    public CompletableFuture<MetadataInfo> getTableById(final long tableId,
                                                        final long timeoutMs)
    {
        return wrappedProxy.getTableById(getSerialVersion(), tableId, NULL_CTX,
                                         timeoutMs);
    }

    /**
     * Gets the specified table with an optional resource cost. If the table
     * is not found, null is returned. The specified cost will be charged
     * against the table's resource limits. If the cost is greater than 0,
     * the table has resource limits, and those limits have been exceeded,
     * either by this call, or by other table activity, a
     * ReadThroughputException will be thrown.
     *
     * @param namespace the table namespace
     * @param tableName the table name
     * @param cost the cost
     * @return a table instance or null
     */
    public CompletableFuture<MetadataInfo> getTable(String namespace,
                                                    String tableName,
                                                    int cost,
                                                    long timeoutMs) {
        return wrappedProxy.getTable(getSerialVersion(), namespace, tableName,
                                     cost, NULL_CTX, timeoutMs);
    }
}
