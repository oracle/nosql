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

import java.rmi.RemoteException;

import oracle.kv.ReadThroughputException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.metadata.MetadataKey;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ContextProxy;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.util.KerberosPrincipals;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RemoteAPI;

import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * The administrative interface to a RepNode needed by clients.
 *
 * @since 21.2
 */
public class ClientRepNodeAdminAPI extends RemoteAPI {

    /* Null value that will be filled in by proxyRemote */
    final static AuthContext NULL_CTX = null;

    /**
     * The proxy for contacting the RepNodeAdmin on the server. Note that this
     * proxy implements the full admin interface, not just the client methods,
     * even though the client API should be limited to the client methods. To
     * maintain compatibility with earlier versions, we use the existing RMI
     * server.
     */
    final RepNodeAdmin proxyRemote;

    ClientRepNodeAdminAPI(RepNodeAdmin remote, LoginHandle loginHdl)
        throws RemoteException
    {
        this(remote, loginHdl, remote.getSerialVersion());
    }

    private ClientRepNodeAdminAPI(RepNodeAdmin remote,
                                  LoginHandle loginHdl,
                                  short serverSerialVersion) {
        super(remote, serverSerialVersion);
        this.proxyRemote = (loginHdl != null) ?
            ContextProxy.create(remote, loginHdl, getSerialVersion()) :
            remote;
    }

    public static ClientRepNodeAdminAPI wrap(RepNodeAdmin remote,
                                             LoginHandle loginHdl)
        throws RemoteException {

        return new ClientRepNodeAdminAPI(remote, loginHdl,
                                         remote.getSerialVersion());
    }

    /**
     * Returns this RN's view of the Topology. In a distributed system like
     * KVS, it may be temporarily different from the Topology at other nodes,
     * but will eventually become eventually consistent.
     */
    public Topology getTopology()
        throws RemoteException {

        return proxyRemote.getTopology(NULL_CTX, getSerialVersion());
    }

    /**
     * Returns the sequence number associated with the Topology at the RN.
     */
    public int getTopoSeqNum()
        throws RemoteException {

        return proxyRemote.getTopoSeqNum(NULL_CTX, getSerialVersion());
    }

    /**
     * Returns a string containing the HA hostname and port for the master in
     * this RN's shard.
     */
    public String getHAHostPort() throws RemoteException {
        final short serialVersion = getSerialVersion();
        return proxyRemote.getHAHostPort(NULL_CTX, serialVersion);
    }

    /**
     * Returns this node's replication state.
     */
    public ReplicatedEnvironment.State getReplicationState()
        throws RemoteException
    {
        final short serialVersion = getSerialVersion();
        return proxyRemote.getReplicationState(NULL_CTX, serialVersion);
    }

    /**
     * Returns this node's vlsn.
     */
    public long getVlsn() throws RemoteException
    {
        return proxyRemote.getVlsn(NULL_CTX, getSerialVersion());
    }

    /**
     * @see RepNodeAdmin#getMetadataSeqNum(Metadata.MetadataType, AuthContext,
     * short)
     */
    public int getMetadataSeqNum(Metadata.MetadataType type)
        throws RemoteException {
        return proxyRemote.getMetadataSeqNum(type, NULL_CTX,
                                             getSerialVersion());
    }

    /**
     * @see RepNodeAdmin#getMetadata(Metadata.MetadataType, AuthContext, short)
     */
    public Metadata<?> getMetadata(Metadata.MetadataType type)
        throws RemoteException {
        return proxyRemote.getMetadata(type, NULL_CTX, getSerialVersion());
    }

    /**
     * @see RepNodeAdmin#getMetadata(Metadata.MetadataType, int, AuthContext,
     * short)
     */
    public MetadataInfo getMetadata(Metadata.MetadataType type, int seqNum)
        throws RemoteException {
        return proxyRemote.getMetadata(type, seqNum, NULL_CTX,
                                       getSerialVersion());
    }

    /**
     * @see RepNodeAdmin#getMetadata(Metadata.MetadataType, MetadataKey, int,
     * AuthContext, short)
     */
    public MetadataInfo getMetadata(Metadata.MetadataType type,
                                    MetadataKey key,
                                    int seqNum)
        throws RemoteException {
        checkVersion(key.getRequiredSerialVersion());
        return proxyRemote.getMetadata(type, key, seqNum, NULL_CTX,
                                  getSerialVersion());
    }

    /**
     * @see RepNodeAdmin#updateMetadata(MetadataInfo, AuthContext, short)
     */
    public int updateMetadata(MetadataInfo metadataInfo)
        throws RemoteException {
        return proxyRemote.updateMetadata(metadataInfo, NULL_CTX,
                                     getSerialVersion());
    }

    /**
     * @see RepNodeAdmin#updateMetadata(Metadata, AuthContext, short)
     */
    public void updateMetadata(Metadata<?> newMetadata)
        throws RemoteException {
        proxyRemote.updateMetadata(newMetadata, NULL_CTX, getSerialVersion());
    }

    public KerberosPrincipals getKerberosPrincipals()
        throws RemoteException {
        return proxyRemote.getKerberosPrincipals(NULL_CTX, getSerialVersion());
    }

    /**
     * Retrieve table metadata information by specific table id.
     *
     * @param tableId number of table id
     * @return metadata information which is a table instance
     * @throws RemoteException
     */
    public MetadataInfo getTableById(final long tableId)
        throws RemoteException {
        return proxyRemote.getTableById(tableId,
                                        NULL_CTX,
                                        getSerialVersion());
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
     * @throws RemoteException
     * @throws ReadThroughputException
     */
    public MetadataInfo getTable(String namespace,
                                 String tableName,
                                 int cost)
        throws RemoteException {
        return proxyRemote.getTable(namespace, tableName, cost,
                                    NULL_CTX, getSerialVersion());
    }

    private void checkVersion(short requiredVersion) {
        if (getSerialVersion() < requiredVersion) {
            throw new UnsupportedOperationException(
                "Command not available because service has not yet been" +
                " upgraded.  (Internal local version=" +
                requiredVersion +
                ", internal service version=" + getSerialVersion() + ")");
        }
    }
}
