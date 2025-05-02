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
import oracle.kv.impl.security.util.KerberosPrincipals;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.VersionedRemote;

import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * The administrative interface to a RepNode needed by clients.
 *
 * @since 21.2
 */
public interface ClientRepNodeAdmin extends VersionedRemote {

    /**
     * Returns this RN's view of the Topology. In a distributed system like
     * KVS, it may be temporarily different from the Topology at other nodes,
     * but will eventually become eventually consistent.
     *
     * It returns null if the RN is not in the RUNNING state.
     *
     * @since 3.0
     */
    public Topology getTopology(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Returns the sequence number associated with the Topology at the RN.
     *
     *  It returns zero if the RN is not in the RUNNING state.
     *
     * @since 3.0
     */
    public int getTopoSeqNum(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Returns a string containing the HA hostname and port for the master in
     * this RN's shard.
     *
     * @since 21.2
     */
    public String getHAHostPort(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Returns this node's replication state.
     *
     * @since 21.2
     */
    public ReplicatedEnvironment.State getReplicationState(AuthContext authCtx,
                                                           short serialVersion)
        throws RemoteException;

    /**
     * Returns this node's vlsn.
     *
     * @since 22.2
     */
    public long getVlsn(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Returns the sequence number associated with the metadata at the RN.
     * If the RN does not contain the metadata or the RN is not in the RUNNING
     * state null is returned.
     *
     * @param type a metadata type
     *
     * @return the sequence number associated with the metadata
     *
     * @since 3.0
     */
    public int getMetadataSeqNum(Metadata.MetadataType type,
                                 AuthContext authCtx,
                                 short serialVersion)
        throws RemoteException;

    /**
     * Gets the metadata for the specified type. If the RN does not contain the
     * metadata or the RN is not in the RUNNING state null is returned.
     *
     * @param type a metadata type
     *
     * @return metadata
     *
     * @since 3.0
     */
    public Metadata<?> getMetadata(Metadata.MetadataType type,
                                   AuthContext authCtx,
                                   short serialVersion)
        throws RemoteException;

    /**
     * Gets metadata information for the specified type starting from the
     * specified sequence number. If the RN is not in the RUNNING state null is
     * returned.
     *
     * @param type a metadata type
     * @param seqNum a sequence number
     *
     * @return metadata info describing the changes
     *
     * @since 3.0
     */
    public MetadataInfo getMetadata(Metadata.MetadataType type,
                                    int seqNum,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * Gets metadata information for the specified type and key starting from
     * the specified sequence number. If the RN is not in the RUNNING state null
     * is returned.
     *
     * @param type a metadata type
     * @param key a metadata key
     * @param seqNum a sequence number
     *
     * @return metadata info describing the changes
     *
     * @throws UnsupportedOperationException if the operation is not supported
     * by the specified metadata type
     *
     * @since 3.0
     */
    public MetadataInfo getMetadata(Metadata.MetadataType type,
                                    MetadataKey key,
                                    int seqNum,
                                    AuthContext authCtx,
                                    short serialVersion)
        throws RemoteException;

    /**
     * Informs the RepNode about an update to the metadata.
     *
     * @param newMetadata the latest metadata
     *
     * @since 3.0
     */
    public void updateMetadata(Metadata<?> newMetadata,
                               AuthContext authCtx,
                               short serialVersion)
        throws RemoteException;

    /**
     * Informs the RepNode about an update to the metadata.
     *
     * @param metadataInfo describes the changes to be applied
     *
     * @return the post-update metadata sequence number at the node
     *
     * @since 3.0
     */
    public int updateMetadata(MetadataInfo metadataInfo,
                              AuthContext authCtx,
                              short serialVersion)
        throws RemoteException;

    /**
     * @since 3.5
     */
    public KerberosPrincipals getKerberosPrincipals(AuthContext authCtx,
                                                    short serialVersion)
        throws RemoteException;

    /**
     * Retrieve table metadata information by specific table id.
     *
     * @param tableId number of table id
     * @param authCtx used for security authentication
     * @param serialVersion
     * @return metadata information which is a table instance, null if
     * the table does not exist
     * @throws RemoteException
     *
     * @since 18.1
     */
    public MetadataInfo getTableById(long tableId,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * Gets the specified table with an optional resource cost. The specified
     * cost will be charged against the table's resource limits if it has
     * limits.
     *
     * @return metadata information which is a table instance, null if
     * the table does not exist
     *
     * @throws ReadThroughputException if cost is non-zero and the table has
     * limits and the operation is throttled
     *
     * @since 18.2
     */
    public MetadataInfo getTable(String namespace,
                                 String tableName,
                                 int cost,
                                 AuthContext authCtx,
                                 short serialVersion)
        throws RemoteException;
}
