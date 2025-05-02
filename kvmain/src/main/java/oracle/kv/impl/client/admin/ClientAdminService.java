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

import java.net.URI;
import java.rmi.RemoteException;

import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.contextlogger.LogContext;
import oracle.kv.impl.util.registry.VersionedRemote;

/**
 * Defines the RMI interface used by the kvclient to asynchronously submit
 * DDL statements, which will be executed by the Admin service.
 */
public interface ClientAdminService extends VersionedRemote {

    /**
     * Ask the master Admin to execute the statement, specifying whether to
     * validate the namespace.
     *
     * @since 18.3
     */
    ExecutionInfo execute(char[] statement,
        String namespace,
        boolean validateNamespace,
        TableLimits limits,
        LogContext lc,
        AuthContext authCtx,
        short serialVersion)
        throws RemoteException;

    /**
     * Added in 18.1/cloud
     *
     * @since 18.1
     */
    ExecutionInfo setTableLimits(String namespace,
                                 String tableName,
                                 TableLimits limits,
                                 AuthContext authCtx,
                                 short serialVersion)
        throws RemoteException;

    /**
     * Get current status for the specified plan.
     */
    ExecutionInfo getExecutionStatus(int planId,
                                     AuthContext authCtx,
                                     short serialVersion)
        throws RemoteException;

    /**
     * Return true if this Admin can handle DDL operations. That currently
     * equates to whether the Admin is a master or not.
     *
     * @param authCtx
     * @param serialVersion
     * @throws RemoteException
     */
    boolean canHandleDDL(AuthContext authCtx, short serialVersion)
            throws RemoteException;

    /**
     * Return the address of the master Admin. If this Admin doesn't know that,
     * return null.
     */
    URI getMasterRmiAddress(AuthContext authCtx, short serialVersion)
            throws RemoteException;

    /**
     * Start cancellation of a plan. Return the current status.
     */
    ExecutionInfo interruptAndCancel(int planId,
                                     AuthContext nullCtx,
                                     short serialVersion)
            throws RemoteException;

    /**
     * Return the current topology used for connecting client to store using
     * Admin service. If topology is not available at this service, return null.
     *
     * @since 24.2
     */
    public Topology getTopology(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Returns the sequence number associated with the Topology. If topology is
     * not available, return Metadata.EMPTY_SEQUENCE_NUMBER.
     *
     * @since 24.2
     */
    public int getTopoSeqNum(AuthContext authCtx, short serialVersion)
        throws RemoteException;
}
