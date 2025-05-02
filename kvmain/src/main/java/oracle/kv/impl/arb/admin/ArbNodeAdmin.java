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

package oracle.kv.impl.arb.admin;

import java.rmi.RemoteException;

import oracle.kv.impl.arb.ArbNodeStatus;
import oracle.kv.impl.mgmt.ArbNodeStatusReceiver;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.util.registry.VersionedRemote;

/**
 * The administrative interface to a ArbNode process.
 * @since 4.0
 */
public interface ArbNodeAdmin extends VersionedRemote {

    /**
     * Indicates that new parameters are available in the storage node
     * configuration file and that these should be reread.
     */
    public void newParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Indicates that new global parameters are available in the storage node
     * configuration file and that these should be reread.
     */
    void newGlobalParameters(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Return this ARB's view of its current parameters. Used for configuration
     * verification.
     */
    public LoadParameters getParams(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Shuts down this ArbNode process cleanly, without specifying a reason.
     *
     * @param force force the shutdown
     * @deprecated since 22.3
     */
    @Deprecated
    public void shutdown(boolean force,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException;

    /**
     * Shuts down this ArbNode process cleanly.
     *
     * @param force force the shutdown
     * @param reason the reason for the shutdown, or null
     * @since 22.3
     */
    public void shutdown(boolean force,
                         String reason,
                         AuthContext authCtx,
                         short serialVersion)
        throws RemoteException;

    /**
     * Returns the <code>ArbNodeStatus</code> associated with the arb node.
     *
     * @return the service status
     */
    public ArbNodeStatus ping(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    /**
     * Returns administrative and configuration information from the
     * arbNode. Meant for diagnostic and debugging support.
     */
    public ArbNodeInfo getInfo(AuthContext authCtx, short serialVersion)
        throws RemoteException;

    public boolean updateMemberHAAddress(String groupName,
                                         String fullName,
                                         String targetHelperHosts,
                                         String newNodeHostPort,
                                         AuthContext authCtx,
                                         short serialVersion)
        throws RemoteException;

    /**
     * Install a receiver for ArbNode status updates, for delivering metrics
     * and service change information to the standardized monitoring/management
     * agent.
     *
     * @deprecated since 21.2
     */
    @Deprecated
    public void installStatusReceiver(ArbNodeStatusReceiver receiver,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException;

}
