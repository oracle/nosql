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

import static oracle.kv.impl.util.SerialVersion.SHUTDOWN_REASON_VERSION;

import java.rmi.RemoteException;

import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ContextProxy;
import oracle.kv.impl.arb.ArbNodeStatus;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.util.registry.RemoteAPI;

/**
 * The administrative interface to a ArbNode process.
 */
public class ArbNodeAdminAPI extends RemoteAPI {

    /* Null value that will be filled in by proxyRemote */
    private final static AuthContext NULL_CTX = null;

    private final ArbNodeAdmin proxyRemote;

    private ArbNodeAdminAPI(ArbNodeAdmin remote, LoginHandle loginHdl)
        throws RemoteException {

        super(remote);
        this.proxyRemote = ContextProxy.create(remote, loginHdl,
                                               getSerialVersion());
    }

    public static ArbNodeAdminAPI wrap(ArbNodeAdmin remote,
                                       LoginHandle loginHdl)
        throws RemoteException {

        return new ArbNodeAdminAPI(remote, loginHdl);
    }

    /**
     * Notifies the AN that new parameters are available in the storage node
     * configuration file and that these should be reread.
     */
    public void newParameters()
        throws RemoteException {

        proxyRemote.newParameters(NULL_CTX, getSerialVersion());
    }

    /**
     * Notifies the AN that new global parameters are available in the storage
     * node configuration file and that these should be reread.
     */
    public void newGlobalParameters()
        throws RemoteException {

        proxyRemote.newGlobalParameters(NULL_CTX, getSerialVersion());
    }

    /**
     * Returns the in-memory values of the parameters for the AN. Used for
     * configuration verification.
     */
    public LoadParameters getParams()
        throws RemoteException {
        return proxyRemote.getParams(NULL_CTX, getSerialVersion());
    }

    /**
     * Shuts down this ArbNode process cleanly, without specifying a reason.
     * Convenience method for testing only.
     *
     * @param force force the shutdown
     */
    public void shutdown(boolean force)
        throws RemoteException {

        shutdown(force, "For testing");
    }

    /**
     * Shuts down this ArbNode process cleanly. If the reason is non-null,
     * include it in the message logged when the service shuts down.
     *
     * @param force force the shutdown
     * @param reason the reason for the shutdown, or null
     */
    /* Permit calls to deprecated overloading */
    @SuppressWarnings("deprecation")
    public void shutdown(boolean force, String reason)
        throws RemoteException {

        if (getSerialVersion() < SHUTDOWN_REASON_VERSION) {
            proxyRemote.shutdown(force, NULL_CTX, getSerialVersion());
            return;
        }

        proxyRemote.shutdown(force, reason, NULL_CTX, getSerialVersion());
    }

    /**
     * Returns the <code>ArbNodeStatus</code> associated with the rep node.
     *
     * @return the service status
     */
    public ArbNodeStatus ping()
        throws RemoteException {

        return proxyRemote.ping(NULL_CTX, getSerialVersion());
    }

    /**
     * Returns administrative and configuration information from the
     * arbNode. Meant for diagnostic and debugging support.
     */
    public ArbNodeInfo getInfo()
        throws RemoteException {

        return proxyRemote.getInfo(NULL_CTX, getSerialVersion());
    }

    /**
     * @param groupName
     * @param targetNodeName
     * @param targetHelperHosts
     * @param newNodeHostPort if null entry is removed.
     * @return true if this node's address can be updated in the JE
     * group database, false if there is no current master, and we need to
     * retry.
     * @throws RemoteException
     */
    public boolean updateMemberHAAddress(String groupName,
                                         String targetNodeName,
                                         String targetHelperHosts,
                                         String newNodeHostPort)
        throws RemoteException{

        return proxyRemote.updateMemberHAAddress(groupName,
                                                 targetNodeName,
                                                 targetHelperHosts,
                                                 newNodeHostPort,
                                                 NULL_CTX,
                                                 getSerialVersion());
    }
}
