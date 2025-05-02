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

package oracle.kv.impl.monitor;

import java.rmi.RemoteException;
import java.util.List;

import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ContextProxy;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.util.registry.RemoteAPI;

/**
 * Client API wrapper for MonitorAgent.
 */
public class MonitorAgentAPI extends RemoteAPI {
    
    private static final AuthContext NULL_CTX = null;

    private final MonitorAgent proxyRemote;
    
    private MonitorAgentAPI(MonitorAgent remote, LoginHandle loginHdl)
        throws RemoteException {

        super(remote);
        this.proxyRemote = ContextProxy.create(remote, loginHdl,
                                               getSerialVersion());
    }

    public static MonitorAgentAPI wrap(MonitorAgent remote,
                                       LoginHandle loginHdl)
        throws RemoteException {

        return new MonitorAgentAPI(remote, loginHdl);
    }

    /**
     * Return all new values of monitored data.
     */
    public List<Measurement> getMeasurements()
        throws RemoteException {

        return proxyRemote.getMeasurements(NULL_CTX, getSerialVersion());
    }
}
