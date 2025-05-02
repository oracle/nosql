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

import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.util.registry.VersionedRemote;

/**
 * A MonitorAgent has instrumentation to deliver to the 
 * {@link Collector}
 * 
 * RepNodes and StorageNodeAgents implement MonitorAgent. The contract
 */
public interface MonitorAgent extends VersionedRemote {
    
    /**
     * Return all new values of monitored data.
     *
     * @since 3.0
     */
    public List<Measurement> getMeasurements(AuthContext authCtx,
                                             short serialVersion)
        throws RemoteException;
}
