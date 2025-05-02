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

package oracle.kv.impl.sna.masterBalance;

import java.rmi.RemoteException;
import java.util.logging.Logger;

import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * MasterBalanceManagerDisabled supplied the placebo method implementations
 * for use when the MasterBalanceManager has been disabled at the SNA
 */
class MasterBalanceManagerDisabled
    implements MasterBalanceManagerInterface {

    final Logger logger;

    MasterBalanceManagerDisabled(Logger logger) {
        super();
        this.logger = logger;
        logger.info("Master balance manager disabled at the SNA");
    }

    @Override
    public void noteState(StateInfo stateInfo,
                          AuthContext authContext,
                          short serialVersion)
        throws RemoteException {
        /* NOP */
    }

    @Override
    public MDInfo getMDInfo(AuthContext authContext,
                            short serialVersion) throws RemoteException {
        /*
         * Returning null to the caller effectively tells it the SN
         * will not participate in master balancing.
         */
        return null;
    }

    @Override
    public boolean getMasterLease(MasterLeaseInfo masterLease,
                                  AuthContext authContext,
                                  short serialVersion) throws RemoteException {
        /* decline all requests for a master lease. */
        return false;
    }

    @Override
    public void transferMastersForShutdown() {
        /* NOP */
    }

    @Override
    public boolean cancelMasterLease(StorageNode lesseeSN,
                                     RepNode rn,
                                     AuthContext authContext,
                                     short serialVersion)
        throws RemoteException {

        logger.info("Unexpected service request to cancel a master lease " +
                    "for RN: " + rn +
                    " Lessee SN: " + lesseeSN);
        return false;
    }

    @Override
    public void overloadedNeighbor(StorageNodeId storageNodeId,
                                   AuthContext authContext,
                                   short serialVersion)
        throws RemoteException {

        /* NOP */
    }

    @Override
    public void noteExit(RepNodeId rnId) {
        /* NOP */
    }

    @Override
    public void shutdown() {
        /* NOP */
    }
}
