/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna.masterBalance;

import java.rmi.RemoteException;

import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.StorageNodeAgentNOP;

/**
 * Mock SNA that provides the master balance interfaces
 */
final class SNAMasterBalance extends StorageNodeAgentNOP {
    private final MasterBalanceManager mbm;

    /**
     * @param mbm
     */
    SNAMasterBalance(MasterBalanceManager mbm) {
        this.mbm = mbm;
    }

    @Override
    public short getSerialVersion() {
       return SerialVersion.CURRENT;
    }

    @Override
    public void noteState(StateInfo stateInfo,
                          AuthContext authContext, 
                          short serialVersion) {
        mbm.noteState(stateInfo, authContext, serialVersion);
    }

    @Override
    public MDInfo getMDInfo(AuthContext authContext, 
                            short serialVersion) throws RemoteException {
        return mbm.getMDInfo(authContext, serialVersion);
    }

    @Override
    public boolean getMasterLease(MasterLeaseInfo masterLease,
                                  AuthContext authContext, 
                                  short serialVersion)
        throws RemoteException {

        return mbm.getMasterLease(masterLease, authContext, serialVersion);
    }

    @Override
    public boolean cancelMasterLease(StorageNode lesseeSN,
                                     RepNode rn,
                                     AuthContext authContext, 
                                     short serialVersion)
        throws RemoteException {

        return mbm.cancelMasterLease(lesseeSN, rn, authContext, serialVersion);
    }
}
