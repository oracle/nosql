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

import oracle.kv.impl.topo.RepNodeId;

/**
 * The interface is implemented by the real MasterBalanceManger and by
 * MasterBalanceManagerDisabled. The latter class is used when the MBM has been
 * disabled at the SNA.
 */
public interface MasterBalanceManagerInterface
    extends MasterBalancingInterface {

    /**
     * Invoked to note that a particular RN has exited. If the RN exit was not
     * abrupt, this call may have been preceded by a noteState change called
     * communicating the DETACHED state. The method is resilient in the face
     * of such redundant calls.
     */
    public void noteExit(RepNodeId rnId);

    /**
     * Cleanup on exit.
     */
    public void shutdown();

    /**
     * Initiate master transfers for RNs that have masters on the current SN in
     * preparation for shutting down the SN. This method also causes the master
     * balance manager to reject requests to transfer masters to this SNA.
     */
    public void transferMastersForShutdown();
}
