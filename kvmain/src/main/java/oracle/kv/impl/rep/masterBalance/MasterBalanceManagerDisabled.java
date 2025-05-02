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

package oracle.kv.impl.rep.masterBalance;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.impl.topo.RepNodeId;

import com.sleepycat.je.rep.StateChangeEvent;

/**
 * MasterBalanceManagerDisabled supplied the placebo method implementations
 * for use when the MasterBalanceManager has been disabled at the RN
 */
class MasterBalanceManagerDisabled implements MasterBalanceManagerInterface {

    private final Logger logger;

    public MasterBalanceManagerDisabled(Logger logger) {
        super();
        this.logger = logger;
        logger.info("Master balance manager disabled at the RN");
    }

    @Override
    public boolean initiateMasterTransfer(RepNodeId replicaId,
                                          int timeout,
                                          TimeUnit timeUnit) {
        logger.info("Unexpected request for master transfer to " + replicaId +
                    "at disabled master balance manager");
        return false;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void noteStateChange(StateChangeEvent stateChangeEvent) {
    }

    @Override
    public void initialize() {
    }

    @Override
    public void startTracker() {
    }

    @Override
    public MasterBalanceStateTracker getStateTracker() {
        /* Used only in test situations, should never be invoked. */
        throw new UnsupportedOperationException("Method not implemented: " +
                                                "getStateTracker");
    }
}
