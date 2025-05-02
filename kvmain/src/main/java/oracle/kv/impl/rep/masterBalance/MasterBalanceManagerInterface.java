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

import oracle.kv.impl.topo.RepNodeId;

import com.sleepycat.je.rep.StateChangeEvent;

/**
 * The abstract base class. It has two subclasses:
 *
 * 1) MasterBalanceManager the real class that implements master balancing
 *
 * 2) MasterBalanceManagerDisabled that's used when MBM has been turned off.
 *
 * Please see MasterBalanceManager for the documentation associated
 * with the abstract methods below.
 */
public interface MasterBalanceManagerInterface {

    public abstract boolean initiateMasterTransfer(RepNodeId replicaId,
                                                   int timeout,
                                                   TimeUnit timeUnit);
    public abstract void shutdown();

    public abstract void noteStateChange(StateChangeEvent stateChangeEvent);

    public abstract void initialize();

    public abstract void startTracker();

    /* For testing only */
    public abstract MasterBalanceStateTracker getStateTracker();
}