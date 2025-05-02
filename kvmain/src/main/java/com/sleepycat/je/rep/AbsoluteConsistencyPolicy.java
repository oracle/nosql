/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.rep;

import java.util.concurrent.TimeUnit;

import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.txn.ReadonlyTxn;
import com.sleepycat.je.utilint.PropUtil;

/**
 * A consistency policy that requires reading on an authoritative master, that
 * is, a master that is currently in contact with a simple majority of
 * ELECTABLE replicas. This is the strongest form of read consistency and
 * ensures that the data being read is up-to-date.
 */
public class AbsoluteConsistencyPolicy implements ReplicaConsistencyPolicy {

    /**
     * The name:{@value} associated with this policy. The name can be used when
     * constructing policy property values for use in je.properties files.
     */
    public static final String NAME = "AbsoluteConsistencyPolicy";

    /*
     * Amount of time (in milliseconds) to wait for consistency to be
     * reached.
     */
    private final int timeoutMs;
    
    private ReadonlyTxn txn;

    /**
     * Create an Absolute Consistency Policy with the required timeout.
     *
     * @param timeout the amount of time to wait for the master to be confirmed
     * as being authoritative by a simple majority of ELECTABLE replicas.
     *
     * @param timeoutUnit the {@code TimeUnit} for the timeout parameter.
     */
    public AbsoluteConsistencyPolicy(long timeout,
                                     TimeUnit timeoutUnit) {
        this.timeoutMs = PropUtil.durationToMillis(timeout, timeoutUnit);
    }

    public void setTxn(final ReadonlyTxn txn) {
        this.txn = txn;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + timeoutMs;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        AbsoluteConsistencyPolicy other = (AbsoluteConsistencyPolicy) obj;
        if (timeoutMs != other.timeoutMs) {
            return false;
        }
        return true;
    }

    @Override
    public void ensureConsistency(EnvironmentImpl envImpl)
        throws InterruptedException {
        RepImpl repImpl = (RepImpl) envImpl;
        if (!repImpl.getState().isMaster()) {
            throw new ReplicaStateException("AbsoluteConsistency requires " +
                "that the node be a Master");
        }
        assert txn != null;
        repImpl.getRepNode().awaitAuthoritativeMaster(txn);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public long getTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(timeoutMs, unit);
    }

    @Override
    public String toString(){
        return getName() + " timeoutMs=" + timeoutMs;
    }
}
