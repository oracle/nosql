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

package com.sleepycat.je.rep.impl;

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;

import java.util.concurrent.TimeUnit;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.impl.node.Replica;
import com.sleepycat.je.utilint.PropUtil;

/**
 * This is used to ensure that the Replica has finished replaying or proceeded
 * past the vlsn specified by the policy. It's like the externally visible
 * CommitPointConsistencyPolicy, except that the latter restricts consistency
 * points to commit vlsns, whereas this policy lets you sync at uncommitted log
 * entries.
 */
public class PointConsistencyPolicy
    implements ReplicaConsistencyPolicy {

    /**
     * The name:{@value} associated with this policy. The name can be used when
     * constructing policy property values for use in je.properties files.
     */
    public static final String NAME = "PointConsistencyPolicy";

    private final long targetVLSN;

    /*
     * Amount of time (in milliseconds) to wait for consistency to be
     * reached.
     */
    private final int timeout;

    public PointConsistencyPolicy(long targetVLSN) {
        this(targetVLSN, Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public PointConsistencyPolicy(long targetVLSN,
                                  long timeout,
                                  TimeUnit timeoutUnit) {
        this.targetVLSN = targetVLSN;
        this.timeout = PropUtil.durationToMillis(timeout, timeoutUnit);
    }

    /**
     * Returns the name:{@value #NAME}, associated with this policy.
     * @see #NAME
     */
    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Ensures that the replica has replayed the replication stream to the
     * point identified by the commit token. If it isn't the method waits until
     * the constraint is satisfied by the replica.
     */
    @Override
    public void ensureConsistency(EnvironmentImpl replicatorImpl)
        throws InterruptedException,
               ReplicaConsistencyException,
               DatabaseException {

        /*
         * Cast is done to preserve replication/non replication code
         * boundaries.
         */
        RepImpl repImpl = (RepImpl) replicatorImpl;
        Replica replica = repImpl.getRepNode().replica();
        replica.getConsistencyTracker().awaitVLSN(targetVLSN,
                                                  this);
    }

    @Override
    public long getTimeout(TimeUnit unit) {
        return PropUtil.millisToDuration(timeout, unit);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((targetVLSN == INVALID_VLSN) ?
                   0 : Long.hashCode(targetVLSN));
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
        PointConsistencyPolicy other = (PointConsistencyPolicy) obj;
        return (targetVLSN == other.targetVLSN);
    }

    @Override
    public String toString() {
        return getName() + " targetVLSN=" + targetVLSN;
    }
}
