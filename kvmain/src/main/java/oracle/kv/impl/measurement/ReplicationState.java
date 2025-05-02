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
package oracle.kv.impl.measurement;

import java.io.Serializable;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

/**
 * Indication of the node's replication state at the time of reporting.
 */
public class ReplicationState implements ConciseStats, Serializable {

    private static final long serialVersionUID = 1L;

    private final long start;
    private final long end;
    private final State state;

    public ReplicationState(long start, long end, State state) {
        this.start = start;
        this.end = end;
        this.state = state;
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public long getEnd() {
        return end;
    }

    @Override
    public String getFormattedStats() {
        return "Replication State: " + state;
    }

    @Override
    public String toString() {
        return state.toString();
    }

    public State getState() {
        return state;
    }
}
