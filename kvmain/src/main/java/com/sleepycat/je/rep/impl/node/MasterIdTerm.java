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

package com.sleepycat.je.rep.impl.node;

/**
 * The immutable Master id + term pair.
 *
 * A term maps to exactly one and the same master id across all the logs in a
 * replication group.
 */
public class MasterIdTerm {

    /**
     * A pair of placeholder values used when an environment is first created.
     *
     * At a master, this pair is rendered obsolete as soon as the master fully
     * initializes itself.
     *
     * At a replica, it's obsoleted as soon as the replica gets its first
     * txn commit record with this pair in it.
     */
    public static MasterIdTerm PRIMODAL_ID_TERM =
        new MasterIdTerm(NameIdPair.NULL_NODE_ID, MasterTerm.NULL);

    public final int nodeId;
    public final long term;

    public MasterIdTerm(int nodeId, long term) {
        this(nodeId, term, true);
    }

    private MasterIdTerm(int nodeId, long term, boolean check) {
        super();
        this.nodeId = nodeId;
        this.term = term;

        if (check) {
            if (!((nodeId == NameIdPair.NULL_NODE_ID) ^
                  (term != MasterTerm.NULL))) {
                throw new IllegalArgumentException("Arguments"+  logString());
            }
        }
    }

    /**
     * A method to create MasterIdTerm without check for tests.
     */
    public static MasterIdTerm createForTest(int nodeId, long term) {
        return new MasterIdTerm(nodeId, term, false);
    }

    public String logString() {
        return String.format("<MasterIdTerm nodeId=%d, term=%,d >",
                              nodeId, term);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + nodeId;
        result = prime * result + (int) (term ^ (term >>> 32));
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
        MasterIdTerm other = (MasterIdTerm) obj;
        if (nodeId != other.nodeId) {
            return false;
        }
        if (term != other.term) {
            return false;
        }
        return true;
    }

}
