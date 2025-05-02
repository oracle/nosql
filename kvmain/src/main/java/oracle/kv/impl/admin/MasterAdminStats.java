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

package oracle.kv.impl.admin;

import java.io.Serializable;
import java.util.SortedMap;

import com.sleepycat.je.rep.ReplicatedEnvironmentStats;

/** Stores JE HA replication statistics associated with a master admin. */
public class MasterAdminStats implements Serializable {
    private static final long serialVersionUID = 1;
    private final SortedMap<String, Long> replicaDelayMillisMap;

    public static MasterAdminStats create(ReplicatedEnvironmentStats stats) {
        return (stats != null) ? new MasterAdminStats(stats) : null;
    }

    private MasterAdminStats(ReplicatedEnvironmentStats stats) {
        replicaDelayMillisMap = stats.getReplicaDelayMap();
    }

    /** @see ReplicatedEnvironmentStats#getReplicaDelayMap */
    public SortedMap<String, Long> getReplicaDelayMillisMap() {
        return replicaDelayMillisMap;
    }
}

