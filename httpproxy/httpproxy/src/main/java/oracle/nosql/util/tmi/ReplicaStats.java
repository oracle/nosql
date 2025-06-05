/*-
 * Copyright (C) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 */

package oracle.nosql.util.tmi;

/**
 * ReplicaStats represent information for a table and its associated replica.
 * For example, if a table exists on YULPP and is replicated to IADPP and
 * YYZPP, then YULPP's table info will have:
 *  - a IADPP replica stats representing how much YULPP lags behind IADPP
 *  - a YYZPP replica stats representing how much YULPP lags behind YYZPP
 */
public class ReplicaStats {
    private static final int CURRENT_VERSION = 1;
    private int version;
    /* The collection timestamp, milliseconds since Epoch */
    private long time;
    /* The replication lag in milliseconds */
    private int replicaLag;

    public ReplicaStats(long time, int replicaLag) {
        version = CURRENT_VERSION;
        this.time = time;
        this.replicaLag = replicaLag;
    }

    public int getVersion() {
        return version;
    }

    public long getTime() {
        return time;
    }

    public int getReplicaLag() {
        return replicaLag;
    }

    @Override
    public String toString() {
        return "ReplicaStats [time=" + time + ", replicaLag=" + replicaLag + "]";
    }
}
