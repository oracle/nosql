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

package oracle.kv.impl.util;

import oracle.kv.Durability;
import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.Durability.SyncPolicy;

/**
 * Utility methods to translate between HA and KV durability.
 */
public class DurabilityTranslator {

    public static com.sleepycat.je.Durability
        translate(Durability durability) {

        final SyncPolicy masterSync = durability.getMasterSync();
        final SyncPolicy replicaSync = durability.getReplicaSync();
        final ReplicaAckPolicy replicaAck = durability.getReplicaAck();

        return new com.sleepycat.je.Durability(translate(masterSync),
                                               translate(replicaSync),
                                               translate(replicaAck));
    }

    public static Durability
        translate(com.sleepycat.je.Durability durability) {

        final com.sleepycat.je.Durability.SyncPolicy masterSync =
            durability.getLocalSync();
        final com.sleepycat.je.Durability.SyncPolicy replicaSync =
            durability.getReplicaSync();
        final com.sleepycat.je.Durability.ReplicaAckPolicy
        replicaAck = durability.getReplicaAck();

        return new Durability(translate(masterSync), translate(replicaSync),
                              translate(replicaAck));
    }

    /**
     * Translates syncPolicy
     */
    public static com.sleepycat.je.Durability.SyncPolicy
        translate(SyncPolicy sync) {

        if (sync == null) {
            return null;
        }

       if (SyncPolicy.NO_SYNC.equals(sync)) {
           return com.sleepycat.je.Durability.SyncPolicy.NO_SYNC;
       } else if (SyncPolicy.WRITE_NO_SYNC.equals(sync)) {
           return com.sleepycat.je.Durability.SyncPolicy.WRITE_NO_SYNC;
       } else if (SyncPolicy.SYNC.equals(sync)) {
           return com.sleepycat.je.Durability.SyncPolicy.SYNC;
       } else {
           throw new IllegalArgumentException("Unknown sync: " + sync);
       }
    }

    public static SyncPolicy
        translate(com.sleepycat.je.Durability.SyncPolicy sync) {

        if (sync == null) {
            return null;
        }

        if (com.sleepycat.je.Durability.SyncPolicy.NO_SYNC.equals(sync)) {
            return SyncPolicy.NO_SYNC;
        } else if (com.sleepycat.je.Durability.SyncPolicy.WRITE_NO_SYNC.
                equals(sync)) {
            return SyncPolicy.WRITE_NO_SYNC;
        } else if (com.sleepycat.je.Durability.SyncPolicy.SYNC.equals(sync)) {
            return SyncPolicy.SYNC;
        }
        throw new IllegalArgumentException("Unknown sync: " + sync);
    }

    public static ReplicaAckPolicy translate
        (com.sleepycat.je.Durability.ReplicaAckPolicy ackPolicy) {

        if (ackPolicy == null) {
            return null;
        }

        if (com.sleepycat.je.Durability.ReplicaAckPolicy.NONE.
                equals(ackPolicy)) {
            return ReplicaAckPolicy.NONE;
        } else if (com.sleepycat.je.Durability.ReplicaAckPolicy.
                SIMPLE_MAJORITY.equals(ackPolicy)) {
            return ReplicaAckPolicy.SIMPLE_MAJORITY;
        } if (com.sleepycat.je.Durability.ReplicaAckPolicy.ALL.
                equals(ackPolicy)) {
            return ReplicaAckPolicy.ALL;
        }
        throw new IllegalArgumentException("Unknown ack: " + ackPolicy);
    }

    public static com.sleepycat.je.Durability.ReplicaAckPolicy translate
        (ReplicaAckPolicy ackPolicy) {

        if (ackPolicy == null) {
            return null;
        }

        if (ReplicaAckPolicy.NONE.equals(ackPolicy)) {
            return com.sleepycat.je.Durability.ReplicaAckPolicy.NONE;
        } else if (ReplicaAckPolicy.SIMPLE_MAJORITY.equals(ackPolicy)) {
            return com.sleepycat.je.Durability.ReplicaAckPolicy.
                SIMPLE_MAJORITY;
        } if (ReplicaAckPolicy.ALL.equals(ackPolicy)) {
            return com.sleepycat.je.Durability.ReplicaAckPolicy.ALL;
        }
        throw new IllegalArgumentException("Unknown ack: " + ackPolicy);
    }
}
