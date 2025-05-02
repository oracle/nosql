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

import java.util.logging.Logger;

import oracle.kv.impl.topo.RepNodeId;

/**
 * Manages replica leases at a SN. A replica lease is associated with a RN that
 * is currently a Master and will transition to a replica to help balance
 * master load. It's established by the source SN.
 *
 * A replica lease is established at a source SN to ensure that an attempt is
 * not made to migrate a master RN multiple times while a master transfer
 * operation is in progress. That is, while the master RN is transitioning to
 * the replica state as a result of the master transfer.
 */
public class ReplicaLeaseManager extends LeaseManager {

    ReplicaLeaseManager(Logger logger) {
        super(logger);
    }

    synchronized void getReplicaLease(ReplicaLease lease) {

        final RepNodeId rnId = lease.getRepNodeId();
        LeaseTask leaseTask = leaseTasks.get(rnId);

        if (leaseTask != null) {
            /*
             * Establish a new lease after canceling the existing lease.
             */
            leaseTask.cancel();
            leaseTask = null;
        }

        leaseTask = new LeaseTask(lease);
        logger.info("Established replica lease:" + lease);
    }

    /* The replica lease. */
    static class ReplicaLease implements LeaseManager.Lease {

        /* The master RN that will transition to a replica. */
        private final RepNodeId rnId;

        private final int leaseDurationMs;

        ReplicaLease(RepNodeId rnId, int leaseDurationMs) {
            super();
            this.rnId = rnId;
            this.leaseDurationMs = leaseDurationMs;
        }

        @Override
        public String toString() {
            return String.format("Replica lease: %s, duration: %d ms",
                                 rnId.getFullName(), leaseDurationMs);
        }

        @Override
        public RepNodeId getRepNodeId() {
            return rnId;
        }

        @Override
        public int getLeaseDuration() {
            return leaseDurationMs;
        }
    }
}
