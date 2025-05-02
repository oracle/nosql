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

import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.MasterLeaseInfo;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;

/**
 * The MasterLeaseManager is the locus of master lease management activity.
 * It's a component of the MasterBalanceManager.
 * <p>
 * A source SN must establish a lease on a replica RN at the target SN before
 * it initiates a master transfer to ensure that the target SN does not itself
 * become unbalanced by multiple concurrent transfers to it.
 * <p>
 * A master lease is used to temporarily reserve a replica RN as the target of
 * a master transfer. It contains the source and target RNs involved in the
 * transfer along with the time period for which it is valid. A target RN can
 * have at most one active lease associated with it.
 */
public class MasterLeaseManager extends LeaseManager {

    MasterLeaseManager(Logger logger) {
        super(logger);
    }

    /**
     * Obtains a lease on an RN as a prelude to initiating a master transfer.
     * If the lesseeSN already holds a lease on the RN, the lease is replaced
     * by a new one. If another lessee already holds the lease the request is
     * rejected.
     *
     * @return true if the lease is granted. Null otherwise.
     */
    synchronized boolean getMasterLease(MasterLeaseInfo masterLease) {

        final RepNodeId rnId = masterLease.rn.getResourceId();
        LeaseTask leaseTask = leaseTasks.get(rnId);

        if (leaseTask != null) {
            final MasterLeaseInfo masterLeaseInfo =
                    (MasterLeaseInfo)leaseTask.getLease();
            if (!masterLeaseInfo.lesseeSN.equals(masterLease.lesseeSN)) {
                logger.info("Rejected master lease request: " + masterLease +
                            " Lease exists:" + leaseTask.getLease());
                return false;
            }

            /*
             * Establish a new lease for the same lessee after canceling the
             * existing lease.
             */
            logger.info("Extending master lease:" + masterLease);
            leaseTask.cancel();
            leaseTask = null;
        }

        leaseTask = new LeaseTask(masterLease);
        logger.info("Established master lease:" + masterLease);
        return true;
    }

    /**
     * Cancels the lease associated with the RN for a specific lessee. If the
     * lease is held by a different lessee SN the request is rejected and the
     * lease stays in effect.
     *
     * @param lesseeSN the SN holding the lease
     *
     * @param rn the RN associated with the lease
     *
     * @return true if the lease was cancelled.
     */
    synchronized boolean cancel(StorageNode lesseeSN,
                                RepNode rn) {

        final LeaseTask leaseTask = leaseTasks.get(rn.getResourceId());
        if ((leaseTask == null) ||
            !(((MasterLeaseInfo)leaseTask.getLease()).
              lesseeSN.equals(lesseeSN))) {
            return false;
        }

        /* SN owns the lease. */
        leaseTask.cancel();
        return true;
    }
}
