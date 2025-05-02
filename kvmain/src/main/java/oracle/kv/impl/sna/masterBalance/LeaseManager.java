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

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.topo.RepNodeId;


/**
 * A generic lease manager suitable for Master and Replica lease management
 * implemented by its subclasses.
 */
abstract class LeaseManager {

    private static final AtomicInteger nextTimerId = new AtomicInteger();

    /**
     * The lease duration tasks that are scheduled.
     */
    protected final Map<RepNodeId, LeaseTask> leaseTasks;

    /**
     *  Implements the timer used to maintain the leases.
     */
    private final Timer leaseTimer = new Timer(
        getClass().getSimpleName() + "-Timer-" + nextTimerId.getAndIncrement(),
        true);

    /**
     * The shutdown flag that is owned by MasterBalanceManger. It's true
     * when a shutdown request is outstanding.
     */
    private final AtomicBoolean shutdown;

    /**
     * The logger that is shared with the MasterBalanceManager.
     */
    protected final Logger logger;

    LeaseManager(Logger logger) {
        this.logger = logger;
        this.shutdown = new AtomicBoolean(false);
        leaseTasks = new ConcurrentHashMap<RepNodeId, LeaseTask>();
    }

    synchronized int leaseCount() {
        return leaseTasks.size();
    }

    /**
     * Returns true if there is a lease associated with the RN
     */
    synchronized boolean hasLease(RepNodeId rnId) {
        return leaseTasks.get(rnId) != null;
    }

    /**
     * Cancels the lease associated with this RN. The lease is cancelled
     * regardless of the current lessee.
     *
     * @param rnId The RN associated with the lease
     *
     * @return true if the lease existed and was cancelled
     */
    synchronized boolean cancel(RepNodeId rnId) {
        assert Thread.holdsLock(this);

        final LeaseTask leaseTask = leaseTasks.get(rnId);
        if (leaseTask == null) {
            return false;
        }
        leaseTask.cancel();
        return true;
    }

    /**
     * Cleanup
     */
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            leaseTimer.cancel();
        }
    }

    public interface Lease {
        public RepNodeId getRepNodeId();
        public int getLeaseDuration();
    }

    /**
     * LeaseTask the timer task that handles lease expirations.
     */
    class LeaseTask extends TimerTask {

        /* The lease associated with this lease task. */
        private final Lease lease;

        LeaseTask(Lease lease) {
            super();
            assert Thread.holdsLock(LeaseManager.this);

            this.lease = lease;
            leaseTasks.put(lease.getRepNodeId(), this);
            leaseTimer.schedule(this, lease.getLeaseDuration());
        }

        public Lease getLease() {
            return lease;
        }

        @Override
        public boolean cancel() {
            synchronized (LeaseManager.this) {
                LeaseTask leaseTask =
                        leaseTasks.remove(lease.getRepNodeId());
                assert leaseTask == this;
                logger.info("Cancelled lease:" + lease +
                            " lease count:" + leaseTasks.size());
                return super.cancel();
            }
        }

        @Override
        public void run() {

            if (shutdown.get()) {
                return;
            }

            try {
                synchronized (LeaseManager.this) {
                    if (leaseTasks.containsKey(lease.getRepNodeId())) {
                        /* Lease duration has expired, cancel the lease. */
                        cancel();
                    }
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE,
                           "Lease expiration task exiting due to exception.",
                           e);
            }
        }
    }
}
