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

package oracle.kv.impl.async.perf;

import java.util.concurrent.atomic.AtomicLong;

import com.sleepycat.je.utilint.DoubleExpMovingAvg;

/**
 * A perf tracker for the resource manager.
 */
public class DialogResourceManagerPerfTracker {

    /** The sample rate for update the statistics. */
    private final static int UPDATE_SAMPLE_RATE = 1024;

    /**
     * Whether the associated resource manager is only for a client.
     */
    private final boolean forClientOnly;

    /**
     * The count of the operations, for use in computing the moving average.
     */
    private final AtomicLong opCount = new AtomicLong();

    /**
     * Tracks an exponential moving average over the last 1 million operations
     * of the number of available permits .
     */
    private final DoubleExpMovingAvg avgAvailablePermits =
        new DoubleExpMovingAvg("avgAvailablePermits", 1000000);

    /**
     * Tracks an exponential moving average over the last 1 million operations
     * of the available permit available percentage, i.e., availableNumPermits
     * / totalNumPermits.
     */
    private final DoubleExpMovingAvg avgAvailablePercentage =
        new DoubleExpMovingAvg("avgAvailablePercentage", 1000000);

    /**
     * Constructs the tracker.
     */
    public DialogResourceManagerPerfTracker(boolean forClientOnly)
    {
        this.forClientOnly = forClientOnly;
    }

    /**
     * Constructs the tracker for testing.
     */
    public DialogResourceManagerPerfTracker()
    {
        this(false);
    }

    /**
     * Updates the stats upon an operation.
     */
    public void update(int availableNumPermits, int totalNumPermits) {
        if (forClientOnly) {
            /*
             * Let's not update for the client.
             *
             * Currently, the dialog resource management is for throttling on
             * the server. In the initial design, one requirement is that a
             * responder endpoint can initiate dialogs to the creator endpoint
             * and therefore server side can initiate dialogs to the client as
             * well. This can be used to reduce a network hop for request
             * forwarding. Under this requirement, the dialog resource
             * management is installed on both client and server. However, we
             * have never utilized that capability and therefore, the dialog
             * resource management is not active on the client.
             *
             * Furthermore, because the DoubleExpMovingAvg#add synchronize on
             * the class, and the client can have a lot of number of threads
             * competing for the synchronization, this could create a
             * performance issue. [KVSTORE-2537]
             *
             * If in the future we need to activate the capability for the
             * client to receive dialogs initiated from the server, we will need
             * to find another way to solve this accounting issue. Perhaps still
             * with sampling but with a much larger sampling rate.
             */
            return;
        }
        final long count = opCount.incrementAndGet();
        if (count % UPDATE_SAMPLE_RATE == 0) {
            avgAvailablePermits.add(availableNumPermits, count);
            avgAvailablePercentage
                .add(((double) availableNumPermits) / totalNumPermits, count);
        }
    }

    /**
     * Returns the perf.
     */
    public DialogResourceManagerPerf obtain() {
        return new DialogResourceManagerPerf(
            avgAvailablePermits.get(), avgAvailablePercentage.get());
    }
}
