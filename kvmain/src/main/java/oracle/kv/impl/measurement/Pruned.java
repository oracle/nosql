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

import oracle.kv.impl.monitor.Metrics;
import oracle.kv.impl.util.FormatUtils;

/**
 * Tracks measurements which have been pruned from the service side repository
 * buffer.
 */
public class Pruned implements Measurement, Serializable {

    private static final long serialVersionUID = 1L;
    private int numRemoved;

    /* Keeps the time span of measurements that have been deleted.*/
    private long start;
    private long end;

    /**
     * Keep track of an item that has been pruned from the buffer.
     *
     * TODO: Ideally, the bounding of the buffer should be more sophisticated.
     * Currently, pruned items are simply removed and counted. In the future,
     * latency stats should be rolled up and aggregated, and pruning can be done
     * with some measure of the priority of the measurement.
     */
    public void record(Measurement target) {

        numRemoved++;

        /*
         * Try to get a sense of what time period is spanned by the pruned
         * items.
         */
        long mStart = target.getStart();
        long mEnd = target.getEnd();

        if (end < mEnd) {
            end = mEnd;
        }

        if (mEnd != 0) {
            if (start > mEnd) {
                start = mEnd;
            }
        }

        if (mStart != 0) {
            if (start == 0) {
                start = mStart;
            } else if (start > mStart) {
                start = mStart;
            }
        }

        // TODO: rollup the latency stats instead of just counting them.
    }

    @Override
    public int getId() {
        return Metrics.PRUNED.getId();
    }

    @Override
    public String toString() {

        if (start == 0) {
            return  numRemoved + " measurements dropped.";
        }

        return numRemoved + " measurements from " +
            FormatUtils.formatDateTime(start) +
            FormatUtils.formatDateTime(end) + " dropped.";
    }

    @Override
        public long getStart() {
        return start;
    }

    @Override
        public long getEnd() {
        return end;
    }

    public boolean exists() {
        return numRemoved > 0;
    }

    public int getNumRemoved() {
        return numRemoved;
    }
}
