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

import java.io.Serializable;

/**
 * The resource manager metrics.
 *
 * This class is deprected and being replaced by {@link
 * DialogResourceManagerPerf}.
 *
 * @deprecated since 22.3
 */
@Deprecated
public class DialogResourceManagerMetrics implements Serializable {

    private static final long serialVersionUID = 1L;

    private final double avgAvailablePermits;
    private final double avgAvailablePercentage;

    public DialogResourceManagerMetrics(double avgAvailablePermits,
                                        double avgAvailablePercentage) {
        this.avgAvailablePermits = avgAvailablePermits;
        this.avgAvailablePercentage = avgAvailablePercentage;
    }

    /**
     * Gets the average number of availabe permits.
     */
    public double getAvgAvailablePermits() {
        return avgAvailablePermits;
    }

    /**
     * Gets the average capacity of availabe permits.
     */
    public double getAvgAvailableCapacity() {
        return avgAvailablePercentage;
    }

    @Override
    public String toString() {
        return String.format(
            "avgAvailablePermits=%s, avgAvailablePercentage=%s",
            avgAvailablePermits, avgAvailablePercentage);
    }
}
