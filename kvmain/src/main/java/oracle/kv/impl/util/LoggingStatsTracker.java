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

import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.measurement.LoggingStats;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.util.server.LoggerUtils.LoggingCounts;

/**
 * Obtains service logging entries at levels SEVERE and WARNING (or
 * SEC_WARNING).
 */
public class LoggingStatsTracker {

    /**
     * Returns stats about any pending non-zero SEVERE, WARNING (or
     * SEC_WARNING), or INFO (or SEC_INFO) log counts for the service with the
     * specified ID in the store with the specified name and clears the counts.
     * Returns null if the counts are zero.
     */
    public static LoggingStats getStats(ResourceId serviceId,
                                        String storeName,
                                        long startTimeMillis,
                                        long endTimeMillis) {
        final LoggingCounts counts =
            LoggerUtils.getLoggingStatsCounts(serviceId, storeName);
        final long severe = counts.getSevere().getAndSet(0);
        final long warning = counts.getWarning().getAndSet(0);
        final long info = counts.getInfo().getAndSet(0);
        if ((severe == 0) && (warning == 0) && (info == 0)) {
            return null;
        }
        return new LoggingStats(serviceId, severe, warning, info,
                                startTimeMillis, endTimeMillis);
    }
}
