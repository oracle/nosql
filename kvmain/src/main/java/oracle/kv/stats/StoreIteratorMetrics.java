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

package oracle.kv.stats;

/**
 * @deprecated since 3.4, no longer supported.
 */
@Deprecated
public interface StoreIteratorMetrics {

    public long getBlockedResultsQueuePuts();

    public long getAverageBlockedResultsQueuePutTime();

    public long getMinBlockedResultsQueuePutTime();

    public long getMaxBlockedResultsQueuePutTime();

    public long getBlockedResultsQueueGets();

    public long getAverageBlockedResultsQueueGetTime();

    public long getMinBlockedResultsQueueGetTime();

    public long getMaxBlockedResultsQueueGetTime();
}
