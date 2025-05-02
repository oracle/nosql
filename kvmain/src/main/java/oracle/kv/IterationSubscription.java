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

package oracle.kv;

import java.util.List;

import oracle.kv.stats.DetailedMetrics;
import oracle.kv.table.TableAPI;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A subinterface of {@link Subscription} implemented by subscriptions supplied
 * when a {@link Subscriber} subscribes to a {@link Publisher} associated with
 * an asynchronous table iteration.
 *
 * @see TableAPI#tableIteratorAsync
 * @since 19.5
 */
public interface IterationSubscription extends Subscription {

    /**
     * Returns per-partition metrics for the iteration. This method may be
     * called at any time during an iteration in order to obtain metrics to
     * that point or it may be called at the end to obtain metrics for the
     * entire scan. If there are no metrics available yet for a particular
     * partition, then that partition will not have an entry in the list.
     *
     * @return the per-partition metrics for iteration
     */
    List<DetailedMetrics> getPartitionMetrics();

    /**
     * Returns per-shard metrics for the iteration. This method may be called
     * at any time during an iteration in order to obtain metrics to that point
     * or it may be called at the end to obtain metrics for the entire scan.
     * If there are no metrics available yet for a particular shard, then that
     * shard will not have an entry in the list.
     *
     * @return the per-shard metrics for the iteration
     */
    List<DetailedMetrics> getShardMetrics();
}
