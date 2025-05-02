/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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
package com.sleepycat.je.rep.stream;

import static com.sleepycat.je.rep.stream.ArbiterFeederStatDefinition.QUEUE_FULL;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Implementation of a master node acting as a FeederSource for an Arbiter.
 */
public class ArbiterFeederSource implements FeederSource {


    private final BlockingQueue<LogItem> queue;
    private final EnvironmentImpl envImpl;
    private final StatGroup stats;
    private final LongStat nQueueFull;

    public ArbiterFeederSource(EnvironmentImpl envImpl)
        throws DatabaseException {

        int queueSize =
            envImpl.getConfigManager().getInt
            (RepParams.ARBITER_OUTPUT_QUEUE_SIZE);
        queue = new ArrayBlockingQueue<>(queueSize);
        this.envImpl = envImpl;
        stats =
            new StatGroup(ArbiterFeederStatDefinition.GROUP_NAME,
                          ArbiterFeederStatDefinition.GROUP_DESC);
        nQueueFull = new LongStat(stats, QUEUE_FULL);
    }

    /**
     * Add commit item to queue if possible.
     *
     * <p>The lifecycle of a commit is different then other log entries due
     * to arbitration. Replicated items have buffers taken from a pool and
     * they must be returned to the pool when no longer in use. Normally the
     * use count is incremented when a feeder gets it from the tip cache. But
     * the commit entry is accessed by the arbiter feeder via its separate
     * queue (the tip cache is not used). The critical thing is that the use
     * count must be incremented only iff added to this queue, since it will
     * be decremented by the feeder only after sending it to the arbiter.</p>
     *
     * <p>However, since arbitration occurs independently from tip cache
     * management, it may not be possible to increment the use count here when
     * the commit it queued, since the item may have already been deallocated.
     * Therefore, the use count is incremented when the commit is logged, even
     * before it is added to the tip cache, and decremented by the Txn commit
     * only if this method returns false. See use of LogParams.incLogItemUsage
     * in Txn and LogManager.</p>
     *
     * <p>Note that currently, a cached buffer is not used for Commit log
     * entries, because the entry itself is cached (via LogItem.cachedEntry
     * and LogParams.immutableLogEntry). Therefore, the resource management
     * here is not strictly needed at this time. But it may be needed in the
     * future if the tip cache policy changes or other log entry types are
     * sent to the arbiter.</p>
     */
    public boolean addCommit(final LogItem commitItem) {

        if (queue.offer(commitItem)) {
            return true;
        }

        /*
         * If the commit could not be added to the queue because
         * the queue is filled. Try to remove an item
         * and replace with the item with the higher VLSN.
         * The Arbiter ack for the higher VLSN is sufficient
         * for transactions with a lower commit VLSN.
         */
        nQueueFull.increment();
        try {
            final LogItem queuedItem = queue.remove();
            final long vlsn = commitItem.header.getVLSN();
            if (queuedItem.header.getVLSN() > vlsn) {

                /*
                 * The removed item has higher vlsn so use that one.
                 */
                if (queue.offer(queuedItem)) {
                    return false;
                }

                /* Release the removed item since we did not used it. */
                queuedItem.decrementUse();
            }
        } catch (NoSuchElementException noe) {
            /* Queue was empty so try to insert one last time. */
        }

        /*
         * Attempt to put the item on the queue. If another
         * thread has inserted and the queue is full, we will
         * skip this transaction for an Arbiter ack attempt. The
         * transaction may still succeed in this case due to acks from
         * Replicas or other Arbiter acked transactions with a higher
         * VLSN.
         */
        return queue.offer(commitItem);
    }

    @Override
    public void shutdown(EnvironmentImpl envImpl) {
    }

    /*
     * @see com.sleepycat.je.rep.stream.FeederSource#getLogRecord
     * (com.sleepycat.je.utilint.VLSN, int)
     */
    @Override
    public OutputWireRecord getWireRecord(
        long vlsn, long waitNs, boolean includeBeforeImage)
        throws DatabaseException, InterruptedException, IOException {

        LogItem commitItem = queue.poll(waitNs, TimeUnit.NANOSECONDS);
        if (commitItem != null) {
            return new OutputWireRecord(envImpl, commitItem) ;
        }
        return null;
    }

    public StatGroup loadStats(StatsConfig config)
        throws DatabaseException {
        StatGroup copyStats = stats.cloneGroup(config.getClear());
        return copyStats;
    }

    @Override
    public String dumpState() {
        return null;
    }
}
