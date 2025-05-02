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

package oracle.kv.impl.monitor.views;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import oracle.kv.impl.monitor.Tracker;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.server.LoggerUtils.SecurityLevel;

/**
 * Tracks logging events as java.util.LogRecords.
 */

public class LogTracker extends Tracker<LogRecord>
    implements ViewListener<LogRecord> {

    /* Run the pruner modulo this. */
    public static final int PRUNE_FREQUENCY = 40;

    private int newInfoCounter = 0;
    private final Logger logger;

    public LogTracker(Logger logger) {
        super();
        this.logger = logger;
    }

    @Override
    protected void prune() {
        long interesting = getEarliestInterestingTimeStamp();

        while (!queue.isEmpty()) {
            if (queue.size() < 40) {

                /*
                 * Leave a few log records in the queue, so that a new client
                 * can display some recent history.
                 */
                break;
            }

            EventHolder<LogRecord> lr = queue.get(0);
            if (lr.getSyntheticTimestamp() > interesting) {
                /* Stop if we've reached the earliest interesting timestamp. */
                break;
            }
            queue.remove(0);
        }

        /*
         * Guarantee that the queue is never larger than a certain max number
         * of entries. If that number is exceeded, then we discard guarantees
         * about keeping messages until all listeners have indicated that they
         * are no longer interesting.
         */
        int currentSize = queue.size();
        if (currentSize > QUEUE_MAX) {
            for (int i = 0; i < (currentSize - QUEUE_MAX); i++) {
                queue.remove(0);
            }
            logger.info("Log queue size=" + currentSize +
                        " exceeds maximum of " + QUEUE_MAX +
                        ", was pruned;" +
                        " some messages prior to this one were lost.");
        }
    }

    @Override
    public void newInfo(ResourceId rId, LogRecord lr) {
        synchronized (this) {
            if (newInfoCounter++ % PRUNE_FREQUENCY == 0) {
                prune();
            }

            long syntheticTimestamp = getSyntheticTimestamp(lr.getMillis());

            queue.add(new EventHolder<LogRecord>
                      (syntheticTimestamp, lr,
                       (lr.getLevel() == Level.SEVERE ||
                        lr.getLevel() == SecurityLevel.SEC_WARNING
                        /* Such are recordable*/)));
        }
        /*
         * If we are still holding the monitor after exiting the synchronized
         * block, it means that newInfo has been called recursively via prune's
         * call to logger.info, above.  If that is the case, then we'll
         * decline to call notifyListeners, because a listener receiving a
         * notification may cause a deadlock.  In any case, notifyListeners
         * will be called when the first newInfo frame regains control.
         */
        if (! Thread.holdsLock(this)) {
            notifyListeners();
        }
    }

    /**
     * Get a list of events that have occurred since the given time.
     */
    @Override
    protected synchronized
        RetrievedEvents<LogRecord> doRetrieveNewEvents(long since) {

        List<EventHolder<LogRecord>> values =
            new ArrayList<EventHolder<LogRecord>>();

        long syntheticStampOfLastRecord = since;
        for (EventHolder<LogRecord> lr : queue) {
            if (lr.getSyntheticTimestamp() > since) {
                values.add(lr);
                syntheticStampOfLastRecord = lr.getSyntheticTimestamp();
            }
        }

        return
            new RetrievedEvents<LogRecord>(syntheticStampOfLastRecord, values);
    }
}
