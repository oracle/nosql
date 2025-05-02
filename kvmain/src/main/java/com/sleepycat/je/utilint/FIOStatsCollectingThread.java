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

package com.sleepycat.je.utilint;

import com.sleepycat.je.log.FileManager.FIOStatsCollector;

/**
 * Marker interface identifying a Thread as being capable of collecting
 * File JE I/O statistics for I/O done by the thread.
 */
public interface FIOStatsCollectingThread {

    /**
     * Collect file i/o stats. If the thread is marked as an
     * FIOStatsCollectingThread, use the collector associated with it.
     * Otherwise associate the stats with the "misc" bucket.
     */
    static void collectIf(boolean read, long bytes,
                          FIOStatsCollector miscCollector) {
        final Thread daemonThread = Thread.currentThread();
        final FIOStatsCollectingThread fscThread =
            (daemonThread instanceof FIOStatsCollectingThread) ?
                ((FIOStatsCollectingThread)daemonThread) : null;

        if (fscThread != null) {
            fscThread.collect(read, bytes);
        } else {
            miscCollector.collect(read, bytes);
        }
    }

    /* The default method when one is not provided by the thread: It discards
     * the statistics.
     */
    default void collect(@SuppressWarnings("unused") boolean read,
                         @SuppressWarnings("unused") long bytes) {}
}
