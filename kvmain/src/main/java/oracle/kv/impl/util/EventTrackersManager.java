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

import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

import oracle.kv.impl.async.IOBufPoolTrackers;

/**
 * A collection of event trackers that share the same environment, e.g., logger
 * and executor, etc.
 *
 * There are desires to track events for correctness. For example, counters
 * incremented for outstanding requests are always decremented when the request
 * is done; buffers managed with reference counting are released correctly. This
 * class provides an entry point to access the trackers.
 */
public class EventTrackersManager {

    /** For testing. */
    public static volatile boolean disableIOBufPoolTrackerTasks = false;

    private final IOBufPoolTrackers ioBufPoolTrackers;

    public EventTrackersManager(Logger logger,
                                ScheduledExecutorService executor) {
        this.ioBufPoolTrackers = new IOBufPoolTrackers(logger, executor);
        if (!disableIOBufPoolTrackerTasks) {
            this.ioBufPoolTrackers.scheduleLoggingTask();
        }
    }

    public IOBufPoolTrackers getIOBufPoolTrackers() {
        return ioBufPoolTrackers;
    }
}
