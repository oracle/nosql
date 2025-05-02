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
 * An object capable of running (run/pause/shutdown/etc) a daemon thread.
 * See DaemonThread for details.
 */
public interface DaemonRunner {
    void runOrPause(boolean run, FIOStatsCollector collector);
    void requestShutdown();
    void shutdown();
    int getNWakeupRequests();
}
