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

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * The subclass that introduces tasks specific to JE.
 */
public class JETaskCoordinator extends TaskCoordinator {

    /** The set of JE cooperating tasks. */
    private static final Set<Task> tasks = new HashSet<>();

    /* Cooperating JE tasks. */

    /** Task for the backup manager to copy a single log file. */
    public static final Task JE_BACKUP_MANAGER_COPY_LOG_FILE_TASK =
        addTask("JEBackupManagerCopyLogFile", 1);

    /** Task for the Checkpointer */
    public static final Task JE_CHECKPOINTER_TASK =
        addTask("JEINCheckpointer", 1);

    /** Task for the cleaner */
    public static final Task JE_CLEANER_TASK =
        addTask("JECleaner", 1);

    /** Task for data erasure */
    public static final Task JE_DATA_ERASURE_TASK =
        addTask("JEDataErasure", 1);

    /** 
     * Task for the cache evictor.  Multiple evictor threads run so configure
     * for multiple permits.
     */
    public static final Task JE_DATA_EVICTION_TASK =
        addTask("JEDataEviction", 10);

    /** Task for IN compression */
    public static final Task JE_INCOMPRESSOR_TASK =
        addTask("JEINCompressor", 1);

    /** Task for flushing the log asynchronously */
    public static final Task JE_LOG_FLUSHER_ASYNC_TASK =
        addTask("JELogFlusherASync", 1);

    /** Task for flushing the log synchronously */
    public static final Task JE_LOG_FLUSHER_SYNC_TASK =
        addTask("JELogFlusherSync", 1);

    /** Task for outputting stats. */
    public static final Task JE_OUTPUT_STATS_TASK =
        addTask("JEOutputStats", 1);

    /** Task for verifying the btree. */
    public static final Task JE_VERIFY_BTREE_TASK =
        addTask("JEVerifyBtree", 1);

    /** Task for verifying the log. */
    public static final Task JE_VERIFY_LOG_TASK =
        addTask("JEVerifyLog", 1);

    /**
     * The task coordinator that supplies JE tasks to the coordinator.
     *
     * @param logger the logger to be used
     *
     * @param envImpl the environment to use for logging or null
     *
     * @param tasks additional housekeeping tasks that will share permits with
     * JE tasks
     *
     * @param active determines whether the coordinator is active
     *
     * @see TaskCoordinator#TaskCoordinator(Logger, EnvironmentImpl, Set,
     * boolean)
     */
    public JETaskCoordinator(final Logger logger,
                             final EnvironmentImpl envImpl,
                             final Set<Task> tasks,
                             final boolean active) {
        super(logger, envImpl, combineTasks(tasks), active);
    }

    private static Task addTask(final String name, final int permits) {
        final Task task = new Task(name, permits);
        if (!tasks.add(task)) {
            throw new IllegalStateException("Duplicate task: " + task);
        }
        return task;
    }

    private static Set<Task> combineTasks(final Set<Task> callerTasks) {
        final Set<Task> allTasks = new HashSet<>(callerTasks);
        allTasks.addAll(tasks);
        return allTasks;
    }
}
