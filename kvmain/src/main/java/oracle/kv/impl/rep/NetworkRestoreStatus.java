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

package oracle.kv.impl.rep;

import java.io.Serializable;

import oracle.kv.impl.rep.RepEnvHandleManager.NetworkRestoreException;

import com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStats;

/**
 * Status of task that performs a network restore from specified source node
 * asynchronously on RepNode. This status contains the errors during network
 * restore, number of retries and also the statistics of JE network backup.
 */
public class NetworkRestoreStatus implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean completed;
    private final int retryNum;
    private final NetworkRestoreException exception;
    private final NetworkBackupStats networkBackStats;

    public NetworkRestoreStatus(boolean completed,
                                int retryNum,
                                NetworkRestoreException exception,
                                NetworkBackupStats networkBackStats) {
        this.retryNum = retryNum;
        this.completed = completed;
        this.exception = exception;
        this.networkBackStats = networkBackStats;
    }

    /**
     * Return if the network restore task is completed.
     */
    public boolean isCompleted() {
        return completed;
    }

    /**
     * Return number of retries while performing JE network backup execution.
     */
    public int getRetryNum() {
        return retryNum;
    }

    /**
     * Return errors occurred during the network restore task.
     */
    public Exception getException() {
        return exception;
    }

    /**
     * Return the JE network backup statistics data of this task, or null if no
     * statistics are available.
     */
    public NetworkBackupStats getNetworkBackStats() {
        return networkBackStats;
    }
}
