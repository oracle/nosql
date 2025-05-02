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

package oracle.kv.impl.diagnostic;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Encapsulates a definition and mechanism for running a step of diagnostic
 * command. Subclasses of DiagnosticTask will define the different types of
 * DiagnosticTask that can be carried out.
 */

public abstract class DiagnosticTask {
    private int totalSubTaskCount = 0;
    private int completedSubTaskCount = 0;

    /* A message queue to store the result messages */
    private Queue<String> messageQueue = new LinkedBlockingQueue<String>();

    public DiagnosticTask() {
        /* Default number of subtask of  DiagnosticTask is 1 */
        this(1);
    }

    public DiagnosticTask(int totalSubTaskCount) {
        this.totalSubTaskCount = totalSubTaskCount;
    }

    public int getTotalSubTaskCount() {
        return totalSubTaskCount;
    }

    public int getCompletedSubTaskCount() {
        return completedSubTaskCount;
    }

    /**
     * Notify to complete a sub task.
     *
     * @param message result message
     */
    public void notifyCompleteSubTask(String message) {
        /* Increment the count of completed sub tasks */
        completedSubTaskCount++;
        /* Add message into message queue */
        messageQueue.add(message);
    }

    public Queue<String> getMessageQueue() {
        return messageQueue;
    }

    protected void setTotalSubTaskCount(int totalSubTaskCount) {
        this.totalSubTaskCount = totalSubTaskCount;
    }

    /**
     * Execute the task.
     * @throws Exception
     */
    public final void execute() throws Exception {
        try {
            doWork();
        } finally {
            /*
             * In the end of execution, all sub-tasks complete, set completed
             * sub tasks as total sub tasks
             */
            completedSubTaskCount = totalSubTaskCount;
        }
    }

    /**
     * Do real work in this method.
     *
     * @throws Exception
     */
    public abstract void doWork() throws Exception;
}
