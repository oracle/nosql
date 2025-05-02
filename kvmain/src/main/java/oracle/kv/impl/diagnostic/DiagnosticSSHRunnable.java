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

import oracle.kv.impl.diagnostic.ssh.SSHClient;

/**
 * Encapsulates a definition and mechanism for communicating with SSH sever
 * to get info and files. Subclasses of DiagnosticSSHRunnable will define the
 * different types of DiagnosticSSHRunnable that can be carried out.
 */

public abstract class DiagnosticSSHRunnable implements Runnable {

    protected SNAInfo snaInfo;
    protected SSHClient client;

    /* The owner of the thread, the owner starts the thread */
    private final DiagnosticTask owner;

    public DiagnosticSSHRunnable(SNAInfo snaInfo, DiagnosticTask owner,
                                 SSHClient client) {
        this.snaInfo = snaInfo;
        this.owner = owner;
        this.client = client;
    }

    @Override
    public void run() {

        String message = snaInfo.getSNAInfo().trim();
        try {
            /* Call doWork to execute the code in derived classes */
            if (!client.isOpen()) {
                message += DiagnosticConstants.NEW_LINE_TAB +
                        client.getErrorMessage();
            } else {
                message += DiagnosticConstants.NEW_LINE_TAB + doWork();
            }
        } catch (Exception e) {
            /* Convert the exception to a message */
            message = snaInfo.getSNAInfo() +
                    DiagnosticConstants.NEW_LINE_TAB + e;
        }

        /* Notify the sub task is completed */
        owner.notifyCompleteSubTask(message.trim());
    }

    /**
     * The real work to be done is contained in the implementations of this
     * method.
     *
     * @return result message of execution of the thread
     * @throws Exception
     */
    public abstract String doWork() throws Exception;
}
