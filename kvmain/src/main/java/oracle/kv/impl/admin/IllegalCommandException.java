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

package oracle.kv.impl.admin;

import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.util.ErrorMessage;

/**
 * Exception to indicate that an administrative command issued by the user has
 * an illegal argument, or is not permissible at the current time or in the
 * current state of the store.
 *
 * The problem should be reported back to the user, and the user should reissue
 * the command.
 */
public class IllegalCommandException extends CommandFaultException {

    private static final long serialVersionUID = 1L;

    private static final ErrorMessage DEFAULT_ERR_MSG =
        ErrorMessage.NOSQL_5100;
    private static final String[] EMPTY_CLEANUP_JOBS = new String[] {};

    public IllegalCommandException(String message) {
        this(message, DEFAULT_ERR_MSG, EMPTY_CLEANUP_JOBS);
    }

    public IllegalCommandException(String message, Throwable t) {
        this(message, t, DEFAULT_ERR_MSG, EMPTY_CLEANUP_JOBS);
    }

    public IllegalCommandException(String message,
                                   ErrorMessage errorMsg,
                                   String[] cleanupJobs) {
        super(message, errorMsg, cleanupJobs);
    }

    public IllegalCommandException(String message,
                                   Throwable t,
                                   ErrorMessage errorMsg,
                                   String[] cleanupJobs) {
        super(message, t, errorMsg, cleanupJobs);
    }
}
