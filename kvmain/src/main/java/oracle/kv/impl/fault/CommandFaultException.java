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

package oracle.kv.impl.fault;

import oracle.kv.util.ErrorMessage;

/**
 * This class provides a way to specify necessary error information to
 * construct a CommandResult, which will be used to generate a standard NoSQL
 * error report to users. It can be directly used as an exception with the error
 * information, or used to wrap another exception with specified error
 * information. Mainly used on server side.
 * <p>
 * For example, to attach error information to an IllegalStateException, the
 * below code can be followed:
 * ...
 *  } catch (IllegalStateException ise) {
 *      throw new CommandFaultException("Encountered IllegalStateException: " +
 *                                      ise.getMessage(),
 *                                      ise,
 *                                      ErrorMessage.NOSQL_5500,
 *                                      null );
 * }
 */
public class CommandFaultException extends OperationFaultException {

    private static final long serialVersionUID = 1L;

    private final ErrorMessage errorMsg;
    private final String[] cleanupJobs;

    /**
     * Ctor. Message of this exception will be used as the description in a
     * CommandResult.
     */
    public CommandFaultException(String msg,
                                 Throwable fault,
                                 ErrorMessage errorMsg,
                                 String[] cleanupJobs) {
        this(msg, errorMsg, cleanupJobs);
        initCause(fault);
    }

    public CommandFaultException(String msg,
                                 ErrorMessage errorMsg,
                                 String[] cleanupJobs) {
        super(msg);
        this.errorMsg = errorMsg;
        this.cleanupJobs = cleanupJobs;
    }

    public String[] getCleanupJobs() {
        return cleanupJobs;
    }

    public ErrorMessage getErrorMessage() {
        return errorMsg;
    }

    public String getDescription() {
        return getMessage();
    }
}
