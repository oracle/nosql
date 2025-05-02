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

package oracle.kv.util.shell;

import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.CommandResult.CommandFails;
import oracle.kv.util.ErrorMessage;

public class ShellException extends Exception {

    private static final long serialVersionUID = 1L;

    /* TODO: should be null? */
    private static final CommandResult DEFAULT_CMDFAILS =
        new CommandFails("Shell command error", ErrorMessage.NOSQL_5100,
                         CommandResult.NO_CLEANUP_JOBS);

    private final CommandResult cmdResult;

    private ShellException(String msg, CommandResult cmdResult) {
        super(msg);
        this.cmdResult = cmdResult;
    }

    public ShellException() {
        this.cmdResult = DEFAULT_CMDFAILS;
    }

    public ShellException(String msg) {
        this(msg, new CommandFails(msg, ErrorMessage.NOSQL_5100,
                                   CommandResult.NO_CLEANUP_JOBS));
    }

    public ShellException(String msg, Throwable cause) {
        super(msg, cause);
        this.cmdResult = new CommandFails(msg, ErrorMessage.NOSQL_5100,
                                          CommandResult.NO_CLEANUP_JOBS);
    }

    public ShellException(String msg,
                          ErrorMessage errorMsg,
                          String[] cleanupJobs) {
        this(msg, new CommandFails(msg, errorMsg, cleanupJobs));
    }

    public ShellException(String msg,
                          Throwable cause,
                          ErrorMessage errorMsg,
                          String[] cleanupJobs) {
        super(msg, cause);
        this.cmdResult = new CommandFails(msg, errorMsg, cleanupJobs);
    }

    public CommandResult getCommandResult() {
        return cmdResult;
    }
}
