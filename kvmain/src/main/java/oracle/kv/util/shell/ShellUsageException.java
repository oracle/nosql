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

public class ShellUsageException extends ShellException {

    private static final long serialVersionUID = 1L;
    private final ShellCommand command;
    private final boolean requireArgument;

    public ShellUsageException(String msg, ShellCommand command) {
        this(msg, command, false);
    }

    public ShellUsageException(String msg,
                               ShellCommand command,
                               boolean requireArgument) {
        super(msg);
        this.command = command;
        this.requireArgument = requireArgument;
    }

    public String getHelpMessage() {
        return command.getBriefHelp();
    }

    public String getVerboseHelpMessage() {
        return command.getVerboseHelp();
    }

    public boolean requireArgument() {
        return requireArgument;
    }
}
