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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.fault.CommandFaultException;

/*
 * A class that implements boilerplate for command classes that have
 * sub-commands, such as "show" and "plan."
 *
 * Such classes extend this and implement their own sub-commands as instances
 * of SubCommand.
 */

public abstract class CommandWithSubs extends ShellCommand {
    private final List<? extends SubCommand> subCommands;
    private final int minArgCount;

    protected CommandWithSubs(List<? extends SubCommand> subCommands,
                              String name,
                              int prefixLength,
                              int minArgCount) {
        super(name, prefixLength);
        this.subCommands = subCommands;
        this.minArgCount = minArgCount;
        Collections.sort(this.subCommands,
                         new Shell.CommandComparator());
    }

    /**
     * Gets the command overview. Subclasses provide a string which is
     * prefixed to the help string for the top level command.
     *
     * @return the command overview
     */
    protected abstract String getCommandOverview();

    public List<? extends SubCommand> getSubCommands() {
        return subCommands;
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        return new CommandWithSubExecutor<String>() {
            @Override
            public String subCommandResult(SubCommand command)
                throws ShellException{
                final String output =
                    command.execute(
                        Arrays.copyOfRange(args, 1, args.length), shell,
                        getCommandLine());
                exitCode = command.exitCode;
                return output;
            }
        }.commonExecute(args, shell);
    }

    private abstract class CommandWithSubExecutor<T> implements Executor<T> {
        @Override
        public T commonExecute(String[] args, Shell shell)
            throws ShellException {
            if ((minArgCount > 0 && args.length < minArgCount) ||
                args.length == 1) {
                shell.badArgCount(CommandWithSubs.this);
            }
            String commandName = args[1];
            SubCommand command = findCommand(commandName);

            if ((command == null) ||
                (command.isHidden() && !shell.getHidden())) {
                Shell.checkHelp(args, CommandWithSubs.this);
                throw new CommandNotFoundException(
                    "Could not find " + name + " subcommand: " + commandName +
                    eol + getVerboseHelp());
            }
            return subCommandResult(command);
        }
        public abstract T subCommandResult(SubCommand command)
            throws ShellException;
    }

    @Override
    public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
        throws ShellException {
        return new CommandWithSubExecutor<ShellCommandResult>() {
            @Override
            public ShellCommandResult subCommandResult(SubCommand command)
                throws ShellException{
                final ShellCommandResult output =
                    command.executeJsonOutput(
                        Arrays.copyOfRange(args, 1, args.length), shell);
                exitCode = command.exitCode;
                return output;
            }
        }.commonExecute(args, shell);
    }

    /**
     * Returns general help string. If called without a sub-command, a
     * multi-line syntax string with appropriate spacing is returned with each
     * sub-command appearing on a separate line. Hidden sub-commands will be
     * included in the list only if the hidden mode is set. If a sub-command is
     * specified the verbose help for that sub-command is returned.
     *
     * The top level command should not need to override this method.
     *
     * @return a help string
     */
    @Override
    protected final String getHelp(String[] args, Shell shell) {
        if (args.length <= 1) {
            String msg = getCommandOverview();
            msg += eol + getBriefHelp(shell.getHidden(),
                                      shell.showDeprecated());
            return msg;
        }
        String commandName = args[1];
        SubCommand command = findCommand(commandName);
        if ((command != null) && (!command.isHidden() || shell.getHidden())) {
            return command.getVerboseHelp();
        }
        return("Could not find " + name + " subcommand: " + commandName +
               eol + getVerboseHelp());
    }

    /**
     * Returns a multi-line syntax string with appropriate spacing is returned
     * with each sub-command appearing on a separate line. Hidden sub-commands
     * will not be included in the list.
     *
     * The top level command should not need to override this method.
     *
     * @return a help string
     */
    @Override
    protected final String getBriefHelp() {
        return getBriefHelp(false, false);
    }

    private String getBriefHelp(boolean showHidden, boolean showDeprecated) {

        final StringBuilder sb = new StringBuilder();
        sb.append("Usage: ").append(name).append(" ");
        final String ws = Shell.makeWhiteSpace(sb.length());
        boolean first = true;
        for (SubCommand command : subCommands) {

            if (!showHidden && command.isHidden()) {
                continue;
            }
            if (!showDeprecated && command.isDeprecated()) {
                continue;
            }
            if (first) {
                sb.append(command.getCommandName());
                first = false;
            } else {
                sb.append(" |").append(eol);
                sb.append(ws).append(command.getCommandName());
            }
        }
        return sb.toString();
    }

    /*
     * The top level command should not need to override this method. Also,
     * this method is only called from super.getBriefHelp() which has been
     * overridden and so should never be invoked.
     */
    @Override
    protected final String getCommandSyntax() {
        throw new AssertionError();
    }

    /* The top level command should not need to override this method */
    @Override
    protected final String getCommandDescription() {
        return "";
    }

    /* Public for use in testing */
    public SubCommand findCommand(String commandName) {
        for (SubCommand command : subCommands) {
            if (command.matches(commandName)) {
                return command;
            }
        }
        return null;
    }

    /*
     * Base abstract class for subcommands
     */
    public static abstract class SubCommand extends ShellCommand {
        protected final static String cantGetHere = "Cannot get here";

        protected SubCommand(String name, int prefixLength) {
            super(name, prefixLength);
        }

        /**
         * Check if the AdminFaultException was caused by an
         * IllegalCommandException and, if so, throw a ShellUsageException to
         * convert the server side command failure into a user visible one.
         */
        protected void convertIllegalCommandException(AdminFaultException afe)
            throws ShellUsageException
        {
            if (IllegalCommandException.class.getName().equals(
                    afe.getFaultClassName())) {
                final CommandResult result = afe.getCommandResult();
                final String msg = (result != null) ?
                    result.getDescription() :
                    afe.getMessage();
                throw new ShellUsageException(msg, this);
            }
        }

        /**
         * Check if the AdminFaultException was caused by an
         * CommandFaultException and, if so, throw a ShellException to convert
         * the server side command failure into a user visible one.
         */
        protected void convertCommandFaultException(AdminFaultException afe)
            throws ShellException
        {
            if (CommandFaultException.class.getName().equals(
                    afe.getFaultClassName())) {
                final CommandResult result = afe.getCommandResult();
                final String msg = (result != null) ?
                    result.getDescription() :
                    afe.getMessage();
                throw new ShellException(msg);
            }
        }
    }

    /**
     * This sub command implements the handling logic for converting
     * previous V1 JSON result from execute() method to the new JSON format
     * fits ShellCommandResult.
     */
    protected static abstract class SubCommandJsonConvert extends SubCommand {

        protected SubCommandJsonConvert(String name, int prefixLength) {
            super(name, prefixLength);
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            return filterJsonResult(execute(args, shell));
        }
    }
}
