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

import static oracle.kv.impl.util.CommandParser.COMMAND_FLAG;
import static oracle.kv.impl.util.CommandParser.LAST_FLAG;

import java.util.List;

import oracle.kv.impl.admin.client.POST;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.util.shell.Shell.CommandHistory;

import oracle.nosql.common.json.ObjectNode;

/*
 * A base class for show commands and the sub commands.
 */
public class ShowCommandBase extends CommandWithSubs {

    public final static String COMMAND = "show";

    private final static String OVERVIEW =
        "Encapsulates commands that display the " +
        "state of the store and its components.";

    public ShowCommandBase(List<? extends SubCommand> subs) {
        super(subs, COMMAND, 2, 2);
    }

    @Override
    protected String getCommandOverview() {
        return OVERVIEW;
    }

    @POST
    public static final class ShowFaults extends SubCommand {

        final static String NAME = "faults";
        private final static String COMMAND_DESC = COMMAND_FLAG + " <index>";

        final static String SYNTAX = COMMAND + " " + NAME + " " +
            CommandParser.optional(LAST_FLAG) + " " +
            CommandParser.optional(COMMAND_DESC) + " " +
            CommandParser.getJsonUsage();

        final static String DESCRIPTION =
            "Displays faulting commands.  By default all available " +
            "faulting commands" + eolt + "are displayed.  Individual " +
            "fault details can be displayed using the" + eolt +
            LAST_FLAG + " and " + COMMAND_FLAG + " flags.";

        public ShowFaults() {
            super(NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            return new ShowFaultExecutor<String>() {

                @Override
                public String
                    indexFaultResult(Shell.CommandHistory history, int index)
                    throws ShellException {
                    return history.dumpCommand(index, true);
                }

                @Override
                public String
                    lastFaultResult(Shell.CommandHistory history)
                    throws ShellException {
                    return history.dumpLastFault();
                }

                @Override
                public String multiFaultResult(CommandHistory history,
                                               int from,
                                               int to)
                    throws ShellException {
                    return history.dumpFaultingCommands(from, to);
                }

            }.commonExecute(args, shell);
        }

        private abstract class ShowFaultExecutor<T>
            implements Executor<T> {

            @Override
            public T commonExecute(String[] args, Shell shell)
                throws ShellException {
                final Shell.CommandHistory history = shell.getHistory();
                final int from = 0;
                final int to = history.getSize();

                if (args.length > 1) {
                    final String arg = args[1];
                    if (LAST_FLAG.equals(arg)) {
                        return lastFaultResult(history);
                    } else if (COMMAND_FLAG.equals(arg)) {
                        final String faultString =
                            Shell.nextArg(args, 1, ShowFaults.this);
                        int fault;
                        try {
                            fault = Integer.parseInt(faultString);
                            /*
                             * The index of command are shown as 1-based index
                             * in output, so covert it to 0-based index when
                             * locating it in CommandHistory list.
                             */
                            int idxFault = toZeroBasedIndex(fault);
                            if (idxFault < 0 ||
                                idxFault >= history.getSize()) {
                                throw new ShellArgumentException(
                                    "Index out of range: " + fault +
                                    "" + eolt + getBriefHelp());
                            }
                            if (history.commandFaulted(idxFault)) {
                                return indexFaultResult(history, idxFault);
                            }
                            throw new ShellArgumentException(
                                "Command " + fault + " did not fault");
                        } catch (IllegalArgumentException e) {
                            invalidArgument(faultString);
                        }
                    } else {
                        shell.unknownArgument(arg, ShowFaults.this);
                    }
                }
                return multiFaultResult(history, from, to);
            }

            public abstract T
                indexFaultResult(Shell.CommandHistory history, int index)
                throws ShellException;

            public abstract T
                lastFaultResult(Shell.CommandHistory history)
                throws ShellException;

            public abstract T
                multiFaultResult(Shell.CommandHistory history,
                                 int from,
                                 int to)
                throws ShellException;
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {

            final ShellCommandResult scr =
                ShellCommandResult.getDefault("show fault");

            return new ShowFaultExecutor<ShellCommandResult>() {

                @Override
                public ShellCommandResult
                    multiFaultResult(CommandHistory history,
                                           int from,
                                           int to)
                    throws ShellException {
                    scr.setReturnValue(
                        history.dumpFaultingCommandsJson(from, to));
                    return scr;
                }

                @Override
                public ShellCommandResult
                    indexFaultResult(CommandHistory history, int index)
                    throws ShellException {
                    scr.setReturnValue(history.dumpCommandJson(index, true));
                    return scr;
                }

                @Override
                public ShellCommandResult
                    lastFaultResult(CommandHistory history)
                    throws ShellException {
                    final ObjectNode lastFault =
                        history.dumpLastFaultJson();
                    if (lastFault == null) {
                        throw new ShellArgumentException("no command fault.");
                    }
                    scr.setReturnValue(lastFault);
                    return scr;
                }
            }.commonExecute(args, shell);
        }

        private int toZeroBasedIndex(int index) {
            return (index > 0) ? (index - 1) : 0;
        }

        @Override
        protected String getCommandSyntax() {
            return SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return DESCRIPTION;
        }
    }
}
