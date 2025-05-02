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

package oracle.kv.impl.util;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import oracle.kv.KVVersion;
import oracle.kv.impl.diagnostic.DiagnosticCollectCommand;
import oracle.kv.impl.diagnostic.DiagnosticSetupCommand;
import oracle.kv.impl.diagnostic.DiagnosticVerifyCommand;
import oracle.kv.impl.diagnostic.ssh.SSHClientManager;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

/**
 * To implement a new command:
 * 1.  Implement a class that extends ShellCommand.
 * 2.  Add it to the static list, commands, in this class.
 *
 * Commands that have subcommands should extend SubCommand.  See one of the
 * existing classes for example code (e.g. DiagnosticCollectCommand).
 */
public class DiagnosticShell extends Shell {

    public static final String COMMAND_NAME = "diagnostics";
    public static final String COMMAND_DESC =
        "runs the diagnostics command line interface";
    public static final String COMMAND_ARGS = "(setup|collect|" + eol +
            "verify)[args]";

    private boolean noprompt = false;
    private String[] commandToRun;
    private int nextCommandIdx = 0;

    private DiagnosticParser parser;

    static final String prompt = "diagnostics-> ";
    static final String usageHeader =
        "Oracle NoSQL Database Diagnostic Utility Commands:" + eol;
    static final String versionString = " (" +
        KVVersion.CURRENT_VERSION.getNumericVersionString() + ")";

    /*
     * The list of commands available. List setup first, since that's the
     * first required step, then collect.
     */
    private static List<? extends ShellCommand> commands =
                       Arrays.asList(new DiagnosticSetupCommand(),
                                     new DiagnosticCollectCommand(),
                                     new DiagnosticVerifyCommand(),
                                     new Shell.ExitCommand(),
                                     new Shell.HelpCommand()
                                     );

    public DiagnosticShell(InputStream input, PrintStream output) {
        super(input, output, false /* disableJlineEventDesignator */);
    }

    @Override
    public void init() {
    }

    @Override
    public void shutdown() {
        /* Clear all SSH Clients */
        SSHClientManager.clearClients();
    }

    @Override
    public List<? extends ShellCommand> getCommands() {
        return commands;
    }

    @Override
    public String getPrompt() {
        return noprompt ? null : prompt;
    }

    @Override
    public String getUsageHeader() {
        return usageHeader;
    }

    /**
     * If retry is true, return that, but be sure to reset the value
     */
    @Override
    public boolean doRetry() {
        return false;
    }

    public void start() {
        init();
        if (commandToRun != null) {
            try {
                run(commandToRun[0], commandToRun);
            } catch (ShellException se) {
                handleShellException(commandToRun[0], se);
            } catch (Exception e) {
                handleUnknownException(commandToRun[0], e);
            }
        } else {
            loop();
        }
        shutdown();
    }

    private class DiagnosticParser extends CommandParser {

        private DiagnosticParser(String[] args) {
            /*
             * The true argument tells CommandParser that this class will
             * handle all flags, not just those unrecognized.
             */
            super(args, true);
        }

        @Override
        protected void verifyArgs() {
            if ((commandToRun != null) &&
                (nextCommandIdx < commandToRun.length)) {
                usage("Flags may not follow commands");
            }
        }

        @Override
        public void usage(String errorMsg) {
            if (errorMsg != null) {
                System.err.println(errorMsg);
            }
            System.err.println(KVSTORE_USAGE_PREFIX + COMMAND_NAME + eolt +
                   COMMAND_ARGS);
            System.exit(1);
        }

        @Override
        protected boolean checkArg(String arg) {
            if (NOPROMPT_FLAG.equals(arg)) {
                noprompt = true;
                return true;
            }
            addToCommand(arg);
            return true;
        }

        /*
         * Add unrecognized args to the commandToRun array.
         */
        private void addToCommand(String arg) {
            if (commandToRun == null) {
                commandToRun = new String[getNRemainingArgs() + 1];
            }
            commandToRun[nextCommandIdx++] = arg;
        }
    }


    public void parseArgs(String args[]) {
        parser = new DiagnosticParser(args);
        parser.parseArgs();
    }

    public static void main(String[] args) {
        DiagnosticShell shell = new DiagnosticShell(System.in, System.out);
        shell.parseArgs(args);
        shell.start();
        if (shell.getExitCode() != EXIT_OK) {
            System.exit(shell.getExitCode());
        }
    }
}
