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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import oracle.kv.impl.security.util.PasswordReader;
import oracle.kv.impl.security.util.ShellPasswordReader;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;

/**
 * To implement a new command:
 * 1.  Implement a class that extends ShellCommand.
 * 2.  Add it to the static list, commands, in this class.
 *
 * Commands that have subcommands should extend SubCommand.  See one of the
 * existing classes for example code (e.g. WalletCommand).
 */

public class SecurityShell extends Shell {

    private static final String WALLET_COMMAND_CLASS =
        "oracle.kv.impl.util.WalletCommand";

    public static final String COMMAND_NAME = "securityconfig";
    public static final String COMMAND_DESC =
        "runs the security configuration command line interface";
    public static final String COMMAND_ARGS =
        CommandParser.getHostUsage() + " " +
        CommandParser.getPortUsage() +
        " [single command and arguments]";

    private CommandParser parser;
    private boolean noprompt = false;
    private int nextCommandIdx = 0;
    private String[] commandToRun;
    private final /* final */ PasswordReader passwordReader;

    static final String prompt = "security-> ";
    static final String usageHeader =
        "Oracle NoSQL Database Security Configuration Commands:" + eol;

    /*
     * The list of commands available
     */
    private final List<ShellCommand> commands;

    /*
     * The flags whose following value will be masked with mask character(*) in
     * the command line history.
     */
    private final static String[] maskFlags = new String[] {
        "-kspwd", "-secret"
    };

    public SecurityShell(InputStream input, PrintStream output,
                         PasswordReader passwordReader) {
        super(input, output, true, maskFlags);
        this.passwordReader = passwordReader;
        commands = new ArrayList<ShellCommand>();

        final List<? extends ShellCommand> basicCommands =
            Arrays.asList(new Shell.ExitCommand(),
                          new Shell.HelpCommand(),
                          new PwdfileCommand(),
                          new SecurityConfigCommand());
        commands.addAll(basicCommands);

        /* Wallet is conditional on EE */
        final ShellCommand walletCommand = findWalletCommand();
        if (walletCommand != null) {
            commands.add(walletCommand);
        }

        Collections.sort(commands, new Shell.CommandComparator());
    }

    @Override
    public void init() {
        /* nothing to do at this time */
    }

    @Override
    public void shutdown() {
        /* nothing to do at this time */
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

    /*
     * If retry is true, return that, but be sure to reset the value
     */
    @Override
    public boolean doRetry() {
        /* nothing to do at this time */
        return false;
    }

    public void start() {
        init();
        if (commandToRun != null) {
            try {
                final String result = run(commandToRun[0], commandToRun);
                output.println(result);
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

    /**
     * For testing.
     */
    void enableHidden() {
        if (!getHidden()) {
            toggleHidden();
        }
    }

    private final class ShellParser extends CommandParser {

        private ShellParser(String[] args) {
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

    public void parseArgs(String[] args) {
        parser = new ShellParser(args);
        parser.parseArgs();
    }

    public static void main(String[] args) {
        final SecurityShell shell =
            new SecurityShell(System.in, System.out,
                              new ShellPasswordReader());
        shell.parseArgs(args);
        shell.start();
        if (shell.getExitCode() != EXIT_OK) {
            System.exit(shell.getExitCode());
        }
    }

    PasswordReader getPasswordReader() {
        return passwordReader;
    }

    private ShellCommand findWalletCommand() {
        final Class<?> walletCommandClass;
        try {
            walletCommandClass = Class.forName(WALLET_COMMAND_CLASS);
        } catch (ClassNotFoundException cnfe) {
            return null;
        }

        try {
            return (ShellCommand)
                walletCommandClass.getDeclaredConstructor().newInstance();
        } catch (IllegalAccessException iae) {
            /* class or ctor() not accessible */
            return null;
        } catch (IllegalArgumentException e) {
            /* Wrong arguments to method */
            return null;
        } catch (InstantiationException ie) {
            /* class is abstract or not true class type, etc. */
            return null;
        } catch (InvocationTargetException e) {
            /* Exception in constructor */
            return null;
        } catch (ExceptionInInitializerError eiie) {
            /* Exception was thrown during class initialization */
            return null;
        } catch (NoSuchMethodException e) {
            /* Method not found */
            return null;
        } catch (SecurityException se) {
            /* No permissions to do instantiation */
            return null;
        }
    }

}
