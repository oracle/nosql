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

package oracle.kv.util.recovery;

import oracle.kv.impl.util.KVStoreMain;

/**
 * The main class of disaster recovery utilities, supports following command
 * lines options:
 *
 * 1. Run RecoverConfig utility. Usage details are as follow:
 *
 * {@literal
 * java -jar <recovery.jar> recoverconfig
 *        -input <admin Database DirectoryPath>
 *        -target <zipfile>
 *        [ -debug ]
 * }
 *
 * 2. Run ARTRequiredFiles utility. Usage details are as follow:
 *
 * {@literal
 * java -jar <recovery.jar> artrequiredfiles
 *        -targetRecoveryTime <targetRecoveryTime>
 *        -config <configuration file path>
 *        -target <zipfile>
 *        [ -debug ]
 * }
 *
 * 3. Run AdminRecover utility. Usage details are as follow:
 *
 * {@literal
 * java -jar <recovery.jar> adminrecover
 *        -config <configuration file path>
 *        -requiredfiles <required files directory path>
 *        -target <output directory path>
 *        [ -debug ]
 * }
 *
 * 4. Run SNRecover utility. Usage details are as follow:
 *
 * {@literal
 * java -jar <recovery.jar> snrecover
 *        -config <configuration file path>
 *        -requiredfile <requiredfile json file path>
 *        -topology <topology output json file path>
 *        -hostname <address of storage node on which SNRecover is to be run>
 *        [ -debug ]
 * }
 *
 * 5.) Run help command to get available recovery related command. Usage
 * details are as follow:
 *
 * {@literal
 * java -jar <recovery.jar> help
 * }
 *
 */

public class RecoveryMain {

    private static final String HELP_COMMAND_NAME = "help";
    private static final String HELP_COMMAND_DESC = "prints usage info";

    public static final String RECOVERY_USAGE_PREFIX =
        "Usage: java -jar KVHOME/lib/recovery.jar ";

    /**
     * Abstract Recovery Command. A Command is identified by its name,
     * which is the first arg to main().
     */
    private static abstract class Command {
        final String name;
        final String description;

        Command(String name, String description) {
            this.name = name;
            this.description = description;
        }

        abstract void run(String[] args)
            throws Exception;

        abstract String getUsageArgs();
    }

    /**
     * The order recovery commands appear in the array is the order they
     * appear in the 'help' output
     */
    private static Command[] ALL_COMMANDS = {

        new Command(RecoverConfig.COMMAND_NAME,
                    RecoverConfig.COMMAND_DESC) {

            @Override
            void run(String[] args) {
                RecoverConfig.main(KVStoreMain.makeArgs(args));
            }

            @Override
            String getUsageArgs() {
                return RecoverConfig.COMMAND_ARGS;
            }
        },

        new Command(ARTRequiredFiles.COMMAND_NAME,
                    ARTRequiredFiles.COMMAND_DESC) {

            @Override
            void run(String[] args) {
                ARTRequiredFiles.main(KVStoreMain.makeArgs(args));
            }

            @Override
            String getUsageArgs() {
                return ARTRequiredFiles.COMMAND_ARGS;
            }
        },

        new Command(AdminRecover.COMMAND_NAME,
                AdminRecover.COMMAND_DESC) {

            @Override
            void run(String[] args) {
                AdminRecover.main(KVStoreMain.makeArgs(args));
            }

            @Override
            String getUsageArgs() {
                return AdminRecover.COMMAND_ARGS;
            }
        },

        new Command(SNRecover.COMMAND_NAME,
                SNRecover.COMMAND_DESC) {

            @Override
            void run(String[] args) {
                SNRecover.main(KVStoreMain.makeArgs(args));
            }

            @Override
            String getUsageArgs() {
                return SNRecover.COMMAND_ARGS;
            }
        },

        new Command(HELP_COMMAND_NAME, HELP_COMMAND_DESC) {

            @Override
            void run(String[] args) {
                usage(null);
            }

            @Override
            String getUsageArgs() {
                final StringBuilder builder = new StringBuilder();
                builder.append('[');
                for (final Command cmd : ALL_COMMANDS) {
                    builder.append(" |\n\t ");
                    builder.append(cmd.name);
                }
                builder.append(']');
                return builder.toString();
            }
        }
    };

    public static void main(String args[])
        throws Exception {

        final String cmdName =
            (args.length == 0) ? HELP_COMMAND_NAME : args[0];

        final Command cmd = findCommand(cmdName);
        if (cmd == null) {
            usage("Unknown command: " + cmdName);
        } else {
            cmd.run(args);
        }
    }

    /**
     * Returns the Recovery Command with the given name, or null
     * if name is not found.
     */
    private static Command findCommand(String name) {
        for (final Command cmd : ALL_COMMANDS) {
            if (cmd.name.equals(name)) {
                return cmd;
            }
        }
        return null;
    }

    /**
     * Top-level recovery usage command.
     */
    private static void usage(String errorMsg) {
        if (errorMsg != null) {
            System.err.println(errorMsg);
        }
        final StringBuilder builder = new StringBuilder();
        builder.append(RECOVERY_USAGE_PREFIX);
        builder.append("\n  <");
        builder.append(ALL_COMMANDS[0].name);
        for (int i = 1; i < ALL_COMMANDS.length; i += 1) {
            builder.append(" |\n   ");
            builder.append(ALL_COMMANDS[i].name);
        }
        builder.append("> [args]");
        System.err.println(builder);
        usageExit();
    }

    /**
     * Does System.exit on behalf of all recovery usage commands.
     */
    private static void usageExit() {
        System.exit(2);
    }
}
