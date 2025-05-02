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

import java.util.Arrays;
import java.util.List;
import oracle.kv.impl.diagnostic.ssh.SSHClient;

import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;

/**
 * Implementation of the command "diagnostics setup". This step creates a
 * list of storage node descriptors that identify the SNs that are the target
 * for the "diagnostics collect" command.
 *
 * Subcommands of setup are:
 *   -add
 *   -list
 *   -delete
 *   -clear
 */
public class DiagnosticSetupCommand extends CommandWithSubs {
    private static final String COMMAND_NAME = "setup";
    private static final String ADD_SUBCOMMAND_NAME = "-add";
    private static final String LIST_SUBCOMMAND_NAME = "-list";
    private static final String DELETE_SUBCOMMAND_NAME = "-delete";
    private static final String CLEAR_SUBCOMMAND_NAME = "-clear";

    private static String storename = null;
    private static String snname = null;
    private static String hostname = null;
    private static String rootdir = null;
    private static String configdir = null;
    private static String sshUser = null;

    private static final
        List<? extends SubCommand> subs =
                       Arrays.asList(new DiagnosticSetupAddSub(),
                                     new DiagnosticSetupListSub(),
                                     new DiagnosticSetupDeleteSub(),
                                     new DiagnosticSetupClearSub());

    public DiagnosticSetupCommand() {
        super(subs, COMMAND_NAME, 4, 0);
    }

    @Override
    protected String getCommandOverview() {
        return "The Setup command creates the list of storage nodes which " +
                "are targeted " + eol + "by the diagnostics collect " +
                "command. This list is saved in the " +
                DiagnosticConstants.CONFIG_FILE_NAME +
                eol + "file.The sn-target-list file can also be created by " +
                "connecting to a running" + eol + "node in the cluster, " +
                "using the collect -logfiles -host <name> -port <number>" +
                eol + "command or verify { -checkLocal | -checkMulti }" +
                eol + " -host <name> -port <number> command." ;
    }

    /**
     * DiagnosticSetupAddSub - implements the "setup -add" subcommand.
     */
    static class DiagnosticSetupAddSub extends SubCommand {

        DiagnosticSetupAddSub() {
            super(ADD_SUBCOMMAND_NAME, 4);
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_NAME + " " + ADD_SUBCOMMAND_NAME + " " +
                   DiagnosticConstants.STORE_FLAG + " <store name>" + eolt +
                   DiagnosticConstants.SN_FLAG + " <SN name>" + eolt +
                   DiagnosticConstants.HOST_FLAG + " <host>" + eolt +
                   DiagnosticConstants.ROOT_DIR_FLAG +
                   " <kvroot directory>" + eolt +
                   DiagnosticConstants.SSH_USER_FLAG +
                   " <SSH username>" + eolt +
                   "[" + DiagnosticConstants.CONFIG_DIRECTORY_FLAG +
                   " <directory of configuration>]" ;
        }

        @Override
        protected String getCommandDescription() {
            return "Add a descriptor for this storage node to the " +
                    DiagnosticConstants.CONFIG_FILE_NAME + " file.";
        }

        @Override
        public String execute(String[] args, Shell shell)
                throws ShellException {

            /* Initialize arguments */
            storename = null;
            snname = null;
            sshUser = null;
            hostname = null;
            rootdir = null;
            configdir = null;

            Shell.checkHelp(args, this);
            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if (DiagnosticConstants.STORE_FLAG.equals(arg)) {
                        storename = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.SN_FLAG.equals(arg)) {
                        snname = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.SSH_USER_FLAG.equals(arg)) {
                        sshUser = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.HOST_FLAG.equals(arg)) {
                        hostname = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.ROOT_DIR_FLAG.equals(arg)) {
                        rootdir = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.CONFIG_DIRECTORY_FLAG.
                            equals(arg)) {
                        configdir = Shell.nextArg(args, i++, this);
                    } else {
                        shell.unknownArgument(args[i], this);
                    }
                }
            }

            if (storename == null || storename.isEmpty()) {
                shell.requiredArg(DiagnosticConstants.STORE_FLAG, this);
            }

            if (snname == null || snname.isEmpty()) {
                shell.requiredArg(DiagnosticConstants.SN_FLAG, this);
            }

            if (hostname == null || hostname.isEmpty()) {
                shell.requiredArg(DiagnosticConstants.HOST_FLAG, this);
            }

            if (sshUser == null || sshUser.isEmpty()) {
                shell.requiredArg(DiagnosticConstants.SSH_USER_FLAG, this);
            }

            if (rootdir == null || rootdir.isEmpty()) {
                shell.requiredArg(DiagnosticConstants.ROOT_DIR_FLAG, this);
            }

            if (configdir == null || configdir.isEmpty()) {
                configdir = DiagnosticConstants.DEFAULT_WORK_DIR;
            }

            String retMsg = ParametersValidator.
                    checkHostname(DiagnosticConstants.HOST_FLAG,
                                  hostname);
            if (retMsg != null) {
                throw new ShellUsageException(retMsg, this);
            }

            SNAInfo snaInfo = new SNAInfo(storename, snname, hostname,
                                          sshUser, rootdir);

            add(snaInfo);
            return null;
        }

        /**
         * Performs the actual work for the setup -add command.
         *
         * @param snaInfo the info of SNA
         */
        private void add(SNAInfo snaInfo)
                throws ShellUsageException {
            DiagnosticConfigFile configFile =
                    new DiagnosticConfigFile(configdir);
            try {
                List<SNAInfo> currentList = configFile.getAllSNAInfo();
                for (SNAInfo existingSNAInfo : currentList) {
                    if (snaInfo.equals(existingSNAInfo)) {
                        throw new Exception("Duplicated line " +
                                            existingSNAInfo.getSNAInfo()  +
                                            " already exists in " +
                                            "configuration file " +
                                            configFile.getFilePath());
                    }
                }
                currentList.add(snaInfo);
                configFile.rewrite(currentList);
            } catch (Exception ex) {
                throw new ShellUsageException(ex.getMessage(), this);
            }
        }
    }

    /**
     * DiagnosticSetupListSub - implements the "setup -list" subcommand.
     */
    static class DiagnosticSetupListSub extends SubCommand {

        DiagnosticSetupListSub() {
            super(LIST_SUBCOMMAND_NAME, 4);
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_NAME + " " + LIST_SUBCOMMAND_NAME +
                   " [" + DiagnosticConstants.CONFIG_DIRECTORY_FLAG +
                   " <configuration file directory>]" +
                   eolt + "[" + DiagnosticConstants.SSH_USER_FLAG +
                   " <SSH username>]";
        }

        @Override
        protected String getCommandDescription() {
            return "Display and validate all storage nodes descriptors in the "
                +  DiagnosticConstants.CONFIG_FILE_NAME + " file.";
        }

        @Override
        public String execute(String[] args, Shell shell)
                throws ShellException {

            /* Initialize arguments */
            configdir = null;
            sshUser = null;

            Shell.checkHelp(args, this);
            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    if (DiagnosticConstants.CONFIG_DIRECTORY_FLAG.
                            equals(args[i])) {
                        configdir = Shell.nextArg(args, i++, this);
                    } else if(DiagnosticConstants.SSH_USER_FLAG.
                            equals(args[i])) {
                        sshUser = Shell.nextArg(args, i++, this);
                    } else {
                        shell.unknownArgument(args[i], this);
                    }
                }
            }

            if (configdir == null || configdir.isEmpty()) {
                configdir = DiagnosticConstants.DEFAULT_WORK_DIR;
            }

            list(shell);
            return null;
        }

        /**
         * Performs the actual work for the setup -list command.
         */
        private void list(final Shell shell) throws ShellUsageException {
            /* Check whether configuration file is valid or not */
            try {
                DiagnosticConfigFile configFile =
                        new DiagnosticConfigFile(configdir);
                configFile.verify();
            } catch (Exception ex) {
                throw new ShellUsageException(ex.getMessage(), this);
            }

            try {
                DiagnosticTaskManager taskManager =
                        new DiagnosticTaskManager(shell);
                DiagnosticSSHTask task =
                        new DiagnosticSSHTask(configdir, sshUser) {
                    @Override
                    public DiagnosticSSHRunnable getSSHRunnable
                        (SNAInfo snaInfo, SSHClient client,
                         List<SNAInfo> taskSNList) {
                        return new ConfigurationFileChecker(snaInfo, this,
                                                            client);
                    }
                };
                taskManager.addTask(task);

                taskManager.execute();

            } catch (Exception ex) {
                throw new ShellUsageException(ex.toString(), this);
            }
        }

        /**
         * A subclass of DiagnosticSSHThread. It is to check whether all parts
         * of a SNAInfo in configuration files are valid or not.
         */
        private class ConfigurationFileChecker extends DiagnosticSSHRunnable {

            public ConfigurationFileChecker(SNAInfo snaInfo,
                                            DiagnosticTask threadOwner,
                                            SSHClient client) {
                super(snaInfo, threadOwner, client);
            }

            @Override
            public String doWork() throws Exception {
                String message = DiagnosticConstants.EMPTY_STRING;
                String retMsg = ParametersValidator.checkHostname("host",
                        snaInfo.getHost());
                if (retMsg != null) {
                    message += (DiagnosticConstants.NEW_LINE_TAB + retMsg);
                    return message.trim();
                }

                if (client == null) {
                    message += "Cannot connect " + snaInfo.getHost();
                    return message.trim();
                }

                if (!client.checkFile(snaInfo.getRootdir())) {
                    message += (DiagnosticConstants.NEW_LINE_TAB +
                            DiagnosticConstants.NOT_FOUND_ROOT_MESSAGE +
                            snaInfo.getRootdir());
                }

                return message.trim();
            }
        }
    }

    /**
     * DiagnosticSetupDeleteSub - implements the "setup -delete" subcommand.
     */
    static class DiagnosticSetupDeleteSub extends SubCommand {
        DiagnosticSetupDeleteSub() {
            super(DELETE_SUBCOMMAND_NAME, 4);
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_NAME + " " + DELETE_SUBCOMMAND_NAME + eolt +
                   "[" + DiagnosticConstants.STORE_FLAG +
                   " <store name>]" + eolt +
                   "[" + DiagnosticConstants.SN_FLAG +
                   " <SN name>]" + eolt +
                   "[" + DiagnosticConstants.HOST_FLAG +
                   " <host>]" + eolt +
                   "[" + DiagnosticConstants.ROOT_DIR_FLAG +
                   " <kvroot directory>]" + eolt +
                   "[" + DiagnosticConstants.SSH_USER_FLAG +
                   " <SSH username>] " + eolt +
                   "[" + DiagnosticConstants.CONFIG_DIRECTORY_FLAG +
                   " <configuration file directory>]" ;
        }

        @Override
        protected String getCommandDescription() {
            return "Delete SNA descriptors from the " +
                    DiagnosticConstants.CONFIG_FILE_NAME +
                    " file";
        }

        @Override
        public String execute(String[] args, Shell shell)
                throws ShellException {

            /* Initialize arguments */
            storename = null;
            snname = null;
            sshUser = null;
            hostname = null;
            rootdir = null;
            configdir = null;

            Shell.checkHelp(args, this);
            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if (DiagnosticConstants.STORE_FLAG.equals(arg)) {
                        storename = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.SN_FLAG.equals(arg)) {
                        snname = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.SSH_USER_FLAG.equals(arg)) {
                        sshUser = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.HOST_FLAG.equals(arg)) {
                        hostname = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.ROOT_DIR_FLAG.equals(arg)) {
                        rootdir = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.CONFIG_DIRECTORY_FLAG.
                            equals(arg)) {
                        configdir = Shell.nextArg(args, i++, this);
                    } else {
                        shell.unknownArgument(args[i], this);
                    }
                }
            }

            if (configdir == null || configdir.isEmpty()) {
                configdir = DiagnosticConstants.DEFAULT_WORK_DIR;
            }

            delete(shell);
            return null;
        }

        /**
         * Performs the actual work for the setup -delete command.
         */
        private void delete(Shell shell) throws ShellUsageException {
            String OUTPUT_MESSAGE = " line has been deleted from " ;
            String PLURAL_OUTPUT_MESSAGE = " lines have been deleted from ";

            /* store the number of deleted SNA info from configuration file */
            int numberOfDeleted = 0;

            DiagnosticConfigFile configFile =
                    new DiagnosticConfigFile(configdir);
            try {
                SNAInfo patternSNAInfo = new SNAInfo(storename, snname,
                                                     hostname, sshUser,
                                                     rootdir);
                numberOfDeleted = configFile.delete(patternSNAInfo);

                /*
                 * Print out message to indicate how many lines SNA info is
                 * deleted from configuration file
                 */
                if (numberOfDeleted > 1) {
                    shell.getOutput().println(numberOfDeleted +
                                              PLURAL_OUTPUT_MESSAGE +
                                              configFile.getFilePath());
                } else {
                    shell.getOutput().println(numberOfDeleted +
                                              OUTPUT_MESSAGE +
                                              configFile.getFilePath());
                }

            } catch (Exception ex) {
                throw new ShellUsageException(ex.getMessage(), this);
            }
        }
    }

    /**
     * DiagnosticSetupClearSub - implements the "setup -clear" subcommand.
     */
    static class DiagnosticSetupClearSub extends SubCommand {

        DiagnosticSetupClearSub() {
            super(CLEAR_SUBCOMMAND_NAME, 4);
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_NAME + " " + CLEAR_SUBCOMMAND_NAME +
                   " [" + DiagnosticConstants.CONFIG_DIRECTORY_FLAG +
                   " <configuration file directory>]" ;
        }

        @Override
        protected String getCommandDescription() {
            return "Clear info of all SNAs from configuration file";
        }

        @Override
        public String execute(String[] args, Shell shell)
                throws ShellException {
            /* Initialize argument */
            configdir = null;

            Shell.checkHelp(args, this);
            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    if (DiagnosticConstants.CONFIG_DIRECTORY_FLAG.
                            equals(args[i])) {
                        configdir = Shell.nextArg(args, i++, this);
                    } else {
                        shell.unknownArgument(args[i], this);
                    }
                }
            }

            if (configdir == null || configdir.isEmpty()) {
                configdir = DiagnosticConstants.DEFAULT_WORK_DIR;
            }

            clear();
            return null;
        }

        /**
         * Performs the actual work for the setup -clear command.
         */
        private void clear() throws ShellUsageException {
            DiagnosticConfigFile configFile =
                    new DiagnosticConfigFile(configdir);
            try {
                configFile.clear();
            } catch (Exception ex) {
                throw new ShellUsageException(ex.getMessage(), this);
            }
        }
    }
}
