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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.security.KeyStore;
import java.security.KeyStore.PasswordProtection;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.diagnostic.ssh.SSHClient;
import oracle.kv.impl.diagnostic.util.TopologyDetector;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.security.ssl.KeyStorePasswordSource;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;

/**
 * This command shell implements the diagnostics verify command. The syntax
 * for the command is: diagnostics verify {-checkLocal | -checkMulti}
 */
public class DiagnosticVerifyCommand extends CommandWithSubs {
    private static final String COMMAND_NAME = "verify";

    private static final String CHECK_LOCAL_SUBCOMMAND_NAME = "-checkLocal";
    private static final String CHECK_MULTI_SUBCOMMAND_NAME = "-checkMulti";

    private static final Logger logger = LoggerUtils.getLogger(
        DiagnosticVerifyCommand.class, "command");

    private static final Set<RemoteFile> remoteFileSet =
            new CopyOnWriteArraySet<RemoteFile>();
    private static final Map<InetAddress, List<RemoteFile>> IPRemoteMap =
            new ConcurrentHashMap<InetAddress, List<RemoteFile>>();

    private static final List<? extends SubCommand> subs =
            Arrays.asList(new DiagnosticVerifyLocalSub(),
                          new DiagnosticVerifyMultiSub());

    private static String snHostname = null;
    private static int snPort = -1;
    private static String sshUser = null;
    private static String user = null;
    private static String securityFile = null;
    private static String configdir = null;

    public DiagnosticVerifyCommand() {
        super(subs, COMMAND_NAME, 4, 0);
    }

    @Override
    protected String getCommandOverview() {
        return "The verify command checks that each SN's configuration " +
            eol + "has valid values and is consistent with other nodes in " +
            eol + "the cluster. A sn-target-list file must exist and can be" +
            eol + "created with diagnostics setup or diagnostics verify " +
            eol + "{ -checkLocal | -checkMulti } -host <name> -port <number> " +
            eol + "command.";
    }

    /**
     * Set user name a specified SNA Info by history SNA Info
     * @param snaInfo the specified SNA Info which has no user name
     * @param list the list of history SNA Info
     */
    private static void setUserByHistory(SNAInfo snaInfo, List<SNAInfo> list) {
        for (SNAInfo si : list) {
            /* Assign the user when find the same IP in history list */
            if (si.getIP().equals(snaInfo.getIP())) {
                snaInfo.setSSHUser(si.getSSHUser());
            }
        }
    }

    /**
     * Connect to a kvstore component to find the latest topology and use it to
     * update the sn target file.
     * @throws Exception
     */
    private static void updateSNTargetFile(Shell shell) throws Exception {

        try {
            /*
             * Connect topology and fetch the host name, port and kvroot of
             * all SNAs, then write the info into configuration file.
             */
            TopologyDetector detector = new TopologyDetector(snHostname,
                                                             snPort,
                                                             user,
                                                             securityFile);
            Topology topo = detector.getTopology(logger);
            List<StorageNode> list = topo.getSortedStorageNodes();
            DiagnosticConfigFile configFile =
                    new DiagnosticConfigFile(configdir);


            /* Store the history SNA Info to set user for new SNA Info */
            List<SNAInfo> historyList = configFile.getAllSNAInfo();
            configFile.clear();
            for (StorageNode sn : list) {
                StorageNodeId snId = sn.getStorageNodeId();
                String rootDir = detector.getRootDirPath(sn);

                if (rootDir != null) {
                    SNAInfo snaInfo = new SNAInfo(topo.getKVStoreName(),
                                                  snId.getFullName(),
                                                  sn.getHostname(),
                                                  rootDir);

                    /* Set user for the new SNA info */
                    setUserByHistory(snaInfo, historyList);

                    /*
                     * Set user for the new SNA info by the specified
                     * parameter
                     */
                    if (sshUser != null) {
                        snaInfo.setSSHUser(sshUser);
                    }

                    /* Add new SNA info into configuration file */
                    configFile.add(snaInfo);
                } else {
                    shell.getOutput().println("Cannot get root directory " +
                                    "path for " + snId.getFullName() +
                                    " on host " + sn.getHostname());
                }
            }
        } catch (Exception ex) {
            throw ex;
        }
    }

    static class DiagnosticVerifyLocalSub extends SubCommand {
        DiagnosticVerifyLocalSub() {
            super(CHECK_LOCAL_SUBCOMMAND_NAME, 6);
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_NAME + " " + CHECK_LOCAL_SUBCOMMAND_NAME + eolt +
                    "[" + DiagnosticConstants.HOST_FLAG +
                    " <host name of a SN in topology>]" + eolt +
                    "[" + DiagnosticConstants.PORT_FLAG +
                    " <registry port of a SN in topology>]" + eolt +
                    "[" + DiagnosticConstants.SSH_USER_FLAG +
                    " <SSH username>]" + eolt +
                    "[" + DiagnosticConstants.USER_FLAG +
                    " <store username>]" + eolt +
                    "[" + DiagnosticConstants.SECURITY_FLAG +
                    " <security-file-path>]" + eolt +
                    "[" + DiagnosticConstants.CONFIG_DIRECTORY_FLAG
                    + " <location of storage node target file>]";
        }

        @Override
        protected String getCommandDescription() {
            return "Verify that each SN's configuration file is valid";
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            /* Initialize arguments */
            snHostname = null;
            snPort = -1;
            sshUser = null;
            user = null;
            securityFile = null;
            configdir = null;

            Shell.checkHelp(args, this);
            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    if (DiagnosticConstants.HOST_FLAG.equals(args[i])) {
                        snHostname = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.PORT_FLAG.equals(args[i])) {
                        String sPort = Shell.nextArg(args, i++, this);
                        snPort = parseInt(sPort);
                        if (snPort <= 0) {
                            String info = DiagnosticConstants.PORT_FLAG +
                                          " should be greater than 0";
                            throw new ShellUsageException(info, this);
                        }
                    } else if (DiagnosticConstants.SSH_USER_FLAG.
                            equals(args[i])) {
                        sshUser = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.USER_FLAG.equals(args[i])) {
                        user = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.SECURITY_FLAG.
                            equals(args[i])) {
                        securityFile = Shell.nextArg(args, i++, this);
                    } else if  (DiagnosticConstants.CONFIG_DIRECTORY_FLAG.
                            equals(args[i])) {
                        configdir = Shell.nextArg(args, i++, this);
                    } else {
                        shell.unknownArgument(args[i], this);
                    }
                }
            }

            if (snPort < 0) {
                if (snHostname != null && !snHostname.isEmpty()) {
                    shell.requiredArg(DiagnosticConstants.PORT_FLAG, this);
                }
            } else {
                if (snHostname == null || snHostname.isEmpty()) {
                    shell.requiredArg(DiagnosticConstants.HOST_FLAG, this);
                }
            }

            if (configdir == null || configdir.isEmpty()) {
                configdir = DiagnosticConstants.DEFAULT_WORK_DIR;
            }

            checkLocalConfig(shell);

            return null;
        }

        /**
         * Copy the config.xml files from each SN to this client, and check
         * the properties in each file.
         *
         * @param shell
         * @throws ShellUsageException
         */
        private void checkLocalConfig(Shell shell) throws ShellUsageException {
            try {
                if (snHostname != null) {
                    updateSNTargetFile(shell);
                }
            } catch (Exception ex) {
                /*
                 * Updating configuration file failed when encounter exception,
                 * and the old configuration file is used in the follow
                 * statements to collect log files.
                 */
                if (ex.getMessage() != null &&
                    ex.getMessage().contains("Problem parsing")) {
                    throw new ShellUsageException(ex.getMessage(), this);
                }
                shell.getOutput().println("Updating configuration file " +
                                    "failed: " + ex.getMessage());
            }

            /*
             * Check whether the SN target list in the diagnostic command's
             * configuration file is valid.
             */
            try {
                DiagnosticConfigFile configFile =
                        new DiagnosticConfigFile(configdir);
                configFile.verify();
            } catch (Exception ex) {
                throw new ShellUsageException(ex.getMessage(), this);
            }

            try {
                /* Clear result directory list */
                remoteFileSet.clear();
                IPRemoteMap.clear();

                DiagnosticTaskManager diagnosticTaskManager =
                        new DiagnosticTaskManager(shell);

                DiagnosticSSHTask SSHtask =
                        new DiagnosticSSHTask(configdir, sshUser) {
                    @Override
                    public DiagnosticSSHRunnable
                        getSSHRunnable(SNAInfo snaInfo, SSHClient client,
                                       List<SNAInfo> taskSNList) {
                        return new ConfigGather(snaInfo, this, client);
                    }
                };

                LocalConfigCheck localConfigCheck = new LocalConfigCheck();

                /* Add tasks into DiagnosticTaskManager */
                diagnosticTaskManager.addTask(SSHtask);
                diagnosticTaskManager.addTask(localConfigCheck);

                /* Execute all tasks in DiagnosticTaskManager one by one */
                diagnosticTaskManager.execute();

                deleteEntireDirectory(new File("tmp"));

            } catch (Exception ex) {
                throw new ShellUsageException(ex.toString(), this);
            }
        }

        /**
         * A subclass of DiagnosticSSHThread. It is to copy xml configuration
         * files from SNAs.
         */
        private class ConfigGather extends DiagnosticSSHRunnable {
            final String HOST = "host";
            final String ROOT_DIR_FLAG = "root directory";
            final String DONT_EXIST = " does not exist";
            final String BLANKSPACE_SIGN = " ";

            public ConfigGather(SNAInfo snaInfo,
                                DiagnosticTask threadOwner,
                                SSHClient client) {
                super(snaInfo, threadOwner, client);
            }

            @Override
            public String doWork() throws Exception {
                String message = DiagnosticConstants.EMPTY_STRING;

                /* Check host is reachable or not */
                String retMsg =
                        ParametersValidator.checkHostname(HOST,
                                                          snaInfo.getHost());

                if (retMsg != null) {
                    message += (DiagnosticConstants.NEW_LINE_TAB + retMsg);
                    return message;
                }

                if (client == null) {
                    message += "Cannot connect " + snaInfo.getHost();
                    return message.trim();
                }

                /* Check root directory exists or not */
                if (!client.checkFile(snaInfo.getRootdir())) {
                    message += (DiagnosticConstants.NEW_LINE_TAB +
                            ROOT_DIR_FLAG + BLANKSPACE_SIGN
                            + snaInfo.getRootdir() + DONT_EXIST);
                }

                if (!message.equals(DiagnosticConstants.EMPTY_STRING)) {
                    return message.trim();
                }

                /* Copy xml configuration file from remote machine */
                File saveFolder = new File("tmp", snaInfo.getStoreName() + "_"
                        + snaInfo.getStorageNodeName() + "_"
                        + snaInfo.getHost());

                List<File> fileList = client.getConfigFile(snaInfo, saveFolder);
                for (File f : fileList) {
                    remoteFileSet.add(new RemoteFile(f, snaInfo));
                }

                message = DiagnosticConstants.NEW_LINE_TAB +
                        "Fetched configuration file from [" +
                        snaInfo.getSNAInfo() + "]";
                return message.trim();
            }
        }

        /**
         * Check whether the properties in each SN's config.xml are consistent.
         * Current checks are:
         *  - confirm that all ports used by the SN are available.
         *  - check that there are a sufficient number of ports in the
         *    HA range
         */
        private class LocalConfigCheck extends DiagnosticTask {
            final String PREFIX_MESSAGE = "\nSN Local Configuration check\n\t";

            @Override
            public void doWork() throws Exception {
                String retMsg = null;
                String message = DiagnosticConstants.EMPTY_STRING;

                if (remoteFileSet.size() == 0) {
                    message = "No configuration file found";
                    /* Notify complete a sub task */
                    notifyCompleteSubTask(PREFIX_MESSAGE + message.trim());
                    return;
                }

                for (RemoteFile rFile : remoteFileSet) {
                    String allMessage = null;
                    BootstrapParams bp =
                            ConfigUtils.
                                getBootstrapParams(rFile.getLocalFile());

                    PortConflictValidator validator =
                            new PortConflictValidator();

                    String title = "Host: " + rFile.getSNAInfo().getHost()
                            + ", config: " + rFile.getSNAInfo().getRootdir()
                            + "/" + rFile.getLocalFile().getName();

                    allMessage = aggregateMessage(allMessage, title, 0);

                    /* Check registry port is already assigned or not */
                    retMsg =
                        validator.check(ParameterState.COMMON_REGISTRY_PORT,
                                        bp.getRegistryPort());
                    allMessage = aggregateMessage(allMessage, retMsg, 1);

                    /* Check HA range ports are already assigned or not */
                    retMsg = validator.check(ParameterState.COMMON_PORTRANGE,
                                             bp.getHAPortRange());
                    allMessage = aggregateMessage(allMessage, retMsg, 1);

                    if (bp.isHostingAdmin()) {
                        /* Check the number of ports in a range is enough */
                        retMsg = ParametersValidator.checkRangePortsNumber
                            (ParameterState.COMMON_PORTRANGE,
                             bp.getHAPortRange(),
                             bp.getCapacity() + 1);
                        allMessage = aggregateMessage(allMessage,
                                                      retMsg, 1);
                    } else {
                        /* Check the number of ports in a range is enough */
                        retMsg = ParametersValidator.checkRangePortsNumber
                            (ParameterState.COMMON_PORTRANGE,
                             bp.getHAPortRange(),
                             bp.getCapacity());
                        allMessage = aggregateMessage(allMessage,
                                                      retMsg, 1);
                    }

                    if (bp.getServicePortRange() != null
                            && !bp.getServicePortRange().isEmpty()) {
                        /*
                         * Check service range ports are already assigned or
                         * not
                         */
                        retMsg = validator.check(
                                    ParameterState.COMMON_SERVICE_PORTRANGE,
                                    bp.getServicePortRange());
                        allMessage = aggregateMessage(allMessage, retMsg, 1);
                    }

                    /* Check trap port is already assigned or not */
                    retMsg = validator.check(
                                ParameterState.COMMON_MGMT_TRAP_PORT,
                                bp.getMgmtTrapPort());
                    allMessage = aggregateMessage(allMessage, retMsg, 1);

                    /* Check poll port is already assigned or not */
                    retMsg = validator.check(
                                ParameterState.COMMON_MGMT_POLL_PORT,
                                bp.getMgmtPollingPort());
                    allMessage = aggregateMessage(allMessage, retMsg, 1);

                    if (allMessage.equals(title)) {
                        allMessage = aggregateMessage(allMessage,
                                                      "SN configuration " +
                                                      "is valid.",
                                                      1);

                    }
                    message += (DiagnosticConstants.NEW_LINE_TAB + allMessage);
                }

                /* Notify complete a sub task */
                notifyCompleteSubTask(PREFIX_MESSAGE + message.trim());
            }

            protected String aggregateMessage(String allMessage,
                                              String addMessage, int level) {
                String TAB = "\t";
                if (addMessage != null && !addMessage.isEmpty()) {
                    if (allMessage == null || allMessage.isEmpty()) {
                        allMessage = addMessage;
                    } else {
                        allMessage += (DiagnosticConstants.NEW_LINE_TAB);
                        /* Different level has different indent */
                        for (int i = 0; i < level; i++) {
                            allMessage += TAB;
                        }
                        allMessage += addMessage;
                    }
                }
                return allMessage;
            }
        }
    }

    /**
     * This class is used to check whether each SN's configuration is
     * consistent with other nodes in the cluster.
     */
    static class DiagnosticVerifyMultiSub extends SubCommand {
        /*
         * Store the generate result directory which save log files from remote
         * machines
         */
        private Map<InetAddress, EnvParams> resultMap =
                new ConcurrentHashMap<InetAddress, EnvParams>();

        DiagnosticVerifyMultiSub() {
            super(CHECK_MULTI_SUBCOMMAND_NAME, 6);
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_NAME + " " + CHECK_MULTI_SUBCOMMAND_NAME + eolt +
                    "[" + DiagnosticConstants.HOST_FLAG +
                    " <host name of a SN in topology>]" + eolt +
                    "[" + DiagnosticConstants.PORT_FLAG +
                    " <registry port of a SN in topology>]" + eolt +
                    "[" + DiagnosticConstants.SSH_USER_FLAG +
                    " <SSH username>]" + eolt +
                    "[" + DiagnosticConstants.USER_FLAG +
                    " <store username>]" + eolt +
                    "[" + DiagnosticConstants.SECURITY_FLAG +
                    " <security-file-path>]" + eolt +
                    "[" + DiagnosticConstants.CONFIG_DIRECTORY_FLAG
                    + " <location of storage node target file>]";
        }

        @Override
        protected String getCommandDescription() {
            return "Determine whether each SN's configuration is consistent " +
                "with other members of the cluster";
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            /* Initialize arguments */
            snHostname = null;
            snPort = -1;
            sshUser = null;
            user = null;
            securityFile = null;
            configdir = null;

            Shell.checkHelp(args, this);
            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    if (DiagnosticConstants.HOST_FLAG.equals(args[i])) {
                        snHostname = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.PORT_FLAG.equals(args[i])) {
                        String sPort = Shell.nextArg(args, i++, this);
                        try {
                            snPort = Integer.valueOf(sPort);
                        } catch (NumberFormatException nfe) {
                            invalidArgument(DiagnosticConstants.PORT_FLAG);
                        }

                        if (snPort <= 0) {
                            String info = DiagnosticConstants.PORT_FLAG +
                                          " should be greater than 0";
                            throw new ShellUsageException(info, this);
                        }
                    } else if (DiagnosticConstants.SSH_USER_FLAG.
                            equals(args[i])) {
                        sshUser = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.USER_FLAG.equals(args[i])) {
                        user = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.SECURITY_FLAG.
                            equals(args[i])) {
                        securityFile = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.CONFIG_DIRECTORY_FLAG.
                            equals(args[i])) {
                        configdir = Shell.nextArg(args, i++, this);
                    } else {
                        shell.unknownArgument(args[i], this);
                    }
                }
            }

            if (snPort < 0) {
                if (snHostname != null && !snHostname.isEmpty()) {
                    shell.requiredArg(DiagnosticConstants.PORT_FLAG, this);
                }
            } else {
                if (snHostname == null || snHostname.isEmpty()) {
                    shell.requiredArg(DiagnosticConstants.HOST_FLAG, this);
                }
            }

            if (configdir == null || configdir.isEmpty()) {
                configdir = DiagnosticConstants.DEFAULT_WORK_DIR;
            }

            checkMulti(shell);

            return null;
        }

        /**
         * check whether the environment parameters among all machines which
         * host the SN
         *
         * @param shell
         * @throws ShellUsageException
         */
        private void checkMulti(Shell shell) throws ShellUsageException {
            try {
                if (snHostname != null) {
                    updateSNTargetFile(shell);
                }
            } catch (Exception ex) {
                /*
                 * Updating configuration file failed when encounter exception,
                 * and the old configuration file is used in the follow
                 * statements to collect log files.
                 */
                if (ex.getMessage() != null &&
                    ex.getMessage().contains("Problem parsing")) {
                    throw new ShellUsageException(ex.getMessage(), this);
                }
                shell.getOutput().println("Updating configuration file " +
                                    "failed: " + ex.getMessage());
            }

            /*
             * Check whether the SN target list in the diagnostic command's
             * configuration file is valid.
             */
            try {
                DiagnosticConfigFile configFile =
                        new DiagnosticConfigFile(configdir);
                configFile.verify();
            } catch (Exception ex) {
                throw new ShellUsageException(ex.getMessage(), this);
            }

            try {
                /* Clear result map */
                resultMap.clear();
                remoteFileSet.clear();
                IPRemoteMap.clear();

                DiagnosticTaskManager diagnosticTaskManager =
                        new DiagnosticTaskManager(shell);

                DiagnosticSSHTask SSHtask =
                        new DiagnosticSSHTask(configdir, sshUser) {
                    @Override
                    public DiagnosticSSHRunnable
                        getSSHRunnable(SNAInfo snaInfo, SSHClient client,
                                       List<SNAInfo> taskSNList) {
                        return new ConfigGather(snaInfo, this, client,
                                                taskSNList);
                    }
                };
                EnvParamsCheck check = new EnvParamsCheck();

                /* Add tasks into DiagnosticTaskManager */
                diagnosticTaskManager.addTask(SSHtask);
                diagnosticTaskManager.addTask(check);

                /* Execute all tasks in DiagnosticTaskManager one by one */
                diagnosticTaskManager.execute();

            } catch (Exception ex) {
                throw new ShellUsageException(ex.toString(), this);
            }
        }

        /**
         * A subclass of DiagnosticSSHThread. It is to copy xml configuration
         * files from SNAs.
         */
        private class ConfigGather extends DiagnosticSSHRunnable {
            final String HOST = "host";
            private List<SNAInfo> list;

            public ConfigGather(SNAInfo snaInfo,
                                DiagnosticTask threadOwner,
                                SSHClient client,
                                List<SNAInfo> list) {
                super(snaInfo, threadOwner, client);
                this.list = list;
            }

            @Override
            public String doWork() throws Exception {
                String message = DiagnosticConstants.EMPTY_STRING;

                /* Check host is reachable or not */
                String retMsg = ParametersValidator.
                        checkHostname(HOST, snaInfo.getHost());

                if (retMsg != null) {
                    message += (DiagnosticConstants.NEW_LINE_TAB + retMsg);
                    return message;
                }

                if (client == null) {
                    message += "Cannot connect " + snaInfo.getHost();
                    return message.trim();
                }

                /*
                 * Fetch latency, Java version and the network status of the
                 * machines
                 */
                long latency = client.getTimeLatency();
                JavaVersionVerifier javaVersion = client.getJavaVersion();
                Map<SNAInfo, Boolean> map = client.getNetWorkStatus(list);

                File saveFolder = new File("tmp", snaInfo.getStoreName() + "_"
                        + snaInfo.getStorageNodeName() + "_"
                        + snaInfo.getHost());

                resultMap.put(snaInfo.getIP(), new EnvParams(latency,
                                                             javaVersion, map,
                                                             saveFolder));

                /* Get configuration files from SNA hosts */
                List<File> fileList = client.getConfigFile(snaInfo, saveFolder);
                List<RemoteFile> rFilelist = IPRemoteMap.get(snaInfo.getIP());
                if (rFilelist == null) {
                    rFilelist = new ArrayList<RemoteFile>();
                    IPRemoteMap.put(snaInfo.getIP(), rFilelist);
                }
                for (File f : fileList) {
                    RemoteFile rf = new RemoteFile(f, snaInfo);
                    remoteFileSet.add(rf);
                    rFilelist.add(rf);
                }

                message = DiagnosticConstants.NEW_LINE_TAB +
                        "Fetched configuration file from [" +
                        snaInfo.getSNAInfo() + "]";
                return message.trim();
            }
        }
        /**
         * This class is used to check the environment parameters of SNAs and
         * the consistency of security policy and KVStore version.
         * The algorithm of computing the clock slew is as follows:
         * 1. Get the latency from client to all SNA hosts.
         * 2. Get the minimum latency and determine the fastest host.
         * 3. Compare the latency of fastest host with other SNA hosts and get
         * the clock slew.
         *
         * The algorithm of compare security policy is as follows:
         * 1. Get the security policy of all SNAs from configuration.
         * 2. Determine whether the SNAs apply security functions.
         * 3. Check whether the secured SNAs uses the same security policy.
         */
        private class EnvParamsCheck extends DiagnosticTask {
            final String PREFIX_MESSAGE = "\nMulti-SNs compatibility check";

            @Override
            public void doWork() throws Exception {
                String message = DiagnosticConstants.EMPTY_STRING;
                String fastestHost = DiagnosticConstants.EMPTY_STRING;
                long minLatency = Long.MAX_VALUE;

                if (remoteFileSet.size() == 0) {
                    message = "No configuration file found";
                    notifyCompleteSubTask(PREFIX_MESSAGE +
                                          DiagnosticConstants.NEW_LINE_TAB +
                                          message);
                    return;
                }

                /* Get the fastest host and minimum latency */
                for (Map.Entry<InetAddress, EnvParams> entry :
                    resultMap.entrySet()) {
                    if (entry.getValue().getLatency() < minLatency) {
                        minLatency = entry.getValue().getLatency();
                        fastestHost = entry.getKey().getHostName();
                    }
                }

                /*
                 * Check clock skew, java version and network status among the
                 * machines which host SNAs.
                 */
                String clockSkewMsg = "Clock skew: ";
                String jdkversionMsg = "Java version: ";
                String networkMsg = "Network connection status: ";

                for (Map.Entry<InetAddress, EnvParams> entry :
                    resultMap.entrySet()) {
                    if (minLatency != Long.MIN_VALUE) {
                        clockSkewMsg += (DiagnosticConstants.NEW_LINE_TAB + "\t"
                                + (entry.getValue().getLatency() - minLatency)
                                + "ms (" + entry.getKey().getHostName() + " to "
                                + fastestHost + ")");
                    } else {
                        clockSkewMsg += (DiagnosticConstants.NEW_LINE_TAB +
                                "\tCannot get clock skew for (" +
                                entry.getKey().getHostName() + " to "
                                + fastestHost + ")");
                    }

                    jdkversionMsg += (DiagnosticConstants.NEW_LINE_TAB + "\t"
                            + entry.getValue().getJavaVersion() + " ("
                            + entry.getKey().getHostName() + ")");

                    Map<SNAInfo, Boolean> map = entry.getValue()
                                                     .getNetworkConnectionMap();
                    for (Map.Entry<SNAInfo, Boolean> e : map.entrySet()) {
                        if (e.getValue()) {
                            networkMsg += (DiagnosticConstants.NEW_LINE_TAB
                                    + "\t" + entry.getKey().getHostName()
                                    + " to " + e.getKey().getHost()
                                    + ": connected");
                        } else {
                            networkMsg += (DiagnosticConstants.NEW_LINE_TAB
                                    + "\t" + entry.getKey().getHostName()
                                    + " to " + e.getKey().getHost()
                                    + ": disconnected");
                        }
                    }

                }
                List<RemoteFile> nonSecuredList = new ArrayList<RemoteFile>();
                Map<PrivateKey, List<RemoteFile>> keyMap =
                        new HashMap<PrivateKey, List<RemoteFile>>();

                Map<RemoteFile, KVVersion> versionsBeforePreReq =
                        new HashMap<RemoteFile, KVVersion>();

                Map<RemoteFile, KVVersion> versionsAfterPreReq =
                        new HashMap<RemoteFile, KVVersion>();

                /*
                 * Check if the SNs have consistent security policies, and
                 * if their KVstore versions are compatible. For SNs hosted
                 * on the same machine, check that their port ranges do not
                 * overlap.
                 */
                String securityMsg = "Security Policy: ";
                String kvversionMsg = "KVStore version: ";

                String overLapMsg = "Port Ranges: ";
                for (RemoteFile rFile : remoteFileSet) {
                    BootstrapParams bp =
                        ConfigUtils.getBootstrapParams(rFile.getLocalFile());
                    /* Group the security policy of all SNAs */
                    if (bp.getSecurityDir() != null
                            && !bp.getSecurityDir().isEmpty()) {
                        File securityDir =
                                new File(rFile.getLocalFile().getParentFile() +
                                         File.separator + bp.getSecurityDir());
                        File securityConfigPath =
                                new File(securityDir,
                                         FileNames.SECURITY_CONFIG_FILE);
                        LoadParameters lp =
                                LoadParameters.getParameters(securityConfigPath,
                                                             null);
                        SecurityParams sp =
                                new SecurityParams(lp, securityConfigPath);

                        /*
                         * Get private key and based on the private key to
                         * group the security policy
                         */
                        final KeyStorePasswordSource pwdSrc =
                                KeyStorePasswordSource.create(sp);
                        final String keyStoreName =
                                sp.getConfigDir() + File.separator +
                                sp.getKeystoreFile();

                        char[] ksPwd = null;
                        ksPwd = pwdSrc.getPassword();
                        KeyStore keyStore = loadStore(keyStoreName, ksPwd,
                                                      "keystore",
                                                      sp.getKeystoreType());

                        PasswordProtection pwdParam =
                                new PasswordProtection(ksPwd);
                        PrivateKeyEntry pkEntry =
                                (PrivateKeyEntry) keyStore.getEntry("shared",
                                                                    pwdParam);
                        PrivateKey privateKey = pkEntry.getPrivateKey();

                        List<RemoteFile> list = keyMap.get(privateKey);
                        if (list == null) {
                            list = new ArrayList<RemoteFile>();
                            keyMap.put(privateKey, list);
                        }
                        list.add(rFile);
                    } else {
                        nonSecuredList.add(rFile);
                    }

                    /* Compare the version of KVStore */
                    if (bp.getSoftwareVersion().
                            compareTo(KVVersion.PREREQUISITE_VERSION) < 0) {
                        versionsBeforePreReq.put(rFile,
                                                 bp.getSoftwareVersion());
                    } else {
                        versionsAfterPreReq.put(rFile,
                                                bp.getSoftwareVersion());
                    }
                }

                /* Build check results of security policy and KVStore version */
                int securityPolicyCount = 0;
                if (!nonSecuredList.isEmpty()) {
                    securityPolicyCount = 1;
                }
                securityPolicyCount += keyMap.size();
                if (securityPolicyCount > 1) {
                    securityMsg += DiagnosticConstants.NEW_LINE_TAB + "\t" +
                            securityPolicyCount +
                            " different security policies are found\n";
                } else if(securityPolicyCount == 1) {
                    securityMsg += DiagnosticConstants.NEW_LINE_TAB + "\t" +
                                "Security policies are consistent\n";
                } else {
                    securityMsg += DiagnosticConstants.NEW_LINE_TAB + "\t" +
                                "No security policy found\n";
                }
                int securityPolicyNO = 1;
                for (Map.Entry<PrivateKey, List<RemoteFile>> entry :
                    keyMap.entrySet()) {
                    for (RemoteFile rf : entry.getValue()) {
                        securityMsg += DiagnosticConstants.NEW_LINE_TAB + "\t" +
                                rf + " secured " + "(security policy " +
                                securityPolicyNO + ")";
                    }
                    securityPolicyNO++;
                    securityMsg += "\n";
                }

                for (RemoteFile rf : nonSecuredList) {
                    securityMsg += DiagnosticConstants.NEW_LINE_TAB + "\t" +
                            rf + " non-secured";
                }

                if (versionsBeforePreReq.size() > 0 &&
                        versionsAfterPreReq.size() > 0) {
                    kvversionMsg += DiagnosticConstants.NEW_LINE_TAB + "\t" +
                        "There are version incompatibilities between " +
                        "nodes in the cluster. Nodes" +
                        DiagnosticConstants.NEW_LINE_TAB +
                        "\twith versions earlier than the required " +
                        "minimum of " +
                         KVVersion.PREREQUISITE_VERSION.
                         getNumericVersionString() + ":\n";

                    for (Map.Entry<RemoteFile, KVVersion> entry :
                        versionsBeforePreReq.entrySet()) {
                        kvversionMsg += DiagnosticConstants.NEW_LINE_TAB  +
                                "\t" + entry.getKey() +
                                " " + entry.getValue().getNumericVersionString();
                    }

                    if (versionsBeforePreReq.size() > 0 &&
                            versionsAfterPreReq.size() > 0) {
                        kvversionMsg += "\n";
                        kvversionMsg += DiagnosticConstants.NEW_LINE_TAB +
                                "\tNodes with compatible versions:\n";
                    }

                    for (Map.Entry<RemoteFile, KVVersion> entry :
                        versionsAfterPreReq.entrySet()) {
                        kvversionMsg += DiagnosticConstants.NEW_LINE_TAB  +
                                "\t" + entry.getKey() +
                                " " + entry.getValue().getNumericVersionString();
                    }
                } else if (versionsBeforePreReq.size() > 0 ||
                        versionsAfterPreReq.size() > 0) {
                    kvversionMsg += DiagnosticConstants.NEW_LINE_TAB +
                            "\tAll nodes have compatible versions:\n";

                    for (Map.Entry<RemoteFile, KVVersion> entry :
                        versionsBeforePreReq.entrySet()) {
                        kvversionMsg += DiagnosticConstants.NEW_LINE_TAB  +
                                "\t" + entry.getKey() + " " +
                                entry.getValue().getNumericVersionString();
                    }

                    for (Map.Entry<RemoteFile, KVVersion> entry :
                        versionsAfterPreReq.entrySet()) {
                        kvversionMsg += DiagnosticConstants.NEW_LINE_TAB  +
                                "\t" + entry.getKey() + " " +
                                entry.getValue().getNumericVersionString();
                    }
                }

                /*
                 * Check the overlap ports of two or more SNAs which are
                 * hosted in a same machine
                 */
                Map<InetAddress, Map<Integer, Set<RemoteFile>>> portMap =
                    new HashMap<InetAddress, Map<Integer, Set<RemoteFile>>>();

                /* Get all ports and their associated remote files */
                for (Map.Entry<InetAddress, List<RemoteFile>> entry :
                                IPRemoteMap.entrySet()) {
                    portMap.put(entry.getKey(),
                                getAllUsedPorts(entry.getValue()));
                }

                /* Build the result of overlap ports check. */
                boolean isOverlap = false;
                String overLapDetail = new String();
                for (Map.Entry<InetAddress,
                        Map<Integer, Set<RemoteFile>>> entry :
                            portMap.entrySet()) {
                    for (Map.Entry<Integer, Set<RemoteFile>> subEntry :
                        entry.getValue().entrySet()) {
                        if (subEntry.getValue().size() > 1) {
                            isOverlap = true;
                            overLapDetail += DiagnosticConstants.NEW_LINE_TAB +
                                    "\t" + "Host: " +
                                    entry.getKey().getHostName() + ", port: " +
                                    subEntry.getKey() + " is used by";
                            for (RemoteFile rFile : subEntry.getValue()) {
                                overLapDetail +=
                                        DiagnosticConstants.NEW_LINE_TAB
                                        + "\t\t" + rFile.toString() + " ";
                            }
                            overLapDetail = overLapDetail.trim() + "\n";
                        }
                    }
                }

                if (isOverlap) {
                    overLapMsg += DiagnosticConstants.NEW_LINE_TAB +
                            "\tConflicts in port allocation:\n" +
                            DiagnosticConstants.NEW_LINE_TAB +
                            "\t" + overLapDetail;
                } else {
                    overLapMsg += DiagnosticConstants.NEW_LINE_TAB + "\t" +
                        "No conflicts in port ranges for SNs hosted on " +
                        "the same node.";
                }

                /* Delete temporary folder which save the configuration files */
                deleteEntireDirectory(new File("tmp"));

                message += DiagnosticConstants.NEW_LINE_TAB +
                        clockSkewMsg.trim() + "\n" +
                        DiagnosticConstants.NEW_LINE_TAB +
                        jdkversionMsg.trim() +  "\n" +
                        DiagnosticConstants.NEW_LINE_TAB +
                        networkMsg.trim() +  "\n" +
                        DiagnosticConstants.NEW_LINE_TAB +
                        securityMsg.trim() +  "\n" +
                        DiagnosticConstants.NEW_LINE_TAB +
                        kvversionMsg.trim() + "\n" +
                        DiagnosticConstants.NEW_LINE_TAB +
                        overLapMsg;

                message = PREFIX_MESSAGE + DiagnosticConstants.NEW_LINE_TAB +
                        message.trim();
                /* Notify complete a sub task */
                notifyCompleteSubTask(message);
            }

            /**
             * Get all used ports and associated configuration files which is
             * declared to use the ports.
             * @param rFileList
             * @return a map contains port and the remote files used the port.
             */
            private Map<Integer, Set<RemoteFile>>
                                getAllUsedPorts(List<RemoteFile> rFileList) {
                Map<Integer, Set<RemoteFile>> map =
                        new HashMap<Integer, Set<RemoteFile>>();

                /* Iterate all configuration files and extract all ports */
                for (RemoteFile rFile : rFileList) {
                    BootstrapParams bp =
                        ConfigUtils.getBootstrapParams(rFile.getLocalFile());
                    int registryPort = bp.getRegistryPort();
                    String HAPortRange = bp.getHAPortRange();
                    String servicePortRange = bp.getServicePortRange();
                    int trapPort = bp.getMgmtTrapPort();
                    int pollPort = bp.getMgmtPollingPort();

                    /* Add the port and associated configuration into map */
                    addPort(registryPort, rFile, map);
                    addPort(HAPortRange, rFile, map);

                    if (servicePortRange != null &&
                            !servicePortRange.isEmpty()) {
                        addPort(servicePortRange, rFile, map);
                    }

                    if (trapPort != 0) {
                        addPort(trapPort, rFile, map);
                    }

                    if (pollPort != 0) {
                        addPort(pollPort, rFile, map);
                    }
                }

                return map;
            }

            /**
             * Add a port and the associated configuration file which is
             * declared to use the port into a map.
             * @param port
             * @param rFile
             * @param map
             */
            private void addPort(int port,
                                  RemoteFile rFile,
                                  Map<Integer, Set<RemoteFile>> map) {
                Set<RemoteFile> rSet = map.get(port);
                if (rSet == null) {
                    rSet = new HashSet<RemoteFile>();
                    map.put(port, rSet);
                }
                rSet.add(rFile);
            }

            /**
             * Add a port range and the associated configuration file which is
             * declared to use the port into a map.
             * @param port
             * @param rFile
             * @param map
             */
            private void addPort(String rangePorts,
                                  RemoteFile rFile,
                                  Map<Integer, Set<RemoteFile>> map) {
                StringTokenizer tokenizer = new StringTokenizer(rangePorts,
                    ParameterUtils.HELPER_HOST_SEPARATOR);
                int firstHAPort = Integer.parseInt(tokenizer.nextToken());
                int secondHAPort = Integer.parseInt(tokenizer.nextToken());
                for (int i=firstHAPort; i<=secondHAPort; i++) {
                    addPort(i, rFile, map);
                }
            }

            /**
             * Get key store of SN
             * @param storeName
             * @param storePassword
             * @param storeFlavor
             * @param storeType
             * @return key store
             * @throws IllegalArgumentException
             */
            private KeyStore loadStore(String storeName,
                                       char[] storePassword,
                                       String storeFlavor,
                                       String storeType)
                throws IllegalArgumentException {

                if (storeType == null || storeType.isEmpty()) {
                    storeType = KeyStore.getDefaultType();
                }

                final KeyStore ks;
                try {
                    ks = KeyStore.getInstance(storeType);
                } catch (KeyStoreException kse) {
                    throw new IllegalArgumentException(
                        "Unable to find a " + storeFlavor +
                        " instance of type " + storeType, kse);
                }

                final FileInputStream fis;
                try {
                    fis = new FileInputStream(storeName);
                } catch (FileNotFoundException fnfe) {
                    throw new IllegalArgumentException(
                        "Unable to locate specified " + storeFlavor + " " +
                        storeName,
                        fnfe);
                }

                try {
                    ks.load(fis, storePassword);
                } catch (IOException ioe) {
                    throw new IllegalArgumentException(
                        "Error reading from " + storeFlavor + " file " +
                        storeName,
                        ioe);
                } catch (NoSuchAlgorithmException nsae) {
                    throw new IllegalArgumentException(
                        "Unable to check " + storeFlavor + " integrity: " +
                        storeName,
                        nsae);
                } catch (CertificateException ce) {
                    throw new IllegalArgumentException(
                        "Not all certificates could be loaded: " + storeName,
                        ce);
                } finally {
                    try {
                        fis.close();
                    } catch (IOException ioe) {
                        /* ignored */
                    }
                }
                return ks;
            }
        }
    }

    /**
     * Delete a whole specified file
     *
     * @param toDeletedFile specified file
     */
    public static void deleteEntireDirectory(File toDeletedFile) {
        /* return when the folder does not exist */
        if (!toDeletedFile.exists()) {
            return;
        }

        /* Delete the specified file when the file is a file */
        if (toDeletedFile.isFile()) {
            toDeletedFile.delete();
            return;
        }

        /* Iterate all files under the specified directory */
        File[] files = toDeletedFile.listFiles();
        for (File f : files) {
            /* Delete it when f is a file */
            if (f.isFile()) {
                f.delete();
            } else if (f.isDirectory()) {
                /* Recurse the f when it is a directory */
                deleteEntireDirectory(f);
            }
        }
        /* Delete empty directory */
        toDeletedFile.delete();
    }
}
