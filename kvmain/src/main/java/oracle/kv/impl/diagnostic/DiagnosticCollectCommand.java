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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

import oracle.kv.impl.diagnostic.LogFileInfo.LogFileType;
import oracle.kv.impl.diagnostic.LogInfo.LogInfoComparator;
import oracle.kv.impl.diagnostic.execution.MasterLogExtractor;
import oracle.kv.impl.diagnostic.execution.SecurityEventExtractor;
import oracle.kv.impl.diagnostic.ssh.SSHClient;
import oracle.kv.impl.diagnostic.util.TopologyDetector;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;

/**
 * This command shell implements the diagnostics collect command. The syntax
 * for the command is:
 *   diagnostics collect -logfiles
 */
public class DiagnosticCollectCommand extends CommandWithSubs {
    private static final String COMMAND_NAME = "collect";
    private static final String LOGFILE_COMMAND_NAME = "-logfiles";
    private static final Logger logger =
        LoggerUtils.getLogger(DiagnosticCollectCommand.class, "command");

    private static String hostname = null;
    private static int port = -1;
    private static String sshUser = null;
    private static String user = null;
    private static String securityFile = null;
    private static String savedir = null;
    private static String configdir = null;
    private static boolean isCompress = true;
    private static String saveLogDir = null;

    private static final
        List<? extends SubCommand> subs =
                       Arrays.asList(new DiagnosticCollectLogSub());

    public DiagnosticCollectCommand() {
        super(subs, COMMAND_NAME, 4, 0);
    }

    @Override
    protected String getCommandOverview() {
        return "The Collect command copies and packages NoSQL DB " +
                "informational log " + eol + "files to a central " +
                "location for troubleshooting. Log files can be " + eol +
                "compressed before copying. A sn-target-list file must exist " +
                "and can be" + eol + "created with diagnostics setup or " +
                "diagnostics collect -logfiles -host <name>" + eol +
                "-port <number>";
    }

    static class DiagnosticCollectLogSub extends SubCommand {
        final String LOG_FILE_NAME_PREFIX = "logs_";
        final String TIMESTAMP_FORMAT = "yyyy-MM-dd-HH.mm.ss";

        /*
         * Store the generate result directory which save log files from
         * remote machines
         */
        private List<File> resultDirList = new CopyOnWriteArrayList<File>();

        private List<LogSectionFileInfo> logFileInfoList =
                new CopyOnWriteArrayList<LogSectionFileInfo>();

        private List<String> securityEventFileList =
                new CopyOnWriteArrayList<String>();

        DiagnosticCollectLogSub() {
            super(LOGFILE_COMMAND_NAME, 8);
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_NAME + " " + LOGFILE_COMMAND_NAME + eolt +
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
                   "[" + DiagnosticConstants.CONFIG_DIRECTORY_FLAG +
                   " <location of storage node target file>]" + eolt +
                   "[" + DiagnosticConstants.SAVE_DIRECTORY_FLAG +
                   " <destination directory for log files>]" + eolt +
                   "[" + DiagnosticConstants.NO_COMPRESS_FLAG +
                   " <if specified, copy log files without compressing>]";
        }

        @Override
        protected String getCommandDescription() {
            return "Collect log files of all SNAs";
        }

        @Override
        public String execute(String[] args, Shell shell)
                throws ShellException {

            /* Initialize arguments */
            hostname = null;
            port = -1;
            sshUser = null;
            user = null;
            securityFile = null;
            savedir = null;
            configdir = null;
            isCompress = true;
            saveLogDir = null;

            Shell.checkHelp(args, this);
            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    final String arg = args[i];
                    if (DiagnosticConstants.HOST_FLAG.equals(arg)) {
                        hostname = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.PORT_FLAG.equals(arg)) {
                        String sPort = Shell.nextArg(args, i++, this);
                        port = parseInt(sPort);
                        if (port <= 0) {
                            String info = DiagnosticConstants.PORT_FLAG +
                                          " should be greater than 0";
                            throw new ShellUsageException(info, this);
                        }
                    } else if (DiagnosticConstants.SSH_USER_FLAG.equals(arg)) {
                        sshUser = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.USER_FLAG.equals(arg)) {
                        user = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.SECURITY_FLAG.equals(arg)) {
                        securityFile = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.SAVE_DIRECTORY_FLAG.
                            equals(arg)) {
                        savedir = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.CONFIG_DIRECTORY_FLAG.
                            equals(arg)) {
                        configdir = Shell.nextArg(args, i++, this);
                    } else if (DiagnosticConstants.NO_COMPRESS_FLAG.
                            equals(arg)) {
                        isCompress = false;
                    } else {
                        shell.unknownArgument(args[i], this);
                    }
                }
            }

            if (port < 0) {
                if (hostname != null && !hostname.isEmpty()) {
                    shell.requiredArg(DiagnosticConstants.PORT_FLAG, this);
                }
            } else {
                if (hostname == null || hostname.isEmpty()) {
                    shell.requiredArg(DiagnosticConstants.HOST_FLAG, this);
                }
            }

            if (configdir == null || configdir.isEmpty()) {
                configdir = DiagnosticConstants.DEFAULT_WORK_DIR;
            }

            /* Check whether the specified config directory exists */
            String retMsg = ParametersValidator.
                    checkDirectory(DiagnosticConstants.CONFIG_DIRECTORY_FLAG,
                                   configdir);
            if (retMsg != null) {
                throw new ShellUsageException(retMsg, this);
            }

            if (savedir == null || savedir.isEmpty()) {
                savedir = DiagnosticConstants.DEFAULT_WORK_DIR;
            } else {
                retMsg = ParametersValidator.
                        checkDirectory(DiagnosticConstants.SAVE_DIRECTORY_FLAG,
                        savedir);
                if (retMsg != null) {
                    throw new ShellUsageException(retMsg, this);
                }
            }

            /* Generate direct directory of saving log files */
            SimpleDateFormat format = new SimpleDateFormat(TIMESTAMP_FORMAT);
            String dateStr = format.format(new Date());
            saveLogDir = savedir + File.separator +
                         LOG_FILE_NAME_PREFIX + dateStr;

            collect(shell);

            return null;
        }

        /**
         * Set SSH user name for a specified SNA Info by existing SNAInfo
         * @param snaInfo the specified SNA Info which has no user name
         * @param list the list of history SNA Info
         */
        private void setUserByExistingSNAs(SNAInfo snaInfo,
        		List<SNAInfo> list) {
            for (SNAInfo si : list) {
                /* Assign the user when find the same IP in history list */
                if (si.getIP().equals(snaInfo.getIP())) {
                    snaInfo.setSSHUser(si.getSSHUser());
                }
            }
        }

        /**
         * Connect topology and use the latest status of topology to update
         * configuration file
         * @throws Exception
         */
        private void updateConfigurationFile(Shell shell) throws Exception {
            try {
                /*
                 * Connect topology and fetch the host name, port and kvroot of
                 * all SNAs, then write the info into configuration file.
                 */
                TopologyDetector detector = new TopologyDetector(hostname,
                                                                 port, user,
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
                        setUserByExistingSNAs(snaInfo, historyList);

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

        /**
         * Collect all log files, and generate admin master log file and
         * security event log file.
         *
         * @param shell
         * @throws ShellUsageException
         */
        private void collect(Shell shell) throws ShellUsageException {
            try {
                if (hostname != null) {
                    updateConfigurationFile(shell);
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

            /* Check whether configuration file is valid or not */
            try {
                DiagnosticConfigFile configFile =
                        new DiagnosticConfigFile(configdir);
                configFile.verify();
            } catch (Exception ex) {
                throw new ShellUsageException(ex.getMessage(), this);
            }

            try {
                /* Clear result directory list */
                resultDirList.clear();

                DiagnosticTaskManager diagnosticTaskManager =
                        new DiagnosticTaskManager(shell);

                DiagnosticSSHTask SSHtask =
                        new DiagnosticSSHTask(configdir, sshUser) {
                    @Override
                    public DiagnosticSSHRunnable getSSHRunnable
                        (SNAInfo snaInfo, SSHClient client,
                         List<SNAInfo> taskSNList) {
                        return new FileCopy(snaInfo, this, client);
                    }
                };
                DiagnosticTask masterTask = new AdminMasterLogMerge();
                DiagnosticTask securityTask = new SecurityEventGenerate();

                /* Add tasks into DiagnosticTaskManager */
                diagnosticTaskManager.addTask(SSHtask);
                diagnosticTaskManager.addTask(masterTask);
                diagnosticTaskManager.addTask(securityTask);

                /* Execute all tasks in DiagnosticTaskManager one by one */
                diagnosticTaskManager.execute();

            } catch (Exception ex) {
                throw new ShellUsageException(ex.toString(), this);
            }
        }

        /**
         * Merge master log sections extracted from all SNAs.
         */
        private class AdminMasterLogMerge extends DiagnosticTask {
            final String ADMIN_MASTER_LOG_FILE_NAME = "admin_master.log";
            final String PREFIX_MESSAGE = "\nAdmin Master Log\n\t";
            final String NO_MASTER_LOG = "No admin master log found";

            @Override
            public void doWork() throws Exception {
                try {
                    logFileInfoList.clear();
                    /* Generate log section file info for all SNs */
                    for (File file : resultDirList) {
                        MasterLogExtractor extractor =
                                new MasterLogExtractor(LogFileType.ADMIN,
                                                       file.getName());
                        extractor.execute(file.getAbsolutePath());
                        List<LogSectionFileInfo> list =
                                extractor.getLogSectionFileInfoList();

                        for (LogSectionFileInfo sectionList : list) {
                            logFileInfoList.add(sectionList);
                        }
                    }
                    if (logFileInfoList.size() > 0) {
                        mergeAdminMasterLog();
                    } else {
                        /* Notify complete a sub task */
                        notifyCompleteSubTask(PREFIX_MESSAGE + NO_MASTER_LOG);
                    }
                } catch (Exception ex) {
                    /* Notify complete a sub task */
                    notifyCompleteSubTask(PREFIX_MESSAGE + ex);
                }
            }

            /**
             * Merge all admin master log sections into a file
             *
             * @throws IOException
             */
            private void mergeAdminMasterLog() throws IOException {
                BufferedWriter bw = null;

                /* Create the destination file admin_master.log */
                File file = new File(saveLogDir + File.separator +
                                     ADMIN_MASTER_LOG_FILE_NAME);
                String message;
                try {
                    bw = new BufferedWriter(new FileWriter(file, true));
                    mergeLog(bw);
                    message = PREFIX_MESSAGE + file.getAbsolutePath();
                } catch (Exception e) {
                    message = PREFIX_MESSAGE + e;
                } finally {
                    if (bw != null)
                        bw.close();

                }
                /* Notify complete a sub task */
                notifyCompleteSubTask(message);
            }

            /**
             * Merge admin master log sections into a file.  All sources of
             * master log sections are in several queues. The master log
             * sections within a list are already sorted by time stamp. Their
             * data structure is as follows:
             * |-------------|
             * |   queue 1   |
             * |-------------|
             * |  section 1  |
             * |  section 2  |
             * |  section 3  |
             * |  ...        |
             * |  section n  |
             * |-------------|
             *
             * |-------------|
             * |   queue 2   |
             * |-------------|
             * |  section 1  |
             * |  section 2  |
             * |  section 3  |
             * |  ...        |
             * |  section n  |
             * |-------------|
             *
             * ...
             *
             * |-------------|
             * |   queue 3   |
             * |-------------|
             * |  section 1  |
             * |  section 2  |
             * |  section 3  |
             * |  ...        |
             * |  section n  |
             * |-------------|
             *
             *
             * The algorithm of merging is as follows:
             * 1. Scan The first section of all queues, choose the section if
             * its time stamp is earliest and pop the section from the queue
             * which contains the section
             *
             * 2. Write all log items of the chosen section in step #1 into
             * destination file.
             *
             * 3. Repeat step #1 and #2 until all queues become empty.
             *
             * @param bw the bufferedWriter of destination file
             * @throws IOException
             */
            private void mergeLog(BufferedWriter bw) throws IOException {
                /* Store LogSectionFileInfo when it has no section */
                List<LogSectionFileInfo> emptyFileInfoList =
                        new ArrayList<LogSectionFileInfo>();

                while (!logFileInfoList.isEmpty()) {
                    LogSectionFileInfo logSectionFileInfo = null;

                    /* Scan all queues and choose the earliest section */
                    Iterator<LogSectionFileInfo> iter =
                            logFileInfoList.iterator();
                    while (iter.hasNext()) {
                        LogSectionFileInfo fileInfo = iter.next();

                        /* Remove empty LogSectionFileInfo */
                        if (fileInfo.isEmpty()) {
                            emptyFileInfoList.add(fileInfo);
                            logFileInfoList.remove(fileInfo);
                            break;
                        }

                        if (logSectionFileInfo == null) {
                            logSectionFileInfo = fileInfo;
                            continue;
                        }

                        Date selectTimestamp = logSectionFileInfo.getFirst().
                                getTimestamp();
                        Date comparedTimestamp = fileInfo.getFirst().
                                getTimestamp();

                        if (selectTimestamp.after(comparedTimestamp)) {
                            logSectionFileInfo = fileInfo;
                        } else if (selectTimestamp.equals(comparedTimestamp)) {
                            fileInfo.pop();
                        }
                    }

                    if (logSectionFileInfo != null) {
                        /* Get begin time stamp of chosen section */
                        String beginTimestamp =
                                logSectionFileInfo.pop().getTimestampString();

                        /*
                         * Get end time stamp of chosen section. The end
                         * time stamp is the begin time stamp of next
                         * section.The last section has no next section,
                         * so its end time stamp is empty
                         */
                        String endTimestamp = DiagnosticConstants.EMPTY_STRING;
                        if (!logSectionFileInfo.isEmpty()) {
                            endTimestamp = logSectionFileInfo.getFirst().
                                    getTimestampString();
                        }

                        /*
                         * Add logSectionFileInfo info into
                         * emptyFileInfoList when all sections in it have
                         * been pop-up
                         */
                        if (logSectionFileInfo.isEmpty()) {
                            logFileInfoList.remove(logSectionFileInfo);
                            emptyFileInfoList.add(logSectionFileInfo);
                        }

                        /* Write log items of chosen section into file */
                        writeLog(bw, logSectionFileInfo.getFilePath(),
                                beginTimestamp, endTimestamp);
                    }
                }

                /* Delete the temporary file */
                for (LogSectionFileInfo fileInfo : emptyFileInfoList) {
                    File file = new File(fileInfo.getFilePath());
                    if (file.exists()) {
                        file.delete();
                    }
                }
            }

            /**
             * Write log items of specified section into destination file.
             *
             * @throws IOException
             */
            private void writeLog(BufferedWriter bw, String filePath,
                                  String beginTimestamp, String endTimestamp)
                    throws IOException {
                File file = new File(filePath);
                BufferedReader br = null;
                boolean isWrite = false;
                try {
                    /*
                     * Open file and read specified section using the time stamp
                     */
                    br = new BufferedReader(new FileReader(file));
                    String line;
                    while ((line = br.readLine()) != null) {
                        LogInfo logInfo = new LogInfo(line);
                        String timestampString = logInfo.getTimestampString();

                        /*
                         * Set isWrite as true and start to read the log of
                         * specified when the time stamp of current log is
                         * equal to begin time stamp
                         */
                        if (timestampString != null &&
                            timestampString.equals(beginTimestamp)) {
                            isWrite = true;
                        }

                        /*
                         * Stop reading the log when the time stamp of current
                         * log is equal to end time stamp
                         */
                        if (timestampString != null &&
                           timestampString.equals(endTimestamp)) {
                            break;
                        }

                        /* Write log item into a file when isWrite is true */
                        if (isWrite) {
                            bw.write(logInfo.toString());
                            bw.newLine();
                        }
                    }
                } finally {
                    if (br != null)
                        br.close();
                }
            }
        }

        /**
         * Generate security event log file. The algorithm is that adding all
         * security event log item into a list, sorting the log items in the
         * list by time stamp and then writing the sorted log items into file.
         */
        private class SecurityEventGenerate extends DiagnosticTask {
            private String SECURITY_EVENT_FILE = "security_event.log";
            private String PREFIX_MESSAGE = "\nSecurity Event Log\n\t";
            private String NO_SECURITY_MESSAGE = "No security event found";

            /*
             * It is assigned as true when log files have security events;
             * or it is assigned as false
             */
            private boolean hasSecurityEvent = true;

            @Override
            public void doWork() throws Exception {
                securityEventFileList.clear();

                /* Extract security events for all SNs one by one */
                for (File file : resultDirList) {
                    SecurityEventExtractor extractor =
                            new SecurityEventExtractor(file.getName());
                    extractor.execute(file.getAbsolutePath());
                    File resultFile = extractor.getResultFile();
                    if (resultFile != null) {
                        securityEventFileList.add(resultFile.getAbsolutePath());
                    }
                }

                /* return when no security event file generated */
                if (securityEventFileList.size() == 0) {
                    /* Notify complete a sub task */
                    notifyCompleteSubTask(PREFIX_MESSAGE +
                                          NO_SECURITY_MESSAGE);
                    return;
                }

                List<LogInfo> securityEventLogList = new ArrayList<LogInfo>();
                BufferedReader br = null;

                /* Add all security event into a list */
                for (String filePath : securityEventFileList) {
                    File f = new File(filePath);
                    try {
                        br = new BufferedReader(new FileReader(f));
                        String line;

                        while ((line = br.readLine()) != null) {
                            securityEventLogList.add(new LogInfo(line));
                        }

                    } finally {
                        if (br != null)
                            br.close();
                    }
                    /* Delete temporary file */
                    f.delete();
                }

                /*
                 * Empty security log file list after getting all security
                 * events from these files
                 */
                securityEventFileList.clear();

                /* Sort security event log items */
                Collections.sort(securityEventLogList, new LogInfoComparator());

                /* Write the sorted security log items into a file */
                BufferedWriter bw = null;
                File file = new File(saveLogDir + File.separator +
                                     SECURITY_EVENT_FILE);
                String message;
                try {
                    bw = new BufferedWriter(new FileWriter(file, true));
                    LogInfo currentLog = null;
                    if (securityEventLogList.isEmpty()) {
                        message = PREFIX_MESSAGE + NO_SECURITY_MESSAGE;

                        /*
                         * Empty security event log list indicates that there
                         * is no security event in all log files
                         */
                        hasSecurityEvent = false;
                    } else {
                        for (LogInfo log : securityEventLogList) {
                            if (currentLog == null ||
                                    !currentLog.equals(log)) {
                                currentLog = log;
                                bw.write(log.toString());
                                bw.newLine();
                            }
                        }
                        message = PREFIX_MESSAGE + file.getAbsolutePath();
                    }
                } catch (Exception e) {
                    message = PREFIX_MESSAGE + e;
                } finally {
                    if (bw != null) {
                        bw.close();
                    }
                }

                /* Delete security event file when no security event found */
                if (!hasSecurityEvent) {
                    file.delete();
                }

                /* Notify complete a sub task */
                notifyCompleteSubTask(message);
            }
        }

        /**
         * A subclass of DiagnosticSSHThread. It is to copy logs, master log
         * section files and security event log file from SNAs.
         */
        private class FileCopy extends DiagnosticSSHRunnable {
            final String PREFIX_MESSAGE = "\n\tLog files copied to ";

            public FileCopy(SNAInfo snaInfo, DiagnosticTask threadOwner,
                            SSHClient client) {
                super(snaInfo, threadOwner, client);
            }

            @Override
            public String doWork() throws Exception {
                String message = DiagnosticConstants.EMPTY_STRING;

                /* Check host is reachable or not */
                String retMsg = ParametersValidator.
                        checkHostname(DiagnosticConstants.HOST_FLAG,
                                      snaInfo.getHost());

                if (retMsg != null) {
                    message += (DiagnosticConstants.NEW_LINE_TAB + retMsg);
                    return message;
                }

                /* Check root directory exists or not */
                if (!client.checkFile(snaInfo.getRootdir())) {
                    message += (DiagnosticConstants.NEW_LINE_TAB +
                            DiagnosticConstants.NOT_FOUND_ROOT_MESSAGE +
                            snaInfo.getRootdir());
                }

                if (!message.equals(DiagnosticConstants.EMPTY_STRING)) {
                    return message.trim();
                }

                /* Get log files from SNA */
                File file = client.getLogFiles(snaInfo, saveLogDir, isCompress);
                resultDirList.add(file);
                message += (PREFIX_MESSAGE + saveLogDir);

                return message.trim();
            }
        }
    }
}
