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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.diagnostic.ssh.SSHClient;
import oracle.kv.impl.diagnostic.ssh.SSHClientManager;
import oracle.kv.impl.util.ThreadUtils.ThreadPoolExecutorAutoClose;
import oracle.kv.util.shell.ShellInputReader;

/**
 * A subclass of DiagnosticTask. It's used to execute a diagnostic task
 * remotely using SSH to invoke the desired command. The class breaks down a
 * single logical task into N subtasks, where N is the number of SNs in the
 * setup configuration file. This class only handles the dispatch and SSH
 * parts; the real work is done in the SSH thread.
 */
public abstract class DiagnosticSSHTask extends DiagnosticTask {
    private List<SNAInfo> snaInfoList;
    private Map<SNAInfo, SSHClient> clientMap =
            new ConcurrentHashMap<SNAInfo, SSHClient>();
    private DiagnosticConfigFile configFile;
    private final String sshUser;

    public DiagnosticSSHTask(String configdir, String sshUser)
            throws Exception {
        configFile = new DiagnosticConfigFile(configdir);
        this.snaInfoList = configFile.getAllSNAInfo();
        this.sshUser = sshUser;

        /*
         * The number of SNA Info is equal to the number of SSH runnable
         * tasks
         */
        setTotalSubTaskCount(snaInfoList.size());

        openClient();
    }

    /**
     * Get an implementation of DiagnosticSSHThread, the implementation does
     * the real work.
     *
     * @param snaInfo the info of a SNA
     * @param client the client used to connect remote hosts via SSH
     * @param taskSNList the list contains remote hosts information of all SNAs
     * @return an implementation of DiagnosticSSHThread
     */
    public abstract DiagnosticSSHRunnable getSSHRunnable
        (SNAInfo snaInfo, SSHClient client, List<SNAInfo> taskSNList);

    /**
     * Open clients for all SNAs
     * @throws Exception
     */
    private void openClient() throws Exception {
        boolean isRewrite = false;
        for (SNAInfo snaInfo : snaInfoList) {
            if (sshUser != null) {
                snaInfo.setSSHUser(sshUser);
                /*
                 * Configuration file need to be rewritten when new users are
                 * set
                 */
                isRewrite = true;
            } else if (snaInfo.getSSHUser() == null ||
                    snaInfo.getSSHUser().equals("")) {
                ShellInputReader inputReader = ShellInputReader.getReader(
                    System.in, System.out);
                String retrievedSSHUser =
                        inputReader.readLine("Enter user to ssh to " +
                        "[" + snaInfo.getSNAInfo() + "]: ");
                snaInfo.setSSHUser(retrievedSSHUser);
                /*
                 * Configuration file need to be rewritten when new users are
                 * set
                 */
                isRewrite = true;
            }
        }

        clientMap = SSHClientManager.getClient(snaInfoList);

        /* Write all SNA Info into configuration file */
        if (isRewrite) {
            final List<SNAInfo> rewrittenList = new ArrayList<SNAInfo>();

            for (Map.Entry<SNAInfo, SSHClient> entry : clientMap.entrySet()) {
                SNAInfo snaInfo = entry.getKey();
                SSHClient client = entry.getValue();

                /*
                 * Empty the user name when the client is not open, because it
                 * may be that user is invalid
                 */
                if (!client.isOpen()) {
                    snaInfo.setSSHUser(null);
                }
                rewrittenList.add(snaInfo);
            }

            configFile.rewrite(rewrittenList);
        }
    }

    @Override
    public final void doWork() throws Exception {
        /* Set a Diagnostic SSH Thread for each SN, to do the real work */
        List<Future<?>> list = new ArrayList<Future<?>>();

        int numberSSHThread = getTotalSubTaskCount();

        try (final ThreadPoolExecutorAutoClose threadExecutor =
                new ThreadPoolExecutorAutoClose(
                    numberSSHThread, numberSSHThread, 0L,
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>())) {

            /* Start SSH runnable tasks */
            for (SNAInfo snaInfo : snaInfoList) {
                SSHClient client = clientMap.get(snaInfo);
                /* Get an implementation of diagnostic SSH Thread */
                DiagnosticSSHRunnable SSHRunnable = getSSHRunnable(snaInfo,
                                                                   client,
                                                                   snaInfoList);
                list.add(threadExecutor.submit(SSHRunnable));
            }

            /* Wait all DiagnosticSSHThreads finish */
            for (Future<?> fs:list) {
                fs.get();
            }
        }
    }
}
