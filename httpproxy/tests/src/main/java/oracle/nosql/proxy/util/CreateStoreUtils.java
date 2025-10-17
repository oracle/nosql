/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.proxy.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeAgentImpl;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 * Utility methods for testing related to Storage Nodes
 */
public class CreateStoreUtils {

    private static final Logger logger =
        LoggerUtils.getLogger(CreateStoreUtils.class, "test");

    public static StorageNodeAgent
        createUnregisteredSNA(String rootDir,
                              PortFinder portFinder,
                              int capacity,
                              String configFileName,
                              boolean useThreads,
                              boolean createAdmin,
                              int memoryMB,
                              Set<Parameter> extraParams)
        throws Exception {

        final String SEC_POLICY_STRING =
        "grant {\n permission java.security.AllPermission;\n};";

        /* generate bootstrap dir */
        File configFile = new File(rootDir + File.separator + configFileName);

        BootstrapParams bp =
            new BootstrapParams(rootDir,
                                portFinder.getHostname(),
                                portFinder.getHostname(),
                                portFinder.getHaRange(),
                                null, /* servicePortRange */
                                null, /* storeName */
                                portFinder.getRegistryPort(),
                                -1,
                                capacity,
                                null, /* storageType */
                                null, // sec dir
                                true, // runBootAdmin
                                null);
        if (memoryMB != 0) {
            bp.setMemoryMB(memoryMB*capacity);
        }
        ConfigUtils.createBootstrapConfig(bp, configFile.toString());

        generateSecurityPolicyFile(rootDir);

        StorageNodeAgent sna =
            startSNA(rootDir, configFileName, useThreads, createAdmin);
        return sna;
    }

    /**
     * Start an instance of SNA assuming the bootstrap directory and file have
     * been created, and specifying whether to disable services.
     */
    public static StorageNodeAgent startSNA(String bootstrapDir,
                                            String bootstrapFile,
                                            boolean useThreads,
                                            boolean createAdmin)
        throws Exception {

        final List<String> snaArgs = new ArrayList<String>();
        snaArgs.add(CommandParser.ROOT_FLAG);
        snaArgs.add(bootstrapDir);
        snaArgs.add(StorageNodeAgent.CONFIG_FLAG);
        snaArgs.add(bootstrapFile);
        if (useThreads) {
            snaArgs.add(StorageNodeAgent.THREADS_FLAG);
        }

        StorageNodeAgentImpl sna = new StorageNodeAgentImpl(createAdmin);
        sna.parseArgs(snaArgs.toArray(new String[snaArgs.size()]));
        /*
         * In testing environments we sometimes run into this exception
         * java.rmi.NoSuchObjectException: no such object in table.
         * cf. [#22835]
         * It appears that retrying several times works around the bug.
         */
        int nretries = 0;
        boolean started = false;
        while (!started) {
            try {
                sna.start();
                started = true;
            } catch (IOException e) {
                if (nretries++ > 100 ||
                    !(e instanceof NoSuchObjectException ||
                      e.getCause() instanceof NoSuchObjectException)) {

                    throw e;
                }
            }
        }

        return sna.getStorageNodeAgent();
    }


    public static void deleteDirs(File f) {
        if (f.isDirectory()) {
            for (File c : f.listFiles()) {
                deleteDirs(c);
            }
        }
        f.delete();
    }

    public static void cleanStoreDir(String testDir, String kvstorename)
        throws Exception {

        File storeDir = new File(testDir + File.separator + kvstorename);
        deleteDirs(storeDir);
    }

    /**
     * Get and wait for a RepNodeAdmin handle to reach one of the states in
     * the ServiceStatus array parameter.
     */
    @SuppressWarnings("null")
    public static RepNodeAdminAPI
        waitForRepNodeAdmin(String storeName,
                            String hostName,
                            int port,
                            RepNodeId rnid,
                            StorageNodeId snid,
                            long timeoutSec,
                            ServiceStatus targetStatus,
                            Logger logger)
    throws RemoteException, NotBoundException {

        RemoteException remoteException = null;
        NotBoundException notBoundException = null;
        RepNodeAdminAPI rnai = null;
        ServiceStatus status = null;

        long limitMs = System.currentTimeMillis() + 1000 * timeoutSec;

        while (System.currentTimeMillis() <= limitMs) {

            /**
             * The stub may be stale, get it again on exception.
             */
            if (remoteException != null || notBoundException != null) {
                rnai = null;
            }
            try {
                if (rnai == null) {
                    rnai = RegistryUtils.getRepNodeAdmin(
                        storeName, hostName, port, rnid, null, logger);
                }
                status = rnai.ping().getServiceStatus();
                if (status == targetStatus) {
                    return rnai;
                }
                remoteException = null;
                notBoundException = null;
            } catch (RemoteException e) {
                remoteException = e;
            } catch (NotBoundException e) {
                notBoundException = e;
            }

            /*
             * Check now for any process startup problems before
             * sleeping.
             */
            if (snid != null) {
                RegistryUtils.checkForStartupProblem(storeName,
                                                     hostName,
                                                     port,
                                                     rnid,
                                                     snid,
                                                     null,
                                                     logger);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                throw new IllegalStateException("unexpected interrupt");
            }
        }

        if (status != null) {
            throw new IllegalStateException
                ("RN current status: " + status + " target status: " +
                 targetStatus);
        }
        if (remoteException != null) {
            throw remoteException;
        }
        throw notBoundException;
    }
    /**
     * Get and wait for a CommandService handle to reach the requested status.
     * Treat UNREACHABLE as "any" and return once the handle is acquired.
     */
    @SuppressWarnings("null")
    public static CommandServiceAPI waitForAdmin(String hostname,
                                                 int registryPort,
                                                 long timeoutSec,
                                                 Logger logger)
        throws RemoteException, NotBoundException {

        ServiceStatus targetStatus = ServiceStatus.RUNNING;

        RemoteException remoteException = null;
        NotBoundException notBoundException = null;
        CommandServiceAPI admin = null;
        ServiceStatus status = null;

        long limitMs = System.currentTimeMillis() + 1000 * timeoutSec;

        while (System.currentTimeMillis() <= limitMs) {

            /**
             * The stub may be stale, get it again on exception.
             */
            if (notBoundException != null || remoteException != null) {
                admin = null;
            }
            try {
                if (admin == null) {
                    admin = RegistryUtils.getAdmin(hostname, registryPort,
                                                   null, logger);
                }

                status = admin.ping() ;

                /**
                 * Treat UNREACHABLE as "any".
                 */
                if (targetStatus == ServiceStatus.UNREACHABLE) {
                    return admin;
                }
                if (status == targetStatus) {
                    return admin;
                }
                remoteException = null;
                notBoundException = null;
            } catch (RemoteException e) {
                remoteException = e;
            } catch (NotBoundException e) {
                notBoundException = e;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                throw new IllegalStateException("unexpected interrupt");
            }
        }

        if (status != null) {
            throw new IllegalStateException("Admin status: " + status +
                                            " Target status: " + targetStatus);
        }
        if (remoteException != null) {
            throw remoteException;
        }
        throw notBoundException;
    }

    protected static void delay(int seconds)
        throws Exception {
        Thread.sleep(seconds*1000);
    }


    private static void generateSecurityPolicyFile(String rootDir) {
        final String SEC_POLICY_STRING =
            "grant {\n permission java.security.AllPermission;\n};";

        File dest = new File
            (rootDir + File.separator + FileNames.JAVA_SECURITY_POLICY_FILE);
        if (!dest.exists()) {
            FileOutputStream output = null;
            try {

                dest.createNewFile();
                output = new FileOutputStream(dest);
                output.write(SEC_POLICY_STRING.getBytes());
            } catch (FileNotFoundException fnf) {
            } catch (IOException ie) {
            } finally {
                if (output != null) {
                    try {
                        output.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }
    }
}
