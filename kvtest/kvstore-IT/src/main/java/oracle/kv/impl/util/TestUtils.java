/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.nio.channels.FileChannel;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.filestore.FileStoreManager;
import oracle.kv.impl.security.login.AdminLoginManager;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.util.StorageNodeUtils.KerberosOpts;
import oracle.kv.impl.util.StorageNodeUtils.SecureOpts;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.ClearServerSocketFactory;
import oracle.kv.impl.util.registry.ClearClientSocketFactory;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.ServerSocketFactory;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * A collection of RepNode-centric utilities for kvstore unit tests.
 */
public class TestUtils {

    /* Common system properties for running tests */
    public final static String DEST_DIR = "testdestdir";
    public final static String REGISTRY_PORT ="registryPort";
    public static String FAILURE_DIR = "failurecopydir";
    public static String DEFAULT_FAIL_DIR = "target/failures";
    public final static String SEC_POLICY_STRING =
        "grant {\n permission java.security.AllPermission;\n};";
    public final static String TEST_DEBUG = "test.debug";
    public static String COPY_LIMIT = "copylimit";
    public final static String KRB_TEST_DIR = "testkrbdir";
    public final static String KDC_TEST_DIR = "testkdcdir";
    public final static String NO_SSL_SYS_PRO = "test.nossl";

    /**
     * A PrintStream that can be used to consume output silently.
     */
    public static final PrintStream NULL_PRINTSTREAM =
       new PrintStream(new OutputStream() {
           @Override
           public void write(int b) { }
       });

    /**
     * A dummy registry PORT number used for initialization only. It is never
     * actually used to start a registry. To get one or more real free ports
     * use the class FreePortLocator
     */
    public final static short DUMMY_REGISTRY_PORT = Short.MIN_VALUE;

    /**
     * The operating system command to use on the current platform to print out
     * all open sockets.
     */
    public static final String[] PRINT_OPEN_SOCKETS_CMD =
        System.getProperty("os.name", "unknown").equals("Mac OS X") ?

        /* Mac OS X */
        new String[] { "lsof",
                       /* List open TCP "files" (really sockets) */
                       "-iTCP",
                       /*
                        * Print ports as numbers, not names, since the names
                        * don't really mean anything to us.
                        */
                       "-P" } :

        /* Linux */
        new String[] { "netstat",
                       /*
                        * -a: Both client and server sockets
                        * -n: Use IP addresses, not hostnames
                        * -t: Terse output
                        * -p: Show program PIDs
                        */
                       "-antp" };

    /** A default client socket factory. */
    public static final ClientSocketFactory DEFAULT_CSF =
        new ClearClientSocketFactory("test", 0, 0, null);

    /** A default server socket factory. */
    public static final ServerSocketFactory DEFAULT_SSF =
        new ClearServerSocketFactory(0, 0, 0, 0);

    private static final Logger logger =
        ClientLoggerUtils.getLogger(TestUtils.class, "Test");

    /** A default thread pool */
    public static final Executor DEFAULT_THREAD_POOL =
        Executors.newCachedThreadPool(new KVThreadFactory("-test", logger));

    public static File getTestDir() {
        String dir = System.getProperty(DEST_DIR);
        if (dir == null || dir.length() == 0) {
            throw new IllegalArgumentException
                ("System property must be set to test data directory: " +
                 DEST_DIR);
        }

        return new File(dir).getAbsoluteFile();
    }

    public static File getKrbTestDir() {
        final String dir = System.getProperty(KRB_TEST_DIR);
        if (dir == null || dir.length() == 0) {
            throw new IllegalArgumentException
                ("System property must be set to test data directory: " +
                 KRB_TEST_DIR);
        }

        return new File(dir);
    }

    public static File getTestKdcDir() {
        final String dir = System.getProperty(KDC_TEST_DIR);
        if (dir == null || dir.length() == 0) {
            throw new IllegalArgumentException
                ("System property must be set to test data directory: " +
                 KDC_TEST_DIR);
        }

        return new File(dir);
    }

    public static boolean isSSLDisabled() {
        return Boolean.getBoolean(NO_SSL_SYS_PRO);
    }

    /**
     * Allow to set up self defined directory store failure copy.
     */
    public static File getFailureCopyDir() {
        String dir = System.getProperty(FAILURE_DIR, DEFAULT_FAIL_DIR);
        File file = new File(dir);
        if (!file.isDirectory()) {
            file.mkdir();
        }

        return file;
    }

    /**
     * If test failed, copy its environment to other location. The default
     * limit is 10, but our test support the value via system property.
     */
    public static int getCopyLimit() {
        String limit = System.getProperty(COPY_LIMIT, "10");

        return Integer.parseInt(limit);
    }

    /**
     * Check to see it th test.debug property is set to a non-null, non-empty
     * value.  Tests may use this to enable verbosity and other features.
     */
    public static boolean testDebugEnabled() {
        String dbg = System.getProperty(TEST_DEBUG);
        return (!(dbg == null || dbg.length() == 0));
    }

    public static void generateBootstrapFile(String pathToFile,
                                             String rootDir,
                                             String hostname,
                                             int port,
                                             String haPortRange,
                                             String haHostname,
                                             boolean runBootstrapAdmin,
                                             int capacity,
                                             List<String> mountDirs)
    throws Exception {
        generateBootstrapFile(pathToFile,
                              rootDir,
                              hostname,
                              port,
                              haPortRange,
                              haHostname,
                              runBootstrapAdmin,
                              capacity,
                              mountDirs,
                              null,
                              null);
    }

    public static void generateBootstrapFile(String pathToFile,
                                             String rootDir,
                                             String hostname,
                                             int port,
                                             String haPortRange,
                                             String haHostname,
                                             boolean runBootstrapAdmin,
                                             int capacity,
                                             List<String> mountDirs,
                                             String adminDir,
                                             String adminDirSize)
    throws Exception {

        BootstrapParams bp =
            new BootstrapParams(rootDir, hostname, haHostname, haPortRange,
                                null /*servicePortRange*/, null /*storeName*/,
                                port, -1, capacity, null /*storageType*/,
                                null /*securityDir*/, runBootstrapAdmin, null);
        bp.setStorgeDirs(mountDirs, null);
        if (adminDir != null) {
            bp.setAdminDir(adminDir, adminDirSize);
        }
        ConfigUtils.createBootstrapConfig(bp, pathToFile);
    }

    /**
     * Overloading of generateBootstrapFile allows configuring mgmt and
     * security.
     */
    public static void generateBootstrapFile(String pathToFile,
                                             String rootDir,
                                             String hostname,
                                             int port,
                                             String haPortRange,
                                             String haHostname,
                                             int capacity,
                                             int memoryMB,
                                             List<String> mountDirs,
                                             String mgmtImpl,
                                             int mgmtPollPort,
                                             String mgmtTrapHost,
                                             int mgmtTrapPort,
                                             File securityDir,
                                             boolean runBootstrapAdmin)
    throws Exception {
        generateBootstrapFile(pathToFile,
                              rootDir,
                              hostname,
                              port,
                              haPortRange,
                              haHostname,
                              capacity,
                              memoryMB,
                              mountDirs,
                              mgmtImpl,
                              mgmtPollPort,
                              mgmtTrapHost,
                              mgmtTrapPort,
                              securityDir,
                              runBootstrapAdmin,
                              null /* userExternalAuth */);
    }

    /**
     * Overloading of generateBootstrapFile allows configuring mgmt and
     * security.
     */
    public static void generateBootstrapFile(String pathToFile,
                                             String rootDir,
                                             String hostname,
                                             int port,
                                             String haPortRange,
                                             String haHostname,
                                             int capacity,
                                             int memoryMB,
                                             List<String> mountDirs,
                                             String mgmtImpl,
                                             int mgmtPollPort,
                                             String mgmtTrapHost,
                                             int mgmtTrapPort,
                                             File securityDir,
                                             boolean runBootstrapAdmin,
                                             String userExternalAuth)
    throws Exception {
        generateBootstrapFile(pathToFile,
                              rootDir,
                              hostname,
                              port,
                              haPortRange,
                              haHostname,
                              capacity,
                              memoryMB,
                              mountDirs,
                              mgmtImpl,
                              mgmtPollPort,
                              mgmtTrapHost,
                              mgmtTrapPort,
                              securityDir,
                              runBootstrapAdmin,
                              userExternalAuth,
                              null);
    }

    public static void generateBootstrapFile(String pathToFile,
                                             String rootDir,
                                             String hostname,
                                             int port,
                                             String haPortRange,
                                             String haHostname,
                                             int capacity,
                                             int memoryMB,
                                             List<String> mountDirs,
                                             String mgmtImpl,
                                             int mgmtPollPort,
                                             String mgmtTrapHost,
                                             int mgmtTrapPort,
                                             File securityDir,
                                             boolean runBootstrapAdmin,
                                             String userExternalAuth,
                                             Set<Parameter> extraParams)
    throws Exception {

        BootstrapParams bp = new BootstrapParams(
            rootDir, hostname, haHostname, haPortRange,
            null /*servicePortRange*/, null /*storeName*/, port, -1,
            capacity, null /*storageType*/,
            (securityDir != null) ? securityDir.toString() : null,
            runBootstrapAdmin/*hostingAdmin*/, mgmtImpl);
        if (memoryMB != 0) {
            memoryMB = bumpMemoryMB(memoryMB, capacity);
            bp.setMemoryMB(memoryMB);
        }
        bp.setStorgeDirs(mountDirs, null);

        if (mgmtImpl != null) {
            bp.setMgmtPollingPort(mgmtPollPort);
            bp.setMgmtTrapHost(mgmtTrapHost);
            bp.setMgmtTrapPort(mgmtTrapPort);
        }

        if (userExternalAuth != null) {
            bp.setUserExternalAuth(userExternalAuth);
        }

        if (extraParams != null) {
            extraParams.forEach((p) -> bp.getMap().put(p));
        }

        ConfigUtils.createBootstrapConfig(bp, pathToFile);
    }

    /**
     * Return memoryMB, increasing it as needed to account for the minimum
     * 1 GB heap size on the Zing JVM.
     */
    private static int bumpMemoryMB(int memoryMB, int capacity) {
        if (!JVMSystemUtils.ZING_JVM) {
            return memoryMB;
        }

        /*
         * Use (capacity + 2) to account for SN and one Admin.
         * Note that MIN_HEAP_MB is 1024 since we're running Zing.
         */
        final int minHeapMem = (JVMSystemUtils.MIN_HEAP_MB * (capacity + 2));

        /* Assume the default SN_RN_HEAP_PERCENT is used in tests. */
        final int heapPct =
            Integer.parseInt(ParameterState.SN_RN_HEAP_PERCENT_DEFAULT);

        /*
         * Heap memory is determined from memoryMB by KVS as follows:
         *   heapMemory = (memoryMB * heapPct) / 100
         * Invert that calculation here to get minMemoryMB from minHeapMem.
         * Add (heapPct - 1) to account for integer truncation.
         */
        final int minMemoryMB = ((minHeapMem * 100) + heapPct - 1) / heapPct;

        return Math.max(memoryMB, minMemoryMB);
    }

    public static void generateSecurityPolicyFile(String pathToFile,
                                                  String content) {

        File dest = new File(pathToFile);
        if (!dest.exists()) {
            FileOutputStream output = null;
            try {

                dest.createNewFile();
                output = new FileOutputStream(dest);
                output.write(content.getBytes());
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

    public static void generateSecurityDir(File securityDir)
        throws Exception {

        generateSecurityDir(securityDir, new SecureOpts(), null);
    }

    public static void generateSecurityDir(File securityDir,
                                           SecureOpts secOpts,
                                           KerberosOpts krbOpts)
        throws Exception {

        securityDir.mkdir();
        if (secOpts.noSSL()) {
            generateSecurityFileWithoutSSL(securityDir, krbOpts);
        } else {
            generateSecurityFile(securityDir, krbOpts);
        }

        final File testSSLDir = SSLTestUtils.getTestSSLDir();

        if (!secOpts.noSSL()) {
            /* Copy the keystore file */
            copyFile(secOpts.getSrcKeyStore(), testSSLDir,
                     SSLTestUtils.SSL_KS_NAME, securityDir);
            /* Copy the truststore file */
            copyFile(secOpts.getSrcTrustStore(), testSSLDir,
                     SSLTestUtils.SSL_TS_NAME, securityDir);
            /* Copy the keystore password file */
            copyFile(secOpts.getPasswordFile(), testSSLDir,
                     SSLTestUtils.SSL_PW_NAME, securityDir);
            /* Copy the client truststore file */
            copyFile(secOpts.getClientTrust(), testSSLDir,
                     SSLTestUtils.SSL_CTS_NAME, securityDir);
        }

        if (krbOpts != null && krbOpts.getKeytabFile() != null) {
            copyFile(KerberosOpts.KEYTAB_NAME_DEFAULT,
                     krbOpts.getKeytabFile().getParentFile(),
                     securityDir);
        }
    }

    public static void copyFile(String filename, File sourceDir, File destDir)
        throws IOException {

        copyFile(filename, sourceDir, filename, destDir);
    }

    public static void copyFile(String srcFileName,
                                File sourceDir,
                                String destFileName,
                                File destDir)
        throws IOException {

        copyFile(new File(sourceDir.getPath(), srcFileName),
                 new File(destDir.getPath(), destFileName));
    }

    /**
     * Copy a file.  This code is copied from FileUtils so that it is available
     * for testing even though FileUtils is not included in kvclient.jar.
     */
    public static void copyFile(File sourceFile, File destFile)
        throws IOException {

        if (!destFile.exists()) {
            destFile.createNewFile();
        }
        final FileInputStream source = new FileInputStream(sourceFile);
        try {
            final FileOutputStream dest = new FileOutputStream(destFile);
            try {
                final FileChannel sourceChannel = source.getChannel();
                dest.getChannel().transferFrom(sourceChannel, 0,
                                               sourceChannel.size());
            } finally {
                try {
                    dest.close();
                } catch (IOException e) { }
            }
        } finally {
            try {
                source.close();
            } catch (IOException e) { }
        }
    }

    public static void generateSecurityFile(File securityDir,
                                            KerberosOpts krbOpts)
        throws Exception {

        final SecurityParams sp = new SecurityParams();
        sp.setSecurityEnabled(true);
        sp.setKeystoreType("PKCS12");
        sp.setKeystoreFile(SSLTestUtils.SSL_KS_NAME);
        sp.setKeystoreSigPrivateKeyAlias(SSLTestUtils.SSL_KS_ALIAS_DEF);
        sp.setTruststoreType("PKCS12");
        sp.setTruststoreFile(SSLTestUtils.SSL_TS_NAME);
        sp.setTruststoreSigPublicKeyAlias(SSLTestUtils.SSL_TS_ALIAS_DEF);
        sp.setPasswordFile(SSLTestUtils.SSL_PW_NAME);
        sp.setInternalAuth("ssl");

        sp.addTransportMap("client");
        sp.setTransFactory("client", SSLTestUtils.SSL_TRANSPORT_FACTORY);
        sp.setTransServerKeyAlias("client", SSLTestUtils.SSL_KS_ALIAS_DEF);

        sp.addTransportMap("internal");
        sp.setTransFactory("internal", SSLTestUtils.SSL_TRANSPORT_FACTORY);
        sp.setTransServerKeyAlias("internal", SSLTestUtils.SSL_KS_ALIAS_DEF);
        sp.setTransClientKeyAlias("internal", SSLTestUtils.SSL_KS_ALIAS_DEF);
        sp.setTransServerIdentityAllowed("internal", "dnmatch(CN=Unit Test)");
        sp.setTransClientAuthRequired("internal", true);
        sp.setTransClientIdentityAllowed("internal", "dnmatch(CN=Unit Test)");

        sp.addTransportMap("ha");
        sp.setTransFactory("ha", SSLTestUtils.SSL_TRANSPORT_FACTORY);
        sp.setTransServerKeyAlias("ha", SSLTestUtils.SSL_KS_ALIAS_DEF);
        sp.setTransClientKeyAlias("ha", SSLTestUtils.SSL_KS_ALIAS_DEF);
        sp.setTransServerIdentityAllowed("ha", "dnmatch(CN=Unit Test)");
        sp.setTransClientAuthRequired("ha", true);
        sp.setTransClientIdentityAllowed("ha", "dnmatch(CN=Unit Test)");

        if (krbOpts != null) {
            sp.setKerberosConfFile(krbOpts.getKrbConf());
            sp.setKerberosKeytabFile(krbOpts.getKeytab());
            sp.setKerberosRealmName(krbOpts.getRealm());
            sp.setKerberosServiceName(krbOpts.getServiceName());
            sp.setKerberosInstanceName(krbOpts.getInstanceName());
        }
        final File securityConfig =
            new File(securityDir, FileNames.SECURITY_CONFIG_FILE);
        ConfigUtils.createSecurityConfig(sp, securityConfig);
    }

    public static void generateSecurityFileWithoutSSL(File securityDir,
                                                      KerberosOpts krbOpts)
        throws Exception {

        final SecurityParams sp = new SecurityParams();
        sp.setSecurityEnabled(true);
        sp.addTransportMap("client");
        sp.setTransType("client", "clear");
        sp.setTransFactory("client", SSLTestUtils.CLEAR_TRANSPORT_FACTORY);

        sp.addTransportMap("internal");
        sp.setTransType("internal", "clear");
        sp.setTransFactory("internal", SSLTestUtils.CLEAR_TRANSPORT_FACTORY);

        sp.addTransportMap("ha");
        sp.setTransType("ha", "clear");
        sp.setTransFactory("ha", SSLTestUtils.CLEAR_TRANSPORT_FACTORY);

        if (krbOpts != null) {
            sp.setKerberosConfFile(krbOpts.getKrbConf());
            sp.setKerberosKeytabFile(krbOpts.getKeytab());
            sp.setKerberosRealmName(krbOpts.getRealm());
            sp.setKerberosServiceName(krbOpts.getServiceName());
            sp.setKerberosInstanceName(krbOpts.getInstanceName());
        }
        final File securityConfig =
            new File(securityDir, FileNames.SECURITY_CONFIG_FILE);
        ConfigUtils.createSecurityConfig(sp, securityConfig);
    }

    /**
     * Create a Filestore password store containing the provided username/
     * password combination.
     */
    public static void makePasswordFile(File passwordFile,
                                        String username,
                                        String password)
        throws Exception {

        final PasswordManager pwdMgr = new FileStoreManager();
        final PasswordStore pwdStore = pwdMgr.getStoreHandle(passwordFile);
        pwdStore.create(null);
        pwdStore.setSecret(username, password.toCharArray());
        pwdStore.save();
        pwdStore.discard();
    }

    /**
     * Creates a registry on the specified port.
     *
     * @see #destroyRegistry
     */
    public static Registry createRegistry(int registryPort)
        throws RemoteException {

        ExportException throwEE = null ;
        /* A little over 2 min (the CLOSE_WAIT timeout.) */
        final int limitMs = 128000;
        final int retryPeriodMs = 1000;

        for (int totalWaitMs = 0; totalWaitMs <= limitMs;
             totalWaitMs += retryPeriodMs) {
            try {
                throwEE = null;
                return LocateRegistry.createRegistry(
                    registryPort, null, DEFAULT_SSF);
            } catch (ExportException ee) {
                throwEE = ee;
                if (ee.getCause() instanceof IOException) {
                    try {
                        Thread.sleep(retryPeriodMs);
                    } catch (InterruptedException e) {
                        throw throwEE;
                    }
                    continue;
                }
                throw throwEE;
            }
        }

        reportCreateRegistrySocketFailure();

        if (throwEE != null) {
            /* Should never be null -- suppress compiler warning. */
            throw throwEE;
        }

        assert(false);

        return null;
    }

    private static void reportCreateRegistrySocketFailure() {

        /**
         * This is not supposed to happen, it indicates that conflicting tests
         * are being run in parallel, or that a preceding test did not
         * shut down cleanly and did not close socket connections.
         *
         * So simply dump debug information to stderr.
         */
        System.err.println(">>> Bind exception despite retries. " +
                            new Date() + "\n");
        /*
         * Print out active java processes in case it's an inter-process
         * bind conflict.
         */
        System.err.println(RepUtils.exec("jps", "-v"));

        /**
         * Print out all sockets.
         */
        System.err.println(RepUtils.exec(PRINT_OPEN_SOCKETS_CMD));

        /**
         * Dump all threads in case it's an intra-thread bind conflict.
         */
        dumpThreads(System.err);
    }

    /**
     * Dump all active threads for debugging purposes.
     */
    public static void dumpThreads(PrintStream s) {

        s.println("[Dump --Dumping stack traces for all threads]");

        final Map<Thread, StackTraceElement[]> stackTraces =
            Thread.getAllStackTraces();

        for (Map.Entry<Thread, StackTraceElement[]> stme :
             stackTraces.entrySet()) {

            s.println(stme.getKey().toString());
            for (StackTraceElement ste : stme.getValue()) {
                s.println("     " + ste);
            }
        }

        s.println("[Dump --Thread dump completed]");
    }

    /**
     * Clears and unexports the specified registry.
     */
    public static void destroyRegistry(Registry registry) {
         try {
             for (String name : registry.list()) {
                 try {
                    registry.unbind(name);
                } catch (NotBoundException e) {
                    /* Ignore, it's just cleanup. */
                }
             }
         }  catch (RemoteException rel) {
            /* Ignore, it's just cleanup. */
         }
         try {
             UnicastRemoteObject.unexportObject(registry, true);

             /*
              * Wait for a very brief time after unexporting the registry to
              * allow the system to close the server socket before we
              * potentially attempt to use it again.  Without this change, some
              * tests were failing occasionally on Linux with BindExceptions.
              */
             Thread.sleep(1);
         } catch (NoSuchObjectException e) {
             /* Ignore, it's just cleanup. */
         } catch (InterruptedException e) {
             /* Ignore, it's just cleanup. */
         }
    }

    /**
     * Creates an async service registry on the specified port.
     */
    public static ListenHandle createServiceRegistry(int registryPort)
        throws IOException {

        IOException throwIOE = null;
        /* A little over 2 min (the CLOSE_WAIT timeout.) */
        final int limitMs = 128000;
        final int retryPeriodMs = 1000;

        for (int totalWaitMs = 0; totalWaitMs <= limitMs;
             totalWaitMs += retryPeriodMs) {
            try {
                return AsyncRegistryUtils.createRegistry(
                    "localhost", registryPort,
                    new ClearServerSocketFactory(0, 0, 0, 0), logger);
            } catch (IOException e) {
                throwIOE = e;
                try {
                    Thread.sleep(retryPeriodMs);
                } catch (InterruptedException e2) {
                    throw throwIOE;
                }
            }
        }

        reportCreateRegistrySocketFailure();

        throw (throwIOE != null) ?
            throwIOE :
            new IOException("Couldn't create service registry");
    }

    /**
     * Remove all files and directories from the test instance directory.
     */
    public static void clearTestDirectory() {
        LoggerUtils.closeAllHandlers();
        File testDir = getTestDir();
        if (!testDir.exists()) {
            return;
        }
        clearDirectory(getTestDir());
    }

    /* Clears out the contents of the directory, recursively */
    private static void clearDirectory(File dir) {
        if (dir.listFiles() == null) {
            return;
        }
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                clearDirectory(file);
            }
            boolean deleteDone = file.delete();
            assert deleteDone: "Couldn't delete " + file;
        }
    }

    /**
     * Forcefully unexports the specified remote object, ignoring if the object
     * is not found.
     */
    public static void safeUnexport(Remote object) {
        try {
            UnicastRemoteObject.unexportObject(object, true);
        } catch (NoSuchObjectException e) {
        }
    }

    /**
     * Calls Closeable.close for each parameter in the order given, if it is
     * non-null.
     *
     * If one or more close methods throws an Exception, all close methods will
     * still be called and the first Exception will be rethrown.  If an Error
     * is thrown by a close method, it will be thrown by this method and no
     * further close methods will be called.  An IOException may be thrown by a
     * close method because is declared by Closeable.close; however, the use of
     * RuntimeExceptions is recommended.
     */
    public static void closeAll(Closeable... objects)
        throws Exception {

        closeAll(null, objects);
    }

    /**
     * Same as closeAll(Closeable...) but allows passing an initial exception,
     * when one may have been thrown earlier during a shutdown procedure.  If
     * null is passed for the firstEx parameter, calling this method is
     * equivalent to calling closeAll(Closeable...).
     */
    public static void closeAll(Exception firstEx, Closeable... objects)
        throws Exception {

        for (Closeable c : objects) {
            if (c == null) {
                continue;
            }
            try {
                c.close();
            } catch (Exception e) {
                if (firstEx == null) {
                    firstEx = e;
                }
            }
        }

        if (firstEx != null) {
            throw firstEx;
        }
    }

    /**
     * Copy everything in test destination directory to another place for
     * future evaluation when test failed.  This code is copied from FileUtils
     * so that it is available for testing even though FileUtils is not
     * included in kvclient.jar.
     */
    public static void copyDir(File fromDir, File toDir)
        throws IOException {

        if (fromDir == null || toDir == null) {
            throw new NullPointerException("File location error");
        }

        if (!fromDir.isDirectory()) {
            throw new IllegalStateException(
                fromDir +  " should be a directory");
        }

        if (!fromDir.exists()) {
            throw new IllegalStateException(
                fromDir +  " does not exist");
        }

        if (!toDir.exists() && !toDir.mkdirs()) {
            throw new IllegalStateException(
                "Unable to create copy dest dir:" + toDir);
        }

        File [] fileList = fromDir.listFiles();
        if (fileList != null && fileList.length != 0) {
            for (File file : fileList) {
                if (file.isDirectory()) {
                    copyDir(file, new File(toDir, file.getName()));
                } else {
                    copyFile(file, new File(toDir, file.getName()));
                }
            }
        }
    }

    /**
     * Split a JVM argument list into discrete argument components.
     * This is intended to support the oracle.kv.jvm.extraargs property.
     * @param extraArgs a String that concatenates JVM configuration
     *   parameters joined by ';'.
     */
    public static String[] splitExtraArgs(String extraArgs) {
        return extraArgs.split(";");
    }

    /**
     * Mark the start of a new test sequence where RMI lookups that yield
     * client socket factories do not treat earlier versions of the socket
     * factory as equal to new versions.  This works around an RMI issue
     * where server interfaces can be unbound, yet leave CSFs with sockets
     * still connected to server ports that have no instance listening.  That
     * scenario does not occur in typical customer use of the product, but does
     * occur in unit tests where servers are repeatedly torn down and restarted
     * within the same process, often with different server port
     * characteristics.
     */
    public static void newCsfGeneration() {
        ClientSocketFactory.newGeneration();
    }

    /*
     * Serialize an object out and then back in.
     */
    public static <T> T serialize(T  obj)
        throws ClassNotFoundException, IOException {

        final Class<?> objClass =
            (obj == null) ? null : obj.getClass();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);
        try {
            oos.writeObject(obj);
        } finally {
            oos.close();
        }

        final ByteArrayInputStream bais =
            new ByteArrayInputStream(baos.toByteArray());
        final ObjectInputStream ois = new ObjectInputStream(bais);
        try {
            @SuppressWarnings("unchecked")
            final T result = (T) ois.readObject();
            assertEquals("Expected EOF after reading serialized object data",
                         -1, ois.read());
            final Class<?> resultClass =
                (result == null) ? null : result.getClass();
            assertEquals(objClass, resultClass);
            return result;
        } finally {
            ois.close();
        }
    }

    /**
     * Check that we can serialize and deserialize an object and that the
     * result is equal to the original.
     */
    public static void checkSerialize(final Object object) {
        final Object deserialized;
        try {
            deserialized = serialize(object);
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException("Unexpected exception: " + e, e);
        }
        assertEquals("After serialization", object, deserialized);
        assertEquals("After serialization",
                     object.hashCode(), deserialized.hashCode());
    }

    /**
     * Check that we can fast serialize and deserialize an object and that the
     * result is equal to the original.
     */
    public static void checkFastSerialize(final FastExternalizable object) {
        final Object deserialized;
        try {
            deserialized = fastSerialize(object);
        } catch (IOException e) {
            throw new RuntimeException("Unexpected exception: " + e, e);
        }
        assertEquals("After fast serialization", object, deserialized);
        assertEquals("After fast serialization",
                     object.hashCode(), deserialized.hashCode());
    }

    /**
     * Check that we can fast serialize and deserialize an object, using a
     * helper for deserialization, and that the result is equal to the
     * original.
     */
    public static <T extends FastExternalizable>
        void checkFastSerialize(final T object,
                                final ReadFastExternal<T> reader) {

        checkFastSerialize(object, reader,
                           FastExternalizable::writeFastExternal);
    }

    /**
     * Check that we can fast serialize and deserialize an object, using
     * helpers for serialization and deserialization, and that the result is
     * equal to the original.
     */
    public static <T extends FastExternalizable>
        void checkFastSerialize(final T object,
                                final ReadFastExternal<T> reader,
                                final WriteFastExternal<T> writer) {

        final Object deserialized;
        try {
            deserialized = fastSerialize(object, reader, writer);
        } catch (IOException e) {
            throw new RuntimeException("Unexpected exception: " + e, e);
        }
        assertEquals("After fast serialization", object, deserialized);
        assertEquals("After fast serialization",
                     object.hashCode(), deserialized.hashCode());
    }

    /**
     * Fast serialize a FastExternalizable object out and then read it back in
     * using the specified reader.
     */
    public static <T extends FastExternalizable>
        T fastSerialize(T obj, ReadFastExternal<T> reader)
        throws IOException {

        return fastSerialize(obj, reader,
                             FastExternalizable::writeFastExternal);
    }

    /**
     * Fast serialize an arbitrary object out using the specified writer and
     * then read it back in using the specified reader.
     */
    public static <T> T fastSerialize(T obj,
                                      ReadFastExternal<T> reader,
                                      WriteFastExternal<T> writer)
        throws IOException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(baos);

        writer.writeFastExternal(obj, out, SerialVersion.CURRENT);
        out.close();

        final ByteArrayInputStream bais =
            new ByteArrayInputStream(baos.toByteArray());
        final DataInputStream in = new DataInputStream(bais);
        try {
            final T result =
                reader.readFastExternal(in, SerialVersion.CURRENT);
            assertEquals("Expected EOF after reading serialized object data",
                         -1, in.read());
            assertEquals(obj.getClass(), result.getClass());
            return result;
        } finally {
            in.close();
        }
    }

    /*
     * Fast serialize an object out and then back in.
     */
    public static <T extends FastExternalizable> T fastSerialize(T obj)
        throws IOException {

        final Constructor<? extends FastExternalizable> objCtor;
        try {
            objCtor = obj.getClass().getDeclaredConstructor(DataInput.class,
                                                            short.class);
            objCtor.setAccessible(true);
        } catch (NoSuchMethodException nsme) {
            throw new IllegalArgumentException(
                "The input object class does not have a FastExternalizable " +
                "constructor",
                nsme);
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos);

        obj.writeFastExternal(dos, SerialVersion.CURRENT);
        dos.close();

        final ByteArrayInputStream bais =
            new ByteArrayInputStream(baos.toByteArray());
        final DataInputStream dis = new DataInputStream(bais);

        try {
            @SuppressWarnings("unchecked")
            final T result =
                (T) objCtor.newInstance(dis, SerialVersion.CURRENT);
            assertEquals("Expected EOF after reading serialized object data",
                         -1, dis.read());
            assertEquals(obj.getClass(), result.getClass());
            return result;
        } catch (Exception e) {
            throw new IOException("Unable to reconstruct object: " +
                                  e.getMessage(), e);
        } finally {
            dis.close();
        }
    }

    /**
     * Assert that a string matches a regular expression pattern.  Consider
     * using the '(?s)' flag in the pattern if you want to match embedded
     * newlines.
     *
     * @param pattern the pattern
     * @param string the string
     * @throws AssertionError if the string does not match the pattern
     */
    public static void assertMatch(String pattern, String string) {
        assertMatch(null, pattern, string);
    }

    /**
     * Assert that a string matches a regular expression pattern, and supply an
     * optional error message.  Consider using the '(?s)' flag in the pattern
     * setting if you want to match embedded newlines.
     *
     * @param message the error message or null
     * @param pattern the pattern
     * @param string the string
     * @throws AssertionError if the string does not match the pattern
     */
    public static void assertMatch(String message,
                                   String pattern,
                                   String string) {
        String formatted = "";
        if ((message != null) && !message.equals("")) {
            formatted = message + " ";
        }
        assertTrue(formatted + "expected match for pattern: " + pattern +
                   "\n  found: " + string,
                   string.matches(pattern));
    }

    /**
     * Creates a set with the specified elements.
     *
     * @param <E> the element type
     * @param elements the elements
     * @return the set
     */
    /*
     * Suppress warnings about potential heap pollution.  This code appears to
     * be safe the same way that Collections.addAll is.
     */
    @SuppressWarnings({"all", "varargs"})
    @SafeVarargs
    public static <E> Set<E> set(@SuppressWarnings("unchecked") E... elements) {
        final Set<E> set = new HashSet<E>();
        Collections.addAll(set, elements);
        return set;
    }

    /**
     * Create and return the AdminLoginManager after it is finished starting
     * up.
     */
    public static AdminLoginManager waitForSecurityStartUp(String host,
                                                           int port) {
        final AdminLoginManager alm =
            new AdminLoginManager(null, true, logger);
        final int MAX_RETRY = 10;
        Exception lastException = null;
        for (int i=0; i < MAX_RETRY; i++) {
            try {
                if (!alm.bootstrap(host, port, null)) {
                    Thread.sleep(1000);
                } else {
                    return alm;
                }
            } catch (Exception e) {
                lastException = e;
            }
        }
        throw new RuntimeException(
            "Wait for admin login service fail", lastException);
    }

    /**
     * Like Collection.removeIf, but returns a count of the number of matching
     * items removed.
    */
    public static <T> int removeIfCount(Collection<T> collection,
                                        Predicate<T> predicate) {
        int count = 0;
        final Iterator<T> iter = collection.iterator();
        while (iter.hasNext()) {
            final T element = iter.next();
            if (predicate.test(element)) {
                iter.remove();
                count++;
            }
        }
        return count;
    }

    /** Compares logging levels. */
    public static int compareLevels(Level l1, Level l2) {
        return l1.intValue() - l2.intValue();
    }

    /*
     * A test hook that throws the specified exception with specified count.
     */
    public static class CountDownFaultHook implements TestHook<Integer> {
        private final AtomicInteger counter;
        private final RuntimeException fault;
        private final String faultThreadName;

        public CountDownFaultHook(int faultCount, RuntimeException fault) {
            this(faultCount, null, fault);
        }

        public CountDownFaultHook(int faultCount,
                                  String threadName,
                                  RuntimeException fault) {
            this.counter = new AtomicInteger(faultCount);
            this.fault = fault;
            this.faultThreadName = threadName;
        }

        @Override
        public void doHook(Integer unused) {
            if (faultThreadName != null) {
                Thread t = Thread.currentThread();

                if(!faultThreadName.equalsIgnoreCase(t.getName())) {
                    return;
                }
            }
            if (counter.decrementAndGet() > 0) {
                throw fault;
            }
        }
    }
}
