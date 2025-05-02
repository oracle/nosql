/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.impl.admin.plan.PlanUtils.runPlan;
import static oracle.kv.impl.util.CommandParser.JSON_V2;
import static oracle.kv.impl.util.FileNames.CLIENT_TRUSTSTORE_FILE;
import static oracle.kv.impl.util.FileNames.KEYSTORE_FILE;
import static oracle.kv.impl.util.FileNames.SECURITY_CONFIG_DIR;
import static oracle.kv.impl.util.FileNames.SECURITY_UPDATES_DIR;
import static oracle.kv.impl.util.FileNames.TRUSTSTORE_FILE;
import static oracle.kv.impl.util.FileUtils.computeSha256Hash;
import static oracle.kv.impl.util.SSLTestUtils.SSL_KS_ALIAS_DEF;
import static oracle.kv.impl.util.SSLTestUtils.SSL_KS_PWD_DEF;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.async.AbstractEndpointGroup;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.test.TestIOHook;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.util.CreateStore;
import oracle.kv.util.Ping;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import com.sleepycat.je.rep.utilint.net.SSLDataChannel;
import com.sleepycat.je.utilint.TestHook;

import org.junit.Test;

/** Test UpdateTlsCredentialsPlan. */
public class UpdateTlsCredentialsTest extends TestBase {
    private static final int startPort = 5000;
    private CreateStore createStore;

    /**
     * A test hook that SecurityUtils test hooks will call if it is non-null
     * and if modHookRoot is either not set or is a parent of the path passed
     * to the SecurityUtils hook. The string passed to modHook will be
     * constructed from the operation name passed to the modHook method and the
     * pathname of the path argument relative to modHookRoot.
     */
    private volatile TestIOHook<String> modHook;
    private volatile Path modHookRoot;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestStatus.setManyRNs(true);
        TestStatus.setActive(true);
        SecurityUtils.addTrustUpdatesStoreHook =
            modHook("addTrustUpdatesStore");
        SecurityUtils.addTrustUpdatesMoveHook = modHook("addTrustUpdatesMove");
        SecurityUtils.addTrustUpdatesDeleteHook =
            modHook("addTrustUpdatesDelete");
        SecurityUtils.installMatchingCopyHook = modHook("installMatchingCopy");
        SecurityUtils.installMatchingMoveHook = modHook("installMatchingMove");
        SecurityUtils.installMatchingDeleteHook =
            modHook("installMatchingDelete");
        SecurityUtils.deleteMatchingHook = modHook("deleteMatching");
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (createStore != null) {
            createStore.shutdown();
        }
        SecurityUtils.addTrustUpdatesStoreHook = null;
        SecurityUtils.addTrustUpdatesMoveHook = null;
        SecurityUtils.addTrustUpdatesDeleteHook = null;
        SecurityUtils.installMatchingCopyHook = null;
        SecurityUtils.installMatchingMoveHook = null;
        SecurityUtils.installMatchingDeleteHook = null;
        SecurityUtils.deleteMatchingHook = null;
        SSLDataChannel.startupHook = null;
    }

    /* Tests */

    @Test
    public void testAll() throws Exception {
        final Set<SSLDataChannel> sslDataChannels = new HashSet<>();
        SSLDataChannel.startupHook = new TestHook<>() {
            @Override
            public void doHook(SSLDataChannel channel) {
                sslDataChannels.add(channel);
            }
        };
        createStore = new CreateStore(kvstoreName, startPort,
                                      3 /* numStorageNodes */,
                                      3 /* replicationFactor */,
                                      10 /* numPartitions */,
                                      1 /* capacity */);
        createStore.setSecure(true);
        createStore.setSeparateRoots(true);
        createStore.setUseThreads(true);
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();

        /* Run the update plan with no updates present */
        int planId =
            cs.createUpdateTlsCredentialsPlan("update-tls-credentials",
                                              true /* retrieve */,
                                              true /* install */);
        runPlan(cs, planId);
        ping();

        /*
         * Run the update plan with only keystore updates present -- plan
         * should fail
         */
        updateCerts(true /* keystoreOnly */);
        final int planId2 =
            cs.createUpdateTlsCredentialsPlan("update-tls-credentials",
                                              true /* retrieve */,
                                              true /* install */);
        checkException(() -> runPlan(cs, planId2),
                       AdminFaultException.class,
                       "Certificate .* in truststore .* does not match");
        ping();

        /* Run the update plan with updates present */
        updateCerts();
        planId =
            cs.createUpdateTlsCredentialsPlan("update-tls-credentials",
                                              true /* retrieve */,
                                              true /* install */);

        /*
         * Only notice modifications to files on SN 1 because the operations on
         * a single SN should have a repeatable order
         */
        modHookRoot = Path.of(createStore.getRootDir(0));

        /* Set hook to collect the labels describing all operations */
        final List<String> labels = new ArrayList<>();
        modHook = labels::add;
        runPlan(cs, planId);

        /* Make sure the new credentials work */
        invalidateSSLSessions(sslDataChannels);
        ping();

        /* Make sure the labels are unique */
        assertEquals(labels.size(), new HashSet<>(labels).size());

        /*
         * Run the update plan with updates present again to confirm the same
         * set of labels
         */
        updateCerts();
        planId = cs.createUpdateTlsCredentialsPlan("update-tls-credentials",
                                                   true /* retrieve */,
                                                   true /* install */);
        final List<String> labels2 = new ArrayList<>();
        modHook = labels2::add;
        runPlan(cs, planId);
        assertEquals(labels, labels2);

        final File rootDir = new File(createStore.getRootDir(0));
        final File configDir = new File(rootDir, SECURITY_CONFIG_DIR);
        final File updatesDir = new File(configDir, SECURITY_UPDATES_DIR);

        /* Now run tests that inject a failure at each label and then retry */
        for (final String label : labels) {
            for (int i = 0; i < 4; i++) {
                final boolean newPlan = ((i & 0x1) == 0);
                final boolean newCreds = ((i & 0x2) == 0);
                final String msg = "Inject failure for label " + label +
                    " newPlan=" + newPlan + " newCreds= " + newCreds;
                try {
                    modHook = l -> {
                        if (label.equals(l)) {
                            throw new RuntimeException(msg);
                        }
                    };

                    /* Create new certificates */
                    updateCerts();
                    final File truststore =
                        new File(updatesDir, TRUSTSTORE_FILE);
                    String truststoreHash = computeSha256Hash(truststore);
                    final File keystore = new File(updatesDir, KEYSTORE_FILE);
                    String keystoreHash = computeSha256Hash(keystore);

                    /* Run plan with new certificates and failure injection */
                    final int pid = cs.createUpdateTlsCredentialsPlan(
                        "update-tls-credentials",
                        true /* retrieve */,
                        true /* install */);
                    cs.approvePlan(pid);
                    cs.executePlan(pid, false /* force */);
                    cs.awaitPlan(pid, 0 /* timeout */, null);
                    final AdminFaultException afe =
                        checkException(() -> cs.assertSuccess(pid),
                                       AdminFaultException.class,
                                       msg);
                    assertEquals(OperationFaultException.class.getName(),
                                 afe.getFaultClassName());

                    /* Possibly create new credentials for use in the retry */
                    if (newCreds) {
                        updateCerts();
                        truststoreHash = computeSha256Hash(truststore);
                        keystoreHash = computeSha256Hash(keystore);
                    }

                    /* Verify connectivity */
                    invalidateSSLSessions(sslDataChannels);
                    ping();

                    /*
                     * Rerun the plan (or run a new plan) without failure
                     * injection
                     */
                    modHook = null;
                    if (newPlan) {
                        final int newPid = cs.createUpdateTlsCredentialsPlan(
                            "update-tls-credentials",
                            true /* retrieve */,
                            true /* install */);
                        runPlan(cs, newPid);
                    } else {
                        cs.executePlan(pid, false /* force */);
                        cs.awaitPlan(pid, 0 /* timeout */, null);
                        cs.assertSuccess(pid);
                    }

                    /* Verify connectivity */
                    invalidateSSLSessions(sslDataChannels);
                    ping();

                    /* Check that files all look right */
                    assertEquals(truststoreHash,
                                 computeSha256Hash(
                                     new File(configDir, TRUSTSTORE_FILE)));
                    assertEquals(keystoreHash,
                                 computeSha256Hash(
                                     new File(configDir, KEYSTORE_FILE)));
                    assertEquals(
                        Collections.emptyList(),
                        Arrays.asList(
                            configDir.listFiles(
                                (dir, name) -> name.endsWith(".tmp"))));
                } catch (Throwable t) {
                    throw new AssertionError(msg + ": " + t, t);
                }
            }
        }

        /*
         * Run plan with changing the set of SNs between creating the plan and
         * executing it
         */
        final int planId3 =
            cs.createUpdateTlsCredentialsPlan("update-tls-credentials",
                                              true /* retrieve */,
                                              true /* install */);
        createStore.addSN(1, false);
        for (String file : new String[] {
                KEYSTORE_FILE, TRUSTSTORE_FILE, CLIENT_TRUSTSTORE_FILE }) {
            Files.copy(Path.of(createStore.getRootDir(0),
                               SECURITY_CONFIG_DIR, file),
                       Path.of(createStore.getRootDir(3),
                               SECURITY_CONFIG_DIR, file),
                       REPLACE_EXISTING);
        }
        Thread.sleep(1000);
        cs.approvePlan(planId3);
        AdminFaultException afe =
            checkException(() -> cs.executePlan(planId3, false /* force */),
                           AdminFaultException.class,
                           "set of storage nodes has changed");
        assertEquals(IllegalCommandException.class.getName(),
                     afe.getFaultClassName());
        ping();
        cs.cancelPlan(planId3);
    }

    /* Other methods */

    private void ping() throws RemoteException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final PrintStream ps = new PrintStream(baos);
        final CommandServiceAPI cs = createStore.getAdmin();
        Ping.pingTopology(cs.getTopology(), cs.getParameters(),
                          false /* showHidden */, JSON_V2, ps,
                          createStore.getAdminLoginManager(),
                          null /* shard */, logger);
        final ObjectNode result = JsonUtils.parseJsonObject(baos.toString());
        assertEquals(JsonUtils.prettyPrint(result),
                     5000, result.get("returnCode").asInt());
    }

    private static void generateCredentials(String truststore, String keystore)
        throws Exception
    {
        final String keyAlgorithm = "RSA";
        final String dn = "cn=Unit Test";
        final String validity = "30";

        exec("keytool", "-genkeypair",
             "-keystore", keystore,
             "-storepass", SSL_KS_PWD_DEF,
             "-keypass", SSL_KS_PWD_DEF,
             "-alias", SSL_KS_ALIAS_DEF,
             "-dname", dn,
             "-keyAlg", keyAlgorithm,
             "-validity", validity);

        execStdout("keytool", "-list",
                   "-keystore", keystore,
                   "-storepass", SSL_KS_PWD_DEF,
                   "-keypass", SSL_KS_PWD_DEF);

        final File tmpFile = File.createTempFile("cert", null);
        try {
            exec("keytool", "-export",
                 "-keystore", keystore,
                 "-storepass", SSL_KS_PWD_DEF,
                 "-alias", SSL_KS_ALIAS_DEF,
                 "-file", tmpFile.toString());
            exec("keytool", "-import",
                 "-keystore", truststore,
                 "-storepass", SSL_KS_PWD_DEF,
                 "-alias", SSL_KS_ALIAS_DEF,
                 "-file", tmpFile.toString(),
                 "-noprompt");
        } finally {
            tmpFile.delete();
        }
    }

    private static void exec(String... command) throws Exception {
        final File out = File.createTempFile("out", null);
        try {
            final int waitMillis = 30000;
            final Process p = new ProcessBuilder(command)
                .redirectOutput(out)
                .redirectError(out)
                .start();
            p.waitFor(waitMillis, MILLISECONDS);
            if (p.exitValue() != 0) {
                fail("Command failed with exit code " +
                     p.exitValue() + ": " + Files.readString(out.toPath()));
            }
        } finally {
            out.delete();
        }
    }

    private static void execStdout(String... command) throws Exception {
        final int waitMillis = 30000;
        final Process p = new ProcessBuilder(command)
            .inheritIO()
            .start();
        p.waitFor(waitMillis, MILLISECONDS);
        if (p.exitValue() != 0) {
            fail("Command failed with exit code " + p.exitValue());
        }
    }

    private void updateCerts() throws Exception {
        updateCerts(false /* keystoreOnly */);
    }

    private void updateCerts(boolean keystoreOnly) throws Exception {
        final Path updatesDir = Path.of(createStore.getRootDir(0),
                                        SECURITY_CONFIG_DIR,
                                        SECURITY_UPDATES_DIR);
        Files.createDirectories(updatesDir);
        final Path truststore = updatesDir.resolve(TRUSTSTORE_FILE);
        final Path keystore = updatesDir.resolve(KEYSTORE_FILE);
        truststore.toFile().delete();
        keystore.toFile().delete();
        generateCredentials(truststore.toString(), keystore.toString());
        for (int i = 1; i <= 2; i++) {
            final Path ud = Path.of(createStore.getRootDir(i),
                                    SECURITY_CONFIG_DIR,
                                    SECURITY_UPDATES_DIR);
            Files.createDirectories(ud);
            if (!keystoreOnly) {
                Files.copy(truststore, ud.resolve(TRUSTSTORE_FILE),
                           REPLACE_EXISTING);
            }
            Files.copy(keystore, ud.resolve(KEYSTORE_FILE),
                       REPLACE_EXISTING);
        }
    }

    /**
     * Return a test hook that will call modHook with a value constructed from
     * the specified op and the pathname supplied to the test hook. Skips
     * calling modHook if modHookRoot is set and is not a parent of the
     * pathname passed to the test hook.
     */
    private TestIOHook<Path> modHook(String op) {
        return p -> {
            if (modHook != null) {
                if (modHookRoot != null) {
                    if (!p.startsWith(modHookRoot)) {
                        return;
                    }
                    p = modHookRoot.relativize(p);
                }
                modHook.doHook(op + ":" + p);
            }
        };
    }

    /** Make sure that all SSL connections need to check credentials. */
    private static
        void invalidateSSLSessions(Set<SSLDataChannel> sslDataChannels)
    {
        /*
         * Invalidate all SSL sessions, which will prevent them from being used
         * for new connections. Iterate over a copy in case new sessions are
         * created while we're doing this.
         */
        Set.copyOf(sslDataChannels).forEach(
            channel -> channel.getSSLEngine().getSession().invalidate());

        /*
         * Close all existing connections, so that new ones will need to be
         * made using new sessions with the latest credentials
         */
        ((AbstractEndpointGroup) AsyncRegistryUtils.getEndpointGroup())
            .shutdownCreatorEndpointHandlers("For testing", true);
    }
}
