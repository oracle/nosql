/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static oracle.kv.impl.util.FileNames.RETRIEVE_SECURITY_UPDATES_FILE;
import static oracle.kv.impl.util.FileNames.SECURITY_CONFIG_DIR;
import static oracle.kv.impl.util.FileNames.SECURITY_UPDATES_DIR;
import static oracle.kv.impl.util.SSLTestUtils.SSL_KS_PWD_DEF;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.util.CreateStore;
import oracle.kv.util.shell.Shell.HelpCommand;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

/** Test 'plan update-tls-credentials'. */
public class PlanUpdateTlsCredentialsCommandTest extends TestBase {
    private static final File testDir = TestUtils.getTestDir();
    private static final File testSslDir = SSLTestUtils.getTestSSLDir();
    private final CommandShell shell = new CommandShell(System.in, System.out);
    private final PlanCommand cmd = new PlanCommand();
    private final ShowCommand showCmd = new ShowCommand();
    private CreateStore createStore;

    @Before
    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestStatus.setManyRNs(true);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (createStore != null) {
            createStore.shutdown();
            createStore = null;
        }
        super.tearDown();
    }

    @Test
    public void testUnexpectedArgs() throws Exception {
        createStore = new CreateStore(kvstoreName,
                                      5000, /* startPort */
                                      1, /* numStorageNodes */
                                      1, /* replicationFactor */
                                      1, /* numPartitions */
                                      1 /* capacity */);
        createStore.start();
        shell.connectAdmin(createStore.getHostname(),
                           createStore.getRegistryPort(),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());
        testUnexpectedArgs(false /* useJson */);
        testUnexpectedArgs(true /* useJson */);
    }

    private void testUnexpectedArgs(boolean useJson) {
        checkException(
            () -> execute(
                "plan update-tls-credentials -retrieve-only -install-only",
                useJson),
            ShellUsageException.class,
            "The -retrieve-only and -install-only flags cannot both be" +
            " specified");

        final AdminFaultException afe = checkException(
            () -> execute("plan update-tls-credentials", useJson),
            AdminFaultException.class,
            "Cannot update TLS credentials for a non-secure store");
        assertEquals(IllegalCommandException.class.getName(),
                     afe.getFaultClassName());
    }

    @Test
    public void testHelp() throws Exception {
        final String helpMessage = new HelpCommand().execute(
            new String[] { "help", "plan", "update-tls-credentials" }, shell);
        assertEquals(
            "Usage: plan update-tls-credentials" +
            " [-retrieve-only|install-only]\n" +
            "\t[-plan-name <name>] [-wait] [-noexecute] [-force]" +
            " [-json|-json-v1]\n" +
            "\tRetrieves and installs credential updates to the set of" +
            " shared TLS\n" +
            "\tcredentials used by SNAs in the store, doing both by default" +
            " or just one\n" +
            "\tif requested.",
            helpMessage);
    }

    @Test
    public void testSecure() throws Exception {

        /* Create a secure store with RF=1 and three shards */
        createStore = new CreateStore(kvstoreName,
                                      5000, /* startPort */
                                      3, /* numStorageNodes */
                                      1, /* replicationFactor */
                                      3, /* numPartitions */
                                      1 /* capacity */);
        createStore.setSecure(true);
        createStore.setSeparateRoots(true);
        createStore.start();
        shell.connectAdmin(createStore.getHostname(),
                           createStore.getRegistryPort(),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());

        /* No updates */
        TestUseJson test = useJson -> {
            executeCheck("plan update-tls-credentials -wait", useJson,
                         "SUCCEEDED");
            executeCheck("plan update-tls-credentials -retrieve-only -wait",
                         useJson, "SUCCEEDED");
            executeCheck("plan update-tls-credentials -install-only -wait",
                         useJson, "SUCCEEDED");
        };
        testUseJson(test);

        /* Update script fails: not executable */
        final File securityDir =
            new File(createStore.getRootDir(0), SECURITY_CONFIG_DIR);
        final File updateDir = new File(securityDir, SECURITY_UPDATES_DIR);
        updateDir.mkdir();
        final File retrieve =
            new File(updateDir, RETRIEVE_SECURITY_UPDATES_FILE);
        retrieve.createNewFile();
        retrieve.setExecutable(false);
        test = useJson -> {
            executeCheck("plan update-tls-credentials -wait", useJson,
                         "ERROR");
            checkLastPlanFailures(
                "Problem executing TLS credentials retrieve command: ",
                2,
                Map.of(1, "RetrieveTlsCredentialsTask on SN sn1"));
            executeCheck("plan update-tls-credentials -retrieve-only -wait",
                         useJson, "ERROR");
            checkLastPlanFailures(
                "Problem executing TLS credentials retrieve command: ",
                2,
                Map.of(1, "RetrieveTlsCredentialsTask on SN sn1"));
            executeCheck("plan update-tls-credentials -install-only -wait",
                         useJson, "SUCCEEDED");
        };
        testUseJson(test);
        retrieve.delete();

        /* Update script fails: undefined command */
        final File keys = new File(updateDir, "store.keys");
        retrieve.createNewFile();
        retrieve.setExecutable(true);
        Files.writeString(retrieve.toPath(),
                          "echo undefined-command > store.keys\n");
        test = useJson -> {
            keys.delete();
            executeCheck("plan update-tls-credentials -wait", useJson,
                         "ERROR");
            checkLastPlanFailures(
                "Error reading from keystore file",
                5,
                Map.of(4, "VerifyTlsCredentialUpdatesTask on SN sn1"));
            keys.delete();
            executeCheck("plan update-tls-credentials -retrieve-only -wait",
                         useJson, "ERROR");
            checkLastPlanFailures(
                "Error reading from keystore file",
                5,
                Map.of(4, "VerifyTlsCredentialUpdatesTask on SN sn1"));
            keys.delete();
            executeCheck("plan update-tls-credentials -install-only -wait",
                         useJson, "SUCCEEDED");
        };
        testUseJson(test);
        retrieve.delete();
        keys.delete();

        /* Update script copies one new truststore */
        final File sslMergeTrust =
            new File(testSslDir, SSLTestUtils.SSL_MERGED_TS_NAME);
        final File updatedMergeTrust = new File(testDir, "store.trust");
        Files.copy(sslMergeTrust.toPath(), updatedMergeTrust.toPath());
        KeyStore truststore =
            SecurityUtils.loadKeyStore(updatedMergeTrust.toString(),
                                       SSL_KS_PWD_DEF.toCharArray(),
                                       "truststore", null /* storeType */);
        Certificate sharedCert = truststore.getCertificate("mykey_1");
        truststore.setCertificateEntry("shared", sharedCert);
        try (final FileOutputStream out = new FileOutputStream(
                 updatedMergeTrust)) {
            truststore.store(out, SSL_KS_PWD_DEF.toCharArray());
        }
        final File trust = new File(updateDir, "store.trust");
        retrieve.createNewFile();
        retrieve.setExecutable(true);
        Files.writeString(retrieve.toPath(),
                          "cp " + updatedMergeTrust + " store.trust\n");
        test = useJson -> {
            trust.delete();
            executeCheck("plan update-tls-credentials -wait", useJson,
                         "ERROR", 5200);
            checkLastPlanFailures(
                "Updates for truststore files are required for SNs: sn2, sn3",
                6,
                Map.of(7, "CheckTlsCredentialsConsistencyTask"));
        };
        testUseJson(test);
        trust.delete();

        /* Update scripts copy all new truststores */
        final File updateDir2 =
            Path.of(createStore.getRootDir(1), SECURITY_CONFIG_DIR,
                    SECURITY_UPDATES_DIR)
            .toFile();
        updateDir2.mkdir();
        final File retrieve2 =
            new File(updateDir2, RETRIEVE_SECURITY_UPDATES_FILE);
        Files.copy(retrieve.toPath(), retrieve2.toPath());
        final File trust2 = new File(updateDir2, "store.trust");
        final File updateDir3 =
            Path.of(createStore.getRootDir(2), SECURITY_CONFIG_DIR,
                    SECURITY_UPDATES_DIR)
            .toFile();
        updateDir3.mkdir();
        final File retrieve3 =
            new File(updateDir3, RETRIEVE_SECURITY_UPDATES_FILE);
        Files.copy(retrieve.toPath(), retrieve3.toPath());
        final File trust3 = new File(updateDir3, "store.trust");
        test = useJson -> {
            trust.delete();
            trust2.delete();
            trust3.delete();
            executeCheck("plan update-tls-credentials -wait", useJson,
                         "SUCCEEDED");
        };
        testUseJson(test);
        retrieve.delete();
        retrieve2.delete();
        retrieve3.delete();
        trust.delete();
        trust2.delete();
        trust3.delete();

        /* One SNA offline */
        createStore.shutdownSNA(1, false /* force */);
        test = useJson -> {
            executeCheck("plan update-tls-credentials -wait", useJson,
                         "ERROR");
            checkLastPlanFailures(
                "Failure contacting storage node sn2",
                2,
                Map.of(2, "RetrieveTlsCredentialsTask on SN sn2"));
        };
        testUseJson(test);

        /* Store shutdown */
        createStore.shutdown(false /* force */);
        createStore = null;
        test = useJson ->
            checkException(() -> execute("plan update-tls-credentials -wait",
                                         useJson),
                           ShellException.class,
                           "Cannot contact admin|" +
                           "Cannot connect to any admins");
        testUseJson(test);
    }

    private static void testUseJson(TestUseJson test) throws Exception {
        test.accept(false);
        test.accept(true);
    }

    interface TestUseJson {
        void accept(boolean useJson) throws Exception;
    }

    private String execute(String command, boolean useJson) throws Exception {
        return execute(cmd, command, useJson);
    }

    private String execute(ShellCommand shellCmd,
                           String command,
                           boolean useJson)
        throws Exception
    {
        if (!useJson) {
             return shellCmd.execute(command.split(" "), shell);
        }
        shell.setJson(true);
        try {
            return shellCmd.executeJsonOutput(
                command.split(" "), shell).convertToJson();
        } finally {
            shell.setJson(false);
        }
    }

    private void executeCheck(String command,
                              boolean useJson,
                              String expectedState)
        throws Exception
    {
        executeCheck(command, useJson, expectedState, 0);
    }

    private void executeCheck(String command,
                              boolean useJson,
                              String expectedState,
                              int expectedReturnCode)
        throws Exception
    {
        final String result = execute(command, useJson);
        if (!useJson) {
            final String expected = "SUCCEEDED".equals(expectedState) ?
                "ended successfully" :
                "ended with errors";
            if ((result == null) || !result.contains(expected)) {
                fail("Expected: " + expected + ", got: " + result +
                     ", plan info: " +
                     execute(showCmd, "show plan -last", false));
            }
        } else {
            final ObjectNode on = JsonUtils.parseJsonObject(result);
            if (expectedReturnCode == 0) {
                expectedReturnCode =
                    "SUCCEEDED".equals(expectedState) ? 5000 : 5500;
            }
            final int returnCode = on.get("returnCode").asInt();
            final String state =
                on.getObject("returnValue").get("state").asText();
            if ((expectedReturnCode != returnCode) ||
                (!expectedState.equals(state))) {
                final String showPlan =
                    execute(showCmd, "show plan -last", false);
                assertEquals("Plan info: " + showPlan,
                             expectedReturnCode, returnCode);
                assertEquals("Plan info: " + showPlan,
                             expectedState, state);
            }
        }
    }

    private ObjectNode getLastPlanResult() throws Exception {
        final ObjectNode on =
            JsonUtils.parseJsonObject(
                execute(showCmd, "show plan -last", true /* useJson */));
        assertEquals(5000, on.get("returnCode").asInt());
        return on.getObject("returnValue");
    }

    private void checkLastPlanFailures(String errorMsg,
                                       int numSucceeded,
                                       Map<Integer, String> errorTasks)
        throws Exception
    {
        final ObjectNode on = getLastPlanResult();
        assertEquals("ERROR", on.get("state").asText());
        final String error = on.get("error").asText();
        assertMatch(errorMsg, error);
        final ObjectNode executionDetails = on.getObject("executionDetails");
        final String detailsString = JsonUtils.prettyPrint(executionDetails);
        final ObjectNode taskCounts = executionDetails.getObject("taskCounts");
        assertEquals("Successful tasks: " + detailsString,
                     numSucceeded, taskCounts.get("successful").asInt());
        assertEquals("Failed tasks: " + detailsString,
                     errorTasks.size(), taskCounts.get("failed").asInt());
        errorTasks = new HashMap<>(errorTasks);
        for (final JsonNode jn : executionDetails.get("finished").asArray()) {
            final ObjectNode task = jn.asObject();
            final int taskNum = task.get("taskNum").asInt();
            final String errorTaskName = errorTasks.remove(taskNum);
            if (errorTaskName == null) {
                continue;
            }
            assertEquals("For task " + taskNum + ": " + detailsString,
                         "ERROR", task.get("state").asText());
            final String name = task.get("name").asText();
            assertTrue("Expected '" + errorTaskName + "', found: " + name,
                       name.contains(errorTaskName));
        }
        assertTrue("Remaining error tasks: " + errorTasks,
                   errorTasks.isEmpty());
    }

    private static void assertMatch(String expectedPattern, String found) {
        if (!Pattern.compile(expectedPattern).matcher(found).find()) {
            fail("Expected '" + expectedPattern + "', found: " + found);
        }
    }
}
