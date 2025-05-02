/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv;

import static java.util.concurrent.TimeUnit.MINUTES;
import static oracle.kv.impl.util.SSLTestUtils.SSL_CTS_NAME;
import static oracle.kv.impl.util.SSLTestUtils.SSL_KS_NAME;
import static oracle.kv.impl.util.SSLTestUtils.SSL_OTHER_CTS_NAME;
import static oracle.kv.impl.util.SSLTestUtils.SSL_OTHER_KS_NAME;
import static oracle.kv.impl.util.SSLTestUtils.SSL_OTHER_PW_NAME;
import static oracle.kv.impl.util.SSLTestUtils.SSL_OTHER_TS_NAME;
import static oracle.kv.impl.util.SSLTestUtils.SSL_PW_NAME;
import static oracle.kv.impl.util.SSLTestUtils.SSL_TS_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.StorageNodeUtils.SecureOpts;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.impl.xregion.XRegionTestBase.ReqRespThread;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Test for problems when using more than one store from the same client.
 */
public class MultiStoreTest extends TestBase {

    private static int nextStoreId = 1;

    /* mock stream managers for multi-region table mode. */
    protected static ReqRespThread reqRespThread1;
    protected static ReqRespThread reqRespThread2;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestUtils.clearTestDirectory();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        LoggerUtils.closeAllHandlers();
        if (mrTableMode) {
            reqRespThread1.stopResResp();
            reqRespThread1 = null;
            reqRespThread2.stopResResp();
            reqRespThread2 = null;
        }

    }

    /**
     * Test a problem where interleaving calls to getTables on two different
     * secure stores caused the last call to get an invalid certificate error
     * because the wrong credentials were being used. The problem was detected
     * when using the multi-region table agent to access multiple secure
     * stores. [#27948]
     *
     * Test additional operations as well. [#27952]
     */
    @Test
    public void testOpsInterleaved() throws Exception {
        testOpsInterleaved("store1", "store2");
    }

    /**
     * Same test, but using the same names for both stores, since that is an
     * additional requirement for multi-region tables. [#27952]
     */
    @Test
    public void testOpsInterleavedSameStoreNames() throws Exception {
        testOpsInterleaved("same-store", "same-store");
    }

    private void testOpsInterleaved(String storeName1, String storeName2)
        throws Exception {

        final KVStore store1 = createStoreInProcess(
            storeName1, SSL_KS_NAME, SSL_PW_NAME, SSL_TS_NAME, SSL_CTS_NAME);
        final TableAPI tableAPI1 = store1.getTableAPI();

        final KVStore store2 = createStoreInProcess(
            storeName2, SSL_OTHER_KS_NAME, SSL_OTHER_PW_NAME,
            SSL_OTHER_TS_NAME, SSL_OTHER_CTS_NAME);
        final TableAPI tableAPI2 = store2.getTableAPI();

        if (mrTableMode) {
            /* Set up mock steam managers for multi-region table mode. */
            reqRespThread1 = mrTableRegionSetUp(store1);
            reqRespThread2 = mrTableRegionSetUp(store2);
        }

        /*
         * Interleaving calls to getTable between these two stores caused the
         * second call to store1 to fail after the store2 call
         */
        assertEquals(null, tableAPI1.getTable("unknown-table"));
        assertEquals(null, tableAPI2.getTable("unknown-table"));
        assertEquals(null, tableAPI1.getTable("unknown-table"));

        /* Try table DDL and DML operations */

        final Table table1 = createTable(store1, "table1", "apple", "red");
        final Table table2 = createTable(store2, "table2", "orange", "orange");
        final Table table1b = createTable(store1, "table1b", "banana",
                                          "yellow");


        checkColor(table1, tableAPI1, "apple", "red");
        checkColor(table2, tableAPI2, "orange", "orange");
        checkColor(table1b, tableAPI1, "banana", "yellow");

        checkColorQuery(store1, "table1", "apple", "red");
        checkColorQuery(store2, "table2", "orange", "orange");
        checkColorQuery(store1, "table1b", "banana", "yellow");
    }

    private KVStore createStoreInProcess(String storeName,
                                         String keystoreName,
                                         String passwordName,
                                         String truststoreName,
                                         String clientTrustName)
        throws Exception {

        final File testDir = TestUtils.getTestDir();
        final File storeDir = new File(testDir,
                                       "store" + nextStoreId++ + storeName);
        assertTrue(storeDir.mkdir());

        final String secoptsFile = storeDir + "-secopts.ser";
        final SecureOpts secOpts = new SecureOpts()
            .setSecure(true)
            .setKeystore(keystoreName)
            .setPasswordFile(passwordName)
            .setTruststore(truststoreName)
            .setClientTruststore(clientTrustName);
        try (ObjectOutputStream out =
             new ObjectOutputStream(new FileOutputStream(secoptsFile))) {
            out.writeObject(secOpts);
            out.flush();
        }

        final int storePort = startStoreInProcess(storeName,
                                                  storeDir,
                                                  5000,
                                                  secoptsFile);
        final Properties storeSecurity = new Properties();
        storeSecurity.setProperty(KVSecurityConstants.TRANSPORT_PROPERTY,
                                  KVSecurityConstants.SSL_TRANSPORT_NAME);
        final File truststore =
            new File(SSLTestUtils.getTestSSLDir(), clientTrustName);
        storeSecurity.setProperty(
            KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
            truststore.toString());
        storeSecurity.setProperty(KVSecurityConstants.AUTH_USERNAME_PROPERTY,
                                  CreateStore.defaultUser);
        final File secDir = new File(storeDir, secOpts.getSecurityDir());
        final File passStore = new File(secDir, CreateStore.defaultPassStore);
        storeSecurity.setProperty(KVSecurityConstants.AUTH_PWDFILE_PROPERTY,
                                  passStore.toString());

        return KVStoreFactory.getStore(
            new KVStoreConfig(storeName, "localhost:" + storePort)
            .setSecurityProperties(storeSecurity));
    }

    private int startStoreInProcess(String storeName,
                                    File storeDir,
                                    int startPort,
                                    String secOpts)
        throws Exception {

        final List<String> command = new ArrayList<>();
        Collections.addAll(command,
                           "java",
                           "-cp", System.getProperty("java.class.path"),
                           "-D" + SSLTestUtils.SSL_DIR_PROP + "=" +
                           System.getProperty(SSLTestUtils.SSL_DIR_PROP),
                           "-ea");
        final String jvmExtraArgs =
            System.getProperty("oracle.kv.jvm.extraargs");
        if (jvmExtraArgs != null) {
            Collections.addAll(command,
                               TestUtils.splitExtraArgs(jvmExtraArgs));
            command.add("-Doracle.kv.jvm.extraargs=" + jvmExtraArgs);
        }
        final boolean enableServerSideLogging =
            Boolean.getBoolean("test.enable.server.side.logging");
        if (enableServerSideLogging) {
            final String loggingConfigFile =
                System.getProperty("java.util.logging.config.file");
            if (loggingConfigFile != null) {
                command.add("-Djava.util.logging.config.file=" +
                            loggingConfigFile);
            }
        }
        Collections.addAll(command,
                           CreateStore.class.getName(),
                           "-store", storeName,
                           "-port", String.valueOf(startPort),
                           "-root", storeDir.toString(),
                           "-num_sns", "1",
                           "-rf", "1",
                           "-partitions", "1",
                           "-threads",
                           "-secure",
                           "-secopts", secOpts);

        final ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        final Process storeProcess = pb.start();
        tearDowns.add(storeProcess::destroyForcibly);

        /* Wait for the process to information about the CLI Admin port */
        final int storePort =
            CompletableFuture.supplyAsync(() -> getCliAdminPort(storeProcess))
            .get(1, MINUTES);
        logger.info("Started store" +
                    " storeName:" + storeName +
                    " storePort:" + storePort);
        return storePort;
    }

    private int getCliAdminPort(Process storeProcess) {
        try {
            final BufferedReader reader = new BufferedReader(
                new InputStreamReader(storeProcess.getInputStream()));
            final Pattern pattern = Pattern.compile("CLI Admin on port (\\d+)");
            while (true) {
                final String line = reader.readLine();
                if (line == null) {
                    throw new RuntimeException("EOF from process");
                }
                final Matcher matcher = pattern.matcher(line);
                if (matcher.matches()) {
                    return Integer.parseInt(matcher.group(1));
                }
                logger.info("Process output: " + line);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unexpected I/O exception: " + e, e);
        }
    }

    private Table createTable(KVStore store,
                              String tableName,
                              String fruit,
                              String color) {
        final String statement = addRegionsForMRTable(
            "create table " + tableName +
            "(name string, color string, primary key (name))");
        final StatementResult result = store.executeSync(statement);
        assertTrue("Failed to create table", result.isSuccessful());
        final TableAPI tableAPI = store.getTableAPI();
        final Table table = tableAPI.getTable(tableName);
        final Row row = table.createRow();
        row.put("name", fruit);
        row.put("color", color);
        tableAPI.put(row, null, null);
        return table;
    }

    private void checkColor(Table table,
                            TableAPI tableAPI,
                            String fruit,
                            String color) {
        final PrimaryKey key = table.createPrimaryKey();
        key.put("name", fruit);
        final String value = tableAPI.get(key, null)
            .get("color")
            .asString()
            .get();
        assertEquals(color, value);
    }

    private void checkColorQuery(KVStore store,
                                 String table,
                                 String fruit,
                                 String color) {
        final StatementResult result =
            store.executeSync("select * from " + table);
        boolean found = false;
        for (final RecordValue value : result) {
            if (found) {
                fail("Found multiple values");
            }
            found = true;
            assertEquals(fruit, value.get("name").asString().get());
            assertEquals(color, value.get("color").asString().get());
        }
        assertTrue("No value found", found);
    }
}
