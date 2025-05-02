/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna.collector;

import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import oracle.kv.LoginCredentials;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.kvlite.KVLite;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;

import org.junit.Test;

public class JMXCollectorAgentTest extends TestBase {

    private static final int START_PORT = 6000;
    private static final int HA_RANGE = 2;
    private static final String HOST_NAME = "localhost";

    private PortFinder portFinder;
    private KVLite kvlite;
    private CommandServiceAPI admin;

    @Override
    public void setUp() throws Exception {

        super.setUp();
        portFinder = new PortFinder(START_PORT, HA_RANGE);
        TestStatus.setManyRNs(true);
        RegistryUtils.clearRegistryCSF();
        suppressSystemOut();
        suppressSystemError();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        if (kvlite != null) {
            kvlite.stop(false);
            kvlite = null;
        }
        LoggerUtils.closeAllHandlers();
        resetSystemOut();
        resetSystemError();
    }

    @Test
    public void testJMXSSLConnection()
        throws Exception {

        String portstr = Integer.toString(portFinder.getRegistryPort());
        kvlite = new KVLite(TestUtils.getTestDir().toString(),
                            kvstoreName,
                            portstr,
                            true, /* run bootadmin */
                            "localhost",
                            portFinder.getHaRange(),
                            null, /* service port range */
                            3, /* numPartitions */
                            null, /* mount point */
                            false, /* useThreads */
                            true, /* isSecure */
                            null, /* no backup to restore */
                            -1,
                            1, /* numStorageNodes */
                            1, /* repfactor */
                            1 /* capacity */);

        kvlite.setPolicyMap(mergeParameterMapDefaults(null));
        kvlite.setMemoryMB(256);
        kvlite.setEnableJmx(true);
        kvlite.start(true);

        /* enable collector service */
        admin = getAdmin(portFinder.getRegistryPort());
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.GLOBAL_TYPE);
        map.setParameter(ParameterState.GP_COLLECTOR_ENABLED, "true");
        map.setParameter(ParameterState.GP_COLLECTOR_INTERVAL, "1 s");

        int planId = admin.createChangeGlobalComponentsParamsPlan(
            "enable collector", map);
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        /*
         * verify if JMXCollectorAgent writes logging statistics emitted by
         * JMX to the file under SN collector
         */
        File storeDir= new File(TestUtils.getTestDir(), kvstoreName);
        File collectorDir = new File(storeDir.getAbsolutePath() +
            "/sn1/" + FileNames.COLLECTOR_DIR);
        assertTrue(collectorDir.exists());

        final boolean result = new PollCondition(500, 30000) {
            private File testFile = null;

            @Override
            protected boolean condition() {
                try {
                    if (testFile == null) {
                        File [] fileList = collectorDir.listFiles();
                        for (File file : fileList) {
                            String fileName = file.getName();
                            if (fileName.contains(
                                FileNames.COLLECTOR_LOGGINGSTATS_FILE_NAME) &&
                                fileName.endsWith(".json")) {
                                testFile = file;
                                break;
                            }
                        }
                    }

                    if (testFile == null) {
                        return false;
                    }
                    try (BufferedReader reader =
                             new BufferedReader(new FileReader(testFile))) {
                        String line = reader.readLine();
                        if (line != null) {
                            JsonNode node = JsonUtils.parseJsonNode(line);
                            if (node.get("serviceId") != null) {
                                return true;
                            }
                        }
                    }
                } catch (IOException ioe) {
                    throw new IllegalStateException(
                        "Unable to read stats files", ioe);
                }
                return false;
            }
        }.await();
        assertTrue(result);
    }

    private CommandServiceAPI getAdmin(int port)
        throws Exception {

        KVStoreLogin storeLogin = new KVStoreLogin("admin",
            TestUtils.getTestDir().getAbsolutePath() +
            "/security/user.security");
        storeLogin.loadSecurityProperties();
        storeLogin.prepareRegistryCSF();
        final LoginCredentials creds =
            storeLogin.makeShellLoginCredentials();
        final LoginManager loginMgr =
            KVStoreLogin.getAdminLoginMgr(HOST_NAME, port, creds, logger);
        return RegistryUtils.getAdmin(HOST_NAME, port, loginMgr, logger);
    }
}
