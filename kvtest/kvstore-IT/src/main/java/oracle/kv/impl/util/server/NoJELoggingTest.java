/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.server;

import static java.util.logging.Level.SEVERE;
import static oracle.kv.util.CreateStore.MB_PER_SN;
import static org.junit.Assert.assertEquals;

import java.io.FileReader;
import java.io.FileWriter;

import oracle.kv.TestBase;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/** Test that logging to JE debug log files is disabled [KVSTORE-878] */
public class NoJELoggingTest  extends TestBase {
    private CreateStore createStore;
    private static final int startPort = 5000;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestStatus.setManyRNs(true);
        TestStatus.setActive(true);
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (createStore != null) {
            createStore.shutdown();
        }
        LoggerUtils.closeAllHandlers();
    }

    /* Tests */

    @Test
    public void testNoJEDebugLogging() throws Exception {
        createStore = new CreateStore(kvstoreName, startPort,
                                      1, /* SNs */
                                      1, /* RF */
                                      10, /* partitions */
                                      1, /* capacity */
                                      MB_PER_SN, /* memoryMB */
                                      false, /* useThreads */
                                      null /* mgmtImpl */);
        createStore.start();

        final Topology topo = createStore.getAdminMaster().getTopology();
        final StorageNodeId[] snIds = createStore.getStorageNodeIds();
        final RegistryUtils registryUtils =
            new RegistryUtils(topo, createStore.getSNALoginManager(snIds[0]),
                              logger);

        /*
         * The logging configuration is not applied to the bootstrap admin, so
         * it will do some JE logging. I don't think it is worth fixing that
         * problem now. Instead, restart the store so a real admin is used,
         * since that is what happens as soon as the admin restarts.
         */
        createStore.restart();

        /*
         * Truncate the admin JE log file so we only make sure the restarted
         * admin does no logging to it
         */
        final String rootDir = createStore.getRootDir();
        final String adminJELogFile =
            String.format("%s/%s/log/admin1.je.info.0",
                          rootDir,
                          kvstoreName);
        new FileWriter(adminJELogFile).close();

        /* Log SEVERE to JE for RN and Admin */
        registryUtils.getAdminTest(snIds[0])
            .logMessage(SEVERE, "Test logging", true /* useJeLogging */);
        registryUtils.getRepNodeTest(topo.getRepNodeIds().iterator().next())
            .logMessage(SEVERE, "Test logging", true /* useJeLogging */);

        /* Make sure the JE log files are empty */
        try (final FileReader in = new FileReader(adminJELogFile)) {
            assertEquals("File should be empty: " + adminJELogFile,
                         -1, in.read());
        }
        final String rnJELogFile =
            String.format("%s/%s/log/rg1-rn1.je.info.0",
                          rootDir,
                          kvstoreName);
        try (final FileReader in = new FileReader(rnJELogFile)) {
            assertEquals("File should be empty: " + rnJELogFile,
                         -1, in.read());
        }
    }
}
