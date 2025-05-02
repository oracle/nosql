/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static oracle.kv.impl.topo.DatacenterType.SECONDARY;
import static oracle.kv.impl.util.TestUtils.assertMatch;
import static oracle.kv.impl.util.TestUtils.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.AdminStatus;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;
import oracle.kv.util.CreateStore.ZoneInfo;

import org.junit.AfterClass;
import org.junit.Test;

/**
 * Non-syntax tests of the repair-admin-quorum CLI command.
 */
public class RepairAdminQuorumCommandTest extends TestBase {
    private static final int numSNs = 5;
    private static final int numPrimarySNs = 3;
    private static final int startPort = 5000;
    private static CreateStore createStore;

    private ByteArrayOutputStream shellOutput;
    private CommandShell shell;
    private RepairAdminQuorumCommand command;

    /** If set to true by a test, shuts down the store during tearDown */
    private boolean shutdown;

    @Override
    public void setUp()
        throws Exception {

        if (createStore == null) {
            clearTestDirectory();
            createStore = new CreateStore(
                RepairAdminQuorumCommandTest.class.getName(),
                startPort,
                numSNs, /* Storage Nodes */
                asList(new ZoneInfo(numPrimarySNs),
                       new ZoneInfo(numSNs - numPrimarySNs, SECONDARY)),
                90, /* Partitions */
                1 /* capacity */);
            createStore.setAdminLocations(1, 2, 3, 4, 5);
            createStore.start();
        }
        TestStatus.setManyRNs(true);
        shellOutput = new ByteArrayOutputStream();
        shell = new CommandShell(System.in, new PrintStream(shellOutput));
        createStore.getAdminMaster();
        shell.connectAdmin("localhost",
                           createStore.getRegistryPort(),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());
        command = new RepairAdminQuorumCommand();
    }

    @AfterClass
    public static void tearDownClass()
        throws Exception {

        if (createStore != null) {
            createStore.shutdown();
            createStore = null;
        }
        LoggerUtils.closeAllHandlers();
    }

    @Override
    public void tearDown() {
        if (shutdown) {
            if (createStore != null) {
                createStore.shutdown();
                createStore = null;
            }
            LoggerUtils.closeAllHandlers();
        }
        logger.info("Shell output for " + testName.getMethodName() + ": " +
                    shellOutput);
    }

    /* Tests */

    /*
     * TODO: Tests to add:
     * - Error insertion before and after each remote method that has side
     *   effects, followed by retries
     */

    @Test
    public void testRepairUsingMaster()
        throws Exception {

        final int masterIndex = createStore.getAdminIndex();
        final int replicaIndex1 = (masterIndex + 1) % numPrimarySNs;
        final int replicaIndex2 = (masterIndex + 2) % numPrimarySNs;
        try {
            createStore.shutdownSNA(replicaIndex1, false);
            createStore.shutdownSNA(replicaIndex2, false);
            command.repairAdminQuorum(
                shell,
                Collections.<DatacenterId>emptySet(),
                Collections.<String>emptySet(),
                singleton(createStore.getAdminId(masterIndex)));
            awaitMaster();
        } finally {
            createStore.startSNA(replicaIndex1);
            createStore.startSNA(replicaIndex2);
        }
    }

    @Test
    public void testRepairUsingReplica()
        throws Exception {

        final int masterIndex = createStore.getAdminIndex();
        final int replicaIndex1 = (masterIndex + 1) % numPrimarySNs;
        final int replicaIndex2 = (masterIndex + 2) % numPrimarySNs;
        try {
            createStore.shutdownSNA(masterIndex, false);
            createStore.shutdownSNA(replicaIndex1, false);
            shell.connectAdmin("localhost",
                               createStore.getRegistryPort(replicaIndex2),
                               createStore.getDefaultUserName(),
                               createStore.getDefaultUserLoginPath());
            command.repairAdminQuorum(
                shell,
                Collections.<DatacenterId>emptySet(),
                Collections.<String>emptySet(),
                singleton(createStore.getAdminId(replicaIndex2)));
            awaitMaster();
        } finally {
            createStore.startSNA(masterIndex);
            createStore.startSNA(replicaIndex1);
        }
    }

    @Test
    public void testRepairUsingSecondaryAndReplica()
        throws Exception {

        shutdown = true;

        final int masterIndex = createStore.getAdminIndex();
        final int replicaIndex1 = (masterIndex + 1) % numPrimarySNs;
        final int replicaIndex2 = (masterIndex + 2) % numPrimarySNs;
        final int secondaryIndex = numSNs - 1;
        createStore.shutdownSNA(masterIndex, false);
        createStore.shutdownSNA(replicaIndex1, false);
        shell.connectAdmin("localhost",
                           createStore.getRegistryPort(secondaryIndex),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());
        command.repairAdminQuorum(
            shell,
            Collections.<DatacenterId>emptySet(),
            Collections.<String>emptySet(),
            set(createStore.getAdminId(replicaIndex2),
                createStore.getAdminId(secondaryIndex)));
        awaitMaster();
    }

    @Test
    public void testRepairUsingSingleSecondary()
        throws Exception {

        shutdown = true;

        final int masterIndex = createStore.getAdminIndex();
        final int replicaIndex1 = (masterIndex + 1) % numPrimarySNs;
        final int replicaIndex2 = (masterIndex + 2) % numPrimarySNs;
        final int secondaryIndex = numPrimarySNs;
        createStore.shutdownSNA(masterIndex, true);
        createStore.shutdownSNA(replicaIndex1, true);
        createStore.shutdownSNA(replicaIndex2, true);
        shell.connectAdmin("localhost",
                           createStore.getRegistryPort(secondaryIndex),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());
        command.repairAdminQuorum(
            shell,
            Collections.<DatacenterId>emptySet(),
            Collections.<String>emptySet(),
            singleton(createStore.getAdminId(secondaryIndex)));
        awaitMaster();
    }

    @Test
    public void testRepairUsingMultipleSecondaries()
        throws Exception {

        shutdown = true;

        final int masterIndex = createStore.getAdminIndex();
        final int replicaIndex1 = (masterIndex + 1) % numPrimarySNs;
        final int replicaIndex2 = (masterIndex + 2) % numPrimarySNs;
        final int secondaryIndex1 = numPrimarySNs;
        createStore.shutdownSNA(masterIndex, true);
        createStore.shutdownSNA(replicaIndex1, true);
        createStore.shutdownSNA(replicaIndex2, true);
        shell.connectAdmin("localhost",
                           createStore.getRegistryPort(secondaryIndex1),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());
        command.repairAdminQuorum(shell,
                                  set(new DatacenterId(2)),
                                  Collections.<String>emptySet(),
                                  Collections.<AdminId>emptySet());
        awaitMaster();
    }

    @Test
    public void testUnknownAdminId()
        throws Exception {

        try {
            command.repairAdminQuorum(shell,
                                      Collections.<DatacenterId>emptySet(),
                                      Collections.<String>emptySet(),
                                      singleton(new AdminId(42)));
            fail("Expected AdminFaultException");
        } catch (AdminFaultException e) {
            assertEquals("Fault class",
                         IllegalCommandException.class.getName(),
                         e.getFaultClassName());
            assertMatch("(?s)" + IllegalCommandException.class.getName() +
                         ":Problems repairing admin quorum" +
                        ".*Requested admins not found: \\[admin42\\].*",
                        e.getMessage());
        }
    }

    @Test
    public void testUnknownZoneId()
        throws Exception {

        try {
            command.repairAdminQuorum(shell,
                                      singleton(new DatacenterId(42)),
                                      Collections.<String>emptySet(),
                                      Collections.<AdminId>emptySet());
            fail("Expected AdminFaultException");
        } catch (AdminFaultException e) {
            assertEquals("Fault class",
                         IllegalCommandException.class.getName(),
                         e.getFaultClassName());
            assertMatch("(?s)" + IllegalCommandException.class.getName() +
                        ":Problems repairing admin quorum" +
                        ".*Zones not found: \\[zn42\\].*",
                        e.getMessage());
        }
    }

    @Test
    public void testMissingAdmins()
        throws Exception {

        try {
            command.repairAdminQuorum(shell,
                                      Collections.<DatacenterId>emptySet(),
                                      Collections.<String>emptySet(),
                                      singleton(new AdminId(1)));
            fail("Expected AdminFaultException");
        } catch (AdminFaultException e) {
            assertEquals("Fault class",
                         IllegalCommandException.class.getName(),
                         e.getFaultClassName());
            assertMatch("(?s)" + IllegalCommandException.class.getName() +
                         ":Problems repairing admin quorum" +
                        ".*Available primary admins not specified.*",
                        e.getMessage());
        }
    }

    @Test
    public void testWithQuorum()
        throws Exception {

        command.repairAdminQuorum(shell,
                                  Collections.<DatacenterId>emptySet(),
                                  Collections.<String>emptySet(),
                                  set(new AdminId(1), new AdminId(2),
                                      new AdminId(3)));
    }

    @Test
    public void testWithQuorumUsingZone()
        throws Exception {

        command.repairAdminQuorum(shell,
                                  set(new DatacenterId(1)),
                                  Collections.<String>emptySet(),
                                  Collections.<AdminId>emptySet());
    }

    /* Utilities */

    private void awaitMaster() {
        final boolean hasMaster = new PollCondition(1000, 90000) {
            @Override
            protected boolean condition() {
                try {
                    AdminStatus adminStatus =
                        shell.getAdmin().getAdminStatus();
                    return adminStatus.getIsAuthoritativeMaster();
                } catch (Exception e) {
                    return false;
                }
            }
        }.await();
        assertTrue("hasMaster", hasMaster);
        assertNotNull("Admin master", createStore.getAdminMaster());
    }
}
