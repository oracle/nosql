/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.client.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.Protocols;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Test the admin discovery component in the DDL execution implementation.
 * Checks the ability to find the Admin master in these scenarios:
 * - all Admins up
 * - 2 out of 3 Admins up, quorum still available
 * - no Admins up
 * - 1 out of 3 Admins up, no quorum
 *
 * Since SNs are searched in id order, vary the location of the target master.
 */
public class FindClientAdminServiceTest extends TestBase {

    private CreateStore createStore;
    private static final int startPort = 5000;
    private static final int rf = 3;
    private static final int numSns = 5;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);
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

    /**
     * The test causes Admin failures, and checks to see that the
     * FindClientAdminService class can still locate an Admin master and return
     * a ClientAdminService.
     */
    @Test
    public void testDDLFinder()
        throws Exception {

        /**
         * 5 SNs, Admin services on sn1, sn3, sn5
         */
        createStore = new CreateStore(kvstoreName,
                                      startPort, numSns, rf, 10, 1,
                                      CreateStore.MB_PER_SN,
                                      true /* useThreads */, null);
        createStore.setAdminLocations(1, 3, 5);
        createStore.start();

        CommandServiceAPI cs = createStore.getAdmin();
        Topology topo = cs.getTopology();
        assertNotNull(topo);

        /*
         * Find an ClientAdminServiceAPI. It should come from the first,
         * master Admin on SN1. This test is leaning on the knowledge that the
         * FindClientAdminService peruses SNs in id order (i.e. sn1, sn2, etc).
         */
        FindClientAdminService finder = new FindClientAdminService
            (topo, logger, createStore.getSNALoginManager(0),
             null /* clientId */, Protocols.get(createStore.createKVConfig()));
        ClientAdminServiceAPI service = finder.getDDLService();
        assertTrue(service != null);

        /*
         * We know the implementation searchs sns in id order. Make sure it
         * didn't have to inspect them all.
         */
        checkInspected(finder, 1);
        assertTrue(service.canHandleDDL());

        /*
         * Cause a failover by killing Admin1, wait for a new Admin to take
         * over.
         */
        RegistryUtils ru = new RegistryUtils(
            topo, createStore.getSNALoginManager(new StorageNodeId(1)),
            logger);
        StorageNodeAgentAPI snai = ru.getStorageNodeAgent(new StorageNodeId(1));
        assertNotNull(snai);
        assertTrue(snai.stopAdmin(false));
        CommandServiceAPI newCS = createStore.getAdminMaster();
        assertFalse(cs.equals(newCS));
        cs = newCS;

        /*
         * The service should either be disconnected, or aware that it is not a
         * master.
         */
        try {
            assertFalse(service.canHandleDDL());
        } catch (RemoteException expected) {
            logger.info("Saw expected exception " + expected);
        }

        /*
         * Find a new DDLService from another SN. At this point, the master
         * is either Admin2 or Admin3, but the finder should only have to
         * visit Admin2 to find the clientAdmin service. Either Admin2 is the
         * master, or it is the replica and will be able to forward to Admin3.
         */
        finder = new FindClientAdminService(
            topo, logger, createStore.getSNALoginManager(0),
            null /* clientId */, Protocols.get(createStore.createKVConfig()));
        service = finder.getDDLService();
        assertTrue(service != null);

        /*
         * We know the implementation searchs sns in id order. Make sure it
         * didn't have to inspect them all.
         */
        checkInspected(finder, 1, 2, 3);
        /* All SNs were up. */
        assertEquals(0, finder.getProblematic().size());
        assertTrue(service.canHandleDDL());

        /*
         * Concoct a situation where this is no quorum. stop both Admin2 and
         * Admin3, and then restart Admin3. It doesn't suffice to just stop
         * Admin2, because if Admin3 was the master, it will not lose mastership
         * until there is an explicit write, and it would still be able to
         * provide an ClientAdminService.
         * For yucks and code coverage of error paths, also shutdown SN2,
         */
        logger.info("Shutdown SN2");
        ru = new RegistryUtils(
            topo, createStore.getSNALoginManager(new StorageNodeId(2)),
            logger);
        StorageNodeAgentAPI snai2 = ru.getStorageNodeAgent(new StorageNodeId(2));
        assertNotNull(snai2);
        snai2.shutdown(true, true);

        logger.info("Stop Admin2 on SN3");
        ru = new RegistryUtils(
            topo, createStore.getSNALoginManager(new StorageNodeId(3)),
            logger);
        StorageNodeAgentAPI snai3 = ru.getStorageNodeAgent(new StorageNodeId(3));
        assertNotNull(snai3);
        assertTrue(snai3.stopAdmin(false));

        logger.info("Stop Admin3 on SN5");
        ru = new RegistryUtils(
           topo, createStore.getSNALoginManager(new StorageNodeId(5)), logger);
        StorageNodeAgentAPI snai5 = ru.getStorageNodeAgent(new StorageNodeId(5));
        assertNotNull(snai5);
        assertTrue(snai5.stopAdmin(false));

        /* Restart, to ensure that this Admin is the master. */
        assertTrue(snai5.startAdmin());

        /* Wait for admin to restart */
        createStore.getAdmin(4, 30 /* seconds */);

        logger.info("No admin quorum, should be no ddl service available.");
        finder = new FindClientAdminService(
            topo, logger, createStore.getSNALoginManager(0),
            null /* clientId */, Protocols.get(createStore.createKVConfig()));
        logger.info("After finding, target info = " + finder.getTargets());
        service = finder.getDDLService();
        checkInspected(finder, 1, 2, 3, 4, 5);
        List<StorageNodeId> problematic = finder.getProblematic();
        assertEquals(1, problematic.size());
        assertEquals(2, problematic.get(0).getStorageNodeId());
        assertTrue(service == null);
    }

    /**
     * Check which SNs were searched by FindClientAdminService.
     *
     * @param finder
     * @param sns
     *            The SNs which should have been searched.
     */
    private void checkInspected(FindClientAdminService finder, int... sns) {
        List<StorageNodeId> inspected = finder.getInspected();
        List<StorageNodeId> expected = new ArrayList<StorageNodeId>();
        for (int i : sns) {
            expected.add(new StorageNodeId(i));
        }

        assertEquals(expected, inspected);
    }
}
