/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.rmi.RemoteException;
import java.util.Enumeration;

import oracle.kv.TestBase;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeTestBase;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Test the mixing of loopback and non-loopback address in the same store.
 * They need to be consistently one or the other.
 */
public class LoopbackAddressTest extends TestBase {

    private final static String KV_NAME = "loopbacktest";
    private final static String LOOPBACK_HOST = "localhost";
    private final static int startPort1 = 13230;
    private final static int startPort2 = 13250;
    private final static int startPort3 = 13270;
    private final static int haRange = 5;

    protected StorageNodeAgent sna1;
    protected StorageNodeAgent sna2;
    protected StorageNodeAgent sna3;
    protected PortFinder portFinder1;
    protected PortFinder portFinder2;
    protected PortFinder portFinder3;

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        if (sna1 != null) {
            sna1.shutdown(true, true);
        }
        if (sna2 != null) {
            sna2.shutdown(true, true);
        }
        if (sna3 != null) {
            sna3.shutdown(true, true);
        }
        LoggerUtils.closeAllHandlers();
    }

    /**
     * Create a store with loopback only addresses.  This works.
     */
    @Test
    public void testLoopbackOnly()
        throws Exception {

        createHomogenous(LOOPBACK_HOST);
    }

    /**
     * Create a store with non-loopback only addresses.  This works.
     */
    @Test
    public void testNoLoopback()
        throws Exception {

        createHomogenous(getNonLocalHost());
    }

    /**
     * Create a store with mixed addresses.  This will fail.
     */
    @Test
    public void testMixed()
        throws Exception {

        portFinder1 = new PortFinder(startPort1, haRange, LOOPBACK_HOST);
        portFinder2 = new PortFinder(startPort2, haRange, LOOPBACK_HOST);
        portFinder3 = new PortFinder(startPort3, haRange,  getNonLocalHost());

        createStorageNodes();
        int registryPort = sna1.getRegistryPort();

        CommandServiceAPI cs =
            StorageNodeTestBase.waitForAdmin(LOOPBACK_HOST, registryPort, 5);
        assert(cs != null);
        cs.configure(KV_NAME);

        DatacenterId dcid = deployDatacenter(cs);

        /*
         * First 2 SNs should work, based on addresses above
         */
        deploySN(cs, dcid, portFinder1);
        deployAdmin(cs, sna1.getStorageNodeId());
        deploySN(cs, dcid, portFinder2);

        /*
         * This should fail
         */
        try {
            deploySN(cs, dcid, portFinder3);
            fail("DeploySN should have failed");
        } catch (Exception expected) {
            assertTrue(expected.getMessage().toLowerCase().
                       indexOf("loopback") > 0);
        }
    }

    /**
     * Returns a non-local hostname for use by the test
     */
    private String getNonLocalHost()
        throws SocketException {

        for (Enumeration<NetworkInterface> interfaces =
            NetworkInterface.getNetworkInterfaces();
            interfaces.hasMoreElements();) {

            for (Enumeration<InetAddress> addresses =
                interfaces.nextElement().getInetAddresses();
                addresses.hasMoreElements();) {

                final InetAddress ia = addresses.nextElement();
                if (! (ia.isLoopbackAddress() ||
                       ia.isAnyLocalAddress() ||
                       ia.isMulticastAddress() ||
                       (ia instanceof Inet6Address))) {
                    /* Found one, any one of these will do. */
                    return ia.getHostAddress();
                }
            }
        }

        fail("Could not find a non-local host name");
        return null;
    }

    /**
     * Create a store with loopback only addresses.  This works.
     */
    private void createHomogenous(String hostname)
        throws Exception {

        portFinder1 = new PortFinder(startPort1, haRange, hostname);
        portFinder2 = new PortFinder(startPort2, haRange, hostname);
        portFinder3 = new PortFinder(startPort3, haRange, hostname);

        createStorageNodes();
        int registryPort = sna1.getRegistryPort();

        CommandServiceAPI cs =
            StorageNodeTestBase.waitForAdmin(hostname, registryPort, 5);
        assert(cs != null);

        cs.configure(KV_NAME);

        final ParameterMap policyMap = mergeParameterMapDefaults(null);
        if (policyMap != null) {
            cs.setPolicies(policyMap);
        }

        DatacenterId dcid = deployDatacenter(cs);

        deploySN(cs, dcid, portFinder1);
        deployAdmin(cs, sna1.getStorageNodeId());
        deploySN(cs, dcid, portFinder2);
        deploySN(cs, dcid, portFinder3);
    }

    /**
     * Deply a storage node.  No need to return anything.  This test only needs
     * to succeed or fail based on this function.
     */
    void deploySN(CommandServiceAPI cs,
                  DatacenterId dcid,
                  PortFinder finder)
        throws Exception {

        int dsnid = cs.createDeploySNPlan
            ("SN", dcid, finder.getHostname(), finder.getRegistryPort(), "");
        cs.approvePlan(dsnid);
        cs.executePlan(dsnid, false);
        cs.awaitPlan(dsnid, 0, null);
        cs.assertSuccess(dsnid);
    }

    void deployAdmin(CommandServiceAPI cs, StorageNodeId snid)
        throws RemoteException {

        int daid = cs.createDeployAdminPlan("deploy admin", snid);
        cs.approvePlan(daid);
        cs.executePlan(daid, false);
        cs.awaitPlan(daid, 0, null);
        cs.assertSuccess(daid);
    }

    DatacenterId deployDatacenter(CommandServiceAPI cs)
        throws RemoteException {

        int ddcid = cs.createDeployDatacenterPlan(
            "deploy data center", "Miami", 1, DatacenterType.PRIMARY, false,
            false);
        cs.approvePlan(ddcid);
        cs.executePlan(ddcid, false);
        cs.awaitPlan(ddcid, 0, null);
        cs.assertSuccess(ddcid);
        Topology topo = cs.getTopology();
        return topo.getSortedDatacenters().get(0).getResourceId();
    }

    void createStorageNodes()
        throws Exception {

        sna1 = StorageNodeTestBase.createUnregisteredSNA
            (portFinder1, 1, "config0.xml", false, true, 512);
        sna2 = StorageNodeTestBase.createUnregisteredSNA
            (portFinder2, 1, "config1.xml", false, false, 512);
        sna3 = StorageNodeTestBase.createUnregisteredSNA
            (portFinder3, 1, "config2.xml", false, false, 512);
    }
}
