/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.sna.StorageNodeTestBase;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterMap;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.StorageNodeMap;
import oracle.kv.impl.topo.Topology;

import org.junit.Test;

public class AdminServiceTest extends AdminTestBase {

    private int nPlans = 0;
    private int nStorageNodes = 0;

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    @Test
    public void testBasic()
        throws Exception {

        int registryPort = portFinder1.getRegistryPort();
        String hostname = "localhost";
        logger.info("Looking up admin: " + GlobalParams.COMMAND_SERVICE_NAME);
        CommandServiceAPI cs =
            StorageNodeTestBase.waitForAdmin(hostname, registryPort, 5);
        assert(cs != null);

        cs.configure(kvstoreName);

        /* Disable the  KVAdminMetadata and KVVersion thread. */
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.ADMIN_TYPE);
        map.setParameter(ParameterState.AP_PARAM_CHECK_ENABLED, "false") ;
        map.setParameter(ParameterState.AP_VERSION_CHECK_ENABLED, "false");
        int p = cs.createChangeAllAdminsPlan("changeAdminParams", null, map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
        cs.assertSuccess(p);
        Map<Integer, Plan> plans = getAllPlans(cs);
        assertEquals(plans.size(), ++nPlans);

        DatacenterId dcid = deployDatacenter(cs);

        /* Deploy a StorageNode */
        StorageNodeId sn1id =
            deployStorageNode(cs, dcid, portFinder1.getRegistryPort());

        /* Deploy an Admin */
        deployAdmin(cs, sn1id, AdminType.PRIMARY);

        /* Deploy another StorageNode */
        deployStorageNode(cs, dcid, portFinder2.getRegistryPort());
    }

    @Test
    public void testSecondaryAdmin()
        throws Exception {
        logger.info("Looking up admin: " + GlobalParams.COMMAND_SERVICE_NAME);
        final CommandServiceAPI cs =
                StorageNodeTestBase.waitForAdmin("localhost",
                                                 portFinder1.getRegistryPort(),
                                                 5);
        assert(cs != null);

        cs.configure(kvstoreName);

        /* Disable the  KVAdminMetadata and KVVersion thread. */
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.ADMIN_TYPE);
        map.setParameter(ParameterState.AP_PARAM_CHECK_ENABLED, "false") ;
        map.setParameter(ParameterState.AP_VERSION_CHECK_ENABLED, "false") ;
        int p = cs.createChangeAllAdminsPlan("changeAdminParams", null, map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
        cs.assertSuccess(p);
        Map<Integer, Plan> plans = getAllPlans(cs);
        assertEquals(plans.size(), ++nPlans);

        final DatacenterId dcid = deployDatacenter(cs);

        /* Deploy a StorageNode */
        final StorageNodeId sn1id =
            deployStorageNode(cs, dcid, portFinder1.getRegistryPort());

        /* Attempt to deploy a Seondary Admin as the first (only) Admin */
        try {
            deployAdmin(cs, sn1id, AdminType.SECONDARY);
            fail("Deploy Admin should have failed because no Primary Admin");
        } catch (AdminFaultException afe) {
            /* Failure should be IllegalCommandException */
            if (!afe.getFaultClassName().
                            equals(IllegalCommandException.class.getName())) {
                throw afe;
            }
        }
        /* null type should default to type of zone (primary) */
        deployAdmin(cs, sn1id, null);

        /* Deploy another StorageNode for the secondary Admin */
        final StorageNodeId sn2id =
                deployStorageNode(cs, dcid, portFinder2.getRegistryPort());

        deployAdmin(cs, sn2id, AdminType.SECONDARY);

        for (AdminParams ap : cs.getParameters().getAdminParams()) {
            if (ap.getType().isSecondary()) {
                return;
            }
        }
        fail("Did not find a Secondary Admin");
    }

    /**
     * Create and run a DeployDatacenterPlan
     *
     * Check a couple of error conditions before succeeding.
     *
     * @throws ExecutionException
     * @throws TimeoutException
     */
    DatacenterId deployDatacenter(CommandServiceAPI cs)
        throws RemoteException {

        try {
            cs.createDeployDatacenterPlan(
                "deploy data center", "Miami", -5, DatacenterType.PRIMARY,
                false, false);
            fail("Plan creation should have failed because of bad RF");
        } catch (AdminFaultException afe) {
            /* pass */
            assertTrue(afe.getFaultClassName().indexOf("IllegalCommand") != -1);
        }

        try {
            cs.createDeployDatacenterPlan(
                "deploy data center", "Miami", 21, DatacenterType.PRIMARY,
                false, false);
            fail("Plan creation should have failed because of bad RF");
        } catch (AdminFaultException afe) {
            /* pass */
            assertTrue(afe.getFaultClassName().indexOf("IllegalCommand") != -1);
        }

        final int ddcid = cs.createDeployDatacenterPlan(
            "deploy data center", "Miami", 1, DatacenterType.PRIMARY, false,
            false);

        Map<Integer, Plan> plans = getAllPlans(cs);
        assertEquals(plans.size(), ++nPlans);

        /*
         * This may seem pointless.  I think there will be other flavors of
         * getPlanIds, with filters, for which this assertion will not hold
         * true.
         */
        assertNotNull(plans.get(ddcid));

        cs.approvePlan(ddcid);
        cs.executePlan(ddcid, false);
        cs.awaitPlan(ddcid, 0, null);
        cs.assertSuccess(ddcid);

        Topology t = cs.getTopology();

        DatacenterMap dcmap = t.getDatacenterMap();
        assertEquals(dcmap.size(), 1);

        Datacenter dc = dcmap.getAll().iterator().next();

        assertEquals(dc.getName(), "Miami");

        return dc.getResourceId();
    }

    /* Create and run a DeployStorageNode plan. */
    StorageNodeId deployStorageNode(CommandServiceAPI cs, DatacenterId dcid,
                                    int registryPort)
        throws RemoteException {

        int dsnid = cs.createDeploySNPlan
            ("deploy storage node", dcid, "localhost", registryPort,
             Integer.toString(nStorageNodes++));

        Map<Integer, Plan> plans = getAllPlans(cs);
        assertEquals(plans.size(), ++nPlans);

        assertNotNull(plans.get(dsnid));

        cs.approvePlan(dsnid);
        cs.executePlan(dsnid, false);
        cs.awaitPlan(dsnid, 0, null);
        cs.assertSuccess(dsnid);

        /* Check the resulting topology changes. */
        Topology t = cs.getTopology();

        StorageNodeMap smap = t.getStorageNodeMap();
        assertEquals(smap.size(), nStorageNodes);

        Collection<StorageNode> allSNs = smap.getAll();

        StorageNode sn = null;
        for (StorageNode s : allSNs) {
            if (s.getRegistryPort() == registryPort) {
                sn = s;
                break;
            }
        }
        assertNotNull(sn);

        assertEquals(sn.getDatacenterId(), dcid);

        /* Check that the default storage node pool was augmented. */

        List<StorageNodeId> pool = cs.getStorageNodePoolIds
            (Parameters.DEFAULT_POOL_NAME);

        assertEquals(pool.size(), nStorageNodes);

        return sn.getResourceId();
    }

    /* Create and run a DeployAdmin plan */
    private void deployAdmin(CommandServiceAPI cs, StorageNodeId snid,
                             AdminType type)
        throws RemoteException {

        int daid = cs.createDeployAdminPlan("deploy admin", snid, type);

        Map<Integer, Plan> plans = getAllPlans(cs);
        assertEquals(plans.size(), ++nPlans);

        assertNotNull(plans.get(daid));

        cs.approvePlan(daid);

        cs.executePlan(daid, false);
        cs.awaitPlan(daid, 0, null);
        cs.assertSuccess(daid);

        /* Disable the  KVAdminMetadata and KVVersion thread. */
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.ADMIN_TYPE);
        map.setParameter(ParameterState.AP_PARAM_CHECK_ENABLED, "false") ;
        map.setParameter(ParameterState.AP_VERSION_CHECK_ENABLED, "false") ;
        int p = cs.createChangeAllAdminsPlan("changeAdminParams", null, map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
        cs.assertSuccess(p);
        plans = getAllPlans(cs);
        assertEquals(plans.size(), ++nPlans);

        /* TODO: to be continued */
    }

    /* Try to subvert the log download servlet */
    void exerciseLogDownLoadServlet(String hostname, int httpPort)
        throws Exception {

        String prefix = "http://" + hostname + ":" + httpPort +
            "/kvadminui/LogDownloadService";

        /* The log parameter is absent. */
        URL url = new URI(prefix).toURL();
        HttpURLConnection c = (HttpURLConnection)url.openConnection();
        assertEquals(500, c.getResponseCode());
        InputStream is = c.getErrorStream();
        assertTrue
            ("Expected error message not present",
             streamToString(is).contains("The log parameter is missing"));
        is.close();

        /* The log parameter has an incorrect suffix, or no suffix. */
        url = new URI(prefix + "?log=___nosuchfile").toURL();
        c = (HttpURLConnection)url.openConnection();
        assertEquals(500, c.getResponseCode());
        is = c.getErrorStream();
        assertTrue
            ("Expected error message not present",
             streamToString(is).contains
             ("The log parameter has an incorrect suffix"));
        is.close();

        /* A classic pathname traversal url. */
        url = new URI(prefix + "?log=../../../../../etc/___nosuchfile.log")
            .toURL();
        c = (HttpURLConnection)url.openConnection();
        assertEquals(500, c.getResponseCode());
        is = c.getErrorStream();
        assertTrue
            ("Expected error message not present",
             streamToString(is).contains
             ("The log parameter contains an illegal character"));
        is.close();

        /*
         *  The log parameter passes other tests but indicates a nonexistent
         * file.
         */
        url = new URI(prefix + "?log=___nosuchfile.log").toURL();
        c = (HttpURLConnection)url.openConnection();
        assertEquals(500, c.getResponseCode());
        is = c.getErrorStream();
        assertTrue
            ("Expected error message not present",
             streamToString(is).contains
             ("There was an error delivering the log file"));
        is.close();

        /* Finally one that should succeed. */
        url = new URI(prefix + "?log=sn1_0.log").toURL();
        c = (HttpURLConnection)url.openConnection();
        assertEquals(200, c.getResponseCode());
        is = c.getInputStream();
        String s = streamToString(is);
        assertTrue
            ("Expected log contents not present, it was " + s,
             s.contains("Starting StorageNodeAgent"));
        is.close();
    }

    private static String streamToString(InputStream is)
        throws Exception {

        char[] buf = new char[8192];

        StringBuilder sb = new StringBuilder();
        Reader isReader = new InputStreamReader(is, "UTF-8");
        int nRead;
        do {
            nRead = isReader.read(buf, 0, buf.length);
            if (nRead > 0) {
                sb.append(buf, 0, nRead);
            }
        } while (nRead > -1);

        return sb.toString();
    }

    private static Map<Integer, Plan> getAllPlans(CommandServiceAPI cs)
        throws RemoteException {

        final TreeMap<Integer, Plan> collector = new TreeMap<Integer, Plan>();
        int currentId = 1;
        while (true) {
            collector.putAll(cs.getPlanRange(currentId, 10));
            int nextId = collector.lastKey().intValue() + 1;
            if (currentId == nextId) {
                break;
            }
            currentId = nextId;
        }
        final Iterator<Plan> itr = collector.values().iterator();
        while (itr.hasNext()) {
            if (itr.next().isSystemPlan()) {
                itr.remove();
            }
        }
        return collector;
    }
}
