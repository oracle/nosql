/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.api.RequestHandlerImpl;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.admin.RepNodeInfo;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupMap;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.ReusingThreadPoolExecutor;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Start, stop, reconfigure nodes.
 */
public class ChangeStoreTest extends TestBase {

    private CreateStore createStore;
    private static final int startPort = 5000;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestUtils.clearTestDirectory();
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
        RepNodeService.startTestHook = null;
        RepNodeService.stopRequestedTestHook = null;

        LoggerUtils.closeAllHandlers();
    }

    @Test
    public void testChangeJEParams()
        throws Exception {

        createStore = new CreateStore(kvstoreName, startPort, 1, 1, 10, 1);
        createStore.start();

        CommandServiceAPI cs = createStore.getAdmin();
        Topology t = cs.getTopology();

        /* Check the cache size setting of the one RepNode in this kvstore */
        RepGroupMap groupMap = t.getRepGroupMap();
        int rnCount = 0;
        RepNodeId repNodeId = null;
        for (RepGroup rg : groupMap.getAll()) {
            for (RepNode rn : rg.getRepNodes()) {
                rnCount++;
                repNodeId = rn.getResourceId();
            }
        }
        assertEquals(1, rnCount);
        assertNotNull(repNodeId);

        Parameters parms = cs.getParameters();

        /**
         * The params should be set to 0, which is the default cache size
         * setting for JE. 0 means that JE calculates the cache size.
         */
        RepNodeParams before = parms.get(repNodeId);

        RegistryUtils ru = new RegistryUtils(
            t, createStore.getSNALoginManager(0), logger);
        RepNodeAdminAPI rnai = ru.getRepNodeAdmin(repNodeId);
        RepNodeInfo info = rnai.getInfo();

        /**
         * Cache size should be non-zero.
         */
        long currentSize = info.getEnvConfig().getCacheSize();
        assertTrue(currentSize != 0L);

        /* Change the cache size to half */
        RepNodeParams after = new RepNodeParams(before);
        long newSize = currentSize/2;
        after.setJECacheSize(newSize);

        /*
         * Set a bad JE_HOST_PORT and verify that the plan fails to be created
         */
        after.setJENodeHostPort("nohost:notaport");
        int planId = 0;
        try {
            planId = cs.createChangeParamsPlan
                ("ChangeParams", repNodeId, after.getMap());
            fail("createChangeParamsPlan should have failed");
        } catch (Exception expected) {
            /* success */
        }

        /*
         * Set a JE_HOST_PORT that is valid on this machine so that the
         * validation code can succeed.  The value will not be actually used
         * because this parameter is read-only and will be filtered out during
         * the operation.
         *
         * The host name used below should be a hostname that is not that of
         * "this" host in order to test the fix for [#20901]; however, it also
         * needs to resolve to a real IP address or the validation will fail.
         * The call to getValidHost() returns a host name in order of
         * preference.
         */
        String hostPort = getValidHost() + ":12345";
        after.setJENodeHostPort(hostPort);
        planId = cs.createChangeParamsPlan
            ("ChangeParams", repNodeId, after.getMap());
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        /* Check the new size. */
        rnai = ru.getRepNodeAdmin(repNodeId);
        info = rnai.getInfo();
        assertEquals(newSize, info.getEnvConfig().getCacheSize());
    }

    /**
     * Return a host name that can resolve to an IP address.  If it is
     * accessible use a well-known host on the internet, if not, use
     * localhost.
     */
    private static String getValidHost() {
        final String wellKnown="www.oracle.com";
        try {
            InetAddress.getByName(wellKnown);
            return wellKnown;
        } catch (Exception ignored) {
        }
        return "localhost";
    }

    /**
     * Test that changing async exec parameters has the desired effect.
     */
    @Test
    public void testChangeAsyncExecParams() throws Exception {

        assumeTrue("Only test using async",
                   oracle.kv.util.TestUtils.useAsync());

        /* Arrange to get the RepNodeService instance */
        final AtomicReference<RepNodeService> repNodeService =
            new AtomicReference<>();
        RepNodeService.startTestHook = rns -> { repNodeService.set(rns); };

        /* Create the store using threads so the hooks will work */
        createStore = new CreateStore(
            kvstoreName, startPort, 1 /* SNs */, 1 /* RF */,
            10 /* partitions */, 1 /* capacity */,
            CreateStore.MB_PER_SN /* memoryMB */, true /* useThreads */,
            null /* mgmtImpl */);
        createStore.start();

        /* Get async thread pool and its parameters */
        assertTrue("Waiting for RepNodeService",
                   new PollCondition(500, 10000) {
                       @Override
                       protected boolean condition() {
                           return repNodeService.get() != null;
                       }
                   }.await());
        RequestHandlerImpl rh = repNodeService.get().getReqHandler();
        ReusingThreadPoolExecutor asyncThreadPool = rh.getAsyncThreadPool();
        assertNotNull(asyncThreadPool);
        int maxThreads = asyncThreadPool.getMaximumPoolSize();
        int keepAliveMs = (int) asyncThreadPool.getKeepAliveTime(MILLISECONDS);
        int queueSize = asyncThreadPool.getQueueCapacity();

        /* Get current parameters */
        CommandServiceAPI cs = createStore.getAdmin();
        RepGroupMap groupMap = cs.getTopology().getRepGroupMap();
        assertEquals(1, groupMap.size());
        RepGroup rg = groupMap.getAll().iterator().next();
        assertEquals(1, rg.getRepNodes().size());
        RepNodeId rnId = rg.getRepNodes().iterator().next().getResourceId();
        RepNodeParams rnParams = cs.getParameters().get(rnId);

        /* Check for RepNodeService stop requests */
        final AtomicReference<Boolean> stopRequested =
            new AtomicReference<>(false);
        RepNodeService.stopRequestedTestHook =
            ignore -> stopRequested.set(true);

        /*
         * Change the maxThreads and keepAliveMs async thread pool parameters
         */
        rnParams.setAsyncExecMaxThreads(--maxThreads);
        rnParams.setAsyncExecThreadKeepAliveMs(++keepAliveMs);
        queueSize++;
        int planId = cs.createChangeParamsPlan(
            "changeParams1", rnId, rnParams.getMap());
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        /* Check results */
        rh = repNodeService.get().getReqHandler();
        asyncThreadPool = rh.getAsyncThreadPool();
        assertEquals(maxThreads, asyncThreadPool.getMaximumPoolSize());
        assertEquals(keepAliveMs,
                     asyncThreadPool.getKeepAliveTime(MILLISECONDS));
        assertEquals(queueSize, asyncThreadPool.getQueueCapacity());
        assertTrue("RN should be stopped", stopRequested.get());

        /* Changing max requests should change queue size. Increase first. */
        stopRequested.set(false);
        int maxRequests = rnParams.getAsyncMaxConcurrentRequests();
        rnParams.setAsyncMaxConcurrentRequests(++maxRequests);
        queueSize++;
        planId = cs.createChangeParamsPlan(
            "changeParams2", rnId, rnParams.getMap());
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        /* Check results */
        rh = repNodeService.get().getReqHandler();
        asyncThreadPool = rh.getAsyncThreadPool();
        assertEquals(maxThreads, asyncThreadPool.getMaximumPoolSize());
        assertEquals(keepAliveMs,
                     asyncThreadPool.getKeepAliveTime(MILLISECONDS));
        assertEquals(queueSize, asyncThreadPool.getQueueCapacity());
        assertTrue("RN should be stopped", stopRequested.get());

        /* Then try decreasing */
        stopRequested.set(false);
        rnParams.setAsyncMaxConcurrentRequests(--maxRequests);
        queueSize--;
        planId = cs.createChangeParamsPlan(
            "changeParams3", rnId, rnParams.getMap());
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        /* Check results */
        rh = repNodeService.get().getReqHandler();
        asyncThreadPool = rh.getAsyncThreadPool();
        assertEquals(maxThreads, asyncThreadPool.getMaximumPoolSize());
        assertEquals(keepAliveMs,
                     asyncThreadPool.getKeepAliveTime(MILLISECONDS));
        assertEquals(queueSize, asyncThreadPool.getQueueCapacity());
        assertTrue("RN should be stopped", stopRequested.get());

        /* Try changing just the keep alive, should not restart */
        stopRequested.set(false);
        rnParams.setAsyncExecThreadKeepAliveMs(++keepAliveMs);
        planId = cs.createChangeParamsPlan(
            "changeParams4", rnId, rnParams.getMap());
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        /* Check results */
        rh = repNodeService.get().getReqHandler();
        asyncThreadPool = rh.getAsyncThreadPool();
        assertEquals(maxThreads, asyncThreadPool.getMaximumPoolSize());
        assertEquals(keepAliveMs,
                     asyncThreadPool.getKeepAliveTime(MILLISECONDS));
        assertEquals(queueSize, asyncThreadPool.getQueueCapacity());
        assertFalse("RN should not be stopped", stopRequested.get());
    }
}
