/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.mgmt.jmx;

import static org.junit.Assert.assertEquals;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.mgmt.MgmtSystemTestBase;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.sna.collector.CollectorRecorder;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.util.CreateStore;
import oracle.nosql.common.sklogger.SkLogger;

import org.junit.Test;

/**
 * Test that a pluggable recorder can be loaded dynamically
 */
public class OptionalRecorderTest extends MgmtSystemTestBase {

    public static int createdFoo = 0;
    public static int closedFoo = 0;
    public static int createdBar = 0;
    public static int closedBar = 0;

    int registryPort[];

    @Override
    public void setUp()
        throws Exception {

        super.setUp();

        TestUtils.clearTestDirectory();

    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    @Test
    public void testAll ()
        throws Exception {
        TestStatus.setActive(true);
        store = new CreateStore(kvstoreName,
                                startPort,
                                1,     /* Storage nodes */
                                1,     /* Replication factor */
                                10,   /* Partitions */
                                1,     /* Capacity */
                                CreateStore.MB_PER_SN,
                                true, /* Use threads */
                                "oracle.kv.impl.mgmt.jmx.JmxAgent");
        if (SECURITY_ENABLE) {
            System.setProperty("javax.net.ssl.trustStore",
                               store.getTrustStore().getPath());
        }

        if (SECURITY_ENABLE) {
            store.start();
        } else {
            store.initStorageNodes();
        }
        PortFinder[] pfs = store.getPortFinders();
        registryPort = new int[pfs.length];


        /*
         * No need to deploy again in secure mode.
         */
        if (!SECURITY_ENABLE) {
            store.start();
        }

        /* Create a new recorder */
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.GLOBAL_TYPE);
        map.setParameter("collectorEnabled", "true");
        map.setParameter("collectorRecorder",
                         "oracle.kv.impl.mgmt.jmx.OptionalRecorderTest$FooRecorder");

        CommandServiceAPI cs = store.getAdmin();
        int p = cs.createChangeGlobalComponentsParamsPlan
                ("AddCollectorRecorder1", map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);

        assertEquals(1, createdFoo);
        assertEquals(0, closedFoo);
        assertEquals(0, createdBar);
        assertEquals(0, closedBar);

        /* Run the plan again, nothing should change */
        p = cs.createChangeGlobalComponentsParamsPlan
                ("AddCollectorRecorder2", map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);

        assertEquals(1, createdFoo);
        assertEquals(0, closedFoo);
        assertEquals(0, createdBar);
        assertEquals(0, closedBar);

        /* Change the recorder */
        map.setParameter("collectorRecorder",
                         "oracle.kv.impl.mgmt.jmx.OptionalRecorderTest$BarRecorder");
        p = cs.createChangeGlobalComponentsParamsPlan
                ("AddCollectorRecorder3", map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);

        assertEquals(1, createdFoo);
        assertEquals(1, closedFoo);
        assertEquals(1, createdBar);
        assertEquals(0, closedBar);

    }

    /**
     * Test that this class can be dynamically created
     */
    public static class FooRecorder implements CollectorRecorder {
        public FooRecorder(@SuppressWarnings("unused") StorageNodeParams snp,
                           @SuppressWarnings("unused") GlobalParams gp,
                           SkLogger sklogger) {
            sklogger.info("---------------Creating a FooRecorder");

            /* Increment a counter as proof that this recorder was created */
            createdFoo++;
        }

        @Override
        public void record(MetricType type, String stat) {
        }

        @Override
        public void updateParams(GlobalParams newGlobalParams,
                                 StorageNodeParams newSNParams) {
        }

        @Override
        public void close() {
            closedFoo++;
        }
    }

    public static class BarRecorder implements CollectorRecorder {
        public BarRecorder(@SuppressWarnings("unused") StorageNodeParams snp,
                           @SuppressWarnings("unused") GlobalParams gp,
                           SkLogger sklogger) {
            sklogger.info("---------------Creating a BarRecorder");

            /* Increment a counter as proof that this recorder was created */
            createdBar++;
        }

        @Override
        public void record(MetricType type, String stat) {
        }

        @Override
        public void updateParams(GlobalParams newGlobalParams,
                                 StorageNodeParams newSNParams) {
        }

        @Override
        public void close() {
            closedBar++;
        }
    }
}
