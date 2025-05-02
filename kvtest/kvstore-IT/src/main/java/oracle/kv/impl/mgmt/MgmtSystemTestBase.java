/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.mgmt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

/**
 * Common methods for Snmp and Jmx tests.
 */
public class MgmtSystemTestBase extends TestBase {

    protected static final int startPort = 5000;

    protected CreateStore store;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        if (store != null) {
            store.shutdown(true);
        }
        super.tearDown();

        LoggerUtils.closeAllHandlers();
    }

    protected void setAdminPollingInterval()
        throws RemoteException {

        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.ADMIN_TYPE);
        map.setParameter("collectorPollPeriod", "500 MILLISECONDS");

        CommandServiceAPI cs = store.getAdmin();
        int p = cs.createChangeAllAdminsPlan("change poll period", null, map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
    }

    protected void setRepNodeStatsInterval(String duration)
        throws RemoteException {

        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.GLOBAL_TYPE);
        map.setParameter("collectorInterval", duration);

        CommandServiceAPI cs = store.getAdmin();
        int p =
            cs.createChangeGlobalComponentsParamsPlan("change stats interval",
                                                      map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
    }

    protected void setArbNodeStatsInterval(String duration)
        throws RemoteException {

        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.GLOBAL_TYPE);
        map.setParameter("collectorInterval", duration);

        CommandServiceAPI cs = store.getAdmin();
        int p =
            cs.createChangeGlobalComponentsParamsPlan("change stats interval",
                                                      map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
        }

    protected void setRepNodeThroughputLimit(int floor)
        throws RemoteException {

        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.REPNODE_TYPE);
        map.setParameter(ParameterState.SP_THROUGHPUT_FLOOR,
                         Integer.toString(floor));

        CommandServiceAPI cs = store.getAdmin();
        int p =
            cs.createChangeAllParamsPlan("change throughput limit", null, map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
    }

    protected void setRepNodeLatencyCeiling(long ceiling)
        throws RemoteException {

        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.REPNODE_TYPE);
        map.setParameter(ParameterState.SP_LATENCY_CEILING,
                         Long.toString(ceiling));

        CommandServiceAPI cs = store.getAdmin();
        int p =
            cs.createChangeAllParamsPlan("change latency ceiling", null, map);
        cs.approvePlan(p);
        cs.executePlan(p, false);
        cs.awaitPlan(p, 0, null);
    }

    /**
     * Perform a bunch of client operations to prime the flow of metrics.
     * Spend howLong seconds doing this.
     */
    protected void doClientOps(long howLong)
        throws Exception {

        long now = new Date().getTime();
        long stopAt = now + (howLong * 1000L);

        KVStore storeHandle = KVStoreFactory.getStore(store.createKVConfig());

        final int nOpsPer = 1000;
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(3);
        while (true) {
            for (int i = 0; i < nOpsPer; i++) {
                Random r = new Random(1L);
                final String keyString = Integer.toString(r.nextInt());
                final Key key = Key.createKey(keyString);
                final Value val =
                    Value.createValue(("woohoo" + keyString).getBytes());

                Version newVersion = storeHandle.put(key, val);
                assertFalse(null == newVersion);
            }

            if (new Date().getTime() >= stopAt) {
                break;
            }

            /*
             * Use multi-threads to do read operations so RequestDispatcher
             * can balance requests to different RNs.
             */
            final List<Callable<Void>> tasks = new ArrayList<>();
            for (int taskNum = 0; taskNum < 3; taskNum++) {
                tasks.add(new Callable<Void>() {
                    @Override
                    public Void call() {
                        for (int i = 0; i < nOpsPer; i++) {
                            Random r = new Random(1L);
                            final String keyString =
                                Integer.toString(r.nextInt());
                            final Key key = Key.createKey(keyString);
                            final Value val = Value.createValue(
                                ("woohoo" + keyString).getBytes());

                            ValueVersion result = storeHandle.get(key);
                            assertEquals(val, result.getValue());
                        }
                        return null;
                    }
                });
            }
            executor.invokeAll(tasks);

            if (new Date().getTime() >= stopAt) {
                break;
            }
        }

        executor.shutdown();
        storeHandle.close();
    }
}
