/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.Value;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.util.CreateStore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test KVStoreConfig.setReadZones() does correct dispatch. */
public class ReadZonesTest extends TestBase {
    private final String expectedReadZone = "Zone3";
    private final int numberOfZones = 3;

    private final int[] RFs = new int[numberOfZones];

    private CreateStore createStore;
    private KVStoreImpl kvstore;

    private Response response;

    private Key key;
    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        /* All zones use the same replicationFactor */
        int replicationFactor = 1;
        Arrays.fill(RFs, replicationFactor);

        int startPort = 5000;
        createStore = new CreateStore(getKVStoreName(),
                startPort,
                numberOfZones * replicationFactor /* numStorageNodes */,
                CreateStore.ZoneInfo.primaries(RFs),
                3 /* numPartitions */,
                1 /* capacity */
                );
        createStore.start();
        String helperHost = "localhost:" + startPort;
        KVStoreConfig config = new KVStoreConfig(getKVStoreName(),
                helperHost);
        config.setReadZones(expectedReadZone);

        kvstore = (KVStoreImpl) KVStoreFactory.getStore(config);
        /* Setup TestHook<Response> to retrieve the response object */
        kvstore.setBeforeExecuteResultHook(arg -> response = arg);
        key = Key.createKey("TheKey");
        kvstore.putIfAbsent(key, Value.createValue("TheValue".getBytes()));
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        createStore.shutdown();
    }

    @Test
    public void testReadZones() {
        int tries = 20;
        for(int i = 0; i < tries; i++) {
            kvstore.get(key, Consistency.NONE_REQUIRED,
                    50, TimeUnit.MILLISECONDS);
            assertEquals(expectedReadZone, getZone(response));
        }
    }

    private String getZone(Response res) {
        return getZoneName(res.getRespondingRN());
    }

    private String getZoneName(final RepNodeId rnId) {
        final Topology topo = kvstore.getTopology();
        final Datacenter zone = topo.getDatacenter(rnId);
        return zone.getName();
    }
}
