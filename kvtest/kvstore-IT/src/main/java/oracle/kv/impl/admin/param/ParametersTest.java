/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.param;

import static org.junit.Assert.assertEquals;

import java.io.File;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.GeneralStore;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.NodeType;

/**
 * Tests Parameters Objects, interfaces and persistability.
 */
public class ParametersTest extends TestBase {

    private static final File TEST_DIR = TestUtils.getTestDir();
    private static final String DEFAULT_KVSNAME = "TestKvstore";

    private int sequence = 1; /* Sequence number for generating ResourceIds */

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        LoggerUtils.closeAllHandlers();
    }

    /**
     * Exercise the basic interfaces, setting defaults, globals, and
     * per-node parameters.
     */
    @Test
    public void testBasic()  {

        Parameters params = new Parameters(DEFAULT_KVSNAME);

        GlobalParams gp = params.getGlobalParams();
        assertEquals(gp.getKVStoreName(), DEFAULT_KVSNAME);

        /* Make a couple of dummy ids. */
        StorageNodeId snid = new StorageNodeId(sequence++);
        RepNodeId rnid = new RepNodeId(sequence++, sequence++);
        DatacenterId dcid = new DatacenterId(sequence++);

        assertEquals(params.get(snid), null);
        assertEquals(params.get(rnid), null);

        DatacenterParams testDCP = new DatacenterParams(dcid, "Panacea FL");

        StorageNodeParams testSNP =
            new StorageNodeParams(snid, "localhost", 1000, null);
        params.add(testDCP);
        assertEquals(params.get(dcid), testDCP);
        params.add(testSNP);
        assertEquals(params.get(snid), testSNP);
        params.remove(snid);
        assertEquals(params.get(snid), null);

    }

    @Test
    public void testPersistence() {

        final int n = 3;
        Parameters params = new Parameters(DEFAULT_KVSNAME);

        /* Make a few more dummy ids. */
        StorageNodeId snids[] = new StorageNodeId[n];
        RepNodeId rnids[] = new RepNodeId[n];
        DatacenterId dcids[] = new DatacenterId[n];

        for (int i = 0; i < n; i++) {
            snids[i] = new StorageNodeId(sequence++);
            rnids[i] = new RepNodeId(sequence++, sequence++);
            dcids[i] = new DatacenterId(sequence++);
        }

        for (int i = 0; i < n; i++) {
            DatacenterParams dcp =
                new DatacenterParams(dcids[i], "datacenter" + i);

            params.add(dcp);

            StorageNodeParams snp =
                new StorageNodeParams(snids[i], "host" + i, 1000+i,
                                      Integer.toString(i));
            params.add(snp);

            ParameterMap newMap = params.copyPolicies();
            RepNodeParams rnp =
                new RepNodeParams(newMap,
                                  snids[i],
                                  rnids[i],
                                  false,
                                  null, 0, null,
                                  null, 0L,  /* mountPoint, size */
                                  null, 0L, /* RN log mount point, size*/
                                  NodeType.ELECTABLE);
            rnp.setJECacheSize(i *100);
            params.add(rnp);
        }

        /* Store this params instance. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        Environment env = new Environment(TEST_DIR, envConfig);
        TransactionConfig tconfig = new TransactionConfig();
        Transaction txn = env.beginTransaction(null, tconfig);

        final GeneralStore store = GeneralStore.getReadTestInstance(env);

        store.putParameters(txn, params);
        txn.commit();

        /* Retrieve it and verify. */
        txn = env.beginTransaction(null, tconfig);
        Parameters m2 = store.getParameters(txn);
        txn.commit();

        GlobalParams gp = m2.getGlobalParams();
        assertEquals(gp.getKVStoreName(), DEFAULT_KVSNAME);

        for (int i = 0; i < n; i++) {
            DatacenterParams dcp = m2.get(dcids[i]);
            assertEquals("datacenter" + i, dcp.getName());

            StorageNodeParams snp = m2.get(snids[i]);
            assertEquals(i, Integer.parseInt(snp.getComment()));
            assertEquals("host" + i, snp.getHostname());

            RepNodeParams rnp = m2.get(rnids[i]);
            assertEquals(i * 100, rnp.getJECacheSize());

            /* Modify some part of the params. */
            snp.setComment(Integer.toString(i * n));
            rnp.setJECacheSize(200 * i);
        }

        /* Update persistent copy. */
        /* DatacenterParams is immutable... */
        txn = env.beginTransaction(null, tconfig);
        store.putParameters(txn, m2);
        txn.commit();

        /* Retrieve and verify. */
        txn = env.beginTransaction(null, tconfig);
        Parameters m3 = store.getParameters(txn);
        txn.commit();

        for (int i = 0; i < n; i++) {
            StorageNodeParams snp = m3.get(snids[i]);
            assertEquals(i * n, Integer.parseInt(snp.getComment()));
            RepNodeParams rnp = m3.get(rnids[i]);
            assertEquals(200 * i, rnp.getJECacheSize());
        }

        store.close();
        env.close();
    }
}
