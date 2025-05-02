/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static oracle.kv.util.CreateStore.MB_PER_SN;
import static oracle.kv.util.CompareTo.greaterThan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Value;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KVSTestBase;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;

import com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStats;

import org.junit.Test;

/**
 * Verify that RNs in a node can be network restored. NetworkRestore is tested
 * more comprehensively in JE. This test is intended to exercise the ILE
 * path in an RN; the ILE results in a network restore operation.
 */
public class KVSNetworkRestoreTest extends KVSTestBase {

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
    }

    /**
     * Exercise network restore in a KVS setting.
     *
     * 1) Create 1 1X2 KVS. An RF of 2 is required to force an ILE.
     * 2) Shut down both RNs
     * 3) Bring up rg1-rn1, it cannot become the master on its own.
     * 4) Remove log files from rg1-rn2
     * 5) Bring up rg1-rn2. It must do a NT from rg1-rn1
     *
     * The above actions are accomplished via Admin commands.
     */
    @Test
    public void testBasic ()
        throws Exception {

        /* Set up a single shard KVS, with a RF of two to facilitate NR */
        initTopology(2, MB_PER_SN, 2, 1, 1);

        final CommandServiceAPI cs = sysAdminInfo.getCommandService();

        /*
         * Populate the store with enough data that we can get the network
         * restore statistics while the operation is in progress
         */
        final long start = System.currentTimeMillis();
        final String storename = cs.getStoreName();
        final URI rmiAddr = cs.getMasterRmiAddress();
        final String hostname = rmiAddr.getHost();
        final int port = rmiAddr.getPort();
        final KVStore store = KVStoreFactory.getStore(
            new KVStoreConfig(storename, hostname + ":" + port));
        final byte[] bytes = new byte[10000];
        for (int i = 0; i < 20000; i++) {
            Arrays.fill(bytes, (byte) i);
            store.put(Key.createKey("k" + i), Value.createValue(bytes));
        }
        logger.fine("Populate time: " + (System.currentTimeMillis() - start) +
                    " ms");

        /* Stop all RNS */
        int planNum = cs.createStopAllRepNodesPlan("stop-all-rns");
        executePlan(planNum);

        /* Start rg1-rn1, it remains in the unknown state. */
        HashSet<RepNodeId> rnIds = new HashSet<RepNodeId>();
        rnIds.add(new RepNodeId(1,1));
        planNum = cs.createStartServicesPlan("start-rg1-rn1", rnIds);
        executePlan(planNum);

        /*
         * Rename the env dir anc create a new empty env dir to force a NR
         * exception on rg1-rn2.
         */
        final RepNodeId rg1rn2Id = new RepNodeId(1,2);
        File rg1rn2EnvDir = getEnvDir(TestUtils.getTestDir(), rg1rn2Id);
        final File envBackup = new File(rg1rn2EnvDir.getParent(), "env.backup");
        rg1rn2EnvDir.renameTo(envBackup);
        /* Create a new empty one in its place. */
        rg1rn2EnvDir.mkdir();

        /* Should cause NR */
        rnIds.clear(); rnIds.add(rg1rn2Id);

        /*
         * Create a thread to check that the RN comes up and to obtain its
         * network restore stats.  This operation needs to happen before the
         * plan to create the RN is complete in order to get the network
         * restore stats before they disappear.
         */
        class CheckThread extends Thread {
            volatile boolean active;
            volatile int statsCount;
            volatile NetworkBackupStats savedStats;
            CheckThread() {
                setDaemon(true);
            }
            @Override
            public void run() {
                active = new PollCondition(100, 30000) {
                    @Override
                    protected boolean condition() {
                        try {
                            RepNodeAdminAPI rg1rn2 =
                                regUtils.getRepNodeAdmin(rg1rn2Id);
                            RepNodeStatus status = rg1rn2.ping();
                            NetworkBackupStats stats =
                                status.getNetworkRestoreStats();
                            if (stats != null) {
                                statsCount++;
                                savedStats = stats;
                            }
                            return status.getReplicationState().isActive();
                        } catch (Exception e) {
                            return false;
                        }
                    }
                }.await();
            }
        }
        CheckThread t = new CheckThread();
        t.start();

        planNum = cs.createStartServicesPlan("start-rg1-rn2", rnIds);
        executePlan(planNum);

        t.join();
        assertTrue("RN active", t.active);
        assertNotNull("Network restore stats", t.savedStats);
        logger.info("Stats count: " + t.statsCount);
        logger.info("Last stats: " + t.savedStats);
        assertEquals("backupFileCount", 1, t.savedStats.getBackupFileCount());
        assertThat("expectedBytes", t.savedStats.getExpectedBytes(),
                   greaterThan(200000000L));
        assertThat("transferRate", t.savedStats.getTransferRate(),
                   /* Very minimal requirement */
                   greaterThan(1000L * 60));
        assertThat("transferredBytes", t.savedStats.getTransferredBytes(),
                   greaterThan(100000000L));
    }

    /**
     * Returns the env dir associated with the RN
     */
    private File getEnvDir(File testDir, RepNodeId rnId) {
        return new File(findRNDir(testDir, rnId.getFullName()), "env");

    }

    /* Recursive search for the named directory. */
    private File findRNDir(File testDir, String rnDirName) {
        for (File f : testDir.listFiles()) {
            if (!f.isDirectory()) {
                continue;
            }
            if (f.getName().equals(rnDirName)) {
                return f;
            }

            final File rnDirFile = findRNDir(f, rnDirName);
            if (rnDirFile != null) {
                return rnDirFile;
            }
        }

        return null;
    }
}
