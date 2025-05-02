/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.tif;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeoutException;

import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.StorageNodeMap;
import oracle.kv.impl.util.KVRepTestConfig;

import org.junit.Test;

/**
 * Unit tests to test TextIndexFeederManager.
 */
public class TextIndexFeederManagerTest extends TextIndexFeederTestBase {

    private final String esClusterName = "tif-test-es-cluster";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        config = new KVRepTestConfig(this, NUM_DC, NUM_SN, REP_Factor,
                                     numPartitions);
        System.setProperty("es.path.home", System.getProperty("testdestdir"));

        final int esHttpPort =
            Integer.parseInt(System.getProperty("es.http.port"));
        final String esClusterMembers =
            "localhost:" + Integer.toString(esHttpPort);

        createSnaConfig(esClusterMembers, esClusterName);
    }

    @Override
    public void tearDown() throws Exception {
        config.stopRNs();
        super.tearDown();
    }

    /**
     * Tests that when a master starts up, TextIndexFeederManager will also
     * starts up and check if any text index exists. If yes, it will start
     * a TextIndexFeeder within the manager.
     *
     * @throws Exception
     */
    @Test
    public void testTIFManager() throws Exception {

        /* for each SN, add search parameters */
        final StorageNodeMap snMap = config.getTopology().getStorageNodeMap();
        StorageNode[] sns = new StorageNode[snMap.size()];
        sns = snMap.getAll().toArray(sns);

        for (StorageNode sn : sns) {
            final StorageNodeId snId = sn.getStorageNodeId();
            final StorageNodeParams params = config.getStorageNodeParams(snId);
            params.setSearchClusterMembers(esClusterName);
        }

        /* create env without loading any data */
        prepareTestEnv(false);

        final RepNodeId rg1masterId = new RepNodeId(1, 1);
        final RepNodeId rg1replicaId = new RepNodeId(1, 2);

        final RepNode rg1rn1 = config.getRN(rg1masterId);
        final RepNode rg1rn2 = config.getRN(rg1replicaId);

        final TextIndexFeederManager tifMaster =
                                         rg1rn1.getTextIndexFeederManager();
        assert (tifMaster != null);

        final TextIndexFeederManager tifReplica =
                                         rg1rn2.getTextIndexFeederManager();
        assert (tifReplica != null);

        /* verify manager status */
        assertTrue("On master TIF manager should return master status",
                   tifMaster.isOnMaster());
        assertFalse("On replica TIF manager should return non-master status",
                    tifReplica.isOnMaster());

        /* verify TIF does not start on both master and replica */
        assertFalse("TIF on master must not run since no text index defined",
                    tifReplica.isTIFRunning());
        assertFalse("TIF on replica should never start",
                    tifReplica.isTIFRunning());

        /* mark the text index as ready */
        metadata.updateIndexStatus(null, "JokeIndex", jokeTable.getFullName(),
                                   IndexImpl.IndexStatus.READY);
        final boolean succ = updateMetadata(rg1rn1, metadata);
        assertTrue("Expect successfully update the table metadata", succ);

        /* restart mater */
        logger.info("now restart master " + rg1masterId);
        final RepNodeService rg1Service =
                                 config.getRepNodeService(rg1masterId);
        rg1Service.stop(true, "test");
        config.startRepNodeSubset(rg1masterId);
        logger.info("master " + rg1masterId + "restarted.");

        /* sleep to allow TIF starts */
        waitForTIFonMasterUp(1000, 10000);

        /*
         * After restart, the manager should start TIF on master because of
         * existent text index, but on replica the manager should never start
         * TIF.
         */
        for (RepNode rn : config.getRNs()) {
            final TextIndexFeederManager manager =
                                             rn.getTextIndexFeederManager();
            if (manager.isOnMaster()) {
                assertTrue("TIF on master should be created due to text index",
                           manager.getTextIndexFeeder() != null);
                assertTrue("TIF on master should be running now",
                           manager.isTIFRunning());
            } else {
                assertTrue("TIF on non-master node should never be created.",
                           manager.getTextIndexFeeder() == null);
            }
        }
    }

    /* wait for TIF on master starts up */
    private void waitForTIFonMasterUp(int pollIntervalMs, int pollTimeoutMs)
        throws TimeoutException {

        boolean success = new PollCondition(pollIntervalMs, pollTimeoutMs) {
            @Override
            protected boolean condition() {

                for (RepNode rn : config.getRNs()) {
                    final TextIndexFeederManager manager =
                        rn.getTextIndexFeederManager();
                    if (manager.isOnMaster()) {
                        return (manager.getTextIndexFeeder() != null) &&
                               (manager.isTIFRunning());
                    }
                }

                return false;
            }
        }.await();

        /* if timeout */
        if (!success) {
            throw new TimeoutException("timeout in polling test ");
        }
    }
}
