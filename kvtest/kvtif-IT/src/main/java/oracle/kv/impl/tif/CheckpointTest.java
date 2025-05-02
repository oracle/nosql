/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.tif;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KVRepTestConfig;

import com.sleepycat.je.rep.ReplicatedEnvironment;

import org.junit.Test;

/**
 * Unit test to test CheckpointManager.
 */
public class CheckpointTest extends TextIndexFeederTestBase {

    /* master id of rg1 */
    private static final RepNodeId rg1masterId = new RepNodeId(1, 1);

    private int esHttpPort;
    private static final String esClusterName = "tif-test-es-cluster";

    private CheckpointManager manager;
    private ElasticsearchHandler esHandler;

    private String ckptESIndexName;
    private String ckptESIndexMapping;
    @Override
    public void setUp() throws Exception {
        super.setUp();
        config = new KVRepTestConfig(this, NUM_DC, NUM_SN, REP_Factor,
                                     numPartitions);
        System.setProperty("es.path.home", System.getProperty("testdestdir"));

        esHttpPort = Integer.parseInt(System.getProperty("es.http.port"));

        final String esMembers = "localhost:" + esHttpPort;

        esHandler =
            ElasticsearchHandler.newInstance(esClusterName, esMembers,
                                             false, null, 5000, logger);

        ckptESIndexName = TextIndexFeeder.deriveCkptESIndexName(kvstoreName);
    }

    @Override
    public void tearDown() throws Exception {

        if (esHandler != null) {
            esHandler.deleteESIndex(ckptESIndexName);
            logger.info("Cleanup: ES index " + ckptESIndexName +
                        " is deleted successfully.");

            esHandler.close();
        }

        config.stopRNs();
        super.tearDown();
    }

    /**
     * Test that user is able to do checkpoint, start periodical
     * checkpoint and cancel future checkpoint.
     *
     * @throws Exception
     */
    @Test
    public void testStartAndCancelCheckpoint() throws Exception {
        /* checkpoint interval in seconds */
        final int interval = 3;
        /* timeout to wait for checkpoint to appear in ES */
        final int timeoutMs = 2 * interval * 1000;

        prepare();

        if (!esHandler.existESIndex(ckptESIndexName)) {
            fail("non-existent checkpoint index " + ckptESIndexName);
        } else if (!esHandler.existESIndexMapping(ckptESIndexName)) {
            fail("non-existent index mapping " + ckptESIndexMapping);
        }

        /* checkpoint to ES */
        final CheckpointState expectedState = createCheckpoint();
        manager.doCheckpoint(expectedState);
        logger.info("Checkpoint committed successfully to " + ckptESIndexName +
                    " under mapping " + ckptESIndexMapping);

        /* read ckpt back from ES */
        final CheckpointState fetchedState = manager.fetchCkptFromES();
        logger.info("Checkpoint fetched successfully from " + ckptESIndexName +
                    " under mapping " + ckptESIndexMapping);
        /* verify */
        verifyCheckpointState(expectedState, fetchedState);
        logger.info("Checkpoint match successful");


        /* now start periodical checkpoint */
        manager.setCkptIntervalSecs(interval);
        manager.startCheckpoint(true);

        waitForCheckpointDone(timeoutMs);

        final long numCkptDone = manager.getNumCheckpointDone();
        assertTrue("Expect checkpoint done ", (numCkptDone > 0));
        final Date timeStamp = manager.getLastCheckpointTime();
        logger.info("# of checkpoint done " +
                    manager.getNumCheckpointDone() +
                    ", time of the last checkpoint " +
                    manager.getLastCheckpointTime());

        /* now cancel it! */
        manager.cancelCheckpoint();

        /* sleep for some time and verify */
        logger.info("Now sleep for another " + (timeoutMs / 1000) + " seconds");
        try {
            Thread.sleep(timeoutMs);
        } catch (InterruptedException e) {
            logger.warning(e.getLocalizedMessage());
        }

        assertTrue("Expect no new checkpoint made since cancellation.",
                   numCkptDone == manager.getNumCheckpointDone());
        assertTrue("Expect same last checkpoint timestamp before cancellation",
                   timeStamp.equals(manager.getLastCheckpointTime()));
    }

    /* waits for a checkpoint committed to ES index */
    private void waitForCheckpointDone(int timeoutMs) {

        boolean success = new PollCondition(1000, timeoutMs) {
            @Override
            protected boolean condition() {
                final CheckpointState checkPointState =
                    manager.fetchCkptFromES();
                return (checkPointState != null);
            }
        }.await();
        assert (success);
    }

    /* prepare a test env, and create, load and verify test data */
    private void prepare() {

        prepareTestEnv();

        createESIndex(ckptESIndexName, CheckpointManager.indexProperties);

        /* now we have everything need to test */
        try {
            manager = new CheckpointManager(ckptESIndexName, esHandler, logger);
        } catch (Exception e) {
            assertTrue(e.getMessage(), false);
        }
        logger.info("Test environment created successfully.");
    }

    /* create an ES index */
    private void createESIndex(final String indexName,
                               Map<String, String> properties) {
        esHandler.deleteESIndex(indexName);
        try {
            esHandler.createESIndex(indexName, properties);
            assertTrue(esHandler.existESIndex(indexName));
        } catch (Exception e) {
            assertTrue(false);
        }
    }

    private CheckpointState createCheckpoint() {
        final RepNode source = config.getRN(rg1masterId);
        final ReplicatedEnvironment sourceEnv = source.getEnv(1000);
        final long testVLSN = 1024;
        final PartitionId testCompletePartitionId = new PartitionId(1);
        final String group = sourceEnv.getGroup().getName();
        final UUID gid = UUID.randomUUID();
        final String srcRN = source.getRepNodeId().getFullName();
        final long vlsn = testVLSN;
        final Set<PartitionId> comp = new HashSet<>();
        comp.add(testCompletePartitionId);
        final long time = System.currentTimeMillis();

        return new CheckpointState(group, gid, srcRN, vlsn, comp, time);

    }
}
