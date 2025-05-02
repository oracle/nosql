/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static com.sleepycat.je.rep.impl.RepParams.REPLAY_MAX_OPEN_DB_HANDLES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.KVVersion;
import oracle.kv.impl.rep.migration.MigrationManager;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.PollCondition;

import org.junit.Test;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * Tests for the RepNode component
 *
 * TODO: add test for failures during startup, e.g. startups requiring a
 * network restore.
 */
public class RepNodeTest extends RepNodeTestBase {

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        MigrationManager.stateNotifyFailure = null;
    }

    @Test
    public void testRNStartUp() throws IOException {
        /* Create a group with two RNs */
        KVRepTestConfig config = new KVRepTestConfig(this, 1, 2, 3, 10);

        for (oracle.kv.impl.rep.RepNode rn : config.getRNs()) {
            rn.startup();
            rn.updateMetadata(config.getTopology());
        }

        /* verify that the version number is recorded in the local database. */
        for (oracle.kv.impl.rep.RepNode rn :  config.getRNs()) {
            Environment env = rn.getEnv(10000);
            assertEquals(KVVersion.CURRENT_VERSION,
                         VersionManager.getLocalVersion(logger, env, null));
        }

        /*
         * At this point, the repNodes should be fully initialized and the
         * partition DBs, should have been created.
         */
        for (oracle.kv.impl.rep.RepNode rn :  config.getRNs()) {
            rn.stop(false);
        }
    }

    /**
     * Verify that the number of replay open handles is expanded dynamically
     * past the 100/RG default setting in response to large numbers of
     * partitions.
     */
    @Test
    public void testRNPartitionHandles() throws IOException {
        /* Create a group with 1K partitions */
        final int nPartitions = 1000;
        final KVRepTestConfig config =
            new KVRepTestConfig(this, 1, 1, 3, nPartitions);

        for (oracle.kv.impl.rep.RepNode rn : config.getRNs()) {
            rn.startup();
            rn.updateMetadata(config.getTopology());
        }

        assertTrue(new PollCondition(500, 60000) {

            @Override
            protected boolean condition() {
                for (oracle.kv.impl.rep.RepNode rn :  config.getRNs()) {
                    final ReplicatedEnvironment env = rn.getEnv(10000);
                    EnvironmentMutableConfig econfig = env.getMutableConfig();
                    int configHandles =
                        Integer.parseInt(econfig.getConfigParam
                                     (REPLAY_MAX_OPEN_DB_HANDLES.getName()));
                    if (configHandles < nPartitions) {
                        return false;
                    }
                }
                return true;
            }

        }.await());

        for (oracle.kv.impl.rep.RepNode rn :  config.getRNs()) {
            rn.stop(false);
        }
    }

    @Test
    public void testMigrationStateTrackerAtStartUp()
        throws Exception {

        /* Create a group with one RN */
        final KVRepTestConfig config = new KVRepTestConfig(this, 1, 1, 1, 10);
        final List<String> failures = new ArrayList<>();

        MigrationManager.stateNotifyFailure = new TestHook<String>() {
            @Override
            public void doHook(String failure) {
                if (failure == null) {
                    failures.clear();
                } else {
                    failures.add(failure);
                }
            }
        };

       final RepNode rn = config.getRNs().get(0);
       rn.startup();

        /* Wait until there is a failure collected */
        assertTrue(new PollCondition(500, 60000) {

            @Override
            protected boolean condition() {
                return failures.size() >= 1;
            }
        }.await());

        /* Partition generation table is unavailable till topology is updated */
        for (String f : failures) {
            assertTrue(f.contains("generation table is unavailable"));
        }
        assertNull(rn.getPartGenTable());

        /* Update topology on this RN */
        rn.updateMetadata(config.getTopology());

        /*
         * Wait until the failure is clear where state tracker have checked
         * partition generation table after it's available.
         */
        assertTrue(new PollCondition(500, 60000) {

            @Override
            protected boolean condition() {
                return failures.size() == 0;
            }
        }.await());
        assertNotNull(rn.getPartGenTable());
    }
}