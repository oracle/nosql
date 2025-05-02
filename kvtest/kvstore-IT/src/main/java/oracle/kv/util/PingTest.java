/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import static oracle.kv.impl.util.TestUtils.assertMatch;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.kv.impl.admin.DeployUtils;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.rep.RepNodeTestBase;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.StorageTypeDetector.StorageType;

import oracle.nosql.common.json.JsonNode;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;

import org.junit.Test;

/**
 * Test to exercise the KVS Ping command.
 */
public class PingTest extends RepNodeTestBase {

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    @Test
    public void testPing()
        throws IOException {

        /* Create two groups with three RNs each */
        final KVRepTestConfig config =
            new KVRepTestConfig(this, 1, 2, 3, 10);
        config.startRepNodeServices();
        final StorageNode sn =
            config.getTopology().getStorageNodeMap().getAll().iterator().next();

        String output = PingUtils.doPing(logger, "-host", sn.getHostname(), "-port",
                               Integer.toString(sn.getRegistryPort()),
                                "-no-exit");
        checkHumanOutput(output, config.getTopology());
        output = PingUtils.doPing(logger, "-host", sn.getHostname(), "-port",
                        Integer.toString(sn.getRegistryPort()),
                        "-json-v1", "-no-exit", "-hidden");

        JsonNode node = PingUtils.checkJsonOutput(output,
                                        config.getTopology(), null, false);

        /* get storage type of RN(1,1) */
        RepNode rn = config.getRN(new RepNodeId(1,1));
        String st = rn.getStorageType();
        /* get storage type from the PING output */
        JsonNode stNode = node.asObject().findFirst("storageType");
        assertNotNull(stNode);
        String stInPing = stNode.asText();
        if (st.equals(StorageType.UNKNOWN.toString())){
            assertEquals("Wrong storage type",
                         "HD (default for UNKNOWN)", stInPing);
        }
        else {
            assertEquals("Wrong storage type",
                         st, stInPing);
        }
    }

    @Test
    public void testUnknownReadOnlyShardPing()
        throws IOException {

        /* Create a shard with three RNs i.e 1x3 */
        final KVRepTestConfig config =
            new KVRepTestConfig(this, 1, 1, 3, 10);
        config.startRepNodeServices();
        final RepNodeId rg1rn1 = new RepNodeId(1,1);
        final RepNodeId rg1rn2 = new RepNodeId(1,2);
        final RepNodeId rg1rn3 = new RepNodeId(1,3);

        /*
         * We will test Shard read-only with one node in
         * RUNNING,UNKNOWN state. And other two nodes in
         * UNREACHABLE state.
         *
         * Sequence of tasks :
         * 1.) Identify Master, Stop that node.
         * 2.) Identify next Master, Stop that node.
         */

        /* Identify Master and stop that node */
        final RepNodeId firstMaster;
        if (config.getRepNodeService(rg1rn1).getRepNode().getEnv(1)
            .getState().equals(State.MASTER)) {
            firstMaster = rg1rn1;
            config.stopRepNodeServicesSubset(true, rg1rn1);
        } else if (config.getRepNodeService(rg1rn2).getRepNode().getEnv(1)
                   .getState().equals(State.MASTER)) {
            firstMaster = rg1rn2;
            config.stopRepNodeServicesSubset(true, rg1rn2);
        } else if (config.getRepNodeService(rg1rn3).getRepNode().getEnv(1)
                   .getState().equals(State.MASTER)) {
            firstMaster = rg1rn3;
            config.stopRepNodeServicesSubset(true, rg1rn3);
        } else {
            throw new RuntimeException("Didn't find master");
        }

        /* Identify next Master and stop that node */
        final AtomicReference<RepNodeService> onlineRNservice =
            new AtomicReference<>();
        final boolean stoppedSecondMaster = PollCondition.await(
            100, 10000,
            () -> {
                if (!rg1rn1.equals(firstMaster) &&
                    config.getRepNodeService(rg1rn1).getRepNode().getEnv(1)
                    .getState().equals(State.MASTER)) {
                    config.stopRepNodeServicesSubset(true, rg1rn1);
                    onlineRNservice.set(rg1rn2.equals(firstMaster) ?
                                        config.getRepNodeService(rg1rn3) :
                                        config.getRepNodeService(rg1rn2));
                    return true;
                }
                if (!rg1rn2.equals(firstMaster) &&
                    config.getRepNodeService(rg1rn2).getRepNode().getEnv(1)
                    .getState().equals(State.MASTER)) {
                    config.stopRepNodeServicesSubset(true, rg1rn2);
                    onlineRNservice.set(rg1rn1.equals(firstMaster) ?
                                        config.getRepNodeService(rg1rn3) :
                                        config.getRepNodeService(rg1rn1));
                    return true;
                }
                if (!rg1rn3.equals(firstMaster) &&
                    config.getRepNodeService(rg1rn3).getRepNode().getEnv(1)
                    .getState().equals(State.MASTER)) {
                    config.stopRepNodeServicesSubset(true, rg1rn3);
                    onlineRNservice.set(rg1rn1.equals(firstMaster) ?
                                        config.getRepNodeService(rg1rn2) :
                                        config.getRepNodeService(rg1rn1));
                    return true;
                }
                return false;
            });

        assertTrue("Need to stop second master", stoppedSecondMaster);

        final boolean reachedUnknownState = PollCondition.await(
            100, 10000,
            () -> State.UNKNOWN.equals(
                onlineRNservice.get().getRepNode().getEnv(1).getState()));
        assertTrue("Need online member to reach UNKNOWN state",
                   reachedUnknownState);

        final StorageNode sn =
            config.getTopology().getStorageNodeMap().getAll().iterator().
            next();

        String output =
            PingUtils.doPing(logger, "-host", sn.getHostname(), "-port",
                             Integer.toString(sn.getRegistryPort()),
                             "-no-exit");
        checkReadOnlyHumanOutput(output, 1, 1);
        output = PingUtils.doPing(logger, "-host", sn.getHostname(), "-port",
                        Integer.toString(sn.getRegistryPort()),
                        "-json-v1", "-no-exit", "-hidden");
        PingUtils.checkReadOnlyShardJsonOutput(output, config.getTopology(),
                                               1, 1);
    }

    @Test
    public void testMasterReadOnlyShardPing()
        throws IOException {

        /* Create a shard with three RNs i.e 1x3 */
        final KVRepTestConfig config =
            new KVRepTestConfig(this, 1, 1, 3, 10);
        config.startRepNodeServices();
        final RepNodeId rg1rn1 = new RepNodeId(1,1);
        final RepNodeId rg1rn2 = new RepNodeId(1,2);
        final RepNodeId rg1rn3 = new RepNodeId(1,3);

        RepNodeService rn11service = config.getRepNodeService(rg1rn1);
        RepNodeService rn12service = config.getRepNodeService(rg1rn2);
        RepNodeService rn13service = config.getRepNodeService(rg1rn3);

        /*
         * We will test Shard read-only with one node in
         * RUNNING,Master(non-authoritative) state.
         * And other two nodes in UNREACHABLE state.
         *
         * Sequence of tasks :
         * 1.) Identify Replica, Stop that node.
         * 2.) Identify next Replica, Stop that node.
         */

        /* Identify Replica and stop that node */
        RepNodeId firstReplica = null;
        if (rn11service.getRepNode().getEnv(1).getState().
                equals(State.REPLICA)) {
            firstReplica = rg1rn1;
            config.stopRepNodeServicesSubset(true, rg1rn1);
        } else if (rn12service.getRepNode().getEnv(1).getState().
                       equals(State.REPLICA)) {
            firstReplica = rg1rn2;
            config.stopRepNodeServicesSubset(true, rg1rn2);
        } else if (rn13service.getRepNode().getEnv(1).getState().
                       equals(State.REPLICA)) {
            firstReplica = rg1rn3;
            config.stopRepNodeServicesSubset(true, rg1rn3);
        }

        rn11service = config.getRepNodeService(rg1rn1);
        rn12service = config.getRepNodeService(rg1rn2);
        rn13service = config.getRepNodeService(rg1rn3);

        /* Identify next Replica and stop that node */
        RepNodeService onlineRNservice = new RepNodeService();
        if (!rg1rn1.equals(firstReplica) &&
            rn11service.getRepNode().getEnv(1).getState().
                equals(State.REPLICA)) {
            config.stopRepNodeServicesSubset(true, rg1rn1);
            onlineRNservice = rg1rn2.equals(firstReplica) ?
                                  config.getRepNodeService(rg1rn3) :
                                  config.getRepNodeService(rg1rn2);
        } else if (!rg1rn2.equals(firstReplica) &&
                   rn12service.getRepNode().getEnv(1).getState().
                       equals(State.REPLICA)) {
            config.stopRepNodeServicesSubset(true, rg1rn2);
            onlineRNservice = rg1rn1.equals(firstReplica) ?
                                  config.getRepNodeService(rg1rn3) :
                                  config.getRepNodeService(rg1rn1);
        } else if (!rg1rn3.equals(firstReplica) &&
                   rn13service.getRepNode().getEnv(1).getState().
                       equals(State.REPLICA)) {
            config.stopRepNodeServicesSubset(true, rg1rn3);
            onlineRNservice = rg1rn1.equals(firstReplica) ?
                                  config.getRepNodeService(rg1rn2) :
                                  config.getRepNodeService(rg1rn1);
        }

        assertEquals(State.MASTER,
                     onlineRNservice.getRepNode().getEnv(1).getState());

        final StorageNode sn =
            config.getTopology().getStorageNodeMap().getAll().iterator().
            next();

        String output =
            PingUtils.doPing(logger, "-host", sn.getHostname(), "-port",
                             Integer.toString(sn.getRegistryPort()),
                             "-no-exit");
        checkReadOnlyHumanOutput(output, 1, 1);
        output = PingUtils.doPing(logger, "-host", sn.getHostname(), "-port",
                        Integer.toString(sn.getRegistryPort()),
                        "-json-v1", "-no-exit", "-hidden");
        PingUtils.checkReadOnlyShardJsonOutput(output, config.getTopology(),
                                               1, 1);
    }

    public static void checkReadOnlyHumanOutput(String output,
                                                int readOnlyShards,
                                                int readOnlyRNs) {
        final List<String> lines = new ArrayList<String>();
        Collections.addAll(lines, output.split("\n"));
        lines.remove(0);/* Removing Topology overview line 1*/
        lines.remove(0);/* Removing Topology overview line 2*/
        lines.remove(0);/* Removing Topology overview line 3*/

        /* Read only shard status */
        String line = lines.remove(0);
        String shardRegex =
            "Shard Status: healthy: 0 writable-degraded: 0 " +
            "read-only: " + readOnlyShards + " offline: 0 total: " +
            readOnlyShards;
        assertMatch("Shard status", shardRegex, line);

        if (readOnlyShards == 2) {
            /*
             * For kv shell level ping output, we need to
             * remove admin health status line.
             */
            lines.remove(0);
        }

        /* Read only RN status */
        line = lines.remove(0);
        if (!line.contains("read-only: " + readOnlyRNs)) {
            assertTrue("RN status expected match for pattern: " +
                       "read-only: " + readOnlyRNs + "\n  found: " + line,
                       false);
        }
    }

    public static void checkOfflineHumanOutput(String output) {
        final List<String> lines = new ArrayList<String>();
        Collections.addAll(lines, output.split("\n"));
        lines.remove(0);/* Removing Topology overview line 1*/
        lines.remove(0);/* Removing Topology overview line 2*/
        lines.remove(0);/* Removing Topology overview line 3*/

        /* Offline shard status */
        String line = lines.remove(0);
        String shardRegex = "Shard Status: healthy: 0 writable-degraded: 0 " +
            "read-only: 0 offline: 2 total: 2";
        assertMatch("Shard status", shardRegex, line);

        /*
         * For kv shell level ping output, we need to
         * remove admin health status line.
         */
         lines.remove(0);

        /* Offline RN status */
        line = lines.remove(0);
        if (!line.contains("offline: 2")) {
            assertTrue("RN status expected match for pattern: " +
                       "offline: 2" + "\n  found: " + line,
                       false);
        }
    }

    /** @see DeployUtils#checkVerifyResultsHumanOutput */
    private static void checkHumanOutput(String output, Topology topo) {
        final List<String> lines = new ArrayList<String>();
        Collections.addAll(lines, output.split("\n"));

        /* Topology overview */
        assertMatch("Topology overview line 1:\n" + output,
                    "Pinging components of store " + topo.getKVStoreName() +
                    " based upon topology sequence #\\d+",
                    lines.remove(0));
        assertMatch("Topology overview line 2:\n" + output,
                    "\\d+ partitions and \\d+ storage nodes",
                    lines.remove(0));
        assertMatch("Topology overview line 3:\n" + output,
                    "Time: .*   Version: .*",
                    lines.remove(0));

        /* Shard status */
        String line = lines.remove(0);
        final String shardRegex = "Shard Status: healthy: (\\d+).*";
        assertMatch("Shard status", shardRegex, line);

        /* Admin status */
        if (lines.get(0).matches("Admin Status: [a-z-]*")) {
            lines.remove(0);
        }

        /* Zone status */
        final List<DatacenterId> zoneIds =
            new ArrayList<DatacenterId>(topo.getDatacenterMap().getAllIds());
        Collections.sort(zoneIds);
        for (final DatacenterId zoneId : zoneIds) {
            assertMatch(zoneId.toString(),
                        "Zone \\[name=.* id=" + zoneId + " type=.*\\]" +
                        "   RN Status: .*",
                        lines.remove(0));
        }

        /* SN status */
        final List<StorageNodeId> snIds = topo.getStorageNodeIds();
        Collections.sort(snIds);
        for (final StorageNodeId snId : snIds) {
            String snLine = lines.remove(0);
            assertMatch(snId.toString(),
                        "Storage Node \\[" + snId + "\\].*",
                        snLine);
            if (snLine.contains("Status: RUNNING")) {
                assertMatch(snId.toString(),
                            ".* Edition: (Enterprise|Community|Basic).*",
                            snLine);
            }

            /* RN status */
            final List<RepNodeId> rnIds =
                new ArrayList<RepNodeId>(topo.getHostedRepNodeIds(snId));
            Collections.sort(rnIds);
            for (final RepNodeId rnId : rnIds) {
                assertMatch(rnId.toString(),
                            "\tRep Node \\[" + rnId + "\\].*",
                            lines.remove(0));
            }
        }

        assertEquals("No more output", 0, lines.size());
    }

    /**
     * Return the portion of a string that matches the first group in a
     * pattern.
     */
    public static String scan(String string, String pattern) {
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(string);
        boolean matches = m.matches();
        assertTrue("Pattern '" + pattern + "' does not match string '" +
                   string + "'",
                   matches);
        String groupValue = m.group(1);
        assertNotNull("Pattern '" + pattern + "' did not provide a group" +
                      " match for string '" + string + "'",
                      groupValue);
        return groupValue;
    }
}
