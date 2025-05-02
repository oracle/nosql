/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import static oracle.nosql.common.json.JsonUtils.getArray;
import static oracle.nosql.common.json.JsonUtils.getAsText;
import static oracle.nosql.common.json.JsonUtils.getObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.util.Ping.ExitCode;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * Helper methods for running ping unit tests
 */
public class PingUtils {

    /* Create the set of valid service status strings, for verification */
    private final static Set<String> SERVICE_STATUS_NAMES = new HashSet<>();
    static {
        for (ServiceStatus s: ServiceStatus.values()) {
            SERVICE_STATUS_NAMES.add(s.toString());
        }
    }

    /* Create the set of valid replication states, for verification. */
    private final static Set<String> REP_STATE_NAMES = new HashSet<>();
    static {
        for (ReplicatedEnvironment.State s:
                 ReplicatedEnvironment.State.values()) {
            REP_STATE_NAMES.add(s.toString());
        }
    }

    /**
     * Perform a top-level ping command and return the output as a string.
     */
    public static String doPing(Logger logger, String... args) {

        final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        final PrintStream originalOut = System.out;
        final PrintStream originalErr = System.err;
        try {
            System.setOut(new PrintStream(outBytes));
            System.setErr(new PrintStream(errBytes));
            Ping.main(args);
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }

        String output =
            outBytes.size() != 0 ? outBytes.toString() : errBytes.toString();

        logger.info("Command: " + Arrays.toString(args) +
                    "\nOutput: " + output);
        return output;
    }

    /**
     * Assert that the JSON result value is formatted correctly, has the
     * expected exit code and error message value.
     */
    static void checkResult(String pingOutput,
                            ExitCode expectedExitCode,
                            ErrorMessage expectedErrMsgCode) {

        JsonNode json = JsonUtils.parseJsonNode(pingOutput);

        /* Check exit code and return code */
        int jsonExitCode = json.get(Ping.EXIT_CODE_FIELD_V1).asInt();
        if (expectedExitCode.value() != jsonExitCode) {
            throw new IllegalStateException("Expected exit code of " +
                                            expectedExitCode.value() +
                                            " but got " + jsonExitCode);
        }

        int jsonReturnCode =
            json.get(CommandJsonUtils.FIELD_RETURN_CODE).asInt();
        if (expectedErrMsgCode.getValue() != jsonReturnCode) {
            throw new IllegalStateException("Expected return code of " +
                                            expectedErrMsgCode.getValue() +
                                            " but got " + jsonReturnCode);
        }
    }

    /**
     * Check JSON formatted output to see if it reflects correct ping output
     * for the specified topology and parameters.
     *
     * @param output the output
     * @param topo the topology
     * @param params the parameters
     * @param topoOverviewOnly whether to only check the topology overview
     * @return the parsed JSON node
     */
    public static JsonNode checkJsonOutput(String output,
                                           Topology topo,
                                           Parameters params,
                                           boolean topoOverviewOnly) {
        final JsonNode json = JsonUtils.parseJsonNode(output);

        /* Topology overview */
        final JsonNode jsonTopo = getObject(json, "topology");
        assertNotNull("topology", jsonTopo);
        assertEquals("storeName", topo.getKVStoreName(),
                     getAsText(jsonTopo, "storeName"));
        if (topoOverviewOnly) {
            return json;
        }

        /* Shard status */
        final ObjectNode jsonShard = getObject(json, "shardStatus");
        assertNotNull("shardStatus", jsonShard);
        assertTrue("healthy shards", jsonShard.get("healthy").isNumber());

        /* Zone status */
        final Iterable<JsonNode> jsonZones = getArray(json, "zoneStatus");
        int zoneCount = 0;
        for (final JsonNode jsonZone : jsonZones) {
            assertNotNull("zone " + zoneCount + " rnSummaryStatus",
                          getObject(jsonZone, "rnSummaryStatus"));
            zoneCount++;
        }
        assertEquals("zoneCount", topo.getDatacenterMap().size(), zoneCount);

        /* SN status */
        final Iterable<JsonNode> jsonSNs = getArray(json, "snStatus");
        int snCount = 0;
        for (final JsonNode jsonSN : jsonSNs) {
            final ObjectNode jsn = jsonSN.asObject();
            final String snId = getAsText(jsn, "resourceId");
            assertNotNull("SN " + snCount + " resourceId", snId);
            final StorageNode sn = topo.get(StorageNodeId.parse(snId));
            assertNotNull(snId, sn);
            if ("RUNNING".equals(getAsText(jsn, "serviceStatus"))) {
                final String version = getAsText(jsn, "version");
                assertNotNull("SN " + snId + " version", version);
                assertTrue("Checking SN " + snId + " version edition: " +
                           version,
                           version.matches(
                               ".* Edition: (Enterprise|Community|Basic).*"));
                assertTrue("Checking master balance ",
                           /* Must be boolean if not null */
                           (jsn.get("isMasterBalanced") == null) ||
                           (jsn.get("isMasterBalanced").isBoolean()));
                assertTrue("Checking serviceStartTime ",
                            getAsText(jsn, "serviceStartTime") != null);
            }

            /* Admin status */
            AdminId adminId = null;
            if (params != null) {
                for (final AdminId aId : params.getAdminIds()) {
                    if (sn.getResourceId().equals
                        (params.get(aId).getStorageNodeId())) {
                        adminId = aId;
                        break;
                    }
                }
            }
            final ObjectNode jsonAdmin = getObject(jsonSN, "adminStatus");
            if (adminId == null) {
                assertNull(snId + " admin", jsonAdmin);
            } else {
                /* There's an Admin json object */
                assertNotNull(snId + " admin", jsonAdmin);

                /* Check the resource id, status, and state */
                String jsonAdminId = getAsText(jsonAdmin, "resourceId");
                assertEquals(adminId.toString(), jsonAdminId);
                String status = getAsText(jsonAdmin, "status");
                assertTrue("status=" + status,
                           SERVICE_STATUS_NAMES.contains(status));
                String repState = getAsText(jsonAdmin, "state");
                if (repState != null) {
                    assertTrue("state=" + repState,
                               REP_STATE_NAMES.contains(repState));
                }
                if (status.equals("RUNNING")) {
                    assertTrue("Checking serviceStartTime ",
                            getAsText(jsonAdmin, "serviceStartTime") != null);
                    assertTrue("Checking stateChangeTime ",
                            getAsText(jsonAdmin, "stateChangeTime") != null);
                }
            }

            /* RN status */
            final Iterable<JsonNode> jsonRNs = getArray(jsonSN, "rnStatus");
            int rnCount = 0;
            for (final JsonNode jsonRN : jsonRNs) {
                final String rnId = getAsText(jsonRN, "resourceId");
                assertNotNull("RN " + rnCount + " resourceId", rnId);
                final RepNode rn = topo.get(RepNodeId.parse(rnId));
                assertNotNull(rnId, rn);
                assertEquals(rnId + " SN", sn.getResourceId(),
                             rn.getStorageNodeId());
                String status = getAsText(jsonRN, "status");
                assertTrue("status=" + status,
                           SERVICE_STATUS_NAMES.contains(status));
                String repState = getAsText(jsonRN, "state");
                if (repState != null) {
                    assertTrue("state=" + repState,
                               REP_STATE_NAMES.contains(repState));
                }
                if (status.equals("RUNNING")) {
                    assertTrue("Checking serviceStartTime ",
                        getAsText(jsonRN, "serviceStartTime") != null);
                    assertTrue("Checking stateChangeTime ",
                        getAsText(jsonRN, "stateChangeTime") != null);
                }
                rnCount++;
            }
            assertEquals("rnCount",
                         topo.getHostedRepNodeIds(sn.getResourceId()).size(),
                         rnCount);
            snCount++;
        }
        assertEquals("snCount", topo.getStorageNodeMap().size(), snCount);
        return json;
    }

    /**
     * Check JSON formatted output to see if it reflects correct shard
     * and RN read only health status output for the specified topology
     * and parameters.
     *
     * @param output the output
     * @param topo the topology
     * @return the parsed JSON node
     */
    public static JsonNode
        checkReadOnlyShardJsonOutput(String output,
                                     Topology topo,
                                     int readOnlyShards,
                                     int readOnlyRNs) {
        final JsonNode json;
        json = JsonUtils.parseJsonNode(output);

        /* Shard read-only status */
        final ObjectNode jsonShard = getObject(json, "shardStatus");
        assertNotNull("shardStatus", jsonShard);
        Integer checkreadonly = jsonShard.get("read-only").asInt();
        assertNotNull("readonly shards",
                      checkreadonly.equals(readOnlyShards) ?
                      checkreadonly : null);

        /* RN read-only status */
        final Iterable<JsonNode> jsonZones = getArray(json, "zoneStatus");
        for (final JsonNode jsonZone : jsonZones) {
            final ObjectNode jsonRN = getObject(jsonZone, "rnSummaryStatus");
            assertNotNull("rnSummaryStatus", jsonRN);
            checkreadonly = jsonRN.get("read-only").asInt();
            assertNotNull("readonly RNs",
                          checkreadonly.equals(readOnlyRNs) ?
                          checkreadonly : null);
        }

        return json;
    }

    /**
     * Check JSON formatted output to see if it reflects correct shard
     * and RN offline health status output for the specified topology
     * and parameters.
     *
     * @param output the output
     * @param topo the topology
     * @return the parsed JSON node
     */
    public static JsonNode
        checkOfflineShardJsonOutput(String output,
                                    Topology topo) {
        final JsonNode json = JsonUtils.parseJsonNode(output);

        /* Shard offline status */
        final ObjectNode jsonShard = getObject(json, "shardStatus");
        assertNotNull("shardStatus", jsonShard);
        Integer checkoffline = jsonShard.get("offline").asInt();
        assertNotNull("offline shards",
                      checkoffline.equals(2) ?
                      checkoffline : null);

        /* RN offline status */
        final Iterable<JsonNode> jsonZones = getArray(json, "zoneStatus");
        for (final JsonNode jsonZone : jsonZones) {
            final ObjectNode jsonRN = getObject(jsonZone, "rnSummaryStatus");
            assertNotNull("rnSummaryStatus", jsonRN);
            checkoffline = jsonRN.get("offline").asInt();
            assertNotNull("offline RNs",
                          checkoffline.equals(2) ?
                          checkoffline : null);
        }

        return json;
    }
}
