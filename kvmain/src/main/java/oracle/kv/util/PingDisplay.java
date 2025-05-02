/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.util;

import static oracle.nosql.common.json.JsonUtils.createObjectNode;
import static oracle.nosql.common.json.JsonUtils.getAsText;
import static oracle.nosql.common.json.JsonUtils.getObject;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.AdminStatus;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.arb.ArbNodeStatus;
import oracle.kv.impl.rep.MasterRepNodeStats;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.impl.util.StorageTypeDetector.StorageType;
import oracle.kv.impl.util.TopologyPrinter;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.ObjectNode;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;

/**
 * This class contains utility methods used by both the Ping and the Verify
 * utilities to display service status information in both JSON and
 * human-readable text format. In general, the methods come in pairs:
 *
 * -XXXtoJSON converts information about components in the store to JSON format
 * -displayXXX takes JSON formatted information and displays it as text
 *
 * In some cases, the XXXtoJSON method do a straightforward translation of
 * information to JSON. In other cases, the XXXtoJSON does some summarizing,
 * aggregation, or other massaging of the service information.
 */
public class PingDisplay {

    /**
     * Information received from a component, or service in the kvstore.
     */
    public interface ServiceInfo {
        /* Starting, running, etc */
        ServiceStatus getServiceStatus();

        /* Master, replica, unknown */
        State getReplicationState();
        boolean getIsAuthoritativeMaster();
    }

    /**
     * Adds overview information about the topology to the JSON node by adding
     * a "topology" field with an object value.
     */
    public static void topologyOverviewToJson(Topology topology,
                                              RepGroupId shard,
                                              ObjectNode jsonTop) {
        final ObjectNode on = jsonTop.putObject("topology");
        on.put("storeName", topology.getKVStoreName());
        on.put("sequenceNumber", topology.getSequenceNumber());
        if (shard == null) {
            on.put("numPartitions",
                   topology.getPartitionMap().getNPartitions());
            on.put("numStorageNodes", topology.getStorageNodeMap().size());
        } else {
            on.put("shardName", shard.toString());
            final Map<RepGroupId, List<PartitionId>> partToRG =
                TopologyPrinter.sortPartitions(topology);
            final List<PartitionId> partIds = partToRG.get(shard);
            on.put("numPartitions", partIds.size());
            on.put("numStorageNodes", topology.getRepNodeIds().size() /
            		topology.getRepGroupIds().size());
        }
        on.put("time", System.currentTimeMillis());
        on.put("version", KVVersion.CURRENT_VERSION.getNumericVersionString());
    }

    /**
     * Converts topology information from the JSON node into a human readable
     * string.
     */
    public static String displayTopologyOverview(JsonNode jsonTop) {
        final ObjectNode on = getObject(jsonTop, "topology");
        if (on == null) {
            return "";
        }
        final Long timeValue = getLong(on, "time");
        final String time = (timeValue == null) ? "?" :
            FormatUtils.formatDateTime(timeValue);

        String result = "store " + getAsText(on, "storeName", "?") +
            " based upon topology sequence #" +
            getAsText(on, "sequenceNumber", "?") + "\n";

        if (on.has("shardName")) {
            result += " shard " + getAsText(on, "shardName", "?") + "\n";
        }

        return result +
            getAsText(on, "numPartitions", "?") + " partitions and " +
            getAsText(on, "numStorageNodes", "?") + " storage nodes\n" +
            "Time: " + time + "   Version: " + getAsText(on, "version", "?");
    }

    /**
     * Returns a JSON node with overview information about the zone that the
     * caller will add as an array element to the "zoneStatus" field.
     */
    public static ObjectNode zoneOverviewToJson
        (Topology topology,
         Datacenter dc,
         Ping.RepNodeStatusFunction rnStatusFunc,
         Ping.ArbNodeStatusFunction anStatusFunc,
         RepGroupId shard) {

        final DatacenterId dcId = dc.getResourceId();
        int online = 0;
        int offline = 0;
        int readonly = 0;
        int onlineANs = 0;
        int offlineANs = 0;
        boolean hasReplicas = false;
        Long maxDelay = null;
        Long maxCatchupTime = null;
        final Collection<RepGroup> repGroups =
            topology.getRepGroupMap().getAll();
        for (final RepGroup rg : repGroups) {
            /*
             * If shard is specified then get status for specific shard
             */
            if (shard != null && !rg.getResourceId().equals(shard)) {
                continue;
            }

            /* Get stats for group master */
            MasterRepNodeStats masterStats = null;
            for (final RepNode rn : rg.getRepNodes()) {
                final RepNodeStatus rnStatus = rnStatusFunc.get(rn);
                if ((rnStatus != null) &&
                    rnStatus.getReplicationState().isMaster()) {
                    masterStats = rnStatus.getMasterRepNodeStats();
                    break;
                }
            }

            for (final RepNode rn : rg.getRepNodes()) {
                final StorageNode sn = topology.get(rn.getStorageNodeId());
                if (!dcId.equals(sn.getDatacenterId())) {
                    continue;
                }

                final RepNodeStatus status = rnStatusFunc.get(rn);

                if (status == null) {
                    offline++;
                    continue;
                }

                if (status.getReplicationState().isActive()) {
                    /*
                     * RN in Master (Non-Authoritative) state or
                     * if write-requests are disabled then it is a
                     * read-only RN.
                     */
                    if ((status.getReplicationState().isMaster() &&
                         !status.getIsAuthoritativeMaster()) ||
                        ("READONLY".equals(status.getEnabledRequestType()))) {
                        readonly++;
                    } else if ("NONE".equals(status.getEnabledRequestType())) {
                       /*
                        * If  none requests are enabled then it is a
                        * offline RN.
                        */
                        offline++;
                    } else {
                        online++;
                    }
                } else if (status.getReplicationState().isUnknown()) {
                    /*
                     * Calculate delay and catchup time only in
                     * active state.
                     *
                     * RN in UKNOWN state irrespective if write-requests
                     * are enabled or disabled is a read-only RN.
                     */
                     readonly++;
                     continue;
                } else  {
                     offline++;
                     continue;
                }

                if (status.getReplicationState().isMaster()) {
                    continue;
                }

                /*
                 * Calculate delay and catchup time for replica in
                 * active state
                 */
                hasReplicas = true;
                Long delay = null;
                Long catchupTime = null;
                final long networkRestoreTime =
                    status.getNetworkRestoreTimeSecs();
                if (networkRestoreTime != 0) {
                    catchupTime = networkRestoreTime;
                } else if (masterStats != null) {
                    final String replicaName = rn.getResourceId().toString();
                    delay = masterStats.getReplicaDelayMillisMap().get(
                        replicaName);
                    catchupTime =
                        masterStats.getReplicaCatchupTimeSecs(replicaName);
                }
                if ((delay != null) &&
                    ((maxDelay == null) || (delay > maxDelay))) {
                    maxDelay = delay;
                }
                if (useForMaxTime(catchupTime, maxCatchupTime)) {
                    maxCatchupTime = catchupTime;
                }
            }

            for (final ArbNode an : rg.getArbNodes()) {
                final ArbNodeStatus status = anStatusFunc.get(an);
                if ((status != null) &&
                    status.getArbiterState().isActive()) {
                    onlineANs++;
                } else {
                    offlineANs++;
                }
            }
        }
        final ObjectNode on = createObjectNode();
        zoneNameToJson(dc, on);
        final ObjectNode rnStatus = on.putObject("rnSummaryStatus");
        rnStatus.put("online", online);
        rnStatus.put("offline", offline);
        rnStatus.put("read-only", readonly);
        rnStatus.put("hasReplicas", hasReplicas);
        if (maxDelay != null) {
            rnStatus.put("maxDelayMillis", maxDelay);
        }
        if (maxCatchupTime != null) {
            rnStatus.put("maxCatchupTimeSecs", maxCatchupTime);
        }
        if (onlineANs > 0 || offlineANs > 0) {
            final ObjectNode anStatus = on.putObject("anSummaryStatus");
            anStatus.put("online", onlineANs);
            anStatus.put("offline", offlineANs);
        }
        return on;
    }

    /**
     * Converts zone overview information from the JSON node into a human
     * readable string.
     */
    public static String displayZoneOverview(JsonNode jsonZone) {
        final ObjectNode jsonRN = getObject(jsonZone, "rnSummaryStatus");
        if (jsonRN == null) {
            return "Zone " + displayZoneName(jsonZone);
        }
        final boolean hasReplicas = getBoolean(jsonRN, "hasReplicas", false);
        final String maxDelay = !hasReplicas ? null :
            getAsText(jsonRN, "maxDelayMillis", "?");
        final Long maxCatchupValue = getLong(jsonRN, "maxCatchupTimeSecs");
        final String maxCatchup = !hasReplicas ? null :
            (maxCatchupValue == null) ? "?" :
            (maxCatchupValue == Long.MAX_VALUE) ? "-" :
            maxCatchupValue.toString();
        return "Zone " + displayZoneName(jsonZone) +
            "   RN Status: online: " + getAsText(jsonRN, "online", "?") +
            " read-only: " + getAsText(jsonRN, "read-only", "?") +
            " offline: " + getAsText(jsonRN, "offline", "?") +
            ((maxDelay != null) ? " maxDelayMillis: " + maxDelay : "") +
            ((maxCatchup != null) ? " maxCatchupTimeSecs: " + maxCatchup : "");
    }

    /**
     * Adds overview information about shards in the topology to the JSON node
     * by adding a "shardStatus" field with an object value.
     */
    public static void shardOverviewToJson
        (Topology topology,
         Ping.RepNodeStatusFunction rnStatusFunc,
         Ping.ArbNodeStatusFunction anStatusFunc,
         RepGroupId shard,
         ObjectNode jsonTop) {

        int totalRF = 0;
        int totalPrimaryRF = 0;
        for (final Datacenter dc : topology.getDatacenterMap().getAll()) {
            final int rf = dc.getRepFactor();
            totalRF += rf;
            if (dc.getDatacenterType().isPrimary()) {
                totalPrimaryRF += rf;
            }
        }

        /*
         * The quorum value is used to compute the writable-degraded
         * state. This state indicates that an RNs/ANs may be unavailable,
         * but simple majority ack writes would succeed.
         */
        final int quorum = (totalPrimaryRF / 2) + 1;
        int healthy = 0;
        int writableDegraded = 0;
        int readonly = 0;
        int offline = 0;
        for (RepGroup rg : topology.getRepGroupMap().getAll()) {
            /*
             * If shard is not null then we need to display
             * status result for particular shard only.
             */
            if (shard != null && !rg.getResourceId().equals(shard)) {
                continue;
            }

            int onlineRNs = 0;
            int onlinePrimaryRNs = 0;
            int onlineReadOnlyRNs = 0;
            int onlineANs = 0;
            int offlineANs = 0;
            for (final RepNode rn : rg.getRepNodes()) {
                final StorageNode sn = topology.get(rn.getStorageNodeId());
                final Datacenter dc = topology.get(sn.getDatacenterId());
                final RepNodeStatus status = rnStatusFunc.get(rn);
                if (status != null) {
                    if (status.getReplicationState().isActive()) {
                        if ("READONLY".
                                equals(status.getEnabledRequestType())) {
                            /*
                             * Write-requests are disabled hence RUNNING,
                             * [MASTER|REPLICA] RNs are read-only.
                             */
                            onlineReadOnlyRNs++;
                        } else if (!("NONE".
                                   equals(status.getEnabledRequestType()))) {
                            /*
                             * None requests are disabled hence RUNNING,
                             * [MASTER|REPLICA] RNs are online else offline.
                             *
                             * We have checked for both READONLY and NONE,
                             * hence enabled request type is ALL.
                             */
                            onlineRNs++;
                            if (dc.getDatacenterType().isPrimary()) {
                                onlinePrimaryRNs++;
                            }
                        }
                    }
                    else if (status.getReplicationState().isUnknown()) {
                        /*
                         * Case added for read-only RNs in RUNNING,UNKNOWN
                         * state. Master (Non-Authoritative) read-only RN
                         * taken care by online case.
                         *
                         * RNs in RUNNING, UNKNOWN state are read-only
                         * irrespective if write-requests are enabled or
                         * disabled.
                         */
                        onlineReadOnlyRNs++;
                    }
                }
            }

            for (final ArbNode an : rg.getArbNodes()) {
                final ArbNodeStatus status = anStatusFunc.get(an);
                if ((status != null) &&
                    status.getArbiterState().isActive()) {
                    onlineANs++;
               } else {
                  offlineANs++;
               }
            }

            if (onlineRNs >= totalRF && offlineANs == 0) {
                healthy++;
            } else if (onlinePrimaryRNs + onlineANs >= quorum) {
                writableDegraded++;
            } else if (onlineRNs > 0 || onlineReadOnlyRNs > 0) {
                readonly++;
            } else {
                offline++;
            }

            if (shard != null) {
                /*
                 * We have got health status of specific shard.
                 * Add to jsonTop and return.
                 */
                final ObjectNode on = jsonTop.putObject("shardStatus");
                if (healthy != 0) {
                    on.putObject("healthy");
                } else if (writableDegraded != 0) {
                    on.putObject("writable-degraded");
                } else if (readonly != 0) {
                    on.putObject("read-only");
                } else {
                    on.putObject("offline");
                }
                return;
            }
        }
        final ObjectNode on = jsonTop.putObject("shardStatus");
        on.put("healthy", healthy);
        on.put("writable-degraded", writableDegraded);
        on.put("read-only", readonly);
        on.put("offline", offline);
        on.put("total", topology.getRepGroupIds().size());
    }

    /**
     * Converts overview information about shards from JSON format into a human
     * readable string.
     */
    public static String displayShardOverview(JsonNode jsonTop) {
        final ObjectNode on = getObject(jsonTop, "shardStatus");
        if (on == null) {
            return "";
        }
        return "Shard Status:" +
            " healthy: " + getAsText(on, "healthy", "?") +
            " writable-degraded: " + getAsText(on, "writable-degraded", "?") +
            " read-only: " + getAsText(on, "read-only", "?") +
            " offline: " + getAsText(on, "offline", "?") +
            " total: " + getAsText(on, "total", "?");
    }

    /**
     * Converts overview information about specific shard from JSON
     * format into a human readable string.
     *
     * Gives health status for a specific shard
     */
    public static String displaySpecificShardOverview(JsonNode jsonTop) {
        final ObjectNode on = getObject(jsonTop, "shardStatus");
        if (on == null) {
            return "";
        }

        if (getObject(on, "healthy") != null) {
            return "Shard Status: healthy";
        }

        if (getObject(on, "writable-degraded") != null) {
            return "Shard Status: writable-degraded";
        }

        if (getObject(on, "read-only") != null) {
            return "Shard Status: read-only";
        }

        if (getObject(on, "offline") != null) {
            return "Shard Status: offline";
        }
        return "Shard Status: unknown";
    }

    /**
     * Adds overview information about admins to the JSON node by adding a
     * "adminStatus" field with a text value.
     */
    public static void adminOverviewToJson
        (Parameters parameters,
         Ping.AdminStatusFunction adminStatusFunc,
         ObjectNode jsonTop) {

        boolean foundAuthoritativeMaster = false;
        boolean foundOnline = false;
        boolean foundOffline = false;
        for (final AdminId adminId : parameters.getAdminIds()) {
            final AdminStatus adminStatus = adminStatusFunc.get(adminId);
            if ((adminStatus == null) ||
                adminStatus.getServiceStatus() != ServiceStatus.RUNNING) {
                foundOffline = true;
            } else {
                foundOnline = true;
                if (adminStatus.getReplicationState().isMaster() &&
                    adminStatus.getIsAuthoritativeMaster()) {
                    foundAuthoritativeMaster = true;
                }
            }
        }
        final String status =
            !foundOnline ? "offline" :
            !foundAuthoritativeMaster ? "read-only" :
            foundOffline ? "writable-degraded" :
            "healthy";
        jsonTop.put("adminStatus", status);
    }

    /**
     * Converts overview information about admins from the JSON node into a
     * human readable string.
     */
    public static String displayAdminOverview(JsonNode jsonTop) {
        final String adminStatus = getAsText(jsonTop, "adminStatus");
        if (adminStatus == null) {
            return "";
        }
        return "Admin Status: " + adminStatus;
    }

    /**
     * Returns a JSON node with information about the replication node that the
     * caller will add as an array element of the "rnStatus" field within the
     * "snStatus" field.
     */
    public static ObjectNode repNodeToJson(RepNode rn,
                                           RepNodeStatus status,
                                           RepNodeStatus masterStatus,
                                           ServiceStatus expected) {
        final ObjectNode on = createObjectNode();
        on.put("resourceId", rn.getResourceId().toString());

        statusToJson(status, on);
        if (expected != null) {
            on.put("expectedStatus", expected.toString());
        }
        if (status == null) {
            return on;
        }
        on.put("sequenceNumber", status.getVlsn());
        on.put("haPort", status.getHAPort());

        String st = status.getStorageType();
        if (st != null) {
            if (st.equals(StorageType.UNKNOWN.toString())) {
                on.put("storageType", "HD (default for UNKNOWN)");
            }
            else {
                on.put("storageType", st);
            }
        }

        final long availableStorageSize = status.getAvailableLogSize();
        final String availableStorageSizeInUnits =
            TopologyPrinter.getSizeInUnits(availableStorageSize);
        final boolean isEnvironmentStatsAvailable =
            status.isEnvironmentStatsAvailable();
        /*
         * If storagedirsize is not specified then availableLogSize
         * will be complete disk space available. It will be same for
         * all RepNodes.
         */
         on.put("availableStorageSize",
                isEnvironmentStatsAvailable ?
                    availableStorageSizeInUnits : "NOT AVAILABLE");
         long sst = status.getServiceStartTime();
         if (sst > 0) {
             on.put("serviceStartTime", FormatUtils.formatDateTime(sst));
         }
         long sct = status.getStateChangeTime();
         if (sct > 0 ) {
             on.put("stateChangeTime", FormatUtils.formatDateTime(sct));
         }

        if (status.getReplicationState().isMaster()) {
            return on;
        }
        final long networkRestoreTime = status.getNetworkRestoreTimeSecs();
        if (networkRestoreTime > 0) {
            on.put("networkRestoreUnderway", true);
            on.put("catchupTimeSecs", networkRestoreTime);
            return on;
        }
        on.put("networkRestoreUnderway", false);
        if (masterStatus != null) {
            final MasterRepNodeStats stats =
                masterStatus.getMasterRepNodeStats();
            if (stats != null) {
                final String replicaName = rn.getResourceId().toString();
                final Long delay =
                    stats.getReplicaDelayMillisMap().get(replicaName);
                if (delay != null) {
                    on.put("delayMillis", delay);
                }
                final Long catchupTime =
                    stats.getReplicaCatchupTimeSecs(replicaName);
                if (catchupTime != null) {
                    on.put("catchupTimeSecs", catchupTime);
                }
                final Long catchupRate =
                    stats.getReplicaCatchupRate(replicaName);
                if (catchupRate != null) {
                    on.put("catchupRateMillisPerMinute", catchupRate);
                }
            }
        }
        return on;
    }

    /**
     * Converts replication node information from the JSON node into a human
     * readable string.
     */
    public static String displayRepNode(JsonNode node) {
        String result = "\tRep Node [" +
            getAsText(node, "resourceId", "?") + "]\tStatus: " +
            displayStatus(node) + displayEnabledRequestType(node);
        final String stopped =
            "UNREACHABLE".equals(getAsText(node, "expectedStatus")) ?
            " (Stopped)" : "";
        if (getAsText(node, "status", "UNREACHABLE").equals("UNREACHABLE")) {
            return result + stopped;
        }

        final Long sequenceNumber = getLong(node, "sequenceNumber");
        result += " sequenceNumber: " +
            ((sequenceNumber != null) ?
             String.format("%,d", sequenceNumber) : "?") +
            " haPort: " + getAsText(node, "haPort", "?");

        final String availableStorage =
            getAsText(node, "availableStorageSize");
        result += " availableStorageSize: " + availableStorage;

        final String storageType =
            getAsText(node, "storageType");
        result += " storageType: " + storageType;

        final String serviceStartTime =
            getAsText(node, "serviceStartTime", "?");
        result += "\tserviceStartTime: " + serviceStartTime;

        final String stateChangeTime =
            getAsText(node, "stateChangeTime", "?");
        result += "\tstateChangeTime: " + stateChangeTime;

        if ("MASTER".equals(getAsText(node, "state"))) {
            return result + stopped;
        }

        final Long catchupTimeValue = getLong(node, "catchupTimeSecs");
        final String catchupTime = (catchupTimeValue == null) ? "?" :
            (catchupTimeValue == Long.MAX_VALUE) ? "-" :
            catchupTimeValue.toString();
        final boolean networkRestoreUnderway =
            getBoolean(node, "networkRestoreUnderway", false);
        return result +
            " delayMillis: " + getAsText(node, "delayMillis", "?") +
            " catchupTimeSecs: " + catchupTime +
            (networkRestoreUnderway ? " networkRestoreUnderway" : "") +
            stopped;
    }

    /**
     * Converts arbiter node information from the JSON node into a human
     * readable string.
     */
    public static String displayArbNode(JsonNode node) {
        String result = "\tArb Node [" +
            getAsText(node, "resourceId", "?") + "]\tStatus: " +
            displayStatus(node);
        final String stopped =
            "UNREACHABLE".equals(getAsText(node, "expectedStatus")) ?
            " (Stopped)" : "";
        if (getAsText(node, "status", "UNREACHABLE").equals("UNREACHABLE")) {
            return result + stopped;
        }
        final Long sequenceNumber = getLong(node, "sequenceNumber");
        result += " sequenceNumber: " +
            ((sequenceNumber != null) ?
             String.format("%,d", sequenceNumber) : "?") +
            " haPort: " + getAsText(node, "haPort", "?") +
            "\tserviceStartTime: " + getAsText(node, "serviceStartTime", "?");
        return result;
    }

    /**
     * Returns a JSON node with information about the storage node, by creating
     * an object node that the caller will add as an array element of the
     * "snStatus" field.
     */
    public static ObjectNode storageNodeToJson(Topology topology,
                                               StorageNode sn,
                                               StorageNodeStatus status) {
        final ObjectNode on = createObjectNode();
        on.put("resourceId", sn.getResourceId().toString());
        on.put("hostname", sn.getHostname());
        on.put("registryPort", sn.getRegistryPort());
        zoneNameToJson(topology.get(sn.getDatacenterId()),
                       on.putObject("zone"));
        if (status != null) {
            on.put("serviceStatus", status.getServiceStatus().toString());
            on.put("version", status.getKVVersion().toString());
            Boolean isMasterBalanced = status.isMasterBalanced();
            if (isMasterBalanced != null) {
                on.put("isMasterBalanced", isMasterBalanced);
            }
            long sst = status.getServiceStartTime();
            if (sst > 0) {
                on.put("serviceStartTime", FormatUtils.formatDateTime(sst));
            }
        } else {
            on.put("serviceStatus", "UNREACHABLE");
        }
        return on;
    }

    /**
     * Converts storage node information from the JSON node into a human
     * readable string.
     */
    public static String displayStorageNode(JsonNode node) {
        final String serviceStatus =
            getAsText(node, "serviceStatus", "UNREACHABLE");
        final Boolean isMasterBalanced = getBoolean(node, "isMasterBalanced");

        return "Storage Node [" +
            getAsText(node, "resourceId", "?") + "] on " +
            getAsText(node, "hostname", "?") + ": " +
            getAsText(node, "registryPort", "?") +
            "    Zone: " + displayZoneName(getObject(node, "zone")) +
            " " +
            (!"UNREACHABLE".equals(serviceStatus) ?
             ("   Status: " + serviceStatus +
              "   Ver: " + getAsText(node, "version", "?")) :
             "UNREACHABLE") +
            ("    isMasterBalanced: " +
             ((isMasterBalanced == null) ? "unknown" :
                 isMasterBalanced.toString()) +
            "\tserviceStartTime: " + getAsText(node, "serviceStartTime", "?"));
    }

    /**
     * Returns a JSON node with information about the admin node that the
     * caller will add as the value of the "adminStatus" field within the value
     * of the "snStatus" field.
     */
    public static ObjectNode adminToJson(AdminId aId,
                                         AdminStatus adminStatus,
                                         ServiceStatus expected) {
        final ObjectNode on = createObjectNode();
        on.put("resourceId", aId.toString());
        statusToJson(adminStatus, on);
        if (expected != null) {
            on.put("expectedStatus", expected.toString());
        }
        if (adminStatus != null) {
            long sst = adminStatus.getServiceStartTime();
            if (sst > 0) {
                on.put("serviceStartTime", FormatUtils.formatDateTime(sst));
            }
            long sct = adminStatus.getStateChangeTime();
            if (sct > 0) {
                on.put("stateChangeTime", FormatUtils.formatDateTime(sct));
            }
            long availableStorageSize = adminStatus.getAvailableLogSize();
            on.put("availableStorageSize",
                   TopologyPrinter.getSizeInUnits(availableStorageSize));
        }
        return on;
    }

    /**
     * Converts admin node information from the JSON node into a human readable
     * string.
     */
    public static String displayAdmin(JsonNode node) {

        String startStatus = "\tAdmin [" + getAsText(node, "resourceId", "?") +
                "]\t\tStatus: " + displayStatus(node);

        final String endStatus = "\tserviceStartTime: " +
                getAsText(node, "serviceStartTime", "?") +
                "\tstateChangeTime: " + getAsText(node, "stateChangeTime", "?") +
                "\tavailableStorageSize: " +
                getAsText(node, "availableStorageSize", "?");

        final String stopped =
                "UNREACHABLE".equals(getAsText(node, "expectedStatus")) ?
                        " (Stopped)" : "";
        if (getAsText(node, "status", "?").equals("UNREACHABLE")) {
            return startStatus + stopped + endStatus;
        }

        return startStatus + endStatus;
    }

    private static String displayStatus(JsonNode node) {
        final String state = getAsText(node, "state");
        final String authoritative =
            ("MASTER".equals(state) &&
             !getBoolean(node, "authoritativeMaster", true)) ?
            " (non-authoritative)" :
            "";
        return getAsText(node, "status", "UNREACHABLE") +
            ((state == null) ?
             "" :
             "," + state + authoritative);
    }

    private static String displayEnabledRequestType(JsonNode node) {
        final String requestsEnabled = getAsText(node, "requestsEnabled");
        return (requestsEnabled == null || "ALL".equals(requestsEnabled)) ?
               "" :
               "READONLY".equals(requestsEnabled) ?
               " readonly requests enabled" :
               /* NONE */
               " requests disabled";
    }

    /**
     * Whether to use the specified time as a maximum time value instead of the
     * maximum provided.  Ignores null values, and uses larger values except
     * that negative values are used in favor of positive ones.
     */
    private static boolean useForMaxTime(Long time, Long maxTime) {
        if (time == null) {
            return false;
        }
        if (maxTime == null) {
            return true;
        }
        if ((time >= 0) && (maxTime < 0)) {
            return false;
        }
        if ((time < 0) && (maxTime >= 0)) {
            return true;
        }
        return time > maxTime;
    }

    private static void zoneNameToJson(Datacenter dc, ObjectNode node) {
        node.put("resourceId", dc.getResourceId().toString());
        node.put("name", dc.getName());
        node.put("type", dc.getDatacenterType().toString());
        node.put("allowArbiters", dc.getAllowArbiters());
        node.put("masterAffinity", dc.getMasterAffinity());
    }

    private static String displayZoneName(JsonNode node) {
        return "[name=" + getAsText(node, "name", "?") +
            " id=" + getAsText(node, "resourceId", "?") +
            " type=" + getAsText(node, "type", "?") +
            " allowArbiters=" + getAsText(node, "allowArbiters", "?") +
            " masterAffinity=" + getAsText(node, "masterAffinity", "?") + "]";
    }

    private static void statusToJson(ServiceInfo status, ObjectNode on) {

        if (status == null) {
            on.put("status", "UNREACHABLE");
        } else {
            on.put("status", status.getServiceStatus().toString());

            if (status instanceof RepNodeStatus) {
                final String enabledRequestType =
                    ((RepNodeStatus) status).getEnabledRequestType();
                on.put("requestsEnabled", enabledRequestType);
            }

            State replicationState = status.getReplicationState();
            if (replicationState != null) {
                on.put("state", replicationState.toString());
                if (replicationState.isMaster()) {
                    on.put("authoritativeMaster",
                            status.getIsAuthoritativeMaster());
                }
            }
        }
    }

    private static void statusToJson(ArbNodeStatus status, ObjectNode on) {
        if (status == null) {
            on.put("status", "UNREACHABLE");
        } else {
            on.put("status", status.getServiceStatus().toString());
            State arbState = status.getArbiterState();
            on.put("state", arbState.toString());
        }
    }

    /**
     * Returns a JSON node with information about the arbiter node that the
     * caller will add as an array element of the "anStatus" field within the
     * "snStatus" field.
     */
    public static ObjectNode arbNodeToJson(ArbNode an,
                                           ArbNodeStatus status,
                                           ServiceStatus expected) {
        final ObjectNode on = createObjectNode();
        on.put("resourceId", an.getResourceId().toString());
        statusToJson(status, on);
        if (expected != null) {
            on.put("expectedStatus", expected.toString());
        }
        if (status == null) {
            return on;
        }
        on.put("sequenceNumber", status.getVlsn());
        on.put("haPort", status.getHAHostPort());
        long sst = status.getServiceStartTime();
        if (sst > 0) {
            on.put("serviceStartTime", FormatUtils.formatDateTime(sst));
        }
        return on;
    }

    private static Boolean getBoolean(JsonNode node, String key) {
        JsonNode val = node.get(key);
        if (val != null && val.isBoolean()) {
            return val.asBoolean();
        }
        return null;
    }

    private static Boolean getBoolean(JsonNode node,
                                      String key,
                                      boolean defValue) {
        Boolean bool = getBoolean(node, key);
        if (bool == null) {
            bool = defValue;
        }
        return bool;
    }

    private static Long getLong(JsonNode node, String key) {
        JsonNode val = node.get(key);
        if (val != null && val.isNumber()) {
            return val.asLong();
        }
        return null;
    }
}
