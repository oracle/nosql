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

package oracle.kv.impl.util;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.topo.LogDirectory;
import oracle.kv.impl.admin.topo.StorageDirectory;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.SizeParameter;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.util.GenerateConfig;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

/**
 * Prints a Topology for visualization.
 */
public final class TopologyPrinter {

    public enum Filter { STORE, DC, RN, SN, SHARD, STATUS, PERF, AN }
    public static final EnumSet<Filter> all =
        EnumSet.allOf(Filter.class);
    public static final EnumSet<Filter> components =
        EnumSet.of(Filter.STORE, Filter.DC, Filter.RN, Filter.SN, Filter.SHARD, Filter.AN);
    /**
     * Private constructor to satisfy CheckStyle and to prevent
     * instantiation of this utility class.
     */
    private TopologyPrinter() {
        throw new AssertionError("Instantiated utility class " +
                                 TopologyPrinter.class);
    }

    /**
     * Dumps the topology to "out".
     * @param params SN capacity and RN mount points are stored in the
     * Parameters class from the Admin DB, and is not available in the topology
     * itself. If params is not null, use it to display that kind of
     * information.
     * @param verbose if true, print partitions for each shard.
     */
    public static void printTopology(Topology t,
                                     PrintStream out,
                                     Parameters params,
                                     EnumSet<Filter> filter,
                                     Map<ResourceId, ServiceChange> statusMap,
                                     Map<ResourceId, PerfEvent> perfMap,
                                     boolean verbose) {
        printTopology(t, out, params, filter, verbose, null, null, statusMap,
                      perfMap);
    }

    /**
     * Dumps the topology to "out".
     *
     * This method can be called on deployed and not-yet-deployed topologies.
     * The latter are tricky, because the Parameters may not contain a
     * RepNodeParams for new RNs. Some information of interest is only available
     * in the RepNodeParam, and not the topology, such as:
     * - ha ports: calculated at deploy time
     * - mount point mappings: in a special map in the candidate
     *
     * This method has to take care to check whether the params-type information
     * is available yet.
     *
     * @param params SN capacity and RN mount points are stored in the
     * Parameters class from the Admin DB, and is not available in the topology
     * itself. If params is not null, use it to display that kind of
     * information.
     * @param filter is used to selectively print portions of the topology.
     * The default is the entire thing.
     * @param verbose if true, print partitions for each shard.
     */
    private static void printTopology(Topology t,
                                      PrintStream out,
                                      Parameters params,
                                      EnumSet<Filter> filter,
                                      boolean verbose,
                                      Map<RepNodeId, StorageDirectory>
                                                                storageDirMap,
                                      Map<RepNodeId, LogDirectory>
                                                                rnLogDirMap,
                                      Map<ResourceId, ServiceChange> statusMap,
                                      Map<ResourceId, PerfEvent> perfMap) {

        int indent = 0;
        final int indentAmount = 2;
        final boolean showStatus = filter.contains(Filter.STATUS) &&
            (statusMap != null);
        final boolean showPerf = filter.contains(Filter.PERF) &&
            (perfMap != null);

        /*
         * Display the store name
         */
        final String storeName = t.getKVStoreName();
        if (filter.contains(Filter.STORE)) {
            out.println("store=" + storeName + "  numPartitions=" +
                        t.getPartitionMap().size() + " sequence=" +
                        t.getSequenceNumber());
            indent += indentAmount;
        }

        /*
         * Display datacenters, sorted by data center ID
         */
        if (filter.contains(Filter.DC)) {
            final List<Datacenter> dcList = t.getSortedDatacenters();
            for (final Datacenter dc : dcList) {
                out.println(makeWhiteSpace(indent) +
                            DatacenterId.DATACENTER_PREFIX + ": " + dc);
            }
            if (dcList.size() > 0) {
                out.println();
            }
        }

        /*
         * Display SNs, sorted by SN id.
         */
        final Map<StorageNodeId, List<RepNodeId>> snMap =
            new TreeMap<StorageNodeId, List<RepNodeId>>();
        for (StorageNodeId snid : t.getStorageNodeMap().getAllIds()) {
            snMap.put(snid, new ArrayList<RepNodeId>());
        }
        final List<RepNode> rnList = t.getSortedRepNodes();
        for (RepNode rn: rnList) {
            final List<RepNodeId> l = snMap.get(rn.getStorageNodeId());
            l.add(rn.getResourceId());
        }

        for (Entry<StorageNodeId, List<RepNodeId>> entry : snMap.entrySet()) {
            final StorageNodeId snId = entry.getKey();
            String capacityInfo = null;
            StorageNodeParams snp = null;
            if (params != null) {
                snp = params.get(snId);
                if (snp != null) {
                    capacityInfo = " capacity=" + snp.getCapacity();
                }
            }
            if (filter.contains(Filter.SN)) {
                out.print(makeWhiteSpace(indent) + "sn=" + t.get(snId));
                /*
                 * TODO : Yet to add details corresponding to admin storage
                 * dirs for this SN.
                 */
                if (capacityInfo != null) {
                    out.print(capacityInfo);
                }
                if (showStatus) {
                    out.println(" " + getStatus(statusMap, snId));
                } else {
                    out.println();
                }

                indent += indentAmount;
            }

            /* List all the RNs on a given SN */
            if (filter.contains(Filter.RN)) {
                final List<String> rnsWithMountPoints =
                    new ArrayList<String>();
                for (RepNodeId rnId : entry.getValue()) {
                    String oneRN = "[" + rnId + "]";

                    if (showStatus) {
                        oneRN += " " + getStatus(statusMap, rnId, params);
                    }

                    if (!verbose) {
                        out.println(makeWhiteSpace(indent) + oneRN);
                        if (showPerf) {
                            out.println
                                (makeWhiteSpace((indent + 1) * indentAmount) +
                                 getPerf(perfMap, rnId));
                        }
                        continue;
                    }

                    /*
                     * If this is verbose mode, display the root directory or
                     * storage directory, if those are available. Root dir SNs
                     * will be displayed first.
                     *
                     * Storage directory and RN log Directory information is
                     * available in two places, depending on whether this
                     * topology is deployed or not. If not deployed, it's
                     * in the topology candidate assigned storage directory
                     * and RN log dir map, because that information is more
                     * up to date for a candidate. If there is no map, then this
                     * is a deployed topo, and we should look in the params.
                     */
                    printStorageRNLogMountPoints(storageDirMap, rnLogDirMap,
                                                 rnsWithMountPoints,
                                                 params, rnId, oneRN,
                                                 out, indent, indentAmount,
                                                 showPerf, perfMap, snp);
                }
                for (String s : rnsWithMountPoints) {
                    out.println(makeWhiteSpace(indent) + s);
                }
            }

            /* List all the ANs on a given SN */
            if (filter.contains(Filter.AN)) {
                for (ArbNodeId anId : t.getHostedArbNodeIds(snId)) {
                    String oneAN = "[" + anId + "]";

                    if (showStatus) {
                        oneAN += " " + getStatus(statusMap, anId, params);
                    }

                    if (!verbose) {
                        out.println(makeWhiteSpace(indent) + oneAN);
                    } else {
                        out.print(makeWhiteSpace(indent) + oneAN);
                        if (snp == null) {
                            out.println();
                        } else {
                            out.println("  " + snp.getRootDirPath());
                        }
                    }
                }
            }

            if (filter.contains(Filter.SN)) {
                indent -= indentAmount;
            }
        }

        /*
         * Display rep groups (shards), sorted by ID
         */
        if (filter.contains(Filter.SHARD)) {
            out.println();
            final Map<RepGroupId, List<PartitionId>> partToRG =
                sortPartitions(t);

            out.println(makeWhiteSpace(indent) + "numShards="
                        + partToRG.size());

            for (Map.Entry<RepGroupId, List<PartitionId>> entry :
                     partToRG.entrySet()) {

                final RepGroupId rgId = entry.getKey();
                final List<PartitionId> partIds = entry.getValue();
                final RepGroup rg = t.get(rgId);
                out.println(makeWhiteSpace(indent) + "shard=" + rg +
                            " num partitions=" + partIds.size());

                if (filter.contains(Filter.RN) ||
                    filter.contains(Filter.AN)) {
                    indent += indentAmount;
                }

                if (filter.contains(Filter.RN)) {
                    final List<RepNode> rns =
                        new ArrayList<RepNode>(rg.getRepNodes());
                    Collections.sort(rns);
                    for (RepNode rn: rns) {
                        out.print(makeWhiteSpace(indent) + rn);
                        if (verbose && params != null) {
                            final RepNodeParams rnp =
                                params.get(rn.getResourceId());
                            if (rnp != null) {
                                out.print(" haPort=" + rnp.getJENodeHostPort());
                            }
                        }
                        out.println();
                    }
                }

                if (filter.contains(Filter.AN)) {
                    final List<ArbNode> ans =
                        new ArrayList<ArbNode>(rg.getArbNodes());
                    Collections.sort(ans);
                    for (ArbNode an: ans) {
                        out.print(makeWhiteSpace(indent) + an);
                        if (verbose && params != null) {
                            final ArbNodeParams anp =
                                params.get(an.getResourceId());
                            if (anp != null) {
                                out.print(" haPort=" + anp.getJENodeHostPort());
                            }
                        }
                        out.println();
                    }
                }

                if (verbose) {
                    out.println(makeWhiteSpace(indent) + "partitions=" +
                                listPartitions(partIds));
                }
                if (filter.contains(Filter.RN) ||
                    filter.contains(Filter.AN)) {
                    indent -= indentAmount;
                }
            }
        }
    }

    /**
     * Print the topology information in JSON format. The example format:
     * {
     * "storeName" : "mystore",
     * "numPartitions" : 20,
     * "sequenceNumber" : 30,
     * "zns" : [ {
     *   "resourceId" : "zn1",
     *   "name" : "1",
     *   "repFactor" : 1,
     *   "type" : "PRIMARY",
     *   "allowArbiters" : false,
     *   "masterAffinity" : false
     *   }, {
     *   "resourceId" : "zn2",
     *   "name" : "2",
     *   "repFactor" : 1,
     *   "type" : "PRIMARY",
     *   "allowArbiters" : false,
     *   "masterAffinity" : false
     *   }, {
     *   "resourceId" : "zn3",
     *   "name" : "3",
     *   "repFactor" : 1,
     *   "type" : "PRIMARY",
     *   "allowArbiters" : false,
     *   "masterAffinity" : false
     * } ],
     * "sns" : [ {
     *   "resourceId" : "sn1",
     *   "hostname" : "localhost",
     *   "registryPort" : 20000,
     *   "zone" : {
     *     "resourceId" : "zn1",
     *     "name" : "1",
     *     "repFactor" : 1,
     *     "type" : "PRIMARY",
     *     "allowArbiters" : false,
     *     "masterAffinity" : false
     *   },
     *   "capacity" : 1,
     *   "rns" : [ {
     *     "resourceId" : "rg1-rn1"
     *   } ],
     *   "ans" : [ ]
     *   }, {
     *   "resourceId" : "sn2",
     *   "hostname" : "localhost",
     *   "registryPort" : 21000,
     *   "zone" : {
     *     "resourceId" : "zn2",
     *     "name" : "2",
     *     "repFactor" : 1,
     *     "type" : "PRIMARY",
     *     "allowArbiters" : false,
     *     "masterAffinity" : false
     *   },
     *   "capacity" : 1,
     *   "rns" : [ {
     *     "resourceId" : "rg1-rn2"
     *   } ],
     *   "ans" : [ ]
     *   }, {
     *   "resourceId" : "sn3",
     *   "hostname" : "localhost",
     *   "registryPort" : 22000,
     *   "zone" : {
     *     "resourceId" : "zn3",
     *     "name" : "3",
     *     "repFactor" : 1,
     *     "type" : "PRIMARY",
     *     "allowArbiters" : false,
     *     "masterAffinity" : false
     *   },
     *   "capacity" : 1,
     *   "rns" : [ {
     *     "resourceId" : "rg1-rn3"
     *   } ],
     *   "ans" : [ ]
     * } ],
     * "shards" : [ {
     *   "resourceId" : "rg1",
     *   "numPartitions" : 20,
     *   "rns" : [ {
     *     "resourceId" : "rg1-rn1",
     *     "snId" : "sn1"
     *   }, {
     *     "resourceId" : "rg1-rn2",
     *     "snId" : "sn2"
     *   }, {
     *     "resourceId" : "rg1-rn3",
     *     "snId" : "sn3"
     *   } ],
     *   "ans" : [ ]
     * } ]
     * }
     *
     * To note : SNRecover.TopologyJSON class and its sub-classes are dependent
     * on TopologyPrinter output for de-serializing topology JSON output. In
     * case of any changes in topology printer output, make sure to do
     * similar required changes in SNRecover.TopologyJSON and its subclass.
     *
     * TODO [#27493] : It will be good to keep SNRecover.TopologyJSON classes
     * with TopologyPrinter so that it is convenient to maintain the
     * relationship between the input and output of JSON. To keep
     * SNRecover.TopologyJSON and TopologyPrinter in sync.
     *
     * @param t the topology instance
     * @param params the parameter value set
     * @param filter filter to control the output result set
     * @param verbose whether to show verbose output
     * @return JSON object representing the topology result
     */
    public static ObjectNode
        printTopologyJson(Topology t,
                          Parameters params,
                          EnumSet<Filter> filter,
                          boolean verbose) {
        final ObjectNode jsonTop = JsonUtils.createObjectNode();

        /* Consolidate store related information */
        final String storeName = t.getKVStoreName();
        if (filter.contains(Filter.STORE)) {
            jsonTop.put("storeName", storeName);
            jsonTop.put("numPartitions", t.getPartitionMap().size());
            jsonTop.put("sequenceNumber", t.getSequenceNumber());
        }

        /* Consolidate DC related information */
        final ArrayNode dcArray =
                jsonTop.putArray(DatacenterId.DATACENTER_PREFIX + "s");
        if (filter.contains(Filter.DC)) {
            final List<Datacenter> dcList = t.getSortedDatacenters();
            for (final Datacenter dc : dcList) {
                dcArray.add(dc.toJson());
            }
        }

        /* Create and order the SN map */
        final Map<StorageNodeId, List<RepNodeId>> snMap =
            new TreeMap<StorageNodeId, List<RepNodeId>>();
        for (StorageNodeId snid : t.getStorageNodeMap().getAllIds()) {
            snMap.put(snid, new ArrayList<RepNodeId>());
        }
        final List<RepNode> rnList = t.getSortedRepNodes();
        for (RepNode rn: rnList) {
            final List<RepNodeId> l = snMap.get(rn.getStorageNodeId());
            l.add(rn.getResourceId());
        }

        /* Consolidate SN information */
        final ArrayNode snArray = jsonTop.putArray("sns");
        for (Entry<StorageNodeId, List<RepNodeId>> entry :
             snMap.entrySet()) {

            final StorageNodeId snId = entry.getKey();
            int capacityInfo = -1;
            String rootDirPath = null;
            StorageNodeParams snp = null;
            if (params != null) {
                snp = params.get(snId);
                if (snp != null) {
                    capacityInfo = snp.getCapacity();
                    rootDirPath = snp.getRootDirPath() + "";
                }
            }

            ObjectNode snNode = JsonUtils.createObjectNode();
            /* Report individual SN information */
            if (filter.contains(Filter.SN)) {
                snNode = t.get(snId).toJson();
                if (capacityInfo != -1) {
                    snNode.put("capacity", capacityInfo);
                }
                if (rootDirPath != null) {
                    snNode.put("rootDirPath", rootDirPath);
                }
            }

            /* Report individual RN information */
            if (filter.contains(Filter.RN)) {
                final ArrayNode rnArray = snNode.putArray("rns");
                for (RepNodeId rnId : entry.getValue()) {
                    final ObjectNode rnNode = JsonUtils.createObjectNode();
                    rnNode.put("resourceId", rnId.toString());

                    /* Report storage directory related information */
                    if (verbose) {
                        String storageDirPath = null;
                        long storageDirSize = 0L;
                        String rnlogDirPath = null;
                        if (params != null) {
                            final RepNodeParams rnp = params.get(rnId);
                            if (rnp != null) {
                                storageDirPath =
                                    rnp.getStorageDirectoryPath();
                                storageDirSize =
                                    rnp.getStorageDirectorySize();
                                rnlogDirPath =
                                    rnp.getLogDirectoryPath();
                            }
                        }

                        final String storageDirEnvPath;
                        if (storageDirPath == null) {
                            if (snp != null) {
                                /*
                                 * RN environment under kvroot
                                 * Full path will be :
                                 *     $kvroot/<kvstorename>/<snId>/<rnId>/env
                                 */
                                storageDirEnvPath =
                                    snp.getRootDirPath() + "/" +
                                    t.getKVStoreName() + "/" +
                                    snId.getFullName() + "/" +
                                    rnId.getFullName() + "/" + "env";
                                rnNode.put("storageDirPath",
                                           snp.getRootDirPath());
                                rnNode.put("storageDirEnvPath",
                                        storageDirEnvPath);
                                rnNode.put("storageDirSize",
                                           storageDirSize);
                            }
                        } else {
                            /*
                             * RN environment under storageDir
                             * Full path will be :
                             *     $storageDir/<rnId>/env
                             */
                            storageDirEnvPath = storageDirPath + "/" +
                                                 rnId.getFullName() + "/" +
                                                 "env";
                            rnNode.put("storageDirPath", storageDirPath);
                            rnNode.put("storageDirEnvPath",
                                       storageDirEnvPath);
                            rnNode.put("storageDirSize", storageDirSize);
                        }

                        if (rnlogDirPath != null) {
                            /*
                             * RNLogDir is specified, else logs will be
                             * kvroot/kvstore/log directory.
                             *
                             * Since logs are by default generated in
                             * kvstore/log on SN start up so no need
                             * to add information for topology JSON output.
                             */
                            rnNode.put("logDirPath", rnlogDirPath);
                        }
                    }
                    rnArray.add(rnNode);
                }
            }

            /* Report arbiter related information */
            final ArrayNode anArray = snNode.putArray("ans");
            if (filter.contains(Filter.AN)) {
                for (ArbNodeId anId : t.getHostedArbNodeIds(snId)) {
                    final ObjectNode anNode = JsonUtils.createObjectNode();
                    anNode.put("resourceId", anId.toString());

                    if (verbose) {
                        if (snp != null) {
                            anNode.put("storageDirPath",
                                       snp.getRootDirPath());
                        }
                    }
                    anArray.add(anNode);
                }
            }
            snArray.add(snNode);
        }

        /* Consolidate sharding results */
        final ArrayNode shardArray = jsonTop.putArray("shards");
        if (filter.contains(Filter.SHARD)) {

            final Map<RepGroupId, List<PartitionId>> partToRG =
                TopologyPrinter.sortPartitions(t);
            for (Map.Entry<RepGroupId, List<PartitionId>> entry :
                     partToRG.entrySet()) {
                final RepGroupId rgId = entry.getKey();
                final List<PartitionId> partIds = entry.getValue();
                final RepGroup rg = t.get(rgId);
                final ObjectNode shardNode = rg.toJson();
                shardNode.put("numPartitions", partIds.size());

                if (filter.contains(Filter.RN)) {
                    final List<RepNode> rns =
                        new ArrayList<RepNode>(rg.getRepNodes());
                    Collections.sort(rns);
                    final ArrayNode rnArray = shardNode.putArray("rns");
                    for (RepNode rn: rns) {
                        final ObjectNode rnNode = rn.toJson();
                        if (verbose && params != null) {
                            final RepNodeParams rnp =
                                params.get(rn.getResourceId());
                            if (rnp != null) {
                                rnNode.put("haPort",
                                           rnp.getJENodeHostPort());
                            }
                        }
                        rnArray.add(rnNode);
                    }
                }

                if (filter.contains(Filter.AN)) {
                    final List<ArbNode> ans =
                        new ArrayList<ArbNode>(rg.getArbNodes());
                    Collections.sort(ans);
                    final ArrayNode anArray = shardNode.putArray("ans");
                    for (ArbNode an: ans) {
                        final ObjectNode anNode = an.toJson();
                        if (verbose && params != null) {
                            final ArbNodeParams anp =
                                params.get(an.getResourceId());
                            if (anp != null) {
                                anNode.put("haPort",
                                           anp.getJENodeHostPort());
                            }
                        }
                        anArray.add(anNode);
                    }
                }

                if (verbose) {
                    shardNode.put("partition",
                                  TopologyPrinter.listPartitions(partIds));
                }
                shardArray.add(shardNode);
            }
        }
        return jsonTop;
    }

    private static void printStorageRNLogMountPoints(
                                          Map<RepNodeId, StorageDirectory>
                                                              storageDirMap,
                                          Map<RepNodeId, LogDirectory>
                                                              rnLogDirMap,
                                          List<String> rnsWithMountPoints,
                                          Parameters params, RepNodeId rnId,
                                          String oneRN,
                                          PrintStream out,
                                          int indent,
                                          int indentAmount,
                                          boolean showPerf,
                                          Map<ResourceId, PerfEvent> perfMap,
                                          StorageNodeParams snp) {
        String storageDirPath = null;
        long storageDirSize = 0L;
        if (storageDirMap != null) {
            final StorageDirectory sd = storageDirMap.get(rnId);
            if (sd != null) {
                storageDirPath = sd.getPath();
                storageDirSize = sd.getSize();
            }
        } else {
            if (params != null) {
                final RepNodeParams rnp = params.get(rnId);
                if (rnp != null) {
                    storageDirPath = rnp.getStorageDirectoryPath();
                    storageDirSize = rnp.getStorageDirectorySize();
                }
            }
        }

        String rnLogDirPath = null;
        if (rnLogDirMap != null) {
            final LogDirectory sd = rnLogDirMap.get(rnId);
            if (sd != null) {
                rnLogDirPath = sd.getPath();
            }
        } else {
            if (params != null) {
                final RepNodeParams rnp = params.get(rnId);
                if (rnp != null) {
                    rnLogDirPath = rnp.getLogDirectoryPath();
                }
            }
        }

        printMountPoints(storageDirPath, storageDirSize,
                         rnLogDirPath, rnsWithMountPoints,
                         rnId, oneRN, out, indent, indentAmount,
                         showPerf, perfMap, snp);
    }

    public static void printMountPoints(String dirPath, long dirSize,
                                   String logDirPath,
                                   List<String> rnsWithMountPoints,
                                   RepNodeId rnId,
                                   String oneRN,
                                   PrintStream out,
                                   int indent,
                                   int indentAmount,
                                   boolean showPerf,
                                   Map<ResourceId, PerfEvent> perfMap,
                                   StorageNodeParams snp) {
        String topologyOutput = "";
        final String storagesize = getSizeInUnits(dirSize);
        if (dirPath == null) {
            topologyOutput = oneRN;
            if (snp == null) {
                out.println();
            } else {
                topologyOutput += " storagedir=" + snp.getRootDirPath() +
                                  " size=" + storagesize;
            }
        } else {
            /*
             * Make a description of the RN and its storage
             * directory and display it below
             */
            topologyOutput += oneRN + " storagedir=" + dirPath +
                              " size=" + storagesize;
        }

        if (logDirPath == null) {
            if (snp == null) {
                out.println();
            } else {
                topologyOutput += " logdir=" + snp.getRootDirPath();
            }
        } else {
            /*
             * Make a description of the RN and its rn log
             * directory and display it below
             */
            topologyOutput += " logdir=" + logDirPath;
        }

        rnsWithMountPoints.add(topologyOutput);

        if (showPerf) {
            rnsWithMountPoints.add
                (makeWhiteSpace((indent - 1) * indentAmount) +
                 getPerf(perfMap, rnId));
        }
    }

    public static String getSizeInUnits(long value) {
        if (value == 0) {
            return "NOT-SPECIFIED";
        }

        final long valueinKB = Math.abs(value/1024);
        final long valueinMB = Math.abs(value/(1024*1024));
        final long valueinGB = Math.abs(value/(1024*1024*1024));

        String prefix = value < 0 ? "-" : "";
        if (valueinGB > 0) {
            return prefix + valueinGB + " GB";
        }
        if (valueinMB > 0) {
            return prefix + valueinMB + " MB";
        }
        if (valueinKB > 0) {
            return prefix + valueinKB + " KB";
        }
        return prefix + value + " Bytes";
    }

    /**
     * Get a Json version of the topology. The format is:
     * {
     *    "name" : <topo_name>,
     *    "store" : <store_name>,
     *    "numPartitions" : <partitions>,
     *    "sequence" : <topo_sequence>,
     *    "zone" : {
     *      "id" : <zone_id>,
     *      "name" : <zone_name>,
     *      "repfactor" : <repfactor>,
     *      "type" : {@literal <PRIMARY|SECONDARY>}
     *    }
     *    "sns" : [
     *      {
     *        "id" : <sn_id>,
     *        "zone_id" : <zone_id>,
     *        "host" : <host_name>,
     *        "port" : <port>,
     *        "capacity" : <capacity>,
     *        "rns" : [<rn1_id>, <rn2_id>,...]
     *        "ans" : [<an1_id>,...]
     *      },
     *      ...
     *    ],
     *    "shards" : [
     *      {
     *        "id" : <shard_id>,
     *        "numPartitions" : <partitions>,
     *        "rns" : [<rn1_id>, <rn2_id>,...]
     *        "ans" : [<an1_id>,...]
     *      },
     *    ...
     *    ]
     * }
     * @param topo to be displayed topology
     * @param params SN capacity and RN mount points are stored in the
     * Parameters class from the Admin DB, and is not available in the topology
     * itself. If params is not null, use it to display that kind of
     * information.
     * @return Json node to display the topology
     */
    public static ObjectNode getTopoJson(Topology topo, Parameters params) {
        final ObjectNode json = JsonUtils.createObjectNode();
        json.put("store", topo.getKVStoreName());
        json.put("numPartitions", topo.getPartitionMap().size());
        json.put("sequence", topo.getSequenceNumber());
        final List<Datacenter> dcList = topo.getSortedDatacenters();
        final ArrayNode zoneNodes = json.putArray("zone");
        for (Datacenter dc : dcList) {
            final ObjectNode zoneNode = JsonUtils.createObjectNode();
            zoneNode.put("id", dc.getResourceId().toString());
            zoneNode.put("name", dc.getName());
            zoneNode.put("repfactor", dc.getRepFactor());
            zoneNode.put("type", dc.getDatacenterType().name());
            zoneNodes.add(zoneNode);
        }
        final ArrayNode snNodes = json.putArray("sns");
        List<StorageNode> snList = topo.getSortedStorageNodes();
        for (StorageNode sn : snList) {
            final StorageNodeId snId = sn.getResourceId();
            final ObjectNode snNode = JsonUtils.createObjectNode();
            snNode.put("id", snId.toString());
            snNode.put("zone_id", sn.getDatacenterId().toString());
            snNode.put("host", sn.getHostname());
            snNode.put("port", sn.getRegistryPort());
            if (params != null) {
                final StorageNodeParams snp = params.get(snId);
                snNode.put("capacity", snp.getCapacity());
            }
            final Set<RepNodeId> rnIds = topo.getHostedRepNodeIds(snId);
            final ArrayNode rnNodes = snNode.putArray("rns");
            for (RepNodeId rnId : rnIds) {
                rnNodes.add(rnId.toString());
            }
            final Set<ArbNodeId> anIds = topo.getHostedArbNodeIds(snId);
            final ArrayNode anNodes = snNode.putArray("ans");
            for (ArbNodeId anId : anIds) {
                anNodes.add(anId.toString());
            }
            snNodes.add(snNode);
        }
        final ArrayNode rgNodes = json.putArray("shards");
        final Map<RepGroupId, List<PartitionId>> partToRG =
            sortPartitions(topo);
        for (Map.Entry<RepGroupId, List<PartitionId>> entry :
            partToRG.entrySet()) {

            final ObjectNode rgNode = JsonUtils.createObjectNode();
            final RepGroupId rgId = entry.getKey();
            rgNode.put("id", rgId.toString());
            final List<PartitionId> partIds = entry.getValue();
            rgNode.put("numPartitions", partIds.size());
            final RepGroup rg = topo.get(rgId);
            final List<RepNode> rns = new ArrayList<>(rg.getRepNodes());
            Collections.sort(rns);
            final ArrayNode rnNodes = rgNode.putArray("rns");
            for (RepNode rn : rns) {
                rnNodes.add(rn.getResourceId().toString());
            }
            final List<ArbNode> ans = new ArrayList<>(rg.getArbNodes());
            Collections.sort(ans);
            final ArrayNode anNodes = rgNode.putArray("ans");
            for (ArbNode an : ans) {
                anNodes.add(an.getResourceId().toString());
            }
            rgNodes.add(rgNode);
        }
        return json;
    }

    /* Format service status information. */
    private static String getStatus(Map<ResourceId, ServiceChange> statusMap,
                                    ResourceId rId) {
        final ServiceChange change = statusMap.get(rId);
        if (change == null) {
            return "UNREPORTED";
        }


        return change.getStatus().toString();
    }

    private static String getStatus(Map<ResourceId, ServiceChange> statusMap,
                                    RepNodeId rnId,
                                    Parameters params) {
        final String status = getStatus(statusMap, rnId);

        /* RNs may be disabled by a stop-RNs plan */
        if (params != null) {
            final RepNodeParams rnp = params.get(rnId);
            if (rnp != null) {
                if (rnp.isDisabled()) {
                    return "Stopped/" + status;
                }
            }
        }

        return status;
    }


    private static String getStatus(Map<ResourceId, ServiceChange> statusMap,
                                    ArbNodeId anId,
                                    Parameters params) {
        final String status = getStatus(statusMap, anId);

        /* ANs may be disabled by a stop-service plan */
        if (params != null) {
            final ArbNodeParams anp = params.get(anId);
            if (anp != null) {
                if (anp.isDisabled()) {
                    return "Stopped/" + status;
                }
            }
        }

        return status;
    }

    /* Format performance information. */
    private static String getPerf(Map<ResourceId, PerfEvent> perfMap,
                                    ResourceId rId) {
        final PerfEvent perf = perfMap.get(rId);
        if (perf == null) {
            return "No performance info available";
        }

        final StringBuilder sb = new StringBuilder();
        final LatencyInfo singleCum = perf.getSingleCum();
        final LatencyInfo multiCum = perf.getMultiCum();
        sb.append("   single-op avg latency=").
            append(NANOSECONDS.toMillis(singleCum.getLatency().getAverage())).
            append(" ms");

        sb.append("   multi-op avg latency=").
            append(NANOSECONDS.toMillis(multiCum.getLatency().getAverage())).
            append(" ms");

        if (perf.needsAlert()) {
            sb.append("[ALERT]");
        }

        return sb.toString();
    }

    /**
     * List the partitions ordered by number, and in range form. For example,
     * gaps would be shown like this:
     * 10-40,45-95,100
     */
    public static String listPartitions(List<PartitionId> partIds) {

        if (partIds.isEmpty()) {
            return "";
        }

        Collections.sort(partIds, new Comparator<PartitionId>() {
                @Override
                public int compare(PartitionId o1, PartitionId o2) {
                   return o1.getPartitionId() - o2.getPartitionId();
                }});

        int first = partIds.get(0).getPartitionId();
        int last = first;
        final StringBuilder sb = new StringBuilder();
        sb.append(first);
        for (PartitionId p : partIds) {
            final int pId = p.getPartitionId();

            if (pId == last) {
                continue;
            }

            if (pId == last + 1) {
                last = pId;
                continue;
            }

            if (last > first) {
                sb.append("-").append(last);
            }

            first = pId;
            last = first;
            sb.append(",").append(first);
        }
        if (last > first) {
            sb.append("-").append(last);
        }
        return sb.toString();
    }

    /**
     * Returns the Topology as a String.
     */
    public static String printTopology(Topology t,
                                       Parameters params,
                                       boolean verbose) {
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        printTopology(t, new PrintStream(outStream), params, all, verbose,
                      null, null, null, null);
        return outStream.toString();
    }

    /**
     * Returns the Topology as a String, for logging messages.
     */
    public static String printTopology(Topology t) {
        return printTopology(t, null, false);
    }

    public static String printTopology(TopologyCandidate tc,
                                       Parameters params,
                                       boolean verbose) {
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        printTopology(tc.getTopology(), new PrintStream(outStream), params,
                      all, verbose, tc.getStorageDirAssignments(params),
                      tc.getRNLogDirAssignments(params),
                      null, null);
        return outStream.toString();
    }


    private static String makeWhiteSpace(int indent) {
        String ret = "";
        for (int i = 0; i < indent; i++) {
            ret += " ";
        }
        return ret;
    }

    /**
     * Return a map of rep groups to partitions ids, so one can tell the
     * number of partitions assigned to each rep group.
     */
    public static
        Map<RepGroupId, List<PartitionId>> sortPartitions(Topology t) {

        final Map<RepGroupId, List<PartitionId>> partToRG =
            new TreeMap<RepGroupId, List<PartitionId>>(new RGComparator());

        for (RepGroup rg: t.getRepGroupMap().getAll()) {
            partToRG.put(rg.getResourceId(), new ArrayList<PartitionId>());
        }

        for (Partition p : t.getPartitionMap().getAll()) {
            final List<PartitionId> pIds = partToRG.get(p.getRepGroupId());
            pIds.add(p.getResourceId());
        }
        return partToRG;
    }

    private static class RGComparator implements Comparator<RepGroupId> {

        @Override
        public int compare(RepGroupId o1, RepGroupId o2) {
           return (o1.getGroupId() - o2.getGroupId());
        }
    }

    /**
     * Displays information about one or all zones in the given
     * <code>Topology</code>. If the <code>id</code> and <code>name</code>
     * parameters are both <code>null</code>, then this method will simply list
     * the ids and names of all of the zones in the store. Otherwise, this
     * method will display information about the zone having the given
     * <code>id</code> or <code>name</code>; including all of the storage nodes
     * deployed to that zone, and whether the zone consists of secondary zones.
     * Note that if the <code>id</code> and <code>name</code> parameters
     * are both non-<code>null</code>, then the value of the <code>id</code>
     * parameter will be used.
     *
     * @param id The id of the zone whose information should be displayed.
     * @param name The name of the zone whose information should be displayed.
     * @param topo The <code>Topology</code> from which to retrieve the
     * information to display.
     * @param out The <code>OutputStream</code> to which the desired
     * information will be written for display.
     * @param params If not <code>null</code>, then each storage node's
     * capacity and datacenter type are retrieved from this object and
     * displayed with that storage node's information.
     */
    public static void printZoneInfo(DatacenterId id,
                                     String name,
                                     Topology topo,
                                     PrintStream out,
                                     Parameters params) {

        int indent = 0;
        final int indentAmount = 2;
        final boolean showAll =
            ((id == null) && (name == null) ? true : false);
        Datacenter showZone = null;

        /*
         * Display zones, sorted by ID
         */
        final List<Datacenter> dcList = topo.getSortedDatacenters();
        for (final Datacenter zone : dcList) {
            if (showAll) {
                out.println(makeWhiteSpace(indent) +
                            DatacenterId.DATACENTER_PREFIX + ": " + zone);
            } else {
                if ((id != null) && id.equals(zone.getResourceId())) {
                    showZone = zone;
                    break;
                } else if ((name != null) && name.equals(zone.getName())) {
                    showZone = zone;
                    break;
                }
            }
        }
        if (showAll) {
            return;
        }

        /* If showZone is null, then the id or name input is unknown */
        if (showZone == null) {
            out.println(makeWhiteSpace(indent) +
                        DatacenterId.DATACENTER_PREFIX +
                        ": unknown id or name");
            return;
        }

        /*
         * For the given zone (id or name), display SNs, sorted by SN id.
         */
        out.println(makeWhiteSpace(indent) +
                    DatacenterId.DATACENTER_PREFIX + ": " + showZone);

        final DatacenterId showZoneId = showZone.getResourceId();
        final List<StorageNode> snList = topo.getSortedStorageNodes();
        String capacityInfo = null;
        StorageNodeParams snp = null;
        indent += indentAmount;
        for (StorageNode sn: snList) {
            if (showZoneId.equals(sn.getDatacenterId())) {
                out.print(makeWhiteSpace(indent) + "[" + sn.getResourceId() +
                    "] " + sn.getHostname() + ":" + sn.getRegistryPort());
                if (params != null) {
                    snp = params.get(sn.getResourceId());
                    if (snp != null) {
                        capacityInfo = " capacity=" + snp.getCapacity();
                    }
                }
                if (capacityInfo != null) {
                    out.print(capacityInfo);
                }
                out.println();
            }
        }
    }

    /**
     * Print the admins information in JSON format. The example format:
     *   "admins" : [ {
     *     "resourceId" : "admin1",
     *     "snId" : "sn1",
     *     "storageDirPath" : "/xyz/admin1",
     *     "storageDirSize" : "3221225472",
     *     "storageDirEnvPath" : "/xyz/admin1/env"
     *   }, {
     *     "resourceId" : "admin2",
     *     "snId" : "sn2",
     *     "storageDirPath" : "/xyz/admin2",
     *     "storageDirSize" : "3221225472",
     *     "storageDirEnvPath" : "/xyz/admin2/env"
     *   }, {
     *     "resourceId" : "admin3",
     *     "snId" : "sn3",
     *     "storageDirPath" : "/xyz/admin3",
     *     "storageDirSize" : "3221225472",
     *     "storageDirEnvPath : "/zyz/admin3/env"
     *   } ]
     *
     * To note : SNRecover.AdminJSON class is dependent on printAdminJSON
     * output for de-serializing admin related JSON output. In case of any
     * changes in output, make sure to do similar required changes in
     * SNRecover.AdminJSON.
     *
     * @param topology the topology instance
     * @param params the parameter value set
     * @return JSON object representing the topology result
     */
    public static ObjectNode printAdminJSON(Topology topology,
                                            Parameters params) {

        final ObjectNode jsonTop = JsonUtils.createObjectNode();

        final List<StorageNodeId> snIds = topology.getSortedStorageNodeIds();

        /* Consolidate Admin information */
        final ArrayNode snArray = jsonTop.putArray("admins");
        for (StorageNodeId snId : snIds) {

            /*
             * Get the single parameter entries -- StorageNode, Admin
             */
            final StorageNodeParams snp = params.get(snId);
            final AdminParams ap =
                GenerateConfig.getAdminParams(snId, params);
            if (ap == null) {
                /*
                 * This SN does not host an admin
                 */
                continue;
            }

            final ParameterMap adminMountMap = snp.getAdminDirMap();
            final ObjectNode adminNode = JsonUtils.createObjectNode();
            adminNode.put("adminId", ap.getAdminId().getFullName());
            adminNode.put("storageNodeId", snId.getFullName());

            /*
             * adminMountMap contains single entry corresponding to
             * individual admin storage directory.
             */
            String adminDir = null;
            long adminDirSize = 0L;
            if (adminMountMap != null) {
                for (Parameter adminDirParam : adminMountMap) {
                    adminDir = adminDirParam.getName();
                    adminDirSize = SizeParameter.getSize(adminDirParam);
                }
            }

            final String adminDirEnvPath;
            if (adminDir == null) {
                /*
                 * admin environment under kvroot
                 * Full path will be :
                 *     $kvroot/<kvstorename>/<snId>/<adminid>/env
                 */
                adminDirEnvPath = snp.getRootDirPath() + "/" +
                                   topology.getKVStoreName() + "/" +
                                   snId.getFullName() + "/" +
                                   ap.getAdminId().getFullName() + "/" +
                                   "env";
                adminNode.put("storageDirPath", snp.getRootDirPath());
                adminNode.put("storageDirEnvPath", adminDirEnvPath);
            } else {
                /*
                 * admin environment under adminDir
                 * Full path will be :
                 *     $adminDir/<adminid>/env
                 */
                adminDirEnvPath = adminDir + "/" +
                                   ap.getAdminId().getFullName() + "/" +
                                   "env";
                adminNode.put("storageDirPath", adminDir);
                adminNode.put("storageDirEnvPath", adminDirEnvPath);
            }
            adminNode.put("storageDirSize", adminDirSize);
            snArray.add(adminNode);
        }
        return jsonTop;
    }
}
