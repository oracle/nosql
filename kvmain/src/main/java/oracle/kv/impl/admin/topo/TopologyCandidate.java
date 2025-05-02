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

package oracle.kv.impl.admin.topo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.kv.impl.admin.param.DatacenterParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.SizeParameter;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TopologyPrinter;

import oracle.nosql.common.json.ObjectNode;

/**
 * The topology candidate packages:
 * - a user provided name
 * - a target topology that is a prospective layout for the store
 * - information about how we arrived at the target topology, used to illuminate
 * the decision process for the user, and as a debugging aid.
 *
 * Candidates are generated and updated in response to user actions. The
 * "topology create" command makes the initial candidate, while commands such
 * as "topology redistribute" modify a candidate. Candidates are only
 * blueprints for potential store layouts. A topology that is actually deployed
 * is represented by a RealizedTopology instance.
 */
public class TopologyCandidate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * NO_NAME is used for situations where we need compatibility with R1,
     * which is before candidates existed, or for topology changing commands
     * that do not require a user created candidate topology. For example, the
     * migrate-sn command changes the topology, but since the parameters to the
     * command adequately describe the change, that command doesn't require
     * that the user supply a candidate,
     */
    public static final String NO_NAME = "none";

    /**
     * Candidate names containing this substring are reserved for system use.
     */
    public static final String RESERVED_SUBSTRING = "$";

    /**
     * Candidate names starting with this prefix are reserved for internal
     * system use.  These names are for topology candidates that are not
     * intended to be referred to by users.
     */
    public static final String INTERNAL_NAME_PREFIX = "$$internal-";

    private final String candidateName;

    private Topology topology;

    /**
     * Map of the rep nodes which have assigned storage directories. The
     * map contains the directory path. The directory size needs to be
     * obtained from the parameters.
     *
     * Note that we now use "storage directory" instead of "mount point", but
     * since the field is serialized it would be a pain to change.
     */
    private final Map<RepNodeId, String> mountPointAssignments
                                                            = new HashMap<>();

    /**
     * Map of the rep nodes which have assigned RN log directories. The
     * map contains the RN log directory path. The directory size needs to be
     * obtained from the parameters.
     */
    private Map<RepNodeId, String> rnLogDirAssignments = new HashMap<>();

    /* For debugging/auditing */
    private final Map<DatacenterId,Integer> maxShards = new HashMap<>();
    private DatacenterId smallestDC;
    private final List<String> auditTrail = new ArrayList<>();

    public TopologyCandidate(String name, Topology topoCopy) {
        candidateName = name;
        topology = topoCopy;
        log("starting number of shards: " +  topoCopy.getRepGroupMap().size());
        log("number of partitions: " + topoCopy.getPartitionMap().size());
    }

    /**
     * Constructor for creating a copy of an existing candidate. Debug and
     * auditing information is _not_ copied from the source candidate.
     */
    private TopologyCandidate(TopologyCandidate source) {
        this(source.getName(), source.getTopology().getCopy());
        mountPointAssignments.putAll(source.mountPointAssignments);
        rnLogDirAssignments.putAll(source.rnLogDirAssignments);
    }

    /**
     * Gets a copy of this candidate. The debug and auditing information is
     * cleared in the returned candidate.
     */
    public TopologyCandidate getCopy() {
        return new TopologyCandidate(this);
    }

    public Topology getTopology() {
       return topology;
    }

    public void resetTopology(Topology topo) {
        topology = topo;
    }

    public String getName() {
       return candidateName;
    }

    public void setShardsPerDC(DatacenterId dcId, int numShards) {
        maxShards.put(dcId, numShards);
    }

    public void setSmallestDC(DatacenterId dcId) {
        smallestDC = dcId;
    }

    String getSmallestDCName(Parameters params) {
        if (smallestDC != null) {
            return smallestDC + "/" + params.get(smallestDC).getName();
        }
        return null;
    }

    /**
     * Display the candidate. If verbose is specified, show the fields meant
     * for auditing/debug use.
     */
    String display(Parameters params) {
        StringBuilder sb = new StringBuilder();
        sb.append("name= ").append(candidateName);
        sb.append("\nzones= ").append(getDatacenters());
        sb.append("\n");
        sb.append(TopologyPrinter.printTopology(this, params, false));

        return sb.toString();
    }

    public String displayAsJson(Parameters params) {
        ObjectNode jsonNode = TopologyPrinter.getTopoJson(topology, params);
        jsonNode.put("name", candidateName);
        return jsonNode.toString();
    }

    public Set<DatacenterId> getDatacenters() {
        Set<DatacenterId> dcIds = new HashSet<>();
        for (Datacenter dc : topology.getDatacenterMap().getAll()) {
            dcIds.add(dc.getResourceId());
        }
        return dcIds;
    }

    public String showDatacenters(Parameters params) {

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<DatacenterId,Integer> entry : maxShards.entrySet()) {
            DatacenterId dcId = entry.getKey();
            DatacenterParams dcp = params.get(dcId);
            sb.append(dcId).append("/").append(dcp.getName());
            sb.append(" maximum shards= ").append(entry.getValue());
            sb.append("\n");
        }
        return sb.toString();
    }

    String showAudit() {
        StringBuilder sb = new StringBuilder();
        sb.append("[Audit]\n");
        for(String s : auditTrail) {
            sb.append(s).append("\n");
        }
        return sb.toString();
    }

    /**
     * Save some information about the building of the topology, for audit/
     * debugging assistance later.
     */
    public void log(String s) {
        auditTrail.add(s);
    }

    /**
     * Saves the specified directory path assignment.
     *
     * @param rnId the rn to be assigned
     * @param path the directory path the rn is assigned to
     */
    void saveDirectoryAssignment(RepNodeId rnId, String path) {
        mountPointAssignments.put(rnId, path);
    }

    /**
     * Removes the assignment for the specified rn.
     *
     * @param rnId the rn to remove
     */
    void removeDirectoryAssignment(RepNodeId rnId) {
        mountPointAssignments.remove(rnId);
    }

    /**
     * Gets a map of directory paths. There will be one entry for
     * each rep node which has a directory assignment.
     */
    public Map<RepNodeId, String> getDirectoryAssignments() {
        return mountPointAssignments;
    }

    /**
     * Saves the specified RN log directory path assignment.
     *
     * @param rnId the rn to be assigned
     * @param path the rn log directory path the rn is assigned to
     */
    void saveRNLogDirectoryAssignment(RepNodeId rnId, String path) {
        rnLogDirAssignments.put(rnId, path);
    }

    /**
     * Removes the assignment for the specified RN log dir.
     *
     * @param rnId the rn to remove
     */
    void removeRNLogDirectoryAssignment(RepNodeId rnId) {
        rnLogDirAssignments.remove(rnId);
    }

    /**
     * Gets a map of RN log directory paths. There will be one entry for
     * each rep node which has a RN log directory assignment.
     */
    public Map<RepNodeId, String> getRNLogDirectoryAssignments() {
        return rnLogDirAssignments;
    }

    /**
     * Gets a StorageDirectory instance for the specified rep node. If the
     * rep node is not assigned a directory, the returned StorageDirectory will
     * be for the root directory.
     */
    StorageDirectory getStorageDir(RepNodeId rnId, Parameters params) {
        return getStorageDir(rnId, this, topology, params);
    }

    /**
     * Gets a RNlogDirectory instance for the specified rep node. If the
     * rep node is not assigned a RN log directory, the returned RN log
     * Directory will be for the root directory.
     */
    LogDirectory getRNLogDir(RepNodeId rnId, Parameters params) {
        return getRNLogDir(rnId, this, params);
    }

    /**
     * Gets a map of StorageDirectory instances. There will be one entry for
     * each rep node which has a directory assignment.
     */
    public Map<RepNodeId, StorageDirectory>
                                  getStorageDirAssignments(Parameters params) {
        final Map<RepNodeId, StorageDirectory> sdm =
                                    new HashMap<>(mountPointAssignments.size());
        for (RepNodeId rnId : mountPointAssignments.keySet()) {
            sdm.put(rnId, getStorageDir(rnId, params));
        }
        return sdm;
    }

    /**
     * Gets a map of RNlogDirectory instances. There will be one entry for
     * each rep node which has a directory assignment.
     */
    public Map<RepNodeId, LogDirectory>
                                  getRNLogDirAssignments(Parameters params) {
        final Map<RepNodeId, LogDirectory> sdm =
                                  new HashMap<>(rnLogDirAssignments.size());
        for (RepNodeId rnId : rnLogDirAssignments.keySet()) {
            sdm.put(rnId, getRNLogDir(rnId, params));
        }
        return sdm;
    }

    /**
     * Gets a StorageDirectory instance for the specified rep node. If
     * candidate is not null the candidate assignments are checked first for
     * a directory path otherwise the path is obtained from the rep node
     * parameters. The directory size is obtained from the storage node
     * parameters.
     */
    static StorageDirectory getStorageDir(RepNodeId rnId,
                                          TopologyCandidate candidate,
                                          Topology topo,
                                          Parameters params) {
        /*
         * If a candidate was specified and it contains an assignment use that,
         * otherwise check the rep node parameters for directory path.
         */
        final String path;
        if ((candidate != null) &&
            candidate.mountPointAssignments.containsKey(rnId)) {
            path = candidate.mountPointAssignments.get(rnId);
        } else {
            final RepNodeParams rnp = params.get(rnId);
            path = (rnp == null) ? null : rnp.getStorageDirectoryPath();
        }

        /*
         * Get the size from the storage note parameters. If path is null use
         * the root directory's size.
         */
        final StorageNodeId snId = topo.get(rnId).getStorageNodeId();
        final StorageNodeParams snp = params.get(snId);
        final long size;
        if (path == null) {
            size = snp.getRootDirSize();
        } else {
            final ParameterMap storageDirMap = snp.getStorageDirMap();
            size = (storageDirMap == null) ? 0L :
                                SizeParameter.getSize(storageDirMap.get(path));
        }
        return new StorageDirectory(path, size);
    }

    /**
     * Gets a RNLogDirectory instance for the specified rep node. If
     * candidate is not null the candidate assignments are checked first for
     * a directory path otherwise the path is obtained from the rep node
     * parameters. The directory size is set to default 0L till rnlogdirsize
     * is exposed.
     */
    static LogDirectory getRNLogDir(RepNodeId rnId,
                                    TopologyCandidate candidate,
                                    Parameters params) {
        /*
         * If a candidate was specified and it contains an assignment use that,
         * otherwise check the rep node parameters for directory path.
         */
        final String path;
        if ((candidate != null) &&
            candidate.rnLogDirAssignments.containsKey(rnId)) {
            path = candidate.rnLogDirAssignments.get(rnId);
        } else {
            final RepNodeParams rnp = params.get(rnId);
            path = (rnp == null) ? null : rnp.getLogDirectoryPath();
        }

        return new LogDirectory(path, 0L);
    }

    private void readObject(ObjectInputStream in)
        throws ClassNotFoundException, IOException {
        in.defaultReadObject();
        /* After an upgrade rnLogDirAssignments can be null */
        if (rnLogDirAssignments == null) {
            rnLogDirAssignments = new HashMap<>();
        }
    }
    
    @Override
    public String toString() {
        return "TopologyCandidate[" + candidateName + "]";
    }
}
