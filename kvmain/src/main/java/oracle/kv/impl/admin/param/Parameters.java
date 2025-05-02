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

package oracle.kv.impl.admin.param;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

/**
 * Classes in the package oracle.kv.impl.admin.param comprise the
 * repository of configuration and management information for a KVS instance.
 * An object implementing Parameters is the gateway to all such information.
 * <p>
 * Parameters information is organized into two broad categories: global
 * parameters and those associated with particular components of the topology.
 * <ol><li>
 * Global parameters include fundamental KVS configuration values, and
 * system-wide default values for per-component parameters.
 * </li><li>
 * Per-component parameters default to the values given in the system-wide
 * defaults, unless an entry exists that corresponds to a particular component.
 * </li></ol>
 * Per-component parameters may be associated with either StorageNode instances
 * or RepNode instances.  Parameters objects containing such parameters are
 * keyed by the same ResourceId as their corresponding topology components.
 * <p>
 * The Parameters object and all of its referents are stored in the GADB.
 */
public class Parameters implements Serializable {

    private static final long serialVersionUID = 1L;

    public final static String DEFAULT_POOL_NAME = "AllStorageNodes";

    /* The components of Parameters. */
    private GlobalParams globalParams;
    private final Map<StorageNodeId, StorageNodeParams> storageNodeParams;
    private final Map<RepNodeId, RepNodeParams> repNodeParams;
    private final Map<DatacenterId, DatacenterParams> datacenterParams;

    private final Map<AdminId, AdminParams> adminParams;
    private int nextAdminId;

    private Map<ArbNodeId, ArbNodeParams> arbNodeParams;


    private final Map<String, StorageNodePool> storageNodePools;

    /*
     * These policy param components have been set by the user with the
     * desired system wide values. They are to be used as the default settings
     * for this particular kvstore. If the user has done no configuration,
     * these standard components are defaulted.
     */
    private final ParameterMap policyParams;

    /**
     * Creates an empty Parameters instance.
     */
    public Parameters(String kvsName) {

        globalParams = new GlobalParams(kvsName);

        repNodeParams = new HashMap<>();
        storageNodeParams = new HashMap<>();
        datacenterParams = new HashMap<>();
        adminParams = new HashMap<>();
        arbNodeParams = new HashMap<>();
        nextAdminId = 1;

        storageNodePools = new HashMap<>();
        addStorageNodePool(DEFAULT_POOL_NAME);

        policyParams = ParameterMap.createDefaultPolicyMap();
    }

    /**
     * Make a shallow copy, for unit testing.
     */
    public Parameters(Parameters orig) {
        globalParams = new GlobalParams(orig.globalParams.getMap().copy());
        repNodeParams = new HashMap<>(orig.repNodeParams);
        storageNodeParams = new HashMap<>(orig.storageNodeParams);
        datacenterParams = new HashMap<>(orig.datacenterParams);
        adminParams = new HashMap<>(orig.adminParams);
        nextAdminId = orig.nextAdminId;
        storageNodePools = new HashMap<>(orig.storageNodePools);
        policyParams = orig.policyParams.copy();
        arbNodeParams = new HashMap<>(orig.arbNodeParams);
    }

    /**
     * Return a copy of the policyParams map.
     */
    public ParameterMap copyPolicies() {
        return policyParams.copy();
    }

    /**
     * Return the actual policyParams map.
     */
    public ParameterMap getPolicies() {
        return policyParams;
    }

    /**
     * Allow a partial map for policy setting and merge vs replace the old map.
     */
    public void setPolicies(ParameterMap newPolicyParams) {
        policyParams.merge(newPolicyParams, true);
    }

    /**
     * Returns the RepNodeParams object associated with the given ResourceId.
     */
    public RepNodeParams get(RepNodeId id) {
        return repNodeParams.get(id);
    }

    /**
     * Add a new RepNodeParams created elsewhere. This should be the first
     * instance of a RepNodeParams.
     */
    public void add(RepNodeParams rnp) {
        update(rnp, true);
    }

    /**
     * Update a RepNodeParams.
     */
    public void update(RepNodeParams rnp) {
        update(rnp, false);
    }

    private void update(RepNodeParams rnp, boolean shouldBeFirst) {
        RepNodeId rnid = rnp.getRepNodeId();
        RepNodeParams ov = repNodeParams.put(rnid, rnp);
        if ((shouldBeFirst) && (ov != null)) {
            throw new NonfatalAssertionException
                ("Attempt to add a duplicate RepNodesParams with id " + rnid);
        }
    }

    /**
     * Removes the RepNodeParams object associated with the given ResourceId.
     */
    public RepNodeParams remove(RepNodeId id) {
        RepNodeParams ov = repNodeParams.remove(id);
        if (ov == null) {
            throw new NonfatalAssertionException
                ("Attempt to remove a nonexistent RepNodesParams with id " +
                 id);
        }
        return ov;
    }

    /**
     * Returns the StorageNodeParams object associated with the given
     * ResourceId.
     */
    public StorageNodeParams get(StorageNodeId id) {
        return storageNodeParams.get(id);
    }

    /**
     * Add a new StorageNodeParams created elsewhere.
     * The StorageNodeId key is taken from the object itself.
     */
    public void add(StorageNodeParams snp) {
        update(snp, true);
    }

    /**
     * Update an existing StorageNodeParams.
     */
    public void update(StorageNodeParams snp) {
        update(snp, false);
    }

    /**
     * Add a new StorageNodeParams created elsewhere.
     * The StorageNodeId key is taken from the object itself.
     */
    private void update(StorageNodeParams snp, boolean shouldBeFirst) {
        StorageNodeId snid = snp.getStorageNodeId();
        StorageNodeParams ov = storageNodeParams.put(snid, snp);
        if (shouldBeFirst && (ov != null)) {
            throw new NonfatalAssertionException
                ("Attempt to add a duplicate StorageNodesParams with id " +
                 snid);
        }
    }

    /**
     * Removes the StorageNodeParams object associated with the given
     * StorageNodeId and remove it from all pools. Method is intentionally
     * idempotent.
     */
    public StorageNodeParams remove(StorageNodeId id) {
        for (StorageNodePool pool : storageNodePools.values()) {
            if (pool.contains(id)) {
                pool.remove(id);
            }
        }
        StorageNodeParams ov = storageNodeParams.remove(id);
        return ov;
    }

    /**
     * Returns the DatacenterParams object associated with the given
     * ResourceId.
     */
    public DatacenterParams get(DatacenterId id) {
        return datacenterParams.get(id);
    }

    /**
     * Get the raw Datacenter map.
     */
    public Map<DatacenterId, DatacenterParams> getDatacenterMap() {
        return datacenterParams;
    }

    /**
     * Add a new DatacenterParams created elsewhere.
     * The DatacenterId key is taken from the object itself.
     */
    public void add(DatacenterParams dcp) {
        DatacenterId dcid = dcp.getDatacenterId();
        DatacenterParams ov = datacenterParams.put(dcid, dcp);
        if (ov != null) {
            throw new NonfatalAssertionException
                ("Attempt to add a duplicate DatacentersParams with id " +
                 dcid);
        }
    }

    /**
     * Removes the DatacenterParams object associated with the given
     * ResourceId.
     */
    public DatacenterParams remove(DatacenterId id) {
        DatacenterParams ov = datacenterParams.remove(id);
        if (ov == null) {
            throw new NonfatalAssertionException
                ("Attempt to remove a nonexistent DatacentersParams with id " +
                 id);
        }
        return ov;
    }

    /*
     * Returns the AdminParams object associated with the given
     * ResourceId.
     */
    public AdminParams get(AdminId id) {
        return adminParams.get(id);
    }

    /**
     * Add a new AdminParams created elsewhere.
     * The AdminId key is taken from the object itself.
     */
    public void add(AdminParams ap) {
        AdminId aid = ap.getAdminId();
        AdminParams ov = adminParams.put(aid, ap);
        if (ov != null) {
            throw new NonfatalAssertionException
                ("Attempt to add a duplicate AdminParams with id " + aid);
        }
    }

    /**
     * Update the AdminParams.
     */
    public void update(AdminParams ap) {
        AdminId aid = ap.getAdminId();
        adminParams.put(aid, ap);
    }

    /**
     * Removes the AdminParams object associated with the given
     * ResourceId.
     */
    public AdminParams remove(AdminId id) {
        AdminParams ov = adminParams.remove(id);
        if (ov == null) {
            throw new NonfatalAssertionException
                ("Attempt to remove a nonexistent AdminsParams with id " +
                 id);
        }
        return ov;
    }

    /**
     * Produce the next AdminId value.  Parameters should be persisted after
     * getting the next value, to ensure that the same value is not given out
     * more than once, even after a crash.
     */
    public AdminId getNextAdminId() {
        return new AdminId(nextAdminId++);
    }

    /**
     * Returns the number of AdminParams instances in the system.
     */
    public int getAdminCount() {
        return adminParams.size();
    }

    /**
     * Returns a list of the desired Admin ids. If <code>null</code> is input
     * for the datacenter id, then the ids of all Admins in the store --
     * regardless of datacenter -- will be returned. Otherwise, the ids of all
     * Admins deployed to the specified datacenter will be returned.
     *
     * @throws IllegalArgumentException if a non-<code>null</code> value is
     *         input for the <code>dcid</code> but <code>null</code> is input
     *         for the <code>topology</code>.
     */
    public Set<AdminId> getAdminIds(DatacenterId dcid, Topology topology) {
        if (dcid == null) {
            return adminParams.keySet();
        }

        if (topology == null) {
            throw new IllegalArgumentException(
                          "topology cannot be null when dcid is non-null");
        }

        final Set<AdminId> adminIds = new HashSet<>();
        for (Map.Entry<AdminId, AdminParams> entry : adminParams.entrySet()) {
            final AdminId aid = entry.getKey();
            final AdminParams params = entry.getValue();
            final StorageNodeId sid = params.getStorageNodeId();
            if (dcid.equals(topology.getDatacenter(sid).getResourceId())) {
                adminIds.add(aid);
            }
        }
        return adminIds;
    }

    public Set<AdminId> getAdminIds() {
        return getAdminIds(null, null);
    }

    /**
     * Returns the parameter map associated with the specified resource ID, if
     * available.
     *
     * @param resourceId the resource ID
     * @return the parameter map, or null if the resource is not found, if
     * there are no parameters associated with the resource, or if the
     * parameters for the resource do not have an underlying parameter map
     */
    public ParameterMap getMap(ResourceId resourceId) {
        final ParamsWithMap params;
        if (resourceId instanceof AdminId) {
            params = get((AdminId) resourceId);
        } else if (resourceId instanceof RepNodeId) {
            params = get((RepNodeId) resourceId);
        } else if (resourceId instanceof StorageNodeId) {
            params = get((StorageNodeId) resourceId);
        } else if (resourceId instanceof ArbNodeId) {
            params = get((ArbNodeId) resourceId);
        } else {
            assert resourceId instanceof ClientId ||
                resourceId instanceof DatacenterId ||
                resourceId instanceof PartitionId ||
                resourceId instanceof RepGroupId :
                "Update getResourceParams for new resource ID type: " +
                resourceId.getClass();
            /* No parameters for these resources */
            return null;
        }
        if (params == null) {
            return null;
        }
        return params.getMap();
    }

    /**
     * Return a set of all AdminParams in the system.
     */
    public Collection<AdminParams> getAdminParams() {
        return adminParams.values();
    }

    /**
     * Return a set of all RepNodeParams in the system.
     */
    public Collection<RepNodeParams> getRepNodeParams() {
        return repNodeParams.values();
    }

    /**
     * Returns the global OperationalParameters object.
     */
    public GlobalParams getGlobalParams() {
        return globalParams;
    }

    /**
     * Update existing GlobalParams
     */
    public void update(GlobalParams gp) {
        globalParams = gp;
    }

    /**
     * Return the names of all storage node pools.
     */
    public Set<String> getStorageNodePoolNames() {
        return storageNodePools.keySet();
    }

    /**
     * Add a new StorageNodePool identified by @param name
     */
    public StorageNodePool addStorageNodePool(String name) {
        StorageNodePool newPool = new StorageNodePool(name);
        StorageNodePool ov = storageNodePools.put(name, newPool);

        if (ov != null) {
            throw new NonfatalAssertionException
                ("Attempt to add a Pool with a name that is already in use: " +
                 name);
        }
        return newPool;
    }

    /**
     * Remove an existing StorageNodePool identified by @param name.
     */
    public void removeStorageNodePool(String name) {
        if (storageNodePools == null) {
            throw new NonfatalAssertionException
                ("Attempt to get StorageNodePools in the wrong context.");
        }

        storageNodePools.remove(name);
    }

    /**
     * Returns the pool of StorageNodes identified by @param name.
     */
    public StorageNodePool getStorageNodePool(String name) {
        if (storageNodePools == null) {
            throw new NonfatalAssertionException
                ("Attempt to get StorageNodePools in the wrong context.");
        }

        return storageNodePools.get(name);
    }

    /**
     * Creates a list of rep node parameters for all nodes. For debug use.
     */
    public String printRepNodeParams() {
        StringBuilder sb = new StringBuilder();

        for (Entry<RepNodeId, RepNodeParams> e : repNodeParams.entrySet()) {
            sb.append("\n-- Parameters for ").append(e.getKey());
            sb.append(" --\n");

            for (Parameter p : e.getValue().getMap()) {
                sb.append(p.getName()).append(" = ");
                sb.append(p.asString()).append("\n");
            }
        }
        return sb.toString();
    }

    /**
     * Return a set of all StorageNodeParams in the system.
     */
    public Collection<StorageNodeParams> getStorageNodeParams() {
        return storageNodeParams.values();
    }

    /**
     * Returns the ArbNodeParams object associated with the given Id.
     */
    public ArbNodeParams get(ArbNodeId id) {
        return arbNodeParams.get(id);
    }

    /**
     * Add a new ArbNodeParams created elsewhere. This should be the first
     * instance of a ArbNodeParams.
     */
    public void add(ArbNodeParams anp) {
        update(anp, true);
    }

    /**
     * Update a ArbNodeParams.
     */
    public void update(ArbNodeParams anp) {
        update(anp, false);
    }

    /**
     * Removes the ArbNodeParams object associated with the given ResourceId.
     */
    public ArbNodeParams remove(ArbNodeId id) {
        ArbNodeParams ov = arbNodeParams.remove(id);
        if (ov == null) {
            throw new NonfatalAssertionException
                ("Attempt to remove a nonexistent ArbNodesParams with id " +
                 id);
        }
        return ov;
    }

    /**
     * Return a set of all ArbNodeParams in the system.
     */
    public Collection<ArbNodeParams> getArbNodeParams() {
        return arbNodeParams.values();
    }

    private void update(ArbNodeParams anp, boolean shouldBeFirst) {
        ArbNodeId anid = anp.getArbNodeId();
        ArbNodeParams ov = arbNodeParams.put(anid, anp);
        if ((shouldBeFirst) && (ov != null)) {
            throw new NonfatalAssertionException
                ("Attempt to add a duplicate ArbNodesParams" +
                 " with id " + anid);
        }
    }

    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        if (arbNodeParams == null) {
            arbNodeParams = new HashMap<>();
        }

        /*
         * Check that whether the password checker flag exist in the policy
         * map. If it does not exist, which happens in upgrade situation,
         * initialize the security parameters in policy map.
         */
        if (!policyParams.exists(
                ParameterState.SEC_PASSWORD_COMPLEXITY_CHECK)) {
            policyParams.merge(
                ParameterMap.createDefaultSecurityPolicyMap(), true);
        }
    }
}
