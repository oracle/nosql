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

package oracle.kv.impl.admin.plan;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.task.DeploySN;
import oracle.kv.impl.admin.plan.task.Utils;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.util.ErrorMessage;

import oracle.nosql.common.json.ObjectNode;

public class DeploySNPlan extends TopologyPlan {

    private static final long serialVersionUID = 1L;

    /* The original inputs. */
    private StorageNodeParams inputSNP;

    private StorageNodeParams newSNParams;
    private final StorageNodeId snId;

    public DeploySNPlan(String planName,
                        Planner planner,
                        Topology topology,
                        DatacenterId datacenterId,
                        StorageNodeParams inputSNP) {
        super(planName, planner, topology);
        this.inputSNP = inputSNP;

        /* Error Checking */

        Datacenter dc = topology.get(datacenterId);
        if (dc == null) {
            throw new IllegalCommandException
                (datacenterId +
                 " is not a valid Zone id.  " +
                 "Please provide the id of an existing Zone.");
        }

        /* Create an updated topology and DataCenterParams */
        StorageNode sn = new StorageNode(dc,
                                         inputSNP.getHostname(),
                                         inputSNP.getRegistryPort());
        /**
         * Copy only those parameters that are relevant to the Storage Node
         * being deployed.
         */
        newSNParams = new StorageNodeParams(inputSNP.getFilteredMap());
        setSearchParamsMaybe(newSNParams, getAdmin());

        StorageNode alreadyExists = alreadyInTopology(sn);
        StorageNode useStorageNode = null;
        boolean isFirst;
        if (alreadyExists != null) {
            useStorageNode = alreadyExists;
            guardAgainstDifferentParams(alreadyExists.getResourceId());

            /*
             * If this is the first SN in the topology, it will host the
             * admin.
             */
            isFirst = (getTopology().getStorageNodeMap().size() == 1);
        } else {

            /*
             * This SN does not exist in the topology. Do appropriate
             * checks. For example, we allow only a single StorageNode to be
             * deployed before an Admin is deployed.
             */

            isFirst = topology.getStorageNodeMap().isEmpty();
            if (planner.getAdmin().getAdminCount() == 0 && !isFirst) {
                throw new IllegalCommandException
                    ("An Admin service instance must be deployed on the " +
                     "first deployed StorageNode before any further " +
                     "StorageNode deployments can take place.",
                     ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
            }
            useStorageNode  = topology.add(sn);
        }

        snId = useStorageNode.getStorageNodeId();
        newSNParams.setStorageNodeId(snId);

        /* Create Tasks */
        addTask(new DeploySN(this, isFirst));

        /*
         * Note that we will save the topology and params after the task
         * executes and successfully creates and registers the SN. Most other
         * plans save topology and params before execution, to make sure that
         * the topology is consistent and saved in the admin db before any
         * kvstore component can access it. DeploySNPlan is a special case
         * where it is safe to store the topology after execution because only
         * one task is executed in the plan (and therefore no issues about
         * atomicity of tasks). Also, there can be no earlier reference to the
         * SN before the plan finishes.
         */
    }

    /**
     * Check for the presence of search cluster registration parameters
     * in any existing SN.  If they are present, copy them into the
     * new SN's parameters.
     *
     * This method is not called if it's the first SN in the topology.
     */
    private void setSearchParamsMaybe
        (StorageNodeParams snp, Admin admin) {

        final Parameters p = admin.getCurrentParameters();
        ParameterMap pm = Utils.verifyAndGetSearchParams(p);

        /* If the search parameters are not set, don't bother merging them. */
        if ("".equals
            (pm.getOrDefault
             (ParameterState.SN_SEARCH_CLUSTER_MEMBERS).asString())) {
            return;
        }

        snp.getMap().merge(pm, false);
    }

    /**
     * Additional parameters are added to the storage node's param set
     * after the bootstrap/registration handshake is done. This method
     * returns the parameters instance that should be augmented.
     */
    public StorageNodeParams getRegistrationParams() {
        return newSNParams;
    }

    @Override
    public String getDefaultName() {
        return "Deploy Storage Node";
    }

    public StorageNodeId getStorageNodeId() {
        return snId;
    }

    /**
     * @return the hostname
     */
    public StorageNodeParams getInputStorageNodeParams() {
        return inputSNP;
    }

    /**
     * @return a StorageNode component object if this SN already exists in the
     * topology.
     */
    private StorageNode alreadyInTopology(StorageNode newStorageNode) {
        for (StorageNode s : getTopology().getStorageNodeMap().getAll()) {
            if (newStorageNode.propertiesEquals(s)) {
                return s;
            }
        }
        return null;
    }

    /**
     * @throw IllegalCommandException if params for this SN already exist, and
     * are different from the new ones proposed.
     */
    private void guardAgainstDifferentParams(StorageNodeId existingId) {

        StorageNodeParams existingParams =
            getAdmin().getStorageNodeParams(existingId);

        if (existingParams == null) {
            return;
        }

        /*
         * When comparing params, exclude those that are set later in the
         * process, such as the storage node id, which is generated when the
         * node is added to the topology, and after SNA registration.
         */
        ParameterMap existingParamsMapCopy =
            existingParams.getFilteredMap().copy();
        existingParamsMapCopy.remove(ParameterState.COMMON_SN_ID);
        existingParamsMapCopy.remove(ParameterState.SN_COMMENT);
        existingParamsMapCopy.remove(ParameterState.ADMIN_WEB_SERVICE_PORT);
        for (String paramName : StorageNodeParams.REGISTRATION_PARAMS) {
            existingParamsMapCopy.remove(paramName);
        }

        ParameterMap newParamsMapCopy = newSNParams.getMap();
        newParamsMapCopy.remove(ParameterState.COMMON_SN_ID);
        newParamsMapCopy.remove(ParameterState.SN_COMMENT);

        if (!existingParamsMapCopy.equals(newSNParams.getMap())) {
            throw new IllegalCommandException
                ("A storage node on " + existingParams.getHostname() +
                 ":" + existingParams.getRegistryPort() + " already " +
                 "exists, but has different parameters. The " +
                 "storage node can't be deployed again unless the " +
                 "parameters are identical. Existing parameters:\n" +
                 existingParamsMapCopy.showContents() + "New parameters:\n" +
                 newParamsMapCopy.showContents());
        }
    }

    private DatacenterId getDatacenterId() {
        return getTopology().getDatacenterId(snId);
    }

    @Override
    public void stripForDisplay() {
        super.stripForDisplay();
        inputSNP = null;
        newSNParams = null;
    }

    /*
     * TODO: These constants can probably be replaced with constants from
     * CommandParser.
     */
    @Override
    public String getOperation() {
        return "plan deploy-sn -zn " + getDatacenterId().getDatacenterId() +
               " -host " + inputSNP.getHostname() +
               " -port " + inputSNP.getRegistryPort();
    }

    /*
     * TODO: replace field names with constants held in a json/command output
     * utility class.
     */
    @Override
    public ObjectNode getPlanJson() {
        ObjectNode jsonTop = JsonUtils.createObjectNode();
        jsonTop.put("plan_id", getId());
        jsonTop.put("resource_id", snId.toString());
        jsonTop.put("zone_id", getDatacenterId().toString());
        jsonTop.put("host", inputSNP.getHostname());
        jsonTop.put("port", inputSNP.getRegistryPort());
        return jsonTop;
    }
}
