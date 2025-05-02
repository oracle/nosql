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

package oracle.kv.impl.api.rgstate;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.Response;
import oracle.kv.impl.api.StatusChanges;
import oracle.kv.impl.api.TopologyInfo;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.StateChangeEvent;

/**
 * The RepGroupStateTable tracks the dynamic (non-structural) state associated
 * with the replication groups and the replication nodes underlying the KV
 * Store. It can be indexed just like the RepGroupMap via the group id and the
 * node number. This state information serves as the basis for request
 * dispatching.
 * <p>
 * The state information, due to its dynamic nature, is not stored
 * persistently. [RNs could potentially benefit, from having it stored
 * persistently, across quick restarts.]
 * <p>
 * The HA state maintained in the table is maintained in one of two ways:
 * <ol>
 * <li>Via the HA listener for state that is local to the rep group associated
 * with this RN, if the state table is associated with a RN and not a KV
 * client.</li>
 * <li>
 * From HA status returned in a <code>Response</code>. This update method
 * applies to both RNs and KV clients.</li>
 * </ol>
 * <p>
 * The state table is loosely coupled with the Topology. It's possible that one
 * or the other tables may be temporarily inconsistent with each other.
 * <p>
 * Associated with the RepGroupStateTable is an update thread that uses
 * internal ping requests attempts to keep the state associated with each RN
 * and RG as current as possible.
 */
public class RepGroupStateTable implements TopologyManager.PostUpdateListener{

    /**
     * Identifies the component (RN or KV Client) that's tracking the
     * state of the nodes.
     */
    private final ResourceId trackerId;

    private final boolean async;

    private final Logger logger;

    /**
     * A lazily populated map from rep group to the state associated with the
     * RepGroup.
     */
    private final ConcurrentHashMap<RepGroupId, RepGroupState> groupMap;

    public RepGroupStateTable(ResourceId trackerId,
                              boolean async,
                              Logger logger) {
        this.trackerId = trackerId;
        this.async = async;
        this.logger = checkNull("logger", logger);
        groupMap = new ConcurrentHashMap<RepGroupId, RepGroupState>();
    }

    /**
     * Returns the RepNodeState associated with every RN in the state table.
     */
    public synchronized Collection<RepNodeState> getRepNodeStates() {
        final Collection<RepNodeState> rns = new HashSet<RepNodeState>();
        for (RepGroupState rgs : groupMap.values()) {
            rns.addAll(rgs.getRepNodeStates());
        }
        return rns;
    }

    /**
     * Returns the RepNodeStates whose requestHandler need repair.
     */
    synchronized Collection<RepNodeState> getRNStatesNeedingRepair() {
        final Collection<RepNodeState> rns = new HashSet<RepNodeState>();
        for (RepGroupState rgs : groupMap.values()) {
            for (RepNodeState state : rgs.getRepNodeStates()) {
                if (state.reqHandlerNeedsRepair()) {
                    rns.add(state);
                }
            }
        }
        return rns;
    }

    /**
     * Returns the group state associated with the groupId. If an entry does
     * not exist, an empty entry is created for it.
     *
     * @param rgId identifies the RepGroup
     *
     * @return the GroupEntry identified by the repGroupId
     */
    public RepGroupState getGroupState(RepGroupId rgId) {
        RepGroupState rgs = groupMap.get(rgId);
        if (rgs != null) {
            return rgs;
        }
        return groupMap.computeIfAbsent(
            rgId,
            (id) -> new RepGroupState(id, trackerId, async, logger));
    }

    public RepNodeState getNodeState(RepNodeId rnId) {
        RepGroupState rg = getGroupState(new RepGroupId(rnId.getGroupId()));
        return rg.get(rnId);
    }

    public State getRepState(RepNodeId rnId) {
        RepGroupState rgs = getGroupState(new RepGroupId(rnId.getGroupId()));
        return rgs.get(rnId).getRepState();
    }

    /**
     * Updates the state table in response to a Listener state change event.
     * <p>
     *
     * @param event the change event
     */
    public void update(StateChangeEvent event) {
        /* Only RNs get state change events, KV clients do not. */
        final RepNodeId rnId = (RepNodeId) trackerId;
        RepGroupState rgs = getGroupState(new RepGroupId(rnId.getGroupId()));

        RepNodeId masterId = null;
        if (event.getState().isReplica() || event.getState().isMaster()) {
            String masterName = event.getMasterNodeName();
            masterId = RepNodeId.parse(masterName);
        }
        rgs.update(rnId,
                   masterId,
                   event.getState(),
                   event.getEventTime());
    }

    /**
     * Updates the table with status changes from a response.
     *
     * @param request part of request/response pair
     * @param response part of the request/response pair
     * @param respMs the elapsed time associated with the response
     */
    public void update(Request request,
                       Response response,
                       int respMs) {

        RepNodeId rnId = response.getRespondingRN();
        RepGroupState rgs = getGroupState(new RepGroupId(rnId.getGroupId()));
        RepNodeState rnState = getNodeState(rnId);
        rnState.updateVLSN(response.getVLSN());
        rnState.accumRespTime(request.isWrite(), respMs);

        final TopologyInfo topoInfo = response.getTopoInfo();
        if ((topoInfo != null) && (topoInfo.getChanges() == null)) {
            /* Responder has obsolete topology */
            rnState.updateTopoSeqNum(topoInfo.getSequenceNumber());
        }

        final StatusChanges changes = response.getStatusChanges();
        if (changes == null) {
            return;
        }

        final RepNodeId masterId = changes.getCurrentMaster();
        rgs.update(rnId, masterId, changes.getState(),
                   changes.getStatusTime());
    }

    /**
     * Merge any topology related changes into the state table
     */
    @Override
    public boolean postUpdate(Topology topology) {
        for (RepGroup rg : topology.getRepGroupMap().getAll()) {
            getGroupState(rg.getResourceId()).update(rg, topology);
        }
        return false;
    }
}
