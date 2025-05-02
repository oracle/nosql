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
package oracle.kv.impl.security.login;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

/**
 * TopoTopoResolver provides an implementation of TopologyResolver that
 * resolves based on a Topology object.
 */
public class TopoTopoResolver implements TopologyResolver {

    private final TopoHandle topoHandle;
    private final SNInfo localSNInfo;
    private final Logger logger;

    /**
     * Creates a TopologyResolver.
     * @param topoHandle a handle to access a topology object
     * @param localSNInfo our local SN Info - may be null if we are still
     * in bootstrap mode
     */
    public TopoTopoResolver(TopoHandle topoHandle,
                            SNInfo localSNInfo,
                            Logger logger) {
        this.topoHandle = topoHandle;
        this.localSNInfo = localSNInfo;
        this.logger = logger;
    }

    public interface TopoHandle {
        Topology getTopology();
    }

    public static class TopoTopoHandle implements TopoHandle {

        private volatile Topology topo;

        public TopoTopoHandle(Topology initialTopo) {
            topo = initialTopo;
        }

        @Override
        public Topology getTopology() {
            return topo;
        }

        public void setTopology(Topology newTopo) {
            this.topo = newTopo;
        }
    }

    public static class TopoMgrTopoHandle implements TopoHandle {

        private volatile TopologyManager topoMgr;

        public TopoMgrTopoHandle(TopologyManager topoMgr) {
            this.topoMgr = topoMgr;
        }

        @Override
        public Topology getTopology() {
            return topoMgr.getTopology();
        }

        public void setTopoMgr(TopologyManager newTopoMgr) {
            this.topoMgr = newTopoMgr;
        }
    }

    /**
     * Resolve a ResourceID to its SNInfo.
     */
    @Override
    public SNInfo getStorageNode(ResourceId target) {

        if (localSNInfo != null &&
            target instanceof StorageNodeId &&
            target.equals(localSNInfo.getStorageNodeId())) {

            logger.fine("TopoTopoResolver resolved target from localSNInfo");

            return localSNInfo;
        }

        final Topology topo = topoHandle.getTopology();
        if (topo == null) {
            logger.info("TopoTopoResolver unable to resolve target: " +
                        target + " without topology");
            return null;
        }

        final Topology.Component<?> comp = topo.get(target);
        if (comp == null) {
            return null;
        }

        if (comp instanceof RepNode) {
            final RepNode rn = (RepNode) comp;
            final StorageNode sn = topo.get(rn.getStorageNodeId());

            if (sn == null) {
                /* only an invalid topology could break this */
                throw new IllegalStateException("corrupted Topology");
            }

            return new SNInfo(sn.getHostname(), sn.getRegistryPort(),
                              sn.getStorageNodeId());
        } else if (comp instanceof StorageNode) {
            final StorageNode sn = (StorageNode) comp;
            return new SNInfo(sn.getHostname(), sn.getRegistryPort(),
                              sn.getStorageNodeId());
        } else {
            return null;
        }
    }

    @Override
    public List<RepNodeId> listRepNodeIds(int maxReturn) {
        final Topology topo = topoHandle.getTopology();
        if (topo == null) {
            return null;
        }

        final List<RepNodeId> rnList = new ArrayList<RepNodeId>();
        for (RepNodeId rnId : topo.getRepNodeIds()) {
            if (rnList.size() >= maxReturn) {
                break;
            }
            rnList.add(rnId);
        }

        return rnList;
    }
}
