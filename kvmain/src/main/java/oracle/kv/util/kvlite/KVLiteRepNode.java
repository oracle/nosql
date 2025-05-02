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

package oracle.kv.util.kvlite;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

import com.sleepycat.je.rep.NodeType;

/**
 * See KVLite.
 * This class creates a standalone store with only a RepNode, no admin.
 */
public class KVLiteRepNode {

    private final StorageNodeAgentAPI sna;
    private final String kvstore;
    private Topology topo;
    private final BootstrapParams bp;

    public KVLiteRepNode(String kvstore,
                         StorageNodeAgentAPI sna,
                         BootstrapParams bp,
                         int numPartitions) {
        this.bp = bp;
        this.sna = sna;
        this.kvstore = kvstore;
        createTopology(numPartitions);
    }

    public void run()
        throws RemoteException {

        registerSNA();
        createRepNode();
    }

    private void registerSNA()
        throws RemoteException {

        StorageNodeParams snp = new StorageNodeParams
            (new StorageNodeId(1), bp.getHostname(),
             bp.getRegistryPort(), "");
        GlobalParams gp = new GlobalParams(kvstore);

        /**
         * This will kill any bootstrap admin that was started.
         */
        sna.register(gp.getMap(), snp.getMap(), false);
    }

    private void createTopology(int numPartitions) {

        topo = new Topology(kvstore);

        /**
         * Use kvstore name as Datacenter. It will have a repFactor of 1.
         */
        Datacenter dc =
            Datacenter.newInstance(kvstore, 1, DatacenterType.PRIMARY, false,
                                   false);
        topo.add(dc);

        /**
         * Now a StorageNode.
         */
        StorageNode sn =
            new StorageNode(dc, bp.getHostname(), bp.getRegistryPort());
        topo.add(sn);

        /**
         * Now RepGroup, RepNode.  NOTE: need to add RG to Topo before adding
         * RNs to RG.
         */
        RepGroup rg = new RepGroup();
        topo.add(rg);
        RepNode rn = new RepNode(sn.getResourceId());
        rg.add(rn);

        /**
         * Partition for the RepGroup.
         */
        for (int i = 0; i < numPartitions; i++) {
            topo.add(new Partition(rg));
        }

        rn = topo.get(new RepNodeId(1, 1));
    }

    private RepNodeParams createRepNodeParams() {

        String host = bp.getHostname();
        int nodeHAPort = bp.getRegistryPort() + 1;
        String directoryPath = null;
        List<String> paths = bp.getStorageDirPaths();
        if (paths != null && paths.size() > 0) {
            directoryPath = paths.get(0);
        }

        RepNodeParams rnp = new RepNodeParams
            (new StorageNodeId(1), new RepNodeId(1, 1),
             false, /* disabled */
             host, nodeHAPort,
             host, nodeHAPort /* helper*/,
             directoryPath, NodeType.ELECTABLE);
        return rnp;
    }

    private void createRepNode()
        throws RemoteException {

        RepNodeParams rnp = createRepNodeParams();
        final Set<Metadata<? extends MetadataInfo>> metadataSet =
                new HashSet<Metadata<? extends MetadataInfo>>(1);
        metadataSet.add(topo);
        sna.createRepNode(rnp.getMap(), metadataSet);
    }
}
