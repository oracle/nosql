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

package oracle.kv.impl.tif;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationNode;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * Object representing all information from the rep node that host the
 * TextIndexFeeder needed to build subscription configuration.
 */
class HostRepNode {

    private final String tifNodeName;
    private final String rootDirPath;
    private final String storeName;
    private final String host;
    private final int port;
    private final StorageNodeId storageNodeId;
    private final RepNodeId repNodeId;

    private final ReplicatedEnvironment repEnv;

    HostRepNode(String tifNodeName, RepNode hostRN) {
        this.tifNodeName = tifNodeName;

        rootDirPath = hostRN.getStorageNodeParams().getRootDirPath();
        storeName = hostRN.getGlobalParams().getKVStoreName();
        storageNodeId = hostRN.getStorageNodeParams().getStorageNodeId();
        repNodeId = hostRN.getRepNodeId();
        repEnv = hostRN.getEnv(60000);
        if (repEnv == null) {
            throw new RNUnavailableException(
                "Environment of host node " + hostRN.getRepNodeId() +
                " is unavailable during initializing");
        }
        final ReplicationNode node =
            repEnv.getGroup().getMember(repEnv.getNodeName());
        host = node.getHostName();
        port = node.getPort();

    }

    public String getTifNodeName() {
        return tifNodeName;
    }

    public String getRootDirPath() {
        return rootDirPath;
    }

    public String getStoreName() {
        return storeName;
    }

    public StorageNodeId getStorageNodeId() {
        return storageNodeId;
    }

    public RepNodeId getRepNodeId() {
        return repNodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public ReplicatedEnvironment getRepEnv() {
        return repEnv;
    }

    @Override
    public String toString() {
        return "TIF: " + tifNodeName +
               "\nkv store: " + storeName +
               "\nhost node:port : " + host + ":" + port +
               "\nsn id: " + storageNodeId;
    }
}
