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

import java.util.List;

import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * TopologyResolver defines an abstract interface used to resolve resource id
 * values.
 */
public interface TopologyResolver {

    /**
     * A class describing a StorageNode component.
     */
    public class SNInfo {
        private final String hostname;
        private final int registryPort;
        private final StorageNodeId snId;

        public SNInfo(String hostname, int registryPort, StorageNodeId snId) {
            this.hostname = hostname;
            this.registryPort = registryPort;
            this.snId = snId;
        }

        public String getHostname() {
            return hostname;
        }

        public int getRegistryPort() {
            return registryPort;
        }

        public StorageNodeId getStorageNodeId() {
            return snId;
        }
    }

    /**
     * Given a resource id, determine the storage node on which is resides.
     *
     * @param rid a ResourceID, which should generally be one of
     *    StorageNodeId, RepNodeId or AdminId
     * @return the SNInfo corresponding to the resource id.  If the
     *    StorageNode cannot be determined or if the resource id is not an
     *    id that equates to a StorageNode component, return null.
     */
    SNInfo getStorageNode(ResourceId rid);

    /**
     * Return a list of known RepNodeIds in the store. This is used to allow
     * an admin to use RepNode services to validate persistent tokens.
     *
     * @param maxReturn a limit on the number of RepNodes to return
     * @return a list or RepNodeIds if the resolver has access to topology
     *   information, or null otherwise
     */
    List<RepNodeId> listRepNodeIds(int maxReturn);
}
