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


import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import oracle.kv.KVStoreException;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Locates a list of storage nodes hosting admins given a list of SN registry
 * locations
 */
public class AdminLocator {

    private static final int MAX_EXAMINE_RN_COUNT = 10;
    private static final String HOST_PORT_SEPARATOR = ":";

    /**
     * Obtains a list of storage nodes hosting admins from the SNs identified
     * via <code>registryHostPorts</code>.
     * <p>
     * The location of these storage nodes is done as a two step process.
     * <ol>
     * <li>It first identifies the current Topology using TopologyLocator.</li>
     * <li>
     * Then it check whether SNs in the current Topology is bound the service
     * commandService.
     * </li>
     * </ol>
     *
     * @param registryHostPorts one or more strings containing the registry
     * host and port associated with the SN. Each string has the format:
     * hostname:port.
     *
     * @param loginMgr a login manager that will be used to supply login tokens
     * to api calls.
     *
     * @param expectedStoreName the name of the target kvstore.
     *
     * @param logger for debug logging
     *
     * @return the storage nodes and CommandserviceAPIs that are found.
     * @throws KVStoreException
     */
    public static
        Map<StorageNode, CommandServiceAPI> get(String[] registryHostPorts,
                                                LoginManager loginMgr,
                                                String expectedStoreName,
                                                Logger logger)
        throws KVStoreException {

        /* Get topology using the registry host/port */
        Topology topo = TopologyLocator.get(registryHostPorts,
                                            MAX_EXAMINE_RN_COUNT,
                                            loginMgr,
                                            expectedStoreName,
                                            null /* clientId */);

        RegistryUtils regUtils = new RegistryUtils(topo, loginMgr, logger);
        Map<StorageNode, CommandServiceAPI> adminMap = new HashMap<>();

        /*
         * Check whether all storage nodes in topology are bound commandService
         * or not. If a storage node is bound the service, it is determined to
         * host an admin.
         */
        for (StorageNode sn : topo.getSortedStorageNodes()) {
            try {
                final List<String> bound =
                    regUtils.getServiceNames(sn.getStorageNodeId());
                for (String boundName : bound) {
                    if (boundName.equals(GlobalParams.COMMAND_SERVICE_NAME)) {
                        CommandServiceAPI cs =
                                regUtils.getAdmin(sn.getStorageNodeId());
                        adminMap.put(sn, cs);
                    }
                }
            } catch (RemoteException | NotBoundException e) {
                /*
                 * Cannot connect the storage node, so there is no active admin
                 * on this SNA
                 */
            }
        }

        return adminMap;
    }

    /**
     * A convenience method to locate a list of storage nodes hosting admins,
     * specifying a single host/port
     * @param hostname
     * @param port
     * @return the storage nodes and CommandserviceAPIs that are found.
     * @throws KVStoreException
     */
    public static Map<StorageNode, CommandServiceAPI> get(String hostname,
                                                          int port,
                                                          Logger logger)
        throws KVStoreException {
        String[] hostports = new String[1];
        hostports[0] = hostname + HOST_PORT_SEPARATOR + port;
        return get(hostports, null, null, logger);
    }
}
