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

import oracle.kv.LoginCredentials;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.StorageNodeParams.RNHeapAndCacheSize;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Logger;

/**
 * KVSTORE-2245: Internal API to compute default RN heap size
 * A class to compute default RN heap size. This class will be used by the
 * cloud team to compute JVM tuning parameters which are based on the heap size.
 * This class in turn uses the existing
 * StorageNodeParams.calculateRNHeapAndCache public static method to compute the
 * heap size. This class takes the following command line arguments:
 *
 * <ul>
 * <li>adminhost    admin sn's host-name or ip-address
 * <li>adminport    admin's port number
 * <li>snid         storage node id
 * <li>securityfile optional login settings file for secure store
 * </ul>
 */
public class RNHeapSizeAPI {

    /**
     * The host-name or ip-address of the SN hosting just Admin or both
     * RN and admin.
     */
    private static String adminHost;

    /**
     * The SNA port of the SN hosting just Admin or both RN and admin.
     */
    private static int adminPort;

    /**
     * The storage node id which hosts the RN's whose default RN heap size
     * need to be computed
     */
    private static StorageNodeId snId;

    /**
     * The login settings file which needs to be provided mandatorily for
     * secure store.
     */
    private static String securityFile;

    public static void main(String[] args)
        throws NotBoundException, RemoteException, IOException {

        final RNHeapSizeAPI rnHeapAPI = new RNHeapSizeAPI();
        rnHeapAPI.processArgs(args);
        final RNHeapAndCacheSize heapAndCache =
            rnHeapAPI.calculateRNHeapAndCache();
        System.out.println("cache bytes  : " + heapAndCache.getCacheBytes());
        System.out.println("cache percent: " + heapAndCache.getCachePercent());
        System.out.println("heap MB      : " + heapAndCache.getHeapMB());
    }

    private RNHeapAndCacheSize calculateRNHeapAndCache()
        throws NotBoundException, RemoteException, IOException {

        final Logger logger =
            LoggerUtils.getLogger(RNHeapSizeAPI.class, "RNHeapSizeAPI");
        LoginManager adminLoginMgr = null;

        if (securityFile != null) {
            final KVStoreLogin adminLogin =
                new KVStoreLogin(null, securityFile);
            adminLogin.loadSecurityProperties();
            adminLogin.prepareRegistryCSF();
            final LoginCredentials loginCreds =
                adminLogin.makeShellLoginCredentials();
            adminLoginMgr = KVStoreLogin.getAdminLoginMgr(adminHost,
                                                          adminPort,
                                                          loginCreds,
                                                          logger);
            if (adminLoginMgr == null) {
                throw new IllegalStateException(
                    "Could not login to adminhost:adminport: " +
                    adminHost + ":" + adminPort + " " +
                    "using security file: " +
                    securityFile);
            }
        }

        final CommandServiceAPI cs =
            RegistryUtils.getAdmin(adminHost,
                                   adminPort,
                                   adminLoginMgr,
                                   logger);
        final Parameters params = cs.getParameters();
        final ParameterMap pMap = params.copyPolicies();
        final Topology topo = cs.getTopology();

        final StorageNodeParams snp = params.get(snId);
        if (snp == null) {
            throw new IllegalArgumentException(
                "Invalid storage node id: " + snId.getStorageNodeId());
        }

        final int numRNsOnSN = topo.getHostedRepNodeIds(snId).size();
        final int numANsOnSN = topo.getHostedArbNodeIds(snId).size();
        final Collection<RepNodeParams> rnpSet =
            cs.getParameters().getRepNodeParams();
        final int rnCachePercent =
            rnpSet.iterator().next().getRNCachePercent();

        return snp.calculateRNHeapAndCache(pMap,
                                           numRNsOnSN,
                                           rnCachePercent,
                                           numANsOnSN);
    }

    private void processArgs(String[] args) {
        if (args.length != 6 && args.length != 8) {
            usage();
        }

        for (int i = 0; i < args.length; i++) {
            if ("-adminhost".equals(args[i])) {
                if (i+1 < args.length) {
                    adminHost = args[++i];
                } else {
                    usage();
                }
            } else if ("-adminport".equals(args[i])) {
                if (i+1 < args.length) {
                    adminPort = Integer.parseInt(args[++i]);
                } else {
                    usage();
                }
            } else if ("-snid".equals(args[i])) {
                if (i+1 < args.length) {
                    final int snIdArg =
                        Integer.parseInt(args[++i].replaceAll("[^\\d]", ""));
                    if (snIdArg <= 0) {
                        throw new IllegalArgumentException(
                            "Invalid storage node id: " + snIdArg);
                    }
                    snId = new StorageNodeId(snIdArg);
                } else {
                    usage();
                }
            } else if ("-security".equals(args[i])) {
                if (i+1 < args.length) {
                    securityFile = args[++i];
                    if (!new File(securityFile).exists()) {
                        throw new IllegalArgumentException(
                            "Security file not found: " + securityFile);
                    }
                } else {
                    usage();
                }
            }
        }

        if (adminHost == null || adminPort == 0 || snId == null) {
            usage();
        }
    }

    private static void usage() {
        throw new IllegalArgumentException(
            "Usage: RNHeapSizeAPI -adminhost <admin host/ip> " +
            "-adminport <admin port> -snid <storage node id> " +
            "[-security <sec file>]");
    }
}
