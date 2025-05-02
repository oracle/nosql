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

import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.logging.Logger;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentImpl;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.SecurityConfigCreator;
import oracle.kv.impl.util.SecurityConfigCreator.GenericIOHelper;
import oracle.kv.impl.util.SecurityConfigCreator.ParsedConfig;
import oracle.kv.impl.util.ServiceUtils;

/**
 * See KVLite.
 * This class creates a simple store with an Admin and RepNode.
 */
public class KVLiteAdmin {

    private final String kvstore;
    private final StorageNodeAgent[] snas;
    private final ParameterMap policyMap;
    private final int numPartitions;
    private final int repfactor;
    private final boolean tableOnly;

    /**
     * The first admin user created for secured KVLite.
     */
    private static final String firstUser = "admin";

    public KVLiteAdmin(String kvstore,
                       StorageNodeAgentImpl[] snaImpls,
                       ParameterMap policyMap,
                       int numPartitions,
                       int repfactor,
                       boolean tableOnly) {
        this.kvstore = kvstore;
        this.policyMap = policyMap;
        this.numPartitions = numPartitions;
        this.snas = new StorageNodeAgent[snaImpls.length];
        for (int i = 0; i < snaImpls.length; i++) {
            snas[i] = snaImpls[i].getStorageNodeAgent();
        }
        this.repfactor = repfactor;
        this.tableOnly = tableOnly;
    }

    public void run(Logger logger)
        throws RemoteException, NotBoundException {

        deployStore(logger);
    }

    /**
     * Use the CommandService to configure/deploy a simple store.
     */
    private void deployStore(Logger logger)
        throws RemoteException, NotBoundException {

        String host = snas[0].getHostname();
        int port = snas[0].getRegistryPort();
        final boolean isSecure = snas[0].getSecurityDir() != null;

        LoginManager alm = snas[0].getLoginManager();
        if (isSecure) {
            alm = KVLite.waitForSecurityStartUp(host, port);
        }
        CommandServiceAPI admin = ServiceUtils.waitForAdmin
            (host, port, alm, 5, ServiceStatus.RUNNING, logger);

        admin.configure(kvstore);
        int planId = admin.createDeployDatacenterPlan(
            "Deploy KVLite", "KVLite", repfactor, DatacenterType.PRIMARY,
            false, false);
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        if (policyMap != null) {
            admin.setPolicies(policyMap);
        }

        planId = admin.createDeploySNPlan
            ("Deploy Storage Node", new DatacenterId(1), host, port, null);
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        planId = admin.createDeployAdminPlan
            ("Deploy Admin Service", snas[0].getStorageNodeId());

        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        admin.addStorageNodePool("KVLitePool");
        admin.addStorageNodeToPool("KVLitePool", snas[0].getStorageNodeId());

        for (int i = 1; i < snas.length; i++) {
            planId = admin.createDeploySNPlan
                ("Deploy SN", new DatacenterId(1), snas[i].getHostname(),
                 snas[i].getRegistryPort(), null);
            admin.approvePlan(planId);
            admin.executePlan(planId, false);
            admin.awaitPlan(planId, 0, null);
            admin.assertSuccess(planId);
            admin.addStorageNodeToPool("KVLitePool",
                                       snas[i].getStorageNodeId());
        }

        admin.createTopology("KVLite", "KVLitePool", numPartitions, false);
        planId = admin.createDeployTopologyPlan("Deploy KVStore", "KVLite",
                                                null);
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        if (isSecure) {
            createSecurity(admin);
        }

        /* Wait for the store to be ready for operations */
        new PollCondition(500, 40000) {
            @Override
            protected boolean condition() {
                try {
                    return admin.isStoreReady(tableOnly);
                } catch (RemoteException e) {
                    return false;
                }
            }
        }.await();
    }

    /*
     * 1. Create the first admin user.
     * 2. Grant readwrite access to the created user.
     * 3. Create security login file for the first admin user.
     */
    private void createSecurity(CommandServiceAPI admin)
        throws RemoteException {
        final char[] password = SecurityUtils.generateUserPassword();
        int planId = admin.createCreateUserPlan("Create User", firstUser,
            true, true, password);
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);

        planId = admin.createGrantPlan("Grant User",
            firstUser, Collections.singleton(RoleInstance.READWRITE_NAME));
        admin.approvePlan(planId);
        admin.executePlan(planId, false);
        admin.awaitPlan(planId, 0, null);
        admin.assertSuccess(planId);
        BootstrapParams bp = snas[0].getBootstrapParams();

        final ParsedConfig config = new ParsedConfig();

        GenericIOHelper ioHelper = EmbeddedMode.isEmbedded()?
            new GenericIOHelper(EmbeddedMode.NULL_PRINTSTREAM):
            new GenericIOHelper(System.out);

        SecurityConfigCreator scCreator =
            new SecurityConfigCreator(bp.getRootdir(),
                                      config,
                                      ioHelper);
        try {
            scCreator.createUserLoginFile(firstUser, password,
                new File(bp.getRootdir(), bp.getSecurityDir()));
        } catch (Exception e) {
            throw new RuntimeException("Caught exception when creating " +
                "security configuration", e);
        }
    }
}
