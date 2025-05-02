/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.admin;

import static oracle.kv.impl.param.ParameterState.COMMON_SERVICE_PORTRANGE;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.param.StringParameter;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.impl.util.registry.Protocols;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

public class ClientRepNodeAdminServicesTest extends TestBase {
    private CreateStore createStore = null;

    @Override
    public void setUp() throws Exception {
        assumeTrue("Test requires async", AsyncControl.serverUseAsync);
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (createStore != null) {
            createStore.shutdown(false);
        }
    }

    // tests the getvlsn method of clientRepNodeAdminAPI    
    @Test
    public void testGetVlsn() throws Exception {
        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      1, /* Storage Nodes */
                                      1, /* Replication Factor */
                                      3, /* Partitions */
                                      1  /* capacity */);
        createStore.start();
        Set<RepNodeId> nodes = new HashSet<RepNodeId>();
        for (StorageNodeId snId : createStore.getStorageNodeIds()) {
            nodes.addAll(createStore.getRNs(snId));
        }
        RegistryUtils regUtils = new RegistryUtils(
                createStore.getAdmin().getTopology(),
                createStore.getAdminLoginManager(),
                logger);
        for (RepNodeId rnId : nodes) {
            long internalApiVlsn = regUtils.getRepNodeAdmin(rnId).
                ping().getVlsn();
            long clientRMIApiVlsn = regUtils.getClientRepNodeAdmin(rnId, 
                    Protocols.RMI_ONLY).getVlsn();
            long clientAsyncApiVlsn = regUtils.getClientRepNodeAdmin(rnId, 
                    Protocols.ASYNC_ONLY).getVlsn();

            assertEquals(internalApiVlsn, clientAsyncApiVlsn);
            assertEquals(internalApiVlsn, clientRMIApiVlsn);
        }
    }

    /**
     * Test attempting to use a service registry reference to an old service
     * whose service endpoint could have been reused by a new service.
     * [KVSTORE-2383]
     */
    @Test
    public void testReuseServiceEndpoint() throws Exception {

        /* Create a store with two RNs */
        createStore = new CreateStore(kvstoreName,
                                      5000, /* startPort */
                                      2, /* numStorageNodes */
                                      1, /* replicationFactor */
                                      3, /* numPartitions */
                                      1  /* capacity */);

        /*
         * Specify a service port range so that different services will reuse
         * the same ports if they are free
         */
        createStore.setExtraParams(
            Collections.singleton(
                new StringParameter(COMMON_SERVICE_PORTRANGE, "6000,6008")));
        createStore.start();

        final StorageNodeId[] snIds = createStore.getStorageNodeIds();
        final StorageNodeId sn1id = snIds[0];
        final StorageNodeId sn2id = snIds[1];
        final RepNodeId rn1id = createStore.getRNs(sn1id).get(0);
        final RepNodeId rn2id = createStore.getRNs(sn2id).get(0);

        /* Prevent rn1 from restarting */
        createStore.getStorageNodeAgent(sn1id).getServiceManager(rn1id)
            .dontRestart();

        /* Kill both RNs */
        for (int i = 0; i < 2; i++) {
            try {
                createStore.getRepNodeTestAPI()[i].processHalt();
            } catch (RemoteException e) {
            }
        }

        /* Wait for rn2 to restart */
        createStore.getRepNodeAdmin(rn2id);

        /* Make sure that attempting to call rn1 fails to find the service */
        checkException(
            () -> createStore.getRepNodeAdmin(rn1id, 0).ping(),
            NoSuchObjectException.class,
            "Attempt to access unknown service with dialog type");
    }
}
