/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.util.registry;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.StandardDialogTypeFamily.MONITOR_AGENT_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.REP_NODE_ADMIN_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.REQUEST_HANDLER_TYPE_FAMILY;
import static oracle.kv.impl.util.TestUtils.DEFAULT_CSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_SSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_THREAD_POOL;
import static oracle.kv.impl.util.TestUtils.safeUnexport;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;

import oracle.kv.TestBase;
import oracle.kv.impl.api.AsyncRequestHandler;
import oracle.kv.impl.api.AsyncRequestHandlerResponder;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.RequestHandler;
import oracle.kv.impl.api.RequestHandlerAPI;
import oracle.kv.impl.api.Response;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.monitor.MonitorAgent;
import oracle.kv.impl.monitor.MonitorAgentResponder;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.admin.RepNodeAdminResponder;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.RepNodeAdminNOP;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Tests the various registry utility method.
 */
public class RegistryUtilsTest extends TestBase {

    private static final LoginManager NULL_LOGIN_MGR = null;

    private KVRepTestConfig config;

    @Override
    public void setUp() throws Exception {

        super.setUp();
        config = new KVRepTestConfig(this, 1, 2, 3, 10);
        config.startupSNs();
    }

    @Override
    public void tearDown() throws Exception {

        config.stopSNs();
        LoggerUtils.closeAllHandlers();
        super.tearDown();
    }

    /* Tests */

    @Test
    public void testTopologyInterfaces()
        throws RemoteException, NotBoundException {

        Topology topo = config.getTopology();

        final RegistryUtils regUtils =
            new RegistryUtils(topo, NULL_LOGIN_MGR, logger);
        RepNodeId r1n1Id = new RepNodeId(1,1);
        StorageNodeId snId = new StorageNodeId(1);
        final RepNodeAdmin rnAdmin = tearDownUnexport(new RepNodeAdminNOP());
        tearDownListenHandle(
            regUtils.rebind(r1n1Id, rnAdmin, DEFAULT_CSF, DEFAULT_SSF,
                            () -> new RepNodeAdminResponder(
                                rnAdmin, DEFAULT_THREAD_POOL, logger)));
        final DummyRequestHandler rh =
            tearDownUnexport(new DummyRequestHandler());
        tearDownListenHandle(
            regUtils.rebind(r1n1Id, rh, DEFAULT_CSF, DEFAULT_SSF,
                            () -> new AsyncRequestHandlerResponder(rh,
                                                                   logger)));

        assertNotNull(regUtils.getRepNodeAdmin(r1n1Id));
        assertNotNull(regUtils.getRequestHandler(r1n1Id));

        /* Check bogus input */
        final RepNodeId badId = new RepNodeId(99, 42);
        assertNull(regUtils.getRequestHandler(badId));

        final StorageNode storageNode = topo.get(snId);

        /* Test that static methods are equivalent. */
        assertEquals(
            regUtils.getRepNodeAdmin(r1n1Id),
            RegistryUtils.getRepNodeAdmin(kvstoreName,
                                          storageNode.getHostname(),
                                          storageNode.getRegistryPort(),
                                          r1n1Id, null /* loginManager */,
                                          logger));
    }

    /* Exercises the static interfaces */
    @Test
    public void testStaticInterfaces()
        throws RemoteException, NotBoundException {

        Topology topo = config.getTopology();

        RepNodeId r1n1Id = new RepNodeId(1,1);
        StorageNodeId snId = new StorageNodeId(1);
        final StorageNode storageNode = topo.get(snId);
        final DummyMonitorAgent agent =
            tearDownUnexport(new DummyMonitorAgent());
        final ListenHandle agentListenHandle =
            tearDownListenHandle(
                RegistryUtils.rebind(storageNode.getHostname(),
                                     storageNode.getRegistryPort(),
                                     kvstoreName,
                                     r1n1Id,
                                     InterfaceType.MONITOR,
                                     agent,
                                     DEFAULT_CSF, DEFAULT_SSF,
                                     MONITOR_AGENT_TYPE_FAMILY,
                                     () -> new MonitorAgentResponder(
                                         agent, DEFAULT_THREAD_POOL, logger),
                                     logger));
        if (agentListenHandle != null) {
            agentListenHandle.shutdown(true);
        }
        final RepNodeAdmin admin = tearDownUnexport(new RepNodeAdminNOP());
        tearDownListenHandle(
            RegistryUtils.rebind(storageNode.getHostname(),
                                 storageNode.getRegistryPort(),
                                 kvstoreName,
                                 r1n1Id,
                                 InterfaceType.ADMIN,
                                 admin,
                                 DEFAULT_CSF, DEFAULT_SSF,
                                 REP_NODE_ADMIN_TYPE_FAMILY,
                                 () -> new RepNodeAdminResponder(
                                     admin, DEFAULT_THREAD_POOL, logger),
                                 logger));
        final DummyRequestHandler rh =
            tearDownUnexport(new DummyRequestHandler());
        tearDownListenHandle(
            RegistryUtils.rebind(storageNode.getHostname(),
                                 storageNode.getRegistryPort(),
                                 kvstoreName,
                                 r1n1Id,
                                 InterfaceType.MAIN,
                                 rh,
                                 DEFAULT_CSF, DEFAULT_SSF,
                                 REQUEST_HANDLER_TYPE_FAMILY,
                                 () -> new AsyncRequestHandlerResponder(
                                     rh, logger),
                                 logger));

        /* Test that static methods are equivalent. */
        final RegistryUtils regUtils =
            new RegistryUtils(topo, NULL_LOGIN_MGR, logger);
        assertEquals(
            regUtils.getRepNodeAdmin(r1n1Id),
            RegistryUtils.getRepNodeAdmin(kvstoreName,
                                          storageNode.getHostname(),
                                          storageNode.getRegistryPort(),
                                          r1n1Id,
                                          null /* loginManager */,
                                          logger));

        final DummyMonitorAgent agent2 =
            tearDownUnexport(new DummyMonitorAgent());
        tearDownListenHandle(
            regUtils.rebind(r1n1Id, agent2, DEFAULT_CSF, DEFAULT_SSF,
                            () -> new MonitorAgentResponder(
                                agent2, DEFAULT_THREAD_POOL, logger)));
        assertNotNull(RegistryUtils.getMonitor(topo.getKVStoreName(),
                                               storageNode.getHostname(),
                                               storageNode.getRegistryPort(),
                                               r1n1Id,
                                               null /* loginManager */,
                                               logger));

    }

    private class DummyRequestHandler extends VersionedRemoteImpl
            implements RequestHandler, AsyncRequestHandler {

        @Override
        public Response execute(Request request) {
            throw new UnsupportedOperationException("Method not implemented: " +
                                                    "execute");
        }

        @Override
        public CompletableFuture<Response> execute(Request request,
                                                   long timeoutMillis) {
            return failedFuture(
                new UnsupportedOperationException(
                    "Method not implemented: execute"));
        }

        @Override
        public CompletableFuture<Short> getSerialVersion(short serialVersion,
                                                         long timeoutMillis) {
            return failedFuture(
                new UnsupportedOperationException(
                    "Method not implemented: getSerialVersion"));
        }
    }

    private class DummyMonitorAgent
        extends VersionedRemoteImpl implements MonitorAgent {

        @Override
        public List<Measurement> getMeasurements(AuthContext authCtx,
                                                 short serialVersion) {
            throw new UnsupportedOperationException("Method not implemented: " +
            "getMeasurements");
        }
    }

    /**
     * Test that specifying a different client ID works gets the right client
     * socket factory. [#27952]
     */
    @Test
    public void testRegistryClientId() {

        final ClientId clientId1 = new ClientId(1);
        final ClientSocketFactory csf1 =
            new ClientSocketFactory("csf1", 3, 4, null /* clientId */);
        RegistryUtils.setRegistryCSF(csf1, "kvstore", clientId1);

        final ClientId clientId2 = new ClientId(2);
        final ClientSocketFactory csf2 =
            new ClientSocketFactory("csf2", 5, 6, null /* clientId */);
        RegistryUtils.setRegistryCSF(csf2, "kvstore", clientId2);

        assertSame(csf1, RegistryUtils.getRegistryCSF(null, clientId1));
        assertSame(csf2, RegistryUtils.getRegistryCSF(null, clientId2));
    }

    /**
     * Test that concurrent calls to get a request handler all timeout
     * appropriately. Tests caching behavior introduced for [KVSTORE-493].
     */
    @Test
    public void testGetRequestHandlerTimeout()
        throws Exception {

        /* Make registry timeouts short */
        final int registryTimeoutMs = 1000;
        final ClientSocketFactory csf = new ClientSocketFactory(
            "csf", registryTimeoutMs, registryTimeoutMs, null /* clientId */);
        RegistryUtils.setRegistryCSF(csf, getKVStoreName(),
                                     null /* clientId */);

        /* Bind request handler */
        final Topology topo = config.getTopology();
        final RegistryUtils regUtils =
            new RegistryUtils(topo, NULL_LOGIN_MGR, logger);
        final RepNodeId r1n1Id = new RepNodeId(1,1);
        final DummyRequestHandler rh =
            tearDownUnexport(new DummyRequestHandler());
        tearDownListenHandle(
            regUtils.rebind(r1n1Id, rh, DEFAULT_CSF, DEFAULT_SSF,
                            () -> new AsyncRequestHandlerResponder(
                                rh, logger)));

        /* Make lookup of request handler block */
        final Semaphore blocked = new Semaphore(0);
        final CountDownLatch wait = new CountDownLatch(1);
        final CompletableFuture<?> doneWaiting = new CompletableFuture<>();
        tearDowns.add(() -> { RegistryUtils.registryLookupHook = null; });
        RegistryUtils.registryLookupHook = name -> {
            try {
                blocked.release();
                if (!wait.await(30, SECONDS)) {
                    throw new RuntimeException("Gave up waiting");
                }
                doneWaiting.complete(null);
            } catch (Throwable t) {
                doneWaiting.completeExceptionally(t);
            }
        };

        /* Make first lookup request, which will block */
        final CompletableFuture<RequestHandlerAPI> request1 =
            CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return regUtils.getRequestHandler(r1n1Id);
                    } catch (RemoteException|NotBoundException e) {
                        throw new CompletionException(e);
                    }
                });
        assertTrue(blocked.tryAcquire(30, SECONDS));

        /*
         * Make second lookup request, which should timeout with an appropriate
         * exception
         */
        final CompletableFuture<RequestHandlerAPI> request2 =
            CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return regUtils.getRequestHandler(r1n1Id);
                    } catch (RemoteException|NotBoundException e) {
                        throw new CompletionException(e);
                    }
                });
        try {
            request2.get(2*registryTimeoutMs, MILLISECONDS);
            fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RemoteException);
            assertTrue(e.getCause().getCause() instanceof TimeoutException);
        }

        /* Allow the blocked operation to proceed */
        wait.countDown();
        doneWaiting.get(30, SECONDS);

        /*
         * Check that first lookup request either times out, or succeeds
         * because RMI doesn't really timeout dependably
         */
        try {
            assertNotNull(request1.get(30, SECONDS));
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RemoteException);
            /*
             * Not really sure what the cause of the RemoteException would be
             * in this case
             */
        }
    }

    /* Other methods */

    private <T extends Remote> T tearDownUnexport(T object) {
        tearDowns.add(() -> safeUnexport(object));
        return object;
    }

    private ListenHandle tearDownListenHandle(ListenHandle listenHandle) {
        if (listenHandle != null) {
            tearDowns.add(() -> listenHandle.shutdown(true));
        }
        return listenHandle;
    }
}
