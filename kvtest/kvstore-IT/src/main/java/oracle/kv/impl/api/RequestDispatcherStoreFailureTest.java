/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.Value;
import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.exception.PersistentDialogException;
import oracle.kv.impl.async.registry.ServiceRegistryAPI;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

public class RequestDispatcherStoreFailureTest extends TestBase {

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        AsyncRegistryUtils.setTestRegistryFactory(null);
    }

    /**
     * Test that the async request dispatcher selects healthy RNs after the SN
     * hosting one RN stops responding to network messages. [KVSTORE-1423]
     */
    @Test
    public void testSNNetworkHang() throws Exception {
        assumeTrue("Test requires async", AsyncControl.serverUseAsync);

        /* Create and start store */
        final CreateStore createStore =
            new CreateStore(kvstoreName,
                            5000, /* startPort */
                            2, /* numStorageNodes */
                            2, /* replicationFactor */
                            10, /* numPartitions */
                            1, /* capacity */
                            CreateStore.MB_PER_SN, /* memoryMB */
                            true, /* useThreads */
                            null, /* mgmtImpl */
                            false, /* mgmtPortsShared */
                            false); /* secure */
        createStore.start();

        /* Get port number for SN 2 */
        final PortFinder pf =
            createStore.getPortFinder(new StorageNodeId(2));
        final int registryPort = pf.getRegistryPort();

        /*
         * Install a ServiceRegistryAPI Factory that will wait 3 seconds and
         * then throw a DialogBackoffException for the registry on SN 2.
         */
        final DelayServiceRegistryFactory factory =
            new DelayServiceRegistryFactory(registryPort);
        AsyncRegistryUtils.setTestRegistryFactory(factory);

        /* Get store */
        final KVStoreConfig kvConfig =
            createStore.createKVConfig(false /* secure */)
            .setConsistency(Consistency.NONE_REQUIRED)
            .setRequestTimeout(5, TimeUnit.SECONDS);
        final KVStore kvstore = KVStoreFactory.getStore(kvConfig);

        /* Insert initial values */
        final int n = 10;
        for (int i = 0; i < n; ++i) {
            kvstore.put(Key.createKey("key-" + i),
                        Value.createValue(("value-" + i).getBytes()));
        }

        /* Do initial gets to make sure that bad SN is detected */
        for (int i = 0; i < n; ++i) {
            kvstore.get(Key.createKey("key-" + i));
        }
        assertTrue("Bad SN should have been called",
                   factory.delayCount.get() >= 1);


        /* Now do gets and make sure they are quick */
        for (int i = 0; i < n; ++i) {
            final long start = System.currentTimeMillis();
            kvstore.get(Key.createKey("key-" + i));
            final long time = System.currentTimeMillis() - start;
            assertTrue("Request took too long: " + time + " ms", time <= 1000);

            /* Wait so that handler gets a chance to try repair */
            Thread.sleep(2000);
        }
    }

    private static class DelayServiceRegistryFactory
            extends ServiceRegistryAPI.Factory {
        private final int registryPort;
        private final ServiceRegistryAPI.Factory stdFactory;
        final AtomicInteger delayCount = new AtomicInteger();

        DelayServiceRegistryFactory(int registryPort) {
            this.registryPort = registryPort;
            stdFactory = AsyncRegistryUtils.getStandardRegistryFactory();
        }
        @Override
        public CompletableFuture<ServiceRegistryAPI>
            wrap(CreatorEndpoint endpoint,
                 ClientId clientId,
                 long timeoutMs,
                 long defaultTimeoutMs,
                 final Logger logger) {
            final NetworkAddress remoteAddress = endpoint.getRemoteAddress();
            if (registryPort != remoteAddress.getPort()) {
                return stdFactory.wrap(endpoint, clientId, timeoutMs,
                                       defaultTimeoutMs, logger);
            }
            return CompletableFuture.supplyAsync(this::delay);
        }

        private ServiceRegistryAPI delay() {
            delayCount.incrementAndGet();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                throw new RuntimeException("Unexpected interrupt");
            }
            throw new PersistentDialogException(
                false, /* hasSideEffect */
                false, /* fromRemote */
                "Simulate network failure after delay",
                null /* cause */);
        }
    }
}

