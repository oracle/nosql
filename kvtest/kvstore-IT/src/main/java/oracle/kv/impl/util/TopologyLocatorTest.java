/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import oracle.kv.KVStoreException;
import oracle.kv.TestBase;

import org.junit.Test;

public class TopologyLocatorTest extends TestBase {

    private static final String VALIDATE_TOPOLOGY_INVALID_MSG =
            "Could not establish an initial Topology from:";

    @Test
    public void testGetFromDeadRN() throws Exception {
        KVRepTestConfig config = new KVRepTestConfig(this, 1, 1, 1, 1);

        try {
            config.startRepNodeServices();
            TopologyLocator.getTopologyHook = (a) -> {
                // Change all RepNodeServices' status to non-RUNNING for
                // mocking failed RN. The hook only triggers after
                // TopologyLocator gets a valid admin handle
                config.getRepNodeServices().forEach(
                        service -> service.getStatusTracker().update(
                                ConfigurableService.ServiceStatus.STARTING));
            };

            String[] registryHostPorts = { "localhost:5001" };
            TopologyLocator.get(registryHostPorts,
                                1,
                                null,
                                null,
                                null);
            fail("expected KVStoreException for RN");
        } catch (KVStoreException iae) {
            assertTrue(iae.getMessage(),
                       iae.getMessage()
                          .contains(VALIDATE_TOPOLOGY_INVALID_MSG));
        } finally {
            TopologyLocator.getTopologyHook = null;
            config.stopRepNodeServices();
        }
    }
}
