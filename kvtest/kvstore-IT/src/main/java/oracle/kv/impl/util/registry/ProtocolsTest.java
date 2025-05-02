/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.registry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import oracle.kv.KVStoreConfig;
import oracle.kv.TestBase;

import org.junit.Test;

public class ProtocolsTest extends TestBase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.clearProperty(KVStoreConfig.USE_ASYNC);
        System.clearProperty(KVStoreConfig.USE_RMI);
    }

    @Test
    public void testGet() {
        assertEquals(Protocols.RMI_ONLY,
                     Protocols.get(
                         new KVStoreConfig("store", "localhost:500")
                         .setUseRmi(false)
                         .setUseAsync(false)));
        assertEquals(Protocols.ASYNC_ONLY,
                     Protocols.get(
                         new KVStoreConfig("store", "localhost:500")
                         .setUseRmi(false)
                         .setUseAsync(true)));
        assertEquals(Protocols.RMI_ONLY,
                     Protocols.get(
                         new KVStoreConfig("store", "localhost:500")
                         .setUseRmi(true)
                         .setUseAsync(false)));
        assertEquals(Protocols.RMI_AND_ASYNC,
                     Protocols.get(
                         new KVStoreConfig("store", "localhost:500")
                         .setUseRmi(true)
                         .setUseAsync(true)));
    }

    @Test
    public void testDefault() {
        /* Check the current default */
        assertEquals(Protocols.RMI_AND_ASYNC, Protocols.getDefault());

        /* Check changing the default */
        System.setProperty(KVStoreConfig.USE_RMI, "false");
        System.setProperty(KVStoreConfig.USE_ASYNC, "false");
        assertEquals(Protocols.RMI_ONLY, Protocols.getDefault());
        System.setProperty(KVStoreConfig.USE_RMI, "false");
        System.setProperty(KVStoreConfig.USE_ASYNC, "true");
        assertEquals(Protocols.ASYNC_ONLY, Protocols.getDefault());
        System.setProperty(KVStoreConfig.USE_RMI, "true");
        System.setProperty(KVStoreConfig.USE_ASYNC, "false");
        assertEquals(Protocols.RMI_ONLY, Protocols.getDefault());
        System.setProperty(KVStoreConfig.USE_RMI, "true");
        System.setProperty(KVStoreConfig.USE_ASYNC, "true");
        assertEquals(Protocols.RMI_AND_ASYNC, Protocols.getDefault());
    }

    @Test
    public void testAccessors() {
        assertTrue(Protocols.RMI_ONLY.useRmi());
        assertFalse(Protocols.RMI_ONLY.useAsync());

        assertFalse(Protocols.ASYNC_ONLY.useRmi());
        assertTrue(Protocols.ASYNC_ONLY.useAsync());

        assertTrue(Protocols.RMI_AND_ASYNC.useRmi());
        assertTrue(Protocols.RMI_AND_ASYNC.useAsync());
    }
}
