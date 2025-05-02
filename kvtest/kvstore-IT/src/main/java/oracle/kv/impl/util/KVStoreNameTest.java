/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;

import oracle.kv.TestBase;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the {@link KVStoreName} class.
 */
public class KVStoreNameTest extends TestBase {
    private static final int startPort = 5000;
    private CreateStore store;

    @Before
    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        KVStoreName.latestKVStoreName = null;
    }

    @After
    @Override
    public void tearDown()
        throws Exception {

        if (store != null) {
            store.shutdown(true);
        }
        super.tearDown();

        LoggerUtils.closeAllHandlers();
    }

    @Test
    public void testNoService() {
        assertEquals(null, KVStoreName.getKVStoreName());
    }

    @Test
    public void testNull() {
        String storeName = "testNullName";
        KVStoreName.noteKVStoreName(storeName);
        assertEquals(storeName, KVStoreName.getKVStoreName());
        KVStoreName.noteKVStoreName(null);
        assertEquals(storeName, KVStoreName.getKVStoreName());
    }

    @Test
    public void testBasic() throws Exception {
        String storeName = "testBasicName";
        store = new CreateStore(storeName,
                                startPort,
                                1,     /* Storage nodes */
                                1,     /* Replication factor */
                                100,   /* Partitions */
                                1,     /* Capacity */
                                CreateStore.MB_PER_SN,
                                true /* Use threads */,
                                null /* mgmtImpl */);
        if (SECURITY_ENABLE) {
            System.setProperty("javax.net.ssl.trustStore",
                               store.getTrustStore().getPath());
        }
        store.start();
        assertEquals(storeName, KVStoreName.getKVStoreName());
    }
}
