/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import oracle.kv.TestBase;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/** Tests for the {@link ManagedService} class */
public class ManagedServiceTest extends TestBase {

    private static final int startPort = 5000;

    private CreateStore createStore;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (createStore != null) {
            createStore.shutdown(true);
            createStore = null;
        }
        LoggerUtils.closeAllHandlers();
    }

    @Test
    public void testGetMainService() throws Exception {
        assertEquals(null, ManagedService.getMainService());

        createStore = new CreateStore(kvstoreName,
                                      startPort,
                                      1, /* numSns */
                                      1, /* rf */
                                      2, /* numPartitions */
                                      1, /* capacity */
                                      CreateStore.MB_PER_SN,
                                      true /* useThreads */,
                                      null /* mgmtImpl */);
        createStore.start();

        ManagedService mainService = ManagedService.getMainService();
        assertNotNull(mainService);
        assertNotNull(mainService.getLogger());
    }
}
