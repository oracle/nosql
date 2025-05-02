/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import oracle.kv.ExecutionFuture;
import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.KVVersion;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.impl.streamservice.MRT.Manager;
import oracle.kv.impl.streamservice.MRT.Request;
import oracle.kv.impl.xregion.XRegionTestBase;
import oracle.kv.table.TableAPI;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Tests converting MRT request messages from FastExternalizable to
 * Java serialization.
 */
public class MRUpgradeTest extends TestBase {

    private CreateStore createStore = null;
    private KVStore store;

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (createStore != null) {
            createStore.shutdown(true);
            createStore = null;
        }
    }

    @Test
    public void testRequestTableUpdate() throws Exception {

        /*
         * Trick the admin into thinking an agent has started and that
         * it has written its version in the response table. An agent version
         * needs to be set before the Admin starts so that it does set the
         * min. agent version to the store version.
         */
        MRTManager.forceAgentVersion = KVVersion.PREREQUISITE_VERSION;
        createStore = new CreateStore(kvstoreName,
                                      5000, 1, 1,
                                      2 /* numPartitions */,
                                      1 /* capacity */,
                                      CreateStore.MB_PER_SN, /* Memory_mb */
                                      true /* useThreads */,
                                      null /* mgmtImpl */);
        createStore.start();
        store = KVStoreFactory.getStore(createStore.createKVConfig());
        store = createStore.getInternalStoreHandle(store);

        /*
         * Create some multi-region tables. This will post messages to the
         * request table. They will remain there because there is no agent
         * present to respond.
         *
         * The messages are written using FastExternalizable because we
         * force the agent version to be an old version.
         */
        XRegionTestBase.setLocalRegionName(store, LOCAL_REGION, null, false);
        XRegionTestBase.createRegion(store, REMOTE_REGION, null, false);

        int nMessages = 2;
        final int N_TABLES = 5;
        for (int i = 0; i < N_TABLES; i++) {
            addTable(i);
            nMessages++;
        }

        /*
         * Check that all of the request messages are there and that they
         * are in the old format.
         */
        final TestManager tm = new TestManager();
        tm.checkRequestMessages(nMessages);

        /* "Upgrade" the agent */
        MRTManager.forceAgentVersion = KVVersion.CURRENT_VERSION;

        /*
         * In a deployment, the messages will be converted once the agent
         * is upgraded when a) the Admin updates the store version (after a
         * SW upgrade) and b) when a request is posted.
         *
         * This test will post a message to force the message update
         */
         addTable(N_TABLES);
         nMessages++;

         /*
          * Messages should be converted to Java. Note that the conversion
          * by the Admin is asynchronous in the message cleaner thread. In
          * _theory_ it could be delayed causing the check to fail. In the
          * test the conversion is being completed before the addTable()
          * returns.
          */
         tm.checkRequestMessages(nMessages);
    }

    private void addTable(int i) throws Exception {
        String statement = "CREATE TABLE testTable" + i +
                           " (id INTEGER, name STRING, PRIMARY KEY (id))" +
                           " IN REGIONS " + REMOTE_REGION;
        final ExecutionFuture ef = store.execute(statement);
        StatementResult result = ef.get();
        assert result.isSuccessful();
    }

    /* Dummy manager, quick hack to use the iterator */
    private class TestManager extends Manager {

        TestManager() {
            super(null);
        }

        @Override
        protected TableAPI getTableAPI() {
            return store.getTableAPI();
        }

        @Override
        protected short getMaxSerialVersion() {
            throw new IllegalStateException("Should not be called");
        }

        @Override
        protected void handleIOE(String error, IOException ioe) {
            throw new IllegalStateException(error, ioe);
        }

        void checkRequestMessages(int expected) {
            final RequestIterator itr = getRequestIterator(0, TimeUnit.SECONDS);
            int count = 0;
            while (itr.hasNext()) {
                final Request req = itr.next();
                /*
                 * Check only the messages which used FastExternalizable for
                 * serialization.
                 */
                switch (req.getType()) {
                    case CREATE_TABLE :
                    case UPDATE_TABLE :
                    case DROP_TABLE :
                    case CREATE_CHILD :
                    case DROP_CHILD :
                    case CREATE_REGION :
                        final short serialVersion = req.getSerialVersion();
                        /* the serial version of all messages should be V3.*/
                        assertEquals("Incorrect serial version for " + req,
                                     Request.V3, serialVersion);
                        break;
                    case DROP_REGION :
                    case STORE_VERSION :
                        break;
                    default : throw new IllegalStateException("unreachable");
                }
                count++;
            }
            assertEquals("Wrong number of request messages", expected, count);
        }
    }
}
