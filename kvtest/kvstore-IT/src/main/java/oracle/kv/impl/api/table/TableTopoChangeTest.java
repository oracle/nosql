/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.EntryStream;
import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.CreateStore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test table operations during topology changes. */
public class TableTopoChangeTest extends TestBase {
    private static final int startPort = 5000;
    private CreateStore createStore;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestStatus.setManyRNs(true);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (createStore != null) {
            createStore.shutdown();
            createStore = null;
        }
        super.setUp();
    }

    /**
     * Test for KVSTORE-2173, where calling bulk put APIs while adding a shard
     * got NullPointerExceptions.
     */
    @Test
    public void testBulkPutNewShard() throws Exception {

        /* Create store with two SNs... */
        createStore = new CreateStore(
            kvstoreName, startPort, 2, /* numStorageNodes */ 1, /* rf */
            20, /* numPartitions */ 1 /* capacity */,
            CreateStore.MB_PER_SN, true /* useThreads */,
            null /* mgmtImpl */);

        /* ... but only use one so there is a single shard */
        createStore.setPoolSize(1);
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();

        /* Do table bulk puts */
        final DoBulkPut doBulkPut = new DoBulkPut();
        doBulkPut.start();

        /* Now add a shard */
        cs.copyCurrentTopology("topo2");
        cs.redistributeTopology("topo2", "AllStorageNodes");
        final int planId = cs.createDeployTopologyPlan(null, "topo2", null);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);

        /* Stop bulkPut and make sure it worked */
        doBulkPut.stop = true;
        doBulkPut.join(30000);
        assertFalse(doBulkPut.isAlive());
        if (doBulkPut.exception.get() != null) {
            throw doBulkPut.exception.get();
        }
    }

    private class DoBulkPut extends Thread {
        final AtomicReference<RuntimeException> exception =
            new AtomicReference<>();
        volatile boolean stop;

        @Override
        public void run() {
            try {
                final KVStore store =
                    KVStoreFactory.getStore(createStore.createKVConfig());
                store.executeSync(
                    "CREATE TABLE people" +
                    " (name STRING, age INTEGER, PRIMARY KEY (name))");
                final TableAPI tableAPI = store.getTableAPI();
                final Table table = tableAPI.getTable("people");
                for (int i = 0; !stop; i++) {
                    final Row row = table.createRow();
                    row.put("name", "person" + i);
                    row.put("age", i);
                    final SingleRowStream stream = new SingleRowStream(row);
                    tableAPI.put(singletonList(stream),
                                 null /* bulkWriteOptions */);
                    stream.await(20000);
                }
            } catch (RuntimeException e) {
                exception.set(e);
            }
        }
    }

    /** Stream with one Row */
    static class SingleRowStream implements EntryStream<Row> {
        final Row row;
        private boolean supplied;
        private boolean completed;

        SingleRowStream(Row row) {
            this.row = row;
        }

        /**
         * Wait for the specified number of milliseconds for the processing of
         * the stream to complete. Throws IllegalStateException if the stream
         * is not complete by the timeout.
         */
        synchronized void await(long timeoutMillis) {
            final long until = System.currentTimeMillis() + timeoutMillis;
            while (!completed) {
                final long wait = until - System.currentTimeMillis();
                if (wait <= 0) {
                    throw new IllegalStateException(
                        "Not completed in " + timeoutMillis + " ms");
                }
                try {
                    Thread.sleep(wait);
                } catch (InterruptedException e) {
                    throw new IllegalStateException("Interrupted", e);
                }
            }
        }

        @Override
        public String name() {
            return "SingleRowStream[" + row + "]";
        }

        @Override
        public synchronized Row getNext() {
            if (supplied) {
                return null;
            }
            supplied = true;
            return row;
        }

        @Override
        public synchronized void completed() {
            completed = true;
            notifyAll();
        }

        @Override
        public void keyExists(Row r) {
            fail("Unexpected call to keyExists: " + r);
        }

        @Override
        public void catchException(RuntimeException e, Row r) {
            throw new RuntimeException(
                "Unexpected call to catchException for row " + r + ": " + e,
                e);
        }
    }
}
