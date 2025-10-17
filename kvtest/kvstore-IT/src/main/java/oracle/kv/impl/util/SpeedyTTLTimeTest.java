/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;
import oracle.kv.TestBase;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Ensures the SpeedyTTLTime works with TTL.
 */
public class SpeedyTTLTimeTest extends TestBase {

    /**
     * Tests that SpeedyTTLTime can advance time in the TTL mechanisms as
     * expected.
     */
    @Test
    public void testInsertWithTTL() throws Exception {
        /* Creates the store and the table. */
        final String storeName = "kvstore";
        final int port = 5000;
        final CreateStore createStore = new CreateStore(storeName, port,
            1 /* nsns */, 1 /* rf */, 1 /* partitions */, 1 /* capacity */,
            2 /* mb */, true /* use threads */, null);
        createStore.start();
        final KVStore kvstore = KVStoreFactory.getStore(
            new KVStoreConfig(storeName, String.format("localhost:%s", port)));
        final String ddl = "create table users " +
            "(id integer, name string, primary key (id))";
        final StatementResult result = kvstore.executeSync(ddl);
        assertTrue(result.isSuccessful());
        /*
         * Starts the TTL simulation with 1 second real time equal to 1 hour TTL
         * time.
         */
        final SpeedyTTLTime speedyTTLTime = new SpeedyTTLTime(1000);
        speedyTTLTime.start();
        /* Inserts a row with 1 hour TTL. */
        final TableAPI api = kvstore.getTableAPI();
        final Table table = api.getTable("users");
        final Row row = table.createRow();
        row.put("id", 1);
        row.put("name", "1");
        row.setTTL(TimeToLive.ofHours(1));
        api.put(row, null /* returnRow */, null/* writeOptions */);
        final PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", 1);
        /* Check immediately and the row should be there. */
        assertNotNull(api.get(pk, null));
        /*
         * Sleep two second (just to be safe) and check. The row should be
         * expired.
         */
        Thread.sleep(2000);
        assertNull(api.get(pk, null));
    }
}
