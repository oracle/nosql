/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.StatementResult;
import oracle.kv.StoreIteratorException;
import oracle.kv.impl.api.table.TableTestBase;
import oracle.kv.impl.test.ExceptionTestHook;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests subscription based iterator added to executeSync and also supports 
 * to add new tests to compare with RMI.
 */
public class DmlSubscriberTest extends TableTestBase {

    final static String userTableStatement =
        "CREATE TABLE Users" +
        "(id INTEGER, firstName STRING, lastName STRING, age INTEGER," +
        "primary key (id))";

    @BeforeClass
    public static void staticSetUp() throws Exception {
        assumeTrue("Test requires async", AsyncControl.serverUseAsync);
        TableTestBase.staticSetUp();
    }

    @AfterClass
    public static void staticTearDown() throws Exception {
        TableTestBase.staticTearDown();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        store.executeSync(userTableStatement);
        addUsers(10);
    }

    @Override
    public void tearDown() throws Exception {
        KVStoreImpl.setSubscriberOnNextHook(null);
        super.tearDown();
    }

    private static void addUsers(int num) {
        TableAPI tableApi = store.getTableAPI();
        Table table = tableApi.getTable("Users");

        for (int i = 0; i < num; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("firstName", ("first " + i));
            row.put("lastName", ("last " + i));
            row.put("age", i + 10);
            tableApi.put(row, null, null);
        }
    }

    @Test
    public void testSubscriptionBasedExecuteSync() throws Exception {
        StatementResult res =
            store.executeSync("SELECT * FROM Users",
                    new ExecuteOptions().setResultsBatchSize(1));
        ((SubscriptionStatementResult) res).
            setIteratorNextHook(
                new ExceptionTestHook<Void, InterruptedException>() {
                    @Override
                    public void doHook(Void v) throws InterruptedException {
                        throw new InterruptedException(
                            "Iterator interrupted exception");
                    }
                });  
        // check the iterator
        TableIterator<RecordValue> iterator = res.iterator();
        assertNotNull(iterator);
        iterator.next(); // 1 result is already cached during executeSync call
        try {
            iterator.next();
            fail("Expected next call to fail");
        } catch (StoreIteratorException e) {
            assertTrue(true);
        }
    }

    @Test
    public void testEnableSyncWithExecuteOptions() throws Exception {
        StatementResult res =
            store.executeSync("SELECT * FROM Users",
                    new ExecuteOptions().setAsync(false));
        if (res instanceof SubscriptionStatementResult) {
            fail("executeSync should not use subscription iterator");
        }
    }

    @Test
    public void testEnableSyncWithSystemProperty() throws Exception {
        System.setProperty(
                ExecuteOptions.USE_SUBSCRIPTION_ITERATOR, "false");
        StatementResult res =
            store.executeSync("SELECT * FROM Users");
        if (res instanceof SubscriptionStatementResult) {
            fail("executeSync should not use subscription iterator");
        }
        System.clearProperty(ExecuteOptions.USE_SUBSCRIPTION_ITERATOR);
    }

    @Test
    public void testExecuteSyncUnBlocksWhenClosed() throws Exception {
        KVStoreImpl.setSubscriberOnNextHook(
                syncDone -> {
                if (syncDone) {
                    throw new RuntimeException("Skip results");
                }
                });
        StatementResult res = store.executeSync("SELECT * FROM Users",
                    new ExecuteOptions().setResultsBatchSize(1));
        final TableIterator<RecordValue> iterator = res.iterator();
        iterator.next(); //first result is cached 

        final AtomicReference<RuntimeException> t1Exception =
            new AtomicReference<>();
        Thread t1 = new Thread(()-> {
                try {
                    //wait for result
                    if (iterator.hasNext()) {
                        t1Exception.set(
                            new RuntimeException(
                                "iterator never returns next element"));
                    }
                } catch (RuntimeException e) {
                    t1Exception.set(e);
                }
        });
        t1.setDaemon(true);
        t1.start();
        Thread t2 = new Thread(()-> {
                /* Closing the iterator should cause thread t1 to exit */
                iterator.close();
        });
        t2.setDaemon(true);
        t2.start();
        t1.join(1000);
        assertTrue("iterator should not have blocked", !t1.isAlive());
        if (t1Exception.get() != null) {
            throw t1Exception.get();
        } 
     }
}
