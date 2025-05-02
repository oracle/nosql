/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.PasswordCredentials;
import oracle.kv.StatementResult;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.systables.SGAttributesTableDesc;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.ThreadUtils.ThreadPoolExecutorAutoClose;
import oracle.kv.table.FieldValueFactory;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.CreateStore;
import oracle.kv.util.CreateStore.SecureUser;
import oracle.kv.util.TableTestUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SequenceGeneratorSecurityTest extends SecureTestBase {
    /*
     * Test sequence generator in a secure store.
     */
    private static KVStore store;
    private static String password;

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        password = "QWEqwe@@@123";
        SecureUser user0 = new SecureUser("Admin", password, true);
        users.add(user0);
        SecureUser user1 = new SecureUser("user1", password, false);
        users.add(user1);
        SecureUser user2 = new SecureUser("user2", password, true);
        users.add(user2);
        SecureUser user3 = new SecureUser("user3", password, true);
        users.add(user3);
        SecureUser user4 = new SecureUser("user4", password, true);
        users.add(user4);
        SecureUser user5 = new SecureUser("user5", password, true);
        users.add(user5);
        SecureUser user6 = new SecureUser("user6", password, true);
        users.add(user6);
        numSNs = 3;
        repFactor = 3;

        startup();

        store = loginKVStoreUser("Admin", password);
        createStore.grantRoles("Admin", "readwrite");
        for (int i = 1; i <= 5; i++) {
            createStore.grantRoles("user" + i, "readwrite");
        }
        waitForTable(store.getTableAPI(), SGAttributesTableDesc.TABLE_NAME);
    }

    @Override
    public void setUp()
        throws Exception {

        /*
         * The CreateStore model executes the SNA in the same process as
         * the client.  When we run a tests that deliberately botches the
         * client transport config, we need to repair that config before
         * running additional tests.
         */
        createStore.getStorageNodeAgent(0).resetRMISocketPolicies();
    }

    /**
     * Creates a KVStoreConfig for use in tests.
     */
    static KVStoreConfig createKVConfig(CreateStore cs) {
        final KVStoreConfig config = cs.createKVConfig();
        config.setDurability
            (new Durability(Durability.SyncPolicy.WRITE_NO_SYNC,
                            Durability.SyncPolicy.WRITE_NO_SYNC,
                            Durability.ReplicaAckPolicy.ALL));
        return config;
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {
        shutdown();
    }

    @Override
    public void tearDown()
        throws Exception {
        store.getStats(true);
        TableTestUtils.removeTables(store.getTableAPI().getTables(),
                                    createStore.getAdmin(),
                                    store);
        int total = 0;
        for (Table table : store.getTableAPI().getTables().values()) {
            if (!((TableImpl)table).isSystemTable()) {
                total++;
            }
        }
        assertTrue(total == 0);

    }


    @Test
    public void testMultiUsersAlterTable() throws InterruptedException {
        testMultiUsersAlterTableInternal("INTEGER");
        testMultiUsersAlterTableInternal("LONG");
        testMultiUsersAlterTableInternal("NUMBER");

    }

    private void testMultiUsersAlterTableInternal(String type) throws InterruptedException {
        StatementResult sr =
            store.executeSync("CREATE Table Test_multiUsersAlter" + type +
            "(id " + type +  " GENERATED ALWAYS AS IDENTITY " +
            "(START WITH 1 INCREMENT BY 2 MAXVALUE 100 CACHE 10 CYCLE)," +
            " name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_multiUsersAlter" + type);

        try (final ThreadPoolExecutorAutoClose executor =
             new ThreadPoolExecutorAutoClose(
                 0, 10, 0L, TimeUnit.MILLISECONDS,
                 new SynchronousQueue<Runnable>(),
                 new KVThreadFactory("testMultiUsersAlter" + type, null))) {

            /*First user run ALTER*/
            Runnable r1 = new Runnable() {
                @Override
                public void run() {
                    KVStore userStore = createStore.getSecureStore(
                        new PasswordCredentials("user3",
                                                password.toCharArray()));
                    try {
                        /*wait for user2 to fill the cache*/
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                    StatementResult sr1 =
                        userStore.executeSync(
                            "ALTER Table Test_multiUsersAlter" + type +
                            " (MODIFY id GENERATED ALWAYS AS IDENTITY " +
                            "(START WITH 0 INCREMENT BY 3 MAXVALUE 6" +
                            " CACHE 1 NO CYCLE))");
                    assertTrue(sr1.isSuccessful());
                    assertTrue(sr1.isDone());
                }
            };

            /*Second User insert rows into table*/
            Runnable r2 = new Runnable() {
                @Override
                public void run() {
                    KVStore userStore = createStore.getSecureStore(
                        new PasswordCredentials("user2",
                                                password.toCharArray()), 5000);
                    TableAPI userApi = userStore.getTableAPI();
                    Table userTable = userApi.getTable("Test_multiUsersAlter" +
                                                       type);
                    Row row = userTable.createRow();
                    row.put("name", "jim");
                    userApi.put(row, null, null);

                    try {
                        Thread.sleep(20000);
                    } catch (InterruptedException e) {
                    }
                    /*Now attribute cache is timed out. New attributes will be
                     *used*/

                    userTable = userApi.getTable("Test_multiUsersAlter" + type);
                    row = userTable.createRow();
                    row.put("name", "joe");
                    userApi.put(row, null, null);

                    row = userTable.createRow();
                    row.put("name", "cezar");
                    userApi.put(row, null, null);
                }
            };

            executor.submit(r1);
            executor.submit(r2);

            executor.awaitTermination(30, TimeUnit.SECONDS);

            PrimaryKey pk1 = table.createPrimaryKey();
            PrimaryKey pk2 = table.createPrimaryKey();

            if (type.equals("INTEGER")) {
                pk1.put("id", 0);
                pk2.put("id", 3);
            } else if (type.equals("LONG")) {
                pk1.put("id", (long)0);
                pk2.put("id", (long)3);
            } else {
                pk1.put("id",
                        FieldValueFactory.createNumber(new BigDecimal(0)));
                pk2.put("id",
                        FieldValueFactory.createNumber(new BigDecimal(3)));
            }

            Row row1 = getRow(pk1, api);
            Row row2 = getRow(pk2, api);

            assertNotNull(row1);
            assertEquals("joe", row1.get("name").asString().toString());

            assertNotNull(row2);
            assertEquals("cezar", row2.get("name").asString().toString());
        }
    }


    @Test
    public void testMultiUsers() throws InterruptedException {
        StatementResult sr =
            store.executeSync("CREATE Table Test_testMultiUsers" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY," +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());


        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_testMultiUsers");
        /*test the case that multiple users insert into a table.*/
        try (final ThreadPoolExecutorAutoClose executor =
             new ThreadPoolExecutorAutoClose(
                 0, 10, 0L, TimeUnit.MILLISECONDS,
                 new SynchronousQueue<Runnable>(),
                 new KVThreadFactory("testMultiUsers", null))) {
            Map<Integer, String> nameMap =
                new ConcurrentHashMap<Integer, String>();
            for (int i = 0; i < 5; i++) {
                final int j = i;
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        KVStore userStore = createStore.getSecureStore(
                            new PasswordCredentials("user" + (j+1),
                                                    password.toCharArray()));
                        TableAPI userApi = userStore.getTableAPI();
                        Table userTable =
                            userApi.getTable("Test_testMultiUsers");
                        Row row = userTable.createRow();
                        row.put("name", "smith" + (j+1));
                        userApi.put(row, null, null);
                        assertNotNull(row.get("id"));
                        nameMap.put(row.get("id").asInteger().get(),
                                    "smith" + (j+1));
                    }
                });
            }

            Thread.sleep(5000);

            for (int i = 0; i < 5; i++) {
                PrimaryKey pk = table.createPrimaryKey();
                pk.put("id", 1000*i+1);

                Row r = getRow(pk, api);
                assertNotNull(r);
                assertEquals(nameMap.get(1000*i+1),
                             r.get("name").asString().get());
            }
        }

    }

    @Test
    public void testPrivilege() throws Exception {
        StatementResult sr =
            store.executeSync("CREATE Table Test_testPrevilege" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY," +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("Test_testPrevilege");

        /*inserting row with privilege INSERT_TABLE*/
        sr = store.executeSync("CREATE ROLE WRITEAllow");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("GRANT INSERT_TABLE ON Test_testPrevilege TO WRITEAllow");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        createStore.grantRoles("user6", "WRITEAllow");

        KVStore userStore = createStore.getSecureStore(
            new PasswordCredentials("user6",
                                    password.toCharArray()));
        TableAPI userApi = userStore.getTableAPI();
        Table userTable = userApi.getTable("Test_testPrevilege");
        Row row = userTable.createRow();
        row.put("name", "smith");
        userApi.put(row, null, null);

        assertNotNull(row.get("id"));

        Thread.sleep(5000);

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", 1);

        Row r = getRow(pk, api);
        assertNotNull(r);
        assertEquals("smith", r.get("name").asString().get());

        /*inserting row with privilege INSERT_ANY_TABLE*/
        sr = store.executeSync("REVOKE INSERT_TABLE ON Test_testPrevilege FROM WRITEAllow");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("GRANT INSERT_ANY_TABLE TO WRITEAllow");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());


        row = userTable.createRow();
        row.put("name", "smith2");
        userApi.put(row, null, null);
        assertNotNull(row.get("id"));

        Thread.sleep(5000);

        pk = table.createPrimaryKey();
        pk.put("id", 2);

        r = getRow(pk, api);
        assertNotNull(r);
        assertEquals("smith2", r.get("name").asString().get());

    }

    @Test
    public void testPrivilegeCreateTable() throws Exception {

        StatementResult sr = store.executeSync("CREATE ROLE CREATEAllow");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("GRANT CREATE_TABLE_IN_NAMESPACE ON NAMESPACE " +
                               "sysdefault TO CREATEAllow");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        createStore.grantRoles("user6", "CREATEAllow");

        KVStore userStore = createStore.getSecureStore(
            new PasswordCredentials("user6",
                                    password.toCharArray()));

        /*Craete table with privilege CREATE_TABLE_IN_NAMESPACE*/
        sr = userStore.executeSync("CREATE Table Test_testPrivilegeCreateSG" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY," +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("REVOKE CREATE_TABLE_IN_NAMESPACE ON NAMESPACE "+
                               "sysdefault FROM CREATEAllow");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("GRANT CREATE_ANY_NAMESPACE TO CREATEAllow");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        /*Craete table with privilege CREATE_ANY_NAMESPACE*/
        sr = userStore.executeSync("CREATE Table Test_testPrivilegeCreateSG2" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY," +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
    }

    @Test
    public void testPrivilegeDeleteTable() throws Exception {

        StatementResult sr = store.executeSync("CREATE ROLE DELETEAllow");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("GRANT DROP_TABLE_IN_NAMESPACE ON NAMESPACE " +
                               "sysdefault TO DELETEAllow");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        createStore.grantRoles("user6", "DELETEAllow");

        KVStore userStore = createStore.getSecureStore(
            new PasswordCredentials("user6",
                                    password.toCharArray()));

        sr = store.executeSync("CREATE Table Test_PrivilegeDeleteTable" +
            "(id INTEGER GENERATED ALWAYS AS IDENTITY," +
            "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        /*Drop table with privilege DROP_TABLE_IN_NAMESPACE*/
        sr = userStore.executeSync("DROP TABLE Test_PrivilegeDeleteTable");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr =
           store.executeSync("REVOKE DROP_TABLE_IN_NAMESPACE ON NAMESPACE " +
                             "sysdefault FROM DELETEAllow");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("GRANT DROP_ANY_NAMESPACE TO DELETEAllow");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("CREATE Table Test_PrivilegeDeleteTable2" +
                               "(id INTEGER GENERATED ALWAYS AS IDENTITY," +
                               "name STRING, PRIMARY KEY (id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        /*Drop table with privilege DROP*/
        sr = userStore.executeSync("DROP TABLE Test_PrivilegeDeleteTable2");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

    }

    private static Row getRow(PrimaryKey pk, TableAPI api)
        throws InterruptedException {

        Row r = null;
        int count = 0;
        while (true) {
            r = api.get(pk, new ReadOptions(Consistency.ABSOLUTE, 1000,
                                            TimeUnit.MILLISECONDS));
            if (r != null) {
                break;
            }
            count++;
            if (count > 10) {
                break;
            }
            Thread.sleep(1000);
        }
        return r;
    }

}
