/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.proxy.kv;

import java.util.ArrayList;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.ListTablesResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.MapValue;

/**
 * A small test to sanity check a proxy + kvstore that's been started
 * by other, external means.
 */
public class KVSmoke {

    protected String endpoint;
    protected StoreAccessTokenProvider authProvider;
    protected NoSQLHandle myhandle;

    public static void  main(String[] argv) {
        KVSmoke smoke = new KVSmoke();
        if (argv.length == 0) {
            System.err.println("Usage: java " +
                "oracle.nosql.proxy.kv.KvSmoke <proxyhost:proxyport>");
        }

        smoke.setupHandle(argv[0]);
        smoke.doSmoke();
        smoke.shutdown();
    }


    private void setupHandle(String endpoint) {
        authProvider = new StoreAccessTokenProvider();
        NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint);
        config.setAuthorizationProvider(authProvider);

        /*
         * Open the handle
         */
        myhandle = NoSQLHandleFactory.createNoSQLHandle(config);
    }

    private void doSmoke() {
        final String tableName = "test";
        final String createTableStatement =
            "CREATE TABLE IF NOT EXISTS " + tableName +
            "(id INTEGER, " +
            " pin INTEGER, " +
            " name STRING, " +
            " PRIMARY KEY(SHARD(pin), id))";

        TableRequest tableRequest = new TableRequest()
            .setStatement(createTableStatement);
        TableResult tres = myhandle.tableRequest(tableRequest);

        tres.waitForCompletion(myhandle, 60000, 1000);
        System.out.println("tres=" + tres + " " + tres.getTableState());

        /*
         * PUT a row
         */
        MapValue value = new MapValue().put("id", 1).
            put("pin", "654321").put("name", "test1");

        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        PutResult putResult = myhandle.put(putRequest);
        System.out.println("put version: " + putResult.getVersion());

        /*
         * GET the row
         */
        MapValue key = new MapValue().put("id", 1).put("pin", "654321");

        GetRequest getRequest = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        GetResult getRes = myhandle.get(getRequest);
        System.out.println("value: " +
                           getRes.getValue().get("name").asString().getValue());

        /*
         * PUT a second row using JSON
         */
        String jsonString =
            "{\"id\": 2, \"pin\": 123456, \"name\":\"test2\"}";

        putRequest = new PutRequest()
            .setValueFromJson(jsonString, null)
            .setTableName(tableName);
        putResult = myhandle.put(putRequest);
        System.out.println("putResult = " + putResult);
    }

    private void shutdown() {
        if (myhandle != null) {
            dropAllTables(myhandle);
            myhandle.close();
        }

        if (authProvider != null) {
            authProvider.close();
            authProvider = null;
        }
    }

    protected void dropAllTables(NoSQLHandle nosqlHandle) {

        /* get the names of all tables under this tenant */
        ListTablesRequest listTables = new ListTablesRequest();
        ListTablesResult lres = nosqlHandle.listTables(listTables);
        ArrayList<TableResult> droppedTables = new ArrayList<TableResult>();
        /* clean up all the tables */
        for (String tableName: lres.getTables()) {
            /* on-prem config may find system tables, which can't be dropped */
            if (tableName.startsWith("SYS$")) {
                continue;
            }

            /* ignore, but note exceptions */
            try {
                TableResult tres = dropTableWithoutWait(nosqlHandle, tableName);
                droppedTables.add(tres);
            } catch (Exception e) {
                System.err.println("DropAllTables: drop fail, table "
                                   + tableName + ": " + e);
            }
        }

        /*
         * don't wait for ACTIVE state. This may mean occasional
         * failures but as long as tests pass that is ok.
         */

        /* wait for all tables dropped */
        for (TableResult tres: droppedTables) {
            /* ignore, but note exceptions */
            try {
                tres.waitForCompletion(nosqlHandle, 100000, 1000);
            } catch (Exception e) {
                System.err.println("DropAllTables: drop wait fail, table "
                                   + tres + ": " + e);
            }
        }
    }

    private TableResult dropTableWithoutWait(NoSQLHandle nosqlHandle,
                                             String tableName) {
        final String dropTableDdl = "drop table if exists " + tableName;
        TableRequest tableRequest = new TableRequest()
                .setStatement(dropTableDdl)
                .setTimeout(100000);
        return nosqlHandle.tableRequest(tableRequest);
    }

}
